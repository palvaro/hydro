//! The request/response service of [`super::super::rpc_retry`] with a bounded server queue that
//! rejects early.
//!
//! The client is the `rpc_retry` client with one addition: a request the server rejects is
//! re-sent after [`RetryPolicy::reject_backoff_ticks`] rather than after the timeout, and a
//! rejection counts as one of the request's [`RetryPolicy::max_attempts`] sends. The server keeps
//! incoming requests in a FIFO and serves at most [`ServerConfig::max_per_tick`] per tick; when
//! the queue left after serving is deeper than [`ServerConfig::max_backlog`], it drops the oldest
//! excess requests and answers each of them with a rejection in the same tick. With
//! `max_backlog = None` the queue is unbounded and the program is `rpc_retry` exactly.
//!
//! # Mechanism and knob
//!
//! `rpc_retry` collapses because a queue deeper than `timeout_ticks * max_per_tick` makes every
//! queued request time out and be sent again, so the server serves each request several times
//! and offered load exceeds capacity after the trigger has ended. A queue bounded at less than
//! `timeout_ticks * max_per_tick` keeps the *server's* queueing delay below the timeout, so a
//! request that is queued is served once, and a request that is not queued learns so at once
//! instead of after a timeout. The rejected requests are re-sent, but a re-send costs the server
//! nothing unless it is admitted, and then it is served once.
//!
//! The bound is a mitigation, not a removal of the hazard. The client still re-sends any request
//! whose reply has not arrived within `timeout_ticks`, and the server, which does not remember
//! what it has served, serves the re-send again. The bound caps the delay the server itself can
//! impose; it cannot cap the delay the network imposes, and a reply held past the timeout still
//! produces a redundant send and a redundant serve. Both configurations are therefore hazardous.
//! The knob is `max_backlog`: `Some(100)` (a server delay of at most 20 ticks against a timeout
//! of 40) recovers from the load trigger, `None` collapses under it exactly as `rpc_retry` does.
//!
//! # Timer parameters
//!
//! - `client_clock`: one element per logical client tick; timeouts, backoffs and latencies are
//!   measured in these ticks. A deployment wires `client.source_interval(period)` into it; the
//!   simulation feeds it from `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Baseline 2 requests per round against a capacity of 5; the trigger offers 12 per round during
//! rounds 100 to 160; timeout 40 rounds, 3 sends per request, backoff after rejection 10 rounds.
//! Tail is rounds 600 to 800, where baseline completions would be 400.
//!
//! | run | max_backlog | trigger | tail completions | tail served first / again | queue at 600 -> 800 | whole run: sent / served / rejected / abandoned for 2200 requests | label |
//! |---|---|---|---|---|---|---|---|
//! | bounded queue | Some(100) | yes | 400 | 400 / 0 | 0 -> 0 (peak 100, 65 at round 200, empty from round 250) | 3033 / 1968 / 1065 / 232 | no ground truth; recovers, and the tool is expected to find the retry (see hold experiment) |
//! | unbounded | None | yes | 0 | 333 / 667 | 1038 -> 1237 | 4937 / 3700 / 0 / 978 | hazardous (collapses) |
//! | no trigger | Some(100) | no | 400 | 400 / 0 | 0 -> 0 | 1600 / 1600 / 0 / 0 | healthy |
//!
//! The bounded run never serves an id twice in 800 rounds, and total sends are 1.38 times the
//! input against the 3 times that `max_attempts` allows. The price is the 232 requests abandoned
//! after three rejections during the trigger, which the unbounded run also abandons (978 of them)
//! without ever recovering. The unbounded run's numbers are the numbers of `rpc_retry`.
//!
//! The bounded configuration's label rests on the hold experiment
//! (`held_replies_make_the_server_redo_work_under_the_bound`). The server has no timer to
//! withhold and the simulator will not let a tick run without releasing something from its only
//! loaded hook, so the experiment lets the fuzzer choose schedules and reads the delay off each
//! run: 24 requests, one per round, four clock elements per round, timeout 2, queue bound 100
//! (never reached), 512 schedules. Under the prompt schedule (the no-trigger control) the 24
//! requests cost 24 sends and 24 serves. Under the fuzzed schedules, 511 of 512 re-sent, the worst
//! cost 41 sends and 41 serves for the same 24 requests, and mean re-sends grew with the longest
//! reply delay a schedule imposed: 5.00 at 2 ticks, 5.78 at 3, 8.20 at 4. The same input costs
//! more work the longer replies are held, which is the hazardous label.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::amplification::{SimOutputs, amplification_check};
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

/// What the client puts on the wire: the client tick the request was issued in, under a
/// client-assigned id.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Request {
    pub id: u64,
    pub issued_tick: u64,
}

/// The server's answer: served, or dropped from a full queue.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Reply {
    Ok { id: u64 },
    Rejected { id: u64 },
}

/// Client-side bookkeeping for a request that has been sent but not yet answered. Times are
/// logical client ticks.
#[derive(Clone, Copy, Debug)]
pub struct InFlight {
    pub issued_tick: u64,
    pub first_sent: u64,
    pub last_sent: u64,
    /// Sends so far: 1 after the first send.
    pub attempt: u32,
    /// Set after a rejection: the tick at which the request is sent again.
    pub resend_at: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
enum Verdict {
    Keep(InFlight),
    Retry(InFlight),
    Abandon(InFlight),
}

/// A request that received its first `Ok`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    /// Client ticks from the first send to the `Ok`.
    pub latency_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Client ticks the client waits for an answer before re-sending.
    pub timeout_ticks: u64,
    /// Total number of sends per request (1 = never retry).
    pub max_attempts: u32,
    /// Client ticks the client waits after a rejection before re-sending.
    pub reject_backoff_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Requests served per server tick: the server's capacity.
    pub max_per_tick: u32,
    /// Deepest queue the server keeps after serving; older requests beyond it are rejected.
    /// `None` means unbounded.
    pub max_backlog: Option<usize>,
}

/// Everything observable about a run.
#[derive(SimOutputs)]
pub struct RpcOutputs<'a> {
    /// Requests that received their first `Ok`, at the client.
    pub completed: Stream<Completion, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Ids given up on after `max_attempts` sends.
    pub abandoned: Stream<u64, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Every request the client puts on the wire (first sends and re-sends).
    pub outgoing: Stream<Request, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request the server serves.
    pub processed: Stream<Request, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request the server drops from a full queue.
    pub rejected: Stream<Request, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Depth of the server's queue at the end of each server tick.
    pub backlog_trace: Stream<usize, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the client/server program. `requests` carries, per application request, the client tick
/// it was issued in; ids are assigned in arrival order.
#[amplification_check(
    name = bounded_100,
    workload(requests = 2),
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3, reject_backoff_ticks: 10 },
    server_config = ServerConfig { max_per_tick: 5, max_backlog: Some(100) },
)]
#[amplification_check(
    name = unbounded,
    workload(requests = 2),
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3, reject_backoff_ticks: 10 },
    server_config = ServerConfig { max_per_tick: 5, max_backlog: None },
)]
pub fn rpc_with_bounded_queue<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<u64, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded>,
    policy: RetryPolicy,
    server_config: ServerConfig,
) -> RpcOutputs<'a> {
    let RetryPolicy {
        timeout_ticks,
        max_attempts,
        reject_backoff_ticks,
    } = policy;
    let ServerConfig {
        max_per_tick,
        max_backlog,
    } = server_config;
    // `q!` closures capture primitives; an unbounded queue is one deeper than anything reachable.
    let max_backlog = max_backlog.unwrap_or(usize::MAX);

    let (replies_complete, replies) = client
        .forward_ref::<Stream<Reply, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>>();

    // ---- Client: logical clock, outstanding table, timeouts, rejections, re-sends -------------
    let (outgoing, completed, abandoned) = sliced! {
        let clock = use::batch(client_clock.enumerate(), nondet!(/** batching only shifts which client tick observes a clock element; every element still advances the clock */));
        let new_requests = use::batch(requests.enumerate(), nondet!(/** batching only shifts which client tick first sends a request, i.e. its timestamps */));
        let replies = use::batch(replies, nondet!(/** batching only affects when an answer is observed and when a timeout or backoff fires */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, InFlight, Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let new_requests = new_requests.map(q!(|(i, issued_tick)| Request { id: i as u64, issued_tick }));

        // An `Ok` completes an outstanding request; an `Ok` wins over a `Rejected` for the same
        // id in the same tick. Answers to ids no longer outstanding are ignored.
        let ok_ids = replies
            .clone()
            .filter_map(q!(|r| match r {
                Reply::Ok { id } => Some(id),
                _ => None,
            }))
            .unique();
        let rejected_ids = replies
            .filter_map(q!(|r| match r {
                Reply::Rejected { id } => Some(id),
                _ => None,
            }))
            .unique()
            .filter_not_in(ok_ids.clone());
        let completed = ok_ids
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(outstanding.clone())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((id, (_, inflight)), now)| Completion {
                id,
                latency_ticks: now - inflight.first_sent,
            }));

        let still = outstanding.filter_key_not_in(ok_ids);

        // A rejected request backs off, or is abandoned if it has no sends left.
        let judged_rejected = rejected_ids
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(still.clone())
            .map(q!(|(_, f)| f))
            .cross_singleton(now_cur.clone())
            .map(q!(move |(f, now)| if f.attempt < max_attempts {
                Verdict::Keep(InFlight {
                    resend_at: Some(now + reject_backoff_ticks),
                    ..f
                })
            } else {
                Verdict::Abandon(f)
            }));

        // Everything else is judged against its backoff or the timeout once per tick.
        let judged_waiting = still
            .filter_key_not_in(rejected_ids)
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .map(q!(move |(f, now)| {
                let resend = InFlight {
                    last_sent: now,
                    attempt: f.attempt + 1,
                    resend_at: None,
                    ..f
                };
                match f.resend_at {
                    Some(at) => {
                        if now >= at {
                            Verdict::Retry(resend)
                        } else {
                            Verdict::Keep(f)
                        }
                    }
                    None => {
                        if now - f.last_sent < timeout_ticks {
                            Verdict::Keep(f)
                        } else if f.attempt < max_attempts {
                            Verdict::Retry(resend)
                        } else {
                            Verdict::Abandon(f)
                        }
                    }
                }
            }));
        let judged = judged_rejected.chain(judged_waiting);

        let kept = judged.clone().filter_map(q!(|v| match v {
            Verdict::Keep(f) => Some(f),
            _ => None,
        }));
        let retried = judged.clone().filter_map(q!(|v| match v {
            Verdict::Retry(f) => Some(f),
            _ => None,
        }));
        let abandoned = judged
            .filter_map(q!(|v| match v {
                Verdict::Abandon(f) => Some(f),
                _ => None,
            }))
            .keys();

        let retries_out = retried
            .clone()
            .entries()
            .map(q!(|(id, f)| Request { id, issued_tick: f.issued_tick }))
            .sort();

        let newly_sent = new_requests.clone().cross_singleton(now_cur.clone()).map(q!(|(r, now)| (
            r.id,
            InFlight {
                issued_tick: r.issued_tick,
                first_sent: now,
                last_sent: now,
                attempt: 1,
                resend_at: None,
            }
        )));

        // Keys of `kept`, `retried` and `newly_sent` are pairwise disjoint, so `first()` is exact.
        outstanding = kept.chain(retried).chain(newly_sent.into_keyed()).first();

        (new_requests.chain(retries_out), completed, abandoned)
    };

    // ---- Server: FIFO, bounded work per tick, bounded queue with early rejection -------------
    let incoming = outgoing.clone().send(server, TCP.fail_stop().bincode());

    let (processed, rejected, replies_out, backlog_trace) = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Request, Tick<_>, Bounded, TotalOrder>>();

        let queued = backlog.chain(arrivals).enumerate();
        let total = queued.clone().count();
        // Positions `[0, max_per_tick)` are served. Of the rest, the newest `max_backlog` stay
        // queued and anything older is rejected.
        let placed = queued.cross_singleton(total).map(q!(move |((i, req), total)| {
            let keep_from = (max_per_tick as usize).max(total.saturating_sub(max_backlog));
            if i < max_per_tick as usize {
                (0u8, req)
            } else if i < keep_from {
                (1u8, req)
            } else {
                (2u8, req)
            }
        }));
        let served = placed.clone().filter_map(q!(|(slot, req)| if slot == 0 { Some(req) } else { None }));
        let rejected = placed.clone().filter_map(q!(|(slot, req)| if slot == 1 { Some(req) } else { None }));
        backlog = placed.filter_map(q!(|(slot, req)| if slot == 2 { Some(req) } else { None }));

        let replies_out = served
            .clone()
            .map(q!(|req| Reply::Ok { id: req.id }))
            .chain(rejected.clone().map(q!(|req| Reply::Rejected { id: req.id })));

        (served, rejected, replies_out, backlog.clone().count().into_stream())
    };

    replies_complete.complete(replies_out.send(client, TCP.fail_stop().bincode()));

    RpcOutputs {
        completed,
        abandoned,
        outgoing,
        processed,
        rejected,
        backlog_trace,
    }
}

/// An open-loop workload for the tests: a fixed number of requests per client tick, with a
/// window in which the rate is higher. This lives in the harness, not in the program.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_per_tick: u32,
    pub trigger_per_tick: u32,
    /// Trigger window, in client ticks since start: `[trigger_start_tick, trigger_end_tick)`.
    pub trigger_start_tick: u64,
    pub trigger_end_tick: u64,
}

impl Workload {
    pub fn requests_at(&self, tick: u64) -> u32 {
        if tick >= self.trigger_start_tick && tick < self.trigger_end_tick {
            self.trigger_per_tick
        } else {
            self.baseline_per_tick
        }
    }

    pub fn total_requests(&self, rounds: u64) -> u64 {
        (0..rounds).map(|t| self.requests_at(t) as u64).sum()
    }
}

/// Simulation under the fixed prompt schedule. A round is one element on `client_clock` plus that
/// tick's requests, followed by quiescence, exactly as in `rpc_retry::sim_tests`.
#[cfg(test)]
mod sim_tests {
    use std::collections::HashSet;

    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        completed: u64,
        latency_sum: u64,
        abandoned: u64,
        /// Requests on the wire this round whose id had not been seen on the wire before.
        sent_first: u64,
        /// Requests on the wire this round whose id had been sent before: re-sends.
        sent_retry: u64,
        /// Requests served this round whose id had not been served before.
        served_first: u64,
        /// Requests served this round whose id had been served before: the server re-doing work.
        served_again: u64,
        /// Requests the server dropped from a full queue this round.
        rejected: u64,
        /// Server queue after this round's server tick (carried over if it did not run).
        backlog: usize,
    }

    fn run(workload: Workload, policy: RetryPolicy, server_config: ServerConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();

        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = rpc_with_bounded_queue(&client, &server, requests, client_clock, policy, server_config);
        let completed = outputs.completed.sim_output();
        let abandoned = outputs.abandoned.sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
        let rejected = outputs.rejected.sim_output();
        let backlog_trace = outputs.backlog_trace.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim().run_prompt(async move || {
            let mut backlog = 0usize;
            let mut sent_ids: HashSet<u64> = HashSet::new();
            let mut served_ids: HashSet<u64> = HashSet::new();
            for tick in 0..rounds as u64 {
                clock_send.send(());
                for _ in 0..workload.requests_at(tick) {
                    request_send.send(tick);
                }
                quiesce().await;

                let mut r = Round::default();
                for c in completed.collect_sorted::<Vec<_>>().await {
                    r.completed += 1;
                    r.latency_sum += c.latency_ticks;
                }
                r.abandoned = abandoned.collect_sorted::<Vec<_>>().await.len() as u64;
                while let Some(req) = outgoing.try_next().await {
                    if sent_ids.insert(req.id) {
                        r.sent_first += 1
                    } else {
                        r.sent_retry += 1
                    }
                }
                while let Some(req) = processed.try_next().await {
                    if served_ids.insert(req.id) {
                        r.served_first += 1
                    } else {
                        r.served_again += 1
                    }
                }
                r.rejected = rejected.collect::<Vec<_>>().await.len() as u64;
                while let Some(depth) = backlog_trace.try_next().await {
                    backlog = depth;
                }
                r.backlog = backlog;
                trace_ref.push(r);
            }
        });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    fn mean_latency(trace: &[Round], from: usize, to: usize) -> f64 {
        let completed = sum(trace, from, to, |r| r.completed);
        assert!(completed > 0, "no completions in rounds [{from}, {to})");
        sum(trace, from, to, |r| r.latency_sum) as f64 / completed as f64
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Baseline 2 requests per round against a capacity of 5 (40% utilization). The trigger
    /// offers 12 per round for 60 rounds, so the queue grows by 7 per round and reaches the bound
    /// of 100 by about round 114; from then on the server rejects about 7 per round, each rejected
    /// request is re-sent 10 rounds later and mostly rejected again, and requests start being
    /// abandoned after their third send from about round 134. Queueing delay never exceeds
    /// `100 / 5 = 20` rounds, below the 40-round timeout, so a queued request is never re-sent and
    /// the server never serves an id twice. When the trigger ends at round 160 the queue is at
    /// 100 and offered load is 2 per round plus the re-sends of the last rejections, which stop
    /// by about round 180; the queue drains at about 3 per round and is empty by about round 200.
    /// Total sends are bounded by `3 x 2200 = 6600` for the 2200 requests offered, and should
    /// come to about 2200 plus one or two re-sends for each of the roughly 300 rejected first
    /// sends.
    ///
    /// With `max_backlog = None` this is `rpc_retry` and should collapse as measured there: a
    /// queue of about 420 at round 160, every request sent three times, offered load 6 per round
    /// against 5 forever.
    pub(super) const WORKLOAD: Workload = Workload {
        baseline_per_tick: 2,
        trigger_per_tick: 12,
        trigger_start_tick: 100,
        trigger_end_tick: 160,
    };
    pub(super) const POLICY: RetryPolicy = RetryPolicy {
        timeout_ticks: 40,
        max_attempts: 3,
        reject_backoff_ticks: 10,
    };
    pub(super) const SERVER: ServerConfig = ServerConfig {
        max_per_tick: 5,
        max_backlog: Some(100),
    };

    pub(super) const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 115, 130, 159, 180, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: completed={} abandoned={} sent_first={} sent_retry={} served_first={} served_again={} rejected={} backlog={}",
                    r.completed, r.abandoned, r.sent_first, r.sent_retry, r.served_first, r.served_again, r.rejected, r.backlog
                );
            }
        }
    }

    fn print_totals(trace: &[Round]) {
        println!(
            "totals: sent first={} retry={} (input {}), served first={} again={}, rejected={}, abandoned={}, completed={}, peak backlog {}",
            sum(trace, 0, trace.len(), |r| r.sent_first),
            sum(trace, 0, trace.len(), |r| r.sent_retry),
            WORKLOAD.total_requests(trace.len() as u64),
            sum(trace, 0, trace.len(), |r| r.served_first),
            sum(trace, 0, trace.len(), |r| r.served_again),
            sum(trace, 0, trace.len(), |r| r.rejected),
            sum(trace, 0, trace.len(), |r| r.abandoned),
            sum(trace, 0, trace.len(), |r| r.completed),
            trace.iter().map(|r| r.backlog).max().unwrap(),
        );
    }

    #[test]
    fn early_rejection_keeps_the_server_from_redoing_work() {
        let trace = run(WORKLOAD, POLICY, SERVER, ROUNDS);
        print_trajectory(&trace);
        print_totals(&trace);

        let pre = &trace[10..100];
        assert!(pre.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.sent_retry == 0 && r.rejected == 0));
        assert_eq!(mean_latency(&trace, 10, 100), 0.0);

        // The trigger did fill the queue, and rejections happened.
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        assert_eq!(peak, 100, "the queue should have reached its bound, peak {peak}");
        assert!(sum(&trace, 100, 200, |r| r.rejected) > 200);
        // The server never serves an id twice, under any part of the run.
        assert_eq!(sum(&trace, 0, ROUNDS, |r| r.served_again), 0);
        // Recovery: the tail is at baseline.
        let tail = &trace[TAIL_START..];
        assert!(tail.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.sent_retry == 0 && r.rejected == 0 && r.abandoned == 0));
        assert_eq!(mean_latency(&trace, TAIL_START, ROUNDS), 0.0);
        assert!(trace[250..].iter().all(|r| r.backlog == 0), "the queue should be empty from round 250");
        // Total work is bounded by a small multiple of the input.
        let input = WORKLOAD.total_requests(ROUNDS as u64);
        let sent = sum(&trace, 0, ROUNDS, |r| r.sent_first + r.sent_retry);
        let served = sum(&trace, 0, ROUNDS, |r| r.served_first + r.served_again);
        assert!(sent <= 2 * input, "sends {sent} should be under twice the input {input}");
        assert!(served <= input);
    }

    /// Control: the unbounded configuration is `rpc_retry` and collapses.
    #[test]
    fn without_the_bound_the_system_collapses() {
        let trace = run(WORKLOAD, POLICY, ServerConfig { max_backlog: None, ..SERVER }, ROUNDS);
        print_trajectory(&trace);
        print_totals(&trace);
        assert!(trace.iter().all(|r| r.rejected == 0));
        let tail = &trace[TAIL_START..];
        let tail_completed = sum(&trace, TAIL_START, ROUNDS, |r| r.completed);
        let tail_first = sum(&trace, TAIL_START, ROUNDS, |r| r.served_first);
        let tail_again = sum(&trace, TAIL_START, ROUNDS, |r| r.served_again);
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): completed {tail_completed} (baseline 400), served first={tail_first} again={tail_again}, backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        assert!(tail_completed < 100);
        assert!(tail_again > tail_first);
        assert!(tail.last().unwrap().backlog > tail.first().unwrap().backlog);
    }

    /// Control: bound in place, no trigger. Nothing is ever rejected.
    #[test]
    fn without_a_trigger_nothing_is_rejected() {
        let trace = run(
            Workload {
                trigger_per_tick: WORKLOAD.baseline_per_tick,
                ..WORKLOAD
            },
            POLICY,
            SERVER,
            ROUNDS,
        );
        print_trajectory(&trace);
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.rejected == 0 && r.abandoned == 0 && r.served_again == 0 && r.backlog == 0));
        assert!(trace[1..].iter().all(|r| r.completed == 2));
    }

    /// Hold experiment for the bounded configuration, which recovers from the load trigger and so
    /// cannot be labeled by collapse. The label is hazardous if some schedule makes the same input
    /// cost more work, and more of it the longer replies are delayed.
    ///
    /// The server has no timer, so the harness cannot hold the reply edge by withholding a clock
    /// as the cache witness does, and the simulator will not let a tick run without releasing
    /// something from its only loaded hook, so a fixed driver cannot hold replies across a round
    /// either. The experiment therefore lets the fuzzer choose schedules and reads the delay off
    /// each run: the harness sends several clock elements per round, and a schedule that releases
    /// clock elements while holding a reply advances the client's logical time past the timeout.
    ///
    /// Hand-computed expectation, written before measuring. One request per round for 24 rounds,
    /// four clock elements per round, timeout 2 ticks, three sends per request, queue bound 100
    /// (never approached, so no rejections), capacity 5. Under the prompt schedule every reply is
    /// observed before the next clock element, latency is 0, and the server serves each of the 24
    /// ids once. Under a fuzzed schedule, a reply held while two clock elements are released makes
    /// the client re-send that id, and the server, whose queue is otherwise empty, serves it a
    /// second time; held across four, a third time. So over the fuzzed runs: every run whose
    /// longest observed latency is below 2 ticks has zero re-sends and zero re-serves; some run
    /// with a latency of 2 or more has both; and the largest re-send count found should be several
    /// times the smallest positive one, growing with the longest latency observed.
    #[test]
    fn held_replies_make_the_server_redo_work_under_the_bound() {
        const ROUNDS: usize = 24;
        const CLOCKS_PER_ROUND: usize = 4;
        let policy = RetryPolicy {
            timeout_ticks: 2,
            max_attempts: 3,
            reject_backoff_ticks: 1,
        };
        let server_config = ServerConfig {
            max_per_tick: 5,
            max_backlog: Some(100),
        };

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_bounded_queue(&client, &server, requests, client_clock, policy, server_config);
        let completed = outputs.completed.sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
        let rejected = outputs.rejected.sim_output();

        // Per fuzzed run: (longest latency observed, re-sends, re-serves, rejections).
        let mut runs: Vec<(u64, u64, u64, u64)> = Vec::new();
        let runs_ref = &mut runs;

        flow.sim().unit_test_fuzz_iterations(512).fuzz(async || {
            let mut sent_ids: HashSet<u64> = HashSet::new();
            let mut served_ids: HashSet<u64> = HashSet::new();
            let mut resends = 0u64;
            let mut reserves = 0u64;
            let mut rejections = 0u64;
            let mut max_latency = 0u64;
            for tick in 0..ROUNDS as u64 {
                for _ in 0..CLOCKS_PER_ROUND {
                    clock_send.send(());
                }
                request_send.send(tick);
                quiesce().await;
                for c in completed.collect_sorted::<Vec<_>>().await {
                    max_latency = max_latency.max(c.latency_ticks);
                }
                while let Some(req) = outgoing.try_next().await {
                    if !sent_ids.insert(req.id) {
                        resends += 1;
                    }
                }
                while let Some(req) = processed.try_next().await {
                    if !served_ids.insert(req.id) {
                        reserves += 1;
                    }
                }
                rejections += rejected.collect::<Vec<_>>().await.len() as u64;
            }
            runs_ref.push((max_latency, resends, reserves, rejections));
        });

        assert!(runs.iter().all(|r| r.3 == 0), "the queue bound should never be reached in this experiment");
        let below_timeout = runs.iter().filter(|r| r.0 < policy.timeout_ticks).count();
        let with_redo = runs.iter().filter(|r| r.1 > 0).count();
        let max_resends = runs.iter().map(|r| r.1).max().unwrap();
        let max_reserves = runs.iter().map(|r| r.2).max().unwrap();
        let min_positive_resends = runs.iter().map(|r| r.1).filter(|&n| n > 0).min().unwrap_or(0);
        // Redundant work by the longest observed latency, to show it grows with delay.
        let mut by_latency: std::collections::BTreeMap<u64, (u64, u64)> = Default::default();
        for r in &runs {
            let e = by_latency.entry(r.0).or_default();
            e.0 += 1;
            e.1 += r.1;
        }
        println!(
            "{} schedules: {} with longest latency below the timeout, {} with re-sends; most re-sends {max_resends}, most re-serves {max_reserves}, fewest positive re-sends {min_positive_resends}",
            runs.len(),
            below_timeout,
            with_redo
        );
        for (lat, (n, resends)) in &by_latency {
            println!("longest latency {lat}: {n} schedules, mean re-sends {:.2}", *resends as f64 / *n as f64);
        }

        // No delay, no redundant work: the mechanism is delay-driven.
        assert!(
            runs.iter().filter(|r| r.0 < policy.timeout_ticks).all(|r| r.1 == 0 && r.2 == 0),
            "a schedule that never delayed a reply past the timeout should not have re-sent or re-served"
        );
        // Some schedule exists under which the same 24 requests cost the server more than 24 serves.
        assert!(with_redo > 0, "the fuzzer should find a schedule that re-sends");
        assert!(max_reserves > 0, "a re-send should make the server serve an id again");
        // Redundant work grows with delay. A reply held past one timeout can cost one re-send, one
        // held past two can cost two, so schedules whose longest latency reached twice the
        // timeout should carry more re-sends on average than those that only reached it once.
        // (The relation is statistical because a clock jump and the reply can land in the same
        // client tick, which gives a long latency with no re-send.)
        let mean = |lo: u64, hi: u64| {
            let sel: Vec<&(u64, u64, u64, u64)> = runs.iter().filter(|r| r.0 >= lo && r.0 < hi).collect();
            (sel.len(), sel.iter().map(|r| r.1).sum::<u64>() as f64 / sel.len().max(1) as f64)
        };
        let (n_once, mean_once) = mean(policy.timeout_ticks, 2 * policy.timeout_ticks);
        let (n_twice, mean_twice) = mean(2 * policy.timeout_ticks, u64::MAX);
        println!("longest latency in [timeout, 2 timeout): {n_once} schedules, mean re-sends {mean_once:.2}; at least 2 timeout: {n_twice} schedules, mean re-sends {mean_twice:.2}");
        if n_once > 0 && n_twice > 0 {
            assert!(mean_twice >= mean_once, "re-sends should grow with the delay the schedule imposed");
        }
    }

    /// The bound holds under schedule exploration: however the schedule holds requests and
    /// answers, the server's queue never exceeds `max_backlog`, no id is sent more than
    /// `max_attempts` times, and the server never serves an id it has served before, because the
    /// run is shorter than the timeout and so only the server's own delay could cause a re-send.
    #[test]
    fn queue_bound_holds_across_schedules() {
        const ROUNDS: usize = 30;
        let workload = Workload {
            baseline_per_tick: 1,
            trigger_per_tick: 6,
            trigger_start_tick: 5,
            trigger_end_tick: 15,
        };
        let policy = RetryPolicy {
            timeout_ticks: 40,
            max_attempts: 3,
            reject_backoff_ticks: 3,
        };
        let server_config = ServerConfig {
            max_per_tick: 2,
            max_backlog: Some(6),
        };

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_bounded_queue(&client, &server, requests, client_clock, policy, server_config);
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
        let backlog_trace = outputs.backlog_trace.sim_output();

        flow.sim().unit_test_fuzz_iterations(128).fuzz(async || {
            let mut sends: std::collections::HashMap<u64, u32> = Default::default();
            let mut served: HashSet<u64> = HashSet::new();
            for tick in 0..ROUNDS as u64 {
                clock_send.send(());
                for _ in 0..workload.requests_at(tick) {
                    request_send.send(tick);
                }
                quiesce().await;
                while let Some(req) = outgoing.try_next().await {
                    let n = sends.entry(req.id).or_default();
                    *n += 1;
                    assert!(*n <= policy.max_attempts, "id {} sent {n} times", req.id);
                }
                while let Some(req) = processed.try_next().await {
                    assert!(served.insert(req.id), "id {} served twice", req.id);
                }
                while let Some(depth) = backlog_trace.try_next().await {
                    assert!(depth <= 6, "queue depth {depth} exceeds the bound");
                }
            }
        });
    }
}
