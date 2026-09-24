//! W4: a request/response service whose client retries with exponential backoff and jitter.
//!
//! The program is the W1 design from [`crate::cluster::rpc_retry`]: the client tags each
//! application request with an id, keeps an outstanding table, and re-sends any request that has
//! waited longer than its timeout, up to [`RetryPolicy::max_attempts`] sends; the server keeps a
//! FIFO backlog and serves at most [`ServerConfig::max_per_tick`] requests per tick. The one
//! difference is how the timeout for a request is chosen. With [`RetryPolicy::backoff`] set, the
//! wait before send `k + 1` is `base_timeout_ticks << k` plus a jitter in
//! `[0, base_timeout_ticks / 2)` derived from a hash of the request id, so retry load decays
//! geometrically and requests spread out instead of firing together. With `backoff` unset every
//! wait is `base_timeout_ticks`, which is exactly W1.
//!
//! The mechanism under study is the retry timer, and the label is **hazardous** in both
//! configurations: whenever the schedule delays a response past the timeout the client sends a
//! request the input did not ask for, and the server serves it. The knob `backoff` decides
//! whether that redundant work can feed back into more of itself; it changes how the storm
//! ends, not whether the timeout causes it. The measured numbers are in the table below.
//!
//! # Timer parameters
//!
//! - `client_clock`: one element per logical client tick; the client's clock advances by one per
//!   element and all timeouts are measured in these ticks. A deployment wires
//!   `client.source_interval(period)` into it; the simulation feeds it from `sim_input`.
//!
//! # Measured
//!
//! Collapse runs (trigger in rounds 100..160; 2 requests per round baseline, capacity 5 per
//! round, base timeout 40, 3 attempts):
//!
//! | run | rounds / tail | tail completed (baseline 400) | tail served again vs first | backlog at tail start -> end | re-sends over the run | outcome |
//! |---|---|---|---|---|---|---|
//! | backoff on, trigger | 1000 / 800..1000 | 400 | 0 vs 400 | 0 -> 0 | 1044 | recovers; backlog last non-zero in round 646 |
//! | backoff on, no trigger | 1000 / 800..1000 | 400 | 0 vs 400 | 0 -> 0 | 0 | healthy throughout |
//! | backoff off, trigger | 800 / 600..800 | 0 | 667 vs 333 | 1038 -> 1237 | grows without bound | collapses (this is W1) |
//!
//! Hold run (backoff on; the response delay is set by the length of the trigger window, since
//! the server's backlog is what holds responses past the timeout): re-sends are work the input
//! did not require, and they grow with the hold.
//!
//! | trigger length (rounds) | first sends (1000 rounds) | re-sends | served again |
//! |---|---|---|---|
//! | 30 | 2300 | 2 | 2 |
//! | 60 | 2600 | 1044 | 1044 |
//! | 90 | 2900 | 3524 | 2390 |
//!
//! At a 90-round trigger the re-sends exceed the first sends even with backoff, and 1134 of
//! them were still queued or abandoned at round 1000, so the run had not recovered; backoff
//! bounds the rate of redundant work per request, not the amount the schedule can cause.
//!
//! With backoff on and the trigger, the backlog peaked at 576 and was last non-zero in round
//! 646, later than the hand-computed estimate of about round 580, because the one re-send per
//! request also applies to the 720 requests issued during the trigger, which adds about 700 to
//! the backlog on top of the 420 the trigger builds directly. Over the 1000-round run the client
//! put 2600 first sends and 1044 re-sends on the wire, so total work was 1.40x the input. Under
//! `fuzz` with 32 iterations over a 200-round run of the same shape the re-send count never
//! exceeded the first-send count under a 30-round trigger; that bounds the rate of redundant
//! work under backoff for that trigger, it does not make the work required.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Request<T> {
    pub id: u64,
    pub body: T,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Response<T> {
    pub id: u64,
    pub body: T,
}

/// Client-side bookkeeping for a request that has been sent but not yet answered. Times are
/// logical client ticks.
#[derive(Clone, Debug)]
pub struct InFlight<T> {
    pub body: T,
    pub first_sent: u64,
    pub last_sent: u64,
    /// Sends so far: 1 after the first send.
    pub attempt: u32,
}

#[derive(Clone, Debug)]
enum Verdict<T> {
    Keep(InFlight<T>),
    Retry(InFlight<T>),
    Abandon(InFlight<T>),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Completion {
    pub id: u64,
    pub latency_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// The wait before the second send. With `backoff` on, later waits double.
    pub base_timeout_ticks: u64,
    /// Total number of sends per request (1 = never retry).
    pub max_attempts: u32,
    /// The mechanism knob. `true` doubles the wait per attempt and adds per-id jitter; `false`
    /// uses `base_timeout_ticks` for every wait, which recovers W1.
    pub backoff: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Requests served per server tick: the server's capacity.
    pub max_per_tick: u32,
}

/// The wait, in client ticks, before send number `attempt + 1` of request `id`.
///
/// This is a plain function so that the test can hand-compute expectations with the same
/// arithmetic the program uses. The jitter is a multiplicative hash of the id, so it is
/// deterministic and spreads ids that were issued together.
pub fn wait_before_next_send(base_timeout_ticks: u64, backoff: bool, id: u64, attempt: u32) -> u64 {
    if !backoff {
        return base_timeout_ticks;
    }
    let base = base_timeout_ticks << (attempt - 1).min(16);
    let jitter_range = (base_timeout_ticks / 2).max(1);
    let jitter = (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) % jitter_range;
    base + jitter
}

pub struct Outputs<'a, T> {
    /// Requests that received their first response, at the client.
    pub completed: Stream<Completion, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Ids given up on after `max_attempts` sends.
    pub abandoned: Stream<u64, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Every request the client puts on the wire (first sends and re-sends).
    pub outgoing: Stream<Request<T>, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request the server serves.
    pub processed: Stream<Request<T>, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Depth of the server's backlog at the end of each server tick.
    pub backlog_trace: Stream<usize, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
}

pub fn rpc_with_backoff<'a, T>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<T, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded>,
    policy: RetryPolicy,
    server_config: ServerConfig,
) -> Outputs<'a, T>
where
    T: Clone + Ord + std::hash::Hash + std::fmt::Debug + Serialize + DeserializeOwned + 'a,
{
    let RetryPolicy {
        base_timeout_ticks,
        max_attempts,
        backoff,
    } = policy;
    let max_per_tick = server_config.max_per_tick;
    // `q!` closures capture primitives only, so the flag crosses as an integer.
    let backoff_flag: u32 = backoff as u32;

    let (responses_complete, responses) = client
        .forward_ref::<Stream<Response<T>, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>>(
        );

    // ---- Client: logical clock, outstanding table, timeouts, re-sends ------------------------
    let (outgoing, completed, abandoned) = sliced! {
        let clock = use::batch(client_clock.enumerate(), nondet!(/** batching only shifts which client tick observes a clock element; every element still advances the clock */));
        let new_requests = use::batch(requests.enumerate(), nondet!(/** batching only shifts which client tick first sends a request, i.e. its timestamps */));
        let responses = use::batch(responses, nondet!(/** batching only affects when a completion is observed and when a timeout fires */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, InFlight<T>, Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let new_requests = new_requests.map(q!(|(i, body)| Request { id: i as u64, body }));

        let responded_ids = responses.map(q!(|r| r.id)).unique();
        let completed = responded_ids
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

        // Everything still waiting is judged once per tick against the wait for its attempt.
        // This is the operator where the mechanism lives: the wait grows with the attempt
        // number when `backoff` is on.
        let judged = outstanding
            .filter_key_not_in(responded_ids)
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .map_with_key(q!(move |(id, (inflight, now))| {
                let wait = crate::cluster::witnesses::backoff_retry::wait_before_next_send(base_timeout_ticks, backoff_flag != 0, id, inflight.attempt);
                if now - inflight.last_sent < wait {
                    Verdict::Keep(inflight)
                } else if inflight.attempt < max_attempts {
                    Verdict::Retry(InFlight {
                        body: inflight.body,
                        first_sent: inflight.first_sent,
                        last_sent: now,
                        attempt: inflight.attempt + 1,
                    })
                } else {
                    Verdict::Abandon(inflight)
                }
            }));

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
            .map(q!(|(id, f)| Request { id, body: f.body }))
            .sort();

        let newly_sent = new_requests.clone().cross_singleton(now_cur.clone()).map(q!(|(r, now)| (
            r.id,
            InFlight {
                body: r.body,
                first_sent: now,
                last_sent: now,
                attempt: 1,
            }
        )));

        outstanding = kept
            .chain(retried)
            .chain(newly_sent.into_keyed())
            .first();

        (new_requests.chain(retries_out), completed, abandoned)
    };

    // ---- Server: FIFO backlog, bounded work per tick ------------------------------------
    let incoming = outgoing.clone().send(server, TCP.fail_stop().bincode());

    let (processed, backlog_trace) = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Request<T>, Tick<_>, Bounded, TotalOrder>>();

        let queued = backlog.chain(arrivals).enumerate();
        let served = queued
            .clone()
            .filter_map(q!(move |(i, req)| if i < max_per_tick as usize { Some(req) } else { None }));
        backlog = queued.filter_map(q!(move |(i, req)| if i >= max_per_tick as usize { Some(req) } else { None }));

        (served, backlog.clone().count().into_stream())
    };

    responses_complete.complete(
        processed
            .clone()
            .map(q!(|req| Response {
                id: req.id,
                body: req.body,
            }))
            .send(client, TCP.fail_stop().bincode()),
    );

    Outputs {
        completed,
        abandoned,
        outgoing,
        processed,
        backlog_trace,
    }
}

/// An open-loop workload: a fixed number of requests per client tick, higher inside a window.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_per_tick: u32,
    pub trigger_per_tick: u32,
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
}

/// One round is one `client_clock` element plus that tick's requests, then `quiesce`, so per
/// round the client sends and judges once and the server serves up to `max_per_tick`.
#[cfg(test)]
mod sim_tests {
    use std::collections::HashSet;

    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        completed: u64,
        abandoned: u64,
        sent_first: u64,
        sent_retry: u64,
        served_first: u64,
        served_again: u64,
        backlog: usize,
    }

    /// Builds the flow and returns the round loop's trace. Shared between `run_prompt` and
    /// `fuzz` so the same harness drives both.
    fn build(flow: &mut FlowBuilder<'_>, policy: RetryPolicy, server_config: ServerConfig) -> Harness {
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_backoff(&client, &server, requests, client_clock, policy, server_config);
        Harness {
            request_send,
            clock_send,
            completed: outputs.completed.map(q!(|c| (c.id, c.latency_ticks))).sim_output(),
            abandoned: outputs.abandoned.sim_output(),
            outgoing: outputs.outgoing.sim_output(),
            processed: outputs.processed.sim_output(),
            backlog_trace: outputs.backlog_trace.sim_output(),
        }
    }

    struct Harness {
        request_send: hydro_lang::sim::SimSender<u64, TotalOrder, ExactlyOnce>,
        clock_send: hydro_lang::sim::SimSender<(), TotalOrder, ExactlyOnce>,
        completed: hydro_lang::sim::SimReceiver<(u64, u64), NoOrder, ExactlyOnce>,
        abandoned: hydro_lang::sim::SimReceiver<u64, NoOrder, ExactlyOnce>,
        outgoing: hydro_lang::sim::SimReceiver<Request<u64>, TotalOrder, ExactlyOnce>,
        processed: hydro_lang::sim::SimReceiver<Request<u64>, TotalOrder, ExactlyOnce>,
        backlog_trace: hydro_lang::sim::SimReceiver<usize, TotalOrder, ExactlyOnce>,
    }

    async fn drive(h: &Harness, workload: Workload, rounds: usize) -> Vec<Round> {
        let mut trace = Vec::with_capacity(rounds);
        let mut backlog = 0usize;
        let mut sent_ids: HashSet<u64> = HashSet::new();
        let mut served_ids: HashSet<u64> = HashSet::new();
        for tick in 0..rounds as u64 {
            h.clock_send.send(());
            for _ in 0..workload.requests_at(tick) {
                h.request_send.send(tick);
            }
            quiesce().await;

            let mut r = Round::default();
            r.completed = h.completed.collect_sorted::<Vec<_>>().await.len() as u64;
            r.abandoned = h.abandoned.collect_sorted::<Vec<_>>().await.len() as u64;
            while let Some(req) = h.outgoing.try_next().await {
                if sent_ids.insert(req.id) {
                    r.sent_first += 1
                } else {
                    r.sent_retry += 1
                }
            }
            while let Some(req) = h.processed.try_next().await {
                if served_ids.insert(req.id) {
                    r.served_first += 1
                } else {
                    r.served_again += 1
                }
            }
            while let Some(depth) = h.backlog_trace.try_next().await {
                backlog = depth;
            }
            r.backlog = backlog;
            trace.push(r);
        }
        trace
    }

    fn run(workload: Workload, policy: RetryPolicy, server_config: ServerConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let h = build(&mut flow, policy, server_config);
        let mut trace = Vec::new();
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            *trace_ref = drive(&h, workload, rounds).await;
        });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    /// Hand-computed expectation, written before measurement.
    ///
    /// Baseline is 2 requests per round against a capacity of 5. The trigger offers 12 per
    /// round for 60 rounds, so it builds a backlog of about 60 x (12 - 5) = 420, a queueing
    /// delay of about 84 rounds against a base timeout of 40.
    ///
    /// Without backoff (W1) every request that waits 84 rounds is sent at 0, 40 and 80, so
    /// offered load becomes 3 x 2 = 6 per round against a capacity of 5 and the backlog grows
    /// forever.
    ///
    /// With backoff the second send happens after 40..60 rounds and the third would happen
    /// 80..100 rounds after that, at 120..160 rounds from the first send, which is later than
    /// the 84-round delay, so at most one re-send per request is issued while the backlog is
    /// deep. Offered load is then at most 2 x 2 = 4 per round against 5, so the backlog shrinks
    /// by at least one per round, and once it falls below 40 x 5 = 200 no request waits past its
    /// first timeout and re-sends stop. Recovery is expected within roughly 160 + 420 = 580
    /// rounds at the latest, so a tail starting at round 600 should be healthy, with 2
    /// completions per round, zero backlog, and total sends bounded by 2 x the input.
    ///
    /// Measured: the backlog was last non-zero in round 646, because the estimate above ignores
    /// that the trigger's own 720 requests are each re-sent once, which adds about 700 to the
    /// backlog. The run is therefore 1000 rounds long with the tail at 800..1000; the program
    /// was not changed.
    const WORKLOAD: Workload = Workload {
        baseline_per_tick: 2,
        trigger_per_tick: 12,
        trigger_start_tick: 100,
        trigger_end_tick: 160,
    };
    const POLICY: RetryPolicy = RetryPolicy {
        base_timeout_ticks: 40,
        max_attempts: 3,
        backoff: true,
    };
    const SERVER: ServerConfig = ServerConfig { max_per_tick: 5 };
    const ROUNDS: usize = 1000;
    const TAIL_START: usize = 800;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 130, 159, 200, 250, 300, 400, 500, 600, 700, 799, 999] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: completed={} abandoned={} sent_first={} sent_retry={} served_first={} served_again={} backlog={}",
                    r.completed, r.abandoned, r.sent_first, r.sent_retry, r.served_first, r.served_again, r.backlog
                );
            }
        }
    }

    #[test]
    fn backoff_still_resends_but_recovers_after_the_trigger() {
        let trace = run(WORKLOAD, POLICY, SERVER, ROUNDS);
        print_trajectory(&trace);

        let pre = &trace[10..100];
        assert!(pre.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.sent_retry == 0));

        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        let recovered_at = trace.iter().rposition(|r| r.backlog > 0).map(|i| i + 1).unwrap();
        let total_first = sum(&trace, 0, ROUNDS, |r| r.sent_first);
        let total_retry = sum(&trace, 0, ROUNDS, |r| r.sent_retry);
        let tail_completed = sum(&trace, TAIL_START, ROUNDS, |r| r.completed);
        let tail_again = sum(&trace, TAIL_START, ROUNDS, |r| r.served_again);
        println!(
            "peak backlog {peak}; backlog last nonzero before round {recovered_at}; sends first={total_first} retry={total_retry}; tail completed {tail_completed}, served again {tail_again}"
        );
        assert!(peak > 200, "the trigger should have built a backlog past the timeout, got {peak}");
        assert!(total_retry > 0, "the trigger should have caused some re-sends");
        assert!(recovered_at <= 700, "backlog should be gone by round 700, was last non-zero before round {recovered_at}");
        let tail = &trace[TAIL_START..];
        assert!(tail.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.sent_retry == 0 && r.abandoned == 0));
        assert!(total_first + total_retry <= 2 * total_first, "total work should be at most 2x the input");
    }

    /// Control: backoff on, no trigger.
    #[test]
    fn without_a_trigger_the_system_stays_healthy() {
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
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.abandoned == 0 && r.served_again == 0 && r.backlog == 0));
        assert!(trace[1..].iter().all(|r| r.completed == 2));
    }

    /// Re-sends are work the input did not ask for; they grow with how long the trigger's backlog
    /// holds responses past the timeout.
    #[test]
    fn redundant_work_grows_with_the_hold() {
        let mut previous = 0u64;
        for hold in [30u64, 60, 90] {
            let trace = run(
                Workload {
                    trigger_end_tick: WORKLOAD.trigger_start_tick + hold,
                    ..WORKLOAD
                },
                POLICY,
                SERVER,
                ROUNDS,
            );
            let first = sum(&trace, 0, ROUNDS, |r| r.sent_first);
            let retry = sum(&trace, 0, ROUNDS, |r| r.sent_retry);
            let again = sum(&trace, 0, ROUNDS, |r| r.served_again);
            println!("hold {hold} rounds: first sends {first}, re-sends {retry}, served again {again}");
            assert!(retry > previous, "re-sends should grow with the hold: {retry} after {previous}");
            previous = retry;
        }
    }

    /// The knob's other setting: same trigger, `backoff = false`, which is W1 and collapses.
    #[test]
    fn without_backoff_the_system_collapses() {
        let trace = run(WORKLOAD, RetryPolicy { backoff: false, ..POLICY }, SERVER, 800);
        print_trajectory(&trace);
        let tail = &trace[600..];
        let tail_completed = sum(&trace, 600, 800, |r| r.completed);
        let tail_first = sum(&trace, 600, 800, |r| r.served_first);
        let tail_again = sum(&trace, 600, 800, |r| r.served_again);
        println!(
            "tail: completed {tail_completed} (baseline 400), served first={tail_first} again={tail_again}, backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        assert!(tail_completed < 100);
        assert!(tail_again > tail_first);
        assert!(tail.last().unwrap().backlog > tail.first().unwrap().backlog);
    }

    /// Benign bound under schedule exploration: a shorter run of the same shape under `fuzz`.
    /// The bound asserted is the one stated in the module docs, re-sends never exceeding first
    /// sends.
    #[test]
    fn work_bound_holds_under_fuzzed_schedules() {
        const SHORT_ROUNDS: usize = 200;
        let workload = Workload {
            trigger_start_tick: 20,
            trigger_end_tick: 50,
            ..WORKLOAD
        };
        let mut flow = FlowBuilder::new();
        let h = build(&mut flow, POLICY, SERVER);
        flow.sim().unit_test_fuzz_iterations(32).fuzz(async || {
            let trace = drive(&h, workload, SHORT_ROUNDS).await;
            let first = sum(&trace, 0, SHORT_ROUNDS, |r| r.sent_first);
            let retry = sum(&trace, 0, SHORT_ROUNDS, |r| r.sent_retry);
            assert!(retry <= first, "re-sends {retry} exceeded first sends {first}");
        });
    }
}
