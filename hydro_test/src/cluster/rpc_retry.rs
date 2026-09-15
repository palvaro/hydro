//! A request/response service with a client-side timeout-and-retry policy.
//!
//! The client forwards application requests to the server, tagging each with an id, and keeps
//! a table of the requests still awaiting a response. A request that has not been answered
//! within [`RetryPolicy::timeout_ticks`] is sent again, up to [`RetryPolicy::max_attempts`]
//! sends in total, after which the client gives up on it and reports it as abandoned. The
//! server keeps incoming requests in a FIFO and serves at most [`ServerConfig::max_per_tick`] of
//! them per tick, answering every request it serves. Both sides export per-window metrics.
//!
//! # Time is an input
//!
//! This is one program that runs both deployed and in the simulator. Nothing in the dataflow
//! reads a wall clock; all time enters through three stream parameters (the pattern used by
//! [`super::raft::raft`] for its election and heartbeat timers):
//!
//! - `client_clock`: one element per logical client tick. Each element advances the client's
//!   logical clock by one; timeouts and latencies are measured in these ticks.
//! - `client_report_tick`, `server_report_tick`: one element per metrics window.
//!
//! A deployment wires `source_interval` streams into these parameters, so a tick is a fixed
//! period of wall time; a simulation feeds them with `sim_input` and thereby controls time
//! itself. The only deployment-specific piece of the program is the server's optional CPU burn
//! per request ([`ServerConfig::service_time`]), which is what makes its capacity real on a
//! machine and is zero in simulation, where `max_per_tick` alone is the capacity.

use std::time::Duration;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

/// What the client puts on the wire: the application's request body under a client-assigned id.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Request<T> {
    pub id: u64,
    pub body: T,
}

/// The server's answer, echoing the request body.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Response<T> {
    pub id: u64,
    pub body: T,
}

/// Client-side bookkeeping for a request that has been sent but not yet answered. Times are
/// logical client ticks (the index of the `client_clock` element).
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

/// A request that received its first response.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Completion {
    pub id: u64,
    /// Client ticks from the first send to the first response.
    pub latency_ticks: u64,
}

/// The client's retry policy.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// How many client ticks the client waits for a response before re-sending.
    pub timeout_ticks: u64,
    /// Total number of sends per request (1 = never retry).
    pub max_attempts: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Upper bound on requests served per server tick: the server's capacity. On a deployment
    /// this also bounds tick length so the server keeps draining its socket while it works
    /// through its backlog.
    pub max_per_tick: u32,
    /// CPU time the server burns per request. Deployment only: makes capacity real on a
    /// machine; must be `Duration::ZERO` in simulation.
    pub service_time: Duration,
}

/// Per-window metrics exported by the client.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientMetrics {
    pub completed: u64,
    pub latency_sum_ticks: u64,
    pub latency_max_ticks: u64,
    /// Requests put on the wire, including re-sends.
    pub sent: u64,
    /// Of `sent`, how many were re-sends.
    pub retried: u64,
    pub abandoned: u64,
}

impl ClientMetrics {
    pub fn mean_latency_ticks(&self) -> f64 {
        if self.completed == 0 {
            0.0
        } else {
            self.latency_sum_ticks as f64 / self.completed as f64
        }
    }
}

/// Per-window metrics exported by the server.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMetrics {
    pub processed: u64,
}

/// Everything observable about a run. A deployment prints the two `*_metrics` streams; tests
/// read whichever of these they need.
pub struct RpcOutputs<'a, T> {
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
    /// `(window index, metrics)` once per `client_report_tick`.
    pub client_metrics:
        Stream<(usize, ClientMetrics), Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(window index, metrics, backlog depth)` once per `server_report_tick`.
    pub server_metrics: Stream<
        (usize, ServerMetrics, usize),
        Process<'a, Server>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >,
}

/// Builds the client/server program. `requests` is the application's request stream at the
/// client; see the module docs for the meaning of the three clock parameters.
///
/// Request bodies must be `Ord` because re-sends are sorted so that the client's outgoing
/// stream is totally ordered; over a single connection the server then receives requests in
/// exactly that order.
pub fn rpc_with_retries<'a, T>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<T, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded>,
    client_report_tick: Stream<(), Process<'a, Client>, Unbounded>,
    server_report_tick: Stream<(), Process<'a, Server>, Unbounded>,
    policy: RetryPolicy,
    server_config: ServerConfig,
) -> RpcOutputs<'a, T>
where
    T: Clone + Ord + std::hash::Hash + std::fmt::Debug + Serialize + DeserializeOwned + 'a,
{
    let RetryPolicy {
        timeout_ticks,
        max_attempts,
    } = policy;
    let ServerConfig {
        max_per_tick,
        service_time,
    } = server_config;
    // `q!` closures can only capture primitives, so the duration crosses into runtime code as
    // nanos.
    let service_nanos = service_time.as_nanos() as u64;

    // Responses come back from the server, which is downstream of `outgoing` (below).
    let (responses_complete, responses) = client
        .forward_ref::<Stream<Response<T>, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>>(
        );

    // ---- Client: logical clock, outstanding table, timeouts, re-sends ------------------------
    let (outgoing, retried_out, completed, abandoned) = sliced! {
        // Each clock element is one logical tick, numbered from 0.
        let clock = use::batch(client_clock.enumerate(), nondet!(/** batching only shifts which client tick observes a clock element; every element still advances the clock */));
        // Ids are assigned in arrival order.
        let new_requests = use::batch(requests.enumerate(), nondet!(/** batching only shifts which client tick first sends a request, i.e. its timestamps */));
        let responses = use::batch(responses, nondet!(/** batching only affects when a completion is observed and when a timeout fires */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, InFlight<T>, Tick<_>, Bounded>>();
        // The client's logical clock: the highest tick number seen so far. A tick of this
        // block that receives no clock element (e.g. one triggered by responses alone) keeps
        // the previous value.
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let new_requests = new_requests.map(q!(|(i, body)| Request { id: i as u64, body }));

        // A response completes an outstanding request. Responses to ids that are no longer
        // outstanding (duplicate answers to re-sends, or already-abandoned requests) are ignored.
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

        // Everything still waiting is judged against the timeout exactly once per tick.
        let judged = outstanding
            .filter_key_not_in(responded_ids)
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .map(q!(move |(inflight, now)| {
                if now - inflight.last_sent < timeout_ticks {
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

        // Re-sends are sorted so the client's outgoing stream is totally ordered.
        let retries_out = retried
            .clone()
            .entries()
            .map(q!(|(id, f)| Request { id, body: f.body }))
            .sort();
        // What the client knows about its own re-sends, for its metrics.
        let retried_out = retried.clone().entries().map(q!(|(id, f)| (id, f.attempt)));

        let newly_sent = new_requests.clone().cross_singleton(now_cur.clone()).map(q!(|(r, now)| (
            r.id,
            InFlight {
                body: r.body,
                first_sent: now,
                last_sent: now,
                attempt: 1,
            }
        )));

        // Keys of `kept`, `retried` and `newly_sent` are pairwise disjoint, so `first()` is exact.
        outstanding = kept
            .chain(retried)
            .chain(newly_sent.into_keyed())
            .first();

        (
            new_requests.chain(retries_out),
            retried_out,
            completed,
            abandoned,
        )
    };

    // ---- Server: FIFO backlog, bounded work per tick ------------------------------------
    // Requests wait in `backlog`, a Hydro-level queue that persists across ticks. Each tick
    // dequeues at most `max_per_tick` requests (and, when deployed, burns `service_time` of CPU
    // on each), so a tick never runs longer than `max_per_tick * service_time`. Keeping ticks
    // short matters on a deployment: a DFIR node only reads its sockets between ticks, and a
    // node blocked writing to a full socket cannot read either.
    let incoming = outgoing.clone().send(server, TCP.fail_stop().bincode());

    let (processed, backlog_depth, backlog_trace) = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Request<T>, Tick<_>, Bounded, TotalOrder>>();

        let queued = backlog.chain(arrivals).enumerate();
        let served = queued
            .clone()
            .filter_map(q!(move |(i, req)| if i < max_per_tick as usize { Some(req) } else { None }));
        backlog = queued.filter_map(q!(move |(i, req)| if i >= max_per_tick as usize { Some(req) } else { None }));

        let processed = served.map(q!(move |req| {
            if service_nanos > 0 {
                let deadline = std::time::Instant::now() + Duration::from_nanos(service_nanos);
                while std::time::Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            }
            req
        }));

        let depth = backlog.clone().count();
        (processed, depth.clone(), depth.into_stream())
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

    // ---- Server metrics -----------------------------------------------------------------
    let server_metrics = sliced! {
        let punctuation = use::batch(server_report_tick.enumerate(), nondet!(/** metrics window boundary */));
        let processed = use::batch(processed.clone(), nondet!(/** metrics window boundary */));
        let backlog_depth = use::snapshot(backlog_depth, nondet!(/** metrics window boundary */));
        let mut acc = use::state(|l| l.singleton(q!(ServerMetrics::default())));

        let window_end = punctuation.first();
        let in_batch = processed.count();
        let merged = acc.clone().zip(in_batch).map(q!(|(a, n)| ServerMetrics {
            processed: a.processed + n as u64,
        }));

        let report = merged
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .zip(backlog_depth)
            .map(q!(|((m, (i, _)), backlog)| (i, m, backlog)));
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ServerMetrics::default())));

        report.into_stream()
    };

    // ---- Client metrics -----------------------------------------------------------------
    let client_metrics = sliced! {
        let punctuation = use::batch(client_report_tick.enumerate(), nondet!(/** metrics window boundary */));
        let completed = use::batch(completed.clone(), nondet!(/** metrics window boundary */));
        let sent = use::batch(outgoing.clone(), nondet!(/** metrics window boundary */));
        let retried = use::batch(retried_out, nondet!(/** metrics window boundary */));
        let abandoned = use::batch(abandoned.clone(), nondet!(/** metrics window boundary */));
        let mut acc = use::state(|l| l.singleton(q!(ClientMetrics::default())));

        let window_end = punctuation.first();
        let from_completed = completed.fold(
            q!(ClientMetrics::default),
            q!(|m, c: Completion| {
                m.completed += 1;
                m.latency_sum_ticks += c.latency_ticks;
                m.latency_max_ticks = m.latency_max_ticks.max(c.latency_ticks);
            }, commutative = manual_proof!(/** sum, count and max are commutative */)),
        );
        let from_sent = sent.count();
        let from_retried = retried.count();
        let from_abandoned = abandoned.count();

        let merged = acc
            .clone()
            .zip(from_completed)
            .zip(from_sent)
            .zip(from_retried)
            .zip(from_abandoned)
            .map(q!(|((((a, c), s), r), ab)| ClientMetrics {
                completed: a.completed + c.completed,
                latency_sum_ticks: a.latency_sum_ticks + c.latency_sum_ticks,
                latency_max_ticks: a.latency_max_ticks.max(c.latency_max_ticks),
                sent: a.sent + s as u64,
                retried: a.retried + r as u64,
                abandoned: a.abandoned + ab as u64,
            }));

        let report = merged
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .map(q!(|(m, (i, _))| (i, m)));
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ClientMetrics::default())));

        report.into_stream()
    };

    RpcOutputs {
        completed,
        abandoned,
        outgoing,
        processed,
        backlog_trace,
        client_metrics,
        server_metrics,
    }
}

/// An open-loop workload for the tests: a fixed number of requests per client tick, with a
/// window in which the rate is higher. This lives in the harness, not in the program.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    /// Requests per client tick outside the trigger window.
    pub baseline_per_tick: u32,
    /// Requests per client tick inside the trigger window.
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
}

/// Simulation of the program under a fixed, fair schedule. A *round* is one element on
/// `client_clock` plus that tick's requests, followed by letting the simulation quiesce, so per
/// round the client runs one tick (send, judge timeouts, re-send), the server one tick (serve up
/// to `max_per_tick`), and the client one more tick to observe the responses. Capacity is
/// therefore `max_per_tick` per round and the timeout is `timeout_ticks` rounds. There is no
/// wall clock anywhere. Request bodies carry the tick they were issued in.
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
        /// Server backlog after this round's server tick (carried over if it did not run).
        backlog: usize,
    }

    /// Runs `rounds` rounds and returns one record per round.
    fn run(workload: Workload, policy: RetryPolicy, server_config: ServerConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();

        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        // Metrics windows are a deployment concern; these never fire.
        let (_client_report_send, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report_send, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            policy,
            server_config,
        );
        // Unordered outputs are read sorted, so map them to something `Ord`.
        let completed = outputs
            .completed
            .map(q!(|c| (c.id, c.latency_ticks)))
            .sim_output();
        let abandoned = outputs.abandoned.sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
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
                for (_, latency) in completed.collect_sorted::<Vec<_>>().await {
                    r.completed += 1;
                    r.latency_sum += latency;
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

    /// Baseline 2 requests per round against a capacity of 5 per round (40% utilization).
    /// The trigger offers 12 per round for 60 rounds, building a backlog of ~420 -- a queueing
    /// delay of ~84 rounds, past the 40-round timeout. From then on every request is sent
    /// three times, so offered load is 3 x 2 = 6 per round against a capacity of 5 even
    /// after the trigger ends.
    pub(super) const WORKLOAD: Workload = Workload {
        baseline_per_tick: 2,
        trigger_per_tick: 12,
        trigger_start_tick: 100,
        trigger_end_tick: 160,
    };
    pub(super) const POLICY: RetryPolicy = RetryPolicy {
        timeout_ticks: 40,
        max_attempts: 3,
    };
    pub(super) const SERVER: ServerConfig = ServerConfig {
        max_per_tick: 5,
        service_time: Duration::ZERO,
    };

    pub(super) const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 130, 159, 200, 250, 300, 400, 500, 600, 700, 799] {
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
    fn retries_cause_metastable_collapse() {
        let trace = run(WORKLOAD, POLICY, SERVER, ROUNDS);
        print_trajectory(&trace);

        // Baseline before the trigger: every request answered within the round it was sent.
        let pre = &trace[10..100];
        assert!(pre.iter().all(|r| r.backlog == 0), "backlog should be empty before the trigger");
        assert!(pre.iter().all(|r| r.completed == 2 && r.sent_retry == 0 && r.abandoned == 0));
        assert_eq!(mean_latency(&trace, 10, 100), 0.0);

        // Long after the trigger ended, offered load is back at baseline but the system has
        // not recovered.
        let tail = &trace[TAIL_START..];
        let tail_completed = sum(&trace, TAIL_START, ROUNDS, |r| r.completed);
        let tail_abandoned = sum(&trace, TAIL_START, ROUNDS, |r| r.abandoned);
        let tail_first = sum(&trace, TAIL_START, ROUNDS, |r| r.served_first);
        let tail_again = sum(&trace, TAIL_START, ROUNDS, |r| r.served_again);
        let baseline_goodput = 2 * (ROUNDS - TAIL_START) as u64;
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): completed {tail_completed} (baseline would be {baseline_goodput}), abandoned {tail_abandoned}, served first={tail_first} again={tail_again}, backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        assert!(
            tail_completed < baseline_goodput / 4,
            "goodput should have collapsed: {tail_completed} vs baseline {baseline_goodput}"
        );
        assert!(
            tail_again > tail_first,
            "the server should spend most of its capacity serving ids it has already served (first={tail_first}, again={tail_again})"
        );
        assert!(
            tail.last().unwrap().backlog > tail.first().unwrap().backlog,
            "the backlog should still be growing at the end of the run"
        );
        assert!(tail.windows(2).all(|w| w[1].backlog >= w[0].backlog), "backlog never shrinks in the tail");
    }

    /// Control: retries armed, no trigger. The loop never fires.
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
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.abandoned == 0 && r.served_again == 0));
        assert!(trace.iter().all(|r| r.backlog == 0));
        assert!(trace[1..].iter().all(|r| r.completed == 2));
        assert_eq!(mean_latency(&trace, 0, ROUNDS), 0.0);
    }

    /// Control: same trigger, no retries. The backlog drains and latency returns to zero.
    #[test]
    fn without_retries_the_system_recovers() {
        let trace = run(WORKLOAD, RetryPolicy { max_attempts: 1, ..POLICY }, SERVER, ROUNDS);
        print_trajectory(&trace);
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.served_again == 0));
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        println!("peak backlog {peak}; tail backlog {}", trace.last().unwrap().backlog);
        assert!(peak > 200, "the trigger should have built a backlog past the timeout, got {peak}");
        let tail = &trace[TAIL_START..];
        assert!(tail.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.abandoned == 0));
        assert_eq!(mean_latency(&trace, TAIL_START, ROUNDS), 0.0);
    }
}

/// E2 of the amplification design (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`):
/// per-edge record counts at the simulator's hooks under two schedules with identical inputs.
///
/// Time is a driver decision here, not a harness loop: every `client_clock` element and every
/// request is sent up front and the driver *meters* the client's clock and request batches (one
/// clock element and `b` requests per client tick), so the client's tick count is logical time.
/// The hold schedule additionally holds items in the client's `use::batch(responses)` buffer for
/// `d` client ticks. With `d = 0` the two schedules coincide, which is the baseline.
#[cfg(test)]
mod amplification_tests {
    use std::collections::BTreeMap;

    use hydro_lang::sim::edge_counts::EdgeCounts;
    use hydro_lang::sim::hold_schedule::HoldScheduleDriver;
    use hydro_lang::sim::lineage::Lineage;
    use hydro_lang::sim::quiesce;

    use super::*;

    /// This file, read at test time (the crate is also staged into the simulator's build, where
    /// an `include_str!` of a sibling would not resolve).
    fn source() -> String {
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/cluster/rpc_retry.rs"
        ))
        .unwrap()
    }

    /// 1-based line of the unique source line containing `needle`; the hooks' locations are
    /// `file:line:col`, so this is how a test names an edge of the program.
    fn line_of(needle: &str) -> usize {
        let source = source();
        let mut hits = source
            .lines()
            .enumerate()
            .filter(|(_, l)| l.contains(needle))
            .map(|(i, _)| i + 1);
        let line = hits.next().unwrap_or_else(|| panic!("no line contains {needle:?}"));
        assert!(hits.next().is_none(), "more than one line contains {needle:?}");
        line
    }

    const CLIENT_SLICE: &str = "let (outgoing, retried_out, completed, abandoned) = sliced";
    const SERVER_SLICE: &str = "let (processed, backlog_depth, backlog_trace) = sliced";
    const SERVER_METRICS_SLICE: &str = "let server_metrics = sliced";
    const CLIENT_METRICS_SLICE: &str = "let client_metrics = sliced";

    /// `(name, slice, element type suffix)` for every counted edge, in report order. A hook's
    /// key is `file:line:col <type>` where the location is that of the `sliced!` block (the
    /// batches inside share it), so an edge is named by the line of its block, looked up at
    /// test time (the `!` is added at runtime so these literals do not match themselves), and
    /// the type of the records crossing it. The first two are the client's clock and request
    /// input, which the driver meters; the third is the `responses` buffer the hold schedule
    /// holds.
    const EDGE_NEEDLES: &[(&str, &str, &str)] = &[
        ("clock (client)", CLIENT_SLICE, "(usize, ())"),
        ("requests (client)", CLIENT_SLICE, "(usize, u64)"),
        ("server -> client (responses)", CLIENT_SLICE, "::Response<u64>"),
        ("client -> server (arrivals)", SERVER_SLICE, "::Request<u64>"),
        ("server processed", SERVER_METRICS_SLICE, "::Request<u64>"),
        ("outgoing (client metrics)", CLIENT_METRICS_SLICE, "::Request<u64>"),
        ("retried (client metrics)", CLIENT_METRICS_SLICE, "(u64, u32)"),
        ("completions (client metrics)", CLIENT_METRICS_SLICE, "::Completion"),
        ("abandoned (client metrics)", CLIENT_METRICS_SLICE, " <u64"),
    ];

    fn edge_pred(slice: &str, element_type: &str) -> impl Fn(&str) -> bool + Clone + 'static {
        let tag = format!("rpc_retry.rs:{}:", line_of(&format!("{slice}!")));
        let suffix = format!("{element_type}>");
        move |key: &str| key.contains(&tag) && key.ends_with(&suffix)
    }

    fn edge_named(name: &str) -> impl Fn(&str) -> bool + Clone + 'static {
        let (_, slice, ty) = EDGE_NEEDLES
            .iter()
            .find(|(n, _, _)| *n == name)
            .unwrap_or_else(|| panic!("unknown edge {name}"));
        edge_pred(slice, ty)
    }

    #[derive(Debug, Default)]
    struct Run {
        counts: EdgeCounts,
        /// id -> number of times it was put on the wire.
        sends: BTreeMap<u64, u32>,
        /// id -> the client tick it was issued in (the request body).
        issued_at: BTreeMap<u64, u64>,
        /// id -> number of times the server served it.
        served: BTreeMap<u64, u32>,
        /// id -> latency in client ticks of the first response.
        completions: BTreeMap<u64, u64>,
        abandoned: Vec<u64>,
        final_backlog: usize,
    }

    impl Run {
        fn records(&self, name: &str) -> u64 {
            self.counts.records(edge_named(name))
        }

        fn print(&self, title: &str) {
            println!("== {title}");
            for (key, e) in &self.counts.0 {
                println!("  [{key}] records={}", e.records);
            }
            for (name, _, _) in EDGE_NEEDLES {
                let e = self.counts.edge(edge_named(name));
                println!(
                    "  {name:<32} records={:<6} decisions={:<5} nonempty={:<5}",
                    e.records, e.decisions, e.nonempty_decisions
                );
            }
            println!(
                "  requests={} sends-per-request histogram={:?} completions={} latency histogram={:?} abandoned={} final backlog={}",
                self.sends.len(),
                histogram(self.sends.values().copied()),
                self.completions.len(),
                histogram(self.completions.values().copied()),
                self.abandoned.len(),
                self.final_backlog
            );
        }
    }

    fn histogram<K: Ord + Copy>(values: impl Iterator<Item = K>) -> BTreeMap<K, usize> {
        let mut h = BTreeMap::new();
        for v in values {
            *h.entry(v).or_default() += 1;
        }
        h
    }

    /// The simulator ports of one wiring of the program.
    #[derive(Clone, Copy)]
    struct Ports {
        completed: hydro_lang::sim::SimReceiver<(u64, u64), NoOrder, ExactlyOnce>,
        abandoned: hydro_lang::sim::SimReceiver<u64, NoOrder, ExactlyOnce>,
        outgoing: hydro_lang::sim::SimReceiver<Request<u64>, TotalOrder, ExactlyOnce>,
        processed: hydro_lang::sim::SimReceiver<Request<u64>, TotalOrder, ExactlyOnce>,
        backlog_trace: hydro_lang::sim::SimReceiver<usize, TotalOrder, ExactlyOnce>,
    }

    type Senders = (
        hydro_lang::sim::SimSender<(), TotalOrder, ExactlyOnce>,
        hydro_lang::sim::SimSender<u64, TotalOrder, ExactlyOnce>,
    );

    fn wire<'a>(policy: RetryPolicy, server_config: ServerConfig) -> (FlowBuilder<'a>, Senders, Ports) {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();

        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_client_report_send, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report_send, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            policy,
            server_config,
        );
        let ports = Ports {
            completed: outputs
                .completed
                .map(q!(|c| (c.id, c.latency_ticks)))
                .sim_output(),
            abandoned: outputs.abandoned.sim_output(),
            outgoing: outputs.outgoing.sim_output(),
            processed: outputs.processed.sim_output(),
            backlog_trace: outputs.backlog_trace.sim_output(),
        };
        (flow, (clock_send, request_send), ports)
    }

    /// Drains every output into `run`.
    async fn drain(p: Ports, run: &mut Run) {
        for (id, latency) in p.completed.collect_sorted::<Vec<_>>().await {
            run.completions.insert(id, latency);
        }
        run.abandoned.extend(p.abandoned.collect_sorted::<Vec<_>>().await);
        while let Some(req) = p.outgoing.try_next().await {
            *run.sends.entry(req.id).or_default() += 1;
            run.issued_at.insert(req.id, req.body);
        }
        while let Some(req) = p.processed.try_next().await {
            *run.served.entry(req.id).or_default() += 1;
        }
        while let Some(depth) = p.backlog_trace.try_next().await {
            run.final_backlog = depth;
        }
    }

    /// Sends `ticks` clock elements and `per_tick` requests per tick up front and runs to
    /// quiescence under `driver`, which is given the clock and request batches already metered.
    fn run_counted(
        policy: RetryPolicy,
        server_config: ServerConfig,
        per_tick: u32,
        ticks: usize,
        driver: impl FnOnce(HoldScheduleDriver) -> HoldScheduleDriver,
    ) -> Run {
        run_traced(policy, server_config, per_tick, ticks, driver).0
    }

    /// [`run_counted`], also returning the lineage log of the run (E3.1: every record released
    /// by a hook, under an id, with what the hook held back).
    fn run_traced(
        policy: RetryPolicy,
        server_config: ServerConfig,
        per_tick: u32,
        ticks: usize,
        driver: impl FnOnce(HoldScheduleDriver) -> HoldScheduleDriver,
    ) -> (Run, Lineage) {
        let (flow, (clock_send, request_send), ports) = wire(policy, server_config);
        let driver = driver(
            HoldScheduleDriver::new()
                .meter(edge_named("clock (client)"))
                .meter_n(edge_named("requests (client)"), per_tick as usize),
        );

        let mut run = Run::default();
        let run_ref = &mut run;
        let (counts, lineage) = flow.sim().run_traced(driver, async move || {
            for tick in 0..ticks as u64 {
                clock_send.send(());
                for _ in 0..per_tick {
                    request_send.send(tick);
                }
            }
            quiesce().await;
            drain(ports, run_ref).await;
        });
        run.counts = counts;
        (run, lineage)
    }

    fn hold_driver(d: u64) -> impl FnOnce(HoldScheduleDriver) -> HoldScheduleDriver {
        move |driver| driver.hold(edge_named("server -> client (responses)"), d)
    }

    /// E1's fixed input: baseline load only, and capacity comfortably above the worst case
    /// offered load (`baseline * max_attempts` per client tick, two client ticks per server tick
    /// under the scheduler's round-robin) so the server never queues and the only source of
    /// delay is the schedule.
    const PER_TICK: u32 = 2;
    const POLICY: RetryPolicy = RetryPolicy {
        timeout_ticks: 40,
        max_attempts: 3,
    };
    const SERVER: ServerConfig = ServerConfig {
        max_per_tick: 20,
        service_time: Duration::ZERO,
    };
    const TICKS: usize = 300;

    /// Hand computation (E1). A request issued at tick `t` whose response is visible at tick
    /// `t + l` under the baseline schedule becomes visible at `t + l + d` when held for `d`
    /// ticks. It is judged at ticks `t + k * tau` for `k = 1, 2, ...` while the response is not
    /// yet visible (a response visible in the judging tick suppresses the verdict): the first
    /// `max_attempts - 1` such judgements re-send it, the next abandons it. Judgements only
    /// happen while the clock is still running (`t + k * tau <= ticks - 1`); when the clock runs
    /// dry the driver flushes everything it holds, so a request still outstanding then completes
    /// at tick `ticks - 1`.
    ///
    /// Returns `(sends, Some(latency))` or `(sends, None)` if abandoned.
    fn expected(t: u64, l: u64, d: u64, policy: &RetryPolicy, ticks: u64) -> (u32, Option<u64>) {
        let tau = policy.timeout_ticks;
        let last_tick = ticks - 1;
        let visible = t + l + d;
        let mut sends = 1;
        for k in 1..=policy.max_attempts as u64 {
            let judged_at = t + k * tau;
            if judged_at > last_tick || visible <= judged_at {
                break;
            }
            if k < policy.max_attempts as u64 {
                sends += 1;
            } else {
                return (sends, None);
            }
        }
        (sends, Some(visible.min(last_tick) - t))
    }

    #[test]
    fn hold_schedule_amplifies_by_the_e1_step_function() {
        let baseline = run_counted(POLICY, SERVER, PER_TICK, TICKS, |d| d);
        baseline.print("baseline (metered clock and requests, d = 0)");
        let n = (TICKS as u64) * PER_TICK as u64;
        assert_eq!(baseline.sends.len() as u64, n);
        assert!(baseline.sends.values().all(|s| *s == 1));
        assert_eq!(baseline.records("client -> server (arrivals)"), n);
        assert_eq!(baseline.records("server processed"), n);
        assert_eq!(baseline.records("server -> client (responses)"), n);
        assert_eq!(baseline.records("completions (client metrics)"), n);
        assert_eq!(baseline.records("retried (client metrics)"), 0);
        assert_eq!(baseline.records("abandoned (client metrics)"), 0);
        assert_eq!(baseline.completions.len() as u64, n);

        let tau = POLICY.timeout_ticks;
        let mut failures = vec![];
        // Straddle the thresholds `tau`, `2 tau` (retry) and `3 tau` (abandon) for both baseline
        // latencies (1 and 2).
        for d in [tau / 2, tau - 2, tau - 1, tau, tau + tau / 2, 2 * tau - 2, 2 * tau - 1, 2 * tau, 3 * tau - 2, 3 * tau] {
            let run = run_counted(POLICY, SERVER, PER_TICK, TICKS, hold_driver(d));
            run.print(&format!("hold d = {d}"));

            let expected: BTreeMap<u64, (u32, Option<u64>)> = baseline
                .completions
                .iter()
                .map(|(id, l)| (*id, expected(baseline.issued_at[id], *l, d, &POLICY, TICKS as u64)))
                .collect();
            let expected_sends: u64 = expected.values().map(|(s, _)| *s as u64).sum();
            let expected_completions = expected.values().filter(|(_, c)| c.is_some()).count() as u64;
            println!(
                "  hand-computed: sends-per-request histogram={:?} total {expected_sends}, completions {expected_completions}, abandoned {}; measured client->server {} processed {} responses {} completions {} abandoned {}",
                histogram(expected.values().map(|(s, _)| *s)),
                n - expected_completions,
                run.records("client -> server (arrivals)"),
                run.records("server processed"),
                run.records("server -> client (responses)"),
                run.records("completions (client metrics)"),
                run.records("abandoned (client metrics)"),
            );

            let mismatched: Vec<_> = run
                .sends
                .iter()
                .filter(|(id, s)| expected[*id].0 != **s)
                .take(5)
                .collect();
            if !mismatched.is_empty() {
                failures.push(format!("d = {d}: per-request sends differ from hand computation, e.g. (id, measured) {mismatched:?}"));
            }
            let mismatched: Vec<_> = expected
                .iter()
                .filter(|(id, (_, c))| run.completions.get(*id) != c.as_ref())
                .take(5)
                .collect();
            if !mismatched.is_empty() {
                failures.push(format!("d = {d}: per-request completions differ from hand computation, e.g. (id, expected) {mismatched:?}"));
            }
            if run.records("client -> server (arrivals)") != expected_sends
                || run.records("server processed") != expected_sends
                || run.records("server -> client (responses)") != expected_sends
                || run.records("outgoing (client metrics)") != expected_sends
                || run.records("retried (client metrics)") != expected_sends - n
            {
                failures.push(format!("d = {d}: edge totals differ from hand-computed {expected_sends}"));
            }
            if run.records("completions (client metrics)") != expected_completions
                || run.records("abandoned (client metrics)") != n - expected_completions
                || run.abandoned.len() as u64 != n - expected_completions
            {
                failures.push(format!("d = {d}: completions/abandoned differ from hand-computed {expected_completions}/{}", n - expected_completions));
            }
            if run.final_backlog != 0 {
                failures.push(format!("d = {d}: final backlog {}", run.final_backlog));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// Control: `max_attempts = 1`. Holding past the timeout changes what the client concludes
    /// (abandon instead of complete) but not the work on any location-crossing edge.
    #[test]
    fn hold_without_retries_changes_no_edge() {
        let policy = RetryPolicy { max_attempts: 1, ..POLICY };
        let n = (TICKS as u64) * PER_TICK as u64;
        let baseline = run_counted(policy, SERVER, PER_TICK, TICKS, |d| d);
        baseline.print("max_attempts = 1, d = 0");
        for d in [POLICY.timeout_ticks + 1, 3 * POLICY.timeout_ticks] {
            let run = run_counted(policy, SERVER, PER_TICK, TICKS, hold_driver(d));
            run.print(&format!("max_attempts = 1, hold d = {d}"));
            for edge in [
                "client -> server (arrivals)",
                "server processed",
                "server -> client (responses)",
                "outgoing (client metrics)",
            ] {
                assert_eq!(run.records(edge), n, "{edge} at d = {d}");
                assert_eq!(run.records(edge), baseline.records(edge), "{edge} at d = {d}");
            }
            assert_eq!(run.records("retried (client metrics)"), 0);
            assert!(run.sends.values().all(|s| *s == 1));
        }
    }

    /// Derivations toward request `id` on `edge`, from the lineage log: every record crossing
    /// the edge is decoded as the harness's type for it and attributed to the request id it
    /// carries. This is E3.1's attribution: by the harness, at the boundary, no parents.
    fn per_request(log: &Lineage, edge: &str) -> BTreeMap<u64, u64> {
        let pred = edge_named(edge);
        match edge {
            "server -> client (responses)" => log.per_goal::<Response<u64>, u64>(pred, |r| vec![r.id]),
            "completions (client metrics)" => log.per_goal::<Completion, u64>(pred, |c| vec![c.id]),
            "abandoned (client metrics)" => log.per_goal::<u64, u64>(pred, |id| vec![id]),
            _ => log.per_goal::<Request<u64>, u64>(pred, |r| vec![r.id]),
        }
    }

    /// The edges whose records answer a request, i.e. where a re-derivation toward request
    /// `id` shows up as another record carrying `id`.
    const WORK_EDGES: &[&str] = &[
        "client -> server (arrivals)",
        "server processed",
        "server -> client (responses)",
        "outgoing (client metrics)",
    ];

    /// E3.1 (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`, "E3: design"):
    /// record ids assigned at the hooks, attribution by the harness. Under every hold `d`, the
    /// number of records carrying request `id` on each location-crossing edge must equal the
    /// hand-computed sends for `id` (E1's step function, per request), and the goal itself (the
    /// completion for `id`) must be derived once. The log's per-record arrival ticks must show
    /// every response held exactly `d` ticks except the end-of-run flush, and the ids a release
    /// reports as held must be exactly the responses that arrived in the last `d` ticks.
    #[test]
    fn boundary_ids_attribute_every_extra_record_to_its_request() {
        let n = (TICKS as u64) * PER_TICK as u64;
        let (baseline, baseline_log) = run_traced(POLICY, SERVER, PER_TICK, TICKS, |d| d);
        baseline.print("baseline (traced)");

        // The log and E2's counter agree on every edge, and every record came across as bincode.
        for (key, e) in &baseline.counts.0 {
            assert_eq!(baseline_log.record_count(|k| k == key), e.records, "record count on {key}");
        }
        assert!(
            baseline_log.releases.iter().flat_map(|r| &r.released).all(|r| r.payload.is_some()),
            "every batch in rpc_retry buffers a Serialize type"
        );
        println!("  log: {} releases, {} records, edges {:?}", baseline_log.releases.len(), baseline_log.releases.iter().map(|r| r.released.len()).sum::<usize>(), baseline_log.edges());

        // Prompt: one derivation per request on every work edge, one completion per request.
        for edge in WORK_EDGES.iter().chain(["completions (client metrics)"].iter()) {
            let per_goal = per_request(&baseline_log, edge);
            assert_eq!(per_goal.len() as u64, n, "{edge}: requests seen");
            assert!(per_goal.values().all(|c| *c == 1), "{edge}: every request derived once under prompt");
        }

        let tau = POLICY.timeout_ticks;
        let mut failures = vec![];
        println!("== E3.1 per-goal derivations (rpc_retry), hold d on responses");
        for d in [tau - 1, tau, 2 * tau, 3 * tau] {
            let (run, log) = run_traced(POLICY, SERVER, PER_TICK, TICKS, hold_driver(d));
            let expected: BTreeMap<u64, u64> = baseline
                .completions
                .iter()
                .map(|(id, l)| (*id, expected(baseline.issued_at[id], *l, d, &POLICY, TICKS as u64).0 as u64))
                .collect();

            // Per goal on every work edge: the hand computation, request by request, and E2's
            // own per-request count from the `outgoing` output.
            let mut hist = BTreeMap::new();
            for edge in WORK_EDGES {
                let per_goal = per_request(&log, edge);
                hist = histogram(per_goal.values().copied());
                if per_goal.len() as u64 != n {
                    failures.push(format!("d = {d}, {edge}: {} requests attributed, expected {n}", per_goal.len()));
                }
                let mismatched: Vec<_> = expected
                    .iter()
                    .filter(|(id, s)| per_goal.get(*id) != Some(*s))
                    .map(|(id, s)| (*id, *s, per_goal.get(id).copied()))
                    .take(5)
                    .collect();
                if !mismatched.is_empty() {
                    failures.push(format!("d = {d}, {edge}: derivations per request differ from hand-computed sends, e.g. (id, expected, measured) {mismatched:?}"));
                }
                let mismatched: Vec<_> = run
                    .sends
                    .iter()
                    .filter(|(id, s)| per_goal.get(*id) != Some(&(**s as u64)))
                    .take(5)
                    .collect();
                if !mismatched.is_empty() {
                    failures.push(format!("d = {d}, {edge}: derivations per request differ from the client's own send count, e.g. {mismatched:?}"));
                }
                if log.record_count(edge_named(edge)) != run.records(edge) {
                    failures.push(format!("d = {d}, {edge}: log has {} records, counter {}", log.record_count(edge_named(edge)), run.records(edge)));
                }
            }
            // The goal: derived once per completed request, never for an abandoned one.
            let completions = per_request(&log, "completions (client metrics)");
            let abandoned = per_request(&log, "abandoned (client metrics)");
            if completions.keys().ne(run.completions.keys()) || completions.values().any(|c| *c != 1) {
                failures.push(format!("d = {d}: completion derivations are not one per completed request"));
            }
            if abandoned.keys().ne(run.abandoned.iter()) || abandoned.values().any(|c| *c != 1) {
                failures.push(format!("d = {d}: abandon records are not one per abandoned request"));
            }

            // The hold, per record: released `d` ticks after arrival, except the end-of-run flush
            // (the last release of the hook), and the held ids at each release are exactly the
            // records that arrived within the last `d` ticks.
            let responses = edge_named("server -> client (responses)");
            let arrived: BTreeMap<u64, u64> = log.records_on(responses.clone()).map(|(_, r)| (r.id, r.arrived)).collect();
            let last_serial = log.releases_on(responses.clone()).map(|r| r.serial).max().unwrap();
            let mut held_exactly_d = 0u64;
            let mut flushed = 0u64;
            let mut max_held = 0usize;
            for release in log.releases_on(responses.clone()) {
                for record in &release.released {
                    let held_for = release.tick - record.arrived;
                    if held_for == d {
                        held_exactly_d += 1;
                    } else if release.serial == last_serial && held_for < d {
                        flushed += 1;
                    } else {
                        failures.push(format!("d = {d}: response {} held {held_for} ticks (arrived {}, released {})", record.id, record.arrived, release.tick));
                    }
                }
                max_held = max_held.max(release.held.len());
                if release.serial != last_serial {
                    let wrong: Vec<_> = release
                        .held
                        .iter()
                        .filter(|id| !(arrived[id] + d > release.tick && arrived[id] <= release.tick))
                        .collect();
                    if !wrong.is_empty() {
                        failures.push(format!("d = {d}: release at tick {} holds ids not within the last {d} ticks: {wrong:?}", release.tick));
                    }
                    let due: usize = arrived.values().filter(|a| **a + d > release.tick && **a <= release.tick).count();
                    if due != release.held.len() {
                        failures.push(format!("d = {d}: release at tick {} holds {} ids, {due} arrived within the last {d} ticks", release.tick, release.held.len()));
                    }
                }
            }
            println!(
                "  d = {d:>3}: derivations/request {hist:?} (hand-computed {:?}); completions {} abandoned {}; responses held exactly d: {held_exactly_d}, flushed at end: {flushed}, max held at once: {max_held}",
                histogram(expected.values().copied()),
                completions.len(),
                abandoned.len()
            );
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// The E0 collapse under the E0 harness (one clock element and that tick's requests per
    /// round, then `quiesce()`), every hook releasing everything (the prompt driver), counted
    /// per edge.
    fn run_e0_counted(
        workload: Workload,
        policy: RetryPolicy,
        server_config: ServerConfig,
        rounds: usize,
    ) -> Run {
        run_e0(workload, policy, server_config, rounds, false).0.0
    }

    /// [`run_e0_counted`], optionally traced (E3.1), with the wall time of the run itself (the
    /// compiled simulation is built before the clock starts).
    fn run_e0(
        workload: Workload,
        policy: RetryPolicy,
        server_config: ServerConfig,
        rounds: usize,
        traced: bool,
    ) -> ((Run, Option<Lineage>), Duration) {
        use hydro_lang::sim::prompt_schedule::PromptScheduleDriver;

        let (flow, (clock_send, request_send), ports) = wire(policy, server_config);
        let compiled = flow.sim().compiled();
        let mut run = Run::default();
        let run_ref = &mut run;
        let thunk = async move || {
            for tick in 0..rounds as u64 {
                clock_send.send(());
                for _ in 0..workload.requests_at(tick) {
                    request_send.send(tick);
                }
                quiesce().await;
                drain(ports, run_ref).await;
            }
        };
        let start = std::time::Instant::now();
        let (counts, lineage) = if traced {
            let (counts, lineage) = compiled.run_traced(PromptScheduleDriver::default(), thunk);
            (counts, Some(lineage))
        } else {
            (compiled.run_counted(PromptScheduleDriver::default(), thunk), None)
        };
        let elapsed = start.elapsed();
        run.counts = counts;
        ((run, lineage), elapsed)
    }

    /// E3.1's cost and coverage on the long run: the E0 collapse (800 rounds, ~15k records
    /// across the hooks) traced and untraced. The traced log must attribute every record on the
    /// work edges to its request with exactly the client's own send counts, and the slowdown is
    /// reported (the design asks for it to be measured, not for a bound).
    #[test]
    fn tracing_the_e0_collapse_attributes_every_send_and_costs_little() {
        let workload = sim_tests::WORKLOAD;
        let policy = sim_tests::POLICY;
        let rounds = sim_tests::ROUNDS;
        let ((untraced, _), untraced_time) = run_e0(workload, policy, sim_tests::SERVER, rounds, false);
        let ((traced, lineage), traced_time) = run_e0(workload, policy, sim_tests::SERVER, rounds, true);
        let lineage = lineage.unwrap();
        traced.print("E0 trigger, prompt driver, E0 harness, traced");
        let records: usize = lineage.releases.iter().map(|r| r.released.len()).sum();
        println!(
            "== E3.1 cost on the E0 run: untraced {:.2?}, traced {:.2?} ({:.2}x), {} releases, {records} records logged",
            untraced_time,
            traced_time,
            traced_time.as_secs_f64() / untraced_time.as_secs_f64(),
            lineage.releases.len()
        );
        assert_eq!(traced.counts, untraced.counts, "tracing changed the run");
        assert_eq!(traced.sends, untraced.sends);
        for edge in ["client -> server (arrivals)", "outgoing (client metrics)"] {
            let per_goal = per_request(&lineage, edge);
            assert_eq!(per_goal.len(), traced.sends.len(), "{edge}: requests attributed");
            let mismatched: Vec<_> = traced.sends.iter().filter(|(id, s)| per_goal[*id] != **s as u64).take(5).collect();
            assert!(mismatched.is_empty(), "{edge}: derivations per request differ from the client's send count, e.g. {mismatched:?}");
        }
        // The server's edges see only what was delivered: sends minus the final backlog.
        let arrivals = per_request(&lineage, "client -> server (arrivals)");
        let processed = per_request(&lineage, "server processed");
        let processed_total: u64 = processed.values().sum();
        let arrivals_total: u64 = arrivals.values().sum();
        assert_eq!(arrivals_total, processed_total + traced.final_backlog as u64);
        let served_total: u64 = traced.served.values().map(|s| *s as u64).sum();
        assert_eq!(processed_total, served_total);
        assert!(processed.iter().all(|(id, c)| traced.served.get(id).copied().unwrap_or(0) as u64 == *c));
    }

    /// The other perturbation axis (E1, last bullet): same prompt policy, no held delivery,
    /// but the E0 trigger builds a queue whose delay exceeds the timeout. The same edges
    /// (client -> server, server processed, server -> client) should show the same gain (3 per
    /// request, saturated at `max_attempts`) as the hold schedule does for `d > 2 tau`.
    #[test]
    fn load_perturbation_shows_the_same_gain_on_the_same_edges() {
        let workload = sim_tests::WORKLOAD;
        let policy = sim_tests::POLICY;
        let rounds = sim_tests::ROUNDS;
        let run = run_e0_counted(workload, policy, sim_tests::SERVER, rounds);
        run.print("E0 trigger, prompt driver, E0 harness");

        let by_window = |from: u64, to: u64| {
            histogram(
                run.sends
                    .iter()
                    .filter(|(id, _)| (from..to).contains(&run.issued_at[*id]))
                    .map(|(_, s)| *s),
            )
        };
        let tau = policy.timeout_ticks;
        let pre = by_window(0, workload.trigger_start_tick - tau);
        let post = by_window(200, rounds as u64 - 2 * tau);
        let served_total: u64 = run.served.values().map(|s| *s as u64).sum();
        let sends_total: u64 = run.sends.values().map(|s| *s as u64).sum();
        println!(
            "  sends per request issued before the trigger [0, {}): {pre:?}; issued in [200, {}): {post:?}; total sends {sends_total} served {served_total} backlog {}",
            workload.trigger_start_tick - tau,
            rounds as u64 - 2 * tau,
            run.final_backlog
        );

        assert_eq!(pre.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert_eq!(post.keys().copied().collect::<Vec<_>>(), vec![policy.max_attempts]);
        assert_eq!(run.records("outgoing (client metrics)"), sends_total);
        assert_eq!(run.records("client -> server (arrivals)"), sends_total);
        assert_eq!(run.records("server processed"), served_total);
        assert_eq!(run.records("server -> client (responses)"), served_total);
        assert_eq!(
            run.records("server processed") + run.final_backlog as u64,
            run.records("client -> server (arrivals)")
        );
        assert_eq!(run.records("completions (client metrics)"), run.completions.len() as u64);
        assert_eq!(run.records("abandoned (client metrics)"), run.abandoned.len() as u64);
    }
}

/// Wall-clock parameters of a deployment.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Period of `client_clock`, and of the workload generator.
    pub clock_period: Duration,
    /// How often both sides print a metrics window.
    pub report_interval: Duration,
}

/// Deployment harness: wires a [`Workload`] generator and `source_interval` clocks into the
/// program and prints one metrics line per window on each side, which is how a deployed run is
/// observed. (Lives here rather than in the test module because `q!` closures must be part of
/// the crate that is staged onto the deployed hosts.)
#[cfg(feature = "tokio")]
pub fn deploy_with_workload<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    workload: Workload,
    policy: RetryPolicy,
    server_config: ServerConfig,
    timing: Timing,
) {
    let clock_nanos = timing.clock_period.as_nanos() as u64;
    let report_nanos = timing.report_interval.as_nanos() as u64;
    let Workload {
        baseline_per_tick,
        trigger_per_tick,
        trigger_start_tick,
        trigger_end_tick,
    } = workload;

    // One timer drives both the clock and the load generator, so a tick's requests are
    // issued in that tick. Request bodies carry the tick they were issued in.
    let ticks = client.source_interval(q!(Duration::from_nanos(clock_nanos)));
    let requests = ticks.clone().enumerate().flat_map_ordered(q!(move |(i, _)| {
        let tick = i as u64;
        let n = if tick >= trigger_start_tick && tick < trigger_end_tick {
            trigger_per_tick
        } else {
            baseline_per_tick
        };
        std::iter::repeat_n(tick, n as usize)
    }));
    let client_report_tick = client.source_interval(q!(Duration::from_nanos(report_nanos)));
    let server_report_tick = server.source_interval(q!(Duration::from_nanos(report_nanos)));

    let outputs = rpc_with_retries(
        client,
        server,
        requests,
        ticks,
        client_report_tick,
        server_report_tick,
        policy,
        server_config,
    );

    outputs.server_metrics.for_each(q!(|(i, m, backlog)| println!(
        "server t={}s processed={} backlog={}",
        i + 1,
        m.processed,
        backlog
    )));

    outputs.client_metrics.for_each(q!(|(i, m)| println!(
        "client t={}s completed={} mean_latency_ticks={:.2} max_latency_ticks={} sent={} retried={} abandoned={}",
        i + 1,
        m.completed,
        m.mean_latency_ticks(),
        m.latency_max_ticks,
        m.sent,
        m.retried,
        m.abandoned
    )));
}


/// Deployment on localhost. The harness supplies the workload (a `source_interval`-driven
/// generator following a [`Workload`]) and the clocks, and observes the program through its
/// printed per-window metrics.
#[cfg(all(test, feature = "tokio"))]
mod deployed_tests {
    use std::time::Duration;

    use hydro_deploy::Deployment;
    use hydro_lang::deploy::{DeployCrateWrapper, TrybuildHost};

    use super::*;

    /// One line of client output, keyed by the second it was reported at.
    #[derive(Debug, Clone)]
    struct ClientLine {
        t: u64,
        completed: u64,
        mean_latency_ticks: f64,
        sent: u64,
        retried: u64,
        abandoned: u64,
    }

    #[derive(Debug, Clone)]
    struct ServerLine {
        t: u64,
        processed: u64,
        backlog: u64,
    }

    fn field<T: std::str::FromStr>(line: &str, key: &str) -> T
    where
        T::Err: std::fmt::Debug,
    {
        let start = line.find(&format!(" {key}=")).expect(key) + key.len() + 2;
        let rest = &line[start..];
        let end = rest.find(' ').unwrap_or(rest.len());
        rest[..end].trim_end_matches('s').parse().unwrap()
    }

    fn parse_client(line: &str) -> ClientLine {
        ClientLine {
            t: field(line, "t"),
            completed: field(line, "completed"),
            mean_latency_ticks: field(line, "mean_latency_ticks"),
            sent: field(line, "sent"),
            retried: field(line, "retried"),
            abandoned: field(line, "abandoned"),
        }
    }

    fn parse_server(line: &str) -> ServerLine {
        ServerLine {
            t: field(line, "t"),
            processed: field(line, "processed"),
            backlog: field(line, "backlog"),
        }
    }

    /// Deploys the program on localhost and collects `seconds` worth of metrics from both sides.
    async fn run(
        workload: Workload,
        policy: RetryPolicy,
        seconds: u64,
    ) -> (Vec<ClientLine>, Vec<ServerLine>) {
        let mut deployment = Deployment::new();
        let localhost = deployment.Localhost();

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        deploy_with_workload(&client, &server, workload, policy, SERVER, TIMING);

        let rustflags = "-C opt-level=3";
        let nodes = flow
            .with_default_optimize()
            .with_process(
                &client,
                TrybuildHost::new(localhost.clone()).rustflags(rustflags),
            )
            .with_process(
                &server,
                TrybuildHost::new(localhost.clone()).rustflags(rustflags),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();
        let mut client_stdout = nodes.get_process(&client).stdout();
        let mut server_stdout = nodes.get_process(&server).stdout();
        let mut client_stderr = nodes.get_process(&client).stderr();
        let mut server_stderr = nodes.get_process(&server).stderr();
        deployment.start().await.unwrap();

        let mut client_lines = vec![];
        let mut server_lines = vec![];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds + 5);
        loop {
            tokio::select! {
                Some(line) = client_stdout.recv() => {
                    if line.starts_with("client ") {
                        println!("{line}");
                        client_lines.push(parse_client(&line));
                    }
                }
                Some(line) = server_stdout.recv() => {
                    if line.starts_with("server ") {
                        println!("{line}");
                        server_lines.push(parse_server(&line));
                    }
                }
                Some(line) = client_stderr.recv() => println!("client stderr: {line}"),
                Some(line) = server_stderr.recv() => println!("server stderr: {line}"),
                _ = tokio::time::sleep_until(deadline) => break,
            }
            if client_lines.iter().any(|l| l.t >= seconds)
                && server_lines.iter().any(|l| l.t >= seconds)
            {
                break;
            }
        }
        (client_lines, server_lines)
    }

    fn mean_latency_in(lines: &[ClientLine], from: u64, to: u64) -> f64 {
        let window: Vec<_> = lines
            .iter()
            .filter(|l| l.t >= from && l.t < to && l.completed > 0)
            .collect();
        assert!(
            !window.is_empty(),
            "no completions between t={from}s and t={to}s"
        );
        window.iter().map(|l| l.mean_latency_ticks).sum::<f64>() / window.len() as f64
    }

    /// Completed requests per second, averaged over `[from, to)`.
    fn goodput_in(lines: &[ClientLine], from: u64, to: u64) -> f64 {
        let window: Vec<_> = lines.iter().filter(|l| l.t >= from && l.t < to).collect();
        assert!(
            !window.is_empty(),
            "no client reports between t={from}s and t={to}s"
        );
        window.iter().map(|l| l.completed as f64).sum::<f64>() / window.len() as f64
    }

    /// Server capacity is 1000 req/s (1 ms of CPU per request). Baseline load is 400 req/s, so
    /// the server is at 40% utilization. The trigger triples load to 1200 req/s for three
    /// seconds, which is over capacity and builds a backlog. With `max_attempts = 3` every
    /// request that waits longer than 200 ms (40 clock ticks) is sent again (twice), so once
    /// the backlog exceeds the timeout the *offered* load becomes 3 x 400 = 1200 req/s -- still
    /// over capacity -- even after the trigger ends.
    const WORKLOAD: Workload = Workload {
        baseline_per_tick: 2,
        trigger_per_tick: 6,
        trigger_start_tick: 2000, // t = 10 s
        trigger_end_tick: 2600,   // t = 13 s
    };
    const POLICY: RetryPolicy = RetryPolicy {
        timeout_ticks: 40, // 200 ms at a 5 ms clock
        max_attempts: 3,
    };
    const SERVER: ServerConfig = ServerConfig {
        max_per_tick: 20,
        service_time: Duration::from_millis(1),
    };
    const TIMING: Timing = Timing {
        clock_period: Duration::from_millis(5),
        report_interval: Duration::from_secs(1),
    };

    const RUN_SECONDS: u64 = 40;

    /// Observed on a laptop: baseline goodput 400/s at <1 tick (5 ms) latency; the trigger
    /// raises latency to ~340 ms and the server backlog to ~2400; after the trigger ends
    /// (t = 13 s) goodput stays at *zero* -- every request is abandoned after 3 attempts before
    /// the server reaches it -- while the server runs at capacity on re-sent requests and its
    /// backlog grows by ~240/s indefinitely.
    #[tokio::test]
    async fn retries_cause_metastable_collapse() {
        let (client, server) = run(WORKLOAD, POLICY, RUN_SECONDS).await;

        let baseline_latency = mean_latency_in(&client, 3, 10);
        let baseline_goodput = goodput_in(&client, 3, 10);
        let tail_goodput = goodput_in(&client, 25, RUN_SECONDS);
        let tail: Vec<_> = server
            .iter()
            .filter(|l| l.t >= 25 && l.t <= RUN_SECONDS)
            .collect();
        let tail_processed: u64 = tail.iter().map(|l| l.processed).sum();
        let tail_client: Vec<_> = client.iter().filter(|l| l.t >= 25).collect();
        let tail_sent: u64 = tail_client.iter().map(|l| l.sent).sum();
        let tail_retried: u64 = tail_client.iter().map(|l| l.retried).sum();
        let tail_abandoned: u64 = tail_client.iter().map(|l| l.abandoned).sum();
        println!(
            "baseline: {baseline_goodput:.0}/s at {baseline_latency:.2} ticks; \
             tail (t>=25s): goodput {tail_goodput:.0}/s, abandoned {tail_abandoned}, \
             client sent {tail_sent} of which re-sent {tail_retried}, server processed {tail_processed}, \
             backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );

        assert!(
            baseline_latency < 4.0,
            "baseline latency should be low, got {baseline_latency} ticks"
        );
        assert!(
            baseline_goodput > 350.0,
            "baseline goodput should be ~400/s, got {baseline_goodput}"
        );
        // Twelve seconds after the trigger ended, offered load is back at baseline but the
        // system has not recovered:
        assert!(
            tail_goodput < baseline_goodput / 4.0,
            "goodput should have collapsed, got {tail_goodput}/s vs baseline {baseline_goodput}/s"
        );
        assert!(
            tail_retried > tail_sent - tail_retried,
            "most of what the client puts on the wire should be re-sends (sent={tail_sent}, re-sent={tail_retried})"
        );
        assert!(
            tail.last().unwrap().backlog > tail.first().unwrap().backlog,
            "the server backlog should still be growing at the end of the run"
        );
    }

    /// Control: retries enabled, but no trigger. The retry loop is armed and never fires.
    #[tokio::test]
    async fn without_a_trigger_the_system_stays_healthy() {
        let workload = Workload {
            trigger_per_tick: WORKLOAD.baseline_per_tick,
            ..WORKLOAD
        };
        let (client, server) = run(workload, POLICY, RUN_SECONDS).await;

        let latency = mean_latency_in(&client, 3, RUN_SECONDS);
        let goodput = goodput_in(&client, 3, RUN_SECONDS);
        let worst = client
            .iter()
            .filter(|l| l.t >= 3)
            .map(|l| l.mean_latency_ticks)
            .fold(0.0, f64::max);
        println!("no trigger: goodput {goodput:.0}/s, mean latency {latency:.2} ticks, worst 1s-window mean {worst:.2} ticks");

        assert!(latency < 4.0, "latency should stay low, got {latency} ticks");
        assert!(goodput > 350.0, "goodput should stay at ~400/s, got {goodput}");
        assert!(client.iter().all(|l| l.retried == 0), "no request should ever time out");
        assert!(client.iter().all(|l| l.abandoned == 0));
        assert!(server.iter().all(|l| l.backlog == 0));
    }

    #[tokio::test]
    async fn without_retries_the_system_recovers() {
        let policy = RetryPolicy {
            max_attempts: 1,
            ..POLICY
        };
        let (client, server) = run(WORKLOAD, policy, RUN_SECONDS).await;

        let baseline = mean_latency_in(&client, 3, 10);
        let tail = mean_latency_in(&client, 25, RUN_SECONDS);
        println!("baseline mean latency {baseline:.2} ticks; tail mean latency {tail:.2} ticks");

        assert!(
            baseline < 4.0,
            "baseline latency should be low, got {baseline} ticks"
        );
        assert!(
            tail < 4.0,
            "latency should return to baseline, got {tail} ticks"
        );
        assert!(goodput_in(&client, 25, RUN_SECONDS) > 350.0);
        assert!(client.iter().all(|l| l.retried == 0));
        assert!(server.iter().filter(|l| l.t >= 25).all(|l| l.backlog == 0));
    }
}
