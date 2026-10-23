//! A request/response service with a client-side timeout-and-retry policy.
//!
//! The client forwards application requests to the server, tagging each with an id, and keeps
//! a table of the requests still awaiting a response. A request that has not been answered
//! within [`RetryPolicy::timeout_ticks`] is sent again, up to [`RetryPolicy::max_attempts`]
//! sends in total, after which the client gives up on it and reports it as abandoned. The
//! server answers every request it receives, in arrival order, with [`handle`]: a real (if
//! small) handler that digests the request body. Both sides export per-window metrics.
//!
//! # No capacity model
//!
//! The program does not model its own capacity. An earlier version capped the server at
//! `max_per_tick` requests per tick and burned a `service_time` of CPU per request in a
//! spin-loop, which put the bottleneck inside the code under test. Both are gone: the server
//! processes every arrival in the tick it arrives in. Its capacity is whatever the real code
//! achieves on the resources it is given, which the Sedge harness sets from outside (a CPU quota
//! per deployed process; see `sedge::capacity`). What remains is the protocol, which is the
//! subject under test: the retry policy.
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
//! A deployment wires `source_interval` streams into these parameters (Sedge binds them
//! in-process), so a tick is a fixed period of wall time on the client's own clock; a simulation
//! feeds them with `sim_input` and thereby controls time itself.

use std::time::Duration;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
#[cfg(feature = "cress")]
use cress::{SimOutputs, amplification_check};
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

/// The server's answer: the handler's digest of the request body, under the request's id.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Response {
    pub id: u64,
    /// [`handle`]'s digest of the request body.
    pub digest: u64,
}

/// The server's request handler: a SipHash digest of the whole body (a checksum service). This
/// is the program's real per-request work, proportional to the request's size; there is no
/// emulated service time. How big requests are is up to whoever sends them (see [`Payload`]).
pub fn handle<T: std::hash::Hash>(req: &Request<T>) -> Response {
    use std::hash::{BuildHasher, BuildHasherDefault};
    let digest = BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default()
        .hash_one(&req.body);
    Response { id: req.id, digest }
}

/// A request body of `N` bytes: what the Sedge harness offers this program. The size of a
/// request is a property of the offered load, chosen outside the program, like its rate; the
/// program only does what a checksum service does with it. (Without it the handler's work is a
/// few nanoseconds per request, and no CPU quota the kernel can enforce makes the server the
/// bottleneck at a rate a single-connection generator can offer.)
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Payload<const N: usize>(pub Vec<u8>);

#[cfg(feature = "sedge")]
impl<const N: usize> sedge::LoadItem for Payload<N> {
    /// `N` bytes that depend on `id`, so that no two requests have the same body.
    fn from_id(id: u64) -> Self {
        let seed = id.to_le_bytes();
        Payload((0..N).map(|i| seed[i % 8] ^ (i as u8)).collect())
    }
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
    /// Requests awaiting a response at the end of the window.
    pub outstanding: u64,
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
#[cfg_attr(feature = "cress", derive(SimOutputs))]
pub struct RpcOutputs<'a, T> {
    /// Requests that received their first response, at the client.
    pub completed: Stream<Completion, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Ids given up on after `max_attempts` sends.
    pub abandoned: Stream<u64, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Every request the client puts on the wire (first sends and re-sends).
    pub outgoing: Stream<Request<T>, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request the server serves.
    pub processed: Stream<Request<T>, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Size of the client's table of requests awaiting a response, at the end of each client
    /// tick.
    pub outstanding_trace: Stream<usize, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(window index, metrics)` once per `client_report_tick`.
    pub client_metrics:
        Stream<(usize, ClientMetrics), Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(window index, metrics)` once per `server_report_tick`.
    pub server_metrics:
        Stream<(usize, ServerMetrics), Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// What the Sedge overload harness watches: completions and abandonments by id (the client's ids
/// are its input's arrival order, which is the harness's sequence number), the client's table of
/// requests awaiting a response as a gauge, and the per-window send, re-send and serve counts as
/// counters. (The server keeps no queue of its own any more: requests wait in its socket and
/// its input channel, which the program cannot see.)
#[cfg(feature = "sedge")]
impl<'a, T> sedge::Observed<'a> for RpcOutputs<'a, T>
where
    T: Serialize + DeserializeOwned + 'a,
{
    fn observe(self, obs: &mut sedge::Observer<'a>) {
        obs.completions(self.completed.map(q!(|c| c.id)));
        obs.abandoned(self.abandoned);
        obs.gauge(
            "outstanding",
            self.client_metrics
                .clone()
                .map(q!(|(_, m)| m.outstanding as f64)),
        );
        obs.counter(
            "served",
            self.server_metrics.map(q!(|(_, m)| m.processed as f64)),
        );
        obs.counter(
            "sent",
            self.client_metrics.clone().map(q!(|(_, m)| m.sent as f64)),
        );
        obs.counter(
            "retried",
            self.client_metrics.map(q!(|(_, m)| m.retried as f64)),
        );
    }
}

/// Builds the client/server program. `requests` is the application's request stream at the
/// client; see the module docs for the meaning of the three clock parameters.
///
/// Request bodies must be `Ord` because re-sends are sorted so that the client's outgoing
/// stream is totally ordered; over a single connection the server then receives requests in
/// exactly that order.
///
/// The two `amplification_check` attributes are the corpus's two labeled configurations; run
/// them with `./scripts/cress hydro_test rpc_with_retries`. Both confine the data to the first
/// 125 rounds of the run so that the report's growth label can read the holds that outlast the
/// input; the three-attempt configuration is expected to read as stopping at a ceiling of three
/// times the input, since every request is sent at most three times.
///
/// The two `overload_check` attributes deploy the same configurations under the Sedge overload
/// harness, each process in its own CPU-capped container: `./scripts/sedge hydro_test
/// rpc_with_retries --factors 1.5,2,4,8`. The server, the resource under test, gets 1 % of a
/// core (1 ms of CPU every 100 ms) and is the bottleneck; the client stands for the callers and
/// gets no quota. μ is measured, not declared (`mu` is only where the probe starts). A
/// 50 ms clock makes the 40-tick timeout 2 seconds. Cress's multiplier for three attempts is 3×,
/// so the prediction is that factors below 3 collapse and factors above it recover, and that one
/// attempt recovers everywhere.
#[cfg_attr(feature = "cress", amplification_check(
    name = three_attempts,
    T = u64,
    workload(requests = 2),
    workload_rounds = 125,
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3 },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = one_attempt,
    T = u64,
    workload(requests = 2),
    workload_rounds = 125,
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 1 },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = three_attempts,
    T = Payload<4096>,
    load = requests,
    mu = 1000,
    cpus(Client = none, Server = 0.01),
    cpu_period_us = 100_000,
    timers(
        client_clock = Duration::from_millis(50),
        client_report_tick = Duration::from_secs(1),
        server_report_tick = Duration::from_secs(1),
    ),
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3 },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = one_attempt,
    T = Payload<4096>,
    load = requests,
    mu = 1000,
    cpus(Client = none, Server = 0.01),
    cpu_period_us = 100_000,
    timers(
        client_clock = Duration::from_millis(50),
        client_report_tick = Duration::from_secs(1),
        server_report_tick = Duration::from_secs(1),
    ),
    policy = RetryPolicy { timeout_ticks: 40, max_attempts: 1 },
))]
#[expect(
    clippy::too_many_arguments,
    reason = "every input stream is a parameter so that the checker can feed each one"
)]
pub fn rpc_with_retries<'a, T>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<T, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded>,
    client_report_tick: Stream<(), Process<'a, Client>, Unbounded>,
    server_report_tick: Stream<(), Process<'a, Server>, Unbounded>,
    policy: RetryPolicy,
) -> RpcOutputs<'a, T>
where
    T: Clone + Ord + std::hash::Hash + std::fmt::Debug + Serialize + DeserializeOwned + 'a,
{
    let RetryPolicy {
        timeout_ticks,
        max_attempts,
    } = policy;
    // Responses come back from the server, which is downstream of `outgoing` (below).
    let (responses_complete, responses) = client
        .forward_ref::<Stream<Response, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>>(
        );

    // ---- Client: logical clock, outstanding table, timeouts, re-sends ------------------------
    let (outgoing, retried_out, completed, abandoned, outstanding_depth, outstanding_trace) = sliced! {
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
        let next_outstanding = kept
            .chain(retried)
            .chain(newly_sent.into_keyed())
            .first();
        let depth = next_outstanding.clone().key_count();
        outstanding = next_outstanding;

        (
            new_requests.chain(retries_out),
            retried_out,
            completed,
            abandoned,
            depth.clone(),
            depth.into_stream(),
        )
    };

    // ---- Server: answer every arrival --------------------------------------------------
    // Each tick, the server handles every request that has arrived since the last one, in
    // arrival order. There is no cap: how many requests a tick holds, and how long it takes, is
    // up to the machine the server runs on.
    let incoming = outgoing.clone().send(server, TCP.fail_stop().bincode());

    let processed = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** batching only affects how many requests share a tick */));
        arrivals
    };

    responses_complete.complete(
        processed
            .clone()
            .map(q!(|req| crate::cluster::rpc_retry::handle(&req)))
            .send(client, TCP.fail_stop().bincode()),
    );

    // ---- Server metrics -----------------------------------------------------------------
    let server_metrics = sliced! {
        let punctuation = use::batch(server_report_tick.enumerate(), nondet!(/** metrics window boundary */));
        let processed = use::batch(processed.clone(), nondet!(/** metrics window boundary */));
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
            .map(q!(|(m, (i, _))| (i, m)));
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
        let outstanding_depth = use::snapshot(outstanding_depth, nondet!(/** metrics window boundary */));
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
                outstanding: 0,
            }));

        let report = merged
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .zip(outstanding_depth)
            .map(q!(|((m, (i, _)), depth)| (i, ClientMetrics { outstanding: depth as u64, ..m })));
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
        outstanding_trace,
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
/// `client_clock` plus that round's requests, followed by letting the simulation quiesce, so per
/// round the client runs one tick (send, judge timeouts, re-send), the server answers everything
/// that arrived, and the client observes the responses. There is no wall clock anywhere, and no
/// capacity either: the program no longer models one, so under a fair schedule every request is
/// answered in the round it was sent, whatever the load. Overload, and the collapse that
/// retries cause under it, are a deployment's business: Sedge supplies the bottleneck from
/// outside with a CPU quota. The delay that sets off retries is Cress's business: it holds the
/// server's arrival batch (see `rpc_retry_check`). Request bodies carry the round they were
/// issued in.
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
        /// The client's outstanding table after this round (carried over if it did not run).
        outstanding: usize,
    }

    /// Runs `rounds` rounds and returns one record per round.
    fn run(workload: Workload, policy: RetryPolicy, rounds: usize) -> Vec<Round> {
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
        );
        // Unordered outputs are read sorted, so map them to something `Ord`.
        let completed = outputs
            .completed
            .map(q!(|c| (c.id, c.latency_ticks)))
            .sim_output();
        let abandoned = outputs.abandoned.sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
        let outstanding_trace = outputs.outstanding_trace.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim().run_prompt(async move || {
            let mut outstanding = 0usize;
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
                while let Some(depth) = outstanding_trace.try_next().await {
                    outstanding = depth;
                }
                r.outstanding = outstanding;
                trace_ref.push(r);
            }
        });
        trace
    }

    /// Baseline 2 requests per round; the trigger offers 12 per round for 60 rounds.
    const WORKLOAD: Workload = Workload {
        baseline_per_tick: 2,
        trigger_per_tick: 12,
        trigger_start_tick: 100,
        trigger_end_tick: 160,
    };
    const POLICY: RetryPolicy = RetryPolicy {
        timeout_ticks: 40,
        max_attempts: 3,
    };
    const ROUNDS: usize = 300;

    fn assert_never_retries(trace: &[Round]) {
        assert!(
            trace
                .iter()
                .all(|r| r.sent_retry == 0 && r.abandoned == 0 && r.served_again == 0),
            "a fair schedule answers every request before its timeout"
        );
    }

    /// No trigger: every request is answered within the round it was sent, and the client's
    /// table holds at most that round's requests.
    #[test]
    fn without_a_trigger_the_system_stays_healthy() {
        let trace = run(
            Workload {
                trigger_per_tick: WORKLOAD.baseline_per_tick,
                ..WORKLOAD
            },
            POLICY,
            ROUNDS,
        );
        assert_never_retries(&trace);
        assert!(trace[1..].iter().all(|r| r.completed == 2));
        assert!(trace.iter().all(|r| r.latency_sum == 0));
    }

    /// The trigger alone builds no backlog in the simulator, because the program has no capacity
    /// of its own to exceed: every one of the trigger's requests is answered in its round, so
    /// retries never fire. (Deployed under a CPU quota, the same trigger is what Sedge uses to
    /// set off the collapse.)
    #[test]
    fn a_trigger_without_a_bottleneck_never_retries() {
        let trace = run(WORKLOAD, POLICY, ROUNDS);
        assert_never_retries(&trace);
        let in_trigger = &trace[WORKLOAD.trigger_start_tick as usize + 1..WORKLOAD.trigger_end_tick as usize];
        assert!(in_trigger.iter().all(|r| r.completed == 12), "{in_trigger:?}");
        assert!(trace.iter().all(|r| r.latency_sum == 0));
        assert!(trace.iter().all(|r| r.outstanding <= 12));
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
    );

    outputs.server_metrics.for_each(q!(|(i, m)| println!(
        "server t={}s processed={}",
        i + 1,
        m.processed
    )));

    outputs.client_metrics.for_each(q!(|(i, m)| println!(
        "client t={}s completed={} mean_latency_ticks={:.2} max_latency_ticks={} sent={} retried={} abandoned={} outstanding={}",
        i + 1,
        m.completed,
        m.mean_latency_ticks(),
        m.latency_max_ticks,
        m.sent,
        m.retried,
        m.abandoned,
        m.outstanding
    )));
}
