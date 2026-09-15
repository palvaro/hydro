//! Ground-truth example of a metastable failure in Hydro: a client that retries on timeout
//! talking to a server with fixed capacity.
//!
//! The client is an *open-loop* load generator: it issues requests at a configured rate
//! regardless of whether earlier requests have completed. Every outstanding request that has
//! not been answered within `timeout_ticks` is re-sent (up to `max_attempts` sends in total)
//! and then abandoned. The server serves at most `max_per_tick` requests per tick from an
//! explicit FIFO backlog, so its capacity is `max_per_tick` requests per server tick, and it
//! answers every request it sees -- including retries of requests it has already answered.
//!
//! The feedback loop is explicit in the dataflow: `outgoing -> server -> responses -> outstanding
//! -> retries -> outgoing`. When a brief overload pushes queueing delay past the timeout,
//! retries add work to an already saturated server, which keeps latency above the timeout,
//! which keeps generating retries. If `baseline_per_tick * max_attempts > max_per_tick`, the
//! system never recovers after the trigger ends.
//!
//! # Time is an input
//!
//! This is one program that runs both deployed and in the simulator. Nothing in the dataflow
//! reads a wall clock; all time enters through three stream parameters (the pattern used by
//! [`super::raft::raft`] for its election and heartbeat timers):
//!
//! - `client_clock`: one element per logical client tick. Each element mints the next burst of
//!   requests (`baseline_per_tick` or `trigger_per_tick` of them) and advances the client's
//!   logical clock by one. Timeouts and latencies are measured in these ticks.
//! - `client_report_tick`, `server_report_tick`: one element per reporting window.
//!
//! [`retry_storm_deployed`] wires `source_interval` streams into these parameters, so on a
//! real deployment a tick is `clock_period` of wall time; a simulation test feeds them with
//! `sim_input` and thereby controls time itself. The only deployment-specific piece of the
//! program is the server's optional CPU burn per request (`service_time`), which is what makes
//! capacity real on a laptop and is set to zero in simulation, where `max_per_tick` alone is
//! the capacity.

use std::time::Duration;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Request {
    pub id: u64,
    /// 1 for the first send, 2 for the first retry, ...
    pub attempt: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Response {
    pub id: u64,
    pub attempt: u32,
}

/// Client-side bookkeeping for a request that has been sent but not yet answered. Times are
/// logical client ticks (the index of the `client_clock` element).
#[derive(Clone, Debug)]
pub struct InFlight {
    pub first_sent: u64,
    pub last_sent: u64,
    pub attempt: u32,
}

#[derive(Clone, Debug)]
enum Verdict {
    Keep(InFlight),
    Retry(InFlight),
    Abandon(InFlight),
}

/// A request that received its first response.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Completion {
    pub id: u64,
    /// Client ticks from the first send to the first response.
    pub latency_ticks: u64,
    /// Number of times the request was sent before it was answered.
    pub attempts: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryStormConfig {
    /// Requests minted per client tick outside the trigger window.
    pub baseline_per_tick: u32,
    /// Requests minted per client tick inside the trigger window.
    pub trigger_per_tick: u32,
    /// Trigger window, in client ticks since start: `[trigger_start_tick, trigger_end_tick)`.
    pub trigger_start_tick: u64,
    pub trigger_end_tick: u64,
    /// Upper bound on requests served per server tick: the server's capacity. On a deployment
    /// this also bounds tick length so the server keeps draining its socket while it works
    /// through its backlog.
    pub max_per_tick: u32,
    /// How many client ticks the client waits for a response before re-sending.
    pub timeout_ticks: u64,
    /// Total number of sends per request (1 = never retry).
    pub max_attempts: u32,
    /// CPU time the server burns per request. Deployment only: makes capacity real on a
    /// machine; must be `Duration::ZERO` in simulation.
    pub service_time: Duration,
    /// Deployment only: wall-clock period of `client_clock`.
    pub clock_period: Duration,
    /// Deployment only: how often both sides print a window report.
    pub report_interval: Duration,
}

impl RetryStormConfig {
    pub fn baseline_rate(&self) -> f64 {
        self.baseline_per_tick as f64 / self.clock_period.as_secs_f64()
    }
    pub fn trigger_rate(&self) -> f64 {
        self.trigger_per_tick as f64 / self.clock_period.as_secs_f64()
    }
    /// Deployed capacity in requests per second, assuming the server is CPU-bound.
    pub fn capacity(&self) -> f64 {
        1.0 / self.service_time.as_secs_f64()
    }
}

/// Per-window statistics printed by the client.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientWindow {
    pub completed: u64,
    pub latency_sum_ticks: u64,
    pub latency_max_ticks: u64,
    pub sent_first: u64,
    pub sent_retry: u64,
    pub abandoned: u64,
}

impl ClientWindow {
    pub fn mean_latency_ticks(&self) -> f64 {
        if self.completed == 0 {
            0.0
        } else {
            self.latency_sum_ticks as f64 / self.completed as f64
        }
    }
}

/// Per-window statistics printed by the server.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerWindow {
    /// Requests processed with `attempt == 1` (productive work).
    pub first_attempts: u64,
    /// Requests processed with `attempt > 1` (amplified work: the same id, re-derived).
    pub retries: u64,
}

/// Everything observable about a run. The deployed wiring prints the two `*_windows`
/// streams; simulation tests read whichever of these they need via `sim_output`.
pub struct RetryStormOutputs<'a> {
    /// Requests that received their first response, at the client.
    pub completions: Stream<Completion, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Ids given up on after `max_attempts` sends.
    pub abandoned: Stream<u64, Process<'a, Client>, Unbounded, NoOrder, ExactlyOnce>,
    /// Every request the client sends (first attempts and retries).
    pub outgoing: Stream<Request, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request the server serves.
    pub processed: Stream<Request, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Depth of the server's backlog at the end of each server tick.
    pub backlog_trace: Stream<usize, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(window index, stats)` once per `client_report_tick`.
    pub client_windows:
        Stream<(usize, ClientWindow), Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(window index, stats, backlog depth)` once per `server_report_tick`.
    pub server_windows: Stream<
        (usize, ServerWindow, usize),
        Process<'a, Server>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >,
}

/// Builds the client/server program. See the module docs for the meaning of the three clock
/// parameters.
pub fn retry_storm<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded>,
    client_report_tick: Stream<(), Process<'a, Client>, Unbounded>,
    server_report_tick: Stream<(), Process<'a, Server>, Unbounded>,
    config: RetryStormConfig,
) -> RetryStormOutputs<'a> {
    let RetryStormConfig {
        baseline_per_tick,
        trigger_per_tick,
        trigger_start_tick,
        trigger_end_tick,
        max_per_tick,
        timeout_ticks,
        max_attempts,
        service_time,
        ..
    } = config;
    // `q!` closures can only capture primitives, so the duration crosses into runtime code as
    // nanos.
    let service_nanos = service_time.as_nanos() as u64;

    // Responses come back from the server, which is downstream of `outgoing` (below).
    let (responses_complete, responses) = client
        .forward_ref::<Stream<Response, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>>(
        );

    // ---- Client: logical clock, open-loop load, outstanding table, timeouts, retries -------
    let (outgoing, completions, abandoned) = sliced! {
        // Each clock element is one logical tick, numbered from 0.
        let clock = use::batch(client_clock.enumerate(), nondet!(/** batching only shifts which client tick observes a clock element; every element still mints its burst and advances the clock */));
        let responses = use::batch(responses, nondet!(/** batching only affects when a completion is observed and when a timeout fires */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, InFlight, Tick<_>, Bounded>>();
        // The client's logical clock: the highest tick number seen so far. A tick of this
        // block that receives no clock element (e.g. one triggered by responses alone) keeps
        // the previous value.
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let now_cur = clock.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        // Open-loop load generator: one burst per clock element; the burst is larger inside
        // the trigger window. Ids are unique because each tick owns the range
        // [tick * trigger_per_tick, (tick + 1) * trigger_per_tick).
        let new_requests = clock.flat_map_ordered(q!(move |(i, _)| {
            let tick = i as u64;
            let n = if tick >= trigger_start_tick && tick < trigger_end_tick {
                trigger_per_tick
            } else {
                baseline_per_tick
            };
            (0..n as u64).map(move |k| (
                tick,
                Request {
                    id: tick * trigger_per_tick as u64 + k,
                    attempt: 1,
                },
            ))
        }));

        // A response completes an outstanding request. Responses to ids that are no longer
        // outstanding (duplicate answers to retries, or already-abandoned requests) are ignored.
        let responded_ids = responses.map(q!(|r| r.id)).unique();
        let completions = responded_ids
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(outstanding.clone())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((id, (_, inflight)), now)| Completion {
                id,
                latency_ticks: now - inflight.first_sent,
                attempts: inflight.attempt,
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

        // Retries are sorted so the client's outgoing stream is totally ordered; over a single
        // connection the server then receives requests in exactly this order.
        let retries_out = retried
            .clone()
            .entries()
            .map(q!(|(id, f)| Request { id, attempt: f.attempt }))
            .sort();

        let newly_sent = new_requests.clone().map(q!(|(tick, r)| (
            r.id,
            InFlight {
                first_sent: tick,
                last_sent: tick,
                attempt: 1,
            }
        )));

        // Keys of `kept`, `retried` and `newly_sent` are pairwise disjoint, so `first()` is exact.
        outstanding = kept
            .chain(retried)
            .chain(newly_sent.into_keyed())
            .first();

        (
            new_requests.map(q!(|(_, r)| r)).chain(retries_out),
            completions,
            abandoned,
        )
    };

    // ---- Server: explicit FIFO backlog, bounded work per tick ---------------------------
    // Requests wait in `backlog`, a Hydro-level queue that persists across ticks. Each tick
    // dequeues at most `max_per_tick` requests (and, when deployed, burns `service_time` of CPU
    // on each), so a tick never runs longer than `max_per_tick * service_time`. Keeping ticks
    // short matters on a deployment: a DFIR node only reads its sockets between ticks, and a
    // node blocked writing to a full socket cannot read either. (With unbounded work per tick,
    // the client and server deadlock on each other's full socket buffers as soon as the server
    // falls behind.)
    let incoming = outgoing.clone().send(server, TCP.fail_stop().bincode());

    let (processed, backlog_depth, backlog_trace) = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Request, Tick<_>, Bounded, TotalOrder>>();

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
                attempt: req.attempt,
            }))
            .send(client, TCP.fail_stop().bincode()),
    );

    // ---- Server report ------------------------------------------------------------------
    let server_windows = sliced! {
        let punctuation = use::batch(server_report_tick.enumerate(), nondet!(/** reporting window boundary */));
        let processed = use::batch(processed.clone(), nondet!(/** reporting window boundary */));
        let backlog_depth = use::snapshot(backlog_depth, nondet!(/** reporting window boundary */));
        let mut acc = use::state(|l| l.singleton(q!(ServerWindow::default())));

        let window_end = punctuation.first();
        let in_batch = processed.fold(
            q!(ServerWindow::default),
            q!(|w, req| if req.attempt == 1 { w.first_attempts += 1 } else { w.retries += 1 },
               commutative = manual_proof!(/** counting */)),
        );
        let merged = acc.clone().zip(in_batch).map(q!(|(a, b)| ServerWindow {
            first_attempts: a.first_attempts + b.first_attempts,
            retries: a.retries + b.retries,
        }));

        let report = merged
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .zip(backlog_depth)
            .map(q!(|((w, (i, _)), backlog)| (i, w, backlog)));
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ServerWindow::default())));

        report.into_stream()
    };

    // ---- Client report ------------------------------------------------------------------
    let client_windows = sliced! {
        let punctuation = use::batch(client_report_tick.enumerate(), nondet!(/** reporting window boundary */));
        let completions = use::batch(completions.clone(), nondet!(/** reporting window boundary */));
        let sent = use::batch(outgoing.clone(), nondet!(/** reporting window boundary */));
        let abandoned = use::batch(abandoned.clone(), nondet!(/** reporting window boundary */));
        let mut acc = use::state(|l| l.singleton(q!(ClientWindow::default())));

        let window_end = punctuation.first();
        let from_completions = completions.fold(
            q!(ClientWindow::default),
            q!(|w, c: Completion| {
                w.completed += 1;
                w.latency_sum_ticks += c.latency_ticks;
                w.latency_max_ticks = w.latency_max_ticks.max(c.latency_ticks);
            }, commutative = manual_proof!(/** sum, count and max are commutative */)),
        );
        let from_sent = sent.fold(
            q!(ClientWindow::default),
            q!(|w, r: Request| if r.attempt == 1 { w.sent_first += 1 } else { w.sent_retry += 1 },
               commutative = manual_proof!(/** counting */)),
        );
        let from_abandoned = abandoned.count();

        let merged = acc
            .clone()
            .zip(from_completions)
            .zip(from_sent)
            .zip(from_abandoned)
            .map(q!(|(((a, c), s), ab)| ClientWindow {
                completed: a.completed + c.completed,
                latency_sum_ticks: a.latency_sum_ticks + c.latency_sum_ticks,
                latency_max_ticks: a.latency_max_ticks.max(c.latency_max_ticks),
                sent_first: a.sent_first + s.sent_first,
                sent_retry: a.sent_retry + s.sent_retry,
                abandoned: a.abandoned + ab as u64,
            }));

        let report = merged
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .map(q!(|(w, (i, _))| (i, w)));
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ClientWindow::default())));

        report.into_stream()
    };

    RetryStormOutputs {
        completions,
        abandoned,
        outgoing,
        processed,
        backlog_trace,
        client_windows,
        server_windows,
    }
}

/// Deployment wiring: `source_interval` clocks at `config.clock_period` and
/// `config.report_interval`, and one printed report line per window on each side, which is how
/// the deployed experiment is observed.
#[cfg(feature = "tokio")]
pub fn retry_storm_deployed<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    config: RetryStormConfig,
) {
    let clock_nanos = config.clock_period.as_nanos() as u64;
    let report_nanos = config.report_interval.as_nanos() as u64;

    let client_clock = client.source_interval(q!(Duration::from_nanos(clock_nanos)));
    let client_report_tick = client.source_interval(q!(Duration::from_nanos(report_nanos)));
    let server_report_tick = server.source_interval(q!(Duration::from_nanos(report_nanos)));

    let outputs = retry_storm(
        client,
        server,
        client_clock,
        client_report_tick,
        server_report_tick,
        config,
    );

    outputs.server_windows.for_each(q!(|(i, w, backlog)| println!(
        "server t={}s processed={} first_attempts={} retries={} backlog={}",
        i + 1,
        w.first_attempts + w.retries,
        w.first_attempts,
        w.retries,
        backlog
    )));

    outputs.client_windows.for_each(q!(|(i, w)| println!(
        "client t={}s completed={} mean_latency_ticks={:.2} max_latency_ticks={} sent_first={} sent_retry={} abandoned={}",
        i + 1,
        w.completed,
        w.mean_latency_ticks(),
        w.latency_max_ticks,
        w.sent_first,
        w.sent_retry,
        w.abandoned
    )));
}

/// Simulation of the same program under a fixed, fair schedule. A *round* is one element on
/// `client_clock` followed by letting the simulation quiesce, so per round the client runs one
/// tick (mint, judge timeouts, retry), the server one tick (serve up to `max_per_tick`), and the
/// client one more tick to observe the responses. Capacity is therefore `max_per_tick` per round
/// and the timeout is `timeout_ticks` rounds. There is no wall clock anywhere.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        completed: u64,
        latency_sum: u64,
        abandoned: u64,
        sent_first: u64,
        sent_retry: u64,
        served_first: u64,
        served_retry: u64,
        /// Server backlog after this round's server tick (carried over if it did not run).
        backlog: usize,
    }

    /// Runs `rounds` rounds and returns one record per round.
    fn run(config: RetryStormConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();

        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        // Reports are a deployment concern; these never fire.
        let (_client_report_send, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report_send, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = retry_storm(
            &client,
            &server,
            client_clock,
            client_report_tick,
            server_report_tick,
            config,
        );
        // Unordered outputs are read sorted, so map them to something `Ord`.
        let completions = outputs
            .completions
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
            for _ in 0..rounds {
                clock_send.send(());
                quiesce().await;

                let mut r = Round::default();
                for (_, latency) in completions.collect_sorted::<Vec<_>>().await {
                    r.completed += 1;
                    r.latency_sum += latency;
                }
                r.abandoned = abandoned.collect_sorted::<Vec<_>>().await.len() as u64;
                while let Some(req) = outgoing.try_next().await {
                    if req.attempt == 1 {
                        r.sent_first += 1
                    } else {
                        r.sent_retry += 1
                    }
                }
                while let Some(req) = processed.try_next().await {
                    if req.attempt == 1 {
                        r.served_first += 1
                    } else {
                        r.served_retry += 1
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
    const BASE: RetryStormConfig = RetryStormConfig {
        baseline_per_tick: 2,
        trigger_per_tick: 12,
        trigger_start_tick: 100,
        trigger_end_tick: 160,
        max_per_tick: 5,
        timeout_ticks: 40,
        max_attempts: 3,
        service_time: Duration::ZERO,
        clock_period: Duration::ZERO,
        report_interval: Duration::ZERO,
    };

    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 130, 159, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: completed={} abandoned={} sent_first={} sent_retry={} served_first={} served_retry={} backlog={}",
                    r.completed, r.abandoned, r.sent_first, r.sent_retry, r.served_first, r.served_retry, r.backlog
                );
            }
        }
    }

    #[test]
    fn retries_cause_metastable_collapse() {
        let trace = run(BASE, ROUNDS);
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
        let tail_retry = sum(&trace, TAIL_START, ROUNDS, |r| r.served_retry);
        let baseline_goodput = 2 * (ROUNDS - TAIL_START) as u64;
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): completed {tail_completed} (baseline would be {baseline_goodput}), abandoned {tail_abandoned}, served first={tail_first} retries={tail_retry}, backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        assert!(
            tail_completed < baseline_goodput / 4,
            "goodput should have collapsed: {tail_completed} vs baseline {baseline_goodput}"
        );
        assert!(
            tail_retry > tail_first,
            "the server should spend most of its capacity on retries (first={tail_first}, retries={tail_retry})"
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
            RetryStormConfig {
                trigger_per_tick: BASE.baseline_per_tick,
                ..BASE
            },
            ROUNDS,
        );
        print_trajectory(&trace);
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.abandoned == 0 && r.served_retry == 0));
        assert!(trace.iter().all(|r| r.backlog == 0));
        assert!(trace[1..].iter().all(|r| r.completed == 2));
        assert_eq!(mean_latency(&trace, 0, ROUNDS), 0.0);
    }

    /// Control: same trigger, no retries. The backlog drains and latency returns to zero.
    #[test]
    fn without_retries_the_system_recovers() {
        let trace = run(RetryStormConfig { max_attempts: 1, ..BASE }, ROUNDS);
        print_trajectory(&trace);
        assert!(trace.iter().all(|r| r.sent_retry == 0 && r.served_retry == 0));
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        println!("peak backlog {peak}; tail backlog {}", trace.last().unwrap().backlog);
        assert!(peak > 200, "the trigger should have built a backlog past the timeout, got {peak}");
        let tail = &trace[TAIL_START..];
        assert!(tail.iter().all(|r| r.backlog == 0 && r.completed == 2 && r.abandoned == 0));
        assert_eq!(mean_latency(&trace, TAIL_START, ROUNDS), 0.0);
    }
}

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
        sent_retry: u64,
        abandoned: u64,
    }

    #[derive(Debug, Clone)]
    struct ServerLine {
        t: u64,
        first_attempts: u64,
        retries: u64,
        backlog: u64,
    }

    fn field<T: std::str::FromStr>(line: &str, key: &str) -> T
    where
        T::Err: std::fmt::Debug,
    {
        let start = line.find(&format!("{key}=")).expect(key) + key.len() + 1;
        let rest = &line[start..];
        let end = rest.find(' ').unwrap_or(rest.len());
        rest[..end].trim_end_matches('s').parse().unwrap()
    }

    fn parse_client(line: &str) -> ClientLine {
        ClientLine {
            t: field(line, "t"),
            completed: field(line, "completed"),
            mean_latency_ticks: field(line, "mean_latency_ticks"),
            sent_retry: field(line, "sent_retry"),
            abandoned: field(line, "abandoned"),
        }
    }

    fn parse_server(line: &str) -> ServerLine {
        ServerLine {
            t: field(line, "t"),
            first_attempts: field(line, "first_attempts"),
            retries: field(line, "retries"),
            backlog: field(line, "backlog"),
        }
    }

    /// Deploys the program on localhost and collects `seconds` worth of reports from both sides.
    async fn run(config: RetryStormConfig, seconds: u64) -> (Vec<ClientLine>, Vec<ServerLine>) {
        let mut deployment = Deployment::new();
        let localhost = deployment.Localhost();

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        retry_storm_deployed(&client, &server, config);

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
    /// request that waits longer than 200 ms (40 clock ticks) is sent again (twice), so once the backlog exceeds
    /// the timeout the *offered* load becomes 3 x 400 = 1200 req/s -- still over capacity --
    /// even after the trigger ends.
    const BASE: RetryStormConfig = RetryStormConfig {
        baseline_per_tick: 2,
        trigger_per_tick: 6,
        trigger_start_tick: 2000, // t = 10 s
        trigger_end_tick: 2600,   // t = 13 s
        max_per_tick: 20,
        timeout_ticks: 40, // 200 ms at a 5 ms clock
        max_attempts: 3,
        service_time: Duration::from_millis(1),
        clock_period: Duration::from_millis(5),
        report_interval: Duration::from_secs(1),
    };

    const RUN_SECONDS: u64 = 40;

    /// Observed on a laptop with `BASE`: baseline goodput 400/s at <1 tick (5 ms) latency; the trigger raises
    /// latency to ~340 ms and the server backlog to ~2400; after the trigger ends (t = 13 s)
    /// goodput stays at *zero* -- every request is abandoned after 3 attempts before the server
    /// reaches it -- while the server runs at capacity with ~2/3 of its work being retries and
    /// its backlog grows by ~240/s indefinitely.
    #[tokio::test]
    async fn retries_cause_metastable_collapse() {
        let (client, server) = run(BASE, RUN_SECONDS).await;

        let baseline_latency = mean_latency_in(&client, 3, 10);
        let baseline_goodput = goodput_in(&client, 3, 10);
        let tail_goodput = goodput_in(&client, 25, RUN_SECONDS);
        let tail: Vec<_> = server
            .iter()
            .filter(|l| l.t >= 25 && l.t <= RUN_SECONDS)
            .collect();
        let tail_retries: u64 = tail.iter().map(|l| l.retries).sum();
        let tail_first: u64 = tail.iter().map(|l| l.first_attempts).sum();
        let tail_abandoned: u64 = client
            .iter()
            .filter(|l| l.t >= 25)
            .map(|l| l.abandoned)
            .sum();
        println!(
            "baseline: {baseline_goodput:.0}/s at {baseline_latency:.2} ticks; \
             tail (t>=25s): goodput {tail_goodput:.0}/s, abandoned {tail_abandoned}, \
             server work first={tail_first} retries={tail_retries}, \
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
            tail_retries > tail_first,
            "the server should be spending most of its capacity on amplified work (retries={tail_retries}, first={tail_first})"
        );
        assert!(
            tail.last().unwrap().backlog > tail.first().unwrap().backlog,
            "the server backlog should still be growing at the end of the run"
        );
    }

    /// Control: retries enabled, but no trigger. The retry loop is armed and never fires.
    #[tokio::test]
    async fn without_a_trigger_the_system_stays_healthy() {
        let config = RetryStormConfig {
            trigger_per_tick: BASE.baseline_per_tick,
            ..BASE
        };
        let (client, server) = run(config, RUN_SECONDS).await;

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
        assert!(client.iter().all(|l| l.sent_retry == 0), "no request should ever time out");
        assert!(client.iter().all(|l| l.abandoned == 0));
        assert!(server.iter().all(|l| l.retries == 0 && l.backlog == 0));
    }

    #[tokio::test]
    async fn without_retries_the_system_recovers() {
        let config = RetryStormConfig {
            max_attempts: 1,
            ..BASE
        };
        let (client, server) = run(config, RUN_SECONDS).await;

        let baseline = mean_latency_in(&client, 3, 10);
        let tail = mean_latency_in(&client, 25, RUN_SECONDS);
        let tail_retries: u64 = server.iter().filter(|l| l.t >= 25).map(|l| l.retries).sum();
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
        assert_eq!(tail_retries, 0);
        assert!(client.iter().all(|l| l.sent_retry == 0));
        assert!(server.iter().filter(|l| l.t >= 25).all(|l| l.backlog == 0));
    }
}
