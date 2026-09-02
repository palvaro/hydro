//! Ground-truth example of a metastable failure in Hydro: a client that retries on timeout
//! talking to a server with fixed capacity.
//!
//! The client is an *open-loop* load generator: it issues requests at a configured rate
//! regardless of whether earlier requests have completed. Every outstanding request that has
//! not been answered within `timeout` is re-sent (up to `max_attempts` sends in total) and then
//! abandoned. The server does a fixed amount of CPU work per request (a busy-wait of
//! `service_time`), so its capacity is `1 / service_time` requests per second, and it answers
//! every request it sees -- including retries of requests it has already answered.
//!
//! The feedback loop is explicit in the dataflow: `outgoing -> server -> responses -> outstanding
//! -> retries -> outgoing`. When a brief overload pushes queueing delay past `timeout`, retries
//! add work to an already saturated server, which keeps latency above `timeout`, which keeps
//! generating retries. If `baseline_rate * max_attempts > capacity`, the system never recovers
//! after the trigger ends.

use std::time::{Duration, Instant};

use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
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

/// Client-side bookkeeping for a request that has been sent but not yet answered.
#[derive(Clone, Debug)]
pub struct InFlight {
    pub first_sent: Instant,
    pub last_sent: Instant,
    pub attempt: u32,
}

#[derive(Clone, Debug)]
enum Verdict {
    Keep(InFlight),
    Retry(InFlight),
    Abandon(InFlight),
}

/// A request that received its first response.
#[derive(Clone, Debug)]
pub struct Completion {
    pub id: u64,
    /// Time from the first send to the first response.
    pub latency: Duration,
    /// Number of times the request was sent before it was answered.
    pub attempts: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryStormConfig {
    /// Load generator tick. Requests are emitted in bursts of `baseline_per_tick` or
    /// `trigger_per_tick` at this period.
    pub load_tick: Duration,
    pub baseline_per_tick: u32,
    pub trigger_per_tick: u32,
    /// Trigger window, in load ticks since start: `[trigger_start_tick, trigger_end_tick)`.
    pub trigger_start_tick: u64,
    pub trigger_end_tick: u64,
    /// CPU time the server burns per request.
    pub service_time: Duration,
    /// Upper bound on requests served per server tick; bounds tick length so the server keeps
    /// draining its socket while it works through its backlog.
    pub max_per_tick: u32,
    /// How long the client waits for a response before re-sending.
    pub timeout: Duration,
    /// Total number of sends per request (1 = never retry).
    pub max_attempts: u32,
    /// How often both sides print a window report.
    pub report_interval: Duration,
}

impl RetryStormConfig {
    pub fn baseline_rate(&self) -> f64 {
        self.baseline_per_tick as f64 / self.load_tick.as_secs_f64()
    }
    pub fn trigger_rate(&self) -> f64 {
        self.trigger_per_tick as f64 / self.load_tick.as_secs_f64()
    }
    pub fn capacity(&self) -> f64 {
        1.0 / self.service_time.as_secs_f64()
    }
}

/// Per-window statistics printed by the client.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ClientWindow {
    pub completed: u64,
    pub latency_sum: Duration,
    pub latency_max: Duration,
    pub sent_first: u64,
    pub sent_retry: u64,
    pub abandoned: u64,
}

impl ClientWindow {
    pub fn mean_latency(&self) -> Duration {
        if self.completed == 0 {
            Duration::ZERO
        } else {
            self.latency_sum / self.completed as u32
        }
    }
}

/// Per-window statistics printed by the server.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ServerWindow {
    /// Requests processed with `attempt == 1` (productive work).
    pub first_attempts: u64,
    /// Requests processed with `attempt > 1` (amplified work: the same id, re-derived).
    pub retries: u64,
}

/// Builds the client/server program. Returns nothing; both processes print their reports to
/// stdout (one line per `report_interval`), which is how the experiment is observed.
pub fn retry_storm<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    config: RetryStormConfig,
) {
    let RetryStormConfig {
        load_tick,
        baseline_per_tick,
        trigger_per_tick,
        trigger_start_tick,
        trigger_end_tick,
        service_time,
        max_per_tick,
        timeout,
        max_attempts,
        report_interval,
    } = config;
    // `q!` closures can only capture primitives, so durations cross into runtime code as nanos.
    let load_tick_nanos = load_tick.as_nanos() as u64;
    let timeout_nanos = timeout.as_nanos() as u64;
    let service_nanos = service_time.as_nanos() as u64;
    let report_nanos = report_interval.as_nanos() as u64;

    // ---- Open-loop load generator -------------------------------------------------------
    // One burst per load tick; the burst is larger inside the trigger window. Ids are unique
    // because each tick owns the range [tick * trigger_per_tick, (tick + 1) * trigger_per_tick).
    let new_requests = client
        .source_interval(q!(Duration::from_nanos(load_tick_nanos)))
        .enumerate()
        .flat_map_ordered(q!(move |(tick, _)| {
            let tick = tick as u64;
            let n = if tick >= trigger_start_tick && tick < trigger_end_tick {
                trigger_per_tick
            } else {
                baseline_per_tick
            };
            (0..n as u64).map(move |k| Request {
                id: tick * trigger_per_tick as u64 + k,
                attempt: 1,
            })
        }));

    // Responses come back from the server, which is downstream of `outgoing` (below).
    let (responses_complete, responses) =
        client.forward_ref::<Stream<Response, Process<'a, Client>, Unbounded, NoOrder>>();

    // ---- Outstanding-request table, completions, timeouts and retries -------------------
    let (outgoing, completions, abandoned) = sliced! {
        let new_requests = use::batch(new_requests, nondet!(/** open-loop arrivals; batching only shifts when a request is stamped as sent */));
        let responses = use::batch(responses, nondet!(/** batching only affects when a completion is observed and when a timeout fires */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, InFlight, Tick<_>, Bounded>>();

        // A response completes an outstanding request. Responses to ids that are no longer
        // outstanding (duplicate answers to retries, or already-abandoned requests) are ignored.
        let responded_ids = responses.map(q!(|r| r.id)).unique();
        let completions = responded_ids
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(outstanding.clone())
            .entries()
            .map(q!(|(id, (_, inflight))| Completion {
                id,
                latency: inflight.first_sent.elapsed(),
                attempts: inflight.attempt,
            }));

        // Everything still waiting is judged against the timeout exactly once per tick.
        let judged = outstanding
            .filter_key_not_in(responded_ids)
            .map(q!(move |inflight| {
                if inflight.last_sent.elapsed() < Duration::from_nanos(timeout_nanos) {
                    Verdict::Keep(inflight)
                } else if inflight.attempt < max_attempts {
                    Verdict::Retry(inflight)
                } else {
                    Verdict::Abandon(inflight)
                }
            }));

        let kept = judged.clone().filter_map(q!(|v| match v {
            Verdict::Keep(f) => Some(f),
            _ => None,
        }));
        let retried = judged.clone().filter_map(q!(|v| match v {
            Verdict::Retry(f) => Some(InFlight {
                first_sent: f.first_sent,
                last_sent: Instant::now(),
                attempt: f.attempt + 1,
            }),
            _ => None,
        }));
        let abandoned = judged
            .filter_map(q!(|v| match v {
                Verdict::Abandon(f) => Some(f),
                _ => None,
            }))
            .entries()
            .map(q!(|(id, _)| id));

        let retries_out = retried
            .clone()
            .entries()
            .map(q!(|(id, f)| Request { id, attempt: f.attempt }));

        let newly_sent = new_requests.clone().map(q!(|r| (
            r.id,
            InFlight {
                first_sent: Instant::now(),
                last_sent: Instant::now(),
                attempt: 1,
            }
        )));

        // Keys of `kept`, `retried` and `newly_sent` are pairwise disjoint, so `first()` is exact.
        outstanding = kept
            .into_keyed_stream()
            .chain(retried.into_keyed_stream())
            .chain(newly_sent.into_keyed())
            .first();

        (
            new_requests.chain(retries_out),
            completions,
            abandoned,
        )
    };

    let outgoing_for_report = outgoing.clone();

    // ---- Server: explicit FIFO backlog, bounded work per tick ---------------------------
    // Requests wait in `backlog`, a Hydro-level queue that persists across ticks. Each tick
    // dequeues at most `max_per_tick` requests and burns `service_time` of CPU on each, so a
    // tick never runs longer than `max_per_tick * service_time`. Keeping ticks short matters:
    // a DFIR node only reads its sockets between ticks, and a node blocked writing to a full
    // socket cannot read either. (With unbounded work per tick, the client and server deadlock
    // on each other's full socket buffers as soon as the server falls behind.)
    let incoming = outgoing
        .send(server, TCP.fail_stop().bincode())
        .assume_ordering::<TotalOrder>(
            nondet!(/** single connection; served FIFO in arrival order */),
        );

    let (processed, backlog_depth) = sliced! {
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Request, Tick<_>, Bounded, TotalOrder>>();

        let queued = backlog.chain(arrivals).enumerate();
        let served = queued
            .clone()
            .filter_map(q!(move |(i, req)| if i < max_per_tick as usize { Some(req) } else { None }));
        backlog = queued.filter_map(q!(move |(i, req)| if i >= max_per_tick as usize { Some(req) } else { None }));

        let processed = served.map(q!(move |req| {
            let deadline = Instant::now() + Duration::from_nanos(service_nanos);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
            req
        }));

        (processed, backlog.clone().count())
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
        let punctuation = use::batch(server.source_interval(q!(Duration::from_nanos(report_nanos))).enumerate(), nondet!(/** wall-clock reporting window */));
        let processed = use::batch(processed, nondet!(/** wall-clock reporting window */));
        let backlog_depth = use::snapshot(backlog_depth, nondet!(/** wall-clock reporting window */));
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
            .zip(backlog_depth);
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ServerWindow::default())));

        report.into_stream()
    };
    server_windows.for_each(q!(|((w, (i, _)), backlog)| println!(
        "server t={}s processed={} first_attempts={} retries={} backlog={}",
        i + 1,
        w.first_attempts + w.retries,
        w.first_attempts,
        w.retries,
        backlog
    )));

    // ---- Client report ------------------------------------------------------------------
    let client_windows = sliced! {
        let punctuation = use::batch(client.source_interval(q!(Duration::from_nanos(report_nanos))).enumerate(), nondet!(/** wall-clock reporting window */));
        let completions = use::batch(completions, nondet!(/** wall-clock reporting window */));
        let sent = use::batch(outgoing_for_report, nondet!(/** wall-clock reporting window */));
        let abandoned = use::batch(abandoned, nondet!(/** wall-clock reporting window */));
        let mut acc = use::state(|l| l.singleton(q!(ClientWindow::default())));

        let window_end = punctuation.first();
        let from_completions = completions.fold(
            q!(ClientWindow::default),
            q!(|w, c: Completion| {
                w.completed += 1;
                w.latency_sum += c.latency;
                w.latency_max = w.latency_max.max(c.latency);
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
                latency_sum: a.latency_sum + c.latency_sum,
                latency_max: a.latency_max.max(c.latency_max),
                sent_first: a.sent_first + s.sent_first,
                sent_retry: a.sent_retry + s.sent_retry,
                abandoned: a.abandoned + ab as u64,
            }));

        let report = merged.clone().filter_if(window_end.clone().is_some()).zip(window_end.clone());
        acc = merged
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| ClientWindow::default())));

        report.into_stream()
    };
    client_windows.for_each(q!(|(w, (i, _))| println!(
        "client t={}s completed={} mean_latency_ms={:.2} max_latency_ms={:.2} sent_first={} sent_retry={} abandoned={}",
        i + 1,
        w.completed,
        w.mean_latency().as_secs_f64() * 1000.0,
        w.latency_max.as_secs_f64() * 1000.0,
        w.sent_first,
        w.sent_retry,
        w.abandoned
    )));
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hydro_deploy::Deployment;
    use hydro_lang::deploy::{DeployCrateWrapper, TrybuildHost};

    use super::*;

    /// One line of client output, keyed by the second it was reported at.
    #[derive(Debug, Clone)]
    struct ClientLine {
        t: u64,
        completed: u64,
        mean_latency_ms: f64,
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
            mean_latency_ms: field(line, "mean_latency_ms"),
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
        retry_storm(&client, &server, config);

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
        window.iter().map(|l| l.mean_latency_ms).sum::<f64>() / window.len() as f64
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
    /// request that waits longer than 200 ms is sent again (twice), so once the backlog exceeds
    /// the timeout the *offered* load becomes 3 x 400 = 1200 req/s -- still over capacity --
    /// even after the trigger ends.
    const BASE: RetryStormConfig = RetryStormConfig {
        load_tick: Duration::from_millis(5),
        baseline_per_tick: 2,
        trigger_per_tick: 6,
        trigger_start_tick: 2000, // t = 10 s
        trigger_end_tick: 2600,   // t = 13 s
        service_time: Duration::from_millis(1),
        max_per_tick: 20,
        timeout: Duration::from_millis(200),
        max_attempts: 3,
        report_interval: Duration::from_secs(1),
    };

    const RUN_SECONDS: u64 = 40;

    /// Observed on a laptop with `BASE`: baseline goodput 400/s at ~2 ms; the trigger raises
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
            "baseline: {baseline_goodput:.0}/s at {baseline_latency:.2} ms; \
             tail (t>=25s): goodput {tail_goodput:.0}/s, abandoned {tail_abandoned}, \
             server work first={tail_first} retries={tail_retries}, \
             backlog {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );

        assert!(
            baseline_latency < 20.0,
            "baseline latency should be low, got {baseline_latency} ms"
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
            .map(|l| l.mean_latency_ms)
            .fold(0.0, f64::max);
        println!("no trigger: goodput {goodput:.0}/s, mean latency {latency:.2} ms, worst 1s-window mean {worst:.2} ms");

        assert!(latency < 20.0, "latency should stay low, got {latency} ms");
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
        println!("baseline mean latency {baseline:.2} ms; tail mean latency {tail:.2} ms");

        assert!(
            baseline < 20.0,
            "baseline latency should be low, got {baseline} ms"
        );
        assert!(
            tail < 20.0,
            "latency should return to baseline, got {tail} ms"
        );
        assert!(goodput_in(&client, 25, RUN_SECONDS) > 350.0);
        assert_eq!(tail_retries, 0);
        assert!(client.iter().all(|l| l.sent_retry == 0));
        assert!(server.iter().filter(|l| l.t >= 25).all(|l| l.backlog == 0));
    }
}
