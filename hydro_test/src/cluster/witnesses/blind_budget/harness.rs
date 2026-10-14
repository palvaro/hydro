//! The harness every configuration's labeling tests share: a deterministic run with a bounded
//! burst and a long tail, a steady-state control, and a closed-form work bound asserted under
//! random schedules. Nothing here knows anything but the service's inputs and outputs.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::quiesce;

use super::{Client, Completion, Dropped, Reply, Request, RetryConfig, Server, retry_service, steady_state_inputs};

/// What one round of the deterministic run did.
#[derive(Clone, Debug, Default)]
pub struct Round {
    pub offered: usize,
    pub sent: usize,
    /// Sends with an attempt number above one.
    pub repeated_sent: usize,
    pub served: usize,
    /// Serves of a copy with an attempt number above one.
    pub repeated_served: usize,
    pub completed: usize,
    pub dropped: usize,
    /// The client's pending count after the round.
    pub pending: usize,
    /// The server's queue length after the round.
    pub backlog: usize,
}

/// Rounds in the deterministic run.
pub const ROUNDS: usize = 800;
/// Requests offered per round outside the burst.
pub const BASELINE: u32 = 2;
/// Requests offered per round during the burst.
pub const BURST: u32 = 12;
/// The burst occupies rounds `START..END`.
pub const START: usize = 100;
pub const END: usize = 160;
/// The tail the collapse and recovery assertions read.
pub const TAIL: std::ops::Range<usize> = 600..800;

/// Runs `ROUNDS` rounds of the service under `config`, offering `BASELINE` requests per round
/// and, when `trigger` is set, `BURST` per round during `START..END`, with one client clock
/// element and one server clock element per round. Returns one record per round.
pub fn trace(config: RetryConfig, trigger: bool) -> Vec<Round> {
    let mut flow = FlowBuilder::new();
    let client = flow.process::<Client>();
    let server = flow.process::<Server>();
    let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (client_tick, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
    let (server_tick, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
    let outputs = retry_service(&client, &server, requests, client_clock, server_clock, config);
    let sent = outputs.sent.sim_output();
    let served = outputs.served.sim_output();
    let replies = outputs.replies.sim_output();
    let completed = outputs.completed.sim_output();
    let dropped = outputs.dropped.sim_output();
    let pending = outputs.pending.sim_output();
    let backlog = outputs.backlog.sim_output();
    let mut out = Vec::with_capacity(ROUNDS);
    let out_ref = &mut out;
    flow.sim().run_prompt(async move || {
        for round in 0..ROUNDS {
            let offered = if trigger && (START..END).contains(&round) { BURST } else { BASELINE };
            request_send.send_many(steady_state_inputs(round, offered));
            client_tick.send(());
            server_tick.send(());
            quiesce().await;
            let sent_items = sent.collect::<Vec<Request>>().await;
            let served_items = served.collect::<Vec<Request>>().await;
            let _ = replies.collect::<Vec<Reply>>().await;
            let completed_n = completed.collect::<Vec<Completion>>().await.len();
            let dropped_n = dropped.collect::<Vec<Dropped>>().await.len();
            let p = pending.collect::<Vec<usize>>().await.last().copied().unwrap_or_default();
            let b = backlog.collect::<Vec<usize>>().await.last().copied().unwrap_or_default();
            out_ref.push(Round {
                offered: offered as usize,
                sent: sent_items.len(),
                repeated_sent: sent_items.iter().filter(|r| r.attempt > 1).count(),
                served: served_items.len(),
                repeated_served: served_items.iter().filter(|r| r.attempt > 1).count(),
                completed: completed_n,
                dropped: dropped_n,
                pending: p,
                backlog: b,
            });
        }
    });
    out
}

/// Sums and endpoints over a slice of rounds, for printing and asserting.
#[derive(Clone, Debug)]
pub struct TailSummary {
    pub offered: usize,
    pub sent: usize,
    pub repeated_sent: usize,
    pub served: usize,
    pub repeated_served: usize,
    pub completed: usize,
    pub dropped: usize,
    pub pending_first: usize,
    pub pending_last: usize,
    pub backlog_first: usize,
    pub backlog_last: usize,
}

pub fn summarize(rounds: &[Round]) -> TailSummary {
    TailSummary {
        offered: rounds.iter().map(|r| r.offered).sum(),
        sent: rounds.iter().map(|r| r.sent).sum(),
        repeated_sent: rounds.iter().map(|r| r.repeated_sent).sum(),
        served: rounds.iter().map(|r| r.served).sum(),
        repeated_served: rounds.iter().map(|r| r.repeated_served).sum(),
        completed: rounds.iter().map(|r| r.completed).sum(),
        dropped: rounds.iter().map(|r| r.dropped).sum(),
        pending_first: rounds.first().map(|r| r.pending).unwrap_or_default(),
        pending_last: rounds.last().map(|r| r.pending).unwrap_or_default(),
        backlog_first: rounds.first().map(|r| r.backlog).unwrap_or_default(),
        backlog_last: rounds.last().map(|r| r.backlog).unwrap_or_default(),
    }
}

/// The steady-state control every configuration must pass: with no burst, every round's
/// requests complete in that round, nothing is dropped, and sends and serves equal the input.
pub fn assert_steady_state(config: RetryConfig) {
    let t = trace(config, false);
    assert!(t.iter().all(|r| r.completed == r.offered && r.backlog <= 2));
    let s = summarize(&t);
    assert_eq!((s.sent, s.served, s.dropped), (ROUNDS * BASELINE as usize, ROUNDS * BASELINE as usize, 0));
}

/// Requests offered in the random-schedule run, all at once.
pub const FUZZ_REQUESTS: u64 = 24;
/// Clock elements delivered to each side in the random-schedule run.
pub const FUZZ_TICKS: usize = 80;
pub const FUZZ_ITERATIONS: usize = 256;

/// Offers `FUZZ_REQUESTS` requests at once and `FUZZ_TICKS` clock elements to each side under
/// `FUZZ_ITERATIONS` random schedules, and asserts that the client's sends and the server's
/// serves are each at most `bound(FUZZ_REQUESTS, FUZZ_TICKS)`.
pub fn assert_random_schedule_bound(config: RetryConfig, bound: impl Fn(u64, u64) -> usize) {
    let bound = bound(FUZZ_REQUESTS, FUZZ_TICKS as u64);
    let mut flow = FlowBuilder::new();
    let client = flow.process::<Client>();
    let server = flow.process::<Server>();
    let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (client_tick, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
    let (server_tick, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
    let outputs = retry_service(&client, &server, requests, client_clock, server_clock, config);
    let sent = outputs.sent.sim_output();
    let served = outputs.served.sim_output();
    flow.sim().unit_test_fuzz_iterations(FUZZ_ITERATIONS).fuzz(async || {
        request_send.send_many(0..FUZZ_REQUESTS);
        for _ in 0..FUZZ_TICKS {
            client_tick.send(());
            server_tick.send(());
            quiesce().await;
        }
        let sends = sent.collect::<Vec<Request>>().await.len();
        let serves = served.collect::<Vec<Request>>().await.len();
        assert!(sends <= bound, "{sends} sends against a bound of {bound}");
        assert!(serves <= bound, "{serves} serves against a bound of {bound}");
    });
}
