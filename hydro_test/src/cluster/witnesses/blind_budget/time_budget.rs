//! A retry client governed by a time-refilled token bucket.
//!
//! | baseline per round | capacity | trigger | timeout | asserted tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 800 repeated sends, 666 repeated serves; backlog 1,100 -> 1,299 | hazardous, by exhibited collapse |
//!
//! The deterministic bounded trigger produces an exhibited collapse: redundant sends and serves
//! remain in rounds 600 through 800, and the server backlog is still growing at the end.
//!
//! The client gains four tokens per client timer element, retains at most forty, and spends one
//! token for each retry. A timeout that finds no token is dropped rather than deferred. A deployment
//! wires `client.source_interval(period)` and `server.source_interval(period)` to the clock
//! parameters of [`retry_service`](super::retry_service).

#[cfg(test)]
mod sim_tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;
    use super::super::*;

    #[derive(Clone, Debug, Default)]
    struct Round { offered: usize, sent: usize, repeated_sent: usize, served: usize, repeated_served: usize, completed: usize, dropped: usize, pending: usize, backlog: usize }

    const CONFIG: RetryConfig = RetryConfig { timeout_ticks: 6, server_capacity: 5, policy: Policy::TimeBudget { initial_tokens: 0, capacity: 40, refill_per_tick: 4 } };
    const ROUNDS: usize = 800;
    const START: usize = 100;
    const END: usize = 160;

    /// Hand-computed expectation, written before measuring.
    ///
    /// Baseline demand is two first sends against five serves, so replies arrive before six ticks.
    /// The bounded trigger adds ten requests per round for sixty rounds, building at least 420 queued
    /// copies before retries. Four refill tokens per round then permit four retries while two new
    /// requests continue, offering six copies against capacity five. The queue should therefore keep
    /// growing after round 160, and the distant tail should contain about 800 retries and at most
    /// 1,000 serves, with useful completions suppressed by old copies.
    fn run(trigger: bool) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (client_tick, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_tick, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = retry_service(&client, &server, requests, client_clock, server_clock, CONFIG);
        let sent = outputs.sent.sim_output();
        let served = outputs.served.sim_output();
        let replies = outputs.replies.sim_output();
        let completed = outputs.completed.sim_output();
        let dropped = outputs.dropped.sim_output();
        let pending = outputs.pending.sim_output();
        let backlog = outputs.backlog.sim_output();
        let mut trace = Vec::with_capacity(ROUNDS);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            for round in 0..ROUNDS {
                let offered = if trigger && (START..END).contains(&round) { 12 } else { 2 };
                request_send.send_many(steady_state_inputs(round, offered as u32));
                client_tick.send(()); server_tick.send(()); quiesce().await;
                let sent_items = sent.collect::<Vec<Request>>().await;
                let sent_n = sent_items.len();
                let repeated_sent = sent_items.iter().filter(|request| request.attempt > 1).count();
                let served_items = served.collect::<Vec<Request>>().await;
                let served_n = served_items.len();
                let repeated_served = served_items.iter().filter(|request| request.attempt > 1).count();
                let _ = replies.collect::<Vec<Reply>>().await;
                let completed_n = completed.collect::<Vec<Completion>>().await.len();
                let dropped_n = dropped.collect::<Vec<Dropped>>().await.len();
                let p = pending.collect::<Vec<usize>>().await.last().copied().unwrap_or_default();
                let b = backlog.collect::<Vec<usize>>().await.last().copied().unwrap_or_default();
                trace_ref.push(Round { offered, sent: sent_n, repeated_sent, served: served_n, repeated_served, completed: completed_n, dropped: dropped_n, pending: p, backlog: b });
            }
        });
        trace
    }

    #[test]
    fn bounded_disturbance_has_a_distant_tail() {
        let trace = run(true);
        assert!(trace[..START].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let tail = &trace[600..800];
        eprintln!("tail sent={} repeated_sent={} served={} repeated_served={} completed={} dropped={} pending {} -> {} backlog {} -> {}",
            tail.iter().map(|r| r.sent).sum::<usize>(), tail.iter().map(|r| r.repeated_sent).sum::<usize>(), tail.iter().map(|r| r.served).sum::<usize>(), tail.iter().map(|r| r.repeated_served).sum::<usize>(),
            tail.iter().map(|r| r.completed).sum::<usize>(), tail.iter().map(|r| r.dropped).sum::<usize>(),
            tail.first().unwrap().pending, tail.last().unwrap().pending, tail.first().unwrap().backlog, tail.last().unwrap().backlog);
        assert_eq!(tail.iter().map(|r| r.repeated_sent).sum::<usize>(), 800);
        assert_eq!(tail.iter().map(|r| r.repeated_served).sum::<usize>(), 666);
        assert_eq!((tail.first().unwrap().backlog, tail.last().unwrap().backlog), (1100, 1299));
    }

    #[test]
    fn steady_state_control() {
        let trace = run(false);
        assert!(trace.iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        assert_eq!(trace.iter().map(|r| r.sent).sum::<usize>(), 1600);
        assert_eq!(trace.iter().map(|r| r.served).sum::<usize>(), 1600);
        assert_eq!(trace.iter().map(|r| r.dropped).sum::<usize>(), 0);
    }
}
