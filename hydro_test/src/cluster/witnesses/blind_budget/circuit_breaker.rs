//! Retries with a circuit breaker, labeled benign by assurance argument.
//!
//! | baseline per round | capacity | trigger | timeout | asserted work | label and basis |
//! |---:|---:|---|---:|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 20..30 | 6 ticks | 220 requests; sends and serves each at most 660 | benign, by assurance argument |
//!
//! New inputs are dropped while the breaker is open or probing. Let `N` be the number of input requests and `A` be `max_attempts`. Each accepted input creates one
//! outstanding entry. That entry starts with one send, increments its attempt counter for every
//! resend, and can never emit after the counter reaches `A`. Therefore total sends are at most
//! `N*A`. A probe increments the same attempt counter. The server serves each wire message at most once, so total serves are also at most `N*A`,
//! under every delivery schedule. This bound depends only on input and the policy parameter, not on
//! run length or reply delay. The deterministic burst and 256 random schedules assert both bounds.
//! After eight consecutive timeout observations the breaker opens for twelve ticks, sends one existing pending request as a probe, and reopens when that probe times out. A deployment supplies both clocks with `source_interval`; tests supply explicit timer streams.

#[cfg(test)]
mod sim_tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;
    use super::super::*;

    const CONFIG: RetryConfig = RetryConfig { timeout_ticks: 6, server_capacity: 5, policy: Policy::CircuitBreaker { max_attempts: 3, timeout_threshold: 8, open_ticks: 12 } };

    /// Hand-computed expectation, written before measuring.
    ///
    /// Baseline offers two requests against five serves. A ten-round burst of twelve offers 100
    /// extras and can queue at least seventy copies. Regardless of that delay, 220 inputs can emit
    /// no more than 660 sends and the server can serve no more than those same 660 copies.
    fn run(burst: bool) -> (usize, usize, usize) {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>(); let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (client_tick, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_tick, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let output = retry_service(&client, &server, requests, client_clock, server_clock, CONFIG);
        let sent = output.sent.sim_output(); let served = output.served.sim_output();
        let replies = output.replies.sim_output(); let completed = output.completed.sim_output();
        let dropped = output.dropped.sim_output(); let pending = output.pending.sim_output(); let backlog = output.backlog.sim_output();
        let a = std::sync::atomic::AtomicUsize::new(0);
        let b = std::sync::atomic::AtomicUsize::new(0);
        let c = std::sync::atomic::AtomicUsize::new(0);
        let (ar, br, cr) = (&a, &b, &c);
        flow.sim().run_prompt(async move || {
            for round in 0..60 { let n = if burst && (20..30).contains(&round) { 12 } else { 2 };
                request_send.send_many(steady_state_inputs(round, n)); client_tick.send(()); server_tick.send(()); quiesce().await; }
            for _ in 0..150 { client_tick.send(()); server_tick.send(()); quiesce().await; }
            let a = sent.collect::<Vec<Request>>().await.len(); let b = served.collect::<Vec<Request>>().await.len();
            let c = completed.collect::<Vec<Completion>>().await.len();
            let _ = replies.collect::<Vec<Reply>>().await; let _ = dropped.collect::<Vec<Dropped>>().await;
            let _ = pending.collect::<Vec<usize>>().await; let _ = backlog.collect::<Vec<usize>>().await;
            ar.store(a, std::sync::atomic::Ordering::Relaxed);
            br.store(b, std::sync::atomic::Ordering::Relaxed);
            cr.store(c, std::sync::atomic::Ordering::Relaxed);
        });
        (a.load(std::sync::atomic::Ordering::Relaxed), b.load(std::sync::atomic::Ordering::Relaxed), c.load(std::sync::atomic::Ordering::Relaxed))
    }

    #[test]
    fn burst_accounting() { let (sent, served, _) = run(true); assert!(sent <= 220 * 3); assert!(served <= 220 * 3); }

    #[test]
    fn steady_state_control() { let (sent, served, completed) = run(false); assert_eq!((sent, served, completed), (120, 120, 120)); }

    #[test]
    fn random_schedule_accounting() {
        let mut flow = FlowBuilder::new(); let client = flow.process::<Client>(); let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (client_tick, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_tick, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let output = retry_service(&client, &server, requests, client_clock, server_clock, CONFIG);
        let sent = output.sent.sim_output(); let served = output.served.sim_output();
        flow.sim().unit_test_fuzz_iterations(256).fuzz(async || {
            request_send.send_many((0..24).map(|x| x as u64));
            for _ in 0..80 { client_tick.send(()); server_tick.send(()); quiesce().await; }
            assert!(sent.collect::<Vec<Request>>().await.len() <= 72);
            assert!(served.collect::<Vec<Request>>().await.len() <= 72);
        });
    }
}
