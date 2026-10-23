//! Retries with an attempt cap and no throttle.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 3 | 800 repeated sends, backlog still growing | hazardous, by exhibited collapse |
//!
//! Each request is sent at most three times and is abandoned at its third timeout, so for `N`
//! input requests sends are at most `3N` and serves at most `3N` under every schedule. The cap
//! bounds the work one request can cause; it does not bound the rate at which the continuing
//! input causes it. While replies take longer than six ticks, every new request is sent three
//! times, which offers six copies per round against a capacity of five, so the queue keeps
//! growing and replies keep taking longer than six ticks.
//!
//! A deployment wires `client.source_interval(period)` and `server.source_interval(period)` to
//! the clock parameters of [`retry_service`](super::retry_service).

#[cfg(test)]
mod sim_tests {
    use super::super::*;

    const CONFIG: RetryConfig = RetryConfig {
        timeout_ticks: 6,
        server_capacity: 5,
        max_attempts: 3,
        policy: Policy::None,
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// The burst leaves a queue of several hundred copies at round 160. From then on every request
    /// times out twice and is sent three times, so the two requests offered each round put six
    /// copies into a queue that drains five, and the queue grows by about one per round for the
    /// rest of the run. In the tail every round has four repeated sends (200 rounds, 800 in all),
    /// the queue is longer at round 800 than at round 600, and every request is abandoned at its
    /// third timeout (400 abandonments in the tail) while its copies are still queued.
    #[test]
    fn bounded_disturbance_collapses() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("no_governance tail: {s:?}");
        assert_eq!(s.repeated_sent, 800);
        assert!(s.backlog_last > s.backlog_first);
        assert!(s.dropped > 0);
    }

    #[test]
    fn steady_state_control() {
        use super::super::harness::*;
        assert_steady_state(CONFIG);
    }

    /// Sends and serves are each at most `3N`.
    #[test]
    fn random_schedule_accounting() {
        use super::super::harness::*;
        assert_random_schedule_bound(CONFIG, |n, _| (3 * n) as usize);
    }
}
