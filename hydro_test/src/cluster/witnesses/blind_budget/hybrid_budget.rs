//! Retries with an attempt cap and a token bucket refilled by both the clock and useful replies.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | bucket | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 3 | 10 initial, 10 capacity, 1 token per client clock element and 1 per 4 useful replies | no repeated sends, queue empty, every request completes | benign, by assurance argument |
//!
//! A retry spends one token; the clock adds one per element and useful replies add one per four,
//! up to a bucket of ten. For `N` input requests and `T` client clock elements, retries are at most
//! `10 + T + floor(N/4)` and sends at most `N + 10 + T + floor(N/4)`; the attempt cap gives `3N`,
//! and the smaller bound applies. Serves are at most sends. During a drain the client offers two
//! new requests, at most one clock retry, and at most a quarter of the server's throughput in
//! reply-earned retries per round, which is at most four and a quarter copies against a capacity
//! of five, so a long queue always drains, more slowly than under either refill alone.
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
        policy: Policy::HybridBudget {
            initial_tokens: 10,
            capacity: 10,
            refill_per_tick: 1,
            replies_per_token: 4,
        },
    };

    /// Hand-computed expectation, written before measuring, and corrected after.
    ///
    /// The first estimate took the retry demand during the drain to be two and a quarter copies
    /// per round for as long as the queue was long, which would have offered four and a quarter
    /// copies against a capacity of five and needed about 750 rounds to drain a queue of 560, past
    /// the end of the run. That overstated the demand: a request has only two retry opportunities,
    /// six and twelve ticks after it is sent, and is abandoned at its third timeout, so a request
    /// offered during the drain stops asking for tokens after eighteen ticks whether or not its
    /// copy has been served. The tokens the clock and the replies supply therefore go unspent for
    /// much of the drain, the queue shrinks faster than three quarters of a copy per round, and it
    /// is empty before round 600. The measured tail has no repeated sends, an empty queue, and every
    /// request completing in its round.
    #[test]
    fn bounded_disturbance_recovers() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("hybrid_budget tail: {s:?}");
        assert_eq!(s.repeated_sent, 0);
        assert_eq!((s.backlog_first, s.backlog_last), (0, 0));
        assert_eq!(s.completed, s.offered);
    }

    #[test]
    fn steady_state_control() {
        use super::super::harness::*;
        assert_steady_state(CONFIG);
    }

    /// Sends and serves are each at most the smaller of `3N` and `N + 10 + T + floor(N/4)`.
    #[test]
    fn random_schedule_accounting() {
        use super::super::harness::*;
        assert_random_schedule_bound(CONFIG, |n, t| (3 * n).min(n + 10 + t + n / 4) as usize);
    }
}
