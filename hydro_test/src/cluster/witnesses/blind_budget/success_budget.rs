//! Retries with an attempt cap and a success-refilled token bucket.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | bucket | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 3 | 10 initial, 10 capacity, 1 token per 4 useful replies | no repeated sends, backlog 0, every request completes | benign, by assurance argument |
//!
//! A retry spends one token, and tokens come only from useful replies, one per four, up to a
//! bucket of ten. Each id yields at most one useful reply, because the first reply for a pending id
//! removes it. So for `N` input requests the client earns at most `floor(N/4)` tokens and issues
//! at most `10 + floor(N/4)` retries, and sends are at most `N + 10 + floor(N/4)`; the attempt cap
//! gives `3N` as well, and the smaller bound applies. Serves are at most sends. Both bounds are
//! functions of the input alone. When nothing is being served, nothing refills, so a client facing
//! a silent server spends its ten initial tokens and then sends nothing but new requests; when the
//! server is serving, retries are at most a quarter of its throughput, so the offered load is at
//! most the new requests plus a quarter of the capacity, and a long queue always drains.
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
        policy: Policy::SuccessBudget {
            initial_tokens: 10,
            capacity: 10,
            replies_per_token: 4,
        },
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// The burst leaves a queue of about 420 copies at round 160 plus the ten initial retries. The
    /// server serves five per round, which earns the client five quarters of a token per round, so
    /// during the drain the client offers two new requests and at most one and a quarter retries
    /// per round against a capacity of five, and the queue shrinks by at least one and three
    /// quarters per round, emptying by about round 400. In the tail there are no repeated sends,
    /// the queue is empty, and every request offered completes in its round.
    #[test]
    fn bounded_disturbance_recovers() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("success_budget tail: {s:?}");
        assert_eq!(s.repeated_sent, 0);
        assert_eq!((s.backlog_first, s.backlog_last), (0, 0));
        assert_eq!(s.completed, s.offered);
    }

    #[test]
    fn steady_state_control() {
        use super::super::harness::*;
        assert_steady_state(CONFIG);
    }

    /// Sends and serves are each at most the smaller of `3N` and `N + 10 + floor(N/4)`.
    #[test]
    fn random_schedule_accounting() {
        use super::super::harness::*;
        assert_random_schedule_bound(CONFIG, |n, _| (3 * n).min(n + 10 + n / 4) as usize);
    }
}
