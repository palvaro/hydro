//! Retries with an attempt cap and a clock-refilled token bucket.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | bucket | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 3 | 10 initial, 10 capacity, 1 token per client clock element | no repeated sends, backlog 0, every request completes | benign, by assurance argument |
//!
//! A retry spends one token, and tokens come from the client's clock at one per element up to a
//! bucket of ten. Two bounds hold under every schedule for `N` input requests and `T` client
//! clock elements. The attempt cap gives sends at most `3N`. The bucket gives retries at most
//! `10 + T`, so sends at most `N + 10 + T`. Serves are at most sends. The first bound is a function
//! of the input alone; the second says that however long replies are late, the client adds at most
//! one copy per clock element to the server's queue, so with two new requests per round the offered
//! load is at most three copies per round against a capacity of five, and a long queue always
//! drains. The bucket of ten bounds the burst of retries a full bucket can release at once.
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
        policy: Policy::TimeBudget {
            initial_tokens: 10,
            capacity: 10,
            refill_per_tick: 1,
        },
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// The burst leaves a queue of about 420 copies at round 160, plus at most one retry per round
    /// and the ten initial tokens. From round 160 the client offers two new requests and at most one
    /// retry per round, three against a capacity of five, so the queue shrinks by at least two per
    /// round and is empty by about round 400. Requests whose two opportunities both find an empty
    /// bucket are abandoned at their third timeout while their original copy is still queued; those
    /// are the dropped records during the drain. In the tail there are no repeated sends, the
    /// queue is empty, and every request offered completes in its round.
    #[test]
    fn bounded_disturbance_recovers() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("time_budget tail: {s:?}");
        assert_eq!(s.repeated_sent, 0);
        assert_eq!((s.backlog_first, s.backlog_last), (0, 0));
        assert_eq!(s.completed, s.offered);
    }

    #[test]
    fn steady_state_control() {
        use super::super::harness::*;
        assert_steady_state(CONFIG);
    }

    /// Sends and serves are each at most the smaller of `3N` and `N + 10 + T`.
    #[test]
    fn random_schedule_accounting() {
        use super::super::harness::*;
        assert_random_schedule_bound(CONFIG, |n, t| (3 * n).min(n + 10 + t) as usize);
    }
}
