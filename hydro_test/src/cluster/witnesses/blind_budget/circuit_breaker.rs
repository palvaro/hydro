//! Retries with an attempt cap and a circuit breaker.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | breaker | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | 3 | opens after 8 consecutive timeouts, stays open 12 ticks, then probes with one pending request | no repeated sends, backlog 0, every request completes | benign, by assurance argument |
//!
//! While the breaker is closed, retries follow the attempt cap. After eight consecutive timeouts
//! it opens: for twelve ticks new requests are refused outright and nothing is re-sent, then one
//! pending request is re-sent as a probe; a reply closes the breaker and a timeout reopens it. For
//! `N` input requests, every accepted request is sent at most three times and every refused request
//! is sent zero times, so sends are at most `3N` and serves at most `3N` under every schedule. That
//! bound is the same as the capped client's without a breaker; what the breaker changes is not the
//! bound but the load during a disturbance, which it cuts to zero by refusing input. The cost is the
//! refused requests, which the `dropped` output carries as `Rejected`.
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
        policy: Policy::CircuitBreaker {
            timeout_threshold: 8,
            open_ticks: 12,
        },
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// The burst makes replies late within a few rounds, eight consecutive timeouts open the
    /// breaker, and from then until the queue is short again the client alternates between twelve
    /// ticks of refusing everything and one probe. Refused requests never reach the server, so the
    /// queue drains at five per round with almost nothing arriving and is empty well before round
    /// 600. In the tail there are no repeated sends, the queue is empty, and every request offered
    /// completes in its round. Over the whole run many requests are refused; that is the price.
    #[test]
    fn bounded_disturbance_recovers_by_refusing_input() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("circuit_breaker tail: {s:?}");
        assert_eq!(s.repeated_sent, 0);
        assert_eq!((s.backlog_first, s.backlog_last), (0, 0));
        assert_eq!(s.completed, s.offered);
        let whole = summarize(&t);
        assert!(whole.dropped > 0);
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
