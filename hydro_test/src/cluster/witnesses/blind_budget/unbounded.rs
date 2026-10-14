//! Retries with no attempt cap and no throttle: the reference point every other configuration
//! is measured against.
//!
//! | baseline per round | capacity | trigger | timeout | attempt cap | expected tail (rounds 600..800) | label and basis |
//! |---:|---:|---|---:|---:|---|---|
//! | 2 requests | 5 serves | 12/round in rounds 100..160 | 6 ticks | none | repeated sends in every round, backlog still growing | hazardous, by exhibited collapse |
//!
//! Every pending request is re-sent on every timeout, and no request is ever abandoned, so the
//! only thing that ends a request is a reply. Once the server's queue is long enough that replies
//! take more than six ticks, every request in the queue is re-sent every six ticks, which lengthens
//! the queue. There is no closed form for the work as a function of the input: it is a function of
//! how long the queue stays long, which is forever once the loop closes.
//!
//! A deployment wires `client.source_interval(period)` and `server.source_interval(period)` to
//! the clock parameters of [`retry_service`](super::retry_service).

#[cfg(test)]
mod sim_tests {
    use super::super::*;

    const CONFIG: RetryConfig = RetryConfig {
        timeout_ticks: 6,
        server_capacity: 5,
        max_attempts: u32::MAX,
        policy: Policy::None,
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// The burst adds ten requests per round for sixty rounds against a spare capacity of three,
    /// so the queue holds several hundred copies by round 160 and replies take far longer than six
    /// ticks. Every request in the queue then re-sends every six ticks, dozens of copies per tick
    /// against a capacity of five, and the queue grows faster than it drains. In the tail there are
    /// repeated sends in every round, the queue is longer at round 800 than at round 600, and far
    /// fewer requests complete than are offered.
    #[test]
    fn bounded_disturbance_collapses() {
        use super::super::harness::*;
        let t = trace(CONFIG, true);
        assert!(t[..100].iter().all(|r| r.completed == r.offered && r.backlog <= 2));
        let s = summarize(&t[TAIL]);
        eprintln!("unbounded tail: {s:?}");
        assert!(t[TAIL].iter().all(|r| r.repeated_sent > 0));
        assert!(s.backlog_last > s.backlog_first);
        assert!(s.completed < s.offered / 2);
    }

    #[test]
    fn steady_state_control() {
        use super::super::harness::*;
        assert_steady_state(CONFIG);
    }
}
