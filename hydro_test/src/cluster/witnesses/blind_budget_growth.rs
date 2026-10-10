//! The checker's growth label on the retry-governance programs, with the input confined to the
//! first 125 rounds of a 1000-round run so that holds of 128, 256 and 512 rounds outlast it.
//!
//! The six configurations share the service, the timeout (6 ticks), the capacity (5 per tick),
//! and, except for `unbounded`, the attempt cap of 3, so they differ in one thing: how a timeout
//! is turned into a re-send. What this module asserts is the growth label, which the verdict does
//! not read: the shape of the extra work once the hold outlasts the input, the multiplier against
//! the unheld run, and the reaction to a hold that never ends. See
//! `design_docs/2026-09_growth_label.md` for the numbers and their reading.
//!
//! Each run takes about six seconds; the six together under a minute.

#[cfg(test)]
mod blind_budget_growth {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use cress::{CheckConfig, Report, Shape, Verdict, check};

    use crate::cluster::witnesses::blind_budget::service::{
        Client, Policy, RetryConfig, Server, retry_service, steady_state_inputs,
    };

    /// Data in rounds `0..125`, clocks in every round of 1000.
    const WORKLOAD_ROUNDS: usize = 125;
    const ROUNDS: usize = 1000;
    /// The server's arrivals edge, `use::batch(incoming, ...)` in `service.rs`.
    const ARRIVALS_EDGE: &str = "service.rs:540";

    fn run(label: &str, max_attempts: u32, policy: Policy) -> Report {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (client_clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_clock_send, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = retry_service(
            &client,
            &server,
            requests,
            client_clock,
            server_clock,
            RetryConfig {
                timeout_ticks: 6,
                server_capacity: 5,
                max_attempts,
                policy,
            },
        );
        let _sent = outputs.sent.sim_output();
        let _served = outputs.served.sim_output();
        let _replies = outputs.replies.sim_output();
        let _completed = outputs.completed.sim_output();
        let _dropped = outputs.dropped.sim_output();
        let _pending = outputs.pending.sim_output();
        let _backlog = outputs.backlog.sim_output();
        let config = CheckConfig::new(ROUNDS).with_workload_rounds(WORKLOAD_ROUNDS);
        assert_eq!(config.holds_beyond_input(), vec![128, 256, 512]);
        let r = check(flow.sim(), &config, async |round| {
            if round < WORKLOAD_ROUNDS {
                request_send.send_many(steady_state_inputs(round, 2));
            }
            client_clock_send.send(());
            server_clock_send.send(());
        });
        println!("[blind_budget_growth {label}]\n{r}");
        assert_eq!(r.verdict, Verdict::Hazardous, "{label}");
        assert!(
            r.location.as_ref().unwrap().hook.contains(ARRIVALS_EDGE),
            "{label}: named {}",
            r.location.as_ref().unwrap().hook
        );
        r
    }

    /// The shape word, the multiplier rounded to two decimals, and the stall reaction of the
    /// report's growth label.
    fn label(r: &Report) -> (String, f64, i64) {
        let g = r.growth.as_ref().expect("hazardous reports carry a growth label");
        (
            g.word(),
            (g.multiplier_value().unwrap() * 100.0).round() / 100.0,
            g.stall.1.stall,
        )
    }

    /// The last marginal rate of the worst shape, per round of delay.
    fn rate(r: &Report) -> f64 {
        match &r.growth.as_ref().unwrap().shape.as_ref().unwrap().1.beyond.as_ref().unwrap().shape {
            Shape::Linear { b, .. } | Shape::Sublinear { b, .. } | Shape::Polynomial { b, .. } => *b,
            Shape::Flat { .. } | Shape::Shed { .. } => 0.0,
        }
    }

    /// No cap and no throttle. Every one of the 248 held requests is re-sent every six ticks for as
    /// long as the hold lasts, about 41 messages per round once all are pending, so the extra work
    /// is linear in the delay. The marginal rates between the holds beyond the input are still
    /// climbing toward that figure (24 and 35 per round), because the release of a long hold is
    /// followed by a drain during which the client keeps re-sending until each id's first copy is
    /// served, and that drain is cut off by the end of the run for the longest holds. The stall
    /// reaction, about 39 per round of the run, is the cleaner estimate of the rate.
    #[test]
    fn unbounded_is_linear_at_the_timeout_rate() {
        let r = run("unbounded", u32::MAX, Policy::None);
        let (word, _, stall) = label(&r);
        assert_eq!(word, "grows");
        let b = rate(&r);
        assert!((20.0..=45.0).contains(&b), "rate {b}");
        assert!(stall >= 35_000, "stall {stall}");
    }

    /// Cap of three: every held request is re-sent twice and then abandoned, so the extra work is
    /// flat at twice the held input (496) once the hold outlasts it, the client sends about three
    /// times its reference count, and the stall reaction is the same 496.
    #[test]
    fn no_governance_is_flat_at_twice_the_input() {
        let r = run("no_governance", 3, Policy::None);
        let (word, multiplier, stall) = label(&r);
        assert_eq!(word, "stops");
        assert!((2.9..=3.0).contains(&multiplier), "multiplier {multiplier}");
        assert!((490..=500).contains(&stall), "stall {stall}");
    }

    /// Cap of three and one token per clock element with ten to start. During the hold there are
    /// about four retry opportunities per round against one token, so the client re-sends once per
    /// tick from the first timeout until the last held request's third timeout, about 130 rounds,
    /// and then stops: about 140 extra messages, flat, with the same stall reaction. The multiplier
    /// is about 1.56.
    #[test]
    fn time_budget_is_flat_at_its_refill_times_the_input_duration() {
        let r = run(
            "time_budget",
            3,
            Policy::TimeBudget {
                initial_tokens: 10,
                capacity: 10,
                refill_per_tick: 1,
            },
        );
        let (word, multiplier, stall) = label(&r);
        assert_eq!(word, "stops");
        assert!((1.45..=1.65).contains(&multiplier), "multiplier {multiplier}");
        assert!((120..=160).contains(&stall), "stall {stall}");
    }

    /// Cap of three and one token per four useful replies with ten to start. Under a hold of the
    /// server's arrivals nothing completes, so the client spends its ten initial tokens and every
    /// held request is abandoned at its third timeout before the hold ends; when the hold releases,
    /// the replies find nothing pending and earn no token. That edge reads flat at ten, and ten
    /// under a stall. The worst hold is a different one: holding the client's own arrivals for 64
    /// rounds releases 128 requests at once, the server drains them with latency above the timeout,
    /// and the completions earn tokens that are spent on requests still inside their eighteen-tick
    /// lifetime, 35 extra messages. With the cap that number is bounded by the completions the
    /// server can make in three timeouts, divided by four, plus the initial bucket, which is a
    /// constant of the configuration and not a fraction of the input as it was before the repair.
    /// The multiplier is therefore about 1.14 at this input and falls as the input grows.
    #[test]
    fn success_budget_is_flat_at_its_initial_bucket() {
        let r = run(
            "success_budget",
            3,
            Policy::SuccessBudget {
                initial_tokens: 10,
                capacity: 10,
                replies_per_token: 4,
            },
        );
        let (word, multiplier, stall) = label(&r);
        assert_eq!(word, "stops");
        assert!((1.10..=1.16).contains(&multiplier), "multiplier {multiplier}");
        assert_eq!(stall, 10);
    }

    /// Both refills: during the hold only the clock refills, so this reads like the time budget,
    /// about 140 extra messages, flat.
    #[test]
    fn hybrid_budget_is_flat_like_the_time_budget() {
        let r = run(
            "hybrid_budget",
            3,
            Policy::HybridBudget {
                initial_tokens: 10,
                capacity: 10,
                refill_per_tick: 1,
                replies_per_token: 4,
            },
        );
        let (word, multiplier, stall) = label(&r);
        assert_eq!(word, "stops");
        assert!((1.45..=1.65).contains(&multiplier), "multiplier {multiplier}");
        assert!((120..=160).contains(&stall), "stall {stall}");
    }

    /// The breaker re-sends during its eight consecutive timeouts, then opens and refuses new
    /// input for the rest of the hold, so the extra work is small and flat and the stall reaction is
    /// negative: it sends less than the unheld run.
    #[test]
    fn circuit_breaker_is_flat_and_sheds() {
        let r = run(
            "circuit_breaker",
            3,
            Policy::CircuitBreaker {
                timeout_threshold: 8,
                open_ticks: 12,
            },
        );
        let (word, multiplier, stall) = label(&r);
        assert!(word == "stops" || word == "shedding", "{word}");
        assert!(multiplier <= 1.15, "multiplier {multiplier}");
        assert!(stall < 0, "stall {stall}");
    }
}
