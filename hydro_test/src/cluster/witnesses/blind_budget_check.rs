//! The frozen amplification checker run on the independently written retry-governance programs.
//!
//! The five configurations under `blind_budget/` were written by an author who knew nothing
//! about the checker. This module was written by an operator who read, for each configuration,
//! only the shared constructor's signature (`retry_service`), the `Policy` and `RetryConfig`
//! type definitions, the `RetryConfig` constant each configuration builds, and the per-round
//! count its own steady-state control feeds. The verdicts in
//! `design_docs/2026-09_blind_budget_results.md` were recorded before any label, module doc,
//! or test body was read.
//!
//! Each test prints the full `Report` and asserts nothing about the verdict.

#[cfg(test)]
mod blind_budget_check {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::amplification::{CheckConfig, Report, check};

    use crate::cluster::witnesses::blind_budget::service::{
        Client, Policy, RetryConfig, Server, retry_service, steady_state_inputs,
    };

    fn config() -> CheckConfig {
        CheckConfig::default()
    }

    fn show(label: &str, r: &Report) {
        println!("[blind_budget {label}]\n{r}");
        if let Some(l) = &r.location {
            println!(
                "[blind_budget {label}] location: hook {} at {} rose {} extra work {} first reacted at {}",
                l.hook, l.source_location, l.rose, l.extra_work, l.first_reaction_at
            );
        }
        for c in &r.curves {
            let extra: Vec<i64> = c.extra_admitted.iter().map(|(_, e)| *e).collect();
            println!("[blind_budget {label}] curve {} extra_admitted {:?}", c.hook, extra);
            for (k, m) in &c.extra_sends_by_member {
                let nonzero: Vec<String> = m
                    .iter()
                    .filter(|(_, d)| **d != 0)
                    .map(|(p, d)| format!("{p}:{d}"))
                    .collect();
                if !nonzero.is_empty() {
                    println!("[blind_budget {label}]   k={k} extra sends {}", nonzero.join(" "));
                }
            }
        }
    }

    /// Every configuration shares the same service, timeout (6 ticks), server capacity (5 per
    /// tick), and steady-state input (2 requests, one client clock element, and one server clock
    /// element per round); only the policy differs.
    fn run(label: &str, policy: Policy) {
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
        let r = check(flow.sim(), &config(), async |round| {
            request_send.send_many(steady_state_inputs(round, 2));
            client_clock_send.send(());
            server_clock_send.send(());
        });
        show(label, &r);
    }

    #[test]
    fn blind_budget_no_governance() {
        run("no_governance", Policy::None { max_attempts: 3 });
    }

    #[test]
    fn blind_budget_time_budget() {
        run(
            "time_budget",
            Policy::TimeBudget {
                initial_tokens: 0,
                capacity: 40,
                refill_per_tick: 4,
            },
        );
    }

    #[test]
    fn blind_budget_success_budget() {
        run(
            "success_budget",
            Policy::SuccessBudget {
                initial_tokens: 10,
                capacity: 40,
                replies_per_token: 4,
            },
        );
    }

    #[test]
    fn blind_budget_hybrid_budget() {
        run(
            "hybrid_budget",
            Policy::HybridBudget {
                initial_tokens: 0,
                capacity: 40,
                refill_per_tick: 3,
                replies_per_token: 4,
            },
        );
    }

    #[test]
    fn blind_budget_circuit_breaker() {
        run(
            "circuit_breaker",
            Policy::CircuitBreaker {
                max_attempts: 3,
                timeout_threshold: 8,
                open_ticks: 12,
            },
        );
    }
}
