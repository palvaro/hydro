//! HydroFrac (lineage-driven fault injection) on [`super::rpc_retry`]: there is nothing for it
//! to check.
//!
//! HydroFrac searches for fault sets (crashes, message omissions) that break a collection the
//! program labels `EventualConsistency`, which it does on cluster-located collections (a
//! `broadcast`, an `assert_has_consistency_of`). `rpc_with_retries` is two processes, a client
//! and a server, and labels nothing: its claims are about load (does the retry policy amplify
//! it?), which is Cress's and Sedge's question, not about agreement under faults. Run against
//! the automatic specification (`Spec::all_labels`), with both processes crashable, HydroFrac
//! reports no labelled collection, and the test pins that down.
//!
//! ```sh
//! cargo test -p hydro_test --lib cluster::rpc_retry_ldfi -- --nocapture
//! ```

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Ldfi};
    use hydro_lang::sim::spec::Spec;

    use crate::cluster::rpc_retry::{Client, RetryPolicy, Server, rpc_with_retries};

    #[test]
    fn rpc_retry_has_no_labels_for_hydrofrac() {
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_client_report, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            RetryPolicy {
                timeout_ticks: 40,
                max_attempts: 3,
            },
        );
        let _abandoned = outputs.abandoned.sim_output();

        let spec = Spec::all_labels(&mut flow);
        let compiled: CompiledSim = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .unit_test_fuzz_iterations(8)
            .compiled();

        let start = std::time::Instant::now();
        let results = Ldfi::new()
            .exploration(Exploration::Fuzz { instances: 8 })
            .crashable("client", &client)
            .crashable("server", &server)
            .spec(&spec)
            .search_all_labels(&compiled, async |run| {
                request.send(1);
                request.send(2);
                hydro_lang::sim::quiesce().await;
                run.end_of_finite_failures();
                for _ in 0..3 {
                    clock.send(());
                    hydro_lang::sim::quiesce().await;
                }
            });
        eprintln!(
            "rpc_with_retries under HydroFrac: {:.1}s, {} labelled collection(s)\n{results}",
            start.elapsed().as_secs_f64(),
            results.verdicts.len()
        );
        assert!(results.verdicts.is_empty(), "{results}");
    }
}
