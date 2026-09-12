//! One deterministic feedback campaign applied unchanged to different Hydro programs.
//!
//! Existing program tests supply the executable boundary specification: valid typed data values,
//! a controllable operational input, and cluster sizing. They do not supply protocol phases or
//! expected labels to [`run_feedback_campaign`]. The runner owns the same geometric state sweep,
//! operational repetitions, quiescence barriers, provenance analysis, and witness vocabulary for
//! every flow below.

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::feedback_campaign::{
        CampaignConfig, CampaignWitness, run_feedback_campaign,
    };

    fn scaled_campaign() -> CampaignConfig {
        // Unlike a conventional small exhaustive sim test, this campaign deliberately reaches a
        // substantial retained population. Schedule exploration is sampled; the generated input
        // and operational sequence itself is fixed and replayable.
        CampaignConfig {
            scales: vec![1, 8, 32],
            operational_repetitions: 64,
            stop_after_stable_repetitions: 2,
        }
    }

    #[test]
    fn generic_campaign_separates_heartbeat_retry_gossip_and_productive_recursion() {
        heartbeat();
        retry();
        gossip();
        transitive_closure();
    }

    fn heartbeat() {
        use crate::cluster::pure_heartbeat::pure_heartbeat;

        const MEMBERS: usize = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        pure_heartbeat(&cluster, timer)
            .entries()
            .for_each(q!(|_| {}, commutative = manual_proof!(/** observation only */)));

        flow.sim()
            .with_cluster_size(&cluster, MEMBERS)
            .with_provenance()
            .unit_test_fuzz_iterations(1)
            .fuzz(async || {
                let report = run_feedback_campaign(
                    CampaignConfig {
                        scales: vec![0],
                        operational_repetitions: 256,
                        stop_after_stable_repetitions: 2,
                    },
                    |_, count| assert_eq!(count, 0),
                    |_| {
                        for member in 0..MEMBERS as u32 {
                            timer_send.send(member, ());
                        }
                    },
                )
                .await;

                assert!(!report.observed_replay(), "heartbeat carries no application ancestry");
                assert!(!report.observed_operational_scaling(), "heartbeat has no data scale");
                assert!(report.witnesses.iter().any(|w| matches!(
                    w,
                    CampaignWitness::OperationalOnlyWork { emissions, .. }
                        if *emissions == MEMBERS * MEMBERS
                )));
            });
    }

    fn retry() {
        use crate::distributed::timeout_retry::{Request, timeout_retry_with_timers};

        let mut flow = FlowBuilder::new();
        let client = flow.process();
        let service = flow.process();
        let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (retry_send, retries) =
            client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (service_send, service_ticks) =
            service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let outputs = timeout_retry_with_timers(
            &client,
            &service,
            requests,
            retries,
            service_ticks,
            0,
        );
        outputs.completed.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));
        outputs.events.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));

        let mut sim = flow.sim();
        let manifest = sim.feedback_boundary_manifest();
        manifest.assert_inputs_covered([
            request_send.port_id(),
            retry_send.port_id(),
            service_send.port_id(),
        ]);
        let inputs: Vec<_> = manifest
            .input_ports()
            .map(|port| (port.port, port.role))
            .collect();
        assert_eq!(inputs.len(), 3, "retry boundary discovery: {inputs:?}");

        sim
            .with_provenance()
            .unit_test_fuzz_iterations(1)
            .fuzz(async || {
                let report = run_feedback_campaign(
                    scaled_campaign(),
                    |start, count| {
                        request_send.send_many((start..start + count).map(|id| Request {
                            id: id as u64,
                            value: format!("work-{id}"),
                        }));
                    },
                    |_| retry_send.send(()),
                )
                .await;

                assert!(report.observed_replay(), "retry must replay retained requests");
                assert!(
                    report.observed_operational_scaling(),
                    "one retry firing must emit more work at larger outstanding populations"
                );
            });
    }

    fn gossip() {
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

        const MEMBERS: usize = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (pump_send, pumps) =
            cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        g_set_gossip(&cluster, updates, pumps)
            .sample_eager(nondet!(/** observation only */))
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .for_each(q!(
                |_| {},
                idempotent = manual_proof!(/** observation only */)
            ));

        flow.sim()
            .with_cluster_size(&cluster, MEMBERS)
            .skip_consistency_assertions()
            .with_provenance()
            .unit_test_fuzz_iterations(1)
            .fuzz(async || {
                let report = run_feedback_campaign(
                    scaled_campaign(),
                    |start, count| {
                        update_send.send_many_unordered(
                            (start..start + count).map(|id| (0u32, id as u32)),
                        );
                    },
                    |_| pump_send.send(0, ()),
                )
                .await;

                assert!(report.observed_replay(), "repeated pumps replay the retained set");
                assert!(
                    report.observed_operational_scaling(),
                    "gossip message bytes must grow with the retained set"
                );
            });
    }

    fn transitive_closure() {
        use crate::local::productive_tc::productive_transitive_closure;

        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (edge_send, edges) =
            process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) =
            process.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(edges, steps);
        let _facts_out = facts
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_output();
        traces.for_each(q!(|_| {}));

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(1)
            .fuzz(async || {
                let report = run_feedback_campaign(
                    scaled_campaign(),
                    |start, count| {
                        // Each edge is one source event, preserving the ancestry needed to
                        // distinguish derived paths. The values form a reverse chain; the runner
                        // still owns the scale increments, step firings, and barriers.
                        edge_send.send_many((start..start + count).map(|id| {
                            let high = 10_000u32 - id as u32;
                            vec![(high - 1, high)]
                        }));
                    },
                    |_| step_send.send(()),
                )
                .await;

                assert!(
                    !report.observed_replay(),
                    "the tested TC output should not resend established facts"
                );
                assert!(
                    report.observed_input_scaling(),
                    "productive data-admission work should grow with a larger graph"
                );
            });
    }
}
