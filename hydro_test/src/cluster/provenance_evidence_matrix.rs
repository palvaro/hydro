//! Protocol-blind feedback evidence-matrix evaluation, with paired program mutations.
//!
//! Each adapter below performs boundary plumbing only: construct the flow, register a deterministic
//! legal-value generator for every discovered data input, register every operational input, and
//! enumerate all legal cluster targets. `run_evidence_matrix` owns the full port/target matrix,
//! fresh simulator instances, scale sweep, operational repetitions, and evidence schema. No
//! adapter selects the tested operational port or asserts a protocol-specific interpretation.
//!
//! Every flow is run as a baseline and as variants. A *refactor* variant inserts an identity map on
//! an input stream and must produce a field-for-field identical matrix; that is asserted, because
//! it is a property of the checker, not of a protocol. A *mechanism* variant changes one program
//! mechanism (a dedup gate, the known-fact gate, periodic publication) or one value generator; its
//! differences are recorded raw and not asserted.

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::feedback_campaign::{
        CampaignConfig, EvidenceMatrix, InputRegistry, run_evidence_matrix,
    };

    fn config() -> CampaignConfig {
        CampaignConfig {
            scales: vec![1, 8, 32],
            schedule_seeds: vec![0x5a17, 0xc0ffee, 0x1234_abcd],
            // Must exceed the largest scale (drain time) plus its stability window (scale + 1).
            operational_repetitions: 100,
            stop_after_stable_repetitions: 2,
        }
    }

    fn open_report() -> File {
        let path = std::env::var("HYDRO_CHECKER_REPORT")
            .unwrap_or_else(|_| "target/protocol_blind_feedback_checker.tsv".to_owned());
        let _ = std::fs::create_dir_all("target");
        File::create(path).unwrap()
    }

    fn write_matrix(out: &mut File, name: &str, report: &EvidenceMatrix) {
        writeln!(out, "# flow={name} manifest").unwrap();
        write!(out, "{}", report.render_manifest_tsv()).unwrap();
        writeln!(out, "# flow={name} cases").unwrap();
        write!(out, "{}", report.render_cases_tsv()).unwrap();
        writeln!(out, "# flow={name} epochs").unwrap();
        write!(out, "{}", report.render_epochs_tsv()).unwrap();
        writeln!(out, "# flow={name} evidence_by_schedule").unwrap();
        write!(out, "{}", report.render_tsv()).unwrap();
        writeln!(out, "# flow={name} schedule_variation").unwrap();
        write!(out, "{}", report.render_schedule_variations_tsv()).unwrap();
    }

    fn write_pair(out: &mut File, baseline: (&str, &EvidenceMatrix), variant: (&str, &EvidenceMatrix)) -> usize {
        let differences = baseline.1.compare(variant.1);
        writeln!(out, "# pair baseline={} variant={} differences={}", baseline.0, variant.0, differences.len()).unwrap();
        write!(out, "{}", EvidenceMatrix::render_comparison_tsv(&differences)).unwrap();
        differences.len()
    }

    #[test]
    fn one_evidence_schema_for_every_flow() {
        let mut out = open_report();

        let heartbeat = heartbeat_flow(false);
        let heartbeat_refactor = heartbeat_flow(true);
        let retry = retry_flow(false, false);
        let retry_dedup = retry_flow(true, false);
        let retry_refactor = retry_flow(false, true);
        let gossip = gossip_flow(true, false);
        let gossip_no_pump = gossip_flow(false, false);
        let gossip_refactor = gossip_flow(true, true);
        let tc_chain = tc_flow(true, false, false);
        let tc_chain_ungated = tc_flow(false, false, false);
        let tc_chain_refactor = tc_flow(true, false, true);
        let tc_cycle = tc_flow(true, true, false);
        let tc_cycle_ungated = tc_flow(false, true, false);

        for (name, matrix) in [
            ("heartbeat", &heartbeat),
            ("heartbeat_refactor", &heartbeat_refactor),
            ("retry", &retry),
            ("retry_dedup", &retry_dedup),
            ("retry_refactor", &retry_refactor),
            ("gossip", &gossip),
            ("gossip_no_pump", &gossip_no_pump),
            ("gossip_refactor", &gossip_refactor),
            ("tc_chain", &tc_chain),
            ("tc_chain_ungated", &tc_chain_ungated),
            ("tc_chain_refactor", &tc_chain_refactor),
            ("tc_cycle", &tc_cycle),
            ("tc_cycle_ungated", &tc_cycle_ungated),
        ] {
            write_matrix(&mut out, name, matrix);
        }

        // Refactor pairs: a checker property, asserted.
        let mut refactor_differences = 0;
        refactor_differences += write_pair(&mut out, ("heartbeat", &heartbeat), ("heartbeat_refactor", &heartbeat_refactor));
        refactor_differences += write_pair(&mut out, ("retry", &retry), ("retry_refactor", &retry_refactor));
        refactor_differences += write_pair(&mut out, ("gossip", &gossip), ("gossip_refactor", &gossip_refactor));
        refactor_differences += write_pair(&mut out, ("tc_chain", &tc_chain), ("tc_chain_refactor", &tc_chain_refactor));

        // Mechanism / generator pairs: recorded raw, not asserted.
        write_pair(&mut out, ("retry", &retry), ("retry_dedup", &retry_dedup));
        write_pair(&mut out, ("gossip", &gossip), ("gossip_no_pump", &gossip_no_pump));
        write_pair(&mut out, ("tc_chain", &tc_chain), ("tc_chain_ungated", &tc_chain_ungated));
        write_pair(&mut out, ("tc_cycle", &tc_cycle), ("tc_cycle_ungated", &tc_cycle_ungated));
        write_pair(&mut out, ("tc_chain", &tc_chain), ("tc_cycle", &tc_cycle));

        assert_eq!(
            refactor_differences, 0,
            "identity-map refactors changed the evidence matrix; see the report pair sections"
        );
    }

    fn heartbeat_flow(refactor: bool) -> EvidenceMatrix {
        use crate::cluster::pure_heartbeat::pure_heartbeat;

        const MEMBERS: usize = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let timer = if refactor { timer.map(q!(|x| x)) } else { timer };
        pure_heartbeat(&cluster, timer)
            .entries()
            .for_each(q!(|_| {}, commutative = manual_proof!(/** observation only */)));

        let mut sim = flow.sim().with_cluster_size(&cluster, MEMBERS);
        let manifest = sim.feedback_boundary_manifest();
        let mut inputs = InputRegistry::new();
        inputs.cluster_operational(&timer_send, 0..MEMBERS as u32, ());
        let compiled = sim.with_provenance().compiled();
        run_evidence_matrix(&compiled, manifest, &inputs, config())
    }

    fn retry_flow(dedup: bool, refactor: bool) -> EvidenceMatrix {
        use crate::distributed::timeout_retry::{LossPolicy, Request, timeout_retry_lossy_with_timers};

        let mut flow = FlowBuilder::new();
        let client = flow.process();
        let service = flow.process();
        let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let requests = if refactor { requests.map(q!(|x| x)) } else { requests };
        let (retry_send, retries) = client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (service_send, service_ticks) =
            service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let outputs = timeout_retry_lossy_with_timers(
            &client,
            &service,
            requests,
            retries,
            service_ticks,
            0,
            LossPolicy {
                dedup_requests_at_service: dedup,
                ..LossPolicy::default()
            },
        );
        outputs.completed.for_each(q!(|_| {}, commutative = manual_proof!(/** observation only */)));
        outputs.events.for_each(q!(|_| {}, commutative = manual_proof!(/** observation only */)));

        let mut sim = flow.sim();
        let manifest = sim.feedback_boundary_manifest();
        let mut inputs = InputRegistry::new();
        inputs.process_data(&request_send, |index| Request {
            id: index as u64,
            value: format!("value-{index}"),
        });
        inputs.process_operational(&retry_send, ());
        inputs.process_operational(&service_send, ());
        let compiled = sim.with_provenance().compiled();
        run_evidence_matrix(&compiled, manifest, &inputs, config())
    }

    fn gossip_flow(pump: bool, refactor: bool) -> EvidenceMatrix {
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

        const MEMBERS: usize = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let updates = if refactor { updates.map(q!(|x| x)) } else { updates };
        let (timer_send, timers) = cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        // "No pump" variant: the operational port still exists but never reaches the publisher.
        let timers = if pump { timers } else { timers.filter(q!(|_| false)) };
        g_set_gossip(&cluster, updates, timers)
            .sample_eager(nondet!(/** observation only */))
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .for_each(q!(|_| {}, idempotent = manual_proof!(/** observation only */)));

        let mut sim = flow
            .sim()
            .with_cluster_size(&cluster, MEMBERS)
            .skip_consistency_assertions();
        let manifest = sim.feedback_boundary_manifest();
        let mut inputs = InputRegistry::new();
        inputs.cluster_data(&update_send, 0..MEMBERS as u32, |index| index as u32);
        inputs.cluster_operational(&timer_send, 0..MEMBERS as u32, ());
        let compiled = sim.with_provenance().compiled();
        run_evidence_matrix(&compiled, manifest, &inputs, config())
    }

    fn tc_flow(gated: bool, cyclic_generator: bool, refactor: bool) -> EvidenceMatrix {
        use crate::local::productive_tc::{productive_transitive_closure, ungated_transitive_closure};

        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (edge_send, edges) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let edges = if refactor { edges.map(q!(|x| x)) } else { edges };
        let (step_send, steps) = process.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = if gated {
            productive_transitive_closure(edges, steps)
        } else {
            ungated_transitive_closure(edges, steps)
        };
        let _facts_out = facts
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_output();
        traces.for_each(q!(|_| {}));

        let mut sim = flow.sim();
        let manifest = sim.feedback_boundary_manifest();
        let mut inputs = InputRegistry::new();
        if cyclic_generator {
            // Five-node directed cycle; indices past 5 repeat edges (exercises `unique`).
            inputs.process_data(&edge_send, |index| {
                let i = index as u32;
                vec![(i % 5, (i + 1) % 5)]
            });
        } else {
            // Descending chain: exercises recursion, no cycles.
            inputs.process_data(&edge_send, |index| {
                let high = 10_000u32 - index as u32;
                vec![(high - 1, high)]
            });
        }
        inputs.process_operational(&step_send, ());
        let compiled = sim.with_provenance().compiled();
        run_evidence_matrix(&compiled, manifest, &inputs, config())
    }
}
