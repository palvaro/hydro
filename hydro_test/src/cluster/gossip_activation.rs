//! Dynamic stage-level evidence for state-based CRDT gossip.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use hydro_deploy::Deployment;
    use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::telemetry::emf::RecordMetricsSidecar;
    use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

    use crate::stage_telemetry::{StageWindow, parse_stage_windows};

    async fn run_gossip(state_size: u32, label: &str) -> Vec<StageWindow> {
        const MEMBERS: usize = 3;
        let path = std::env::current_dir().unwrap().join(format!(
            "target/gossip-stage-{label}-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let updates: Stream<u32, _, _, NoOrder> = cluster
            .source_iter(q!(0..state_size))
            .weaken_ordering();
        let pumps = cluster.source_interval(q!(Duration::from_millis(100)));
        let state = g_set_gossip(&cluster, updates.into(), pumps);
        state
            .sample_every(
                q!(Duration::from_millis(100)),
                nondet!(/** terminal observation only */),
            )
            .assume_ordering::<TotalOrder>(nondet!(/** terminal observation only */))
            .for_each(q!(|_| {}, idempotent = manual_proof!(/** terminal observation only */)));

        let sidecar = RecordMetricsSidecar::builder()
            .file_path(path.to_string_lossy().into_owned())
            .interval(Duration::from_millis(100))
            .build();
        let _nodes = flow
            .with_default_optimize()
            .with_cluster(&cluster, (0..MEMBERS).map(|_| deployment.Localhost()))
            .with_sidecar_all(&sidecar)
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        deployment.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        deployment.stop().await.unwrap();

        read_stage_records(&path)
    }

    fn read_stage_records(path: &Path) -> Vec<StageWindow> {
        parse_stage_windows(&std::fs::read_to_string(path).unwrap())
    }

    fn pump_network_totals(records: &[StageWindow]) -> (u64, u64, u64) {
        records
            .iter()
            .filter(|record| record.has_interval_source)
            .fold((0, 0, 0), |(messages, bytes, micros), record| {
                (
                    messages + record.network_message_count,
                    bytes + record.network_byte_count,
                    micros + record.poll_duration_micros,
                )
            })
    }

    #[tokio::test]
    async fn larger_retained_state_increases_gossip_volume_not_message_gain() {
        let small = run_gossip(4, "small").await;
        let large = run_gossip(400, "large").await;
        let (small_messages, small_pump_bytes, small_poll_micros) = pump_network_totals(&small);
        let (large_messages, large_pump_bytes, large_poll_micros) = pump_network_totals(&large);

        // Measured facts under identical 3-member membership, pump rate, and
        // duration, varying only retained element count (4 vs 400):
        //
        //   1. Pump message cardinality stays in the same order of magnitude
        //      (fixed message gain); wall-clock windows are noisy, so we only
        //      require the counts to remain within 2x of each other.
        //   2. Serialized pump byte volume grows substantially with retained
        //      state. In the reference run bytes grew ~67x; we assert a much
        //      more conservative >=10x to stay reproducible.
        //
        // These are measurements. Whether gossip becomes unstable is a separate
        // capacity/workload question and is NOT claimed here.
        assert!(large_messages < small_messages * 2 + 1);
        assert!(small_messages < large_messages * 2 + 1);
        assert!(
            small_pump_bytes > 0 && large_pump_bytes >= small_pump_bytes * 10,
            "expected retained-state byte growth: small={small_pump_bytes} large={large_pump_bytes}"
        );
        eprintln!(
            "GOSSIP_EVIDENCE small_msgs={small_messages} large_msgs={large_messages} \
             small_bytes={small_pump_bytes} large_bytes={large_pump_bytes} \
             small_poll={small_poll_micros}us large_poll={large_poll_micros}us"
        );
    }
}
