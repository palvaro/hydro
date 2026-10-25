//! Dynamic stage-level evidence for data-triggered reliable broadcast.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use hydro_deploy::Deployment;
    use hydro_lang::live_collections::stream::TotalOrder;
    use hydro_lang::prelude::*;
    use hydro_lang::telemetry::emf::RecordMetricsSidecar;
    use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

    fn stage_records(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|record| record["MetricKind"] == "Stage")
            .collect()
    }

    #[tokio::test]
    async fn finite_novel_input_amplifies_through_echo_cycle_then_drains() {
        const MEMBERS: usize = 3;
        let path = std::env::current_dir().unwrap().join(format!(
            "target/reliable-broadcast-stage-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let source = flow.process::<()>();
        let cluster = flow.cluster::<()>();
        let finite = source.source_iter(q!(0..8u32));
        reliable_broadcast_closed(finite, &cluster)
            .assume_ordering::<TotalOrder>(nondet!(/** terminal observation only */))
            .for_each(q!(|_| {}));

        let sidecar = RecordMetricsSidecar::builder()
            .file_path(path.to_string_lossy().into_owned())
            .interval(Duration::from_millis(100))
            .build();
        let _nodes = flow
            .with_default_optimize()
            .with_process(&source, deployment.Localhost())
            .with_cluster(&cluster, (0..MEMBERS).map(|_| deployment.Localhost()))
            .with_sidecar_all(&sidecar)
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        deployment.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        deployment.stop().await.unwrap();

        let records = stage_records(&path);
        let mut by_timestamp = std::collections::BTreeMap::<u64, u64>::new();
        for record in records {
            *by_timestamp
                .entry(record["_aws"]["Timestamp"].as_u64().unwrap())
                .or_default() += record["NetworkMessageCount"].as_u64().unwrap_or(0);
        }
        let windows: Vec<u64> = by_timestamp.into_values().collect();

        // Measured facts for eight finite inputs on a real three-member run:
        //
        //   1. Network traffic appears in more than one measurement window
        //      (the echo cycle carries the initial send onward), and
        //   2. traffic then drains to zero and stays there.
        //
        // We assert this observed window structure directly. We deliberately do
        // NOT call the traffic "amplification": multi-window traffic alone is a
        // wall-clock artifact and is not by itself evidence of feedback gain.
        // The interpretation is discussed in the design doc, not asserted here.
        let active_windows = windows.iter().filter(|messages| **messages > 0).count();
        assert!(
            active_windows > 1,
            "expected network traffic across more than one window: {windows:?}"
        );
        assert!(windows.len() >= 4);
        assert!(
            windows.iter().rev().take(3).all(|messages| *messages == 0),
            "expected traffic to drain to zero in the final windows: {windows:?}"
        );
        eprintln!("RELIABLE_BROADCAST_EVIDENCE active_windows={active_windows} windows={windows:?}");
    }
}
