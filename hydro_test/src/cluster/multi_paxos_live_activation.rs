//! Dynamic smell-test evidence for Multi-Paxos-live's retained redo queue.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use hydro_deploy::Deployment;
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;
    use hydro_lang::live_collections::stream::TotalOrder;
    use hydro_lang::prelude::*;
    use hydro_lang::telemetry::emf::RecordMetricsSidecar;
    use hydro_std::ec_inference_demos::multi_paxos_live::multi_paxos_live;

    use crate::stage_telemetry::{StageWindow, network_totals, parse_stage_windows};

    fn stage_records(path: &Path) -> Vec<StageWindow> {
        parse_stage_windows(&std::fs::read_to_string(path).unwrap())
    }

    async fn run_with_pending(count: u32, label: &str) -> Vec<StageWindow> {
        const N: usize = 3;
        let path = std::env::current_dir().unwrap().join(format!(
            "target/multi-paxos-live-{label}-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let commands = proposers
            .source_iter(q!(0..count))
            .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0));
        let timeouts = proposers
            .source_interval_delayed(
                q!(Duration::from_millis(100)),
                q!(Duration::from_secs(10)),
            )
            .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0));
        let outputs = multi_paxos_live(&acceptors, &learners, 2, N, timeouts.into(), commands.into());
        outputs
            .learned
            .assume_ordering::<TotalOrder>(nondet!(/** terminal observation only */))
            .for_each(q!(|_| {}));

        let sidecar = RecordMetricsSidecar::builder()
            .file_path(path.to_string_lossy().into_owned())
            .interval(Duration::from_millis(100))
            .build();
        let _nodes = flow
            .with_default_optimize()
            .with_cluster(&acceptors, (0..N).map(|_| deployment.Localhost()))
            .with_cluster(&proposers, (0..N).map(|_| deployment.Localhost()))
            .with_cluster(&learners, (0..N).map(|_| deployment.Localhost()))
            .with_sidecar_all(&sidecar)
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        deployment.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        deployment.stop().await.unwrap();

        stage_records(&path)
    }

    #[tokio::test]
    async fn one_election_releases_work_proportional_to_retained_pending_set() {
        let one = run_with_pending(1, "one").await;
        let many = run_with_pending(20, "many").await;
        let (one_messages, one_bytes) = network_totals(&one);
        let (many_messages, many_bytes) = network_totals(&many);

        // Measured fact under the same election activation: aggregate network
        // volume grows with the size of the retained pending set (1 vs 20
        // commands). The reference run observed 32 msgs / 782 bytes vs 450 msgs
        // / 12,372 bytes; we assert a conservative >=2x byte growth to stay
        // reproducible.
        //
        // This records that the released work scales with retained pending
        // state. It is not, by itself, a proof that the redo queue makes the
        // protocol metastable.
        assert!(
            one_bytes > 0 && many_bytes >= one_bytes * 2,
            "expected redo byte volume to grow with pending set: one={one_bytes} many={many_bytes}"
        );
        eprintln!(
            "MULTI_PAXOS_LIVE_EVIDENCE one_msgs={one_messages} many_msgs={many_messages} \
             one_bytes={one_bytes} many_bytes={many_bytes}"
        );
    }
}
