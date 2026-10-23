//! Minimal pure-heartbeat negative control for feedback-hazard research.
//!
//! Each timer event emits one fixed-size message to every member. There are no
//! acknowledgements, retries, outstanding-work state, or response path.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// A fixed-size liveness signal. Sequence numbers aid observation only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Heartbeat {
    pub sequence: u64,
}

/// Emits exactly one heartbeat per timer event at each member, then broadcasts
/// it to the closed cluster membership.
pub fn pure_heartbeat<'a, Node: 'a>(
    cluster: &Cluster<'a, Node>,
    timer: Stream<(), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
) -> KeyedStream<
    MemberId<Node>,
    Heartbeat,
    Cluster<'a, Node, EventualConsistency>,
    Unbounded,
    TotalOrder,
    ExactlyOnce,
> {
    timer
        .enumerate()
        .map(q!(|(sequence, ())| Heartbeat {
            sequence: sequence as u64,
        }))
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
}

#[cfg(test)]
mod tests {
    use dfir_lang::graph::GraphNode;
    use hydro_deploy::Deployment;
    use hydro_lang::telemetry::emf::RecordMetricsSidecar;

    use super::*;
    use crate::stage_telemetry::{StageWindow, parse_stage_windows};

    /// A window that would previously have been flagged as "retained-state
    /// network work without ordinary input": a stateful stage that emits
    /// network work in a window where it drained no ordinary handoff items.
    /// This is a *description of a measured shape*, not a hazard verdict. Pure
    /// heartbeat, having no retained state on its work path, should never
    /// exhibit it.
    fn retained_state_stage_emits_network_without_ordinary_input(window: &StageWindow) -> bool {
        window.run_count > 0
            && (window.retained_state_reads > 0 || window.retained_state_writes > 0)
            && window.input_items == 0
            && window.network_message_count > 0
            && window.network_byte_count > 0
    }

    #[test]
    fn interval_drives_network_without_feedback_handoff() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let timer = cluster.source_interval(q!(std::time::Duration::from_millis(10)));
        pure_heartbeat(&cluster, timer)
            .entries()
            .assume_ordering::<TotalOrder>(nondet!(/** only drives IR inspection */))
            .for_each(q!(|_| {}));

        let mut built =
            flow.with_default_optimize::<hydro_lang::compile::embedded::EmbeddedDeploy>();
        let preview = built.preview_compile();
        let graph = preview.dfir_for(&cluster).unwrap();
        assert!(graph.node_ids().any(|node_id| {
            graph
                .operator_tag(node_id)
                .is_some_and(|tag| tag.starts_with("interval__"))
        }));
        assert!(
            !graph
                .node_ids()
                .any(|node_id| graph.handoff_delay_type(node_id).is_some())
        );
        assert_eq!(
            graph
                .nodes()
                .filter(|(_, node)| matches!(node, GraphNode::Operator(op) if op.name_string() == "dest_sink"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stage_trace_observes_fixed_interval_work_without_feedback() {
        const MEMBERS: usize = 3;
        let trace_path = std::path::PathBuf::from(format!(
            "target/heartbeat-stage-trace-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&trace_path);
        let mut deployment = Deployment::new();
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let timer = cluster.source_interval(q!(std::time::Duration::from_millis(40)));
        pure_heartbeat(&cluster, timer).entries().for_each(q!(
            |_| {},
            commutative = manual_proof!(/** terminal observation only */)
        ));
        let sidecar = RecordMetricsSidecar::builder()
            .file_path(trace_path.to_string_lossy().into_owned())
            .interval(std::time::Duration::from_millis(100))
            .build();
        let _nodes = flow
            .with_default_optimize()
            .with_cluster(&cluster, (0..MEMBERS).map(|_| deployment.Localhost()))
            .with_sidecar_all(&sidecar)
            .deploy(&mut deployment);
        deployment.deploy().await.unwrap();
        deployment.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        deployment.stop().await.unwrap();

        let contents = std::fs::read_to_string(&trace_path).unwrap();
        let windows = parse_stage_windows(&contents);
        let records: Vec<serde_json::Value> = contents
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|record: &serde_json::Value| record["MetricKind"] == "Stage")
            .collect();
        for record in &records {
            if record["NetworkMessageCount"].as_u64().unwrap_or(0) > 0
                || record["HasIntervalSource"] == true
            {
                eprintln!(
                    "NETWORK_STAGE interval={} messages={} bytes={} runs={} names={}",
                    record["HasIntervalSource"],
                    record["NetworkMessageCount"],
                    record["NetworkByteCount"],
                    record["RunCount"],
                    record["OperatorNames"],
                );
            }
        }
        assert!(records.iter().any(|record| {
            record["HasIntervalSource"] == true
                && record["RunCount"].as_u64().unwrap_or(0) > 0
                && record["NetworkMessageCount"].as_u64().unwrap_or(0) > 0
                && record["NetworkByteCount"].as_u64().unwrap_or(0) > 0
                && record["OperatorNames"]
                    .as_array()
                    .is_some_and(|names| names.iter().any(|name| name == "dest_sink"))
        }));
        // Pure heartbeat has no retained state on its work path, so no window
        // should show a stateful stage emitting network work with zero ordinary
        // input. This asserts the measured shape directly rather than delegating
        // to a reusable (and unsound) classifier.
        let retained_state_windows: Vec<_> = windows
            .iter()
            .filter(|window| retained_state_stage_emits_network_without_ordinary_input(window))
            .collect();
        eprintln!("HEARTBEAT_RETAINED_STATE_WINDOWS {retained_state_windows:?}");
        assert!(
            retained_state_windows.is_empty(),
            "pure heartbeat must not show a retained-state stage emitting network work without ordinary input"
        );
    }

    #[test]
    fn fixed_gain_without_feedback() {
        const MEMBERS: usize = 3;
        const PULSES: usize = 4;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer)
            .entries()
            .sim_cluster_output();

        flow.sim()
            .with_cluster_size(&cluster, MEMBERS)
            .exhaustive(async || {
                for member in 0..MEMBERS as u32 {
                    for _ in 0..PULSES {
                        timer_send.send(member, ());
                    }
                }
                for member in 0..MEMBERS as u32 {
                    let messages = received.collect_sorted::<Vec<_>>(member).await;
                    assert_eq!(messages.len(), PULSES * MEMBERS);
                }
            });
    }
}
