//! Raw stage-telemetry parsing over DFIR EMF windows.
//!
//! This module contains only the mechanical, reproducible parts of the earlier
//! `feedback_smells` module: the record shape emitted by Hydro's EMF stage
//! sidecar and a parser for its JSON-lines output. It reports *measurements*.
//! It does not classify programs, diagnose metastability, or prove safety.
//!
//! The interpretive `SmellFinding` predicates that previously lived alongside
//! this code were quarantined (see `feedback_smells.rs`) because they were not
//! a sound linter: they conflated ordinary queue drainage with retained-state
//! reactivation, treated any multi-window traffic as amplification, and treated
//! a small-vs-large volume delta as a program smell. Tests should assert on the
//! concrete fields below and describe what those numbers mean, rather than
//! delegating a verdict to a reusable predicate.

use serde::Deserialize;

/// One stage measurement as emitted by Hydro's EMF stage sidecar.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StageWindow {
    pub location_name: String,
    pub subgraph_id: String,
    pub operator_names: Vec<String>,
    pub has_interval_source: bool,
    pub input_items: u64,
    pub feedback_input_items: u64,
    pub output_items: u64,
    pub network_message_count: u64,
    pub network_byte_count: u64,
    pub retained_state_reads: u64,
    pub retained_state_writes: u64,
    pub run_count: u64,
    pub poll_duration_micros: u64,
}

/// Sums `(network_message_count, network_byte_count)` across the given windows.
///
/// This is a plain aggregation of measured quantities. It draws no conclusion
/// about whether the totals are healthy, hazardous, or amplified.
pub fn network_totals(windows: &[StageWindow]) -> (u64, u64) {
    windows.iter().fold((0, 0), |(messages, bytes), window| {
        (
            messages + window.network_message_count,
            bytes + window.network_byte_count,
        )
    })
}

/// Parses stage records from the JSON-lines output of the telemetry sidecar.
pub fn parse_stage_windows(contents: &str) -> Vec<StageWindow> {
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|record| record["MetricKind"] == "Stage")
        .filter_map(|record| serde_json::from_value(record).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> StageWindow {
        StageWindow {
            location_name: "test".to_owned(),
            subgraph_id: "sg0".to_owned(),
            operator_names: vec!["map".to_owned()],
            has_interval_source: false,
            input_items: 0,
            feedback_input_items: 0,
            output_items: 1,
            network_message_count: 1,
            network_byte_count: 10,
            retained_state_reads: 1,
            retained_state_writes: 1,
            run_count: 1,
            poll_duration_micros: 1,
        }
    }

    #[test]
    fn network_totals_sums_measurements() {
        let mut second = window();
        second.network_message_count = 4;
        second.network_byte_count = 90;
        assert_eq!(network_totals(&[window(), second]), (5, 100));
    }

    #[test]
    fn parse_stage_windows_reads_stage_records_only() {
        let contents = concat!(
            r#"{"MetricKind":"Other","LocationName":"skip"}"#,
            "\n",
            r#"{"MetricKind":"Stage","LocationName":"keep","SubgraphId":"sg0","OperatorNames":["map"],"HasIntervalSource":false,"InputItems":0,"FeedbackInputItems":0,"OutputItems":1,"NetworkMessageCount":2,"NetworkByteCount":20,"RetainedStateReads":0,"RetainedStateWrites":0,"RunCount":1,"PollDurationMicros":3}"#,
            "\n",
        );
        let windows = parse_stage_windows(contents);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].location_name, "keep");
        assert_eq!(windows[0].network_message_count, 2);
    }
}
