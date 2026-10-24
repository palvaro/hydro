//! QUARANTINED: unsound "smell" predicates. Not compiled.
//!
//! This module is retained only as a record of an approach that did **not**
//! work. It is gated permanently off with `#![cfg(any())]` so it never
//! builds or runs, per the recovery plan in `AI_WORK_STATUS.md`
//! (recommendation 4: quarantine until a precise observable property is
//! defined).
//!
//! # Why these predicates were quarantined
//!
//! The `SmellFinding` predicates below were presented as reusable classifiers,
//! but they are not a sound linter. Each one confuses a measurement with a
//! diagnosis:
//!
//! - `retained_state_network_work_without_ordinary_input` flags *any* stateful
//!   stage that emits network work in a window with no ordinary handoff input.
//!   Ordinary queue drainage after an upstream burst produces exactly this
//!   shape, so the predicate cannot separate reactivation of retained work from
//!   a stage simply finishing buffered input.
//! - `state_dependent_network_volume` flags a byte-volume increase between a
//!   "small" and "large" configuration. Larger configured/retained state is
//!   expected to move more bytes even for perfectly productive programs;
//!   comparing two hand-picked sizes does not isolate a feedback hazard.
//! - `multi_window_network_amplification` treats network traffic spanning more
//!   than one measurement window as amplification. Window boundaries are wall
//!   clock artifacts; ordinary finite work routinely straddles two windows
//!   without any feedback gain.
//!
//! Experiment selection was also hand-authored, and no general repository-wide
//! analysis was ever demonstrated. The raw measurements these predicates
//! consumed are still useful; that reproducible parsing code now lives in
//! `crate::stage_telemetry`. Tests assert directly on measured `StageWindow`
//! fields instead of calling these predicates.
//!
//! Do not re-enable this module without first defining a precise, testable
//! observable property that distinguishes the phenomenon of interest from the
//! benign shapes listed above.

// Gated permanently off: `cfg(any())` is always false, so this module never
// compiles or runs. It is kept in-tree only as a record of a quarantined,
// unsound approach. Do not re-enable without a precise observable property.
#![cfg(any())]

use serde::Serialize;

use crate::stage_telemetry::StageWindow;

/// Machine-readable smell finding produced by the (unsound) generic predicates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SmellFinding {
    /// A retained-state stage emitted physical network work while consuming no
    /// ordinary handoff items in the same measurement window.
    RetainedStateNetworkWorkWithoutOrdinaryInput {
        location_name: String,
        subgraph_id: String,
        operator_names: Vec<String>,
        network_messages: u64,
        network_bytes: u64,
        output_items: u64,
    },
    /// Increasing retained/configured state increased serialized work volume.
    StateDependentNetworkVolume {
        small_messages: u64,
        large_messages: u64,
        small_bytes: u64,
        large_bytes: u64,
    },
    /// A finite input caused network traffic in more than one window.
    MultiWindowNetworkAmplification {
        active_windows: usize,
        total_messages: u64,
    },
}

/// UNSOUND. Flags windows where a stateful stage emits network work with no
/// ordinary handoff input. Also matches ordinary queue drainage.
pub fn retained_state_network_work_without_ordinary_input(
    windows: &[StageWindow],
) -> Vec<SmellFinding> {
    windows
        .iter()
        .filter(|window| {
            window.run_count > 0
                && (window.retained_state_reads > 0 || window.retained_state_writes > 0)
                && window.input_items == 0
                && window.network_message_count > 0
                && window.network_byte_count > 0
        })
        .map(
            |window| SmellFinding::RetainedStateNetworkWorkWithoutOrdinaryInput {
                location_name: window.location_name.clone(),
                subgraph_id: window.subgraph_id.clone(),
                operator_names: window.operator_names.clone(),
                network_messages: window.network_message_count,
                network_bytes: window.network_byte_count,
                output_items: window.output_items,
            },
        )
        .collect()
}

/// UNSOUND. Flags a byte-volume increase between two configurations; larger
/// retained state moves more bytes even for productive programs.
pub fn state_dependent_network_volume(
    small: &[StageWindow],
    large: &[StageWindow],
    minimum_byte_ratio: u64,
) -> Option<SmellFinding> {
    let (small_messages, small_bytes) = crate::stage_telemetry::network_totals(small);
    let (large_messages, large_bytes) = crate::stage_telemetry::network_totals(large);
    (small_bytes > 0 && large_bytes >= small_bytes.saturating_mul(minimum_byte_ratio)).then_some(
        SmellFinding::StateDependentNetworkVolume {
            small_messages,
            large_messages,
            small_bytes,
            large_bytes,
        },
    )
}

/// UNSOUND. Treats traffic spanning multiple wall-clock windows as
/// amplification; ordinary finite work straddles windows without gain.
pub fn multi_window_network_amplification(messages_by_window: &[u64]) -> Option<SmellFinding> {
    let active_windows = messages_by_window
        .iter()
        .filter(|messages| **messages > 0)
        .count();
    (active_windows > 1).then_some(SmellFinding::MultiWindowNetworkAmplification {
        active_windows,
        total_messages: messages_by_window.iter().sum(),
    })
}
