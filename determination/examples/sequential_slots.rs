//! Example 3: Sequential Slots with Application Dependency (Depth 2)
//!
//! Two quorum rounds where the first decides a threshold value, and the
//! second uses that threshold (via collect_dynamic_quorum) to determine
//! when ITS quorum is reached.
//!
//! Slot 1: "How many acks do we need?" — collects proposals, picks majority.
//! Slot 2: "Has the operation succeeded?" — uses slot 1's answer as threshold.
//!
//! The commitments do NOT commute: slot 2's batch commitment has a different
//! EFFECT depending on slot 1's outcome (a threshold of 2 vs 3 means
//! different ack sets reach quorum).
//!
//! Determination depth: 2 (two sequential layers)

use std::collections::HashSet;
use std::hash::Hash;

use hydro_lang::prelude::*;
use hydro_std::quorum::{collect_quorum, collect_dynamic_quorum};

/// Slot 1 decides a quorum threshold; slot 2 uses it.
pub fn sequential_quorum_slots<'a, K: Clone + Eq + Hash + 'static>(
    node: &Process<'a, ()>,
    // Slot 1: proposals for what the threshold should be
    threshold_votes: Stream<((), Result<usize, ()>), Process<'a, ()>, Unbounded>,
    // Slot 2: acks for actual operations, keyed by operation id
    operation_acks: Stream<(K, Result<(), ()>), Process<'a, ()>, Unbounded>,
    // Fixed quorum size for slot 1 itself
    slot1_quorum: usize,
) -> Stream<K, Process<'a, ()>, Unbounded> {
    // --- Slot 1: decide the threshold ---
    // Commitment layer 1: batch boundary for threshold votes.
    let (confirmed_thresholds, _) =
        collect_quorum_with_response(threshold_votes, slot1_quorum, slot1_quorum);

    // Extract the decided threshold (take the max of confirmed values).
    let decided_threshold: Singleton<usize, _, _> = confirmed_thresholds
        .map(q!(|(_key, threshold)| threshold))
        .fold(q!(|| 0usize), q!(|max, val| { if val > *max { *max = val; } }));

    // --- Slot 2: use decided threshold ---
    // Commitment layer 2: batch boundary for operation acks.
    // This commitment's EFFECT depends on slot 1's outcome —
    // a threshold of 2 vs 3 means different sets of acks reach quorum.
    let confirmed_ops = collect_dynamic_quorum(operation_acks, decided_threshold);

    confirmed_ops
}
