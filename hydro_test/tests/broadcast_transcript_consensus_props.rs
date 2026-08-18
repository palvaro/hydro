//! Property-based tests for the broadcast-transcript consensus decision function.
//!
//! Feature: broadcast-transcript-consensus

use hydro_lang::location::MemberId;
use hydro_test::cluster::broadcast_transcript_consensus::{
    quorum_size, DecisionState, TranscriptMsg,
};
use proptest::prelude::*;

/// A dummy cluster tag for testing purposes.
struct TestCluster;

/// Helper: create a MemberId<TestCluster> from a numeric index.
fn make_test_member_id(id: usize) -> MemberId<TestCluster> {
    MemberId::from_raw_id(id as u32)
}

// Feature: broadcast-transcript-consensus, Property 3: Quorum Threshold Commitment
//
// For any slot S, ballot B, value V, and set of member IDs A where |A| >= quorum_size,
// if the decision function processes AcceptAck messages from each member in A for (S, B),
// and a prior Accept(B, S, V) was processed, then slot S SHALL be marked committed with value V.
// If |A| < quorum_size, slot S SHALL NOT be committed for that ballot.
//
// **Validates: Requirements 2.3**

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn quorum_threshold_commitment(
        cluster_size in 3..=7usize,
        slot in 0..5usize,
        ballot in 0..10usize,
        value in "[a-z]{1,3}",
        num_acks in 1..=7usize,
    ) {
        let quorum = quorum_size(cluster_size);
        // Ensure num_acks doesn't exceed cluster_size
        let num_acks = num_acks.min(cluster_size);

        let mut state: DecisionState<String, TestCluster> = DecisionState::new();

        // First, process an Accept so the value is recorded
        state.process(TranscriptMsg::Accept {
            ballot,
            slot,
            value: value.clone(),
        }, quorum);

        // Then process AcceptAck from num_acks distinct members
        for member_id in 0..num_acks {
            state.process(TranscriptMsg::AcceptAck {
                ballot,
                slot,
                from: make_test_member_id(member_id),
            }, quorum);
        }

        // Assert: committed iff num_acks >= quorum
        if num_acks >= quorum {
            prop_assert!(
                state.committed_slots.contains(&slot),
                "Expected slot {} to be committed with {} acks (quorum = {})",
                slot, num_acks, quorum
            );
            // For slot 0, it should be in committed_log too (no gap-filling needed)
            if slot == 0 {
                prop_assert_eq!(state.committed_log.len(), 1);
                prop_assert_eq!(&state.committed_log[0].message, &value);
                prop_assert_eq!(state.committed_log[0].ballot, ballot);
                prop_assert_eq!(state.committed_log[0].slot, slot);
            }
        } else {
            prop_assert!(
                !state.committed_slots.contains(&slot),
                "Expected slot {} NOT to be committed with {} acks (quorum = {})",
                slot, num_acks, quorum
            );
            // No committed entries for this slot
            prop_assert!(state.committed_log.iter().all(|e| e.slot != slot));
        }
    }
}
