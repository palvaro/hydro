//! Example 2: Single Quorum (Depth 1)
//!
//! A proposer broadcasts requests to a cluster of acceptors.
//! Acceptors ack, and the proposer uses collect_quorum to wait for f+1
//! responses. Confirmed keys are folded into a growing set.
//!
//! The nondet! commitment: which acks land in which batch determines
//! WHEN a key reaches quorum. All batch commitments commute — reordering
//! batches doesn't change WHICH keys eventually reach quorum.
//!
//! Determination depth: 1 (one layer of commuting batch commitments)

use std::collections::HashSet;
use std::hash::Hash;

use hydro_lang::prelude::*;
use hydro_std::quorum::collect_quorum;

/// Proposer sends requests, collects quorum of acks, accumulates confirmed keys.
pub fn quorum_then_accumulate<'a, K: Clone + Eq + Hash + 'static>(
    proposer: &Process<'a, ()>,
    acks: Stream<(K, Result<(), ()>), Process<'a, ()>, Unbounded>,
    quorum_size: usize,
) -> Singleton<HashSet<K>, Process<'a, ()>, Unbounded> {
    // collect_quorum internally uses nondet! for batching of ack arrivals.
    // This is the commitment point: which acks arrive in which batch.
    let (confirmed, _errors) = collect_quorum(acks, quorum_size, quorum_size);

    // Downstream: fold confirmed keys into a growing set.
    // This is purely monotone — set only grows.
    confirmed.fold(
        q!(|| HashSet::new()),
        q!(|set, key| { set.insert(key); }),
    )
}
