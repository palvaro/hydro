//! Example 2: Single Quorum — Leader Election (Depth 1)
//!
//! A proposer broadcasts requests to a cluster of acceptors. Acceptors ack,
//! and the proposer uses collect_quorum to detect which keys have reached
//! quorum. The FIRST key to reach quorum is designated as "leader."
//!
//! The nondet! commitment: which acks land in which batch determines
//! which key reaches quorum FIRST. This choice propagates directly to
//! the output — the leader identity depends on the schedule.
//!
//! Unlike Example 1 (monotone accumulation) or a simple "fold into set"
//! downstream, this program's output is NOT insensitive to batch ordering.
//! The batch boundary's nondeterminism is NOT absorbed — it reaches the
//! output interface.
//!
//! Determination depth: 1 (one layer of batch commitments — they commute
//! with each other since each key's quorum is independent, but the "first"
//! selection makes the output contingent on which commitment fires first)

use std::hash::Hash;

use hydro_lang::prelude::*;
use hydro_std::quorum::collect_quorum;

/// Proposer collects quorum of acks; the first key to reach quorum becomes leader.
/// The output (leader identity) depends on batch ordering — genuine depth 1.
pub fn quorum_leader_election<'a, K: Clone + Eq + Hash + 'static>(
    node: &Process<'a, ()>,
    acks: Stream<(K, Result<(), ()>), Process<'a, ()>, Unbounded>,
    quorum_size: usize,
) -> Singleton<Option<K>, Process<'a, ()>, Unbounded> {
    // collect_quorum internally uses nondet! for batching of ack arrivals.
    // This is the commitment point: which acks arrive in which batch
    // determines which key crosses the threshold first.
    let (confirmed, _errors) = collect_quorum(acks, quorum_size, quorum_size);

    // Downstream: keep only the FIRST confirmed key.
    // This fold is NOT commutative — the first element wins.
    // The batch nondeterminism propagates: different schedules → different leaders.
    confirmed.fold(
        q!(|| None),
        q!(|leader, key| {
            if leader.is_none() {
                *leader = Some(key);
            }
        }),
    )
}
