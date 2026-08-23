//! Compile-time pins for the ordering × consistency taxonomy.
//!
//! These tests assert *type-level* facts documented in
//! `design_docs/2026-08_ordering_consistency_taxonomy.md`. They contain no
//! runtime assertions: the claim under test is what the type checker accepts,
//! so compilation (plus `flow.finalize()`) is the test.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;

/// Single-writer TO,EC is free (taxonomy doc §2): a Process→Cluster
/// `broadcast_closed` of a `TotalOrder` stream over `fail_stop` preserves the
/// total order (`MinOrder<TotalOrder, TotalOrder>`) *and* earns EC. The
/// "agreed log" corner of the taxonomy requires no consensus when there is
/// one writer.
#[test]
fn single_writer_broadcast_is_total_order_ec() {
    let mut flow = FlowBuilder::new();
    let writer = flow.process::<()>();
    let replicas = flow.cluster::<()>();

    let (_send, data) = writer.sim_input::<u32, TotalOrder, ExactlyOnce>();

    let _: Stream<u32, Cluster<'_, (), EventualConsistency>, _, TotalOrder, _> =
        data.broadcast_closed(&replicas, TCP.fail_stop().bincode());

    let _ = flow.finalize();
}

/// Multi-writer TO,EC via a single merging leader — primary/backup — with
/// **zero consensus** (taxonomy doc §4).
///
/// Why this type-checks, and why that is correct: `entries_partially_ordered`
/// returns `L::DropConsistency`, but on a `Process`, `DropConsistency = Self`
/// — consistency is a cross-replica property and a single node has no
/// replicas to disagree with. The interleaving is still `nondet!`
/// (run-contingent choice), but within one execution the choice, once made,
/// is data, and FIFO broadcast of data from one node is TO,EC by the
/// single-writer rule.
///
/// **Caveat the type system does not yet record** (taxonomy doc §5–§7): this
/// EC label silently assumes the leader's survival — F = {leader} in the
/// three-place reading "C^◇ among N given F". A leader crash mid-broadcast
/// leaves live replicas holding different prefixes forever. The label is
/// indistinguishable from Raft's, whose F is ∅; tracking F in the type is
/// the open design item this test exists to pin the motivation for.
#[test]
fn multi_writer_leader_merge_is_total_order_ec_with_untracked_spof() {
    let mut flow = FlowBuilder::new();
    let writers = flow.cluster::<()>();
    let leader = flow.process::<()>();
    let replicas = flow.cluster::<()>();

    // Each writer: its own TO,NC stream (the most common type in the system).
    let (_w, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

    // Writers → leader: keyed by writer, per-key TotalOrder, no cross-key order.
    let at_leader = local.send(&leader, TCP.fail_stop().bincode());

    // The leader manufactures THE interleaving. nondet! (run-contingent
    // choice); no consistency downgrade on a Process (DropConsistency = Self).
    let merged =
        at_leader.entries_partially_ordered(nondet!(/** leader dictates the interleaving */));

    // Leader → replicas: FIFO broadcast of the chosen sequence. TO,EC.
    let log: Stream<
        (MemberId<()>, u32),
        Cluster<'_, (), EventualConsistency>,
        _,
        TotalOrder,
        _,
    > = merged.broadcast_closed(&replicas, TCP.fail_stop().bincode());

    let _ = log;
    let _ = flow.finalize();
}
