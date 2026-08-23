//! Compile-time pins for the ordering × consistency taxonomy.
//!
//! These tests assert *type-level* facts documented in
//! `design_docs/2026-08_ordering_consistency_taxonomy.md`. They contain no
//! runtime assertions: the claim under test is what the type checker accepts,
//! so compilation (plus `flow.finalize()`) is the test.
//!
//! The leader-merge construction (taxonomy doc §4) is now a real demo function,
//! [`leader_merge_broadcast`](crate::ec_inference_demos::leader_merge::leader_merge_broadcast),
//! and its type-pin lives alongside it in that module.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
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

