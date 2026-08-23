//! Multi-writer total-order EC via a single merging leader — **primary/backup**
//! — with *zero consensus*.
//!
//! This is the demo for taxonomy doc §4
//! (`design_docs/2026-08_ordering_consistency_taxonomy.md`): multi-writer TO,EC
//! is expressible today, out of safe primitives, without consensus. The
//! construction is four operators:
//!
//! 1. each writer produces its own local TO,NC stream (`sim_input` on a cluster);
//! 2. writers → leader keyed by writer (`send` from a cluster source yields a
//!    `KeyedStream<MemberId<Writer>, …>`, per-key `TotalOrder`, no cross-key order);
//! 3. the leader *manufactures* the interleaving
//!    ([`entries_partially_ordered`](hydro_lang::live_collections::keyed_stream::KeyedStream::entries_partially_ordered),
//!    guarded by `nondet!` — the run-contingent choice);
//! 4. leader → replicas via `broadcast_closed`: a single writer FIFO-broadcasting
//!    a `TotalOrder` stream over `fail_stop`, which is TO,EC by the single-writer
//!    rule.
//!
//! # Why it type-checks, and why that is *correct*
//!
//! `entries_partially_ordered` returns `L::DropConsistency`, but on a `Process`
//! `DropConsistency = Self` — consistency is a cross-replica property and a
//! single node has no replicas to disagree with. The interleaving is still
//! `nondet!`, but within one execution the choice, once made, is data, and FIFO
//! broadcast of data from one node is TO,EC.
//!
//! # The caveat the type system does not yet record (taxonomy doc §5–§7)
//!
//! This EC label silently assumes the leader's survival — `F = {leader}` in the
//! three-place reading "C^◇ among N given F". A leader crash mid-broadcast leaves
//! live replicas holding different prefixes forever. The stamped EC is
//! indistinguishable from Raft's, whose `F` is ∅; what consensus adds is *fault
//! tolerance of the merge decision*, not the total order itself. Tracking `F` in
//! the type is the open design item this demo exists to motivate: it is the
//! `holders-at-first-visibility = 1` end of the spectrum whose other ends are
//! reliable broadcast (growing) and Paxos (`f+1`).

use hydro_lang::live_collections::keyed_stream::KeyedStream;
use hydro_lang::live_collections::stream::{IsOrdered, MinOrder, Ordering, Retries, TotalOrder};
use hydro_lang::nondet::NonDet;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Merge many per-writer substreams at a single leader and broadcast the chosen
/// interleaving to a replica cluster — primary/backup, multi-writer TO,EC with
/// no consensus.
///
/// `at_leader` is the per-writer keyed stream that has already been `send`-ed to
/// the leader (per-key `TotalOrder` from the transport). The leader flattens it
/// into one totally ordered sequence — the `nondet` witness records that the
/// cross-writer interleaving is a run-contingent choice — and FIFO-broadcasts
/// that sequence to `replicas`, earning `EventualConsistency` from the network
/// policy `via`.
///
/// See the module docs for the `F = {leader}` fault-dependency the resulting EC
/// label does not yet record.
pub fn leader_merge_broadcast<'a, K, V, Leader, Replicas, O, R, N>(
    at_leader: KeyedStream<K, V, Process<'a, Leader>, Unbounded, O, R>,
    replicas: &Cluster<'a, Replicas>,
    via: N,
    nondet_interleaving: NonDet,
) -> Stream<
    (K, V),
    Cluster<'a, Replicas, N::ConsistencyGuarantee>,
    Unbounded,
    <TotalOrder as MinOrder<N::OrderingGuarantee>>::Min,
    R,
>
where
    K: Clone + Serialize + DeserializeOwned + 'a,
    V: Clone + Serialize + DeserializeOwned + 'a,
    Leader: 'a,
    Replicas: 'a,
    O: Ordering + IsOrdered,
    R: Retries,
    N: hydro_lang::networking::NetworkFor<(K, V)>,
    TotalOrder: MinOrder<N::OrderingGuarantee>,
{
    // The leader manufactures THE interleaving. `nondet!` (run-contingent
    // choice); no consistency downgrade on a Process (DropConsistency = Self).
    let merged = at_leader.entries_partially_ordered(nondet_interleaving);

    // Leader → replicas: FIFO broadcast of the chosen sequence. TO,EC — but
    // F = {leader}, untracked (see module docs).
    merged.broadcast_closed(replicas, via)
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;

    use super::leader_merge_broadcast;

    /// Compile-time pin (taxonomy doc §4): the leader-merge construction is
    /// multi-writer TO,EC and type-checks with **zero consensus** — one
    /// `nondet!`, no consistency `manual_proof!`. Compilation plus
    /// `flow.finalize()` is the test; there are no runtime assertions.
    ///
    /// This pins the motivation for typed `F`: the stamped `EventualConsistency`
    /// silently carries `F = {leader}` and is indistinguishable from Raft's
    /// (`F = ∅`). See the module docs and taxonomy doc §5–§7.
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

        // Merge at the leader and FIFO-broadcast the chosen sequence.
        let log: Stream<
            (MemberId<()>, u32),
            Cluster<'_, (), EventualConsistency>,
            _,
            TotalOrder,
            _,
        > = leader_merge_broadcast(
            at_leader,
            &replicas,
            TCP.fail_stop().bincode(),
            nondet!(/** leader dictates the interleaving */),
        );

        let _ = log;
        let _ = flow.finalize();
    }
}
