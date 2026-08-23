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
use hydro_lang::live_collections::stream::{
    ExactlyOnce, IsOrdered, MinOrder, NoOrder, Ordering, Retries, TotalOrder,
};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
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

/// The **honest-type variant**: leader-merge from a distinguished cluster
/// member, returning the keyed stream the type system natively produces —
/// per-sender substreams, each TotalOrder + EC — rather than forcing a
/// flattened signature.
///
/// [`leader_merge_slots_from_member`]'s docs explain why the naive member-leader
/// port cannot output `Stream<T, Cluster<EC>, TotalOrder>`: member-locality is
/// invisible to the types, so a cluster-source broadcast is a multi-writer
/// broadcast and there is no safe door out of keyed-by-sender. This variant
/// treats that refusal as a feature and stops one step earlier: the broadcast's
/// own output type
///
/// ```text
/// KeyedStream<MemberId<L2>, (writer, value), Cluster<L2, EC>, per-key TotalOrder>
/// ```
///
/// is *already* the single-writer row of the taxonomy applied per sender: for
/// each key, every member eventually holds the same sequence. EC inferred, the
/// full merged order preserved (per key), zero consistency assertions, and —
/// unlike the slot route — no `ExactlyOnce` requirement, since nothing is
/// enumerated.
///
/// The interesting property is the one the types deliberately do not state:
/// **all keys but the distinguished member's are empty.** That is a runtime
/// invariant of this dataflow (only member 0 receives writer data), pinned by
/// the behavior test, not by the signature. Consumers who know the leader pick
/// its key; consumers who don't are forced by the type to confront the
/// cross-key question — which is the correct default, because under leader
/// *succession* this same signature keeps telling the truth: key 0's substream
/// ends, key 1's begins, and how epochs splice across keys is exactly the
/// fencing residue (taxonomy doc §8).
pub fn leader_merge_keyed_from_member<'a, T, W, C, L2, R>(
    local: Stream<T, Cluster<'a, W, C>, Unbounded, TotalOrder, R>,
    cluster: &Cluster<'a, L2>,
    nondet_interleaving: NonDet,
) -> KeyedStream<
    MemberId<L2>,
    (MemberId<W>, T),
    Cluster<'a, L2, EventualConsistency>,
    Unbounded,
    TotalOrder,
    R,
>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    W: 'a,
    C: hydro_lang::location::cluster::Consistency,
    L2: 'a,
    R: Retries,
{
    // Step 1: Route every writer's stream to the distinguished member.
    let at_member = local
        .map(q!(|item| (MemberId::from_raw_id(0), item)))
        .into_keyed()
        .demux(cluster, TCP.fail_stop().bincode());

    // Step 2: The distinguished member manufactures THE interleaving (the one
    // nondet!). TotalOrder at the cluster location, NoConsistency — honestly,
    // since the types cannot see that only member 0 has data.
    let merged = at_member.entries_partially_ordered(nondet_interleaving);

    // Step 3: Broadcast, and stop. The keyed output is the honest type:
    // per-sender TO,EC (the single-writer row, per key). At runtime every
    // key but member 0's is empty — a fact for tests and consumers, not types.
    merged.broadcast_closed(cluster, TCP.fail_stop().bincode())
}

/// The **slot route**: leader-merge where the leader is a *distinguished cluster
/// member* rather than a separate `Process`, with the order shipped as data.
///
/// # Why the naive port of [`leader_merge_broadcast`] does not type-check
///
/// The Process version's clean TO,EC typing is load-bearing on
/// `Process::DropConsistency = Self`. On a `Cluster`,
/// `DropConsistency = Cluster<NoConsistency>` — and "only member 0 has data" is
/// member-locality, which the type system deliberately does not track (taxonomy
/// doc §1: anything touching `CLUSTER_SELF_ID` is `NoConsistency`). So after the
/// member's broadcast you hold `KeyedStream<MemberId<_>, T, Cluster<EC>, per-key
/// TO>` and there is **no safe door** to `Stream<T, Cluster<EC>, TotalOrder>`:
/// `values()` keeps EC and surrenders order; `entries_partially_ordered` keeps
/// order and — this time genuinely — strips consistency. That is taxonomy doc
/// §3c's missing morphism, hit head-on: to the types, a distinguished member is
/// a potential multi-writer. (The refusal is a feature: see
/// [`leader_merge_keyed_from_member`] for the variant that embraces the keyed
/// output type instead of flattening at all.)
///
/// # The slot route (taxonomy doc §3c / §8)
///
/// Make the order *data* before broadcasting, so the cross-member flattening is
/// never needed:
///
/// 1. route every writer's stream to the distinguished member (`demux` to raw
///    id 0) — sharded receipt, TO,NC;
/// 2. the member merges (`entries_partially_ordered`, the one `nondet!`) and
///    `enumerate()`s the chosen sequence into `(slot, value)` facts — the order
///    now rides in the slots, not in transit;
/// 3. broadcast the facts to the whole cluster; `values()` keeps EC and drops
///    only the transit order, which no longer matters.
///
/// Output: the NoOrder,EC bag of slot facts, with **zero consistency
/// assertions** — EC is inferred end-to-end. Each member can recover the
/// sequence locally by dense-prefix extraction (deterministic on the bag, and
/// monotone, hence EC-preserving per §3c). Slot uniqueness — §3c's residue — is
/// free here: a single author cannot assign one slot twice.
///
/// # Fault story (taxonomy doc §8)
///
/// Same SPOF as [`leader_merge_broadcast`] — F = {member 0}, still untracked —
/// but two things moved: the leader is now itself a member of N (a beneficiary),
/// and succession is at least *expressible* (re-route the demux to member 1),
/// which is the first plank of §8's author-succession story and immediately
/// raises the fencing residue if attempted. Composing this with the
/// reliable-broadcast echo would clear convergence-F to ∅ (§8's hardened
/// variant); the demo keeps the plain broadcast so its ledger stays comparable
/// to the Process version's.
pub fn leader_merge_slots_from_member<'a, T, W, C, L2>(
    local: Stream<T, Cluster<'a, W, C>, Unbounded, TotalOrder, ExactlyOnce>,
    cluster: &Cluster<'a, L2>,
    nondet_interleaving: NonDet,
) -> Stream<
    (usize, (MemberId<W>, T)),
    Cluster<'a, L2, EventualConsistency>,
    Unbounded,
    NoOrder,
    ExactlyOnce,
>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    W: 'a,
    C: hydro_lang::location::cluster::Consistency,
    L2: 'a,
{
    // Step 1: Every writer routes its (locally ordered) stream to the
    // distinguished member. Sharded receipt at the cluster: keyed by writer,
    // per-key TotalOrder, NoConsistency (member-local data — §1's TO,NC).
    let at_member = local
        .map(q!(|item| (MemberId::from_raw_id(0), item)))
        .into_keyed()
        .demux(cluster, TCP.fail_stop().bincode());

    // Step 2: The distinguished member manufactures THE interleaving (the one
    // nondet!) and turns it into (slot, value) facts. The order is now data.
    // Only member 0 has input, but the types cannot see that — which is
    // exactly why the naive port fails and this one does not need it to.
    let slotted = at_member
        .entries_partially_ordered(nondet_interleaving)
        .enumerate();

    // Step 3: Broadcast the slot facts to the whole cluster. values() keeps
    // EC and surrenders only the transit order — the slots carry the real one.
    // Zero consistency assertions: EC inferred from the network policy.
    slotted
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
        .values()
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::keyed_stream::KeyedStream;
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;

    use super::{
        leader_merge_broadcast, leader_merge_keyed_from_member, leader_merge_slots_from_member,
    };

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

    /// Compile-time pin for the **slot route** (taxonomy doc §3c / §8): with the
    /// leader as a distinguished *cluster member*, the naive port cannot reach
    /// TO,EC (no safe door out of keyed-by-sender on a cluster — the missing
    /// morphism), but shipping the order as `(slot, value)` data type-checks to
    /// a NoOrder,EC fact bag with zero consistency assertions. Same single
    /// `nondet!` as the Process version; EC inferred end-to-end.
    #[test]
    fn member_leader_slot_route_is_ec_with_order_as_data() {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let log_cluster = flow.cluster::<()>();

        // Each writer: its own TO,NC stream.
        let (_w, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // Route to member 0, merge there, slot, broadcast. NoOrder,EC out.
        let facts: Stream<
            (usize, (MemberId<()>, u32)),
            Cluster<'_, (), EventualConsistency>,
            _,
            NoOrder,
            _,
        > = leader_merge_slots_from_member(
            local,
            &log_cluster,
            nondet!(/** the distinguished member dictates the interleaving */),
        );

        let _ = facts;
        let _ = flow.finalize();
    }

    /// Behavior test for the slot route: across every explored interleaving,
    /// every member of the log cluster materializes the *same* set of slot
    /// facts, with dense slots (0..n) — the bag from which each member can
    /// deterministically extract the same sequence.
    #[test]
    fn member_leader_slot_route_delivers_same_dense_facts_to_all() {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let log_cluster = flow.cluster::<()>();

        let (in_send, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = leader_merge_slots_from_member(
            local,
            &log_cluster,
            nondet!(/** the distinguished member dictates the interleaving */),
        )
        .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&log_cluster, 2)
            .exhaustive(async || {
                // Two writers, one value each.
                in_send.send(0, 10);
                in_send.send(1, 20);

                // Each log member must deliver exactly 2 slot facts, and after
                // sorting the two members' bags must be identical.
                let member0: Vec<(usize, (MemberId<()>, u32))> =
                    out_recv.collect_n_sorted(0, 2).await;
                let member1: Vec<(usize, (MemberId<()>, u32))> =
                    out_recv.collect_n_sorted(1, 2).await;
                assert_eq!(member0, member1, "log members diverged");

                // Dense slots {0, 1}; values are the two writers' inputs.
                let slots: Vec<usize> = member0.iter().map(|(s, _)| *s).collect();
                assert_eq!(slots, vec![0, 1], "slots not dense");
                let mut values: Vec<u32> = member0.iter().map(|(_, (_, v))| *v).collect();
                values.sort();
                assert_eq!(values, vec![10, 20]);
            });
    }

    /// Compile-time pin for the **honest-type variant**: the member-leader
    /// merge outputs the keyed stream the type system natively produces —
    /// `KeyedStream<MemberId<log>, (writer, value), Cluster<EC>, per-key
    /// TotalOrder>` — EC inferred, full merged order preserved per key, zero
    /// consistency assertions. The type deliberately does *not* state that all
    /// keys but one are empty; that runtime property is pinned by the behavior
    /// test below.
    #[test]
    fn member_leader_keyed_is_per_key_total_order_ec() {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let log_cluster = flow.cluster::<()>();

        let (_w, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let log: KeyedStream<
            MemberId<()>,
            (MemberId<()>, u32),
            Cluster<'_, (), EventualConsistency>,
            _,
            TotalOrder,
            _,
        > = leader_merge_keyed_from_member(
            local,
            &log_cluster,
            nondet!(/** the distinguished member dictates the interleaving */),
        );

        let _ = log;
        let _ = flow.finalize();
    }

    /// Behavior test for the honest-type variant: every log member receives
    /// the full merged log, and **every entry is keyed by the distinguished
    /// member** — the "all keys but one are empty" runtime invariant the
    /// signature deliberately does not claim.
    #[test]
    fn member_leader_keyed_all_data_under_distinguished_key() {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let log_cluster = flow.cluster::<()>();

        let (in_send, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = leader_merge_keyed_from_member(
            local,
            &log_cluster,
            nondet!(/** the distinguished member dictates the interleaving */),
        )
        .entries()
        .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&log_cluster, 2)
            .exhaustive(async || {
                // Two writers, one value each.
                in_send.send(0, 10);
                in_send.send(1, 20);

                let distinguished = MemberId::<()>::from_raw_id(0);
                for member in 0..2u32 {
                    let got: Vec<(MemberId<()>, (MemberId<()>, u32))> =
                        out_recv.collect_n_sorted(member, 2).await;

                    // All entries keyed by the distinguished member — every
                    // other sender's substream is empty.
                    assert!(
                        got.iter().all(|(sender, _)| *sender == distinguished),
                        "member {member} saw entries from a non-distinguished sender: {got:?}"
                    );
                    // The full merged log arrived.
                    let mut values: Vec<u32> = got.iter().map(|(_, (_, v))| *v).collect();
                    values.sort();
                    assert_eq!(values, vec![10, 20], "member {member} missing log entries");
                }
            });
    }
}
