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
//!
//! The two crash-injection demos below factor this precisely. Part 1
//! (`leader_merge_dissemination_hole_*`): with plain broadcast, a leader crash
//! violates agreement — but that is a *dissemination* defect, repaired without
//! consensus by shipping order as data over reliable broadcast. Part 2
//! (`leader_merge_plus_reliable_broadcast_agrees_but_blocks_at_f1`): the repaired
//! construction agrees in every execution yet is **blocking** at `F = 1` — a
//! reachable dead state where a post-crash write is permanently uncommittable.
//! Non-blockingness under minority crashes (a possibility property with a finite
//! witness, not an FLP-forbidden liveness claim) is the thing consensus actually
//! buys: author succession.

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

    /// **Crash demo, part 1 — the dissemination hole (fixable without
    /// consensus).** The module docs state the caveat in prose: the stamped
    /// TO,EC silently assumes the leader survives (`F = {leader}`), and "a
    /// leader crash mid-broadcast leaves live replicas holding different
    /// prefixes forever." This test makes that a sim-found fact: crash injection
    /// lets the leader die at a send boundary, delivering an independent
    /// per-replica prefix, and the exhaustive search finds an execution where
    /// the replicas' logs diverge permanently.
    ///
    /// But note what this does and does not show. Divergence is an *agreement*
    /// failure of the dissemination step, and dissemination fault-tolerance is
    /// the cheap part: swap the leader's plain broadcast for reliable broadcast
    /// (with order shipped as data) and this divergence disappears — see
    /// [`leader_merge_plus_reliable_broadcast_agrees_but_blocks_at_f1`], which
    /// pins the deeper hole that survives the swap: the construction is
    /// **blocking** at F = 1. What consensus adds over leader_merge is not
    /// better dissemination but *author succession*.
    #[test]
    fn leader_merge_dissemination_hole_leader_crash_diverges_replicas() {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let leader = flow.process::<()>();
        let replicas = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let at_leader = local.send(&leader, TCP.fail_stop().bincode());

        let log = leader_merge_broadcast(
            at_leader,
            &replicas,
            TCP.fail_stop().bincode(),
            nondet!(/** leader dictates the interleaving */),
        );

        let out_recv = log
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let mut saw_divergence = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 1)
            .with_cluster_size(&replicas, 2)
            .with_crashable_process(&leader)
            .exhaustive(async || {
                in_send.send(0, 10);
                in_send.send(0, 20);

                let received: Vec<(MemberId<()>, (MemberId<()>, u32))> =
                    out_recv.collect_sorted().await;

                let logs: Vec<Vec<u32>> = (0..2u32)
                    .map(|replica| {
                        received
                            .iter()
                            .filter(|(r, _)| *r == MemberId::from_raw_id(replica))
                            .map(|(_, (_, v))| *v)
                            .collect()
                    })
                    .collect();

                // Safety in EVERY execution: each replica holds a prefix of the
                // leader's chosen order (FIFO; the crash cuts a suffix, never
                // reorders).
                for (replica, log) in logs.iter().enumerate() {
                    assert!(
                        [vec![], vec![10], vec![10, 20]].contains(log),
                        "replica {replica} holds a non-prefix log {log:?}"
                    );
                }

                if logs[0] != logs[1] {
                    saw_divergence = true;
                }
                if logs.iter().all(|log| log.len() == 2) {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_divergence,
            "expected an execution where the leader crash leaves replicas holding \
             different log prefixes forever — the agreement loss consensus prevents"
        );
        assert!(
            saw_full_delivery,
            "sanity: the crash-free execution replicates the full log everywhere"
        );
    }

    /// **Crash demo, part 2 — the succession hole (this is what consensus is).**
    /// Patch part 1's dissemination hole: the leader ships its chosen order *as
    /// data* (slot indices, since the echo destroys FIFO) and disseminates via
    /// [`reliable_broadcast_closed`]. Now, in **every** explored execution —
    /// including every leader-crash timing — the replicas' delivered logs are
    /// *identical dense prefixes* of the leader's order: agreement is fully
    /// repaired without a whiff of consensus.
    ///
    /// What cannot be repaired by more dissemination: the construction is
    /// **blocking** (in the Skeen sense) at F = 1. The exhaustive search finds a
    /// reachable *dead state*: quiescent, fault budget respected, network
    /// connected and fail-stop — and a write submitted after the crash is
    /// permanently uncommittable, because the sole author is dead and nothing
    /// can safely replace him. Note this is deliberately NOT phrased as a
    /// liveness violation: "≤ F crashes ⇒ eventual commit" is unattainable
    /// anyway (FLP). The property consensus actually adds is *non-blockingness*
    /// — no reachable dead state under minority crashes — which has a finite
    /// witness and is exactly what the sim's controlled quiescence can check.
    /// A future Paxos counterpart (needs crashable cluster members) would assert
    /// the complement: no dead state exists.
    #[test]
    fn leader_merge_plus_reliable_broadcast_agrees_but_blocks_at_f1() {
        use crate::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let leader = flow.process::<()>();
        let replicas = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let at_leader = local.send(&leader, TCP.fail_stop().bincode());

        // The leader manufactures the interleaving and ships order AS DATA:
        // (slot, entry). Reliable broadcast's echo re-delivers out of order, but
        // slots let every replica reconstruct the leader's exact total order.
        let slotted = at_leader
            .entries_partially_ordered(nondet!(/** leader dictates the interleaving */))
            .enumerate();

        let log = reliable_broadcast_closed(slotted, &replicas);

        let out_recv = log
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let mut saw_blocked = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 1)
            .with_cluster_size(&replicas, 2)
            .with_crashable_process(&leader)
            .exhaustive(async || {
                // Phase 1: two writes; run to the phase barrier (the leader may
                // crash at any explored send boundary along the way).
                in_send.send(0, 10);
                in_send.send(0, 20);
                hydro_lang::sim::quiesce().await;

                // Phase 2: a write submitted after the phase-1 fixpoint. If the
                // leader is dead, no protocol step can ever commit it.
                in_send.send(0, 30);

                let received: Vec<(MemberId<()>, (usize, (MemberId<()>, u32)))> =
                    out_recv.collect_sorted().await;

                let logs: Vec<Vec<(usize, u32)>> = (0..2u32)
                    .map(|replica| {
                        received
                            .iter()
                            .filter(|(r, _)| *r == MemberId::from_raw_id(replica))
                            .map(|(_, (slot, (_, v)))| (*slot, *v))
                            .collect()
                    })
                    .collect();

                // AGREEMENT in every execution — the contrast with part 1: the
                // echo makes per-entry delivery all-or-nothing across replicas.
                assert_eq!(
                    logs[0], logs[1],
                    "reliable dissemination must not let replicas diverge"
                );

                // And the agreed log is a dense prefix of the leader's chosen
                // order in every execution (FIFO staging + echo preserve
                // prefix-closedness of the delivered slot set).
                let expected = [(0, 10u32), (1, 20), (2, 30)];
                assert!(
                    logs[0].as_slice() == &expected[..logs[0].len()],
                    "agreed log {:?} is not a dense prefix of the leader's order",
                    logs[0]
                );

                // The dead-state witness: final quiescence, write 30 submitted,
                // never committed anywhere — and nothing can ever change that.
                if logs[0].len() < 3 {
                    saw_blocked = true;
                }
                if logs[0].len() == 3 {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_blocked,
            "expected a reachable dead state: leader crashed, replicas agree, and the \
             post-crash write is permanently uncommittable — leader_merge + RB is blocking \
             at F = 1, which is precisely the hole consensus (author succession) closes"
        );
        assert!(
            saw_full_delivery,
            "sanity: the crash-free execution commits all three writes everywhere"
        );
    }

    /// **Crash demo, part 2b — the member-leader variant fails the same test
    /// Raft passes, under the identical fault model.** The distinguished-member
    /// route ([`leader_merge_slots_from_member`]: all writers' values funnel to
    /// member 0 *of the log cluster*, which slots and broadcasts them to its
    /// peers) is run under `with_crashable_cluster(log_cluster, 1)` — the
    /// *same* opt-in as Raft's `any_single_crash_cannot_block_progress`: which
    /// member dies, when, and with which per-recipient cut are all search
    /// dimensions; nothing is targeted. The client is crash-agnostic, retrying
    /// its write through every writer each phase.
    ///
    /// Raft's test asserts that under this fault model the write *always*
    /// commits on a majority. This test pins that the member-leader merge
    /// **fails** that property existentially: the search finds executions where
    /// the retried write never reaches any log member — the crash landed on
    /// member 0, the sole author, and no peer can take over the merge. There is
    /// nothing to elect and no log to splice: single-author total order is
    /// blocking at F = 1 no matter how the client retries.
    #[test]
    fn member_leader_single_crash_can_block_progress() {
        const WRITERS: usize = 2;

        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<()>();
        let log_cluster = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, local) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = leader_merge_slots_from_member(
            local,
            &log_cluster,
            nondet!(/** the distinguished member dictates the interleaving */),
        )
        .send(&node, TCP.fail_stop().bincode())
        .entries()
        .sim_output();

        let mut saw_blocked = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, WRITERS)
            .with_cluster_size(&log_cluster, 2)
            .with_crashable_cluster(&log_cluster, 1)
            .fuzz(async || {
                // Phase 1: a first write, retried to every writer (the client
                // does not know who is alive). The fault search may crash a
                // writer at any of its send boundaries along the way.
                for w in 0..WRITERS as u32 {
                    in_send.send(w, 10);
                }
                hydro_lang::sim::quiesce().await;

                // Phase 2: a second write, again retried to every writer. With
                // F = 1, at least one recipient of the retry is alive.
                for w in 0..WRITERS as u32 {
                    in_send.send(w, 20);
                }

                let received: Vec<(MemberId<()>, (usize, (MemberId<()>, u32)))> =
                    out_recv.collect_sorted().await;

                let delivered_20 = (0..2u32)
                    .filter(|replica| {
                        received.iter().any(|(r, (_, (_, v)))| {
                            *r == MemberId::from_raw_id(*replica) && *v == 20
                        })
                    })
                    .count();

                if delivered_20 == 0 {
                    // The retried write reached no log member, ever — the sole
                    // author is dead. This is the dead state Raft's test
                    // proves unreachable under the same fault model.
                    saw_blocked = true;
                }
                if delivered_20 == 2 {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_blocked,
            "expected an execution where one crash (the distinguished member) blocks \
             the retried write forever — the single-author dead state"
        );
        assert!(
            saw_full_delivery,
            "sanity: some execution delivers the second write to every log member"
        );
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
