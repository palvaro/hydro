//! Rung 4 of the quorum→consensus ladder: **multi-decree consensus** — an
//! epoch-keyed log of [synod](super::synod), consumed by the M1 splice reader
//! ([`splice_epoch_log`]). Herlihy's universal construction; the ladder
//! terminates here because the hierarchy does.
//!
//! # The construction: synod per slot, phase 1 amortized per epoch
//!
//! Proposers (a cluster, so they can duel) are told to lead by the caller —
//! the Ω discipline, exactly as at rung 3, except a round now buys a whole
//! *epoch* rather than one decree. Ballot = [`Ts`] unchanged; **epoch =
//! `round`** (caller contract: rounds are globally distinct across proposer
//! members, so ballot order and epoch order agree — the deterministic
//! epoch↦member map of `2026-08_epoch_keyed_consensus_splice.md` §2).
//!
//! - **Phase 1, once per epoch, covering every slot at once:** prepare(b) to
//!   all acceptors; promises carry the acceptor's *entire* per-slot accepted
//!   map; [`covering_quorum_slotted`] merges a majority of maps slot-wise
//!   (per-slot max-by-ballot) and fires once. This amortization is what makes
//!   the rung multi-decree rather than "synod in a loop".
//! - **Phase 2, per (ballot, slot):** the rung-0 [`quorum`] mint unchanged —
//!   accepted-acks at majority ⇒ chosen, per slot.
//! - **The acceptor is synod's refusal kernel with a map:** one
//!   `max_promised` fences *every* slot (endorsement-is-promise, the splice
//!   doc's §4(b), for free from the epoch structure), and `accepted` is a
//!   per-slot max-by-ballot register. Same tick-serialization, same two batch
//!   relaxations, same safety argument as synod's module docs — now slot-wise.
//!   No EC label available or wanted: still the determination kernel.
//!
//! # The splice, made protocol (what is genuinely new at this rung)
//!
//! On covering, the new leader runs the **epoch plan** ([`EpochPlan::new`]):
//!
//! - **Declared start** `s_e` = the leader's locally *learned* chosen prefix —
//!   a monotone lower bound on the committed prefix (any view is safe: a
//!   stale one only means more re-proposal). Slots below `s_e` stay owned by
//!   older epochs; their chosen facts are already stable and disseminating.
//! - **Adopt-highest, per slot:** every covering slot at or beyond `s_e` is
//!   re-proposed under the new ballot at the same slot — rung 3's one rule,
//!   run once per slot. Chosen entries intersect the covering, so revision is
//!   retransmission, slot-wise.
//! - **No-op filling:** covering holes at or beyond `s_e` are proposed as
//!   `None`. Safe by the free-choice rule (a hole in a majority covering
//!   means no chosen entry there — the intersection argument); required for
//!   liveness (an unfillable hole would stall the splice forever).
//! - New commands are sequenced after the recovery range — the **leader
//!   kernel**, the one authored (order-sensitive) slice at the proposer.
//!   Ballot management is thereby in-protocol: one phase 1 per epoch, then
//!   phase-2-only appends for every subsequent command under that epoch.
//!
//! **Start facts ride chosen entries.** A learner absorbs
//! `Start { e, s_e }` only alongside a chosen entry of epoch `e`, never from
//! a bare declaration. This anchors splice *ownership* to actual choices: a
//! candidate that covers and dies without choosing anything leaves no trace
//! in any learner's ownership map, so it can never orphan a slot. (A bare
//! Start would let a dead epoch own slots it will never fill — a permanent
//! stall the sim would find.)
//!
//! # Learning: where EC re-enters on consensus output
//!
//! A chosen certificate is a stable fact, and disseminating stable facts is
//! broadcast-shaped work — the one planned edge from the consensus branch
//! back into the dissemination branch (ladder doc §3). The proposer that
//! mints a `Durable` certificate ships the chosen decree to the learner
//! cluster, and learners run RB's echo cycle over it (merge + `unique` +
//! re-broadcast through a `forward_ref` on the EC location), so a proposer
//! crash mid-ship cannot strand a minority of learners: deliveries are
//! all-or-nothing among live learners, the fact bags converge, and
//! [`splice_epoch_log`]'s EC singleton is honest. The output types carry
//! `EventualConsistency` **compiler-checked** (explicit annotations below) —
//! monotone shell in, refusal kernel, monotone shell out (§3a).
//!
//! **The transportable-certificate decision** (deferred since rung 0, forced
//! here): [`Durable`](super::quorum::Durable) is deliberately not
//! `Serialize`, so the certificate itself never crosses a wire. What crosses
//! is the *fact*, unwrapped at the minting proposer, and its authority is the
//! **channel**: only the audited chosen-output path feeds the learning
//! broadcast, so a learner's trust that "this decree was chosen" is trust in
//! the sender's dataflow — the same convention-grade sealing as the mints
//! themselves (`#[doc(hidden)]`, honest ledger). The heavier alternative —
//! shipping the attestor set for receiver-side re-verification — remains
//! future design.
//!
//! # Ledger
//!
//! One new mint ([`covering_quorum_slotted`], the map-shaped covering — the
//! generalization debt recorded on `covering_quorum` now has two data
//! points), zero consistency assertions in this protocol body, and two
//! non-monotone kernels, each a finalizing slice: the acceptor (refusal) and
//! the leader kernel (authored sequencing — `leader_merge`'s `nondet!` seam,
//! now epoch-scoped). Everything else is mints, joins, folds, and broadcasts.
//!
//! # Caller contracts (honest ledger)
//!
//! - **Rounds are globally distinct across proposer members** (the Ω driver
//!   owns rounds; round-robin in the tests). Reusing a round forges a ballot
//!   — synod's distinct-rounds red test witnesses the failure mode.
//! - **One outstanding lead per member**; commands submitted while a member
//!   is deposed (or before its first covering) are sequenced under a stale
//!   epoch and fenced — *lost, not corrupted*. Resubmission is the caller's
//!   job, matching "progress is a driver discipline" from rung 3.
//! - Progress claims are Ω-conditional (FLP tax, unchanged); a transiently
//!   stalled splice (e.g. a chosen decree whose certificate died with its
//!   proposer) is repaired by the next elected epoch's re-proposal —
//!   revision-as-retransmission is also the repair mechanism.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;

use hydro_lang::live_collections::singleton::Singleton;
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, EventualConsistency};
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::epoch_splice::{SpliceFact, SpliceState, splice_epoch_log};
use super::quorum::{SlotMap, Ts, covering_quorum_slotted, quorum};

/// Responses from acceptors to proposers (the acceptor's identity rides on
/// the channel keying, as in synod).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum ToProposer<V> {
    /// Phase-1 promise: the full per-slot accepted map.
    Promise {
        b: Ts,
        accepted: SlotMap<Option<V>>,
    },
    /// Phase-2 ack for one slot.
    Accepted { b: Ts, slot: usize },
}

/// The new leader's plan for its epoch, computed once per covering: the
/// declared start slot, the recovery proposals (adopt-highest per slot,
/// no-op-filled holes), and the next free slot for fresh commands.
///
/// Pure and unit-tested; the leader kernel calls [`EpochPlan::new`] inside
/// staged code (see the module docs for why each piece is safe).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochPlan<V> {
    /// Declared start slot `s_e`: the leader's learned chosen prefix.
    pub start: usize,
    /// `(slot, value)` proposals for the recovery range `[start, next)`:
    /// the covering's per-slot max where present, `None` (no-op) at holes.
    pub recovery: Vec<(usize, Option<V>)>,
    /// First slot available for fresh commands.
    pub next: usize,
}

impl<V: Clone> EpochPlan<V> {
    /// Plan an epoch from the covering's adopted map and the leader's
    /// locally learned chosen prefix.
    ///
    /// - `start = learned_prefix`: a monotone lower bound on the committed
    ///   prefix (stale is safe — it only widens the recovery range).
    /// - Recovery covers `[start, max_adopted_slot]`: adopt-highest where
    ///   the covering saw a value (which, by quorum intersection, includes
    ///   every chosen entry at or beyond `start`), no-op where it saw none
    ///   (free choice: nothing was chosen there).
    pub fn new(adopted: BTreeMap<usize, (Ts, Option<V>)>, learned_prefix: usize) -> Self {
        let start = learned_prefix;
        let horizon = match adopted.keys().next_back() {
            Some(&max) if max >= start => max + 1,
            _ => start,
        };
        let recovery = (start..horizon)
            .map(|slot| (slot, adopted.get(&slot).and_then(|(_, v)| v.clone())))
            .collect();
        EpochPlan {
            start,
            recovery,
            next: horizon,
        }
    }
}

/// The outputs of [`multi_paxos`], per location.
pub struct MultiPaxosOutputs<'a, P, LRN, V> {
    /// Chosen decrees as uniformly learned at each learner:
    /// `(epoch, declared_start, slot, value)`; `value = None` is a leader
    /// no-op filler (consumers skip it). EC — every live learner converges
    /// to the same set.
    pub learned: Stream<
        (u64, usize, usize, Option<V>),
        Cluster<'a, LRN, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
    /// The spliced log at each learner: the M1 reader folded over the
    /// learned facts. EC, compiler-pinned.
    pub log: Singleton<SpliceState<Option<V>>, Cluster<'a, LRN, EventualConsistency>, Unbounded>,
    /// Chosen decrees observed at each proposer (its own ballots' mints plus
    /// everyone's via the proposer-side learning broadcast) — indexical,
    /// correctly un-EC.
    pub chosen: Stream<(u64, usize, usize, Option<V>), Cluster<'a, P>, Unbounded, NoOrder, ExactlyOnce>,
}

/// Multi-decree consensus over a static acceptor cluster, with uniform
/// learning into the epoch-keyed splice reader. See the module docs.
///
/// `leads` carries rounds (globally distinct across members — caller
/// contract; the Ω driver owns escalation) telling this member to establish
/// an epoch. `commands` carries values this member proposes; they are
/// sequenced under its most recent established epoch (queued until the
/// first one exists).
pub fn multi_paxos<'a, V, P, A, LRN>(
    acceptors: &Cluster<'a, A>,
    learners: &Cluster<'a, LRN>,
    majority: usize,
    leads: Stream<u64, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    commands: Stream<V, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
) -> MultiPaxosOutputs<'a, P, LRN, V>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
    LRN: 'a,
{
    multi_paxos_inner(acceptors, learners, majority, leads, commands, true)
}

/// **Deliberately broken — exists only to be refuted.** Identical to
/// [`multi_paxos`] except the leader ignores the covering (as if phase 1
/// returned an empty map): no adoption, no recovery, fresh commands sequenced
/// straight from the declared start. The simulator must find executions with
/// two different values chosen at one slot
/// (see `multi_paxos_without_adoption_violates_agreement`), proving per-slot
/// adopt-highest is load-bearing at this rung too.
#[doc(hidden)]
pub fn multi_paxos_without_adoption_for_refutation<'a, V, P, A, LRN>(
    acceptors: &Cluster<'a, A>,
    learners: &Cluster<'a, LRN>,
    majority: usize,
    leads: Stream<u64, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    commands: Stream<V, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
) -> MultiPaxosOutputs<'a, P, LRN, V>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
    LRN: 'a,
{
    multi_paxos_inner(acceptors, learners, majority, leads, commands, false)
}

fn multi_paxos_inner<'a, V, P, A, LRN>(
    acceptors: &Cluster<'a, A>,
    learners: &Cluster<'a, LRN>,
    majority: usize,
    leads: Stream<u64, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    commands: Stream<V, Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    adopt_highest: bool,
) -> MultiPaxosOutputs<'a, P, LRN, V>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
    LRN: 'a,
{
    let proposer_cluster = leads.location().clone();

    // Each lead signal becomes a ballot: (round, own member id).
    let ballots = leads.map(q!(move |round| Ts {
        round,
        writer: CLUSTER_SELF_ID.clone().into_tagless()
    }));

    // ---- Phase 1 out: prepare(b) to every acceptor -------------------------
    let prepares = ballots
        .broadcast_closed(acceptors, TCP.fail_stop().bincode())
        .entries(); // (proposer, b) at each acceptor

    // Phase-2 accepts close a cycle (they depend on promises, which depend on
    // the acceptor): forward_ref at the acceptor location, synod-style.
    let (accepts_handle, accepts_fwd) = prepares
        .location()
        .forward_ref::<Stream<(MemberId<P>, (Ts, usize, Option<V>)), _, Unbounded, NoOrder>>();

    // ---- The acceptor: synod's refusal kernel, with a per-slot map ---------
    // Order-sensitive by design (refusal does not commute): one slice,
    // tick-serialized, no EC label. One max_promised fences every slot
    // (endorsement-is-promise); accepted is a per-slot max-by-ballot register.
    // Batch discipline and its two safe relaxations: synod's module docs,
    // applied slot-wise.
    let acceptor_out = sliced! {
        let mut max_promised = use::state(|l| l.singleton(q!(None)));
        let mut accepted = use::state(|l| l.singleton(q!(BTreeMap::new())));

        let prepare_batch = use::batch(prepares, nondet!(
            /// Arrival timing of prepares — which tick considers them. All
            /// checks are against the monotone max_promised, so batching
            /// only permutes legal serializations.
        ));
        let accept_batch = use::batch(accepts_fwd, nondet!(
            /// Arrival timing of accepts, same argument.
        ));

        // Accepts, checked against tick-start max_promised.
        let acc_checked = accept_batch.cross_singleton(max_promised.clone());

        let acks = acc_checked.clone().filter_map(q!(
            |((proposer, (b, slot, _v)), mp): ((_, (Ts, usize, _)), Option<Ts>)| {
                if mp.as_ref().map(|m| b >= *m).unwrap_or(true) {
                    Some((proposer, ToProposer::Accepted { b, slot }))
                } else {
                    None
                }
            }
        ));

        // Fold passing accepts into the per-slot accepted register (max by
        // ballot per slot; no slot ever regresses).
        let batch_accepted = acc_checked
            .filter_map(q!(|((_proposer, (b, slot, v)), mp): ((_, (Ts, usize, _)), Option<Ts>)| {
                if mp.as_ref().map(|m| b >= *m).unwrap_or(true) {
                    Some((slot, (b, v)))
                } else {
                    None
                }
            }))
            .fold(
                q!(|| BTreeMap::new()),
                q!(|acc: &mut BTreeMap<usize, (Ts, _)>, (slot, (b, v))| {
                    let dominated = acc.get(&slot).map(|(a, _)| *a < b).unwrap_or(true);
                    if dominated {
                        acc.insert(slot, (b, v));
                    }
                }, commutative = manual_proof!(
                    /** per-slot max by the total ballot order is commutative:
                    writer ids make cross-proposer ties impossible, rounds are
                    globally distinct (caller contract), and one value is
                    proposed per (ballot, slot). */
                )),
            );

        let new_accepted = accepted.zip(batch_accepted).map(q!(
            |(mut old, batch): (BTreeMap<usize, (Ts, _)>, BTreeMap<usize, (Ts, _)>)| {
                for (slot, (b, v)) in batch {
                    let dominated = old.get(&slot).map(|(a, _)| *a < b).unwrap_or(true);
                    if dominated {
                        old.insert(slot, (b, v));
                    }
                }
                old
            }
        ));
        accepted = new_accepted.clone();

        // Promises: checked against tick-start max_promised, reporting the
        // post-batch accepted map (reporting MORE accepted history than a
        // strict serialization is always safe — adopt-highest only becomes
        // more conservative).
        let proms = prepare_batch
            .clone()
            .cross_singleton(max_promised.clone())
            .cross_singleton(new_accepted)
            .filter_map(q!(
                |(((proposer, b), mp), acc): (((_, Ts), Option<Ts>), BTreeMap<usize, (Ts, _)>)| {
                    if mp.as_ref().map(|m| b > *m).unwrap_or(true) {
                        Some((proposer, ToProposer::Promise {
                            b,
                            accepted: acc.into_iter().collect(),
                        }))
                    } else {
                        None
                    }
                }
            ));

        // The commitment: max_promised advances by this batch's prepares and
        // refuses lower ballots — at every slot — forever after.
        let batch_max_prepare = prepare_batch.map(q!(|(_p, b)| b)).fold(
            q!(|| None),
            q!(|acc: &mut Option<Ts>, b| {
                if acc.as_ref().map(|a| *a < b).unwrap_or(true) {
                    *acc = Some(b);
                }
            }, commutative = manual_proof!(/** max is commutative (total order) */)),
        );
        max_promised = max_promised.zip(batch_max_prepare).map(q!(|(old, batch)| {
            match (old, batch) {
                (None, b) => b,
                (a, None) => a,
                (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
            }
        }));

        acks.chain(proms)
    };

    // Route responses back to their proposers.
    let from_acceptors = acceptor_out
        .into_keyed()
        .demux(&proposer_cluster, TCP.fail_stop().bincode())
        .entries(); // (acceptor, ToProposer) at each proposer

    let promises = from_acceptors.clone().filter_map(q!(|(acceptor, msg)| match msg {
        ToProposer::Promise { b, accepted } => Some((b, (acceptor, accepted))),
        _ => None,
    }));

    let accepted_acks = from_acceptors.filter_map(q!(|(acceptor, msg)| match msg {
        ToProposer::Accepted { b, slot } => Some(((b, slot), acceptor)),
        _ => None,
    }));

    // ---- Phase 1 in: the slotted covering certificate ----------------------
    let covered = covering_quorum_slotted(majority, promises)
        .map(q!(|(b, cov)| (b, cov.into_aggregate())));

    let covered = if adopt_highest {
        covered
    } else {
        // The refutation variant: ignore the covering. UNSAFE by design.
        covered.map(q!(|(b, _map)| (b, BTreeMap::new())))
    };

    // The leader's learned chosen prefix is fed by the learning broadcast
    // below — a cycle (chosen facts depend on the kernel, whose start-slot
    // choice reads learned facts), closed with a forward_ref. The label is
    // deliberately dropped: this is an indexical planning input, and any
    // stale view is a safe lower bound.
    let (learned_slots_handle, learned_slots_fwd) =
        proposer_cluster.forward_ref::<Stream<usize, _, Unbounded, NoOrder>>();

    // ---- The leader kernel: establishment + sequencing ---------------------
    // The one authored slice at the proposer (finalizing: slot assignment and
    // start declaration are sealed one-shot choices — leader_merge's seam,
    // epoch-scoped). Everything it emits is a proposal (b, start, slot, value).
    let proposed = sliced! {
        let cov_batch = use::batch(covered, nondet!(
            /// Establishment timing: which tick the covering certificate is
            /// acted on. Any covering is valid (mint), and the learned-prefix
            /// snapshot below is a monotone lower bound at any tick.
        ));
        let cmd_batch = use::batch(commands, nondet!(
            /// Command arrival timing — which tick sequences them. Slot
            /// assignment is the authored choice (the leader owns its
            /// epoch's order), so any batching yields a legal log.
        ));
        let learned_batch = use::batch(learned_slots_fwd, nondet!(
            /// Learning arrival timing. The learned set is monotone, so at
            /// any tick it is a safe lower bound on the committed prefix: a
            /// stale view only lowers the declared start, widening the
            /// (idempotent-safe) recovery re-proposal range.
        ));

        let mut cur = use::state(|l| l.singleton(q!(None)));
        let mut learned = use::state(|l| l.singleton(q!(BTreeSet::new())));
        let mut pending = use::state_null::<Stream<V, _, Bounded, TotalOrder>>();

        // Accumulate learned chosen slots (monotone set union).
        let batch_learned = learned_batch.fold(
            q!(|| BTreeSet::new()),
            q!(|set: &mut BTreeSet<usize>, slot| {
                set.insert(slot);
            }, commutative = manual_proof!(/** set insert is commutative */)),
        );
        let new_learned = learned.zip(batch_learned).map(q!(
            |(mut old, batch): (BTreeSet<usize>, BTreeSet<usize>)| {
                old.extend(batch);
                old
            }
        ));
        learned = new_learned.clone();

        // Epoch plans for this batch's coverings (normally at most one:
        // one-outstanding-lead contract).
        let est = cov_batch.cross_singleton(new_learned).map(q!(
            |((b, adopted), learned): ((Ts, BTreeMap<usize, (Ts, _)>), BTreeSet<usize>)| {
                let prefix = (0usize..).take_while(|s| learned.contains(s)).count();
                (b, EpochPlan::new(adopted, prefix))
            }
        ));

        // Recovery proposals: adopt-highest + no-op holes, at [start, next).
        let recovery = est.clone().flat_map_unordered(q!(|(b, plan): (Ts, EpochPlan<_>)| {
            let start = plan.start;
            plan.recovery
                .into_iter()
                .map(move |(slot, v)| (b.clone(), start, slot, v))
        }));

        // The member's current epoch: max-by-ballot across establishments.
        let est_max = est
            .map(q!(|(b, plan): (Ts, EpochPlan<_>)| (b, plan.start, plan.next)))
            .fold(
                q!(|| None),
                q!(|acc: &mut Option<(Ts, usize, usize)>, (b, start, next)| {
                    if acc.as_ref().map(|(a, _, _)| *a < b).unwrap_or(true) {
                        *acc = Some((b, start, next));
                    }
                }, commutative = manual_proof!(
                    /** max by the total ballot order; rounds are globally
                    distinct (caller contract), so no ties. */
                )),
            );

        let new_cur = cur.zip(est_max).map(q!(|(old, batch)| match (old, batch) {
            (None, b) => b,
            (a, None) => a,
            (Some(a), Some(b)) => Some(if a.0 >= b.0 { a } else { b }),
        }));

        // Sequence commands under the current epoch; queue if none yet.
        let cmds = pending.chain(cmd_batch);
        let n_cmds = cmds.clone().count();
        let indexed = cmds.enumerate();

        let assigned = indexed.clone().cross_singleton(new_cur.clone()).filter_map(q!(
            |((i, v), cur): ((usize, _), Option<(Ts, usize, usize)>)| {
                cur.map(|(b, start, next)| (b, start, next + i, Some(v)))
            }
        ));

        pending = indexed.cross_singleton(new_cur.clone()).filter_map(q!(
            |((_i, v), cur): ((usize, _), Option<(Ts, usize, usize)>)| {
                if cur.is_none() { Some(v) } else { None }
            }
        ));

        cur = new_cur.zip(n_cmds).map(q!(|(c, n): (Option<(Ts, usize, usize)>, usize)| {
            c.map(|(b, start, next)| (b, start, next + n))
        }));

        recovery.chain(assigned.weaken_ordering::<NoOrder>())
    };

    // ---- Phase 2 out: accepts to every acceptor, closing the cycle ---------
    let accepts = proposed
        .clone()
        .map(q!(|(b, _start, slot, v)| (b, slot, v)))
        .broadcast_closed(acceptors, TCP.fail_stop().bincode())
        .entries();
    accepts_handle.complete(accepts);

    // ---- Chosen: a Durable certificate per (ballot, slot) -------------------
    let certified = quorum(majority, accepted_acks).map(q!(|cert| cert.into_fact()));

    let chosen = certified
        .map(q!(|bs| (bs, ())))
        .join(proposed.map(q!(|(b, start, slot, v)| ((b, slot), (start, v)))))
        .map(q!(|((b, slot), ((), (start, v)))| (b.round, start, slot, v)))
        .weaken_ordering::<NoOrder>();

    // ---- Learning, leg 1: proposers' learned prefixes ----------------------
    // Chosen slots to every proposer (including self), closing the planning
    // cycle. Plain broadcast, label dropped: a planning lower bound, not a
    // consistency claim.
    let learned_at_proposers = chosen
        .clone()
        .map(q!(|(_epoch, _start, slot, _v)| slot))
        .broadcast_closed(&proposer_cluster, TCP.fail_stop().bincode())
        .values()
        .weaken_consistency();
    learned_slots_handle.complete(learned_at_proposers);

    // ---- Learning, leg 2: uniform dissemination to learners ----------------
    // Chosen decrees are stable facts; disseminating them is broadcast-shaped
    // work, and the learner-side echo cycle (RB's pattern) makes delivery
    // all-or-nothing among live learners even if the shipping proposer
    // crashes mid-broadcast. EC is inferred around the cycle — the label
    // legitimately re-enters on consensus output here.
    let initial = chosen
        .clone()
        .broadcast_closed(learners, TCP.fail_stop().bincode())
        .values();
    let (echo_handle, echo_fwd) = initial
        .location()
        .forward_ref::<Stream<(u64, usize, usize, Option<V>), _, Unbounded, NoOrder>>();
    let learned = initial.merge_unordered(echo_fwd).unique();
    let echo = learned
        .clone()
        .broadcast_closed(learners, TCP.fail_stop().bincode())
        .values();
    echo_handle.complete(echo);

    // ---- The splice: rung 4 consumes M1 -------------------------------------
    // Start facts ride chosen entries (module docs: ownership is anchored to
    // actual choices). The explicit annotations are the compile pin: if any
    // step failed to carry EC to the learner facts and through the splice
    // fold, this would not build.
    let facts: Stream<
        SpliceFact<Option<V>>,
        Cluster<'a, LRN, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    > = learned.clone().flat_map_unordered(q!(|(epoch, start, slot, value)| {
        [
            SpliceFact::Start { epoch, start_slot: start },
            SpliceFact::Entry { epoch, slot, value },
        ]
    }));

    let log: Singleton<SpliceState<Option<V>>, Cluster<'a, LRN, EventualConsistency>, Unbounded> =
        splice_epoch_log(facts);

    MultiPaxosOutputs {
        learned,
        log,
        chosen,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::super::epoch_splice::{SpliceFact, SpliceState};
    use super::super::quorum::Ts;
    use super::{EpochPlan, multi_paxos, multi_paxos_without_adoption_for_refutation};

    const N: usize = 3;
    const MAJORITY: usize = 2; // N/2 + 1
    const F: usize = 1;
    const LEARNERS: usize = 2;

    fn ts(round: u64) -> Ts {
        use hydro_lang::location::member_id::TaglessMemberId;
        Ts {
            round,
            writer: TaglessMemberId::from_raw_id(0),
        }
    }

    // ---- EpochPlan unit tests (the pure planning rule) ---------------------

    /// Fresh log, nothing learned, empty covering: start at 0, no recovery.
    #[test]
    fn plan_fresh_log() {
        let plan = EpochPlan::<u32>::new(BTreeMap::new(), 0);
        assert_eq!(plan, EpochPlan { start: 0, recovery: vec![], next: 0 });
    }

    /// Adopt-highest: covering slots at or beyond the learned prefix are
    /// re-proposed; slots below it are left to their owning epochs.
    #[test]
    fn plan_adopts_beyond_prefix_only() {
        let adopted = BTreeMap::from([
            (0, (ts(1), Some(10u32))),
            (1, (ts(1), Some(11))),
            (2, (ts(1), Some(12))),
        ]);
        let plan = EpochPlan::new(adopted, 1);
        assert_eq!(plan.start, 1);
        assert_eq!(plan.recovery, vec![(1, Some(11)), (2, Some(12))]);
        assert_eq!(plan.next, 3);
    }

    /// No-op filling: a covering hole inside the recovery range is proposed
    /// as `None` (free choice — nothing was chosen there), never skipped
    /// (a skipped slot would stall the splice forever).
    #[test]
    fn plan_fills_holes_with_noops() {
        let adopted = BTreeMap::from([(0, (ts(1), Some(10u32))), (2, (ts(1), Some(12)))]);
        let plan = EpochPlan::new(adopted, 0);
        assert_eq!(
            plan.recovery,
            vec![(0, Some(10)), (1, None), (2, Some(12))]
        );
        assert_eq!(plan.next, 3);
    }

    /// A learned prefix beyond everything the covering saw: nothing to
    /// recover, commands start at the prefix.
    #[test]
    fn plan_prefix_beyond_covering() {
        let adopted = BTreeMap::from([(0, (ts(1), Some(10u32)))]);
        let plan = EpochPlan::new(adopted, 2);
        assert_eq!(plan, EpochPlan { start: 2, recovery: vec![], next: 2 });
    }

    /// An adopted no-op is re-proposed as a no-op (it is a value, not a hole
    /// to be re-filled differently).
    #[test]
    fn plan_readopts_noops() {
        let adopted = BTreeMap::from([(0, (ts(1), None::<u32>))]);
        let plan = EpochPlan::new(adopted, 0);
        assert_eq!(plan.recovery, vec![(0, None)]);
    }

    // ---- Sim-test helpers ---------------------------------------------------

    /// Splice a learner's learned tuples at the harness (the M1 pattern:
    /// the in-flow fold is compile-pinned; per-member observation of an
    /// unbounded singleton is done test-side), returning the committed
    /// values with no-ops skipped (the RSM skip rule).
    fn splice_of(tuples: &[(u64, usize, usize, Option<u32>)]) -> Vec<u32> {
        let mut state = SpliceState::new();
        for (epoch, start, slot, v) in tuples {
            state.absorb(SpliceFact::Start { epoch: *epoch, start_slot: *start });
            state.absorb(SpliceFact::Entry { epoch: *epoch, slot: *slot, value: *v });
        }
        state.splice().into_iter().filter_map(|v| *v).collect()
    }

    /// Per-slot agreement: across every learned tuple (any epoch, any
    /// learner), at most one value per slot. Returns a violating slot.
    fn slot_divergence(tuples: &[(u64, usize, usize, Option<u32>)]) -> Option<usize> {
        let mut per_slot: BTreeMap<usize, BTreeSet<Option<u32>>> = BTreeMap::new();
        for (_epoch, _start, slot, v) in tuples {
            per_slot.entry(*slot).or_default().insert(*v);
        }
        per_slot.into_iter().find(|(_, vs)| vs.len() > 1).map(|(s, _)| s)
    }

    // ---- Sim tests -----------------------------------------------------------

    /// Smoke: one leader, one epoch, two commands — every learner's splice is
    /// the leader's sequence. Also pins the multi-decree amortization: one
    /// phase 1, two decrees.
    #[test]
    fn multi_paxos_single_leader_appends_in_order() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&learners, LEARNERS)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                cmd_send.send(0, 20u32);

                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert_eq!(
                        splice_of(&got),
                        vec![10, 20],
                        "learner {member} must splice the leader's sequence, got {got:?}"
                    );
                }
            });
    }

    /// **The rung-4 money test: succession splices the log.** Epoch 1 (member
    /// 0) commits a decree; epoch 2 (member 1) is then elected, declares its
    /// start from its learned prefix, and continues the log. Every learner
    /// converges to the concatenation, with slot 0 still owned by epoch 1 —
    /// the splice reader doing real cross-epoch work (nontrivial start slot).
    #[test]
    fn multi_paxos_succession_splices_the_log() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_cluster_size(&learners, LEARNERS)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                hydro_lang::sim::quiesce().await;

                lead_send.send(1, 2);
                cmd_send.send(1, 20u32);

                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert_eq!(
                        splice_of(&got),
                        vec![10, 20],
                        "learner {member} must splice across the succession, got {got:?}"
                    );
                    // The quiesce barrier makes epoch 2's plan deterministic:
                    // member 1 has learned slot 0, so it declares start 1 and
                    // re-proposes nothing — slot 0 stays owned by epoch 1.
                    assert_eq!(
                        got,
                        vec![(1, 0, 0, Some(10)), (2, 1, 1, Some(20))],
                        "learner {member}: epoch 2 must continue (start 1), not rewrite"
                    );
                }
            });
    }

    /// **Per-slot agreement under concurrently dueling leaders.** No
    /// barriers: prepares, promises, accepts, coverings, recovery
    /// re-proposals, and learning all race. In EVERY explored execution, no
    /// slot has two different chosen values anywhere in the system, and both
    /// learners converge to the same learned set at quiescence.
    #[test]
    fn multi_paxos_per_slot_agreement_under_dueling_leaders() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_cluster_size(&learners, LEARNERS)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                lead_send.send(1, 2);
                cmd_send.send(1, 20u32);

                let mut per_learner: Vec<Vec<(u64, usize, usize, Option<u32>)>> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    per_learner.push(learned_recv.collect_sorted(member).await);
                }

                let all: Vec<_> = per_learner.iter().flatten().copied().collect();
                assert!(
                    slot_divergence(&all).is_none(),
                    "AGREEMENT VIOLATED: two values chosen at slot {:?}: {all:?}",
                    slot_divergence(&all)
                );
                assert!(
                    !all.is_empty(),
                    "the higher ballot always completes; something must be chosen"
                );
                assert_eq!(
                    per_learner[0], per_learner[1],
                    "learners must converge to the same learned set at quiescence"
                );
            });
    }

    /// Dueling leaders AND one untargeted acceptor crash: per-slot agreement
    /// and learner convergence still hold in every explored execution.
    #[test]
    fn multi_paxos_agreement_under_dueling_and_acceptor_crash() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_cluster_size(&learners, LEARNERS)
            .with_crashable_cluster(&acceptors, F)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                lead_send.send(1, 2);
                cmd_send.send(1, 20u32);

                let mut per_learner: Vec<Vec<(u64, usize, usize, Option<u32>)>> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    per_learner.push(learned_recv.collect_sorted(member).await);
                }

                let all: Vec<_> = per_learner.iter().flatten().copied().collect();
                assert!(
                    slot_divergence(&all).is_none(),
                    "AGREEMENT VIOLATED under acceptor crash at slot {:?}: {all:?}",
                    slot_divergence(&all)
                );
                assert_eq!(
                    per_learner[0], per_learner[1],
                    "learners must converge at quiescence despite the acceptor crash"
                );
            });
    }

    /// **RED: per-slot adopt-highest is load-bearing.** The refutation
    /// variant ignores the covering, so a new epoch whose learned prefix is
    /// stale re-uses a slot that a lower ballot already filled — and both
    /// values reach "chosen". The search must witness two different chosen
    /// values at one slot (the multi-decree form of synod's no-adoption red,
    /// and the splice doc's §4(a) counterexample made mechanical).
    #[test]
    fn multi_paxos_without_adoption_violates_agreement() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs =
            multi_paxos_without_adoption_for_refutation(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        let mut saw_divergence = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_cluster_size(&learners, LEARNERS)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                lead_send.send(1, 2);
                cmd_send.send(1, 20u32);

                let mut all: Vec<(u64, usize, usize, Option<u32>)> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    all.extend(got);
                }
                if slot_divergence(&all).is_some() {
                    saw_divergence = true;
                }
            });

        assert!(
            saw_divergence,
            "without per-slot adoption, the search must find two different values \
             chosen at one slot"
        );
    }

    /// **RED: the intersection premise is load-bearing, slot-wise.** Quorum
    /// size 1 of 3: coverings and choices need not intersect, so dueling
    /// leaders each assemble a covering that missed the other's accepts and
    /// both values get chosen at slot 0. Attacks both new-mint uses at once
    /// (the slotted covering's `manual_proof!` and the per-slot Durable
    /// gate). The search must witness it.
    #[test]
    fn multi_paxos_sub_majority_quorum_violates_agreement() {
        const BROKEN_QUORUM: usize = 1;

        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, BROKEN_QUORUM, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        let mut saw_divergence = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_cluster_size(&learners, LEARNERS)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                lead_send.send(1, 2);
                cmd_send.send(1, 20u32);

                let mut all: Vec<(u64, usize, usize, Option<u32>)> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    all.extend(got);
                }
                if slot_divergence(&all).is_some() {
                    saw_divergence = true;
                }
            });

        assert!(
            saw_divergence,
            "with non-intersecting quorums, the search must find two different values \
             chosen at one slot"
        );
    }

    /// **Progress under the Ω discipline.** A single designated leader (no
    /// duel — that is the oracle's job) with one untargeted acceptor crash:
    /// in EVERY explored execution both decrees are chosen, uniformly
    /// learned, and spliced — no dead state at F = 1 given Ω.
    #[test]
    fn multi_paxos_progress_under_acceptor_crash() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (lead_send, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&learners, LEARNERS)
            .with_crashable_cluster(&acceptors, F)
            .fuzz(async || {
                lead_send.send(0, 1);
                cmd_send.send(0, 10u32);
                cmd_send.send(0, 20u32);

                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert_eq!(
                        splice_of(&got),
                        vec![10, 20],
                        "learner {member} must splice both decrees despite the crash, got {got:?}"
                    );
                }
            });
    }
}
