//! A **liveness wrapper** for [`multi_paxos`](super::multi_paxos):
//! in-protocol leader election, deliberately janky, deliberately separate.
//!
//! # Separation contract
//!
//! The safety core (`multi_paxos.rs`) is consumed as-is; the only change it
//! needed was *additive* — publishing its establishment events
//! (`MultiPaxosOutputs::established`), a fact its leader kernel already
//! computed (the wide-interface lesson, ladder doc §3b). This module only
//! *produces* the core's inputs — a `leads` stream the core's safety
//! argument already quantifies over (the dueling-leaders tests explore
//! arbitrary concurrent leads), and the command stream. Nothing here can
//! weaken a safety claim; the worst a broken election can do is stall,
//! which is the FLP-honest failure mode. Election timers are **input
//! streams** (the raft / `broadcast_transcript_consensus` pattern): the
//! simulator drives them as ordinary inputs, a deployment wires
//! `source_interval`.
//!
//! # The election, minimal by construction
//!
//! No heartbeats, no NACKs, no leases. Each member, on an election-timer
//! interrupt, campaigns iff **it has pending work that is not completing**:
//! some value it admitted has not been observed chosen (via the core's
//! `chosen` output), and the completed set did not grow since the previous
//! interrupt. An idle system never elects; a stable leader suppresses
//! elections by making progress, not by heartbeating.
//!
//! # The redo queue: the wrapper owns command admission
//!
//! The core consumes each command exactly once, and work sequenced under an
//! epoch that gets fenced is lost (core contract). The first draft of this
//! wrapper fed commands straight through and the fuzzer promptly found the
//! consequence: a member that re-campaigns while its own accepts are in
//! flight fences *itself*, and the lost commands are never retried — a
//! reachable dead state. So the wrapper holds submitted values and releases
//! `submitted − completed` to the core **on its own establishment events**
//! (`MultiPaxosOutputs::established`, published by the core for exactly
//! this purpose): a freshly established epoch immediately proposes
//! everything still owed. Once an epoch is held, newly arriving commands
//! are released immediately — the steady state is phase-2-only (the
//! multi-decree amortization; a release under a stale, fenced epoch is
//! simply lost and comes back through the redo path). Values may
//! consequently appear at multiple slots (re-released work that was chosen
//! but not yet observed, or resubmitted by a client); per-slot agreement is
//! untouched, and collapsing duplicates is the state machine's job, as in
//! any redo log.
//!
//! # Structurally distinct rounds (a contract, discharged)
//!
//! Campaign rounds are broadcast among proposers, and a member's next round
//! is the smallest `r ≡ my_index (mod num_proposers)` exceeding every round
//! it has seen. Distinct members occupy disjoint residue classes, so the
//! core's **globally-distinct-rounds caller contract holds by construction**
//! for every program that drives the core through this shell — the E0 cell
//! in the trust accounting, upgraded to structure.
//!
//! # Honest ledger
//!
//! - Commands sequenced under an epoch that is fenced mid-flight are lost
//!   (core contract, unchanged). **Resubmission stays with the caller**;
//!   the progress test's driver resubmits round-robin, crash-agnostically,
//!   exactly like the Raft progress test. Duplicate suppression for
//!   resubmitted commands is the state machine's job (rids), not this
//!   layer's.
//! - A member may campaign while its previous campaign's covering is still
//!   in flight (timer ticks are not gated on phase completion). Rounds stay
//!   distinct and the core's establishment folds take the max ballot, so
//!   this is livelock noise, not a safety issue.
//! - Progress remains Ω-conditional in the honest sense: the timer inputs
//!   *are* the failure detector, and the tests drive them fairly.

use std::collections::BTreeSet;
use std::hash::Hash;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::multi_paxos::{MultiPaxosOutputs, multi_paxos};

/// Per-member election-kernel state. Public because staged (`q!`) code is
/// compiled outside this module in deploy mode; not part of the API.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ElectionState<V> {
    /// Highest campaign round seen anywhere (round allocation).
    pub max_round: u64,
    /// Admitted values not yet observed chosen — the redo queue. Bounded by
    /// outstanding work: completed values are retired immediately, so
    /// per-tick state cost does not grow with history.
    pub pending: BTreeSet<V>,
    /// Completions ever observed (stall detection).
    pub completed: u64,
    /// `completed` as of the last timer interrupt.
    pub completed_at_last_timeout: u64,
    /// Has this member ever established an epoch? (Gates steady-state
    /// releases.)
    pub have_epoch: bool,
}

impl<V> Default for ElectionState<V> {
    // Manual impl: `derive` would demand `V: Default` for no reason.
    fn default() -> Self {
        ElectionState {
            max_round: 0,
            pending: BTreeSet::new(),
            completed: 0,
            completed_at_last_timeout: 0,
            have_epoch: false,
        }
    }
}

/// [`multi_paxos`] with in-protocol leader election. `election_timeouts`
/// carries member-local timer interrupts; `num_proposers` must match the
/// proposer cluster's deploy-time size (used for round residue classes).
/// `V: Ord` because the redo queue is a set of values (module docs) — which
/// is also why `commands` may arrive with any ordering.
pub fn multi_paxos_live<'a, V, P, A, LRN, O>(
    acceptors: &Cluster<'a, A>,
    learners: &Cluster<'a, LRN>,
    majority: usize,
    num_proposers: usize,
    election_timeouts: Stream<(), Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    commands: Stream<V, Cluster<'a, P>, Unbounded, O, ExactlyOnce>,
) -> MultiPaxosOutputs<'a, P, LRN, V>
where
    V: Clone + Eq + Ord + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
    LRN: 'a,
    O: hydro_lang::live_collections::stream::Ordering,
{
    let proposers = commands.location().clone();

    // Three cycles close through the election kernel: campaign rounds
    // circulating among proposers (round allocation), completions from the
    // core's `chosen` output (stall detection / redo-queue retirement), and
    // establishment events from the core (redo-queue release timing).
    let (campaigns_handle, campaigns_seen) =
        proposers.forward_ref::<Stream<u64, _, Unbounded, NoOrder>>();
    let (completions_handle, completions) =
        proposers.forward_ref::<Stream<V, _, Unbounded, NoOrder>>();
    let (established_handle, established) =
        proposers.forward_ref::<Stream<(u64, usize), _, Unbounded, NoOrder>>();

    // ---- The election kernel: one small slice per member --------------------
    // Liveness-only state; no safety claim rides on any of it. Owns command
    // admission: the core sees a value only when this member has an
    // established epoch to sequence it under (the redo queue, module docs).
    let (leads, releases) = sliced! {
        let timeout_batch = use::batch(election_timeouts, nondet!(
            /// Timer arrival timing IS the failure detector: it decides only
            /// WHEN campaigns fire, never what may be chosen — the core's
            /// safety quantifies over arbitrary lead streams.
        ));
        let campaign_batch = use::batch(campaigns_seen, nondet!(
            /// A stale view of others' rounds can only pick a round that is
            /// too low; such a campaign is fenced by acceptors and retried
            /// at the next interrupt. Liveness noise only.
        ));
        let completion_batch = use::batch(completions, nondet!(
            /// A stale completed set can only trigger a spurious campaign or
            /// a duplicate release, both safe (module docs).
        ));
        let est_batch = use::batch(established, nondet!(
            /// Release timing: any establishment of mine is a valid moment
            /// to release owed work; a release fenced by a still-newer
            /// ballot is re-released at the next establishment.
        ));
        let cmd_batch = use::batch(commands, nondet!(
            /// Admission timing: which tick a value joins the redo queue.
            /// Values are released only under an established epoch, so
            /// batching only delays.
        ));

        // The kernel's state; see [`ElectionState`]'s field docs.
        let mut state = use::state(|l| l.singleton(q!(ElectionState::default())));

        let batch_max_round = campaign_batch.fold(
            q!(|| 0u64),
            q!(|acc, r| {
                if r > *acc {
                    *acc = r;
                }
            }, commutative = manual_proof!(/** max is commutative */)),
        );
        // Admission order is irrelevant: the redo queue is a set.
        let new_cmds = cmd_batch.fold(
            q!(|| BTreeSet::new()),
            q!(|acc: &mut BTreeSet<_>, v| {
                acc.insert(v);
            }, commutative = manual_proof!(/** set insert is commutative */)),
        );
        let new_completions = completion_batch.fold(
            q!(|| BTreeSet::new()),
            q!(|acc: &mut BTreeSet<_>, v| {
                acc.insert(v);
            }, commutative = manual_proof!(/** set insert is commutative */)),
        );
        let tick = timeout_batch.location().clone();
        let n_est = est_batch.count();
        let n_timeouts = timeout_batch.count();

        let state_ref = state.by_mut();
        let batch_max_round_ref = batch_max_round.by_ref();
        let new_cmds_ref = new_cmds.by_ref();
        let new_completions_ref = new_completions.by_ref();
        let n_est_ref = n_est.by_ref();
        let n_timeouts_ref = n_timeouts.by_ref();

        let releases: Stream<V, _, Bounded> = tick.source_iter(q!(Vec::new()));
        let releases_ref = releases.by_mut();

        // One in-place transition per tick. ElectionState stays bounded by
        // outstanding work and is never cloned through a graph tee.
        let leads = tick
            .singleton(q!(()))
            .into_stream()
            .filter_map(q!(move |_| {
                let st = &mut *state_ref;
                if *batch_max_round_ref > st.max_round {
                    st.max_round = *batch_max_round_ref;
                }
                for v in new_completions_ref.iter() {
                    if st.pending.remove(v) {
                        st.completed += 1;
                    }
                }

                let mut campaign = None;
                if *n_timeouts_ref > 0 {
                    let stalled =
                        !st.pending.is_empty() && st.completed == st.completed_at_last_timeout;
                    if stalled {
                        let me = CLUSTER_SELF_ID.get_raw_id() as u64;
                        let n = num_proposers as u64;
                        let r = (st.max_round / n + 1) * n + me;
                        st.max_round = r;
                        campaign = Some(r);
                    }
                    st.completed_at_last_timeout = st.completed;
                }

                if *n_est_ref > 0 {
                    st.have_epoch = true;
                    st.pending.extend(new_cmds_ref.iter().cloned());
                    releases_ref.extend(st.pending.iter().cloned());
                } else if st.have_epoch {
                    st.pending.extend(new_cmds_ref.iter().cloned());
                    releases_ref.extend(new_cmds_ref.iter().cloned());
                } else {
                    st.pending.extend(new_cmds_ref.iter().cloned());
                }

                campaign
            }));

        (leads, releases)
    };

    // Campaigns circulate to every proposer (round allocation)...
    campaigns_handle.complete(
        leads
            .clone()
            .broadcast_closed(&proposers, TCP.fail_stop().bincode())
            .values()
            .weaken_consistency(),
    );

    // ...and the untouched safety core runs on the kernel's outputs: leads
    // as its Ω input, redo-queue releases as its command stream.
    let outputs = multi_paxos(acceptors, learners, majority, leads, releases);

    // Close the observation cycles from the core's public outputs. Both go
    // through a point-to-point network hop TO SELF: logically these edges
    // are local, but a direct local edge would form a within-tick dataflow
    // cycle (kernel → core → chosen/established → kernel) that the deploy
    // partitioner rejects (and `defer_tick` cannot break it — lazy ticks
    // strand deferred items at quiescence). The self-hop breaks the cycle
    // at a real async boundary, and both edges are off the request critical
    // path: they drive elections and redo-queue retirement, never client
    // responses.
    completions_handle.complete(
        outputs
            .chosen
            .clone()
            .filter_map(q!(|(_epoch, _start, _slot, v)| v))
            .map(q!(move |v| (CLUSTER_SELF_ID.clone(), v)))
            .into_keyed()
            .demux(&proposers, TCP.fail_stop().bincode())
            .values(),
    );
    established_handle.complete(
        outputs
            .established
            .clone()
            .map(q!(move |e| (CLUSTER_SELF_ID.clone(), e)))
            .into_keyed()
            .demux(&proposers, TCP.fail_stop().bincode())
            .values(),
    );

    outputs
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::super::epoch_splice::{SpliceFact, SpliceState};
    use super::multi_paxos_live;

    const N_ACCEPTORS: usize = 3;
    const MAJORITY: usize = 2;
    const N_PROPOSERS: usize = 2;
    const LEARNERS: usize = 2;
    const F: usize = 1;

    /// Splice a learner's learned tuples at the harness (same pattern as the
    /// core's tests), values only, no-ops skipped.
    fn splice_of(tuples: &[(u64, usize, usize, Option<u32>)]) -> Vec<u32> {
        let mut state = SpliceState::new();
        for (epoch, start, slot, v) in tuples {
            state.absorb(SpliceFact::Start {
                epoch: *epoch,
                start_slot: *start,
            });
            state.absorb(SpliceFact::Entry {
                epoch: *epoch,
                slot: *slot,
                value: *v,
            });
        }
        state.splice().into_iter().filter_map(|v| *v).collect()
    }

    /// Per-slot agreement across all learned tuples.
    fn slot_divergence(tuples: &[(u64, usize, usize, Option<u32>)]) -> Option<usize> {
        let mut per_slot: BTreeMap<usize, BTreeSet<Option<u32>>> = BTreeMap::new();
        for (_epoch, _start, slot, v) in tuples {
            per_slot.entry(*slot).or_default().insert(*v);
        }
        per_slot
            .into_iter()
            .find(|(_, vs)| vs.len() > 1)
            .map(|(s, _)| s)
    }

    /// A member with pending work elects itself on a timer interrupt and
    /// commits — no driver-supplied rounds anywhere. The quiesce barrier
    /// makes it deterministic: both commands are counted as submitted before
    /// the single interrupt fires, so one campaign sequences both.
    #[test]
    fn live_stalled_member_elects_itself_and_commits() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (timeout_send, timeouts) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(
            &acceptors,
            &learners,
            MAJORITY,
            N_PROPOSERS,
            timeouts,
            commands,
        );
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N_ACCEPTORS)
            .with_cluster_size(&proposers, N_PROPOSERS)
            .with_cluster_size(&learners, LEARNERS)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                cmd_send.send(0, 10u32);
                cmd_send.send(0, 20u32);
                hydro_lang::sim::quiesce().await;
                timeout_send.send(0, ());

                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert_eq!(
                        splice_of(&got),
                        vec![10, 20],
                        "learner {member}: self-election must commit both commands, got {got:?}"
                    );
                }
            });
    }

    /// **The test this layer exists for: an untargeted PROPOSER crash cannot
    /// block progress.** The driver is crash-agnostic (the Raft progress
    /// test's discipline): each round it resubmits the command to the next
    /// member round-robin and fires everyone's election timer — it never
    /// knows who died or who leads. In EVERY explored execution some live
    /// member elects itself and the value is chosen, uniformly learned, and
    /// spliced. This is the portfolio row's progress cell with the Ω oracle
    /// now *inside* the protocol (timer inputs are the only driver privilege
    /// left).
    #[test]
    fn live_proposer_crash_cannot_block_progress() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (timeout_send, timeouts) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(
            &acceptors,
            &learners,
            MAJORITY,
            N_PROPOSERS,
            timeouts,
            commands,
        );
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N_ACCEPTORS)
            .with_cluster_size(&proposers, N_PROPOSERS)
            .with_cluster_size(&learners, LEARNERS)
            .with_crashable_cluster(&proposers, F)
            .fuzz(async || {
                // Crash-agnostic driver: resubmit round-robin, fire all
                // timers, barrier, repeat. At most one proposer is dead, so
                // at least two rounds hit a live member.
                for round in 0..4u32 {
                    cmd_send.send(round % N_PROPOSERS as u32, 10u32);
                    for member in 0..N_PROPOSERS as u32 {
                        timeout_send.send(member, ());
                    }
                    hydro_lang::sim::quiesce().await;
                }

                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert!(
                        got.iter().any(|(_, _, _, v)| *v == Some(10)),
                        "learner {member}: the value must survive any single proposer crash, \
                         got {got:?}"
                    );
                    assert!(
                        slot_divergence(&got).is_none(),
                        "learner {member}: resubmission must never break per-slot agreement"
                    );
                }
            });
    }

    /// **Elections cannot break safety.** Both members submit and both get
    /// timer interrupts with no barriers — campaigns, fencing, recovery
    /// re-proposals, and an untargeted acceptor crash all race. Per-slot
    /// agreement and learner convergence hold in every explored execution
    /// (the "liveness layer only narrows the adversary" claim, mechanical).
    #[test]
    fn live_concurrent_elections_preserve_agreement() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (timeout_send, timeouts) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(
            &acceptors,
            &learners,
            MAJORITY,
            N_PROPOSERS,
            timeouts,
            commands,
        );
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N_ACCEPTORS)
            .with_cluster_size(&proposers, N_PROPOSERS)
            .with_cluster_size(&learners, LEARNERS)
            .with_crashable_cluster(&acceptors, F)
            .fuzz(async || {
                cmd_send.send(0, 10u32);
                cmd_send.send(1, 20u32);
                for member in 0..N_PROPOSERS as u32 {
                    timeout_send.send(member, ());
                    timeout_send.send(member, ());
                }

                let mut per_learner: Vec<Vec<(u64, usize, usize, Option<u32>)>> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    per_learner.push(learned_recv.collect_sorted(member).await);
                }
                let all: Vec<_> = per_learner.iter().flatten().copied().collect();
                assert!(
                    slot_divergence(&all).is_none(),
                    "AGREEMENT VIOLATED under concurrent self-elections: {all:?}"
                );
                assert_eq!(
                    per_learner[0], per_learner[1],
                    "learners must converge at quiescence"
                );
            });
    }

    /// **Colocated deployment: every node is proposer + acceptor + learner.**
    /// The Maelstrom/bench topology (one cluster of n nodes, all roles on
    /// every node), pinned at the sim level: same cluster passed as all
    /// three role arguments, self-election still commits, learners still
    /// converge.
    #[test]
    fn live_colocated_smoke() {
        let mut flow = FlowBuilder::new();
        let nodes = flow.cluster::<()>();

        let (timeout_send, timeouts) = nodes.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = nodes.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(&nodes, &nodes, MAJORITY, N_ACCEPTORS, timeouts, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&nodes, N_ACCEPTORS)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                cmd_send.send(0, 10u32);
                cmd_send.send(0, 20u32);
                hydro_lang::sim::quiesce().await;
                timeout_send.send(0, ());

                for member in 0..N_ACCEPTORS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert_eq!(
                        splice_of(&got),
                        vec![10, 20],
                        "colocated node {member} must converge, got {got:?}"
                    );
                }
            });
    }

    /// **Raft test parity, the headline: `any_single_crash_cannot_block_
    /// progress`, identical topology.** One cluster of 3, every node all
    /// three roles, untargeted crash budget F = 1 over the WHOLE node (its
    /// proposer, acceptor, and learner die together — exactly what a real
    /// node crash does), crash-agnostic round-robin driver. In every
    /// explored execution, agreement holds and at least N − F nodes learn
    /// the value. Same claim, fault model, and driver discipline as Raft's
    /// `any_single_crash_cannot_block_progress`.
    #[test]
    fn live_colocated_any_single_crash_cannot_block_progress() {
        const N: usize = 3;

        let mut flow = FlowBuilder::new();
        let nodes = flow.cluster::<()>();

        let (timeout_send, timeouts) = nodes.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = nodes.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(&nodes, &nodes, MAJORITY, N, timeouts, commands);
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&nodes, N)
            .with_crashable_cluster(&nodes, F)
            .fuzz(async || {
                for round in 0..4u32 {
                    cmd_send.send(round % N as u32, 10u32);
                    for member in 0..N as u32 {
                        timeout_send.send(member, ());
                    }
                    hydro_lang::sim::quiesce().await;
                }

                // Crash-agnostic assertion: a crashed node's learner output
                // simply ends; agreement must hold everywhere, and at least
                // N − F nodes must have learned the value.
                let mut deliverers = 0usize;
                for member in 0..N as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    assert!(
                        slot_divergence(&got).is_none(),
                        "node {member}: per-slot agreement must hold under the crash"
                    );
                    if got.iter().any(|(_, _, _, v)| *v == Some(10)) {
                        deliverers += 1;
                    }
                }
                assert!(
                    deliverers >= N - F,
                    "at least {} live nodes must learn the value; only {deliverers} did",
                    N - F
                );
            });
    }

    /// **Safety beyond the crash budget** (raft-parity for
    /// `leader_without_quorum_commits_nothing`, strengthened): with up to
    /// TWO of three acceptors crashed — beyond the design budget — progress
    /// may legitimately die, but per-slot agreement must still hold in
    /// every explored execution. Only progress needs a majority; safety
    /// needs nothing.
    #[test]
    fn live_safety_holds_beyond_crash_budget() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();

        let (timeout_send, timeouts) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outs = multi_paxos_live(
            &acceptors,
            &learners,
            MAJORITY,
            N_PROPOSERS,
            timeouts,
            commands,
        );
        let learned_recv = outs.learned.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N_ACCEPTORS)
            .with_cluster_size(&proposers, N_PROPOSERS)
            .with_cluster_size(&learners, LEARNERS)
            .with_crashable_cluster(&acceptors, 2)
            .fuzz(async || {
                cmd_send.send(0, 10u32);
                cmd_send.send(1, 20u32);
                for member in 0..N_PROPOSERS as u32 {
                    timeout_send.send(member, ());
                    timeout_send.send(member, ());
                }

                let mut all: Vec<(u64, usize, usize, Option<u32>)> = Vec::new();
                for member in 0..LEARNERS as u32 {
                    let got: Vec<(u64, usize, usize, Option<u32>)> =
                        learned_recv.collect_sorted(member).await;
                    all.extend(got);
                }
                assert!(
                    slot_divergence(&all).is_none(),
                    "agreement must survive crashes beyond the budget: {all:?}"
                );
            });
    }
}

/// A third program for the amplification design
/// (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`), not written for it and
/// not a leader-election theorem: the election kernel's *redo queue*. On a timer interrupt with
/// pending work and no completion since the previous interrupt, the member campaigns, and on
/// establishment it releases its **entire** pending set to the core again. So a delay on the
/// acceptors' acks past the timer period costs a campaign plus one more proposal of every
/// pending command to every acceptor, and the extra work per stall is proportional to the
/// backlog, not to the delay. Nothing in `multi_paxos_live` or `multi_paxos` is changed here.
#[cfg(test)]
mod amplification_tests {
    use std::collections::BTreeMap;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::edge_counts::EdgeCounts;
    use hydro_lang::sim::hold_schedule::{EdgePolicy, HoldScheduleDriver};
    use hydro_lang::sim::lineage::Lineage;
    use hydro_lang::sim::quiesce;

    use super::super::quorum::Ts;
    use super::multi_paxos_live;

    const N_ACCEPTORS: usize = 3;
    const MAJORITY: usize = 2;
    const N_PROPOSERS: usize = 2;
    const LEARNERS: usize = 1;
    /// Phase 2 runs for `TICKS` kernel ticks: one command per tick (the metered clock, which is
    /// also what lets the timer hook be paced: the scheduler forces the last undecided hook of a
    /// tick to release when nothing else did) and a timer interrupt every `PERIOD` ticks.
    const TICKS: u64 = 200;
    const PERIOD: u64 = 10;

    /// 1-based line of the unique line of `file` (under `src/`) containing `needle`; hook keys
    /// carry the `sliced!` location.
    fn line_of(file: &str, needle: &str) -> usize {
        let source = std::fs::read_to_string(format!("{}/src/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut hits = source.lines().enumerate().filter(|(_, l)| l.contains(needle)).map(|(i, _)| i + 1);
        let line = hits.next().unwrap_or_else(|| panic!("no line of {file} contains {needle:?}"));
        assert!(hits.next().is_none(), "more than one line of {file} contains {needle:?}");
        line
    }

    fn slice_pred(file: &'static str, needle: &'static str, type_part: &'static str) -> impl Fn(&str) -> bool + Clone + 'static {
        let tag = format!("{}:{}:", file.rsplit('/').next().unwrap(), line_of(file, &format!("{needle}!")));
        move |key: &str| key.contains(&tag) && key.contains(type_part)
    }

    const KERNEL: (&str, &str) = ("ec_inference_demos/multi_paxos_live.rs", "let (leads, releases) = sliced");

    /// The kernel's timer batch: the `()` hook of the election kernel's slice.
    fn kernel_timer() -> impl Fn(&str) -> bool + Clone + 'static {
        slice_pred(KERNEL.0, KERNEL.1, " <()>")
    }
    /// The acceptors' acks on their way into the proposer's quorum mint (`quorum`'s batch).
    fn acks() -> impl Fn(&str) -> bool + Clone + 'static {
        slice_pred("ec_inference_demos/quorum.rs", "let certified = sliced", "MemberId")
    }
    /// Phase-2 proposals arriving at the acceptors (the work edge).
    fn accepts() -> impl Fn(&str) -> bool + Clone + 'static {
        slice_pred("ec_inference_demos/multi_paxos.rs", "let acceptor_out = sliced", "usize")
    }

    type Accept = (MemberId<()>, (Ts, usize, usize, Option<u32>));

    struct Outcome {
        counts: EdgeCounts,
        lineage: Lineage,
        /// Learned `(epoch, start, slot, value)` at learner 0.
        learned: Vec<(u64, usize, usize, Option<u32>)>,
        /// Establishments at the active proposer.
        established: Vec<(u64, usize)>,
        /// The kernel's command and completion hooks (both `u32`), as found by the control run.
        edges: Option<(String, String)>,
    }

    /// Phase 1: one command and one interrupt establish an epoch and commit. Phase 2: `TICKS`
    /// commands and `TICKS / PERIOD` interrupts sent up front, the commands metered one per
    /// kernel tick and the interrupts paced one per `PERIOD`; the kernel's completion hook (the
    /// self-hop carrying this proposer's chosen decrees back to its kernel) is scheduled by
    /// `policy`. That hook shares the tick with the metered command clock, which is what makes
    /// a hold expressible there (the acks hook at the quorum mint is alone in its tick and is
    /// forced to release whenever the tick runs; measured: every policy on it gave the prompt
    /// numbers). Only proposer 0 gets commands and interrupts, so there is no duel: whatever
    /// campaigns is the redo loop alone.
    /// Which edge the policy applies to.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Move {
        /// The kernel's completion hook, which shares a tick with the metered command clock.
        Completions,
        /// The acceptors' acks at the quorum mint, alone in their tick: only expressible with
        /// `with_empty_ticks_allowed`, which `run` then switches on.
        Acks,
    }

    fn run(policy: EdgePolicy, edges: Option<(String, String)>) -> Outcome {
        run_move(policy, edges, Move::Completions)
    }

    fn run_move(policy: EdgePolicy, edges: Option<(String, String)>, held: Move) -> Outcome {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();
        let (timeout_send, timeouts) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (cmd_send, cmds) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let outs = multi_paxos_live(&acceptors, &learners, MAJORITY, N_PROPOSERS, timeouts, cmds);
        let learned_recv = outs.learned.sim_cluster_output();
        let est_recv = outs.established.sim_cluster_output();

        let mut driver = HoldScheduleDriver::new();
        if let Some((cmd_edge, completion_edge)) = edges.clone() {
            driver = driver
                .meter(move |key: &str| key == cmd_edge)
                .with_policy(kernel_timer(), EdgePolicy::Periodic { period: PERIOD, count: Some(1) });
            driver = match held {
                Move::Completions => driver.with_policy(move |key: &str| key == completion_edge, policy),
                Move::Acks => driver.with_policy(acks(), policy),
            };
        }
        let mut learned = Vec::new();
        let mut established = Vec::new();
        let (learned_ref, est_ref) = (&mut learned, &mut established);
        let phase2 = edges.is_some();
        let sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N_ACCEPTORS)
            .with_cluster_size(&proposers, N_PROPOSERS)
            .with_cluster_size(&learners, LEARNERS);
        let run = || sim.run_traced(driver, async move || {
                cmd_send.send(0, 0u32);
                quiesce().await;
                timeout_send.send(0, ());
                quiesce().await;
                if phase2 {
                    for c in 1..=TICKS as u32 {
                        cmd_send.send(0, c);
                    }
                    for _ in 0..TICKS / PERIOD {
                        timeout_send.send(0, ());
                    }
                    quiesce().await;
                }
                learned_ref.extend(learned_recv.collect_sorted::<Vec<_>>(0).await);
                est_ref.extend(est_recv.collect_sorted::<Vec<_>>(0).await);
            });
        let (counts, lineage) = match held {
            Move::Completions => run(),
            Move::Acks => hydro_lang::sim::edge_counts::with_empty_ticks_allowed(run),
        };
        Outcome { counts, lineage, learned, established, edges }
    }

    /// The control run (phase 1 only) settles which of the kernel's two `u32` hooks is the
    /// command hook and which the completion hook: the command was sent and quiesced before the
    /// interrupt, so the command hook's release with a record comes first in the log; the
    /// completion is delivered after the establishment. Checked again by the phase-2 prompt run
    /// (the metered hook releases `TICKS + 1` records one per tick).
    fn control() -> (Outcome, (String, String)) {
        let control = run(EdgePolicy::Prompt, None);
        let kernel_u32 = slice_pred(KERNEL.0, KERNEL.1, "<u32>");
        let mut first_release: Vec<(u64, String)> = vec![];
        for release in control.lineage.releases_on(kernel_u32) {
            if !release.released.is_empty() && !first_release.iter().any(|(_, k)| *k == release.edge) {
                first_release.push((release.serial, release.edge.clone()));
            }
        }
        first_release.sort();
        assert_eq!(first_release.len(), 2, "the kernel's two u32 hooks: {first_release:?}");
        let completion = first_release.pop().unwrap().1;
        let command = first_release.pop().unwrap().1;
        (control, (command, completion))
    }

    impl Outcome {
        /// Accept records at the acceptors carrying each command.
        fn accepts_per_command(&self) -> BTreeMap<u32, u64> {
            self.lineage.per_goal::<Accept, u32>(accepts(), |(_, (_, _, _, v))| v.into_iter().collect())
        }
        /// Chosen slots per command at learner 0 (the goal edge; more than one is a duplicate
        /// decree for the same command).
        fn chosen_per_command(&self) -> BTreeMap<u32, u64> {
            let mut m = BTreeMap::new();
            for (_, _, _, v) in &self.learned {
                if let Some(v) = v {
                    *m.entry(*v).or_default() += 1;
                }
            }
            m
        }
        fn campaigns(&self) -> u64 {
            self.established.len() as u64 - 1
        }
        fn print(&self, title: &str) {
            println!("== {title}");
            for (key, e) in &self.counts.0 {
                println!("  [{key}] records={} decisions={}", e.records, e.decisions);
            }
            println!(
                "  accepts {}, acks {}, campaigns after phase 1 {}, learned slots {}, accepts per command {:?}, chosen per command {:?}",
                self.counts.records(accepts()),
                self.counts.records(acks()),
                self.campaigns(),
                self.learned.len(),
                histogram(self.accepts_per_command().values().copied()),
                histogram(self.chosen_per_command().values().copied())
            );
        }
    }

    fn histogram(values: impl Iterator<Item = u64>) -> BTreeMap<u64, usize> {
        let mut h = BTreeMap::new();
        for v in values {
            *h.entry(v).or_default() += 1;
        }
        h
    }

    /// Hand computation. Prompt: after phase 1 the epoch exists; each command is released once,
    /// accepted once per acceptor (`N_ACCEPTORS` accept records per command) and chosen once; a
    /// command completes `l` kernel ticks after release, and as long as `l < PERIOD` every
    /// interrupt finds a completion since the previous one: no campaign. Completions held a
    /// constant `d` on their way to the kernel: the kernel sees them `l + d` after release, so only the interrupts at `k * PERIOD <
    /// l + d` (k >= 1) find no completion since the last one; each campaigns and re-releases
    /// every pending command (all commands released so far, ~`k * PERIOD` of them) to every
    /// acceptor. After that a completion arrives every tick and no interrupt stalls again: the
    /// campaigns are `ceil((l + d) / PERIOD) - 1`, a transient, bounded by `d`. Completions
    /// released in bursts every `d` ticks (`Periodic`): an interrupt stalls iff no burst fell in its period,
    /// i.e. never for `d < PERIOD`, and for `d > PERIOD` in about a `1 - PERIOD / d` fraction of
    /// the periods for the whole run — sustained, growing with the run, and each stall
    /// re-proposes every pending command, whose number grows with `d`. Every re-proposal that is
    /// accepted is chosen at a new slot: duplicate decrees for the same command.
    #[test]
    fn redo_queue_re_proposes_the_whole_backlog_per_stall() {
        let (control, edges) = control();
        control.print("multi_paxos_live, phase 1 only (control)");
        let prompt = run(EdgePolicy::Prompt, Some(edges.clone()));
        prompt.print(&format!("multi_paxos_live, {TICKS} commands one per tick, timer every {PERIOD}, prompt"));
        assert_eq!(prompt.counts.records(|k| k == edges.0), TICKS + 1, "the metered hook is the command hook");
        assert_eq!(prompt.counts.edge(|k| k == edges.0).nonempty_decisions, TICKS + 1, "one command per kernel tick");
        let prompt_accepts = prompt.accepts_per_command();
        assert_eq!(prompt_accepts.len() as u64, TICKS + 1, "every command accepted somewhere");
        assert!(prompt_accepts.values().all(|c| *c == N_ACCEPTORS as u64), "{:?}", histogram(prompt_accepts.values().copied()));
        assert!(prompt.chosen_per_command().values().all(|c| *c == 1));
        assert_eq!(prompt.campaigns(), 0, "no stall under prompt delivery");
        let prompt_work = prompt.counts.records(accepts());

        let mut rows = vec![];
        for d in [1u64, 2, 5, 8, 10, 12, 15, 20, 30, 50] {
            let held = run(EdgePolicy::Hold(d), Some(edges.clone()));
            held.print(&format!("multi_paxos_live, completions held d = {d}"));
            let burst = run(EdgePolicy::Periodic { period: d, count: None }, Some(edges.clone()));
            burst.print(&format!("multi_paxos_live, completions in bursts every {d}"));
            rows.push((d, held, burst));
        }
        println!("== summary (multi_paxos_live, {TICKS} commands, timer every {PERIOD}, prompt: {prompt_work} accepts): d / constant: accepts, campaigns, accepts per command, chosen per command / bursty: same");
        for (d, held, burst) in &rows {
            println!(
                "  d = {d:>2}: constant {} / {} / {:?} / {:?}   bursty {} / {} / {:?} / {:?}",
                held.counts.records(accepts()),
                held.campaigns(),
                histogram(held.accepts_per_command().values().copied()),
                histogram(held.chosen_per_command().values().copied()),
                burst.counts.records(accepts()),
                burst.campaigns(),
                histogram(burst.accepts_per_command().values().copied()),
                histogram(burst.chosen_per_command().values().copied()),
            );
        }
        let mut failures = vec![];
        let periods = TICKS / PERIOD;
        for (d, held, burst) in &rows {
            // Constant delay: a transient of at most ceil((l + d) / PERIOD) campaigns with l < PERIOD.
            let bound = (d + PERIOD - 1) / PERIOD + 1;
            if held.campaigns() > bound {
                failures.push(format!("constant d = {d}: {} campaigns, hand-computed at most {bound}", held.campaigns()));
            }
            // Bursty: none below the period; sustained above it. At d = PERIOD exactly, one
            // campaign was measured where the hand computation said none (a single period
            // straddled a burst boundary: the phase of the bursts against the timer); recorded in
            // the design doc, bounded here.
            if *d < PERIOD && burst.campaigns() != 0 {
                failures.push(format!("bursty d = {d}: {} campaigns, hand-computed 0", burst.campaigns()));
            }
            if *d == PERIOD && burst.campaigns() > 1 {
                failures.push(format!("bursty d = {d}: {} campaigns, hand-computed 0 (1 measured, the boundary case)", burst.campaigns()));
            }
            if *d > PERIOD {
                let expected = periods - periods * PERIOD / d;
                if burst.campaigns() + 2 < expected {
                    failures.push(format!("bursty d = {d}: {} campaigns, hand-computed about {expected}", burst.campaigns()));
                }
                // Sustained (bursty) against transient (constant): compared on campaigns; the
                // accept count crosses over only from d = 15 (at d = 12 four small re-releases
                // did less work than one large one: 639 vs 645).
                if burst.campaigns() <= held.campaigns() {
                    failures.push(format!("bursty d = {d}: {} campaigns not above constant delay's {}", burst.campaigns(), held.campaigns()));
                }
            }
            // Every command's accept count is a multiple of N_ACCEPTORS: whole re-proposals.
            for o in [held, burst] {
                if o.accepts_per_command().values().any(|c| c % N_ACCEPTORS as u64 != 0) {
                    failures.push(format!("d = {d}: accepts per command not whole re-proposals: {:?}", histogram(o.accepts_per_command().values().copied())));
                }
            }
        }
        // The natural move: hold the acceptors' acks. That hook is alone in its tick, so it needs
        // the scheduler option that lets a tick run empty (`with_empty_ticks_allowed`); without it
        // every policy on this edge measured the prompt numbers. Hand computation: the kernel
        // sees completions late by the same `d`, so the same staircase as above, campaigns within
        // one of the completion-hold row at the same `d`; the difference is where the delay sits.
        println!("== the acks move, ticks allowed to run empty: d / constant: accepts, campaigns, decrees per command / bursty: same");
        let mut ack_rows = vec![];
        for d in [5u64, 12, 20, 50] {
            let held = run_move(EdgePolicy::Hold(d), Some(edges.clone()), Move::Acks);
            let burst = run_move(EdgePolicy::Periodic { period: d, count: None }, Some(edges.clone()), Move::Acks);
            println!(
                "  d = {d:>2}: constant {} / {} / {:?}   bursty {} / {} / {:?}",
                held.counts.records(accepts()),
                held.campaigns(),
                histogram(held.chosen_per_command().values().copied()),
                burst.counts.records(accepts()),
                burst.campaigns(),
                histogram(burst.chosen_per_command().values().copied()),
            );
            ack_rows.push((d, held, burst));
        }
        for (d, held, burst) in &ack_rows {
            let (_, held_c, burst_c) = rows.iter().find(|(dd, ..)| dd == d).map(|(dd, h, b)| (*dd, h.campaigns(), b.campaigns())).unwrap();
            if held.campaigns().abs_diff(held_c) > 1 {
                failures.push(format!("acks move, constant d = {d}: {} campaigns vs {held_c} holding completions", held.campaigns()));
            }
            if burst.campaigns().abs_diff(burst_c) > 1 {
                failures.push(format!("acks move, bursty d = {d}: {} campaigns vs {burst_c} holding completions", burst.campaigns()));
            }
            if *d < PERIOD && (held.counts.records(accepts()) != prompt_work || burst.counts.records(accepts()) != prompt_work) {
                failures.push(format!("acks move, d = {d}: work changed below the threshold"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
