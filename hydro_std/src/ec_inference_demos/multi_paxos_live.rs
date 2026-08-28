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
        let n_est = est_batch.count();
        let n_timeouts = timeout_batch.count();

        let decided = state
            .zip(batch_max_round)
            .zip(new_cmds)
            .zip(new_completions)
            .zip(n_est)
            .zip(n_timeouts)
            .map(q!(move |(((((mut st, batch_max), cmds), comps), ests), touts): (((((ElectionState<_>, u64), BTreeSet<_>), BTreeSet<_>), usize), usize)| {
                if batch_max > st.max_round {
                    st.max_round = batch_max;
                }
                // Retirement: completed values leave the pending set at
                // once, keeping state proportional to outstanding work.
                for v in comps {
                    if st.pending.remove(&v) {
                        st.completed += 1;
                    }
                }

                let mut campaign = None;
                if touts > 0 {
                    let stalled =
                        !st.pending.is_empty() && st.completed == st.completed_at_last_timeout;
                    if stalled {
                        // Smallest round in my residue class above max_round:
                        // distinct members can never collide (module docs).
                        let me = CLUSTER_SELF_ID.get_raw_id() as u64;
                        let n = num_proposers as u64;
                        let r = (st.max_round / n + 1) * n + me;
                        st.max_round = r;
                        campaign = Some(r);
                    }
                    st.completed_at_last_timeout = st.completed;
                }

                // Releases. On establishment: everything pending (the redo).
                // Otherwise, if this member already holds an epoch:
                // just-arrived commands go straight through — the steady
                // state is phase-2-only, no election per batch (the
                // multi-decree amortization; a release under a stale, fenced
                // epoch is simply lost and comes back through the redo path).
                let release: Vec<_> = if ests > 0 {
                    st.have_epoch = true;
                    st.pending.extend(cmds);
                    st.pending.iter().cloned().collect()
                } else if st.have_epoch {
                    let fresh: Vec<_> = cmds.iter().cloned().collect();
                    st.pending.extend(cmds);
                    fresh
                } else {
                    st.pending.extend(cmds);
                    Vec::new()
                };

                (st, campaign, release)
            }));

        state = decided.clone().map(q!(|(st, _, _)| st));

        let leads = decided
            .clone()
            .filter_map(q!(|(_, campaign, _)| campaign))
            .into_stream();
        let releases = decided
            .map(q!(|(_, _, release)| release))
            .into_stream()
            .flat_map_ordered(q!(|release| release));

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
            state.absorb(SpliceFact::Start { epoch: *epoch, start_slot: *start });
            state.absorb(SpliceFact::Entry { epoch: *epoch, slot: *slot, value: *v });
        }
        state.splice().into_iter().filter_map(|v| *v).collect()
    }

    /// Per-slot agreement across all learned tuples.
    fn slot_divergence(tuples: &[(u64, usize, usize, Option<u32>)]) -> Option<usize> {
        let mut per_slot: BTreeMap<usize, BTreeSet<Option<u32>>> = BTreeMap::new();
        for (_epoch, _start, slot, v) in tuples {
            per_slot.entry(*slot).or_default().insert(*v);
        }
        per_slot.into_iter().find(|(_, vs)| vs.len() > 1).map(|(s, _)| s)
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
            &acceptors, &learners, MAJORITY, N_PROPOSERS, timeouts, commands,
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
            &acceptors, &learners, MAJORITY, N_PROPOSERS, timeouts, commands,
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
            &acceptors, &learners, MAJORITY, N_PROPOSERS, timeouts, commands,
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
            &acceptors, &learners, MAJORITY, N_PROPOSERS, timeouts, commands,
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

