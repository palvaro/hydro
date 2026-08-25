//! State-based CRDT gossip with EC inferred from transport.
//!
//! Same trick as `reliable_broadcast_closed`: broadcast_closed earns EC,
//! forward_ref lives on that EC location, the cycle closes cleanly.
//! The difference: we re-broadcast the *folded state*, not raw elements.
//!
//! The gossip pump (when to re-offer state to peers) is an explicit tick-stream
//! **parameter** rather than an internal timer: the simulator's tokio runtime
//! has no time driver, so `sample_every`/`source_interval` cannot run under it —
//! the same reason the Raft demo takes its election and heartbeat timers as
//! inputs. Production callers pass `cluster.source_interval(...)`; sim tests
//! drive the pump explicitly, which also makes pump timing a controlled part of
//! the explored schedule.
//!
//! The state is a `BTreeSet` (not `HashSet`): the gossiped payload must itself
//! be `Hash`, because the live variant's fan-out compiles to a top-level
//! `join_multiset -> multiset_delta()` whose delta map is keyed by the item.
//! `HashSet` is not `Hash`; `BTreeSet` is.

use std::collections::BTreeSet;
use std::hash::Hash;

use hydro_lang::live_collections::stream::{AtLeastOnce, ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// G-Set CRDT via state-based gossip. EC fully inferred, no manual_proof on consistency.
pub fn g_set_gossip<
    'a,
    T: Clone + Ord + Hash + Serialize + DeserializeOwned + 'static,
    L2: 'a,
>(
    cluster: &Cluster<'a, L2>,
    local_updates: Stream<T, Cluster<'a, L2>, Unbounded, NoOrder>,
    gossip_ticks: Stream<(), Cluster<'a, L2>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Singleton<BTreeSet<T>, Cluster<'a, L2, EventualConsistency>, Unbounded>
{
    // Step 1: Broadcast local updates to all peers. EC inferred.
    let initial = local_updates
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
        .values();

    // Soundness of EC on this cycle:
    // 1. initial is already EC (earned from broadcast_closed + fail_stop).
    // 2. We only connect EC streams to the forward_ref (echo also goes
    //    through broadcast_closed + fail_stop).
    // 3. Any op that introduces per-member nondeterminism (batch, sample_every,
    //    etc.) returns L::DropConsistency, which would break the cycle types.
    //    The ops we use (merge, fold, clone) don't take nondet!, so they
    //    preserve L — and the type system would reject anything that doesn't.
    let (gossip_handle, gossip_from_peers) =
        initial.location().forward_ref::<Stream<BTreeSet<T>, _, Unbounded, NoOrder, AtLeastOnce>>();

    // Step 2: Merge initial element stream with full-state gossip from peers (flattened).
    let all_elements = initial.merge_unordered(gossip_from_peers.flatten_unordered());

    // Step 3: Fold into G-Set state. Input is EC + NoOrder → commutative proof required.
    let gset_state = all_elements.fold(
        q!(|| BTreeSet::new()),
        q!(|set, v| { set.insert(v); },
           commutative = manual_proof!(/** set insert is commutative */),
           idempotent = manual_proof!(/** set insert is idempotent */)),
    );

    // Step 4: Re-broadcast state to all peers on each gossip pump tick.
    // The snapshot drops consistency, but broadcast_closed earns it back fresh.
    let sampled = sliced! {
        let snapshot = use::snapshot(
            gset_state.clone(),
            nondet!(/** gossip pump timing — performance, not correctness */),
        );
        let pump = use::batch(
            gossip_ticks,
            nondet!(/** gossip pump timing — performance, not correctness */),
        );
        snapshot.filter_if(pump.first().is_some()).into_stream()
    }
    .weaken_retries::<AtLeastOnce>();

    let echo = sampled
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
        .values();

    // Close the cycle. echo is EC, forward_ref is EC — types match.
    gossip_handle.complete(echo);

    gset_state
}

/// G-Set CRDT gossip over **dynamic membership** — the "developer-invented
/// protocol" demonstration for the factored EC-minting rule
/// ([`fan_out`](crate::ec_inference_demos::fan_out::fan_out)).
///
/// Structurally this is [`g_set_gossip`] with both broadcasts swapped from
/// `broadcast_closed` (static membership) onto the generic rule over the **live**
/// membership view. Note what is absent:
///
/// - zero `assert_has_consistency_of` in this protocol,
/// - zero `manual_proof!` on consistency (the two `manual_proof!`s below are the
///   fold's ACI obligations — a property of the *combiner*, orthogonal to EC).
///
/// EC on the output is inferred from typed premises alone: the membership view is
/// [`EventuallyComplete`](crate::ec_inference_demos::fan_out::EventuallyComplete) (minted by the
/// runtime oracle), the fan-out join retains both sides, `fail_stop` is
/// EC-preserving, and the `forward_ref` cycle closes coinductively on the EC
/// location. Swap `fail_stop` for plain `lossy` and this function no longer
/// compiles — the claimed EC return type cannot be met.
pub fn g_set_gossip_live<
    'a,
    T: Clone + Ord + Hash + Serialize + DeserializeOwned + 'static,
    L2: 'a,
>(
    cluster: &Cluster<'a, L2>,
    local_updates: Stream<T, Cluster<'a, L2>, Unbounded, NoOrder>,
    gossip_ticks: Stream<(), Cluster<'a, L2>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Singleton<BTreeSet<T>, Cluster<'a, L2, EventualConsistency>, Unbounded>
{
    use crate::ec_inference_demos::fan_out::{MembershipView, fan_out};

    // Step 1: Fan local updates out over the LIVE membership relation. EC minted
    // by the generic rule; a member that joins late crosses all prior updates.
    let initial = fan_out(
        local_updates,
        MembershipView::live(cluster, cluster),
        cluster,
        TCP.fail_stop().bincode(),
    )
    .values();

    let (gossip_handle, gossip_from_peers) =
        initial.location().forward_ref::<Stream<BTreeSet<T>, _, Unbounded, NoOrder, AtLeastOnce>>();

    // Step 2: Merge fresh elements with full-state gossip from peers.
    let all_elements = initial.merge_unordered(gossip_from_peers.flatten_unordered());

    // Step 3: Fold into G-Set state. ACI proofs are about the combiner, not EC.
    let gset_state = all_elements.fold(
        q!(|| BTreeSet::new()),
        q!(|set, v| { set.insert(v); },
           commutative = manual_proof!(/** set insert is commutative */),
           idempotent = manual_proof!(/** set insert is idempotent */)),
    );

    // Step 4: Re-gossip state over the live relation on each pump tick.
    // The snapshot downgrades; the fan-out rule re-earns EC fresh.
    let sampled = sliced! {
        let snapshot = use::snapshot(
            gset_state.clone(),
            nondet!(/** gossip pump timing — performance, not correctness */),
        );
        let pump = use::batch(
            gossip_ticks,
            nondet!(/** gossip pump timing — performance, not correctness */),
        );
        snapshot.filter_if(pump.first().is_some()).into_stream()
    }
    .weaken_retries::<AtLeastOnce>();

    let echo = fan_out(
        sampled,
        MembershipView::live(cluster, cluster),
        cluster,
        TCP.fail_stop().bincode(),
    )
    .values();

    // Close the cycle coinductively: echo is EC, forward_ref is EC.
    gossip_handle.complete(echo);

    gset_state
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;

    use super::{g_set_gossip, g_set_gossip_live};

    /// G-Set states must only grow: pins that no observed snapshot ever loses
    /// an element relative to its predecessor.
    fn assert_monotone(snapshots: &[BTreeSet<u32>], member: u32) {
        for pair in snapshots.windows(2) {
            assert!(
                pair[0].is_subset(&pair[1]),
                "member {member}: G-Set state shrank between snapshots: {:?} -> {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    /// First-ever behavior test for the static gossip demo: every member's
    /// state converges to the union of all members' contributions, in every
    /// explored execution. No pump ticks are needed in the crash-free case —
    /// the initial broadcast alone spreads every element; the pump exists for
    /// healing (see `g_set_gossip_survivors_converge_under_member_crash`).
    ///
    /// Fuzz, not exhaustive: the snapshot hook inside the `sliced!` pump forks
    /// on every state change and the echo cycle keeps re-offering, so the
    /// exhaustive search does not terminate in reasonable time even at n=2
    /// (verified: >5 min without completing).
    #[test]
    fn g_set_gossip_converges_static() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (upd_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (_tick_send, ticks) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();

        let state = g_set_gossip(&cluster, updates, ticks);

        let obs_tick = cluster.tick();
        let out_recv = state
            .snapshot(&obs_tick, nondet!(/** test observation */))
            .all_ticks()
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 2)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                upd_send.send_many_unordered([(0, 1u32), (1, 2u32)]);

                for member in 0..2u32 {
                    let snapshots: Vec<BTreeSet<u32>> = out_recv.collect(member).await;
                    assert_monotone(&snapshots, member);
                    assert_eq!(
                        snapshots.last(),
                        Some(&BTreeSet::from([1, 2])),
                        "member {member} did not converge to the union"
                    );
                }
            });
    }

    /// Minimal regression net for the sim's empty-fold-batch bug: a top-level
    /// fold whose hook was serviced (empty, trivial release) before real input
    /// arrived used to have its `scan` state permanently terminated, silently
    /// dropping every later element — observed as a member never absorbing its
    /// own gossiped element. n=1 keeps the search space tiny so this runs
    /// exhaustively. (Fix: `TopLevelFoldHook::release_decision` skips empty
    /// batches; the generated scan panics if one ever arrives.)
    #[test]
    fn g_set_gossip_live_n1_own_element_reaches_own_state() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (upd_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (_tick_send, ticks) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();

        let state = g_set_gossip_live(&cluster, updates, ticks);

        let obs_tick = cluster.tick();
        let out_recv = state
            .snapshot(&obs_tick, nondet!(/** test observation */))
            .all_ticks()
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 1)
            .with_dynamic_membership(&cluster)
            .exhaustive(async || {
                upd_send.send_many_unordered([(0, 10u32)]);
                let snapshots: Vec<BTreeSet<u32>> = out_recv.collect(0).await;
                assert_eq!(
                    snapshots.last(),
                    Some(&BTreeSet::from([10])),
                    "member 0 did not converge"
                );
            });
    }

    /// First-ever behavior test for the dynamic gossip demo: across every
    /// explored join timing (a member's `Joined` fact may reach observers
    /// after updates have already flowed), every member converges to the full
    /// union — the late joiner is caught up by the retained fan-out join
    /// re-firing accumulated data when its join arrives.
    ///
    /// Fuzz for the same state-space reason as
    /// [`g_set_gossip_converges_static`]; the n=1 exhaustive coverage lives in
    /// `g_set_gossip_live_n1_own_element_reaches_own_state`.
    #[test]
    fn g_set_gossip_live_late_joiner_converges() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (upd_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (_tick_send, ticks) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();

        let state = g_set_gossip_live(&cluster, updates, ticks);

        let obs_tick = cluster.tick();
        let out_recv = state
            .snapshot(&obs_tick, nondet!(/** test observation */))
            .all_ticks()
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .with_dynamic_membership(&cluster)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                upd_send.send_many_unordered([(0, 10u32), (1, 20u32)]);

                for member in 0..3u32 {
                    let snapshots: Vec<BTreeSet<u32>> = out_recv.collect(member).await;
                    assert_monotone(&snapshots, member);
                    assert_eq!(
                        snapshots.last(),
                        Some(&BTreeSet::from([10, 20])),
                        "member {member} did not converge to the union"
                    );
                }
            });
    }

    /// The fault column: gossip's pump is its retry loop (premise (d) of the
    /// dissemination theorem). One untargeted crash (`with_crashable_cluster`)
    /// may cut a member's initial broadcast mid-fan-out, leaving peers with
    /// different partial states — the same wound that permanently diverges
    /// plain broadcast. The periodic re-offer of *merged state* heals it:
    /// pumping the survivors' gossip ticks must, within bounded rounds and in
    /// every explored execution, bring all live members to identical states
    /// (with every stale/crashed member's last state a subset). Element loss
    /// is legal — a member that crashes before its element reaches anyone
    /// takes it to the grave — but survivor *divergence* is not.
    #[test]
    fn g_set_gossip_survivors_converge_under_member_crash() {
        const N: usize = 3;
        const MAX_ROUNDS: usize = 8;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (upd_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (tick_send, ticks) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();

        let state = g_set_gossip(&cluster, updates, ticks);

        let obs_tick = cluster.tick();
        let out_recv = state
            .snapshot(&obs_tick, nondet!(/** test observation */))
            .all_ticks()
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, N)
            .with_crashable_cluster(&cluster, 1)
            .fuzz(async || {
                // Every member contributes one element; the crash search may
                // kill any member at any of its send boundaries with any
                // per-recipient cut of its in-flight sends.
                upd_send.send_many_unordered([(0, 10u32), (1, 11u32), (2, 12u32)]);

                let mut histories: Vec<Vec<BTreeSet<u32>>> = vec![Vec::new(); N];
                let mut converged = false;

                for _round in 0..MAX_ROUNDS {
                    // Pump the gossip re-offer at every member (dead members
                    // ignore it), then settle and observe.
                    for member in 0..N as u32 {
                        tick_send.send(member, ());
                    }
                    hydro_lang::sim::quiesce().await;
                    for member in 0..N as u32 {
                        let new: Vec<BTreeSet<u32>> = out_recv.collect(member).await;
                        histories[member as usize].extend(new);
                        assert_monotone(&histories[member as usize], member);
                    }

                    // Survivor-agnostic convergence check: at least N - 1
                    // members share an identical latest state, and every other
                    // member's latest state is a subset of it (a crashed
                    // member's history is frozen wherever it died).
                    let finals: Vec<&BTreeSet<u32>> =
                        histories.iter().filter_map(|h| h.last()).collect();
                    let converged_now = finals.iter().any(|candidate| {
                        let agree = finals.iter().filter(|f| f == &candidate).count();
                        agree >= N - 1 && finals.iter().all(|f| f.is_subset(candidate))
                    });
                    if converged_now {
                        converged = true;
                        break;
                    }
                }

                assert!(
                    converged,
                    "survivors failed to converge within {MAX_ROUNDS} pump rounds; \
                     latest states: {:?}",
                    histories
                        .iter()
                        .map(|h| h.last().cloned())
                        .collect::<Vec<_>>()
                );
            });
    }
}
