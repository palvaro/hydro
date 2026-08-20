//! Orchestrator-backed membership view: an **EC-typed** view of the live
//! member set of a cluster, per the design in
//! `design_docs/2026-08_orchestrated_membership_ec_dissemination.md` (M-source).
//!
//! # What this provides
//!
//! [`orchestrator_view`] folds the membership event stream into a
//! [`KeyedSingleton`] mapping each member id ever observed to its current
//! presence (`true` = in the view). The result is typed
//! `EventualConsistency`, blessed by the **single trusted axiom** of this
//! module: the deployment orchestrator (e.g. a ZooKeeper-backed overseer on
//! ECS) guarantees that every observer's *folded* view converges to the same
//! live-member set once churn stops — even though observers' raw event
//! streams may differ in ordering, prefix, and content (a join+leave pair
//! completing before an observer subscribes is invisible to it).
//!
//! The `MonotonicKeys` bound is meaningful: the *key set* of the view is the
//! monotone envelope (every id ever seen in this observer's view — grows
//! only), while the `bool` values carry the current, non-monotone presence.
//! One object carries both: fan-out machinery consumes the monotone key
//! envelope; protocols needing the current set consume the values.
//!
//! # What is EC here — and what is not
//!
//! EC attaches to the **folded set only** (state-based projection). The key
//! envelope is *not* EC (observers' envelopes differ forever) and is not
//! claimed to be. History-dependent projections (join counts, "ever joined")
//! do not converge and never will.
//!
//! # Status of the trust boundary
//!
//! The sim's dynamic-membership model delivers every join to every observer
//! (currently joins-only), which satisfies the orchestrator contract, so
//! sim-tested protocols exercise an honest model. **Deploy-mode backing is
//! not implemented**: in a real deployment the raw membership stream does
//! not by itself carry the convergence guarantee — wiring this primitive to
//! an actual orchestrator (ZK/ECS) is the pending obligation that makes the
//! axiom true outside the sim.

use hydro_lang::live_collections::keyed_singleton::{KeyedSingleton, MonotonicKeys};
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder};
use hydro_lang::location::cluster::{Consistency, EventualConsistency};
use hydro_lang::location::{Cluster, Location, MemberId, MembershipEvent};
use hydro_lang::nondet::NonDet;
use hydro_lang::prelude::*;

/// The monotone member **envelope** of `cluster`, as observed at `at`: the
/// append-only stream of member ids ever observed to join.
///
/// This is `NoConsistency` — observers' envelopes genuinely differ forever —
/// and monotone by construction (ids are only ever added). Under the
/// orchestrator axiom (see [`orchestrator_view`]), every observer's envelope
/// eventually contains every member of the converged view, which is exactly
/// the premise fan-out machinery needs: over-approximation is safe for
/// delivery, under-approximation is fatal.
pub fn member_envelope<'a, L, C2>(
    at: &L,
    cluster: &Cluster<'a, C2>,
    nondet_start: NonDet,
) -> Stream<MemberId<C2>, L::DropConsistency, Unbounded, NoOrder, ExactlyOnce>
where
    L: Location<'a> + hydro_lang::location::TopLevel<'a>,
    C2: 'a,
{
    at.source_cluster_membership_stream(cluster, nondet_start)
        .entries()
        .filter_map(q!(|(id, ev)| match ev {
            MembershipEvent::Joined => Some(id),
            MembershipEvent::Left => None,
        }))
}

/// An eventually-consistent view of the live member set of `cluster`, as
/// observed at `at`, backed by the deployment orchestrator.
///
/// Returns a [`KeyedSingleton`] from member id to current presence:
/// - **keys** = the monotone envelope (ids ever present in this observer's
///   view; grows only — feed this to fan-out machinery),
/// - **values** = current presence (`true` if in the view now; non-monotone).
///
/// The output is typed `EventualConsistency` on the strength of the
/// orchestrator's guarantee that all observers' folded views converge to the
/// same set once membership churn stops. This is this module's one trusted
/// axiom (see module docs for its exact scope and pending deploy-mode
/// obligation).
pub fn orchestrator_view<'a, L, C, C2>(
    at: &Cluster<'a, L, C>,
    cluster: &Cluster<'a, C2>,
    nondet_start: NonDet,
) -> KeyedSingleton<MemberId<C2>, bool, Cluster<'a, L, EventualConsistency>, MonotonicKeys>
where
    L: 'a,
    C2: 'a,
    C: Consistency,
{
    at.source_cluster_membership_stream(cluster, nondet_start)
        .fold(
            q!(|| false),
            q!(|present, ev| {
                *present = match ev {
                    MembershipEvent::Joined => true,
                    MembershipEvent::Left => false,
                }
            }),
        )
        .assert_has_consistency_of::<Cluster<'a, L, EventualConsistency>>(manual_proof!(
            /// ORCHESTRATOR AXIOM (unchecked): the deployment substrate
            /// guarantees that each observer's per-member last-event-wins fold
            /// of its own membership stream converges, once churn stops, to
            /// the same live-member set on every observer. Observers' raw
            /// streams may differ (ordering, prefix, compressed join+leave
            /// pairs); only this state-based fold is claimed convergent.
            /// The sim's dynamic-membership model satisfies this contract;
            /// deploy-mode orchestrator backing is a pending obligation.
        ))
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::keyed_singleton::{KeyedSingleton, MonotonicKeys};
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::location::{Cluster, MemberId};
    use hydro_lang::prelude::*;

    use super::orchestrator_view;

    /// Compile-time check: the view is EC-typed with `MonotonicKeys`.
    #[test]
    fn orchestrator_view_is_ec_typed() {
        let mut flow = FlowBuilder::new();
        let observers = flow.cluster::<()>();
        let target = flow.cluster::<()>();

        let _: KeyedSingleton<
            MemberId<()>,
            bool,
            Cluster<'_, (), EventualConsistency>,
            MonotonicKeys,
        > = orchestrator_view(&observers, &target, nondet!(/** test */));

        let _ = flow.finalize();
    }

    /// Behavior: every observer's envelope eventually contains every member of
    /// the target cluster, across all explored join timings. (The sim model is
    /// joins-only today, so envelope == view; leave/compression modeling is the
    /// design doc's M-view milestone.)
    #[test]
    fn member_envelope_converges_to_full_membership() {
        let mut flow = FlowBuilder::new();
        let observers = flow.cluster::<()>();
        let target = flow.cluster::<()>();
        let node = flow.process::<()>();

        let present_ids =
            super::member_envelope(&observers, &target, nondet!(/** test */));

        let out_recv = present_ids
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let instances = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&observers, 2)
            .with_cluster_size(&target, 2)
            .with_dynamic_membership(&target)
            .exhaustive(async || {
                // Each of the 2 observers eventually sees both target members
                // present, regardless of join timing.
                let mut expected = Vec::new();
                for observer in [0u32, 1] {
                    for member in [0u32, 1] {
                        expected.push((
                            MemberId::from_raw_id(observer),
                            MemberId::<()>::from_raw_id(member),
                        ));
                    }
                }
                out_recv.assert_yields_only_unordered(expected).await
            });

        assert!(
            instances > 1,
            "expected the membership hook to explore multiple join timings, got {instances}"
        );
    }
}
