//! The generic EC-minting rule: fan out data over a typed membership view.
//!
//! # One rule, three premises
//!
//! `EventualConsistency` (EC) on a fanned-out stream rests on three
//! independently-certifiable premises, each carried by a type:
//!
//! 1. **Membership completeness**: every `Joined` fact is eventually delivered to
//!    every holder of data. This is a property of the membership *source* (the
//!    runtime membership oracle, or static deploy-time `ClusterIds`), not of any
//!    particular protocol. It is captured here as the [`EventuallyComplete`] type
//!    state on a [`MembershipView`].
//! 2. **Holder persistence**: the data side of the fan-out join is retained
//!    (append-only), so a member that joins late still crosses all prior data.
//!    The symmetric-hash join over two `Unbounded` inputs provides this; operators
//!    that cut or prune (`batch`, `sample_every`, …) already mechanically downgrade
//!    consistency, and a future bounded-retention tier would have to weaken the
//!    membership view instead ([`Sampled`]).
//! 3. **Channel eventual delivery**: the network failure policy delivers the same
//!    messages to every live member, tracked by
//!    [`NetworkFor::ConsistencyGuarantee`] (`fail_stop` /
//!    `lossy_delayed_forever` → EC, plain `lossy` → `NoConsistency`).
//!
//! Given those three premises, the coinductive (greatest-fixed-point) argument is
//! uniform: every element eventually crosses every member that ever joins, so all
//! live destinations materialize the same elements. That argument is discharged
//! exactly once, by the single `manual_proof!` in [`fan_out`] /
//! [`fan_out_from_process`]. Protocol libraries built on top (broadcast, gossip,
//! transcript consensus) then carry **zero** consistency assertions of their own.
//!
//! (For the theory behind this decomposition — EC read as eventual common
//! knowledge among the live members — see
//! `design_docs/2026-08_epistemic_foundations_ec_inference.md`. Nothing in this
//! module requires that background.)
//!
//! # History: the per-combinator mints this rule replaced (and the one survivor)
//!
//! EC used to be minted by monolithic per-combinator assertions:
//! `broadcast_closed` (in `hydro_lang`) and
//! [`broadcast_live`](crate::ec_inference_demos::broadcast_live) (in this crate)
//! each carried a bespoke `manual_proof!`. When this module landed,
//! `broadcast_live`'s was deleted — it is now a thin client of [`fan_out`] over
//! [`MembershipView::live`], with no proof of its own.
//!
//! `broadcast_closed`'s mint **survives in parallel**, because `hydro_lang`
//! cannot depend on this crate. Conceptually it is [`fan_out`] over
//! [`MembershipView::static_members`] (the degenerate, complete-at-time-zero
//! view) — and that instantiation is pinned by a compile test below — but its
//! actual code path still runs its own `assert_has_consistency_of_trusted` in
//! `hydro_lang`. Promoting this rule into `hydro_lang`'s trusted base would let
//! `broadcast_closed` become a real client and retire that duplicate.
//!
//! # Why membership needs its own label (and it is NOT a consistency label)
//!
//! Different members may observe joins at different times and in different orders —
//! the membership stream is correctly `NoConsistency`, and that is fine, because
//! `joined(p)` is a *stable fact* (once true, stays true): the view is a monotone
//! lower bound converging to the true join relation. What the minting rule needs
//! is not cross-member agreement but *eventual completeness*: no join fact is
//! permanently withheld from a data holder. [`EventuallyComplete`] tracks exactly
//! that, and it is minted at exactly two trusted places:
//!
//! - [`MembershipView::live`]: the runtime membership oracle's contract.
//! - [`MembershipView::static_members`]: deploy-time `ClusterIds`, complete at
//!   time zero by construction (the degenerate, common-knowledge case).
//!
//! A snapshot of membership can never be [`EventuallyComplete`] — snapshotting
//! freezes the view, so a later join is permanently withheld. That is the
//! type-level form of the "never fan out over a membership snapshot" guardrail.

use std::marker::PhantomData;

use hydro_lang::live_collections::boundedness::Boundedness;
use hydro_lang::live_collections::stream::{ExactlyOnce, MinOrder, NoOrder, Ordering, Retries};
use hydro_lang::location::cluster::{ClusterIds, Consistency, NoConsistency};
use hydro_lang::location::{Location, MemberId, MembershipEvent, TopLevel};
use hydro_lang::networking::NetworkFor;
use hydro_lang::prelude::*;
use hydro_lang::properties::ConsistencyProof;
use serde::Serialize;
use serde::de::DeserializeOwned;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::EventuallyComplete {}
    impl Sealed for super::Sampled {}
}

/// Type-state marker for how faithfully a [`MembershipView`] tracks the true
/// (monotone) join relation of a cluster.
pub trait Completeness: sealed::Sealed {}

/// Every `Joined` fact is eventually delivered to every observer of this view.
///
/// This is premise (1) of the EC-minting rule: it does **not** say observers see
/// joins at the same time or in the same order (they don't need to — `joined(p)`
/// is a stable fact), only that no join is permanently withheld.
pub enum EventuallyComplete {}
impl Completeness for EventuallyComplete {}

/// A point-in-time or otherwise incomplete view of membership: joins occurring
/// after the cut are permanently invisible.
///
/// There is deliberately **no** [`fan_out`] over a [`Sampled`] view — fanning out
/// over incomplete membership permanently excludes late joiners, which is exactly
/// the unsoundness that makes the legacy snapshotting `broadcast` `NoConsistency`.
pub enum Sampled {}
impl Completeness for Sampled {}

/// A view of the (monotone, `Joined`-filtered) membership relation of a target
/// cluster, observed at some location, tagged with a [`Completeness`] type state.
///
/// The underlying stream is `NoConsistency` on purpose: per-observer join *timing*
/// is nondeterministic. The type state carries the one property the EC-minting
/// rule actually needs.
pub struct MembershipView<'a, Target, Obs, Cpl: Completeness = EventuallyComplete>
where
    Obs: Location<'a>,
{
    ids: Stream<MemberId<Target>, Obs, Unbounded, NoOrder, ExactlyOnce>,
    _cpl: PhantomData<(&'a (), Cpl)>,
}

impl<'a, Target: 'a, Obs: Location<'a>> MembershipView<'a, Target, Obs, EventuallyComplete> {
    /// The **live** membership relation, from the runtime membership oracle.
    ///
    /// This is one of the two trusted mints of [`EventuallyComplete`]: the
    /// deployment runtime guarantees that every member's `Joined` event is
    /// eventually delivered to every observer (dropped prefixes are covered
    /// because joins are stable facts and the relation is monotone).
    pub fn live<Observer>(observer: &Observer, cluster: &Cluster<'a, Target>) -> Self
    where
        Observer: TopLevel<'a> + Location<'a, DropConsistency = Obs>,
    {
        let ids = observer
            .source_cluster_membership_stream(
                cluster,
                nondet!(
                    /// Join timing is nondeterministic per observer, but joins are
                    /// stable facts: the view is a monotone lower bound of the true
                    /// relation, and completeness (no join permanently withheld) is
                    /// the oracle's contract.
                ),
            )
            .entries()
            .filter_map(q!(|(id, ev)| match ev {
                MembershipEvent::Joined => Some(id),
                MembershipEvent::Left => None,
            }));

        MembershipView {
            ids,
            _cpl: PhantomData,
        }
    }

    /// The **static** membership relation, from deploy-time `ClusterIds`.
    ///
    /// The degenerate case of [`EventuallyComplete`]: the full member set is known
    /// to every observer at time zero by construction, so completeness holds
    /// trivially. Conceptually, `broadcast_closed` is [`fan_out`] over this view
    /// (though its actual code path in `hydro_lang` carries its own parallel
    /// mint — see the module docs).
    pub fn static_members<Observer>(observer: &Observer, cluster: &Cluster<'a, Target>) -> Self
    where
        Observer: Location<'a, DropConsistency = Obs>,
    {
        let cluster_ids = ClusterIds {
            key: Location::id(cluster).key(),
            _phantom: PhantomData,
        };

        let ids = observer
            .source_iter(q!(cluster_ids
                .iter()
                .map(|id| MemberId::from_tagless(id.clone()))))
            .weaken_boundedness()
            .weaken_ordering();

        MembershipView {
            ids,
            _cpl: PhantomData,
        }
    }

    /// Escape hatch: assert completeness of a custom membership source (e.g. an
    /// application-level registry), with a [`manual_proof!`] obligation.
    ///
    /// The obligation is precisely: *every join fact carried by this stream's
    /// underlying relation is eventually delivered to this observer* — no
    /// cross-member consistency is required.
    pub fn from_stream_asserted(
        ids: Stream<MemberId<Target>, Obs, Unbounded, NoOrder, ExactlyOnce>,
        _completeness: impl ConsistencyProof,
    ) -> Self {
        MembershipView {
            ids,
            _cpl: PhantomData,
        }
    }
}

/// **The one EC-minting rule.** Fans a cluster stream out to every member of `to`
/// drawn from an [`EventuallyComplete`] membership view, over a network policy
/// whose [`NetworkFor::ConsistencyGuarantee`] determines the output consistency.
///
/// This subsumes `broadcast_closed` (static view) and
/// [`broadcast_live`](crate::ec_inference_demos::broadcast_live) (live view): both are thin clients.
/// A protocol author who fans out over *any* `EventuallyComplete` view via an
/// EC-preserving policy gets EC minted here — with the coinductive argument
/// discharged once, below, instead of per-protocol.
pub fn fan_out<'a, T, L, L2, C, O, R, N>(
    source: Stream<T, Cluster<'a, L, C>, Unbounded, O, R>,
    members: MembershipView<'a, L2, Cluster<'a, L, NoConsistency>, EventuallyComplete>,
    to: &Cluster<'a, L2>,
    via: N,
) -> KeyedStream<MemberId<L>, T, Cluster<'a, L2, N::ConsistencyGuarantee>, Unbounded, NoOrder, R>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    C: Consistency,
    O: Ordering + MinOrder<N::OrderingGuarantee>,
    R: Retries,
    N: NetworkFor<T>,
{
    // Premise (2), holder persistence: both sides of the join are Unbounded and
    // retained by the symmetric-hash join, so a late joiner crosses all prior data.
    source
        .weaken_consistency()
        .cross_product(members.ids)
        .map(q!(|(data, member_id)| (member_id, data)))
        .into_keyed()
        .demux(to, via)
        .assert_has_consistency_of::<Cluster<'a, L2, N::ConsistencyGuarantee>>(manual_proof!(
            /// The single trusted minting step, from three typed premises:
            /// (1) the membership view is `EventuallyComplete` — every join fact
            /// eventually reaches this sender; (2) the data side is retained by the
            /// symmetric-hash join — some holder persists every element; (3) the
            /// network policy (tracked by `NetworkFor::ConsistencyGuarantee`)
            /// eventually delivers every sent message to every live member.
            /// Coinductively: every element eventually crosses every member that
            /// ever joins and is delivered, so all live destinations materialize
            /// the same elements. Static `ClusterIds` is the limit case.
        ))
}

/// [`fan_out`] for a [`Process`] source. The output is a plain `Stream` (a process
/// has no source-member identity), mirroring the `broadcast_closed` /
/// `broadcast_live_from_process` split.
pub fn fan_out_from_process<'a, T, L, L2, B, O, R, N>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    members: MembershipView<'a, L2, Process<'a, L>, EventuallyComplete>,
    to: &Cluster<'a, L2>,
    via: N,
) -> Stream<T, Cluster<'a, L2, N::ConsistencyGuarantee>, Unbounded, NoOrder, R>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    B: Boundedness,
    O: Ordering + MinOrder<N::OrderingGuarantee>,
    R: Retries,
    N: NetworkFor<T>,
{
    source
        .cross_product(members.ids)
        .map(q!(|(data, member_id)| (member_id, data)))
        .into_keyed()
        .demux(to, via)
        .assert_has_consistency_of::<Cluster<'a, L2, N::ConsistencyGuarantee>>(manual_proof!(
            /// Same three premises as `fan_out`; the process source is trivially a
            /// consistent holder.
        ))
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::keyed_stream::KeyedStream;
    use hydro_lang::location::cluster::{EventualConsistency, NoConsistency};
    use hydro_lang::location::{Cluster, MemberId};
    use hydro_lang::prelude::*;

    use super::{MembershipView, fan_out};

    /// Compile-time check of the minting rule itself: output consistency tracks
    /// the network policy premise, for BOTH mints of `EventuallyComplete`
    /// (live oracle and static `ClusterIds`). The `lossy` arm proves EC is earned
    /// from the premises, not hard-coded.
    #[test]
    fn fan_out_consistency_tracks_policy_for_both_views() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let to = flow.cluster::<()>();

        let (_s1, d1) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (_s2, d2) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (_s3, d3) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // Live view + fail_stop ⇒ EC.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), EventualConsistency>, _, _, _> =
            fan_out(
                d1.clone(),
                MembershipView::live(d1.location(), &to),
                &to,
                TCP.fail_stop().bincode(),
            );

        // Static view + fail_stop ⇒ EC (broadcast_closed as a fan_out instance).
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), EventualConsistency>, _, _, _> =
            fan_out(
                d2.clone(),
                MembershipView::static_members(d2.location(), &to),
                &to,
                TCP.fail_stop().bincode(),
            );

        // Live view + plain lossy ⇒ premise (3) fails ⇒ NoConsistency.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), NoConsistency>, _, _, _> = fan_out(
            d3.clone(),
            MembershipView::live(d3.location(), &to),
            &to,
            TCP.lossy(nondet!(/** test */)).bincode(),
        );

        let _ = flow.finalize();
    }

    /// **Premise 2, refuted under crash faults.** The epistemic doc's
    /// correction predicted this: [`fan_out`]'s single trusted mint tacitly
    /// assumes the data holder does not crash ("premise 2 hardens to *some
    /// correct holder persists*, which is structural only in the
    /// replicate-cycle"). With crash injection the prediction is now a
    /// sim-found fact: crash a cluster-source member mid-fan-out (explored,
    /// untargeted — `with_crashable_cluster(source, 1)`) and the exhaustive
    /// search finds executions where the **live** destination members
    /// permanently disagree: one delivered the element, the other never will,
    /// and the only holder is dead. The EC label `fan_out` mints is refuted
    /// under crash faults.
    ///
    /// This is the mechanical justification for the planned Tier-1 move
    /// (`2026-08_orchestrated_membership_ec_dissemination.md`): demote
    /// `fan_out` to a mechanical primitive and attach the crash-honest EC mint
    /// to the replicate cycle — the echo is exactly what makes the holder set
    /// self-perpetuating, and `reliable_broadcast_closed_agreement_under_sender_crash`
    /// confirms the cycle survives the same fault.
    #[test]
    fn fan_out_ec_mint_refuted_under_source_crash() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let dest = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = fan_out(
            data.clone(),
            MembershipView::static_members(data.location(), &dest),
            &dest,
            TCP.fail_stop().bincode(),
        )
        .entries()
        .send(&node, TCP.fail_stop().bincode())
        .entries()
        .sim_output();

        let mut saw_divergence = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&source, 2)
            .with_cluster_size(&dest, 2)
            .with_crashable_cluster(&source, 1)
            .exhaustive(async || {
                in_send.send(0, 42);

                let received: Vec<(MemberId<()>, (MemberId<()>, u32))> =
                    out_recv.collect_sorted().await;

                // The destination members never crash: any disagreement below
                // is disagreement among LIVE members — precisely what the EC
                // label asserts cannot persist.
                let delivered: Vec<bool> = (0..2u32)
                    .map(|d| {
                        received.iter().any(|(dest_member, (src, v))| {
                            *dest_member == MemberId::from_raw_id(d)
                                && *src == MemberId::from_raw_id(0)
                                && *v == 42
                        })
                    })
                    .collect();

                if delivered[0] != delivered[1] {
                    saw_divergence = true;
                }
                if delivered[0] && delivered[1] {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_divergence,
            "premise 2's crash-hole must be witnessed: a source-member crash mid-fan-out \
             leaves live destinations permanently diverged (no echo, no second holder)"
        );
        assert!(
            saw_full_delivery,
            "sanity: the crash-free execution delivers to every destination"
        );
    }

    /// Self-delivery edge case: `fan_out` over a live view must deliver a
    /// member's element to ITSELF under dynamic membership. The RB-live tests
    /// can't catch a broken self-send (any peer's echo masks it); gossip's
    /// "own element" convergence depends on it. (n=1, exhaustive, tiny.)
    #[test]
    fn fan_out_live_self_delivery_under_dynamic_membership() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = fan_out(
            data.clone(),
            MembershipView::live(data.location(), &cluster),
            &cluster,
            TCP.fail_stop().bincode(),
        )
        .entries()
        .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 1)
            .with_dynamic_membership(&cluster)
            .exhaustive(async || {
                in_send.send(0, 7);
                let got: Vec<(MemberId<()>, u32)> = out_recv.collect_sorted(0).await;
                assert!(
                    got.contains(&(MemberId::from_raw_id(0), 7)),
                    "self-delivery failed, member 0 got: {got:?}"
                );
            });
    }

    /// Sim test of the axiom behind `EventuallyComplete` (premise 1 of the
    /// minting rule): every observer's live view eventually contains every
    /// member of the target cluster, across all explored join timings.
    ///
    /// This is the "sim as test oracle for the premise the types trust"
    /// division of labor: the type system *assumes* coverage when
    /// `MembershipView::live` mints `EventuallyComplete`; this test lets the
    /// exhaustive search try to refute it. (Relocated from the retired
    /// `orchestrated_membership` module, whose envelope-coverage test was
    /// testing exactly this premise.)
    #[test]
    fn live_view_coverage_axiom_holds_under_dynamic_membership() {
        let mut flow = FlowBuilder::new();
        let observers = flow.cluster::<()>();
        let target = flow.cluster::<()>();
        let node = flow.process::<()>();

        let view = MembershipView::live(&observers, &target);

        let out_recv = view
            .ids
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
                // Each of the 2 observers eventually sees both target members,
                // regardless of join timing.
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
