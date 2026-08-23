//! The generic EC-minting rule: fan out data over a typed membership view.
//!
//! # The epistemic decomposition
//!
//! Today, `EventualConsistency` (EC) is minted by monolithic trusted combinators:
//! `broadcast_closed` (in `hydro_lang`) and [`broadcast_live`](crate::broadcast_live)
//! (in this crate) each carry their own bespoke `manual_proof!`. The knowledge-theoretic
//! reading of EC — eventual common knowledge (C^◇) among the live members, per
//! Halpern & Moses — shows that both proofs are instances of ONE rule with three
//! independently-certifiable premises:
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
//! live destinations materialize the same elements. That argument appears exactly
//! once, in [`fan_out`] / [`fan_out_from_process`]. Protocol libraries built on top
//! (broadcast, gossip, transcript consensus) then carry **zero** consistency
//! assertions of their own.
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
use hydro_lang::live_collections::stream::{
    ExactlyOnce, MinOrder, NoOrder, Ordering, Retries,
};
use hydro_lang::location::cluster::{ClusterIds, Consistency, NoConsistency};
use hydro_lang::location::{Location, MemberId, MembershipEvent, TopLevel};
use hydro_lang::networking::NetworkFor;
use hydro_lang::properties::ConsistencyProof;
use hydro_lang::prelude::*;
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
    /// to every observer at time zero (it is model-level common knowledge), so
    /// completeness holds trivially. `broadcast_closed` is [`fan_out`] over this
    /// view.
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
/// [`broadcast_live`](crate::broadcast_live) (live view): both are thin clients.
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
