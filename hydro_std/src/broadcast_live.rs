//! `broadcast_live`: fan out a cluster stream to all members of a destination
//! cluster over the **live, monotone** membership relation, earning
//! `EventualConsistency` (EC) by construction.
//!
//! # Relationship to `broadcast_closed`
//!
//! [`Stream::broadcast_closed`](hydro_lang::live_collections::stream::Stream) fans
//! out over `ClusterIds` — static, deploy-time-fixed membership — and asserts EC
//! from the network policy. `broadcast_live` is the direct generalization: it
//! fans out over the *live* membership stream
//! (`source_cluster_membership_stream`), filtered to `Joined` ids and kept as a
//! growing relation (no snapshot). `ClusterIds` is simply the limit of this
//! monotone relation.
//!
//! Contrast [`Stream::broadcast`], which `use::snapshot`s the membership set
//! inside a `sliced!` block. Snapshotting freezes one side of the data × members
//! join, so `member` deltas never re-fire and late joiners are missed — which is
//! exactly why its output is `NoConsistency`. Joining against the live relation
//! keeps the delta symmetry: a new `Joined` id re-fires the cross-product against
//! all accumulated data, catching the joiner up.
//!
//! # EC argument (the single trusted step)
//!
//! The intermediate member-id stream is `NoConsistency` — correctly, since the
//! *timing* of each member's join is per-member and nondeterministic. EC is
//! re-earned on the *delivered* result: with an EC-preserving network policy
//! (`fail_stop` / `lossy_delayed_forever`, tracked by
//! [`NetworkFor::ConsistencyGuarantee`](hydro_lang::networking::NetworkFor)),
//! every element eventually crosses every member that ever joins and is delivered
//! to it, so all destinations materialize the same elements. That is the one
//! `manual_proof!` in this combinator — the same fact `broadcast_closed`
//! discharges, generalized from a static set to the monotone relation.
//!
//! # Soundness caveat (bounds this to append-only data)
//!
//! The argument needs *both* join sides retained. The symmetric-hash join over
//! two `Unbounded` inputs keeps them, so a late joiner still crosses all prior
//! data. Pruning the data side (bounded retention) breaks this — a joiner
//! arriving after a prune misses data — which is a genuine consistency weakening,
//! not covered here.

use hydro_lang::live_collections::stream::{MinOrder, NoOrder, Ordering, Retries};
use hydro_lang::location::{Location, MemberId, MembershipEvent};
use hydro_lang::networking::NetworkFor;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Broadcasts elements of a cluster stream to all members of a destination
/// cluster over the live monotone membership relation, earning EC by
/// construction from the network policy.
///
/// This is the dynamic-membership generalization of `broadcast_closed`: the
/// static `ClusterIds` source is replaced by the live `Joined`-filtered
/// membership stream, kept live (not snapshotted).
pub fn broadcast_live<'a, T, L, L2, C, O, R, N>(
    source: Stream<T, Cluster<'a, L, C>, Unbounded, O, R>,
    to: &Cluster<'a, L2>,
    via: N,
) -> KeyedStream<
    MemberId<L>,
    T,
    Cluster<'a, L2, N::ConsistencyGuarantee>,
    Unbounded,
    NoOrder,
    R,
>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    C: hydro_lang::location::cluster::Consistency,
    O: Ordering + MinOrder<N::OrderingGuarantee>,
    R: Retries,
    N: NetworkFor<T>,
{
    // The live membership relation of the destination cluster, observed at the
    // source. Filtered to `Joined`, kept as a growing (monotone) stream — NOT
    // snapshotted. Its consistency is `NoConsistency`: join *timing* is
    // per-member and nondeterministic.
    let members = source
        .location()
        .source_cluster_membership_stream(
            to,
            nondet!(/** dropped membership prefixes don't affect broadcast delivery */),
        )
        .entries()
        .filter_map(q!(|(id, ev)| match ev {
            MembershipEvent::Joined => Some(id),
            MembershipEvent::Left => None,
        }));

    // Join data × live members at `NoConsistency` (both sides growing; the
    // symmetric-hash join re-fires on deltas to either input, so a late joiner
    // crosses all accumulated data), then demux and re-earn EC on delivery.
    source
        .weaken_consistency()
        .cross_product(members)
        .map(q!(|(data, member_id)| (member_id, data)))
        .into_keyed()
        .demux(to, via)
        .assert_has_consistency_of::<Cluster<'a, L2, N::ConsistencyGuarantee>>(manual_proof!(
            /// Live monotone membership + an EC-preserving network policy: every element
            /// eventually crosses every member that ever joins and is delivered to it, so
            /// all live destinations materialize the same elements. `ClusterIds` (the
            /// `broadcast_closed` case) is the limit of this relation.
        ))
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::keyed_stream::KeyedStream;
    use hydro_lang::location::cluster::{EventualConsistency, NoConsistency};
    use hydro_lang::location::{Cluster, MemberId};
    use hydro_lang::prelude::*;

    use super::broadcast_live;

    /// Compile-time check that `broadcast_live`'s output consistency tracks the
    /// network failure policy — the M1 core claim. `fail_stop` /
    /// `lossy_delayed_forever` yield `EventualConsistency` over the *live*
    /// membership relation; plain `lossy` yields only `NoConsistency`.
    ///
    /// The `lossy` arm's type annotation is what proves EC is genuinely *earned*
    /// from the policy, not hard-coded: if `broadcast_live` always claimed EC,
    /// this arm would fail to compile.
    #[test]
    fn broadcast_live_consistency_tracks_failure_policy() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let to = flow.cluster::<()>();

        // An unbounded live source stream at each source member.
        let (_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (_send2, data2) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (_send3, data3) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // fail_stop: same messages to every live member ⇒ EC over live membership.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), EventualConsistency>, _, _, _> =
            broadcast_live(data, &to, TCP.fail_stop().bincode());

        // lossy_delayed_forever: drops modeled as indefinite delays ⇒ still EC.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), EventualConsistency>, _, _, _> =
            broadcast_live(data2, &to, TCP.lossy_delayed_forever().bincode());

        // plain lossy: permanent per-member drops ⇒ only NoConsistency.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), NoConsistency>, _, _, _> =
            broadcast_live(data3, &to, TCP.lossy(nondet!(/** test */)).bincode());

        let _ = flow.finalize();
    }

    /// Behavior test (static membership): `broadcast_live` must deliver every
    /// element to every destination member — i.e. under fixed membership it
    /// behaves exactly like `broadcast_closed`. This is the "if membership turns
    /// out fixed, it just works" claim, checked by the sim exploring delivery
    /// interleavings. (The sim cannot yet vary join *timing* — that is M0 — so
    /// this exercises the static-membership fanout, not late-joiner catch-up.)
    #[test]
    fn broadcast_live_delivers_to_all_members_static() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let dest = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // Broadcast over the live membership relation, then route dest deliveries
        // to a process keyed by dest member so we can observe per-member receipt.
        let out_recv = broadcast_live(data, &dest, TCP.fail_stop().bincode())
            .entries()
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&source, 2)
            .with_cluster_size(&dest, 2)
            .exhaustive(async || {
                // Source member 0 broadcasts 123. Every dest member should receive
                // (source_member_0, 123); the process observes it keyed by dest member.
                in_send.send(0, 123);

                out_recv
                    .assert_yields_only_unordered(vec![
                        (MemberId::from_raw_id(0), (MemberId::from_raw_id(0), 123)),
                        (MemberId::from_raw_id(1), (MemberId::from_raw_id(0), 123)),
                    ])
                    .await
            });
    }

    /// Dynamic-membership path (M0): same broadcast, but the destination cluster
    /// opts into `with_dynamic_membership`, so members' `Joined` events are
    /// released by the `MembershipHook` at nondeterministic times rather than all
    /// up front. Every member must still eventually receive the broadcast — the
    /// live join re-fires the fanout as each member joins. This exercises the full
    /// hook-backed membership source end to end.
    #[test]
    fn broadcast_live_delivers_with_dynamic_membership() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let dest = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = broadcast_live(data, &dest, TCP.fail_stop().bincode())
            .entries()
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let instances = flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&source, 2)
            .with_cluster_size(&dest, 2)
            .with_dynamic_membership(&dest)
            .exhaustive(async || {
                in_send.send(0, 123);

                // Regardless of *when* each dest member joins, both eventually
                // receive the broadcast element (the live join catches up late
                // joiners). We assert the full delivered multiset.
                out_recv
                    .assert_yields_only_unordered(vec![
                        (MemberId::from_raw_id(0), (MemberId::from_raw_id(0), 123)),
                        (MemberId::from_raw_id(1), (MemberId::from_raw_id(0), 123)),
                    ])
                    .await
            });

        // Sanity: the dynamic-membership hook must actually fork the search on
        // join *timing*, so more than one execution is explored. (The static
        // variant explores far fewer.) If this were 1, the hook wouldn't be
        // varying anything and the test would be vacuous.
        assert!(
            instances > 1,
            "expected the membership hook to explore multiple join timings, got {instances}"
        );
    }

    /// The real late-join catch-up test. Source member 0 sends *three* messages,
    /// and the destination cluster joins dynamically. Across **every** explored
    /// join timing — including ones where a dest member joins only after all
    /// three messages already exist — every member must still receive all three.
    ///
    /// This is what would actually catch a broken catch-up: if the live join did
    /// not re-fire the accumulated data when a late member joins (e.g. if
    /// `broadcast_live` snapshotted membership like `broadcast` does), a
    /// late-joining member would be missing messages and the full-multiset
    /// assertion would fail in that interleaving.
    #[test]
    fn broadcast_live_late_joiner_catches_up_on_all_messages() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let dest = flow.cluster::<()>();
        let node = flow.process::<()>();

        let (in_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = broadcast_live(data, &dest, TCP.fail_stop().bincode())
            .entries()
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let instances = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&source, 2)
            .with_cluster_size(&dest, 2)
            .with_dynamic_membership(&dest)
            .exhaustive(async || {
                in_send.send(0, 10);
                in_send.send(0, 20);
                in_send.send(0, 30);

                // Full (dest_member × message) delivery set: both dest members
                // (0, 1) receive all three values from source member 0, no matter
                // when they joined.
                let mut expected = Vec::new();
                for dest_member in [0u32, 1] {
                    for value in [10u32, 20, 30] {
                        expected.push((
                            MemberId::from_raw_id(dest_member),
                            (MemberId::from_raw_id(0), value),
                        ));
                    }
                }

                out_recv.assert_yields_only_unordered(expected).await
            });

        assert!(
            instances > 1,
            "expected multiple join timings to be explored, got {instances}"
        );
    }
}
