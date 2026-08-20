//! `broadcast_live`: fan out a cluster stream to all members of a destination
//! cluster over the **live, monotone** membership relation. Purely mechanical
//! fan-out: the output is `NoConsistency` — no consistency is asserted here.
//!
//! # Relationship to `broadcast_closed`
//!
//! [`Stream::broadcast_closed`](hydro_lang::live_collections::stream::Stream) fans
//! out over `ClusterIds` — static, deploy-time-fixed membership — and asserts EC
//! from the network policy. `broadcast_live` generalizes the *fan-out* to the
//! live membership stream (`source_cluster_membership_stream`), filtered to
//! `Joined` ids and kept as a growing relation (no snapshot), but makes **no**
//! EC claim: a single hop cannot guarantee "every element reaches every member
//! that ever joins" if the sender crashes mid-broadcast. That guarantee — and
//! the one trusted EC assertion — belongs to the layer that supplies it:
//! [`crate::reliable_broadcast::reliable_broadcast_live`], whose echo cycle
//! discharges delivery even across sender failure.
//!
//! Contrast [`Stream::broadcast`], which `use::snapshot`s the membership set
//! inside a `sliced!` block. Snapshotting freezes one side of the data × members
//! join, so `member` deltas never re-fire and late joiners are missed. Joining
//! against the live relation keeps the delta symmetry: a new `Joined` id
//! re-fires the cross-product against all accumulated data, catching the joiner
//! up. That live re-firing is the mechanical property this module provides.
//!
//! # Retention caveat (bounds this to append-only data)
//!
//! Late-joiner catch-up needs *both* join sides retained. The symmetric-hash
//! join over two `Unbounded` inputs keeps them, so a late joiner still crosses
//! all prior data. Pruning the data side (bounded retention) breaks this — a
//! joiner arriving after a prune misses data.

use hydro_lang::live_collections::stream::{MinOrder, NoOrder, Ordering, Retries};
use hydro_lang::location::cluster::NoConsistency;
use hydro_lang::location::MemberId;
use hydro_lang::networking::NetworkFor;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Broadcasts elements of a cluster stream to all members of a destination
/// cluster over the live monotone membership relation. Mechanical fan-out
/// only: the output is `NoConsistency`. The EC assertion for reliable
/// delivery lives in [`crate::reliable_broadcast::reliable_broadcast_live`].
pub fn broadcast_live<'a, T, L, L2, C, O, R, N>(
    source: Stream<T, Cluster<'a, L, C>, Unbounded, O, R>,
    to: &Cluster<'a, L2>,
    via: N,
) -> KeyedStream<
    MemberId<L>,
    T,
    Cluster<'a, L2, NoConsistency>,
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
    // The live membership envelope of the destination cluster, observed at the
    // source: monotone (join ids only ever added), NOT snapshotted, honestly
    // `NoConsistency`. See `orchestrated_membership::member_envelope`.
    let members = crate::orchestrated_membership::member_envelope(
        source.location(),
        to,
        nondet!(/** dropped membership prefixes don't affect broadcast delivery */),
    );

    // Join data × live members at `NoConsistency` (both sides growing; the
    // symmetric-hash join re-fires on deltas to either input, so a late joiner
    // crosses all accumulated data), then demux. No consistency assertion:
    // demux's output is naturally `NoConsistency`.
    source
        .weaken_consistency()
        .cross_product(members)
        .map(q!(|(data, member_id)| (member_id, data)))
        .into_keyed()
        .demux(to, via)
}

/// Like [`broadcast_live`], but broadcasts from a [`Process`] source instead of a
/// cluster. Mirrors the two-impl split of `broadcast_closed`
/// ([`Stream::broadcast_closed`](hydro_lang::live_collections::stream::Stream)):
/// a process source has no source-member identity, so the output is a plain
/// `Stream` (not a `KeyedStream<MemberId<L>, ...>`).
///
/// The body is otherwise identical to [`broadcast_live`]: fan out over the
/// *live, monotone* membership relation (not a snapshot), so a member joining
/// late is caught up when the join re-fires the accumulated data. Output is
/// `NoConsistency`; no consistency assertion is made here.
pub fn broadcast_live_from_process<'a, T, L, L2, B, O, R, N>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    to: &Cluster<'a, L2>,
    via: N,
) -> Stream<T, Cluster<'a, L2, NoConsistency>, Unbounded, NoOrder, R>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    B: hydro_lang::live_collections::boundedness::Boundedness,
    O: Ordering + MinOrder<N::OrderingGuarantee>,
    R: Retries,
    N: NetworkFor<T>,
{
    let members = crate::orchestrated_membership::member_envelope(
        source.location(),
        to,
        nondet!(/** dropped membership prefixes don't affect broadcast delivery */),
    );

    source
        .cross_product(members)
        .map(q!(|(data, member_id)| (member_id, data)))
        .into_keyed()
        .demux(to, via)
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::keyed_stream::KeyedStream;
    use hydro_lang::location::cluster::NoConsistency;
    use hydro_lang::location::{Cluster, MemberId};
    use hydro_lang::prelude::*;

    use super::broadcast_live;

    /// Compile-time check that `broadcast_live`'s output is `NoConsistency`
    /// regardless of the network failure policy: it is a mechanical fan-out and
    /// makes no consistency claim. The EC assertion lives in
    /// `reliable_broadcast_live`, which supplies the echo cycle that actually
    /// justifies it.
    #[test]
    fn broadcast_live_output_is_no_consistency() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let source = flow.cluster::<()>();
        let to = flow.cluster::<()>();

        // An unbounded live source stream at each source member.
        let (_send, data) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (_send3, data3) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // fail_stop: still NoConsistency — no EC is minted at the fan-out layer.
        let _: KeyedStream<MemberId<()>, u32, Cluster<'_, (), NoConsistency>, _, _, _> =
            broadcast_live(data, &to, TCP.fail_stop().bincode());

        // plain lossy: NoConsistency as well.
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

    /// Process-source late-join catch-up (via `broadcast_live_from_process`).
    /// A process broadcasts three messages to a dynamically-joining cluster.
    /// Across every explored join timing — including a member joining only after
    /// all three messages exist — every member receives all three, because the
    /// live join re-fires the accumulated data. Non-cyclic, so the exhaustive
    /// search stays small.
    #[test]
    fn broadcast_live_from_process_late_joiner_catches_up() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        use super::broadcast_live_from_process;

        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let dest = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = broadcast_live_from_process(data, &dest, TCP.fail_stop().bincode())
            .sim_cluster_output();

        let instances = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&dest, 2)
            .with_dynamic_membership(&dest)
            .exhaustive(async || {
                in_send.send(10);
                in_send.send(20);
                in_send.send(30);

                // Every dest member delivers all three values, no matter when it
                // joined — a member that joins after a send is caught up by the
                // live join re-firing the accumulated data.
                for member in 0..2u32 {
                    let got: Vec<u32> = out_recv.collect_n_sorted(member, 3).await;
                    assert_eq!(got, vec![10, 20, 30], "member {member} missing messages");
                }
            });

        assert!(
            instances > 1,
            "expected multiple join timings to be explored, got {instances}"
        );
    }
}
