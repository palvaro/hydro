//! Reliable broadcast: if any correct member delivers a message, all correct
//! members eventually deliver it.
//!
//! # The property
//!
//! - **Validity**: If the sender is correct and broadcasts m, all correct
//!   members eventually deliver m.
//! - **Agreement**: If ANY correct member delivers m, then ALL correct members
//!   eventually deliver m.
//!
//! Agreement is the hard part. It means that even if the sender crashes
//! mid-broadcast (delivering to some members but not others), and even if
//! some of THOSE members also crash mid-forward, as long as any one correct
//! member received m, all correct members will eventually get it.
//!
//! # The canonical solution
//!
//! Upon receiving m for the first time (from ANY source — sender or another
//! member), re-broadcast m to all members, then deliver m locally.
//!
//! This creates a cycle: member A receives m, re-broadcasts to B and C.
//! B receives m from A, re-broadcasts to A and C. The cycle terminates
//! because deduplication suppresses re-processing of already-seen messages.
//! But it IS a dataflow cycle: the re-broadcast output depends on received
//! re-broadcasts from others.
//!
//! # EC Inference
//!
//! Despite the cycle, EC is **fully inferred** — no `manual_proof!` needed.
//! The trick: declare the `forward_ref` on the EC-typed location obtained
//! from `broadcast_closed`'s output (which infers EC from the network policy).
//! Since the completing stream also comes from `broadcast_closed` (same policy),
//! it matches the forward_ref's EC location type. The type checker is satisfied:
//!
//! 1. `initial` = `broadcast_closed` (Process→Cluster) → EC inferred
//! 2. `forward_ref` declared on `initial.location()` → EC location
//! 3. `merge_unordered(initial, rebroadcast_fwd)` → both EC, merge preserves EC
//! 4. `unique()` → EC preserved
//! 5. `broadcast_closed` (Cluster→Cluster) on new_messages → EC inferred (output)
//! 6. `complete(echo)` → echo is EC, forward_ref expects EC → types match
//!
//! The key insight: `broadcast_closed` *establishes* EC from the network policy
//! regardless of input consistency. So both the initial broadcast and the
//! re-broadcast independently produce EC streams, and the forward_ref can be
//! declared at EC because it will always be completed by an EC stream.

use hydro_lang::live_collections::boundedness::Boundedness;
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::hash::Hash;

/// Reliable broadcast from a Process to a Cluster.
///
/// Implements the canonical echo-based reliable broadcast protocol.
/// Every member that receives a message (from the sender OR from another
/// member's re-broadcast) forwards it to all members before delivering.
/// Deduplication ensures each message is delivered exactly once.
///
/// # EC inference
///
/// The output carries `EventualConsistency` **fully inferred** by the type
/// system — no `manual_proof!` needed. The key trick: the `forward_ref` is
/// declared on the EC-typed location from `broadcast_closed`'s output, and
/// the completing stream also comes from `broadcast_closed` (same network
/// policy), so the types match around the cycle.
///
/// # Cost
///
/// O(n²) messages per input message (every member re-broadcasts to every
/// member). Dedup ensures exactly-once delivery despite redundancy.
pub fn reliable_broadcast_closed<
    'a,
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L,
    L2: 'a,
    B: Boundedness,
    O: hydro_lang::live_collections::stream::Ordering,
    R: hydro_lang::live_collections::stream::Retries,
>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    cluster: &Cluster<'a, L2>,
) -> Stream<T, Cluster<'a, L2, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>
where
    O: hydro_lang::live_collections::stream::MinOrder<
            hydro_lang::live_collections::stream::TotalOrder,
        >,
{
    reliable_broadcast_closed_with_echoes(source, cluster).0
}

/// [`reliable_broadcast_closed`], with the echo stream exported: returns
/// `(deliveries, echoes)`, where `echoes` is keyed by the echoing member.
///
/// The wide interface is what makes the echo cycle *reusable* (CGR-style
/// module composition): an echo is an attestation "member m holds this
/// message", which is exactly the input a quorum certificate mint wants.
/// [`uniform_reliable_broadcast_closed`](crate::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed)
/// is a five-line client of this function.
pub fn reliable_broadcast_closed_with_echoes<
    'a,
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L,
    L2: 'a,
    B: Boundedness,
    O: hydro_lang::live_collections::stream::Ordering,
    R: hydro_lang::live_collections::stream::Retries,
>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    cluster: &Cluster<'a, L2>,
) -> (
    Stream<T, Cluster<'a, L2, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>,
    hydro_lang::live_collections::keyed_stream::KeyedStream<
        hydro_lang::location::MemberId<L2>,
        T,
        Cluster<'a, L2, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
)
where
    O: hydro_lang::live_collections::stream::MinOrder<
            hydro_lang::live_collections::stream::TotalOrder,
        >,
{
    // Step 1: Initial broadcast from process to cluster. EC inferred.
    let initial = source.broadcast_closed(cluster, TCP.fail_stop().bincode());

    // Soundness of EC on this cycle:
    // 1. initial is already EC (earned from broadcast_closed + fail_stop).
    // 2. We only connect EC streams to the forward_ref (echo also goes
    //    through broadcast_closed + fail_stop).
    // 3. Any op that introduces per-member nondeterminism (batch, sample_every,
    //    etc.) returns L::DropConsistency, which would break the cycle types.
    //    The ops we use (merge, unique, clone) don't take nondet!, so they
    //    preserve L — and the type system would reject anything that doesn't.
    let (rebroadcast_handle, rebroadcast_fwd) = initial
        .location()
        .forward_ref::<Stream<T, _, Unbounded, NoOrder>>();

    // Step 2: Merge initial delivery with re-broadcasts from other members.
    // Both are EC (same location type) — merge preserves EC.
    let all_received = initial.merge_unordered(rebroadcast_fwd);

    // Step 3: Deduplicate — only process each message once. EC preserved.
    let new_messages = all_received.unique();

    // Step 4: Re-broadcast new messages to all members (the echo step),
    // KEEPING the keying by echoer. broadcast_closed infers EC — matches the
    // forward_ref's EC location.
    let echo = new_messages
        .clone()
        .broadcast_closed(cluster, TCP.fail_stop().bincode());

    // Close the cycle. echo is EC, forward_ref is EC — types match.
    rebroadcast_handle.complete(echo.clone().values());

    // Step 5: Deliver. new_messages is EC throughout. No manual_proof!
    (new_messages, echo)
}

/// Reliable broadcast whose re-broadcast (echo) step fans out over the **live,
/// monotone** membership relation via
/// [`broadcast_live`](crate::ec_inference_demos::broadcast_live::broadcast_live),
/// instead of static `broadcast_closed`.
///
/// This is the key M2 validation: it swaps *only* the cyclic echo step onto
/// `broadcast_live` and leaves the `forward_ref` cycle otherwise identical to
/// [`reliable_broadcast_closed`]. If EC still infers around the cycle, then
/// `broadcast_live` is a genuine drop-in generalization of `broadcast_closed`:
///
/// - If membership turns out fixed, the live relation simply stops growing at
///   the full set and this delivers exactly what `broadcast_closed` would — so
///   it "just works," now over a dynamic-membership-capable primitive.
/// - The `forward_ref` is still declared on an EC location, and the completing
///   `echo` still passes through an EC-earning broadcast (now `broadcast_live` +
///   `fail_stop`), so the cycle types match with no `manual_proof!` on
///   consistency here — the single trusted step lives inside `broadcast_live`.
///
/// The initial process→cluster broadcast uses
/// [`broadcast_live_from_process`](crate::ec_inference_demos::broadcast_live::broadcast_live_from_process),
/// so a member that joins after the initial send is caught up by the echo.
pub fn reliable_broadcast_live<
    'a,
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    B: Boundedness,
    O: hydro_lang::live_collections::stream::Ordering,
    R: hydro_lang::live_collections::stream::Retries,
>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    cluster: &Cluster<'a, L2>,
) -> Stream<T, Cluster<'a, L2, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>
where
    O: hydro_lang::live_collections::stream::MinOrder<
            hydro_lang::live_collections::stream::TotalOrder,
        >,
{
    // Step 1: Initial broadcast from process to cluster over the LIVE membership
    // relation, so a member that joins after the send is caught up by the echo.
    let initial = crate::ec_inference_demos::broadcast_live::broadcast_live_from_process(
        source,
        cluster,
        TCP.fail_stop().bincode(),
    );

    // forward_ref on the EC location produced by the initial broadcast.
    let (rebroadcast_handle, rebroadcast_fwd) = initial
        .location()
        .forward_ref::<Stream<T, _, Unbounded, NoOrder>>();

    // Step 2: Merge initial delivery with re-broadcasts. Both EC → merge preserves EC.
    let all_received = initial.merge_unordered(rebroadcast_fwd);

    // Step 3: Deduplicate. EC preserved.
    let new_messages = all_received.unique();

    // Step 4: Re-broadcast newly-seen messages over the LIVE membership relation.
    // `broadcast_live` + fail_stop earns EC on delivery — matching the EC
    // forward_ref location, closing the cycle with no consistency manual_proof!.
    let echo = crate::ec_inference_demos::broadcast_live::broadcast_live(
        new_messages.clone(),
        cluster,
        TCP.fail_stop().bincode(),
    )
    .values();

    // Close the cycle. echo is EC, forward_ref is EC — types match.
    rebroadcast_handle.complete(echo);

    // Step 5: Deliver. new_messages is EC throughout. No manual_proof!
    new_messages
}

#[cfg(test)]
mod tests {
    use hydro_lang::prelude::*;

    use super::{reliable_broadcast_closed, reliable_broadcast_live};

    /// Baseline: `reliable_broadcast_closed` (static membership) delivers the
    /// broadcast message to every cluster member.
    #[test]
    fn reliable_broadcast_closed_delivers_to_all() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = reliable_broadcast_closed(data, &cluster).sim_cluster_output();

        let count = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async || {
                in_send.send(42);
                // Every one of the 3 members must deliver the broadcast value.
                for member in 0..3u32 {
                    let got: Vec<u32> = out_recv.collect_n_sorted(member, 1).await;
                    assert_eq!(got, vec![42], "member {member} did not deliver 42");
                }
            });
        // Static RB explores exactly 1 execution even at 3 nodes: fail-stop
        // delivery is deterministic and the echo cycle does not fork the search.
        // (Contrast the dynamic-membership case, whose blowup is entirely the
        // membership hook — see `reliable_broadcast_live_*`.)
        assert_eq!(count, 1, "static RB should explore a single execution");
    }

    /// Full-protocol late-join catch-up (M2 payoff). Both the initial broadcast
    /// and the echo fan out over *live* membership (`broadcast_live_from_process`
    /// and `broadcast_live`), so a member that joins after the initial send genuinely
    /// misses it and must be caught up by another member's echo re-broadcast — the
    /// reliable-broadcast guarantee, now over dynamic membership. Also exercises the
    /// cluster-observed hook-keying branch (the echo observes the cluster's own
    /// membership, one `MembershipHook` per member).
    ///
    /// Runs `exhaustive` at 3 nodes. This is tractable because the
    /// `MembershipHook` treats membership as an ordinary unordered stream: it
    /// forks only on join *timing* (release the next member now, or defer it),
    /// never on the *order* in which members are released — member order is
    /// unobservable for a symmetric fan-out, so forking on it was pure redundancy
    /// that previously multiplied the search to millions of executions. With that
    /// removed, every genuine join-vs-message interleaving is still explored.
    #[test]
    fn reliable_broadcast_live_delivers_under_dynamic_membership() {
        use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = reliable_broadcast_live(data, &cluster).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .with_dynamic_membership(&cluster)
            .exhaustive(async || {
                in_send.send(42);
                for member in 0..3u32 {
                    let got: Vec<u32> = out_recv.collect_n_sorted(member, 1).await;
                    assert_eq!(got, vec![42], "member {member} did not deliver 42");
                }
            });
    }

    /// **The distinguishing fault, part 1: plain broadcast is not reliable
    /// broadcast.** In every crash-free execution, `broadcast_closed` and
    /// `reliable_broadcast_closed` are observationally identical — the echo cycle
    /// is pure redundancy. The *only* content of RB's agreement property is what
    /// happens when the sender crashes mid-broadcast, which is exactly the state
    /// crash injection explores: a per-recipient prefix of the sender's sends
    /// survives.
    ///
    /// This test pins that plain `broadcast_closed` **violates agreement** under
    /// a sender crash: the exhaustive search must find an execution where one
    /// member delivered the message and the other never will (the sender is dead;
    /// nobody echoes). Contrast [`reliable_broadcast_closed_agreement_under_sender_crash`],
    /// which runs the identical fault against the echo protocol and finds no such
    /// execution.
    #[test]
    fn broadcast_closed_violates_agreement_under_sender_crash() {
        use hydro_lang::location::MemberId;

        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();
        let node = flow.process::<()>();

        let out_recv = sender
            .source_iter(q!(vec![42u32]))
            .broadcast_closed(&cluster, TCP.fail_stop().bincode())
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let mut saw_agreement_violation = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 2)
            .with_crashable_process(&sender)
            .exhaustive(async || {
                let received: Vec<(MemberId<()>, u32)> = out_recv.collect_sorted().await;
                let delivered: Vec<bool> = (0..2u32)
                    .map(|m| received.contains(&(MemberId::from_raw_id(m), 42)))
                    .collect();

                if delivered[0] != delivered[1] {
                    saw_agreement_violation = true;
                }
                if delivered[0] && delivered[1] {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_agreement_violation,
            "plain broadcast should violate agreement under a sender crash: some execution \
             must deliver to one member but never the other"
        );
        assert!(
            saw_full_delivery,
            "sanity: the crash-free execution delivers to everyone"
        );
    }

    /// **The distinguishing fault, part 2: the echo cycle is load-bearing.** The
    /// identical harness and fault as
    /// [`broadcast_closed_violates_agreement_under_sender_crash`] — sender
    /// crashes mid-broadcast, reaching a nondeterministic per-member prefix — but
    /// through `reliable_broadcast_closed`. Agreement now holds in **every**
    /// explored execution: if any member delivers 42, its re-broadcast (the
    /// members never crash) delivers it to everyone; if the sender dies before
    /// reaching anyone, nobody delivers, which agreement permits.
    ///
    /// This is the first test in which the echo cycle does observable work: under
    /// static membership and no faults, static RB explores a single execution
    /// identical to plain broadcast (see `reliable_broadcast_closed_delivers_to_all`).
    #[test]
    fn reliable_broadcast_closed_agreement_under_sender_crash() {
        use hydro_lang::location::MemberId;

        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();
        let node = flow.process::<()>();

        let source = sender.source_iter(q!(vec![42u32]));

        let out_recv = reliable_broadcast_closed(source, &cluster)
            .send(&node, TCP.fail_stop().bincode())
            .entries()
            .sim_output();

        let mut saw_full_delivery = false;
        let mut saw_no_delivery = false;

        let count = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 2)
            .with_crashable_process(&sender)
            .exhaustive(async || {
                let received: Vec<(MemberId<()>, u32)> = out_recv.collect_sorted().await;
                let delivered: Vec<bool> = (0..2u32)
                    .map(|m| received.contains(&(MemberId::from_raw_id(m), 42)))
                    .collect();

                // AGREEMENT, in every execution: if any member delivers, all do.
                assert_eq!(
                    delivered[0], delivered[1],
                    "reliable broadcast must not let members diverge under a sender crash \
                     (delivered: {delivered:?})"
                );

                if delivered[0] && delivered[1] {
                    saw_full_delivery = true;
                }
                if !delivered[0] && !delivered[1] {
                    saw_no_delivery = true;
                }
            });

        assert!(
            saw_full_delivery,
            "some execution (e.g. crash-free) delivers to everyone"
        );
        assert!(
            saw_no_delivery,
            "some execution (sender dies before reaching anyone) delivers to no one — \
             which agreement permits"
        );
        assert!(
            count > 1,
            "expected the crash hook to fork the search, got {count} execution(s)"
        );
    }
}
