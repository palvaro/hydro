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
use serde::de::DeserializeOwned;
use serde::Serialize;
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
    let (rebroadcast_handle, rebroadcast_fwd) =
        initial.location().forward_ref::<Stream<T, _, Unbounded, NoOrder>>();

    // Step 2: Merge initial delivery with re-broadcasts from other members.
    // Both are EC (same location type) — merge preserves EC.
    let all_received = initial.merge_unordered(rebroadcast_fwd);

    // Step 3: Deduplicate — only process each message once. EC preserved.
    let new_messages = all_received.unique();

    // Step 4: Re-broadcast new messages to all members (the echo step).
    // broadcast_closed infers EC — matches the forward_ref's EC location.
    let echo = new_messages
        .clone()
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
        .values();

    // Close the cycle. echo is EC, forward_ref is EC — types match.
    rebroadcast_handle.complete(echo);

    // Step 5: Deliver. new_messages is EC throughout. No manual_proof!
    new_messages
}

/// Reliable broadcast whose fan-out (initial and echo) goes over the **live,
/// monotone** membership relation via [`broadcast_live`], instead of static
/// `broadcast_closed`.
///
/// - If membership turns out fixed, the live relation simply stops growing at
///   the full set and this delivers exactly what `broadcast_closed` would.
/// - A member that joins after the initial send is caught up by the echo: the
///   live join re-fires accumulated messages on each `Joined` delta.
///
/// # The single trusted EC step lives HERE, not in `broadcast_live`
///
/// `broadcast_live` is a mechanical `NoConsistency` fan-out — a single hop
/// cannot promise delivery to every eventual member if the sender crashes
/// mid-broadcast. The echo cycle is what discharges that: every member that
/// receives a message re-broadcasts it over live membership before delivering.
/// The whole cycle therefore runs at `NoConsistency`, and EC is asserted
/// exactly once, on the delivered stream, by the `manual_proof!` below. That
/// axiom is an unchecked human claim; it additionally rests on a *coverage*
/// premise — each joiner is eventually known to at least one live node holding
/// the message log — supplied (unverified) by the membership substrate.
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
    // NoConsistency: broadcast_live makes no consistency claim.
    let initial = crate::broadcast_live::broadcast_live_from_process(
        source,
        cluster,
        TCP.fail_stop().bincode(),
    );

    // forward_ref on the NoConsistency location; the entire cycle runs at
    // NoConsistency, no type-level EC trick needed to close it.
    let (rebroadcast_handle, rebroadcast_fwd) =
        initial.location().forward_ref::<Stream<T, _, Unbounded, NoOrder>>();

    // Step 2: Merge initial delivery with re-broadcasts from other members.
    let all_received = initial.merge_unordered(rebroadcast_fwd);

    // Step 3: Deduplicate — only process each message once.
    let new_messages = all_received.unique();

    // Step 4: Re-broadcast newly-seen messages over the LIVE membership relation.
    let echo = crate::broadcast_live::broadcast_live(
        new_messages.clone(),
        cluster,
        TCP.fail_stop().bincode(),
    )
    .values();

    // Close the cycle (both sides NoConsistency).
    rebroadcast_handle.complete(echo);

    // Step 5: Deliver, asserting EC exactly once — the single trusted step.
    new_messages.assert_has_consistency_of::<Cluster<'a, L2, EventualConsistency>>(manual_proof!(
        /// UNCHECKED AXIOM (coverage-based): assuming each joiner is eventually
        /// known to at least one live node holding the message log, the echo
        /// cycle (every receiver re-broadcasts over live monotone membership
        /// before delivering, with fail_stop networking) ensures every message
        /// eventually reaches every member that ever joins — even if the
        /// original sender crashes mid-broadcast. All members therefore
        /// materialize the same delivered set in the limit. Nothing verifies
        /// this claim; the coverage premise itself is an assumption about the
        /// membership substrate (no join may be dropped by ALL observers).
    ))
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

        let count = flow.sim()
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
    /// Uses `fuzz` rather than `exhaustive`. Measured cause: static RB explores
    /// exactly 1 execution even at 3 nodes (fail-stop delivery is deterministic;
    /// the echo cycle doesn't fork the search). Turning on dynamic membership
    /// jumps a *2-node* run to 294 executions — so the blowup is entirely the
    /// `MembershipHook`, which forks join timing on every scheduler round of the
    /// echo cycle rather than only at observable points. 294 is already far more
    /// than the handful of genuinely-distinct join orderings, and it compounds
    /// with cluster size, so n=3 exhaustive is impractical *today*. This is a hook
    /// defect, not a property of RB or of exhaustive search: a hook that forked
    /// only when releasing a member changes an observable outcome would very
    /// likely make n=3 exhaustive tractable. `fuzz` covers the 3-node model in the
    /// meantime; single executions always terminate (verified).
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
            .fuzz(async || {
                in_send.send(42);
                for member in 0..3u32 {
                    let got: Vec<u32> = out_recv.collect_n_sorted(member, 1).await;
                    assert_eq!(got, vec![42], "member {member} did not deliver 42");
                }
            });
    }
}
