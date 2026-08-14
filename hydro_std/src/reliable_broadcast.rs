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
