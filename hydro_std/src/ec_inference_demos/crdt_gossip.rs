//! State-based CRDT gossip with EC inferred from transport.
//!
//! Same trick as `reliable_broadcast_closed`: broadcast_closed earns EC,
//! forward_ref lives on that EC location, the cycle closes cleanly.
//! The difference: we re-broadcast the *folded state*, not raw elements.

use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;

use hydro_lang::live_collections::stream::{AtLeastOnce, NoOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// G-Set CRDT via state-based gossip. EC fully inferred, no manual_proof on consistency.
pub fn g_set_gossip<
    'a,
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'static,
    L2: 'a,
>(
    cluster: &Cluster<'a, L2>,
    local_updates: Stream<T, Cluster<'a, L2>, Unbounded, NoOrder>,
) -> Singleton<HashSet<T>, Cluster<'a, L2, EventualConsistency>, Unbounded>
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
        initial.location().forward_ref::<Stream<HashSet<T>, _, Unbounded, NoOrder, AtLeastOnce>>();

    // Step 2: Merge initial element stream with full-state gossip from peers (flattened).
    let all_elements = initial.merge_unordered(gossip_from_peers.flatten_unordered());

    // Step 3: Fold into G-Set state. Input is EC + NoOrder → commutative proof required.
    let state = all_elements.fold(
        q!(|| HashSet::new()),
        q!(|set, v| { set.insert(v); },
           commutative = manual_proof!(/** set insert is commutative */),
           idempotent = manual_proof!(/** set insert is idempotent */)),
    );

    // Step 4: Periodically re-broadcast state to all peers.
    // sample_every drops consistency, but broadcast_closed earns it back fresh.
    let echo = state
        .clone()
        .sample_every(
            q!(Duration::from_millis(100)),
            nondet!(/** gossip interval — performance, not correctness */),
        )
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
        .values();

    // Close the cycle. echo is EC, forward_ref is EC — types match.
    gossip_handle.complete(echo);

    state
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
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'static,
    L2: 'a,
>(
    cluster: &Cluster<'a, L2>,
    local_updates: Stream<T, Cluster<'a, L2>, Unbounded, NoOrder>,
) -> Singleton<HashSet<T>, Cluster<'a, L2, EventualConsistency>, Unbounded>
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
        initial.location().forward_ref::<Stream<HashSet<T>, _, Unbounded, NoOrder, AtLeastOnce>>();

    // Step 2: Merge fresh elements with full-state gossip from peers.
    let all_elements = initial.merge_unordered(gossip_from_peers.flatten_unordered());

    // Step 3: Fold into G-Set state. ACI proofs are about the combiner, not EC.
    let state = all_elements.fold(
        q!(|| HashSet::new()),
        q!(|set, v| { set.insert(v); },
           commutative = manual_proof!(/** set insert is commutative */),
           idempotent = manual_proof!(/** set insert is idempotent */)),
    );

    // Step 4: Periodically re-gossip state over the live relation.
    // sample_every downgrades; the fan-out rule re-mints EC fresh.
    let echo = fan_out(
        state.clone().sample_every(
            q!(Duration::from_millis(100)),
            nondet!(/** gossip interval — performance, not correctness */),
        ),
        MembershipView::live(cluster, cluster),
        cluster,
        TCP.fail_stop().bincode(),
    )
    .values();

    // Close the cycle coinductively: echo is EC, forward_ref is EC.
    gossip_handle.complete(echo);

    state
}
