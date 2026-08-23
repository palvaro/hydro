//! Compile-fail examples demonstrating that the EC-inferred gossip
//! pattern is sound — the type system prevents the dangerous cases.
//!
//! These are NOT runnable tests. They document what the type system
//! rejects and why.

// ═══════════════════════════════════════════════════════════════════════════
// ABUSE 1: Trying to complete a forward_ref with a NoConsistency stream
// ═══════════════════════════════════════════════════════════════════════════
//
// The forward_ref is created on an EC location:
//     initial.location().forward_ref::<Stream<T, _, Unbounded, NoOrder>>()
//
// initial.location() is Cluster<'a, L2, EventualConsistency>.
// So the forward_ref produces:
//     ForwardHandle<'a, Stream<T, Cluster<'a, L2, EC>, Unbounded, NoOrder>>
//
// If you try to complete it with a stream on Cluster<'a, L2, NoConsistency>,
// the types don't unify:
//
//     let bad_stream: Stream<T, Cluster<'a, L2, NoConsistency>, ...> = ...;
//     rebroadcast_handle.complete(bad_stream);
//     //                          ^^^^^^^^^^
//     //  ERROR: expected Stream<_, Cluster<'_, L2, EventualConsistency>, ...>
//     //            found Stream<_, Cluster<'_, L2, NoConsistency>, ...>
//
// The `complete` method requires `impl Into<C>` where C is the exact type
// the forward_ref was created with. Location types are INVARIANT in the
// consistency parameter — Cluster<L2, EC> ≠ Cluster<L2, NoConsistency>.


// ═══════════════════════════════════════════════════════════════════════════
// ABUSE 2: Trying to use a lossy network in the echo step
// ═══════════════════════════════════════════════════════════════════════════
//
// If you change the echo broadcast to use TCP.lossy():
//
//     let echo = new_values
//         .broadcast_closed(cluster, TCP.lossy(nondet!(...)).bincode())
//         .values();
//     //  echo: Stream<T, Cluster<'a, L2, NoConsistency>, ...>
//
//     rebroadcast_handle.complete(echo);
//     //  ERROR: NoConsistency ≠ EventualConsistency
//
// The network's ConsistencyGuarantee flows into broadcast_closed's return
// type. Lossy → NoConsistency. The forward_ref expects EC. Type mismatch.
// You CANNOT close the cycle with a weaker network.


// ═══════════════════════════════════════════════════════════════════════════
// ABUSE 3: Trying to merge an EC stream with a non-EC stream
// ═══════════════════════════════════════════════════════════════════════════
//
// merge_unordered requires both sides to have the SAME location type L:
//
//     pub fn merge_unordered(self, other: Stream<T, L, ...>) -> Stream<T, L, ...>
//
// If `initial` is on Cluster<EC> and you try to merge with something on
// Cluster<NoConsistency>:
//
//     let no_consistency_stream: Stream<T, Cluster<'a, L2, NoConsistency>, ...> = ...;
//     initial.merge_unordered(no_consistency_stream);
//     //  ERROR: mismatched types
//     //  expected Cluster<'_, L2, EventualConsistency>
//     //  found    Cluster<'_, L2, NoConsistency>
//
// You cannot "launder" a NoConsistency stream into an EC stream by merging.
// The location type is the same on both sides of merge_unordered.


// ═══════════════════════════════════════════════════════════════════════════
// ABUSE 4: Trying to bootstrap EC from nothing (no broadcast)
// ═══════════════════════════════════════════════════════════════════════════
//
// You can't create a forward_ref on an EC cluster without first having
// something on an EC cluster. The plain `cluster` parameter is
// Cluster<'a, L2> which is Cluster<'a, L2, NoConsistency>.
//
//     cluster.forward_ref::<Stream<T, _, Unbounded, NoOrder>>();
//     // This gives ForwardHandle<Stream<T, Cluster<L2, NoConsistency>, ...>>
//     // NOT EventualConsistency!
//
// The ONLY way to get an EC location is:
// (a) broadcast_closed with a fail_stop/lossy_delayed_forever network
// (b) assert_has_consistency_of (requires manual_proof — explicit human assertion)
// (c) broadcast_from_member with fail_stop
//
// Our pattern uses (a). The EC is EARNED by the broadcast, not conjured.


// ═══════════════════════════════════════════════════════════════════════════
// ABUSE 5: Trying to skip the dedup and get EC for free
// ═══════════════════════════════════════════════════════════════════════════
//
// If you remove the `unique()` step, the pattern still compiles and is
// still EC. Is this unsound?
//
// NO — the EC guarantee is about all members eventually seeing the same
// set of elements. Whether you deduplicate or not doesn't affect that.
// Duplicates are a performance issue, not a consistency issue.
// For a G-Set CRDT, `set.insert(x)` is idempotent so duplicates are
// absorbed harmlessly.
//
// The soundness argument:
// - broadcast_closed guarantees every live member receives every element
// - The echo step re-broadcasts to compensate for sender failure mid-broadcast
// - With or without dedup, all live members converge


// ═══════════════════════════════════════════════════════════════════════════
// SUMMARY: Where is the trusted boundary?
// ═══════════════════════════════════════════════════════════════════════════
//
// broadcast_closed uses `assert_has_consistency_of_trusted(manual_proof!(...))`.
// This is the ONE place where the library "puts its thumb on the scale."
// It's inside hydro_lang itself — user code cannot call _trusted variants.
//
// The trusted assertion says: "given this network's failure policy delivers
// the same messages to all live members, broadcast produces EC output."
//
// Everything downstream of that is MECHANICALLY ENFORCED:
// - forward_ref types must match (Location invariance)
// - merge_unordered requires same Location
// - complete() requires exact type match
// - broadcast_closed's return type is determined by the network policy
//
// So the pattern is NOT "putting your thumb on the scale." It is:
// 1. One trusted axiom inside the library (broadcast_closed + fail_stop → EC)
// 2. All user-level composition is type-checked — no escape hatches
// 3. The cycle closes ONLY if every piece independently produces EC
//
// The user's `manual_proof!` on commutativity is a separate obligation —
// it's about the FOLD being order-independent, not about consistency.
// Even if the fold were wrong (non-commutative), the stream would still
// be EC — it would just produce non-deterministic values. EC ≠ determinism.
// EC means "all members agree." Commutativity means "the agreed-upon value
// doesn't depend on message ordering."
