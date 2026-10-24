# Design Document: Paxos-EC

## Overview

Paxos-EC is a consensus protocol for Hydro whose `EventualConsistency` on the committed log is **inferred by the type system** rather than asserted via `manual_proof!`. It achieves the same output signature as Raft:

```rust
Stream<LogEntry<T>, Atomic<Cluster<'a, Tag, EventualConsistency>>, Unbounded, TotalOrder>
```

The key insight: every broadcast in the protocol uses `broadcast_from_member` + `fail_stop`, giving EC by inference. The committed log is derived from EC streams via deterministic transforms, so EC propagates automatically. The only `manual_proof!` is the safety argument: no two valid certificates can commit conflicting values for the same slot (Paxos's core invariant, proved by quorum intersection).

## Core Principle: EC Propagation

The type system tracks `EventualConsistency` through the dataflow:

- **EC in, deterministic transform → EC out.** No exploration needed — if the input agrees across members, the output agrees by construction.
- **EC in, nondeterministic transform → NoConsistency out.** The type system strips EC at any `ObserveNonDet` boundary.
- **NoConsistency in, EC broadcast out → EC out.** A `broadcast_from_member` + `fail_stop` always produces EC, regardless of input consistency — because the broadcast guarantees every member receives the same data from that sender.

The protocol is structured so that every transition from NoConsistency back to EC happens at a broadcast boundary. The quorum logic (inherently non-deterministic — it depends on which acks arrive when) lives entirely in the leader's local processing between receiving p2p acks and emitting the next EC broadcast.

## Design: Certificate-Carrying EC Broadcasts

### Motivation

In classical Paxos descriptions, a quorum is an ephemeral event witnessed by the leader. The leader "knows" it got a majority. This makes EC hard to infer — the knowledge is local.

We make quorums portable by reifying them as **certificates**: data structures that prove a quorum was achieved. Certificates are carried on EC broadcast streams. Any member receiving a certificate can verify it. This makes the protocol data-centric rather than leader-centric.

### The Streams

```
┌─────────────────────────────────────────────────────────┐
│ Phase 1: Prepare                                        │
│                                                         │
│   proposals ──── EC broadcast (leader → all)            │
│       │                                                 │
│       ├── phase1_acks ── p2p to leader (NoConsistency)  │
│       │         (promise + previously accepted values)  │
│       ▼                                                 │
│   Leader assembles Phase1Certificate                    │
│                                                         │
├─────────────────────────────────────────────────────────┤
│ Phase 2: Accept                                         │
│                                                         │
│   accepts ──── EC broadcast (leader → all)              │
│       │        carries Phase1Certificate                │
│       │                                                 │
│       ├── phase2_acks ── p2p to leader (NoConsistency)  │
│       │         (I accepted this ballot for this slot)  │
│       ▼                                                 │
│   Leader assembles CommitCertificate                    │
│                                                         │
├─────────────────────────────────────────────────────────┤
│ Commit                                                  │
│                                                         │
│   commits ──── EC broadcast (leader → all)              │
│       │        carries CommitCertificate                │
│       ▼                                                 │
│   committed_log ── deterministic derivation (sort by    │
│                    slot, dedup) → EC + TotalOrder        │
└─────────────────────────────────────────────────────────┘
```

### Derived Collections

| Collection | Type | Derivation | Consistency |
|---|---|---|---|
| `proposals` | `Stream<Prepare, Cluster<EC>>` | `broadcast_from_member` + fail_stop | EC (inferred) |
| `maxBallot` | `KeyedSingleton<Slot, Ballot, Cluster<EC>>` | Monotonic max over `proposals` ∪ `accepts` | EC (propagation: deterministic monotonic fn of EC input) |
| `accepts` | `Stream<Accept<T>, Cluster<EC>>` | `broadcast_from_member` + fail_stop | EC (inferred) |
| `acceptedProposal` | `KeyedSingleton<Slot, Ballot, Cluster<EC>>` | Max ballot accepted per slot, from `accepts` | EC (propagation: monotonic max of EC) |
| `acceptedValue` | `KeyedSingleton<Slot, Value, Cluster<NoConsistency>>` | Value associated with max accepted ballot | **NoConsistency** (non-monotonic: overwritten by higher ballots; same acceptor may accept many conflicting values over time) |
| `commits` | `Stream<Commit<T>, Cluster<EC>>` | `broadcast_from_member` + fail_stop | EC (inferred) |
| `committed_log` | `Stream<LogEntry<T>, Atomic<Cluster<EC>>, TotalOrder>` | Deterministic: dedup by slot, emit in slot order | EC (propagation) + TotalOrder (structural: slot ordering) |

### Certificate Data Structures

```rust
/// Proof that a quorum promised ballot B for slot S.
/// Any holder can verify: promises.len() >= quorum_size.
struct Phase1Certificate<T> {
    slot: Slot,
    ballot: Ballot,
    /// Each respondent's promise: their max previously-accepted (ballot, value)
    /// for this slot, if any. The leader must re-propose the highest such value.
    promises: Vec<(MemberId, Option<(Ballot, T)>)>,
}

/// Proof that a quorum accepted (slot, ballot, value).
/// Any holder can verify: acceptors.len() >= quorum_size.
struct CommitCertificate {
    slot: Slot,
    ballot: Ballot,
    value_hash: Hash,  // or carry the value directly
    acceptors: Vec<MemberId>,
}
```

### The p2p (Non-EC) Streams

The ack streams are point-to-point, non-EC, and ephemeral:

```rust
// Phase 1: member → candidate leader
phase1_acks: Stream<Promise<T>, Cluster<NoConsistency>, Unbounded, NoOrder>

// Phase 2: member → leader
phase2_acks: Stream<AcceptAck, Cluster<NoConsistency>, Unbounded, NoOrder>
```

These are consumed by the leader inside a `sliced!` block that accumulates until quorum, then emits a certificate onto an EC broadcast. The type system sees:

```
NoConsistency input → (opaque accumulation) → data → broadcast_from_member → EC output
```

The transition from NoConsistency to EC happens at the broadcast boundary. The quorum logic doesn't need to be typed — it's just the code that decides *when* to broadcast and *what certificate to include*.

### Batching and Nondeterminism

A critical insight: **batching affects what is chosen, NOT agreement.**

Moving a batch boundary may change which proposal wins a slot (a later proposal might arrive in the same batch vs. the next). But it cannot cause two conflicting values to both be committed for the same slot. The quorum intersection argument is batch-independent.

The `nondet!` annotations on batch boundaries acknowledge that the *specific* committed values depend on scheduling, but the *agreement property* (all members commit the same value for each slot) does not.

## EC Inference Chain

The committed log's EC is inferred through this chain:

1. `commits` stream is EC — **inferred** from `broadcast_from_member` + fail_stop
2. `committed_log` = dedup by slot + sort by slot number — **deterministic** function of EC `commits`
3. Deterministic function of EC → EC — **propagation rule**

No `manual_proof!` on EC anywhere. The type system does it all.

## Where `manual_proof!` Lives

**Exactly one `manual_proof!`:** the safety argument.

The safety claim: no two valid `CommitCertificate`s for the same slot carry different values.

Justification (prose, not mechanically checked):
> Phase 1 forces any leader with ballot B' > B to learn (from a quorum of promises)
> the highest previously-accepted value for the slot. By quorum intersection, at
> least one member in the new leader's quorum also accepted the old leader's value.
> The new leader must re-propose that value. Therefore all committed values for
> the same slot are identical.

This is Paxos's fundamental invariant. It's the one thing the type system cannot derive structurally.

## Comparison with Existing Implementations

| | Raft | typed_consensus | **Paxos-EC** |
|---|---|---|---|
| Output type | EC + TotalOrder + Atomic | EC + TotalOrder + Atomic | EC + TotalOrder + Atomic |
| EC derivation | `manual_proof!` on entire output | Mix of inferred + `manual_proof!` on composition | **Fully inferred** (EC propagation) |
| Safety argument | Folded into EC manual_proof | Spread across multiple manual_proofs | **Single manual_proof** (no slot conflicts) |
| TotalOrder derivation | `flat_map_ordered` (structural) | `.sort()` in sliced block | Slot-order emission (structural) |
| Protocol structure | Monolithic step function | Decomposed into ~8 sliced blocks | Broadcast → certificate → broadcast chain |
| Lines of code | ~800 (raft_step) | ~1300 | TBD |
| Simulator validation | Skips consistency assertions | Skips consistency assertions | Should validate EC directly |

## Implementation Sketch

```rust
pub fn paxos_ec<'a, T>(
    requests: Stream<T, Cluster<'a, Tag>, Unbounded, NoOrder>,
    election_interrupts: Stream<(), Cluster<'a, Tag>>,
    config: PaxosConfig,
    cluster: &Cluster<'a, Tag>,
) -> Stream<LogEntry<T>, Atomic<Cluster<'a, Tag, EventualConsistency>>, Unbounded, TotalOrder>
where
    T: Clone + Serialize + DeserializeOwned + Ord + 'a,
{
    // Phase 1: leader broadcasts Prepare (EC inferred)
    let proposals_ec: Stream<_, Cluster<'a, Tag, EC>, _, _> = ...;
    
    // Phase 1 acks: p2p, NoConsistency
    let phase1_acks: Stream<_, Cluster<'a, Tag>, _, _> = ...;
    
    // Leader accumulates quorum, assembles Phase1Certificate
    // Phase 2: leader broadcasts Accept with certificate (EC inferred)
    let accepts_ec: Stream<_, Cluster<'a, Tag, EC>, _, _> = ...;
    
    // Phase 2 acks: p2p, NoConsistency
    let phase2_acks: Stream<_, Cluster<'a, Tag>, _, _> = ...;
    
    // Leader accumulates quorum, assembles CommitCertificate
    // Commit: leader broadcasts with certificate (EC inferred)
    let commits_ec: Stream<_, Cluster<'a, Tag, EC>, _, _> = ...;
    
    // Derive committed log: deterministic fn of EC commits → EC
    // Sort by slot → TotalOrder
    let committed_log = commits_ec
        .dedup_by_slot()     // deterministic: first cert per slot wins (all same value)
        .sort_by_slot()      // structural: emit in slot order → TotalOrder
        .atomic();
    
    committed_log
    // NO assert_has_consistency_of needed — EC is inferred all the way through
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Safety — No Conflicting Commits Per Slot

*For any* execution explored by the simulator (all possible message orderings, batch boundaries, and concurrent ballot scenarios), the protocol SHALL never produce two CommitCertificates for the same slot that carry different values, regardless of how many ballots attempt to propose for that slot.

**Validates: Requirements 11.2, 12.1, 12.2, 13.4, 15.2, 15.3**

### Property 2: maxBallot Fencing Correctness

*For any* cluster member with current maxBallot = B, when that member receives a Prepare with ballot W or an Accept with ballot W: if W > B (for Prepare) or W >= B (for Accept), the member SHALL respond; if W <= B (for Prepare) or W < B (for Accept), the member SHALL not respond. Furthermore, maxBallot SHALL only increase monotonically across all message processing.

**Validates: Requirements 2.3, 2.4, 4.3, 4.4, 13.1, 13.2, 13.3**

### Property 3: Phase1Certificate Assembly and Value Selection

*For any* set of Promise responses: if the set contains at least floor(N/2)+1 responses for a ballot, a Phase1Certificate SHALL be assembled with promises.len() >= quorum; if any respondent reports a previously-accepted value, the proposed value SHALL be the one with the highest previously-accepted ballot among all quorum respondents; if no respondent reports a previous acceptance, a new client value SHALL be proposed. If fewer than floor(N/2)+1 responses are collected, no certificate SHALL be produced.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

### Property 4: CommitCertificate Assembly Threshold

*For any* set of phase-2 acknowledgements for a (slot, ballot, value) triple: if the set contains at least floor(N/2)+1 acks, a CommitCertificate SHALL be assembled with acceptors.len() >= quorum. If fewer acks are collected, no CommitCertificate SHALL be produced.

**Validates: Requirements 5.1, 5.2**

### Property 5: Certificate Verification Correctness

*For any* Phase1Certificate or CommitCertificate, the verification function SHALL return true if and only if the certificate's quorum vector length meets the floor(N/2)+1 threshold for the configured cluster size. Messages carrying certificates that fail verification SHALL be discarded.

**Validates: Requirements 10.1, 10.2, 10.3**

### Property 6: Gap-Filling Produces TotalOrder

*For any* sequence of CommitCertificates arriving in arbitrary slot order, the gap-filling mechanism SHALL buffer commits for slots beyond the current emission frontier and SHALL emit entries only in contiguous ascending slot order. The output SHALL never contain a gap (missing slot between two emitted entries).

**Validates: Requirements 7.1, 7.2, 6.4**

### Property 7: Committed Log Deterministic Derivation

*For any* set of Commit broadcasts received by a member, the committed_log SHALL be computed as: dedup by slot (first certificate per slot retained, all carrying same value by safety), then emit in slot order. This derivation is deterministic and produces the same result regardless of reception order.

**Validates: Requirements 6.1, 6.2**

### Property 8: EC Convergence at Quiescence

*For any* execution where all messages are eventually delivered (no permanent partition), all live cluster members SHALL eventually observe identical committed_log contents — for every slot committed on any member, every other live member eventually commits the same value for that slot.

**Validates: Requirements 11.3**

### Property 9: Liveness Under Stable Ballot

*For any* client request submitted while a single ballot is active, all cluster members are reachable, and no leader change occurs, the request SHALL be committed within 50 simulator ticks after submission.

**Validates: Requirements 11.5**

### Property 10: Liveness Under Leader Failure (Gap Recovery)

*For any* leader failure scenario (failure after Accept broadcast but before Commit, or failure after Phase1Certificate assembly but before Accept broadcast), a successor leader's Phase 1 SHALL discover previously-accepted values from quorum responses and either re-commit them or issue a no-op, filling any gaps in the committed log within bounded time.

**Validates: Requirements 14.1, 14.2, 14.3, 7.4**

### Property 11: Election Ballot Monotonicity

*For any* cluster member, when an election interrupt fires, the initiated ballot SHALL be strictly greater than the member's current maxBallot, ensuring progress toward a new leadership epoch.

**Validates: Requirements 14.4**

## Open Questions

### 1. How does TotalOrder work with streaming slots?

Raft emits entries as they commit — the leader might commit slot 5 before slot 3 (if slot 3's acks arrive later). To maintain TotalOrder emission, we need a gap-filling mechanism: buffer commits until all prior slots are filled, then emit in order.

This is a deterministic transform (buffering + in-order release), so it preserves EC. But it has liveness implications: a permanently-uncommitted slot blocks all later emissions. Multi-Paxos handles this with no-ops. How does this interact with the type system?

### 2. Does `acceptedValue` need to exist as a named collection?

In this design, the accepted value is embedded inside certificates (Phase1Certificate carries each promise's previously-accepted value). It doesn't need to be a standalone collection. Does this simplify the design or hide important structure?

### 3. Multi-slot pipelining (Multi-Paxos optimization)

In Multi-Paxos, the leader holds a ballot across many slots without re-running phase 1. The Phase1Certificate covers a range. Can we express this as: "phase 1 was run once; its certificate authorizes proposals for slots ≥ start_slot"? Does this affect EC typing?

### 4. Certificate verification

Should members verify incoming certificates against the EC streams? For example: "this CommitCertificate claims members {A, B, C} accepted — do I see those acceptances in my local view of the EC accept stream?" This is defense-in-depth (the protocol is correct without it), but it could:
- Detect byzantine faults
- Serve as a runtime assertion the simulator can check
- Shrink the trust boundary of the `manual_proof!`

### 5. Can the simulator validate the safety invariant directly?

The one `manual_proof!` claims: no conflicting certificates for the same slot. The simulator could check this by: at quiescence, assert that for every slot, all observed CommitCertificates carry the same value. If it ever finds a counterexample, the protocol is wrong. This would give empirical confidence in the manual proof.

### 6. Relationship to `broadcast_from_member` precondition

`broadcast_from_member` assumes a single member produces data. In this design, different leaders broadcast in different views. Is this multiple calls to `broadcast_from_member` (one per view), or one ongoing stream that multiple leaders write to at different times? If the latter, does the EC inference still hold?

The argument: at any given time, at most one leader is broadcasting (elections ensure exclusivity). But the type system can't verify that. We may need either:
- Separate `broadcast_from_member` calls per view (structurally enforced)
- A `broadcast_closed` from the cluster to itself (every member sends, EC inferred for the union — this actually works regardless of how many leaders are active)

### 7. Can we eliminate `manual_proof!` entirely?

The remaining `manual_proof!` is: "no conflicting certificates for the same slot." Could we make the type system verify this by:
- Requiring that `CommitCertificate` construction takes a `Phase1Certificate` as input (enforcing that phase 1 happened)
- Having the `Phase1Certificate` carry a proof that the proposed value is consistent with prior acceptances
- Making the certificate constructors the only way to produce these types (sealed/opaque constructors)

This would push the safety argument into the type system via phantom types / linear types / capabilities. Is it worth the complexity?

### 8. Liveness under leader failure

If a leader dies after broadcasting `accepts` but before broadcasting `commits`, the value is accepted by a quorum but never committed. A new leader's phase 1 will discover the acceptance and re-propose it. But from the committed_log's perspective, there's a gap. Does the gap-filling logic (question 1) handle this naturally, or does it need special treatment?
