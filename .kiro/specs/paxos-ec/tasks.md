# Implementation Plan: Paxos-EC

## Overview

Build a certificate-carrying Paxos consensus protocol for Hydro where EventualConsistency on the committed log is fully inferred by the type system. The implementation uses three EC broadcast phases (Prepare, Accept, Commit), each carrying reified quorum certificates. The single `manual_proof!` is the Paxos safety invariant (quorum intersection prevents conflicting commits for the same slot).

The protocol lives in `hydro_test/src/cluster/paxos_ec.rs` with module registration in `hydro_test/src/cluster/mod.rs`. Tests use the Hydro deterministic simulator (`exhaustive` + `fuzz` modes).

## Tasks

- [x] 1. Set up module structure and define core data types
  - [x] 1.1 Create `hydro_test/src/cluster/paxos_ec.rs` with module doc comment, imports, and all core data types
    - Define cluster tag `Nodes` struct
    - Define `PaxosConfig` with `cluster_size: usize` (quorum = cluster_size / 2 + 1)
    - Define `Ballot` as `usize` (encoded as `round * cluster_size + member_id` for uniqueness)
    - Define `Slot` as `usize`
    - Define `LogEntry<T>` with fields: `message: T`, `ballot: Ballot`, `slot: Slot`
    - Define `Prepare` with fields: `ballot: Ballot`, `slot_range_start: Slot`
    - Define `Promise<T>` with fields: `ballot: Ballot`, `from: MemberId`, `accepted: Vec<(Slot, Ballot, T)>` (previously-accepted entries)
    - Define `Phase1Certificate<T>` with fields: `ballot: Ballot`, `slot: Slot`, `promises: Vec<(MemberId, Option<(Ballot, T)>)>`
    - Define `Accept<T>` with fields: `ballot: Ballot`, `slot: Slot`, `value: T`, `certificate: Phase1Certificate<T>`
    - Define `AcceptAck` with fields: `ballot: Ballot`, `slot: Slot`, `from: MemberId`
    - Define `CommitCertificate<T>` with fields: `ballot: Ballot`, `slot: Slot`, `value: T`, `acceptors: Vec<MemberId>`
    - Define `Commit<T>` with fields: `certificate: CommitCertificate<T>`
    - Derive `Clone, Debug, PartialEq, Eq, Serialize, Deserialize` on all types (manual impls where `MemberId<ClusterTag>` is involved)
    - _Requirements: 1.1, 1.2, 3.1, 3.2, 5.1, 5.2_

  - [x] 1.2 Register the module in `hydro_test/src/cluster/mod.rs`
    - Add `pub mod paxos_ec;` entry
    - _Requirements: 1.1_

- [x] 2. Implement maxBallot fencing logic
  - [x] 2.1 Implement `max_ballot_fence` function
    - Accept Prepare stream and Accept stream (both EC-typed from broadcast)
    - Maintain per-member `max_ballot: Ballot` state (monotonically increasing)
    - Update maxBallot on Prepare with ballot > current maxBallot
    - Update maxBallot on Accept with ballot > current maxBallot
    - Filter Prepares: respond only if ballot > current maxBallot
    - Filter Accepts: respond only if ballot >= current maxBallot
    - Return (filtered_prepares, filtered_accepts) streams
    - _Requirements: 13.1, 13.2, 13.3, 13.5, 2.3, 2.4, 4.3, 4.4_

  - [x] 2.2 Write property test for maxBallot fencing correctness
    - **Property 2: maxBallot Fencing Correctness**
    - **Validates: Requirements 2.3, 2.4, 4.3, 4.4, 13.1, 13.2, 13.3**
    - Use `flow.sim().exhaustive(...)` with varying ballot sequences
    - Assert: Prepare with ballot <= maxBallot produces no Promise
    - Assert: Accept with ballot < maxBallot produces no Ack
    - Assert: maxBallot only increases monotonically

- [x] 3. Implement Phase 1: Prepare broadcast and Promise response
  - [x] 3.1 Implement `phase1_prepare_broadcast` function
    - Accept election trigger stream and member's current ballot state
    - Compute new ballot: `(current_round + 1) * cluster_size + member_id` (globally unique)
    - Broadcast Prepare via `broadcast_from_member` + `fail_stop` → EC inferred on output stream
    - Return `Stream<Prepare, Cluster<'a, Tag, EventualConsistency>, _, _>`
    - _Requirements: 2.1, 2.2, 14.4_

  - [x] 3.2 Implement `phase1_promise_response` function
    - Accept filtered Prepare stream (from maxBallot fence)
    - Respond with Promise carrying previously-accepted (slot, ballot, value) pairs for requested slot range
    - Send Promise point-to-point to the candidate leader via `demux`
    - Return `Stream<Promise<T>, Cluster<'a, Tag>, _, _>` (NoConsistency, p2p)
    - _Requirements: 2.3, 3.1, 14.1_

  - [x] 3.3 Write property test for Phase1Certificate assembly and value selection
    - **Property 3: Phase1Certificate Assembly and Value Selection**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**
    - Use `flow.sim().exhaustive(...)` with varying promise payloads
    - Assert: certificate assembled only at quorum (floor(N/2)+1)
    - Assert: if any respondent reports accepted value, highest-ballot value is re-proposed
    - Assert: if no prior acceptance, new client value proposed

- [x] 4. Implement Phase1Certificate assembly
  - [x] 4.1 Implement `assemble_phase1_certificate` function
    - Accept Promise stream (p2p to leader)
    - Accumulate promises per (ballot, slot) in `sliced!` block
    - Count distinct member identities (deduplicate same-member responses per Requirement 3.6)
    - Once count >= floor(N/2)+1: assemble `Phase1Certificate<T>`
    - Select value: highest previously-accepted ballot value if any; else next client request with `nondet!()` annotation
    - Return (certificate stream, selected value stream)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 5. Checkpoint - Phase 1 complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement Phase 2: Accept broadcast with certificate
  - [x] 6.1 Implement `phase2_accept_broadcast` function
    - Accept Phase1Certificate stream and selected value stream
    - Construct `Accept<T>` carrying: ballot, slot, value, Phase1Certificate
    - Broadcast via `broadcast_from_member` + `fail_stop` → EC inferred
    - Return `Stream<Accept<T>, Cluster<'a, Tag, EventualConsistency>, _, _>`
    - _Requirements: 4.1, 4.2_

  - [x] 6.2 Implement `phase2_accept_ack` function
    - Accept filtered Accept stream (from maxBallot fence, ballot >= maxBallot)
    - Record accepted (slot, ballot, value) in per-member state
    - Handle idempotent re-acceptance for duplicate (slot, ballot) pairs
    - Send AcceptAck point-to-point to leader via `demux`
    - Return `Stream<AcceptAck, Cluster<'a, Tag>, _, _>` (NoConsistency, p2p)
    - _Requirements: 4.3, 4.5, 13.5_

  - [x] 6.3 Write property test for CommitCertificate assembly threshold
    - **Property 4: CommitCertificate Assembly Threshold**
    - **Validates: Requirements 5.1, 5.2**
    - Use `flow.sim().exhaustive(...)` with varying ack arrival orders
    - Assert: CommitCertificate assembled exactly at quorum threshold
    - Assert: no certificate below quorum
    - Assert: stale-ballot acks are discarded

- [x] 7. Implement CommitCertificate assembly
  - [x] 7.1 Implement `assemble_commit_certificate` function
    - Accept AcceptAck stream (p2p to leader)
    - Accumulate acks per (slot, ballot) in `sliced!` block
    - Count distinct member identities toward quorum
    - Discard stale-ballot acks (ballot < leader's current ballot)
    - Once count >= floor(N/2)+1: assemble `CommitCertificate<T>`
    - _Requirements: 5.1, 5.2, 5.5, 5.6_

- [x] 8. Implement Phase 3: Commit broadcast with certificate
  - [x] 8.1 Implement `phase3_commit_broadcast` function
    - Accept CommitCertificate stream
    - Construct `Commit<T>` carrying the full CommitCertificate (value carried directly, not hashed)
    - Broadcast via `broadcast_from_member` + `fail_stop` → EC inferred
    - Return `Stream<Commit<T>, Cluster<'a, Tag, EventualConsistency>, _, _>`
    - _Requirements: 5.3, 5.4_

- [x] 9. Checkpoint - All three phases implemented
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement certificate verification (defense-in-depth)
  - [x] 10.1 Implement `verify_phase1_certificate` inline filter
    - On Accept receipt: check `certificate.promises.len() >= quorum_size`
    - Discard messages failing verification; emit diagnostic event
    - Pass verified messages unchanged to downstream processing
    - Verification is deterministic → preserves EC propagation
    - _Requirements: 10.1, 10.3, 10.4, 10.5_

  - [x] 10.2 Implement `verify_commit_certificate` inline filter
    - On Commit receipt: check `certificate.acceptors.len() >= quorum_size`
    - Discard messages failing verification; emit diagnostic event
    - Pass verified messages unchanged to downstream processing
    - _Requirements: 10.2, 10.3, 10.4, 10.5_

  - [x] 10.3 Write property test for certificate verification
    - **Property 5: Certificate Verification Correctness**
    - **Validates: Requirements 10.1, 10.2, 10.3**
    - Use `flow.sim().exhaustive(...)` with valid and invalid certificates
    - Assert: certificates meeting quorum threshold pass through
    - Assert: certificates below threshold are discarded

- [x] 11. Implement committed log derivation and gap-filling
  - [x] 11.1 Implement `derive_committed_log` function
    - Accept verified Commit stream (EC-typed)
    - Dedup by slot: retain first CommitCertificate per slot, discard duplicates
    - Gap-fill: buffer commits for slots beyond current frontier, emit in contiguous ascending order
    - Initialize emission frontier to slot 0
    - Discard commits for slots below frontier (already emitted)
    - Wrap output in Atomic boundary with TotalOrder
    - Return `Stream<LogEntry<T>, Atomic<Cluster<'a, Tag, EventualConsistency>>, Unbounded, TotalOrder>`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 7.3, 7.5_

  - [x] 11.2 Write property test for gap-filling TotalOrder emission
    - **Property 6: Gap-Filling Produces TotalOrder**
    - **Validates: Requirements 7.1, 7.2, 6.4**
    - Use `flow.sim().exhaustive(...)` with commits arriving out-of-slot-order
    - Assert: output is contiguous ascending slot order with no gaps
    - Assert: commits beyond frontier are buffered until prior slots fill

  - [x] 11.3 Write property test for committed log deterministic derivation
    - **Property 7: Committed Log Deterministic Derivation**
    - **Validates: Requirements 6.1, 6.2**
    - Use `flow.sim().exhaustive(...)` with known commits
    - Assert: committed_log = dedup by slot + emit in slot order
    - Assert: result is the same regardless of reception order

- [x] 12. Implement the single `manual_proof!` annotation
  - [x] 12.1 Add the Paxos safety invariant `manual_proof!`
    - Place exactly one `manual_proof!` annotation asserting: no two CommitCertificates for the same slot carry different values
    - Include doc comment with quorum intersection argument (1-3 sentences): "Phase 1 forces any leader with ballot B' > B to learn the highest previously-accepted value for the slot from a quorum. By quorum intersection, at least one quorum respondent also accepted the old value. The new leader must re-propose that value."
    - Verify: removing the annotation causes a compilation failure
    - Verify: no other `manual_proof!` or `assert_has_consistency_of` exists in the module
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 13. Checkpoint - Core protocol logic complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Wire the public `paxos_ec()` API function
  - [x] 14.1 Implement the top-level `paxos_ec` function
    - Accept inputs: `requests: Stream<T, Cluster<'a, Tag>, Unbounded, NoOrder>`, `election_interrupts: Stream<(), Cluster<'a, Tag>>`, `config: PaxosConfig`, `cluster: &Cluster<'a, Tag>`
    - T bound: `Clone + Serialize + DeserializeOwned + Ord + 'a`
    - Wire internal components: election → phase1_prepare_broadcast → max_ballot_fence → phase1_promise_response → assemble_phase1_certificate → phase2_accept_broadcast → max_ballot_fence → phase2_accept_ack → assemble_commit_certificate → phase3_commit_broadcast → verify_commit_certificate → derive_committed_log
    - Return `Stream<LogEntry<T>, Atomic<Cluster<'a, Tag, EventualConsistency>>, Unbounded, TotalOrder>`
    - Ensure output type matches Raft's output signature exactly
    - No heartbeat timer input — internal leader liveness via election interrupt stream
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 14.2 Write `manual_proof!` audit test
    - Read the module source in a `#[test]` function
    - Assert exactly 1 occurrence of `manual_proof!`
    - Assert it contains the quorum intersection doc comment
    - Assert no `assert_has_consistency_of` calls exist
    - _Requirements: 8.1, 8.2, 8.3_

- [x] 15. Implement election ballot generation and leader change logic
  - [x] 15.1 Implement ballot generation on election interrupt
    - Compute globally-unique ballot: `(round + 1) * cluster_size + member_id`
    - Ensure ballot is strictly greater than member's current maxBallot
    - Trigger Phase 1 Prepare for the new ballot
    - _Requirements: 14.4, 14.5_

  - [x] 15.2 Implement leader failure recovery (Phase 2a re-proposal)
    - Successor leader's Phase 1 discovers accepted-but-uncommitted values from Promise responses
    - Re-propose the value with highest previously-accepted ballot for each affected slot
    - If no prior acceptance exists, propose new client value or no-op for gap-fill
    - _Requirements: 14.1, 14.2, 14.3, 7.4_

- [x] 16. Checkpoint - Full protocol wired and compiling
  - Ensure all tests pass, ask the user if questions arise.

- [x] 17. Implement simulator safety tests
  - [x] 17.1 Write safety test: single ballot, 3-node cluster
    - **Property 1: Safety — No Conflicting Commits Per Slot**
    - **Validates: Requirements 11.2, 12.1, 12.2**
    - Use `flow.sim().exhaustive(...)` with 3-node cluster
    - Submit requests, verify committed log has no slot conflicts
    - Verify all members produce identical committed_log at quiescence

  - [x] 17.2 Write safety test: concurrent ballots, 3-node cluster
    - **Property 1: Safety — No Conflicting Commits Per Slot**
    - **Validates: Requirements 11.2, 12.1, 12.2, 13.4**
    - Use `flow.sim().fuzz(...)` with 8192 iterations, 3-node cluster
    - Inject concurrent election interrupts to create competing ballots
    - Assert no slot has conflicting CommitCertificates across any member

  - [x] 17.3 Write safety test: concurrent ballots, 5-node cluster
    - **Property 1: Safety — No Conflicting Commits Per Slot**
    - **Validates: Requirements 11.2, 12.1, 12.2, 13.4**
    - Use `flow.sim().fuzz(...)` with 8192 iterations, 5-node cluster
    - Assert no slot conflicts under larger cluster concurrency

  - [x] 17.4 Write eager safety check instrumentation
    - **Property 1: Safety — No Conflicting Commits Per Slot**
    - **Validates: Requirements 12.3, 12.4**
    - Instrument Commit stream at each member's receive path to collect CommitCertificates
    - Eagerly check each received certificate against previously collected for same slot
    - Panic immediately on conflict (don't defer to quiescence)

- [x] 18. Implement simulator liveness and convergence tests
  - [x] 18.1 Write liveness test: stable ballot commits within 50 ticks
    - **Property 9: Liveness Under Stable Ballot**
    - **Validates: Requirements 11.5**
    - Use `flow.sim().exhaustive(...)` with single ballot, no failures
    - Assert all submitted requests committed within 50 simulator ticks

  - [x] 18.2 Write convergence test: EC convergence at quiescence
    - **Property 8: EC Convergence at Quiescence**
    - **Validates: Requirements 11.3**
    - Use `flow.sim().fuzz(...)` with multiple ballots
    - At quiescence: assert all live members have identical committed_log prefixes

  - [x] 18.3 Write liveness test: leader failure recovery
    - **Property 10: Liveness Under Leader Failure (Gap Recovery)**
    - **Validates: Requirements 14.1, 14.2, 14.3, 7.4, 11.6**
    - Use `flow.sim().fuzz(...)` with leader failure injection
    - Assert successor leader discovers and re-commits accepted-but-uncommitted slots
    - Assert gap-fill releases buffered later slots

- [x] 19. Checkpoint - Safety and liveness verified
  - Ensure all tests pass, ask the user if questions arise.

- [x] 20. Implement Raft test suite parity and integration tests
  - [x] 20.1 Verify `paxos_ec(...)` passes existing Raft integration test patterns
    - Substitute `paxos_ec(...)` for `raft(...)` in test harness
    - Same client driver, request payloads, cluster topologies, assertion logic
    - Only configuration struct differs
    - _Requirements: 15.1, 15.3_

  - [x] 20.2 Write end-to-end integration test: sustained load
    - Deploy on multiple processes (not just simulation)
    - Verify committed log correctness under sustained client load for at least 10 seconds
    - _Requirements: 15.2_

  - [x] 20.3 Write end-to-end integration test: leader failure recovery
    - Deploy 3+ node cluster
    - Kill leader process mid-operation
    - Verify successor leader recovers and commits pending entries
    - Verify all members converge to same committed log within bounded time
    - _Requirements: 15.4, 15.5_

- [x] 21. Implement batching non-determinism annotations
  - [x] 21.1 Add `nondet!()` annotations to all batch boundaries
    - Annotate every `use(...)` and `use::atomic(...)` within `sliced!` blocks
    - Document that batching affects which proposal wins a slot but cannot cause conflicting commits
    - _Requirements: 16.1_

  - [x] 21.2 Write property test for batching safety
    - **Property 1: Safety — No Conflicting Commits Per Slot (batch-variant)**
    - **Validates: Requirements 16.2, 16.3, 16.4**
    - Use `flow.sim().exhaustive(...)` varying batch boundaries across all `nondet!()` points
    - Assert no slot has conflicting CommitCertificates in any explored execution

- [x] 22. Final checkpoint - Full protocol verified
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties via Hydro's deterministic simulator
- The protocol requires exactly **1** `manual_proof!` annotation: the Paxos safety invariant (quorum intersection prevents conflicting commits for the same slot)
- All EC guarantees on broadcast streams (Prepare, Accept, Commit) are inferred by the type system via `broadcast_from_member` + `fail_stop`
- EC on the committed_log is inferred via propagation: deterministic transforms of EC streams produce EC streams
- Performance benchmarks and deployment infrastructure are deferred — they are not pure coding tasks
- The `nondet!()` annotations document and enable simulation exploration of batch boundaries without affecting the safety argument

### EC Inference Chain Summary

The committed log's EC is inferred through this complete chain:
1. `commits` stream → EC inferred from `broadcast_from_member` + `fail_stop`
2. `verify_commit_certificate` → deterministic filter of EC → EC (propagation)
3. `dedup_by_slot` → deterministic function of EC → EC (propagation)
4. `gap_fill` → deterministic buffering + in-order release of EC → EC (propagation)
5. No `manual_proof!` on EC anywhere — the type system does it all

### Integration Test Completion Criteria

Each integration test must satisfy ALL of the following:
- **Runtime bound**: A single fuzz iteration must complete within 60 seconds (no hangs)
- **Safety tests (17.1-17.4)**: No slot has conflicting CommitCertificates across any member in any explored execution
- **Liveness test (18.1)**: Commits happen within 50 simulator ticks under stable ballot with no failures
- **Convergence test (18.2)**: At quiescence, all live members observe the same committed log prefix
- **Leader failure (18.3)**: Successor leader discovers accepted values and re-commits them, filling gaps

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "3.2"] },
    { "id": 3, "tasks": ["3.3", "4.1"] },
    { "id": 4, "tasks": ["6.1"] },
    { "id": 5, "tasks": ["6.2", "6.3"] },
    { "id": 6, "tasks": ["7.1"] },
    { "id": 7, "tasks": ["8.1"] },
    { "id": 8, "tasks": ["10.1", "10.2"] },
    { "id": 9, "tasks": ["10.3", "11.1"] },
    { "id": 10, "tasks": ["11.2", "11.3", "12.1"] },
    { "id": 11, "tasks": ["14.1", "15.1"] },
    { "id": 12, "tasks": ["14.2", "15.2", "21.1"] },
    { "id": 13, "tasks": ["17.1", "17.4"] },
    { "id": 14, "tasks": ["17.2", "17.3", "18.1"] },
    { "id": 15, "tasks": ["18.2", "18.3"] },
    { "id": 16, "tasks": ["20.1"] },
    { "id": 17, "tasks": ["20.2", "20.3", "21.2"] }
  ]
}
```
