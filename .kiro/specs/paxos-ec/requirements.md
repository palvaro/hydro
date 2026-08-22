# Requirements Document

## Introduction

Paxos-EC is a consensus protocol for the Hydro distributed dataflow framework that achieves the same output signature as Raft — a committed log with `EventualConsistency`, `TotalOrder`, and `Atomic` guarantees — but where EC on the committed log is **fully inferred by the type system** rather than asserted via `manual_proof!`. The protocol reifies quorums as certificates carried on EC broadcast streams. Every broadcast uses `broadcast_from_member` + `fail_stop`, giving EC by inference. The committed log is derived from EC streams via deterministic transforms, so EC propagates automatically. The only `manual_proof!` is the Paxos safety invariant: no two valid certificates can commit conflicting values for the same slot.

The protocol follows a three-phase broadcast structure: Phase 1 Prepare (EC broadcast) → Phase 1 Acks (p2p) → Phase 2 Accept (EC broadcast with Phase1Certificate) → Phase 2 Acks (p2p) → Commit (EC broadcast with CommitCertificate) → committed_log (deterministic derivation). This certificate-carrying design makes quorums portable and verifiable by any member, not just the leader who witnessed them.

## Glossary

- **Paxos_EC**: The consensus protocol module that uses certificate-carrying EC broadcasts to produce a committed log with fully inferred EventualConsistency
- **Cluster**: A Hydro replicated group of processes participating in the consensus protocol
- **Ballot**: A monotonically increasing identifier for a leadership attempt; higher ballots supersede lower ballots
- **Slot**: A position in the replicated log, identified by a zero-based index
- **Leader**: The cluster member currently sequencing client requests into slots under a given ballot
- **Phase1Certificate**: A data structure proving that a quorum promised a given ballot for a given slot, carrying each respondent's previously-accepted (ballot, value) pair
- **CommitCertificate**: A data structure proving that a quorum accepted a (slot, ballot, value) triple, carrying the list of accepting member identities
- **Quorum**: A majority subset of cluster members (floor(N/2) + 1 where N is cluster size)
- **Promise**: A response from a cluster member to a Prepare, pledging not to accept proposals from lower-numbered ballots, and reporting previously-accepted values
- **Prepare**: A phase-1 message broadcast by a candidate leader to establish a new ballot
- **Accept**: A phase-2 message broadcast by the leader carrying the value to accept and a Phase1Certificate proving authorization
- **Commit**: A phase-3 message broadcast by the leader carrying a CommitCertificate proving quorum acceptance
- **EventualConsistency (EC)**: Hydro's type-level guarantee that all live members of a cluster eventually observe the same values on a stream
- **broadcast_from_member**: A Hydro networking primitive enabling intra-cluster broadcast with EC inferred by the type system when combined with fail_stop transport
- **fail_stop**: A transport guarantee where the sender either delivers to all or crashes; used with broadcast_from_member to infer EC
- **Committed_Log**: The sequence of (slot, value) entries derived deterministically from committed certificates; the primary output of the protocol
- **Gap_Fill**: A mechanism to emit TotalOrder output by buffering committed entries until all prior slots are filled, then releasing in slot order
- **LogEntry**: The output record type containing the committed payload, the ballot in which it was committed, and its slot index
- **Simulator**: Hydro's deterministic simulation engine that exhaustively explores non-deterministic execution schedules
- **EC_Propagation**: The type system rule that deterministic transforms of EC streams produce EC streams
- **maxBallot**: Per-member state tracking the highest ballot seen across Prepare and Accept streams; used for fencing

## Requirements

### Requirement 1: Protocol Function Signature

**User Story:** As a Hydro application developer, I want Paxos-EC to expose the same output signature as Raft and typed_consensus, so that I can substitute it without changing application code.

#### Acceptance Criteria

1. THE Paxos_EC SHALL accept the following inputs: a stream of client requests typed as `Stream<T, Cluster<'a, Tag>, Unbounded, NoOrder>`; a stream of election interrupts typed as `Stream<(), Cluster<'a, Tag>>`; a `PaxosConfig` containing at minimum the cluster size (from which the quorum threshold floor(N/2) + 1 is derived); and a reference to the Cluster, where `Tag` is a generic cluster tag type parameter and `T` satisfies `Clone + Serialize + DeserializeOwned + Ord + 'a`
2. THE Paxos_EC SHALL return a committed log stream typed as `Stream<LogEntry<T>, Atomic<Cluster<'a, Tag, EventualConsistency>>, Unbounded, TotalOrder>` where `LogEntry<T>` contains the client payload of type `T`, the ballot (as `usize`) in which the entry was committed, and the slot index (as `usize`)
3. WHEN an application replaces a call to `raft(...)` or `typed_consensus(...)` with a call to `paxos_ec(...)`, THE Paxos_EC SHALL require no changes to the types or structure of the surrounding dataflow that consumes the committed log output, meaning the committed log stream's type parameters (`Atomic`, `EventualConsistency`, `Unbounded`, `TotalOrder`) and element type (`LogEntry<T>`) are identical to those returned by `raft` and `typed_consensus`
4. THE Paxos_EC SHALL NOT require a heartbeat timer input stream; internal leader liveness detection SHALL be handled within the protocol using the election interrupt stream and internal heartbeat broadcasts

### Requirement 2: Phase 1 Prepare Broadcast with Inferred EC

**User Story:** As a protocol designer, I want the Phase 1 Prepare broadcast to have EC inferred by the type system, so that no manual proof is needed for the initial ballot establishment.

#### Acceptance Criteria

1. WHEN a candidate Leader initiates a new ballot, THE Paxos_EC SHALL broadcast a Prepare message (containing the ballot number and a slot range identifying the contiguous set of slots the leader intends to sequence) to all cluster members using `broadcast_from_member` with `fail_stop` transport so that the resulting stream's EC type is inferred by the compiler without any `manual_proof!` annotation
2. THE Paxos_EC SHALL ensure the Prepare broadcast stream is typed as `Stream<Prepare, Cluster<'a, Tag, EC>, _, _>` with EC inferred at the broadcast boundary
3. WHEN a cluster member receives a Prepare with a ballot strictly greater than its current maxBallot, THE Paxos_EC SHALL update the member's maxBallot to the received ballot and respond with a Promise sent point-to-point to the candidate Leader, containing the member's previously-accepted (ballot, value) pair for each slot in the Prepare's slot range (or None for slots with no prior acceptance)
4. IF a cluster member receives a Prepare with a ballot less than or equal to its current maxBallot, THEN THE Paxos_EC SHALL discard the Prepare without responding

### Requirement 3: Phase 1 Certificate Assembly

**User Story:** As a protocol designer, I want the leader to assemble a Phase1Certificate from quorum responses, so that the quorum evidence is portable and verifiable by any member.

#### Acceptance Criteria

1. WHEN a candidate Leader collects Promise responses from at least floor(N/2) + 1 distinct cluster members for a given (ballot, slot) pair, THE Paxos_EC SHALL assemble a Phase1Certificate containing the ballot, the slot, each respondent's member identity, and each respondent's previously-accepted (ballot, value) pair (or None if the respondent had not previously accepted any value for that slot)
2. THE Paxos_EC SHALL ensure the Phase1Certificate contains the responding member identities and their promise payloads such that any cluster member can verify quorum achievement by checking that the promises vector contains at least floor(N/2) + 1 entries with distinct member identities
3. WHEN the Phase1Certificate indicates that one or more respondents previously accepted a value for the slot, THE Paxos_EC SHALL re-propose the value associated with the highest previously-accepted ballot among the quorum respondents
4. WHEN the Phase1Certificate indicates that no respondent previously accepted any value for the slot, THE Paxos_EC SHALL propose the next available client request value for that slot, with the selection point annotated as `nondet!()` since batching determines which request fills which slot
5. IF fewer than floor(N/2) + 1 Promise responses have been received for a ballot, THEN THE Paxos_EC SHALL not assemble a Phase1Certificate and SHALL continue accumulating responses until either quorum is reached or the ballot is superseded by a higher ballot
6. IF multiple Promise responses are received from the same cluster member for the same (ballot, slot) pair, THEN THE Paxos_EC SHALL count that member only once toward the quorum threshold

### Requirement 4: Phase 2 Accept Broadcast with Certificate and Inferred EC

**User Story:** As a protocol designer, I want the Phase 2 Accept broadcast to carry a Phase1Certificate and have EC inferred by the type system, so that every member can verify the leader's authorization.

#### Acceptance Criteria

1. WHEN the Leader has assembled a Phase1Certificate for a slot, THE Paxos_EC SHALL broadcast an Accept message carrying the Phase1Certificate, the slot, the ballot, and the proposed value using `broadcast_from_member` with `fail_stop` transport so that EC is inferred by the compiler
2. THE Paxos_EC SHALL ensure the Accept broadcast stream is typed as `Stream<Accept<T>, Cluster<'a, Tag, EC>, _, _>` with EC inferred at the broadcast boundary
3. WHEN a cluster member receives an Accept message with a ballot greater than or equal to its current maxBallot, THE Paxos_EC SHALL update maxBallot to the Accept's ballot (if higher), record the accepted (slot, ballot, value) in its accepted-values state, and respond with a phase-2 acknowledgement (containing the slot, ballot, and the member's identity) sent point-to-point to the leader
4. IF a cluster member receives an Accept message with a ballot less than its current maxBallot, THEN THE Paxos_EC SHALL discard the Accept without responding and without modifying any local state (fencing by maxBallot)
5. IF a cluster member receives a duplicate Accept message for the same (slot, ballot) pair it has already accepted, THEN THE Paxos_EC SHALL respond with an acknowledgement idempotently without recording a second acceptance

### Requirement 5: Commit Certificate Assembly and Broadcast with Inferred EC

**User Story:** As a protocol designer, I want the Commit broadcast to carry a CommitCertificate and have EC inferred, so that the entire chain from Prepare to Commit maintains fully inferred EC.

#### Acceptance Criteria

1. WHEN the Leader collects phase-2 acknowledgements from at least floor(N/2) + 1 distinct cluster members for a given (slot, ballot, value), THE Paxos_EC SHALL assemble a CommitCertificate containing the slot, ballot, the committed value (carried directly, not as a hash), and the list of accepting member identities
2. THE Paxos_EC SHALL ensure the CommitCertificate contains sufficient information for any cluster member to verify that a quorum accepted the value (the acceptors vector length is at least floor(N/2) + 1 with distinct member identities)
3. WHEN a CommitCertificate has been assembled, THE Paxos_EC SHALL broadcast a Commit message carrying the CommitCertificate using `broadcast_from_member` with `fail_stop` transport so that EC is inferred by the compiler
4. THE Paxos_EC SHALL ensure the Commit broadcast stream is typed as `Stream<Commit<T>, Cluster<'a, Tag, EC>, _, _>` with EC inferred at the broadcast boundary
5. IF fewer than floor(N/2) + 1 phase-2 acknowledgements have been received for a (slot, ballot) pair, THEN THE Paxos_EC SHALL not assemble a CommitCertificate and SHALL continue accumulating acknowledgements
6. IF a stale-ballot phase-2 acknowledgement arrives (for a ballot lower than the leader's current ballot), THEN THE Paxos_EC SHALL discard the acknowledgement without counting it toward any quorum

### Requirement 6: Committed Log Derivation via EC Propagation

**User Story:** As a protocol designer, I want the committed log to be derived from EC commit broadcasts via deterministic transforms, so that EC propagates automatically without any manual proof on consistency.

#### Acceptance Criteria

1. THE Paxos_EC SHALL derive the committed log by applying deterministic transforms to the EC-typed Commit broadcast stream: first dedup by slot (retaining one CommitCertificate per slot and discarding subsequent duplicates), then emit entries in contiguous ascending slot order via the gap-filling mechanism defined in Requirement 7
2. THE Paxos_EC SHALL ensure that the dedup-by-slot operation is deterministic: the first CommitCertificate received per slot is retained and all subsequent CommitCertificates for the same slot are discarded without effect, which is safe because all certificates for the same slot carry the same value by the safety invariant (Requirement 8, criterion 2)
3. THE Paxos_EC SHALL ensure EC propagates from the Commit stream to the committed_log by the type system's propagation rule: the dedup and gap-fill operations are deterministic functions of the EC-typed Commit input, therefore the compiler infers EC on the output without requiring any explicit annotation
4. THE Paxos_EC SHALL produce the committed_log as a TotalOrder stream ordered by ascending slot index starting from slot 0, wrapped in an Atomic boundary where each atomic batch consists of the contiguous run of slot entries released by a single gap-fill emission
5. THE Paxos_EC SHALL NOT require any `manual_proof!` annotation or `assert_has_consistency_of` call for the EC property of the committed log; the Rust compiler must infer EC on the committed_log type without any such annotation present in the source

### Requirement 7: Gap-Filling for TotalOrder Emission

**User Story:** As a protocol designer, I want gap-filling to ensure TotalOrder emission even when commits arrive out of slot order, so that the committed log output is contiguous and ordered.

#### Acceptance Criteria

1. WHEN a CommitCertificate arrives for slot S but slots prior to S have not yet been committed, THE Paxos_EC SHALL buffer the commit and not emit it to the committed_log until all prior slots are filled
2. WHEN all slots from the current emission frontier through slot S have committed values, THE Paxos_EC SHALL emit entries for slots in ascending order from the frontier through S, and advance the emission frontier to S + 1
3. THE Paxos_EC SHALL initialize the emission frontier to slot 0 (the first valid slot index) and ensure gap-filling is a deterministic transform (buffering plus in-order release) that preserves the EC property of its input
4. IF a slot remains uncommitted due to leader failure, THEN THE Paxos_EC SHALL rely on a new leader's Phase 1 to discover the accepted value and re-commit it, or issue a no-op to fill the gap, enabling the gap-filling mechanism to release any buffered later slots
5. IF a CommitCertificate arrives for a slot below the current emission frontier (already emitted), THEN THE Paxos_EC SHALL discard the duplicate without re-emitting or altering the committed_log

### Requirement 8: Minimal Manual Proof Surface — Single Safety Invariant

**User Story:** As a protocol designer, I want exactly one manual_proof! in the entire module (the Paxos safety invariant), so that the correctness argument is minimal and auditable.

#### Acceptance Criteria

1. THE Paxos_EC module SHALL require zero `manual_proof!` annotations for EventualConsistency on any broadcast stream (Prepare, Accept, Commit) or on the committed_log, with EC inferred entirely by the type system via `broadcast_from_member` + `fail_stop` and EC propagation
2. THE Paxos_EC module SHALL require exactly one `manual_proof!` for the Paxos safety invariant: no two CommitCertificates whose acceptors vector length is at least floor(N/2) + 1 for the same slot carry different values
3. THE Paxos_EC module SHALL contain a total of exactly 1 `manual_proof!` annotation, and no `manual_proof!` annotations beyond the one listed in criterion 2
4. THE `manual_proof!` annotation SHALL contain a doc comment explaining the quorum intersection argument in 1-3 sentences: Phase 1 forces any leader with ballot B' > B to learn the highest previously-accepted value for the slot from a quorum; by quorum intersection, at least one quorum respondent also accepted the old value; the new leader must re-propose that value
5. IF the single `manual_proof!` annotation is removed from the Paxos_EC module, THEN the module SHALL fail to compile, confirming that the annotation is necessary for the type system to accept the safety invariant claim

### Requirement 9: EC Inference Chain Completeness

**User Story:** As a protocol designer, I want the EC inference chain to be complete from broadcast boundaries through to the final output, so that no gap in the chain requires a manual proof.

#### Acceptance Criteria

1. THE Paxos_EC SHALL ensure that every transition from NoConsistency to EC occurs exclusively at a `broadcast_from_member` + `fail_stop` boundary (the Prepare, Accept, and Commit broadcasts), and that no other stream in the module acquires EC typing through any other mechanism
2. THE Paxos_EC SHALL ensure that the quorum accumulation logic (inherently non-deterministic — depends on which acks arrive when) operates entirely on NoConsistency-typed streams within a single member, and that its output feeds directly into a `broadcast_from_member` + `fail_stop` call that re-establishes EC on the resulting broadcast stream
3. THE Paxos_EC SHALL ensure that the committed_log's EC is inferred through the complete propagation chain: Commit stream is EC (inferred from broadcast) → dedup by slot is deterministic → gap-fill buffering and in-order release is deterministic → deterministic function of EC produces EC (propagation rule), with each transform individually satisfying the type system's determinism requirement
4. THE Paxos_EC SHALL ensure that the p2p ack streams (phase1_acks, phase2_acks) are typed as NoConsistency and that their non-deterministic consumption affects only which value is proposed or committed (content selection) but cannot prevent the downstream broadcast from having EC inferred at its boundary
5. IF any intermediate stream between a NoConsistency-typed ack stream and an EC-typed broadcast stream requires an EC annotation, THEN THE Paxos_EC SHALL obtain that EC exclusively from a `broadcast_from_member` + `fail_stop` call, never from `manual_proof!` or `assert_has_consistency_of`

### Requirement 10: Certificate Verification as Defense-in-Depth

**User Story:** As a protocol designer, I want members to verify incoming certificates inline in the dataflow, so that byzantine faults or protocol bugs can be detected at runtime.

#### Acceptance Criteria

1. WHEN a cluster member receives an Accept message carrying a Phase1Certificate, THE Paxos_EC SHALL apply an inline verification filter that checks whether the promises vector contains at least floor(N/2) + 1 entries (where N is the cluster size)
2. WHEN a cluster member receives a Commit message carrying a CommitCertificate, THE Paxos_EC SHALL apply an inline verification filter that checks whether the acceptors vector contains at least floor(N/2) + 1 entries (where N is the cluster size)
3. IF certificate verification fails (insufficient quorum size in the certificate), THEN THE Paxos_EC SHALL discard the message and emit a diagnostic event on a separate observation stream accessible to the simulator
4. WHEN a certificate passes verification, THE Paxos_EC SHALL pass the message through the filter unchanged to downstream processing
5. THE Paxos_EC SHALL ensure certificate verification is a deterministic function of the certificate contents and the cluster size N, preserving EC propagation on the verified stream

### Requirement 11: Simulation Testability

**User Story:** As a protocol developer, I want Paxos-EC to be testable under Hydro's deterministic simulator, so that I can verify safety and the EC inference chain under all possible execution schedules.

#### Acceptance Criteria

1. THE Paxos_EC SHALL expose its dataflow construction as a function that accepts simulation input streams (client requests via `sim_input` and election interrupts via `sim_input`) and produces streams callable with `sim_cluster_output`, such that the resulting `FlowBuilder` can be passed to `flow.sim().exhaustive(...)` and `flow.sim().fuzz(...)`
2. WHEN the simulator explores all batch boundaries and message orderings, THE Paxos_EC SHALL never produce two different committed values for the same slot index across any cluster member (safety property)
3. WHEN the simulator reaches quiescence (no in-flight messages and no pending ticks remain), THE Paxos_EC SHALL have identical committed log prefixes on all non-crashed cluster members — that is, for every slot index committed on more than one member, the committed value is the same, and all members share the same set of committed slot indices
4. THE Paxos_EC SHALL support configurable cluster sizes of 3 to 7 nodes via `with_cluster_size` for simulation tests
5. WHILE a single ballot is active and all cluster members are reachable and no leader change occurs, THE Paxos_EC SHALL commit every submitted request within a simulator-bounded number of ticks not exceeding 50 ticks after submission (liveness under no failures)
6. WHEN the simulator injects a leader failure (by ceasing message delivery to or from a member) and a new ballot is triggered via the election interrupt stream, THE Paxos_EC SHALL eventually commit all previously-accepted-but-uncommitted slots under the successor leader's ballot (liveness under single failure)
7. THE Paxos_EC simulation test SHALL compile successfully with the committed log output typed as EC without any `manual_proof!` annotation in the protocol module beyond the single Paxos safety invariant, confirming that the EC inference chain is complete from broadcast boundaries to the final output

### Requirement 12: Safety Invariant Validation by Simulator

**User Story:** As a protocol developer, I want the simulator to directly validate the single manual_proof! claim (no conflicting certificates for the same slot), so that empirical evidence supports the manual proof.

#### Acceptance Criteria

1. WHEN the simulator reaches quiescence (no messages in flight and all members' tick processing complete), THE Paxos_EC SHALL assert that for every slot from 0 through the highest committed slot index observed, all CommitCertificates collected across all cluster members carry the same value for that slot regardless of which ballot produced the certificate
2. IF the simulator ever observes two CommitCertificates for the same slot whose committed values are not equal (compared by Rust `PartialEq` on the value type `T`), THEN THE Paxos_EC SHALL immediately panic with an error message identifying the conflicting slot index, the two ballot numbers, and the two differing values
3. THE Paxos_EC SHALL instrument the Commit broadcast stream at each cluster member's receive path to collect all CommitCertificates into a per-member log accessible to the simulator, without altering the stream's type signature or removing EC inference from the Commit stream
4. WHEN a CommitCertificate is received by any cluster member during simulation, THE Paxos_EC SHALL eagerly check the received certificate against previously collected certificates for the same slot and trigger the safety violation panic defined in criterion 2 if a conflict is detected, rather than deferring all checks to quiescence

### Requirement 13: Fencing via maxBallot

**User Story:** As a protocol designer, I want the maxBallot state to fence out stale-ballot messages preemptively, so that the simulator cannot find a safety violation where two ballots commit the same slot.

#### Acceptance Criteria

1. THE Paxos_EC SHALL maintain a per-member maxBallot state, initialized to 0 (representing no ballot seen), that monotonically increases and is updated to the ballot value of any received Prepare or Accept message whose ballot is strictly greater than the current maxBallot
2. WHILE a cluster member's maxBallot is B, THE Paxos_EC SHALL reject (not acknowledge) any Accept message from a ballot less than B
3. WHILE a cluster member's maxBallot is B, THE Paxos_EC SHALL reject (not respond with a Promise) any Prepare message with a ballot less than or equal to B
4. WHEN the simulator explores any execution where two leaders hold ballots B1 < B2 and both attempt phase-2 Accept broadcasts for the same slot, THE Paxos_EC SHALL ensure that at most one of those ballots achieves floor(N/2) + 1 Accept acknowledgements for that slot, because once floor(N/2) + 1 members update maxBallot to B2, fewer than floor(N/2) + 1 members remain that would acknowledge an Accept for B1
5. WHEN a cluster member receives an Accept message with a ballot equal to its current maxBallot, THE Paxos_EC SHALL accept (acknowledge) the message without updating maxBallot

### Requirement 14: Leader Failure and Liveness Recovery

**User Story:** As a protocol designer, I want the protocol to handle leader failure at any point in the three-phase broadcast chain, so that accepted-but-uncommitted values are eventually committed by a successor leader.

#### Acceptance Criteria

1. IF a Leader fails after broadcasting Accepts but before broadcasting Commits, THEN THE Paxos_EC SHALL ensure a successor leader's Phase 1 discovers the accepted value (via Promise responses carrying previously-accepted values for the affected slots) and re-proposes the value associated with the highest previously-accepted ballot among quorum respondents for each such slot
2. IF a Leader fails after assembling a Phase1Certificate but before broadcasting Accepts, THEN THE Paxos_EC SHALL ensure the successor leader runs its own Phase 1 covering at minimum all slots from the current emission frontier onward, and proposes either the previously-accepted value (if one exists in quorum responses) or a new value for each slot
3. WHEN a successor leader commits a slot that was previously uncommitted due to leader failure, THE Paxos_EC SHALL fill the gap with the committed value (either the re-proposed previously-accepted value or a no-op representing an empty consensus decision that carries no client payload), enabling the gap-filling mechanism to release buffered later slots in ascending order
4. WHEN an election interrupt fires on a cluster member, THE Paxos_EC SHALL initiate a new ballot with a ballot number that is both strictly greater than the member's current maxBallot and unique across all cluster members (e.g., by encoding the member's identity into the ballot structure), ensuring no two members can generate the same ballot number
5. IF multiple cluster members receive election interrupts concurrently and initiate competing recovery ballots for the same uncommitted slot, THEN THE Paxos_EC SHALL guarantee that at most one of the competing ballots achieves a Phase 2 accept quorum for that slot (by the quorum intersection property and maxBallot fencing from Requirement 13)

### Requirement 15: Integration Testing and Raft Test Suite Parity

**User Story:** As a protocol developer, I want Paxos-EC to pass the same suite of integration tests used on Raft plus full-scale end-to-end tests, so that I have confidence the protocol works correctly in realistic deployment scenarios.

#### Acceptance Criteria

1. THE Paxos_EC SHALL pass the complete existing Raft integration test suite (all tests in `hydro_test/src/cluster/` that exercise committed log correctness, leader election, view changes, and liveness) by substituting `paxos_ec(...)` for `raft(...)` with no test logic changes beyond configuration differences
2. THE Paxos_EC SHALL include full-scale end-to-end integration tests that deploy the protocol on multiple processes (not just in-process simulation) and verify committed log correctness under sustained client load for at least 10 seconds
3. WHEN running the Raft test suite against Paxos-EC, THE test harness SHALL use the same client driver, request payloads, cluster topologies, and assertion logic as the Raft version, differing only in the protocol constructor call and configuration struct
4. THE Paxos_EC end-to-end integration tests SHALL exercise leader failure scenarios (killing a leader process mid-operation and verifying the successor leader recovers and commits pending entries) on a cluster of at least 3 nodes
5. THE Paxos_EC end-to-end integration tests SHALL verify that all cluster members converge to the same committed log within a bounded time after all client requests have been submitted and all failures have been recovered

### Requirement 16: Batching and Non-Determinism Safety

**User Story:** As a protocol designer, I want batching non-determinism to affect only which proposal wins a slot (not agreement), so that the safety argument is batch-independent.

#### Acceptance Criteria

1. THE Paxos_EC SHALL annotate every `use(...)` and `use::atomic(...)` call within `sliced!` blocks with a `nondet!()` whose documentation states that batching affects which proposal wins a slot but cannot cause two conflicting values to both be committed for the same slot
2. WHEN the simulator explores two different batch boundary assignments for the same set of input messages, THE Paxos_EC SHALL never produce two CommitCertificates for the same slot carrying different values in either execution, even though the specific value committed to a given slot MAY differ between the two executions
3. WHEN the simulator runs `flow.sim().exhaustive(...)` varying batch boundaries across all `nondet!()` annotations, THE Paxos_EC SHALL pass the safety assertion (no slot has conflicting CommitCertificates) in every explored execution, confirming that the quorum intersection argument is batch-independent
4. IF two simulator executions that differ only in batch boundary placement produce different committed values for the same slot, THEN THE Paxos_EC SHALL ensure both values are valid client requests that were proposed during the respective executions and that no execution contains two different committed values for that slot

