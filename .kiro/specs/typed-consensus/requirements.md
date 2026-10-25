# Requirements Document

## Introduction

A typed consensus protocol for the Hydro distributed dataflow framework that serves as a drop-in alternative to Raft. Rather than implementing a monolithic consensus protocol and asserting EventualConsistency via `manual_proof!`, this protocol composes primitive type-level EC guarantees — each inferred by the compiler from `broadcast_from_member` + `fail_stop` — into a full consensus protocol whose committed log is provably EC.

The protocol follows the Paxos two-phase structure (Prepare/Promise then Propose/Accept/Commit), with each phase's broadcast yielding an EC-typed stream inferred by the type system. Cross-view safety is ensured by quorum intersection during view changes, requiring only one small manual proof. The protocol must match Raft's input/output signature, support Hydro's deterministic simulator for testing, and deliver comparable performance.

## Glossary

- **Typed_Consensus**: The consensus protocol module that composes EC-typed building blocks to produce a committed log with EventualConsistency guarantees
- **Cluster**: A Hydro replicated group of processes participating in the consensus protocol
- **View**: A period of leadership by a single cluster member; identified by a monotonically increasing view number
- **Leader**: The cluster member that sequences client requests into log slots during a given view
- **Slot**: A position in the replicated log, identified by a zero-based index
- **Proposal**: A (view, slot, value) triple broadcast by the leader to all cluster members
- **Commit_Notification**: A message broadcast by the leader indicating that a particular slot in a particular view has been committed (acknowledged by a quorum)
- **Prepare**: A phase-1 message broadcast by a candidate leader to establish a new view and fence out prior views
- **Promise**: A response from a cluster member to a Prepare, pledging not to acknowledge proposals from lower-numbered views
- **Quorum**: A majority subset of cluster members (floor(cluster_size / 2) + 1)
- **Fencing**: The mechanism by which a new view's Prepare/Promise exchange prevents prior-view proposals from reaching quorum
- **EventualConsistency (EC)**: Hydro's type-level guarantee that all live members of a cluster eventually observe the same values on a stream
- **broadcast_from_member**: A Hydro networking primitive enabling intra-cluster broadcast with EC inferred by the type system
- **Committed_Log**: The sequence of (slot, value) entries that have been committed across all views; the primary output of the protocol
- **View_Transfer_Proof**: A typed witness ensuring that a new view starts sequencing at a slot beyond all previously committed slots
- **LogEntry**: The output record type containing the committed payload, the view in which it was committed, and its slot index
- **Simulator**: Hydro's deterministic simulation engine that exhaustively explores non-deterministic execution schedules

## Requirements

### Requirement 1: Protocol Function Signature

**User Story:** As a Hydro application developer, I want the typed consensus protocol to expose the same input/output signature as Raft, so that I can substitute it without changing application code.

#### Acceptance Criteria

1. THE Typed_Consensus SHALL accept the following inputs: a stream of client requests typed as `Stream<T, Cluster<'a, ClusterTag, Con>, Unbounded, O>` where `Con` is a generic consistency parameter and `O` is a generic ordering parameter; a protocol configuration containing at minimum the cluster size as a `usize`; a network transport factory typed as `impl Fn() -> Net` where `Net: NetworkFor<ProtocolRpc<T, ClusterTag>>`; and a `NonDet` annotation
2. THE Typed_Consensus SHALL return a committed log stream typed as `Stream<LogEntry<T>, Atomic<Cluster<'a, ClusterTag, EventualConsistency>>, Unbounded, TotalOrder>` where `LogEntry<T>` contains the client payload of type `T`, the view number as a `usize`, and the slot index as a `usize`
3. THE Typed_Consensus SHALL return a redirected-requests stream typed as `Stream<(T, Option<MemberId<ClusterTag>>), Cluster<'a, ClusterTag, NoConsistency>, Unbounded, TotalOrder>` carrying the original request payload and optionally the identity of the current leader to redirect to
4. WHEN an application replaces a call to `raft(...)` with a call to `typed_consensus(...)`, THE Typed_Consensus SHALL require no changes to the types or structure of the surrounding dataflow; only the protocol configuration struct and any protocol-specific timer streams (election and heartbeat interrupts) may differ between the two call sites
5. IF the Typed_Consensus protocol does not require election or heartbeat timer streams as inputs, THEN THE Typed_Consensus SHALL omit those parameters from its signature rather than accepting and ignoring them

### Requirement 2: Per-View Proposal Broadcast with Inferred EC

**User Story:** As a protocol designer, I want each view's proposal broadcast to have EC inferred by the type system, so that no manual proof is needed for individual view correctness.

#### Acceptance Criteria

1. WHEN the Leader broadcasts proposals in a view, THE Typed_Consensus SHALL use `broadcast_from_member` with `fail_stop` transport so that the resulting stream's EC type is inferred by the compiler without any `manual_proof!` annotation
2. THE Typed_Consensus SHALL assign each client request a unique (view, slot) pair by sequencing requests through the leader within a `sliced!` block, where slot indices are contiguous starting from the view's start slot and each (view, slot) pair is globally unique across all views
3. WHILE a view is active, THE Typed_Consensus SHALL ensure that only the designated leader for that view produces proposals; non-leader members receiving client requests SHALL forward them to the redirected-requests output stream
4. THE Typed_Consensus SHALL ensure that no two proposals within a single view share the same slot index, enforced by the sequential slot counter within the leader's `sliced!` block

### Requirement 3: Per-View Commit Broadcast with Inferred EC

**User Story:** As a protocol designer, I want the commit notification broadcast to have EC inferred by the type system, so that the committed log's consistency follows from composition.

#### Acceptance Criteria

1. WHEN at least floor(N/2) + 1 cluster members (where N is the cluster size) acknowledge a Proposal for a given (view, slot), THE Typed_Consensus SHALL broadcast a Commit_Notification using `broadcast_from_member` with `fail_stop` transport so that EC is inferred by the compiler
2. THE Typed_Consensus SHALL count acknowledgements using a commutative fold (with exactly one `manual_proof!` on commutativity) and emit a commit notification only when the count reaches the quorum threshold; once emitted for a (view, slot), no additional commit notification SHALL be produced for that same (view, slot)
3. THE Typed_Consensus SHALL include the view number and slot index in each Commit_Notification so that members can reconstruct the committed log by joining commits with proposals on the (view, slot) composite key

### Requirement 4: Phase-1 Prepare/Promise (View Fencing)

**User Story:** As a protocol designer, I want a two-phase view-change mechanism that preemptively fences prior views, so that no two views can commit conflicting entries for the same slot.

#### Acceptance Criteria

1. WHEN a cluster member initiates a new view, THE Typed_Consensus SHALL broadcast a Prepare message (containing the new view number) to all cluster members using `broadcast_from_member`
2. WHEN a cluster member receives a Prepare with a view number strictly greater than its current maximum promised view, THE Typed_Consensus SHALL respond with a Promise (containing the new view number and the member's maximum committed slot index) and update its maximum promised view to the received view number
3. IF a cluster member receives a Prepare with a view number less than or equal to its current maximum promised view, THEN THE Typed_Consensus SHALL discard the Prepare without responding
4. WHILE a cluster member has promised view V, THE Typed_Consensus SHALL suppress acknowledgements for proposals from any view less than V such that those acknowledgements are never emitted to the network
5. WHEN the candidate leader collects Promises from a quorum (strictly more than half the cluster membership), THE Typed_Consensus SHALL produce a start signal that unblocks proposal sequencing in the new view, ensuring no proposal in the new view is emitted before this signal fires

### Requirement 5: Cross-View Safety via Quorum Read

**User Story:** As a protocol designer, I want view transitions to discover the maximum committed slot from the prior view, so that the new view starts sequencing beyond all previously committed slots.

#### Acceptance Criteria

1. WHEN a new leader collects responses from at least floor(N/2) + 1 members (where the cluster has N members) after broadcasting a Prepare, THE Typed_Consensus SHALL extract the maximum committed slot reported by each responding member
2. WHEN the quorum of Promise responses has been collected, THE Typed_Consensus SHALL compute the new view's start slot as max_committed + 1, where max_committed is the maximum committed slot value across all quorum responses (yielding start slot 1 when no slots have been committed and all members report 0)
3. IF fewer than floor(N/2) + 1 Promise responses have been received, THEN THE Typed_Consensus SHALL not emit a start slot and SHALL continue accumulating responses across subsequent ticks until the quorum threshold is met
4. THE Typed_Consensus SHALL encapsulate the quorum-intersection safety argument in a View_Transfer_Proof requiring exactly one `manual_proof!` annotation attesting that any committed entry was acknowledged by floor(N/2) + 1 members, so by pigeonhole at least one quorum respondent witnessed every committed entry

### Requirement 6: Committed Log Composition

**User Story:** As a protocol designer, I want the global committed log to be the union of per-view committed entries, so that EC of the whole follows from EC of the parts.

#### Acceptance Criteria

1. THE Typed_Consensus SHALL compute the committed log on each member by performing an inner join of the proposal stream with the commit notification stream on the composite key (view, slot), emitting one committed entry per matched pair
2. THE Typed_Consensus SHALL merge committed entries from all views into a single stream using an unordered merge, producing at most one committed entry per slot index across all views
3. IF two committed entries with the same slot index but different values appear in the merged stream, THEN THE Typed_Consensus SHALL treat this as a safety violation detectable by the simulator
4. THE Typed_Consensus SHALL produce the committed log as a TotalOrder stream ordered by ascending slot index, wrapped in an Atomic boundary such that each observation of the log is a prefix-consistent snapshot of committed entries
5. THE Typed_Consensus SHALL guarantee that the committed log on each member is EventuallyConsistent, derived from the EC property of the per-view commit streams combined via the conflict-free unordered merge

### Requirement 7: Minimal Manual Proof Surface

**User Story:** As a protocol designer, I want the number of manual_proof! annotations to be minimal and auditable, so that correctness confidence is high.

#### Acceptance Criteria

1. THE Typed_Consensus module SHALL require zero manual_proof! annotations for per-view eventually-consistent broadcasts (prepare, proposal, and commit broadcasts), with EC inferred entirely by the type system via broadcast_from_member
2. THE Typed_Consensus module SHALL require exactly one manual_proof! for quorum intersection safety (the View_Transfer_Proof constructor), justifying that once f+1 members promise a view, at most one view commits each slot
3. THE Typed_Consensus module SHALL require exactly one manual_proof! for commutativity of ack counting within the commit threshold fold
4. THE Typed_Consensus module SHALL contain a total of exactly 2 manual_proof! annotations, and no manual_proof! annotations beyond those listed in criteria 2 and 3
5. WHEN auditing the Typed_Consensus module, THE manual_proof! annotations SHALL each contain a doc comment explaining the correctness argument in 1-3 sentences

### Requirement 8: Simulation Testability

**User Story:** As a protocol developer, I want the typed consensus protocol to be testable under Hydro's deterministic simulator, so that I can verify safety under all possible execution schedules.

#### Acceptance Criteria

1. THE Typed_Consensus SHALL expose its dataflow construction as a function that accepts `sim_input` streams (for client requests and cluster configuration) and produces streams terminating in `sim_output` or `sim_cluster_output`, such that the resulting `FlowBuilder` can be passed to both `flow.sim().exhaustive(...)` and `flow.sim().fuzz(...)`
2. WHEN the simulator explores all batch boundaries and message orderings via `exhaustive` or `fuzz`, THE Typed_Consensus SHALL never produce two different committed values for the same slot index across any cluster member (safety property)
3. WHILE a single view is active and all cluster members are reachable and no leader change occurs, THE Typed_Consensus SHALL commit every submitted request within a simulator-bounded number of ticks not exceeding 50 ticks after submission (liveness property under no failures)
4. THE Typed_Consensus SHALL support configurable cluster sizes of 3 to 7 nodes via `with_cluster_size` for simulation tests
5. THE Typed_Consensus SHALL produce a committed log stream whose consistency type is `EventuallyConsistent` as inferred by the Hydro type system without manual type assertions

### Requirement 9: View Change and Leader Election

**User Story:** As a Hydro application developer, I want the protocol to handle leader changes, so that the system remains available when a leader fails or a new view is triggered.

#### Acceptance Criteria

1. WHEN an election timer expires on a cluster member, THE Typed_Consensus SHALL initiate a new view with a view number equal to the member's current view number plus one
2. WHEN a follower member receives a heartbeat from the current leader whose view number equals the follower's current view number, THE Typed_Consensus SHALL reset that follower's election timer to its full duration
3. IF a follower member receives a heartbeat with a view number less than the follower's current view number, THEN THE Typed_Consensus SHALL discard the heartbeat without resetting the election timer
4. IF a leader stops broadcasting heartbeats such that no heartbeat is received by a follower within one full election timeout period, THEN THE Typed_Consensus SHALL allow that follower's election timer to expire and trigger a view change
5. WHEN a view change completes with a quorum of promises collected (where quorum equals floor(N/2) + 1 members for a cluster of size N), THE Typed_Consensus SHALL resume accepting client requests in the new view with all entries committed in prior views present in the new leader's log
6. WHILE a new leader is collecting promises during a view change, THE Typed_Consensus SHALL reject client requests directed at that leader until the quorum of promises is reached

### Requirement 10: Performance Comparability

**User Story:** As a systems researcher, I want the typed consensus protocol to achieve throughput and latency within the same order of magnitude as the Raft implementation, so that the composable approach is practical.

#### Acceptance Criteria

1. WHEN processing a sustained stream of client requests for at least 30 seconds after a 5-second warm-up period with a cluster of 3 nodes and a stable leader (no leader election occurring during the measurement window), THE Typed_Consensus SHALL achieve median commit throughput (requests committed per second) within 2x of the Raft implementation under identical deployment conditions (same cluster size, same hardware, same network configuration)
2. WHEN processing a single client request with a stable leader (leader established and idle for at least 1 second with no in-flight requests), THE Typed_Consensus SHALL achieve p50 commit latency within 2x of the Raft implementation under identical deployment conditions, measured over at least 100 sequential single-request trials on a 3-node cluster
3. THE Typed_Consensus SHALL provide a benchmark harness that executes both Typed_Consensus and Raft workloads using the same client driver, request payload, and cluster topology on any of the supported deployment targets (local, GCP, AWS)
4. IF the Typed_Consensus exceeds the 2x throughput or latency threshold in any benchmark run, THEN THE benchmark harness SHALL report the measured ratio and the absolute values for both implementations

### Requirement 11: Correct Fencing Under Concurrent Views

**User Story:** As a protocol designer, I want the fencing mechanism to be preemptive rather than reactive, so that the simulator cannot find a safety violation where two views commit the same slot.

#### Acceptance Criteria

1. THE Typed_Consensus SHALL gate proposal broadcasting on receiving acknowledgements from a majority (at least floor(N/2) + 1 out of N members) of promise responses for the current view, ensuring no proposal message for view V is sent to any replica before the leader of view V has received the promise quorum for view V
2. WHEN two views overlap (the leader of view V-1 has not failed and a new leader initiates phase-1 for view V), THE Typed_Consensus SHALL ensure that at most one view can achieve a phase-2 accept quorum (majority acknowledgement of a proposal) for any given slot number, because once f+1 members promise view V they will reject accept requests from view V-1
3. WHEN the simulator fuzzes concurrent view scenarios across all possible message orderings and batch boundaries, THE Typed_Consensus SHALL never produce two commit notifications for the same slot number that contain different values, regardless of how many views attempt to propose for that slot
4. IF a leader of view V broadcasts a proposal for slot S before receiving its phase-1 promise quorum for view V, THEN THE Typed_Consensus SHALL treat this as a protocol violation detectable by the simulator as an invalid causal ordering
