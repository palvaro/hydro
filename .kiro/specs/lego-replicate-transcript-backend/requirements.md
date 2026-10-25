# Requirements Document

## Introduction

This feature integrates the existing `broadcast_transcript_consensus` protocol as the ordering and replication engine inside the `lego_replicate` transparent-replication library. This is "Option B": rather than swapping `broadcast_transcript_consensus` in for individual `lego_replicate` protocol primitives, the integration uses `broadcast_transcript_consensus` — which already emits a committed total order of entries — to replace `lego_replicate`'s hand-composed ordering path (slot assignment, star-accumulate replication, decide, and ordered delivery) as a single ordering engine.

The `ReplicableService` trait, the application adapter, and the client router remain on top of the new engine. The engine sequences opaque byte payloads; only the application adapter deserializes them and applies them to the user's service. The committed total order is applied deterministically on every replica, and responses are routed back to the originating client.

Two prerequisites and known blockers frame this work. First, the `lego-replicate-v2` branch does not currently compile against the in-tree `hydro_lang` because the `sim_input` API changed from one generic parameter to three. Second, the branch's last working state reported `throughput=0` because the client-response-routing path was never completed — the same "single designated responder" bottleneck class seen when wiring `broadcast_transcript_consensus` to a linearizable key-value workload. Both must be addressed for the integration to function.

This document specifies requirements only. No implementation is described here.

## Glossary

- **Lego_Replicate**: The transparent-replication library that exposes the `ReplicableService` trait, an application adapter, and a client router. Referred to as the library under integration.
- **Replicable_Service**: The user-implemented trait (`ReplicableService`) with an observationally deterministic `apply(&mut self, Command) -> Response`, plus `is_read_only(&Command) -> bool`, `snapshot(&self) -> Vec<u8>`, and `restore(&mut self, &[u8])`, and associated types `Command` and `Response`.
- **Application_Adapter**: The `lego_replicate` component that deserializes committed opaque payloads, invokes `Replicable_Service::apply`, and produces responses for routing. Formerly step 7 of the composed pipeline.
- **Client_Router**: The `lego_replicate` component (`Router`) that directs client requests to the ordering engine and routes responses back to the originating client.
- **Consensus_Engine**: The `broadcast_transcript_consensus` protocol used as the ordering and replication engine. It performs its own leader election and emits a committed total order of entries.
- **Committed_Order**: The total order of committed entries emitted by the Consensus_Engine, applied deterministically on every replica.
- **Opaque_Payload**: A `Vec<u8>` command payload sequenced by the Consensus_Engine without interpretation; deserialized only by the Application_Adapter.
- **Read_Only_Command**: A command for which `Replicable_Service::is_read_only` returns true.
- **EC_Boundary**: The point at which the EventualConsistency-typed committed output of the Consensus_Engine is converted for downstream consumption, requiring a single `assert_has_consistency_of` (the paxos_ec / raft pattern).
- **Legacy_Path**: The current `lego_replicate` replication path backed by `CorePaxos` and the hand-composed 8-step pipeline (view manager, state transfer, slot assignment, star-accumulate, decide, ordered deliver, application adapter, read-only bypass).
- **Throughput**: Committed client operations completed per unit time, as measured by the existing benchmark harness (`hydro_std::bench_client`).

## Requirements

### Requirement 1: Prerequisite — Lego_Replicate Compiles Against Current Hydro

**User Story:** As a Hydroflow developer, I want `lego_replicate` to compile and run against the in-tree `hydro_lang` before any engine swap, so that the integration starts from a known-good baseline.

#### Acceptance Criteria

1. WHEN the Lego_Replicate library is compiled against the in-tree `hydro_lang`, THE Lego_Replicate library SHALL complete compilation with zero compile errors.
2. WHERE the `sim_input` API requires three generic parameters (`T`, `O: Ordering`, `R: Retries`), THE Lego_Replicate library SHALL supply all three parameters at every `sim_input` call site.
3. WHEN the Legacy_Path is exercised against the in-tree `hydro_lang`, THE Lego_Replicate library SHALL execute every existing failover end-to-end test and terminate each test with a definitive pass or fail result.
4. WHEN the failover end-to-end test suite is executed against the in-tree `hydro_lang`, THE Lego_Replicate library SHALL report zero failing tests to establish the known-good baseline.
5. WHILE the Consensus_Engine integration has not passed its equivalent failover end-to-end tests, THE Lego_Replicate library SHALL keep the Legacy_Path executable and passing all of its failover end-to-end tests.

### Requirement 2: Consensus_Engine Replaces the Ordering Path

**User Story:** As a Hydroflow developer, I want `broadcast_transcript_consensus` to provide the committed total order that `lego_replicate` previously produced through slot assignment, star-accumulate, decide, and ordered delivery, so that a single ordering engine drives replication.

#### Acceptance Criteria

1. THE Lego_Replicate library SHALL produce the Committed_Order exclusively through the Consensus_Engine, and SHALL NOT invoke the Legacy_Path steps of slot assignment, star-accumulate replication, decide, or ordered delivery.
2. THE Consensus_Engine SHALL sequence each client command as an Opaque_Payload while preserving its byte content unchanged and without deserializing it.
3. WHEN the Consensus_Engine commits Opaque_Payloads, THE Lego_Replicate library SHALL deliver them to the Application_Adapter in ascending sequence-position order, each paired with its assigned sequence position.
4. THE Committed_Order SHALL assign sequence positions that are unique, contiguous, and monotonically increasing, with no gaps and no duplicates.
5. THE Lego_Replicate library SHALL preserve the `Replicable_Service` trait, the Application_Adapter, and the Client_Router as the layer on top of the Consensus_Engine.

### Requirement 3: Deterministic Application on All Replicas

**User Story:** As a distributed systems engineer, I want every replica to apply the committed total order identically, so that all replicas reach the same service state.

#### Acceptance Criteria

1. WHEN the Committed_Order is delivered to a replica, THE Application_Adapter SHALL deserialize each Opaque_Payload into a `Replicable_Service::Command` and invoke `apply` exactly once per payload in ascending Committed_Order sequence position, beginning at the first not-yet-applied position and skipping no position.
2. THE Application_Adapter SHALL apply committed commands on every replica strictly in ascending Committed_Order sequence position, such that for any two positions i and j where i < j, the command at position i is applied before the command at position j.
3. WHEN two replicas have each applied the identical prefix of the Committed_Order (sequence positions 0 through N), THE replicas SHALL produce byte-for-byte equal results from `snapshot`.
4. IF deserialization of an Opaque_Payload at a Committed_Order sequence position fails, THEN THE Application_Adapter SHALL halt application at that position, leave the `Replicable_Service` state unchanged from immediately before that position (no partial or corrupted command applied), and surface an error identifying the failed sequence position and indicating the deserialization failure cause.
5. IF an Opaque_Payload at a given Committed_Order sequence position is delivered more than once, THEN THE Application_Adapter SHALL invoke `apply` for that position at most once.

### Requirement 4: Response Routing Without a Single-Node Bottleneck

**User Story:** As a client of the replicated service, I want my response returned to me by the replica that handled my request, so that throughput is not serialized through one designated responder.

#### Acceptance Criteria

1. WHEN the Application_Adapter produces a `Response` for a committed command, THE Client_Router SHALL route that `Response` to exactly the one client that originated the corresponding request, as identified by the `Response`'s correlation identifier.
2. THE Client_Router SHALL return each `Response` from the replica that committed the corresponding command, such that no single node handles more than `ceil(100/N)` percent of total responses under balanced load, where `N` is the number of active replicas.
3. WHILE client requests are issued at a sustained rate of at least 1,000 requests per second for at least 60 continuous seconds, THE Lego_Replicate library SHALL report a Throughput strictly greater than 0 requests per second, replacing the prior `throughput=0` state.
4. WHEN a client request enters the Consensus_Engine, THE Consensus_Engine SHALL attach a unique correlation identifier that remains associated with the resulting committed `Response`, so THE Client_Router can match each `Response` to its originating request.
5. IF a committed `Response` carries a correlation identifier that matches no pending originating request, THEN THE Client_Router SHALL discard that `Response` without routing it to any client and SHALL record an error indication identifying the unmatched correlation identifier.
6. IF the originating client connection is no longer available when its `Response` is ready to route, THEN THE Client_Router SHALL drop that `Response`, record an error indication, and continue routing other responses without blocking.
7. WHERE a single-responder design is intentionally retained for a specific configuration, THE Lego_Replicate library SHALL document that tradeoff and its Throughput impact explicitly.

### Requirement 5: EventualConsistency Boundary Preservation

**User Story:** As a Hydroflow developer, I want the EC boundary of the Consensus_Engine handled correctly, so that the committed stream carries the consistency guarantees the integration depends on.

#### Acceptance Criteria

1. THE integration SHALL apply exactly one `assert_has_consistency_of` assertion at the EC_Boundary, where EC_Boundary is defined as the single point at which the EventualConsistency-typed committed output of the Consensus_Engine is first consumed by the ordering-engine integration, and the asserted consistency SHALL be EventualConsistency, matching the placement used in the paxos_ec and raft implementations.
2. THE integration SHALL preserve the EventualConsistency consistency parameter on every operation applied to the Consensus_Engine committed output between its production and the EC_Boundary assertion, introducing no operation that changes that parameter to a different or weaker consistency and no `manual_proof!` on consistency applied to that output.
3. THE integration SHALL contain exactly one consistency assertion in total — the EC_Boundary assertion of criterion 1 — and no other `assert_has_consistency_of` anywhere in the ordering-engine integration.
4. IF the ordering-engine integration contains a consistency assertion other than the single EC_Boundary assertion, or contains zero consistency assertions, THEN THE integration SHALL be treated as non-conforming and SHALL fail verification with an indication identifying the offending or missing assertion.

### Requirement 6: Read-Only Command Handling

**User Story:** As a client issuing read-only operations, I want reads served correctly under the Consensus_Engine leader model, so that read-only optimizations are reconciled with the new engine rather than silently broken.

#### Acceptance Criteria

1. WHEN a command satisfies `Replicable_Service::is_read_only`, THE Lego_Replicate library SHALL return a `Response` that reflects the effects of every write present in the serving replica's Committed_Order at the time the read is evaluated, and no writes ordered after that point.
2. WHERE the Consensus_Engine leader model supports leader-local reads, WHILE the serving replica holds a valid leader role as reported by the Consensus_Engine, THE Lego_Replicate library SHALL serve Read_Only_Commands from that replica without appending them to the Committed_Order.
3. IF a Read_Only_Command is received by a replica that does not hold a valid leader role and leader-local reads are enabled, THEN THE Lego_Replicate library SHALL NOT return a locally-evaluated `Response` and SHALL return an error indication that the read cannot be served consistently, without modifying the Committed_Order.
4. WHERE leader-local reads are not reconciled with the Consensus_Engine leader model, THE Lego_Replicate library SHALL route Read_Only_Commands through the Committed_Order such that each read's `Response` reflects the effects of all writes ordered before it in the Committed_Order.
5. THE Lego_Replicate library SHALL expose, in its published documentation, which of the two read-only strategies (leader-local reads or Committed_Order-routed reads) is active and the consistency level each strategy provides.

### Requirement 7: View Change and State Transfer Reconciliation

**User Story:** As a Hydroflow developer, I want the redundant view-management and state-transfer machinery reconciled with the Consensus_Engine's own leader election, so that there is a single source of truth for leadership.

#### Acceptance Criteria

1. THE integration SHALL derive leadership identity (leader member and ballot/term) exclusively from the Consensus_Engine, and SHALL NOT source leadership identity from the Legacy_Path `CorePaxos` view manager.
2. WHERE the Legacy_Path `CorePaxos` view manager duplicates leadership sequencing already performed by the Consensus_Engine, THE integration SHALL ensure the duplicated view manager produces no leadership or view-change output consumed by any downstream component.
3. IF the Legacy_Path `CorePaxos` view manager and the Consensus_Engine disagree on the current leader, THEN THE integration SHALL resolve leadership in favor of the Consensus_Engine.
4. WHEN a recovering or lagging replica rejoins, THE integration SHALL reconstruct that replica's state from the Consensus_Engine's Committed_Order such that its applied prefix equals the Committed_Order prefix for the slots it has received.
5. IF replica state recovery from the Committed_Order fails, THEN THE integration SHALL leave the replica's state unchanged (no partial prefix applied) and surface a recovery-failure indication.
6. THE integration SHALL document exactly one decision — retained, replaced, or removed — for the Legacy_Path state-transfer primitive, together with the rationale.

### Requirement 8: Leader Failover

**User Story:** As an operator, I want a leader crash tolerated by the integrated system, so that the replicated service continues serving after re-election and recovery.

#### Acceptance Criteria

1. WHEN the current leader stops emitting Accept traffic and keepalives for the current ballot, AND a quorum (cluster_size / 2 + 1) of replicas remains reachable, THE Consensus_Engine SHALL elect a new leader within a bounded number of election windows (not exceeding 3 election windows).
2. WHEN a new leader is elected and accumulates a quorum of Promises, THE Consensus_Engine SHALL resume producing Committed_Order entries within a bounded number of ticks (not exceeding 10 ticks).
3. WHEN a new leader is elected after a crash, THEN for every Slot committed before the crash, THE surviving replicas SHALL hold the identical value in their Committed_Order for that Slot (no fork).
4. WHILE a quorum of replicas remains reachable after a leader crashes mid-load, THE client operations that were submitted but not yet present in the Committed_Order SHALL commit under the new leader within a bounded number of ticks (not exceeding 20 ticks) after the new leader begins producing Committed_Order entries.
5. WHILE Accept traffic or keepalives for the current ballot are observed within the current election window, IF a follower's election timer fires, THEN THE Consensus_Engine SHALL NOT start a competing election (leader-activity gating).

### Requirement 9: Linearizable Correctness Preservation

**User Story:** As a distributed systems engineer, I want the integrated system to preserve linearizable key-value behavior and log prefix consistency, so that the swap does not regress correctness.

#### Acceptance Criteria

1. THE integrated system SHALL preserve linearizable behavior for the replicated key-value service such that an external linearizability checker (for example, Maelstrom lin-kv) and the library's own test suite both report zero linearizability violations across a complete validation run.
2. FOR ALL pairs of replicas, THE integrated system SHALL maintain committed logs that are prefix-consistent, such that one replica's committed log is a prefix of the other replica's committed log, both replicas store identical values at every shared log slot, and the count of divergent (forked) slots is zero.
3. WHILE clients issue operations continuously for a sustained-load validation period of at least 60 seconds, THE integrated system SHALL return only fully committed values and SHALL expose zero torn (partially written) values to clients.
4. IF a previously committed value would be rolled back or replaced by a divergent value at the same log slot, THEN THE integrated system SHALL reject the conflicting operation, preserve the previously committed value, and surface an indication that the operation was rejected.
5. WHEN the swap is applied, THE integrated system SHALL pass all existing `lego_replicate` failover end-to-end tests with zero test failures.

### Requirement 10: Benchmark Comparison Against the Legacy Path

**User Story:** As an evaluator, I want the integrated engine benchmarked against the existing Paxos-backed `lego_replicate` path, so that the performance effect of the swap is measured.

#### Acceptance Criteria

1. THE benchmark SHALL measure Throughput of the Consensus_Engine-backed path and the Legacy_Path using the same `hydro_std::bench_client` harness configuration, including identical client count, workload, and cluster size across both paths.
2. THE benchmark SHALL discard a fixed warmup window and report steady-state Throughput measured over a fixed measurement window, using the same warmup and measurement window sizes for both paths.
3. THE benchmark SHALL report the Throughput distribution (minimum, median, mean, and maximum) for each path.
4. THE benchmark SHALL report the Consensus_Engine-backed Throughput relative to the Legacy_Path Throughput as a ratio (or percentage) over the median Throughput.
5. THE benchmark results SHALL state caveats naming transport, cluster size, and read-only strategy so that reported differences reflect the protocols rather than harness artifacts.
6. IF either path fails to reach steady state within its warmup window, THEN THE benchmark SHALL flag that run as invalid and SHALL exclude it from the reported comparison.
