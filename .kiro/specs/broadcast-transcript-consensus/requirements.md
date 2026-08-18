# Requirements Document

## Introduction

A consensus protocol that follows the architecture of `broadcast_consensus.rs`: every protocol message is `broadcast_from_member` to all members, every member sees the same EC transcript, and each member folds that transcript with a commutative decision function to extract committed entries.

The protocol borrows correctness mechanisms from Paxos (ballot fencing, quorum counting, Phase1Certificate recovery) as needed to guarantee agreement, but the plumbing is `broadcast_consensus` plumbing — not the dual-path architecture of `paxos_ec`, and not the `raft_step` state machine loop.

API-compatible with `raft_server` (same output type signatures). Testing parity with `raft.rs`.

## Glossary

- **Transcript**: The EC stream produced by `broadcast_from_member` + `fail_stop` — every protocol message, visible to every member
- **Decision Function**: The commutative fold over the Transcript that extracts committed entries by counting quorums
- **Ballot**: `round * cluster_size + member_id` — globally unique, totally ordered
- **Slot**: Zero-indexed log position
- **Quorum**: `cluster_size / 2 + 1`

## Requirements

### Requirement 1: Broadcast Transcript Architecture

**User Story:** As a Hydroflow developer, I want a consensus protocol that follows the `broadcast_consensus.rs` pattern — broadcast all messages, fold the transcript — so that EC is fully inferred without manual consistency proofs.

#### Acceptance Criteria

1. Every protocol message (Prepare, Promise, Accept, AcceptAck) SHALL be broadcast to all members via `broadcast_from_member` with `TCP.fail_stop().bincode()`
2. The committed log SHALL be derived from a commutative fold over the EC transcript — the same pattern as the `todo!()` in `broadcast_consensus.rs`, but implemented
3. Zero `assert_has_consistency_of` in the module
4. `manual_proof!` annotations SHALL only justify commutativity/idempotency of fold operations — never consistency

### Requirement 2: Decision Function

**User Story:** As a Hydroflow developer, I want the decision function to be a commutative, idempotent fold that extracts committed entries from the transcript by counting quorums per slot.

#### Acceptance Criteria

1. The fold SHALL be commutative: same messages in any order produce the same result
2. The fold SHALL be idempotent: duplicate messages do not change state
3. When the fold observes AcceptAck messages from a quorum of distinct senders for the same (slot, ballot), it SHALL mark that slot committed with the corresponding value
4. The fold SHALL track per-slot: highest promised ballot, highest accepted (ballot, value), and the set of AcceptAck senders per (slot, ballot)

### Requirement 3: Protocol Correctness

**User Story:** As a distributed systems engineer, I want agreement, validity, and liveness guarantees borrowed from Paxos.

#### Acceptance Criteria

1. Agreement: if two members both commit slot S, they commit the same value
2. Ballot fencing: a member that has seen Prepare(B) rejects Accept messages with ballot < B
3. Phase1Certificate required before issuing Accepts for a new ballot
4. Paxos recovery: a new leader re-proposes the highest-ballot previously-accepted value per slot
5. Validity: every committed value was originally a client proposal
6. Liveness: under a stable leader with quorum reachable, every proposal eventually commits

### Requirement 4: Message Generation

**User Story:** As a Hydroflow developer, I want each member to generate protocol messages (Prepare, Promise, Accept, AcceptAck) in response to local events and the observed transcript, feeding them back into the broadcast.

#### Acceptance Criteria

1. On election timer (no recent leader activity observed): broadcast Prepare with a fresh ballot
2. On observing Prepare with ballot > current promise: broadcast Promise carrying previously-accepted entries
3. On accumulating a quorum of Promises: begin broadcasting Accept for pending proposals
4. On observing Accept with ballot >= current promise: broadcast AcceptAck
5. Non-leaders redirect client requests with a leader hint
6. Leaders suppress their own election timer

### Requirement 5: API Compatibility with Raft

**User Story:** As a Hydroflow developer, I want this to be a drop-in replacement for `raft_server`.

#### Acceptance Criteria

1. Committed output: `Stream<LogEntry<T>, Atomic<Cluster<'a, ClusterTag, EventualConsistency>>, Unbounded, TotalOrder>`
2. Redirected output: `Stream<(T, Option<MemberId<ClusterTag>>), Cluster<'a, ClusterTag, NoConsistency>, Unbounded, TotalOrder>`
3. Leader views output: `Stream<LeaderView<ClusterTag>, Cluster<'a, ClusterTag>>`
4. Inputs: `requests` stream, `election_timer_interrupts` stream, network policy, config with `cluster_size`
5. No `heartbeat_timer_interrupts` required — leader activity is observed directly from the transcript

### Requirement 6: Testing Parity with Raft

**User Story:** As a Hydroflow developer, I want the same categories of correctness tests as `raft.rs`.

#### Acceptance Criteria

1. Safety: concurrent elections + message reorderings never fork the committed log
2. Liveness: stable leader commits proposals within bounded ticks
3. Convergence: after quiescence, all members hold the same committed prefix
4. Election: at most one leader per ballot; contested elections resolve via retry
5. Replication: entries survive leader changes (Figure 8 analog)
6. Deterministic harness driving the protocol with controlled delivery
7. Fuzz/simulation tests exploring random delivery schedules

### Requirement 7: Plumbing Patterns and EC Cycle

**User Story:** As a Hydroflow developer, I want the implementation to use the EC-preserving `forward_ref` cycle pattern from `reliable_broadcast` and `crdt_gossip`, so that EC propagates through the message generation feedback loop without manual proofs.

#### Acceptance Criteria

1. `broadcast_from_member` with `TCP.fail_stop().bincode()` for the transcript — this is the EC source
2. The `forward_ref` SHALL be declared on the EC-typed location obtained from `broadcast_from_member`'s output (i.e., `transcript.location().forward_ref::<...>()`) — the same trick as `reliable_broadcast_closed` and `g_set_gossip`
3. The stream that completes the `forward_ref` SHALL also pass through `broadcast_from_member` (or `broadcast_closed`) with `fail_stop`, so EC types match around the cycle — no `manual_proof!` on consistency needed to close the loop
4. Message generation (Prepare, Promise, Accept, AcceptAck) feeds back into the transcript via this cycle: generated messages → broadcast → EC transcript → fold + generate more messages
5. `nondet!()` on batch operations documenting acknowledged non-determinism
6. `manual_proof!()` only on fold commutativity/idempotency
7. `sliced!` blocks where tick-scoped atomic processing is needed

## Extension Requirements: End-to-End KVS Comparison with MultiPaxos

> These requirements extend the spec to evaluate broadcast-transcript consensus
> against MultiPaxos (`CorePaxos` via the `PaxosLike` trait — **not** paxos-ec)
> using the same replicated key-value store harness that `paxos_bench` uses.

### Requirement 8: Bounded State via Checkpointing

**User Story:** As an operator running the protocol under sustained load, I want per-member state and per-tick work to stay bounded, so throughput does not degrade over time and memory does not grow without bound.

#### Glossary additions

- **Checkpoint**: A sequence number `s` signalling that all slots `< s` have been applied to the state machine (KV store) and their consensus bookkeeping may be discarded.

#### Acceptance Criteria

1. WHEN the state machine (`kv_replica`) emits a checkpoint sequence `s`, THE consensus module SHALL prune per-slot bookkeeping (`ack_sets`, `accepted`, `committed_slots`) for all slots `< s`.
2. THE committed log emitted with checkpoint-driven truncation SHALL be identical to the committed log emitted without truncation — truncation reclaims memory only and never changes observable output (Truncation Safety).
3. WHILE under sustained load with periodic checkpoints, THE per-tick processing cost SHALL NOT grow monotonically with the total number of committed entries (no throughput decay).
4. THE emission frontier SHALL be preserved across truncation so that no committed entry is re-emitted or dropped.

### Requirement 9: Failover Under Idle and Load

**User Story:** As an operator, I want correct leader failover whether the cluster is busy or idle, without spurious elections while a leader is healthy.

#### Acceptance Criteria

1. WHILE a leader is observed active in the transcript (Accept traffic for the current ballot), a follower whose election timer fires SHALL NOT start a competing election (leader-activity gating).
2. WHEN the leader is idle (no client requests), THE leader SHALL emit periodic keepalives so followers can distinguish a live idle leader from a failed one.
3. WHEN a leader stops emitting activity and keepalives, a follower SHALL detect the absence within a bounded number of election windows and campaign.
4. THE "no heartbeat" property SHALL be understood to apply to *replication* only; liveness detection uses a keepalive timer (input), analogous to raft's `heartbeat_timer_interrupts`.

### Requirement 10: KVS Integration and Linearizability

**User Story:** As a Hydroflow developer, I want broadcast-transcript consensus to drive a replicated KV store via `kv_replica`, so I can test full end-to-end functionality against MultiPaxos.

#### Acceptance Criteria

1. THE committed output SHALL be adaptable to the `Stream<(usize, Option<KvPayload<K,V>>), Cluster<Replica>, Unbounded, NoOrder>` interface consumed by `kv_replica`, using `LogEntry.slot` as the sequence number.
2. THE checkpoint sequence emitted by `kv_replica` SHALL feed back into the consensus module's checkpoint input (Requirement 8).
3. WHEN clients issue KV operations under sustained load, all replicas' KV stores SHALL converge to the same state, and reads SHALL never observe torn or rolled-back values (linearizable prefix).
4. WHEN the leader fails mid-load, pending operations SHALL eventually commit under a new leader, and surviving replicas SHALL agree on all committed slots (Figure-8 safety).

### Requirement 11: Fair Benchmark Methodology

**User Story:** As an evaluator, I want an apples-to-apples comparison against MultiPaxos, so that reported differences reflect the protocols, not benchmark artifacts.

#### Acceptance Criteria

1. THE benchmark harness (client count, workload generator, cluster size, checkpoint frequency, measurement window) SHALL be identical for broadcast-transcript and MultiPaxos.
2. Pacing knobs (keepalive interval for broadcast-transcript, heartbeat interval for MultiPaxos) SHALL be matched or independently parameterized, and their effect on closed-loop latency documented, so neither protocol is artificially throttled.
3. Reported results SHALL discard warmup windows and report steady-state throughput (min/median/mean/max) and latency percentiles (p50/p99).
4. THE comparison SHALL be run at cluster sizes 3, 5, and 7 to expose the O(n²) (broadcast-transcript) vs O(n) (MultiPaxos) message-complexity crossover.
5. Reported results SHALL state honest caveats (localhost transport, cluster size, idle-period behavior).
