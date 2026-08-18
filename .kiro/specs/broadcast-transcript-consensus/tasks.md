# Implementation Plan: Broadcast Transcript Consensus

## Overview

Implement a consensus protocol following the `broadcast_consensus.rs` pattern in `hydro_test/src/cluster/broadcast_transcript_consensus.rs`. The module broadcasts every protocol message via `broadcast_from_member`, folds the EC transcript with a commutative decision function to extract commits, and generates protocol messages reactively in a `sliced!` block. API-compatible with `raft_server` outputs.

Build order: types → decision function (pure, testable standalone) → message generation → dataflow wiring (forward_ref cycle) → integration tests.

## Tasks

- [x] 1. Define core types and module skeleton
  - [x] 1.1 Create `hydro_test/src/cluster/broadcast_transcript_consensus.rs` with module-level doc comment, imports, and type definitions
    - Define `Ballot` (type alias for `usize`), `Slot` (type alias for `usize`)
    - Define `TranscriptMsg<T, ClusterTag>` enum with variants: `Prepare`, `Promise`, `Accept`, `AcceptAck` (as specified in design)
    - Define `DecisionState<T, ClusterTag>` struct with fields: `promises`, `accepted`, `ack_sets`, `committed_slots`, `committed_log`
    - Define `MessageGenState<T, ClusterTag>` struct with fields: `max_promised`, `current_round`, `accepted`, `pending_requests`, `next_slot`, `is_leader`, `known_leader`, `promises_received`, `phase1_complete`
    - Define `BroadcastConsensusConfig` struct with `cluster_size` field
    - Define `LogEntry<T>` struct (with `message`, `ballot`, `slot` fields)
    - Define `LeaderView<ClusterTag>` struct (with `ballot`, `leader` fields)
    - Define `BroadcastConsensusOutputs<'a, T, ClusterTag>` struct matching `RaftOutputs` type signatures
    - Derive `Serialize`, `Deserialize`, `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash` as appropriate
    - Add the module to `hydro_test/src/cluster/mod.rs`
    - _Requirements: 1.1, 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 2. Implement the decision function (commutative fold)
  - [x] 2.1 Implement `DecisionState::new()` and `DecisionState::process(msg, quorum_size)` method
    - `process` handles each `TranscriptMsg` variant:
      - `Prepare`: record promise (update highest promised ballot per-slot is NOT needed here — that's message generation's concern; decision function just observes)
      - `Promise`: no-op for commit extraction (promises don't directly cause commits)
      - `Accept`: record accepted (slot, ballot, value) — highest ballot per slot
      - `AcceptAck`: insert sender into `ack_sets[(slot, ballot)]`; if set reaches quorum, mark slot committed
    - Committed entries withheld until all preceding slots are also committed (gap-filling for TotalOrder output)
    - Implement `committed_entries(&self) -> &[LogEntry<T>]` accessor
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 2.2 Write property test: Fold Commutativity (Property 1)
    - **Property 1: Fold Commutativity**
    - Generate random `Vec<TranscriptMsg>`, permute, apply fold in different orders, assert same `committed_log`
    - **Validates: Requirements 1.2, 2.1**

  - [x] 2.3 Write property test: Fold Idempotency (Property 2)
    - **Property 2: Fold Idempotency**
    - Generate random `DecisionState` and `TranscriptMsg`, apply message twice, assert state unchanged after second application
    - **Validates: Requirements 2.2**

  - [x] 2.4 Write property test: Quorum Threshold Commitment (Property 3)
    - **Property 3: Quorum Threshold Commitment**
    - Generate Accept + AcceptAck messages for a slot; assert committed iff `|ack_set| >= quorum_size`
    - **Validates: Requirements 2.3**

  - [x] 2.5 Write property test: Agreement (Property 4)
    - **Property 4: Agreement**
    - Generate valid protocol traces, take two different subsets of the transcript, apply fold to each, assert if both commit slot S they commit the same value
    - **Validates: Requirements 3.1, 6.1**

  - [x] 2.6 Write property test: Ballot Uniqueness (Property 13)
    - **Property 13: Ballot Uniqueness**
    - Generate distinct member IDs and rounds, compute ballots, assert all ballots are distinct
    - **Validates: Requirements 6.4**

- [x] 3. Checkpoint - Decision function
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement message generation logic
  - [x] 4.1 Implement `MessageGenState::new(member_id, cluster_size)` constructor and helper methods
    - `make_ballot(round, member_id, cluster_size) -> Ballot` encoding function
    - `extract_member(ballot, cluster_size) -> usize` decoding function
    - `quorum_size(cluster_size) -> usize` helper
    - _Requirements: 7.5, 7.7_

  - [x] 4.2 Implement `MessageGenState::process_tick(...)` method that drives message generation per tick
    - Input: batched transcript messages, election timer fired flag, batched client requests, `me: MemberId`, `cluster_size`
    - Output: `MessageGenOutput<T, ClusterTag>` containing `outbound: Vec<TranscriptMsg>`, `redirected: Vec<(T, Option<MemberId>)>`, `view_transition: Option<LeaderView>`
    - On election timer + not leader + no recent leader activity: emit `Prepare` with fresh ballot (Req 4.1)
    - On observing `Prepare` with ballot > `max_promised`: emit `Promise` with all accepted entries, update `max_promised` (Req 4.2)
    - On accumulating quorum of `Promise` messages for own ballot: compute Phase1Certificate, begin emitting `Accept` for pending + recovered proposals (Req 4.3, 3.3, 3.4)
    - On observing `Accept` with ballot >= `max_promised`: emit `AcceptAck`, update local accepted state (Req 4.4)
    - Non-leader receiving client requests: add to `redirected` with leader hint (Req 4.5)
    - Leader: suppress election timer (Req 4.6), assign slots to pending requests and emit Accept
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 4.3 Write property test: Ballot Fencing (Property 5)
    - **Property 5: Ballot Fencing**
    - Generate `MessageGenState` with `max_promised = B`, present `Accept` with `ballot < B`, assert no `AcceptAck` emitted
    - **Validates: Requirements 3.2**

  - [x] 4.4 Write property test: Phase1Certificate Guards Accept Emission (Property 6)
    - **Property 6: Phase1Certificate Guards Accept Emission**
    - Generate `MessageGenState` without quorum of Promises for ballot B, assert no `Accept` emitted for B
    - **Validates: Requirements 3.3, 4.3**

  - [x] 4.5 Write property test: Paxos Recovery (Property 7)
    - **Property 7: Paxos Recovery**
    - Generate quorum of Promise messages with previously-accepted values, assert leader re-proposes highest-ballot value per slot
    - **Validates: Requirements 3.4, 6.5**

  - [x] 4.6 Write property test: Promise Emission (Property 9)
    - **Property 9: Promise Emission**
    - Generate `MessageGenState` with `max_promised = B_old`, present Prepare with `ballot = B_new > B_old`, assert Promise emitted with correct accepted entries
    - **Validates: Requirements 4.2**

  - [x] 4.7 Write property test: AcceptAck Emission (Property 10)
    - **Property 10: AcceptAck Emission**
    - Generate `MessageGenState` with `max_promised = B`, present Accept with `ballot >= B`, assert AcceptAck emitted
    - **Validates: Requirements 4.4**

- [x] 5. Checkpoint - Message generation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Wire the dataflow: forward_ref cycle, broadcast, fold, sliced! block
  - [x] 6.1 Implement the `broadcast_transcript_consensus` public function
    - Declare `forward_ref` on EC-typed location from `broadcast_from_member` output (the reliable_broadcast trick)
    - Merge seed/generated messages, broadcast via `TCP.fail_stop().bincode()` → EC transcript
    - Clone transcript into two concerns:
      - **Concern 1 (commit extraction)**: commutative fold with `manual_proof!` on commutativity + idempotency, producing committed entries
      - **Concern 2 (message generation)**: `sliced!` block batching transcript, timer, and requests with `nondet!()` annotations; advance `MessageGenState` per tick; emit outbound messages
    - Close the forward_ref cycle: generated messages → `broadcast_from_member` → complete handle
    - Extract `redirected` and `leader_views` streams from sliced! block outputs
    - Wrap committed fold output with gap-filling logic for TotalOrder emission
    - Return `BroadcastConsensusOutputs` struct
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

  - [x] 6.2 Add `NonDet` parameter and `nondet!()` annotations on all batch operations
    - Document acknowledged non-determinism (message delivery order, request batching) in nondet annotations
    - _Requirements: 7.5_

- [x] 7. Checkpoint - Compilation and type-level verification
  - Ensure all tests pass, ask the user if questions arise.
  - Verify: no `assert_has_consistency_of` in module (Req 1.3)
  - Verify: `manual_proof!` only on fold annotations (Req 1.4, 7.6)
  - Verify: output type signatures match `RaftOutputs` pattern (Req 5.1-5.5)

- [x] 8. Unit tests (example-based)
  - [x] 8.1 Write unit tests for the decision function
    - Empty transcript → no commits
    - Single slot: Accept + quorum of AcceptAcks → LogEntry emitted with correct fields
    - Sub-quorum AcceptAcks → no commit
    - Gap-filling: slot 1 committed before slot 0 → slot 1 withheld until slot 0 commits
    - Duplicate AcceptAcks don't double-count (set semantics)
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 8.2 Write unit tests for message generation
    - Election timer fires → Prepare emitted with fresh ballot (Req 4.1)
    - Non-leader receives request → redirect with leader hint (Req 4.5)
    - Leader suppresses election timer (Req 4.6)
    - Observe Prepare with higher ballot → Promise emitted (Req 4.2)
    - Quorum of Promises → Accept emitted for pending proposals (Req 4.3)
    - Ballot fencing: stale Accept ignored (Req 3.2)
    - _Requirements: 3.2, 4.1, 4.2, 4.3, 4.5, 4.6_

- [ ] 9. Integration tests (deterministic simulation)
  - [ ] 9.1 Write integration tests matching raft.rs test categories
    - Stable leader commits stream of proposals within bounded ticks (Req 6.2)
    - Leader crash → new election → pending proposals re-committed (Req 6.5)
    - Network partition heals → members converge to same committed prefix (Req 6.3)
    - Concurrent elections + message reorderings never fork the committed log (Req 6.1)
    - At most one leader per ballot; contested elections resolve via retry (Req 6.4)
    - Use deterministic delivery harness matching raft.rs test patterns (Req 6.6)
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

  - [ ] 9.2 Write property test: Validity (Property 8)
    - **Property 8: Validity**
    - Generate random protocol executions (sequences of client proposals + elections), assert every committed value exists in the original proposal set
    - **Validates: Requirements 3.5**

  - [ ] 9.3 Write property test: Convergence (Property 11)
    - **Property 11: Convergence**
    - Generate complete protocol execution (all messages delivered), run fold on each member's transcript, assert all members produce same committed log
    - **Validates: Requirements 6.3**

  - [ ] 9.4 Write property test: Safety Under Adversarial Schedules (Property 12)
    - **Property 12: Safety Under Adversarial Schedules**
    - Generate random delivery schedules (reorderings, partial deliveries, concurrent elections), assert committed logs of any two members are prefix-consistent
    - **Validates: Requirements 6.1, 6.7**

- [ ] 10. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.
  - Verify complete requirements coverage across all tasks

## Extension: End-to-End KVS Comparison with MultiPaxos

> **Goal:** Evaluate broadcast-transcript consensus against a real standard —
> MultiPaxos (`CorePaxos` via the `PaxosLike` trait) — using the *same* end-to-end
> replicated key-value store harness that `paxos_bench` uses (`bench_client` →
> consensus → `kv_replica` with checkpointing → response). This tests full
> functional correctness (linearizable KV store, failover, recovery) and
> apples-to-apples performance against a battle-tested protocol, not just the
> in-module micro-benchmark.
>
> **Prerequisite reality checks baked into these tasks** (surfaced during the
> raft comparison): (a) fair benchmarking must control for pacing knobs
> (heartbeat/keepalive intervals) and warmup; (b) bounded state is mandatory —
> without checkpoint-driven truncation, `DecisionState` grows unboundedly and
> per-tick cost degrades; (c) failover under symmetric timeouts requires the
> leader-activity gating plus idle keepalives.
>
> **Note:** These tasks also require companion additions to `requirements.md`
> (KVS linearizability, bounded-state/checkpointing, failover-under-load,
> fair-benchmark methodology) and `design.md` (checkpoint mechanism, kv_replica
> integration). Author those alongside the tasks below.

- [ ] 11. Bounded state via internal self-checkpointing (no external signal)
  - [x] 11.1 Internal self-checkpointing — NO public API change, NO external checkpoint input
    - KEY INSIGHT: unlike Paxos/Raft (which need a replica-applied checkpoint to coordinate acceptor-log truncation), this protocol folds the whole transcript on every member, so a committed slot's `ack_sets`/`accepted` are immutable and never re-read. "Committed" is itself the checkpoint.
    - The `sliced!` block calls `decision_ref.truncate(committed_len)` each tick after emitting deltas. Output-preserving, validated by the `truncation_safety` proptest. Zero signature change.
    - _Requirements: 8.1, 8.3_
  - [x] 11.2 Implement `DecisionState` truncation
    - On observing a checkpoint at slot `s`, prune `ack_sets`, `accepted`, `promises`, and `committed_slots` entries for slots `< s` (clamped to the contiguous committed prefix so flushing is unaffected)
    - Preserve the emission frontier semantics so no committed entry is re-emitted or lost across truncation
    - _Requirements: 8.1_
  - [x] 11.3 Write property test: Truncation preserves committed prefix
    - **Property: Truncation Safety** — `truncation_safety` proptest asserts committed log with interleaved checkpoints equals committed log without truncation; plus `truncate_prunes_committed_prefix_state` and `truncate_preserves_future_commits` unit tests
    - _Validates: 8.2_
  - [ ] 11.4 Write a sustained-load micro-test asserting per-tick work does not grow with log length
    - Drive N proposals with periodic checkpoints; assert steady-state (no monotonic throughput decay), guarding against the snapshot/accumulator regression found in task 6
    - _Validates: (new) bounded-state requirement_

- [ ] 12. Complete failover: leader-activity gating + idle keepalives
  - [ ] 12.1 (DONE if already implemented) Leader-activity gating — suppress follower elections while leader is observed active
    - _Requirements: 4.1, 5.5_
  - [ ] 12.2 Add idle keepalives
    - Leader emits a periodic keepalive (empty `Accept`/dedicated keepalive variant) so followers can detect liveness during request-idle periods
    - Add the keepalive timer as an input (mirrors raft's `heartbeat_timer_interrupts`); document that "no heartbeat" holds only for replication, not liveness
    - _Requirements: (new) failover-under-idle requirement; 5.5_
  - [ ] 12.3 Write failing-then-passing tests for idle failover
    - Idle cluster, leader stops → a follower detects absence of keepalives and takes over within bounded windows
    - Live idle leader → followers never usurp (no dueling) with symmetric timeouts
    - _Validates: (new) failover-under-idle requirement_

- [x] 13. KVS integration: wire broadcast-transcript into `kv_replica`
  - [x] 13.1 Adapt committed output to the sequenced-KV interface
    - `committed.end_atomic().weaken_consistency().map(|e| (e.slot, Some(e.message))).weaken_ordering::<NoOrder>()` → `kv_replica`. No external checkpoint feedback needed (self-checkpointing, task 11.1).
    - _Requirements: 10.1_
  - [x] 13.2 Build `broadcast_transcript_kv_bench` in `consensus_bench.rs`
    - `bench_client` → route to leader → broadcast-transcript → `kv_replica` (checkpoint_freq 1000) → route processed KV back to originating client → latency/throughput. `T = KvPayload<u32, (MemberId<BenchClient>, i32)>`. Deployment test `broadcast_transcript_kv_some_throughput`.
    - _Requirements: 10.1, 10.2, 11.1_

- [x] 14. MultiPaxos baseline harness parity
  - [x] 14.1 Use existing `paxos_bench` (`CorePaxos` + `kv_replica`) as the MultiPaxos baseline
    - Patched `paxos_some_throughput` to collect steady-state (15 windows, discard 3 warmup) matching the broadcast KVS bench methodology. MultiPaxos = `CorePaxos` (dual-path proposers/acceptors/replicas), f=1, checkpoint 1000, 100 clients — comparable to the broadcast KVS bench. (Explicitly NOT paxos-ec.)
    - _Requirements: 11.1_

- [x] 15. End-to-end KVS correctness tests (simulation-level, deterministic)
  - [x] 15.1 Linearizable KV store — replicated-store convergence
    - `kv_store()` folds each member's committed log into a `HashMap` (mirrors `kv_replica`); `assert_kv_stores_converged()` checks all replicas agree. Tests: `kvs_single_key_last_write_wins`, `kvs_multi_key_store_converges`, `kvs_delete_semantics`, `kvs_five_node_convergence` (5-node), and proptest `kvs_linearizable_under_adversarial_delivery` (256 cases).
    - _Requirements: 10.3_
  - [x] 15.2 Leader failure — writes survive, stores converge
    - `kvs_leader_crash_writes_survive`: pre-crash writes present in every surviving member's store after re-election + recovery; value-based convergence (Paxos agrees on value, not ballot).
    - _Requirements: 10.4_
  - [x] 15.3 Partition heal — store converges
    - `kvs_partition_heal_store_converges`: after heal + re-election-driven recovery, the replicated store (not just log prefix) converges.
    - _Requirements: 10.3_
  - [ ] 15.4 (NEW, FOUND BY TESTS) Lagging-replica catch-up under a stable leader
    - GAP: a member that misses an `Accept` does NOT catch up from new traffic under a stable leader — missed slots are only re-sent when a re-election triggers Paxos recovery. Raft retransmits to lagging followers (nextIndex/matchIndex); broadcast-transcript has no such path. Add a leader-driven retransmission/catch-up mechanism (or document as a liveness limitation).
    - _Requirements: (new) catch-up requirement_
  - [x] 15.5 Deployed (localhost process) end-to-end functional test
    - `broadcast_transcript_sustained_load`: standalone 3-node deployment on real localhost processes under `TCP.fail_stop()`, each member emits committed entries; asserts progress, cross-member agreement (no fork), and gap-free contiguity. Verified: 450 committed entries, 150 contiguous slots (0..=149), agreement holds. (Deployed leader-kill failover deferred — fragile under fail_stop, as the removed paxos_ec integration tests showed.)
    - _Requirements: 10.3, 6.2_

- [ ] 16. Fair benchmark methodology and reporting
  - [x] 16.1 Control for pacing knobs
    - Matched steady-state methodology (15 windows, discard 3 warmup) across all benches; MultiPaxos leader-liveness timers are not per-commit pacing (unlike raft's heartbeat, which was corrected 50ms→1ms earlier). Documented.
    - _Requirements: 11.1, 11.2, 11.3_
  - [x] 16.2 Report steady-state throughput across cluster sizes
    - Ran the 3/5/7 sweep for both protocols (broadcast `_n3/_n5/_n7`, MultiPaxos `_f1/_f2/_f3`). Result: broadcast leads at every size but its margin shrinks with n (1.30x → 1.21x → 1.11x), the O(n²) signature — see writeup below.
    - _Requirements: 11.4_
  - [ ] 16.3 Write up the comparison (latency percentiles + message counts still TODO)
    - Throughput sweep done; p50/p99 latency and message-count instrumentation remain
    - State honest caveats (localhost, cluster size, idle behavior)

- [x] 15.6 Independent external validation: Maelstrom `lin-kv` workload (Jepsen/Knossos linearizability checker)
  - Added `hydro_test/src/maelstrom/lin_kv.rs`: a `lin_kv_server` wiring Maelstrom's real `read`/`write`/`cas` KV workload through `broadcast_transcript_consensus`, plus `lin_kv_single_node_maelstrom` and `lin_kv_3_node_maelstrom` deployment tests (all members fire the same election timer — genuine concurrent elections, not a pinned leader).
  - **Both pass**: Jepsen's Knossos `CASRegister` linearizability checker independently verified `:linearizable {:valid? true}` for every key, `:failures []`, across a real 3-process deployment. This is external validation (not our own test assertions) — the standard tool for catching consensus forking/lost-write/non-linearizable-read bugs.
  - Real bugs found and fixed along the way: `sliced!` macro requires all `use::` declarations before other statements; `q!` staged closures cannot call external free functions (must be inline or a method on a captured value); `CLUSTER_SELF_ID.get_raw_id()` panics under Maelstrom's string node ids (fixed via membership-list-position lookup, matching the technique `broadcast_transcript_consensus` already uses internally); internally-tagged enums (`#[serde(tag=...)]`) and `serde_json::Value` both require `deserialize_any`, incompatible with the bincode-based consensus wire format (fixed via a wire-format/internal type split, with `JsonValue` storing canonical JSON text as a bincode-safe `String`).
  - To rerun: download a Maelstrom release tarball (`https://github.com/jepsen-io/maelstrom/releases`), extract it, and run with `MAELSTROM_PATH=/path/to/maelstrom cargo test -p hydro_test --lib -- maelstrom::lin_kv --test-threads=1`. Requires a JVM (Java 11+); `gnuplot` is optional (only affects cosmetic result plots, not the linearizability verdict).
  - _Requirements: 10.3, 10.4 (independent verification)_

- [x] 15.7 Adversarial SLAM test: Maelstrom partition nemesis, both protocols, side-by-side
  - Added `lin_kv_3_node_partition_stress` (`broadcast_transcript_consensus`) and `raft_lin_kv_3_node_partition_stress` (`raft::raft`) in `hydro_test/src/maelstrom/lin_kv.rs`: 3-node cluster, `--nemesis partition`, `--nemesis-interval 5`, 12 concurrent workers, `--rate 30`, `--time-limit 45`, 3 repetitions.
  - Both protocols were parameterized with a `net: impl Fn() -> Net` network-fault-model argument (mirroring `raft::raft`'s existing signature), with an explicit `Net: NetworkFor<Msg, ConsistencyGuarantee = EventualConsistency>` bound. Verified against `hydro_lang::networking` that both `fail_stop()` and `lossy_delayed_forever()` satisfy `EventualConsistency` (plain `lossy()` does not — it's `NoConsistency` — and correctly fails to typecheck). Normal deployment tests use `fail_stop()`; the partition-nemesis stress tests use `lossy_delayed_forever()` since Maelstrom's partition nemesis needs message loss to be recoverable, not permanent-death.
  - **Found and fixed two real bugs in the test harness itself while investigating a suspicious result**:
    1. `MaelstromDeployment::run()` in `hydro_lang/src/deploy/maelstrom/deploy_maelstrom.rs` returned `Ok(())` as soon as it saw the *first* `"Everything looks good!"` line on the child process's stdout, instead of draining to EOF and checking the exit status. Fixed by draining stdout fully and using `spawned.wait()`'s exit status as the source of truth.
    2. Even after that fix, passing `--test-count 3` (Jepsen's/Maelstrom's own repeat-N-times CLI flag) still only produced *one* "Running test"/"Analysis complete" cycle in practice against the pinned Maelstrom v0.2.3 release, despite `jepsen.cli/single-test-cmd`'s `doseq` loop (confirmed by reading its Clojure source) implying it should run 3 full cycles inside one process. Rather than depend on that opaque, version-sensitive behavior, added `MaelstromDeployment::run_repeated(count)`, which loops `count` independent `maelstrom test` process invocations from Rust itself (each fully drained/checked via the fix above). Both stress tests now call `.run_repeated(3)` instead of passing `--test-count 3`.
  - **Results (post-fix, both protocols, verified 3/3 real repetitions each via `run_repeated`)**: `broadcast_transcript_consensus` — `Everything looks good!` x3, zero `:valid? false` across all repetitions, 157.27s. `raft::raft` — `Everything looks good!` x3, zero `:valid? false` across all repetitions, 167.93s. Both independently confirmed linearizable under partition nemesis by Jepsen/Knossos, and both harness bugs verified fixed by checking that "Running test" / "Analysis complete" / "Everything looks good!" each appear exactly 3 times in the output (not 1).
  - To rerun: `MAELSTROM_PATH=/path/to/maelstrom cargo test -p hydro_test --lib -- maelstrom::lin_kv::tests::lin_kv_3_node_partition_stress --nocapture --test-threads=1` (and `raft_lin_kv_3_node_partition_stress` for the raft counterpart). `gnuplot` must be installed (`brew install gnuplot`) or each repetition's top-level `:valid?` reports `:unknown` due to a plotting exception even when the actual linearizability sub-result is `true`.
  - _Requirements: 10.3, 10.4, 11.4 (adversarial network-fault comparison)_

- [ ] 17. Final checkpoint (extension)
  - All KVS end-to-end tests pass for both protocols; benchmark report produced; requirements/design updated to cover the extension

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The decision function (task 2) is pure and testable without any Hydroflow infrastructure
- Message generation (task 4) is also pure — takes batched inputs, returns output vectors
- Only task 6 requires the full Hydroflow dataflow machinery (forward_ref, sliced!, broadcast_from_member)
- Integration tests (task 9) require the deterministic simulation harness from `hydro_test`

### Extension notes (tasks 11–17)

- The KVS comparison reuses the existing `paxos_bench` + `kv_replica` harness; MultiPaxos is `CorePaxos` (in `paxos.rs`) via the `PaxosLike` trait.
- Checkpointing (task 11) is a hard prerequisite for a meaningful sustained-load comparison — `kv_replica` already emits checkpoint sequences; the work is consuming them to bound `DecisionState`.
- Idle keepalives (task 12) mean the "no heartbeat" property is scoped to replication only; liveness detection during idle needs a keepalive timer.
- Fair benchmarking (task 16) must control for pacing knobs (heartbeat vs keepalive interval) and report multiple cluster sizes to expose the O(n²) vs O(n) crossover.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.4", "2.5", "2.6", "4.2"] },
    { "id": 3, "tasks": ["4.3", "4.4", "4.5", "4.6", "4.7", "8.1"] },
    { "id": 4, "tasks": ["6.1", "6.2", "8.2"] },
    { "id": 5, "tasks": ["9.1", "9.2", "9.3", "9.4"] },
    { "id": 6, "tasks": ["11.1", "12.1"] },
    { "id": 7, "tasks": ["11.2", "12.2"] },
    { "id": 8, "tasks": ["11.3", "11.4", "12.3"] },
    { "id": 9, "tasks": ["13.1", "14.1"] },
    { "id": 10, "tasks": ["13.2"] },
    { "id": 11, "tasks": ["15.1", "15.2", "15.3", "16.1"] },
    { "id": 12, "tasks": ["16.2", "16.3"] },
    { "id": 13, "tasks": ["17"] }
  ]
}
```
