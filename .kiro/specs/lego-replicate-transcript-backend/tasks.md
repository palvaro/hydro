# Implementation Plan: Lego_Replicate Transcript Backend (Option B)

## Overview

This plan replaces `lego_replicate`'s hand-composed ordering path with the in-tree
`broadcast_transcript_consensus` engine, driving lego's existing application adapter and
client router from the engine's committed total order. Implementation language is **Rust**
(the design's concrete signatures, `proptest` suite, and `sim_input<T, O, R>` migration are all Rust).

The work is strictly sequenced per the design:

1. **Build step 0 first** — bring the `lego-replicate-v2` branch into the working tree and make
   the Legacy_Path compile and go green against in-tree `hydro_lang` (the `sim_input` 1→3
   generic-param migration) *before* any engine swap.
2. **Adapter seam** — `RequestEnvelope` as the engine's generic `T`, committed stream consumed via
   `end_atomic().weaken_consistency()` in a `sliced!` block, slot-order apply.
3. **Correlation-id response routing** — replace the `is_first_member` single-responder bottleneck.
4. **Read-only handling**, then **view-manager / state-transfer removal**.
5. **Validation** — reuse the Maelstrom lin-kv + `run_repeated` harness and the `consensus_bench`
   comparison harness.

The 8 correctness properties from the design are implemented as `proptest` tests
(`proptest.cases = 256`, tagged `Feature: lego-replicate-transcript-backend, Property N: ...`).

## Tasks

- [ ] 1. Establish the compile-and-green baseline (build step 0)
  - [ ] 1.1 Bring `lego_replicate` into the working tree
    - Rebase / merge the `lego-replicate-v2` git branch into the workspace so `lego_replicate`
      lives in-tree alongside the current `hydro_lang` (it is not currently in the working tree)
    - Register the crate in the workspace `Cargo.toml` members so it participates in the build
    - _Requirements: 1.1_

  - [ ] 1.2 Migrate every `sim_input` call site from one to three generic parameters
    - Replace each `x.sim_input::<T>()` with `x.sim_input::<T, O, R>()`, choosing `O: Ordering` /
      `R: Retries` per site (client-input ports → `TotalOrder, ExactlyOnce`; order-insensitive merge
      feeds → `NoOrder, AtLeastOnce`), letting downstream bounds pin the choice
    - Compile `lego_replicate` against in-tree `hydro_lang` to zero compile errors
    - _Requirements: 1.1, 1.2_

  - [ ]* 1.3 Run the Legacy_Path failover end-to-end baseline suite
    - Execute every existing `lego_replicate` failover e2e test against in-tree `hydro_lang`
    - Confirm each test terminates with a definitive pass/fail and the suite is green
    - _Requirements: 1.3, 1.4_

- [ ] 2. Introduce the integration data types
  - [ ] 2.1 Define the envelope and correlation types
    - Add `RequestEnvelope { corr: CorrelationId, payload: Vec<u8> }` deriving
      `Clone, PartialEq, Eq, Serialize, Deserialize` (satisfies the engine's `T` bound)
    - Add `CorrelationId { origin: TaglessMemberId, client: ClientConnId, seq: u64 }`,
      `ClientConnId` (transport-specific), and `ResponseEnvelope { corr, response: Vec<u8> }`
    - _Requirements: 2.2, 4.4_

  - [ ]* 2.2 Write property test for opaque payload preservation
    - **Property 1: Opaque payload round-trip preservation**
    - **Validates: Requirements 2.2**
    - `proptest` over arbitrary `Vec<u8>` (empty, large, non-UTF8); assert bytes survive
      envelope wrap → commit → adapter recovery byte-for-byte, engine deserializes nothing
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 1: Opaque payload round-trip preservation`

- [ ] 3. Build the adapter seam (client → engine → application adapter)
  - [ ] 3.1 Wire client payloads into the engine's `requests` stream
    - In the client router, `map` each `(ClientConn, Vec<u8>)` into a `RequestEnvelope`, minting
      `corr` from `(CLUSTER_SELF_ID, conn, local monotonic seq)`; deserialize nothing here
    - Produce `Stream<RequestEnvelope, Cluster<'a, Replica>, Unbounded, impl Ordering>`
    - _Requirements: 2.1, 2.2_

  - [ ] 3.2 Consume the committed stream at the EC boundary
    - Consume `engine_out.committed` via `.end_atomic().weaken_consistency()` (the exact pattern used
      by `lin_kv_server` / `consensus_bench`); add **no** `assert_has_consistency_of` in the lego module
    - Feed the weakened stream into a `sliced!` block for batched, ordered apply
    - _Requirements: 5.1, 5.2, 5.3, 2.3_

  - [ ] 3.3 Implement the slot-order apply fold
    - Inside `sliced!`, sort each tick's batch by `LogEntry.slot`, track `applied_through`, apply each
      not-yet-applied slot exactly once in ascending order, and refuse to apply any slot `!= applied_through`
    - On deserialization failure at a slot, halt without advancing `applied_through`, leave service state
      unchanged, and surface an error naming the slot and cause; emit `(corr, Response)` only for fully-applied entries
    - _Requirements: 2.3, 2.4, 3.1, 3.2, 3.4, 3.5_

  - [ ]* 3.4 Write property test for the sequence-position contract
    - **Property 2: Sequence-position contract**
    - **Validates: Requirements 2.3, 2.4**
    - `proptest` over random committed logs; assert positions are unique/contiguous/monotonic and
      each payload is paired with its own ascending position; adapter rejects any slot `!= applied_through`
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 2: Sequence-position contract`

  - [ ]* 3.5 Write property test for deterministic ordered application
    - **Property 3: Deterministic ordered application (exactly once)**
    - **Validates: Requirements 3.1, 3.2, 9.3**
    - `proptest` over random sequences + random already-applied prefix with a mock `ReplicableService`
      recording apply calls; assert exactly-once ascending application and no partial-response exposure
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 3: Deterministic ordered application (exactly once)`

  - [ ]* 3.6 Write property test for idempotent duplicate delivery
    - **Property 4: Idempotent duplicate delivery**
    - **Validates: Requirements 3.5**
    - `proptest` over sequences with random duplicated slots; assert per-slot apply multiplicity == 1
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 4: Idempotent duplicate delivery`

  - [ ]* 3.7 Write unit tests for the deserialization-failure halt
    - Assert halt at a random failing slot preserves the prior snapshot and errors with slot + cause
    - _Requirements: 3.4_

- [ ] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement correlation-id response routing
  - [ ] 5.1 Select the responder by `corr.origin` (replace the `is_first_member` gate)
    - After computing `resp` for an applied entry, emit `(corr, resp)` only when
      `entry.message.corr.origin == CLUSTER_SELF_ID.tagless()`; suppress on all other replicas
    - _Requirements: 4.1, 4.2, 4.3_

  - [ ] 5.2 Route responses to the originating client connection
    - Maintain a `pending: Map<CorrelationId, ClientConn>` of requests this replica originated; on an
      owned committed response, route to exactly the one originating client and clear the pending entry
    - _Requirements: 4.1_

  - [ ] 5.3 Handle unmatched-correlation and dead-connection errors
    - If `corr.origin == self` but `(client, seq)` matches no pending request, discard and record an
      "unmatched correlation" error carrying `corr`
    - If the originating connection is gone, drop the response, record an error, and continue the
      per-tick emission loop without blocking
    - _Requirements: 4.5, 4.6_

  - [ ] 5.4 Add the retained single-responder config flag
    - Add a `responder = SingleDesignated` config option that pins responses to one node (legacy behavior)
      and document the O(1)-responder throughput tradeoff in the option's doc comment
    - _Requirements: 4.7_

  - [ ]* 5.5 Write property test for response-correlation correctness
    - **Property 7: Response-correlation correctness**
    - **Validates: Requirements 4.1, 4.2, 4.4**
    - `proptest` over random requests with unique corr across N origins; assert request↔response
      bijection by corr, responder == `corr.origin`, others suppress, and ≤ `ceil(100/N)%` per-node share
      under balanced origins
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 7: Response-correlation correctness`

  - [ ]* 5.6 Write unit tests for correlation error paths
    - Unmatched correlation discarded + error recorded (Req 4.5); dead connection dropped, error
      recorded, subsequent responses still routed (Req 4.6)
    - _Requirements: 4.5, 4.6_

- [ ] 6. Implement read-only command handling
  - [ ] 6.1 Route read-only commands through the committed order (default strategy)
    - Wrap a `Read_Only_Command` in a `RequestEnvelope` like a write; when it commits at slot `s`,
      evaluate it against state after applying slots `0..s` and return the result
    - Select the path via `ReplicableService::is_read_only`
    - _Requirements: 6.1, 6.4_

  - [ ] 6.2 Add the opt-in leader-local read path
    - Guarded by the engine's `is_leader`: while a replica holds a valid leader role, serve reads from
      local state without appending to the order; a non-leader receiving a read under this mode returns
      a "cannot serve read consistently" error, evaluates nothing locally, and does not modify the order
    - Off by default
    - _Requirements: 6.2, 6.3_

  - [ ] 6.3 Document the active read strategy and its consistency level
    - In the module docs, state that Committed_Order-routed reads are the active default (linearizable)
      and leader-local reads are the opt-in (sequential/leader-monotonic)
    - _Requirements: 6.5_

  - [ ]* 6.4 Write property test for read-reflects-prefix
    - **Property 8: Read-reflects-prefix (read-your-writes)**
    - **Validates: Requirements 6.1, 6.4**
    - `proptest` over random write sequences with a read inserted at a random committed position S;
      assert the read reflects exactly the writes ordered before S and none at/after S
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 8: Read-reflects-prefix (read-your-writes)`

  - [ ]* 6.5 Write unit tests for the leader-local read path
    - Leader serves read locally with unchanged order (Req 6.2); non-leader under leader-local mode
      errors with no local response and unchanged order (Req 6.3)
    - _Requirements: 6.2, 6.3_

- [ ] 7. Remove the view manager and state-transfer primitive
  - [ ] 7.1 Remove the `CorePaxos` view manager and re-back `View` from `leader_views`
    - Delete the Legacy_Path view manager so it produces no leadership/view output consumed downstream;
      derive leadership identity solely from the engine's `leader_views`, projecting `LeaderView → View`
      (hold/`last()` for a `Singleton` "current view")
    - _Requirements: 7.1, 7.2, 7.3_

  - [ ] 7.2 Remove the state-transfer primitive; recover by committed-order replay
    - Delete the dedicated state-transfer primitive; a lagging/recovering replica reconstructs state by
      replaying the committed prefix so its applied prefix equals the received committed prefix
    - Document the single decision — state-transfer **removed** — with rationale in the module docs
    - _Requirements: 7.4, 7.6_

  - [ ] 7.3 Handle recovery/replay failure
    - If replay fails at a slot, leave state at the last cleanly-applied slot (no partial prefix) and
      surface a recovery-failure indication naming the slot (halt-and-report discipline)
    - _Requirements: 7.5_

  - [ ]* 7.4 Write property test for deterministic replica convergence
    - **Property 5: Deterministic replica convergence**
    - **Validates: Requirements 3.3, 7.4**
    - `proptest` over a random command sequence applied on two replicas (one live, one via replay);
      assert byte-for-byte equal `snapshot` after each identical prefix, using an observationally-deterministic mock service
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 5: Deterministic replica convergence`

  - [ ]* 7.5 Write unit tests for view derivation and recovery failure
    - `View` derived solely from `leader_views`, no `CorePaxos` output wired (Req 7.1, 7.2); recovery
      failure leaves state unchanged and surfaces an indication (Req 7.5)
    - _Requirements: 7.1, 7.2, 7.5_

- [ ] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Wire the engine into lego and handle leader failover
  - [ ] 9.1 Wire the engine call site behind a path selector
    - Call `broadcast_transcript_consensus` with the `requests` stream, `election_timer_interrupts`,
      `BroadcastConsensusConfig { cluster_size }`, the `net: impl Fn() -> Net` closure
      (`fail_stop` for simulation, `lossy_delayed_forever` for partition-facing), and `nondet`; ensure
      `T: Eq`; select engine-backed vs Legacy_Path via feature/config so both coexist during transition
    - Ensure the Committed_Order is produced exclusively by the engine (no slot-assignment/star-accumulate/decide/ordered-deliver)
    - _Requirements: 2.1, 8.1, 8.2_

  - [ ] 9.2 Forward non-leader requests via the `redirected` stream
    - Route engine `redirected: (T, leader hint)` submissions to the current leader while leaving
      `corr.origin` untouched, so submitted-but-uncommitted ops commit under a new leader after failover
    - _Requirements: 8.4_

  - [ ]* 9.3 Write property test for prefix-consistency / no-fork
    - **Property 6: Prefix-consistency / no-fork**
    - **Validates: Requirements 8.3, 9.2, 9.4**
    - `proptest` over adversarial delivery + crash/election schedules across replicas (reuse the engine's
      `StepBroadcastCluster` harness pattern); assert pairwise prefix relation, zero forked slots, and
      rejection of divergent-value overwrites with the prior value preserved
    - Tag: `// Feature: lego-replicate-transcript-backend, Property 6: Prefix-consistency / no-fork`

  - [ ]* 9.4 Write unit test for leader-activity gating
    - Assert a follower's election timer firing does not start a competing election while Accept/keepalive
      activity is observed in the window
    - _Requirements: 8.5_

- [ ] 10. Build and run validation harnesses
  - [ ] 10.1 Extend the Maelstrom lin-kv server with `corr.origin` responder routing
    - Modify the in-tree `hydro_test/src/maelstrom/lin_kv.rs` server to replace the single-responder
      `is_first_member` gate with the `corr.origin` responder rule, wired to the engine-backed path
      under the existing `run_repeated` pattern (`TCP.lossy_delayed_forever().bincode()`)
    - _Requirements: 4.1, 4.2_

  - [ ]* 10.2 Run the Maelstrom lin-kv linearizability validation
    - Run 3 independent randomized repetitions with partition/kill nemeses under ≥60s sustained load;
      assert zero linearizability violations, prefix-consistency, and zero torn values
    - _Requirements: 9.1, 9.2, 9.3_

  - [ ] 10.3 Build the `consensus_bench` comparison harness vs the Legacy path
    - Use `hydro_std::bench_client` via the `consensus_bench` shape against both the engine-backed path and
      the Legacy MultiPaxos (`CorePaxos`) path with identical client count / workload / cluster size /
      warmup / measurement windows; discard warmup, report throughput min/median/mean/max and the
      engine-to-legacy median ratio, emit transport / cluster-size / read-strategy caveats, and flag
      runs that miss steady state as invalid
    - _Requirements: 4.3, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

  - [ ]* 10.4 Run the lego failover e2e suite against the engine-backed path
    - Run every existing `lego_replicate` failover e2e test against the engine-backed path with zero
      failures while keeping the Legacy_Path suite green throughout the transition
    - _Requirements: 1.5, 8.1, 8.2, 8.3, 8.4, 9.5_

  - [ ]* 10.5 Add the single-EC-assertion static check
    - Static/compile-time check that the lego integration module contains zero `assert_has_consistency_of`
      (so the engine's sole assertion is the only one, total == 1); flag and name any deviation (zero or two)
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ] 11. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test/validation sub-tasks and can be skipped for a faster MVP.
- Build step 0 (task 1) MUST complete before any engine-swap work; the Legacy_Path stays executable and
  green until the engine-backed path passes its equivalent failover e2e tests (Req 1.5), so the two paths
  coexist behind a selector during the transition.
- The single `assert_has_consistency_of` is inherited from the engine; the lego integration adds none
  (Req 5). Task 10.5 statically enforces this.
- Each property test uses `proptest` with `proptest.cases = 256` and the
  `Feature: lego-replicate-transcript-backend, Property N: ...` tag.
- Property tests are placed close to the implementation they validate to catch errors early.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3", "2.1"] },
    { "id": 3, "tasks": ["2.2", "3.1"] },
    { "id": 4, "tasks": ["3.2"] },
    { "id": 5, "tasks": ["3.3"] },
    { "id": 6, "tasks": ["3.4", "3.5", "3.6", "3.7"] },
    { "id": 7, "tasks": ["5.1"] },
    { "id": 8, "tasks": ["5.2", "5.3"] },
    { "id": 9, "tasks": ["5.4", "5.5", "5.6"] },
    { "id": 10, "tasks": ["6.1"] },
    { "id": 11, "tasks": ["6.2"] },
    { "id": 12, "tasks": ["6.3", "6.4", "6.5"] },
    { "id": 13, "tasks": ["7.1"] },
    { "id": 14, "tasks": ["7.2", "7.3"] },
    { "id": 15, "tasks": ["7.4", "7.5", "9.1"] },
    { "id": 16, "tasks": ["9.2"] },
    { "id": 17, "tasks": ["9.3", "9.4", "10.1"] },
    { "id": 18, "tasks": ["10.2", "10.3", "10.4"] },
    { "id": 19, "tasks": ["10.5"] }
  ]
}
```
