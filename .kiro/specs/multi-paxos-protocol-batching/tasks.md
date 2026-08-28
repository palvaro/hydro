# Implementation Plan: Multi-Paxos Protocol Batching

## Branch and baseline discipline

- [ ] 0. Start in a fresh branch
  - [ ] Create `perf/multi-paxos-protocol-batching` from the commit containing this spec.
  - [ ] Confirm no unrelated working-tree changes.
  - [ ] Run and retain same-host unbatched controls at concurrency 32 and 128 before modifying code.
  - [ ] Record raw throughput and latency windows, compiler profile, host, commit, and exact commands.
  - [ ] Do not use the obsolete ~1.3k req/s pre-hot-state-fix result as the baseline.

## Tasks

- [ ] 1. Define exact contiguous batch types and pure helpers
  - [ ] 1.1 Add `DecreeBatch<V>` (or transparent phase-specific wrappers) with ballot, epoch start, first slot, and non-empty exact values vector.
  - [ ] 1.2 Add pure iteration/projection helpers from an exact batch to `(slot, value)` entries.
  - [ ] 1.3 Add constructors that reject or avoid empty batches and checked slot arithmetic where appropriate.
  - [ ] 1.4 Test encode/decode and flatten round trips, including `None` recovery holes.
  - [ ] 1.5 Test that splitting one dense vector at arbitrary boundaries projects to the same per-slot sequence.
  - _Requirements: 1.1–1.6, 3.1–3.2, 8.2_

- [ ] 2. Batch leader recovery and fresh-command proposals
  - [ ] 2.1 Change the in-place leader kernel to emit one fresh-command batch per non-empty tick rather than one proposal fact per command.
  - [ ] 2.2 Emit a dense recovery batch containing adopted values and no-op holes.
  - [ ] 2.3 Advance `next_slot` once by batch length and preserve epoch `start` unchanged.
  - [ ] 2.4 Keep recovery and fresh commands as separate batches initially.
  - [ ] 2.5 Preserve bounded in-place `pending` state and incremental learned-prefix state.
  - [ ] 2.6 Add kernel-level tests for slot assignment, recovery holes, and mixed recovery/new-command ticks.
  - _Requirements: 1.2–1.6, 4.5, 7.4, 8.2_

- [ ] 3. Process and acknowledge exact batches at acceptors
  - [ ] 3.1 Replace phase-2 accept wire items with exact batches.
  - [ ] 3.2 In the existing single acceptor closure, check the entire batch against tick-start `max_promised`.
  - [ ] 3.3 Apply passing batches to the in-place per-slot accepted map.
  - [ ] 3.4 Emit one `AcceptedBatch` carrying exact contents per passing batch.
  - [ ] 3.5 Preserve the current prepare/promise ordering semantics and post-accept promise snapshot.
  - [ ] 3.6 Test duplicate idempotence, rejected lower ballot, overlapping higher ballot, and same-tick prepare/accept behavior.
  - _Requirements: 2.1–2.6, 3.1–3.2, 8.2_

- [ ] 4. Certify exact batches and remove per-slot quorum work
  - [ ] 4.1 Feed `(DecreeBatch<V>, acceptor)` facts to the optimized distinct-attestor quorum combinator.
  - [ ] 4.2 Emit chosen batches directly from `Durable<DecreeBatch<V>>`.
  - [ ] 4.3 Ensure no proposal-history join is introduced.
  - [ ] 4.4 Preserve/add the per-slot `chosen` output by deterministic local projection.
  - [ ] 4.5 Expose `chosen_batches` additively if it makes downstream batching explicit.
  - [ ] 4.6 Test no certificate below threshold, duplicate acceptor exclusion, exact-content binding, and overlapping differently partitioned certificates.
  - _Requirements: 3.3–3.7, 4.2, 4.4, 8.2_

- [ ] 5. Keep batches intact through proposer and learner dissemination
  - [ ] 5.1 Broadcast chosen batches—not entries—to proposer learning.
  - [ ] 5.2 Locally project slots after proposer receipt and update the dense learned prefix.
  - [ ] 5.3 Broadcast chosen batches—not entries—to learners.
  - [ ] 5.4 Echo/unique exact batches through the current learner EC cycle.
  - [ ] 5.5 Flatten only after EC-typed learning and generate the existing per-slot splice facts.
  - [ ] 5.6 Test convergence with duplicates, reordered batches, overlapping partitions, and proposer crash during initial learner shipment.
  - _Requirements: 4.1–4.5, 5.1–5.5, 8.2_

- [ ] 6. Adapt the live-election wrapper
  - [ ] 6.1 Retire completed request values from locally projected leader `chosen` entries.
  - [ ] 6.2 Keep `ElectionState.pending` bounded by outstanding work.
  - [ ] 6.3 Preserve campaign round construction, timeout progress detection, redo semantics, and two-second pinned benchmark timeout.
  - [ ] 6.4 Run all existing live wrapper crash/concurrent-election tests.
  - _Requirements: 6.1–6.4, 8.1–8.3_

- [ ] 7. Preserve benchmark fairness and add instrumentation
  - [ ] 7.1 Continue responding from member 0's leader-local chosen quorum point.
  - [ ] 7.2 Keep learner dissemination active in the benchmark.
  - [ ] 7.3 Add temporary counters for batch sizes and logical protocol messages at phase 2 and learner dissemination boundaries.
  - [ ] 7.4 Mark zero-sample/process-failure runs invalid in any report integration touched by this work.
  - [ ] 7.5 Add a small deterministic test for response projection/dedup if the adapter shape changes.
  - _Requirements: 6.3, 7.3, 7.5, 9.1–9.4_

- [ ] 8. Check inference and seam budget mechanically
  - [ ] 8.1 Verify the learner output still typechecks as `Cluster<..., EventualConsistency>` without a consistency assertion.
  - [ ] 8.2 Search the task diff for new `assert_has_consistency_of`, `assume_`, consistency casts, `manual_proof!`, `nondet!`, and forward-ref obligations.
  - [ ] 8.3 Update trust/complexity accounting only with measured changes; explain any seam-budget increase before merge.
  - [ ] 8.4 Confirm no bare cumulative frontier or digest-based acknowledgement was added.
  - _Requirements: 4.1–4.7, 7.1–7.4_

- [ ] 9. Correctness checkpoint
  - [ ] Run targeted pure/unit tests for batch representation and acceptor/quorum behavior.
  - [ ] Run all `ec_inference_demos::multi_paxos` tests.
  - [ ] Run all `multi_paxos_live` tests, including exhaustive/fuzz crash tests under their current bounds.
  - [ ] Run relevant epoch splice and quorum tests.
  - [ ] Run the affected `hydro_test` benchmark build (`--no-run`).
  - [ ] Run scoped formatting/lint checks; distinguish unrelated existing failures.
  - _Requirements: 8.1–8.3_

- [ ] 10. Performance checkpoint
  - [ ] 10.1 Run Multi-Paxos and Raft at concurrency 32, 128, and 256, three fresh deployments each.
  - [ ] 10.2 Preserve raw windows and reject failed/zero-sample points.
  - [ ] 10.3 Report steady p50/p99/p99.9, throughput distribution, batch-size distribution, and messages/operation.
  - [ ] 10.4 Verify the final-three/first-three p50 ratio is ≤1.25 and c32 regression is ≤10%.
  - [ ] 10.5 Verify the concurrency-128 target: ≥1.7x unbatched Multi-Paxos and ≤2.0x gap to same-run Raft.
  - [ ] 10.6 If the target is missed, profile learner echo, quorum/unique state, splice/drop behavior, bincode, and demux; document evidence rather than claiming completion.
  - _Requirements: 9.1–9.8_

- [ ] 11. Final review
  - [ ] Ensure comments accurately distinguish scheduler batching from protocol batching.
  - [ ] Ensure benchmark/report prose uses the corrected ~24k/~5 ms starting point.
  - [ ] Ensure no obsolete ~1.3k or claimed 18x improvement remains in new documentation.
  - [ ] Summarize correctness evidence, seam-census delta, performance evidence, and remaining bottlenecks in the final change description.
