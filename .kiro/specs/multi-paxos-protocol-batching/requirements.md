# Requirements: Multi-Paxos Protocol Batching

## Context and corrected baseline

The implementation work described here MUST occur in a fresh branch created from the commit containing this specification. Recommended branch name: `perf/multi-paxos-protocol-batching`.

This work starts **after** the growing-state hot-path fix. Do not claim the older ~1.3k req/s result as the baseline. The relevant observed baselines are:

- Pilot saturation report, concurrency 128: Multi-Paxos ~24,064 req/s, p50 ~5.072 ms; Raft ~89,776 req/s, p50 ~0.915 ms.
- Same-machine verification after the hot-state fix and leader-local `chosen` response: Multi-Paxos median ~23,500 req/s with p50 ~4.11–4.40 ms; Raft median ~67,415 req/s with p50 commonly ~0.94 ms.
- At concurrency 32 in the pilot, both protocols delivered about 20k req/s and Multi-Paxos p50 was not worse. The apparent ~5x latency gap at concurrency 128 is queueing after Multi-Paxos reaches its throughput ceiling, not an inherent 5x unloaded consensus RTT.

Raft currently batches protocol work: `AppendEntriesRequest.entries` is a `Vec<LogEntry<T>>`, followers acknowledge a replicated frontier, and one heartbeat exchange amortizes framing, routing, and acknowledgement processing over multiple operations. Multi-Paxos currently batches only at the Hydro scheduler tick: it assigns individual slots, then sends, quorum-counts, and disseminates each slot as an individual protocol fact.

## 1. Exact batch representation

1.1. The steady-state phase-2 wire representation SHALL carry a non-empty contiguous batch:

```rust
AcceptBatch<V> {
    ballot: Ts,
    epoch_start: usize,
    first_slot: usize,
    values: Vec<Option<V>>,
}
```

1.2. Element `values[offset]` SHALL represent slot `first_slot + offset`.

1.3. `epoch_start` SHALL retain the existing ownership/splice meaning; it MUST NOT be confused with `first_slot`.

1.4. Empty batches SHALL never be emitted.

1.5. Recovery no-ops (`None`), adopted values, and new commands (`Some(V)`) SHALL be representable without changing their current semantics.

1.6. Batch boundaries SHALL be a representation/scheduling choice only. They MUST NOT alter the chosen per-slot log for a fixed legal serialization.

## 2. Acceptor semantics

2.1. An acceptor SHALL check an entire `AcceptBatch` against the same tick-start `max_promised` fence used by the current implementation.

2.2. If the batch ballot passes the fence, the acceptor SHALL apply every element to its per-slot accepted map using the current max-by-ballot rule.

2.3. If the batch ballot fails the fence, the acceptor SHALL acknowledge none of that batch.

2.4. Acceptance of one batch MUST be one staged sequential transition over in-place state; it MUST NOT clone or replay the full accepted history.

2.5. Duplicate delivery of the same exact batch SHALL be idempotent.

2.6. Overlapping batches at different ballots SHALL preserve the existing per-slot fencing and adopt-highest rules.

## 3. Exact batch acknowledgements and quorum

3.1. A phase-2 acknowledgement SHALL bind the acceptor to the exact batch contents and boundaries:

```rust
AcceptedBatch<V> {
    ballot: Ts,
    epoch_start: usize,
    first_slot: usize,
    values: Vec<Option<V>>,
}
```

3.2. The first implementation SHALL carry the exact vector, not a bare `accepted_through` frontier and not an unchecked digest.

3.3. The existing generic quorum mint SHALL certify the exact batch fact and count distinct acceptors.

3.4. No batch SHALL become chosen below the configured quorum threshold.

3.5. Once an exact batch reaches quorum, the proposer SHALL emit a chosen-batch fact without an unbounded proposal-history join.

3.6. A chosen batch SHALL deterministically project to per-slot chosen facts. Projection SHALL not add a new consistency assertion or unchecked cast.

3.7. A certificate for one batch MAY overlap a certificate from another ballot or use different boundaries; safety SHALL still be checked and tested per slot.

## 4. Inferred consistency and seam budget

4.1. The learner-facing EC guarantee SHALL remain inferred from the existing fail-stop broadcast/echo structure.

4.2. There SHALL be no new `assert_has_consistency_of`, `assume_*`, consistency cast, or `manual_proof!` that mints EC.

4.3. Exact chosen batches SHALL be disseminated as batches through the learner broadcast/echo cycle and flattened deterministically after the EC boundary.

4.4. The existing public per-slot `learned` and splice/log semantics SHALL remain available. If a new batch output is exposed, it SHALL be additive.

4.5. Batch grouping nondeterminism MAY be documented with `nondet!`, but the documentation SHALL state that grouping changes representation/timing only, not per-slot agreement.

4.6. Expected trust-census impact:
- S1 consistency assertions: no increase.
- S3 assumptions/casts: no increase.
- S5 caller obligations: no increase.
- S2 algebraic obligations: no increase preferred; at most one local, explicit lemma for deterministic certified-batch projection if the type system requires it.
- S4 nondeterminism: at most one documented batch-boundary choice.

4.7. Any increase beyond that budget SHALL be called out for review before merging.

## 5. Learner and proposer dissemination

5.1. Chosen facts SHALL be broadcast to proposers in batch/range form where possible; the implementation MUST NOT immediately recreate one network message per slot.

5.2. Initial learner shipment and learner echo SHALL transport exact chosen batches rather than individual chosen values.

5.3. The all-to-all learner echo may remain in this iteration because it carries the inferred-EC/failure property. Removing it is out of scope unless separately justified without increasing the seam budget.

5.4. Learner-side flattening SHALL preserve `epoch_start`, slot number, and value exactly.

5.5. Duplicate batches and overlapping differently partitioned batches SHALL converge to the same per-slot splice facts.

## 6. Liveness wrapper and client completion

6.1. The live-election wrapper SHALL observe per-request completion even when the safety core processes chosen batches.

6.2. Completion retirement SHALL remain bounded by outstanding work, not total history.

6.3. The benchmark SHALL continue to respond from member 0's leader-local `chosen` quorum point, matching Raft's leader-local majority commit semantics. Learner dissemination SHALL continue asynchronously.

6.4. Redo/resubmission behavior SHALL remain idempotent at the state-machine/request-id level.

## 7. Explicit non-goals for the first implementation

7.1. Do not implement bare cumulative `AcceptedThrough { ballot, end_slot }` acknowledgements. Such a frontier introduces a new dense-prefix/value-binding invariant unless encoded with a separately reviewed typed certificate.

7.2. Do not introduce a hash/digest acknowledgement unless collision resistance and value binding are explicitly modeled and approved as a new trust seam.

7.3. Do not remove the learner echo merely to improve benchmark numbers.

7.4. Do not weaken Paxos phase 1, recovery no-op filling, epoch ownership, or splice semantics.

7.5. Do not optimize by disabling outputs in the benchmark that production callers would still execute. Optional/lazy output construction may be proposed separately and measured transparently.

## 8. Correctness validation

8.1. All existing Multi-Paxos and live-wrapper tests SHALL pass.

8.2. New tests SHALL cover:
- batch encode/flatten round-trip;
- empty-batch rejection;
- exact quorum threshold and distinct-acceptor counting;
- duplicate batch idempotence;
- overlapping batches at higher ballots;
- the same per-slot values proposed with different legal batch boundaries;
- recovery batches containing adopted values and no-op holes;
- concurrent/dueling leaders with differently partitioned overlapping batches;
- acceptor and proposer crash progress under the existing fault budget;
- learner convergence when duplicate/overlapping batch facts arrive in different orders.

8.3. Simulator tests SHALL assert per-slot agreement, contiguous splice output, and no regression in crash liveness—not merely equality of batch objects.

## 9. Performance validation

9.1. Benchmark comparisons SHALL use fresh deployments, identical topology/workload, leader-local completion, and same-run Raft controls.

9.2. Run concurrency points 32, 128, and 256 with at least three repetitions each, discarding the documented warmup windows and preserving raw windows.

9.3. Record protocol batch-size distribution and logical phase-2 request/reply counts per committed operation. Temporary instrumentation may be removed only after results are captured.

9.4. A run with zero samples, process failure, broken pipe, timeout, or stack overflow SHALL be reported as failed/invalid, never as zero-throughput performance data.

9.5. Mandatory regression gates:
- p50 SHALL remain flat across steady windows; median p50 of the final three steady windows must be no more than 25% above the first three.
- concurrency-32 throughput and p50 SHALL not regress by more than 10% from an immediately preceding same-host unbatched control.
- no correctness or liveness test regressions.

9.6. Performance target at concurrency 128: achieve at least 1.7x the same-host unbatched Multi-Paxos throughput and reduce the same-run Raft throughput gap to at most 2.0x, without violating 9.5.

9.7. Stretch target: Multi-Paxos reaches at least 70% of same-run Raft throughput with p50 no more than 2x Raft p50.

9.8. If 9.6 is missed, the work SHALL still report batch sizes, message counts, CPU/profile evidence, and the next identified bottleneck. It MUST NOT claim success solely from a lower latency percentile at reduced load.
