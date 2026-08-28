# Design: Exact Protocol Batching for Multi-Paxos

## Summary

The optimization batches adjacent phase-2 decrees as one exact Paxos fact. A quorum certifies the exact vector; only then is it deterministically projected into per-slot chosen facts. Exact batches also travel through proposer learning and the learner EC echo, amortizing network framing, serialization, scheduling, deduplication, and quorum bookkeeping.

The design deliberately does **not** start with Raft-style bare cumulative acknowledgements. Raft can acknowledge a log frontier because its AppendEntries state machine establishes a dense-prefix/log-matching invariant. Adding that invariant to the decomposed Paxos ladder would enlarge the proof seam. Certifying exact vectors captures most batching benefit while preserving the current per-slot safety argument and inferred-consistency boundary.

## Current versus proposed path

Current steady state for a scheduler batch of `k` commands:

```text
k × Accept(slot, value) broadcast
k × per-slot Accepted replies
k × quorum facts
k × chosen-to-proposer broadcasts
k × chosen-to-learner broadcasts
k × learner all-to-all echoes
```

Proposed steady state:

```text
1 × AcceptBatch(first_slot, Vec<value>) broadcast
1 × exact AcceptedBatch reply per acceptor
1 × quorum fact for the exact batch
1 × chosen-batch-to-proposer broadcast
1 × chosen-batch-to-learner broadcast
1 × learner all-to-all batch echo
then deterministic local flattening to k per-slot facts
```

Payload bytes remain O(k). Message envelopes, serialization calls, runtime scheduling, hash-table operations, and quorum keys become O(1) per batch.

## Data model

Use one canonical representation for phase-2 accepts, acknowledgements, chosen batches, and learned batches where practical:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DecreeBatch<V> {
    pub ballot: Ts,
    pub epoch_start: usize,
    pub first_slot: usize,
    pub values: Vec<Option<V>>,
}
```

`DecreeBatch` invariants:

1. `values` is non-empty.
2. It names exactly the dense slot interval
   `[first_slot, first_slot + values.len())`.
3. `epoch_start` is the established epoch's declared splice start and is identical for every decree in the batch.
4. The vector contents bind acknowledgements to exact values. No external proposal join is needed.

If separate nominal types improve API clarity, use transparent wrappers/newtypes around the same fields:

```rust
struct AcceptBatch<V>(DecreeBatch<V>);
struct AcceptedBatch<V>(DecreeBatch<V>);
struct ChosenBatch<V>(DecreeBatch<V>);
```

Do not use only `(ballot, first_slot, len)` as the quorum fact.

## Leader kernel batching

The current in-place leader kernel already materializes the tick's commands and recovery proposals. Change its output from individual tuples to `DecreeBatch` values.

### New commands

For an established epoch `(ballot, epoch_start, next_slot)` and command vector `commands`:

```rust
DecreeBatch {
    ballot,
    epoch_start,
    first_slot: next_slot,
    values: commands.into_iter().map(Some).collect(),
}
```

Advance `next_slot` by `values.len()` once.

### Recovery

`EpochPlan::new` currently creates a dense recovery range containing adopted values and `None` holes. Emit that whole dense range as one batch when non-empty. If recovery size needs a transport cap, split it into contiguous chunks without changing contents.

### Combining recovery and fresh commands

Prefer separate batches in the first version:
- one recovery batch;
- one fresh-command batch.

This avoids accidentally mixing epoch-establishment recovery and new work in a way that complicates tests. Combining adjacent batches can be a later local optimization.

## Acceptor transition

Preserve the existing carefully documented tick semantics:

1. Materialize incoming accept and prepare batches once.
2. Check all accepts against tick-start `max_promised`.
3. For each passing batch, update each named slot in the in-place accepted map by max ballot.
4. Emit one exact acknowledgement for each passing batch.
5. Process promises against tick-start fencing while reporting post-accept state, as today.
6. Advance `max_promised` by the batch's prepares.

Pseudocode inside the single staged closure:

```rust
for (proposer, batch) in accept_batches {
    if tick_start_max_promised.map_or(true, |mp| batch.ballot >= mp) {
        for (offset, value) in batch.values.iter().cloned().enumerate() {
            let slot = batch.first_slot + offset;
            update_slot_max(&mut accepted, slot, batch.ballot.clone(), value);
        }
        responses.push((proposer, ToProposer::AcceptedBatch(batch)));
    }
}
```

This is a loop inside one authored transition, not `k` dataflow facts crossing graph operators.

## Quorum certification

The existing optimized `quorum` combinator supports any `F: Clone + Eq + Hash`. Certify the exact `DecreeBatch<V>`:

```rust
let chosen_batches = quorum(majority, accepted_batch_acks)
    .map(|cert| ChosenBatch(cert.into_fact()));
```

One acceptor contributes once because quorum tracks distinct `MemberId`s. The fact includes values, so there is no proposal-history join and no additional statement that a frontier implies particular contents.

Memory remains one `fired` entry per chosen batch rather than one per slot. A later retirement/frontier design may reduce this further, but is not required for the first batch implementation.

## Certified batch projection

Flattening a certified exact batch is a deterministic structural transform:

```rust
fn entries<V>(batch: ChosenBatch<V>) -> impl Iterator<Item = ChosenEntry<V>> {
    let ChosenBatch(DecreeBatch {
        ballot,
        epoch_start,
        first_slot,
        values,
    }) = batch;

    values.into_iter().enumerate().map(move |(offset, value)| {
        ChosenEntry {
            epoch: ballot.round,
            epoch_start,
            slot: first_slot + offset,
            value,
        }
    })
}
```

Logical argument: if a quorum accepted the exact vector, that same quorum accepted each member at its structurally named slot. This is not a new EC mint. Prefer expressing it as ordinary deterministic `map`/`flat_map`; add no proof annotation unless compilation forces a local algebraic obligation.

The core should expose both if useful:

```rust
pub chosen_batches: Stream<ChosenBatch<V>, Cluster<P>, ...>,
pub chosen: Stream<(u64, usize, usize, Option<V>), Cluster<P>, ...>,
```

Keep `chosen` for compatibility and liveness completion retirement. The benchmark may consume `chosen_batches` or `chosen`, but responses remain leader-local.

## EC dissemination

Do **not** flatten before network dissemination.

```text
chosen_batches
  ├─ broadcast batches to proposer cluster (learned-prefix planning)
  └─ broadcast batches to learners
       └─ learner echo broadcasts batches
            └─ unique exact batches
                 └─ deterministic flatten
                      └─ per-slot SpliceFact generation
```

For proposer learned-prefix state, flatten locally after the one batch broadcast and update the dense prefix incrementally. For learners, flatten after the EC-typed echo merge/unique so the expensive all-to-all path sees one item per batch.

Different legal batch partitions may produce overlapping per-slot facts during retries or succession. Existing splice absorption is idempotent and keyed; tests must verify convergence. If exact-batch `.unique()` cannot deduplicate differently partitioned overlap, per-slot duplicate elimination remains after local flattening, not on the wire.

## Liveness wrapper

The wrapper currently observes per-value `chosen` completion and removes values from a bounded `pending` set. Preserve that interface by locally flattening chosen batches at the proposer. Do not make the wrapper infer completion from only a slot frontier because commands may be retried and duplicate values are meaningful at the protocol layer.

The two-second pinned benchmark election period remains. It prevents a saturated but progressing leader from being misclassified as stalled.

## Benchmark semantics

Respond from member 0 when the leader-local exact batch reaches majority (`chosen`), not after learner echo. This matches the Raft harness, which responds when its leader advances local commit index. Replica learning continues and remains measured as background system load.

Add temporary counters or logs at these boundaries:
- commands per `AcceptBatch`;
- accept batches sent;
- accepted-batch replies received;
- chosen batches;
- learner batch deliveries/echoes;
- committed operations.

Report envelopes and payload bytes separately. Batching reduces envelopes, not serialized value bytes.

## Seam accounting

The intended proof/inference ledger is unchanged:

- The acceptor's order-sensitive refusal kernel remains one authored slice.
- The leader's slot assignment remains one authored slice.
- The quorum combinator still mints `Durable` only from distinct acceptors.
- Learner EC still comes from fail-stop broadcast plus echo.
- The splice still consumes per-slot EC facts.
- No consistency assertion/cast is introduced.

A bare cumulative acknowledgement would add the statement:

> `end_slot` binds this acceptor to the leader's exact dense prefix and no conflicting holes.

That statement spans wire representation and acceptor log state, so it is intentionally deferred.

## Expected bottlenecks after batching

If the throughput target is not met, investigate in this order:

1. Learner all-to-all echo volume and batch serialization bytes.
2. Ever-growing `fired`/`unique` sets; consider batch/slot retirement with an explicitly reviewed frontier.
3. Splice `Arc` fact-chain drop/materialization costs and the prior high-concurrency stack overflow.
4. Batch-size distribution—small batches indicate scheduler/timing limits.
5. CPU time in bincode, hashing, and network demux.

Do not return to full-state cloning or disable the inferred-EC learner path to manufacture a benchmark win.
