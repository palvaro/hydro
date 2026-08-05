# Model: Sequential Slots with Application Dependency

## Specification (CIDR'27 Formalism)

### Input (I)
Two streams of messages:
- Stream 1: threshold votes — proposals for what quorum size to use: {((), Ok(2)), ((), Ok(3)), ((), Ok(2)), ...}
- Stream 2: operation acks — acks for actual operations: {(k₁, Ok(())), (k₁, Ok(())), ...}

### Output (O)
Sets of confirmed operation keys (from stream 2).

### Allowed Outputs: S(i)
**This is where the cross-slot dependency lives.**

S(i) depends on which threshold is decided in slot 1:
- If slot 1 decides threshold = 2: S(i) = {subsets of keys with ≥ 2 acks in stream 2}
- If slot 1 decides threshold = 3: S(i) = {subsets of keys with ≥ 3 acks in stream 2}

Before slot 1 resolves, S(i) contains outputs from BOTH possible worlds.
After slot 1 commits to a threshold, S(i) narrows to only those outputs
consistent with that threshold.

### Extension Order (⪯)
Set inclusion on confirmed operation keys: o₁ ⪯ o₂ iff o₁ ⊆ o₂.

## Monotonicity Check

**Claim:** S(i) is NOT monotone before both commitments resolve.

**Witness of non-monotonicity:**

Let stream 2 contain exactly 2 successful acks for key k₁.

- Under threshold = 2: k₁ is confirmable. Process exposes o₁ = {k₁}.
- Under threshold = 3: k₁ is NOT confirmable. Process exposes o₂ = {}.

Is there a common extension u ∈ S(i) with {k₁} ⊆ u and {} ⊆ u?
That requires u ⊇ {k₁}, so u must confirm k₁. But under threshold = 3,
k₁ cannot be confirmed (only 2 acks). No such u exists in S(i) when
threshold = 3 is the resolution.

The exposures {k₁} and {} are incompatible — they arise from different
resolutions of slot 1, and no single output extends both. **Coordination
required.**

## Worked Example (Full Trace)

**Setup:**
- Slot 1 quorum size: 2 (need 2 votes to decide threshold)
- Stream 1 votes: ((), Ok(2)), ((), Ok(3)), ((), Ok(2))
- Stream 2 acks: (k₁, Ok(())), (k₁, Ok(()))

**Execution under Schedule A (slot 1 decides threshold = 2):**
- Batch 1 of votes: {((), Ok(2)), ((), Ok(2))} → quorum for value 2
- Decided threshold = max(2, 2) = 2
- Slot 2 uses threshold = 2: k₁ has 2 acks ≥ 2 → **k₁ confirmed**
- Output: {k₁}

**Execution under Schedule B (slot 1 decides threshold = 3):**
- Batch 1 of votes: {((), Ok(3)), ((), Ok(3))} → quorum for value 3
  (assuming different vote distribution)
- Decided threshold = 3
- Slot 2 uses threshold = 3: k₁ has 2 acks < 3 → **k₁ NOT confirmed**
- Output: {}

**The two outputs {k₁} and {} have no common extension under set inclusion
when the specification constrains outputs to be consistent with the decided
threshold.**

## Commitment Basis

**Φ = {φ_batch1, φ_batch2}**

- φ_batch1: batch boundary for slot 1 (threshold votes). Determines which
  votes are processed together → which threshold value wins.
- φ_batch2: batch boundary for slot 2 (operation acks). Determines when
  operation keys cross the quorum threshold.

### Commutativity Analysis

**φ_batch1 and φ_batch2 do NOT commute.**

The EFFECT of φ_batch2 depends on the OUTCOME of φ_batch1:
- If φ_batch1 resolves to threshold = 2, then φ_batch2 with 2 acks for k₁ → confirms k₁
- If φ_batch1 resolves to threshold = 3, then φ_batch2 with 2 acks for k₁ → does NOT confirm k₁

The same batch of acks (same φ_batch2 resolution) produces different effects
depending on slot 1's outcome. Therefore: Spec(H · φ_batch1 · φ_batch2) ≠ Spec(H · φ_batch2 · φ_batch1).

### Dataflow Evidence

In the Hydro code, the dependency is visible:
```
decided_threshold ──────→ collect_dynamic_quorum(operation_acks, decided_threshold)
       ↑                              ↑
   slot 1 output              slot 2 commitment point
```

There is a direct dataflow path from slot 1's commitment output to slot 2's
commitment input. The compiler can see this.

## Determination Depth

**Depth = 2**

- Layer 1: φ_batch1 (threshold votes batching)
- Layer 2: φ_batch2 (operation acks batching) — depends on Layer 1's outcome

Two sequential layers. Two rounds of coordination needed.

## Connection to Hydro Code

The program has:
- `collect_quorum` for slot 1 (contains `nondet!` — genuine commitment)
- `collect_dynamic_quorum` for slot 2 (contains `nondet!` — genuine commitment)
- A dataflow edge from slot 1's output (`decided_threshold`) to slot 2's input

A compiler pass would:
1. Identify both `nondet!` points as genuine commitments
2. Trace the dataflow: slot 1's output flows into slot 2's threshold parameter
3. Conclude: slot 2's commitment EFFECT depends on slot 1's outcome
4. Assign: Layer 1 = {φ_batch1}, Layer 2 = {φ_batch2}
5. Output: depth = 2

## Generalization

This pattern appears whenever:
- A coordination result is used as a PARAMETER for further coordination
- State machine replication: slot N's command determines the state that slot N+1 operates on
- Reconfiguration: a membership change (slot 1) affects quorum requirements (slot 2)
- Conditional writes: a schema migration (slot 1) determines validity of writes (slot 2)
