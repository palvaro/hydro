# Model: Single Quorum

## Specification (CIDR'27 Formalism)

### Input (I)
A set of ack messages A = {(k₁, ok), (k₁, ok), (k₁, ok), (k₂, ok), (k₂, err), ...}
arriving at the proposer. Each ack is keyed by operation id and carries success/failure.

### Output (O)
Sets of confirmed keys: subsets of {k₁, k₂, ...} that have reached quorum.

### Allowed Outputs: S(i)
For input i (a set of arriving acks), S(i) = all subsets of keys whose ack count
in i meets or exceeds the quorum threshold. If key k has ≥ quorum_size successful
acks in i, then k MAY appear in an output. S(i) contains every subset of such
confirmable keys.

### Extension Order (⪯)
Set inclusion on confirmed keys: o₁ ⪯ o₂ iff o₁ ⊆ o₂.

## Monotonicity Check

**Claim:** S(i) is monotone (for the downstream output).

**Proof:** Take any finite collection of exposed outputs o₁, ..., oₖ ∈ S(i).
Each oⱼ is a set of confirmed keys. Their union u = o₁ ∪ ... ∪ oₖ. Each key
in u appeared in some oⱼ, meaning it had enough acks — so u ∈ S(i).
For every j, oⱼ ⊆ u. Common extension exists. ✓

**But wait** — the specification of collect_quorum ITSELF is non-monotone
in a subtle way: WHEN a key reaches quorum depends on batch boundaries.
The downstream set is monotone; the quorum-crossing event is the commitment.

## Worked Example

Input: acks for key k₁ arrive in some order.
- Quorum threshold: 3
- Acks received: (k₁, ok), (k₁, ok), (k₁, ok), (k₂, ok), (k₂, ok)

**Schedule A:** First batch = {(k₁,ok), (k₁,ok), (k₁,ok)} → k₁ confirmed in batch 1.
**Schedule B:** First batch = {(k₁,ok), (k₁,ok)} → k₁ NOT confirmed yet. Second batch adds third → k₁ confirmed in batch 2.

Both schedules produce the SAME final output: {k₁} ∈ confirmed set.
The commitment (batch boundary) affects WHEN but not WHETHER.

At the output interface (the accumulated set), both processes expose {k₁}.
Common extension: {k₁} ⪯ {k₁}. ✓

**Non-monotone sub-specification:** Consider intermediate observations.
If one process exposes "k₁ confirmed at time t₁" and another exposes
"k₁ confirmed at time t₂ ≠ t₁" — these are different outputs with no
common extension under equality. But we model only the SET of confirmed
keys, not timing. Under set inclusion, it's monotone.

## Commitment Basis

**Φ = {φ_batch}**

φ_batch: the batch boundary operator inside `collect_quorum`'s `sliced!` block.
It determines which acks are processed together in one tick. This is a genuine
commitment: it narrows S(i) by determining the order of quorum-crossing events.

All instances of φ_batch commute: reordering which batch each ack lands in
may change WHEN keys reach quorum but not WHICH keys ultimately reach quorum
(given all acks eventually arrive). The final confirmed set is the same.

## Determination Depth

**Depth = 1**

Single layer of commuting batch commitments. Once all batches are processed,
the result is determined. No commitment depends on another commitment's outcome.

## Connection to Hydro Code

The program has:
- `collect_quorum` which internally uses `sliced!` with `nondet!` (batch boundary)
- Downstream: a commutative `fold` (set insertion)

A compiler pass would:
1. Identify the `nondet!` inside `collect_quorum` as a genuine commitment (batch boundary affects downstream)
2. Identify the downstream fold as monotone (commutative, set inclusion)
3. Find no dependency between commitment instances (each key's quorum is independent)
4. Assign depth = 1
