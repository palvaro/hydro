# Model: Single Quorum — Leader Election

## Specification (CIDR'27 Formalism)

### Input (I)
A set of ack messages arriving at the proposer:
```
i = {(k₁, ok), (k₁, ok), (k₁, ok), (k₂, ok), (k₂, ok), (k₂, ok)}
```
Both k₁ and k₂ have enough acks to reach quorum (threshold = 3).

### Output (O)
A leader identity: an element of {k₁, k₂, ...} (whichever key reaches quorum first).

### Allowed Outputs: S(i)
S(i) = {k₁, k₂} — both are valid leaders, since both can reach quorum.

### Extension Order (⪯)
Equality: o₁ ⪯ o₂ iff o₁ = o₂. Once a leader is chosen, it cannot be
"extended" to a different leader. This is NOT a growing set — it's a
single irrevocable choice.

## Monotonicity Check

**Claim:** S(i) is NOT monotone.

**Witness:** Two processes (or two schedules) can independently expose:
- Process/Schedule A exposes: o₁ = k₁ (k₁'s acks arrived first)
- Process/Schedule B exposes: o₂ = k₂ (k₂'s acks arrived first)

Is there a common extension u ∈ S(i) with o₁ ⪯ u and o₂ ⪯ u?
Under equality: u must equal k₁ AND u must equal k₂. Impossible since k₁ ≠ k₂.

**No common extension exists. Coordination is required.**

## Worked Example

**Setup:**
- Quorum threshold: 3
- Acks available: (k₁, ok) ×3, (k₂, ok) ×3

**Schedule A:**
- Batch 1: {(k₁, ok), (k₁, ok), (k₁, ok)} → k₁ reaches quorum
- k₁ is first to confirm → **leader = k₁**
- Output: k₁

**Schedule B:**
- Batch 1: {(k₂, ok), (k₂, ok), (k₂, ok)} → k₂ reaches quorum
- k₂ is first to confirm → **leader = k₂**
- Output: k₂

Two different schedules produce incompatible outputs (k₁ ≠ k₂, no common
extension under equality). The batch boundary is a genuine commitment —
it determines the output.

## Commitment Basis

**Φ = {φ_batch}**

φ_batch: the batch boundary operator inside `collect_quorum`'s `sliced!` block.
It determines which acks are processed together in one tick, and therefore
which key crosses the quorum threshold first.

**Why this is a genuine commitment (not absorbed):**
The downstream fold (`if leader.is_none() { *leader = Some(key) }`) is
NOT commutative — it is sensitive to arrival order. The first key wins.
Different batch orderings → different first keys → different outputs.

Contrast with the depth-0 version (set accumulation): there the downstream
fold IS commutative (set insert), so batch ordering doesn't affect the final
output. The nondeterminism is absorbed. Here it is not.

### Commutativity of φ_batch instances

All instances of φ_batch commute WITH EACH OTHER: the batch boundaries for
k₁'s acks and k₂'s acks are independent (each key's quorum is evaluated
independently within collect_quorum). But the COLLECTION of batch commitments
as a whole determines a total order of quorum-crossing events, and the
first one wins.

More precisely: the commitments commute in the sense that they don't affect
each other's EXISTENCE (whether each key reaches quorum). But they produce
a schedule-dependent ORDERING of confirmation events, and the downstream
"first" selection is sensitive to that ordering. This is a single-layer
phenomenon — one set of commuting commitments whose joint resolution
determines the output.

## Determination Depth

**Depth = 1**

One layer of batch commitments. They commute pairwise (each key's quorum
is independent), but their joint timing determines which key wins the race.
The "first" selection makes the output contingent on the commitment resolution.

- Depth 0 outputs: "key k₁ has reached quorum" (robust — it will eventually
  be true regardless of schedule)
- Depth 1 output: "the leader is k₁" (contingent — depends on which key
  crossed first)

## Connection to Hydro Code

The program has:
- `collect_quorum` which internally uses `sliced!` with `nondet!` (batch boundary)
- Downstream: a NON-commutative fold (first element wins)

A compiler pass would:
1. Identify the `nondet!` inside `collect_quorum` as a potential commitment
2. Trace the dataflow path from `nondet!` to the output
3. Check: is the downstream fold commutative? **No** (order matters for "first")
4. Conclude: the batch nondeterminism is NOT absorbed → genuine commitment
5. No dependency between commitment instances → single layer
6. Assign depth = 1

### Contrast with Depth-0 Version

If the downstream were `fold(HashSet::new(), |set, key| set.insert(key))`:
- The fold IS commutative (set insert order doesn't matter)
- All schedules produce the same final set {k₁, k₂}
- The batch nondeterminism is ABSORBED
- Depth = 0

The difference between depth 0 and depth 1 here is entirely in the
downstream composition — not in the `nondet!` point itself. The same
`nondet!` can contribute depth 0 or depth 1 depending on what consumes it.
