# Model: Sequential Slots (Enumerated)

## Instance

- Slot 1: decides a quorum threshold. Two possible values: 2 or 3.
  - Votes available: 2, 2, 3, 3. Slot 1 quorum = 2.
- Slot 2: confirms operations using the decided threshold.
  - Key k₁ has exactly 2 acks.

## Input

```
i = (slot1_votes = {2, 2, 3, 3}, slot2_acks = {ack(k₁), ack(k₁)}, slot1_quorum = 2)
```

## Output Domain (O)

The output is a pair: (decided_threshold, confirmed_set).

Valid outputs (k₁ can only be confirmed if 2 acks ≥ threshold):

```
o₁ = (⊥, ∅)         — nothing decided yet
o₂ = (2, ∅)         — threshold=2 decided, k₁ not yet confirmed
o₃ = (3, ∅)         — threshold=3 decided, k₁ not yet confirmed
o₄ = (2, {k₁})     — threshold=2, k₁ confirmed (2 acks ≥ 2) ✓
     (3, {k₁})     — INVALID: 2 acks < 3. Not in O.
```

## Allowed Outputs: S(i)

```
S(i) = {o₁, o₂, o₃, o₄}
     = {(⊥,∅), (2,∅), (3,∅), (2,{k₁})}
```

All four are reachable by some schedule:
- o₁ = (⊥, ∅): before slot 1 resolves
- o₂ = (2, ∅): slot 1 decided threshold=2, slot 2 hasn't processed acks yet
- o₃ = (3, ∅): slot 1 decided threshold=3, slot 2 hasn't processed acks yet
- o₄ = (2, {k₁}): slot 1 decided threshold=2, slot 2 confirmed k₁

## Extension Order (⪯)

Threshold refines from ⊥; confirmed set grows; but only within consistent threshold.

Pairs in ⪯:
```
o₁ ⪯ o₁ (reflexive)
o₁ ⪯ o₂    (⊥,∅) extends to (2,∅)
o₁ ⪯ o₃    (⊥,∅) extends to (3,∅)
o₁ ⪯ o₄    (⊥,∅) extends to (2,{k₁})
o₂ ⪯ o₂ (reflexive)
o₂ ⪯ o₄    (2,∅) extends to (2,{k₁})
o₃ ⪯ o₃ (reflexive)
o₄ ⪯ o₄ (reflexive)
```

NOT in ⪯:
- o₂ ⪯ o₃ — NO (threshold 2 → 3 is not extension, it's contradiction)
- o₃ ⪯ o₂ — NO
- o₃ ⪯ o₄ — NO (threshold mismatch)
- o₄ ⪯ o₃ — NO

Hasse diagram:

```
    o₄ = (2, {k₁})
    |
    o₂ = (2, ∅)        o₃ = (3, ∅)
        \              /
         o₁ = (⊥, ∅)
```

## Monotonicity Check (Exhaustive)

| Collection | Common extension in S(i)? |
|-----------|--------------------------|
| {o₁} | o₁ ⪯ o₂ (or o₃, o₄) ✓ |
| {o₂} | o₂ ⪯ o₄ ✓ |
| {o₃} | o₃ ⪯ o₃ (terminal) ✓ |
| {o₄} | o₄ ⪯ o₄ (terminal) ✓ |
| {o₁, o₂} | o₄ extends both ✓ |
| {o₁, o₃} | o₃ extends both ✓ |
| {o₁, o₄} | o₄ extends both ✓ |
| **{o₂, o₃}** | Need u: o₂⪯u AND o₃⪯u. No such u in S(i). **✗** |
| {o₂, o₄} | o₄ extends both ✓ |
| **{o₃, o₄}** | Need u: o₃⪯u AND o₄⪯u. No such u. **✗** |
| {o₁, o₂, o₃} | Need u extending o₂ and o₃. None. **✗** |
| {o₁, o₂, o₄} | o₄ extends all ✓ |
| {o₁, o₃, o₄} | Need u extending o₃ and o₄. None. **✗** |

**S(i) is NOT monotone.** Witness: {o₂, o₃} = {(2,∅), (3,∅)} — incompatible threshold decisions.

## Commitment Basis

**Φ = {φ₁_A, φ₁_B, φ₂}**

Slot 1 commitments (which threshold wins):
- φ₁_A = "two votes for 2 are batched first" → decides threshold = 2
- φ₁_B = "two votes for 3 are batched first" → decides threshold = 3

Slot 2 commitment (ack processing):
- φ₂ = "k₁'s acks are batched and evaluated against the threshold"

### Effects of φ₁ on S(i):

```
Before φ₁:  S(i) = {o₁, o₂, o₃, o₄}
After φ₁_A: S(i) = {o₁, o₂, o₄}       — o₃ ruled out (threshold ≠ 3)
After φ₁_B: S(i) = {o₁, o₃}            — o₂, o₄ ruled out (threshold ≠ 2)
```

### Effects of φ₂ (depends on which φ₁ fired):

```
After φ₁_A then φ₂: S(i) = {o₄}        — k₁ confirmed (2 ≥ 2)
After φ₁_B then φ₂: S(i) = {o₃}        — k₁ NOT confirmed (2 < 3)
```

## Non-Commutativity (Demonstrates Depth > 1)

**φ₂'s effect depends on φ₁'s outcome:**

- If φ₁_A fired (threshold=2): φ₂ narrows to {o₄} = {(2,{k₁})}
- If φ₁_B fired (threshold=3): φ₂ narrows to {o₃} = {(3,∅)}

Same operator φ₂, applied to different post-φ₁ states, produces different results.
**They do NOT commute: φ₂'s effect is conditional on φ₁.**

In the determination provenance sense: Spec(H·φ₁_A·φ₂) ≠ Spec(H·φ₁_B·φ₂),
and the choice between φ₁_A and φ₁_B must be resolved BEFORE φ₂ can be evaluated.

## Determination Depth

**Depth = 2**

- Layer 1: {φ₁_A or φ₁_B} — resolves threshold (one fires per determination)
- Layer 2: {φ₂} — resolves k₁ confirmation (effect depends on Layer 1's outcome)

Two sequential rounds of commitment. Layer 2 cannot be evaluated until Layer 1 resolves.

## Dataflow Evidence

In the Hydro code, the dependency is a direct edge:

```
slot1: collect_quorum(threshold_votes) → decided_threshold
                                              |
                                              v
slot2: collect_dynamic_quorum(operation_acks, decided_threshold)
```

The compiler sees: slot 1's output (decided_threshold) is an INPUT to slot 2's
commitment point (collect_dynamic_quorum's threshold parameter). Therefore
slot 2's commitment depends on slot 1's outcome. Depth = 2.
