# Model: Leader Election via Quorum (Enumerated)

## Instance

Two keys: {k₁, k₂}. Quorum threshold: 2.
Acks available: ack(k₁), ack(k₁), ack(k₂), ack(k₂).
Both keys have enough acks to reach quorum. The "leader" is whichever key reaches quorum first.

## Input

```
i = {ack(k₁), ack(k₁), ack(k₂), ack(k₂)}, threshold = 2
```

## Output Domain (O)

The leader identity, or "undecided":

```
O = {⊥, k₁, k₂}
```

- ⊥ = no key has reached quorum yet
- k₁ = k₁ reached quorum first (leader)
- k₂ = k₂ reached quorum first (leader)

## Allowed Outputs: S(i)

```
S(i) = {⊥, k₁, k₂}
```

All three are reachable: ⊥ before any batch completes; k₁ if k₁'s acks are batched first; k₂ if k₂'s acks are batched first.

## Extension Order (⪯)

Once a leader is chosen, it's irrevocable. ⊥ can extend to either; k₁ and k₂ cannot extend to each other.

```
⊥ ⪯ k₁
⊥ ⪯ k₂
k₁ ⪯ k₁ (reflexive)
k₂ ⪯ k₂ (reflexive)
⊥ ⪯ ⊥ (reflexive)
```

Hasse diagram:

```
k₁        k₂
 \        /
    ⊥
```

(k₁ and k₂ are incomparable — neither extends the other.)

## Monotonicity Check (Exhaustive)

| Collection | Common extension in S(i)? |
|-----------|--------------------------|
| {⊥} | ⊥ ⪯ k₁ (or k₂) ✓ |
| {k₁} | k₁ ⪯ k₁ ✓ |
| {k₂} | k₂ ⪯ k₂ ✓ |
| {⊥, k₁} | k₁ extends both ✓ |
| {⊥, k₂} | k₂ extends both ✓ |
| **{k₁, k₂}** | Need u with k₁ ⪯ u AND k₂ ⪯ u. No such u ∈ S(i). **✗** |
| {⊥, k₁, k₂} | Same — no common extension. **✗** |

**S(i) is NOT monotone.** The pair (k₁, k₂) witnesses the failure.

Two processes exposing different leaders have made incompatible commitments. Coordination is required.

## Commitment Basis

**Φ = {φ_A, φ_B}**

- φ_A = "k₁'s acks are batched first" (batch commitment favoring k₁)
- φ_B = "k₂'s acks are batched first" (batch commitment favoring k₂)

Effects on S(i):

```
Before any commitment: S(i) = {⊥, k₁, k₂}
After φ_A:             S(i) = {⊥, k₁}        (k₂ ruled out — k₁ won the race)
After φ_B:             S(i) = {⊥, k₂}        (k₁ ruled out — k₂ won the race)
```

After either commitment, the remaining S(i) IS monotone:
- {⊥, k₁}: ⊥ ⪯ k₁. Every collection has a common extension. ✓
- {⊥, k₂}: ⊥ ⪯ k₂. Every collection has a common extension. ✓

## Commutativity

φ_A and φ_B are MUTUALLY EXCLUSIVE (not both applicable) — they represent
alternative resolutions of the same nondeterminism. Within a single determination,
exactly one fires.

However, if there were multiple independent keys racing (k₃, k₄, ...) with their
own batch commitments, those would commute with each other (each key's quorum is
independent). The single "first wins" selection is the non-monotone output.

## Determination Depth

**Depth = 1**

Single layer: one batch commitment resolves the race. After resolution, the
residual is monotone.

## Contrast: Depth-0 Version

If the output were a SET of confirmed keys (not "first wins"):
- O = {∅, {k₁}, {k₂}, {k₁,k₂}}
- S(i) = {∅, {k₁}, {k₂}, {k₁,k₂}}
- ⪯ = set inclusion
- Every collection has {k₁,k₂} as common extension → monotone → depth 0

The same `nondet!` point, different downstream composition, different depth.
