# Model: Monotone Accumulation (Enumerated)

## Instance

Two nodes: {p, q}. Two values: {a, b}.
- p receives: {a}
- q receives: {b}
- Both broadcast to each other.

## Input

```
i = (p_local={a}, q_local={b})
```

## Output Domain (O)

Pairs of sets — what each node exposes:

```
O = (set_at_p, set_at_q) where set_at_p, set_at_q ⊆ {a, b}
```

Possible output pairs (both nodes expose a subset of {a,b} containing at least their local value):

```
o₁ = ({a}, {b})        — no messages delivered yet
o₂ = ({a,b}, {b})      — q's broadcast arrived at p
o₃ = ({a}, {a,b})      — p's broadcast arrived at q
o₄ = ({a,b}, {a,b})    — both broadcasts delivered
```

## Allowed Outputs: S(i)

```
S(i) = {o₁, o₂, o₃, o₄}
```

All four are reachable by some delivery schedule.

## Extension Order (⪯)

Componentwise set inclusion: (A₁,B₁) ⪯ (A₂,B₂) iff A₁ ⊆ A₂ and B₁ ⊆ B₂.

Hasse diagram:

```
        o₄ = ({a,b}, {a,b})
       /                    \
o₂ = ({a,b}, {b})    o₃ = ({a}, {a,b})
       \                    /
        o₁ = ({a}, {b})
```

All pairs in ⪯ (including reflexive):
- o₁ ⪯ o₁, o₁ ⪯ o₂, o₁ ⪯ o₃, o₁ ⪯ o₄
- o₂ ⪯ o₂, o₂ ⪯ o₄
- o₃ ⪯ o₃, o₃ ⪯ o₄
- o₄ ⪯ o₄

## Monotonicity Check (Exhaustive)

For every finite collection from S(i), does a common extension exist in S(i)?

| Collection | Common extension in S(i)? |
|-----------|--------------------------|
| {o₁} | o₁ ⪯ o₄ ✓ |
| {o₂} | o₂ ⪯ o₄ ✓ |
| {o₃} | o₃ ⪯ o₄ ✓ |
| {o₄} | o₄ ⪯ o₄ ✓ |
| {o₁, o₂} | o₄ extends both ✓ |
| {o₁, o₃} | o₄ extends both ✓ |
| {o₁, o₄} | o₄ extends both ✓ |
| {o₂, o₃} | o₄ extends both ✓ |
| {o₂, o₄} | o₄ extends both ✓ |
| {o₃, o₄} | o₄ extends both ✓ |
| {o₁, o₂, o₃} | o₄ extends all ✓ |
| {o₁, o₂, o₃, o₄} | o₄ extends all ✓ |

**Every collection has a common extension. S(i) is monotone. ✓**

## Commitment Basis

**Φ = ∅**

No commitments needed. No output exposure rules out any future extension.

## Determination Depth

**Depth = 0**

All outputs are robust.
