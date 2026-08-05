# Model: Monotone Accumulation

## Specification (CIDR'27 Formalism)

### Input (I)
A set of values V = {a, b, c, ...} arriving at cluster nodes p₁, p₂, ..., pₙ.
Each node receives some subset of V.

### Output (O)
Sets of values. Each node exposes a set representing "values I have accumulated."

### Allowed Outputs: S(i)
For input i (a distribution of values across nodes), S(i) = all subsets of V that
are reachable by some delivery schedule. Concretely: every subset of V that includes
at least the node's own local values.

### Extension Order (⪯)
Set inclusion: o₁ ⪯ o₂ iff o₁ ⊆ o₂.

## Monotonicity Check

**Claim:** S(i) is monotone.

**Proof:** Take any finite collection of exposed outputs o₁, ..., oₖ ∈ S(i).
Each oⱼ is a subset of V. Their union u = o₁ ∪ ... ∪ oₖ is also a subset of V,
and u ∈ S(i) (reachable by delivering all relevant messages). For every j, oⱼ ⊆ u,
so oⱼ ⪯ u. The common extension exists. ∎

## Worked Example

Cluster: {p, q}. Values arriving: p receives {a, b}, q receives {c}.

After broadcast, both can accumulate {a, b, c}. At any intermediate point:
- p might expose {a, b} (before receiving q's broadcast)
- q might expose {c} (before receiving p's broadcast)

Common extension: {a, b, c} ∈ S(i), and {a,b} ⊆ {a,b,c}, {c} ⊆ {a,b,c}. ✓

No matter what order messages arrive, the union is always a valid common extension.

## Commitment Basis

**Φ = ∅**

There are no genuine commitments. The `fold` is commutative (set insert order
doesn't matter). No `nondet!` points introduce contingency.

## Determination Depth

**Depth = 0**

All outputs are robust — they hold under every possible determination.
No coordination is needed.

## Connection to Hydro Code

The program has:
- No `nondet!` annotations
- No `sliced!` blocks
- Only a commutative fold (set union)

A compiler pass would classify all outputs as depth-0 (robust).
