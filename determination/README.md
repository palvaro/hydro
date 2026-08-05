# Determination Depth Tracking for Hydro

## Goal

Build compiler-assisted determination depth tracking by connecting three ideas:

1. **Complete CALM (CIDR'27 formalism):** A specification S maps inputs to allowed outputs with an extension order ⪯. The system is coordination-free iff S is monotone — every finite collection of independently exposed outputs has a common allowed extension.

2. **Determination Provenance:** When S is NOT monotone, commitments resolve ambiguity. Commitments layer: some depend on others' outcomes, inducing a *filtration* that measures depth — how many sequential rounds of coordination each output depends on.

3. **Hydro's `nondet!` macro:** Every point in a Hydro dataflow where behavior becomes schedule-dependent requires a `NonDet` guard. These are exactly the points where commitment events can occur.

## The Constructive Bridge

The insight: Hydro's `nondet!` sites are an embryonic commitment basis. The compiler can:

1. **Identify commitment points** — `nondet!` sites where the nondeterminism is genuine (not innocuous/commutative)
2. **Compute the dependency graph** — does commitment B's effect depend on commitment A's outcome? (Is there a dataflow path from A's output to B's input?)
3. **Derive determination depth** — commitments with no dependency path between them commute (same layer); those connected by a path are in different layers (depth increases)

## Key Subtlety: Cross-Slot Dependencies

Commitments that appear independent at the protocol level (e.g., Paxos slots) may have application-level dependencies when downstream logic uses one slot's output to determine another's behavior. The compiler must trace dependencies through the FULL dataflow — including application logic — not just the coordination protocol.

## Formalism (from CIDR'27)

- **S ⊆ I × O** — specification: a relation from inputs to allowed outputs
- **⪯** — extension order on O: o ⪯ o' means o' safely extends o without contradiction
- **Monotonicity** — for every input i and every finite collection o₁,...,oₖ ∈ S(i), ∃ u ∈ S(i) with oⱼ ⪯ u for all j
- **Commitment** — exposure of o irrevocably rules out all o' ≺ o
- **Coordination** — waiting for communication before making an exposure commitment

From determination provenance:
- **Commitment basis Φ** — the set of operators that narrow S(i)
- **Depth** — number of sequential layers in the determination (commitments that commute share a layer; those that depend on each other's outcomes are in different layers)

## Directory Structure

- `examples/` — minimal Hydro programs (~30-50 lines each)
- `models/` — symbolic specifications in CIDR formalism (one per example)

## Examples

| Example | Description | Depth | Why |
|---------|------------|-------|-----|
| `monotone_accumulation` | Set-union broadcast | 0 | No commitments; all exposed outputs have union as common extension |
| `single_quorum` | Quorum + monotone consumption | 1 | Batch boundary is a commitment; all such commitments commute |
| `sequential_slots` | Quorum decides threshold for second quorum | 2 | Second quorum's effect depends on first's outcome |

## Open Questions

1. Can the compiler determine commutativity of commitment points from dataflow graph structure alone?
2. What's the right annotation for programmers? Binary (innocuous vs. genuine commitment) seems sufficient — depth is computed, not declared.
3. How do we handle application-level cross-slot dependencies that are invisible at the protocol level?
4. Can determination depth serve as a latency cost model? (Depth k ≈ k sequential coordination rounds)
