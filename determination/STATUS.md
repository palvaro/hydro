# Determination Depth Project — Status Report

## What Exists (on `determination-depth` branch)

### Theory & Models
- `determination/README.md` — project overview connecting CIDR'27 CALM, determination provenance, Hydro's nondet!
- `determination/models/` — three fully enumerated symbolic models (monotone accumulation depth 0, leader election depth 1, sequential slots depth 2)
- `determination/examples/` — corresponding Hydro source code for each example
- Obsidian vault notes (`complete_calm_discussion.md`) — full intellectual trajectory of the theoretical discussion

### Static Analysis (`hydro_lang/src/determination.rs`)
- Walks actual `HydroNode` IR tree
- Finds `ObserveNonDet` nodes (nondet! sites)
- Determines absorption via:
  - Input stream `StreamOrder::NoOrder` → fold proven commutative by type system
  - Closure AST whitelist: detects `+=`, `|=`, `.insert(`, `max`/`min` as commutative patterns
- Computes dependency graph (any dataflow path = dependency, conservative)
- Computes depth via topological sort (Kahn's algorithm)
- End-to-end tests: DSL → IR → analysis → assertion (all pass)

### Results on Real Code

| Program | nondet points | absorbed | genuine | depth |
|---------|--------------|----------|---------|-------|
| Example 1 (batch + `+=`) | 1 | 1 | 0 | **0** ✓ |
| Example 2 (batch + first-wins) | 1 | 0 | 1 | **1** ✓ |
| Example 3 (two dependent batches) | 2 | 0 | 2 | **2** ✓ |
| Full Paxos + bench | 87 | 41 | 46 | **9** (likely overestimate) |

### Test file for Paxos
- `hydro_test/tests/determination_paxos.rs` — runs analysis on full Paxos, prints results

## Known Issues & Limitations

### Static Analysis
1. **Depth 9 for Paxos is likely too high.** The analysis treats ANY dataflow path as a dependency. Many batch points affect only timing, not outcomes. Key-partitioned independence (e.g., independent slots) is not detected.
2. **Closure analysis is a string-matching whitelist.** Works for `+=` and `.insert(` but won't catch more complex commutative patterns. Not principled.
3. **`commutative = manual_proof!(...)` is erased from IR.** The type-level proof doesn't leave a trace in the IR. We rely on closure analysis instead. A better approach would be to preserve this information in the IR node.
4. **`Batch` vs `ObserveNonDet` confusion.** Early in the project we thought `ObserveNonDet` was the primary nondet point. It's actually `Batch` (for batching nondeterminism) and `ObserveNonDet` (for ordering/retry strengthening). Both matter.

### Theoretical
1. **The Paxos/CALM disagreement** (from earlier discussion): the paper claims vote-counting is monotone (only membership is coordinated), but value selection is genuinely non-monotone. This is a longstanding disagreement between Alvaro and Hellerstein.
2. **"Absorption" is our term** — not from either paper. It names the phenomenon where downstream composition makes a nondet point's nondeterminism irrelevant to the output.
3. **Module boundaries as abstraction**: programmer claims "my module absorbs its own nondet" (local); compiler checks inter-module composition (global). Clean separation but not yet formalized.

## What's Next

### Dynamic Monotonicity Checker (the other prong)
- Use Hydro's simulator (`flow.sim().exhaustive(...)`) to explore schedules
- For each schedule, record what outputs are exposed
- Check: do all exposed outputs have a common extension in S(i)?
- Violation = concrete witness that a commitment point is genuine
- Would tell us: is Paxos REALLY depth 9, or is the static analysis over-conservative?

### Needed for dynamic checker:
- Understand simulator internals (how does `exhaustive` enumerate schedules?)
- Define the "output collection" interface (what counts as an "exposed output"?)
- Define the extension order for each program (what does "common extension" mean for Paxos outputs?)
- Build the checker as a test harness on top of `sim()`

### Improving Static Analysis
- Recognize key-partitioned independence (slots don't interfere)
- Preserve `commutative = manual_proof!` into IR
- Add source location to NonDetPoint for interpretability
- Possibly: prune the dependency graph using dominator analysis

### Understanding Paxos Depth
- Why does the analysis say 9? Need to trace the actual dependency chain.
- Hypothesis: timeout → leader election → ballot → phase1 → phase2 → commit → client response, each with a batch boundary that the analysis considers a genuine commitment.
- The dynamic checker would disambiguate: which of these chains actually produce different outputs under different schedules?

## Key Insight Chain (from the discussion)

1. Complete CALM tells you WHETHER coordination is needed (spec-level)
2. Determination provenance tells you WHAT the coordination does (commitment structure)
3. Hydro's `nondet!` / `Batch` nodes mark WHERE nondeterminism enters (program-level)
4. Absorption analysis determines IF the nondeterminism reaches the output
5. Dependency analysis determines HOW commitments layer (depth)
6. The dynamic checker CONFIRMS whether the depth is real (concrete witnesses)

The bridge from theory to practice: Complete CALM asks "is S(i) monotone?" which is undecidable for programs in general. We decompose it: static analysis gives an upper bound on depth (conservative, decidable); dynamic exploration gives a lower bound (concrete, bounded). Together they bracket the true determination depth.
