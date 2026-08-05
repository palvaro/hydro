# Static Analysis: Determination Depth

## Overview

This module implements a static analysis pass over Hydro's IR (`HydroNode` tree)
to compute determination depth — how many sequential layers of coordination
a program requires.

## Approach

The analysis proceeds in four phases:

### Phase 1: Identify Nondet Points

Walk the IR tree and find all `ObserveNonDet` nodes. Each is a potential
commitment point — a source of nondeterminism that may or may not propagate
to the output interface.

### Phase 2: Absorption Analysis

For each nondet point, trace all dataflow paths to the output roots.
Along each path, check whether the nondeterminism is "absorbed" by the
downstream composition:

- **Absorbed (depth 0):** The path goes through a commutative fold over
  a `NoOrder` stream, or an operator that is insensitive to the specific
  form of nondeterminism (e.g., set-based accumulation).
- **Propagates (depth ≥ 1):** No absorbing operator on the path. The
  nondeterminism reaches the output.

A nondet point is a **genuine commitment** only if at least one path to
an output does NOT contain an absorber.

### Phase 3: Dependency Graph

Among genuine commitments, compute dependencies: commitment B depends on
commitment A if there is any dataflow path from A's output to B's input
(conservative — ignores key partitioning).

### Phase 4: Depth Computation

The determination depth is the length of the longest chain in the
dependency graph among genuine commitments. Commitments with no
dependencies between them are in the same layer (they commute);
dependent commitments are in different layers.

## Conservative Design

This first pass is intentionally conservative (may over-report depth):

- A fold is "absorbing" only if we can prove it: the input stream has
  `NoOrder` (meaning the type system already verified order-insensitivity).
- Two commitments are "dependent" if there's ANY path between them
  (ignores key-partitioning that might make them independent).
- `ObserveNonDet { trusted: true }` is still treated as a potential
  commitment (trusted means "don't simulate" not "doesn't matter").

## Output

```rust
DepthAnalysis {
    nondet_points: Vec<NonDetPoint>,     // all nondet! sites found
    genuine_commitments: Vec<NodeId>,     // those not absorbed
    dependency_edges: Vec<(NodeId, NodeId)>, // A → B means B depends on A
    layers: Vec<Vec<NodeId>>,            // commitments grouped by layer
    depth: usize,                        // max layer index
}
```

## Connection to the Models

For each example program, the static analysis should produce depth results
matching the symbolic models:

- `monotone_accumulation`: depth = 0 (no genuine commitments)
- `single_quorum` (leader election): depth = 1 (batch commitment propagates through non-commutative fold)
- `sequential_slots`: depth = 2 (slot 2's commitment depends on slot 1's output)
