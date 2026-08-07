# Hydro DFIR `loop { ... }` Codegen — Failure Triage & Diagnosis

## Summary

After upgrading Hydro's production code-gen to emit ticks as gated DFIR
`loop { ... }` contexts (see `feat(hydro_lang): emit production ticks as gated DFIR
loops (#2902 phase 1)`), several tests fail while building the DFIR flat graph.

The DFIR flat-graph builder now enforces that any dataflow edge crossing a `loop { ... }`
boundary must go through a **windowing** operator when *entering* the loop (e.g.
`batch()`, `batch_lazy()`) and an **un-windowing** operator when *exiting* the loop
(e.g. `all_iterations()`). This check lives in
`dfir_lang/src/graph/flat_graph_builder.rs` (search for
`entering a loop context must be a windowing operator`).

Hydro's production DFIR builder (`ProdDfirBuilder` in
`hydro_lang/src/compile/ir/mod.rs`) places every operator inside the loop context of
its node's *location* (a `Tick`/`Atomic` location ⇒ inside that tick's loop; a
top-level location ⇒ at the graph root). Explicit boundary IR nodes — `Batch`,
`YieldFromTick`, `BeginAtomic`, `EndAtomic` — correctly emit the windowing /
un-windowing operators. However, **some operators consume input idents that were
produced in a different loop context without routing them through a boundary
operator**, so the generated edge crosses a loop boundary illegally. This is the
"missing loop ingress/egress operators" bug.

## Failing tests (7)

```
hydro_lang live_collections::keyed_stream::tests::reduce_watermark_bounded
hydro_lang live_collections::keyed_stream::tests::reduce_watermark_filter
hydro_lang live_collections::keyed_stream::tests::reduce_watermark_garbage_collect
hydro_lang live_collections::optional::tests::into_singleton_unbounded_top_level_none_cardinality
hydro_test  cluster::paxos_bench::tests::paxos_ir
hydro_test  cluster::paxos_bench::tests::paxos_some_throughput
hydro_test  cluster::paxos_log_bench::tests::paxos_log_some_throughput
```

They split into **three distinct root causes**, all in the loop-boundary plumbing.

---

## Class A — `ReduceKeyedWatermark`: watermark exits the tick loop without un-windowing

**Tests:** `reduce_watermark_bounded`, `reduce_watermark_filter`,
`reduce_watermark_garbage_collect`

**Error:**
```
Operator `map(...)` exiting a loop context must be an un-windowing operator, but is not.
```

**Failing flat graph** (from `reduce_watermark_bounded`):

```
1v1 source_iter([(0,100),(1,101),(2,102),(2,102)])   (top level)    ─┐  input keyed stream
2v1 source_iter([2])  (top level)                                    │  watermark source
3v1 persist::<'static>()  (top level)                                │
4v1 batch()             <-- ENTER tick loop (windowing, correct)     │
6v1 map(|x| (Some(x), None))   (top level)  <── 1v1                  │
7v1 map(|w| (None, Some(w)))   (top level)  <── 4v1  ✗ ILLEGAL       │  reads a value that
5v1 chain()  (top level)  <── [0]6v1, [1]7v1                         │  lives INSIDE the loop
8v1 fold_no_replay::<'static>(...)  (top level)                      │  but is emitted OUTSIDE
9v1 flat_map(...)  (top level)                                       │
10v1 map(serialize) -> 11v1 dest_sink                               ─┘
```

Node `7v1` (`map(|w| (None, Some(w)))`) is emitted at the reduce's output location
(top level), but its predecessor `4v1` (`batch()`) lives inside the tick loop. The
edge `4v1 -> 7v1` exits the loop with no un-windowing operator.

### Why

`KeyedStream::reduce_watermark` takes a watermark of type
`Optional<O2, Tick<L::Root>, Bounded>` (always on a `Tick`) but produces a
`KeyedSingleton` at the *keyed stream's* location, which is top-level in these tests.
So the watermark value must cross from the tick loop out to the top level.

In `hydro_lang/src/compile/ir/mod.rs`, the `HydroNode::ReduceKeyedWatermark` arm
(≈ line 5374) emits:

```rust
#chain_ident = chain();
#input_ident     -> map(|x| (Some(x), None))    -> [0]#chain_ident;
#watermark_ident -> map(|watermark| (None, Some(watermark))) -> [1]#chain_ident;
#fold_ident = #chain_ident -> #agg_operator::<'…>( … ) -> flat_map( … );
```

all via `add_dfir_at(&out_location, …)` (top level). The `#watermark_ident` was
produced inside the tick loop (via the tick source's `batch()`), so
`#watermark_ident -> map(...)` illegally exits the loop.

Pre-#2902, tick and top-level statements shared one flat graph (no loop contexts), so
this "just worked". With real loop contexts, the watermark now needs an explicit
un-windowing operator (`all_iterations()`) before being consumed at the top level.

### Fix (implemented — resolves `reduce_watermark_bounded`)

Add a builder hook `DfirBuilder::unwindow_for_consume` that un-windows an ident (via
`all_iterations()`) when it is produced inside a tick loop but consumed at a location
outside that loop, and call it for the watermark input of `ReduceKeyedWatermark`. The
default (simulation) impl is the identity; `ProdDfirBuilder` inserts the un-windowing
operator. See the *Fix* section below.

This fully fixes `reduce_watermark_bounded` (a pure top-level consumer). The other two
watermark tests use `.snapshot(&node_tick)` right after the reduce, which feeds the
top-level reduce output **back into the same tick** — see the note under Class C.

---

## Class B — Paxos: `all_iterations()` output re-enters a tick loop without windowing

**Tests:** `paxos_ir`, `paxos_some_throughput`, `paxos_log_some_throughput`

**Errors (8 per graph), e.g.:**
```
Operator `tee(...)`            entering a loop context must be a windowing operator, but is not.
Operator `cross_singleton(...)` entering a loop context must be a windowing operator, but is not.
Operator `enumerate(...)`       entering a loop context must be a windowing operator, but is not.
Operator `map(...)`             entering a loop context must be a windowing operator, but is not.
Operator `chain(...)`           entering a loop context must be a windowing operator, but is not.
```

With node IDs surfaced, **every** offending edge has an `all_iterations()`
predecessor, e.g.:

```
tee(14v1)             <- all_iterations(13v1)
tee(33v1)             <- all_iterations(32v1)
cross_singleton(156v1) <- all_iterations(155v1)
enumerate(199v1)      <- all_iterations(198v1)
map(211v1)            <- all_iterations(210v1)
cross_singleton(234v1) <- all_iterations(233v1)
tee(259v1)            <- all_iterations(258v1)
chain(287v1)          <- all_iterations(286v1)
```

Tracing one, the surrounding chain is:

```
10v1 chain_first_n(1) -> 11v1 all_iterations() -> 12v1 batch_lazy() -> 13v1 all_iterations() -> 14v1 tee()
      (in loop)            (EXIT loop)              (ENTER loop)          (EXIT loop)             (ENTER loop, ✗ no batch)
```

Two problems are visible:

1. **Missing windowing on re-entry.** A value that was un-windowed to the top level by
   `all_iterations()` (`13v1`) is fed straight into `tee()` (`14v1`), which lives
   inside a tick loop — there is no `batch()`/`batch_lazy()` on that edge. This is the
   direct cause of the "entering a loop context must be a windowing operator" errors.

2. **Redundant round-trips.** `12v1 batch_lazy() -> 13v1 all_iterations()` is a
   windowing operator immediately followed by an un-windowing operator (a no-op
   round-trip through the loop boundary).

The offending consumers are `Tee` (fan-out shared across locations),
`CrossSingleton`, `Enumerate`, `Map`, and `Chain` — operators whose *input* was
produced by a `YieldFromTick` (`all_iterations()`) at top level but whose own node
location is a tick. These operators consume the cross-loop ident directly instead of
routing it through a `Batch` boundary node.

This is the same underlying issue as Class A but in the *entering* direction, and it
is more pervasive because it is triggered by `Tee` fan-out and singleton references
that span loop contexts (paxos leans heavily on `defer_tick`/`all_ticks` feedback and
shared teed collections).

---

## Class C — round-trips through a loop + the `#3048` loop-ingress constraint create a false cycle

**Tests:** `into_singleton_unbounded_top_level_none_cardinality`, and (after the Class A
fix) `reduce_watermark_filter`, `reduce_watermark_garbage_collect`.

**Error:**
```
Failed to partition (cycle detected).:
Cyclical dataflow within a tick is not supported. …
```

Note this error comes from `partition_graph` (`flat_to_partitioned.rs`), **not** from
the windowing check in `flat_graph_builder.rs`.

### `into_singleton_unbounded_top_level_none_cardinality`

**Failing flat graph:**

```
1v1 source_iter([123]) -> 2v1 persist -> 3v1 batch()      <-- ENTER loop L
3v1 -> 4v1 all_iterations()                               <-- EXIT loop L  (immediately!)
4v1 -> 5v1 filter(|_| false) -> 6v1 map(|v| Some(v))      (top level)
6v1 -> 7v1 batch_lazy()                                   <-- ENTER loop L (again)
7v1 -> [0]11v1 chain_first_n(1)   (in loop L)
… -> 12v1 all_iterations() -> 13v1 batch_lazy() -> 14v1 fold::<'tick> …   (in/out of L again)
```

The data enters and exits the *same* tick loop `L` several times. Two things combine to
make partitioning fail:

1. **Redundant round-trips.** `3v1 batch() -> 4v1 all_iterations()` (and
   `12v1 all_iterations() -> 13v1 batch_lazy()`) are windowing/un-windowing operators
   placed back-to-back, which are semantic no-ops.

2. **The `#3048` loop-ingress ordering constraint.** In `flat_to_partitioned.rs` (block
   headed "Loop-ingress ordering constraints. Fix #3048"), for every forward edge
   `src -> dst` where `dst` is inside a loop that does not contain `src`, `src` is added
   as a predecessor of **every** node inside that loop. When a top-level node both
   feeds into `L` *and* is (transitively) downstream of `L`, this manufactures a cycle:
   `L-node -> … -> src -> (pred of, via #3048) -> L-node`.

### `reduce_watermark_filter` / `reduce_watermark_garbage_collect`

After the Class A fix removes the windowing error, these hit the same false cycle. The
reduce is emitted at the top level; its watermark comes from tick `L` (an ingress into
`L`), and the following `.snapshot(&node_tick)` batches the top-level reduce output
**back into `L`** (another ingress into `L`). Via the `#3048` constraint, the reduce's
output node becomes a predecessor of the watermark's in-`L` `batch()` node, closing a
false cycle:

```
watermark batch() (in L) -> all_iterations() -> map -> chain -> fold -> flat_map (top level)
        ^-------------------------- (#3048: pred of every L node) --------------┘  (via snapshot ingress)
```

### Root cause

The coarse `#3048` heuristic ("an external sender must precede *every* node in the
loop") is too strong once code-gen (a) bounces data in and out of the same loop and
(b) has a top-level operator that both reads from and writes into the same tick.

Both failing shapes are **re-entrant ticks**: a value that was produced inside a tick
(and therefore already left it) is fed *back into the same tick*. Semantically this is
not representable as a single DFIR `loop { ... }` context, so it should not be allowed
at the Hydro level in the first place.

### Fix (implemented — resolves all three Class C tests)

Re-entering a tick is avoided by consuming the top-level value in a **distinct** tick
(`node.tick()` a second time) rather than the one that produced it:

- `into_singleton_unbounded_top_level_none_cardinality`: the `.snapshot(...)`/`.batch(...)`
  now target a new `consume_tick` instead of the `node_tick` that produced the value via
  `.latest()`.
- `reduce_watermark_filter` / `reduce_watermark_garbage_collect`: the post-reduce
  `.snapshot(...)` now targets a new `snapshot_tick` instead of the `node_tick` that
  supplies the reduce's watermark (and, for `garbage_collect`, its tick-triggered input).

Because the producing tick is only ever *exited* and the consuming tick is only ever
*entered*, no loop is re-entered, the `#3048` false cycle disappears, and (combined with
the Class A fix) all three tests pass. The distinct tick is structurally identical, so
the test assertions are unchanged.

A natural follow-up is to detect and reject re-entrant ticks at the Hydro API level with
a clear error, rather than only failing deep in DFIR partitioning.

---

## Root cause (shared)

The production builder emits each operator in the loop context of its own location and
relies on explicit `Batch` / `YieldFromTick` / `BeginAtomic` / `EndAtomic` IR nodes to
insert windowing / un-windowing operators at loop boundaries. But:

- Operators whose *inputs* live in a different loop context than the operator's own
  location (`ReduceKeyedWatermark` watermark input — Class A; `Tee`/`CrossSingleton`/
  `Enumerate`/`Map`/`Chain` consuming a `YieldFromTick` output — Class B) do **not**
  route those inputs through a boundary operator, producing edges that illegally
  cross the loop boundary.
- The transition logic can also emit **adjacent** `batch*()`/`all_iterations()` pairs,
  which are no-ops that additionally make a tick loop appear cyclic to the partitioner
  (Class C).

## Fix status

- **Class A (`ReduceKeyedWatermark`)** — **fixed** for the pure top-level case. A new
  builder hook `DfirBuilder::unwindow_for_consume` un-windows an ident (via
  `all_iterations()`) when it is produced inside a tick loop but consumed at a location
  outside that loop. The default implementation is the identity (simulation does not emit
  `loop { ... }` contexts, so it needs no change); `ProdDfirBuilder` overrides it to
  insert `all_iterations()`. The `ReduceKeyedWatermark` emit arm now passes its watermark
  input through this hook.

  Verified: `reduce_watermark_bounded` now passes. Full `hydro_lang` suite
  (`--features deploy,sim`): 4 failures → 3 failures, **no new failures / regressions**.
  Simulation behavior is unchanged (identity default).

- **`reduce_watermark_filter` / `reduce_watermark_garbage_collect`** — **fixed** (Class C).
  Their `.snapshot(&node_tick)` re-entered the same tick that supplied the reduce's
  watermark; snapshotting into a distinct `node.tick()` removes the re-entrancy, and with
  the Class A fix both now pass.

- **Class C (`into_singleton` + the two watermark snapshot cases)** — **fixed** by
  rewriting the tests to avoid re-entering a tick (consume the top-level value in a
  second, distinct `node.tick()`). See the Class C *Fix* section. The whole `hydro_lang`
  suite (`--features deploy,sim`) now passes (was 4 failures → 0).

- **Class B (paxos)** — diagnosed but **not** fixed. It requires reworking the
  loop-boundary emission so that cross-loop-context consumers (especially `Tee` fan-out
  and singleton references fed by a `YieldFromTick`/`all_iterations()`) route through a
  windowing boundary on loop entry, and/or eliding redundant adjacent
  `batch*()`/`all_iterations()` pairs. This touches shared `Tee`/reference plumbing and
  carries a higher risk of regressing currently-passing tests, so it is left as a
  follow-up.

## Fix — code changes

`hydro_lang/src/compile/ir/mod.rs`:

- New `DfirBuilder::unwindow_for_consume(in_ident, in_location, out_location) -> Ident`
  trait method with an identity default (used by the simulation builder).
- `ProdDfirBuilder::unwindow_for_consume` inserts
  `#out = #in -> all_iterations();` at the root level of `in_location`'s graph when
  `in_location`'s loop context differs from `out_location`'s (i.e. the value is leaving a
  loop), returning the new ident; otherwise returns the input ident unchanged.
- The `HydroNode::ReduceKeyedWatermark` emit arm now binds the `watermark` child to read
  its location and routes the `watermark_ident` through `unwindow_for_consume` before
  building the `chain()`/`fold`/`flat_map` pipeline.
