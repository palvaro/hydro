# Molly-style visualizations for hydrofrac

_Design notes for `hydro_lang::sim::viz`. The sibling document `molly-viz-research.md`
inventories the data; this one describes what was built on it._

Molly reported every counterexample at two zoom levels: the Lamport (spacetime) diagram of the
failing run, and the why-provenance graph of the goal in a run where it held. Hydrofrac has
four lineage levels (`ldfi::Level`), each a different rule for the backward closure, so the
provenance view has more than one zoom: the same goal can be drawn at execution, slice,
record, or counterfactual granularity, and a graph shows where a finer level fell back to a
coarser one.

## Provenance graph (`sim::viz::provenance`)

### What is recorded

`Ldfi::with_provenance_graphs()` makes every derivation come with a `ProvGraph`
(`Schedule::provenance`, parallel to `Schedule::derivations`). The graph is the DAG the
closure actually walked; the flat `Derivation` is exactly its fault points
(`ProvGraph::dependencies()` equals the derivation, which the `hydro_test` test
`ldfi_viz::provenance_graphs_agree_with_derivations_at_every_level` checks at Levels 1–3).
Recording never changes a derivation: the recorder only observes the walk.

Edges point from the thing that depends to the thing it depends on, so the goal is at the
top and the fault points at the bottom.

| Node kind | Stands for | Key (dedup within a run) |
|---|---|---|
| `Goal { label }` | the view / port / witness set that was checked (`POST`, `some output on port p`, `witnessed outputs`, …) | the label |
| `Output { node, exec, port, display }` | an output that witnessed the goal | `(exec, port, member, bytes)` |
| `Exec { node, exec, tick, location }` | one execution (async run or tick execution) | the `ExecId` |
| `Message { message, from, to, fate, observation, fault_point }` | a send and its delivery; `fault_point` says whether a fault could have prevented it (i.e. whether it is in the flat derivation) | the index into the run's sends |
| `Alive { node, through, message }` | a witnessed output's producer having survived to emit it | `(node, sends)` |
| `Base { label }` | something computed from nothing off a channel (constant, timer, external input) | the label |
| `Opaque { reason }` | a step the configured level could not resolve, under which the fallback hangs | `(anchor key, reason)` |

| Edge kind | From → to | Produced by |
|---|---|---|
| `Checked` | goal → output (or goal → message, for the no-goal "every pre-EFF delivery" derivation) | the goal reader |
| `Produced` | output or message → the execution that produced it | Levels 0/1 (`closure`) |
| `Consumed` | execution → message it consumed (hook release, top-level receive, arrival an unscripted batch may have released) | Levels 0/1 |
| `Carried` | execution → earlier execution whose state it carried (same tick) or whose yields it consumed | Levels 0/1 |
| `Derived` | output or message → message in its record provenance; also → `Base` when the provenance is empty | Level 2 (`record_closure`) |
| `Counterfactual` | output or message → delivery kept by replay | Level 3 (`counterfactual`) |
| `Fallback` | any → `Opaque` | every fallback: opaque record → L3/L1, replay did not reproduce the target, no scripted inputs, no replay oracle |
| `Alive` | output → `Alive` | `witnessed` / `witnessed_final` |

Every edge carries the `ProvLevel` that produced it, so a Level 2 graph whose closure
visited an opaque record shows the `Opaque` node, then Level 3 edges (or Level 1 `Produced`
/ `Consumed` edges) beneath it. This is the "more levels of zoom": one derivation, drawn with
the level each step was actually resolved at.

### How it is recorded

The closure functions in `ldfi::Lineage` take an *anchor*, the graph node of the thing being
derived (an `Output` or a `Message`), and hang their edges from it; the anchor is `None` when
no graph is being recorded and every recording helper is then a no-op (one `Option` check).
Send derivations are memoized by the closure; so that a memoized derivation can be spliced
into every graph that reaches it, each is recorded into a sub-graph of its own
(`ProvRecorder::push` / `pop`) that is kept beside the memoized derivation
(`Lineage::send_prov_memo`) and merged (by key) wherever the send is reached again.

Goals with several derivations (`any_output`, `all_members`) record one graph per output
derivation; `all_members` merges the per-member graphs along the same cross product that
unions the derivations. `PRE` (the requirements) is derived with no goal begun and leaves no
graph.

### Rendering

- `provenance::dot(&graph, &opts)`: Graphviz, `rankdir=TB`, executions and outputs grouped
  in a `subgraph cluster` per node, messages between the clusters. Goal: double octagon;
  fault-point message: green note; reliable / non-crashable message: grey dashed note;
  omitted or crashed message: red note; opaque: red dashed box. Edge styles per kind, with the
  level as a faint label.
- `provenance::mermaid(&graph, &opts)`: `flowchart TD` with the same grouping and
  `classDef`s.
- `ProvOptions { collapse_execs, show_levels, show_non_fault_points, title }`.
  `collapse_execs` draws `ProvGraph::collapsed()`, which elides the `Exec` nodes (connecting
  what depended on an execution directly to what it consumed, through chains of `Carried`
  edges): the compact Molly-like "goal → messages" picture.
- `ProvGraph::to_json()` for other front ends (`serde` on every type; the dedup index is not
  serialized).

Snapshots of both renderings for 2PC (Level 2, and Level 1 with its execution nodes) live in
`hydro_test/src/cluster/snapshots/two_pc_{record,slice}_*.snap`.

### Limits

- Nodes are executions, records (as sends), and deliveries; the operators a record traversed
  are not tracked (`Prov` is a set of delivery ids), so the graph shows *which* messages and
  executions a fact rests on, not the `join`/`fold` that combined them. The static slice
  (`sim::slice`) could annotate edges with the sink/channel site later.
- Under `Level::Record` the record provenance keeps the first derivation of a duplicate
  element; alternative derivations are not drawn (they are not in the flat derivation either).
- The `Message` key is the send's index in the run, so graphs of different runs cannot be
  merged (they are not meant to be; the Lamport diagram of a *failing* run and the provenance
  graph of a *good* run are separate pictures, as in Molly).
