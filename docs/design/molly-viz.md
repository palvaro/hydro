# Molly-style visualizations for hydrofrac

Molly (Alvaro, Rosen, Hellerstein, SIGMOD 2015) showed every counterexample at two zoom
levels: the **Lamport diagram** of the failing run, and the **why-provenance** of the goal
in a run where it held. Hydrofrac has two lineage levels (`Level::Execution`, the
execution-granular closure, and `Level::Precise`, the default) and `Precise` draws on two
oracles: record provenance from `SimFlow::with_provenance` where the program carries it, and
counterfactual **replay** of a node at the records it does not (`Prov::OPAQUE` records of a
provenance-compiled program, and every record of a program compiled without provenance). A
provenance graph therefore shows which of the three sources produced each step.
This document describes what is implemented in `hydro_lang::sim::viz` and how it fits the
search.

See `molly-viz-research.md` for the inventory of what the runtime records and what the
search used to keep (nothing structural: every level collapsed to a flat `Derivation`).

## Zoom level 0 and 1: Lamport diagrams

### The `Trace` (`sim/viz/trace.rs`)

A `Trace` is the compact, serializable (`serde`) record of one run that the renderers
draw from:

| Field | Content |
|---|---|
| `nodes: Vec<TraceNode>` | one per `(root location, member)` seen in the run: `label` (the crashable label `name` / `name@member`, `observer` for a `Spec`'s observer, else the location's debug form), `location`, `member`, `is_observer`, `crashable`, `crash: Option<Crash>` (`BeforeAnySend` or `AfterEvent(i)`, the send after which the installed crash took effect) |
| `events: Vec<TraceEvent>` | every event in global scheduling order: `node`, `exec` (`ExecId`), `tick`, and `kind ∈ {Exec, Send(m), Recv(m), Output{display, dropped}}` |
| `messages: Vec<TraceMessage>` | every send, in send order: `channel`, `channel_id`, `display`, `from`/`to` (node indices), `send_event`, `recv_event: Option` (`None`: never consumed—omitted, lost to a crash, or still in flight at the horizon), `fate ∈ {Delivered, Omitted, Crashed}`, `observation`, `lossy`, `delivery_id`, `ordinal` (the sender's send ordinal, crashable senders only), `payload_hash` |
| `eff_cut: Option<usize>` | the end of finite failures, as an index into `events` |

A trace keeps **no payload bytes**: a message is matched to a `Dependency` by `(channel_id,
from member, to member, FNV-1a hash of the wire bytes)` (`Trace::support(&Derivation) ->
BTreeSet<usize>`), and rendered by its `display`.

Receives are paired to sends the way the lineage pairs them: a top-level `Recv` by
delivery id, a scripted hook's `Released` items by the lineage's release matching (earliest
unmatched delivered send with the same bytes to that node on the hook's channels). The
builder is `build_trace` in `sim/ldfi.rs`, run from `RunRecord::schedule` on the `Lineage`
it has just built.

### Retention policy

`Schedule.trace: Option<Rc<Trace>>`:

- **always** retained for a failing schedule (`!goal_held`): failing schedules are rare and
  are what a counterexample shows;
- retained for **every** schedule under `Ldfi::with_traces()`, which is what the witness
  picture (level 1) needs. Memory grows with the schedules explored, so it is opt-in.

### Renderers (`sim/viz/lamport.rs`)

Both take a `&Trace` and `&LamportOptions { highlight: BTreeSet<usize>, show_observer,
show_outputs, placement, title, max_label }`.

- `lamport::mermaid` emits a Mermaid `sequenceDiagram`: `participant pN as label`; `A->>B`
  for a delivered and consumed message, `A-)B … (in flight)` for one still on the wire at
  the horizon, `A-xB … (omitted)`, `A--xB … (lost: sender crashed)`; `Note over X: 💀
  crashed` at the crash point; `Note over first,last: EFF`; `Note over X: ⇒ output`.
  Highlighted messages are prefixed `★` and boxed in a `rect`. A sequence diagram draws a
  message as one instant, so `Placement` chooses it: `Receive` (default) places a delivered
  message at its receive, so each lane's inbound order is the order the node consumed its
  messages (what decided its behaviour, and what the simulator's exploration varies);
  `Send` places it at its send. A crash note follows the row of the send that triggered it,
  and the sends lost to that crash follow the note, as its consequence.
- `lamport::dot` emits Graphviz DOT in the classic spacetime style: a header box per node,
  the node's events chained top to bottom, one rank per event in global order held by an
  invisible time spine (`t_head → t0 → … → t_eff → … → t_end`), a message as an edge from
  its send point to its receive point (`constraint=false`, so the ranks stay chronological),
  red dashed to a `✗` for an omission, gray dashed for a send lost to a crash, dotted to the
  bottom of the destination's lane for a message in flight, `💀` at crash points, an `EFF`
  label on its rank. Highlighted messages are bold and amber.

The observer and the traffic into it are hidden by default (`show_observer`).

### The counterexample view (`sim/viz/mod.rs`)

`Outcome::viz() -> Option<Counterexample>` bundles:

- `failing_trace()`, `lamport_mermaid()`, `lamport_dot()` — level 0, the failing run under
  the counterexample's faults;
- `witness()` — the last explored schedule in which the goal held with a non-empty
  derivation and a retained trace (needs `with_traces()`); `support_lamport_mermaid()`,
  `support_lamport_dot()` — level 1, that run with `Trace::support(derivation)`
  highlighted;
- `markdown()` — both as a Markdown report with embedded Mermaid blocks.

## Zoom level 2+: the provenance graph (`sim/viz/provenance.rs`)

### What is recorded

`Ldfi::with_provenance_graphs()` makes every derivation come with a `ProvGraph`
(`Schedule::provenance`, parallel to `Schedule::derivations`). The graph is the DAG the
closure actually walked; the flat `Derivation` is exactly its fault points
(`ProvGraph::dependencies()` equals the derivation, which the `hydro_test` test
`ldfi_viz::provenance_graphs_agree_with_derivations_at_every_level` checks at both levels).
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
| `Opaque { reason }` | a step the configured oracle could not resolve, under which the fallback hangs | `(anchor key, reason)` |

| Edge kind | From → to | Produced by |
|---|---|---|
| `Checked` | goal → output (or goal → message, for the no-goal "every pre-EFF delivery" derivation) | the goal reader |
| `Produced` | output or message → the execution that produced it | `Execution` (`closure`) |
| `Consumed` | execution → message it consumed (hook release, top-level receive, arrival an unscripted batch may have released) | `Execution` |
| `Carried` | execution → earlier execution whose state it carried (same tick) or whose yields it consumed | `Execution` |
| `Derived` | output or message → message in its record provenance; also → `Base` when the provenance is empty | `Record` (`record_closure`) |
| `Counterfactual` | output or message → delivery kept by replay | `Replay` (`counterfactual`, at a record without usable provenance under `Precise`, and at the observer for a hand-written specification) |
| `Fallback` | any → `Opaque` | every fallback: opaque record → replay → execution closure, replay did not reproduce the target, no scripted inputs, no replay oracle |
| `Alive` | output → `Alive` | `witnessed` / `witnessed_final` |

Every edge carries the `ProvLevel` that produced it (`Execution`, `Record`, `Replay`), so a
`Precise` graph whose closure visited an opaque record shows the `Opaque` node, then `Replay`
edges (or, where the replay did not reproduce the target, `Execution` `Produced` / `Consumed`
edges) beneath it. This is the "more levels of zoom": one derivation, drawn with the oracle
each step was actually resolved by.

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

Snapshots of both renderings for 2PC (`Precise` on the provenance-compiled program, whose
edges are all `record`, and `Execution` with its execution nodes) live in
`hydro_test/src/cluster/snapshots/two_pc_{record,execution}_*.snap`.

### Limits

- Nodes are executions, records (as sends), and deliveries; the operators a record traversed
  are not tracked (`Prov` is a set of delivery ids), so the graph shows *which* messages and
  executions a fact rests on, not the `join`/`fold` that combined them. The IR traversal in
  `sim::slice` (which names the channels) could annotate edges with the sink/channel site
  later.
- Under `Level::Precise` the record provenance keeps the first derivation of a duplicate
  element; alternative derivations are not drawn (they are not in the flat derivation either).
- The `Message` key is the send's index in the run, so graphs of different runs cannot be
  merged (they are not meant to be; the Lamport diagram of a *failing* run and the provenance
  graph of a *good* run are separate pictures, as in Molly).

## Limitations and follow-ups

- External outputs are rendered as a hex prefix of their bytes: `LineageEvent::Output`
  carries no `display` (the output sink's codegen does not know the element type, unlike
  the network send's). Threading a `__maybe_debug__` rendering through
  `create_external_output` would fix this.
- Non-crashable nodes have no human name; they are labelled by their location's debug form
  (`Process(loc2v1)`).
- A `Trace` is built from the `Lineage`'s pairing of releases to sends, so a release the
  lineage could not match (`Schedule::unmatched_releases`) shows as a message in flight.
