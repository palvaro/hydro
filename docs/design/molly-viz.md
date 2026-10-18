# Molly-style visualizations for hydrofrac

Molly (Alvaro, Rosen, Hellerstein, SIGMOD 2015) showed every counterexample at two zoom
levels: the **Lamport diagram** of the failing run, and the **why-provenance** of the goal
in a run where it held. Hydrofrac tracks lineage at four levels (`Level::Execution`,
`Slice`, `Record`, `Counterfactual`), each a tighter support of the goal than the last, so
the ladder here has more rungs. This document describes what is implemented in
`hydro_lang::sim::viz` and how it fits the search.

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

## Zoom level 2+: provenance graphs

_To be written by the provenance-graph thread (`sim/viz/provenance.rs`)._

## Limitations and follow-ups

- External outputs are rendered as a hex prefix of their bytes: `LineageEvent::Output`
  carries no `display` (the output sink's codegen does not know the element type, unlike
  the network send's). Threading a `__maybe_debug__` rendering through
  `create_external_output` would fix this.
- Non-crashable nodes have no human name; they are labelled by their location's debug form
  (`Process(loc2v1)`).
- A `Trace` is built from the `Lineage`'s pairing of releases to sends, so a release the
  lineage could not match (`Schedule::unmatched_releases`) shows as a message in flight.
