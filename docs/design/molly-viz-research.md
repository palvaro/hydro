# Molly-style visualizations for hydrofrac: what data exists

_Scratch research doc. Branch studied: `ldfi-hydrofrac` (`7d58ad20e0`). Line numbers refer to
that branch. No renderer is designed here; this only inventories the data a Lamport diagram and
a provenance graph would need, and where it is kept or lost._

Molly shows two zoom levels per counterexample: (1) a spacetime/Lamport diagram of the failing
run with omissions and crashes marked, (2) the why-provenance graph of the goal in the
*successful* runs (goal → rule firing → subgoals → base facts). Section 0 is the headline
answer on hydrofrac's "four levels"; A–E follow the research brief.

## 0. The four lineage levels

Terminology: the docs say "four lineage levels" (`docs/design/hydrofrac.md:226-237`); the code
numbers them Level 0–3 in `Level` (`hydro_lang/src/sim/ldfi.rs:2585-2602`); the history doc
has one section per level (`docs/design/history-hydrofrac-development.md:584-660`). Each level
is a different *rule for computing the backward closure* of an output. All four produce the
same output type, a flat `Derivation = BTreeSet<Dependency>` (`ldfi.rs:358`), and the
dispatch is `Lineage::derive` (`ldfi.rs:1640-1655`) / `Lineage::send_derivation`
(`ldfi.rs:1658-1672`).

| # | `Level` | Granularity of a node in the closure | Where computed | Needs |
|---|---|---|---|---|
| 0 | `Execution` | **Execution** (one async run or one tick execution, `ExecId`). An execution depends on every message its hooks released into it, every arrival at its node before it, and every earlier execution of the same tick (persisted state); an async run on its receives and the tick executions since its previous run; a send on the execution that performed it. | `Lineage::closure(root, sink = None)` `ldfi.rs:2200-2400`, `push_ticks_before` `ldfi.rs:2404` | nothing from the program |
| 1 | `Slice` (default) | **Execution, narrowed per sink** by a static backward slice of the IR (`SinkSlice { direct, carried, opaque }`, `slice.rs:59-66`): only the hooks/channels/yields that can reach the producing `Network`/`SendExternal` sink count, split into same-execution (`direct`) and through-`defer_tick` (`carried`). Opaque sinks fall back to Level 0. | `Lineage::closure(root, Some(sink))`; slices from `sim::slice::Slices` (`slice.rs:110-121`) computed once per compiled program | nothing from the program |
| 2 | `Record` | **Record**: each collection element carries `Tagged<T>(value, Prov)` where `Prov` is a sorted set of *delivery ids* (`runtime/provenance.rs:24, 100`). A network send / external output strips the tag into `LineageEvent::Sent.prov` / `Output.prov` (`runtime/lineage.rs:121, 144`). The closure follows delivery id → the `Sent` that minted it → its `prov` → … | IR rewrite `sim::provenance::rewrite` (`provenance.rs:490-700`), runtime `Tagged`/`Prov`; closure `Lineage::record_closure` (`ldfi.rs:1929-1956`) | `SimFlow::with_provenance()`; unsupported operators panic at compile time; `Reference`-capturing closures emit `Prov::OPAQUE` (`provenance.rs:367 wrap_opaque`) and that step falls back to Level 3 or 1 |
| 3 | `Counterfactual` | **Delivery, by intervention**: the node is replayed alone (`CompiledSim::replay`, hooks under `ReplayScript`) with its recorded deliveries removed one at a time, latest first, greedy deletion; a delivery is in the derivation iff the output disappears without it (1-minimal sufficient set). | `Lineage::counterfactual` (`ldfi.rs:1770-1882`), `counterfactual_output/_send` (`ldfi.rs:1694, 1719`), `Replayer` (`ldfi.rs:2535`) | hooked batches at the node (replay feeds hooks); falls back to Level 2 → 1 when the node does not reproduce its output with everything kept (`Schedule::replay_fallbacks`) |

Derivation relationships (coarse ↔ fine):

- Level 1 is Level 0 with the same event graph filtered by a per-sink static predicate; the
  slice is purely static (`Slices::precise(sink)`), and `opaque` sinks degrade to Level 0.
- Level 2 replaces the execution graph with the record graph but still *bottoms out* in the
  same `SentRecord`s and uses `send_dependency` (`ldfi.rs:1571`) for the leaves; where a
  record's `prov` is `OPAQUE` it recurses via `send_derivation`, which is Level 3 if a
  replayer is present, else Level 1 (`recorded_send_closure`, `ldfi.rs:1675`).
- Level 3 is per-node and per-step; the deliveries it keeps are followed to their sends with
  `send_derivation`, so a Level 3 derivation is a chain of per-node 1-minimal sets.
- There is also a *fifth*, observer-side step that is always counterfactual regardless of
  `Level`: a hand-written `Spec`'s view is replayed at the observer with facts removed one at
  a time (`spec.rs:60-72`, `Lineage::spec_view` `ldfi.rs:2093-2135`); the generated EC spec
  skips it (`generated == true` returns the recorded derivation).

**What is retained from each level.** Nothing structural. Every level is collapsed to a
`Derivation` (flat `BTreeSet<Dependency>`) inside `RunRecord::schedule` (`ldfi.rs:1074-1176`),
and `Schedule` keeps `derivations: Vec<Derivation>`, `observed`, `requirements` (all flat) plus
counters `replays`, `replay_fallbacks`, `unmatched_releases` (`ldfi.rs:362-398`). The
`Lineage` struct (`ldfi.rs:1218-1260`: `execs`, `ticks_at`, `released`, `recvs`, `sent`,
`by_id`, `outputs`) and the `RunRecord.events: Vec<LineageEvent>` log are dropped after
`schedule()` returns. The `Level` used is not recorded in `Schedule`/`Outcome` (only on `Ldfi`).

## A. Lamport-diagram data

### Types (`hydro_lang/src/sim/ldfi.rs`)

```rust
pub struct Message {                      // ldfi.rs:111-125
    pub channel: String,                  // ChannelInfo::label: `.name()`, else file:line of the send, else `ch#id`
    channel_id: usize,                    // private: sink id (StmtId of the Network node)
    pub from_member: Option<u32>, pub to_member: Option<u32>,
    payload: Vec<u8>,                     // private: wire bytes; identity = (channel_id, from, to, payload)
    pub display: String,                  // payload's Debug rendering, or first 8 bytes hex (render_payload ldfi.rs:2443)
}
pub enum Fault { Omit(Message), Crash { node: String, after: Option<SendId> } }   // ldfi.rs:194-207
pub struct SendId { channel: usize, to_member: Option<u32>, bytes: Vec<u8>, display: String } // ldfi.rs:210 (all private)
pub struct Dependency { pub message: Message, pub sender: Option<(String, u64)>, pub lossy: bool } // ldfi.rs:278-288
pub type SendSequences = BTreeMap<String, Vec<SendId>>;  // ldfi.rs:290  node label -> its performed sends, in order
pub type Derivation = BTreeSet<Dependency>;              // ldfi.rs:358
pub struct Schedule { goal_held, derivations: Vec<Derivation>, observed: Derivation,
    delivered: Vec<Message>, dropped: Vec<Message>, crashed_sends: Vec<(String, u64)>,
    sends: Rc<SendSequences>, pre_eff_sends: BTreeMap<String, usize>, requirements: Derivation,
    unmatched_releases, vacuous, replays, replay_fallbacks }                 // ldfi.rs:362-398
pub struct Hypothesis { pub faults: FaultSet, pub schedules: Vec<Schedule> } // ldfi.rs:431
pub enum Outcome { Counterexample { faults, schedule: Schedule, runs: Vec<Hypothesis> },
    Certificate { derivations, runs, exploration, grade }, Budget { runs }, Vacuous { runs }, Exhausted { runs } } // ldfi.rs:474-516
pub struct LabelResults { pub verdicts: Vec<LabelResult> }  // ldfi.rs:3339; LabelResult { collection: EcCollection, verdict: LabelVerdict } :3269
```

### What `Schedule` actually contains

Built in `RunRecord::schedule` (`ldfi.rs:1074-1176`) from `Lineage.sent`, which is populated
in `LineageEvent` order (`ldfi.rs:1307-1370`):

- `delivered: Vec<Message>` — every protocol send (observer sends excluded) with
  `SendFate::Delivered`, **in send order** (the order of `Sent` events = the order the filter
  saw them; the doc comment says "delivery order" but it is the global send sequence).
  `Delivered` means "put on the wire" (`runtime/lineage.rs:62-64`); it does *not* say the
  receiver consumed it before the horizon.
- `dropped: Vec<Message>` — sends with `SendFate::Omitted`.
- `crashed_sends: Vec<(node_label, ordinal)>` — sends with `SendFate::Crashed`, i.e. attempted
  after the node died (**only for crashable nodes**: `sender` is `None` otherwise and they are
  filtered out, `ldfi.rs:1087`). The message content is not kept, only `(node, ordinal)`.
- `sends: SendSequences` — per crashable node, its performed (Delivered + Omitted, non-observer)
  sends as `SendId`s in order; `pre_eff_sends` counts how many preceded EFF.
- `observed: Derivation` — every delivery a fault could have prevented (the blind universe).

What a `Message` lacks for a diagram: no `ExecId`, no `DeliveryId`, no receive event, no
sender node when the sender is not crashable (only `from_member`; the location is implied by
the channel), no timestamp. The sender's local ordinal exists only inside `Dependency.sender`
and `crashed_sends`, and only for crashable nodes.

### What the runtime log has (and loses)

The raw log, `Vec<LineageEvent>` (`runtime/lineage.rs:84-146`), is sufficient for a full
spacetime diagram:

- `Exec { id: ExecId, location, member, tick }` — one per execution, **`ExecId` is a global
  scheduling order** (`lineage.rs:49-50`: `a < b` means `a` ran before `b`). Per-node local
  time = index of the exec among that node's execs (`Lineage.ticks_at` / `asyncs_at`,
  `ldfi.rs:1222-1226`). Tick vs async is distinguishable.
- `Sent { exec, id: DeliveryId, channel, from, from_member, to, to_member, bytes, display, fate, prov }`
  — sender node = `(from, from_member)`, sender's event = `exec`; `fate ∈ {Delivered, Omitted, Crashed}`.
  Crashed sends are logged with their full content and destination (lost in `Schedule`).
- `Recv { exec, channel, to, to_member, send: DeliveryId }` — receiver node and the receiving
  execution, keyed to the send by `DeliveryId` (`Lineage.by_id`, `recvs_at`). For hooked
  batches the arrival into a tick is a `Released { exec, hook_id, member, items }` matched to a
  send by bytes/channel (`ldfi.rs:1399-1438`).
- `Output { exec, sink, port, member, bytes, dropped, prov }` — external outputs; `dropped`
  marks outputs suppressed because the node was dead.

So from the log one can reconstruct, per message, `(sender node, sender exec)` and
`(receiver node, receiving exec)` or "in flight at horizon" (a `Delivered` send with no `Recv`
/ matched `Released`). The crash instant of a node is the first `Sent` with `fate == Crashed`
or, for `CrashPoint::BeforeAnySend`, the start. `Fault::Crash.after: SendId` identifies the
last successful send by `(channel, to_member, bytes)`. EFF is `RunRecord.eff_cut`, an index
into the event log (`ldfi.rs:881`), so the diagram can draw the EFF line.

Nodes are `NodeKey = (LocationId, Option<u32>)` (`lineage.rs:53`). Human labels exist only for
crashable nodes (`Search.nodes: BTreeMap<String, NodeKey>`, `ldfi.rs:2567`; `label@member`
via `member_label` `ldfi.rs:2522`); other nodes only have `LocationId`'s `Debug` such as
`Cluster(loc2v1)`. The observer process (`Spec::observer`, `spec.rs:314`) is a node too; its
inbound sends are `observation == true` and excluded from `delivered`. There is no notion of
"round"; the only clocks are `ExecId` and per-node exec index.

**Availability**: the log lives in `RunRecord.events` (`ldfi.rs:1040`, private) and is
consumed by `Lineage::new`; `Schedule` keeps none of it. `lineage_events()` is
`pub(crate)` (`compiled.rs:419`). A Lamport renderer therefore needs either (a) a new
retained structure on `Schedule` (e.g. an `Rc<Vec<LineageEvent>>` or a reduced
`Vec<MessageEvent>` with send/recv execs), or (b) to be built inside `RunRecord::schedule`.

## B. Provenance graph data

### Is there a tree/DAG?

Transiently, yes, at two granularities; retained, no.

1. **Execution DAG (Levels 0/1).** `Lineage` (`ldfi.rs:1218-1260`) is an indexed graph:
   execs → releases/recvs → sends (`SentRecord.exec`) → execs. `closure()` walks it with an
   explicit stack and inserts leaves into a `BTreeSet`; intermediate exec nodes and edges are
   never materialized as a result. Nodes of this DAG would be *executions*, not operators.
2. **Record DAG (Level 2).** `Sent.prov` / `Output.prov` give, per emitted record, the set of
   delivery ids it was computed from; each delivery id maps to one `Sent` (`by_id`). That is a
   genuine why/how-provenance DAG at *message* granularity: output record → {deliveries} →
   each delivery's send record → {deliveries} → … → base (empty `prov`: constants, timers,
   external inputs). `record_closure` (`ldfi.rs:1929`) flattens it.
3. **Level 3** yields a per-node 1-minimal set; its "tree" is the recursion through
   `send_derivation`, again flattened.

What flattening loses: the intermediate nodes (which execution / which record produced what),
the edge structure (which delivery fed which send), and the alternative derivations (dedup
keeps the first provenance seen, `runtime/provenance.rs:14-16`). The *operator path* is not
recoverable at any level: `Prov` is a set of delivery ids unioned across operators with no op
ids; the only operator-level artifact is the static `SinkSlice` (which hooks/channels feed a
sink) and `ChannelInfo` (channel label = `.name()` or `file:line`). Sinks and channels are
keyed by `StmtId` (`slice.rs:28-30`), so a renderer could label a provenance DAG's *edges* by
channel/sink site, and could decorate a message node with the static slice of the sink that
produced it, but not with the dynamic rule firing (`join`/`fold`/…) Molly's graph shows.

### Getting from a counterexample back to it

- `Outcome::Counterexample { faults, schedule, runs }` (`ldfi.rs:477-484`): `schedule` is the
  **failing** schedule, whose `derivations` is **empty** (`POST` did not hold, `ldfi.rs:1125-1145`).
  The successful derivations are in `runs[..n-1]` (each `Hypothesis.schedules[*].derivations`),
  flat. This matches Molly: the provenance graph is of the good runs; the Lamport diagram of
  the bad one.
- `Dependency.message` + `Dependency.sender` are the leaves. There is no pointer from a
  `Dependency` back to a `Sent`/`Recv`/exec.

### What "the goal" is

The goal is a `Spec` view (`spec.rs:1-30`): `PRE` and `POST` are `Optional`s in the observer's
tick; a view holds iff its final `sim_output` value is non-empty (`Spec::holds`, used at
`ldfi.rs:999-1016`). For the generated EC spec the goal is the count equality in
`spec_views::eventual_consistency` (`hydro_lang/src/spec_views.rs:51-62`): distinct
`(member, fact)` pairs at correct members = present facts × correct members; there is no `PRE`.
Concretely a counterexample's "failed goal fact" is a `(member, fact)` pair that some correct
member holds and another lacks; the pairs each correct member *did* hold are `Output` events
on the `facts` port (`FactFeed.port`, `spec.rs:471`; collected into `RunRecord.spec_facts`,
`ldfi.rs:953-968`) — but per-member attribution is lost because `spec_facts` is a
`BTreeSet<Vec<u8>>` of facts only. Under `search_all_labels`, which collection the goal is
about is `LabelResult.collection: EcCollection` (`slice.rs:129-156`: `label` = source site,
`op`, `location`, `minted`, `exit`). For the hand-written API, `Goal::Any/AllMembers/Witnessed`
(`ldfi.rs:1097-1125`) name a `sim_output` port.

## C. Where the data is reachable

- Entry points: `Ldfi::search(&CompiledSim, body) -> Outcome` (`ldfi.rs:2792`),
  `Ldfi::search_all_labels(..) -> LabelResults` (`ldfi.rs:2813`), `Ldfi::run` (goal API,
  `ldfi.rs:2880`), `Ldfi::fuzz_faults[_with] -> FuzzOutcome` (`ldfi.rs:3448/3467`).
  `Outcome` is **returned** (and `Display`ed; the counterexample display prints `delivered`,
  `omitted`, `sends lost to crashes`, `ldfi.rs:600-646`). Tests destructure it, e.g.
  `hydro_test/src/cluster/two_pc_ldfi.rs:424-447`, `:470-490`.
- Everything in `Outcome`/`Schedule` is `Clone + Debug`, **no serde** derives
  (`ldfi.rs:361, 430, 473`). `Message`, `SendId` have private fields (`channel_id`, `payload`,
  `bytes`), so an exporter must live in `hydro_lang::sim::ldfi` or get accessors.
- Features (`hydro_lang/Cargo.toml:21-62`): `ldfi = ["sim", "dep:good_lp"]`; `sim` already
  pulls `dep:serde_json`; `serde` (derive) is unconditional (`Cargo.toml:170`). `viz` is a
  separate feature (`"build", dep:auto_impl, clap, itertools, serde_json`) gating
  `hydro_lang::viz` (`lib.rs:141-144`). Module gating: `sim::ldfi/spec/observe` are
  `#[cfg(feature = "ldfi")]`; `sim::provenance/slice` are sim-only (`sim/mod.rs:64-80`).

## D. Existing viz conventions

- `hydro_lang/src/viz/render.rs:99-141`: trait `HydroGraphWrite { type Err; write_prologue,
  write_node_definition(VizNodeKey, &NodeLabel, HydroNodeType, Option<LocationKey>,
  Option<LocationType>, Option<&Backtrace>), write_edge(src, dst, &HashSet<HydroEdgeProp>,
  Option<&str>), write_location_start/_end, write_node, write_epilogue }`. Backends:
  `HydroDot<W: fmt::Write>` (`graphviz.rs:16-34`, emits `digraph HydroIR { layout=dot;
  compound=true; ... }`, locations as subgraphs/clusters) and `HydroMermaid<W>`
  (`mermaid.rs:29-50`), both over `IndentedGraphWriter` (`render.rs:61`). `escape_dot`
  (`graphviz.rs:11`), `escape_mermaid` (`mermaid.rs:11`). A `HydroGraphStructure`
  (`render.rs:588`) is the intermediate model; `GraphApi` (`api.rs:18`) renders/writes files;
  `json.rs` is a JSON backend. This trait is IR-shaped (node types `Source/Transform/Join/…`);
  a Lamport diagram does not fit it, but a provenance DAG could reuse `HydroDot`'s
  prologue/escaping style and location clustering if we add a sibling writer, or just emit DOT
  directly in `sim::ldfi` to avoid pulling `viz` (which requires `build`).
- `docs/design/hydrofrac.md` has **no** visualization item; "Next steps" (`:398-405`) list
  scalar-input checks, convergence witnesses, decision-trace replay, Raft under the generated
  spec. `ldfi-on-sim-hooks.md:904-947` compares with Molly's lineage but not its rendering.

## E. How a counterexample is produced

Search loop `Ldfi::run` (`ldfi.rs:2880-3250`): probe (no faults) learns `FACTS`
(`ldfi.rs:2955-2975`), then each hypothesis is run under `sim.exhaustive` or `fuzz_bounded`
(`ldfi.rs:2947-2951`); each instance yields a `RunRecord`, turned into a `Schedule` after the
instance returns (`ldfi.rs:2984-2987`, so counterfactual replays can run). `failing =
schedules.iter().find(|s| !s.goal_held)` (`ldfi.rs:2993`): the **first explored schedule under
the breaking hypothesis in which `POST` did not hold** is reported, with `runs` = every
hypothesis so far (`ldfi.rs:3116-3121`). Successful schedules' `derivations` become `Clause`s
(derivation + `SendSequences` + crash premises, `ldfi.rs:401-411`), and the next hypothesis is
a minimal hitting set (`good_lp`) over `Clause::candidate_faults`.

## Summary of gaps for the two visualizations

| Need | Exists at runtime | Retained in `Outcome` |
|---|---|---|
| Per-node ordered events, send/recv pairing, in-flight at horizon | `Vec<LineageEvent>` (Exec/Sent/Recv/Released/Output) | **No** (only `delivered`/`dropped` in send order, `crashed_sends` as `(node, ordinal)`) |
| Crash instant per node | first `Sent{fate: Crashed}` or `BeforeAnySend`; `Fault::Crash.after` | `Fault` (yes), `SendSequences` (yes, crashable nodes only) |
| EFF marker | `RunRecord.eff_cut` | `pre_eff_sends` counts only |
| Human node names | crashable nodes only (`Search.nodes`) | labels inside `Fault`/`Dependency` strings only |
| Provenance DAG (exec- or record-level) | `Lineage` indexes; `Sent.prov`/`Output.prov` | **No** (flat `Derivation`) |
| Operator/rule per derivation step | not tracked (`Prov` is a set of delivery ids) | — |
| Which `(member, fact)` pairs held / were missing | `Output` events on the facts port, per member | `spec_facts` facts only, no member |
| Serialization | none | none (`Clone + Debug` only; private fields) |
