# hydrofrac-lite: design plan

Status: proposal. No code yet. A fresh branch that shares no files with the research line
(`hydrofrac-input-coverage` @ `0d801bbafc`, "the research branch" below), which stays as the
record of every optimization that was explored.

## 1. Goal

One job, done with no configuration: **for every collection a Hydro program labels
`EventualConsistency`, show that under the fault model its transports declare, every correct
member ends with the same facts, or produce a counterexample and its Lamport diagram.**

Non-goals:
- hand-written specifications (PRE/POST) and the old goal API;
- experiment machinery: lineage-level ablations, disjunctive provenance, aggregate witnesses,
  blind and random baselines, `LDFI_*` knobs;
- deploy-mode fault injection;
- delay, reordering and Byzantine faults (unchanged from the research branch).

Design rule: **convention over configuration.** Every choice the research branch exposed as a knob
is either fixed by convention, with the evidence that fixed it cited, or left to the research branch.

## 2. What a user writes

```rust
let mut flow = FlowBuilder::new();
// ... program, with its `EventualConsistency` labels ...
let check = hydrofrac::EcCheck::new(&mut flow);          // taps every label, adds the observer
let sim = flow.sim().with_cluster_size(&members, 3).compiled();
check.run(&sim, async || {                               // drive inputs; quiesce is implied
    send.send(Item(1));
    send.send(Item(2));
})
.assert_holds();                                         // panics with the report; writes diagrams
```

That is the whole API surface for the common case. Compare a typical research harness
(`hydro_test/src/cluster/ec_ldfi.rs:263-294`):
- `Ldfi::new().level(Precise)` (already the default), `.crashable(..)`, `.crashable_cluster(..)`
  (always every node), `.spec(&spec)`;
- `Spec::all_labels`;
- on the sim builder, `.skip_consistency_assertions()` and `.with_provenance()`;
- an explicit `quiesce().await`.

Across the 14 EC harnesses, the only per-test variation beyond inputs and cluster sizes is:
- `exploration(Fuzz { 8 })` (6 uses: ABD, gossip, uniform broadcast);
- `crash_budget(1)` (2 uses, multi-Paxos).

Those two become the only knobs:

| knob | default | why it can't be a convention |
|---|---|---|
| `.max_crashes(n)` | unbounded, plus the two vacuity constraints (§4.5) | multi-Paxos is impractical unbounded; nothing in the program says how many crashes matter |
| `.sample_schedules(n)` | exhaustive | racy programs (G-Set, uniform broadcast) have too many schedules (uniform broadcast's probe alone: >359k) |

Conventions that replace the research branch's per-harness calls:
- **every process and every cluster member is crashable**;
- **the fault model is the transport type** (lossy channels admit omission, `fail_stop` channels only
  their sender's crash);
- **EFF = end of run** (no EC harness ever called `end_of_finite_failures`; retrospective
  "Settled questions");
- the program's own consistency assertions are skipped automatically under the check;
- a hypothesis budget of 1024, after which the verdict is `Budget`, not a certificate (the retrospective
  recommends raising 256 to 1024);
- counterexample diagrams are always written, to `target/hydrofrac/<test>/<label>/`.

## 3. What is in, what is out

Every row cites the evidence. "Measured" refers to the records in `docs/design/hydrofrac-lite-data/`, all
on the research branch at `0d801bbafc`:
- A = positional, Precise (`rec-positional.tsv`, 125 searches);
- B = Auto with open sent sets and schedule reduction on, Precise (`rec-auto-open.tsv`, 125 searches);
- run C = the EC searches at Precise vs Execution, positional (`level/`, 54 searches);
- the earlier reduction-off comparison, positional vs `sent-set-open` (123/123 verdicts agree, hypotheses
  1,947 → 2,866). Its raw records were lost to temp-dir cleanup; only these totals survive.

| mechanism | lite | evidence |
|---|---|---|
| Sender-side filter on every `Network` send, transport type as fault model | **in** | the redesign that made every channel a fault point by construction; the consumer-side seam was closed (retrospective, "Fault point and fault model") |
| Omission identity `(channel, sender, destination, wire bytes)` | **in** | settled (retrospective) |
| Crash model: **positional only**, `crash(node after m)` | **in** | one fault names one message (O(1)), against a whole sent set (O(n)). Sent sets only pay where they enable schedule reduction, and in this corpus that saved ~1% of search time: B, 22 reduced searches, 977 schedules, ~5 s, almost all from one test (948 → 5). Where reduction doesn't apply, sent sets only add hypotheses (+47% overall with reduction off) |
| Static order check (`order.rs`) + one schedule per hypothesis, with open sent sets | **phase 2, out of v1** | Sound as measured: all 125 B verdicts agree, and no reduced search saw a never-seen send. It's the remedy if a program's schedule space explodes (uniform broadcast's probe: >359k schedules), so it's documented and measured but not built until a real program needs it |
| Generated EC spec: observer process, fixed fact domain from the failure-free probe, POST as a count over (correct member, fact) | **in** | the view is monotone by construction; this is what makes the inference sound (retrospective, wall 2; `spec_views.rs:63-95`) |
| Scalar labels checked at their input stream (`covered_by`) | **in** | already implemented in the research branch (`observe.rs:144-170`) |
| Vacuity: empty fact domain is `Vacuous`; empty derivation is a vacuous run; crash-all and crash-all-but-one-of-the-checked-cluster are excluded | **in** | settled (`1538508755`; ablation doc on crash bounds) |
| PRE, preserve points, `required` | **out** | the generated EC spec has no PRE; `required` is always empty on the EC path |
| Lineage: record provenance only, no `Level` | **in** | run C: Execution alone is 2.4× the wall time on EC, up to 14.6× on multi-Paxos, with less minimal counterexamples (§5) |
| Counterfactual replay, standing hook policies | **out** | replay only served opaque records and programs compiled without provenance; lite always compiles with provenance and falls back to the sound execution rule (§5) |
| Disjunctive provenance, aggregate witnesses, witness caps | **out** | no reduction on any protocol, and +10–40% hypotheses on broadcast; aggregate witnesses are draw-dominated (retrospective, "Disjunction", "Aggregate-local witnesses") |
| Old goal API (`goal*`, `Run::witness*`, `require*`) | **out** | decided "to be deleted"; 2 remaining callers, both expressible as specs (retrospective) |
| `Strategy::Blind`, `fuzz_faults`, `LDFI_RECORD`, `LDFI_*` env vars | **out of the API**; see §7 for the honesty check | experiment and diagnostic knobs ("Every knob not needed for the automatic path is an experiment or a diagnostic", retrospective) |
| HiGHS backend | **out**: same `good_lp` ILP on the pure-Rust `microlp` backend | §4.4 |
| Lamport diagram of each counterexample | **in** | requested; `build_trace` + trace data + one backend |
| Provenance-graph viz, witness pictures, payload parser in the report | **out** | output-only; about 1.4k lines |
| Per-label report with source-derived names (`source_names`) | **in** | the cost of output people can read |

## 4. Design

### 4.1 Crate layout

A new crate, `hydrofrac`, depending on `hydro_lang` with `sim`. It holds staged code (`q!`)
the same way `hydro_std` does, so the generated-spec views (`spec_views.rs` today) move into it
and stop being an ungated public module of `hydro_lang`. All hydrofrac logic lives in this crate;
`hydro_lang` only gains the extension points in §6.

Modules, with rough sizes ported or rewritten from the research branch:

| module | content | ~lines |
|---|---|---|
| `observe` | label enumeration (`ec_collections`), `LabelPlan` / `NotChecked` / `covered_by`, the tap pass | 600 |
| `spec` | observer, correct-member and fact feeds, the generated POST view (staged) | 350 |
| `fault` | `FaultState`: the per-send filter, positional crash points | 250 |
| `lineage` | derivations from the event log: record provenance, with the execution rule as the fallback for opaque records | 700 |
| `provenance` | the record-provenance IR rewrite (via E2) and its runtime tags | 1600 |
| `search` | the Molly loop, crash bounds, vacuity, the hitting-set ILP | 650 |
| `report` | per-label report, names | 600 |
| `lamport` | trace extraction + one diagram backend | 450 |
| **total** | | **≈5.1k**, against ≈12.7k of logic in the research branch |

### 4.2 Lineage

The event log records executions, sends with their fate and a delivery id, receives, and which
execution produced each observation. Every record carries the delivery ids it was computed from: the
provenance rewrite tags records as they flow through operators, and sends carry the tags across the
network. The support of an observation is the deliveries its record's tags name, closed backwards
through the sends that carried them.

Where the rewrite can't see into a record (a closure that takes state by `&`/`&mut`), the record is
opaque and its support falls back to the **execution rule**:
- a tick execution depends on the arrivals released into it and on earlier executions at its node;
- an async run depends on what it received.

The execution rule is sound for every program; it only costs precision. The research branch's
`record_closure` and `closure` (`ldfi.rs:3370`, `:3966`) are these two rules.

### 4.3 The loop

1. Run the failure-free probe; it learns the fact domain. An empty domain gives `Vacuous`. The probe
   is reused as hypothesis 0, so the failure-free run executes once, not twice as in the research branch.
2. For each label: propose the minimal admissible fault set that kills every derivation seen so far.
   Run it over all schedules, or over `sample_schedules(n)` of them.
3. If a correct member lacks a fact, report a counterexample. Otherwise the run's derivations become new
   clauses.
4. Stop when no admissible set exists (a certificate) or the hypothesis budget runs out (`Budget`).

### 4.4 Hitting sets: the research branch's ILP, on a pure-Rust solver

The EC path has no PRE, so the model is the research branch's `solve_hypothesis` without its PRE terms:
- one binary per candidate fault, minimising count;
- cover constraints, each conditional on its crash premises;
- at most one crash per node and the crash bounds;
- no superset of a vacuous set, and an exact no-good for each tried set.

Writing it as an ILP keeps it short; the model is about 150 lines.
- **Solver:** `good_lp` with the `microlp` backend (pure Rust, branch-and-bound) instead of HiGHS:
  `good_lp = { version = "1.15", default-features = false, features = ["microlp"] }` and
  `.using(good_lp::microlp)`. The research code uses no HiGHS-specific API.
- **Optimality:** with a zero MIP gap and no time limit, a solution is returned only when proven optimal, so
  certificates stay certificates.
- **Why not HiGHS:** HiGHS is the reason `cargo test` on `hydro_std`/`hydro_test` currently needs cmake ≥ 3.15,
  libclang and a C++ toolchain, and it is used nowhere else.
- **Risk, speed:** `microlp` has no presolve or cuts, and the dense no-good rows are its worst case. Benchmark
  against HiGHS on the largest EC search before committing, and set `opt-level = 3` for `microlp` in dev
  builds. If it's too slow, HiGHS stays available behind an opt-in feature.
- **Fix to carry over:** the research branch's tie-break weight `0.5/n` can add up to more than one fault,
  so for sets of size ≥ 3 a k-set can cost more than a (k+1)-set. Use `0.5/(n·n)`.

### 4.5 Crash bounds

Unbounded by default, with the two vacuity constraints from `1538508755`:
- never crash every node (Σ ≤ N−1);
- never crash all, or all but one, of the checked cluster (Σ ≤ |C|−2), since a single survivor agrees with itself.

`.max_crashes(n)` is the only override.

### 4.6 Output

- **The report:** one block per cluster, one row per label, with verdict, hypotheses and the counterexample in
  source names.
- **The diagram:** a Lamport diagram for each counterexample, from the failing schedule's trace (always
  kept).
- **Backend: DOT only** (the research branch's Lamport layout, ~430 lines). Always write the `.dot`;
  also write an `.svg` when Graphviz's `dot` is on `PATH`, so no new build dependency.
  - **Why not Mermaid:** a `sequenceDiagram` draws every message as one horizontal arrow, so a message sent
    before a crash and delivered after it is drawn as if the dead node sent it (see the broadcast render in
    `docs/design/hydrofrac-lite-img/broadcast_live_sender_crash.png`). In-flight messages across a fault are exactly what counterexamples are about,
    so Mermaid misleads where it matters. A second "preview" backend is also against the one-convention rule.
  - Real samples of the research emitter's Mermaid output are in `docs/design/hydrofrac-lite-data/level/mermaid/`.
- **Legibility fixes**, from the research branch's output, apply to the DOT backend:
  - **Facts render as raw bincode** (`⇒ 01000000`). The observation taps ship `(index, bincode)`, and the
    tap pass knows each label's element type, so the report decodes them with `Debug`.
  - **Recovery after EFF repeats itself.** ISR's 51-line diagram is mostly identical heartbeat/view rounds.
    Collapse consecutive identical rounds into one with a `×k` note.
  - **Wide protocols:** payloads set the lane width. Abbreviate long payloads to a legend.

## 5. Lineage level: record provenance stays (measured)

`Precise` (record provenance, plus replay for opaque records) had earned its place on **hand-written specs
under load** (2PC at `n=4 load=8`: 101 → 23 hypotheses), but had never been measured on the EC corpus.

**Run C** (`docs/design/hydrofrac-lite-data/level/`) covered 20 EC tests and 54 searches, positional, with a
scratch-only patch so `LDFI_LEVEL` overrides the harnesses' hard-coded `.level(Precise)`. Its two halves
are run P (Precise, `rec-ec-precise.tsv`) and run E (Execution, `rec-ec-exec.tsv`).
- **Verdicts:** 54/54 agree.
- **Totals, Execution vs Precise:** 1.8× the hypotheses (768 → 1393), 1.6× the schedules, **2.4× the wall
  time** (82.6 s → 201.5 s).
- **Worst cases:** multi-Paxos `:281` at 8.8× hypotheses and 14.6× wall; ISR `:343`/`:360` at 7–8× hypotheses and
  7–8× wall; reliable broadcast `:130` at 4.6× hypotheses.
- **Fine at Execution:** simple broadcast, G-Set, 2PC labels, uniform broadcast and `leader_merge` cost the same at both levels.
- **Counterexample quality:** Execution's are less minimal. ISR `:360` gives `{crash(client), crash(zk)}` against
  Precise's `{crash(zk)}`, because the coarse rule blames the client too.

**Decision: lite keeps record provenance as its only lineage**, so `Level` disappears as a concept.
- **Every compile instruments:** `EcCheck` always applies the provenance rewrite, and `with_provenance()`
  stops being a user call.
- **Opaque records fall back to the execution rule.** Counterfactual replay stays out. Replay only ever served
  records the rewrite can't see into (closures taking `&`/`&mut` state, `provenance.rs:520`) and programs
  compiled without provenance, which lite rules out by construction. The execution rule is sound, so the
  fallback costs precision only where records are opaque.
- **Unmeasured:** that "provenance without replay" variant. It's the first benchmark of step 4 in §8: count
  the opaque fallbacks in the EC corpus (the research branch logs `replays` / `replay_fallbacks` per schedule)
  and compare with run P.
- **What this costs lite:**
  - the provenance rewrite (`sim/provenance.rs`, ~900 lines of logic) and its runtime
    (`runtime/provenance.rs`, ~700) move into the `hydrofrac` crate, entering through E2;
  - core gains E5 (§6.2): sends carry a provenance tag on the simulator's wire.
  - What stays out: replay, the hook policies, `erase_guarded` and `map_tick_closure`.

## 6. Changes to core Hydro

Two principles:
- **Nothing changes for anyone who does not enable `hydrofrac`.** Today the research branch's send filter,
  event log and provenance modules are gated only by `sim`, so every simulator user pays for an unbounded
  log and cloned send bytes. Lite gates all of it behind a `hydrofrac` feature of `hydro_lang`, so
  nothing runs when the feature is off.
- **Extension points, not hydrofrac logic.** Each one is generic, small and testable without hydrofrac.

### 6.1 Independent fixes (land first, separate PRs, valuable on their own)

| PR | change | why it stands alone |
|---|---|---|
| F1 | `hydro_std::quorum::collect_quorum_with_response`: result no longer depends on batching | correctness bug; changes Paxos snapshots |
| F2 | Maelstrom many-to-many: `"port"` in the body, demux by port | wire-format bug for any cluster with several intra-cluster channels |
| F3 | sim gaps: top-level `Reduce` (replaces `todo!`), `AtLeastOnce → ExactlyOnce` retry cast, snapshot of an unbounded top-level `Optional`, `collapse_ticks_around_atomic`, clearer `todo!` messages | simulator coverage for ordinary programs |
| F4 | `CompiledSim::first_schedule`, and `exhaustive_then` / `fuzz_bounded_then` with a callback between executions | a deterministic single run for debugging; bounded memory for long explorations (the research branch measured 10 GB → 241 MB from folding results between executions) |
| F5 | `ClusterIds: Copy`, `Counter::next_count` | trivial, additive |

### 6.2 Extension points lite needs (one PR, all behind `hydrofrac`)

| id | extension point | size | justification |
|---|---|---|---|
| E1 | **Send interceptor:** the simulator calls an installed `dyn SimNetworkHook` on every `Network` send (it may drop the send) and receive, at execution boundaries, and on external outputs. It is `None` by default. | ~250 (builder.rs, graph.rs, compiled.rs) | the single seam for any fault injection or network tracing in the simulator; zero cost when absent; replaces the research branch's hard-wired `record_sent`/`LineageOutputSender` |
| E2 | **Pre-compile IR pass hook** on `SimFlow`: `with_ir_pass(f: fn(&mut [HydroRoot]))`, run before network compilation, plus `HydroRoot::input_mut` | ~40 | lets external crates add sim-only instrumentation (taps) without core knowing about them; also useful for test oracles |
| E3 | **Eager locations:** a location whose ticks run outside the schedule choice | ~40 | an observation-only location must not multiply schedules (64× more schedules on broadcast when the observer was explored) |
| E4 | **Read access to channel metadata:** `pub` (doc-hidden) access to each network node's name, endpoints and whether it is lossy, and `(de)serialize_bincode_with_type` exposed `pub(crate)` → `#[doc(hidden)] pub` | ~20 | the channel table and the tap serialization live in the hydrofrac crate rather than in core |
| E5 | **Send metadata on the simulator's wire:** E1's interceptor can attach an opaque `u64` tag to each send and read it back on receive (the research branch's `(u64 id, Bytes)` sim wire format), and E2 passes run in a declared order (observation taps before the provenance rewrite) | ~100 (estimate) | record provenance has to cross the network; an opaque tag keeps provenance itself out of core, and the ordering keeps taps traced like any operator |

Total about 450 lines in core, against about 1,680 in the research branch. Phase 2 (schedule
reduction) would add doc-hidden read access to the nondeterministic-operator markers for the order check.

### 6.3 Research-branch core changes lite deliberately avoids

| change | why it isn't needed |
|---|---|
| `create_network(.., input_element_type)` | breaks the public `DfirBuilder` trait only to print payloads; lite renders payloads from the observer side and abbreviates them |
| `broadcast_closed` rewrite (`flat_map_ordered` over `ClusterIds`) | changes codegen for every cluster→cluster broadcast only to hide the member-id list from the observation pass. In lite that list is checked like any other labelled collection. If such rows become noise, a suppression list in lite's own observation pass is the place to filter them, with no core change |
| `pub mod spec_views` (ungated) | moves into the hydrofrac crate as staged code |
| `Dfir::map_tick_closure` / `erase_guarded` | guards counterfactual replays that panic; lite doesn't replay. A program panicking under injected faults is reported as a counterexample |
| standing hook policies (`scripted.rs`, `tick_input.rs`, `observation.rs`), `BatchHook::{deliver_all, replay, omit, ..}` | replay and traced pass-through only; the `BatchHook` additions have no callers. The execution rule is sound without them |
| `with_provenance` as a core `SimFlow` API, and provenance branches in `builder.rs` | the rewrite enters through E2 and its tags through E5's opaque `u64`, so core knows nothing about provenance |
| fine-grained sink slices in `slice.rs` | leftovers of the removed `Level::Slice` (about 300 lines unused) |
| `location_names`, `keyed_singleton.rs` formatting | cosmetic |

## 7. Validation

- **The EC corpus:**
  - Port the EC harnesses (`ec_ldfi.rs`, `ec_demos_ldfi.rs`, `ec_all_labels_classes.rs`, ABD
    `hydrofrac_tests`) to the lite API.
  - Acceptance: **the same verdict per label as the research branch's positional records**
    (`docs/design/hydrofrac-lite-data/rec-positional.tsv`), and the ISR unclean-election bug found.
- **Honesty check, test-only:** a brute-force enumerator over every admissible fault set up to size 2, run on the small EC
  demos. Agreement with lite's verdicts tests the lineage pruning without a `Strategy::Blind` in the API.
- **Solver check:** on every ported search, the `microlp` and HiGHS backends return sets of the same size,
  checked once in CI against HiGHS behind its opt-in feature.
- **Performance budget:** total EC-corpus search time no worse than the positional records' EC rows.

## 8. Plan of work

1. F1–F5 as independent PRs.
2. E1–E5 in one core PR, behind `hydrofrac`, with unit tests that don't use hydrofrac. Each point gets a
   small in-tree test hook, for example a test interceptor that drops every message on one channel.
3. The `hydrofrac` crate: `fault`, `lineage`, `provenance`, `search` and `observe` first, then `spec`,
   then `report` and `lamport`.
4. The ported corpus and the brute-force cross-check; compare with the positional records. First
   benchmarks: "provenance without replay" against run P (count of opaque fallbacks, hypotheses, wall),
   and `microlp` against HiGHS.
5. Phase 2, only when a program's schedule space demands it: open sent sets + the order check + one
   schedule per hypothesis, with the never-seen-send tripwire as a hard failure in reduced searches.

## 9. Decisions

Settled:
1. **Crash model:** positional only; sent sets and schedule reduction are phase 2 (§3).
2. **Lamport backend:** DOT only; Mermaid rejected because it can't draw messages in flight across a fault (§4.6).
3. **`trust_order_insensitive(site)`:** not in v1 (moot until phase 2).
4. **Crate placement:** a separate `hydrofrac` crate; core only gets §6.2.
5. **`broadcast_closed`:** not changed; any later noise is filtered in lite's observation pass.
6. **Solver:** the `good_lp` ILP on `microlp`, pending a benchmark against HiGHS.
7. **Lineage:** record provenance only, with the execution rule as the opaque-record fallback and no replay
   (§5, run C).

Open:
- **Replay:** stays out unless the "provenance without replay" benchmark shows the opaque fallback costing
  much on the EC corpus.
