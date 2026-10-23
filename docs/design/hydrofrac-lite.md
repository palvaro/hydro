# hydrofrac-lite: design plan

Status: proposal, revised after review. A fresh branch that shares no files with the research line
(`hydrofrac-input-coverage` @ `0d801bbafc`, "the research branch" below), which stays as the
record of every optimization that was explored.

**Base:** `origin/main` @ `90d9d26736`, as is, with no rebase onto upstream. That is where the EC corpus lives:
ABD, multi-Paxos and the broadcast demos are among its 75 commits ahead of `upstream/main`, which is only 53
commits ahead the other way and has none of the sim-hook work. Two consequences:
- core changes are written against `90d9`'s single-file `sim/runtime.rs`, not the research branch's
  `sim/runtime/` split;
- `90d9` already has its own crash injection and provenance, which the extension points absorb (§6.2).

The research line's delta proper is `4de7c45e41..0d801bbafc`. Some removals in that range only revert unrelated work,
so it is read hunk by hunk, never cherry-picked whole.

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

Design rules:
- **Convention over configuration.** Every choice the research branch exposed as a knob is either fixed by convention,
  with the evidence that fixed it cited, or left to the research branch.
- **Nothing exotic.** Lite and its ported corpus use only what `main` has, plus the extension points in §6.2. Nothing
  that exists only on the research branch: no `BatchHook`, no `flow.sim_hook()`, no standing hook policies, no
  `NonDet::hooked`. A program that needs such machinery is rewritten on plain `main` features or left out.

## 2. What a user writes

```rust
let mut flow = FlowBuilder::new();
// ... program, with its `EventualConsistency` labels ...
let check = hydrofrac::EcCheck::new(&mut flow);          // taps every label, adds the observer
let sim = flow.sim().with_cluster_size(&members, 3).compiled();
check.run(&sim, async || {                               // faults may strike while this drives inputs;
    send.send(Item(1));                                  // on return the network heals and the run quiesces
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
- an explicit `quiesce().await`;
- in some harnesses, an explicit `run.end_of_finite_failures()` (§4.6).

Across the EC harnesses, the per-test variation beyond inputs and cluster sizes is:
- `exploration(Fuzz { 8 })` (6 uses: ABD, gossip, uniform broadcast);
- `crash_budget(1)` (2 uses, multi-Paxos);
- `end_of_finite_failures()` (`gossip_all_labels`, `ec_demos_ldfi.rs:404`; also ISR's harness, now out of scope);
- restricted crashable sets (2PC `…with_the_coordinator_correct`, ISR `…with_the_harness_fault_model`).

The first two become the only knobs. The last two become conventions: heal (§4.6) and the crash sweep (§4.5).

| knob | default | why it can't be a convention |
|---|---|---|
| `.max_crashes(n)` | no cap: the sweep stops only at a counterexample or at the vacuity bounds (§4.5) | caps the crash sweep. Multi-Paxos is impractical unbounded, and nothing in the program says how many crashes matter |
| `.sample_schedules(n)` | exhaustive | racy programs (G-Set, uniform broadcast) have too many schedules (uniform broadcast's probe alone: >359k) |

Conventions that replace the research branch's per-harness calls:
- **every process and every cluster member is crashable**; there are no restricted crashable sets. What a
  restriction used to express ("holds if the coordinator stays up"), the crash sweep now reports (§4.5);
- **the fault model is the transport type** (lossy channels admit omission, `fail_stop` channels only
  their sender's crash);
- **heal when the run closure returns** (§4.6): omissions are admitted only while the closure runs, and
  crashes persist;
- **the crash sweep is the output**: for each label, the largest number of crashes it tolerates and a
  counterexample one crash beyond that (§4.5);
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
| Generated EC spec: observer process, fixed fact domain from the failure-free probe, POST as a count over (correct member, fact) | **in**; the observer batches with a plain `batch` on its eager location (E3), not the research `Spec::accumulate`'s `BatchHook` / `NonDet::hooked` | the view is monotone by construction; this is what makes the inference sound (retrospective, wall 2; `spec_views.rs:63-95`) |
| Scalar labels checked at their input stream (`covered_by`) | **in** | already implemented in the research branch (`observe.rs:144-170`) |
| Vacuity: empty fact domain is `Vacuous`; empty derivation is a vacuous run; crash-all and crash-all-but-one-of-the-checked-cluster are excluded | **in** | settled (`1538508755`; ablation doc on crash bounds) |
| Heal at the end of the run closure (Molly's EFF, renamed) | **in**, as a convention | without it every retrying protocol has a meaningless omit-every-retry counterexample (§4.6) |
| Crash sweep: bound 0, 1, 2, …, keeping learned clauses | **in**, as the default output | derivations don't depend on the bound, so raising it is cheap. It replaces restricted crashable sets and `crash_budget` (§4.5) |
| Restricted crashable sets (`.crashable(..)` on a subset) | **out** | every node is crashable; the sweep reports what a restriction used to assume (§4.5) |
| PRE, preserve points, `required` | **out** | the generated EC spec has no PRE; `required` is always empty on the EC path |
| Lineage: record provenance only, no `Level` | **in** | run C: Execution alone is 2.4× the wall time on EC, up to 14.6× on multi-Paxos, with less minimal counterexamples (§5) |
| Counterfactual replay, standing hook policies, `BatchHook`, `flow.sim_hook()` | **out** | replay only served opaque records and programs compiled without provenance; lite always compiles with provenance and falls back to the sound execution rule (§5). The hooks exist only on the research branch ("nothing exotic", §1) |
| Disjunctive provenance, aggregate witnesses, witness caps | **out** | no reduction on any protocol, and +10–40% hypotheses on broadcast; aggregate witnesses are draw-dominated (retrospective, "Disjunction", "Aggregate-local witnesses") |
| Old goal API (`goal*`, `Run::witness*`, `require*`) | **out** | decided "to be deleted"; 2 remaining callers, both expressible as specs (retrospective) |
| `Strategy::Blind`, `fuzz_faults`, `LDFI_RECORD`, `LDFI_*` env vars | **out of the API**; see §7 for the honesty check | experiment and diagnostic knobs ("Every knob not needed for the automatic path is an experiment or a diagnostic", retrospective) |
| HiGHS backend | **out**: same `good_lp` ILP on the pure-Rust `microlp` backend | §4.4 |
| Lamport diagram of each counterexample | **in** | requested; `build_trace` + trace data + one backend |
| Provenance-graph viz, witness pictures, payload parser in the report | **out** | output-only; about 1.4k lines |
| Per-label report with source-derived names (`source_names`) | **in** | the cost of output people can read |

## 4. Design

### 4.1 Crate layout

A new crate, `hydrofrac`, depending on `hydro_lang` with its `hydrofrac` feature (which implies `sim`; §6). It holds staged code (`q!`)
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
- **What "released into it" needs:** on the research branch, a tick's arrivals came from the hook policies'
  `Released` events, which lite drops. So E1 reports release attribution at each tick boundary
  (`on_release`: which received deliveries the tick consumed), and `closure` loses its hook-release arm.
- **Known opaque case, `leader_merge`:** the research rewrite mishandles `entries_partially_ordered` on a keyed
  stream, so `leader_merge` was compiled without provenance there (`ec_demos_ldfi.rs:216-225`). Lite always
  rewrites, so the rewrite detects this operator shape and marks its records opaque rather than emitting wrong code,
  with a test.
  - The consequence is precision only: those records take the execution rule. Run C measured `leader_merge` at
    the same cost under Execution as under Precise (§5), so on this corpus the fallback should cost nothing.
  - A proper rewrite for the operator can come later if the fallback turns out to cost much.

### 4.3 The loop

1. Run the failure-free probe; it learns the fact domain. An empty domain gives `Vacuous`. The probe
   is reused as hypothesis 0, so the failure-free run executes once, not twice as in the research branch.
   - The probe's POST holds trivially, because the domain is empty while the probe runs. So its clauses come
     from the records on the held-facts port, not from POST.
2. For each label, starting at crash bound 0: propose the minimal admissible fault set that kills every
   derivation seen so far. Run it over all schedules, or over `sample_schedules(n)` of them.
3. If a correct member lacks a fact, report a counterexample. Otherwise the run's derivations become new
   clauses.
4. When no admissible set exists at bound k, the label tolerates k crashes. Raise the bound to k+1, keep every
   clause, and continue at step 2 (§4.5).
5. Stop at a counterexample, at the top of the sweep (a certificate for the largest bound reached), or when the
   hypothesis budget runs out (`Budget`).

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

### 4.5 Crash sweep

Every node is crashable, and the output for each label is a **crash sweep**, not a single verdict at one bound.
- **The sweep:** search at crash bound 0 (omissions only), then 1, 2, …. When no admissible set exists at bound k,
  the label tolerates k crashes, and the search raises the bound to k+1.
- **Clauses carry over.** A derivation is a set of sends and executions seen in a run, and whether a fault set
  kills it doesn't depend on the bound. The no-goods for tried sets and the no-superset rule for vacuous sets
  stay valid too. Raising the bound only admits more sets, so each step resumes from everything already learned
  instead of starting a new search.
- **The top of the sweep** is the smallest of:
  - never crash every node (Σ ≤ N−1);
  - never crash all, or all but one, of the checked cluster (Σ ≤ |C|−2), since a single survivor agrees with
    itself (both from `1538508755`);
  - the `.max_crashes(n)` cap, when given (the only override).
- **The verdict** per label is "tolerates k crashes; breaks at k+1 with ⟨counterexample⟩", or "tolerates every
  admissible number of crashes" (a certificate at the top), or `Budget`. A counterexample found at bound k+1 has
  exactly k+1 crashes, because bound k was exhausted first.
- **What replaces restricted crashable sets.** The research branch answered questions like "does 2PC commit if the
  coordinator stays up?" by making only some nodes crashable (2PC `…with_the_coordinator_correct`: two
  certificates, two vacuous). Under the sweep, the same question is answered by the bound at which 2PC's labels
  break and the counterexample that breaks them: expected to be one crash, naming the coordinator (to be confirmed
  when 2PC is ported, §7). The sweep gives one witness per label, not a
  characterisation of which nodes must stay up. Enumerating every minimal (k+1)-crash counterexample (a no-good
  per counterexample, then continue) is a natural extension, but not in v1.

### 4.6 Heal (Molly's "end of finite failures", renamed)

**What it is for.** A lossy channel admits the omission of any message. A protocol that recovers by retrying or
re-gossiping sends m, then its retry m′, then m″, and the adversary can omit each one. A finite run has finitely
many retries, so "omit every retry" is a finite fault set, and the hitting-set search finds it. That counterexample
is real, but it says nothing: no retrying protocol survives a network that drops everything forever. Molly's
answer was to bound the adversary to a window of faults, after which the network behaves and the protocol's own
recovery has to deliver. The research branch called the end of that window `end_of_finite_failures` (EFF).

**Convention.**
- Faults are admitted while the `run` closure executes, including any `quiesce().await` inside it.
- When the closure returns, the network **heals**: omissions stop applying, and no crash point fires after heal
  (the research `FaultState::end_finite_failures` semantics). Crashes that already happened persist.
- The check then quiesces (implied, as in §2). For a body that already ends in `quiesce().await` this does
  nothing, since healing creates no new work.
- There is no public knob. Internally the boundary is called `heal`.
- **Consequence for harness authors:** recovery that the harness drives (gossip rounds, 2PC's timeout inputs) runs
  inside the closure, so it runs under faults. A protocol whose recovery only works on a healed network has no
  way to express that in v1. No program in the corpus needs it (below). If one does, the remedy is a second,
  fault-free closure (`run(inputs).then_heal(recovery)`), and it stays out until such a program appears.

**Evidence.**
- **The plan's earlier claim was false.** The original "no EC harness ever called `end_of_finite_failures`" was
  wrong: `gossip_all_labels` called it after the updates and before the gossip rounds (`ec_demos_ldfi.rs:404`), and so
  did ISR's harness (out of scope).
- **Gossip.** Its sibling harness `ec_demo_g_set_gossip_certifies` drives the same rounds without EFF and
  certifies with the same counts (13 hypotheses, 104 schedules, `rec-positional.tsv`), because gossip's channels are
  `fail_stop` and admit no omission. Heal at the end should therefore keep gossip's certificates (`crdt_gossip.rs`
  `:54`, `:84`, `:138`). Confirm this when gossip is ported (§7).
- **Every other harness.** Every other EC harness already ran with EFF at the end of the run (the default), and
  every body ends with a `quiesce().await`. So heal at the end of the closure reproduces what they measured.

### 4.7 Output

- **The report:** one block per cluster, one row per label, with the sweep verdict (§4.5), hypotheses and the
  counterexample in source names.
- **The diagram:** a Lamport diagram for each counterexample, from the failing schedule's trace (always
  kept). The heal point is marked on every lane.
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
  - **Recovery repeats itself.** Heartbeat and retry rounds after a fault are mostly identical (the research
    branch's ISR diagram was 51 lines, mostly identical heartbeat/view rounds). Collapse consecutive identical
    rounds into one with a `×k` note.
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
  7–8× wall; reliable broadcast `:130` at 4.6× hypotheses. (ISR is out of scope for lite. Its rows stay here as
  measurements, not as targets.)
- **Fine at Execution:** simple broadcast, G-Set, 2PC labels, uniform broadcast and `leader_merge` cost the same at both levels.
- **Counterexample quality:** Execution's are less minimal. ISR `:360` gives `{crash(client), crash(zk)}` against
  Precise's `{crash(zk)}`, because the coarse rule blames the client too.

**Decision: lite keeps record provenance as its only lineage**, so `Level` disappears as a concept.
- **Every compile instruments:** `EcCheck` always applies the provenance rewrite (as a post-network E2 pass), so
  enabling provenance stops being a user call. (`90d9` has its own, unrelated `SimFlow::with_provenance`; see §6.2
  for how it is folded.)
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
  - core gains E5 (§6.2): sends carry an opaque tag on the simulator's wire.
  - What stays out: replay, the hook policies, `erase_guarded` and `map_tick_closure`.
  - The one known opaque operator shape, keyed `entries_partially_ordered` (`leader_merge`), is detected and
    marked opaque (§4.2).

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
| F3 | sim gaps: top-level `Reduce` (replaces `todo!`), `AtLeastOnce → ExactlyOnce` retry cast, clearer `todo!` messages | simulator coverage for ordinary programs. The research branch's other two F3 items, the unbounded top-level `Optional` snapshot and `collapse_ticks_around_atomic`, depend on upstream #3146/#3162/#3155, which `90d9` lacks, so they are dropped (on `90d9` every tick has a `ClockId`, so the second bug may not exist) |
| F4 | `CompiledSim::first_schedule`, and `exhaustive_then` / `fuzz_bounded_then` with a callback between executions, all public | a deterministic single run for debugging; bounded memory for long explorations (the research branch measured 10 GB → 241 MB from folding results between executions). `first_schedule` is hand-ported, since the research version sits on `deterministic()`, which `90d9` lacks |
| F5 | `ClusterIds: Copy`, `Counter::next_count` | trivial, additive |

### 6.2 Extension points lite needs (one PR, all behind `hydrofrac`)

| id | extension point | size | justification |
|---|---|---|---|
| E1 | **Network hook:** the simulator calls an installed `dyn SimNetworkHook` on every `Network` send (it may drop the send, or deliver it with a `u64` tag), on every receive, at execution begin, at each tick boundary with **release attribution** (`on_release`: which received deliveries the tick consumed, needed by the execution rule, §4.2), and on external outputs (it may drop output from a dead node). It is `None` by default. | ~300 (builder.rs, graph.rs, compiled.rs, runtime.rs) | the single seam for any fault injection or network tracing in the simulator; zero cost when absent; replaces the research branch's hard-wired `record_sent`/`LineageOutputSender` and its `Released` events from hook policies |
| E2 | **IR pass hook** on `SimFlow`: `with_ir_pass(stage, f)` with two stages that run in a declared order, plus `HydroRoot::input_mut`. `PreNetwork` runs before network compilation (observation taps add `Network` nodes); `PostNetwork` runs after it (the provenance rewrite has to see the compiled sends). The original plan's single pre-network stage was wrong for the rewrite (research `flow.rs:200` vs `:230`) | ~60 | lets external crates add sim-only instrumentation without core knowing about it; also useful for test oracles. Precedent on `90d9`'s line: `extension.rs::ir_mut` @ `4de7c45e41` |
| E3 | **Eager locations:** `SimFlow::with_eager_location(&Process)`, a location whose ticks run outside the schedule choice and that is excluded from the process count | ~40 | an observation-only location must not multiply schedules (64× more schedules on broadcast when the observer was explored). Replaces the research branch's match on `type_name::<Observer>()` (`built.rs:174-190`) |
| E4 | **Read access to channel metadata:** a channel table recorded by `SimBuilder::create_network` (id, name, endpoints, transport kind: lossy or `fail_stop`) and exposed on `CompiledSim`; read accessors for location names, cluster members and node count (these back the crash bounds); `(de)serialize_bincode_with_type` exposed `pub(crate)` → `#[doc(hidden)] pub` | ~40 | the channel table and the tap serialization live in the hydrofrac crate rather than in core. Recording at `create_network` replaces the research `slice::compute`, which mirrors the emitter's statement counter |
| E5 | **Send metadata on the simulator's wire:** with a hook installed, the wire is `(u64, Bytes)`, where the `u64` is the tag E1's `on_send` returned. A `PostNetwork` pass may make a `Network` or `SendExternal` input `(T, Meta)`; core peels the opaque `Meta` before serialising and hands it to `on_send`, and re-attaches the `u64` after deserialising (`(u64, T)`) for the pass's own map | ~100 (estimate) | record provenance has to cross the network; an opaque tag and an opaque side value keep provenance itself out of core |

Total about 540 lines in core, against about 1,680 in the research branch. Phase 2 (schedule
reduction) would add doc-hidden read access to the nondeterministic-operator markers for the order check.

**Folding `90d9`'s own sim work.** `90d9` already has two mechanisms that overlap E1 and E5. Lite re-expresses them
on the extension points instead of adding parallel paths:
- **Crash injection**
  - What `90d9` has: `with_crashable_process` / `with_crashable_cluster` (`flow.rs:211`), `CrashHook`
    (`runtime.rs:1768`), `crash_channels_ident` (`builder.rs:146`, used in every `create_network` arm).
  - What lite does: it becomes an in-tree `SimNetworkHook` implementation (E1), keeping its user-facing API and
    tests.
- **Forward provenance**
  - What `90d9` has: `SimFlow::with_provenance` (`flow.rs:96`), `sim/provenance.rs` + `provenance_ir.rs`, tags in a
    network "side frame".
  - What lite does: its side frame becomes E5's tag and side value, and its IR rewrite a `PostNetwork` E2 pass. The
    `with_provenance` name stays with this existing feature; lite's rewrite is internal to `EcCheck`.

Before each fold, the users of each mechanism are inventoried (`feedback_campaign.rs`, `hydro_test`,
`consensus_gauntlet`) and their tests must keep passing. If folding one of them costs more than it saves, it stays
as is and that is recorded here.

### 6.3 Research-branch core changes lite deliberately avoids

| change | why it isn't needed |
|---|---|
| `create_network(.., input_element_type)` | breaks the public `DfirBuilder` trait only to print payloads; lite renders payloads from the observer side and abbreviates them |
| `broadcast_closed` rewrite (`flat_map_ordered` over `ClusterIds`) | changes codegen for every cluster→cluster broadcast only to hide the member-id list from the observation pass. In lite that list is checked like any other labelled collection. If such rows become noise, a suppression list in lite's own observation pass is the place to filter them, with no core change |
| `pub mod spec_views` (ungated) | moves into the hydrofrac crate as staged code |
| `Dfir::map_tick_closure` / `erase_guarded` | guards counterfactual replays that panic; lite doesn't replay. A program panicking under injected faults is reported as a counterexample |
| standing hook policies (`scripted.rs`, `tick_input.rs`, `observation.rs`), `BatchHook::{deliver_all, replay, omit, ..}`, `flow.sim_hook()`, `NonDet::hooked` | replay and traced pass-through only. The `BatchHook` method additions have no callers; `BatchHook` itself and `flow.sim_hook()` are research-only and are used by 2PC, ISR and the research `Spec::accumulate`. The execution rule is sound without them, and release attribution comes from E1's `on_release`. Those users are rewritten on plain `main` features or dropped (§7) |
| the research branch's `with_provenance` as a core `SimFlow` API, and its provenance branches in `builder.rs` | the rewrite enters through E2 and its tags through E5's opaque `u64`, so core knows nothing about lite's provenance. (`90d9`'s own `with_provenance` is a different, existing feature, folded onto E2/E5 in §6.2) |
| fine-grained sink slices in `slice.rs` | leftovers of the removed `Level::Slice` (about 300 lines unused) |
| `location_names`, `keyed_singleton.rs` formatting | cosmetic |

## 7. Validation

- **The EC corpus.** Port these harnesses to the lite API:
  - `hydro_test/src/cluster/ec_ldfi.rs` (6 tests);
  - `hydro_test/src/cluster/ec_all_labels_classes.rs` (1 test);
  - `hydro_test/src/cluster/ec_demos_ldfi.rs`, minus its two phase-2 tests (`order_insensitivity_of_the_corpus`
    and `reliable_broadcast_certifies_in_one_schedule_per_hypothesis`): 9 tests;
  - ABD's `hydrofrac_tests` (2 tests). These need two research-branch demo deltas brought to `main` first:
    `abd_register_reliable_phase2` and `reliable_broadcast_closed_from_cluster`.
- **2PC.** Rewrite `two_pc_ldfi.rs`'s program and harness on plain `main` features, with no `BatchHook` or
  `flow.sim_hook()`, every node crashable, and its EC labels checked by `EcCheck`. Its hand-written-spec and
  strategy-comparison tests are not ported.
- **ISR is out of scope.** It is not ported, and its unclean-election bug is not an acceptance item: it was only
  ever found through hand-written PRE/POST specs, which lite drops.
- **Acceptance: the same verdict per label as the research branch's positional records**
  (`docs/design/hydrofrac-lite-data/rec-positional.tsv`).
  - **A record row** is one line of that file: one search the research branch ran. It has 13 tab-separated columns:
    test, label, crash model, exploration, verdict, normalized counterexample (`{omit(..) | crash(<node>)}`), raw
    counterexample, hypotheses, schedules, wall time, peak RSS, exploration note, novel sends.
  - **Which rows count:** the rows of the ported files, plus 2PC's `…with_the_harness_fault_model`. Excluded: ISR, and
    every row from a restricted crashable set (2PC `…with_the_coordinator_correct`, ISR
    `…with_the_harness_fault_model`), along with strategy comparisons and other non-EC rows.
  - **Keying:** by (file, collection name, occurrence), falling back to the operator when a collection has no name.
    The `file:line` labels in the records don't match `main`, because the demo sources differ (for example, `abd.rs`
    has about 1.1k lines of diff). Single-spec rows (label `-`) map to the label of the stream they observed.
  - **Matching the sweep (§4.5) to a record:**
    - a research certificate (crashes bounded only by vacuity) must be "tolerates every admissible number of
      crashes";
    - a research counterexample must be "breaks at k+1", with k+1 no greater than the record's crash count;
    - the counterexample sets are expected to match, and any difference is explained in the comparison output (the
      sweep finds the fewest crashes first, the research search the fewest faults);
    - multi-Paxos compares at `.max_crashes(1)`, its research `crash_budget(1)`.
  - The ported tests write the same 13 columns from their results, test-side, with no env knob in the API.
    `compare.py` is reused with the new key function.
- **Heal check:** gossip's three certificates (`crdt_gossip.rs` `:54`, `:84`, `:138`) must survive heal at the
  end of the run closure (§4.6).
- **Opaque check:** `leader_merge`'s keyed `entries_partially_ordered` is reported opaque, not miscompiled, and
  its verdict matches its record (§4.2).
- **Honesty check, test-only:** a brute-force enumerator over every admissible fault set up to size 2, run on the small EC
  demos. Agreement with lite's verdicts tests the lineage pruning without a `Strategy::Blind` in the API.
- **Solver check:** on every ported search, the `microlp` and HiGHS backends return sets of the same size,
  checked once in CI against HiGHS behind its opt-in feature.
- **Performance budget:** total EC-corpus search time no worse than the positional records' EC rows. The sweep tries
  the low-bound hypotheses first, and those are admissible at higher bounds too. So it should cost about the same
  as one search at the top bound, but this is measured, not assumed.

## 8. Plan of work

1. F1, F2, F3 (minus the two upstream-dependent items), F4 and F5 as independent PRs on `90d9`, each with a new
   test (the research branch has no core tests for F3/F4).
2. E1–E5 in one core PR, behind `hydrofrac`, with unit tests that don't use hydrofrac. Each point gets a
   small in-tree test hook, for example a test interceptor that drops every message on one channel. The same PR
   folds `90d9`'s crash injection and forward provenance onto E1/E5/E2 (§6.2). In parallel with it: the
   `hydrofrac` crate skeleton and `search` (the ILP with the `0.5/(n·n)` tie-break, and the crash sweep as pure
   logic), which need no simulator. Benchmark `microlp` against HiGHS here, early.
3. The rest of the `hydrofrac` crate, in dependency order:
   - `fault` (on E1/E4) and `provenance` (on E2/E5, with the `leader_merge` opaque detection);
   - then `lineage`;
   - then `observe`, then `spec` (eager observer, plain `batch`);
   - then the `EcCheck` loop with heal and the sweep;
   - then `report` and `lamport`.
4. The ported corpus, in order: `ec_ldfi.rs`, `ec_all_labels_classes.rs`, `ec_demos_ldfi.rs`, ABD (after its demo
   deltas), then 2PC rewritten without hooks. Compare each with the records as it lands, then run the brute-force
   cross-check. First benchmarks: "provenance without replay" against run P (count of opaque fallbacks, hypotheses,
   wall), and the sweep's cost against the records.
5. Phase 2, only when a program's schedule space demands it: open sent sets + the order check + one
   schedule per hypothesis, with the never-seen-send tripwire as a hard failure in reduced searches.

## 9. Decisions

Settled:
1. **Crash model:** positional only; sent sets and schedule reduction are phase 2 (§3).
2. **Lamport backend:** DOT only; Mermaid rejected because it can't draw messages in flight across a fault (§4.7).
3. **`trust_order_insensitive(site)`:** not in v1 (moot until phase 2).
4. **Crate placement:** a separate `hydrofrac` crate; core only gets §6.2.
5. **`broadcast_closed`:** not changed; any later noise is filtered in lite's observation pass.
6. **Solver:** the `good_lp` ILP on `microlp`, pending a benchmark against HiGHS.
7. **Lineage:** record provenance only, with the execution rule as the opaque-record fallback and no replay
   (§5, run C). Keyed `entries_partially_ordered` is detected and treated as opaque (§4.2).
8. **Base:** `origin/main` @ `90d9d26736` as is, with no rebase onto upstream. `90d9`'s crash injection and forward
   provenance are folded onto the extension points (§6.2).
9. **Scope:** ISR is out. 2PC stays, rewritten on plain `main` features. Nothing exotic: no research-only hooks (§1).
10. **Heal:** EFF is renamed and becomes a convention. Faults are admitted while the run closure executes; the
    network heals when it returns; crashes persist; no public knob (§4.6).
11. **Crash sweep:** the default output. Every node is crashable, with no restricted crashable sets, and
    `.max_crashes(n)` caps the sweep (§4.5).
12. **Verdict comparison:** keyed by (file, collection name, occurrence), with an operator fallback. ISR and
    restricted-crash rows are excluded (§7).

Open:
- **Replay:** stays out unless the "provenance without replay" benchmark shows the opaque fallback costing
  much on the EC corpus.
- **A fault-free recovery phase** (`then_heal(recovery)`): only if a program's recovery works only on a healed
  network (§4.6).
- **All minimal counterexamples at the breaking bound:** an extension of the sweep, if one witness per label
  proves too little (§4.5).

Open:
- **Replay:** stays out unless the "provenance without replay" benchmark shows the opaque fallback costing
  much on the EC corpus.
