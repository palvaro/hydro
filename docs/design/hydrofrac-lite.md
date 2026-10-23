# hydrofrac-lite: design plan

Status: implemented on branch `hydrofrac-lite`; this document is the plan revised to match what was built. A fresh
branch that shares no files with the research line
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
let mut check = hydrofrac::EcCheck::new();
let sim = check                                          // taps every label; the host checks them
    .instrument(flow.sim().with_cluster_size(&members, 3))
    .compiled();
check.run(&sim, async || {                               // faults may strike while this drives inputs
    send.send(Item(1));                                  // and while the run settles; then the network
    send.send(Item(2));                                  // heals and the run quiesces again
})
.assert_holds();                                         // panics with the report; writes diagrams
```

The check instruments the `SimFlow` rather than the `FlowBuilder`: everything it adds (the taps, hook points,
provenance) is simulator-only and is registered on the `SimFlow`, so the program itself is untouched.

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
- **heal at the end of the run** (§4.6): faults are admitted while the closure drives inputs and while the work it
  caused settles; then the network heals (no more omissions or new crashes; crashes that happened persist) and the
  run quiesces again;
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
| Static order check (`order.rs`): its cone walk | **in**, for certification only | ported as the conservative schedule-sensitivity walk that decides which labels can be certified at all (§4.9) |
| One schedule per hypothesis, with open sent sets (schedule reduction) | **phase 2, out of v1** | All 125 B verdicts agree with A, and no reduced search saw a never-seen send; but A's certificates on schedule-sensitive labels are not ground truth (§4.9), so that agreement says less than it seemed to. It's the remedy if a program's schedule space explodes (uniform broadcast's probe: >359k schedules), so it's documented and measured but not built until a real program needs it |
| Generated EC spec: fixed fact domain from the failure-free probe, POST as "every correct member holds every domain fact some correct member holds" | **in**, evaluated **on the host** from per-member tap outputs: no observer process, no staged views, no `BatchHook` | the view is monotone by construction; this is what makes the inference sound (retrospective, wall 2; `spec_views.rs:63-95`). Why the host's view is as sound as the observer's: §4.7 |
| Scalar labels checked at their input stream (`covered_by`) | **in** | already implemented in the research branch (`observe.rs:144-170`) |
| Vacuity: empty fact domain is `Vacuous`; empty derivation is a vacuous run; crash-all and crash-all-but-one-of-the-checked-cluster are excluded | **in** | settled (`1538508755`; ablation doc on crash bounds) |
| Heal at the end of the run (Molly's EFF, renamed) | **in**, as a convention | without it every retrying protocol has a meaningless omit-every-retry counterexample (§4.6) |
| Crash sweep: bound 0, 1, 2, …, keeping learned clauses | **in**, as the default output | derivations don't depend on the bound, so raising it is cheap. It replaces restricted crashable sets and `crash_budget` (§4.5) |
| Restricted crashable sets (`.crashable(..)` on a subset) | **out** | every node is crashable; the sweep reports what a restriction used to assume (§4.5) |
| PRE, preserve points, `required` | **out** | the generated EC spec has no PRE; `required` is always empty on the EC path |
| Certificates only for site-free labels; a schedule-sensitive label's search is a heuristic ending `Exhausted` | **in** | Molly's pruning assumes one execution per fault set, which a `nondet!` site breaks: the 2PC-with-CTP false certificate (§4.9) |
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

A new crate, `hydrofrac`, depending on `hydro_lang` with its `hydrofrac` feature (which implies `sim`; §6). It is set up for staged
code (`q!`) the same way `hydro_std` is. The generated-spec views (`spec_views.rs`, an ungated public module of
`hydro_lang` on the research branch) are not carried over: the specification is evaluated on the host (§4.7).
All hydrofrac logic lives in this crate; `hydro_lang` only gains the extension points in §6.

Modules, with the planned size and the size as built (lines of non-test source files, doc comments included):

| module | content | planned | built |
|---|---|---|---|
| `observe` | label enumeration (`ec_collections`), `LabelPlan` / `NotChecked` / `covered_by`, the tap pass, source names | 600 | 1,945 |
| `spec` | the generated POST, evaluated on the host over the taps' observations (§4.7) | 350 | 296 |
| `fault` | `FaultHook` (a `SimNetworkHook`): the per-send filter, positional crash points, heal, the event log | 250 | 573 |
| `lineage` | derivations from the event log: record provenance, with the execution rule as the fallback for opaque records | 700 | 459 |
| `provenance` | the record-provenance IR rewrite (via E2) and its runtime tags; the schedule-sensitivity walk (§4.9) | 1600 | 2,287 |
| `search` | crash bounds, vacuity, the hitting-set ILP, the crash sweep | 650 | 1,167 |
| `check` | `EcCheck`: the loop, heal, verdicts and results, the enumeration (§4.9) and the test-only brute-force cross-check | (in `search`) | 2,588 |
| `report` | per-label report, names | 600 | 275 |
| `lamport` | trace extraction + the DOT backend | 450 | 1,407 |
| **total** | | **≈5.1k** | **≈11.0k**, against ≈12.7k of logic in the research branch |

The overrun is mostly in what the plan didn't foresee: the schedule-sensitivity walk and the enumeration (§4.9), the
source-name and fact rendering for readable reports, and the DOT layout carried over whole. Tests are another ≈5.3k.

### 4.2 Lineage

The event log records executions, sends with their fate and a delivery id, receives, and which
execution produced each observation. Every record carries the delivery ids it was computed from: the
provenance rewrite tags records as they flow through operators, and sends carry the tags across the
network. The support of an observation is the deliveries its record's tags name, closed backwards
through the sends that carried them.

Where the rewrite can't see into a record (an operator it leaves as an island, such as `ResolveFutures*` or
a multi-version network, or a receive with no tag), the record is opaque and its support falls back to the
**execution rule**: a record produced in execution X at node n depends on every delivery n received before X
ended, in program order.

The execution rule is sound for every program; it only costs precision. The research branch's
`record_closure` and `closure` (`ldfi.rs:3370`, `:3966`) are these two rules.
- **No release attribution needed:** on the research branch, a tick's arrivals came from the hook policies'
  `Released` events, which lite drops. The plan was an `on_release` callback in E1; it was dropped as too
  invasive (it would thread item identity through every hook buffer). E1's callbacks arrive in strict program
  order instead (receives interleaved with execution begin and end), so "every receive at the node before the
  execution ended" is directly computable. It is coarser than "what the tick consumed", and still sound.
- **State read by reference** (`by_ref` / `by_mut` closures) is a sound over-approximation: the record names
  every delivery the state was built from, so it stays `Ids`, not opaque.
- **`leader_merge` is precise, not opaque.** The research rewrite mishandled `entries_partially_ordered` on a keyed
  stream, so `leader_merge` was compiled without provenance there (`ec_demos_ldfi.rs:216-225`). The cause was a
  keyed→flat `ObserveNonDet` whose metadata had the wrong shape. Lite keeps the node's metadata honest
  (`(K, Tagged<V>)`) and converts after it; a test checks that every replica record names the right writer's
  delivery. Every corpus program runs instrumented with 0 opaque records and no islands.

### 4.3 The loop

1. Run the failure-free probe; it learns the fact domain. An empty domain gives `Vacuous`. The probe
   is reused as hypothesis 0, so the failure-free run executes once, not twice as in the research branch.
   - The probe's POST is evaluated against the domain it has just learned, so a program whose members disagree
     with no fault at all is a counterexample at hypothesis 0. Its clauses come from the same tap observations as
     every other run's (§4.7).
2. For each label, starting at crash bound 0: propose the minimal admissible fault set that kills every
   derivation seen so far. Run it over all schedules, or over `sample_schedules(n)` of them.
3. If a correct member lacks a fact, report a counterexample. Otherwise the run's derivations become new
   clauses.
4. When no admissible set exists at bound k, the label tolerates k crashes. Raise the bound to k+1, keep every
   clause, and continue at step 2 (§4.5).
5. Stop at a counterexample, at the top of the sweep (a certificate for the largest bound reached), or when the
   hypothesis budget runs out (`Budget`).
6. For a schedule-sensitive label (§4.9) steps 2 and 4 use the heuristic instead (a hypothesis need only kill one
   schedule's derivations per earlier run), and the top of the sweep is `Exhausted`, not a certificate.

### 4.4 Hitting sets: the research branch's ILP, on a pure-Rust solver

The EC path has no PRE, so the model is the research branch's `solve_hypothesis` without its PRE terms:
- one binary per candidate fault, minimising count;
- cover constraints, each conditional on its crash premises;
- at most one crash per node and the crash bounds;
- no superset of a vacuous set, and an exact no-good for each tried set.
- for a schedule-sensitive label (§4.9), disjunctive groups: one group per earlier run, satisfied when *some*
  clause of it is (a binary `y_c ≤ hits(c) + missing premises(c)` per clause, and `Σ y_c ≥ 1`). A group of one
  clause is the ordinary cover row. `hitting_set_any` / `Sweep::next_any`.

Writing it as an ILP keeps it short; the model is about 150 lines.
- **Solver:** `good_lp` with the `microlp` backend (pure Rust, branch-and-bound) instead of HiGHS:
  `good_lp = { version = "1.15", default-features = false, features = ["microlp"] }` and
  `.using(good_lp::microlp)`. The research code uses no HiGHS-specific API.
- **Optimality:** with a zero MIP gap and no time limit, a solution is returned only when proven optimal, so
  certificates stay certificates.
- **Why not HiGHS:** HiGHS is the reason `cargo test` on `hydro_std`/`hydro_test` currently needs cmake ≥ 3.15,
  libclang and a C++ toolchain, and it is used nowhere else.
- **Speed, measured:** `microlp` has no presolve or cuts, and dense no-good rows are its worst case. The model
  therefore writes only rows that can cut something off (`ilp::Model::Lean`): a tried set gets an explicit
  no-good only when no cover or vacuous row already excludes it, which in a learning search is almost never.
  On a synthetic learning search (6 nodes × 20 sends, cap 2) that is 6 / 16 / 25 ms per hypothesis at
  100 / 200 / 300 hypotheses, against 15 / 29 / 84 ms with every row; 80 random instances check that both
  models agree on feasibility and optimal cost. `opt-level = 3` is set for `microlp` in dev builds.
- **HiGHS:** available behind the opt-in `highs` feature of `hydrofrac`, with a cross-check and a benchmark
  written but `#[ignore]`d. Neither has been run: HiGHS needs cmake ≥ 3.15 and libclang, which the
  development machine lacks.
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
  admissible number of crashes" (a certificate at the top), or `Budget`. For a schedule-sensitive label (§4.9) the
  same sweep only searches: "no counterexample with up to k crashes found, breaks at k+1", or `Exhausted` at the
  top. A counterexample found at bound k+1 has
  exactly k+1 crashes, because bound k was exhausted first.
- **Exhausted over every fault set, not only the proposed ones.** The solver's universe holds each delivery's
  *weakest* killers: its omission, and its sender's crash right after the send before it. That is enough to find
  minimal sets, but not to conclude that none exists. A run seen later can name an earlier crash point of a node,
  one that kills deliveries of several older clauses that no single weakest candidate kills together (two runs in
  which `n` sends `a, b` and `b, a`, each deriving the fact from its second send: the crash after the first send
  spares the other run's delivery, `crash(n)` kills both). With one crash per node, the only hitting set may then
  go through that point, and the sweep would certify a bound it should have broken. So before concluding at a
  bound, the sweep asks once more over a **completed universe** (`search::completion`):
  - `crash(n)` for every sender `n` of a delivery. It kills every delivery of `n`, so it hits every clause any
    crash of `n` hits through a delivery;
  - where `crash(n)` is itself a clause's premise or in a vacuous set, replacing a crash by it is not neutral, so
    there every crash point of `n` that kills some delivery is added instead;
  - every fault in a premise or a vacuous set.

  Then any fault set `H` that hits every clause maps to one `H*` in the completed universe that does too, with no
  more crashes: drop the faults that kill nothing (an omission that kills a delivery names it, and is already a
  candidate), and replace each remaining crash of `n` outside the universe by `crash(n)`. `H*` hits a superset of
  what `H` hits (premises only go missing, never appear, since `crash(n)` is no premise), crashes a subset of
  `H`'s nodes, and is a superset of a vacuous set only if `H` is. So "no admissible set at bound k" holds of every
  positional fault set, which makes the certificate sound and the "exactly k+1" claim above true. The completion is
  asked only when the weakest candidates admit nothing, so it costs a hypothesis only where it finds one.
  `certificates_hold_over_every_positional_fault_set` checks this on random runs with reordered sends; without the
  completion it finds false certificates.
- **What replaces restricted crashable sets.** The research branch answered questions like "does 2PC commit if the
  coordinator stays up?" by making only some nodes crashable (2PC `…with_the_coordinator_correct`: two
  certificates, two vacuous). Under the sweep, the same question is answered by the bound at which 2PC's labels
  break and the counterexample that breaks them. Measured (§7): plain 2PC's `committed`, `prepares` and `commits`
  tolerate 0 crashes and break at 1 with the coordinator's crash; with cooperative termination, `prepares` and
  `commits` do the same and `committed` finds no counterexample with 2 participants, and breaks at 2 crashes with
  3. The sweep gives one witness per label, not a
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
- Faults are admitted while the `run` closure executes, including any `quiesce().await` inside it, and while
  the work it caused settles: when the closure returns, the check first quiesces under faults. (The simulator
  runs the program only while the closure is blocked, so a closure that sends its inputs and returns has
  caused no sends yet; healing at that point would admit no fault at all.)
- Then the network **heals**: omissions stop applying, and no crash point fires after heal
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
  `:54`, `:84`, `:138`). Confirmed for `all_elements` and the G-Set when gossip was ported; the `echo` hop now
  breaks at 1 crash, since a member may crash mid-round (§7).
- **Every other harness.** Every other EC harness already ran with EFF at the end of the run (the default), and
  every body ends with a `quiesce().await`. So heal at the end of the closure reproduces what they measured.

### 4.7 The specification is checked on the host

The research branch evaluated the generated specification inside the program: taps sent every labelled element
over a `fail_stop` channel to an **observer** process, and a staged view there (`spec_views::eventual_consistency`)
computed `POST`. Lite computes the same `POST` on the host, from the taps' outputs:
- **Taps:** a `PreNetwork` pass tees each checked collection, at every member, into an external output of its own
  (the IR `sim_cluster_output` builds). Each element arrives through `SimNetworkHook::on_output` as
  `(identity, Debug)`, with the provenance rewrite's meta. The identity is the element's bincode if its type is
  `Serialize`, else its `Debug` string, else a `Hash` digest (chosen by autoref specialization; `hydro_std`'s
  `Durable<T>` is deliberately not `Serialize`). A `Debug` identity is only as good as `Debug` is injective.
- **The view:** for each member, the union of the facts its taps emitted (`spec::MemberView`).
- **`POST`:** every correct member holds every domain fact that some correct member holds (`spec::evaluate`). The
  domain is what correct members held in the failure-free probe (`spec::learn_domain`). This is the research
  formula, unchanged.

No observer process, no staged views, no `BatchHook`, and no eager location: an external output adds no schedule
choice. E3 stays in core as a generic extension point, but lite does not use it.

**Why this is as sound as the observer.** What makes the hitting-set inference sound is that a run's derivation of
`POST` implies `POST`: a fault set that leaves every delivery of the derivation intact can't break it. The research
branch got that from two facts: the domain is fixed, and the view only grows. With the domain and the correct
members fixed, the derivation of `POST` is the deliveries behind every required `(member, fact)` pair, and a run
that recomputes all of them satisfies `POST` again. Both facts carry over to the host unchanged, so what has to
be checked is that the host sees the same `(member, fact)` pairs the observer would have:

| concern | observer (research) | host (lite) |
|---|---|---|
| **The view only grows** | `Spec::accumulate`: facts so far, accumulated across the observer's ticks | a union of tap outputs, never retracted. A tap emits an element once it exists, and a stream's elements are never withdrawn |
| **Facts in flight** | taps crossed a network channel the fault filter bypassed; quiescence delivered them before `POST` was read | taps are synchronous outputs of the execution that derived the element, so nothing is ever in flight between a member and the host. Network messages in flight at heal are still delivered: heal stops omissions and new crashes, and the check quiesces before reading the view |
| **Facts from a member that crashes mid-run** | the member's later sends stop; its earlier taps arrive | the halted member emits nothing more; what it emitted before it crashed stays in the view, but the member is not correct, so it owes nothing, and `required` ranges over facts held by *correct* members, so its facts make nothing required. A domain fact only crashed members held is required of nobody, as in Molly: the correct members agree that nobody has it |
| **The domain** | fed from the probe | the same: the facts correct members held in the probe, where every member is correct. Facts a faulty run produces outside the domain are reported (`outside_domain`), not required, which keeps the view a fixed function of its derivation (research `Spec::eventual_consistency`, "Why the domain is fixed") |
| **Who is correct** | fed before the run from the hypothesis' planned crashes | decided **at the end of the run**: the members not halted. A crash point that never fires (its node never sends the message it names) leaves the member correct, and so obliged. This is stricter than the planned set, never weaker: excluding a member that never crashed could hide its disagreement. The run's clauses are conditional on the crashes that did fire (§4.4) |

**What a fact is, and which labels are not checked.** A fact is an element of a labelled `Stream`, or an entry
`(key, value)` of a labelled `KeyedStream`. Both only grow, so a union of what was emitted is the collection's
contents. A `Singleton`, `Optional` or `KeyedSingleton` changes value, and a union of its versions is not its
value: member A going `1 → 2` and member B going `2 → 1` have the same versions but disagree at the end. A union
view would certify that run. So a scalar label is **not checked** (`NotCheckedReason::NotAStream`) unless it is
**covered**: it is a `fold` / `reduce` (or keyed form) of a checked labelled stream, so the type system vouches
that it is a deterministic function of that stream's contents (`commutative = ..` on an unordered input). Then the
stream is checked instead. The test `an_overwritten_singleton_is_not_tapped` builds the `1 → 2` case under a
deliberately wrong `manual_proof!` and fails if such a label is ever tapped or classified as checked.

**A limitation inherited, not introduced.** The specification is set-based: it checks that members agree on a
labelled stream's *contents*, not on its *order*. A `TotalOrder` labelled stream whose members see the same
elements in different orders passes, and so does a covered `fold` over it. The research observer had the same
limit. Order is the subject of the phase-2 order check (§3).

### 4.8 Output

- **The report:** one block per cluster, one row per label, with the sweep verdict (§4.5), hypotheses and the
  counterexample in source names.
- **The diagram:** a Lamport diagram for each counterexample, from the failing schedule's trace (always
  kept). The heal point is marked on every lane.
- **Backend: DOT only** (the research branch's Lamport layout, with fixed positions). Always write the `.dot`;
  also write an `.svg` (rendered with `neato -n2`) when Graphviz is on `PATH`, so no new build dependency.
  - **Why not Mermaid:** a `sequenceDiagram` draws every message as one horizontal arrow, so a message sent
    before a crash and delivered after it is drawn as if the dead node sent it (see the broadcast render in
    `docs/design/hydrofrac-lite-img/broadcast_live_sender_crash.png`). In-flight messages across a fault are exactly what counterexamples are about,
    so Mermaid misleads where it matters. A second "preview" backend is also against the one-convention rule.
  - Real samples of the research emitter's Mermaid output are in `docs/design/hydrofrac-lite-data/level/mermaid/`.
- **Legibility fixes**, from the research branch's output, apply to the DOT backend:
  - **Facts render as raw bincode** (`⇒ 01000000`). Each tap ships `(identity, Debug)`: its closure is generated
    for the concrete element type and picks the `Debug` rendering by autoref specialization, so reports print facts
    as the program's values (staged module paths shortened, `MemberId::<Client>(0)` as `Client@0`), and fall back
    to hex for types without `Debug`.
  - **Message payloads render as raw bincode** (`peer[..]: 0100000001000000`). The host sees only a message's
    bytes, so the provenance rewrite renders the payload with `Debug` at the send, where the channel's element
    type is known (the same autoref specialization), and appends it to the send's metadata after the lineage
    (`provenance::split_meta`). Reports and diagrams print `Decided(Txn(1))`; types without `Debug` stay hex.
  - **Recovery repeats itself.** Heartbeat and retry rounds after a fault are mostly identical (the research
    branch's ISR diagram was 51 lines, mostly identical heartbeat/view rounds). Collapse consecutive identical
    rounds into one with a `×k` note.
  - **Wide protocols:** payloads set the lane width. Abbreviate long payloads to a legend.

### 4.9 Schedule sensitivity: which labels can be certified

**The finding.** The search prunes a hypothesis when a derivation seen in an earlier run survives it: the
derivation's deliveries all still happen, so the fact is still derived. Molly's runs are deterministic, so this
is sound there. The simulator's are not. It runs top-level (async) dataflow deterministically, but branches
wherever the program took a `nondet!` guard: a `batch` (which tick an element lands in), an atomic section's
entry, an untrusted `observe_nondet`, `merge_ordered`, a fold over an unordered input, timers. A derivation
through such a site is a derivation *under the schedule that produced it*, and the rule "a hypothesis must kill
every derivation seen in every schedule" then prunes fault sets that break the label in other schedules. Two
ways:
- **U1, existence.** Whether a record exists depends on the choice. 2PC with cooperative termination, three
  participants (`committed`): under `{crash(coordinator after commit→P0), crash(P0 after Decided→P1)}`, P1
  commits, and P2 commits only if its query reaches P1 *after* P1 decided; in the other order P1 has nothing to
  answer, and P2 never commits. The run under the single coordinator crash had a schedule in which P2 learned
  the decision through P1. That derivation survives the two-crash set, so the set was never proposed, and the
  label was certified after 12 hypotheses, while 40% of the set's schedules split the participants (found while
  porting 2PC, confirmed by direct injection and by the brute-force enumeration, §7). `ask_once` in
  `hydro_test/src/cluster/ec_bruteforce.rs` is the three-member miniature: one crash, exhaustively enumerable.
- **U2, send order.** `crash(n after m)` cuts `n`'s sends at `m`, and which sends precede `m` depends on the order
  `n` sent them in. A site anywhere upstream of what `n` receives can reorder `n`'s sends, so the positional kill
  relation computed in one schedule's order is wrong in another, even for a derivation that passes no site.

**Which labels are safe.** A label is **site-free** when neither its causal cone nor the cone of any node that
sends a delivery in it contains a site. Every fault set then has exactly one execution: nothing the label
depends on, and nothing that orders its senders' sends, is chosen. That is Molly's setting, and the strong rule
and its certificates stand. Every other label is **schedule-sensitive**.

**The walk** (`hydrofrac::provenance::sensitivity`, run by the tap pass; its result is `LabelPlan::sites`) follows
the research branch's order check (`order.rs`, `Cones::nondet_upstream`): upstream through operators, networks
back to their senders, cycles back to their sinks, and captured state back to its producer and every closure that
writes it. For U2 it walks, for each location that sends a delivery in the label's cone, the cone of everything
at that location. It is **conservative**: what it cannot see through counts as a site too. Every IR node kind is
classified explicitly (a new one does not compile until it is classified), and these are sites:
- an unknown operator (`Placeholder`);
- a captured reference it cannot resolve to its state cell, or a `for_each` writing through one;
- a cycle whose sink it cannot find, or a labelled node it cannot find again;
- an external input whose order is unspecified (`sim_input::<_, NoOrder, _>`: the simulator delivers it in send
  order, but nothing in the program says a deployment will);
- an arbitrary `source_stream`, embedded sources, futures (`resolve_futures*`), async streams and scans, and
  multi-version networks;
- every operator the provenance rewrite left as an opaque island (§5).

**What the check does with it.**
- A site-free label is searched as before; its certificates are certificates.
- A schedule-sensitive label is **never certified**. Its search is a heuristic for finding counterexamples: each
  earlier run is one disjunctive group of the distinct derivations its schedules observed, and a hypothesis need
  only kill one schedule's (rule (a) below; an indicator binary per derivation in the ILP, §4.4). Clauses are
  deduplicated by their kill relation, the derivation plus each sender's send order up to it, so schedules that
  order a sender's sends differently each contribute their own crash points (U2). When the heuristic runs out of
  hypotheses the verdict is **`Exhausted`**: "no counterexample (with up to k crashes) found; not a certificate
  (schedule-sensitive: batch at two_pc_ctp.rs:105, …)". A counterexample's tolerance is likewise phrased "no
  counterexample with up to k crashes found, breaks at k+1" (`Counterexample::tolerance_certified` is false).
- `Results::assert_holds` rejects `Exhausted`, with a message naming the label; `assert_no_counterexample` accepts
  it. `Results::rows` reports it as `exhausted` (the budget is now `budget`).
- Before settling for `Exhausted`, a small enough fault-set space is **enumerated** for a bounded certificate
  (d2, below).

**Bounded certificates by enumeration (d2).** When a sensitive label's heuristic runs out of hypotheses, every
schedule is explored (no `sample_schedules`), and the label's admissible fault sets fit within
`ENUMERATION_LIMIT` (2000 sets, a constant, not a knob), the check runs every one of them over every schedule
(`check/enumerate.rs`). No lineage is involved, so schedule sensitivity does not matter.
- **The universe** is the fault model's, learned from the runs: `omit(m)` for every message sent on a lossy
  channel before the heal; `crash(n)` for every crashable node, and `crash(n after m)` for every message `n` sent
  before the heal. Every run harvests its candidates, so the search's runs seed it.
- **The sets** are every admissible set (the sweep's bounds: one crash per node, Σ ≤ N−1, Σ ≤ |C|−2 in the
  checked cluster, and the cap up to which the sweep searched) of at most `s` faults, for the largest `s` that fits
  the limit; when no admissible set is larger, every admissible set. The enumeration repeats until no set's run
  reveals a new candidate.
- **Why the learned universe suffices.** Take an admissible set `H = H' ∪ {g}` of at most `s` faults with `g`
  outside the final universe. `H'` is admissible and smaller, so it ran over every schedule. Under the same choices,
  `H` and `H'` run identically until `g` first fires, and `g` fires only when its message is sent before the heal,
  which `H'`'s run would then have shown, putting `g` in the universe. So `g` never fires, and `H` runs as `H'`
  does; by induction every set within the bound behaves like one that ran. This needs every schedule, which is why
  sampling disables it.
- **The verdict** is `CertifiedByEnumeration { tolerates, max_faults, sets, sites }`: "holds under any crashes by
  enumeration (4 fault sets over every schedule; schedule-sensitive: batch at …)". `max_faults` is `None` when every
  admissible set was run, else the size bound ("… in every set of at most 3 faults"), and `tolerates` is then the
  crash count covered in full (`s` minus the omission candidates). It is a certificate within those bounds:
  `assert_holds` accepts it, and `rows` reports `enumerated`. If a set breaks the label, the verdict is that
  counterexample instead (fewest crashes, then fewest faults), and its tolerance is certified when every set with
  fewer crashes was covered.
- **Where it applies.** It needs exhaustive exploration, and on the ported corpus every sensitive program is
  sampled (their schedule spaces explode: the echoing broadcast's failure-free run alone passes 240k schedules,
  2PC with cooperative termination 66k under one crash), so no corpus verdict changes. It certifies the batched
  exchange in `hydrofrac`'s tests (4 sets) and is cross-checked against the brute force there. Where the heuristic
  misses a counterexample (the failure sketch of (a) below), a feasible enumeration finds it, since it runs every
  set; no corpus program or small construction has shown such a miss yet.

**The candidates that were weighed:**
- **(a) kill one schedule's derivations per earlier run.** Not sound in general: nothing relates the schedules of
  one fault set to those of another, so a fault can remove a message that made the favourable schedule reachable.
  Good at finding counterexamples, though, and used here only as the search heuristic for sensitive labels. It
  finds the CTP-3 split at hypothesis 13 and `ask_once`'s crash at hypothesis 1.
- **(b) no lineage certificate for a sensitive label.** Sound if the walk is complete, which conservatism buys.
  **Adopted, with (a) as the heuristic.**
- **(c) per-record taint:** records that pass a site carry a flag, and only tainted derivations lose their pruning
  power. Handles U1 only; sound only together with a conservative crash rule for senders whose order a site can
  change (`crash(n after m)` kills every send of `n` but `m` and its causal predecessors). Unmeasured.
- **(d2) bounded enumeration:** enumerate every admissible set up to the sweep's bound, over every schedule, for a
  bounded certificate. About Σ_{j≤k} C(N, j)·(s+1)^j hypotheses for N nodes of s sends each: 2PC with three
  participants at k ≤ 2 is about 770 sets, but each needs every schedule, which 2PC's schedule space rules out;
  multi-Paxos is out on both counts. **Adopted, when it fits** (above).
- **(d3) `trust_order_insensitive(site)`:** a user assertion that a site is harmless. A knob, against the
  convention (§1).

**What it costs.** On the corpus, most top-level outputs are sensitive: gossip (all but `initial`), uniform
broadcast's deliveries, `leader_merge`, multi-Paxos `learned`, ABD, the echoing broadcast and 2PC `committed`.
Their certificates become `Exhausted`; counterexamples are unaffected. The plain broadcasts,
`broadcast_live_from_process` and `hydro_std`'s reliable broadcast (closed and live) stay site-free and keep their
certificates. The static table (`hydro_test/tests/ec_schedule_sensitivity.rs`) counts 49 of 59 corpus labels, in 11
of 14 programs, as schedule-sensitive. Verdict changes on the ported corpus (every other label is unchanged; a
counterexample on a sensitive label keeps its fault set, but its tolerance now reads "no counterexample with up to
k crashes found"):

| program, label | before | after |
|---|---|---|
| 2PC with cooperative termination, 3 participants, `committed` | certified (false) | **breaks at 2**: crash(coordinator after commit→P0), crash(P0 after Decided→P1); hypothesis 13 |
| `ask_once` (the miniature), the known values | certified (false) | **breaks at 1**: crash(member 0 after its announcement to member 1); hypothesis 1 |
| 2PC with cooperative termination, 2 participants, `committed` | certified | `Exhausted` (no counterexample under any crash bound) |
| hydro_test's echoing reliable broadcast, deliveries | certified | `Exhausted` |
| uniform broadcast (thresholds 1, 2), delivered set and quorum certificates | certified | `Exhausted` |
| G-Set gossip (closed, live), `all_elements` / the G-Set | certified | `Exhausted` |
| multi-Paxos `learned`, `facts` (cap 1) | certified up to 1 crash | `Exhausted` up to 1 crash |
| ABD with reliable phase 2, `phase2_fwd`, the register | certified | `Exhausted` |
| `hydro_std` reliable broadcast closed/live `new_messages`, plain broadcasts, `broadcast_live` | certified / counterexample | unchanged (site-free) |

**The research branch has the same hole.** Its `HypothesisFold` adds every schedule's clause independently, which
is the strong rule. Its certificates in `rec-positional.tsv` for labels with a sensitive cone (gossip, uniform
broadcast, ABD, multi-Paxos, 2PC) are therefore **not ground truth**; only its counterexamples, and its
certificates on site-free labels, are. The research branch found the CTP bug only through a hand-written spec,
whose PRE counted a dead node's commit.

**Next.** d2 is in, but the corpus's sensitive programs are too large to explore exhaustively, so the gap it
closes there is the schedule space, not the fault-set space. Next is (c), per-record taint with the conservative
crash rule, measured first by how many first-observation derivations in the sensitive programs are untainted.

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
  records the rewrite can't see into (on the research branch, closures taking `&`/`&mut` state,
  `provenance.rs:520`) and programs compiled without provenance, which lite rules out by construction. Lite's
  rewrite tracks state read by reference as a sound over-approximation instead (§4.2). The execution rule is
  sound, so the fallback costs precision only where records are opaque.
- **Measured: "provenance without replay" costs nothing on this corpus.** Every corpus program runs instrumented
  with 0 opaque records and no islands, so the execution-rule fallback never fires there. Against the research
  records (run P's harnesses), lite needs fewer hypotheses on most ported main labels (reliable broadcast
  49 → 25, uniform 105 → 38–84, gossip 13 → 10, ABD reliable 24 → about 21) and somewhat more on multi-Paxos
  `learned` (26 → about 30, once the completed universe and the sensitive-label heuristic are in). The sampled
  searches vary from run to run.
- **What this costs lite:**
  - the provenance rewrite lives in the `hydrofrac` crate (`provenance/`), entering through E2. Its generated
    code may name only `hydro_lang` and std (a simulation dylib cannot link `hydrofrac`), so it reuses `90d9`'s
    existing `hydro_lang::sim::provenance::{Tagged, Tag, TagSet, apply1}` as its runtime;
  - core gains E5 (§6.2): sends carry an opaque tag on the simulator's wire.
  - What stays out: replay, the hook policies and `erase_guarded`. (`map_tick_closure` came back, doc-hidden,
    for the panic guard, E6.)
  - The one known opaque operator shape, keyed `entries_partially_ordered` (`leader_merge`), is detected and
    marked opaque (§4.2).

## 6. Changes to core Hydro

Two principles:
- **Nothing changes for anyone who does not enable `hydrofrac`.** Today the research branch's send filter,
  event log and provenance modules are gated only by `sim`, so every simulator user pays for an unbounded
  log and cloned send bytes. Lite gates all of it behind a `hydrofrac` feature of `hydro_lang`, and even with
  the feature on, codegen changes only for a flow that opts in (`with_network_hook_points()`). The one exception
  is the panic guard (E6), which every simulator user gets: one handler call per tick poll, and no change in
  behaviour unless the program panics.
- **Extension points, not hydrofrac logic.** Each one is generic, small and testable without hydrofrac.

### 6.1 Independent fixes (land first, separate PRs, valuable on their own)

| PR | change | why it stands alone |
|---|---|---|
| F1 | `hydro_std::quorum::collect_quorum_with_response`: result no longer depends on batching | correctness bug; changes Paxos snapshots |
| F2 | Maelstrom many-to-many: `"port"` in the body, demux by port | wire-format bug for any cluster with several intra-cluster channels |
| F3 | sim gaps: top-level `Reduce` (replaces `todo!`), a snapshot of an unbounded top-level `Optional` (`sim::runtime::OptionalHook`), `AtLeastOnce → ExactlyOnce` retry cast, clearer `todo!` messages | simulator coverage for ordinary programs. The `Optional` snapshot was planned as dropped, but on `90d9` nothing could consume a top-level unbounded `Optional`, so top-level `Reduce` was unusable without it; it was re-derived for `90d9` (about 190 lines, no existing code touched). `collapse_ticks_around_atomic` is dropped: it depends on upstream #3155, which `90d9` lacks, and on `90d9` every tick has a `ClockId`, so the bug may not exist |
| F4 | `CompiledSim::first_schedule`, and `exhaustive_then` / `fuzz_bounded_then` with a callback between executions, all public | a deterministic single run for debugging; bounded memory for long explorations (the research branch measured 10 GB → 241 MB from folding results between executions). `first_schedule` is hand-ported, since the research version sits on `deterministic()`, which `90d9` lacks |
| F5 | `ClusterIds: Copy`, `Counter::next_count` | trivial, additive |

### 6.2 Extension points lite needs (one PR; E1–E5 behind `hydrofrac`, E6 for every simulator user)

| id | extension point | size | justification |
|---|---|---|---|
| E1 | **Network hook:** a flow opts in with `SimFlow::with_network_hook_points()`; without it, codegen is unchanged. `CompiledSim::set_network_hook(Rc<dyn SimNetworkHook>)` installs a hook that is called on every `Network` send (`on_send` returns `None` to drop it, or `Some(tag)` to deliver it carrying the tag), on every receive (`on_recv`), on external outputs (`on_output`, may drop), at execution begin and end (`on_exec_begin` / `on_exec_end`) and at instance start; `take_halts` lets it halt a node, through the same halting path crash injection uses. Callbacks arrive in strict program order, which replaces the planned `on_release` (§4.2) | ~600 (`network_hook.rs`, builder.rs, compiled.rs, graph.rs, runtime.rs), net of `create_network`'s four send arms rewritten into one emitter | the single seam for any fault injection or network tracing in the simulator; replaces the research branch's hard-wired `record_sent`/`LineageOutputSender` and its `Released` events from hook policies |
| E2 | **IR pass hook** on `SimFlow`: `with_ir_pass(stage, f)` with two stages that run in a declared order, plus `HydroRoot::input_mut`. A pass gets `&mut Vec<HydroRoot>`, so it can add roots (each tap is an external output of its own, §4.7). `PreNetwork` runs before network compilation (the taps); `PostNetwork` runs after it (the provenance rewrite has to see the compiled sends). The original plan's single pre-network stage was wrong for the rewrite (research `flow.rs:200` vs `:230`). Doc-hidden accessors the rewrite needs: `ClosureExpr::{expr, singleton_refs, into_parts}`, `handoff_ref_ident` | ~100 | lets external crates add sim-only instrumentation without core knowing about it; also useful for test oracles. Precedent on `90d9`'s line: `extension.rs::ir_mut` @ `4de7c45e41` |
| E3 | **Eager locations:** `SimFlow::with_eager_location(&Process)`, a location whose ticks run outside the schedule choice and that is excluded from the process count | ~40 | built for an observer process, which lite no longer has (§4.7); kept as a generic extension point (an observation-only location must not multiply schedules: 64× more on broadcast when the observer was explored) |
| E4 | **Read access to simulation metadata:** a provided `DfirBuilder::network_metadata(..)` hook, called just before `create_network`, through which `SimBuilder` records the channel table (id, name, endpoints, transport: `FailStop` or `Lossy`, whether intercepted); `CompiledSim::{channels, outputs, locations, node_count}`; `(de)serialize_bincode_with_type` exposed `#[doc(hidden)] pub` | ~80 | the channel table and the tap serialization live in the hydrofrac crate rather than in core. Recording at `create_network` replaces the research `slice::compute`, which mirrors the emitter's statement counter; the provided method keeps `DfirBuilder` non-breaking |
| E5 | **Send metadata on the simulator's wire:** with hook points on, the wire is `(u64, X)`, where the `u64` is the tag `on_send` returned. A `PostNetwork` pass's wrapped serialize calls `sim::network_hook::rt::set_send_meta(bytes)`, and the send that follows hands those bytes to `on_send` (or `on_output`); a wrapped deserialize reads its receive's tag with `rt::recv_tag()`. Serialize and send are fused per item, and a second `set_send_meta` before a send is a debug panic | ~100 | record provenance has to cross the network; an opaque tag and an opaque side value keep provenance itself out of core, and core never sees the pass's types (the planned `(T, Meta)` peeling would have) |
| E6 | **Panic guard** (`sim/panic_guard.rs`, `sim` feature, not only `hydrofrac`): generated code wraps each DFIR tick (through a new doc-hidden `dfir_rs::Dfir::map_tick_closure`) and instantiation in `catch_unwind` inside the simulation dylib and hands the message to the host, which re-raises it with `resume_unwind`; a host hook that panics inside a dylib callback is caught, stashed and re-raised after the tick | ~250 | the dylib has its own std, so its panics are foreign exceptions to the host and aborted the whole process. A program that panics under injected faults has to be a counterexample, not a crash of the test run |

Not intercepted: embedded (non-bincode) channels, used only by `hydro_test`'s `echo_network_embedded` example, and
multi-version networks, which the provenance rewrite leaves as islands.

In total, core gains about 1.7k inserted lines outside tests (about 0.4k removed): E1–E6 about 1.2k, the F fixes
about 0.4k. The plan's estimate of 540 lines for E1–E5 was low, mostly because `create_network`'s send arms had to
be reworked to share one emitter with crash injection. The research branch's core delta was about 1,680. Phase 2
(schedule reduction) would add doc-hidden read access to the nondeterministic-operator markers.

**Folding `90d9`'s own sim work.** `90d9` already has two mechanisms that overlap E1 and E5:
- **Crash injection** (`with_crashable_process` / `with_crashable_cluster`, `CrashHook`, `crash_channels_ident`;
  used by about 10 `hydro_std` `ec_inference_demos` and by `hydro_test`).
  - `CrashHook` is a *nondeterminism* hook: crash timing and the per-recipient prefix are search choices drawn
    from the driver. Re-expressing that as a host-side deterministic interceptor would cost more than it saves, so
    it stays a `SimHook`.
  - What is shared instead: one send-site emitter composes interception (when on) and crash staging, and E1's
    halts go through the same `halt_location`. The `ec_inference_demos` tests, its users, pass unchanged.
- **Forward provenance** (`SimFlow::with_provenance`, `sim/provenance.rs` + `provenance_ir.rs`, tags framed inside
  the bytes): it keeps its public API and now registers its IR rewrite as a `PostNetwork` E2 pass. Lite's
  rewrite is separate and internal to `EcCheck`, but its generated code reuses this module's `Tagged`, `Tag`,
  `TagSet` and `apply1`: simulation dylibs can only name `hydro_lang` and std, never `hydrofrac`, so
  generated code cannot call into the hydrofrac crate.

### 6.3 Research-branch core changes lite deliberately avoids

| change | why it isn't needed |
|---|---|
| `create_network(.., input_element_type)` | breaks the public `DfirBuilder` trait only to print payloads; lite renders facts from the taps (§4.8) and abbreviates payloads |
| `broadcast_closed` rewrite (`flat_map_ordered` over `ClusterIds`) | changes codegen for every cluster→cluster broadcast only to hide the member-id list from the observation pass. Lite's observation pass drops instead every labelled collection whose only sources are cluster membership (a `ClusterMembers` source, or `source_iter` over `ClusterIds`), with no core change |
| `pub mod spec_views` (ungated) | not carried over: the specification is evaluated on the host (§4.7) |
| `erase_guarded`, and `map_tick_closure` for replay | guards counterfactual replays that panic; lite doesn't replay. (A doc-hidden `Dfir::map_tick_closure` does come back, for a different job: the panic guard, E6, which is what lets a program panicking under injected faults be reported as a counterexample) |
| standing hook policies (`scripted.rs`, `tick_input.rs`, `observation.rs`), `BatchHook::{deliver_all, replay, omit, ..}`, `flow.sim_hook()`, `NonDet::hooked` | replay and traced pass-through only. The `BatchHook` method additions have no callers; `BatchHook` itself and `flow.sim_hook()` are research-only and are used by 2PC, ISR and the research `Spec::accumulate`. The execution rule is sound without them, using E1's program-order callbacks (§4.2). Those users are rewritten on plain `main` features or dropped (§7) |
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
  - `Results::rows()` (doc-hidden) gives the same columns from a run, test-side, with no env knob in the API
    (`ReportRow::to_tsv`). The comparison so far was done per ported file, by hand, against the records. Node names
    differ (lite names nodes after their location types, `Source`, `Member@0`, where the records use the research
    harnesses' registered names), so `compare.py` needs a name mapping before it can be reused.
- **Heal check:** gossip's three certificates (`crdt_gossip.rs` `:54`, `:84`, `:138`) must survive heal at the
  end of the run closure (§4.6). Measured: `all_elements` and the G-Set it covers find no counterexample under any
  crash set (now `Exhausted`, since gossip is schedule-sensitive, §4.9); the `echo` hop breaks at 1 crash, because
  a member may now crash mid-round, which the research harness's early EFF ruled out.
- **Provenance check:** `leader_merge`'s keyed `entries_partially_ordered` is tracked precisely, not miscompiled
  (§4.2), and its verdict matches its record. Every corpus program runs instrumented with 0 opaque records.
- **Honesty check, test-only:** a brute-force enumerator over every admissible fault set up to size 2, run on the small EC
  demos. Agreement with lite's verdicts tests the lineage pruning without a `Strategy::Blind` in the API. It includes
  order-sensitive programs (`ask_once`, 2PC with cooperative termination and three participants, plain 2PC,
  `leader_merge`): there `Exhausted` agrees only if the enumeration finds no breaking set within the bound the search
  swept, which tests the heuristic (§4.9). Before the fix, `ask_once` and CTP-3 disagreed (false certificates).
- **Records and schedule sensitivity:** research certificates on schedule-sensitive labels are not ground truth
  (§4.9). Lite reports them `Exhausted`; the comparison treats a research certificate there as "no counterexample
  found" on both sides.
- **Solver check:** on every ported search, the `microlp` and HiGHS backends return sets of the same size,
  checked once in CI against HiGHS behind its opt-in feature. Not yet run: the test exists (`#[ignore]`), but
  HiGHS could not be built on the development machine (§4.4).
- **Performance budget:** total EC-corpus search time no worse than the positional records' EC rows. The sweep tries
  the low-bound hypotheses first, and those are admissible at higher bounds too. So it should cost about the same
  as one search at the top bound, but this is measured, not assumed.

## 8. Plan of work

1. F1, F2, F3 (minus `collapse_ticks_around_atomic`), F4 and F5 as independent PRs on `90d9`, each with a new
   test (the research branch has no core tests for F3/F4; F2 has none, since it needs a Maelstrom run).
2. E1–E5 in one core PR, behind `hydrofrac`, with unit tests that don't use hydrofrac. Each point gets a
   small in-tree test hook, for example a test interceptor that drops every message on one channel. The same PR
   folds `90d9`'s crash injection and forward provenance onto E1/E5/E2 (§6.2). In parallel with it: the
   `hydrofrac` crate skeleton and `search` (the ILP with the `0.5/(n·n)` tie-break, and the crash sweep as pure
   logic), which need no simulator. Benchmark `microlp` against HiGHS here, early.
3. The rest of the `hydrofrac` crate, in dependency order:
   - `fault` (on E1/E4) and `provenance` (on E2/E5; `leader_merge` turned out precise, §4.2);
   - then `lineage`;
   - then `observe`, then `spec` (host-side, §4.7);
   - then the `EcCheck` loop with heal and the sweep;
   - then `report` and `lamport`.
4. The ported corpus, in order: `ec_ldfi.rs`, `ec_all_labels_classes.rs`, `ec_demos_ldfi.rs`, ABD (after its demo
   deltas), then 2PC rewritten without hooks. Compare each with the records as it lands, then run the brute-force
   cross-check. First benchmarks: "provenance without replay" against run P (count of opaque fallbacks, hypotheses,
   wall), and the sweep's cost against the records.
5. Done: the completed universe that makes certificates hold of every positional fault set (§4.5), and bounded
   certificates for small schedule-sensitive programs by enumeration (d2, §4.9), sharing the brute-force
   enumerator's universe. Next: per-record taint with a conservative crash rule (c), measured first.
6. Phase 2, only when a program's schedule space demands it: open sent sets + the order check + one
   schedule per hypothesis, with the never-seen-send tripwire as a hard failure in reduced searches.

## 9. Decisions

Settled:
1. **Crash model:** positional only; sent sets and schedule reduction are phase 2 (§3).
2. **Lamport backend:** DOT only; Mermaid rejected because it can't draw messages in flight across a fault (§4.8).
3. **`trust_order_insensitive(site)`:** not in v1. It would win back certificates on schedule-sensitive labels
   (§4.9), but it is a per-site knob, against convention over configuration.
4. **Crate placement:** a separate `hydrofrac` crate; core only gets §6.2.
5. **`broadcast_closed`:** not changed; any later noise is filtered in lite's observation pass.
6. **Solver:** the `good_lp` ILP on `microlp` (§4.4). The comparison against HiGHS is still to be run, on a
   machine that can build it.
7. **Lineage:** record provenance only, with the execution rule as the opaque-record fallback and no replay
   (§5, run C). Keyed `entries_partially_ordered` turned out to be rewritable precisely (§4.2).
8. **Base:** `origin/main` @ `90d9d26736` as is, with no rebase onto upstream. `90d9`'s forward provenance is
   folded onto E2; its crash injection stays a search-driven `SimHook` that shares E1's send-site emitter and
   halting path (§6.2).
9. **Scope:** ISR is out. 2PC stays, rewritten on plain `main` features. Nothing exotic: no research-only hooks (§1).
10. **Heal:** EFF is renamed and becomes a convention. Faults are admitted while the run closure executes and
    while the work it caused settles; then the network heals; crashes persist; no public knob (§4.6).
11. **Crash sweep:** the default output. Every node is crashable, with no restricted crashable sets, and
    `.max_crashes(n)` caps the sweep (§4.5).
12. **Verdict comparison:** keyed by (file, collection name, occurrence), with an operator fallback. ISR and
    restricted-crash rows are excluded (§7).
13. **Schedule sensitivity:** only site-free labels are certified by lineage. A conservative static walk finds the
    sites (choices, and anything it cannot see through, including send-order effects on senders); a sensitive
    label's search uses the one-schedule-per-run heuristic and ends `Exhausted`, never certified by it (§4.9).
14. **Completed universe:** before concluding at a bound, the sweep asks once more with `crash(n)` for every
    sender (every crash point of `n`, where `crash(n)` is a premise or vacuous), so a certificate covers every
    positional fault set, not only the weakest candidates (§4.5).
15. **Bounded certificates by enumeration:** a sensitive label whose heuristic found nothing is enumerated outright
    when every schedule is explored and its admissible sets fit within 2000 (a constant): every set run means a
    certificate within the enumerated bounds, `CertifiedByEnumeration` (§4.9).

Open:
- **A fault-free recovery phase** (`then_heal(recovery)`): only if a program's recovery works only on a healed
  network (§4.6).
- **All minimal counterexamples at the breaking bound:** an extension of the sweep, if one witness per label
  proves too little (§4.5).
