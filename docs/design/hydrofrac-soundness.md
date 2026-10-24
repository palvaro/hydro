# hydrofrac: the soundness argument

This document states what a hydrofrac certificate claims and argues why it is true. It is written to be read and
attacked without the other design documents: every term is defined here. For each argument it gives the claim, the
assumptions, the proof, where it lives in code, and what tests check it.

**Code reference:** `hydrofrac-races` at `dd5c4d05a5` (first written at `7df7e0753a`; section numbers are unchanged
since). Paths are relative to the repository root; `hydrofrac/src/` is
abbreviated `hf/`. Line numbers are at that commit.

**Status words.** Every step is marked:
- **proved**: argued here, step by step;
- **checked in code**: an assertion or a structural check in the code enforces it;
- **tested**: tests exercise it, including brute-force cross-checks (§15);
- **assumed**: taken as given about Hydro, the simulator or the user's program, and listed in §16.

Two false certificates were found while writing this document. Both were fixed before it was finished, and both now
have regression tests. They are recorded in §17.

## 0. Setting and terms

- **Program.** A Hydro dataflow program compiled for the simulator (`hydro_lang::sim`). It has *nodes*: processes,
  and members of clusters. A *label* is a collection the user marked `EventualConsistency` on a cluster. The check
  observes it through a *tap*: each member's tap emits each record of the labelled stream once, as an *output*.
- **Harness.** Two async closures. The *inputs* (the fault phase) send fixed inputs to the program, and may wait for
  its outputs, but may not place a barrier (§12). The optional *recovery* runs after the heal, and may call
  `quiesce()`. hydrofrac runs `inputs; quiesce(); heal(); quiesce(); recovery; quiesce()` (`drive` in
  `hf/check/engine.rs`, shared by the search and `hf/check/brute_force.rs` `execute`).
- **Schedule.** One complete execution of the simulator for a fixed fault set. The simulator branches only at
  *hooks*: batch releases, snapshot versions, order choices and the tick to run next. `exhaustive` enumerates every
  sequence of hook decisions.
- **Fault model.**
  - `omit(m)`: every copy of message `m` on a lossy channel is dropped, until the heal.
  - `crash(n)`: node `n` is dead from instance start.
  - `crash(n after m)`: `n` dies right after it first sends `m`, if it sends `m` before the heal.
  - Messages are *content-addressed*: a `MessageId` is `(channel, from, to, payload bytes)` (`hf/fault/mod.rs:154`).
  - A *fault set* has at most one crash per node.
  - **The heal**: from the heal on, no omission applies and no new crash fires; crashes that fired persist
    (`hf/fault/mod.rs:389`).
- **Correct member.** A member of the label's cluster whose crash did not fire in the run (`hf/check/engine.rs:364`,
  from the hook's record).
- **Fact domain.** The facts correct members held at the end of the failure-free runs: the *probe*, read over every
  schedule unless §14 allows one (`hf/check/engine.rs:164`, `hf/spec.rs:113`).
- **The specification of a label** (`hf/spec.rs:156`), evaluated on one run:
  - **POST**: every correct member holds every domain fact that some correct member holds;
  - **no divergence**: a fact outside the domain that some correct member holds is held by every correct member
    (`hf/check/races_engine.rs`, `read_schedule`; `hf/check/brute_force.rs:338`).

  A *member view* is the union of what the member's tap emitted. It never shrinks.
- **Bounds** (§10). A fault set is *within the bounds* if:
  - it crashes at most one node per crash;
  - it crashes at most `max_crashes` nodes;
  - it never crashes every crashable node;
  - it never crashes more than `|members| − 2` members of the label's cluster.

## 1. The certificate theorem

**Claim (Claim A).** Suppose `EcCheck::run` reports `Certified { tolerates }` for a label, and the search was not
sampled. Then for every fault set `H` within the bounds (with at most `k` crashes, where `k` is the tolerance), and
every schedule `σ` of `H` the harness permits, POST holds for that label at the end of `σ`.

**Assumptions.** Everything in §16. In particular: the program is a Hydro program, the simulator explores every
schedule, the harness is deterministic, and the rewrite preserves the program's behaviour.

**Objects.**
- A **pair** `p = (c, f)` is a member `c` of the cluster and a domain fact `f`.
- A **clause** of `p` is a derivation `D` of `f` at `c`, seen in some run, with its *premises*: the fragile steps it
  passed (§3).
  - The derivation is a set of *dependencies*. Each is a delivered message, whether its channel is lossy, and its
    sender if that sender can crash.
  - A clause with no premise is **robust**.
- **Kill rule** (§5, `hf/races/clause.rs:62`). Literals are substituted their adversarial values (§1.1):
  - `omit(m)` kills a dependency on `m` over a lossy channel;
  - `crash(n)` kills every dependency sent by `n`;
  - `crash(n after m)` kills a dependency `d ≠ m` sent by `n` unless `after(d, m)` is *proved* false (§6).
- **Admissible.** A set `H` is admissible for `p` if it does not *excuse* `p` (§8: `crash(c)` at start, or a dead set
  for `f` contained in `H`), and it kills every robust clause of `p` (`hf/races/clause.rs:151`).
- **Pruned.** `H` is pruned if it is admissible for no pair (`hf/races/clause.rs:168`).
- **The candidates `U`** (`hf/races/clause.rs:263`) are every fault some run revealed:
  - `omit(m)` for each lossy message sent before the heal;
  - `crash(n)` for each crashable node;
  - `crash(n after m)` for each message `n` sent before the heal;
  - and, unless sends are complete (§6.2), one `Unseen(n)` per crashable node.

  `Unseen(n)` is "crash `n` after a message no run showed". It kills every dependency sent by `n`, and excuses
  nothing.
- **The program** (`hf/races/ilp.rs:134`). Proposals are subsets of `U` within the bounds that are admissible for some
  pair. Every tried proposal is excluded by an exact no-good.

**Certificate.** The sweep concludes at bound `k` when the program has no solution at bound `k` over the final `U`,
with every tried proposal excluded (`hf/races/sweep.rs:253`). If `U` grows after a bound concluded, the sweep
restarts from zero crashes, so a conclusion always refers to the current `U` (`hf/races/sweep.rs:256-260`).
`Certified(UpTo k)` and `Certified(All)` are emitted only from such a conclusion. `All` additionally needs the bound
to have reached the vacuity limit or saturation (§10).

**Proof.** Fix `H` within the bounds and a schedule `σ` of `H`. Let `H°` be its *image* (`hf/races/clause.rs:322`,
§9):
- known faults stay;
- omissions of messages no run showed are dropped;
- a crash after a message no run showed becomes `Unseen(n)`, or is dropped when sends are complete.

`H°` is within the same bounds (§9). There are two cases.

1. **`H°` is admissible for no pair.** Take any pair `p = (c, f)`.
   - **`H°` excuses `p`.** Then `H` excuses it too (§9), so either `c` is crashed at start in `σ` (not correct), or
     `H` contains a set under which `f` is dead, and no correct member holds `f` in `σ` (§8).
   - **Otherwise some robust clause `D` of `p` is killed by no candidate of `H°`.** Then no fault of `H` kills `D`
     (§9), except crashes after messages never sent, which never fire.
     - Under the kill rule with proved literals, "not killed" means every dependency of `D` is delivered in every
       schedule where its sending record exists (§5, §6).
     - By Lemma R (§2), in `σ`, `c` holds `f` or `c` crashed.

   So for every domain fact `f`, either every correct member holds it, or no correct member does. If some pair of
   `f` is excused by a dead set, no correct member holds `f`; every other pair of `f` is held or its member crashed.
   That is POST.
2. **`H°` is admissible for some pair.** It is within the bounds, so the program would admit it unless it was tried.
   So `H°` was tried. A tried proposal that is still admissible was explored without stopping early. An exploration
   stops early only when its set is already pruned (`hf/check/races_engine.rs:580`), and clauses only grow, so a set
   pruned then stays pruned. Its schedules all satisfied the specification, or the search would have reported a
   counterexample. Let `S = H° \ {Unseen(·)}`.
   - `S` was **settled**: every schedule of `S` was run, or one schedule was run under the one-schedule policy, which
     shows every schedule's facts (§14).
   - If `H°` holds an `Unseen(n)`, then `S` was explored in full (`hf/races/sweep.rs`, `Next::Try { in_full }`;
     `hf/check/races_engine.rs:606`).
   - Every fault of `H` not in `S` is either:
     - an omission of a message no run showed; or
     - a crash after a message no run showed, where either sends are complete (§6.2), or `H°` has an `Unseen` and
       `S` was explored in full.
   - No run of `S` sends such a message. Either every run of `S` was read and its pre-heal sends harvested, or sends
     are complete and no run of any fault set sends it. So none of these faults ever acts, and every schedule of `H`
     is a schedule of `S` (§9).
   - `σ` is therefore a schedule of `S`, and `S`'s schedules all satisfied the specification. **Proved.**

**Where it lives.**
- `hf/races/sweep.rs:10-35`: the theorem in the module docs;
- `hf/races/sweep.rs:253`: `next`;
- `hf/races/ilp.rs:134`: the program;
- `hf/check/races_engine.rs:458`: the search loop;
- `hf/check/races_engine.rs:531`: exploration.

**Tests.**
- **Randomised completion test:** `hf/races/tests.rs` `completion_covers_every_fault_set`, 24 seeds, half with
  complete sends and half with `Unseen`. It checks against a fault space larger than `U` that every `H` is pruned or
  has its image tried.
- **Unit tests** for the program and the sweep: `hf/races/tests.rs`, 25 tests.
- **Brute-force cross-checks** of every certificate in the corpus (§15).

### 1.1 Free literals are substituted, not searched

**Claim.** Solving the program with the free literals substituted (§5) has the same solutions in the fault variables
as solving it with every literal as a variable.

**Proof.**
- A row of pair `p` reads `Σ_c x_c · [c kills a dependency of D, given the literals] + Σ_ℓ (1 − ℓ) ≥ z_p`.
- A free crash literal appears only as a factor of a kill: `crash(n after m)` kills `d` only if `after(d, m)`. So
  setting it true only helps the row.
- A free premise literal `ℓ` appears only as `(1 − ℓ)`, so setting it false only helps the row.
- Neither kind appears in bounds, no-goods or exemption rows, which are over faults only.
- So for any fault assignment, there is a literal assignment satisfying the rows if and only if the substituted rows
  hold.
- With the substitution, a free crash literal means "the crash kills", and a free premise means "the row imposes
  nothing". A fragile clause therefore imposes nothing in the certificate scope.

The adversary may now use literal values no run realises, such as `after(d, m)` and `after(m, d)` together. That only
strengthens it, which is sound. **Proved.**

**Where.** `hf/races/literals.rs:18-30` (the argument), `hf/races/clause.rs:62` (the kill rule), `hf/races/ilp.rs`
(`Scope::includes`: fragile clauses only in the heuristic scope).

## 2. Lemma R: robust derivations hold in every schedule

**Claim.** Let `D` be a robust derivation of fact `f` at member `c`, read from any run. Let `H` be a fault set under
which no dependency of `D` is killed by the kill rule (with proved literals). Then in every schedule of `H`, at the end
of the run, `c`'s tap has emitted `f`, or `c` has crashed.

**Assumptions** (§16):
- (A1) the simulator branches only at hooks;
- (A2) DFIR is deterministic between hook decisions;
- (A3) user closures are pure;
- (A4) records are compared by content;
- (A5) the provenance rewrite over-approximates each record's dependencies, and records the fragile sites it
  passed (§3);
- (A6) at the end of the run every pending hook input has been released: quiescence;
- (A7) the network delivers every send that is not omitted, and the receiver processes it;
- (A8) the harness sends the same inputs in every schedule.

**Proof** by induction along `D`, from its sources to `c`'s tap.
- **Sources.** Each source record of `D` is a harness input or a fixed value (iterator, singleton or cluster
  membership). By (A8) and the allowlist (§3), it exists in every schedule.
- **Robust steps.** Every step on `D` is on the allowlist (§3), and passed no fragile site, because `D` has no
  premise. Each such step is set-preserving: once its input records exist in a run, its output record exists by the
  end of the run, whatever the batching. Two things make this hold:
  - by (A6), quiescence releases every batch hook's pending input into a tick, and that tick runs;
  - outputs are functions of their inputs' contents, by (A2), (A3) and (A4).

  The step-by-step justifications are the allowlist table in §3.
- **Sends.** Take a dependency `d ∈ D`, a message sent by node `n` to node `n'`.
  - **Its sending record exists**, by induction.
  - **`d` is not omitted.** If its channel is lossy and `d` was sent before the heal, `H ∌ omit(d)`, since otherwise
    `D` would be killed. After the heal no omission applies.
  - **The send is not cut by a crash of `n`.**
    - If `H` crashes `n` at start, `D` is killed: excluded.
    - If `H` has `crash(n after m)` and `d ≠ m`, then `after(d, m)` is proved false (otherwise the kill rule kills
      `D`). So `d` is sent before `m` in every run, and the crash fires only after `m` is sent. The send happens.
    - If `d = m`, the crash fires right after `d` is sent.
    - A crash after a message `n` never sends never fires.
    - After the heal no new crash fires.
  - **The receiver `n'` processes `d`.** By (A7), and `n'` is not crashed before it receives `d`.
    - If `n' = c`, that is the conclusion's "or `c` has crashed".
    - If `n'` is an intermediate node, `D` has a later dependency sent by `n'`. A crash of `n'` before that send would
      kill it by the rule above, so `n'` was alive at the send, hence when it received `d`, since it received `d`
      before computing the send's record.
- **The end.** `c` computes `f` and its tap emits it, unless `c` is dead. A dead node's outputs are dropped
  (`hf/fault/mod.rs:504`), and that is the "or crashed" case. The member view only grows (`hf/spec.rs:45`).

**Duplicates.** Messages are content-addressed, so `omit(m)` drops every copy and `crash(n after m)` fires after the
first copy. A dependency on a later copy of `m` is served by the first copy, whose content is the same (A4).
**Proved**, given A1–A8.

**Corollary (pruning).** If `H` is pruned, POST holds in every schedule of `H` (§1, case 1). **Proved.**

**Where.**
- `hf/lineage/mod.rs:285` (`derivation`), `hf/lineage/mod.rs:302` (`fragile_sites`), `hf/lineage/mod.rs:324` (the
  closure);
- `hf/check/races_engine.rs:198` (premises from fragile sites);
- `hf/races/clause.rs:62`, `hf/races/clause.rs:168`.

**Tests.**
- Lineage unit tests: `hf/lineage/tests.rs`, `hf/lineage/sim_tests.rs`, `hf/lineage/fragile_tests.rs` (8 tests on
  fragile tagging end to end).
- The brute-force cross-checks (§15) test Lemma R's use in every certificate.

## 3. Fragile steps: the allowlist and its completeness

**Definition.** A step is *robust* if the set of records it emits over a run is a function of the set of records it
receives over the run, whatever the batching and order, and does not depend on whether a later tick runs. Every other
step is *fragile*. The rewrite tags every record a fragile step emits with that step's site, and lineage collects the
sites on a derivation's backward closure (`hf/lineage/mod.rs:302`). A derivation with no site is robust.

**Claim (completeness).** Every IR node kind whose output can depend on batching, order or tick scheduling is tagged,
or is an opaque island.

**Structure** (`hf/provenance/fragile.rs:195`, `View::fragile`). An allowlist:
- each robust kind has an arm with its justification;
- the default arm (`_ => false`, `hf/provenance/fragile.rs:268`) makes everything else fragile, including node kinds
  added to the IR later;
- a closure that reads Hydro state by reference is fragile before any arm (`closure_over_state`);
- so is a closure, or a source expression, that names outside or shared state (`impure_closure`, A3): `static`,
  `thread_local`, `Cell`/`RefCell`/`OnceCell`/`Lazy*`, `Mutex`/`RwLock`, `Atomic*`, `unsafe`, randomness, `Instant`,
  `SystemTime`, `tokio`, and `std::`/`core::` `env`, `fs`, `io`, `net`, `process`, `ptr`, `thread`, `time`
  (`hf/provenance/fragile.rs`, `names_impure`). The guard is textual: it cannot see into functions the closure calls,
  and it flags some pure closures, which only costs schedules. Such a closure is also a schedule choice (§3.1).

**The table** (every `HydroNode` kind at `dd5c4d05a5`):

| kind | verdict | why |
|---|---|---|
| `Placeholder` | — | never reached (`View::nodes` skips it) |
| `Cast`, `ObserveNonDet` | robust | pass-through; `observe_nondet` reorders (a choice), it never adds or drops |
| `Tee`, `Reference`, `EndAtomic`, `AssertIsConsistent`, `Counter`, `UnboundSingleton` | robust | pass-through, in the same execution |
| `CycleSource` | robust | emits what its sink received. At top level it is async; in a tick it is read through a `DeferTick`, which is fragile |
| `YieldConcat` (`all_ticks`, `latest`) | robust | the union of every tick's batch, emitted at the end of that tick. `latest` is a singleton, read only through a snapshot, which is fragile |
| `DeferTick` | **fragile**, except the dedup idiom's state edge | a deferred record exists only if a later tick runs, and a tick runs only when a hook releases (`hydro_lang/src/sim/compiled.rs:2022`). The dedup edge only feeds a negation's negative side, whose tags the rewrite drops (`hf/provenance/rewrite.rs:1142`), provided nothing else reads the idiom's state untagged (below) |
| `Map`, `FlatMap`, `Filter`, `FilterMap`, `Inspect` | robust (pure closure) | each output is a function of one input record (A3) |
| `PartitionShared`, `PartitionSide` | robust unless the closure reads state | as `filter` |
| `Chain`, `MergeOrdered`, `Sort`, `Unique` | robust | set-preserving: the union, the same set reordered, or the distinct ones |
| `Network` | robust | delivers each send unless a fault removes it (A7) |
| `Batch` or `BeginAtomic` of a stream, into a tick that snapshots no singleton | robust | decides when a record enters a tick, never whether: the hook can always release pending input, and quiescence runs it (A6). The simulator lowers `atomic` to a batch (`hydro_lang/src/sim/builder.rs:818`) |
| `Batch` or `BeginAtomic` of a stream, into a tick that also snapshots a singleton | **fragile** (`batch_beside_snapshot`) | the simulator runs that tick only once the singleton has a value (`SingletonHook::is_ready`, `hydro_lang/src/sim/runtime.rs:591`). If what feeds the singleton never arrives, the tick never runs, and the batched records never exist (A6) |
| `Batch` or `BeginAtomic` of a singleton, optional or keyed singleton (a *snapshot*) | **fragile** | the value seen depends on how much has been folded |
| `Source::Iter` and `SingletonSource` at top level; `ClusterMembers`, `ExternalNetwork`; `ExternalInput` | robust | fixed values emitted once, the static membership (A14), the harness's inputs (A8) |
| `Source::Iter` and `SingletonSource` inside a tick (`tick.singleton`, `optional_first_tick`) | **fragile** (`tick_constant`), except a dedup idiom's initial state | emitted in every tick that runs, and only if one runs; whether (and how often) the tick runs depends on its deliveries |
| other `Source`s (`Stream`, `Interval`, `Spin`, `Embedded*`) | **fragile** | how many records exist depends on time |
| `Join`, `CrossProduct` at top level | robust | a symmetric hash join over the run emits every matching pair once both sides arrived |
| `JoinHalf` at top level | robust | its build side is bounded and accumulated before probing (`hydro_lang/src/compile/ir/mod.rs:4484`) |
| `Join`, `CrossProduct`, `JoinHalf` in a tick | **fragile** | the two records may land in different ticks |
| `CrossSingleton` | **fragile** | the singleton may not hold the value yet ("trigger meets state") |
| `AntiJoin`; `Difference` | **fragile** (but the dedup idiom, below) | the negative side may hold more or less at that tick |
| `Fold`, `FoldKeyed`, `Reduce`, `ReduceKeyed`, `Scan` (anywhere) | **fragile** | inside a tick, the value depends on the batch; at top level, the order can be a schedule choice (`assume_ordering` at top level is a `TopLevelStreamOrderHook`, `hydro_lang/src/sim/builder.rs:1139-1201`). `first`, `last` and keyed `first` lower to these |
| `Enumerate`, `ChainFirst` | **fragile** | they depend on order |
| `ResolveFutures*`, `FlatMapStreamBlocking`, `ScanAsyncBlocking`, `ReduceKeyedWatermark`, `VersionedNetwork*` | opaque island | not tagged. Every record through an island falls back to the execution rule and counts as fragile (`OPAQUE_SITE`, `hf/lineage/mod.rs:153`), and an island makes the program not fragile-free (`hf/check/races_engine.rs`, `shape`) |

**Structural exemptions** (each proved):
- **Constant inputs** (`hf/provenance/fragile.rs`, `View::constant`). A non-allowlisted step whose every input is
  built from iterator and singleton sources by stateless operators computes the same records from the same inputs.
  Inside a tick those sources are tagged themselves (`tick_constant`), so their tags still reach the step's output.
  Sources are excluded from this rule, since a node with no inputs would pass it vacuously: they decide in their own
  arm. `DeferTick` is excluded too: its records exist only if a later tick runs.
- **The dedup idiom** (`hf/provenance/fragile.rs`, `View::dedup_idiom`): `fresh = arrived.filter_not_in(seen);
  seen = seen.chain(fresh)`, with `seen` starting from a constant. Its output over the run is every record of
  `arrived` not in the initial state. Proof:
  - a record `r` of `arrived` is emitted in the first tick it is in, unless it is already in `seen`;
  - `seen` holds only the initial state and earlier outputs;
  - so each record not in the initial state is emitted exactly once, in whatever tick it arrives.

  The exemption covers the `Difference`, the `DeferTick` carrying `seen`, and the initial state's nodes. It applies
  only if their records reach nothing else untagged: `seen` is read only by the negation and the cycle's sink, the
  sink only by its `CycleSink` root, the cycle source only by the `DeferTick`, which only `seen` reads, and the initial
  state's nodes only by each other and `seen`. A reader is also allowed if every path from it to a root passes a node
  that tags whatever it emits (a fold, reduce or scan, a `cross_singleton`, `enumerate`, `chain_first`, or a join in a
  tick, outside a trusted quorum: `View::covered`). Another reader (`seen.clone().all_ticks()`) would emit records
  that exist only if a later tick runs, untagged.
- **Trusted `quorum`** (§7): its internal nodes are skipped (`hf/provenance/fragile.rs:76`).

**Tests.**
- `hf/lineage/fragile_tests.rs` (8 tests);
- `hf/check/brute_tests.rs` `a_top_level_scan_over_a_chosen_order_is_fragile`, `deferred_records_are_fragile`,
  `tick_constants_are_fragile`, `a_dedup_state_read_elsewhere_is_fragile`, `review_batch_beside_an_unready_snapshot`
  and `impure_closures_are_fragile_choices` (the regressions of §17);
- `races_certify_a_batched_echo` (a batch is robust).

**Not proved:** that the table's per-kind semantics match what DFIR and the simulator do. That rests on (A1), (A2),
(A6) and code reading, and is checked only by the brute-force cross-checks.

### 3.1 The choice-free shortcut

**Claim.** In a program with no schedule choice, every derivation may be treated as robust. `read_schedule` drops
premises when `choice_free` (`hf/check/races_engine.rs:198`).

**What "choice-free" means.** The choice scan (`hf/provenance/choices.rs:73`, `choice`) finds nothing. The scan is an
allowlist too: an operator is choice-free only if its kind is in `ALLOWED` (`hf/provenance/choices.rs:22`) and no
special rule names it. A `batch`, a snapshot, an untrusted `observe_nondet`, a fold over an unordered input, an
unordered external input, an ordered lossy network, any source but iterators, membership and external networks, a
closure that names outside state (`impure_closure`, §3), and every unlisted kind are choices.

**Proof.**
- Each fault set has exactly one schedule (A1, with no hook decisions to make).
- By A13 the program is monotone: with no batch, no tick receives outside data, so no non-monotone operator is fed
  from outside it. Faults then only remove records.
- A totally ordered stream reaching a top-level fold or scan comes from one FIFO chain (merging two senders gives
  `NoOrder`, and restoring an order is a choice). Under faults its sequence is a subsequence of the failure-free one,
  and the fold's or scan's record carries every earlier input in its lineage (`hf/provenance/rewrite.rs:608`,
  `wrap_acc`; `:628`, `wrap_scan`).
- So a derivation whose dependencies all survive `H` is recomputed under `H`, which is Lemma R's conclusion.

**Proved**, given A1, A2 and A13. **Not tested** by any gate at `dd5c4d05a5`:
- the choice-free corpus programs (`reliable_broadcast_closed`, S1, S2) are also fragile-free, so the shortcut decides
  nothing there that the fragile-free rule would not;
- forcing `choice_free = false` changes no gate's verdict (found by the dead-code audit).

## 4. Premise literals

**Claim.** It is sound to treat every premise literal as free.

**Proof.** §1.1. A free premise makes its row impose nothing, so a fragile derivation protects no pair, and no
certificate rests on it. No per-step premise lemma is needed, because no premise is ever proved. **Proved.**

The cost is that a label whose every derivation passes a fragile step pays brute-force cost: the program proposes
every set over `U`.

## 5. Crash literals and the order-blind crash rule

**Claim.** Whenever the kill rule says `crash(n after m)` does not kill a dependency `d` sent by `n`, `d` is sent in
every schedule in which its sending record exists.

**Proof.**
- **`d = m`:** the crash fires right after `m`'s first send, so `m` is sent.
- **`after(d, m)` proved false:** `d` is sent before `m` in every run (§6.1), and the crash fires only once `m` is sent.
- **Otherwise the rule says the crash kills `d`**, so there is nothing to prove.
- **Never sent:** if `m` is never sent, the crash never fires (`hf/fault/mod.rs:475-477`).

The observed position of a send in a run (the ordinal) is never used to decide a kill (`hf/races/clause.rs:62`). It
is only a tie-break for weights. Lite's positional rule, which decided kills from one run's send order, was unsound
in both directions (S1, S2: `hf/check/brute_tests.rs` `suspected_*`) and is deleted. **Proved.**

**Where.** `hf/races/clause.rs:62`, `hf/races/literals.rs:69` (`after`).

**Tests.** `hf/races/tests.rs` (crash rule tests); `suspected_positional_crash_order_{tolerance,certificate}` (S2,
brute force agrees).

## 6. Proved literals

### 6.1 `broadcast_closed` fan-out order

**Claim.** On a channel marked as a closed broadcast, whenever a node sends `(v → k)`, it has already sent `(v → j)`
for every `j < k`. This holds in every run, under every fault set.

**Recognition** (`hf/provenance/fanout.rs:62-135`). A `Network` is marked only if its IR is exactly:

`Network(to cluster C) ← Cast ← Map(|(data, member_id)| (member_id, data)) ← Map(|((), (v1, v2))| (v1, v2)) ←
JoinHalf(probe: Map(|v| ((), v)) ← data, build: Map(|v| ((), v)) ← [AssertIsConsistent ←] Source::Iter(C's ids))`.

- Each closure is matched by its text, as stageleft quotes it from `hydro_lang`'s own source files.
- The id source must name the destination cluster's key.
- The mark is an `op.id` that nothing else sets (`FANOUT_MARK`).

**Proof.**
- `JoinHalf` compiles to `join_multiset_half`. It accumulates its whole build input before reading the probe side
  (`hydro_lang/src/compile/ir/mod.rs:4484-4520`), then emits, for each probe item, one output per build value, in
  build order.
- The key is `()`, so there is one build vector: the cluster ids, which the simulator lists as `0..n` from one
  `source_iter`.
- The maps and the cast are one-to-one, and the network sends its input items in order (`map(serialize) →
  for_each(send)`).
- So each `v` is sent as `(v → 0), …, (v → n−1)`, in that order.
- A crash between two of these sends cuts a suffix, never a prefix.

**Proved**, given (A2) and the cited DFIR semantics. Also **checked in code** (the strict match) and **tested**
(`hf/check/fanout_tests.rs`: under every schedule, 1,332 for one test, every payload goes to members in increasing id
order).

**Where it is used.** `hf/races/literals.rs:92-114` (`FanOutOrder::precedes`), `hf/races/literals.rs:69` (`after`).

### 6.2 Never sent, and complete sends

**Claim.**
1. A crash after a message never sent never fires.
2. In a fragile-free program, every message any run of any fault set sends is sent in the probe, so the probe's
   harvest names every possible crash point.

**Proof.**
1. `hf/fault/mod.rs:475-477`: the crash fires only on the send of its message, before the heal. **Checked in code.**
2. In a fragile-free program, every record has a robust derivation, including every send's record. A send under `H`
   in some schedule has a robust derivation `D`, all of whose dependencies were delivered under `H`. Without faults,
   `D`'s dependencies are not killed. So by Lemma R, `D`'s record exists in every failure-free schedule, and the
   probe sees the send. **Proved**, given Lemma R.

   This is what lets a fragile-free program use no `Unseen` candidate (`hf/races/sweep.rs:150`,
   `hf/check/races_engine.rs`: `sends_complete` when `fragile_free`).

### 6.3 Exact crash points

`crash(n after m)` is *exact* if sends are complete and every message `n` can send has a proved literal `after(·, m)`
(`hf/races/sweep.rs:169`). Then in every run the crash cuts exactly the sends whose literal is true. **Proved**, from
6.1 and 6.2.

## 7. Trusted `quorum`

**Claim.** For the exact compiled form of `hydro_std::ec_inference_demos::quorum::quorum`
(`hydro_std/src/ec_inference_demos/quorum.rs:121-185`):
- every `Durable(f)` it emits has, as lineage, the deliveries of `f`'s own attestations;
- and if those deliveries occur in a run, from `threshold` distinct attestors, the run emits `Durable(f)`, whatever
  the batching.

**Recognition** (`hf/provenance/quorum.rs:55`, `:184`, `:250`):
- the mint closure, matched by its text (with any threshold);
- the fold `Vec::new` / `push` over a `Batch`;
- the state initialisers `HashMap::new()`, `HashSet::new()` and `()`;
- the state cells by shape.

A copy of the same logic written elsewhere is not recognised (`hydro_test/src/cluster/ec_quorum_trust.rs`
`hand_written_quorum_stays_fragile`).

**Proof** (of existence):
- `acks.unique()` (`:133`) keeps one attestation per `(fact, attestor)`.
- `pending` (`:142`) persists each unfired fact's attestor set across ticks, and only adds to it.
- `fired` (`:143`) records fired facts. A fact fires at most once, and later attestations of it are ignored.
- The test `attestors.len() >= threshold` (`:162`) is monotone.
- By (A6), every batched attestation is released by quiescence. Each one reaches `pending` or finds `f` fired, and in
  that case `Durable(f)` was already emitted.
- So the count reaches the threshold, and `Durable(f)` is emitted.

**Lineage** (`hf/provenance/rewrite.rs:806`, `quorum_node`). The rewrite records, per unfired fact, the tags of every
attestation of it seen so far, and gives `Durable(f)` exactly those. That is a superset of the first `threshold`
attestations, which is sound.

**Trust boundary:** the audited code `quorum.rs:121-185`, as `unique` is trusted. **Proved** (code reading) and
**tested**:
- `quorum_lineage_is_the_facts_attestations`: certificates are robust, and their lineage is their own fact's
  deliveries;
- URB cross-checks with 2 members (`races_targets_urb2_*`);
- the planted local-attestation bug (`races_planted_local_attestation`).

## 8. Dead facts

**Claim.** Suppose all of:
- the program is fragile-free;
- every crash of `S` is a start crash, or an exact crash point (§6.3) of a node outside the label's cluster;
- the one run of `S` shows no correct member holding `f`.

Then under every `H ⊇ S`, in every schedule, no correct member holds `f`.

**Proof** (`hf/races/sweep.rs:177-226`). Suppose some correct member `c` of `H` holds `f` in a schedule of `H`, by a
derivation `D`, which is robust because the program is fragile-free.
- **No fault of `S` kills `D`:**
  - an omission in `S ⊆ H` did not drop `D`'s deliveries;
  - a start crash's node sent nothing, so `D` has no dependency from it;
  - for an exact `crash(n after m)` in `S`, a dependency of `D` from `n` with a true literal would be sent after `m`,
    which the crash in `H` cuts. So all its literals are false, and the rule spares it.
- **`c` is correct under `S`:** its start crashes are in `H`, where `c` is correct, and `S`'s other crashes are of
  nodes outside the cluster.
- By Lemma R, `c` holds `f` in every schedule of `S`: a contradiction.

The argument uses only Lemma R and proved literals, never an order observed in a run, so S2-style issues cannot reach
it. The one-run verification is valid because of §14: one schedule shows every schedule's facts. **Proved.**

**Where.**
- `hf/check/races_engine.rs:611-620` (the call, with its preconditions);
- `hf/races/sweep.rs:205` (`mark_dead` asserts the crash kinds);
- `hf/races/ilp.rs` (exemption rows);
- `hf/races/clause.rs:137` (`excused`).

**Tests.**
- `hf/races/tests.rs` `a_dead_fact_excuses_its_pairs_under_supersets`, `a_dead_set_with_an_inexact_crash_point_is_refused`;
- the gate rows `new_messages` 1–3 payloads and URB / echo over 3 members (§15).

## 9. Completion of the fault universe

**Claim.** The image `H°` of any `H` (§1) has these properties:
1. it is within every bound `H` is;
2. it excuses a pair only if `H` does;
3. a fault of `H` that kills a robust clause's dependency is in `H°`, or represented by an `Unseen` that kills at least
   as much;
4. the faults of `H` not in `H°` never act in a run of `H°`'s settled set (§1, case 2).

**Proof.**
1. `H°` is `H` with some faults dropped and some crashes renamed to `Unseen` of the same node. That is no more crashes
   per node, and no more crashes in total or in the cluster.
2. Only `crash(n)` and dead sets excuse. `crash(n)` is always known, so it is kept. Dead sets hold only known faults,
   and `H°`'s known faults are `H`'s. `Unseen` excuses nothing.
3. A clause's dependencies were all delivered in some run, before the heal if they are lossy, so their omissions and
   their senders' crash points are known. A crash after an unseen message becomes `Unseen(n)`, which kills every
   dependency of `n`.
4. See §1, case 2. An omission of a message never sent never acts. A crash after it never fires (§6.2).

**Proved.** One limit: property 3 needs every dependency a *lossy* clause names to have been sent before the heal.
Lineage marks a dependency lossy only if it was sent before the heal (`hf/lineage/mod.rs`, `lossy = before_heal &&
is_faulty`).

**Where.** `hf/races/clause.rs:263` (`candidates`), `:322` (`image`); `hf/check/races_engine.rs:76` (`Revealed::of`,
the harvest).

**Tests.** `completion_covers_every_fault_set`, `the_image_keeps_known_faults_and_maps_the_rest`,
`an_unseen_crash_is_resolved_by_a_run_in_full`.

## 10. Bounds and vacuity

**Claim.** Certificates quantify over fault sets within the bounds and say nothing outside them. The bounds
(`hf/search/mod.rs:116-140`, `hf/check/engine.rs:251`) are:
- at most one crash per node;
- at most `max_crashes` crashes, if capped (`Tolerance::UpTo(k)`);
- never every crashable node: at most `nodes − 1`;
- at most `|members| − 2` members of the label's cluster.

**What they exclude, and why.**
- Crashing every node, or all but one member, makes agreement vacuous: a single survivor agrees with itself.
- **The consequence is real.** In a 2-member cluster no member crash is admissible. So a certificate there covers
  only crashes of nodes outside the cluster (sources, coordinators) and omissions.

`Tolerance::All` means one of:
- the bound reached `min(nodes − 1, …)`;
- or no proposal can hold more crashes, because the universe's crashable nodes are saturated
  (`hf/races/sweep.rs`, `next`).

**Checked in code** (`hf/races/clause.rs:181` `within_bounds`; `hf/check/brute_force.rs:276`, the same bounds for the
oracle).

## 11. The host-side specification check

**What is checked** (`hf/observe/`):
- **One label per labelled node.** Every IR node where an `EventualConsistency` label is minted or exits is its own
  collection, by node identity (`hf/observe/collections.rs`, `ec_collections`). The tap pass re-identifies the same
  node by address, and panics unless every searched label is tapped at exactly one node (`hf/observe/tap.rs`, `tap`).
  Two collections labelled on one source line (a helper called twice, a library function instantiated twice) are two
  labels, named `#1`, `#2`, ... (§17, fix 7).
- **Not checked: local inputs only.** A collection no network delivery can reach. Inside a tick, a node is also fed by
  whatever feeds that tick's entry points (its `batch`es and `atomic`s, `tick_entries`): if no delivery reaches them
  the tick never runs, so its constants and cycles are fed by the network too (§17, fix 3).
- **Not checked: scalars.** A labelled singleton, optional or keyed singleton is not checked, even when it folds a
  checked stream: the specification compares sets of facts, and members agreeing on a stream's set can disagree on a
  fold of it (a count or a sum over duplicates, any fold that is not idempotent). The IR does not say whether a fold
  is idempotent (§17, fix 8).
- **Fact identity** (A4): a fact is its element's `bincode` encoding if the type is `Serialize`; else its `Debug`
  text; else a 64-bit `Hash` digest (`hf/observe/tap.rs`, `tap_closure`; `hydro_std`'s `Durable` is not
  `Serialize`). The fallbacks are sound only if `Debug` is injective on the labelled values and the digest has no
  collision among them (§18).

**Claims and proofs.**
- **Monotone view.** A member's view is the union of its tap's emissions (`hf/spec.rs:45`). A tap emits each record
  once it exists, and a dead node emits nothing (`hf/fault/mod.rs:504`). **Checked in code.**
- **Correct members** are decided at the end of the run: the crash set the fault hook recorded
  (`hf/check/engine.rs:364`). A crash point that never fired leaves its member correct. **Checked in code.**
- **Domain** (`hf/check/engine.rs:164`): the facts correct members held at the end of each probe schedule, unioned over
  the probe's schedules.
  - The probe is every schedule, unless the program is choice-free (one schedule), or fragile-free (one schedule
    shows every schedule's facts, §14).
  - A sampled probe certifies nothing.
  - **If the probe were not exhaustive** in a fragile program, a fact seen only in unexplored schedules would be
    outside the domain. It would be judged only by the divergence check of explored runs, and Claim A would not cover
    it. That is why the probe must be exhaustive unless one of the two exceptions applies.
- **Divergent facts.** In every explored run, a fact outside the domain held by some but not all correct members is a
  counterexample (`hf/check/races_engine.rs`, `read_schedule`; the oracle: `hf/check/brute_force.rs:338`).
- **An empty domain** makes the label `Vacuous` (`hf/check/races_engine.rs`, `check`), and it is not searched; brute
  force drops such labels too. A fact that appears only under faults (a failover's output, say) is then never
  checked (§18).

**Where.** `hf/spec.rs:113`, `:158`.

**Tests.** `hf/spec.rs` unit tests; `hf/check/tests.rs`.

## 12. Heal semantics

**Claim.** Faults act only before the heal: omissions while not healed (`hf/fault/mod.rs:470`), crash points firing
only before the heal (`:487-489`), start crashes at instance start (`:438-451`). After the heal every send is
delivered, and crashes that fired persist: a crashed node sends and outputs nothing (A11).

**The harness shape** (`drive`, `hf/check/engine.rs`), in two phases:
1. **The fault phase**: the inputs closure, with barriers forbidden. Faults are admitted while it drives inputs and
   while the work it caused settles.
2. **The heal**, then **the recovery phase**: the recovery closure, with no faults. It models what happens after the
   faults stop, such as timer ticks and retries. Without a recovery closure nothing runs after the heal, as before.

The run ends with a quiesce, and the specification is read on the end state.

**No barrier in the fault phase.** A `quiesce()` inside the inputs would remove every interleaving across it (§18),
so the certificate would cover less than the harness seems to say. The check forbids it: inside the inputs,
`quiesce()` panics, and so does sending input after an observation forced the simulation to quiesce
(`forbid_barriers`, `hydro_lang/src/sim/compiled.rs`). The panic is the harness's error, not a counterexample (the
check re-raises it). Waiting for an output stays allowed: the work that ran was needed to produce it. **Checked in
code**; test `a_barrier_in_the_fault_phase_panics`.

**Sends after the heal.** The recovery phase sends messages after the heal. The rules for them:
- **Never omitted, and no crash point fires on them** (`hf/fault/mod.rs:470`, `:476`).
- **No candidate faults.** The harvest stops at the heal, in the search (`hf/check/races_engine.rs`,
  `Revealed::of`) and in brute force (`hf/check/brute_force.rs`, `harvest`). So no omission of a post-heal message,
  and no crash point after one, is ever proposed.
- **Lineage.** A post-heal delivery is never omittable (`lossy = before_heal && faulty`, `hf/lineage/mod.rs`). It
  still depends on its sender's crash. That is right, not slack: a sender whose crash fired before the heal sends
  nothing after it, so `crash(n)` and `crash(n after m)` kill `n`'s post-heal deliveries.
- **The crash rule** (§5) still holds. If `m` is sent before the heal in one schedule and only after it in another,
  `crash(n after m)` does not fire in the second. Each case of §5 is about a crash that fires, so firing less keeps
  it sound.
- **The invariant** is "no omission and no crash firing after the heal", not "no send after the heal".

**What this excludes.** A fault during the recovery phase, such as an omission of a retry, is outside the model
(§18). A certificate says: whatever faults strike during the fault phase, once they stop and the recovery runs,
the label holds. **Checked in code**; test `the_recovery_phase_runs_healed` (a resend in the recovery phase repairs
every omission, and the enumeration agrees).

**The invariant lineage relies on** (A15): after the heal no send is omitted and no crash fires. Sends after the heal
are fine. Lineage marks a delivery omittable only if it was sent before the heal (`lossy = before_heal && faulty`,
`hf/lineage/mod.rs:212`), and every delivery keeps its sender-crash dependency, before or after the heal: a node that
crashed before the heal sends nothing after it, so `crash(n)`, and `crash(n after m)` with `m` sent before the heal, kill
`n`'s later deliveries too. Checked over every instance's log when its record is taken (`hf/fault/mod.rs`,
`EventLog::check_heal`, which panics). A recovery phase after the heal must keep this invariant, or change the rules
above.

## 13. Claims A and B

- **Claim A** (§1): POST for every fault set within the bounds and every schedule. Certified by `Certified`.
- **Claim B**: no divergence on facts outside the domain, for every such fault set and schedule. Certified (`claim_b`,
  `hf/check/races_engine.rs:519`) when the label is certified, and one of:
  - **the program is fragile-free.** Every fact under `H` has a robust derivation, which survives no-faults, so by
    Lemma R it is in the domain. No fact is outside the domain. **Proved**;
  - **the program is choice-free.** Then it is monotone (A13): a non-monotone operator needs a bounded input, only a
    tick provides one, and a tick's outside input arrives through a batch, which is a choice. In a monotone program
    faults only remove facts. Every fact under `H` is also a failure-free fact (its derivation's dependencies exist
    without faults, and every step is monotone), so it is in the domain. No fact is outside the domain. **Proved**,
    given A13;
  - **no early stop, and every set over `U` within the bounds was tried** (`Sweep::covers_everything`, a program with
    no admissibility rows). Then every `H` maps to a tried set explored in full, and its divergence was checked.
    **Proved.**

## 14. Exploration policies

- **Every schedule** (`Policy::All`): exhaustive over hook decisions (`hydro_lang/src/sim/compiled.rs:816`), or a
  sample (no certificate).
- **Early stop** (`hf/check/races_engine.rs:580`): an exploration stops when its set is pruned by the clauses learned
  so far. The remaining schedules are covered by the pruning corollary (§2). Disabled for `in_full` runs.
- **One schedule** (`Policy::One`, `hf/check/races_engine.rs:254`), in two cases:
  - **choice-free program with only start crashes:** one schedule is the only schedule;
  - **fragile-free program, and every crash of the set a start crash or exact (§6.3).** Every fact of one schedule is
    a fact of every schedule, and conversely. Proof: take a fact's robust derivation from one schedule. Its
    dependencies were delivered under the set, so no omission kills them, no start-crashed node sent them, and, with
    every literal fixed, a dependency from an exact crash point's node must have a false literal (a true one would
    have been cut). So it survives the set, and by Lemma R the fact exists in every schedule. The correct members are
    the same in every schedule: start crashes always fire, and exact crash points fire iff their message is sent,
    which is itself a robust fact. Divergence is the same in every schedule too. **Proved.**

## 15. Brute force as the oracle

**What it is** (`hf/check/brute_force.rs`, test-only).
- **Every fault set** within the bounds of at most `max_faults` faults, over a universe harvested from runs and grown
  to a fixpoint (`:251`, `:298`, `:533`).
- **Its own execution loop** (`:467`): it reads outputs straight from the event log (`:338`), with no lineage,
  provenance reading or pruning.
- **Agreement** (`:630`):
  - a certificate for `k` crashes means no enumerated set with at most `k` crashes breaks the label;
  - a counterexample after tolerating `t` means none with at most `t` crashes does, and the counterexample has
    `t + 1` crashes;
  - a counterexample's set, rerun, breaks the label again.

**What it shares with the search:**
- the simulator and its exhaustive exploration (bolero; A1, A16), and the harness (A8);
- the instrumented, compiled program, including the provenance rewrite (A5 and A9 below);
- the label planning and the taps (`hf/observe/`), so which collections are checked and how facts are identified
  (A4);
- the fault hook (`hf/fault/mod.rs`), with whole-message omission (`omit(m)` drops every copy of a content-identical
  message, §18);
- the universe harvest, which mirrors the search's `Revealed`;
- the specification evaluation (`hf/spec.rs`);
- the bounds, except one: brute force does not apply the `nodes − 1` bound (it is implied for the configurations it
  runs, which have more nodes than `max_faults`).

A bug in any of these would hit both sides: the two fatal false certificates of §17 (fixes 7 and 9) did. A bug in
lineage, literals, the kill rule, the program, completion, dead facts, quorum lineage or the policies would show as a
disagreement. The oracle covers at most `MAX_FAULTS = 2` faults; a certificate's claim beyond two faults rests on the
argument alone.

**Coverage.** Brute-force-covered configurations, all agreeing:
- `hydrofrac` `check::brute_tests`: plain, lossy and echo broadcasts, relay chains, ownership exchange, provenance
  precision, S1, S2, the planted bug, the batched echo, and the two §17 regressions;
- `hydro_test` `ec_bruteforce`: `ask_once`, CTP-3 with one-shot racing timers (sampled), `reliable_broadcast_closed` 1–3 payloads, plain
  broadcasts 1–3 payloads, echo broadcast over 2 members;
- `ec_races_targets`: URB over 2 members (thresholds 1 and 2), CTP-2 with timers after the faults and racing;
- `ec_two_pc_hydrofrac`: CTP-2 with one-shot and recurring racing timers (sampled), CTP-2 with timers after the
  faults (exhaustive), CTP-3 with recurring racing timers (sampled);
- `ec_quorum_trust`: the planted local attestation.

**Argument-only** (brute force cannot finish): URB over 3 members, echo broadcast over 3 members, ABD, G-Set gossip,
multi-Paxos.

## 16. Assumptions

| # | assumption | status |
|---|---|---|
| A1 | The simulator branches only at hooks, and `exhaustive` enumerates every hook decision sequence | ASSUMED (`hydro_lang/src/sim/compiled.rs`; the simulator's design) |
| A2 | DFIR is deterministic between hook decisions | ASSUMED |
| A3 | User closures are pure (no hidden state, no I/O): each output is a function of the inputs | ASSUMED for functions closures call. Partly CHECKED IN CODE: a closure that reads Hydro state by reference is fragile (`reads_state`); a closure or source expression that names outside or shared state is fragile and a choice (`names_impure`, a textual guard, §3) |
| A4 | Records are compared by content; messages are content-addressed; facts are identified by `bincode`, else `Debug`, else a 64-bit hash (§11) | CHECKED IN CODE for `Serialize` types (`hf/fault/mod.rs:154`, `MessageId`; `hf/observe/tap.rs`, `tap_closure`); ASSUMED that `Debug` is injective, and the hash collision-free, on the labelled values of other types |
| A5 | The provenance rewrite over-approximates every record's dependencies and tags every fragile step's outputs; opaque operators fall back to the execution rule | TESTED (`hf/lineage/*tests*.rs`, provenance tests, every brute-force cross-check); not proved |
| A6 | Quiescence means no tick can run (`SimTick::can_run`, `hydro_lang/src/sim/compiled.rs:2022`): a tick runs only when every one of its hooks is ready and some hook can release. A hook that is not ready blocks its tick even with input pending: a snapshot of a singleton that never got a value (`SingletonHook::is_ready`, `hydro_lang/src/sim/runtime.rs:591`; every other hook is always ready, `:78`). So a batch's pending input is released at quiescence only if its tick snapshots no singleton (§3, `batch_beside_snapshot`). Nothing else makes a tick run (§17, fix 2). Under the `hydrofrac` feature such a stuck tick no longer aborts the process: the run quiesces with that member never running the tick (`compiled.rs`, the "never became ready" check) | CHECKED IN CODE (`compiled.rs:2022`, `quiesce` at `:292`) |
| A7 | The simulated network delivers every send that no injected omission drops; lossy channels are intercepted by the fault hook | CHECKED IN CODE (`hf/fault/mod.rs:348-353` asserts interception) |
| A8 | The harness, in both phases, is deterministic: the same inputs, in the same order, in every schedule; it may wait for outputs but does not branch on them | ASSUMED |
| A9 | The rewrite preserves the program's behaviour (records carry tags but compute the same values) | TESTED (`hf/check/tests.rs`, `hydrofrac_provenance_tests` in `hydro_test` and `hydro_std`) |
| A10 | Messages are sent one at a time, in production order, per channel | ASSUMED (simulator `for_each(send)`); used by §6.1 |
| A11 | A crash fires right after the first send of its message and halts the node (no further sends or outputs) | CHECKED IN CODE (`hf/fault/mod.rs:468-489`, `:516-534`) |
| A12 | Cluster ids are listed `0..n` by one `source_iter` | ASSUMED from `hydro_lang/src/sim/graph.rs`; TESTED by `fanout_tests` |
| A13 | A Hydro program with no schedule choice is monotone: non-monotone operators need a bounded input, which only a tick provides, and outside data enters a tick only through a batch (a choice) | ASSUMED (Hydro's type system); used only for Claim B in choice-free programs (§13) |
| A14 | Membership is static, and only hydrofrac injects crashes: no `with_dynamic_membership` (its `MembershipHook` makes joins a schedule choice), no `with_crashable_process`/`with_crashable_cluster` (the simulator's own `CrashHook`, whose crashes are not in the fault hook's record, so its crashed members would count as correct) | CHECKED IN CODE: `EcCheck::instrument` sets `SimFlow::refuse_sim_faults`, and `compiled()` panics if either was used, before or after (`hydro_lang/src/sim/flow.rs`) |
| A15 | After the heal no send is omitted and no crash fires (sends are fine); every delivery keeps its sender-crash dependency (§12) | CHECKED IN CODE (`hf/fault/mod.rs`, `EventLog::check_heal`, over every instance's log) |
| A16 | `exhaustive` runs to the end: bolero's exhaustive driver stops silently at `BOLERO_RANDOM_TEST_TIME_MS` and returns normally | CHECKED IN CODE: both engines refuse to start an exhaustive exploration with the variable set (`hf/check/races_engine.rs`, `refuse_truncated_exhaustive`); exhaustive runs ignore bolero's other limits |

## 17. Found while writing this document (fixed)

1. **Top-level folds and scans** (fixed in `7c50e2f9ad`).
   - `scan` (and the folds and reduces) were fragile only inside a tick. A top-level `assume_ordering` is a schedule
     choice, so a top-level `scan` after it depended on order, yet was robust.
   - A program built on that was certified, while brute force broke it with no fault.
   - `fragile.rs` became the allowlist of §3.
   - Regression: `a_top_level_scan_over_a_chosen_order_is_fragile`.
2. **`defer_tick`** (fixed in `7df7e0753a`).
   - A deferred record is emitted only if a later tick runs, and nothing pending in a `defer_tick` makes a tick
     runnable (A6).
   - A program built on that was certified, while brute force broke it with no fault.
   - `DeferTick` is now fragile, but for the dedup idiom's state edge.
   - Regression: `deferred_records_are_fragile`.

In both, no corpus certificate changed: no corpus program relied on the unsound entries.

Found by review (rev 2 and reviewers #1 and #3), each reproduced against brute force before the fix (unless noted)
and fixed structurally:

3. **Constants inside a tick** (`tick.singleton`, `optional_first_tick`, a tick's `source_iter`).
   - They were robust, with empty lineage, so no fault could kill them. They are emitted in every tick that runs, and
     only if one runs, which depends on the tick's deliveries.
   - Also, the label planning called such a collection "local inputs only" and did not check it.
   - Now: `tick_constant` is fragile (except a dedup idiom's initial state), the constant exemption no longer covers
     `DeferTick`, and a node inside a tick is fed by whatever feeds the tick's entry points.
   - Regression: `tick_constants_are_fragile` (certified before the fix; omitting the delivery that triggers the tick
     breaks it).
4. **The dedup idiom with other readers.** The exemption ignored other readers of the idiom's state:
   `seen.clone().all_ticks()` emits carried records, which exist only if a later tick runs. Certified, while brute
   force broke it with no fault. Now the exemption requires the exclusivity of §3. Regression:
   `a_dedup_state_read_elsewhere_is_fragile`.
5. **A batch beside a snapshot** (reviewers #1 and #2). A tick that snapshots a singleton runs only once the
   singleton has a value (A6). Omitting what feeds it left a member's tick stuck, and that member never emitted the
   batched records. The batch was robust, so the label was certified; and the simulator aborted the process on the
   stuck tick, so brute force could not see it either. Now such a batch (and an `atomic`, which the simulator lowers
   to a batch) is fragile (`batch_beside_snapshot`), and under the `hydrofrac` feature the stuck tick quiesces instead
   of aborting. Regression: `review_batch_beside_an_unready_snapshot`. `BeginAtomic` also gets `Batch`'s kind check (a
   snapshot is fragile); no program showed that one break, since the simulator cannot yet yield a keyed singleton out
   of an atomic region.
6. **Impure closures** (A3). A closure with a hidden counter was robust, and in a choice-free program its one
   schedule stood for all. Now a closure (or source expression) that names outside state is fragile and a choice.
   Regression: `impure_closures_are_fragile_choices`.
7. **Label merging** (reviewer #3, fatal; brute force agreed). Every IR node with one `(label, op, kind, cluster)` key
   was tapped into one label, so a helper labelling two collections on one line unioned them: a member missing a fact
   in one was masked by the other. Now each labelled node is its own label, and the tap pass checks it tapped each
   searched label exactly once (§11). Regression: `one_labelling_line_twice_is_two_labels`.
8. **Covered scalars** (reviewer #3). A labelled fold inherited its input stream's certificate, but agreeing sets do
   not fix a non-idempotent fold. Now scalars are not checked (§11).
9. **Silent truncation of exhaustive exploration** (reviewer #3, fatal; brute force agreed). With
   `BOLERO_RANDOM_TEST_TIME_MS` set, `exhaustive` stopped after one schedule and both engines certified. Now both refuse
   to run (A16). Regression: `a_time_bounded_exhaustive_run_is_refused`.
10. **The simulator's own faults** (reviewers #1 and #2): `with_crashable_*` and `with_dynamic_membership` are refused
    (A14). Regression: `the_simulators_own_crashes_are_refused`.
11. **The heal invariant** (reviewer #1) is now checked over every log (A15).

## 18. Known gaps: what certificates do not cover

- **Harness quiescing.** A `quiesce()` between two inputs removes every interleaving across it
  (`hydro_lang/src/sim/compiled.rs`, `quiesce`). The fault phase now forbids it (§12), so this is a gap only for
  what the recovery phase does: its rounds run one after another, with no faults.
- **2-member vacuity** (§10). In a 2-member cluster no member crash is admissible, so URB-2 certificates cover only
  source crashes.
- **No faults during recovery** (§12). Faults stop at the heal. A protocol whose correctness depends on retries is
  certified only when its retries run in the recovery phase, and only against faults during the fault phase.
  Without timers nothing is sent after the settle, so the heal changes nothing in such programs.
- **Liveness.** Certificates are about the end state of runs the simulator completes, and nothing here certifies
  progress.
- **Exploration gaps.** `hydro_lang`'s exhaustive exploration missed a bug that sampling found
  (`docs/design/sim-exhaustive-misses-abd-kv*.md`, ABD-KV). That is a gap in exploring schedules (against A1), not
  a liveness question.
- **Order of facts.** The specification compares sets of facts per member, not their order. Two members holding the
  same facts in different orders agree.
- **Sampled searches.** `sample_schedules` certifies nothing (`Verdict::Exhausted`), except that a fragile-free
  program's one-schedule sets are exact (§14).
- **Covering values / aggregates.** `covering_quorum` (ABD, synod) is not trusted, so its labels pay brute-force cost
  and are argued only by exploration.
- **Claim B for choice-free programs** (§13) rests on A13 (monotonicity from Hydro's types), not on the search.
  Outside the three certified cases, Claim B is checked only in explored runs, and the report says so.
- **Trusted code.** `quorum.rs:121-185` (§7), the fragile table's DFIR semantics (§3), and the fan-out lowering (§6.1)
  are trusted by reading, and checked only by the tests cited.
- **Impurity beyond the textual guard** (A3). A closure that calls a function with hidden state, or a type whose
  `Clone` or `Hash` is impure, is not detected.
- **Fact identity without `Serialize`** (A4, §11). `Debug` text or a 64-bit hash identifies facts of other types.
- **Unchecked labels.** Scalars (§11), labels with an empty domain (`Vacuous`: a fact that appears only under faults
  is never checked), and collections inside a tick are reported, not checked.
- **Whole-message omission.** `omit(m)` drops every copy of a content-identical message, so "lose the first copy,
  deliver the retry" is never explored.
- **Stuck ticks are the simulator's semantics.** A tick blocked on a snapshot that never got a value never runs in
  the simulator (A6); a deployment might run it with an empty snapshot. hydrofrac reports what the simulator does.
- **Panics in pruned sets.** Claim A covers the runs of completed schedules: a set the search prunes is not run, so a
  program panic only that set would cause is not reported.
- **Other order-observed rules.** None remain. The kill rule never reads an observed order (§5), and dead facts use
  only proved literals (§8). The weights' tie-break uses observed positions, but only to order proposals, never to
  decide soundness.
