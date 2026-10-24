p='docs/design/hydrofrac-soundness.md'; s=open(p).read()
def rep(o,n):
    global s
    assert s.count(o)==1,o[:90]
    s=s.replace(o,n)

# §11: label identity + not-checked rules + empty domain
rep("""## 11. The host-side specification check

**Claims and proofs.**""","""## 11. The host-side specification check

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

**Claims and proofs.**""")
rep("""- **Divergent facts.** In every explored run, a fact outside the domain held by some but not all correct members is a
  counterexample (`hf/check/races_engine.rs`, `read_schedule`; the oracle: `hf/check/brute_force.rs:338`).""",
"""- **Divergent facts.** In every explored run, a fact outside the domain held by some but not all correct members is a
  counterexample (`hf/check/races_engine.rs`, `read_schedule`; the oracle: `hf/check/brute_force.rs:338`).
- **An empty domain** makes the label `Vacuous` (`hf/check/races_engine.rs`, `check`), and it is not searched; brute
  force drops such labels too. A fact that appears only under faults (a failover's output, say) is then never
  checked (§18).""")

# §12 pointer: heal invariant
rep("""**What this excludes.** A fault after the settle, such as an omission of a retry sent after the heal, is outside the
model (§18). **Checked in code.**""","""**What this excludes.** A fault after the settle, such as an omission of a retry sent after the heal, is outside the
model (§18). **Checked in code.**

**The invariant lineage relies on** (A15): after the heal no send is omitted and no crash fires. Sends after the heal
are fine. Lineage marks a delivery omittable only if it was sent before the heal (`lossy = before_heal && faulty`,
`hf/lineage/mod.rs:212`), and every delivery keeps its sender-crash dependency, before or after the heal: a node that
crashed before the heal sends nothing after it, so `crash(n)`, and `crash(n after m)` with `m` sent before the heal, kill
`n`'s later deliveries too. Checked over every instance's log when its record is taken (`hf/fault/mod.rs`,
`EventLog::check_heal`, which panics). A recovery phase after the heal must keep this invariant, or change the rules
above (the recovery-phase rules are in this section's recovery part).""")

# §15: what it shares, coverage bound
rep("""**What it shares with the search:**
- the instrumented, compiled program, including the provenance rewrite (A5 and A9 below);
- the fault hook (`hf/fault/mod.rs`);
- the specification evaluation (`hf/spec.rs`);
- the bounds.

A bug in any of these would hit both sides. A bug in lineage, literals, the kill rule, the program, completion,
dead facts, quorum lineage or the policies would show as a disagreement.""","""**What it shares with the search:**
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
argument alone.""")

# §16 assumptions
rep("""| A3 | User closures are pure (no hidden state, no I/O) | ASSUMED; closures that read Hydro state by reference are detected and fragile (CHECKED IN CODE, `hf/provenance/fragile.rs`, `reads_state`) |
| A4 | Records are compared by content; messages are content-addressed | CHECKED IN CODE (`hf/fault/mod.rs:154`, `MessageId`; facts are bytes) |""",
"""| A3 | User closures are pure (no hidden state, no I/O): each output is a function of the inputs | ASSUMED for functions closures call. Partly CHECKED IN CODE: a closure that reads Hydro state by reference is fragile (`reads_state`); a closure or source expression that names outside or shared state is fragile and a choice (`names_impure`, a textual guard, §3) |
| A4 | Records are compared by content; messages are content-addressed; facts are identified by `bincode`, else `Debug`, else a 64-bit hash (§11) | CHECKED IN CODE for `Serialize` types (`hf/fault/mod.rs:154`, `MessageId`; `hf/observe/tap.rs`, `tap_closure`); ASSUMED that `Debug` is injective, and the hash collision-free, on the labelled values of other types |""")
rep("""| A6 | Quiescence means no tick can run: every batch hook has released its pending input, and a tick ran for it. A tick runs only when every hook is ready (`SingletonHook::is_ready`, `hydro_lang/src/sim/runtime.rs:591`) and some hook can release (`compiled.rs:2022`). Nothing else makes a tick run (§17, second fix) | CHECKED IN CODE (`compiled.rs:2022`, `quiesce` at `:292`) |""",
"""| A6 | Quiescence means no tick can run (`SimTick::can_run`, `hydro_lang/src/sim/compiled.rs:2022`): a tick runs only when every one of its hooks is ready and some hook can release. A hook that is not ready blocks its tick even with input pending: a snapshot of a singleton that never got a value (`SingletonHook::is_ready`, `hydro_lang/src/sim/runtime.rs:591`; every other hook is always ready, `:78`). So a batch's pending input is released at quiescence only if its tick snapshots no singleton (§3, `batch_beside_snapshot`). Nothing else makes a tick run (§17, fix 2). Under the `hydrofrac` feature such a stuck tick no longer aborts the process: the run quiesces with that member never running the tick (`compiled.rs`, the "never became ready" check) | CHECKED IN CODE (`compiled.rs:2022`, `quiesce` at `:292`) |""")
rep("""| A13 | A Hydro program with no schedule choice is monotone: non-monotone operators need a bounded input, which only a tick provides, and outside data enters a tick only through a batch (a choice) | ASSUMED (Hydro's type system); used only for Claim B in choice-free programs (§13) |""",
"""| A13 | A Hydro program with no schedule choice is monotone: non-monotone operators need a bounded input, which only a tick provides, and outside data enters a tick only through a batch (a choice) | ASSUMED (Hydro's type system); used only for Claim B in choice-free programs (§13) |
| A14 | Membership is static, and only hydrofrac injects crashes: no `with_dynamic_membership` (its `MembershipHook` makes joins a schedule choice), no `with_crashable_process`/`with_crashable_cluster` (the simulator's own `CrashHook`, whose crashes are not in the fault hook's record, so its crashed members would count as correct) | CHECKED IN CODE: `EcCheck::instrument` sets `SimFlow::refuse_sim_faults`, and `compiled()` panics if either was used, before or after (`hydro_lang/src/sim/flow.rs`) |
| A15 | After the heal no send is omitted and no crash fires (sends are fine); every delivery keeps its sender-crash dependency (§12) | CHECKED IN CODE (`hf/fault/mod.rs`, `EventLog::check_heal`, over every instance's log) |
| A16 | `exhaustive` runs to the end: bolero's exhaustive driver stops silently at `BOLERO_RANDOM_TEST_TIME_MS` and returns normally | CHECKED IN CODE: both engines refuse to start an exhaustive exploration with the variable set (`hf/check/races_engine.rs`, `refuse_truncated_exhaustive`); exhaustive runs ignore bolero's other limits |""")

# §17
rep("""In both, no corpus certificate changed: no corpus program relied on the unsound entries.""",
"""In both, no corpus certificate changed: no corpus program relied on the unsound entries.

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
11. **The heal invariant** (reviewer #1) is now checked over every log (A15).""")

# §18 additions
rep("""- **Other order-observed rules.** None remain.""","""- **Impurity beyond the textual guard** (A3). A closure that calls a function with hidden state, or a type whose
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
- **Other order-observed rules.** None remain.""")
rep("""- **Heal before retries** (§12). Faults stop at the heal. A protocol whose correctness depends on retries is
  certified only against faults during the settle; omissions of messages sent after it are not modelled.""",
"""- **Heal before retries** (§12). Faults stop at the heal. A protocol whose correctness depends on retries is
  certified only against faults during the settle; omissions of messages sent after it are not modelled. Without
  timers nothing is sent after the settle, so the heal changes nothing in such programs.""")
rep("""- **Liveness.** Certificates are about the end state of runs the simulator completes. `hydro_lang`'s exhaustive
  exploration can miss a liveness bug that sampling finds (`docs/design/sim-exhaustive-misses-abd-kv*.md`, ABD-KV),
  and nothing here certifies progress.""","""- **Liveness.** Certificates are about the end state of runs the simulator completes, and nothing here certifies
  progress.
- **Exploration gaps.** `hydro_lang`'s exhaustive exploration missed a bug that sampling found
  (`docs/design/sim-exhaustive-misses-abd-kv*.md`, ABD-KV). That is a gap in exploring schedules (against A1), not
  a liveness question.""")
open(p,'w').write(s)
print('ok')
