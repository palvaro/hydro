# Cress port notes

Working notes for porting the Hydro schedule-amplification checker ("Cress") from
`/Users/palvarox/code/hydro-metastable-3` (read-only) onto upstream `hydro-project/hydro` `main`.

Upstream base: `d3dffb4afa` (feat(hydro_lang)!: add `verus_panic!` ...). Local clone:
`/Users/palvarox/code/hydro-cress-fresh`, remotes `upstream` = hydro-project, `origin` = palvaro/hydro.

Environment: every cargo command needs
`export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk` on this machine
(the default SDK's `libSystem.tbd` is rejected by `rust-lld`). Do not run stable `cargo fmt`.

## Phase 1: `rpc_retry.rs` alone

Done:

- `hydro_lang/src/sim/prompt_schedule.rs` copied verbatim from metastable-3. Verified that upstream's
  hooks (`runtime/tick_input.rs`) still phrase their bolero questions the same way: inclusive
  range = "how many to release" (answer max), exclusive range = "which one" (answer min), bool =
  "do something unusual" (answer false). `PassthroughSingletonHook` asks nothing.
- `CompiledSim::run_with_driver(driver, thunk)` added to `sim/compiled.rs` after `fuzz_repro`,
  mirroring it with a caller-supplied `bolero` `Driver`. `run_with_scheduler` made `pub(crate)`.
- `SimFlow::run_with_driver` and `SimFlow::run_prompt` added to `sim/flow.rs`.
- `hydro_test/src/cluster/rpc_retry.rs` ported from the metastable-3 working tree with three
  omissions: the `#[amplification_check]` attributes and `#[derive(SimOutputs)]` (return in
  Phase 4) and the `deployed_tests` module (localhost trybuild deployment; dropped so that
  `cargo test --lib rpc_retry` never launches anything). `deploy_with_workload` is kept.
  Registered in `hydro_test/src/cluster/mod.rs`. No API drift: compiled unchanged.

Commands:

- `cargo check -p hydro_lang --features sim` -> ok.
- `cargo check -p hydro_test --lib --tests` -> ok, 0 errors.
- `cargo test -p hydro_test --lib rpc_retry -- --nocapture --test-threads=1` -> 3 passed in 42 s.
  Collapse trajectory under three attempts: tail (rounds 600..800) completed 0 of a baseline 400,
  abandoned 400, served first=333 again=667, backlog 1038 -> 1237. One attempt: peak backlog 420,
  tail backlog 0. No trigger: all healthy.
- `cargo test -p hydro_lang --features sim --lib sim::` -> 123 passed, 3 ignored (unchanged suite).

Note on commits: the sandbox tooling folds every edit into the single sandbox commit; the worker
cannot create separate commits (`.git` of the parent clone is read-only from the sandbox). Phase
boundaries are recorded here instead. Phase 1 files: `hydro_lang/src/sim/{prompt_schedule.rs,
mod.rs, compiled.rs, flow.rs}`, `hydro_test/src/cluster/{rpc_retry.rs, mod.rs}`.

## Phase 2: Cress on the simulator's scripted hooks (design change from metastable-3)

The user asked that Cress reuse upstream's scripted-hook facility instead of extending the scheduler.
Result: no scheduler change at all. The only simulator additions remain `PromptScheduleDriver` and
`run_with_driver` (Phase 1), plus the self-contained location fix below.

- `hydro_lang/src/sim/scripted_schedule.rs` (new): `instrument(&mut SimFlow)` is an IR pass run by
  the checker after the user's function built the flow. It sets `op.sim_hook_id` on every batch and
  snapshot the simulator can script (streams, keyed streams, singletons, keyed singletons; not
  snapshots of top-level folds over unordered input, not `Optional` snapshots, which keep their
  autonomous hooks and are answered by the prompt driver) and tees every `Network` input into a
  `SendExternal` sim output mapped to `()`, which the host counts per sender location and member.
  `Schedule` is the host-side loop: every bound hook is put in `auto_pause`; `settle()` repeats
  {quiesce; next tick (round robin) with anything waiting; install `All` / `RevealNext` /
  `RevealLatest` per hook of that tick} until nothing waits. A held hook never receives a decision.
  Decisions are bincode blobs of unit-payload variants (`BatchDecision::<()>::All` etc.), identical
  for every item type, so the checker needs no type knowledge. Admitted counts come from the hooks'
  `status_blob()` (`buffered`) before each release.
- `hydro_test/src/cluster/rpc_retry_check.rs` (new, prototype): hand-wired sweep on rpc_retry,
  2 req/round for 125 rounds, horizon 1000, timeout 40, capacity 5.
  `cargo test -p hydro_test --lib rpc_retry_check -- --nocapture --test-threads=1`: 2 passed, 27 s
  (about 1 s per 1000-round run; 11 hooks x 11 lengths + baseline per configuration).
  Three attempts: unheld client sends 250; arrivals batch (rpc_retry.rs:302) held 64 -> +93 client
  sends, 128 -> +496 (746 total, the target), 256/512/1000 -> +496. Response batch: +48 at 64,
  +280 at 128, +496 from 256. Request batch: +481 at 128. Metrics hooks: 0 everywhere.
  One attempt: no count rises under any hold (only drops from work pushed past the horizon): benign.
  m3's appendix (data in every round) also puts the first reaction at 64 and +48 on the response
  batch at 64. Exact parity on every figure is checked in Phase 5 against the hand-wired m3 tests.
- Location naming ported from m3 commit 60b38fa26b (copy_span `span_location!`,
  `Backtrace::user_location`, `sliced!` records the `use::` span, `location_for_op` prefers it).
  Without it every hook in a `sliced!` block in another crate reports the `sliced!` line. This also
  fixes upstream's own "scripted hook has buffered input but no decision" messages for such hooks.
  Sim suite 123 passed after the change; the `sliced!` trace snapshots are unchanged.

Known deviations from m3 to carry into the report/limitations:
- `Optional` snapshots (e.g. rpc_retry's `backlog_depth`, now an `Optional` in upstream) cannot be
  held; the report will list them as not holdable. m3 held it and found zero gain.
- A tick triggered by an unbound (autonomous) hook runs with the bound hooks contributing nothing
  that step; the loop releases them on the next step. Same records, one tick later.
- Keyed snapshots are advanced with `RevealLatest` (no per-key "next" decision exists).
- Binding a scripted hook requires the item type to be serde-serializable (a dylib compile error
  otherwise). All rpc_retry types qualify; the corpus will show how often this bites.

## Phase 3: `sim::amplification::check` on the scripted schedule (acceptance)

- `hydro_lang/src/sim/amplification.rs` ported from the m3 working tree. `check` now runs
  `instrument` on the `SimFlow`, compiles once, and every run is `run_with_driver(PromptScheduleDriver)`
  with a `Schedule` driving the bound hooks; `hold_one_hook.rs` and `work_counts.rs` are not ported.
  `WorkCounts` lives in this module and is built from `scripted_schedule::Counts`. `Report` gains
  `not_holdable` (decision points the simulator cannot script), printed after the curves.
  Verdict rule, ranking, growth label and rendering are unchanged from m3.
- `hydro_test/src/cluster/rpc_retry_check.rs`: asserts verdict, location (`rpc_retry.rs:302`),
  first reaction 64, client sends 250 at k=0 and 746 at k=128/256/512/999, one not-holdable point;
  one attempt benign.
  `cargo test -p hydro_test --lib rpc_retry_check -- --nocapture --test-threads=1`: 2 passed, 38 s.
  Three attempts: growth stops, multiplier 2.98x the unheld run's 250 sends from the client.

Cosmetic difference from m3: senders print as `Process(loc1v1)` rather than by type name. Could use
the flow's location names later.

## Phase 4: attribute, derive, harness, `cargo-check-amplification`

- `hydro_lang/src/sim/amplification_harness.rs`, `hydro_amplification_macro/` and
  `cargo-check-amplification` ported from the m3 working tree (the command-line-runner design:
  checks are registered under `cfg(hydro_amplification)` via `ctor`; the script builds a tiny
  runner binary under `target/amplification/runner` with `RUSTFLAGS=--cfg hydro_amplification`).
  Runner's stageleft pin updated to the workspace's 0.16.0. Workspace `unexpected_cfgs` check-cfg
  gains `cfg(hydro_amplification)`.
- `hydro_amplification_macro` is a workspace member and an optional dependency of `hydro_lang`
  enabled by `sim`. `hydro_test` gains a default feature `sim = ["hydro_lang/sim"]`; the attribute
  and derive on `rpc_retry.rs` are behind `cfg_attr(feature = "sim", ...)` so the library builds
  without it.
- `./cargo-check-amplification hydro_test rpc_with_retries`:
  `✓ BENIGN  rpc_with_retries (one_attempt)` and
  `! HAZARDOUS  rpc_with_retries (three_attempts)  growth: stops  multiplier: 2.98x  location:
  hydro_test/src/cluster/rpc_retry.rs:321, batch of Request<u64>` (the arrivals batch; the line
  moved from 302 because the attributes sit above the function). 46 s / 49 s per configuration on
  a warm build (the generated harness also feeds the two metrics-tick streams every round, which
  the hand-wired test leaves silent, so it runs 9 decision points). First run after a clean
  build: 5 m 16 s of runner build plus the dylib build.

## Phase 5: corpus port and parity

Ported unchanged from the m3 working tree: `hydro_test/src/cluster/witnesses/` (12 programs,
`blind/` 7, `blind_budget/` service + 6 policies + harness, `blind_check.rs`,
`blind_budget_growth.rs`), `hydro_test/src/cluster/pure_heartbeat.rs` (its m3 test module depended
on m3-only stage telemetry; only the sim test `fixed_gain_without_feedback` is kept),
`hydro_test/src/local/productive_tc.rs`. `g_set_gossip` moved from m3's
`hydro_std/src/ec_inference_demos/crdt_gossip.rs` to `hydro_test/src/cluster/witnesses/crdt_gossip.rs`
(the `_live` variant, which needs the rest of those demos, dropped). `checker_experiments.rs` and
`library_check` not ported. `cargo check -p hydro_test --lib --tests`: 0 errors, no program edits.

- `blind_budget_growth`: the test constant `ARRIVALS_EDGE` said `service.rs:531`; the m3 working
  tree's uncommitted edit to service.rs moved that batch to line 539 (the checker named 539, which is
  the arrivals batch). Constant updated. `cargo test -p hydro_test --lib blind_budget_growth --
  --nocapture --test-threads=1`: 6 passed in 271 s. Labels: unbounded linear 34.8/round, stall
  38.6/round; no_governance stops 2.98x; time_budget stops 1.56x stall 139; success_budget stops
  1.14x stall 10; hybrid stops 1.56x; circuit_breaker stops 1.12x. All as m3 recorded.

## Phase 5 handoff (first worker's open items, verbatim from its closing report)

The first worker's sandbox tooling wedged before it could write these down; the successor pasted
them here first.

Phase 5 state at handoff: whole corpus ported, compiles with zero program edits. blind_budget_growth
6/6. blind_check first pass: bounded_fanout/token_bucket benign; log_catchup hazardous :227 first 8
+825 sends (m3 :226/8/825, one-line diff unexplained); two_phase_commit hazardous :248 first 4
+246504 (m3 :247/4/246504); credit_flow FALSE HAZARDOUS (+2 everywhere at hold 1), a within-round
tick-order artifact; visibility_queue failed only because hydro_lang was edited mid-run (trybuild
refuses recompiling the dylib); batch_flush ignored as in m3.

1. UNVERIFIED FIX in scripted_schedule.rs: Schedule now orders ticks by
   BTreeMap<(root LocationId, member, tick LocationId)> and always picks the first with data (no
   rotating cursor), mirroring how the simulator re-admits ticks by location. Rerun rpc_retry_check,
   blind_budget_growth, blind_check (credit_flow must be benign; nothing else should move) and the
   sim suite.
2. Rerun blind_check::blind_visibility_queue (m3: hazardous visibility_queue.rs:207 batch of
   Delivery, first 8, 165336 extra sends).
3. PERF: two_phase_commit horizon 1000 took ~17 min (m3 36 s); horizon 100 -> 31 s, 300 -> 17 s
   total, so per-message cost in the storm (~250k msgs/run). Env knobs added for profiling:
   AMPLIFICATION_TRACE=1 (per-run tick executions/status reads/messages/admitted),
   AMPLIFICATION_HORIZON=n (blind_check config), AMPLIFICATION_NO_SEND_COUNT=1 (instrument skips
   the network tee). Suspect: drain_sends does one try_next().await per message (each poll runs
   script_unconsumed_description + SettlePauseGuard); fix by bulk-draining the receiver or a
   cheaper count channel. Remove/document the env knobs before the PR.
4. Full sweep `./cargo-check-amplification hydro_test` vs the 27-row m3 table (overview.md lines
   1049-1077). First run pays ~5 min runner build.
5. Cosmetic: senders print as Process(loc1v1) (m3 did too).
6. The sandbox target/ dir was lost once when the sandbox path changed; logs under $TMPDIR were
   GC'd; keep results in tracked files.

## Phase 5 (successor): verification and performance

Correction to handoff item 3: m3's two_phase_commit check took 361 s, not 36 s (overview.md,
"blind: two_phase_commit", `run time: 361.1 s`; the sweep table says 362.8 s). The port's ~17 min
was therefore about 2.8x m3, not 28x.

- Sim suite with the tick-ordering fix: `cargo test -p hydro_lang --features sim --lib sim::` ->
  123 passed, 3 ignored (target/logs/sim_suite.log).
- `rpc_retry_check` with the tick-ordering fix and the old per-message drain: hazardous at the
  arrivals batch, first reaction 64, client sends 250 -> 746 at 128/256/512/999, 2.98x, stops. The
  test's stale `rpc_retry.rs:302` constant (Phase 4 moved the batch to line 321) is replaced by a
  lookup of the `use::batch(incoming,` line in the source.
- Perf fix: `drain_sends` used `try_next().await` once per message and once per (send, member) on an
  empty channel; each call locks the receiver, builds a settle guard and checks the script barrier,
  and the empty call runs the scheduler's settle handshake. Replaced by
  `SimReceiver::drain_delivered` / `SimClusterReceiver::drain_delivered` (`pub(crate)`, in
  `sim/compiled.rs`): a synchronous poll of the output channel with a no-op waker that removes and
  counts what is already delivered. Valid because `settle` reaches quiescence before draining.
- Line numbers in m3's sweep table (overview.md lines 1049-1077) are one lower than the port's for
  every witness that carries `workload_rounds = 125`: m3 commit dc4ea76756, made after that table was
  captured, added that one attribute line above each function (34 files). So m3 `log_catchup.rs:226`
  is the port's `:227` (the `appends` batch) and m3 `two_phase_commit.rs:247` is the port's `:248`;
  the witness sources are byte-identical to m3's working tree (checked with `diff`).
- `blind_budget_growth` after the drain fix: 6 passed in 95 s (was 271 s); every label identical
  to m3's table in growth_label.md 3b.
- `blind_check` after the tick-ordering fix: credit_flow benign (was falsely hazardous, +2);
  bounded_fanout and token_bucket benign; log_catchup hazardous at :227 (first 8, +1 message,
  825 over the curve, as m3).
- `blind_check` after both fixes: 6 passed, 1 ignored (batch_flush), 907 s of test time;
  two_phase_commit 562 s (was ~1020 s; m3 361 s with four checks sharing the machine),
  visibility_queue 298 s (m3 211 s). Both curves match m3 figure for figure (two_phase_commit
  246504 at the 999 hold on four hooks, 170090/182351/... on the first; visibility_queue 165336,
  +437 and +152818 at the peak). Named points :248 and :208 are m3's :247 and :207 (+1, see above).
- Profiling knobs removed: `AMPLIFICATION_TRACE` (amplification.rs) with the `tick_runs` and
  `status_reads` counters it read, `AMPLIFICATION_NO_SEND_COUNT` (scripted_schedule.rs), and
  `AMPLIFICATION_HORIZON` (blind_check.rs, now `CheckConfig::default()` as in m3).
  `AMPLIFICATION_RESULTS_DIR` stays: the script sets it and the harness documents it.
- Clippy (CI runs `cargo clippy --all-targets -- -D warnings` in default, no-default-features and
  all-features configurations): `hydro_lang --features sim` had 28 warnings, all in the ported
  checker files, now 0 (`#[expect]` with reasons, `let ... else`, `if let`, `copied()`,
  `InputCounter::next` renamed `next_value` because clippy confuses it with `Iterator::next`; only
  generated code called it). `rpc_retry.rs` gets `#[expect(clippy::too_many_arguments)]` on
  `rpc_with_retries`. The corpus witnesses still carry 66 clippy warnings (casts, `is_multiple_of`,
  doc backticks, module-inception); they are byte-identical to m3 and belong to the corpus branch,
  which would need a cleanup pass before any upstreaming of its own.
- `cargo clippy -p hydro_test --lib --tests --no-default-features` fails on the corpus branch: the
  corpus modules use `hydro_lang::sim::amplification` unconditionally (only `rpc_retry.rs` is
  gated with `cfg_attr(feature = "sim", ...)`), so without `sim` the `hydro_test` lib does not
  build. Gating the module declarations with `#[cfg(feature = "sim")]` was tried and reverted: the
  stageleft dylib is built with `--no-default-features --features <features found on the running
  binary>,hydro___test,hydro___feature_sim_runtime`, and under `cargo-check-amplification` the
  running binary is the generated runner, on which no features are found, so the gated modules
  vanished from the dylib and every corpus check failed to compile. Two fixes are available for
  the corpus branch: gate each witness file like `rpc_retry.rs` (edits to 17 m3-identical files),
  or have the runner's generated `Cargo.toml` mirror the checked crate's features so that
  `features::find()` reports them. The PR branch, which has no corpus, is clean in both
  configurations.
- Reviewer point, `hydro_test` default feature `sim`: the attribute's expansion is entirely under
  `#[cfg(hydro_amplification)]`, which only `cargo-check-amplification` sets, and the runner
  enables `hydro_lang/sim` itself. The attribute could therefore be written
  `#[cfg_attr(hydro_amplification, hydro_lang::sim::amplification::amplification_check(...))]`
  with no `sim` feature on `hydro_test` at all; the cost is that a mistyped attribute would be
  caught only by the runner, not by `cargo check`. Left as the first worker had it.
- Reviewer point: `HydroNode`/`HydroRoot` size assertions in `compile/ir/mod.rs` moved from
  280/152 to 288/160 bytes for the 8-byte `UserLocation` field on `Backtrace` (ignored tests that
  run only with every feature on).
- BUG found by the first full sweep, fixed: `instrument` teed only `Network` nodes whose input was a
  `CollectionKind::Stream` and silently skipped every other kind. A `demux` to a cluster has a
  `KeyedStream<MemberId, T>` input, so every cluster-targeted program (`bounded_fanout`, both
  `election`s, both `gossip_with_resend`s) ran with 0 network messages counted; the election rows
  named the wrong hook with a 1.03x/1.10x multiplier over admitted records instead of m3's
  `from 0` over sends. Fix: a keyed input is flattened with `HydroNode::Cast` to a stream of
  entries (what `KeyedStream::entries` emits) before the `Map(|_| ())` tee; any other input kind
  now panics with a message instead of being skipped. After the fix the election rows read 4000
  messages in the unheld run and match m3 on every figure (3948/13/3780 and 3944/3936/3764).
- `cargo-check-amplification` symlinked the workspace `Cargo.lock` into the generated runner; the
  runner's own `cargo run` resolution was written back through the link (496/443-line churn in the
  first worker's sandbox and again here). The script now copies the lockfile; `Cargo.lock` was
  restored to upstream's content plus the `hydro_amplification_macro` entry (10 added lines).
- Sweep rows recorded before the counting fix are kept in target/logs/results_prefix_before_fix.tsv.

## Phase 5 result (successor)

Full sweep `./cargo-check-amplification hydro_test` after the counting fix: 34 configurations, exit 0,
about 33 minutes of check time (results in `design_docs/cress_port/sweep_results.tsv`, per-row
comparison in `design_docs/cress_port/parity_table.md`). Against m3: every verdict (13 benign,
21 hazardous), every first-reaction hold, every shape and every multiplier agree; every named
decision point is the same `use::` operator after the line shift, except `lease_renewal
resend_after_4`, where a 13-record (0.09%) difference in one hold's peak flips a tie between the
`Ack` batch (m3) and the server clock batch (port), both edges of the same loop, with verdict,
first reaction, shape and multiplier unchanged. Cause: the port runs ready ticks in a fixed
(location, member) order one per settle step, where m3's driver followed the simulator's ready
order; visible only on this 20-member program.

Witnesses' own labeling tests, `cargo test -p hydro_test --lib witnesses::`: 88 passed, 4 failed,
1 ignored, 570 s. The four failures are all `crdt_gossip` (`g_set_gossip`, moved from m3's
`hydro_std`): upstream's simulator has `todo!("non-trusted observe_nondet not yet supported ...
at top-level locations")` in `sim/builder.rs` for `assume_retries` on an unordered top-level stream,
which m3's older simulator accepted. Not a port defect; the program has no attribute in m3 either.

### Files, by group

1. Simulator additions in `hydro_lang` core (neutral, no scheduler change): `sim/compiled.rs`
   (`CompiledSim::run_with_driver`, `pub(crate) run_with_scheduler`, `drain_delivered` on both
   receivers), `sim/flow.rs` (`SimFlow::run_with_driver`, `run_prompt`), `sim/mod.rs` (module
   declarations), `sim/prompt_schedule.rs` (new), `sim/builder.rs` (`location_for_op` prefers the
   recorded `use::` position), `compile/ir/backtrace.rs` (`UserLocation`,
   `__macro_get_backtrace_at`), `compile/ir/mod.rs` (two size assertions), `live_collections/
   sliced/mod.rs` (records the `use::` span), `copy_span/src/lib.rs` (`span_location!`).
2. Checker library: `sim/scripted_schedule.rs`, `sim/amplification.rs`, `sim/amplification_harness.rs`.
3. Macro and script: `hydro_amplification_macro/`, `cargo-check-amplification`, `Cargo.toml`
   (member, check-cfg), `hydro_lang/Cargo.toml` (optional dep under `sim`), `Cargo.lock` (+10).
4. rpc_retry: `hydro_test/src/cluster/rpc_retry.rs`, `rpc_retry_check.rs`, `hydro_test/Cargo.toml`
   (`sim` feature, default), `hydro_test/src/cluster/mod.rs`.
5. Corpus (separate branch): `hydro_test/src/cluster/witnesses/**`, `pure_heartbeat.rs`,
   `local/productive_tc.rs`, `local/mod.rs`; `design_docs/cress_port/` (sweep results, parity table).

### Known limitations

- `Optional` snapshots cannot be scripted by upstream's simulator and so cannot be held; the report
  lists them as not holdable (rpc_retry's `backlog_depth` is one).
- Binding a scripted hook requires serde-serializable item types (dylib compile error otherwise);
  every program in the corpus qualifies.
- Keyed snapshots are advanced with `RevealLatest` (no per-key "next version" decision exists).
- A tick triggered by an unbound (autonomous) hook runs with the bound hooks contributing nothing
  that step; they release on the next step. Same records, one tick later. Together with the fixed
  tick order this is the source of the lease_renewal near-tie flip.
- Network sends whose input is neither a stream nor a keyed stream make the checker panic rather
  than run uncounted; none exist in the corpus.
- `assume_retries` on unordered top-level streams is unsupported by upstream's simulator (the
  `crdt_gossip` failures above).
- The corpus branch does not build with `--no-default-features` (see above); the PR branch does.
- Run time is about 1.5x to 2x m3's on the storming programs (one scheduler round trip per tick).

## Phase 6: review fixes and the `cress` crate (third worker)

The reviewer's report on the first PR branch found four things to fix before a PR and several a
maintainer would push back on. All were taken, and the user asked at the same time for the checker
to leave `hydro_lang` for its own crate. The result supersedes the "Files, by group" list above.

### What changed

- **Snapshot holds** (reviewer 1, 2). The schedule now tracks which snapshot instances have ever
  revealed a version. A held snapshot that has never revealed anything is revealed as the ordinary
  schedule would when its tick is about to run (m3's "nothing to pin yet" rule, which the first
  port dropped); without it the simulator aborted the process. Every snapshot decision is now
  `RevealLatest`, so a released snapshot jumps to the newest queued version in one step, and under
  the unheld schedule a tick never runs once per stale queued version (a fold emits its initial
  value and then one version per element, so two versions queue before the first tick). Keyed
  snapshots already did this. Unit tests: `cress/src/tests.rs` (7 tests on a one-tick program with
  a batch and a snapshot; `cargo test -p cress`).
- **`Verdict::NotExplored`** (reviewer 4). A run in which no decision point ever had input, or
  which admitted no records and sent no messages, is no longer printed as benign; the verdict
  carries the reason, the report prints it, the CLI prints it red with a cross, and the runner
  exits 1. Corpus effect: `pure_heartbeat` reads not explored (0 records admitted, 9000 messages,
  no decision point), where m3 read benign.
- **Refusal of pre-scripted flows** (reviewer 3): `instrument` panics naming the hooks.
- **Scriptability predicate** (reviewer 7) mirrors the builder's: `Fold` at a root location,
  unbounded, input `NoOrder` and `ExactlyOnce`, singletons only; the dead `FoldKeyed` arm is gone.
- **Iteration cap** in `settle` (reviewer 15): 1,000,000 ticks per round, then a panic naming the
  tick. Dead "forced release" fields and the duplicated verdict line removed (reviewer 8, 9). The
  macro's unused `sim_outputs` helper attribute is gone; the derive documents that it always uses
  `::cress` (reviewer 13). The script no longer hard-codes an SDK path and reads the `stageleft`
  version from the workspace manifest (reviewer 12).
- **Crate split.** `cress/` (package `cress`): `check.rs` (the former `amplification.rs`),
  `schedule.rs` (the former `scripted_schedule.rs`), `harness.rs`, `tests.rs`, `build.rs`
  (`stageleft_tool::gen_final!`, so its tests can stage programs). `cress_config_macro/` replaces
  `hydro_amplification_macro/`; `scripts/cress` replaces `cargo-check-amplification`
  (`./scripts/cress hydro_test rpc_with_retries`); results under `target/cress/`. `hydro_lang` no
  longer depends on a macro crate and no `--cfg` rustflag exists any more.
- **`hydro_lang::sim::extension`**: the public surface the checker needs, documented:
  `SimFlow::ir_mut`, `SimFlow::cluster_sizes`, `SimFlow::unused_external_ports`,
  `drain_delivered(port, member)`, `HookControl` (`location`, `set_auto_pause`, `set_hold`,
  `status_blob`) via `hook_control(id, member)`, `install_decision(id, member, blob)`,
  `bincode_serialize_fn::<T>()`. `CompiledSimInstance::run_with_scheduler` is public.
  `DecisionFuture::from_blob` is `pub(crate)`. The `prompt_schedule` docs, `run_with_driver` and
  `run_prompt` say that the driver stands in for `deterministic()` until every hook kind is
  scriptable or `deterministic()` takes a default policy, and should then be deleted.
- **Feature gating** (reviewer 5). `hydro_test`'s default features are upstream's again. It lists
  `cress = { optional = true }`; every use in the library (`use cress::...`, `derive(SimOutputs)`,
  `amplification_check`, one `impl InputValue`) is `cfg(feature = "cress")`, and the hand-wired
  checker test modules (`rpc_retry_check`, `blind_check`, `blind_budget_growth`) are compiled only
  with that feature: `cargo test -p hydro_test --features cress --lib rpc_retry_check`. The reason
  they cannot be plain `cfg(test)` modules with a dev-dependency: the simulator compiles a staged
  copy of `hydro_test` in which test modules are kept but a dev-dependency that is also an optional
  dependency is not enabled, so `cress` would be unresolved there. `scripts/cress` enables the
  feature on the checked crate and errors with instructions if the crate lacks it.

### Verification (SDKROOT set; all on this tree)

- `cargo test -p cress`: 7 passed.
- `cargo test -p hydro_lang --features sim --lib sim::`: 123 passed, 3 ignored.
- `cargo check -p hydro_test --lib`, `--lib --tests`, `--lib --no-default-features`,
  `--lib --features cress`: all clean (the corpus now builds without the feature too).
- `cargo test -p hydro_test --features cress --lib rpc_retry -- --test-threads=1`: 5 passed.
  three_attempts: hazardous, `src/cluster/rpc_retry.rs:325, batch of Request<u64>`, first
  reaction 64, client sends 250 unheld and 746 at holds 128/256/512/999, growth stops, 2.98x.
  one_attempt: benign.
- `./scripts/cress hydro_test rpc_with_retries`: one_attempt BENIGN; three_attempts HAZARDOUS,
  stops, 2.98x, `hydro_test/src/cluster/rpc_retry.rs:325, batch of Request<u64>`.
- `cargo clippy -p hydro_lang --features sim --lib`, `-p cress --lib --tests`,
  `-p cress_config_macro`: no warnings.
- `cargo doc -p hydro_lang --features sim --no-deps`: one warning, pre-existing upstream
  (`deploy/mod.rs` links `hydro_deploy`). `cargo doc -p cress --no-deps`: clean.
- `./scripts/cress hydro_test` (full sweep, 46 min of checks): 34 configurations; 13 benign,
  20 hazardous, 1 not explored (`pure_heartbeat`, see above). Against the previous sweep every
  verdict, first reaction, extra work, growth and multiplier is identical except that one row, and
  every location is the same operator one line lower (the added `cfg` line). Table:
  `design_docs/cress_port/parity_table.md`; raw: `sweep_results.tsv`.
- `cargo test -p hydro_test --features cress --lib blind_budget_growth -- --test-threads=1`:
  6 passed (the `ARRIVALS_EDGE` constant moved to `service.rs:540`).
- `cargo test -p hydro_test --features cress --lib blind_check -- --test-threads=1`: 6 passed,
  1 ignored (`batch_flush`, as in m3), 853 s; log_catchup +825 sends first at 8, two_phase_commit
  +246504 first at 4, visibility_queue +165336 first at 8: m3's figures.

### hydro_lang diff size

`git diff --stat d3dffb4afa -- hydro_lang copy_span`: 11 files, +580/-26. Of that, the location
fix (copy_span, backtrace.rs, ir/mod.rs, sliced/mod.rs, builder.rs) is about 190 lines, the new
`extension.rs` 157, `prompt_schedule.rs` 130 (half of it the "why this exists" docs), and
`compiled.rs`/`flow.rs`/`hooks.rs`/`mod.rs` about 110 together.

### Branches (assembled 2026-10-09 from bookmark `sandbox-a32e1414-57da-4173-84b4-e86b3d308f4f`)

`feat/schedule-amplification-checker` (the PR), five commits on upstream `d3dffb4afa`:

1. `feat(hydro_lang,copy_span): name sliced! hooks by the line of their own use:: call`
2. `feat(hydro_lang): simulator entry points for externally driven schedules`
3. `feat(cress,cress_config_macro): Cress, the schedule-amplification checker`
4. `feat: scripts/cress runner`
5. `test(hydro_test): request/reply retry service with a Cress check`

27 files, +5615/-27; of that `hydro_lang` + `copy_span` is 11 files, +578/-27. `Cargo.lock` at
commits 3 and 4 is the final lockfile without `hydro_test`'s `cress` entry (what `cargo` writes
for that tree), so every commit has a consistent lockfile.

`cress-corpus` = the PR branch plus commit 6,
`test(hydro_test): Cress witness corpus and parity results` (the witnesses, `pure_heartbeat`,
`productive_tc`, `design_docs/cress_port/`, this file). Not for the PR.

Standalone verification of the PR branch (commit 5 checked out in a scratch worktree, then
removed): `cargo check -p hydro_test --lib --no-default-features` and `--lib` clean;
`cargo test -p cress` 7 passed; `cargo test -p hydro_lang --features sim --lib sim::` 123 passed,
3 ignored; `cargo test -p hydro_test --features cress --lib rpc_retry -- --test-threads=1`
5 passed with the figures above; `./scripts/cress hydro_test rpc_with_retries` BENIGN /
HAZARDOUS stops 2.98x `rpc_retry.rs:325`, and the worktree stayed clean (no lockfile rewrite).

### Reviewer findings not taken

- 10 (multi-version clusters): `instrument` still assumes member ids `0..size`; no corpus program
  uses `next_version()`. Left for a follow-up.
- 11 (`Install` duplicates `DecisionFuture`): resolved differently; the duplicate is gone and the
  checker calls `hydro_lang::sim::extension::install_decision`, which wraps `DecisionFuture`.
- 12 in part: `--threads` was already gone from the script before this round; the runner stays
  sequential.
- 14 (commit message accuracy): the six messages were rewritten to describe the code as it is.

## Phase 7: fixes from the survey of hydro_test (fourth worker)

Four survey threads tried the attribute on every program in `hydro_test/src/{cluster,distributed,
external_client,tutorials}` outside the corpus. The programs that consume time (raft, paxos,
compartmentalized paxos, `external_client/echo`) are blocked by the upstream simulator (internal
`source_interval`/`sample_every`/`timeout` clocks abort with "no reactor running"; top-level
unbounded `Reduce` and `assume_retries` on top-level streams are `todo!()`), not by Cress; raft
runs through a wrapper. The survey found one Cress bug and three usability gaps, fixed here. All
changes are in `cress`, `cress_config_macro` and `hydro_test`; `hydro_lang` and `copy_span` are
untouched (still +578/-27).

### What changed

- **Atomic contexts** (`cress/src/schedule.rs`, `instrument`). `LocationId::Atomic(_)` is
  `is_top_level()`, so the pass bound every `use::atomic` read inside an atomic context and the
  simulator refused the dylib ("sim hooks are not supported on `batch_atomic` / `snapshot_atomic`").
  Five of the eight tutorials use that pattern and crashed the checker before its first run. The
  `Batch` arm now also matches `HydroNode::BeginAtomic` (the `.atomic()` entry, which the simulator
  compiles through the same `batch` path and hooks), returns early when the input location is
  `Atomic`, and records the hook's tick as the tick inside `Atomic(tick)`. The entry appears in
  reports as a batch at the line of the `.atomic()` call. Test:
  `cress/src/tests.rs::atomic_entry_is_the_decision_point_and_the_read_is_not` (tutorial shape;
  benign, two decision points, nothing unholdable).
- **`NoOrder` inputs** (`cress_config_macro`). The generated workload called `.send()`, which
  `SimSender<_, NoOrder, _>` lacks. The classifier records the ordering; `NoOrder` streams are fed
  with `send_many_unordered([v])` (process and cluster forms). A generic ordering or `AtLeastOnce`
  retries is a compile error with a message, since the simulator's input senders feed nothing else.
- **`KeyedStream` inputs** (`cress_config_macro`). `KeyedStream<K, V, Loc, Unbounded, O, R>`
  parameters were classified as plain values. They are now inputs: the harness opens a `sim_input`
  of `(K, V)`, converts it with `.into_keyed()`, and feeds pairs from `InputValue`. The `round`
  closure's field is `SimSender<(K, V), O, R>`.
- **All-senders row** (`cress/src/check.rs`). Every curve of a program that sends anything prints
  "extra messages, all senders" under "extra messages, busiest sender", so that a role migration
  (one member up, another down, total flat, as in raft's leadership change) is visible without
  arithmetic. The verdict rule is unchanged. On rpc_retry the arrivals-batch curve now shows
  client +496 against a total of +248: the server sends 248 fewer replies.
- **Examples**. `hydro_test/src/cluster/two_pc.rs` carries the attribute with the default generated
  workload (a `NoOrder` input, a cluster, a generic payload): benign control. New
  `hydro_test/src/cluster/cress_inputs_check.rs` (gated on `cress`, like `rpc_retry_check`): a
  counter with a `KeyedStream` input, a `NoOrder` input and an atomic context, annotated, plus a
  `#[test]` that runs the registered check through `harness::run_registered_checks` and asserts
  benign. The tutorial files carry no attribute.

### Verification (all with `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`)

- `cargo test -p cress`: 8 passed.
- `cargo test -p hydro_lang --features sim --lib sim::`: 123 passed, 3 ignored.
- `cargo test -p hydro_test --features cress --lib rpc_retry -- --test-threads=1`: 5 passed;
  three_attempts hazardous at `src/cluster/rpc_retry.rs:325, batch of Request<u64>`, first
  reaction 64, client sends 250 -> 746 at 128/256/512/999, stops, 2.98x; one_attempt benign.
- `cargo test -p hydro_test --features cress --lib cress_inputs_check`: 1 passed (18.6 s).
- `./scripts/cress hydro_test rpc_with_retries`: BENIGN one_attempt; HAZARDOUS three_attempts,
  stops, 2.98x, `rpc_retry.rs:325`.
- `./scripts/cress hydro_test two_pc`: BENIGN three_participants (71 s including the runner build).
- `cargo clippy -p cress -p cress_config_macro`: 0 warnings.
- `cargo check -p hydro_test --lib`, `--lib --tests`, `--lib --no-default-features`,
  `--lib --features cress`: clean.
- Full sweep `./scripts/cress hydro_test`: 66 min wall clock (busier machine than the 46-min run);
  36 configurations = the 34 corpus rows plus `two_pc` and `keyed_counter_with_unordered_gets`.
  Every corpus row identical to the previous sweep in verdict, location, first reaction, extra
  work, growth and multiplier (`diff` of the two `results.tsv` after dropping the time column shows
  only the two added rows). Runner exit 1 as designed, because `pure_heartbeat` is not explored.
  `design_docs/cress_port/sweep_results.tsv` replaced with this run.
- `cargo test -p hydro_test --features cress --lib blind_budget_growth -- --test-threads=1`:
  6 passed (82 s). `blind_check`: 6 passed, 1 ignored (611 s). Both as before.

### Branches (reassembled from bookmark `sandbox-32541199-17ff-4683-b27c-952af3705048`)

Same six commit groups as Phase 6, same messages except: commit 3 mentions atomic contexts,
`NoOrder` and `KeyedStream` inputs, and the all-senders row; commit 5 adds `two_pc` and
`cress_inputs_check`. Commits 1 and 2 are reused unchanged (`58f1036cfe`, `1181ec4b82`), since
`hydro_lang` and `copy_span` did not change in this pass. New commits: 3 `f159ced532`, 4
`14bc1e4b66`, 5 `eaa95d8953`, 6 `955b787741`. `cress-corpus` -> commit 6. The PR branch
`feat/schedule-amplification-checker` could not be moved by the worker because the main worktree
had it checked out (with an uncommitted edit to `raft.rs`); it still points at the previous
commit 5 (`3051e5d7e9`). From the main worktree, `git reset --keep eaa95d8953` moves it and keeps
the uncommitted edit.

Standalone verification of commit 5 (`eaa95d8953`) in a scratch worktree, then removed:
`cargo check -p hydro_test --lib --no-default-features` clean; `cargo test -p cress` 8 passed;
`cargo test -p hydro_test --features cress --lib rpc_retry -- --test-threads=1` 5 passed,
hazardous at `rpc_retry.rs:325`; `cargo test -p hydro_test --features cress --lib
cress_inputs_check` 1 passed; `./scripts/cress hydro_test rpc_with_retries` BENIGN / HAZARDOUS
stops 2.98x `rpc_retry.rs:325`, worktree clean afterwards.
