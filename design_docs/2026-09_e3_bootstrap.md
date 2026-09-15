You are continuing step 2 of the metastability project in this Hydro fork. Your working
directory must be a checkout of branch `metastable-2` at the commit that lands E3.2 or later
(history: bbd3ca02c5 step zero, 92bd747dc7 E0, 568b285878 E2, 456ba45584 program hygiene,
d4590f8fda E2b, 3a8808ed54 E3 design + this file, 17957c8674 E3.1, then E3.2 with the update of
this file). First action: run `git branch --show-current && git log --oneline -8` and confirm.
If you are on any other branch (in particular `metastable`, an aborted earlier attempt), stop
and say so.

E3.1 and E3.2 are DONE and measured; your task is E3.3. Do not redo E3.1/E3.2; read their
results and their code and build on them.

Read, in this order, before doing anything else:

1. design_docs/2026-09_amplification_as_adversarial_scheduling.md, the design record. Read
   all of it. "E3: design" defines goals, derivation counts, the side log, the operator rules
   and the three steps; "E3: results" (at the end) records E3.1 and E3.2 with every number and
   its hand computation. The E3.2 Raft table is the situation E3.3 must resolve: by ancestry,
   the five pending entries have 324 derivations under the storm (292 of them RequestVotes,
   which E3.1 could not attribute) but 800 under prompt (heartbeats and acks descend from the
   entries through the state and through the heartbeat/ack exchange), so ancestry alone has the
   same wrong sign as E2's edge count. The design's rule is: a record is a re-derivation toward
   g iff its ancestry contains g's input AND a delivery the adversary held (a negative leaf).
   E3.3 is that second conjunct. If E3.3 cannot make prompt = 0 re-derivations and storm > 0,
   growing with the run, on Raft, E3 has failed; say so.
2. hydro_test/src/cluster/rpc_retry.rs, the benign request/response program (client with a
   `RetryPolicy`, server with a `ServerConfig`) and its harnesses: `sim_tests` (E0 rounds),
   `amplification_tests` (E2: metered clock and requests sent up front, hold on responses,
   per-request hand computation in `expected`), `deploy_with_workload` and `deployed_tests`.
   The workload lives in the harness (`Workload`), not the program. Do not put anything about
   triggers, attempts or retries back into the program or on the wire.
3. hydro_test/src/cluster/raft.rs, module `amplification_tests` at the end (E2b): how the two
   `()` timer hooks are told apart by the control run, how the election timer is paced with
   `Periodic`, and the storm table. `raft_server` itself is unchanged and must stay so.
4. The E3 instrument, in this order: hydro_lang/src/sim/lineage.rs (host side: the
   `Lineage` log with `releases` (E3.1 boundary records with `held` ids per release),
   `derivations`/`drops`/`outputs`/`operators` (E3.2), `record_release`, `record_event`, and
   the analysis: `children_with`, `descendants`, `per_goal_by_ancestry_with`, `drops_at`);
   hydro_lang/src/sim/lineage_rt.rs (the program's side: `derive`, `dropped`, `output`,
   `state_version`, `LineageVec`, `PrependId`/`SplitId`, `apply`; forwards to a host sink fn
   passed through `__hydro_runtime`); hydro_lang/src/sim/lineage_pass.rs (the IR rewrite, with
   the operator rules in its module doc and the design doc's E3.2 table). Then
   hydro_lang/src/sim/edge_counts.rs and hold_schedule.rs (E2: per-edge counting,
   `HookContext`, `HoldScheduleDriver` with `Prompt`, `Metered(n)`, `Periodic`, `Hold(d)`),
   `run_hooks` in hydro_lang/src/sim/compiled.rs (the decision loop, the forcing rule, and
   where boundary releases are logged), and hydro_lang/src/sim/builder.rs `fn batch` (hooks
   built from IR metadata; `format_item_id`, and the edge key naming `T` not `(u64, T)`).
5. The existing IR passes that run before sim codegen, the precedent for E3.2's pass:
   `apply_dynamic_membership` in hydro_lang/src/sim/graph.rs and
   `splice_versioned_networks` in hydro_lang/src/sim/versioned_network.rs.
6. .kiro/steering/*.md and AGENTS.md, project rules.

Your task is E3.3 as specified in "E3: design": the perturbation link. Negative leaves from
held buffers: when a tick's derivations happen while a hook of that tick holds records
(`Release::held`, non-empty), those held records are the negative support of that tick's
derivations (the design's anti-join rule generalised: "the records that would have suppressed
it are sitting in a hook's buffer, unreleased"; in Raft the anti-join is inside `raft_step`, not
in the IR, so the tick-level rule is the one available). The log already has what is needed:
every `Derivation` carries `after_release` (the number of releases logged before it), every
`Release` carries `tick`, `member`, `edge`, `serial` and `held`; a derivation belongs to the
tick whose hook releases immediately precede it. Then: a record on an edge is a re-derivation
toward g iff its ancestry (positive edges) contains g's input record and contains a derivation
whose tick had a non-empty held set on a hook feeding it (or, sharper, whose held set contains
a record whose ancestry contains g). Show on rpc_retry that every extra record's ancestry
contains a held response and no record under d = 0 does; on Raft that under prompt the
re-derivation count is 0 for every entry and under the storm it is the election traffic and
grows with the run; hand-compute both before measuring. Decide (and record) the forcing-rule
question from the design's open list while doing it. Do not write the adversarial search (E4).
Do not modify `raft_server` or the program half of `rpc_retry.rs`. Extend the design doc with
"E3.3" under "E3: results", with the numbers and the hand-computed values they were compared
against, and update this file for E4 when done.

Rules that are not negotiable, learned the hard way on this project:

- Work in short milestones. Each ends with a command actually run and its output saved to a
  file under $TMPDIR and grepped. Never go more than ~15 tool calls without a compile or a
  test run. Run long commands ONCE with all output redirected to a file; there is no
  `timeout` on mac; escape `!` in shell; do not run stable `cargo fmt`. Builds may need
  SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk.
- Report only what you can point to output for. If something does not work, say precisely
  what and stop; do not tune assertions or configs silently. When a result contradicts the
  design's prediction (E2b did), record the contradiction, do not explain it away.
- Hand-compute the expected numbers before measuring, and compare per record (per request,
  per entry), not only in aggregate. The E2 tests show the pattern.
- Do not borrow from branch `metastable`. Its lineage carried root sets in payloads and
  labelled sources; the design doc records why both are rejected.
- Use existing Hydro programs for anything beyond the two ground truths; do not write bespoke
  gossip/heartbeat/TC programs.
- Commit on `metastable-2` only when asked, conventional-commit message recording the
  measured numbers, like 568b285878 and d4590f8fda do.

Things about this codebase that cost time to discover; do not rediscover them:

- The compiled simulation is a separate dylib. Its statics (thread-locals, globals) are not
  shared with the test process. Anything the program side must report to the host must be
  passed in as a function pointer or sent over a channel (see `println_handler` in
  `compiled.rs`). E3's side log must be shipped out this way.
- The crate under test is *staged* (copied and rewritten by stageleft) into the trybuild
  project. In the staged copy every `impl` block is dropped, `#[test]` functions are dropped,
  and `#[cfg(test)]` modules are kept only behind a feature. So: a non-test helper function
  in a test module must not call methods on a local struct (use fields or free functions);
  `include_str!` of a sibling file does not resolve there (read the file at runtime via
  `env!("CARGO_MANIFEST_DIR")`); code with `q!` closures needed by a deployed binary must
  live outside `#[cfg(test)]` (see `deploy_with_workload`).
- A hook's source location is the `sliced!` invocation, not the `use::batch` line; edges are
  keyed `file:line:col#index <element type>`. A proper operator id is on E3's list.
- The scheduler forces the last undecided hook in a tick to release at least one item if no
  other hook released. A held delivery can only stay held while a metered clock (or another
  releasing hook) is in the same tick. The E0-style harness (one clock tick then
  `quiesce()`) cannot hold anything; send inputs up front and pace them with the driver.
- Under the scheduler's round-robin the server tick in `rpc_retry` runs once per two client
  ticks, so baseline latency is 1 or 2 ticks, not constant; use the per-request measured
  baseline in hand computations.
- `TopLevelFoldHook` (top-level folds, e.g. CRDT gossip) asks a third question protocol the
  hold driver does not implement, and under the prompt driver releases one element per
  decision, not all. Top-level dataflow with no `batch` (reliable broadcast) has no hooks at
  all and is invisible to hook counting; E3's pass is where that changes.
- In `hydro_test`, `cargo test --lib cluster::` also runs deployment benchmarks that fail on
  this machine with a macOS dyld "library load denied by system policy" error. Unrelated;
  the deployed `rpc_retry` tests pass in the same run.
- The sandbox git tooling folds working changes into the top commit. To land a separate
  commit on `metastable-2`, apply the diff in the user's checkout and commit there (see how
  456ba45584 and d4590f8fda were made), then reset the sandbox to it.
- Full regression set that must stay green: `cargo test -p hydro_lang --features sim`
  (355 tests, ~6 min), `cargo test -p hydro_test --lib rpc_retry::` (9 sim + 3 deployed, ~2
  min), `cargo test -p hydro_test --lib raft::` (26, ~3 min), `cargo test -p hydro_std --lib
  amplification_control`. Run them sequentially in ONE command; never run two cargo test
  processes on this repo at once (see below).

Things learned in E3.1/E3.2; do not rediscover them:

- Landing a commit: the user's checkout /Users/palvarox/code/hydro-metastable-2 is a git
  worktree of /Users/palvarox/code/hydro-clean, so `git add`/`commit` there need write access to
  /Users/palvarox/code/hydro-clean/.git as well (ask for both). Procedure: `git diff <last
  landed commit> HEAD > patch` in the sandbox, `git -C <checkout> apply`, add the specific
  files, commit with a conventional-commit message carrying the measured numbers (see
  17957c8674). The sandbox's own git dir is under hydro-clean too; do not bother resetting it,
  diff by content.
- Two `cargo test` processes on this repo at the same time (e.g. a replayed/duplicated tool
  command) rebuild the same trybuild dylib under each other and one of them SIGSEGVs. One such
  crash was seen in E3.2 and reproduced by nothing else; the same test passed alone five times.
  If you see a SIGSEGV, first check for a concurrent run before suspecting the FFI.
- The compiled program's closures are type-checked in isolation: a `let __f = |res| ..` copy of
  an unannotated staged closure cannot infer its parameter, and neither can an immediately
  invoked one. `lineage_rt::apply(closure, arg)` works because rustc checks closure arguments
  after the other arguments. The `q!` closures are wrapped in `fnN_type_hint::<..>(..)` and can
  be `let`-bound freely.
- `by_ref`/`by_mut` handles (Raft's `raft_step`) reach the closure as DFIR handoff references
  `__hydro_singleton_ref_i`: `&T`/`&mut T` for singletons, `&Vec`/`&mut Vec` for streams where
  the Vec is `dfir_rs::bumpalo::collections::Vec`, not std. The pass shadows them per call; a
  `by_mut` singleton is assumed mutated and gets a new `StateVersion` id after every call.
- Keyed collections are `(K, (u64, V))` under the pass; `Cast` between layouts re-nests; joins,
  anti-joins, `fold_keyed`, the demux network all key on `K`. `fold_keyed`'s accumulator sees
  values only. `KeyedStream::first()` is a `Scan` with `Option<(K, U)>` output; its parents are
  per key, a generic scan's are cumulative within the tick.
- Volume: derivation records come overwhelmingly from state re-derived every tick (`outstanding`
  while held, the server backlog): 17.5k at d = 0 vs 425k at d = 120 on the E2 configuration,
  3.6M on the E0 collapse (2.26x wall time). Reachability per goal over a children index is fine
  at these sizes; do not materialise ancestor sets per record.
- Ids: the program allocates from 1; the host's own boundary ids (pass off) start at 1 << 63.
  With the pass on, the hook reports `pending_release_ids`/`buffered_ids` and the host strips
  the 8-byte id prefix from payloads, so `Record::decode::<T>()` gives `T` either way and every
  E3.1 harness check runs unchanged on an instrumented program.
- Edge keys are unchanged under the pass (the hook names its element by `T`); `HoldScheduleDriver`
  predicates and the E2 `edge_named` helpers work on instrumented programs.
- `Lineage::per_goal_by_ancestry_with(pred, goals, follow_state)`: `follow_state = false` cuts
  state-version-to-state-version edges only; on Raft it still gives 800 under prompt, because
  the heartbeat/ack exchange carries the ancestry without the state chain. Do not expect an
  analysis-time cut to replace E3.3.
- The design's E3 open question on operator ids: the pass numbers operators in traversal order
  and stores the table in `Lineage::operators` (`kind`, `file:line:col`, source line, type);
  `HydroIrOpMetadata::id` is set to it. The hooks' edge keys still use E2's location+index
  scheme; switching them was judged not worth the churn in E3.2.
