You are continuing step 2 of the metastability project in this Hydro fork. Your working
directory must be a checkout of branch `metastable-2` at the commit that lands the negative
leaves and the redo-queue result or later (history: bbd3ca02c5 step zero, 92bd747dc7 the
canonical program and the sim collapse, 568b285878 per-edge counting and the hold schedule,
456ba45584 program hygiene, d4590f8fda Raft election storms, 3a8808ed54 lineage design,
17957c8674 boundary ids, bffbb372cb the lineage pass, then negative leaves + multi_paxos_live).
First action: run `git branch --show-current && git log --oneline -8` and confirm. If you are on
any other branch (in particular `metastable`, an aborted earlier attempt), stop and say so.

Read, in this order, before doing anything else:

1. design_docs/2026-09_amplification_as_adversarial_scheduling.md, the design record, all of
   it. The last four sections matter most: "Negative leaves" (the per-record rule, exact on
   rpc_retry, bracketing on Raft), "What lineage is for, revised" (lineage is the explanation and
   the move proposer; the *verdict* is derivations per goal compared across two schedules on the
   same inputs, swept over the perturbation), "A third program: the redo queue in
   multi_paxos_live" (the first result on code not written for this project and not a
   leader-election theorem: bursty delivery of a proposer's own completion notifications past its
   timer period makes it re-propose its whole backlog every stall, 3.5x the accept traffic and up
   to 5 decrees per command), and "Next: the sweep", which is your task list.
2. The three harnesses, which are the sweep's validation set and show every idiom you need:
   hydro_test/src/cluster/rpc_retry.rs `amplification_tests` (metered clock, `Hold(d)`, per-request
   hand computation in `expected`, the negative-leaves test), hydro_test/src/cluster/raft.rs
   `amplification_tests` (two `()` timer hooks told apart by a control run, `Periodic` pacing,
   the storm table, the negative-leaves test with its d = 1..3 control), and
   hydro_std/src/ec_inference_demos/multi_paxos_live.rs `amplification_tests` (commands as the
   metered clock, the completion self-hop as the held edge, campaigns and decrees per command).
   The programs under test are unchanged and must stay so.
3. The instrument: hydro_lang/src/sim/edge_counts.rs and hold_schedule.rs (per-edge counting,
   `HookContext`, `HoldScheduleDriver` with `Prompt`, `Metered(n)`, `Periodic`, `Hold(d)`);
   `run_hooks` and the tick loop in hydro_lang/src/sim/compiled.rs (the decision loop, the
   forcing rule, `begin_tick`/`end_tick`); hydro_lang/src/sim/lineage.rs (`Lineage`: releases with
   `held`, derivations with `tick`, `per_goal` by payload, `per_goal_by_ancestry`,
   `re_derivations`); lineage_rt.rs and lineage_pass.rs only if you touch the pass.
4. .kiro/steering/*.md and AGENTS.md, project rules.

Your task is "Next: the sweep", in this order:

- The scheduler change first: a driver policy under which a hook may stay held even when no
  other hook in its tick releases (today the last undecided hook is forced to release at least
  one item, so an edge without a metered co-hook cannot be held; multi_paxos_live's acceptor-acks
  edge is the measured example). Keep the default behaviour for every existing test.
- Then the sweep as a tool, not a hand-written test: given a program, fixed inputs, the metered
  clock edge(s) and a goal extractor at the hooks, for every hook edge x {constant, bursty} x d
  report derivations per goal against the prompt run, the program's own progress signals if any
  (terms, campaigns), the threshold and whether the gain is transient or sustained. Validate it by
  reproducing the three tables in the design doc automatically before running it on anything new.
- Then run it on the rest of the corpus as controls: synod, abd, crdt_gossip, uniform_broadcast.
  Expected "no moves" or bounded; a sustained loop is a finding, record it.
- Then, if time: the redo queue under load (acceptors with a per-tick capacity): does the
  backlog run away?

Do not resume lineage precision work for opaque closures (needs a counterfactual fork; the
search loop's business), do not build a separate payload-multiset verdict (per-goal counts carry
it), and do not put anything about triggers, attempts or retries into any program or on the wire.
Extend the design doc with the numbers and the hand-computed values they were compared against.

Rules that are not negotiable, learned the hard way on this project:

- Work in short milestones. Each ends with a command actually run and its output saved to a
  file under $TMPDIR and grepped. Never go more than ~15 tool calls without a compile or a
  test run. Run long commands ONCE with all output redirected to a file; there is no
  `timeout` on mac; escape `!` in shell; do not run stable `cargo fmt`. Builds may need
  SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk.
- Report only what you can point to output for. If something does not work, say precisely
  what and stop; do not tune assertions or configs silently. When a result contradicts the
  design's prediction (the Raft election storm did: the edge count fell while the program stormed), record the contradiction, do not explain it away.
- Hand-compute the expected numbers before measuring, and compare per record (per request,
  per entry), not only in aggregate. The hold-schedule tests in rpc_retry show the pattern.
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
  `compiled.rs`). The lineage side log is shipped out this way.
- The crate under test is *staged* (copied and rewritten by stageleft) into the trybuild
  project. In the staged copy every `impl` block is dropped, `#[test]` functions are dropped,
  and `#[cfg(test)]` modules are kept only behind a feature. So: a non-test helper function
  in a test module must not call methods on a local struct (use fields or free functions);
  `include_str!` of a sibling file does not resolve there (read the file at runtime via
  `env!("CARGO_MANIFEST_DIR")`); code with `q!` closures needed by a deployed binary must
  live outside `#[cfg(test)]` (see `deploy_with_workload`).
- A hook's source location is the `sliced!` invocation, not the `use::batch` line; edges are
  keyed `file:line:col#index <element type>`. The pass assigns a proper operator id (`Lineage::operators`), the hooks do not use it yet.
- The scheduler forces the last undecided hook in a tick to release at least one item if no
  other hook released. A held delivery can only stay held while a metered clock (or another
  releasing hook) is in the same tick. The round-per-clock-element harness (one clock tick then
  `quiesce()`) cannot hold anything; send inputs up front and pace them with the driver.
- Under the scheduler's round-robin the server tick in `rpc_retry` runs once per two client
  ticks, so baseline latency is 1 or 2 ticks, not constant; use the per-request measured
  baseline in hand computations.
- `TopLevelFoldHook` (top-level folds, e.g. CRDT gossip) asks a third question protocol the
  hold driver does not implement, and under the prompt driver releases one element per
  decision, not all. Top-level dataflow with no `batch` (reliable broadcast) has no hooks at
  all and is invisible to hook counting; the lineage pass observes every edge and is where that changes.
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

Things learned in the boundary-ids and lineage-pass steps; do not rediscover them:

- Landing a commit: the user's checkout /Users/palvarox/code/hydro-metastable-2 is a git
  worktree of /Users/palvarox/code/hydro-clean, so `git add`/`commit` there need write access to
  /Users/palvarox/code/hydro-clean/.git as well (ask for both). Procedure: `git diff <last
  landed commit> HEAD > patch` in the sandbox, `git -C <checkout> apply`, add the specific
  files, commit with a conventional-commit message carrying the measured numbers (see
  17957c8674). The sandbox's own git dir is under hydro-clean too; do not bother resetting it,
  diff by content.
- Two `cargo test` processes on this repo at the same time (e.g. a replayed/duplicated tool
  command) rebuild the same trybuild dylib under each other and one of them SIGSEGVs. One such
  crash was seen once during the lineage-pass work and reproduced by nothing else; the same test passed alone five times.
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
  while held, the server backlog): 17.5k at d = 0 vs 425k at d = 120 on the hold-schedule configuration (b = 2, tau = 40, 300 ticks),
  3.6M on the 800-round sim collapse (2.26x wall time). Reachability per goal over a children index is fine
  at these sizes; do not materialise ancestor sets per record.
- Ids: the program allocates from 1; the host's own boundary ids (pass off) start at 1 << 63.
  With the pass on, the hook reports `pending_release_ids`/`buffered_ids` and the host strips
  the 8-byte id prefix from payloads, so `Record::decode::<T>()` gives `T` either way and every
  boundary-log harness check runs unchanged on an instrumented program.
- Edge keys are unchanged under the pass (the hook names its element by `T`); `HoldScheduleDriver`
  predicates and the `edge_named` helpers work on instrumented programs.
- `Lineage::per_goal_by_ancestry_with(pred, goals, follow_state)`: `follow_state = false` cuts
  state-version-to-state-version edges only; on Raft it still gives 800 under prompt, because
  the heartbeat/ack exchange carries the ancestry without the state chain. Do not expect an
  analysis-time cut to replace the negative leaves (and see the design doc for what those can and cannot do).
- The design's open question on operator ids: the pass numbers operators in traversal order
  and stores the table in `Lineage::operators` (`kind`, `file:line:col`, source line, type);
  `HydroIrOpMetadata::id` is set to it. The hooks' edge keys still use the location+index
  scheme; switching them was judged not worth the churn.

Things learned in the negative-leaves step and on multi_paxos_live; do not rediscover them:

- A tick with buffered items is runnable (`SimTick::can_run`), and the forcing rule then makes the
  last undecided hook release: a `Periodic` or `Hold` policy on a hook that is alone in its slice
  is silently a prompt policy (measured: every policy on `quorum`'s acks batch gave the prompt
  numbers). Pace a co-hook in the same slice (a metered input stream is the usual one) or change
  the scheduler.
- The hook's decision index counts every decision including earlier phases of the harness (Raft:
  the heartbeat ticks are decisions 3–10, not 1–8). Express per-tick hand computations relative
  to the first release of the edge in question.
- Hook keys are `file:line:col#index <type>`; two hooks of the same type in one slice (Raft's two
  `()` timers, multi_paxos_live's `u32` commands and completions) are told apart by a control run
  (record counts, or the serial of the first non-empty release in the lineage log).
- `Lineage::re_derivations` walks ancestors per record on the edge: fine on Raft (7k derivations),
  103 s on rpc_retry at d = 120 (482k). Do not run it on the 800-round sim collapse without reformulating.
- The anti-join now emits a derivation per surviving record (it used to pass the id through), so
  derivation counts on rpc_retry are ~10% higher than the lineage-pass step's table; every other
  number there is unchanged and re-asserted.
- No lineage pass is needed for the count-based verdict: `SimFlow::run_traced` without
  `with_lineage()` gives the boundary log, `Lineage::per_goal` decodes payloads at the hooks, and
  that is what all three tables were produced with.
- Regression set, run sequentially in ONE command: `cargo test -p hydro_std --lib -- multi_paxos_live::amplification_tests amplification_control`
  (~70 s), `cargo test -p hydro_lang --features sim` (~6 min), `cargo test -p hydro_test --lib --
  --test-threads=1 rpc_retry:: raft::` (~25 min; the negative-leaves test on rpc_retry alone is
  ~9 min, mostly analysis).
