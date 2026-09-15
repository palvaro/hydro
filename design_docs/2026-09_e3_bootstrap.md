You are continuing step 2 of the metastability project in this Hydro fork. Your working
directory must be a checkout of branch `metastable-2` at commit d4590f8fda or later (four
commits: bbd3ca02c5 step zero, 92bd747dc7 E0, 568b285878 E2, 456ba45584 program hygiene,
d4590f8fda E2b; a fifth adds this file). First action: run
`git branch --show-current && git log --oneline -6` and confirm. If you are on any other
branch (in particular `metastable`, an aborted earlier attempt), stop and say so.

Read, in this order, before doing anything else:

1. design_docs/2026-09_amplification_as_adversarial_scheduling.md, the design record. Read
   all of it. The sections "E2: results", "E2b" and "E3: design" are your immediate context.
   "E3: design" is your task; it defines goals, derivation counts, the side-log
   representation, the operator rules, and a three-step plan (E3.1, E3.2, E3.3) each ending
   with a measurement. It also lists what the two ground truths must show, hand-computed
   before measuring, and the case where E3 must disagree with E2's counts (Raft under a
   delay: per-edge records fall, derivations per pending entry grow). If E3 cannot show that
   disagreement, E3 has failed; say so.
2. hydro_test/src/cluster/rpc_retry.rs, the benign request/response program (client with a
   `RetryPolicy`, server with a `ServerConfig`) and its harnesses: `sim_tests` (E0 rounds),
   `amplification_tests` (E2: metered clock and requests sent up front, hold on responses,
   per-request hand computation in `expected`), `deploy_with_workload` and `deployed_tests`.
   The workload lives in the harness (`Workload`), not the program. Do not put anything about
   triggers, attempts or retries back into the program or on the wire.
3. hydro_test/src/cluster/raft.rs, module `amplification_tests` at the end (E2b): how the two
   `()` timer hooks are told apart by the control run, how the election timer is paced with
   `Periodic`, and the storm table. `raft_server` itself is unchanged and must stay so.
4. hydro_lang/src/sim/edge_counts.rs and hydro_lang/src/sim/hold_schedule.rs (the E2
   instrument: per-edge counting at hooks, `HookContext`, `HoldScheduleDriver` with `Prompt`,
   `Metered(n)`, `Periodic`, `Hold(d)`), then `run_hooks` in hydro_lang/src/sim/compiled.rs
   (the scheduler's decision loop, including the forcing rule that makes holds impossible
   unless another hook in the tick releases), and hydro_lang/src/sim/builder.rs `fn batch`
   (where hooks are constructed from IR metadata; the natural home for an operator id).
5. The existing IR passes that run before sim codegen, the precedent for E3.2's pass:
   `apply_dynamic_membership` in hydro_lang/src/sim/graph.rs and
   `splice_versioned_networks` in hydro_lang/src/sim/versioned_network.rs.
6. .kiro/steering/*.md and AGENTS.md, project rules.

Your task is E3 as specified in "E3: design": lineage as side derivation records, sufficient
to compute derivations per goal, in three steps, each ending with a run whose output is
recorded. Do not write the adversarial search (E4). Do not modify `raft_server` or the
program half of `rpc_retry.rs`. Extend the design doc with an "E3: results" section per step,
with the numbers and the hand-computed values they were compared against.

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
  (350 tests, ~6 min), `cargo test -p hydro_test --lib rpc_retry::` (6 sim + 3 deployed),
  `cargo test -p hydro_test --lib raft::` (E2b takes ~4 min), `cargo test -p hydro_std --lib
  amplification_control`.
