# Handoff: code review and upstream PRs for hydrofrac, cress and sedge

Written 2026-10-27 by the Infinity root thread `70aca8b8`, for an agent that reviews the integrated tools and cuts PRs against `hydro-project/hydro`.

## 1. Rules

- **Never push to `upstream`** (`hydro-project/hydro`). Its push URL is set to `DISABLED` in the user's clone. PRs are opened from branches on the user's fork, `origin` (`git@github.com:palvaro/hydro.git`).
- **Commit messages:** Conventional Commits, with a Markdown body (`AGENTS.md`). Breaking changes get `!` and a `BREAKING CHANGE:` footer.
- **Code:** pure, idiomatic Hydro. Protocol state lives in Hydro collections and operators, never in hand-written Rust state inside `scan`/`fold` closures. No in-program capacity simulation, no mocks.
- **Claims:** never claim a result you haven't verified. Say what you ran.
- **PR order:** the user wants upstream PRs in this order: **hydrofrac, then cress, then sedge**.
- **Polish:** the user prioritizes working prototypes over polish, and is wary of showing unreviewed work upstream. Review before proposing any PR, and ask the user before opening one.

## 2. The base: `integ/frac-cress-sedge` @ `ebd4272032`

This is upstream `main` `858ba1a6b2` (2026-10-23) plus 13 commits, already in PR order. It is on the fork.

| # | sha | commit | PR |
|---|---|---|---|
| 1 | `0926bce036` | feat(hydro_lang): simulator extension points for hydrofrac, behind the `hydrofrac` feature | hydrofrac |
| 2 | `1bdd95d96d` | fix(hydro_std): `collect_quorum_with_response` output no longer depends on batching (a real bug hydrofrac found; regression test) | standalone, can go first |
| 3 | `2cf54fe725` | feat(hydrofrac): the crate | hydrofrac |
| 4 | `e4f8fa80cc` | test(hydro_std,hydro_test): hydrofrac target corpus and checks | hydrofrac |
| 5 | `295f57245f` | docs(hydrofrac): user guide and design notes | hydrofrac |
| 6 | `8d8f8a9629` | feat(hydro_std): `reliable_send`, resend-until-acked delivery over a lossy link | hydrofrac, or standalone |
| 7 | `1f261916a9` | chore: rustfmt the hydrofrac port | squash into 1–6 |
| 8 | `ea1eb018dd` | feat(hydro_lang,copy_span): name `sliced!` hooks by the line of their own `use::` call | cress |
| 9 | `ab05779fca` | feat(hydro_lang): simulator entry points for externally driven schedules | cress |
| 10 | `75600b9168` | feat(cress,cress_config_macro): the crates and `scripts/cress` | cress |
| 11 | `fda9af1746` | test(hydro_test): cress corpus and checks (retry service, Raft, 2PC, witnesses) | cress |
| 12 | `d62d66d2aa` | feat(sedge,sedge_macro,hydro_deploy_integration): sedge, capacity imposed from outside via containers | sedge |
| 13 | `ebd4272032` | test(hydro_test): `sedge` feature | sedge |


**Verified at `ebd4272032`** (Rust 1.98.1):
- `cargo check` of hydro_lang, hydrofrac, cress, sedge, hydro_std, hydro_test (`--all-targets --features hydro_test/cress,hydro_test/sedge`): OK, with 9 cosmetic warnings (unnecessary qualifications in `sim/graph.rs:454` and `sedge/src/verdict.rs`; unused fields in `witnesses/blind_budget/harness.rs:98`).
- `cargo test -p cress`: 8/8. `cargo test -p sedge --lib`: 20/20. `cargo test -p hydrofrac --features cross-check`: 183/183.
- `cargo test -p hydro_std --lib hydrofrac`: 12 passed, 3 ignored.
- `cargo test -p hydro_test --features cress --lib -- rpc_retry_check cress_inputs_check`: 3/3.
- `cargo test -p hydro_test --lib cluster::ec_`: 66 passed, 0 failed, 10 ignored.

**Not verified:**
- that commits 1–7 build alone (needed for a separate hydrofrac PR); expected but not checked;
- clippy on the whole stack;
- `hydro_test/tests/ec_schedule_choices.rs` (it compiles; it wasn't run).

## 3. What was left out of the base, deliberately

- **The fork's research** (the 75 commits on `origin/main`): `feedback_campaign`, the `sim/provenance*` analysis, `CrashHook`/`MembershipHook` with staged sends, `consensus_gauntlet`, typed consensus, and similar. hydrofrac does NOT depend on them. It injects its own faults through `SimNetworkHook` and refuses the simulator's own crash injection (`refuse_sim_faults`). They are preserved on `origin/main` and under `archive/2026-10-26/*`.
- **27 `ec_inference_demos` research tests** that need the fork's crash and membership injection. They are compiled out as `#[cfg(any())] // fork-only sim`. Delete or port them before a PR.
- **One small `sim/provenance.rs` stays** with the helpers the hydrofrac crate uses (e.g. `apply1`); the fork's analysis is gone.
- **`relay_broadcast`** (`hydro_test/src/cluster/relay_broadcast.rs`) is not canon: it hides protocol state in a hand-written `OutstandingTable` inside `scan`. Do not revive it. The same goes for `hydro_test/src/distributed/reliable_delivery.rs`.
- **Design docs, run data, PDFs and images,** except `design_docs/sedge_outside_capacity.md` and `design_docs/sedge_outside_capacity/demo_rpc_retry.md`. The latter links to images that exist only in the archive.
- **Not ported yet:** the newest `hydrofrac-races` work (`b169662e51..2b186f4bd6`, 2026-10-26). That's "easier-to-read counterexample diagrams" (about 500 lines in `hydrofrac/src/lamport/dot.rs`, `implies.rs`, `check/engine.rs`, `check/results.rs`, `observe/`) plus the updated guide. It touches only the crate and the docs; port it before the hydrofrac PR. Also not ported: the fork's `OptionalHook` for `Reduce` with optional intermediates. It conflicts with upstream's new `Optional`, and nothing hit it.

## 4. Adaptations to upstream worth reviewing closely

- **hydrofrac:**
  - With the `hydrofrac` feature on, an unhooked `AtLeastOnce → ExactlyOnce` cast is a no-op, as on the fork. Without it, upstream panics on idempotent folds such as G-Set gossip.
  - In upstream, `latest()` yields an `Optional`, so a tick runs even when its snapshot is empty. As a result, `review_batch_beside_an_unready_snapshot` now expects no counterexample, and brute force and oracle 2 agree.
  - Rust ≥ 1.98 prints generic arguments in function names. hydrofrac strips them when naming labels.
  - `collect_n_sorted` was removed upstream, so tests use `collect_n_sorted_only`.
- **cress:**
  - The decision and status blobs use MessagePack (`rmp_serde::to_vec_named`), following upstream #3225, and cress no longer depends on bincode. `bincode_serialize_fn` became `default_codec_serialize_fn`.
  - The instrumentation pass also binds `HydroNode::Snapshot` (#3230). Without that, 3 of 8 cress tests failed.
- **raft.rs:** a 3-way merge. The manual `PartialEq` on `AppendEntriesRequest` no longer compares `leader`, because upstream removed that field.
- **sedge:** `BuildOutput::shared_library_path` became `shared_library_paths: Vec<_>` in `capacity.rs`.

## 5. Candidate follow-up PRs, on the `demo/*` branches (all on the fork)

These are stacked on top of the base: `demo/cress-reliable-send` → `demo/sedge-reliable-send` → `demo/budget-variants` → `demo/budget-runs` → `demo/act3-lossy`. The siblings `demo/hydrofrac-reliable-send`, `demo/frac-sweeps`, `demo/cress-sweeps` and `demo/showcase-doc` branch off along that line.

- **`2ad28e7c5f` feat(hydro_lang)** (on `demo/sedge-reliable-send`). `HydroDeploy` panicked on any non-`fail_stop` TCP channel ("Unsupported networking info: Tcp { fault: LossyDelayedForever }"). This change deploys `LossyDelayedForever` as plain TCP; `Lossy` is still refused. The argument: in-order delivery that stops at failure is one of the behaviors "delayed forever, `NoOrder`" allows. **The user has NOT approved this yet. Ask first.** It's a small, separate upstream PR.
- **cress `test_safety_only` attribute option** (`4eba67a308`, on `demo/cress-reliable-send`). Simulating lossy channels requires `.test_safety_only()`.
- **The budgeted `reliable_send` variants** (on `demo/budget-variants`): `ResendBudget { capacity, tokens_per_tick, acks_per_token }`, plus time, success and hybrid wrappers. The experiments recommend the **hybrid**; see the report. The variants need a `TotalOrder` source, unlike V0.
- **sedge:** the `=exact_name` function filter, and the `sedge_plot.py` panels for wire writes, CPU/throttling, drops and the undelivered backlog (`8c4c22e1fc`, `37d55d13c1`).
- **Tests:** the cress and sedge checks for `reliable_send` (`reliable_send_check.rs`), the hydrofrac T2b and starvation tests (`ec_reliable_send.rs`), and the cress sweep wrapper `reliable_send_sweep`.

## 6. Problems found along the way (issues or follow-ups)

1. **Sedge's verdict ignores lost work.** It judges only values offered inside its windows, so under real loss every variant said RECOVERED while 1–2k values were still undelivered. It should count outstanding lost work.
2. **`scripts/cress` wipes `conservation.tsv`/`results.tsv`** on every run, so a run on one function erases every other function's rows, which sedge reads. Use `CRESS_RESULTS_DIR` to isolate runs.
3. **The cress and sedge function filters are substring matches.** `reliable_send_pair` also matches `reliable_send_pair_*_budget`. Sedge now accepts `=exact`; cress doesn't.
4. **The `witnesses/blind_budget/` corpus isn't pure Hydro:** `service.rs` keeps a hand-written `BTreeMap` and token state in a `fold`. It also disagrees with itself on k (4 vs 5).
5. **hydrofrac's sampled fallback can miss counterexamples.** Past its schedule budget, with crashes disabled, at success-budget B = 5–7, it reported `Exhausted` (correctly not a certificate) and didn't find the counterexample.
6. **hydrofrac's cost is about 2^k** for k lossy messages before the heal, so it can't check multi-value resend loss. The open research item, keyed negation or per-key Claim B, sits next to review-3 hole R1, which has `reliable_send`'s exact shape. It is recorded in `docs/design/hydrofrac-soundness.md` §18 (on `demo/hydrofrac-reliable-send`).
7. **hydrofrac verdicts on rate-limited protocols depend on the harness's number of recovery ticks.** For example, a 1-token time bucket needs 3 ticks for 3 lost values.
8. **Cress labels any extra work HAZARDOUS,** by design. Severity is in the growth and the multiplier.

## 7. Environment

- **Rust 1.98.1** (pinned by upstream) is installed in `~/.rustup`.
- **pandoc 3.5 and typst 0.12** are in `~/.local/bin`, for the PDFs.
- **Docker** works. Sedge pins the harness and the nodes to cpusets; use `--harness-cpus`/`--node-cpus` to keep other work off them.
- **Sandboxes:** the `.git` is read-only, so do git work in a `git clone --shared` scratch clone under `$TMPDIR`, and push branches back with write permission.
- **The 16-core host is shared.** For heavy runs, partition cores with `taskset`.

## 8. Where things are

- **The integration base:** `origin/integ/frac-cress-sedge`.
- **The reliable-send study:** the full report is `docs/design/reliable-send-three-tools.{md,pdf}`; the abridged one (in progress) is `docs/design/reliable-send-three-tools-short.{md,pdf}`; both are on `origin/demo/showcase-doc`.
- **Run data** (not in git) is under `/local/home/palvarox/hydro/target/`: `sedge-demo-*`, `cress-sweeps`, `frac-diagrams`.
- **The prior handoff for the reliable-delivery demo:** `HANDOFF-reliable-delivery-demo.md`, untracked, in the user's working copy.
- **Backups of every pre-integration branch:** `origin/archive/2026-10-26/*`. 119 branches; the build artifacts were stripped from 3 of them.
