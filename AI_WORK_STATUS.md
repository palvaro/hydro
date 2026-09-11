# AI Work Status

Updated: 2026-09-11
Branch: `metastable` (base `f364049fef`); provenance work on sandbox branch
`sandbox-64b2e9b3-375c-408a-89af-0583efa4b9f2`; on `metastable` as `76e88cd06f`, `2767f70cbb`, `085b1c38ac`, `2599584e3f`, and the docs commit after it
Known-good checkpoint: `3ce50ee78bb34dcee6410e28bed2d26f8cf4c274`

## Trust boundary

`3ce50ee7` remains the last known-good checkpoint for the metastability ground-truth design,
productive transitive-closure demo, timeout/retry deployment, weak-metastability evidence,
pure-heartbeat control, and initial DFIR stage/handoff telemetry.

`f39e978d` / `f364049f` are the telemetry research handoff: exact network message/byte
accounting, controlled activation measurements, and the outcome report explaining why aggregate
stage/window telemetry cannot classify feedback cycles. Its predicates are quarantined.

The provenance commits (`76e88cd06f` onward) are the provenance handoff: a sim-time forward-lineage tool that
*does* classify emissions on the ground-truth programs, plus a loop-gain probe. It is a validated
measurement tool with recorded limits, **not** a repository-wide classifier and **not** a collapse
verdict.

Start with:

- `design_docs/reports/2026-09_provenance_feedback_cycle_classification.md` — approach, raw
  classifier output, results per program, lessons, limits, and the four-class hypothesis;
- `hydro_lang/src/sim/provenance.rs` — `Tagged<T>`, tags, network framing, emission log,
  `classify`/`attribute`;
- `hydro_lang/src/sim/provenance_ir.rs` — the IR pass that threads lineage through a program;
- `hydro_test/src/cluster/provenance_ground_truth.rs` — 9 tests: heartbeat, timeout/retry (+ loop
  gain, + black hole, + request loss), gossip (+ loop gain, observation only), transitive
  closure, Raft.
- `design_docs/reports/2026-09_dfir_feedback_cycle_analysis_outcome.md` — why the telemetry
  approach failed (still accurate; the provenance work is its answer, not its rescue).

## What is established

- Under `SimFlow::with_provenance()`, every emission's lineage is exact except downstream of
  `by_ref`/`by_mut` closures, where it is a marked over-approximation. Tracking is passive:
  recorded executions replay identically.
- Retry: N requests → N `Productive`; one retry tick → N `Reactivated` funded by N data tags; one
  service pulse → 1 `Productive`; second tick → N−1 (N ∈ {1,4,9}). Loop gain N·k for k ∈ {1,2,3}.
  Black-holed responses: constant per-tick waste, never drains. Request loss inside the service:
  necessary retries mislabelled `Reactivated` (the method's recorded negative result).
- Transitive closure: all output facts `Productive` with exact combination lineage; drains.
- Heartbeat: all `FixedOperational`, gain = cluster size, no data lineage.
- Gossip (unlabelled): first pump `Productive`, subsequent pumps `Reactivated`; messages constant
  in N, bytes grow; a member's output independent of pumps received.
- Raft: election `FixedOperational`; replication `Productive`, bytes ∝ log; steady heartbeat
  fixed-size.

## Known limits (see report §Limits)

Sim-time only; timers must be `sim_input_operational`. Lineage cannot distinguish "join derived a
new tuple" from "fold batched old tuples" — recurrence does. Coarse lineage downstream of opaque
state collapses labels to "first productive, rest reactivated"; `payload_hash` and the program's
own dedup gates are the fallback, and under application-level loss there is no fallback. History
advances on network *receipt*; the simulator's network is reliable, so loss must be modelled in
the program for now. Several node kinds unsupported (panic with a clear message).

## Environment note

Builds here require `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`: the
default 27.0 SDK's `.tbd` files are rejected by this toolchain's `rust-lld`. Do not run
`cargo fmt` with the stable toolchain on this repo; `rustfmt.toml` uses nightly-only options and
stable fmt reformats unrelated files.

## Working-tree warning

The original checkout contains unrelated pre-existing modifications and untracked files
(`dyn_raft.rs`, `abd.rs`, `quorum.rs`, docs). Do not bulk-stage, reset, or clean it. The
provenance commits contain exactly the 13 + 3 files listed in their messages.
