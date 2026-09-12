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
- `design_docs/reports/microbus_probe/` — probe of the MicroBus catchup v2 client (MicroBus
  commit `b7f18651bccd33582a8957d37e29408012a2eeda`, branch `hydro`); our test file and its
  output only, no MicroBus source.
- `hydro_test/src/cluster/provenance_survey.rs` — 4 tests: reliable broadcast, uniform
  broadcast, Multi-Paxos (`hydro_std`), dynamic-membership Raft. `paxos.rs` deferred (needs its
  election timer threaded out).
- `hydro_test/src/cluster/provenance_buffers.rs` — 3 tests emitting the per-buffer admission
  table for the three archetypes (retry work buffer, TC fixpoint buffer, gossip fixed-rate
  buffer); the tables are dumped to `provenance_dump.txt`.
- `hydro_lang/src/sim/feedback_campaign.rs` and
  `hydro_test/src/cluster/provenance_generic_campaign.rs` — deterministic generic retained-state
  campaign. The runner owns scales (1/8/32), repeated operational firing, quiescence, stable-output
  stopping, provenance analysis, and bounded witnesses; adapters supply only legal typed values
  and typed input handles. One unchanged campaign separates heartbeat, retry, gossip, and TC.
  `SimFlow::feedback_boundary_manifest()` inventories all external inputs/roles/outputs/cycles,
  and adapter coverage is checked by numeric port ID.
- `design_docs/reports/2026-09_dfir_feedback_cycle_analysis_outcome.md` — why the telemetry
  approach failed (still accurate; the provenance work is its answer, not its rescue).

## What is established

- Under `SimFlow::with_provenance()`, every emission's lineage is exact except downstream of
  `by_ref`/`by_mut` closures, where it is a marked over-approximation. Tracking is passive:
  recorded executions replay identically.
- Retry: N requests → N `Productive`; one retry tick → N `Reactivated` funded by N data tags; one
  service pulse → 1 `Productive`; second tick → N−1 (N ∈ {1,4,9}). Loop gain N·k for k ∈ {1,2,3}.
  Black-holed responses: constant per-tick waste, never drains. Request loss inside the service:
  retries still `Reactivated` — labels describe the sender's mechanism, not a send's utility.
- Transitive closure: all output facts `Productive` with exact combination lineage; drains.
- Heartbeat: all `FixedOperational`, gain = cluster size, no data lineage.
- Gossip (unlabelled): first pump `Productive`, subsequent pumps `Reactivated`; messages constant
  in N, bytes grow; a member's output independent of pumps received.
- Raft and dyn_raft: election `FixedOperational`; replication `Productive`, bytes ∝ log; steady
  heartbeat fixed-size (labelled `Reactivated` by coarse lineage).
- Multi-Paxos: election `FixedOperational`; replication `Productive`; phase-1 covering replay on
  every new lead is `Reactivated`, 86 B per acceptor, recurring at constant size with no new
  commands — the Paxos reactivation mechanism, bounded by the uncheckpointed log.
- Reliable / uniform broadcast: all network sends `Productive`, one per (sender, recipient,
  message); drains; duplicate input does not echo.
- MicroBus catchup client: open and timeout/reopen `FixedOperational` (44 B); gap keepalive
  `Reactivated`, fixed 44 B per interval while stalled; loop closure depends on the C++ server.

## Where this is going (see report §"Which buffers to bound" / §"The per-buffer table")

The deliverable is a deterministic decision process per buffer, not a warning per program. The
first generic campaign now exists: it discovers the graph boundary, rejects adapters that omit an
input, grows typed data at fixed scales, repeats one operational input, waits for stable output or
drain, and emits bounded witnesses. Applied unchanged, it finds operational-only heartbeat work,
state-scaled retry replay, state-scaled gossip replay, and productive TC scaling without ancestry
replay. This is the first evidence that the distinction is not wholly defined by bespoke phase
scripts. Remaining blockers to “any dataflow”: rewrite internal intervals as controllable sim
inputs; make adapters declarative over every discovered port; run scales and control/trigger pairs
in fresh instances; apply unchanged to held-out broadcast/Paxos/Raft examples; infer gates and
downstream causal gain. `paxos.rs` still needs its timer exposed until interval rewriting exists.

## Known limits (see report §Limits)

Sim-time only; timers must be `sim_input_operational`. Lineage cannot distinguish "join derived a
new tuple" from "fold batched old tuples" — recurrence does. Coarse lineage downstream of opaque
state collapses labels to "first productive, rest reactivated"; `payload_hash` and the program's
own dedup gates are the fallback. History is the sender's own emission record (mechanism, not utility);
receipts are logged but not consulted by the classifier. Several node kinds unsupported (panic with a clear message).

## Environment note

Builds here require `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`: the
default 27.0 SDK's `.tbd` files are rejected by this toolchain's `rust-lld`. Do not run
`cargo fmt` with the stable toolchain on this repo; `rustfmt.toml` uses nightly-only options and
stable fmt reformats unrelated files.

## Working-tree warning

The original checkout contains unrelated pre-existing modifications and untracked files
(`dyn_raft.rs`, `abd.rs`, `quorum.rs`, docs). Do not bulk-stage, reset, or clean it. The
provenance commits contain exactly the 13 + 3 files listed in their messages.
