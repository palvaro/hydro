# AI Work Status

Updated: 2026-09-14
Branch: `metastable`

## Read this first: what was actually established

The foundation this project was supposed to start from does **not** exist:

- There is no timeout/retry program written in Hydro dataflow. `hydro_test/src/distributed/timeout_retry.rs`
  keeps the client's outstanding-request table and the service's queue in Rust collections mutated inside
  closures (`by_mut`/`by_ref`). The retry mechanism lives in Rust; Hydro only carries the messages. The
  provenance tracker therefore sees every retry-loop measurement only as a marked over-approximation
  ("coarse"). This was written by an earlier AI session and never flagged.
- The retry program was never shown to be in a metastable state. `finite_burst_enters_weakly_metastable_regime`
  observes 12 requests (~1.5 s) after a burst, then stops load. That shows a backlog clearing under a short
  timeout, not a self-sustaining bad regime under continued baseline load. The report's phrase "weak metastable
  regime" overstates the measurement.
- Transitive closure was never empirically stressed; it is a simulated demo with counters plus an argument.
- Commit `fa2319b25d` claimed a generic feedback checker. It was generic instrumentation around hand-designed
  experiments. The corrective matrix work (this tree) is real but: input roles (data vs operational) are
  *declared* by the test author via `sim_input_operational`, not discovered; the previous matrix never fired
  two operational inputs together, so the retry loop never closed in it; zero-work cases vanished from output.
- The transitive-closure rows in every matrix so far are an artifact: the demo requires the whole graph in one
  message, the generic generator sends one edge per message, later batches overwrite the frontier. TC
  gated-vs-ungated comparisons in the table are **not evidence about the gate**.

No claim in this repository about the provenance tracker's correctness has been independently audited; the
tracker agrees with hand-written expectations on four small programs (the tests below).

## What changed in this session (all in this commit)

- `EmissionRecord.source`: records carry the emitting root location.
- `feedback_campaign.rs`: an "all operational inputs fired together" case; explicit `NoEmission` rows and a
  per-case/per-epoch ledger; label-free stopping shape (size, literal data-ancestry set, op-tag presence,
  coarse flag, payload only for data-bearing emissions) with a stability window of `max(configured, scale+1)`;
  `no_lineage` column; `fired_tag_msgs` / `other_node_msgs` / `other_node_msgs_exact` / `returned_msgs`
  (work at other nodes carrying this firing's tag, and how much is addressed back to the firing node);
  `terminal_by_scale`; `EvidenceMatrix::compare` for paired program versions; manifest section in output.
- `provenance_evidence_matrix.rs`: baseline plus variants (retry ± service dedup, TC ± known gate under chain
  and cyclic generators, gossip ± pump, identity-map refactors), three seeds. Asserts refactor pairs are
  identical (they are). Mechanism pairs are recorded, not asserted.
- `timeout_retry.rs`: `LossPolicy.dedup_requests_at_service` (default off). `productive_tc.rs`:
  `ungated_transitive_closure` variant; original behaviour unchanged.

Raw output: `design_docs/reports/2026-09_protocol_blind_feedback_evidence.tsv` (regenerated). Headline rows,
seed 0x5a17, both timers fired: without dedup the service emits 108 responses and its tail is `Unresolved`
at scale 32 (budget 100 firings exhausted); with dedup, 41 responses (one per request), `EmptyTail`.
Client-side rows identical. All downstream attribution on retry is coarse (`other_node_msgs_exact = 0`).
Gossip: peers emit 44 messages carrying the sender's timer tag, 0 returned; bytes 12/40/136 at scales 1/8/32.

## Validation run this session

`cargo test -p hydro_lang --features sim --lib feedback_campaign`: 6 passed.
`cargo test -p hydro_test --lib provenance`: all listed tests passed (matrix test ~2m45s).
Both with `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`. Do not run stable `cargo fmt`.

## If this work is resumed

1. Write timeout/retry with the outstanding table and the service queue as Hydro operators.
2. Deploy it, burst it, keep baseline load running for a minute, and measure whether attempts/request stays
   above 1. Tune until it does or explain why it cannot.
3. Attempt the same on TC and fail.
4. Only then apply the provenance instrument.

## Working-tree warning

The checkout contains unrelated pre-existing modifications and untracked files (`dyn_raft.rs`, `abd.rs`,
`quorum.rs`, docs, images). Do not bulk-stage, reset, or clean it.

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
- `hydro_lang/src/sim/feedback_campaign.rs`,
  `hydro_test/src/cluster/provenance_evidence_matrix.rs`, and
  `design_docs/reports/2026-09_protocol_blind_feedback_evidence.tsv` — protocol-blind evidence
  matrix. The IR manifest enumerates every input and role; the typed registry must cover all ports
  and legal cluster targets. The runner crosses every data target with every operational target at
  scales 1/8/32 in fresh instances under two deterministic scheduler seeds. Every physical edge
  gets the same literal evidence columns; no semantic class or program-specific expected result is
  produced. Legal typed value generators remain program-specific and can bias coverage.
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

The next deliverable is an agreed decision model over a protocol-blind evidence matrix, not more
program-specific labels. The matrix now discovers every boundary input, rejects missing adapters,
enumerates all data/operational port and member-target combinations, runs scales 1/8/32 in fresh
instances under two deterministic scheduler seeds, and reports identical per-edge facts: work and
byte curves, causal ancestry predicates, payload recurrence, exact/coarse precision, tail behavior,
and fields that vary across schedules. It intentionally draws no semantic conclusion. Current raw
evidence shows that retry and gossip both have operationally triggered dominated ancestry with
state-scaled work; distinguishing capacity-consuming amplification requires a generic downstream
gain experiment. Remaining blockers: internal intervals as controllable inputs, less biased value
generation, held-out broadcast/Paxos/Raft matrices, structural gates, and downstream causal gain.
`paxos.rs` still needs its timer exposed until interval rewriting exists.

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
