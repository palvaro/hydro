# AI Work Status

Updated: 2026-09-10
Branch: `metastable`
Research handoff: `f39e978d43ab147a1b02a4354a1b6e76e3a521ec`
Known-good checkpoint: `3ce50ee78bb34dcee6410e28bed2d26f8cf4c274`

## Trust boundary

`3ce50ee7` remains the last known-good checkpoint for the metastability ground-truth design, productive transitive-closure demo, timeout/retry deployment, weak-metastability evidence, pure-heartbeat control, and initial DFIR stage/handoff telemetry.

`f39e978d` is the research handoff for the subsequent instrumentation and analysis. It adds exact serialized network message/payload-byte accounting, controlled activation measurements, reproducible stage-record parsing, and the final outcome report. It is intentionally **not** a validated general classifier.

Start with:

- `design_docs/reports/2026-09_dfir_feedback_cycle_analysis_outcome.md` — succinct result, measurements, lessons, and blockers;
- `design_docs/reports/2026-09_dfir_activation_evidence.md` — detailed stage/network evidence;
- `design_docs/reports/2026-09_hydro_feedback_smell_inventory.md` — corrected evidence inventory and failed-predicate analysis;
- `hydro_test/src/stage_telemetry.rs` — reusable raw telemetry parsing;
- `hydro_test/src/feedback_smells.rs` — quarantined, permanently disabled record of the unsound predicates.

## What is established

- Exact serialized Hydro network messages and payload bytes can be attributed to DFIR stages.
- G-Set gossip measured 63 messages in both the 4- and 400-element runs, while serialized bytes increased from 1,512 to 101,304.
- Reliable broadcast produced traffic windows such as `[24, 72, 0, ...]` and drained.
- Multi-Paxos-live measured 32 messages / 782 bytes for one pending command and 450 messages / 12,372 bytes for twenty in the selected runs.
- Timeout/retry traces show retained-state stages emitting physical work after operational activation; the ground-truth deployment separately establishes weak metastability.
- Pure heartbeat remains the fixed-size, membership-bounded no-feedback control.

These are execution measurements for selected programs, not a repository-wide analysis.

## Why the classifier failed

The available stage/window aggregates do not provide causal provenance or a generic logical-progress signal. They cannot reliably distinguish retained-state reactivation from queue drainage, productive work spanning measurement windows, normal byte growth with larger state, or protocol progress encoded inside arbitrary state-machine logic. Compiler fusion and heterogeneous/interprocedural cycles further obscure the relevant pre/post-gate flow.

The reusable `SmellFinding` predicates are therefore quarantined and must not be treated as a linter or classifier. The underlying DFIR/network telemetry remains useful measurement infrastructure for a better analysis.

## Working-tree warning

This checkout still contains unrelated pre-existing modifications and untracked files. Do not bulk-stage, reset, or clean it. The handoff files are exactly those recorded by commits `3ce50ee7` and `f39e978d`, plus this status update.
