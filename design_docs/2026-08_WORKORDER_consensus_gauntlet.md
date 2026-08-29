# WORK ORDER: `consensus_gauntlet` — the standardized comparison harness

2026-08. Self-contained brief for a fresh session. Read this file, then the
references in §6, then start. Branch: **`consensus-gauntlet`** (new branch
off `eventual_consistency`). A sibling session is concurrently rewriting
the internals of `multi_paxos{,_live}.rs` on `eventual_consistency`
(`2026-08_WORKORDER_perf_bymut.md`); their public signatures are frozen —
code against them, never edit them, and treat every existing backend as
**read-only**.

## 1. Why

This repo has now hand-built the same comparison approximately five times:
a novel consensus implementation vs. the library raft/paxos, measured on
correctness (sim + Maelstrom lin-kv), performance (bench_client
throughput/latency), and complexity/trust (grep censuses, LOC counts, the
accounting doc's Tier-0). Every round re-invents wiring, drivers, and
report formats, and the results are scattered across commit messages and
doc prose. Standardize it: **one harness, parameterized by a backend, that
runs the gauntlet and emits one report.**

## 2. Shape

A new workspace crate, `consensus_gauntlet`, depending on `hydro_test` +
`hydro_std`. Stand-alone-ish on purpose: depending only on public APIs is
the forcing function that keeps backend interfaces honest. **Wrap, don't
move**: `consensus_bench.rs`, `lin_kv.rs`, `bench_client`, and the
backends stay where they are for now (the sibling session runs them);
consolidation/deletion is a follow-up after both branches land.

### The backend adapter

Do not invent a grand unifying trait on day one. The de-facto interface
already exists — the lin-kv wiring shape:

```text
fn build(cluster, cluster_size, requests, timers…) → committed stream
```

Standardize that as a small trait or builder-closure type whose output is
canonical: `(slot: usize, value: Option<V>)` committed entries at every
node (colocated roles), plus whatever timer inputs the backend needs
declared as inputs (the raft/BTC/multi_paxos pattern: sim and deployment
drive them; the harness owns the intervals). First four instances:

1. `raft::raft` (reference; adapter exists in spirit as
   `raft_lin_kv_server` + `raft_bench`).
2. Library Paxos via `PaxosLike` (`paxos_with_client.rs`; note it claims
   no output label and forwards `NonDet` obligations — the report should
   surface that, not hide it).
3. `broadcast_transcript_consensus`.
4. `multi_paxos_live` (colocated; splice-cursor + `(client, msg_id)` dedup
   — the existing `multi_paxos_lin_kv_server` shows the required state
   machine; its dedup exists because the redo queue legitimately
   duplicates, and the canonical-form adapter must preserve that).

Backend capabilities differ; the adapter must declare them rather than
fake uniformity: `supports_partition_nemesis` (multi_paxos's core
hardcodes `fail_stop`, so no), which timers it needs, checkpointing
support. The report prints capability gaps as first-class results.

### The three gauntlet tiers

1. **lin-kv ladder** (Maelstrom, `#[cfg_attr(not(maelstrom_available),
   ignore)]`): smoke (20 s) → kill nemesis (60 s, 3 reps) → partition
   nemesis (45 s, 3 reps, only if the capability allows). Identical
   configs across backends — that identity is the point: a failure
   attributes to the protocol, not the harness. Copy the exact configs
   from `lin_kv.rs`'s existing tests.
2. **Performance**: the `bench_client` closed loop (pinned member-0
   leader, `inc_i32` workload), standardized warmup-discard (first 3
   windows) and steady-state stats (min/median/mean/max over 12 windows),
   plus the latency percentiles per window so **growth curves** are
   visible in the report, not just steady-state medians — a climbing p50
   at flat load is a finding (it caught a real quadratic bug in
   multi_paxos on this harness's first run).
3. **Complexity & trust census** (Tier 0 of
   `2026-08_trust_and_complexity_accounting.md` §9-§10): per backend,
   protocol-body LOC, seams by kind (S1 consistency asserts, S2 algebra
   proofs, S3/S4 assumers+introducers, S5 forwarded `NonDet` params),
   kernel count (`sliced!`), cut count, cycle count (`forward_ref`).
   Scripted (syn-based parsing beats grep; grep is acceptable v1 with the
   body/test split done properly — split on the first line-anchored
   `#[cfg(test)]`). This gives the accounting doc's measurement protocol
   its permanent mechanical home; A-bucket classification stays manual
   and out of scope here.

### The report

One entry point (a binary or a single ignored-by-default test) → one
markdown report: a table per tier, backends as columns, capability gaps
explicit, environment header (host, date, commit). Human-pasteable into
design docs — that is its job.

## 3. What NOT to do

- Do not port the per-protocol **sim safety suites** into the harness:
  their drivers are protocol-specific by design (Ω discipline differs per
  protocol). The harness's correctness tier is the external checker
  (Maelstrom/Knossos), which is protocol-agnostic by construction.
- Do not edit `multi_paxos{,_live}.rs`, `raft.rs`, `paxos.rs`, or the
  existing bench/lin_kv files except: additive `pub` visibility where the
  new crate needs access (each such change is one line and must be
  flagged in the commit message).
- Do not chase a single scalar score. The report is a profile; the
  accounting doc §11's Goodhart warning applies to this harness directly.

## 4. Acceptance

1. `cargo test -p consensus_gauntlet` green (harness unit tests: adapter
   canonicalization, census script against a fixture).
2. The perf tier runs locally against raft + multi_paxos_live and
   reproduces the known result shape (raft flat ~60k req/s territory;
   multi_paxos slow — possibly fixed by the sibling session by then;
   numbers land in the report, not in prose claims).
3. The lin-kv tier compiles and runs against raft with `MAELSTROM_PATH`
   set (if unavailable on the machine, the entries must at least build —
   same standard as the existing ignored tests).
4. The census tier emits the §9 table for all four backends and matches
   the hand-counted numbers already recorded in the accounting doc
   (raft: 1 S1 / 1 S2 / 2 S5, etc.) — that match is the harness's own
   red test.
5. A sample report committed under `design_docs/reports/` as the format's
   worked example.

## 5. Branch & merge discipline

Work on `consensus-gauntlet`; rebase over `eventual_consistency` before
finishing (the sibling lands there). Conventional commits. Push the
branch; do not merge to `eventual_consistency` yourself — the merge is
reviewed after both sessions land.

## 6. Read before starting

- `hydro_test/src/maelstrom/lin_kv.rs` — the three existing backends'
  lin-kv wiring + the Maelstrom test configs (your tier-1 source of
  truth).
- `hydro_test/src/cluster/consensus_bench.rs` — the bench harness shape,
  the pinned-leader timer trick, the throughput-scrape test pattern.
- `hydro_test/src/cluster/paxos_with_client.rs` — `PaxosLike`, the
  existing abstraction attempt (learn from its scope: build/with_client).
- `design_docs/2026-08_trust_and_complexity_accounting.md` — §2 (seam
  taxonomy), §9 (census protocol + the hand-counted numbers your census
  must reproduce), §11 (Goodhart).
- `design_docs/2026-08_research_agenda.md` §3 — the portfolio table the
  report ultimately feeds.
- Working rules: watchdog long test runs; wire types in `q!` must be
  `pub` for deploy-mode staging; `sliced!` requires `use::` declarations
  first; never name host generics inside `q!`.
