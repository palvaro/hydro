# Lineage-level ablation

A re-measurement of what each lineage level of hydrofrac (`hydro_lang::sim::ldfi::Level`) buys,
on the current code. The levels are `Execution` (L0: every input of the execution and of earlier
executions of the same tick), `Slice` (L1: L0 narrowed by the static slice of the producing
sink; the `Ldfi::new` default), `Record` (L2: record-level provenance from
`SimFlow::with_provenance`, falling back to L1 where a record carries none) and
`Counterfactual` (L3: node-local replay with deliveries removed one at a time, falling back to
L2). Each level runs the *same* search (same fault model, crash budget, exploration and
hypothesis budget) on the *same* provenance-compiled program; only the backward closure differs.

- Date: 2026-10-19.
- Commit: `ldfi-hydrofrac` at `7d58ad20e0`, measured on the `molly viz` sandbox base
  `38471d0dd1` (which adds `sim::viz` on top: trace and provenance-graph recording in
  `ldfi.rs`, off by default here, and no change to the closure or the search).
- Machine: 8 × Intel Xeon Platinum 8375C @ 2.90 GHz, 15 GiB, debug build (`cargo test`),
  rustc 1.96.0. Other measurement runs were sharing the machine, so wall times are indicative
  (±30 %), not benchmarks.
- Harnesses: each protocol's own `pub(crate) mod harness` (the one its unit tests use), driven
  by the `ldfi_ablation_*.rs` modules in `hydro_test/src/cluster/`. Each module is gated on
  `LDFI_ABLATION_OUT=<path>` (the Markdown it writes) and returns at once without it.
- Per cell: `verdict` (`CEX` = counterexample; `CERT(hand)` = certificate graded
  `Grade::HandWritten`, as every specification here is hand-written; `BUDGET`; `VACUOUS`;
  `EXHAUSTED`), `hypotheses` (`Outcome::runs().len()`, including the failure-free run),
  `schedules` (Σ schedules over hypotheses), `mean derivation size` (mean number of
  `Dependency` leaves per derivation, over every derivation of every schedule in which the
  goal held), and wall seconds for the search (compilation excluded). The detail tables add
  the number of derivations observed, how many are distinct, and the counterfactual replays
  run (`Schedule::replays`; non-zero at every level because specification views are derived
  counterfactually at the observer) with how many fell back to recorded lineage
  (`Schedule::replay_fallbacks`).

## 2PC

`hydro_test/src/cluster/ldfi_ablation_two_pc.rs` (`level_ablation_two_pc`, and the per-`n`
shards `level_ablation_two_pc_n{2,3,4}` for `--test-threads=3`), on
`two_pc_ldfi::harness::compile_with(n, ctp, txns, provenance = true, 8)`: the coordinator and
every participant crashable, links `fail_stop` (no omission), specification = agreement on
`Txn(1)` among the correct participants (`PRE`: some participant committed it; `POST`: every
correct participant did). `txns` is the workload: the other transactions are background load
on the same program (the harness's `with_txns` sets it at run time, so there is one trybuild
compilation per `(n, ctp)`).

Grid: participants `n ∈ {2, 3, 4}` × workload `load ∈ {1, 4, 8}` × crash budget
`cr ∈ {0, 1}` × `ctp ∈ {false, true}` × four levels = 144 searches. Exploration
`Fuzz { instances: 1 }` (one schedule per hypothesis, as in the earlier ablation in
`ldfi_grid.rs` the "11 vs 73" figure comes from), hypothesis budget 400, one run per cell.
Whole grid: 162 s of search in three shards. Exhaustive exploration is used as a
cross-check below; it is infeasible on the whole grid (864 schedules per hypothesis at
`n=3 load=8`, 521 s for one level of one cell).

### Plain 2PC (`ctp=false`): the blocking bug

Cell = `verdict / hypotheses / schedules / mean derivation size / seconds`.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| n=2 load=1 cr=0 | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.2s |
| n=2 load=1 cr=1 | CEX / 2 / 2 / 8.00 / 0.1s | CEX / 2 / 2 / 8.00 / 0.1s | CEX / 3 / 3 / 7.00 / 0.1s | CEX / 2 / 2 / 8.00 / 0.1s |
| n=2 load=4 cr=0 | CERT(hand) / 1 / 1 / 32.00 / 0.1s | CERT(hand) / 1 / 1 / 32.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.3s |
| n=2 load=4 cr=1 | CEX / 6 / 6 / 28.00 / 0.5s | CEX / 6 / 6 / 28.00 / 0.4s | CEX / 4 / 4 / 6.67 / 0.1s | CEX / 2 / 2 / 8.00 / 0.2s |
| n=2 load=8 cr=0 | CERT(hand) / 1 / 1 / 64.00 / 0.1s | CERT(hand) / 1 / 1 / 64.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.3s |
| n=2 load=8 cr=1 | CEX / 6 / 6 / 60.00 / 0.3s | CEX / 6 / 6 / 60.00 / 0.4s | CEX / 2 / 2 / 8.00 / 0.1s | CEX / 2 / 2 / 8.00 / 0.4s |
| n=3 load=1 cr=0 | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.2s |
| n=3 load=1 cr=1 | CEX / 2 / 2 / 12.00 / 0.1s | CEX / 2 / 2 / 12.00 / 0.1s | CEX / 4 / 4 / 10.67 / 0.2s | CEX / 2 / 2 / 12.00 / 0.2s |
| n=3 load=4 cr=0 | CERT(hand) / 1 / 1 / 48.00 / 0.1s | CERT(hand) / 1 / 1 / 48.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.3s |
| n=3 load=4 cr=1 | CEX / 8 / 8 / 42.00 / 0.5s | CEX / 8 / 8 / 42.00 / 0.5s | CEX / 2 / 2 / 12.00 / 0.1s | CEX / 6 / 6 / 10.40 / 0.9s |
| n=3 load=8 cr=0 | CERT(hand) / 1 / 1 / 96.00 / 0.1s | CERT(hand) / 1 / 1 / 96.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.6s |
| n=3 load=8 cr=1 | CEX / 8 / 8 / 90.00 / 0.9s | CEX / 8 / 8 / 90.00 / 1.1s | CEX / 6 / 6 / 10.40 / 0.4s | CEX / 6 / 6 / 10.40 / 2.0s |
| n=4 load=1 cr=0 | CERT(hand) / 1 / 1 / 16.00 / 0.1s | CERT(hand) / 1 / 1 / 16.00 / 0.1s | CERT(hand) / 1 / 1 / 16.00 / 0.1s | CERT(hand) / 1 / 1 / 16.00 / 0.2s |
| n=4 load=1 cr=1 | CEX / 2 / 2 / 16.00 / 0.1s | CEX / 2 / 2 / 16.00 / 0.1s | CEX / 2 / 2 / 16.00 / 0.1s | CEX / 2 / 2 / 16.00 / 0.2s |
| n=4 load=4 cr=0 | CERT(hand) / 1 / 1 / 64.00 / 0.2s | CERT(hand) / 1 / 1 / 64.00 / 0.1s | CERT(hand) / 1 / 1 / 16.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 0.5s |
| n=4 load=4 cr=1 | CEX / 10 / 10 / 56.00 / 0.9s | CEX / 10 / 10 / 56.00 / 0.7s | CEX / 2 / 2 / 16.00 / 0.1s | CEX / 8 / 8 / 14.29 / 2.3s |
| n=4 load=8 cr=0 | CERT(hand) / 1 / 1 / 128.00 / 0.2s | CERT(hand) / 1 / 1 / 128.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 1.0s |
| n=4 load=8 cr=1 | CEX / 10 / 10 / 120.00 / 2.3s | CEX / 10 / 10 / 120.00 / 2.3s | CEX / 8 / 8 / 14.29 / 1.2s | CEX / 2 / 2 / 16.00 / 1.4s |

Detail: `derivations observed (goal-held schedules) / distinct derivations / replays / replay fallbacks`.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| n=2 load=1 cr=0 | 1 / 1 / 6 / 0 | 1 / 1 / 6 / 0 | 1 / 1 / 6 / 0 | 1 / 1 / 32 / 2 |
| n=2 load=1 cr=1 | 1 / 1 / 8 / 0 | 1 / 1 / 8 / 0 | 2 / 2 / 12 / 0 | 1 / 1 / 49 / 4 |
| n=2 load=4 cr=0 | 1 / 1 / 18 / 0 | 1 / 1 / 18 / 0 | 1 / 1 / 18 / 0 | 1 / 1 / 92 / 2 |
| n=2 load=4 cr=1 | 5 / 5 / 74 / 0 | 5 / 5 / 74 / 0 | 3 / 3 / 36 / 0 | 1 / 1 / 131 / 4 |
| n=2 load=8 cr=0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 172 / 2 |
| n=2 load=8 cr=1 | 5 / 5 / 162 / 0 | 5 / 5 / 162 / 0 | 1 / 1 / 46 / 0 | 1 / 1 / 259 / 4 |
| n=3 load=1 cr=0 | 1 / 1 / 8 / 0 | 1 / 1 / 8 / 0 | 1 / 1 / 8 / 0 | 1 / 1 / 49 / 3 |
| n=3 load=1 cr=1 | 1 / 1 / 11 / 0 | 1 / 1 / 11 / 0 | 3 / 3 / 23 / 0 | 1 / 1 / 73 / 6 |
| n=3 load=4 cr=0 | 1 / 1 / 26 / 0 | 1 / 1 / 26 / 0 | 1 / 1 / 26 / 0 | 1 / 1 / 148 / 3 |
| n=3 load=4 cr=1 | 7 / 7 / 146 / 0 | 7 / 7 / 146 / 0 | 1 / 1 / 32 / 0 | 5 / 5 / 553 / 18 |
| n=3 load=8 cr=0 | 1 / 1 / 50 / 0 | 1 / 1 / 50 / 0 | 1 / 1 / 50 / 0 | 1 / 1 / 280 / 3 |
| n=3 load=8 cr=1 | 7 / 7 / 326 / 0 | 7 / 7 / 326 / 0 | 5 / 5 / 148 / 0 | 5 / 5 / 1017 / 18 |
| n=4 load=1 cr=0 | 1 / 1 / 10 / 0 | 1 / 1 / 10 / 0 | 1 / 1 / 10 / 0 | 1 / 1 / 68 / 4 |
| n=4 load=1 cr=1 | 1 / 1 / 14 / 0 | 1 / 1 / 14 / 0 | 1 / 1 / 14 / 0 | 1 / 1 / 99 / 8 |
| n=4 load=4 cr=0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 212 / 4 |
| n=4 load=4 cr=1 | 9 / 9 / 242 / 0 | 9 / 9 / 242 / 0 | 1 / 1 / 42 / 0 | 7 / 7 / 1105 / 32 |
| n=4 load=8 cr=0 | 1 / 1 / 66 / 0 | 1 / 1 / 66 / 0 | 1 / 1 / 66 / 0 | 1 / 1 / 404 / 4 |
| n=4 load=8 cr=1 | 9 / 9 / 546 / 0 | 9 / 9 / 546 / 0 | 7 / 7 / 264 / 0 | 1 / 1 / 563 / 8 |

### 2PC with cooperative termination (`ctp=true`): the certificate

Cell = `verdict / hypotheses / schedules / mean derivation size / seconds`.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| n=2 load=1 cr=0 | CERT(hand) / 1 / 1 / 8.00 / 0.0s | CERT(hand) / 1 / 1 / 8.00 / 0.0s | CERT(hand) / 1 / 1 / 8.00 / 0.0s | CERT(hand) / 1 / 1 / 8.00 / 0.1s |
| n=2 load=1 cr=1 | CERT(hand) / 9 / 9 / 7.50 / 0.1s | CERT(hand) / 9 / 9 / 7.50 / 0.1s | CERT(hand) / 9 / 9 / 7.25 / 0.1s | CERT(hand) / 9 / 9 / 7.25 / 0.2s |
| n=2 load=4 cr=0 | CERT(hand) / 1 / 1 / 32.00 / 0.1s | CERT(hand) / 1 / 1 / 32.00 / 0.0s | CERT(hand) / 1 / 1 / 8.00 / 0.0s | CERT(hand) / 1 / 1 / 8.00 / 0.2s |
| n=2 load=4 cr=1 | CERT(hand) / 27 / 27 / 24.21 / 0.8s | CERT(hand) / 27 / 27 / 24.21 / 0.9s | CERT(hand) / 13 / 13 / 6.83 / 0.5s | CERT(hand) / 13 / 13 / 6.83 / 1.0s |
| n=2 load=8 cr=0 | CERT(hand) / 1 / 1 / 64.00 / 0.1s | CERT(hand) / 1 / 1 / 64.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.1s | CERT(hand) / 1 / 1 / 8.00 / 0.3s |
| n=2 load=8 cr=1 | CERT(hand) / 51 / 51 / 44.36 / 2.9s | CERT(hand) / 51 / 51 / 44.32 / 3.4s | CERT(hand) / 13 / 13 / 6.83 / 0.3s | CERT(hand) / 13 / 13 / 6.83 / 1.5s |
| n=3 load=1 cr=0 | CERT(hand) / 1 / 1 / 12.00 / 0.0s | CERT(hand) / 1 / 1 / 12.00 / 0.0s | CERT(hand) / 1 / 1 / 12.00 / 0.0s | CERT(hand) / 1 / 1 / 12.00 / 0.1s |
| n=3 load=1 cr=1 | CERT(hand) / 13 / 13 / 12.00 / 0.2s | CERT(hand) / 13 / 13 / 12.67 / 0.3s | CERT(hand) / 13 / 13 / 11.33 / 0.2s | CERT(hand) / 13 / 13 / 11.33 / 0.7s |
| n=3 load=4 cr=0 | CERT(hand) / 1 / 1 / 48.00 / 0.1s | CERT(hand) / 1 / 1 / 48.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.3s |
| n=3 load=4 cr=1 | CERT(hand) / 40 / 40 / 40.19 / 3.2s | CERT(hand) / 40 / 40 / 39.67 / 2.6s | CERT(hand) / 18 / 18 / 10.89 / 0.6s | CERT(hand) / 18 / 18 / 10.89 / 3.1s |
| n=3 load=8 cr=0 | CERT(hand) / 1 / 1 / 96.00 / 0.1s | CERT(hand) / 1 / 1 / 96.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.1s | CERT(hand) / 1 / 1 / 12.00 / 0.4s |
| n=3 load=8 cr=1 | CERT(hand) / 76 / 76 / 72.27 / 9.9s | CERT(hand) / 76 / 76 / 71.76 / 10.0s | CERT(hand) / 18 / 18 / 10.89 / 0.6s | CERT(hand) / 18 / 18 / 10.89 / 3.2s |
| n=4 load=1 cr=0 | CERT(hand) / 1 / 1 / 16.00 / 0.0s | CERT(hand) / 1 / 1 / 16.00 / 0.0s | CERT(hand) / 1 / 1 / 16.00 / 0.0s | CERT(hand) / 1 / 1 / 16.00 / 0.1s |
| n=4 load=1 cr=1 | CERT(hand) / 17 / 17 / 18.12 / 0.5s | CERT(hand) / 17 / 17 / 18.25 / 0.5s | CERT(hand) / 17 / 17 / 15.38 / 0.5s | CERT(hand) / 17 / 17 / 15.38 / 1.8s |
| n=4 load=4 cr=0 | CERT(hand) / 1 / 1 / 64.00 / 0.2s | CERT(hand) / 1 / 1 / 64.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 0.1s | CERT(hand) / 1 / 1 / 16.00 / 0.4s |
| n=4 load=4 cr=1 | CERT(hand) / 53 / 53 / 57.04 / 6.8s | CERT(hand) / 53 / 53 / 57.96 / 6.8s | CERT(hand) / 23 / 23 / 14.92 / 0.8s | CERT(hand) / 23 / 23 / 14.92 / 6.5s |
| n=4 load=8 cr=0 | CERT(hand) / 1 / 1 / 128.00 / 0.2s | CERT(hand) / 1 / 1 / 128.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 0.2s | CERT(hand) / 1 / 1 / 16.00 / 1.1s |
| n=4 load=8 cr=1 | CERT(hand) / 101 / 101 / 101.05 / 23.4s | CERT(hand) / 101 / 101 / 99.80 / 22.1s | CERT(hand) / 23 / 23 / 14.92 / 1.3s | CERT(hand) / 23 / 23 / 14.92 / 11.2s |

Detail: `derivations observed (goal-held schedules) / distinct derivations / replays / replay fallbacks`.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| n=2 load=1 cr=0 | 1 / 1 / 6 / 0 | 1 / 1 / 6 / 0 | 1 / 1 / 6 / 0 | 1 / 1 / 32 / 2 |
| n=2 load=1 cr=1 | 4 / 4 / 20 / 0 | 4 / 4 / 20 / 0 | 4 / 4 / 20 / 0 | 4 / 4 / 120 / 8 |
| n=2 load=4 cr=0 | 1 / 1 / 18 / 0 | 1 / 1 / 18 / 0 | 1 / 1 / 18 / 0 | 1 / 1 / 92 / 2 |
| n=2 load=4 cr=1 | 14 / 14 / 140 / 0 | 14 / 14 / 140 / 0 | 6 / 6 / 56 / 0 | 6 / 6 / 406 / 12 |
| n=2 load=8 cr=0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 172 / 2 |
| n=2 load=8 cr=1 | 22 / 22 / 356 / 0 | 22 / 22 / 356 / 0 | 6 / 6 / 104 / 0 | 6 / 6 / 726 / 12 |
| n=3 load=1 cr=0 | 1 / 1 / 8 / 0 | 1 / 1 / 8 / 0 | 1 / 1 / 8 / 0 | 1 / 1 / 49 / 3 |
| n=3 load=1 cr=1 | 6 / 6 / 42 / 0 | 6 / 6 / 42 / 0 | 6 / 6 / 42 / 0 | 6 / 6 / 315 / 18 |
| n=3 load=4 cr=0 | 1 / 1 / 26 / 0 | 1 / 1 / 26 / 0 | 1 / 1 / 26 / 0 | 1 / 1 / 148 / 3 |
| n=3 load=4 cr=1 | 21 / 21 / 324 / 0 | 21 / 21 / 324 / 0 | 9 / 9 / 126 / 0 | 9 / 9 / 1172 / 27 |
| n=3 load=8 cr=0 | 1 / 1 / 50 / 0 | 1 / 1 / 50 / 0 | 1 / 1 / 50 / 0 | 1 / 1 / 280 / 3 |
| n=3 load=8 cr=1 | 33 / 33 / 876 / 0 | 33 / 33 / 876 / 0 | 9 / 9 / 246 / 0 | 9 / 9 / 2053 / 27 |
| n=4 load=1 cr=0 | 1 / 1 / 10 / 0 | 1 / 1 / 10 / 0 | 1 / 1 / 10 / 0 | 1 / 1 / 68 / 4 |
| n=4 load=1 cr=1 | 8 / 8 / 72 / 0 | 8 / 8 / 72 / 0 | 8 / 8 / 72 / 0 | 8 / 8 / 643 / 32 |
| n=4 load=4 cr=0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 34 / 0 | 1 / 1 / 212 / 4 |
| n=4 load=4 cr=1 | 28 / 28 / 584 / 0 | 28 / 28 / 584 / 0 | 12 / 12 / 224 / 0 | 12 / 12 / 2471 / 48 |
| n=4 load=8 cr=0 | 1 / 1 / 66 / 0 | 1 / 1 / 66 / 0 | 1 / 1 / 66 / 0 | 1 / 1 / 404 / 4 |
| n=4 load=8 cr=1 | 44 / 44 / 1624 / 0 | 44 / 44 / 1624 / 0 | 12 / 12 / 448 / 0 | 12 / 12 / 4444 / 48 |

#### Repeats on the "11 vs 73" cell

`n=3 load=8 cr=1`, five repeats under `Fuzz { instances: 1 }` (`LDFI_ABLATION_REPEATS=5`),
hypotheses per repeat:

| variant | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| 2PC (bug) | 8, 8, 8, 8, 8 | 8, 8, 8, 8, 8 | 2, 6, 2, 2, 2 | 2, 6, 2, 6, 6 |
| 2PC+CTP (certificate) | 76, 76, 76, 76, 76 | 76, 76, 76, 76, 76 | 18, 18, 18, 18, 18 | 18, 18, 18, 18, 18 |

#### Exhaustive cross-check (`n=2`, `cr=1`)

`LDFI_ABLATION_EXPLORATION=exhaustive`: every schedule per hypothesis. Cell =
`verdict / hypotheses / schedules / mean derivation size / seconds`.

| variant | config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|---|
| 2PC | n=2 load=1 cr=1 | CEX / 2 / 24 / 8.00 / 0.3s | CEX / 2 / 24 / 8.00 / 0.3s | CEX / 2 / 24 / 8.00 / 0.3s | CEX / 2 / 24 / 8.00 / 1.0s |
| 2PC | n=2 load=4 cr=1 | CEX / 6 / 96 / 28.00 / 2.5s | CEX / 6 / 96 / 28.00 / 2.6s | CEX / 2 / 32 / 8.00 / 0.7s | CEX / 2 / 32 / 8.00 / 2.6s |
| 2PC | n=2 load=8 cr=1 | CEX / 6 / 96 / 60.00 / 5.1s | CEX / 6 / 96 / 60.00 / 5.2s | CEX / 2 / 32 / 8.00 / 1.5s | CEX / 2 / 32 / 8.00 / 4.9s |
| 2PC+CTP | n=2 load=1 cr=1 | CERT(hand) / 9 / 158 / 8.33 / 1.4s | CERT(hand) / 9 / 158 / 8.33 / 1.4s | CERT(hand) / 9 / 158 / 7.83 / 1.4s | CERT(hand) / 9 / 158 / 7.83 / 6.4s |
| 2PC+CTP | n=2 load=4 cr=1 | CERT(hand) / 27 / 1142 / 27.89 / 22.2s | CERT(hand) / 27 / 1142 / 27.89 / 17.4s | CERT(hand) / 13 / 414 / 7.81 / 6.0s | CERT(hand) / 13 / 414 / 7.81 / 45.1s |
| 2PC+CTP | n=2 load=8 cr=1 | CERT(hand) / 51 / 2486 / 50.90 / 51.7s | CERT(hand) / 51 / 2486 / 50.90 / 48.1s | CERT(hand) / 13 / 414 / 7.81 / 8.4s | CERT(hand) / 13 / 414 / 7.81 / 71.9s |

One further exhaustive cell, plain 2PC `n=3 load=8 cr=1`, L0 only (the rest timed out):
`CEX / 8 / 6912 / 90.00 / 520.8s`, the same 8 hypotheses as under one schedule per
hypothesis.

### Observations (2PC)

- **Verdicts agree across all four levels in all 36 configurations**: plain 2PC under one
  crash is a counterexample (the coordinator dying between two `commit` sends) at every
  level; 2PC with cooperative termination under one crash is a certificate at every level;
  `cr=0` is a certificate after the failure-free run at every level (links are `fail_stop`,
  so the fault universe is empty). The exhaustive cross-check agrees with the one-schedule
  runs on every verdict and, on `n=2`, on every hypothesis count except L2 on the plain-2PC
  bug (2 under exhaustive vs. 3 and 4 under one schedule at load 1 and 4). No soundness
  disagreement.
- **L0 = L1 everywhere**, in hypotheses and (to within the schedule drawn) in derivation
  size: the static slice never separates records that share a hook on this program, as the
  earlier measurement said. Their derivation size grows linearly with load (`4·n·load`
  dependencies in the failure-free run: four per participant per transaction), their
  certificate cost roughly nine hypotheses per background transaction (`n=3`: 13 → 40 → 76
  at load 1 → 4 → 8).
- **L2 reduces hypotheses vs. L1 exactly when there is background load.** At `load=1` the
  four levels coincide (9 / 13 / 17 hypotheses to certify at `n=2/3/4`; 2 to the bug). At
  `load=4` and `load=8` L2's derivation stays at `4·n` dependencies and its hypothesis
  count stays flat across loads: certificates in 13 (`n=2`), 18 (`n=3`), 23 (`n=4`) against
  27 / 51, 40 / 76, 53 / 101 for L1 at load 4 / 8, i.e. **2.1–2.3× fewer at load 4 and
  3.9–4.4× fewer at load 8**. On the bug, L1 needs 6–10 hypotheses at load ≥ 4 and L2 needs
  2 in 7 of its 11 runs (grid plus repeats; 4, 6, 6 or 8 in the other four, when the
  schedule drawn leaves a background transaction's dependency in the derivation).
- **The "11 vs 73" claim** (2PC+CTP, `n=3`, load 8, one crash) reproduces for the coarse
  levels (73 → **76**, deterministic over five repeats) and partially for record provenance:
  **18** rather than 11, also deterministic. The ratio is **4.2×**, not 6.6×. L2's count is
  load-invariant as claimed (18 at load 4 and at load 8) and its mean derivation is 10.9
  dependencies against 72 for L0/L1. The shift against the earlier figures is uniform
  across levels and loads, not specific to L2: the load-1 certificate, where all levels
  coincide, moved from 10 to 13, and L2's load-4 count from 11 to 18. The likely cause is
  the harness rather than the lineage (the specification and the hooked channels have
  changed since; `history-hydrofrac-development.md` §9 records the same kind of shift on
  ISR, 20 → 33, from channels the first harness never hooked becoming fault points), but
  this was not verified here.
- **L3 does not help beyond L2 on 2PC.** Its hypothesis count equals L2's on every
  certificate and on the bug differs only by the schedule drawn, in either direction (grid:
  4 → 2, 2 → 6, 2 → 8, 8 → 2; repeats: 2 → 6 twice). Its derivations are the same as L2's
  (same size, same distinct count).
  It costs 5–10× L2's replays and 3–10× its wall time (11.2 s vs. 1.3 s at `n=4 load=8`
  to certify; 72 s vs. 8 s under exhaustive exploration at `n=2 load=8`), and it reports
  replay fallbacks in every schedule (`n` per failure-free schedule: 2 at `n=2`, 3 at
  `n=3`, 4 at `n=4`; a node that does not reproduce its output under replay with everything
  kept falls back to L2), so part of every L3 derivation is L2's.
- **Wall time** follows hypotheses at L0/L1/L2 (about 0.1–0.25 s per hypothesis at these
  sizes under one schedule each; about 1 s under exhaustive at `n=2 load=8`); L2 is the fastest
  level everywhere it differs from L1 (0.6 s vs. 10 s at `n=3 load=8`; 1.3 s vs. 23 s at
  `n=4 load=8`).
