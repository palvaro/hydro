# Lineage-level ablation

A re-measurement of what each lineage level of hydrofrac (`hydro_lang::sim::ldfi::Level`) buys,
on the current code. **The levels measured in the body of this document are the four of the
code as it was at the time**: `Execution` (L0: every input of the execution and of earlier
executions at the node), `Slice` (L1: L0 narrowed by the static slice of the producing sink;
the `Ldfi::new` default then), `Record` (L2: record-level provenance from
`SimFlow::with_provenance`, falling back to L1 where a record carries none, replaying the node at
an opaque record) and `Counterfactual` (L3: node-local replay with deliveries removed one at a
time, falling back to L2). Each level runs the *same* search (same fault model, crash budget,
exploration and hypothesis budget) on the *same* provenance-compiled program; only the backward
closure differs. The outcome of the measurement, the collapse to two levels (`Execution`,
`Record`) under Molly's fault model, is in the [final section](#final-two-levels-under-the-molly-fault-model).

- Date: 2026-10-19.
- Commit: `ldfi-hydrofrac` at `7d58ad20e0`, measured on the `molly viz` sandbox base
  `38471d0dd1` (which adds `sim::viz` on top: trace and provenance-graph recording in
  `ldfi.rs`, off by default here, and no change to the closure or the search). The ISR table
  and the "after the fix" notes were re-measured after the soundness fix described in the last
  section (exact dependency ordinals; sequenced observations traced), same day, same machine.
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

## ISR replication

Harness: `isr_replication::harness` (`hydro_test`), 3 replicas, 4 rounds, 1 write; the
hand-written durability specification (`PRE`: the client was acknowledged for `Write(1)` and
ZK's final view names a leader; `POST`: that leader's log holds `Write(1)`); replicas crashable,
omissions on the lossy replication channel within the first round. Two encodings of the same
protocol: **closure** (one opaque step function per tick) and **dataflow** (per-message-type
streams). *unclean* is the buggy variant (election from outside the ISR), *clean* the correct one.

Every cell searches the **same provenance-compiled program** (`with_provenance()`), so the levels
differ only in which lineage the backward closure follows. `fuzz k` = `Exploration::Fuzz
{ instances: k }` (k = 8 is what the unit tests use; k = 1 is the measurement grid's one schedule
per hypothesis). Hypothesis budget 256 (the default). Harness: `ldfi_ablation_isr.rs`, test
`level_ablation_isr` (gated on `LDFI_ABLATION_OUT`); one run, debug build, ~5 min total.

Cell = `verdict / hypotheses / schedules / mean derivation size / wall secs`; mean derivation size
is the mean number of dependencies per derivation over goal-held schedules. Replay counts are in
the notes below rather than the cells.

| encoding | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| closure | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 5.7s (r1680) | bug / 14 / 112 / 16.3 / 5.7s (r1680) | bug / 9 / 72 / 4.2 / 23.2s (r7589) | bug / 9 / 72 / 3.6 / 19.7s (r6484) |
| closure | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.8s (r210) | bug / 14 / 14 / 16.3 / 0.8s (r210) | bug / 9 / 9 / 4.1 / 2.9s (r940) | bug / 9 / 9 / 3.6 / 2.6s (r828) |
| closure | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 3.5s (r1104) | cert / 1 / 8 / 4.0 / 2.9s (r936) |
| closure | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 11.3s (r3232) | cert / 31 / 248 / 15.0 / 11.3s (r3232) | cert / 12 / 96 / 4.5 / 24.0s (r7711) | cert / 12 / 96 / 3.6 / 20.9s (r6766) |
| closure | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 1.8s (r404) | cert / 31 / 31 / 15.0 / 1.7s (r404) | cert / 12 / 12 / 4.2 / 3.0s (r951) | cert / 12 / 12 / 3.6 / 2.7s (r856) |
| closure | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 3.5s (r1104) | cert / 1 / 8 / 4.0 / 2.9s (r936) |
| dataflow | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 6.3s (r1680) | bug / 14 / 112 / 16.3 / 6.3s (r1680) | bug / 12 / 96 / 10.6 / 5.4s (r1440) | bug / 3 / 24 / 4.0 / 6.4s (r1796) |
| dataflow | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.9s (r210) | bug / 14 / 14 / 16.3 / 0.9s (r210) | bug / 12 / 12 / 10.6 / 0.8s (r180) | bug / 3 / 3 / 4.0 / 0.8s (r232) |
| dataflow | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.8s (r512) | cert / 4 / 32 / 17.8 / 1.8s (r512) | cert / 1 / 8 / 13.0 / 0.5s (r144) | cert / 1 / 8 / 4.0 / 2.2s (r632) |
| dataflow | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 12.4s (r3232) | cert / 31 / 248 / 15.0 / 12.5s (r3232) | cert / 18 / 144 / 10.8 / 7.0s (r1776) | cert / 12 / 96 / 3.6 / 15.8s (r4480) |
| dataflow | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 1.9s (r404) | cert / 31 / 31 / 15.0 / 1.9s (r404) | cert / 18 / 18 / 10.8 / 1.0s (r222) | cert / 12 / 12 / 3.6 / 2.0s (r560) |
| dataflow | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.8s (r512) | cert / 4 / 32 / 17.8 / 1.8s (r512) | cert / 1 / 8 / 13.0 / 0.5s (r144) | cert / 1 / 8 / 4.0 / 2.2s (r632) |

Replay fallbacks were 0 in every cell; `(rN)` = counterfactual replays. Replays occur at L0/L1
too because the hand-written specification's final-view derivation (`spec_view`) is always
taken by counterfactual replay and unioned with the recorded one, independently of the level.
This table is **after the fix** described in [Soundness bug: two lineage gaps](#soundness-bug-two-lineage-gaps-not-vacuous-hypotheses)
below (same harness, same machine, 2026-10-19); the pre-fix table, with its false certificates,
is kept there for the record.

### Observations

- **Verdicts agree across all four levels in every cell**: *unclean* is a counterexample at
  every level (always `{omit(repl[0→1]), crash(replica@0 after client-ack)}`), *clean* a
  certificate. Before the fix L0, L1 and L3 certified the buggy variant (see below).
- **L2 vs L1 hypothesis reduction.** 14 → 9 (bug, closure), 14 → 12 (bug, dataflow), 31 → 12
  (clean, closure), 31 → 18 (clean, dataflow), 4 → 1 (cr=0). Mean derivation size at L0/L1 is
  now 15–18 dependencies (the exact ordinals keep one dependency per heartbeat round instead of
  collapsing the post-`EFF` heartbeats of a replica into one), 4–11 at L2. L0 and L1 are
  identical in every cell: the static slice buys nothing over the execution closure on this
  program, in either encoding.
- **L3 vs L2.** Dataflow: 12 → 3 hypotheses on the bug (the failure-free derivation is
  `{hb[0→]#3, hb[1→], hb[2→], observed[0→]#2}`, so `crash(replica@0 after client-ack)` is
  hypothesis #2 and the omission is added at #3), 18 → 12 on the certificate. Closure: L3 equals
  L2 in hypotheses (9 / 12) with slightly smaller derivations (3.6 vs 4.2–4.5); the opaque step
  is replayed per record at L2 (`Prov::OPAQUE`), so L2 and L3 do similar work there (≈7000 vs
  ≈6500 replays at fuzz 8). L2 on the dataflow encoding is the cheapest level (0.5–7 s).
- **The 14/27 → 6/14 claim** (closure, L2 → L3, bug/certificate hypotheses) does **not**
  reproduce: closure L2 9/12, L3 9/12. The harness has changed since those numbers were taken
  (hand-written `witness_final` goal → `Spec` with `observe_sequenced(views).max()`; the client
  acknowledgements and views are now fault points), and the L3 "6" of the earlier measurement was
  the false certificate.
- cr=0 is a degenerate row: with no crashes the only faults are first-round omissions, and L2/L3
  certify from the failure-free run alone (1 hypothesis) because no omission kills the recorded
  derivation; L0/L1 need 4.

## Reliable broadcast

Harness: `hydro_test/src/cluster/ldfi_ablation_broadcast.rs` (`LDFI_ABLATION_OUT=… cargo test -p hydro_test level_ablation_broadcast -- --test-threads=1 --nocapture`), the search of `reliable_broadcast.rs`'s unit tests: the broadcaster and every member crashable (`fail_stop` links, so crashes are the only faults), specification `PRE` = some member delivered `m`, `POST` = every correct member delivered `m` (written positively, or through `filter_not_in` as "negated"). One program per configuration, compiled with provenance and shared by all four levels; exhaustive schedule exploration; one message. Wall times were measured on a loaded machine (sibling ablations running concurrently) and are indicative only.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| plain, n=3, 1 crash, POST positive | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.2s (r148 f0) |
| reliable (echo), n=3, 1 crash, POST positive | certificate / 15 / 3289 / 6.81 / 28.6s (r21168 f0) | certificate / 15 / 3289 / 6.81 / 30.2s (r21168 f0) | certificate / 10 / 2641 / 4.55 / 23.4s (r17280 f0) | certificate / 10 / 2641 / 4.55 / 64.1s (r65388 f0) |
| reliable (echo), n=3, 2 crashes, POST positive | certificate / 73 / 10845 / 4.77 / 76.0s (r54872 f0) | certificate / 73 / 10845 / 4.77 / 107.1s (r54872 f0) | certificate / 32 / 7271 / 3.38 / 52.6s (r38664 f0) | certificate / 32 / 7271 / 3.38 / 186.2s (r142059 f0) |
| plain, n=3, 1 crash, POST negated | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.3s (r148 f0) |
| reliable (echo), n=3, 1 crash, POST negated | certificate / 15 / 3289 / 6.81 / 32.2s (r21168 f0) | certificate / 15 / 3289 / 6.81 / 39.8s (r21168 f0) | certificate / 10 / 2641 / 4.55 / 30.9s (r17280 f0) | certificate / 10 / 2641 / 4.55 / 85.1s (r65388 f0) |
| plain, n=4, 1 crash, POST positive | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 1.5s (r732 f0) |
| reliable (echo), n=4, 1 crash | not run: 92,160 schedules per hypothesis exhaustively (~5 min each), at every level | | | |

Cells: `verdict / hypotheses / schedules / mean derivation size / wall secs`; `(rN fM)` = counterfactual replays and replay fallbacks. Mean derivation size is the mean number of `Dependency` leaves over the derivations of goal-held schedules; a member's delivery counts as two leaves (the protocol message it rests on and the member's observation send to the specification's observer), so `6.00` at n=3 and `8.00` at n=4 are the n deliveries of plain broadcast.

### Observations

- **Verdicts agree across all four levels in every configuration** (violated for plain broadcast, certificate for reliable broadcast). No soundness disagreement.
- **L0 = L1.** Execution and slice lineage produce identical hypotheses, schedules, and derivations in every row. Each member tick has two inputs (the broadcaster's batch and the echo batch) and both feed the deliveries, so the static slice narrows nothing here. (Wall-time differences between the two columns are machine noise.)
- **L2 vs L1 (plain broadcast): no change.** Plain broadcast's derivation is already minimal at L0 — each delivery rests on exactly one fan-out send — and the search is two hypotheses at every level.
- **L2 vs L1 (reliable broadcast): fewer, smaller derivations and fewer hypotheses.** At 1 crash: 15 → 10 hypotheses (−33%), 3289 → 2641 schedules (−20%), mean derivation 6.81 → 4.55 leaves (−33%). At 2 crashes: 73 → 32 hypotheses (−56%), 10845 → 7271 schedules (−33%), 4.77 → 3.38 leaves (−29%). The execution-level lineage of a member tick charges a delivery to *every* message in the batch (the broadcaster's send and every echo that arrived in the same tick); record provenance charges it to the one it was deduplicated from, so fewer crash points become candidates and fewer hypotheses have to be refuted. The L2 run took 20–30% less wall time than L1 despite the provenance bookkeeping.
- **L3 vs L2: identical derivations, 2.7–3.5× the wall time.** Counterfactual replay reproduces L2's hypotheses, schedules, and mean derivation size exactly in every row (zero fallbacks), with 3.8× as many replays (e.g. 17,280 → 65,388 at 1 crash). Record provenance already sees everything replay can see in this program (no closure-encoded state the rewrite misses).
- **Replays at L0–L2 are not zero.** The `rN` counts at L0–L2 are the counterfactual derivation of the *hand-written specification views* at the observer, which the search runs at every level (`ldfi.rs`, "For a hand-written view, two derivations are taken together…"); they scale with schedules (≈6.5 replays per schedule). L3 adds the per-node replays on top.
- **Positive vs negated `POST`: identical at every level.** The observer-side counterfactual makes the two formulations indistinguishable (same hypotheses, schedules, derivations), as the unit tests claim.
- **Size axis.** At n=4 the plain-broadcast counterexample is still found in 2 hypotheses at every level (30 schedules, 8 leaves). Reliable broadcast at n=4 was not run: exhaustive exploration enumerates 92,160 schedules per hypothesis (~310 s each on this machine), and the lineage level does not change the per-hypothesis schedule count, only the number of hypotheses.

## Reliable delivery

Harness: the same file, test `level_ablation_broadcast_and_delivery` (`LDFI_ABLATION_ONLY=delivery` runs this half alone), the search of `reliable_delivery.rs`'s unit tests: `msg` and `ack` channels `lossy_delayed_forever` (omission is the only fault), `eot` timer rounds with omissions admitted in the first `eff`, goal = the receiver received the message by the end of the horizon. "hooks" binds the receiver's message batch and the sender's ack batch to scripted hooks. Compiled with provenance; exhaustive schedule exploration (which is one schedule per hypothesis here).

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| give up after 2, eot=4 eff=2, no hooks | violated / 3 / 3 / 1.00 / 0.1s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s |
| give up after 2, eot=4 eff=2, hooks | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s (r6 f2) |
| retry until ack, eot=4 eff=2, no hooks | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s |
| retry until ack, eot=4 eff=2, hooks | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s (r9 f3) |
| give up after 3, eot=6 eff=3, hooks | violated / 4 / 4 / 1.00 / 0.0s | violated / 4 / 4 / 1.00 / 0.1s | violated / 4 / 4 / 1.00 / 0.1s | violated / 4 / 4 / 1.00 / 0.1s (r9 f3) |
| retry until ack, eot=6 eff=3, hooks | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.1s (r12 f4) |

Cells as above. A mean below 1.00 for the retry-until-ack certificates is the last hypothesis's empty derivation (the delivery that got through was sent after `EFF`, outside the fault vocabulary).

### Observations

- **Verdicts agree across all four levels in every configuration.** No soundness disagreement.
- **The levels are indistinguishable on this protocol.** Every row has the same hypotheses (`{}`, `{msg_1}`, `{msg_1, msg_2}`, … one per pre-`EFF` round), one schedule per hypothesis, and a derivation of exactly one leaf (the one `msg` delivery the goal rests on; acks never appear). The goal is a single external output with a single-delivery derivation, so there is nothing for L2 or L3 to narrow below L0/L1: the program's per-execution lineage is already exact.
- **Hooks change nothing in the verdict or the search**, as the unit tests claim; they only change where lineage is read (traced hook releases instead of receives).
- **L3 replays fall back on every step when the batches are hooked** (`r6 f2`, `r9 f3`, `r12 f4`: one fallback per goal-held schedule, i.e. every replayed derivation step), and runs no replays at all without hooks (there is no traced execution to replay; the lineage comes from the receives). With a hooked batch the node evidently does not reproduce its output under solo replay with everything kept, so the closure falls back to the recorded (L2) lineage — the same one-leaf derivation. The outcome is unaffected; worth a look at why a hooked batch does not replay, but not a soundness issue in these results.
- **Workload axis (eot=6, eff=3):** hypotheses grow by one per extra pre-`EFF` round at every level; nothing else changes.

## Raft

Harness: `hydro_test/src/cluster/ldfi_ablation_raft.rs` (`level_ablation_raft`, gated on
`LDFI_ABLATION_OUT`), a copy of `raft_ldfi.rs`'s durability harness with the configuration
shrunk so that the sixteen-cell grid fits in about half an hour (the Raft half took 19.5
minutes, the Paxos half 8): **2 rounds** before the end of
finite failures instead of 3, **1 sampled schedule per hypothesis** instead of 4, and a
hypothesis cap of **64** (the original searches run 90–235 hypotheses × 4 schedules and take
30–68 minutes each). Three members, crash budget 1, `TCP.lossy_delayed_forever()` on the
intra-cluster channel, heal phase unchanged (2 cycles × 3 heartbeat rounds). The program is
compiled once per variant with `with_provenance()` and all four levels run on it. Fuzz
schedules draw OS entropy, so repeated runs differ; wall times were taken on an 8-core host with
one other search running concurrently.

Cells: `verdict / hypotheses / schedules / mean derivation size / seconds`. Mean derivation size
is the mean number of `Dependency` leaves over the derivations of goal-held schedules.

| goal | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| durability of a committed entry | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, bug = election restriction ignores term | certificate / 59 / 59 / 46.4 / 19s | certificate / 59 / 59 / 46.4 / 19s | budget / 64 / 64 / 10.5 / 244s | budget / 64 / 64 / 10.4 / 253s |
| durability of a committed entry | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, correct | certificate / 59 / 59 / 46.4 / 19s | certificate / 59 / 59 / 46.4 / 19s | budget / 64 / 64 / 10.5 / 306s | budget / 64 / 64 / 10.1 / 255s |
| durability of a committed entry | correct, as above but max=160 (L2 only) | – | – | certificate / 107 / 107 / 10.3 / 403s | – |

Replays / replay fallbacks per cell:

| config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|
| bug | 1,984 / 0 | 1,984 / 0 | 16,510 / 0 | 18,732 / 0 |
| correct | 1,984 / 0 | 1,984 / 0 | 16,286 / 0 | 18,741 / 0 |
| correct, max=160 | – | – | 27,594 / 0 | – |

An earlier pass over the same grid (same configuration, different schedule draws, heavier
machine load) gave the same L0/L1 cells to the digit and L2/L3 cells of `budget / 64 / 64 /
10.5–10.9 / 310–373s`.

### Observations

- **L2 vs L1 derivation size: 4.4× smaller** (46.4 → 10.1–10.5 dependencies per derivation).
  Raft is one `raft_step` closure per tick over a `RaftServerState`, so at L0/L1 the static
  slice is a single opaque sink and every output depends on every delivery into the member;
  L0 and L1 are identical cells (same hypotheses, same derivations, same wall time), as
  `hydrofrac.md` predicts for a closure-encoded protocol.
- **L3 vs L2: no difference.** Under the provenance rewrite the step's outputs are
  `Prov::OPAQUE`, so L2 already replays through the step; L3 adds replay of the dataflow-style
  parts (the observer, the channel plumbing) and lands within 1% on derivation size and within
  10% on replay count. On this harness L2 *is* counterfactual lineage.
- **Fewer dependencies did not mean fewer hypotheses.** L0/L1 certify in 59 hypotheses; L2 was
  still running at 64 and, given 160, certified in 107 (on the correct variant). The
  over-approximate clause ("everything delivered to the member") is hit by nearly any fault
  set, and successive runs saturate the fault universe quickly; the precise clauses each name a
  different path (an original `AppendEntries`, its heartbeat retransmission, a vote) and the
  solver has to defeat each one separately. Wall time is dominated by replay either way:
  ~4–5 s per hypothesis at L2/L3 (260–290 replays per hypothesis) against ~0.3 s at L0/L1, so
  under this budget L2/L3 cost ~13× the wall time of L1 for a verdict that did not arrive.
- **Replays at L0/L1 are not zero** (1,984 per search). The specification is hand-written, and
  `spec_view` derives a hand-written view's final value by counterfactual replay *of the
  observer* at every level (unioned with the recorded derivation); only the protocol nodes'
  lineage follows the configured level. None of the Raft cells recorded a replay fallback; the
  replays that panicked inside the generated crate ("index out of bounds: the len is 1 but the
  index is 1", the known leader-indexed-past-its-log case) are counted as "target not produced"
  and keep the delivery, not as fallbacks.
- **Verdict agreement.** No cell contradicts another: the correct variant is certified at
  every level that finished (L0, L1, L2@160). The bug variant is certified at L0/L1 and
  unresolved (`budget`) at L2/L3. The injected bug *is* reachable in the original harness
  (3 rounds, 4 schedules: counterexample after 90 hypotheses at L2); whether it is reachable
  in 2 rounds with 1 schedule is not known, so the L0/L1 certificates on the bug variant are
  either correct for the shrunken schedule or a sampled-schedule miss, and this grid cannot tell
  which. **Not flagged as a soundness bug**, but worth a longer L2 run on `raft-bug` with
  `LDFI_ABLATION_MAX_HYP=256` (≈20 min) before quoting the L0/L1 cells as evidence.
- **Operators blocking L2: none.** Raft compiles under the provenance rewrite (the state-carrying
  closure is wrapped by `wrap_opaque`), so no cell is `n/a`.

## Paxos (core, for reference)

Same harness file, a copy of `paxos_ldfi.rs`'s core harness (two proposers, three acceptors,
`LeaderTiming::Driven`, lossy acceptor links, crash budget 1), with 1 sampled schedule per
hypothesis instead of 4 and a hypothesis cap of 256 (the original certificate search ran 372
hypotheses × 4 schedules in about 30 minutes). With one schedule the failure-free run is
vacuous (`PRE`, "proposer 0 chose `x`", does not hold) in roughly a third of the draws; the
harness redraws up to four times and reports the count.

| goal | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| agreement across a leader change | p=2 a=3 cr=1 sched=1 max=256, bug = commit quorum off by one | violated / 44 / 44 / 20.6 / 3s | violated / 43 / 43 / 22.5 / 3s | violated / 67 / 67 / 15.0 / 15s | violated / 82 / 82 / 15.9 / 24s |
| agreement across a leader change | p=2 a=3 cr=1 sched=1 max=256, correct | budget / 256 / 256 / 21.1 / 87s | budget / 256 / 256 / 21.6 / 96s | certificate / 195 / 195 / 18.1 / 70s | certificate / 236 / 236 / 19.5 / 160s |

Replays / replay fallbacks / vacuous redraws of the failure-free run:

| config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|
| bug | 147 / 0 / 3 | 187 / 0 / 2 | 259 / 0 / 2 | 638 / 0 / 0 |
| correct | 746 / 0 / 0 | 760 / 0 / 2 | 594 / 0 / 0 | 1,348 / 0 / 0 |

Two earlier passes (before the vacuous redraw existed) are informative about variance: with
cap 64 the bug was found at L1 in 38 and at L2 in 28 hypotheses, and L0/L3 hit the cap; with
cap 2048, L0 certified the correct variant in 180 hypotheses (46s) and L1 in 151 (31s), while
L2 drew a vacuous schedule and L3 had not finished after 25 minutes and was killed. Counts at
one schedule per hypothesis move by ±50% between passes; the table above is a single pass.

### Observations

- **L2 vs L1 derivation size: ~1.3–1.5× smaller** (22.5 → 15.0 on the bug variant, 21.6 → 18.1
  on the correct one): Paxos is dataflow-style, so the slice already separates P1/P2 traffic
  and record provenance removes the rest of the over-approximation within an execution.
- **L3 vs L2:** derivations of the same size (15.9 vs 15.0, 19.5 vs 18.1), 2–2.5× the replays,
  1.6–2.3× the wall time. Replay sees nothing the rewrite missed here.
- **Hypotheses:** as on Raft, smaller derivations did not shorten the search for the bug
  (L0/L1 43–44, L2 67, L3 82); the counterexamples differ by level (`p2a` omissions to two
  acceptors at L0/L1, a `p1b` omission at L2/L3), so the levels walk different paths through
  the fault lattice. On the correct variant L2 and L3 certified within 256 where L0 and L1 did
  not; the earlier pass had L0/L1 certifying in 151–180, so this is within schedule variance.
- **Verdict agreement:** every level that reached a verdict agrees (bug → `violated` at all
  four; correct → `certificate` or `budget`). No soundness disagreement.
- **Operators blocking L2: none**; core Paxos compiles under the rewrite.

## Soundness bug: two lineage gaps, not vacuous hypotheses

The first measurement of the ISR grid found L0, L1 and L3 **certifying the unclean-election
bug** that L2 finds (`ldfi_finds_the_unclean_election_bug{,_dataflow}` failed with
`LDFI_NO_PROVENANCE=1` and with `LDFI_LEVEL=replay`). The table as measured then, for the record
(bold = wrong verdict):

| encoding | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| closure | unclean cr=1 fuzz 8 | **cert** / 14 / 112 / 7.4 / 6.9s | **cert** / 14 / 112 / 7.4 / 6.5s | bug / 9 / 72 / 3.8 / 36.3s | **cert** / 6 / 48 / 1.7 / 9.7s |
| closure | unclean cr=1 fuzz 1 | **cert** / 14 / 14 / 7.4 / 0.5s | **cert** / 14 / 14 / 7.4 / 0.5s | bug / 9 / 9 / 3.9 / 2.9s | **cert** / 6 / 6 / 1.7 / 0.7s |
| closure | unclean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.9s | cert / 4 / 32 / 8.8 / 1.2s | cert / 1 / 8 / 4.0 / 4.2s | cert / 1 / 8 / 1.0 / 2.3s |
| closure | clean cr=1 fuzz 8 | cert / 14 / 112 / 7.4 / 3.2s | cert / 14 / 112 / 7.4 / 3.6s | cert / 12 / 96 / 4.0 / 23.7s | cert / 6 / 48 / 1.7 / 5.8s |
| closure | clean cr=1 fuzz 1 | cert / 14 / 14 / 7.4 / 0.5s | cert / 14 / 14 / 7.4 / 0.5s | cert / 12 / 12 / 3.6 / 2.9s | cert / 6 / 6 / 1.7 / 0.7s |
| closure | clean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.1s | cert / 4 / 32 / 8.8 / 1.1s | cert / 1 / 8 / 4.0 / 3.4s | cert / 1 / 8 / 1.0 / 1.8s |
| dataflow | unclean cr=1 fuzz 8 | **cert** / 14 / 112 / 7.4 / 3.8s | **cert** / 14 / 112 / 7.4 / 3.6s | bug / 9 / 72 / 4.1 / 2.7s | **cert** / 6 / 48 / 1.7 / 3.0s |
| dataflow | unclean cr=1 fuzz 1 | **cert** / 14 / 14 / 7.4 / 0.6s | **cert** / 14 / 14 / 7.4 / 0.6s | bug / 9 / 9 / 4.1 / 0.4s | **cert** / 6 / 6 / 1.7 / 0.4s |
| dataflow | unclean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.3s | cert / 4 / 32 / 8.8 / 1.3s | cert / 1 / 8 / 4.0 / 0.4s | cert / 1 / 8 / 1.0 / 0.9s |
| dataflow | clean cr=1 fuzz 8 | cert / 14 / 112 / 7.4 / 3.5s | cert / 14 / 112 / 7.4 / 3.5s | cert / 13 / 104 / 4.3 / 3.4s | cert / 6 / 48 / 1.7 / 3.0s |
| dataflow | clean cr=1 fuzz 1 | cert / 14 / 14 / 7.4 / 0.6s | cert / 14 / 14 / 7.4 / 0.6s | cert / 13 / 13 / 4.3 / 0.5s | cert / 6 / 6 / 1.7 / 0.4s |
| dataflow | clean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.2s | cert / 4 / 32 / 8.8 / 1.2s | cert / 1 / 8 / 4.0 / 0.4s | cert / 1 / 8 / 1.0 / 0.9s |

The diagnosis written down at the time — vacuous hypotheses (`PRE` false) contribute no clause
and are read as coverage, so the hitting sets exhaust before the real counterexample is tried —
does **not** survive the solver's own trace (`LDFI_SOLVER_DEBUG=1`). Vacuous hypotheses do add no
clause and do prune their supersets (`ldfi.rs`, `vacuous_sets` in `Ldfi::run` and the "no
superset of a vacuous hypothesis" constraint in `solve_hypothesis`), but the counterexample
`X = {omit(repl[0→1]), crash(replica@0 after client-ack)}` is a superset of none of the vacuous
sets the search saw (`{crash(r0 before any send)}`, `{omit(repl[0→1]), crash(r0 after repl[0→2])}`,
`{omit(repl[0→1]), crash(r0 after repl[0→1])}`). `X` was never proposed because **`X` was not a
hitting set of the clauses the search had**: some clause claimed `POST` rested only on deliveries
`X` does not kill, while the simulator shows `X` kills `POST`. Two unrelated gaps produced such
clauses, one per failing level pair.

### Root cause 1 (L0, L1): dependency ordinals clamped at `EFF`

A clause's dependency carries `(sender, ordinal)` and a crash point `crash(n after m)` kills it
iff `m`'s position in the clause's run is below the ordinal (`Dependency::killed_by`).
`Lineage::crash_ordinal` *clamped* the ordinal to the sender's number of pre-`EFF` sends, on the
documented assumption that crash points lie before the end of finite failures, so that every
post-`EFF` send was "killed by exactly the crashes that kill the sender's last pre-`EFF` send".
The simulator does not share the assumption: a crash point is named by its message and
`FaultState::filter` (`sim/runtime/lineage.rs`) kills the node right after that message is sent
in *every* run, `EFF` or not (the ISR harness's own doc says "crashes anywhere"). In the run of
the second hypothesis, `{omit(repl[0→1])}`, replica 0's sends were

```
#0 repl[0→1]  #1 repl[0→2]  | EFF |  #2 shrink  #3 hb  #4 client-ack  #5 hb  #6 hb  #7 hb
```

and every replica-0 dependency of that run's clause carried the clamped ordinal `#2`. For
`crash(replica@0 after client-ack)` the relation computed `position(client-ack) = 4 < 2`, i.e.
the crash kills nothing of the clause — while it kills `#5–#7`, the heartbeats that keep 0 the
leader in ZK's final view, which is exactly how `POST` falls. The omission world's clause could
never be hit by the crash the bug needs, so `X` was never a hitting set; the ILP ran out of
hitting sets and certified (14 hypotheses). L2 found the bug only by hypothesis order: its
smaller failure-free clause made `{crash(replica@0 after client-ack)}` hypothesis #2, *before*
any omission-world clause existed, and `X` then hit that crash world's clause through the
omission.

**Fix.** The ordinal is exact (`crash_ordinal` returns the sender's real send position) and the
clamp moves to where crash points are *proposed*: `Dependency::candidate_faults` takes the
clause's `pre_eff_sends` and mints, for a post-`EFF` delivery, the crash right after the
sender's last pre-`EFF` send (`Clause` carries `pre_eff_sends`; the preserve points of
requirements index the same way). The universe is unchanged — only pre-`EFF`-named crash points
are ever proposed — and `killed_by` now agrees with the simulator wherever a proposed point
fires. Clauses grow (one dependency per heartbeat round instead of one collapsed dependency), so
mean derivation sizes change at every level. Module docs, `Schedule::pre_eff_sends` and
`Dependency` now state the actual semantics: crash points are *named* before `EFF` in the run
that minted them and fire wherever the message is sent. Unit test
`post_eff_crash_point_kills_later_sends_but_is_not_proposed`.

The alternative — enforcing the stated model in the simulator (`FaultState::filter` ignores a
crash point once `EFF` has passed, as `history-hydrofrac-development.md` §5.4 describes the
intent: "a crash is permanent, as in Molly, but it happens inside the finite-failures window",
introduced after a Raft heal-phase crash produced a false violation) — would also make the
levels agree, but in the other direction: tried as a one-line experiment, the unclean variant
then **certifies at every level** (31 hypotheses at L0, the same count as the clean variant),
because the leader's acknowledgement is post-`EFF` in the omission world and the crash the bug
needs never fires. The ISR unit tests, the harness doc ("crashes anywhere") and the negation
history all treat `{omit(repl[0→1]), crash(replica@0 after client-ack)}` as the expected
counterexample, so at this point the search was made to agree with the simulator rather than
the simulator with the search. The two readings of "finite failures" for crashes were then
reconciled the other way, as the [final section](#final-two-levels-under-the-molly-fault-model)
records: the simulator confines crash points to the window (Molly's model) and the ISR harness
moves `EFF` past the first round, where the leader's acknowledgement lies in every world.

### Root cause 2 (L3): sequenced observations were never traced to their sends

`Spec::observe_sequenced` applies `.enumerate()` *before* the observer's hooked batch, so the
hook's release payload is `bincode((usize, View))` while the send's wire bytes are
`bincode(View)`. `payload_matches` only stripped the cluster `MemberId` prefix, so every view
release was unmatched — the "32 untraced deliveries" (4 views × 8 schedules) printed on every ISR
log line, at every level. L0–L2 are unaffected (the execution closure follows receives by
delivery id, record provenance follows prov ids), but `Lineage::counterfactual` takes as
removable candidates only releases matched to a send (`matters`), so at L3 the views were fed to
every observer replay as fixed facts and their lineage (ZK ← heartbeats) was never followed:
failure-free derivation `{observed[0→]#2}`, requirement `{client-ack#2}`, a universe of two crash
points, no omission ever considered, certificate in 6. (The observer's `produced` predicate was
already "`POST`'s last value is unchanged", not `!pre || post`; the vacuity reading of this level
was also wrong.)

**Fix.** `payload_matches` also accepts a serialized `usize` prefix. The L3 failure-free
derivation becomes `{hb[0→]#3, hb[1→]#1, hb[2→]#1, observed[0→]#2}`, the same shape as L2's, and
the search goes hypothesis #2 `{crash(replica@0 after client-ack)}`, #3 `X`, violated.

### Vacuous hypotheses

Left as they were: a hypothesis whose every schedule has `PRE` false contributes no clause and
excludes its supersets. Nothing in this investigation shows it changing a verdict; the superset
pruning remains the documented heuristic ("vacuity is not monotone in the fault set",
`history-hydrofrac-development.md` §10), i.e. a potential, so far unobserved, source of a
missed hitting set.

### After the fix

- **ISR**: the table in [ISR replication](#isr-replication) above. Verdicts agree at every level
  (bug / cert); regression test `ldfi_finds_the_unclean_election_bug_at_every_level` runs the
  dataflow search at all four levels. Hypothesis counts moved: L0/L1 bug 14 (cert) → 14 (bug), clean 14 → 31
  (larger clauses, more hitting sets to refute); L2 bug 9 → 9 (closure), 9 → 12 (dataflow),
  clean 12 → 12 / 13 → 18; L3 bug 6 (cert) → 9 (closure) / 3 (dataflow), clean 6 → 12.
- **2PC** (`level_ablation_two_pc`, same grid, fuzz 1, deterministic across repeats): every
  verdict unchanged; every certificate's hypothesis count unchanged (ctp=true, all 18 cells);
  mean derivation sizes at L0/L1 move by <1 (uncollapsed post-`EFF` dependencies). The
  counterexample cells shift by a few hypotheses in both directions, from the different clause
  shapes: ctp=false L2 `n=2 load=4` 4 → 2, `n=2 load=8` 2 → 4, `n=3 load=1` 4 → 2, `n=3 load=8`
  6 → 2, `n=4 load=8` 8 → 2; L3 `n=3 load=1` 2 → 4, `n=4 load=4` 8 → 2. The 2PC tables above are
  the pre-fix measurement.
- **Reliable broadcast / reliable delivery** (`level_ablation_broadcast_and_delivery`,
  exhaustive; two runs, each losing one L2 cell to trybuild's recompilation guard when a source
  file changed under it, the union covering every cell): every cell identical to the tables
  above in verdict, hypotheses, schedules, mean derivation size and replay counts (e.g. reliable broadcast n=3, 2 crashes: 73 / 10845 / 4.77 at
  L0–L1, 32 / 7271 / 3.38 at L2–L3, r142059 at L3). Neither protocol has post-`EFF` sends that a
  proposed crash point could reach, nor sequenced observations, so neither gap applied.
- Not re-run: the `#[ignore]`d long certifications (Raft durability, Paxos / compartmentalized
  Paxos agreement, Paxos KV prefix agreement; 15–60 min each). Their harnesses have post-`EFF`
  sends, so their hypothesis counts may move; their verdicts should not (the fix only adds
  hits to clauses the simulator already refutes, and traces deliveries that were held fixed).

## Final: two levels under the Molly fault model

Same day, same machine, same harnesses, after two changes that the measurement above led to.

**The fault model is Molly's.** Every failure happens at a time `≤ EFF`. The simulator's filter
(`FaultState::filter`, `sim/runtime/lineage.rs`) now fires an `AfterSend` crash point only while
finite failures last: a crash point is *named* by a pre-`EFF` send of the run that minted it, and
in another run it fires only if that send happens before `EFF` there; if the named send happens
after `EFF`, or never, the node does not crash in that run (a node already dead stays dead). The
search's kill relation follows (`Dependency::killed_by` takes the run's pre-`EFF` send counts and
counts a crash as firing only within that prefix); the exact-ordinal fix of the previous section
is kept, since a pre-`EFF` crash does kill the sender's post-`EFF` sends; a requirement sent after
`EFF` no longer gets a preserve point (no crash that fires in that run keeps it). The ISR harness's
`body` declares `EFF` after the first round of replica timers and ZK's first view, so the leader's
acknowledgement and first heartbeat lie inside the window in every world; the unclean-election
counterexample `{omit(repl[0→1]), crash(replica@0 after client-ack)}` is then an admissible
failure (the acknowledgement is replica 0's send #4 in the omission world, before `EFF`), and no
level needs a crash that fires after `EFF` to find it. Unit tests:
`sim::runtime::lineage::tests::crash_point_named_by_a_post_eff_send_does_not_fire`,
`ldfi::tests::crash_points_fire_only_before_eff_and_kill_every_later_send`.

**Two levels.** `Level` is `Execution` (the execution-granular closure, no static slice) and
`Record` (record provenance, counterfactual replay at `Prov::OPAQUE` records, execution closure
as the fallback). `Record` is `Ldfi::new()`'s default; a program compiled without
`with_provenance()` is searched at `Execution` (the search logs the substitution). `Slice` and
`Counterfactual` are gone, with the sink-slice filtering in the closure and the standalone replay
path; `sim/slice.rs` still computes the program's channels and hook inputs, which the closure and
`search_all_labels` need. `ProvLevel` tags are `Execution`, `Record`, `Replay`.

### Four levels, after the fault-model change (before the collapse)

Re-measured to separate the two changes. Verdicts and hypothesis counts are **identical to the
post-fix tables above in every cell** of ISR (closure unclean cr=1: bug 14 / 14 / 9 / 9; clean
cr=1: cert 31 / 31 / 12 / 12; dataflow unclean: bug 14 / 14 / 12 / 3; clean: cert 31 / 31 / 18 /
12; cr=0: 4 / 4 / 1 / 1), of 2PC (all 36 configurations; every certificate count unchanged, the
counterexample cells moving by schedule draw as before) and of reliable broadcast / delivery. The
ISR hypothesis *sequence* differs (the universe now contains `crash(replica after hb)` for the
first-round heartbeats, and `{crash(replica@0 after hb)}` is hypothesis #5 at L0 where
`{crash(replica@0 after client-ack)}` used to be), the totals coincide. No verdict changed.

### Two levels

Cell = `verdict / hypotheses / schedules / mean derivation size / wall secs (replays)`.

ISR replication (`level_ablation_isr`, provenance-compiled, budget 256):

| encoding | config | Execution | Record |
|---|---|---|---|
| closure | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 5.7s (r1680) | bug / 9 / 72 / 4.2 / 23.3s (r7589) |
| closure | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.8s (r210) | bug / 9 / 9 / 4.1 / 3.0s (r940) |
| closure | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 3.5s (r1104) |
| closure | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 11.3s (r3232) | cert / 12 / 96 / 4.5 / 24.4s (r7728) |
| closure | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 1.7s (r404) | cert / 12 / 12 / 4.1 / 3.0s (r934) |
| closure | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 3.5s (r1104) |
| dataflow | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 6.3s (r1680) | bug / 12 / 96 / 10.6 / 5.4s (r1440) |
| dataflow | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.9s (r210) | bug / 12 / 12 / 10.6 / 0.8s (r180) |
| dataflow | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.8s (r512) | cert / 1 / 8 / 13.0 / 0.5s (r144) |
| dataflow | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 12.4s (r3232) | cert / 18 / 144 / 10.8 / 7.1s (r1776) |
| dataflow | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 1.9s (r404) | cert / 18 / 18 / 10.8 / 1.0s (r222) |
| dataflow | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 1.9s (r512) | cert / 1 / 8 / 13.0 / 0.5s (r144) |

Replay fallbacks 0 in every cell. Every cell equals the corresponding L0 (for `Execution`) and L2
(for `Record`) cell of the four-level table.

2PC (`level_ablation_two_pc_n{2,3,4}`, fuzz 1, budget 400; `cr=0` rows are the failure-free
certificate at every level and are omitted):

| variant | config | Execution | Record |
|---|---|---|---|
| 2PC | n=2 load=1 cr=1 | CEX / 2 / 2 / 8.00 / 0.0s | CEX / 2 / 2 / 8.00 / 0.0s |
| 2PC | n=2 load=4 cr=1 | CEX / 6 / 6 / 28.00 / 0.1s | CEX / 2 / 2 / 8.00 / 0.0s |
| 2PC | n=2 load=8 cr=1 | CEX / 6 / 6 / 60.00 / 0.3s | CEX / 2 / 2 / 8.00 / 0.1s |
| 2PC | n=3 load=1 cr=1 | CEX / 2 / 2 / 12.00 / 0.0s | CEX / 4 / 4 / 10.67 / 0.1s |
| 2PC | n=3 load=4 cr=1 | CEX / 8 / 8 / 42.00 / 0.3s | CEX / 2 / 2 / 12.00 / 0.1s |
| 2PC | n=3 load=8 cr=1 | CEX / 8 / 8 / 90.00 / 0.7s | CEX / 6 / 6 / 10.40 / 0.3s |
| 2PC | n=4 load=1 cr=1 | CEX / 2 / 2 / 16.00 / 0.0s | CEX / 2 / 2 / 16.00 / 0.0s |
| 2PC | n=4 load=4 cr=1 | CEX / 10 / 10 / 56.00 / 0.7s | CEX / 8 / 8 / 14.29 / 0.3s |
| 2PC | n=4 load=8 cr=1 | CEX / 10 / 10 / 120.00 / 1.9s | CEX / 2 / 2 / 16.00 / 0.2s |
| 2PC+CTP | n=2 load=1 cr=1 | CERT(hand) / 9 / 9 / 7.50 / 0.1s | CERT(hand) / 9 / 9 / 7.25 / 0.1s |
| 2PC+CTP | n=2 load=4 cr=1 | CERT(hand) / 27 / 27 / 24.21 / 0.6s | CERT(hand) / 13 / 13 / 6.83 / 0.1s |
| 2PC+CTP | n=2 load=8 cr=1 | CERT(hand) / 51 / 51 / 44.36 / 2.3s | CERT(hand) / 13 / 13 / 6.83 / 0.2s |
| 2PC+CTP | n=3 load=1 cr=1 | CERT(hand) / 13 / 13 / 12.83 / 0.2s | CERT(hand) / 13 / 13 / 11.33 / 0.2s |
| 2PC+CTP | n=3 load=4 cr=1 | CERT(hand) / 40 / 40 / 39.81 / 2.1s | CERT(hand) / 18 / 18 / 10.89 / 0.4s |
| 2PC+CTP | n=3 load=8 cr=1 | CERT(hand) / 76 / 76 / 71.85 / 9.2s | CERT(hand) / 18 / 18 / 10.89 / 0.6s |
| 2PC+CTP | n=4 load=1 cr=1 | CERT(hand) / 17 / 17 / 17.00 / 0.3s | CERT(hand) / 17 / 17 / 15.38 / 0.3s |
| 2PC+CTP | n=4 load=4 cr=1 | CERT(hand) / 53 / 53 / 56.86 / 5.9s | CERT(hand) / 23 / 23 / 14.92 / 0.8s |
| 2PC+CTP | n=4 load=8 cr=1 | CERT(hand) / 101 / 101 / 100.66 / 27.8s | CERT(hand) / 23 / 23 / 14.92 / 1.3s |

Every verdict and every certificate count as in the four-level grid; the counterexample cells
move by the schedule drawn (2, 4, 6 or 8 at `Record`, as in the repeats above).

Reliable broadcast and reliable delivery (`level_ablation_broadcast_and_delivery`, exhaustive):

| config | Execution | Record |
|---|---|---|
| plain broadcast, n=3, 1 crash, POST positive | violated / 2 / 8 / 6.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 0.1s (r54) |
| reliable broadcast, n=3, 1 crash, POST positive | certificate / 15 / 3289 / 6.81 / 27.6s (r21168) | certificate / 10 / 2641 / 4.55 / 22.7s (r17280) |
| reliable broadcast, n=3, 2 crashes, POST positive | certificate / 73 / 10845 / 4.77 / 73.8s (r54872) | certificate / 32 / 7271 / 3.38 / 51.5s (r38664) |
| plain broadcast, n=3, 1 crash, POST negated | violated / 2 / 8 / 6.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 0.1s (r54) |
| reliable broadcast, n=3, 1 crash, POST negated | certificate / 15 / 3289 / 6.81 / 27.5s (r21168) | certificate / 10 / 2641 / 4.55 / 22.2s (r17280) |
| plain broadcast, n=4, 1 crash, POST positive | violated / 2 / 30 / 8.00 / 0.5s (r264) | violated / 2 / 30 / 8.00 / 0.4s (r264) |
| reliable delivery, give up after 2, eot=4 eff=2 (hooks or not) | violated / 3 / 3 / 1.00 | violated / 3 / 3 / 1.00 |
| reliable delivery, retry until ack, eot=4 eff=2 (hooks or not) | certificate / 3 / 3 / 0.67 | certificate / 3 / 3 / 0.67 |
| reliable delivery, give up after 3, eot=6 eff=3, hooks | violated / 4 / 4 / 1.00 | violated / 4 / 4 / 1.00 |
| reliable delivery, retry until ack, eot=6 eff=3, hooks | certificate / 4 / 4 / 0.75 | certificate / 4 / 4 / 0.75 |

Identical to the L0 and L2 columns above.

Raft and core Paxos (`level_ablation_raft`, the shrunken configurations of the Raft and Paxos
sections above; one sampled schedule per hypothesis, so counts move by ±50% between passes):

| protocol | config | Execution | Record |
|---|---|---|---|
| Raft durability | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, bug | certificate / 59 / 59 / 66.5 / 17s (r1984) | budget / 64 / 64 / 10.4 / 243s (r16384) |
| Raft durability | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, correct | certificate / 59 / 59 / 66.5 / 17s (r1984) | budget / 64 / 64 / 10.4 / 267s (r17589) |
| Paxos agreement | p=2 a=3 cr=1 sched=1 max=256, bug | violated / 84 / 84 / 20.0 / 13s (r355) | violated / 27 / 27 / 16.0 / 2s (r115) |
| Paxos agreement | p=2 a=3 cr=1 sched=1 max=256, correct | budget / 256 / 256 / 24.1 / 136s (r776) | certificate / 230 / 230 / 19.2 / 116s (r552) |

Verdicts as in the four-level tables (Raft: `Execution` certifies both variants in 59, `Record`
runs out of budget at 64; Paxos: the bug is found at both levels, the correct variant certifies
only at `Record` within 256). Raft's `Execution` derivations are larger than the L0 cells above
(66.5 against 46.4 leaves): the old L0 walked the *sub*-derivations of sends through the sink
slice (`closure(exec, Some(channel))`), and the slice narrowed Raft's channel plumbing even though
the step itself was opaque; the pure execution closure does not. The hypothesis count is the
same 59, as the certificate's shape is decided by the opaque step's over-approximation either
way. The Paxos counterexample at `Execution` is a `p1b` omission this pass (84 hypotheses; 44
with a `p2a` omission in the pass above) — schedule variance, not a change in the search.

### Regression suites

`cargo test -p hydro_test ldfi -- --test-threads=1`: 52 passed, 12 ignored (the long
certifications), 594 s. `cargo test -p hydro_lang --features ldfi --lib sim`: 212 passed.
`ldfi_viz::provenance_graphs_agree_with_derivations_at_every_level` checks both levels; the
`Execution` snapshot (`two_pc_execution_dot`, formerly `two_pc_slice_dot`) changed in its edge
set, since the walk no longer filters through the slice, with the same fault points.

### Recommendation record

**Retained:** `Level::Execution` and `Level::Record`; `Record` is the default and the level every
harness in the tree uses when it compiles with provenance. `Record` keeps counterfactual replay as
its oracle for `Prov::OPAQUE` records (the closure-encoded ISR step, Raft's `raft_step`), which is
what it already did; it is the only place replay runs on the protocol's nodes, besides the
derivation of a hand-written specification view at the observer. **Removed:** `Slice`, because on
every protocol measured (2PC, ISR in both encodings, reliable broadcast, reliable delivery, Raft,
Paxos) it produced the same hypotheses and derivations as `Execution` — the static slice never
separated a record from the inputs of its execution where that would have changed a verdict or a
count; and `Counterfactual`, because wherever `Record` applied it produced the same derivations
and, on 2PC and broadcast, the same hypothesis counts, at two to ten times the replays and wall
time, and its one real advantage (the dataflow ISR bug in 3 hypotheses instead of 12) does not
pay for a Raft search that runs five seconds a hypothesis. **Why `Record` and not `Execution` by
default:** `Record` beats `Execution` on hypotheses wherever there is background load or
redundancy (2PC+CTP 101 → 23 at `n=4 load=8`, reliable broadcast 73 → 32, ISR 31 → 12/18), is
faster in wall time on every dataflow-style program, and is the level the Molly-style provenance
graphs are drawn from; it loses only on Raft, where the opaque step forces a replay per record and
the execution closure's single clause certifies in 19 s — a program that is closure-encoded end to
end should pick `Execution` explicitly, and a program compiled without `with_provenance()` gets
it automatically. **Fault model:** every failure at a time `≤ EFF`, enforced by the simulator and
mirrored by the kill relation; a harness that means to admit a crash must place `EFF` after the
send that names it, as the ISR harness now does.

## Disjunctive provenance

Molly's lineage from one good run is a boolean formula over messages: a conjunction within a
derivation, a **disjunction** across the alternative derivations of one fact, and the
hitting-set solver prunes every fault set that leaves some alternative intact without running
it. Hydrofrac's record provenance kept one witness per record (at a dedup, the first copy's),
so a run yielded one clause per goal fact and the alternatives were rediscovered by simulation.
This section recovers the disjunction behind a flag and measures what it buys. Same machine,
same harnesses as the section above, the `disjunctive` column added to each table (`Record`
level with `Ldfi::disjunctive_provenance(true)`; `record` is the same level with it off).

### What was built

**A record's provenance is a formula** (`sim/runtime/provenance.rs`). `Prov` is a shared,
lazily-resolved handle over `Leaf(delivery) | All(children) | Alts(children)`: pass-through
operators share the handle, combinations (`join`, `cross`, folds) build `All`, and a
deduplicating operator builds `Alts`. It has to be lazy and shared because DFIR's `unique` is
streaming: the first copy of a value is emitted (and possibly sent) before its duplicate
arrives in the same tick, so the alternative can only reach the emitted record through a cell
both share. The rewrite (`sim/provenance.rs`) inserts a map before every `unique` inside a
tick that gives each arrival a fresh `Alts` cell, shared by the copy `unique` stores and the
record it emits and by nothing upstream; `Tagged::eq` — the only hook `unique`'s hash set
offers — pushes the duplicate's alternatives into that cell when the values are equal.
Absorption is opt-in (only an `Alts` cell absorbs), so no other comparison site creates an
alternative; `filter_not_in` is compiled as an anti-join with a plain negative side, so a
positive record is never compared with a negative one as `Tagged`s; a top-level (`'static`)
`unique` is not reboxed, since a duplicate arriving in a later execution is no derivation of
what the first copy already produced. The formula is resolved to a minimal DNF of witnesses
when the lineage log is read (`LineageHandles::snapshot`), under a cap (`Ldfi::max_witnesses`,
default 8): a conjunction whose cross product exceeds it falls back to the single union
witness (a superset of a real witness: sound, less pruning) and a disjunction keeps its first
`cap` alternatives (dropping alternatives is sound); both count as `witness_overflows`. With a
cap of 1 the resolution is exactly the old single derivation (first copy wins at a dedup,
union at a combination), which `RecordProv::ids` always holds, so the flag-off path reads
what it always read.

**The closure follows every witness** (`sim/ldfi.rs`). `record_closures` expands each witness
of a record through the (memoized) derivations of the sends behind its deliveries — a cross
product under the same cap, minimized (no derivation a superset of another: the smaller
clause implies the larger) — and the goal assemblers conjoin lists: `any_output` and
`all_members` take every derivation of every output, `witnessed` crosses the facts' lists,
the hand-written specification view conjoins the observer's counterfactual derivation with
each recorded alternative (unioning it into them, the first attempt, erased every alternative
and measured as a no-op), and a replayed (opaque) record's kept deliveries are conjoined
through their own alternatives (`counterfactual_all`; the replay itself is shared with the
single path through a memo, so the replay counts are the same in both columns). Every
derivation becomes a clause, and the ILP must hit them all: Molly's pruning.

Flag off, the search is unchanged: `cargo test -p hydro_lang --features ldfi --lib sim`
(221 tests) passes, the 2PC / ISR / broadcast tables below reproduce the `record` column of
the section above cell for cell (up to the schedule draw under `fuzz 1`), and
`two_pc_ldfi::tests::single_derivation_search_is_unchanged_by_the_provenance_formula` pins
the thirteen hypotheses of the exhaustive 2PC+CTP `n=2 load=4` search, in order, to the
sequence recorded from the tree before this change.

### Where the alternatives are

Whether the disjunction can do anything depends on a run *exhibiting* two derivations of one
fact. `LDFI_PROV_DEBUG` prints the records whose formula resolved to more than one witness:

- **2PC with cooperative termination:** none in the failure-free run. A participant queries
  its peers only after a timeout while uncertain, and the body fires the timers after the
  commits land; the peer path fires only in runs where the coordinator crashed, and there the
  participant hears *only* the peer. Alternatives exist across worlds, not within one run,
  and Molly's single-run lineage would see exactly what hydrofrac's does. At `n ≥ 3` some
  crash runs do batch a `commit` and a peer's `Decided` into the same tick (clauses per run
  0.54–1.06 against 0.46–0.52), which is where the table's extra derivations come from.
- **ISR replication:** none, in either encoding (the one write reaches each follower once;
  heartbeats are folded, not deduplicated).
- **Reliable broadcast with echo:** every member's delivery has two or three witnesses in most
  schedules (`[[0], [3]]`: the broadcaster's `initial` or member 1's echo), since every echo is
  sent unconditionally and a tick often batches the initial with an echo.

### 2PC (fuzz 1, budget 400, cr=1)

Cell = `verdict / hypotheses / schedules / mean derivation size / clauses per run / seconds`.

| variant | config | exec | record | disjunctive |
|---|---|---|---|---|
| 2PC | n=2 load=1 | CEX / 2 / 2 / 8.00 / 0.50 / 0.0s | CEX / 2 / 2 / 8.00 / 0.50 / 0.0s | CEX / 2 / 2 / 8.00 / 0.50 / 0.0s |
| 2PC | n=2 load=4 | CEX / 6 / 6 / 28.00 / 0.83 / 0.1s | CEX / 2 / 2 / 8.00 / 0.50 / 0.0s | CEX / 2 / 2 / 8.00 / 0.50 / 0.0s |
| 2PC | n=2 load=8 | CEX / 6 / 6 / 60.00 / 0.83 / 0.3s | CEX / 4 / 4 / 6.67 / 0.75 / 0.1s | CEX / 4 / 4 / 6.67 / 0.75 / 0.1s |
| 2PC | n=3 load=1 | CEX / 2 / 2 / 12.00 / 0.50 / 0.0s | CEX / 2 / 2 / 12.00 / 0.50 / 0.0s | CEX / 2 / 2 / 12.00 / 0.50 / 0.0s |
| 2PC | n=3 load=4 | CEX / 8 / 8 / 42.00 / 0.88 / 0.3s | CEX / 2 / 2 / 12.00 / 0.50 / 0.1s | CEX / 6 / 6 / 10.40 / 0.83 / 0.2s |
| 2PC | n=3 load=8 | CEX / 8 / 8 / 90.00 / 0.88 / 0.7s | CEX / 2 / 2 / 12.00 / 0.50 / 0.1s | CEX / 6 / 6 / 10.40 / 0.83 / 0.3s |
| 2PC | n=4 load=1 | CEX / 2 / 2 / 16.00 / 0.50 / 0.0s | CEX / 2 / 2 / 16.00 / 0.50 / 0.0s | CEX / 2 / 2 / 16.00 / 0.50 / 0.0s |
| 2PC | n=4 load=4 | CEX / 10 / 10 / 56.00 / 0.90 / 0.6s | CEX / 2 / 2 / 16.00 / 0.50 / 0.1s | CEX / 2 / 2 / 16.00 / 0.50 / 0.1s |
| 2PC | n=4 load=8 | CEX / 10 / 10 / 120.00 / 0.90 / 1.7s | CEX / 2 / 2 / 16.00 / 0.50 / 0.2s | CEX / 2 / 2 / 16.00 / 0.50 / 0.2s |
| 2PC+CTP | n=2 load=1 | CERT(hand) / 9 / 9 / 7.50 / 0.44 / 0.1s | CERT(hand) / 9 / 9 / 7.25 / 0.44 / 0.1s | CERT(hand) / 9 / 9 / 7.25 / 0.44 / 0.1s |
| 2PC+CTP | n=2 load=4 | CERT(hand) / 27 / 27 / 24.36 / 0.52 / 0.6s | CERT(hand) / 13 / 13 / 6.83 / 0.46 / 0.1s | CERT(hand) / 13 / 13 / 6.83 / 0.46 / 0.1s |
| 2PC+CTP | n=2 load=8 | CERT(hand) / 51 / 51 / 44.45 / 0.43 / 2.3s | CERT(hand) / 13 / 13 / 6.83 / 0.46 / 0.2s | CERT(hand) / 13 / 13 / 6.83 / 0.46 / 0.2s |
| 2PC+CTP | n=3 load=1 | CERT(hand) / 13 / 13 / 12.67 / 0.46 / 0.2s | CERT(hand) / 13 / 13 / 11.33 / 0.46 / 0.1s | CERT(hand) / 13 / 13 / 11.71 / 0.54 / 0.2s |
| 2PC+CTP | n=3 load=4 | CERT(hand) / 40 / 40 / 39.67 / 0.53 / 2.0s | CERT(hand) / 18 / 18 / 10.89 / 0.50 / 0.4s | CERT(hand) / 18 / 18 / 11.20 / 0.56 / 0.4s |
| 2PC+CTP | n=3 load=8 | CERT(hand) / 76 / 76 / 72.12 / 0.43 / 9.4s | CERT(hand) / 18 / 18 / 10.89 / 0.50 / 0.6s | CERT(hand) / 18 / 18 / 11.36 / 0.61 / 0.7s |
| 2PC+CTP | n=4 load=1 | CERT(hand) / 17 / 17 / 18.12 / 0.47 / 0.4s | CERT(hand) / 17 / 17 / 15.38 / 0.47 / 0.3s | CERT(hand) / 17 / 17 / 16.83 / 1.06 / 0.4s |
| 2PC+CTP | n=4 load=4 | CERT(hand) / 53 / 53 / 57.04 / 0.53 / 5.8s | CERT(hand) / 23 / 23 / 14.92 / 0.52 / 0.8s | CERT(hand) / 23 / 23 / 16.56 / 0.78 / 0.9s (2 overflows) |
| 2PC+CTP | n=4 load=8 | CERT(hand) / 101 / 101 / 100.98 / 0.44 / 29.0s | CERT(hand) / 23 / 23 / 14.92 / 0.52 / 1.3s | CERT(hand) / 23 / 23 / 16.38 / 1.04 / 1.5s |

Every verdict identical; every certificate count identical. The plain-2PC `n=3` counterexample
cells (2 against 6) move by the schedule draw, as in every earlier pass (`record` has drawn
2, 4, 6 and 8 there). Witness overflows: 2 in the whole grid (`n=4 load=4`), none elsewhere.

### ISR replication (provenance-compiled, budget 256)

Cell = `verdict / hypotheses / schedules / mean derivation size / clauses per run / seconds (replays)`.

| encoding | config | exec | record | disjunctive |
|---|---|---|---|---|
| closure | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 6.86 / 5.9s (r1680) | bug / 9 / 72 / 4.2 / 6.22 / 24.5s (r7631) | bug / 9 / 72 / 4.3 / 6.22 / 24.6s (r7682) |
| closure | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.86 / 0.9s (r210) | bug / 9 / 9 / 4.4 / 0.78 / 3.2s (r974) | bug / 9 / 9 / 4.1 / 0.78 / 3.2s (r940) |
| closure | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 8.00 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 8.00 / 3.7s (r1104) | cert / 1 / 8 / 4.0 / 8.00 / 3.6s (r1104) |
| closure | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 6.45 / 11.6s (r3232) | cert / 12 / 96 / 4.3 / 5.33 / 25.4s (r7683) | cert / 12 / 96 / 4.4 / 5.33 / 25.0s (r7646) |
| closure | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 0.81 / 1.7s (r404) | cert / 12 / 12 / 4.6 / 0.67 / 3.2s (r975) | cert / 12 / 12 / 4.5 / 0.67 / 3.2s (r958) |
| closure | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 8.00 / 1.7s (r512) | cert / 1 / 8 / 4.0 / 8.00 / 3.5s (r1104) | cert / 1 / 8 / 4.0 / 8.00 / 3.6s (r1104) |
| dataflow | unclean cr=1 fuzz 8 | bug / 14 / 112 / 16.3 / 6.86 / 6.9s (r1680) | bug / 12 / 96 / 10.6 / 6.67 / 5.8s (r1440) | bug / 12 / 96 / 10.6 / 6.67 / 5.8s (r1440) |
| dataflow | unclean cr=1 fuzz 1 | bug / 14 / 14 / 16.3 / 0.86 / 1.0s (r210) | bug / 12 / 12 / 10.6 / 0.83 / 0.8s (r180) | bug / 12 / 12 / 10.6 / 0.83 / 0.8s (r180) |
| dataflow | unclean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 8.00 / 2.0s (r512) | cert / 1 / 8 / 13.0 / 8.00 / 0.6s (r144) | cert / 1 / 8 / 13.0 / 8.00 / 0.6s (r144) |
| dataflow | clean cr=1 fuzz 8 | cert / 31 / 248 / 15.0 / 6.45 / 13.6s (r3232) | cert / 18 / 144 / 10.8 / 5.78 / 7.8s (r1776) | cert / 18 / 144 / 10.8 / 5.78 / 7.8s (r1776) |
| dataflow | clean cr=1 fuzz 1 | cert / 31 / 31 / 15.0 / 0.81 / 2.0s (r404) | cert / 18 / 18 / 10.8 / 0.72 / 1.1s (r222) | cert / 18 / 18 / 10.8 / 0.72 / 1.1s (r222) |
| dataflow | clean cr=0 fuzz 8 | cert / 4 / 32 / 17.8 / 8.00 / 2.1s (r512) | cert / 1 / 8 / 13.0 / 8.00 / 0.6s (r144) | cert / 1 / 8 / 13.0 / 8.00 / 0.6s (r144) |

Identical to `record` in every cell (the odd replay count differs by the draw); no record in
ISR has more than one witness, so there is nothing to disjoin. Overflows 0.

### Reliable broadcast (n=3, echo), the protocol with alternatives

Exhaustive exploration, cell = `verdict / hypotheses / schedules / mean derivation size / clauses per run / seconds (replays)`:

| config | exec | record | disjunctive |
|---|---|---|---|
| plain, 1 crash, POST positive | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) |
| reliable (echo), 1 crash, POST positive | certificate / 15 / 3289 / 6.81 / 219.20 / 30.3s (r21168) | certificate / 10 / 2641 / 4.55 / 264.00 / 24.7s (r17280) | certificate / 11 / 2651 / 4.75 / 868.55 / 24.2s (r16620) |
| reliable (echo), 2 crashes, POST positive | certificate / 73 / 10845 / 4.77 / 148.53 / 78.7s (r54872) | certificate / 32 / 7271 / 3.38 / 227.16 / 53.3s (r38664) | certificate / 39 / 7280 / 3.71 / 454.92 / 50.1s (r35132) |
| plain, 1 crash, POST negated | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) | violated / 2 / 8 / 6.00 / 3.00 / 0.1s (r54) |
| reliable (echo), 1 crash, POST negated | certificate / 15 / 3289 / 6.81 / 219.20 / 29.8s (r21168) | certificate / 10 / 2641 / 4.55 / 264.00 / 24.1s (r17280) | certificate / 11 / 2651 / 4.75 / 868.55 / 23.5s (r16620) |
| plain, n=4, 1 crash, POST positive | violated / 2 / 30 / 8.00 / 12.00 / 0.5s (r264) | violated / 2 / 30 / 8.00 / 12.00 / 0.5s (r264) | violated / 2 / 30 / 8.00 / 12.00 / 0.5s (r264) |

One schedule per hypothesis (`LDFI_ABLATION_EXPLORATION=fuzz1`, three repeats), hypotheses only:

| config | exec | record | disjunctive |
|---|---|---|---|
| reliable (echo), 1 crash, POST positive | 15, 15, 15 | 9, 9, 10 | 11, 13, 12 |
| reliable (echo), 2 crashes, POST positive | 74, 74, 74 | 33, 32, 32 | 52, 47, 46 |
| reliable (echo), 1 crash, POST negated | 15, 15, 15 | 9, 10, 10 | 12, 12, 11 |
| plain (both forms), 1 crash | 2 | 2 or 4 | 2 or 4 |

Clauses per run: 0.9 → 2.3–3.1 (1 crash), 0.94 → 1.8–2.0 (2 crashes). Overflows 0 (the cap of 8 is
never reached at `n=3`). Verdicts identical.

### Observations

**The disjunction does what Molly promises, and it costs more runs than it saves.** The
hypothesis sequences of the exhaustive 1-crash search
(`reliable_broadcast::tests::ldfi_certifies_reliable_broadcast_with_disjunctive_provenance`,
`LDFI_BROADCAST_DIAG=1`):

```
single:       {} → crash(broadcaster after initial[→1]) → crash(broadcaster after initial[→0])
              → crash(member@0 after echo[0→2]) → … after echo[0→1] → … after echo[0→0]
              → crash(member@1 after echo[1→2]) → … after echo[1→1]
              → crash(member@2 after echo[2→2]) → crash(broadcaster before any send)        (10)
disjunctive:  {} → crash(member@0 after echo[0→2]) → … after echo[0→1] → … after echo[0→0]
              → crash(member@1 after echo[1→2]) → … after echo[1→1] → crash(member@1 before any send)
              → crash(member@2 after echo[2→2]) → crash(member@2 after echo[2→0])
              → crash(member@2 before any send) → crash(broadcaster before any send)         (11)
```

The failure-free run yields 2160 clauses instead of 360 (six derivations per schedule: each
member's delivery by the initial or by an echo), and the two `crash(broadcaster after
initial[→k])` hypotheses the single search ran are **pruned without running**: the clauses say
an echo alternative survives them. That is the pruning Molly's formula buys, and it is real.
But every alternative's deliveries are now dependencies, so their senders' early sends become
crash-point candidates — `crash(member@1 before any send)`, `crash(member@2 after echo[2→0])`,
`crash(member@2 before any send)` — which the single-derivation search never named (nothing it
derived depended on member 1's first send) and therefore never tried. Three runs for two. Under
two crashes the universe grows the same way and the combinations with it, 32 → 46–52.

**Why hydrofrac did not need the disjunction.** Hydrofrac's fault universe is grown lazily
from the derivations it has seen, and a crash point is only proposed once some derivation
depends on the send it is named by. A fault that kills no dependency of any observed derivation
is never tried, which is itself most of the pruning Molly gets from the formula: the single
path never ran `crash(member@1 before any send)` because it never had a reason to. Molly's
universe is every message of the run, so the disjunction is what keeps *its* search from
running every one of them; hydrofrac gets that from the universe instead. The DNF's
certificate is relative to a larger universe (it also covers fault sets that combine the newly
named crash points, which the single path's certificate does not), so it is a stronger
certificate at more runs, not a cheaper one.

**Where it would pay.** A protocol in which the failure-free run exhibits alternatives *and*
whose alternative deliveries are sends the single path already names: then the pruning is free
and the universe does not grow. Neither corpus protocol has that shape — in 2PC+CTP and ISR
alternatives do not fire within a run at all; in broadcast the alternatives are echoes whose
senders had no other dependency. Schedule diversity under exhaustive exploration already
harvests the alternatives the single path misses (264 clauses per run on broadcast with one
witness each, since different schedules batch differently), which is why the exhaustive and
the `fuzz 1` columns move the same way.

**The cap cost nothing.** Two overflows in the whole measurement (2PC+CTP `n=4 load=4`), none on
broadcast or ISR; the formulas stay small because the alternatives are two or three per record
and the goals conjoin three members at most. Resolution and expansion add no measurable wall
time (every `disjunctive` cell is within a tenth of a second of `record`; the replays are
shared).

### Core Paxos (p=2, a=3, cr=1, one sampled schedule per hypothesis, budget 256)

Cell = `verdict / hypotheses / mean derivation size / clauses per run / seconds (replays)`.

| variant | exec | record | disjunctive |
|---|---|---|---|
| bug (commit quorum off by one) | violated / 162 / 23.6 / 0.58 / 67s (r665) | violated / 77 / 15.9 / 0.65 / 18s (r307) | violated / 134 / 16.0 / 0.60 / 81s (r539) |
| correct | certificate / 203 / 18.4 / 0.45 / 44s (r496) | budget / 256 / 19.5 / 0.60 / 540s (r956) | certificate / 223 / 18.9 / 0.42 / 136s (r548) |

Overflows 0. With one sampled schedule per hypothesis these counts move by ±50% between passes
(the section above had `record` certify at 230 and `exec` run out of budget at 256, the
reverse of this pass), and clauses per run are the same 0.4–0.65 in every column: no Paxos
record resolved to more than one witness (a `p2b` quorum is a fold, a conjunction), so the
columns differ by the draw alone. The `budget` against `certificate` pair is a budget effect
of that variance, not a verdict disagreement.

**What the measurement does not say.** The counterexample cells are decided by the schedule
draw at every level and say nothing here; so does Paxos at one schedule per hypothesis. Raft was
not measured (its record cells run four minutes each and its lineage is replayed, which yields
one derivation per record by construction).

### Status

`Ldfi::disjunctive_provenance(bool)`, default off; `Ldfi::max_witnesses(k)`, default 8;
`Schedule::witness_overflows`. The formula representation stays on regardless (it is what the
record level now reads its single derivation from, bit for bit). Off is the recommendation:
on the corpus it never reduced the hypothesis count and raised it by 10–50% where alternatives
exist. A variant worth one more experiment is **DNF clauses over the single-derivation
universe** — keep every alternative as a cover constraint but propose crash points only from
the first-witness derivations, dropping (soundly) any clause the universe cannot hit — which
would keep the two pruned broadcaster hypotheses without admitting the three new member ones.
