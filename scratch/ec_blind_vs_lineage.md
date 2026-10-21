# LDFI (`Strategy::Lineage`) vs brute force (`Strategy::Blind`) on the 19 EC-corpus tests

Branch measured: `docs/hydrofrac-inventory` @ `8ac5c6c1d4` (exported with `git archive` into `$TMPDIR/h`).
**Measurement-only patch** (`scratch/measurement-only.patch`, NOT for commit), confined to `hydro_lang/src/sim/ldfi.rs`:

- `LDFI_STRATEGY=blind|lineage` overrides the configured strategy; `LDFI_MAX_HYPOTHESES=<n>` overrides the budget (set to 100000, so the default 256 never applied; no search hit it).
- `Ldfi::run` is wrapped so it prints `MEASURE strategy= verdict= hypotheses= schedules= elapsed_ms=` for every search (one per label under `search_all_labels`).
- Blind logs its universe size on each iteration.
- `LDFI_BLIND_AS_CERT=1`: an `Exhausted` blind outcome is handed back to the test as a `Certificate` with no derivations, so multi-search tests aren't cut short by their own `assert!(Certificate)`. `MEASURE` still records the real verdict (`Exhausted`).

Toolchain: pip wheels `cmake` 4.0.3 + `libclang` in a `$TMPDIR` venv (`PATH`, `LIBCLANG_PATH`), the same setup as earlier runs on this host. Runs used `cargo test -q -p <pkg> --lib <test> -- --exact --nocapture --test-threads=1`, with 3 (Lineage) or 4 (Blind) tests running at once on 8 cores. Every run was single-shot, so treat the times as roughly ±20%. Hypothesis counts are deterministic.

## Summary

47 searches across the 19 tests (labels in `search_all_labels`, plus tests that call `search` twice).

| verdict class | searches | hypotheses L / B | search time (ms) L / B | L fewer / B fewer / tie |
|---|---|---|---|---|
| counterexample | 25 | 114 / 302 | 20 663 / 48 492 | 12 / 7 / 6 |
| certificate (L) ≡ exhausted (B) | 18 | 475 / 1 012 | 53 207 / 141 906 | 16 / 0 / 2 |
| vacuous | 4 | 0 / 0 | 200 / 197 | 0 / 0 / 4 |
| **total** | **47** | **589 / 1 314 (2.2×)** | **74 s / 191 s (2.6×)** | **28 / 7 / 12** |

- **Verdicts agree on all 47 searches.** Every Lineage `Certificate` corresponds to a Blind `Exhausted`, every counterexample to a counterexample, and every vacuous run to a vacuous run. **Both strategies found the same counterexample fault set in all 25 counterexample cases.**
- **No DNF.** The slowest Blind search was 29 s (g_set gossip). The 1800 s timeout and the 100k hypothesis cap were never reached.
- **Where Blind wins** (7 searches, all counterexamples on small programs, 2–3 hypotheses vs 5):
  - Single-hop broadcasts: `ec_broadcast_closed_{all_labels,fail_stop,cluster_to_cluster}`, `ec_demo_broadcast_live`, and `ec_all_labels_classes` ×2 (5 vs 2 each). The breaking source crash comes first or second in Blind's weakest-first order. LDFI's first ILP solution kills one member's derivation at a time (`crash(member@i before any send)` ×3), and these hold before it gets to the source crash.
  - `reliable_broadcast.rs:156` (3 vs 2).
- **Where LDFI wins big:**
  - multi_paxos all-labels: 105 vs 363 hypotheses; 5 vs 50 at :281, 8 vs 44 at :286/:549.
  - Certificates in general: reliable broadcast 36 vs 115, g_set gossip 13 vs 40, ISR harness model :343 4 vs 68, multi_paxos learned 25 vs 52. LDFI also wins several counterexamples: reliable_broadcast.rs:130 (5 vs 20), ISR :360/:564 (8 vs 24), leader_merge (2 vs 4), abd.rs:141 (2 vs 5), and 2PC :86 (2 vs 4).
  - LDFI's per-hypothesis cost is higher (it builds lineage and calls the ILP), but wherever it needs fewer hypotheses it is also faster in wall time, with one exception: `separates_plain_from_reliable` spec #2, where 21 vs 31 hypotheses took 12.0 s vs 11.0 s (LDFI explored 4076 schedules vs 3511).
- **Ties:** 2PC coordinator-correct :86/:92 (1 vs 1: the bounds admit no fault, so only the failure-free run happens), the 4 vacuous 2PC labels, 2PC harness :92 (2 vs 2), abd.rs:150/:161/:314 (2 vs 2), lossy omission (2 vs 2), and the plain-broadcast half of `separates_plain_from_reliable` (2 vs 2).
- **Closest case:** `ec_demo_uniform_broadcast` needs 105 vs 115 hypotheses (840 vs 920 schedules). Lineage barely prunes here, because nearly every observed delivery is in the lineage of the uniform-delivery goal.
- **Fault space.** Blind's `Exhausted` count is the number of admissible fault sets over the observed universe, after Blind's superset-of-vacuous pruning. It is the exhaustive space, so for certificates column `hyp B` *is* the exhaustive space: 115 for reliable/uniform broadcast, 52 for multi_paxos, 40 for g_set, 68/16 for ISR, 31 for reliable vs plain. For counterexamples Blind stops early, so the space is at least `hyp B`; universe sizes |U| are listed for scale. `U L` is the size of the ILP's lineage universe (candidate faults from clauses plus preserve points) at its last iteration. `U B` is every candidate fault of every observed delivery. `U L` can exceed `U B` on tiny programs because the lineage universe includes preserve points (crashes right after a send).
- Crash bounds (the same under both strategies): most tests use `crashes unbounded; not all N nodes at once; at most 1 of the checked cluster's 3 crashable member(s)`. Both multi_paxos tests use `at most 1 crash(es); not all 7 nodes at once` (one multi_paxos label has `at most 0 of 1` members). Both 2PC tests use `at most 0 of the checked cluster's 2 crashable member(s)`.
- The only nonzero test exit is Blind `ec_demo_reliable_broadcast_all_labels`. It fails on the test's own `assert_eq!(runs.len(), 36)` (Blind took 115), which happens after all 3 labels were searched, so no data is missing.

## Per search

Columns are Lineage / Blind. `ms` is time spent in the search (`Ldfi::run`); it excludes the trybuild compile and test setup. `U` is the universe size at the last iteration. `fewer` says which strategy needed fewer hypotheses.

| test | label / search | verdict L / B | hypotheses | schedules | ms | U | fewer | counterexample (L / B) |
|---|---|---|---|---|---|---|---|---|
| `ec_ldfi::ec_broadcast_closed_all_labels` | `ec_ldfi.rs:270` | cex / cex | 5 / 2 | 5 / 2 | 43 / 7 | 9 / 6 | B | L {crash(source after bcast[→1]:Item(2))} / B {crash(source after bcast[→1]:Item(2))} |
| `ec_ldfi::ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash` | `spec #1` | cex / cex | 5 / 2 | 5 / 2 | 39 / 7 | 9 / 6 | B | L {crash(source after bcast[→1]:Item(2))} / B {crash(source after bcast[→1]:Item(2))} |
| `ec_ldfi::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash` | `spec #1` | cex / cex | 5 / 2 | 5 / 2 | 42 / 10 | 9 / 6 | B | L {crash(source@0 after bcast[0→1]:Item(10))} / B {crash(source@0 after bcast[0→1]:Item(10))} |
| `ec_ldfi::ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission` | `spec #1` | cex / cex | 2 / 2 | 2 / 2 | 8 / 7 | 15 / 12 | tie | L {omit(bcast[→0]:Item(1))} / B {omit(bcast[→0]:Item(1))} |
| `ec_ldfi::ec_generated_spec_separates_plain_from_reliable_broadcast` | `spec #1` | cex / cex | 2 / 2 | 8 / 8 | 44 / 31 | 6 / 3 | tie | L {crash(broadcaster after initial[→1]:Payload(42))} / B {crash(broadcaster after initial[→1]:Payload(42))} |
| `ec_ldfi::ec_generated_spec_separates_plain_from_reliable_broadcast` | `spec #2` | cert / exhausted | 21 / 31 | 4076 / 3511 | 11960 / 10997 | 9 / 12 | L |  |
| `ec_demos_ldfi::ec_demo_broadcast_live_breaks_under_a_sender_crash` | `spec #1` | cex / cex | 5 / 2 | 5 / 2 | 41 / 10 | 9 / 6 | B | L {crash(source after bcast[→1]:2)} / B {crash(source after bcast[→1]:2)} |
| `ec_demos_ldfi::ec_demo_leader_merge_breaks_under_a_leader_crash` | `spec #1` | cex / cex | 2 / 4 | 4 / 8 | 36 / 59 | 10 / 7 | L | L {crash(leader after merged[→1]:(MemberId::<hydro_test_hydro_trybuild::__staged::cluster::ec_demos_ldfi::tests::Writer>(0), 100))} / B {crash(leader after merged[→1]:(MemberId::<hydro_test_hydro_trybuild::__staged::cluster::ec_demos_ldfi::tests::Writer>(0), 100))} |
| `ec_demos_ldfi::ec_demo_reliable_broadcast_all_labels` | `reliable_broadcast.rs:130` | cex / cex | 5 / 20 | 5 / 20 | 60 / 279 | 9 / 24 | L | L {crash(source after reliable_broadcast.rs:130[→1]:2)} / B {crash(source after reliable_broadcast.rs:130[→1]:2)} |
| `ec_demos_ldfi::ec_demo_reliable_broadcast_all_labels` | `reliable_broadcast.rs:156` | cex / cex | 3 / 2 | 3 / 2 | 37 / 11 | 27 / 24 | B | L {crash(member@0 after reliable_broadcast.rs:156[0→1]:2)} / B {crash(member@0 after reliable_broadcast.rs:156[0→1]:2)} |
| `ec_demos_ldfi::ec_demo_reliable_broadcast_all_labels` | `reliable_broadcast.rs:149` | cert / exhausted | 36 / 115 | 36 / 115 | 403 / 4397 | 15 / 24 | L |  |
| `ec_demos_ldfi::ec_demo_reliable_broadcast_certifies` | `spec #1` | cert / exhausted | 36 / 115 | 36 / 115 | 447 / 4194 | 15 / 24 | L |  |
| `ec_demos_ldfi::ec_demo_reliable_broadcast_certifies` | `spec #2` | cert / exhausted | 36 / 115 | 36 / 115 | 399 / 4095 | 15 / 24 | L |  |
| `ec_demos_ldfi::ec_demo_uniform_broadcast` | `spec #1` | cert / exhausted | 105 / 115 | 840 / 920 | 5178 / 7318 | 27 / 24 | L |  |
| `ec_demos_ldfi::ec_demo_uniform_broadcast` | `spec #2` | cert / exhausted | 105 / 115 | 840 / 920 | 5151 / 6947 | 27 / 24 | L |  |
| `ec_demos_ldfi::ec_demo_g_set_gossip_certifies` | `spec #1` | cert / exhausted | 13 / 40 | 104 / 320 | 5000 / 20287 | 23 / 39 | L |  |
| `ec_demos_ldfi::ec_demo_g_set_gossip_certifies` | `spec #2` | cert / exhausted | 13 / 40 | 104 / 320 | 7595 / 29126 | 21 / 39 | L |  |
| `ec_demos_ldfi::ec_demo_multi_paxos_learned_under_one_crash` | `spec #1` | cert / exhausted | 25 / 52 | 200 / 416 | 2386 / 4846 | 43 / 51 | L |  |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:281` | cex / cex | 5 / 50 | 40 / 400 | 748 / 7420 | 6 / 51 | L | L {crash(proposer@0 after multi_paxos.rs:281[0→1]:Ts { round: 1, writer: Legacy { raw_id: 0 } })} / B {crash(proposer@0 after multi_paxos.rs:281[0→1]:Ts { round: 1, writer: Legacy { raw_id: 0 } })} |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:286` | cex / cex | 8 / 44 | 64 / 352 | 1297 / 7189 | 19 / 51 | L | L {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))} / B {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))} |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:549` | cex / cex | 8 / 44 | 64 / 352 | 1330 / 7184 | 19 / 51 | L | L {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))} / B {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))} |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:569` | cert / exhausted | 7 / 34 | 56 / 272 | 1182 / 5496 | 26 / 51 | L |  |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:570` | cert / exhausted | 6 / 34 | 48 / 272 | 1177 / 5406 | 26 / 51 | L |  |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:582` | cex / cex | 11 / 37 | 88 / 296 | 1930 / 6246 | 39 / 51 | L | L {crash(proposer@0 after multi_paxos.rs:582[0→1]:(1, 0, 0, Some(7)))} / B {crash(proposer@0 after multi_paxos.rs:582[0→1]:(1, 0, 0, Some(7)))} |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:591` | cex / cex | 8 / 16 | 64 / 128 | 1620 / 3113 | 51 / 51 | L | L {crash(learner@0 after multi_paxos.rs:591[0→1]:(1, 0, 0, Some(7)))} / B {crash(learner@0 after multi_paxos.rs:591[0→1]:(1, 0, 0, Some(7)))} |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:588` | cert / exhausted | 26 / 52 | 208 / 416 | 3715 / 7808 | 43 / 51 | L |  |
| `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` | `multi_paxos.rs:608` | cert / exhausted | 26 / 52 | 208 / 416 | 4167 / 7731 | 43 / 51 | L |  |
| `ec_all_labels_classes::all_labels_reports_a_label_inside_a_tick` | `ec_all_labels_classes.rs:31` | cex / cex | 5 / 2 | 948 / 282 | 4230 / 1998 | 9 / 6 | B | L {crash(source after bcast[→1]:2)} / B {crash(source after bcast[→1]:2)} |
| `ec_all_labels_classes::all_labels_reports_a_label_inside_a_tick` | `ec_all_labels_classes.rs:41` | cex / cex | 5 / 2 | 948 / 282 | 3870 / 1721 | 9 / 6 | B | L {crash(source after bcast[→1]:2)} / B {crash(source after bcast[→1]:2)} |
| `isr_replication::ldfi_all_labels_break_under_a_sender_crash` | `isr_replication.rs:360` | cex / cex | 8 / 24 | 64 / 192 | 1813 / 5270 | 9 / 31 | L | L {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })} / B {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })} |
| `isr_replication::ldfi_all_labels_break_under_a_sender_crash` | `isr_replication.rs:343` | cex / cex | 2 / 4 | 16 / 32 | 615 / 1065 | 6 / 28 | L | L {crash(client after write[→1]:Write(1))} / B {crash(client after write[→1]:Write(1))} |
| `isr_replication::ldfi_all_labels_break_under_a_sender_crash` | `isr_replication.rs:564` | cex / cex | 8 / 24 | 64 / 192 | 1776 / 5211 | 9 / 31 | L | L {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })} / B {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })} |
| `isr_replication::ldfi_all_labels_with_the_harness_fault_model` | `isr_replication.rs:360` | cert / exhausted | 7 / 16 | 56 / 128 | 1632 / 3506 | 6 / 15 | L |  |
| `isr_replication::ldfi_all_labels_with_the_harness_fault_model` | `isr_replication.rs:343` | cert / exhausted | 4 / 68 | 32 / 544 | 994 / 16112 | 3 / 18 | L |  |
| `isr_replication::ldfi_all_labels_with_the_harness_fault_model` | `isr_replication.rs:564` | cert / exhausted | 7 / 16 | 56 / 128 | 1606 / 3426 | 6 / 15 | L |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_harness_fault_model` | `two_pc_ldfi.rs:86` | cex / cex | 2 / 4 | 20 / 100 | 134 / 565 | 4 / 13 | L | L {crash(coordinator after prepare[→0]:Txn(1))} / B {crash(coordinator after prepare[→0]:Txn(1))} |
| `two_pc_ldfi::ldfi_all_labels_with_the_harness_fault_model` | `two_pc_ldfi.rs:92` | cex / cex | 2 / 2 | 64 / 64 | 392 / 389 | 8 / 6 | tie | L {crash(coordinator after commit[→0]:Txn(1))} / B {crash(coordinator after commit[→0]:Txn(1))} |
| `two_pc_ldfi::ldfi_all_labels_with_the_harness_fault_model` | `two_pc_ldfi.rs:98` | vacuous / vacuous | 0 / 0 | 0 / 0 | 50 / 52 | None / None | tie |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_harness_fault_model` | `two_pc_ldfi.rs:161` | vacuous / vacuous | 0 / 0 | 0 / 0 | 53 / 48 | None / None | tie |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_coordinator_correct` | `two_pc_ldfi.rs:86` | cert / exhausted | 1 / 1 | 16 / 16 | 110 / 109 | 2 / 2 | tie |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_coordinator_correct` | `two_pc_ldfi.rs:92` | cert / exhausted | 1 / 1 | 16 / 16 | 105 / 105 | 4 / 2 | tie |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_coordinator_correct` | `two_pc_ldfi.rs:98` | vacuous / vacuous | 0 / 0 | 0 / 0 | 48 / 48 | None / None | tie |  |
| `two_pc_ldfi::ldfi_all_labels_with_the_coordinator_correct` | `two_pc_ldfi.rs:161` | vacuous / vacuous | 0 / 0 | 0 / 0 | 49 / 49 | None / None | tie |  |
| `abd::all_labels_reports_the_register_as_not_a_stream` | `abd.rs:150` | cex / cex | 2 / 2 | 16 / 16 | 146 / 151 | 12 / 13 | tie | L {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} / B {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} |
| `abd::all_labels_reports_the_register_as_not_a_stream` | `abd.rs:161` | cex / cex | 2 / 2 | 16 / 16 | 145 / 147 | 12 / 12 | tie | L {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} / B {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} |
| `abd::all_labels_reports_the_register_as_not_a_stream` | `abd.rs:141` | cex / cex | 2 / 5 | 16 / 40 | 123 / 257 | 6 / 12 | L | L {crash(client@0 after abd.rs:141[0→1]:Query { rid: 1 })} / B {crash(client@0 after abd.rs:141[0→1]:Query { rid: 1 })} |
| `abd::all_labels_reports_the_register_as_not_a_stream` | `abd.rs:314` | cex / cex | 2 / 2 | 16 / 16 | 144 / 145 | 12 / 14 | tie | L {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} / B {crash(client@0 after abd.rs:314[0→1]:Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: 10 })} |

## Labels not checked (identical in both strategies)

- `abd::all_labels_reports_the_register_as_not_a_stream` `abd.rs:165`: Singleton (not a set-valued collection)
- `ec_all_labels_classes::all_labels_reports_a_label_inside_a_tick` `ec_all_labels_classes.rs:36`: inside a tick
- `ec_demos_ldfi::ec_demo_multi_paxos_all_labels` `epoch_splice.rs:190`: Singleton (not a set-valued collection)

## Reproducing

`scratch/run1.sh PKG TEST STRATEGY OUTNAME` (needs `$TMPDIR/env.sh` with PATH/LIBCLANG_PATH/CARGO_TARGET_DIR) and `scratch/all.sh {lineage|blind} PARALLELISM`. Then `parse.py` and `report.py` regenerate the table from `scratch/logs/`.
