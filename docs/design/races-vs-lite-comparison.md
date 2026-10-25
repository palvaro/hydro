# Races (the only engine) against the lite baseline

Stage 2 of docs/design/hydrofrac-races.md §12: `EcCheck::new()` is the races search, and lite is deleted. This file
compares every label result of `docs/design/races-vs-lite-lite-baseline.md` (240 rows, lite at `d2e851a8c5`) with the
same label in the same test now.

**How it was measured.**
- The same temporary per-label print at the end of `EcCheck::run` as the baseline. It was reverted and is not
  committed.
- The baseline's commands:
  - `cargo test -p hydrofrac --features cross-check --lib`;
  - `cargo test -p hydro_std --lib hydrofrac`;
  - `cargo test -p hydro_test --lib cluster::ec_`.
- The same machine class, a debug build, and parallel test threads.
- Renamed tests are matched by their new name:
  - `both_agree_*` became `brute_force_agrees_*`;
  - `races_targets_*_lite` became `races_targets_*`;
  - `a_program_with_a_schedule_choice_is_exhausted_not_certified` became
    `a_schedule_choice_with_robust_derivations_is_certified`;
  - `assert_holds_rejects_an_exhausted_search` became `assert_holds_rejects_a_sampled_search`.
- A label that recurs in a test is matched by its occurrence.

**Columns.**
- The verdict notation is the baseline's.
- "Comparison" is one of:
  - `same`: the identical verdict;
  - `same claim`: the same kind of verdict and the same certification of its tolerance;
  - `stronger`: a certificate where there was an exhaustion, a higher certified bound, or a certified tolerance where
    there was an uncertified one;
  - `cert→cex`: a counterexample where lite certified; the enumeration confirms it (S1).
- `×` is the ratio new / lite; **bold** marks more than 2×.

## Summary

- **Matched:** 232 of 240 rows. The other 8 come from `fragile_mode_leaves_the_check_unchanged`, which compared lite
  with lite in fragile mode, and is deleted.
- **No row is weaker.**
  - 185 are the same and 4 make the same claim.
  - 42 are stronger:
    - 20 `EXH` become `CERT` (16 `All`, 4 `UpTo 0`);
    - 22 counterexamples keep their verdict, but their tolerance is now certified.
  - 1 is `cert→cex`: S1's false certificate.
- **Totals over the matched rows:** fault sets 1,340 → 2,856 (2.1×); schedules 85,051 → 59,647 (0.70×).
- **Past 2×:** 29 rows on sets, 30 on schedules, in 7 groups:
  1. **`reliable_broadcast_closed` `new_messages`, 2 and 3 payloads** (5.4×, 6.1×; 6 rows). Lite's positional rule
     knew the order between two payloads' broadcasts. The order-free rule does not (§11's regression), and §12 chose to
     leave those literals free. The program has no choices, so each set is one schedule.
  2. **ABD over reliable phase two, sampled, `phase2_fwd` and `abd_register_state`** (8.4×). The register state goes
     through `covering_quorum`, which is not trusted (§8.2), so every derivation is fragile. No pair is protected, and
     the ILP proposes every fault set within the crash bounds over the revealed faults: 176 sets.
     - The old races' fallback enumerated at most two faults (6.2×).
     - Lite's heuristic needed to kill only one schedule's clause per run, and stopped at 21.
     - The cost is what §12.4 accepted for labels whose derivations pass a fragile step.
  3. **G-Set gossip `all_elements`, sampled** (3.7–4.0×). Same cause: the gossip snapshot is a fragile step.
  4. **CTP-3 `committed`, sampled** (3.2×; 4 rows in 2 tests) and its `prepares` (2.0–2.5×). Lite's heuristic jumped to
     a breaking set. The search runs every one-crash set first, since no derivation protects `committed`.
  5. **Multi-Paxos `prepares`** (8.0×) and **`accepts`** (2.2–2.6×). The ballot logic is fragile. The counterexample is a
     one-crash set, and the search tries the other admissible one-crash sets first.
  6. **One-crash counterexamples of other labels of fragile programs, 2 → 5 sets** (2.5×): ABD `queries`, gossip
     `initial` and `echo`. The candidates are now every crash point the runs revealed, not only the clauses' named
     ones. The tie-break tries later crash points first, and a crash after a send that came later in the run cuts
     nothing, but its literal is free, so it is proposed and run.
  7. **CTP-2 quiescing `prepares`** (2.0× sets, 52× schedules). This row is stronger: lite's tolerance (`tol=Some(0)`)
     was not certified, and now it is. The search ran every schedule of the sets that certify it.

## Update: item C (sequenced broadcasts, then the tie-break)

Measured with the same per-label print, over the 280 label rows of the three suites (`hydrofrac --features
cross-check`, `hydro_std --lib hydrofrac`, `hydro_test --lib cluster::ec_`), from `664c60e2f1`:

| step | fault sets | schedules | rows cheaper / dearer | verdicts |
|---|---|---|---|---|
| before | 3,615 | 80,409 | | |
| + sequenced broadcasts (§6.4 of `hydrofrac-soundness.md`) | 2,639 | 55,660 | 26 / 4 | 1 stronger: `ec_demo_uniform_broadcast` label 0's one-crash tolerance is now certified: its crash sets run one schedule, exactly. That demo has no brute-force cross-check, so this rests on §6.4 (ARGUED) |
| + tie-break by proved strength | 2,555 | 52,295 | 29 / 38 against the previous step | none: same kind, fault count, tolerance and certification. 2 rows find another one-crash counterexample (2 missing pairs instead of 1) |

- **Group 1 is resolved.** `reliable_broadcast_closed` `new_messages`: 2 payloads 134 → 24 sets, 3 payloads 281 → 49
  (lite: 25, 46). The payloads come from one harness input, so the order between their broadcasts is now proved.
  Also: plain broadcast with 3 payloads 9,920 → 2 schedules, the uniform broadcast demo's two sampled labels 282 → 78 and 80 sets.
- **Group 6 is mixed.** Proposing the earliest-sent of equally strong crash points first helps where the break is an
  early crash, and costs where it is a late one.
  - Cheaper: ABD `queries` 5 → 3 (both ABD tests), the G-Set gossip demo's first label 50 → 12 (its two runs
    summed), Multi-Paxos `prepares` 16 → 6, `accepts_fwd` 12 → 8, `accepts` 15 → 8.
  - Dearer: several CTP labels 3 → 5, ABD `initial` 3 → 5, Multi-Paxos `initial` 7 → 14.
  - On balance it saves 3% of sets and 6% of schedules.

## Every matched row

| test | label | lite verdict | new verdict | comparison | lite sets / schedules | new sets / schedules | sets × | schedules × |
|---|---|---|---|---|---:|---:|---:|---:|
| `check::brute_tests::brute_force_agrees_on_a_lossy_relay_chain` | `relayed (brute_tests.rs)` | CEX(1 crashes, 2 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same claim | 8 / 8 | 8 / 8 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_lossy_broadcast` | `delivered (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_plain_broadcast` | `delivered (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echo_broadcast_in (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echoed (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echoed (brute_tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 6 / 6 | 3 / 3 | 0.50 | 0.50 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `delivered (brute_tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 7 / 7 | 3 / 3 | 0.43 | 0.43 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `owned (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `heard (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `heard (brute_tests.rs #1)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `known (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echo_broadcast_in (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echoed (brute_tests.rs)` | CERT(All) | CERT(All) | same | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echoed (brute_tests.rs #1)` | CERT(All) | CERT(All) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `delivered (brute_tests.rs)` | CERT(All) | CERT(All) | same | 3 / 3 | 2 / 2 | 0.67 | 0.67 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echo_broadcast_in (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echoed (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same claim | 11 / 11 | 5 / 5 | 0.45 | 0.45 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echoed (brute_tests.rs #1)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 3 faults; tol=Some(0) cert) | same | 7 / 7 | 5 / 5 | 0.71 | 0.71 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `delivered (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 3 faults; tol=Some(0) cert) | same | 11 / 11 | 5 / 5 | 0.45 | 0.45 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `a (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `echoed (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same claim | 11 / 11 | 5 / 5 | 0.45 | 0.45 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `echoed (brute_tests.rs #1)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 3 faults; tol=Some(0) cert) | same | 7 / 7 | 5 / 5 | 0.71 | 0.71 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `facts (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | CEX(1 crashes, 3 faults; tol=Some(0) cert) | same | 11 / 11 | 5 / 5 | 0.45 | 0.45 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `b (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::the_cross_check_catches_wrong_verdicts` | `delivered (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::a_lossy_broadcast_breaks_by_omission_alone` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` | `owned (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` | `heard (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` | `heard (brute_tests.rs #1)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` | `g (brute_tests.rs)` | CERT(UpTo 0) | CERT(UpTo 0) | same | 1 / 1 | 1 / 1 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` | `known (brute_tests.rs)` | CERT(UpTo 0) | CEX(0 crashes, 1 faults; tol=None cert) | cert→cex | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `check::tests::a_body_that_panics_is_a_counterexample` | `delivered (tests.rs)` | CEX(0 crashes, 0 faults, panic; tol=None cert) | CEX(0 crashes, 0 faults; tol=None cert) | same | 1 / 2 | 1 / 0 | 1.00 | 0.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` | `owned (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` | `heard (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` | `heard (brute_tests.rs #1)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` | `g (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` | `known (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(0 crashes, 1 faults; tol=None cert) | same claim | 3 / 3 | 3 / 3 | 1.00 | 1.00 |
| `check::tests::a_plain_broadcast_tolerates_no_crash_and_breaks_at_a_mid_fan_out_crash` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::a_label_that_sees_nothing_without_faults_is_vacuous` | `delivered (tests.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 1 | 1 / 1 | 1.00 | 1.00 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 6 / 6 | 3 / 3 | 0.50 | 0.50 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 7 / 7 | 3 / 3 | 0.43 | 0.43 |
| `check::tests::counterexamples_write_their_lamport_diagram` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::assert_holds_panics_with_the_report` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::sampled_schedules_find_the_same_counterexample` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 8 | 2 / 2 | 1.00 | 0.25 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `echoed (tests.rs #1)` | CERT(UpTo 1) | CERT(UpTo 1) | same | 5 / 5 | 2 / 2 | 0.40 | 0.40 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `delivered (tests.rs)` | CERT(UpTo 1) | CERT(UpTo 1) | same | 4 / 4 | 2 / 2 | 0.50 | 0.50 |
| `check::tests::message_payloads_render_by_their_debug` | `orders (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::members_that_disagree_without_faults_break_at_the_probe` | `tagged (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::members_that_disagree_without_faults_break_at_the_probe` | `tagged (tests.rs #1)` | CEX(0 crashes, 0 faults; tol=None cert) | CEX(0 crashes, 0 faults; tol=None cert) | same | 1 / 1 | 1 / 1 | 1.00 | 1.00 |
| `check::tests::the_report_states_omission_only_counterexamples` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_report_is_in_source_names` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_report_is_in_source_names` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_report_is_in_source_names` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 6 / 6 | 3 / 3 | 0.50 | 0.50 |
| `check::tests::the_report_is_in_source_names` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 7 / 7 | 3 / 3 | 0.43 | 0.43 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 6 / 6 | 3 / 3 | 0.50 | 0.50 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | CEX(2 crashes, 2 faults; tol=Some(1) cert) | same | 7 / 7 | 3 / 3 | 0.43 | 0.43 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 1750 | 2 / 2 | 1.00 | 0.00 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `echoed (tests.rs)` | EXH(All) | CERT(All) | stronger | 3 / 1751 | 3 / 3 | 1.00 | 0.00 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `echoed (tests.rs #1)` | EXH(All) | CERT(All) | stronger | 2 / 1743 | 2 / 2 | 1.00 | 0.00 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `delivered (tests.rs)` | EXH(All) | CERT(All) | stronger | 3 / 1751 | 2 / 2 | 0.67 | 0.00 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `initial (tests.rs)` | EXH(UpTo 0) | CERT(UpTo 0) | stronger | 1 / 1742 | 1 / 1 | 1.00 | 0.00 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `echoed (tests.rs)` | EXH(UpTo 0) | CERT(UpTo 0) | stronger | 1 / 1742 | 1 / 1 | 1.00 | 0.00 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `echoed (tests.rs #1)` | EXH(UpTo 0) | CERT(UpTo 0) | stronger | 1 / 1742 | 1 / 1 | 1.00 | 0.00 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `delivered (tests.rs)` | EXH(UpTo 0) | CERT(UpTo 0) | stronger | 1 / 1742 | 1 / 1 | 1.00 | 0.00 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `phase2_fwd (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `abd_register_state (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `abd_register_state (abd.rs #1)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `queries (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 5 / 33 | **2.50** | **2.06** |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `phase2 (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `phase2_fwd (abd.rs)` | EXH(All) | EXH(All) | same | 21 / 168 | 176 / 1408 | **8.38** | **8.38** |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `abd_register_state (abd.rs)` | EXH(All) | EXH(All) | same | 21 / 168 | 176 / 1408 | **8.38** | **8.38** |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `abd_register_state (abd.rs #1)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `queries (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 5 / 33 | **2.50** | **2.06** |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 9 / 72 | 12 / 89 | 1.33 | 1.24 |
| `cluster::ec_bruteforce::brute_force_agrees_on_ask_once` | `known (ec_bruteforce.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 43 | 4 / 74 | 2.00 | 1.72 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 46 / 46 | 281 / 281 | **6.11** | **6.11** |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 10 / 10 | 8 / 8 | 0.80 | 0.80 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 214 | 2 / 2 | 1.00 | 0.01 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` | `echoes (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 215 | 2 / 2 | 0.67 | 0.01 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` | `echo (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 215 | 2 / 2 | 0.67 | 0.01 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` | `deliveries (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 215 | 2 / 2 | 0.67 | 0.01 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 25 / 25 | 134 / 134 | **5.36** | **5.36** |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 8 | 2 / 2 | 1.00 | 0.25 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 6 | 1 / 1 | 1.00 | 0.17 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 6 | 1 / 1 | 1.00 | 0.17 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 8 | 2 / 2 | 1.00 | 0.25 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 282 | 3 / 224 | 1.50 | 0.79 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 222 | 1 / 1 | 1.00 | 0.00 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 222 | 1 / 1 | 1.00 | 0.00 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 282 | 3 / 224 | 1.50 | 0.79 |
| `cluster::ec_bruteforce::brute_force_finds_the_ask_once_race` | `known (ec_bruteforce.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 43 | 4 / 74 | 2.00 | 1.72 |
| `cluster::ec_demos_hydrofrac::ec_demo_broadcast_live_breaks_under_a_sender_crash` | `fan_out_from_process (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_leader_merge_breaks_under_a_leader_crash` | `leader_merge_broadcast (leader_merge.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 4 / 8 | 3 / 4 | 0.75 | 0.50 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 5 / 257 | **2.50** | **2.01** |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 64 | 1 / 64 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 64 | 1 / 64 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | CEX(2 crashes, 2 faults; tol=Some(1)) | same | 13 / 832 | 42 / 2627 | **3.23** | **3.16** |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 5 / 257 | **2.50** | **2.01** |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 64 | 1 / 64 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 64 | 1 / 64 | 1.00 | 1.00 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | CEX(2 crashes, 2 faults; tol=Some(1)) | same | 13 / 832 | 42 / 2627 | **3.23** | **3.16** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `initial (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 5 / 33 | **2.50** | **2.06** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `all_elements (crdt_gossip.rs)` | EXH(All) | EXH(All) | same | 10 / 80 | 37 / 296 | **3.70** | **3.70** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `gset_state (crdt_gossip.rs)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `echo (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 7 / 49 | **3.50** | **3.06** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `initial (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 5 / 33 | **2.50** | **2.06** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `all_elements (crdt_gossip.rs)` | EXH(All) | EXH(All) | same | 10 / 80 | 37 / 296 | **3.70** | **3.70** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `gset_state (crdt_gossip.rs)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `echo (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 7 / 49 | **3.50** | **3.06** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `all_elements (crdt_gossip.rs)` | EXH(All) | EXH(All) | same | 10 / 80 | 40 / 320 | **4.00** | **4.00** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `gset_state (crdt_gossip.rs)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `all_elements (crdt_gossip.rs)` | EXH(All) | EXH(All) | same | 10 / 80 | 40 / 320 | **4.00** | **4.00** |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `gset_state (crdt_gossip.rs)` | COV(1) | COV(1) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 25 / 25 | 134 / 134 | **5.36** | **5.36** |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 25 / 25 | 134 / 134 | **5.36** | **5.36** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `prepares (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 16 / 121 | **8.00** | **7.56** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `accepts_fwd (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 5 / 40 | 11 / 81 | **2.20** | **2.02** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `accepts (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 5 / 40 | 11 / 81 | **2.20** | **2.02** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned_at_proposers (multi_paxos.rs)` | VAC(SingleMember) | VAC(SingleMember) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned_at_proposers (multi_paxos.rs #1)` | VAC(SingleMember) | VAC(SingleMember) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `initial (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 16 / 128 | 6 / 41 | 0.38 | 0.32 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `echo (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 16 / 128 | 6 / 41 | 0.38 | 0.32 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned (multi_paxos.rs)` | EXH(UpTo 1) | EXH(UpTo 1) | same | 31 / 248 | 52 / 416 | 1.68 | 1.68 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `facts (multi_paxos.rs)` | EXH(UpTo 1) | EXH(UpTo 1) | same | 31 / 248 | 52 / 416 | 1.68 | 1.68 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `splice_epoch_log (epoch_splice.rs)` | COV(8) | COV(8) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `prepares (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 16 / 121 | **8.00** | **7.56** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `accepts_fwd (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 5 / 40 | 13 / 97 | **2.60** | **2.42** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `accepts (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 5 / 40 | 13 / 97 | **2.60** | **2.42** |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned_at_proposers (multi_paxos.rs)` | VAC(SingleMember) | VAC(SingleMember) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned_at_proposers (multi_paxos.rs #1)` | VAC(SingleMember) | VAC(SingleMember) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `initial (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 16 / 128 | 7 / 49 | 0.44 | 0.38 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `echo (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 16 / 128 | 9 / 65 | 0.56 | 0.51 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned (multi_paxos.rs)` | EXH(UpTo 1) | EXH(UpTo 1) | same | 31 / 248 | 52 / 416 | 1.68 | 1.68 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `facts (multi_paxos.rs)` | EXH(UpTo 1) | EXH(UpTo 1) | same | 31 / 248 | 52 / 416 | 1.68 | 1.68 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `splice_epoch_log (epoch_splice.rs)` | COV(8) | COV(8) | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `fan_out_from_process (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `new_messages (reliable_broadcast.rs)` | CERT(All) | CERT(All) | same | 25 / 25 | 134 / 134 | **5.36** | **5.36** |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_all_labels` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_all_labels_writes_diagrams` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 3 / 3 | 1.50 | 1.50 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission` | `at_members (ec_hydrofrac.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | CEX(0 crashes, 1 faults; tol=None cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash` | `at_members (ec_hydrofrac.rs #1)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | same | 2 / 2 | 2 / 2 | 1.00 | 1.00 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 8 | 2 / 2 | 1.00 | 0.25 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 6 | 1 / 1 | 1.00 | 0.17 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 6 | 1 / 1 | 1.00 | 0.17 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 8 | 2 / 2 | 1.00 | 0.25 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `at_members (ec_hydrofrac_classes.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 282 | 3 / 224 | 1.50 | 0.79 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `in_tick (ec_hydrofrac_classes.rs)` | NC | NC | same | 0 / 0 | 0 / 0 | 1.00 | 1.00 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `out (ec_hydrofrac_classes.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 282 | 3 / 224 | 1.50 | 0.79 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 12524 | 3 / 9920 | 1.50 | 0.79 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 9918 | 1 / 1 | 1.00 | 0.00 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 9918 | 1 / 1 | 1.00 | 0.00 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 12524 | 3 / 9920 | 1.50 | 0.79 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 128 | 2 / 2 | 1.00 | 0.02 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echoes (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 5 / 320 | 3 / 3 | 0.60 | 0.01 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 5 / 320 | 3 / 3 | 0.60 | 0.01 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `deliveries (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 27 / 1728 | 6 / 6 | 0.22 | 0.00 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `distinct_acks (quorum.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `certified (quorum.rs)` | EXH(All) | EXH(All) | same | 84 / 672 | 140 / 1106 | 1.67 | 1.65 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | EXH(All) | same | 84 / 672 | 140 / 1106 | 1.67 | 1.65 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 4 / 193 | 2.00 | 1.51 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` | `peer_in (two_pc_ctp.rs)` | EXH(All) | EXH(All) | same | 5 / 320 | 6 / 384 | 1.20 | 1.20 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` | `peer (two_pc_ctp.rs)` | EXH(All) | EXH(All) | same | 5 / 320 | 6 / 384 | 1.20 | 1.20 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 42 | 4 / 2177 | 2.00 | **51.83** |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 2144 | 3 / 65 | 1.50 | 0.03 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 32 | 1 / 32 | 1.00 | 1.00 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 32 | 1 / 32 | 1.00 | 1.00 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` | `committed (two_pc_ctp.rs)` | EXH(All) | CERT(All) | stronger | 5 / 2428 | 6 / 2460 | 1.20 | 1.01 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 112 | 2 / 2 | 1.00 | 0.02 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` | `echo (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` | `distinct_acks (quorum.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` | `certified (quorum.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 2 / 2 | 0.67 | 0.02 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 2 / 2 | 0.67 | 0.02 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `distinct_acks (quorum.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 10 | 1.50 | 0.62 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `certified (quorum.rs)` | EXH(All) | EXH(All) | same | 84 / 672 | 141 / 1107 | 1.68 | 1.65 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | EXH(All) | same | 84 / 672 | 142 / 1116 | 1.69 | 1.66 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0) cert) | stronger | 2 / 112 | 2 / 2 | 1.00 | 0.02 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` | `echo (reliable_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` | `distinct_acks (quorum.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` | `certified (quorum.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | CERT(All) | stronger | 3 / 113 | 3 / 3 | 1.00 | 0.03 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 4 / 193 | 2.00 | 1.51 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `peer_in (two_pc_ctp.rs)` | EXH(All) | EXH(All) | same | 5 / 320 | 6 / 384 | 1.20 | 1.20 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `peer (two_pc_ctp.rs)` | EXH(All) | EXH(All) | same | 5 / 320 | 6 / 384 | 1.20 | 1.20 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 128 | 3 / 129 | 1.50 | 1.01 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 4 / 25 | 2.00 | 1.56 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 8 | 1 / 8 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 8 | 1 / 8 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 4 / 25 | 2.00 | 1.56 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 16 | 3 / 17 | 1.50 | 1.06 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 8 | 1 / 8 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 8 | 1 / 8 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `committed (two_pc_ctp.rs)` | EXH(All) | EXH(All) | same | 5 / 40 | 6 / 48 | 1.20 | 1.20 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 512 | 5 / 1025 | **2.50** | **2.00** |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | CEX(1 crashes, 1 faults; tol=Some(0)) | same | 2 / 512 | 3 / 513 | 1.50 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 256 | 1 / 256 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | VAC(EmptyDomain) | same | 1 / 256 | 1 / 256 | 1.00 | 1.00 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | CEX(2 crashes, 2 faults; tol=Some(1)) | same | 13 / 3328 | 42 / 10503 | **3.23** | **3.16** |
