# Lite baseline over lite's corpus (raw numbers)

The lite-vs-races comparison is paused until the races encoding is redesigned. This file keeps the lite half of it: one
row per label for every `EcCheck` run in the test suites, in lite mode (`EcCheck::new()`).

**How it was measured.**
- Code: the `hydrofrac-races` branch at `d2e851a8c5`, so this is lite mode as of that commit. Lite's engine there is not
  byte-for-byte the one at `58d0b3bcbf`.
- Instrumentation: a temporary print at the end of `EcCheck::run` wrote one line per label. It was reverted and is not
  committed. A temporary `EcCheck::new()` → `EcCheck::races()` switch was also added, but was off for these runs.
- Hardware and settings: an 8-core, 15 GB machine, debug profile, default test threads (parallel), so `wall ms` is noisy.

**Columns.** These come from `Stats` (`hydrofrac/src/check/results.rs`):
- `sets` is `hypotheses`: the fault sets tried, counting the failure-free probe.
- `schedules` is the number of schedules explored over those sets. A run shared by several labels counts once for each
  label, so subtotals overcount real executions.
- `wall ms` is the label's search time, counting the runs it caused.

**Verdicts.**
- `CERT(All)` / `CERT(UpTo k)` mean `Certified`.
- `CEX(c crashes, f faults; tol=…)` means `Counterexample` with `c` crashes and `f` faults in total. `tol` is the
  cleared crash bound, and `cert` means that bound is certified.
- `EXH(…)` is `Exhausted`, `BUDGET` is `Budget`, `VAC(reason)` is `Vacuous`, `NC` is `NotChecked` and `COV(i)` is
  `CoveredBy` label `i`.

A test may run several checks. Its rows then appear in run order, and the same label can recur.

† marks a test added on the races branch, i.e. not present at `58d0b3bcbf`. The rest are lite's corpus proper.

## `hydrofrac`

Command: `cargo test -p hydrofrac --features cross-check --lib --no-fail-fast` → 173 passed, 0 failed, 0 ignored. 80 lite label results from 28 tests.

Left out: 29 rows from runs built with `EcCheck::races()` directly, in `races_agree_on_a_lossy_broadcast`, `races_agree_on_a_lossy_relay_chain`, `races_agree_on_a_plain_broadcast`, `races_agree_on_an_echo_broadcast`, `races_agree_on_an_echo_over_a_lossy_first_hop`, `races_agree_where_a_total_loss_masks_a_partial_loss`, `races_certify_a_batched_echo`, `races_find_a_broadcast_whose_members_do_not_re_forward`.

| test | label | verdict | sets | schedules | wall ms |
|---|---|---|---:|---:|---:|
| `check::brute_tests::brute_force_agrees_on_a_lossy_relay_chain` | `relayed (brute_tests.rs)` | CEX(1 crashes, 2 faults; tol=Some(0) cert) | 8 | 8 | 10 |
| `check::brute_tests::brute_force_agrees_on_a_lossy_broadcast` | `delivered (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 1 |
| `check::brute_tests::brute_force_agrees_on_a_plain_broadcast` | `delivered (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echo_broadcast_in (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 5 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echoed (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `echoed (brute_tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 51 |
| `check::brute_tests::brute_force_agrees_on_an_echo_broadcast` | `delivered (brute_tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 28 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `owned (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `heard (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 3 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `heard (brute_tests.rs #1)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::brute_tests::brute_force_agrees_on_an_ownership_exchange` | `known (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echo_broadcast_in (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 3 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echoed (brute_tests.rs)` | CERT(All) | 3 | 3 | 3 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `echoed (brute_tests.rs #1)` | CERT(All) | 2 | 2 | 2 |
| `check::brute_tests::brute_force_agrees_on_a_two_member_exchange` | `delivered (brute_tests.rs)` | CERT(All) | 3 | 3 | 2 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echo_broadcast_in (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 4 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echoed (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 11 | 11 | 21 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `echoed (brute_tests.rs #1)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 7 | 7 | 4 |
| `check::brute_tests::brute_force_agrees_on_an_echo_over_a_lossy_first_hop` | `delivered (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 11 | 11 | 7 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `a (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 4 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `echoed (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 11 | 11 | 22 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `echoed (brute_tests.rs #1)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 7 | 7 | 4 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `facts (brute_tests.rs)` | CEX(1 crashes, 3 faults; tol=Some(0) cert) | 11 | 11 | 9 |
| `check::brute_tests::brute_force_agrees_where_provenance_precision_matters` | `b (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 38 |
| `check::brute_tests::the_cross_check_catches_wrong_verdicts` | `delivered (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::tests::a_lossy_broadcast_breaks_by_omission_alone` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 1 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` † | `owned (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 2 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` † | `heard (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 3 | 3 | 3 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` † | `heard (brute_tests.rs #1)` | CEX(0 crashes, 1 faults; tol=None cert) | 3 | 3 | 1 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` † | `g (brute_tests.rs)` | CERT(UpTo 0) | 1 | 1 | 1 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_certificate` † | `known (brute_tests.rs)` | CERT(UpTo 0) | 2 | 2 | 1 |
| `check::tests::a_body_that_panics_is_a_counterexample` | `delivered (tests.rs)` | CEX(0 crashes, 0 faults, panic; tol=None cert) | 1 | 2 | 126 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` † | `owned (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 3 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` † | `heard (brute_tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 3 | 3 | 3 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` † | `heard (brute_tests.rs #1)` | CEX(0 crashes, 1 faults; tol=None cert) | 3 | 3 | 2 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` † | `g (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 3 |
| `check::brute_tests::suspected_total_loss_masks_a_partial_loss_tolerance` † | `known (brute_tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 3 | 3 | 2 |
| `check::tests::a_plain_broadcast_tolerates_no_crash_and_breaks_at_a_mid_fan_out_crash` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::tests::a_label_that_sees_nothing_without_faults_is_vacuous` | `delivered (tests.rs)` | VAC(EmptyDomain) | 1 | 1 | 0 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 9 |
| `check::tests::an_echo_broadcast_tolerates_one_crash_and_breaks_at_two` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 8 |
| `check::tests::counterexamples_write_their_lamport_diagram` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 9 |
| `check::tests::assert_holds_panics_with_the_report` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 9 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 10 |
| `check::tests::sampled_schedules_find_the_same_counterexample` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 8 | 20 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `echoed (tests.rs #1)` | CERT(UpTo 1) | 5 | 5 | 30 |
| `check::tests::the_crash_cap_ends_the_sweep_with_a_bounded_certificate` | `delivered (tests.rs)` | CERT(UpTo 1) | 4 | 4 | 37 |
| `check::tests::message_payloads_render_by_their_debug` | `orders (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 1 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 1 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 5 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 12 |
| `check::tests::fragile_mode_leaves_the_check_unchanged` † | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 10 |
| `check::tests::members_that_disagree_without_faults_break_at_the_probe` | `tagged (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `check::tests::members_that_disagree_without_faults_break_at_the_probe` | `tagged (tests.rs #1)` | CEX(0 crashes, 0 faults; tol=None cert) | 1 | 1 | 0 |
| `check::tests::the_report_states_omission_only_counterexamples` | `delivered (tests.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 2 |
| `check::tests::the_report_is_in_source_names` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::the_report_is_in_source_names` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::the_report_is_in_source_names` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 10 |
| `check::tests::the_report_is_in_source_names` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 9 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 4 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `echoed (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 3 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `echoed (tests.rs #1)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 6 | 6 | 7 |
| `check::tests::the_check_tracks_record_provenance_precisely` | `delivered (tests.rs)` | CEX(2 crashes, 2 faults; tol=Some(1) cert) | 7 | 7 | 7 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `initial (tests.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 1750 | 5127 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `echoed (tests.rs)` | EXH(All) | 3 | 1751 | 5118 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `echoed (tests.rs #1)` | EXH(All) | 2 | 1743 | 5117 |
| `check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified` | `delivered (tests.rs)` | EXH(All) | 3 | 1751 | 5127 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `initial (tests.rs)` | EXH(UpTo 0) | 1 | 1742 | 5546 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `echoed (tests.rs)` | EXH(UpTo 0) | 1 | 1742 | 5560 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `echoed (tests.rs #1)` | EXH(UpTo 0) | 1 | 1742 | 5546 |
| `check::tests::assert_holds_rejects_an_exhausted_search` | `delivered (tests.rs)` | EXH(UpTo 0) | 1 | 1742 | 5547 |

Subtotal: 80 rows, 272 sets, 14228 schedules, 43328 ms.

## `hydro_std`

Command: `cargo test -p hydro_std --lib ec_abd_hydrofrac --no-fail-fast` → 2 passed, 0 failed, 1 ignored (`races_targets_probe_abd`, "measurement"). 11 lite label results from 2 tests.

| test | label | verdict | sets | schedules | wall ms |
|---|---|---|---:|---:|---:|
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `phase2_fwd (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 75 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `abd_register_state (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 53 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `abd_register_state (abd.rs #1)` | COV(1) | 0 | 0 | 0 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `queries (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 61 |
| `ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two` | `phase2 (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 53 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `phase2_fwd (abd.rs)` | EXH(All) | 21 | 168 | 708 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `abd_register_state (abd.rs)` | EXH(All) | 21 | 168 | 121 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `abd_register_state (abd.rs #1)` | COV(1) | 0 | 0 | 0 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `queries (abd.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 73 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 73 |
| `ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 9 | 72 | 175 |

Subtotal: 11 rows, 63 sets, 504 schedules, 1392 ms.

## `hydro_test`

Command: `cargo test -p hydro_test --lib cluster::ec_ --no-fail-fast` → 46 passed, 0 failed, 10 ignored (`ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members_with_two_payloads`, `ec_hydrofrac::tests::ec_echo_broadcast_exhaustively`, and the eight `ec_races_targets::races_targets_probe_*`). 149 lite label results from 35 tests.

Left out: 55 rows from runs built with `EcCheck::races()` directly, in `both_agree_on_a_plain_broadcast_with_three_payloads`, `both_agree_on_a_plain_broadcast_with_two_payloads`, `both_agree_on_an_echo_broadcast_over_two_members`, `both_agree_on_reliable_broadcast_closed_with_three_payloads`, `both_agree_on_reliable_broadcast_closed_with_two_payloads`, `races_agree_on_a_plain_broadcast`, `races_agree_on_ask_once`, `races_agree_on_reliable_broadcast_closed`, `races_agree_on_two_pc_cooperative_termination`, `races_alone_on_an_echo_broadcast_over_three_members`, `races_targets_ctp2_quiescing_races`, `races_targets_ctp2_racing_sampled_races`, `races_targets_urb2_threshold1_races`, `races_targets_urb2_threshold2_races`.

| test | label | verdict | sets | schedules | wall ms |
|---|---|---|---:|---:|---:|
| `cluster::ec_bruteforce::brute_force_agrees_on_ask_once` | `known (ec_bruteforce.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 43 | 160 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 50 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` † | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 35 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_three_payloads` † | `new_messages (reliable_broadcast.rs)` | CERT(All) | 46 | 46 | 388 |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 17 |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 9 |
| `cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed` | `new_messages (reliable_broadcast.rs)` | CERT(All) | 10 | 10 | 47 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 214 | 743 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` † | `echoes (reliable_broadcast.rs)` | EXH(All) | 3 | 215 | 723 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` † | `echo (reliable_broadcast.rs)` | EXH(All) | 3 | 215 | 720 |
| `cluster::ec_bruteforce::both_agree_on_an_echo_broadcast_over_two_members` † | `deliveries (reliable_broadcast.rs)` | EXH(All) | 3 | 215 | 721 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 12 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` † | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 15 |
| `cluster::ec_bruteforce::both_agree_on_reliable_broadcast_closed_with_two_payloads` † | `new_messages (reliable_broadcast.rs)` | CERT(All) | 25 | 25 | 266 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 8 | 29 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 6 | 12 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 6 | 12 |
| `cluster::ec_bruteforce::brute_force_agrees_on_a_plain_broadcast` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 8 | 12 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 282 | 1497 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` † | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 222 | 1351 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` † | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 222 | 1351 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_two_payloads` † | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 282 | 1352 |
| `cluster::ec_bruteforce::brute_force_finds_the_ask_once_race` | `known (ec_bruteforce.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 43 | 315 |
| `cluster::ec_demos_hydrofrac::ec_demo_broadcast_live_breaks_under_a_sender_crash` | `fan_out_from_process (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 10 |
| `cluster::ec_demos_hydrofrac::ec_demo_leader_merge_breaks_under_a_leader_crash` | `leader_merge_broadcast (leader_merge.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 4 | 8 | 53 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 690 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 895 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 64 | 475 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 64 | 475 |
| `cluster::ec_bruteforce::brute_force_finds_the_two_pc_cooperative_termination_violation` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | 13 | 832 | 4386 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 838 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 1055 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 64 | 538 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 64 | 538 |
| `cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | 13 | 832 | 4950 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `initial (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 483 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `all_elements (crdt_gossip.rs)` | EXH(All) | 10 | 80 | 1079 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `gset_state (crdt_gossip.rs)` | COV(1) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `echo (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 531 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `initial (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 424 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `all_elements (crdt_gossip.rs)` | EXH(All) | 10 | 80 | 1177 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `gset_state (crdt_gossip.rs)` | COV(1) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `echo (crdt_gossip.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 353 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 833 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `all_elements (crdt_gossip.rs)` | EXH(All) | 10 | 80 | 2391 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_all_labels` | `gset_state (crdt_gossip.rs)` | COV(1) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 797 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `all_elements (crdt_gossip.rs)` | EXH(All) | 10 | 80 | 2121 |
| `cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies` | `gset_state (crdt_gossip.rs)` | COV(1) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 42 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 28 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_all_labels` | `new_messages (reliable_broadcast.rs)` | CERT(All) | 25 | 25 | 223 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 5 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `new_messages (reliable_broadcast.rs)` | CERT(All) | 25 | 25 | 104 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `prepares (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 597 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `accepts_fwd (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 40 | 1373 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `accepts (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 40 | 559 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned_at_proposers (multi_paxos.rs)` | VAC(SingleMember) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned_at_proposers (multi_paxos.rs #1)` | VAC(SingleMember) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `initial (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 16 | 128 | 3140 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `echo (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 16 | 128 | 2117 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `learned (multi_paxos.rs)` | EXH(UpTo 1) | 31 | 248 | 4505 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `facts (multi_paxos.rs)` | EXH(UpTo 1) | 31 | 248 | 3059 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels` | `splice_epoch_log (epoch_splice.rs)` | COV(8) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `prepares (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 282 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `accepts_fwd (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 40 | 912 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `accepts (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 40 | 254 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned_at_proposers (multi_paxos.rs)` | VAC(SingleMember) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned_at_proposers (multi_paxos.rs #1)` | VAC(SingleMember) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `initial (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 16 | 128 | 3182 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `echo (multi_paxos.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 16 | 128 | 1710 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `learned (multi_paxos.rs)` | EXH(UpTo 1) | 31 | 248 | 4039 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `facts (multi_paxos.rs)` | EXH(UpTo 1) | 31 | 248 | 3032 |
| `cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_learned_under_one_crash` | `splice_epoch_log (epoch_splice.rs)` | COV(8) | 0 | 0 | 0 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `fan_out_from_process (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 13 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `new_messages (reliable_broadcast.rs)` | CERT(All) | 25 | 25 | 169 |
| `cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies` | `fan_out (fan_out.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 6 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_all_labels` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 77 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_all_labels_writes_diagrams` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 2 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission` | `at_members (ec_hydrofrac.rs)` | CEX(0 crashes, 1 faults; tol=None cert) | 2 | 2 | 2 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash` | `at_members (ec_hydrofrac.rs)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 15 |
| `cluster::ec_hydrofrac::tests::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash` | `at_members (ec_hydrofrac.rs #1)` | CEX(1 crashes, 1 faults; tol=Some(0) cert) | 2 | 2 | 10 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 8 | 40 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 6 | 13 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 6 | 13 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 8 | 13 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `at_members (ec_hydrofrac_classes.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 282 | 734 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `in_tick (ec_hydrofrac_classes.rs)` | NC | 0 | 0 | 0 |
| `cluster::ec_hydrofrac_classes::tests::a_label_inside_a_tick_is_reported_not_checked` | `out (ec_hydrofrac_classes.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 282 | 581 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 12524 | 50942 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` † | `echoes (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 9918 | 42741 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` † | `echo (reliable_broadcast.rs)` | VAC(EmptyDomain) | 1 | 9918 | 42741 |
| `cluster::ec_bruteforce::both_agree_on_a_plain_broadcast_with_three_payloads` † | `deliveries (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 12524 | 42742 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 964 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echoes (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 320 | 1373 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 5 | 320 | 665 |
| `cluster::ec_hydrofrac::tests::ec_generated_spec_separates_plain_from_reliable_broadcast` | `deliveries (reliable_broadcast.rs)` | EXH(All) | 27 | 1728 | 6411 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 266 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 261 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `distinct_acks (quorum.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 135 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `certified (quorum.rs)` | EXH(All) | 84 | 672 | 14102 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | 84 | 672 | 9429 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` † | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 574 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` † | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 659 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` † | `peer_in (two_pc_ctp.rs)` | EXH(All) | 5 | 320 | 653 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` † | `peer (two_pc_ctp.rs)` | EXH(All) | 5 | 320 | 428 |
| `cluster::ec_races_targets::races_targets_ctp2_racing_sampled_lite` † | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 405 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` † | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 42 | 274 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` † | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 2144 | 11858 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` † | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 32 | 244 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` † | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 32 | 244 |
| `cluster::ec_races_targets::races_targets_ctp2_quiescing_lite` † | `committed (two_pc_ctp.rs)` | EXH(All) | 5 | 2428 | 1052 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 112 | 483 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` † | `echo (reliable_broadcast.rs)` | EXH(All) | 3 | 113 | 216 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` † | `distinct_acks (quorum.rs)` | EXH(All) | 3 | 113 | 215 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` † | `certified (quorum.rs)` | EXH(All) | 3 | 113 | 237 |
| `cluster::ec_races_targets::races_targets_urb2_threshold1_lite` † | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | 3 | 113 | 217 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 307 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `echo (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 245 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `distinct_acks (quorum.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 155 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `certified (quorum.rs)` | EXH(All) | 84 | 672 | 9123 |
| `cluster::ec_demos_hydrofrac::ec_demo_uniform_broadcast` | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | 84 | 672 | 5574 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` † | `initial (reliable_broadcast.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 112 | 288 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` † | `echo (reliable_broadcast.rs)` | EXH(All) | 3 | 113 | 175 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` † | `distinct_acks (quorum.rs)` | EXH(All) | 3 | 113 | 174 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` † | `certified (quorum.rs)` | EXH(All) | 3 | 113 | 174 |
| `cluster::ec_races_targets::races_targets_urb2_threshold2_lite` † | `uniform_reliable_broadcast_closed (uniform_broadcast.rs)` | EXH(All) | 3 | 113 | 174 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 530 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 626 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `peer_in (two_pc_ctp.rs)` | EXH(All) | 5 | 320 | 638 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `peer (two_pc_ctp.rs)` | EXH(All) | 5 | 320 | 396 |
| `cluster::ec_two_pc_hydrofrac::ctp2_racing_timers_search_finds_the_crash` | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 128 | 341 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 109 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 81 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 8 | 58 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 8 | 58 |
| `cluster::ec_two_pc_hydrofrac::plain_two_pc_breaks_at_one_coordinator_crash` | `committed (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 59 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 32 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 16 | 43 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 8 | 20 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 8 | 20 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_survives_the_coordinators_crash` | `committed (two_pc_ctp.rs)` | EXH(All) | 5 | 40 | 43 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `prepares (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 512 | 1576 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `commits (two_pc_ctp.rs)` | CEX(1 crashes, 1 faults; tol=Some(0)) | 2 | 512 | 2121 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `peer_in (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 256 | 937 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `peer (two_pc_ctp.rs)` | VAC(EmptyDomain) | 1 | 256 | 937 |
| `cluster::ec_two_pc_hydrofrac::two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants` | `committed (two_pc_ctp.rs)` | CEX(2 crashes, 2 faults; tol=Some(1)) | 13 | 3328 | 9860 |

Subtotal: 149 rows, 1039 sets, 70353 schedules, 331193 ms.

## Total

240 lite label results; 1374 sets, 85085 schedules, 375913 ms summed over rows.

