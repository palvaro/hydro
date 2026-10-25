# Handoff: hydrofrac-races

Written at a forced stop (agents moved to a new server). Plain English; every claim below is marked DEMONSTRATED
(test name), ARGUED (doc section), or NOT CHECKED.

> **WARNING: races certificates are NOT trustworthy yet.** Three review rounds found real false certificates.
> Most are now fixed and tested, but the fixes have had no second review. Oracle 2 is not merged, and parts of the
> test suite were not run after the last merge (see "Test status"). Until a fresh adversarial review of the fixed
> code passes, treat every `Certified` verdict as "no counterexample found".

## 1. What races is (10 lines)

hydrofrac checks that every correct member of a cluster ends with the same facts in a labelled collection. It is an
LDFI loop run inside the Hydro simulator.
1. Run the program and record, for each (member, fact) pair, the deliveries the fact was derived from.
2. A hitting-set ILP proposes the smallest fault set that removes every known derivation of some pair.
3. Run that fault set over the simulator's schedules; stop at a counterexample, or certify when no admissible set is
   left.
4. **The races idea:** a derivation may prune a fault set only if it is *robust*, meaning it exists in every
   schedule. A derivation that passed through a schedule choice that could change it (`nondet!` batch/snapshot,
   impure closure, tick constant, …) is *fragile*. Fragile derivations steer the search but never prune.
5. Crashes are named by which message they follow, so they need no send-order assumption. The only order facts
   used are ones proved from the compiled program (broadcast fan-out order, exact crash points, trusted `quorum`).

Details: `hydrofrac-races.md` (§2 the claim, §4 the mechanism, §11 results, §12 order in the formula) and
`hydrofrac-soundness.md` (the standalone argument; §16 assumptions A1–A16; §17 fixes; §18 known gaps).

## 2. Branch map (local repo /local/home/palvarox/hydro; also pushed to origin as new branches)

| branch | tip | what |
|---|---|---|
| `hydrofrac-races` | the commit adding this file | **The work.** Base: lean + races engine + review fixes (B) + harness discipline (D) + this file |
| `hydrofrac-races-wip-oracle2` | cb5b07d1e1 | WIP second oracle, on dd5c4d05a5; **merged** into `hydrofrac-races` as 752079ddfb |
| `hydrofrac-races-wip-soundness` | 1171c0137d | worker B's review fixes alone (merged into `hydrofrac-races`) |
| `hydrofrac-races-wip-harness` | 14b674c97a | worker N's item D alone, on dd5c4d05a5 (merged into `hydrofrac-races`) |
| `hydrofrac-lite-lean` | 58d0b3bcbf | **Retired** baseline, UNSOUND: S1 and S2 below |
| `s1-suspicions` | bc08c5204d | lite-lean + the S1 demonstration test |
| `origin/hydrofrac-sensitivity` | d9944a51dd | old research (per-label sensitivity walk); mined, not merged; untouched |
| `origin/hydrofrac-lite` | 7a9e200a91 | old lite; untouched |

The lite unsoundness:
- **S1**, DEMONSTRATED (`s1-suspicions`): lite's union clause for a total-loss fact gives a false certificate. Races
  fixes it with per-(member, fact) clauses.
- **S2**, DEMONSTRATED (`suspected_positional_crash_order_*`): lite's positional crash rule ("crash after the m-th
  send") is wrong across fault sets because send order changes. Never import it.

## 3. User decisions in force

- Priorities: soundness first, then cost (up to ~2× lite's is fine), then code size. No knobs.
- Lite is retired; races is the only engine. DPOR is out.
- Trusted order facts: `quorum` (hydro_std `ec_inference_demos/quorum.rs`) only in its exact compiled form, and
  `broadcast_closed` fan-out order.
- The two ABD-KV bugs (`quorum.rs` ~450-452 `retain`, and the one in `sim-exhaustive-misses-abd-kv.md`) are left
  **UNFIXED** on purpose, as blind targets.
- No force-push. Do not touch `origin/hydrofrac-lite` or `origin/hydrofrac-sensitivity`.

## 4. Open soundness findings (review rounds 1–3)

Status: **fixed+tested** (named test), **fixed untested**, **documented only**, **in progress**, **not started**.

| id | finding (plain English) | status |
|---|---|---|
| R2-F1 | Tick constants (a `source_iter`/singleton inside a tick) re-emit every tick; they were treated as local inputs, and once fed they certified | fixed+tested `tick_constants_are_fragile` |
| R2-F2 + R1 | A tick with a never-ready snapshot never runs, so its batch is never released; the simulator aborted the process | fixed+tested `review_batch_beside_an_unready_snapshot` (`batch_beside_snapshot` is fragile; under the `hydrofrac` feature a stuck tick quiesces instead of aborting) |
| R2-F3 | Dedup state read by other operators leaked absence-dependence: a break with zero faults | fixed+tested `a_dedup_state_read_elsewhere_is_fragile` |
| R2-F4 | `BeginAtomic` lacked Batch's kind check | fixed; no repro possible (the sim cannot yield that shape) |
| R2-F5 | Impure closures (counters, `Cell`, statics) make outputs schedule-dependent | fixed+tested `impure_closures_are_fragile_choices` (an allowlist since review 2, `provenance/purity.rs`; ARGUED, names are trusted, not resolved) |
| R2-F6 / R1-F3 | The simulator's own crash hooks and dynamic membership bypass the fault model | fixed+tested `the_simulators_own_crashes_are_refused` (`SimFlow::refuse_sim_faults`); A14 |
| R2-F7 | A6 wording | done (§16) |
| R1-F2 | No omission or crash may happen after the heal | fixed (`EventLog::check_heal`, checked on every run); no dedicated failing test |
| R1-F4/F5 | Wording | done: §0 (labels, bounds, what a certificate claims), §1.1 (notation, why one literal assignment is best for every row, the heuristic scope), §10 (bounds, what `Tolerance` says). The reviewers' original notes were not in the repo; this is a re-review against the code |
| R3-F1 | **FATAL.** Collections labelled on the same source line were merged into one label, masking a member's missing fact | fixed+tested `one_labelling_line_twice_is_two_labels`. Labels are keyed by IR node; the tap asserts one node per label |
| R3-F2 | **FATAL.** `BOLERO_RANDOM_TEST_TIME_MS` silently truncates "exhaustive" runs in both the search and brute force | fixed+tested `a_time_bounded_exhaustive_run_is_refused`. Both refuse to run when it is set. Other bolero limits are NOT CHECKED |
| R3-F3 | Fact identity falls back to `Debug` text or a 64-bit hash for non-`Serialize` types | fixed+tested `a_labelled_type_without_serialize_is_refused` (a false certificate before: `Rc<dyn Any>` facts all read `Any { .. }`). Labelled types must be `Serialize`; `Durable` is now `Serialize` (still not `Deserialize`) |
| R3-F4 | Covered scalars inherited the stream's certificate; count/sum can diverge while sets agree | fixed: scalars are never covered (NotChecked). Tested in hydrofrac/hydro_std; the hydro_test `effective` change passes `ec_demo_g_set_gossip_certifies` and `_all_labels` (closed and live: the G-Set is `NotChecked`, and its stream `all_elements` finds no counterexample under any crash set on sampled schedules) |
| R3-F5 | Empty domain gives Vacuous without a search; facts that appear only under faults are never checked | fixed+tested `a_fact_only_faults_create_is_checked`: `Vacuous` only in fragile-free or choice-free programs (§13 proves no fact exists); otherwise the label is searched for Claim B over every set within the bounds; brute force no longer drops such labels, and refutes a breakable `Vacuous` |
| R3-F6/F7/F8 | §15 shared components; the oracle's ≤2-fault limit; the missing nodes−1 bound; omission drops every content-identical copy; heal/timer wording; panics in pruned sets; ABD-KV is an exploration gap | documented (§15, §18) |
| oracle 2 | A second brute force sharing as little as possible: plain taps, no provenance rewrite, one output per collection | **merged** in 752079ddfb (`hydrofrac/src/check/oracle2.rs`); runs in every `EcCheck::cross_check` and reports its disagreements prefixed `oracle 2:`. `SimFlow::try_fork` copies `refuse_sim_faults`, and the `CrossCheck` changes are reconciled in `brute_force.rs` |

## 4a. Known open holes (round 3)

Found by review round 3 and **not fixed** in the prototype (`hydrofrac-races-int` @ `56effda043`). Each is pinned by
a test on a round-3 branch, and each can produce a wrong certificate or a missed failure.

| id | hole (plain English) | pinned by |
|---|---|---|
| R1 | A "this delivery cannot be lost" conclusion, drawn from a run where the send happened after the heal, is reused in a run where the same send happens before the heal, where it *can* be lost | `review3b_a_post_heal_copy_protects_a_pre_heal_loss` |
| R2 | The same hole, but the break needs 3 faults, so both oracles (capped at 2) miss it too | `review3b_a_three_fault_break_beyond_both_oracles` |
| R3 | The purity allowlist is fooled by a counter kept outside the closure | `hydrofrac-review3-a` |
| R4 | The purity allowlist is fooled by a user function passed by name instead of a closure literal | `hydrofrac-review3-a` |
| R5 | A label on a single-member cluster is `Vacuous` and never run under faults, so a panic in the recovery phase is missed | `hydrofrac-review3-a` |
| R6 | Harness inputs sent after a wait for an output are assumed to be present in every run, but a fault can stall the wait so they are never sent | `review3b_an_input_sent_after_a_wait_is_not_in_every_run` |

Round-3 test branches (local, not merged): `hydrofrac-review3-a`, `hydrofrac-review3-b`.

## 5. Item D: harness discipline (merged)

- **API:** `EcCheck::run` is unchanged. New: `run_then_recover(sim, inputs, recover)` and
  `cross_check_then_recover`. The search and brute force share one `drive()`:
  1. inputs, with barriers forbidden;
  2. quiesce;
  3. heal;
  4. recovery (no faults);
  5. quiesce.
- **Barrier ban:** `quiesce()` in the fault phase panics; so does sending input after a forced quiescence. Waiting
  for outputs is allowed. The check reports the panic as a harness error. Core change: `forbid_barriers` in
  `hydro_lang/src/sim/compiled.rs` (`hydrofrac` feature).
- **Verdict changes, all DEMONSTRATED in `ec_two_pc_hydrofrac.rs` / `ec_bruteforce.rs`, agreeing with brute force:**
  - CTP-2, timer racing the transaction, one-shot: was Exhausted, now a **counterexample found unaided**. A single
    crash of the coordinator right after its commit to P0 makes P1 never commit
    (`ctp2_one_shot_timer_breaks_at_one_coordinator_crash`).
  - CTP-2, timer ticks only in recovery, exhaustive, ≤2 faults: **Certified**, 6 sets / 2,460 schedules, the same
    as brute force. This is a narrower claim: no crash strikes the peer exchange.
  - CTP-2, racing timer + 1 recovery tick: Exhausted (sampled), not a certificate.
  - CTP-3, racing one-shot timer: it now **breaks at a single crash** (the barrier harness needed two). The
    coordinator dies after its commit to one participant, and another participant, whose single query reached its
    peers before they decided, never commits. Brute force: 42 of 200 sets break it.
  - CTP-3, racing + 1 recovery tick: no set breaks it, sampled only. Exhaustive exploration is infeasible
    (>223k schedules), so there is no certificate.
  - Gossip and multi-Paxos demos: barriers removed or moved to recovery; their expectations are unchanged.

## 6. Queued (in order)

1. ~~Merge oracle 2~~ (done, 752079ddfb); run it over every cross-checked program.
2. Close R3-F3 (require `Serialize` for labelled types) and R1-F4/F5 wording.
3. **Second review round:** fresh adversarial reviewers on the fixed code.
4. **C:** done on branch `hydrofrac-races-itemc` (ITEM-C2).
   - **Order between successive broadcasts:** restricted to *sequenced* closed broadcasts, whose data is directly a
     `TotalOrder`, `ExactlyOnce` harness input of a process. It is ARGUED in `hydrofrac-soundness.md` §6.4, and
     every executed run is checked against it. It is DEMONSTRATED against brute force under every schedule of every
     fault set of at most two faults, next to a relay of S2's shape (`sequenced_broadcasts_keep_the_harness_order_under_every_fault_set`).
     A mutation that marks the relay sequenced fails that test. Network-fed broadcasts keep their payload literals
     free.
   - **The tie-break:** crash points are ordered by proved strength, with the earliest sent first among equals. Only
     the order changes. Costs are in `races-vs-lite-comparison.md`, "Update: item C".
   - The choice-free one-schedule test was already in place (`664c60e2f1`).
5. Redo the lite-vs-races cost table on the final code. Rows above 2× today: ABD phase 2 8.4×, `new_messages` 5–6×,
   gossip ~4×, CTP-3 3.2×, Paxos prepares 8×.
6. Future: a completion/liveness spec; rebase onto upstream.

## 7. How to verify

```
git checkout hydrofrac-races
cargo check -p hydrofrac -p hydro_test -p hydro_std --all-targets --features hydrofrac/cross-check
cargo test -p hydrofrac --features cross-check            # fast; the core suite
cargo test -p hydro_std --lib hydrofrac
cargo test -p hydro_test --lib cluster::ec_               # ~8 min
```

- If you see "colliding StableCrateId": `rm -rf target/hydro_trybuild/hydrofrac`.
- Never set `BOLERO_RANDOM_TEST_TIME_MS` for these tests (the checker now refuses).

## 8. Process lessons

- A worker must not close until its parent has fetched its work: a closed thread's sandbox is deleted, and work was
  lost this way. Hand work over as git bundles and fetch them into the main repo at every milestone.
- Report in plain English: "a single crash of X makes member Y miss fact Z", never "breaks at 1".
- Separate demonstrated, argued and guessed. Verify every number before reporting it.
- Reviewers found what tests did not: both fatal bugs were shared by the checker and its brute-force oracle. Keep an
  oracle that shares as little code as possible.

## 9. Test status at handoff

On `hydrofrac-races`, i.e. the merge of B's review fixes and N's item D, plus this file:
- `cargo check -p hydrofrac -p hydro_test -p hydro_std --all-targets --features hydrofrac/cross-check`: **builds**.
  One existing warning: `unused mut` in hydro_lang `feedback_campaign.rs`.
- `cargo test -p hydrofrac --features cross-check`: **152 passed, 0 failed** (1 ignored doc test).
- `cargo test -p hydro_std --lib hydrofrac`: **12 passed, 0 failed, 3 ignored**.
- `cargo test -p hydro_test --lib cluster::ec_`: **NOT RUN on the merged tip**; the run timed out while compiling.
  On their own branches, N's D passed it (43 passed, 10 ignored). B's fixes were never run against it, and they
  touch hydro_test's G-Set `effective` expectation. Run it first.
- Clippy: NOT RUN.
- Merge note: the code merged without conflicts. Only three hunks of `hydrofrac-soundness.md` conflicted, and they
  were resolved by hand (§12 claim, A6–A8, §18 heal/liveness bullets).

## 10. Teaching examples and diagrams

`hydro_test/src/cluster/ec_teaching.rs` holds five minimal programs, written the way a user would write them: a
Hydro sim test, plus `EcCheck::new()`, `check.instrument(..)`, `check.run(..)`, `println!("{results}")` and one
user-level assertion on a collection by its binding name (`results.label("received").verdict.is_counterexample()`).
Each one shows a way a broadcast loses agreement, or keeps it. To run all five, one at a time, with the checker's
report printed:

```sh
cargo test -p hydro_test --lib -- cluster::ec_teaching:: --test-threads=1 --nocapture
```

To run just one, name it:

```sh
cargo test -p hydro_test --lib -- cluster::ec_teaching::one_lost_message_leaves_a_receiver_without_the_item --nocapture
```

`hydro_test/src/cluster/ec_teaching_checks.rs` runs the same five programs as hydrofrac regression tests: it
asserts the exact faults (which node crashes and where, which message is lost), the crash tolerance and the
certificate, and cross-checks every verdict against brute-force enumeration (`cross_check`, up to 2 faults). To
run both files:

```sh
cargo test -p hydro_test --lib -- cluster::ec_teaching --test-threads=1 --nocapture
```

Every counterexample writes its diagrams to `target/hydrofrac/<test>/<label>/`, or under `$CARGO_TARGET_DIR` when
that is set. The label directory is the binding name and the file (`received_ec_teaching.rs`, or
`received_ec_teaching_checks.rs` for the regression tests), so the two files do not overwrite each other. A second
run of the same label in one test gets a `-2` suffix.
- `counterexample.svg` (and `.dot`): the focused diagram. It shows the broken fact's lineage, the faults and the
  heal, and only the reported label's decoded outputs. Its caption counts what it left out
  (`2 unrelated messages and 8 unrelated outputs not shown`). This is the one the report's `diagram:` line names.
- `counterexample-full.svg` (and `.dot`): the whole run, with every output, including other labels' and the raw
  bytes of `sim_cluster_output` ports.

The `.svg` is written only when Graphviz's `neato` is on the `PATH`. Otherwise run `neato -n2 -Tsvg <file>.dot`.

| test (`ec_teaching.rs`) | label dir | what the focused diagram shows |
|---|---|---|
| `a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item` | `received_ec_teaching.rs` | 3 lanes, 2 arrows, the sender's crash |
| `one_lost_message_leaves_a_receiver_without_the_item` | `received_ec_teaching.rs` | 3 lanes, 2 arrows, the omitted one red |
| `a_cluster_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item` | `received_ec_teaching.rs` | 3 lanes, 2 arrows, the crash |
| `receivers_that_echo_survive_a_sender_crash` | `from_sender_ec_teaching.rs` | 3 lanes, 8 arrows (bcast and echoes). The raw single-hop label breaks, but `delivered` holds |
| `two_redundant_senders_survive_one_crash_but_not_two` | `received_ec_teaching.rs` (and `-2`), `items_ec_teaching.rs` | `items`: 4 lanes, 4 arrows, both crashes |

API notes from writing the teaching file:
- `Results` has no `counterexample()` and `LabelResult` has no `holds()`, so a user asserts through
  `results.label(name).verdict` (`is_counterexample()`, `!fails()`).
- `.weaken_consistency()` does not retract a claim: the `broadcast_closed` it is applied to is still a minted
  `EventualConsistency` collection, and hydrofrac still checks it (it exits at the cast). So a program built on a
  closed broadcast (the echo, the redundant senders) always reports the raw hop's counterexample, and
  `results.assert_holds()` cannot certify it; the examples assert the collection that matters by name.
