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
| `hydrofrac-races-wip-oracle2` | cb5b07d1e1 | WIP second oracle, on dd5c4d05a5, **not merged** (see §4) |
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
| R2-F5 | Impure closures (counters, `Cell`, statics) make outputs schedule-dependent | fixed+tested `impure_closures_are_fragile_choices` (a textual guard, `names_impure`; ARGUED, not complete) |
| R2-F6 / R1-F3 | The simulator's own crash hooks and dynamic membership bypass the fault model | fixed+tested `the_simulators_own_crashes_are_refused` (`SimFlow::refuse_sim_faults`); A14 |
| R2-F7 | A6 wording | done (§16) |
| R1-F2 | No omission or crash may happen after the heal | fixed (`EventLog::check_heal`, checked on every run); no dedicated failing test |
| R1-F4/F5 | Wording | partly done; §0, §1.1 and §10 still open |
| R3-F1 | **FATAL.** Collections labelled on the same source line were merged into one label, masking a member's missing fact | fixed+tested `one_labelling_line_twice_is_two_labels`. Labels are keyed by IR node; the tap asserts one node per label |
| R3-F2 | **FATAL.** `BOLERO_RANDOM_TEST_TIME_MS` silently truncates "exhaustive" runs in both the search and brute force | fixed+tested `a_time_bounded_exhaustive_run_is_refused`. Both refuse to run when it is set. Other bolero limits are NOT CHECKED |
| R3-F3 | Fact identity falls back to `Debug` text or a 64-bit hash for non-`Serialize` types | **documented only** (A4, §11, §18). Open: require `Serialize` |
| R3-F4 | Covered scalars inherited the stream's certificate; count/sum can diverge while sets agree | fixed: scalars are never covered (NotChecked). Tested in hydrofrac/hydro_std; the hydro_test `effective` change is untested |
| R3-F5 | Empty domain gives Vacuous without a search; facts that appear only under faults are never checked | fixed+tested `a_fact_only_faults_create_is_checked`: `Vacuous` only in fragile-free or choice-free programs (§13 proves no fact exists); otherwise the label is searched for Claim B over every set within the bounds; brute force no longer drops such labels, and refutes a breakable `Vacuous` |
| R3-F6/F7/F8 | §15 shared components; the oracle's ≤2-fault limit; the missing nodes−1 bound; omission drops every content-identical copy; heal/timer wording; panics in pruned sets; ABD-KV is an exploration gap | documented (§15, §18) |
| oracle 2 | A second brute force sharing as little as possible: plain taps, no provenance rewrite, one output per collection | **in progress**, branch `hydrofrac-races-wip-oracle2`. 92/143 tests passed before the move, 0 failed, no disagreement on 17 brute tests; its planted-bug test has not run. To merge: `SimFlow::try_fork` must copy `refuse_sim_faults` (marked `// merge:`), and its `CrossCheck` changes must be reconciled in `brute_force.rs` |

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

1. Merge oracle 2; run it over every cross-checked program.
2. Close R3-F3 (require `Serialize` for labelled types) and R1-F4/F5 wording.
3. **Second review round:** fresh adversarial reviewers on the fixed code.
4. **C:**
   - the tie-break fix;
   - an IR-level proof of order between successive `TotalOrder` broadcasts, which must hold under every fault set
     (S2) or be restricted;
   - a test that exercises `choices.rs`'s one-schedule path and fails if that optimisation is removed.
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
