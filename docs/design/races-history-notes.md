# Which `nondet!` choices can affect a label: what `sens` found

These are research notes on `sens` = `d9944a51dd` (hydrofrac-sensitivity) and `lean` = `58d0b3bcbf`, with base
`90d9d26736`. "Doc" means `docs/design/hydrofrac-lite.md` on the branch named. **D** means demonstrated by a
named test or measurement. **A** means argued only.

## 1. The per-label sensitivity walk (`hydrofrac/src/provenance/sensitivity.rs` on sens, 1323 lines)

**History.**
- `c210ed4218` added it as a diagnostic, ported from the research branch's `order.rs` `Cones::nondet_upstream`.
- `b8460cf75d` made it binding: no certificate for a sensitive label.
- `1a8cdadf75` added the Calm analysis.
- `d9944a51dd` added the `TCP.lossy` fix.

**The two failure modes** (module docs, lines 1–48; sens doc §4.9):
- **U1, existence.** A derivation through a site holds only under the schedule that produced it.
- **U2, send order.** `crash(n after m)` cuts `n`'s sends at a position. A site upstream of what `n` sends
  can reorder those sends, so the kill relation from one schedule is wrong in another.

**`Analysis::Conservative` (`Cones::walk`, line 666).**
- It walks upstream from each label through every operator:
  - across a `Network` back to the sender;
  - through cycles back to the `forward_ref` sink;
  - through captured `by_ref`/`by_mut` state back to its producer and to every closure that writes it.
- Every `HydroNode` kind is classified with no wildcard (`classify`, line 242; `nondet_op`, line 395).
- **Choice sites:** `batch`, atomic entry, untrusted `observe_nondet` (unless it is a bare retry cast),
  the implied `observe_for_mut`, `merge_ordered`, a fold/scan over an unordered input, and timers.
- **Opaque sites:**
  - `Placeholder`;
  - unresolved state, a `for_each` writing through it;
  - a cycle without its sink;
  - `sim_input::<_, NoOrder,_>`, `source_stream`, embedded sources;
  - futures and async streams, multi-version networks;
  - provenance islands.
- **U2:** for each location that sends a delivery in the cone, it also walked that location's whole cone
  (`sends_by_location`, line 555). Those sites are flagged `send_order`.

**Unsound or too coarse:**
- **Soundness gap (A, fixed).** `TCP.lossy` keeps `TotalOrder` and can drop a middle message. That moves
  positions after it, so a fault creates a fact. The channel's `nondet!` is not an IR node, so both analyses
  called the cone site-free. Fixed in `d9944a51dd`: `ordered_lossy_network`, test
  `an_ordered_lossy_channel_is_opaque`. It can't be reached today because `SimBuilder` `todo!`s `Tcp<Lossy>`.
- **Premises, never proven (A):**
  - closure purity is a text heuristic (no `let mut`, `Cell`, `Mutex`, `rand`, …);
  - a site affects only what is downstream of it;
  - the number of ticks a location runs is observable only through sites.
- **Too coarse:** U2 binds almost everywhere. Under Calm, 8 labels were U1-discharged and still sensitive
  because the discharged site feeds a send (sens doc, end of §4.9).

**Why lean replaced it** (`b220aa97b2` body; lean doc §4.9, lines 370–430).
- `program_choices` (`provenance/choices.rs`, about 200 lines) is an allowlist scan of the whole IR. Any
  choice anywhere means no label is certified.
- It visits a superset of every label's cone and every sender's cone, so it needs no U1/U2 argument.
- The cost: 3 labels that were site-free per label (the `initial` hops of uniform broadcast and of
  `hydro_test`'s two broadcasts) lose "tolerates 0 crashes certified". No verdict changes. 4 of 15
  programs are choice-free.

## 2. The "CALM refinement" (`Analysis::Calm`, `walk_calm` line 1033; commit `1a8cdadf75`; sens doc lines 560–692)

**How it walks.** Each node is walked in one of two modes:
- **tolerant:** only the set of records over the run matters;
- **strict:** order, batching or state is observed.

A label starts tolerant if it is a stream outside any tick.

**Set-preserving operators** (they keep the walk tolerant):
- pure `map`/`filter`/`flat_map`/`filter_map`/`inspect`/`partition`;
- casts and order-only `observe_nondet`;
- `batch`, atomic sections, `all_ticks`, `chain`, `merge_ordered`, `unique`, `sort`, `defer_tick`;
- networks and consistency assertions;
- top-level `join`/`cross_product` with both inputs persisted;
- `filter_not_in` against its own history: the `use::state` dedup idiom, recognised structurally (a
  `defer_tick` cycle whose sink is the union of itself and the difference).

**Strict:**
- `cross_singleton`/`filter_if`;
- joins inside a tick, anti-joins, other differences;
- `first`, `enumerate`;
- all folds, reduces, scans and counts, **including lattice folds** (not attempted);
- watermarks, futures;
- state read by reference, impure closures, stream→singleton crossings.

**What gets discharged.** A site reached only tolerant is moved to `LabelCone::harmless` if it is a pure
order/batch choice: `batch`, `begin_atomic`, `merge_ordered`, order-only `observe_nondet`, or an unordered
external input. These always count on any path:
- `fold_unordered`;
- `by_mut` closures;
- timers and spins ("decide how many records exist");
- watermarks;
- opaque sites.

**U2 is still conservative,** but only the cone of the inputs of a sender's networks is walked.

**The argument (A, 4 steps).**
1. Strict regions contain no site.
2. Tolerant operators are monotone set functions of `cum(in)`. This relies on:
   - quiescence flushing every batch and deferred tick;
   - `MessageId` being content-addressed.
3. So each derivation is schedule-independent.
4. Positional crashes are the exception, which is why U2 stays.

**Validation (D):**
- `assert_refines` in `hydro_test/tests/ec_schedule_sensitivity.rs` checks that Calm reports a subset of
  the conservative walk's sites on all 14 programs.
- `ask_once_is_sensitive_in_its_own_cone` (sens `ec_bruteforce.rs:250`): the `batch` is kept, via
  `filter_if`.
- `two_pc_ctp_committed_is_sensitive_in_its_own_cone`: via the in-tick `JoinHalf`, e.g.
  `batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156`.
- `brute_force_agrees_on_a_monotone_batched_echo` (`hydrofrac/src/check/brute_tests.rs:296`): `fail_stop`
  and lossy first hop, every admissible set of ≤2 faults, every schedule. The search's new certificates
  were confirmed.
- Every corpus brute-force cross-check still agreed.

**Counterexamples:** none found to Calm itself. The only gap found was the `TCP.lossy` one, which is a
classification hole shared with Conservative.

**Corpus counts** (59 labels, 14 programs):

| analysis | sensitive | sensitive in own cone (U1) |
|---|---|---|
| Conservative | 49 labels, 11 programs | 42 labels, 11 programs |
| Calm | 47 labels, 11 programs | 34 labels, 8 programs |

- The only verdict change: uniform `echo`/`distinct_acks` now read "tolerates 0, breaks at 1", certified.
  That came from the U2 refinement alone.
- The per-label table is in `docs/design/hydrofrac-lite-data/sensitivity-calm.md` (sens).

## 3. Casualty sweeps (`7a9e200a91`, `hydro_test/src/cluster/ec_casualties.rs`, all `#[ignore]`)

**The question:** were the research branch's certificates on sensitive labels actually *wrong*? The research
branch is `hydrofrac-input-coverage@0d801bbafc`.

**Method:** `EcCheck::cross_check` with no pruning, every admissible set, sampled or exhaustive schedules.
Env knobs: `CASUALTY_SAMPLES`, `CASUALTY_FAULTS`, `CASUALTY_ITEMS`, `CASUALTY_ROUNDS`. Each sweep took
≤ about 20 min.

**Result (D): none break.**

| label | fault sets | schedules per set |
|---|---|---|
| echo broadcast `deliveries` | 65 (≤2 crashes) | 4,096 |
| uniform broadcast, thresholds 1 and 2 (`casualty_uniform_broadcast`) | 176 each | 1,024 |
| G-Set gossip, closed and live, 3 rounds (`casualty_gossip`) | 40 each | 1,024 |
| multi-Paxos `learned`/`facts`, cap 1 (`casualty_multi_paxos`) | 52 | 512 |
| ABD reliable phase 2 (`hydro_std` `ec_abd_hydrofrac.rs:179` `reliable_phase_two_brute_force_sweep`; default 16 samples, 1,024 via env) | 176 | 1,024 |
| CTP-2, 1 txn | 6 | every schedule |
| CTP-2, 4 txns | 18 | 4,096 |

**Not established:**
- A sample can miss a rare schedule, and the learned universe can miss crash points.
- The harnesses **quiesce between inputs and timers**, which removes most `ask_once`-style races. "Not
  wrong" holds only relative to those harnesses.

## 4. Demonstrated facts per program

- **`ask_once`** (`ec_bruteforce.rs` `ask_once`; announcement and timer race, 3 members, `TCP.fail_stop`,
  three `batch`es, `filter_if` on a count and on queries):
  - **One crash breaks it:** `crash(member 0 after announce→member 1)`. Member 2's query reaches member 1
    before the announcement does, so member 1 has nothing to answer.
  - `brute_force_finds_the_ask_once_race`: 8 sets, 132 schedules, about 0.2 s, exhaustive.
  - The strong rule certified it after 2 hypotheses (`907db0e7af`, then-ignored
    `brute_force_agrees_on_ask_once`). The heuristic finds it at hypothesis 1.
- **CTP-3 `committed`:**
  - **Breaking set:** `{crash(Coordinator after commit→P0), crash(P0 after Decided→P1)}`, which splits the
    participants in about 40% of schedules.
  - Lean `ec_two_pc_hydrofrac.rs:300` `two_crashes_split_three_participant_cooperative_termination`: about
    1,600 of 4,096 sampled schedules split.
  - Under the strong rule it was falsely certified after 12 hypotheses. The heuristic finds it at
    hypothesis 13.
  - `brute_force_finds_the_two_pc_cooperative_termination_violation`: 200 sets, 12,800 schedules, about 36 s,
    64 samples per set. The miss probability is about 10⁻¹⁴, but the sample is not reproducible.
  - Lean `two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants`: about 20 s at
    256 samples.
  - **Schedule counts:**
    - timers to P1 and P2: failure-free 1,872; one coordinator crash >66k at about 350/s;
    - P2's timer only: failure-free 936; one crash >104k at about 400/s.
- **CTP-2, quiescing harness** (timers fire after the txn settles): no counterexample under any crash bound
  (`Exhausted`). Lean test `two_pc_with_cooperative_termination_survives_the_coordinators_crash`.
- **CTP-2, racing timers** (timers sent together with the txn, no `quiesce`):
  - **One crash breaks it:** `crash(Coordinator after commit→P0)`.
  - Direct injection splits the participants on 208 of seeds 1..=256 (about 80%), seed 1 included
    (`ctp2_racing_split_survey`).
  - Regression tests, on sens in `ec_casualties.rs` and on lean in `ec_two_pc_hydrofrac.rs:480/492`:
    - `ctp2_racing_timers_split_by_direct_injection` (seeds 1..=8, about 3 s);
    - `ctp2_racing_timers_search_finds_the_crash` (64 samples, 1 fault, hypothesis 2, about 5 s).
  - Exhaustively, one fault set passed 100k schedules unfinished.
  - **A:** lean doc §4.9 says CTP-2 racing was "certified wrongly" under the strong rule. No test shows
    this: it was found after `b8460cf75d`.
- **Uniform broadcast / G-Set / multi-Paxos / ABD reliable phase 2:** no breaking set in the §3 sweeps.
  - Uniform `echo`/`distinct_acks` do break at 1: `crash(Member@0 after echo→Member@1)`
    (`ec_demo_uniform_broadcast`).
  - Gossip's `echo` hop breaks at 1 under heal-at-end (lean §7).
  - ABD's failure-free probe is >21k schedules at 100–130/s.
  - Plain ABD breaks when the client crashes mid phase 2.
- **Echo broadcast (hydro_test):** one exhaustive run is >160k schedules (240k failure-free). It was killed
  after 31 min (`ab5709cf50`).

## 5. Machinery for recording or directing choices

- **Directing (D, exists on lean):**
  - `CompiledSim::fuzz_repro(bytes, …)` (`hydro_lang/src/sim/compiled.rs:753`) replays a bolero byte
    driver.
  - `schedule_bytes(seed)` is a 64 KiB xorshift stream that names one schedule reproducibly. It is used by
    `ctp2_racing_split`.
  - `first_schedule` (`compiled.rs:930`) uses all-zero bytes, i.e. the first alternative at every choice.
  - `exhaustive_then` / `fuzz_bounded_then` (`compiled.rs:816/889`).
  - Each hook draws its choice with `produce().generate(driver)` (`sim/runtime.rs:286`, `SimHook::
    autonomous_decision`). Bytes are consumed positionally. **Nothing maps bytes to sites.**
- **Recording:** no record-level "passed through choice X" tag exists on either branch.
  - Record provenance (E5 `u64` tags, `TagSet`) tracks deliveries only.
  - `NondetSite::via` is static.
  - The only per-choice trace is the simulator log: `release_decision(log_writer)` prints
    `--> file:line ^ releasing items: […]` when `instance.log` is set.
- **Research-only, not on sens or lean:** `BatchHook::{replay, deliver_all, omit}`, the scripted,
  `tick_input` and observation hook policies, `NonDet::hooked`, `flow.sim_hook()`. Lean doc §6.3 lists them
  as avoided.

## 6. Brute-force cross-checks (`cross-check` feature)

**`hydro_test/src/cluster/ec_bruteforce.rs`** (`MAX_FAULTS = 2`):

| test | sens | lean | notes |
|---|---|---|---|
| `brute_force_agrees_on_a_plain_broadcast` | ✓ | ✓ | |
| `brute_force_agrees_on_broadcast_live` | ✓ | — | |
| `brute_force_agrees_on_reliable_broadcast_closed` | ✓ | ✓ | |
| `ask_once_is_sensitive_in_its_own_cone` | ✓ | — | static |
| `brute_force_finds_the_ask_once_race` | ✓ | ✓ | 0.2 s |
| `brute_force_agrees_on_ask_once` | ✓ | ✓ | lean asserts `results.choices` is non-empty |
| `brute_force_finds_the_two_pc_cooperative_termination_violation` | ✓ | ✓ | 36 s |
| `brute_force_agrees_on_two_pc_cooperative_termination` | ✓ | ✓ | |
| `brute_force_agrees_on_plain_two_pc` | ✓ | — | 2 participants, exhaustive; breaks at 1 |
| `brute_force_agrees_on_leader_merge` | ✓ | — | exhaustive |

On sens at `907db0e7af`, the default run was 7 passed, 2 ignored, 37.7 s.

**`hydrofrac/src/check/brute_tests.rs`** (no per-test timings recorded):
- **Both branches:**
  - plain and lossy broadcast;
  - echo broadcast (3 and 2 members);
  - two-member exchange;
  - echo over a lossy first hop;
  - lossy relay chain;
  - ownership exchange;
  - provenance precision;
  - `the_cross_check_catches_wrong_verdicts` (negative control).
- **Sens only:**
  - `brute_force_agrees_with_a_certificate_by_enumeration`;
  - `…_on_a_batched_echo_over_a_lossy_first_hop`;
  - `…_on_a_monotone_batched_echo`.

**Other, sens only** (not ported to lean, per `8459116d0a`):
- every `ec_casualties.rs` sweep (minutes each, ≤ about 20 min);
- the ABD sweep;
- the Calm/Conservative comparison.

**Bounded enumeration (d2, `e709320970`, sens only):** `CertifiedByEnumeration` with `ENUMERATION_LIMIT`
2000. It needs exhaustive schedules, so no corpus verdict changed. Lean doesn't have it.
