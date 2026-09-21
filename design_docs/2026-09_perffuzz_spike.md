# Spike: amplification as a fuzzing problem (PerfFuzz applied to schedules)

Status: done, one day. Result: coverage-guided fuzzing of raw simulator schedules is a working
**generic search** for amplifying schedules; no **generic score** of a single run was found that
ranks what it finds, at any granularity cheaply observable (dataflow edges, basic blocks, or
their difference against the prompt run). The one untested path is a per-decision counterfactual
fork. Harness: `rpc_retry::amplification_tests::perffuzz_hail_mary_*` and
`raft::amplification_tests::perffuzz_hail_mary_*` (both `#[ignore]`, run by hand).

This answers the alternative raised in `2026-09_bootstrap_the_sweep.md` ("coverage-guided fuzzing
with amplification as the coverage metric ... not started").

## The question

The sweep (`2026-09_amplification_as_adversarial_scheduling.md`) finds every known storm, but its
verdict, derivations per goal, needs one declared line of program knowledge per program: which
payload field names a goal (a request id; a committed entry). The objection: if the programmer
must say what counts as re-done work, they already know where the amplification is. The spike
asked whether the problem can be posed as classic fuzzing, PerfFuzz-style (Lemieux, Padhye, Sen,
Song, ISSTA 2018: independently maximize hit counts at every program location), with

1. the simulator's raw decision sequence as the fuzz input (it already is: a bolero `Driver`
   reads it), and
2. a measure of *work* that needs no program knowledge, computable from one execution.

Two things were kept strictly apart: the fuzz **target** (what libFuzzer sees) and the
**scorer** (what we compute on replay). The target never observes anything: it sends fixed inputs
and waits for quiescence. So whatever the corpus contains, the fuzzer found it without being told
what amplification is. The scorer is where every candidate measure was tried.

## Setup

- `cargo sim` as it exists: sanitizer coverage with inline 8-bit counters on both the test binary
  and the simulated program's dylib, libFuzzer via bolero. libFuzzer's default notion of a new
  *feature* is (basic block, hit-count bucket) with buckets 1, 2, 3, 4–7, 8–15, 16–31, 32–127,
  128+. This is retention by *novelty*, not maximization; it is PerfFuzz-adjacent, not PerfFuzz.
- A schedule is the byte string; when it runs out, bolero's driver answers every question with
  the minimum, which for the hooks means "release nothing unless forced". The empty input is
  therefore the "withhold every delivery the forcing rule lets you withhold" schedule, with the
  clock and requests still moving (they satisfy the forcing rule). No metering, no policy.
- Programs unchanged. Small fixed workloads so a raw schedule fits in the fuzz input:
  - `rpc_retry`: 48 ticks, 1 request per tick, timeout 4, `max_attempts` 3 (control: 1).
  - Raft: 3 members, one election, then 48 heartbeat interrupts per member and an election
    interrupt every 4 ticks (control: no election interrupts).
- Replay scorer: `fuzz_repro(bytes)` per corpus entry; program-specific score read from the
  outputs (`rpc_retry`: sends per request from `outgoing`; Raft: highest term from
  `leader_views`); generic candidates alongside. Controls for every corpus: the empty input, and
  random byte strings of several lengths.

## Result 1: the search works

One campaign per cell, 5–17 minutes, 20–330 exec/s (instrumented) vs 375–630 uninstrumented.

| program | corpus | min | max | verdict |
|---|---|---|---|---|
| `rpc_retry`, 3 attempts | 795 | 48 sends | 144 sends | gain 3, saturates at `max_attempts` |
| `rpc_retry`, 1 attempt | 722 | 48 | 48 | no moves |
| Raft, election timer every 4 | 578 | term 1 | term 21 | 20 elections after the first |
| Raft, heartbeats only | 495 | term 1 | term 1 | no moves |

Every row: fuzz the raw schedule under stock coverage, replay, take max and min of the
program-specific score over the corpus. No edge named, no policy, no scheduler option, no lineage.
The two controls are E2's hand-computed results.

Caveats. On `rpc_retry` the maximum is reached by the empty input (withhold every response) and by
half of the random 4-byte inputs; the corpus spans 48..144 but so does random sampling, so the
coverage guidance is not shown to matter there. On Raft the empty input reaches term 13 (one
election per interrupt, members alone) and 24 random inputs never exceeded 13 (longer ones did
worse, max 8); the corpus has 48 entries above 13, up to 21. That is the one place coverage
retention exceeded both baselines, on one run each; not repeated.

## Result 2: no generic score of a single run

Every generic measure below is computed with no program knowledge and compared against the
program-specific score by Spearman rank correlation ρ over the corpus (1 = same ranking, could
replace it). Controls have a constant score, so for them the raw size of the measure is reported.

### Dataflow-edge totals (`edge_counts`, passive, exists already)

| | measure | ρ | quietest → stormiest |
|---|---|---|---|
| `rpc_retry` | total records over all hooks | **0.994** | 336 → 816 |
| Raft | total records over all hooks | 0.51 | 187 → 279 |
| Raft | total decisions (ticks run) | 0.49 | 228 → 632 |
| Raft | nonempty decisions on the *election timer* hook | 0.70 | (needs to know which hook) |

Additive amplification (retries add records) is seen by the plain total. Displacement (Raft's
storm replaces 2 heartbeats per tick with fewer election messages) is not: E2b's finding, now
also under the fuzzer's schedules.

### Per-basic-block execution counts of the program dylib

Mechanism (recorded so it is not rediscovered): the program dylib is compiled with sancov in
`trace-pc-guard` mode plus the PC table by setting `RUSTFLAGS` in the test's *runtime*
environment (the simulator's runtime compile honors it, `trybuild/generate.rs`); the host test
binary is built uninstrumented under `CARGO_ENCODED_RUSTFLAGS` (highest cargo precedence, dropped
by the test before the dylib compiles) with `--cfg perffuzz_blocks -Clink-arg=-export_dynamic`,
and defines `__sanitizer_cov_trace_pc_guard{,_init}` and `__sanitizer_cov_pcs_init` (gated behind
the cfg because libFuzzer defines the same symbols under `cargo sim`). The dylib link needs
`-undefined dynamic_lookup`. Guard-init and PC-table constructors run in no fixed order; match
ranges by entry count. `dladdr` resolves the dylib's local symbols on macOS. Counts are `u64`
(no 8-bit wraparound). 1.04M blocks instrumented, ~8.6k (Raft) / ~14k (`rpc_retry`) executed.

Raft storm corpus, 603 schedules, vs highest term:

| aggregate over anonymous blocks | ρ | @term 1 | @term 21 |
|---|---|---|---|
| total executions (SlowFuzz) | 0.46 | 1.06M | 3.65M |
| max over blocks of count/median | 0.00 | 1.7 | 14.3 |
| sum over blocks of ln(count/median) | 0.49 | | |
| blocks at their corpus-wide max (PerfFuzz) | 0.30 | 2058 | 2122 |
| same four, blocks in the `hydro_test` crate only (608) | ≤ 0.61 | | |

`rpc_retry`: total ρ 0.88 (worse than the edge total's 0.99: framework noise).

**The signal is there and cannot be picked out.** 209 blocks track the term with ρ > 0.9 (best
0.993): `HashSet::clear`, `RawTable::clear`, `VecDeque::push_back`, `Serialize for
RaftRpc::RequestVote`, i.e. the election path, 3 executions at term 1, 57 at term 21. Most are
std/hashbrown generics called from Raft, so a crate-name filter throws them out. The widest-spread
blocks in the program are `drop_in_place<AppendEntriesRequest>` and `Clone for
AppendEntriesRequest`: 0 → 490 executions, ρ = −0.19. Heartbeats being cloned and dropped in
buffers scale with how much a schedule withholds and *anti*-correlate with the storm, since
elections displace heartbeats. Noise 10× the signal, pointing the other way.

Controls: Raft heartbeats-only, total executions range 332k → 2.5M (7.5×) across schedules with
zero amplification; one block ranges 24 → 1754 (73×). `rpc_retry` 1 attempt: max ratio-to-median
12 with no amplification. So "some block ran 14× more than usual" is smaller than what the
controls produce from nothing.

### Counterfactual against the prompt run (positive part)

For each schedule, per-block counts minus the prompt schedule's counts, positive part only,
summed: work that exists in this schedule and not under prompt delivery. Asymmetric by design so
that displaced (lost) work cannot mask added work. It does suppress the heartbeat displacement,
and fails for a second reason:

| corpus | median excess over prompt | ρ vs score |
|---|---|---|
| Raft storm | 3.5M | 0.47 (= plain total) |
| Raft control | 1.8M | — |
| `rpc_retry` | 10.9M | 0.88 |
| `rpc_retry` control | 4.3M | — |

The excess in all four corpora sits in the same blocks: `dfir_pipes::mut_unit`,
`Context::from_task`, `drop_in_place<PullStep<..>>`, the dataflow runtime's per-tick operator
setup and teardown. Any schedule that is not prompt runs more ticks (piecemeal releases), every
tick has a fixed framework cost, and that cost is 10–100× the election work. Restricting to the
program crate leaves the same picture (Raft 104k vs control 52k; top blocks are per-tick drops of
pull steps over Raft types).

Normalizing by ticks reintroduces displacement (per tick, the storm does *less* than steady
state). The two confounds are complementary:

- totals, per-location maxima: defeated by displacement;
- excess over prompt: defeated by tick count;
- excess per tick: displacement again.

## Conclusions

1. Raw-schedule fuzzing with stock coverage is a generic, cheap search for amplifying schedules
   and discriminates all four cases. It is a candidate search accelerator for the sweep, not a
   replacement: it gives max and min but not the threshold shape, and its high entries are
   largely the driver's default (withhold everything), which is the "slow server" perturbation,
   reached with no search.
2. No generic single-run work measure ranks displacement-type amplification. Additive
   amplification is ranked by the plain edge total. To score Raft one must know that terms are
   what count, as the sweep's goal extractor does. The objection stands for every measure tried.
3. Per-block symbolized counts are a good *explanation* tool once a score exists (the 209 election
   blocks are legible), not a score.
4. Provenance (the E3 stack) and the time-only intuition both lead to the same wall: what is
   re-derived in Raft is an abstract goal (a leader) whose concrete records differ by term, while
   the records that are literally repeated (heartbeats carrying the same entry) are benign.
   Knowing that `term` is a version stamp is the program knowledge.

## The one untested path: per-decision counterfactual forks

The whole-run counterfactual above fails because the two runs differ in tick count. A fork at one
decision does not: replay a schedule up to the tick where a delivery is held, release it, continue
under the *same policy* (a `HoldScheduleDriver`, not raw bytes: flipping one byte re-decodes every
later decision), and diff the two runs per edge or per block. Both branches run the same clock, so
the diff is the consequence of one delivery. Sum positive parts over all held deliveries.

Hypothesis: this ranks amplification with no program knowledge and is ≈ 0 where no schedule
changes the work. It is the executable form of E3.3's negative leaves (which failed on Raft
because the absence test is inside `raft_step`) and LDFI's move; the design doc left it to "the
search loop". Acceptance and kill criteria, all hand-computed already:

- `rpc_retry`, hold past the timeout: releasing response k removes exactly k's retry (one send,
  one arrival, one processed, one response) and nothing else.
- `rpc_retry`, `max_attempts = 1`: every fork's diff ≈ 0 in program work.
- Raft storm: releasing a held `AppendEntries` removes the election it would have suppressed; the
  diff is the RequestVote/vote path.
- Raft heartbeats-only: diffs ≈ 0. **If comparable to the storm's, the path is dead.**

Per-fork framework noise should be O(1 record), not O(ticks); the control says whether it is.
Cost: one replay per held record per schedule, hundreds of ~1 s replays on the existing
harnesses. It needs only the light instrument (`edge_counts`, `hold_schedule`, the harnesses),
not lineage.

## Artifacts

The harness is not on this branch; it lives on branch `spike/perffuzz-metastability` (worktree
`.infinity/.sandboxes/perffuzz-hail-mary`, uncommitted at the time of writing), as additions to
`hydro_test/src/cluster/rpc_retry.rs` and `raft.rs`:

- `perffuzz_hail_mary_*_generate_corpus` (run under `cargo sim`, `--ignored`) and
  `*_evaluate_corpus` (`HYDRO_PERFFUZZ_CORPUS=<dir>`; variants via `HYDRO_PERFFUZZ_MAX_ATTEMPTS`,
  `HYDRO_PERFFUZZ_ELECTION_EVERY`). Per-block counting needs the `perffuzz_blocks` build described
  in `raft.rs::block_counts` (register the cfg in `hydro_test/build.rs` with
  `cargo::rustc-check-cfg=cfg(perffuzz_blocks)`).
- Shared analysis in `raft.rs`: `print_generic_columns` (edge columns), `print_block_columns`
  (block aggregates, symbols, excess over prompt), `spearman`.
- Corpora and logs from the runs above were under `/tmp/perffuzz-*` (not kept).
