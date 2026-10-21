# Spike: brute-force fault enumeration vs. lineage-driven search

## What we did

- Added `Strategy::BruteForce` to `hydro_lang::sim::ldfi`: the same run loop and the same
  observed fault universe as `Strategy::Blind`, but the next hypothesis comes from a pure
  combinatorial enumerator (`enumerate_hypothesis`, a depth-first walk over combinations by
  increasing size, then lexicographic in the weakest-first order) instead of the HiGHS ILP, and
  the omissions of one hypothesis are capped by a new `Ldfi::max_omissions(n)` (default
  unbounded). Crash constraints (one crash per node, crash budget, "not every node", checked
  cluster cap) and the tried / vacuous-superset pruning are mirrored from `solve_hypothesis`.
- Unit tests: brute force with no omission cap proposes exactly the sets the blind ILP does
  over a mixed omission/crash universe; the caps, the bounds and the vacuous sets prune.
- Added a `brute` column, and wall-clock (`Instant`, search only, compile excluded) to the
  existing head-to-head harnesses: `two_pc_ldfi::compare_search_strategies`,
  `isr_replication::compare_search_strategies`, and a new
  `ec_ldfi::compare_search_strategies_echo_broadcast` (echo reliable broadcast, three members,
  crash budget 1 and 2, exhaustive). Each asserts every strategy agrees on whether a violation
  exists.
- Ran all three in the default `dev` (unoptimized) profile, `--test-threads=1`, on the branch
  `hydrofrac-crash-budget` plus this patch. Logs: `ec_echo.log`, `two_pc.log`, `isr.log`.

## Measurements

"instances" is simulator instances (schedules explored, summed over hypotheses). `exh` is
`Exploration::Exhaustive`; `fuzz k` is `Exploration::Fuzz { instances: k }` per hypothesis.
`brute` ran with `max_omissions(2)` everywhere.

| example | config | strategy | outcome | hypotheses | instances | wall seconds |
|---|---|---|---|---|---|---|
| echo reliable broadcast, n=3 | crash budget 1, exh | ldfi (Precise) | certificate | 10 | 2641 | 7.7 |
| echo reliable broadcast, n=3 | crash budget 1, exh | blind | exhausted | 13 | 2329 | 5.9 |
| echo reliable broadcast, n=3 | crash budget 1, exh | brute | exhausted | 13 | 2329 | 6.0 |
| echo reliable broadcast, n=3 | crash budget 2, exh | ldfi (Precise) | certificate | 21 | 4076 | 10.6 |
| echo reliable broadcast, n=3 | crash budget 2, exh | blind | exhausted | 31 | 3511 | 9.1 |
| echo reliable broadcast, n=3 | crash budget 2, exh | brute | exhausted | 31 | 3511 | 8.9 |
| 2pc, n=2 | 1 crash, exh | ldfi | violated | 2 | 24 | 0.2 |
| 2pc, n=2 | 1 crash, exh | blind | violated | 2 | 24 | 0.2 |
| 2pc, n=2 | 1 crash, exh | brute | violated | 2 | 24 | 0.2 |
| 2pc + ctp, n=2 | 1 crash, exh | ldfi | certificate | 9 | 158 | 1.0 |
| 2pc + ctp, n=2 | 1 crash, exh | blind | exhausted | 14 | 238 | 1.8 |
| 2pc + ctp, n=2 | 1 crash, exh | brute | exhausted | 14 | 238 | 1.8 |
| 2pc + ctp, n=2 | 2 crashes, exh | ldfi | violated | 5 | 120 | 1.0 |
| 2pc + ctp, n=2 | 2 crashes, exh | blind | violated | 15 | 262 | 1.9 |
| 2pc + ctp, n=2 | 2 crashes, exh | brute | violated | 15 | 262 | 1.8 |
| 2pc, n=3 | 1 crash, exh | ldfi | violated | 2 | 1152 | 15.9 |
| 2pc, n=3 | 1 crash, exh | blind | violated | 2 | 1152 | 15.9 |
| 2pc, n=3 | 1 crash, exh | brute | violated | 2 | 1152 | 15.4 |
| 2pc + ctp, n=3 | 1 crash, fuzz 4 | ldfi | certificate | 13 | 52 | 0.5 |
| 2pc + ctp, n=3 | 1 crash, fuzz 4 | blind | exhausted | 30 | 120 | 1.6 |
| 2pc + ctp, n=3 | 1 crash, fuzz 4 | brute | exhausted | 27 | 108 | 1.3 |
| 2pc + ctp, n=3 | 2 crashes, fuzz 4 | ldfi | violated | 7 | 28 | 0.5 |
| 2pc + ctp, n=3 | 2 crashes, fuzz 4 | blind | violated | 35 | 140 | 2.4 |
| 2pc + ctp, n=3 | 2 crashes, fuzz 4 | brute | violated | 52 | 208 | 2.9 |
| isr unclean, closure | 1 crash, fuzz 8 | ldfi | violated | 9 | 72 | 24.0 |
| isr unclean, closure | 1 crash, fuzz 8 | blind | violated | 19 | 152 | 57.5 |
| isr unclean, closure | 1 crash, fuzz 8 | brute | violated | 19 | 152 | 57.4 |
| isr unclean, dataflow | 1 crash, fuzz 8 | ldfi | violated | 12 | 96 | 6.0 |
| isr unclean, dataflow | 1 crash, fuzz 8 | blind | violated | 19 | 152 | 9.7 |
| isr unclean, dataflow | 1 crash, fuzz 8 | brute | violated | 19 | 152 | 9.4 |
| isr clean, closure | 1 crash, fuzz 8 | ldfi | certificate | 12 | 96 | 25.0 |
| isr clean, closure | 1 crash, fuzz 8 | blind | exhausted | 63 | 504 | 169.5 |
| isr clean, closure | 1 crash, fuzz 8 | brute | exhausted | 63 | 504 | 165.9 |
| isr clean, dataflow | 1 crash, fuzz 8 | ldfi | certificate | 18 | 144 | 7.9 |
| isr clean, dataflow | 1 crash, fuzz 8 | blind | exhausted | 63 | 504 | 30.4 |
| isr clean, dataflow | 1 crash, fuzz 8 | brute | exhausted | 63 | 504 | 28.2 |

The random fuzzer columns of the existing harnesses were unchanged by this spike (2pc: violated
in 25 / 24 instances for n=2 / n=3 without CTP, none in 2000 with CTP; ISR: violated in 128 / 29
instances unclean, none in 504 clean).

## Raw observations

- Verdicts agree across the three strategies in every row (the harness asserts it; no
  disagreement was observed).
- Under exhaustive exploration `brute` and `blind` proposed the same hypotheses in the same
  number (2pc and echo broadcast are `fail_stop`-only, so the universe is crashes only and
  `max_omissions` never binds; ISR's lossy `repl` channel yields too few omittable messages for
  a cap of 2 to bind within 63 hypotheses). Under sampled exploration their hypothesis counts
  differ (27 vs 30, 52 vs 35) because the universe grows with whatever schedules were sampled
  and the ILP's sum-of-indices tie-break is not the enumerator's lexicographic order.
- Wall time tracks simulator instances, not the solver: the ILP's per-hypothesis cost was not
  measurable against runs of 0.1–3 s per hypothesis (brute ≤ blind by 0–4 % everywhere).
- `ldfi` used fewer hypotheses than `blind`/`brute` in every row where they differed, but on
  echo broadcast it explored *more* schedules (2641 vs 2329; 4076 vs 3511) and took longer,
  because its weakest-first crash points leave more of the protocol running per hypothesis.
- The first search of a fresh `CompiledSim` paid ~45 s of trybuild compilation in an earlier
  version of the echo-broadcast harness; the harness now compiles once per comparison and the
  numbers above exclude it.
