# Why `CompiledSim::exhaustive` misses an ABD-KV liveness bug that random sampling finds

For simulator maintainers. Labels: **DEMONSTRATED** (command and output below), **ARGUED** (from code, with `file:line`),
**GUESSED**. All measurements are at commit `6f6672ce63`, debug build, on one 8-core machine.

## Program, harness, bug

- Harness: `abd_kv_two_writes_failure_free_completion` in
  `hydro_std/src/ec_inference_demos/ec_abd_kv_hydrofrac.rs`. It runs `abd_kv_register` (`abd.rs:584`) with 3 replicas,
  majority 2 and 1 client. The client sends two writes to different keys back to back (`rid 1: k1=10`, `rid 2: k2=20`)
  with no `quiesce()` between them. The harness then runs until quiescence and checks that both writes were acked.
  There are no faults.
- Bug (left unfixed): `covering_quorum_retiring`, `quorum.rs:450-452`, runs
  `pending.retain(|k| active.contains(k) || newly_active.contains(k))` whenever any activation arrives. That drops the
  buffered replies for a request whose activation has not been batched yet. That request then never reaches a majority
  and hangs. `quorum_retiring` (`quorum.rs:244-246`) has the same pattern.
- **DEMONSTRATED** with the simulator log of a minimized failing schedule (see "Method"):
  1. The replicas answer both queries.
  2. In the client's covering-quorum tick (`quorum.rs:414`), the `response_batch` hook releases replies for rid 1 and
     rid 2. Meanwhile the `active_batch` hook (`quorum.rs:415`) releases *nothing*, over four ticks (choices #18, #21,
     #24, #27).
  3. The `active_batch` hook then releases *only* `(1, Write{k1})` (choice #32). The `retain` drops rid 2's buffered
     replies.
  4. rid 2 is activated one tick later, but too few replies are left. Write 2 is never acked.

## Measured rates (DEMONSTRATED)

| exploration | schedules | time | failing |
|---|---|---|---|
| `fuzz_bounded(10_000)` (bolero random bytes) | 10,000 | 61.8 s | **261** (write 1 missing 142, write 2 missing 119) |
| `exhaustive()`, killed after a 15-minute budget | 195,579 | 840 s of exploring | **0** (last progress line: 180,000 schedules, incomplete 0) |
| uniform random per choice (trace driver, below) | 2,000 | 10 s | 37 (1.85%) |

A previous run measured 243 of 10,000 and 0 in about 140,000 schedules (16 minutes). The rates agree.

## Why depth-first order never gets there

**Engine order (ARGUED).** Bolero's exhaustive driver
(`bolero-generator-hydro-0.13.6/src/driver/exhaustive.rs`) treats the run as a stack of choice frames:

- `step()` scans from the *last* frame back, increments the deepest frame that is not at its bound, and truncates
  everything after it. This is a depth-first odometer: the last decision varies fastest.
- `select(bound)` returns the frame's value, which starts at 0. So every choice's first alternative is "0":
  - `bool` is `false`.
  - `(a..=b)` is `a`.
  - the scheduler's `(0..n).any()` (`compiled.rs:2199`) picks the first ready tick.

**What "0" means for each hook (ARGUED).**

| hook | code | first alternative |
|---|---|---|
| `TotalOrder` batch | `runtime.rs:215` | releases the *minimum* number of items |
| `NoOrder` batch | `runtime.rs:286` | `false` means "don't stop", then index 0, repeated: releases *everything* available |

So exhaustive's first schedule is the maximally synchronous one for unordered batches. All pending activations land
together in the first client tick, which makes the bug impossible. The bug needs the unordered `active_batch` to say
"stop" (value 1) in several ticks, then split the two activations.

**Where the decisive choices are (DEMONSTRATED).** A schedule has 44–88 non-trivial choices (median 71). I fixed the
first *k* choices to 0 and drew the rest uniformly:

| k (choices fixed at 0) | failing samples |
|---|---|
| 0 | 19/1000 |
| 20 | 367/1000 |
| 32 | 5/1000 |
| 33 | 0/3000 |
| 36 and above | 0/1000 |

So **every failure needs a non-default value at index ≤ 32**. I greedily minimized all 37 sampled failures:

- Each still needs **9–46 non-default choices**, most of them 9–15.
- The deepest non-default choice is at index #20–#76.
- For the schedule above, the 11 non-default indices are #7, #10, #11, #16, #18, #21, #23, #24, #25, #27 and #32.

**The arithmetic (ARGUED from the demonstrated numbers).** DFS changes the choice at index 32 only after it has
enumerated the whole subtree below the all-default prefix of length 33.

- Knuth's estimator (the mean, over random walks, of the product of `(bound + 1)` below the prefix) puts that subtree
  at about **5.8e20 schedules** (1,000 walks).
- The estimator is noisy: the prefix of length 32 gave 4.7e21, and the whole tree gave 4e35 in one run and 1.7e36 in
  another. Read these as orders of magnitude.
- Exhaustive runs about 233 schedules/s here. 5.8e20 / 233 ≈ 2.5e18 s, about **8e10 years**.
- The subtree below prefix length 52 is about 1e6 schedules and below 56 about 4e3. So after 15 minutes (2e5
  schedules), exhaustive has only been varying roughly the last 10–15 choices of the first schedule: late phase-2
  ack/fold permutations, long after the window where the bug can happen.

Random sampling succeeds because about 2% of uniform choice sequences hit the window.

## SUGGESTION (not implemented)

1. **Randomize the order, not just the leaves.** Exhaustive could use a random but complete order: a random
   permutation of each frame's alternatives, or random restarts that each run DFS under a randomly sampled prefix.
   The bug is found as soon as some prefix up to index 32 is non-default, which is likely at a 2% sample rate. You
   keep "exhaustive eventually" and get sampling's early hit rate.
2. **Bound deviations per *stream*, not per *bool* (delay bounding).** Plain iterative deepening on the number of
   non-default choices does not help much. Minimal failures need at least 9 deviations among about 60 positions
   (C(60, 9) ≈ 1.5e10). But most of those deviations are one semantic event repeated: "keep holding back this
   unordered batch" (#18/#21/#24/#27). Charging one delay for deferring an item across any number of ticks, as in
   delay-bounded scheduling, would plausibly reduce this bug to about 2–3 delays: hold the activations until rid 2's
   replies are buffered, then split the activations (GUESSED).
3. **Explore both polarities first.** The `NoOrder` default "release everything" (`runtime.rs:286`) hides any "reply
   arrives before its activation" race. Running the first schedule, or a whole bounded pass, with the opposite
   per-hook default ("release at most one / nothing") is cheap and targets exactly this class (GUESSED that it alone
   suffices).

## Method and repro commands

```sh
# rates
ABD_KV_FUZZ=10000 cargo test -p hydro_std --lib \
  ec_abd_kv_hydrofrac::abd_kv_two_writes_failure_free_completion -- --ignored --nocapture
timeout 900 cargo test -p hydro_std --lib \
  ec_abd_kv_hydrofrac::abd_kv_two_writes_failure_free_completion -- --ignored --nocapture
# add ABD_KV_FUZZ=10000 ABD_KV_PANIC=1 to get a simulator replay log of the first failure
```

The choice-level data came from a local, **uncommitted** instrumentation patch. It adds
`hydro_lang::sim::trace_driver::TraceDriver`, a `bolero::Driver` with the same `select(bound)` semantics as the
exhaustive driver. The driver follows a script, then either all zeros or uniform random, and records each
`(value, bound)`. The patch also adds `CompiledSim::run_traced`, which runs one instance in exhaustive mode under that
driver, and an `#[ignore]`d `abd_kv_two_writes_trace` test.

The test runs these steps:
1. all-default schedule
2. uniform sampling
3. greedy minimization
4. Knuth estimates
5. depth scan
6. logged replay

Its knobs are `ABD_KV_TRACE_SEEDS`, `ABD_KV_ZERO_PREFIX`, `ABD_KV_DEPTH_SCAN`, `ABD_KV_KNUTH_SAMPLES`, `ABD_KV_MIN_ALL`
and `ABD_KV_TRACE_LOG` together with `HYDRO_SIM_LOG=1`. Run it with
`RUSTFLAGS='--cfg abd_kv_trace' cargo test -p hydro_std --lib ec_abd_kv_hydrofrac::abd_kv_two_writes_trace -- --ignored --nocapture`.

## Finding 2: deferred data never schedules a tick, in the simulator or in deployment

Different question: if a tick's only pending work is data deferred to the next tick, does the next tick run?

- **Hydro uses the lazy variant (ARGUED).** `HydroNode::DeferTick` always compiles to `defer_tick_lazy()`
  (`hydro_lang/src/compile/ir/mod.rs:4821`). Deployment and the simulator use the same lowering.
- **DFIR (ARGUED).** Only a non-lazy `defer_tick()` with buffered data calls `schedule_subgraph(true)` at the end of a
  tick (`dfir_lang/src/graph/meta_graph.rs:2126-2153`, `2205-2210`). `defer_tick_lazy` handoffs are just swapped in.
  The next tick runs only after an external wake: `run_available` stops when `can_start_tick` is unset
  (`dfir_rs/src/scheduled/context.rs:353-370`), and `run` then parks (`:391-407`). A deployed process runs
  `dfir.run().await` (`hydro_lang/src/compile/trybuild/generate.rs:282`).
- **DEMONSTRATED** (throwaway test in `dfir_rs/tests`, `cargo test -p dfir_rs`). It feeds `source_stream -> X -> for_each`
  one item and calls `run_available_sync()` once:
  - `X = defer_tick_lazy()` gives `outputs [], 1 tick`.
  - `X = defer_tick()` gives `outputs [1], 2 ticks`.
  - Then an item sent on an *unrelated* `source_stream` in the same graph releases the lazy item (`[]`, then `[1]`).
- **Simulator (ARGUED).** `SimTick::can_run` (`hydro_lang/src/sim/compiled.rs:2022-2027`) needs one of the tick's
  *own* hooks to be able to release. Deferred data does not count. The tick DFIR's `run_tick()` result is only
  asserted (`:2254`, `:2277`). That result reflects nothing pending: lazy handoffs never set `can_start_tick`. So no
  signal is ignored.

**Verdict.** On the question asked, the simulator matches deployment. When no later event reaches the process,
deferred records are never released in either one. The counterexample from `deferred_records_are_fragile`
(`hydrofrac/src/check/brute_tests.rs:912`) can therefore happen in deployment. That test sends 1 and 2 to two cluster
members, and each member batches them into a tick, `defer_tick`s them and outputs them. A member whose last batch
holds the last message never emits it, so the members disagree with no fault.

**Remaining divergence (ARGUED, other direction).** A deployed process runs *every* tick on *any* wake of its single
DFIR. That includes input meant for other ticks or top-level streams, and spurious waker fires. The simulator runs a
tick only when that tick's hooks have input. So the simulator never explores "deferred records released by an
unrelated wake". It may miss bugs that need that release, but it never invents an impossible schedule here.
