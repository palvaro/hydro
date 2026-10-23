# Sedge by the round: using Cress to specialize the overload harness

**Date:** 2026-10-15. **Tree:** branch `cress_n_sedge` plus the changes below. **Question:** can
Sedge check a program that accepts and completes *work* but has no request/completion shape, with
Cress telling it what the program's outputs mean? Prototype on one program,
`cache_with_expiry` (`hydro_test/src/cluster/witnesses/cache_thundering_herd.rs`), plus a
regression on `retry_service`.

Sedge as it stood (`2026-10_sedge_on_corpus.md`, section 1) fed one stream with
`LoadItem::from_id(seq)`, read completions by id from a hand-written `Observed` impl, and judged
goodput, latency, abandonments and gauges. `cache_with_expiry` defeated it on both ends: its
workload is ten hot keys walked eight lookups at a time, which `from_id` cannot produce, and the
collapse it exhibits leaves every lookup answered (by a stale fill) while the origin is buried,
which an id-based verdict does not see. This document records what was changed in Cress and
Sedge so that the same attribute drives both, what Cress says about the cache's outputs, what
Sedge then reads on localhost, and what is left.

## 1. Changes

### Cress: the conservation table

Every run of the hold sweep now counts, beside the hook releases and network sends it already
read, the records on each of the program's declared output streams, and the number of times
each tick executed.

- `SimOutputs` gains `sink_named(self, name, &mut OutputPorts)`; `#[derive(SimOutputs)]`
  implements it by sinking each field under its name and recording the field's `sim_output`
  port, its root location and whether its item type is a primitive number
  (`cress/src/harness.rs`, `cress_config_macro`). `hydro_lang::sim::extension` exposes the port
  of a `SimReceiver` for this (`receiver_port`, `cluster_receiver_port`).
- `Schedule` counts tick executions per `(root, member, tick)` (a tick runs under this schedule
  only when the host installs a decision, so the count is exact) and drains the named output
  ports at the end of a run (`cress/src/schedule.rs`).
- `check_with_outputs` (`check` is the old signature over it) classifies every output stream
  from its count in the unheld run and under every hold of every decision point
  (`cress/src/check.rs`, `Conservation`, `OutputKind`):
  - **level**: in every run its count equals the execution count of one tick at its location,
    so it emits exactly one record per tick execution (a sampled depth);
  - **conserved**: its count is the unheld run's under every *finite* hold (the hold to the end
    of the run is read separately as the stall column, since records delayed forever cannot
    complete);
  - **amplified**: some finite hold raised its count above the unheld run's;
  - **shed**: holds only lowered it; **silent**: nothing in any run.
- The report prints the table (stream, kind, unheld count, range over finite holds, range under
  the never-ending hold, the hold that produced the largest count) and, beneath the existing
  curves, each stream's count at every hold length under every decision point. The one-line
  verdict gains an `outputs:` line, and `target/cress/conservation.tsv` gets one row per stream
  (crate, function, configuration, stream, kind, numeric, unheld, finite min, finite max, stall
  min, stall max) for Sedge to read back.

### The attribute: one workload for both tools

Cress's `round = |round, inputs| {...}` closure mixes data sends with clock sends
(`inputs.cache_clock.send(())`), and on a deployment the clocks are wall-clock timers. Both
macros now accept the split form:

```rust
timers(cache_clock = Duration::from_millis(50), origin_clock = Duration::from_millis(50)),
round_data = |round, inputs| {
    for i in 0..8u64 { inputs.lookups.send(((round * 8 + i) % 10) as Key); }
},
```

In Cress (`cress_config_macro`), the streams named in `timers(...)` are clocks the harness feeds
itself, each with as many elements per round as its period divides the longest period (so a
round is one element of the slowest clock; both cache clocks are 50 ms, so one each); `inputs`
has a `SimSender` field per *data* stream only; the closure is called for rounds
`0..workload_rounds` and not afterwards. `round` and `workload(...)` keep working. In Sedge
(`sedge_macro`), `round_data` replaces `load = ...` (either is accepted; exactly one is
required); `inputs` has a `sedge::RoundSender<T>` per data stream, with the same `send(T)`
method, and the harness writes what the closure buffered to each stream's port after the call.
The three cache configurations carry the same closure text in their `amplification_check` and
`overload_check` attributes. Cress's results for them are unchanged by the conversion
(10.20×, 13.51×, benign; `target/cress/results.tsv`).

### Sedge: rounds, generic observation, generalized verdict

- **Offered load is rounds.** The generator (`sedge/src/generator.rs`) offers *rounds* at a
  Poisson rate ρ: `send_round(round)` returns how many records it wrote, and the recorder
  numbers the records of the data stream from zero. `mu` is now in rounds per second; the
  profile is ρ = µ/f, trigger `--trigger-multiple` × µ. The `load = ...` form is the case of one
  record per round.
- **Timers are harness-driven.** A `timers(...)` stream is an external `()` port the harness
  ticks at the period (`TimerDrive`, `drive_timer` in `sedge/src/harness.rs`) instead of a
  `source_interval` inside the process. This is what makes a **capacity dip** possible:
  `--dip origin_clock=8` multiplies that timer's period by 8 during the trigger window, so with
  `--trigger-multiple 1` the trigger removes capacity instead of adding load.
- **Generic observation.** `Observer::stream(name, stream)` registers an output stream whole
  (`sedge/src/program.rs`); `#[derive(sedge::SedgeOutputs)]` on the output struct registers every
  field. Records are serialized to the harness with bincode as the program's own type and read
  back through `serde_json::Value` (`Observation`): a plain number is a value (and an id if
  integral), a record with an integer `id` field carries an id, anything else is counted. No
  `q!` closure is needed in the program's crate. The recorder (`sedge/src/trace.rs`) keeps, per
  stream, records per second and the latest value per second; the TSV has `<stream>_n` and, for
  numeric streams, `<stream>_v` columns (the old `gauge`/`counter` names are now `backlog_v`,
  `retried_n`). The hand-written `Observed` methods still work and feed the same tables.
- **Kinds from Cress.** `run_trial` reads `target/cress/conservation.tsv` for the function and
  configuration (`cress_kinds`); streams it does not name fall back to the item type (numeric →
  level, else work). Explicit `gauge`s are levels, `counter`s are work. Cress's `shed` and
  `silent` map to work.
- **Verdict** (`judge_with_kinds`, `sedge/src/verdict.rs`). Per stream in the baseline window
  [5, 10) s and the tail window [37, 60) s: the mean rate, and for levels the first- and
  second-half means. Goodput is the completion fraction by id when an id source exists (the
  explicit `completions`, or else the single conserved stream whose records carry ids, chosen
  automatically; for the cache that is `completed.id`), otherwise the smallest tail/baseline
  rate ratio over the conserved streams. Amplification is the largest `tail / (baseline + 2/s)`
  over the amplified and unclassified streams. RECOVERED: goodput ≥ 95 %, no abandonments,
  latency within slack when read, no level rising, amplification ≤ 1.5. DRAINING: a level
  falling and none rising. COLLAPSED: amplification ≥ 3, or goodput ≤ 50 %, or a level rising
  while goodput, latency or amplification is degraded. DRAINING otherwise. The old verdict is
  the case with an explicit completion stream and gauges and no amplified stream.

## 2. The conservation table for `cache_with_expiry`

Horizon 1000 rounds, data in rounds 0..125 (1000 lookups), five decision points held for
1, 2, …, 512 rounds and to the end. Columns: unheld count; range over finite holds of every
point; range under the hold that never ends.

| stream | request_dated_no_coalesce | fill_dated_no_coalesce | coalesce |
|---|---|---|---|
| `completed` | conserved 1000; 1000; 8..1000 | conserved 1000; 1000; 8..1000 | conserved 1000; 1000; 8..1000 |
| `fetches` | amplified 98; 14..1000; 8..940 | amplified 74; 14..1000; 8..940 | shed 70; 10..70; 8..18 |
| `fills_applied` | amplified 70; 4..166; 4..10 | amplified 70; 10..940; 4..10 | shed 70; 4..70; 4..10 |
| `fills_redundant` | amplified 28; 0..126; 0..4 | amplified 4; 0..862; 0..4 | silent 0 |
| `fills_stale` | amplified 0; 0..936; 0 | silent 0 | amplified 0; 0..10; 0 |
| `processed` | amplified 98; 14..1000; 4..940 | amplified 74; 14..1000; 4..940 | shed 70; 10..70; 4..14 |
| `waiting_trace` | level 1028 (one per cache tick) | level 1022 | level 1021 |
| `origin_backlog_trace` | level 1000 (one per origin tick) | level 1000 | level 1000 |

Readings:

- `completed` is conserved in all three: every lookup completes once under every finite hold.
  Under the hold that never ends it falls to 8 (hold the lookups themselves) or 64–128 (hold
  fills, fetches, the origin clock), which is the stall column doing its job.
- The two non-coalescing configurations raise `fetches` and `processed` from 98/74 to 1000 under
  a hold of 128 rounds of the lookups' own batch point: every lookup becomes a fetch. That is
  the herd Cress's 10.20× and 13.51× multipliers were reading from the sends.
- `coalesce` sheds: a long hold lowers fetches from 70 to 10 (one per hot key) and never raises
  them. Its one amplified stream is `fills_stale` (0 → 10): a fill held past the TTL arrives
  stale instead of applied, which is the same fill reclassified, not more work, and Cress's
  verdict (sends and admitted records) stays benign. A consumer of the table should read
  amplified streams against the sends count before calling them cost.
- Both traces are detected as levels by the tick-execution match, which is exact; the numeric
  payload hint was not needed. The level counts vary with the hold (616..1250) because a held
  tick runs fewer times and the release runs it more, so "count equals rounds" would not have
  found them.

## 3. Sedge on the cache

Deployed constants: cache and origin clocks 50 ms, `max_fetch_per_tick = 4` → 80 fetches/s,
`ttl_ticks = 20` → 1 s, ten hot keys, eight lookups per round, `mu = 80` rounds/s. µ in rounds
is nominal: healthy, a round costs about 0.06 fetches (ten expiries a second over the lookups);
in a full herd every lookup is a fetch and the origin sustains 8 lookups × 10 rounds/s = 80/s,
so the herd is self-sustaining while 8ρ > 80, i.e. f < 8, which is what Cress's multipliers
(10.2×, 13.5× > f) predict for f = 2 and 4. One trial per factor, seed 1, default profile (10 s
at ρ, 5 s at 2µ = 160 rounds/s, 45 s at ρ), kinds from the table above, goodput and latency by
id from `completed`.

| configuration | f=2 (ρ = 40 rounds/s) | f=4 (ρ = 20) | tail detail |
|---|---|---|---|
| `request_dated_no_coalesce` | **COLLAPSED** | **COLLAPSED** | f=2: `fetches` 316/s against 35/s baseline (= the lookup rate: every lookup a fetch), `fills_stale` 87/s against 0, `processed` at capacity, origin backlog 10.6 k → 13.4 k, goodput 100 %, p50 62 ms against 0.1 ms. f=4: `fetches` 168/s against 17/s, backlog 6.8 k → 7.7 k |
| `fill_dated_no_coalesce` | **COLLAPSED** | DRAINING | f=2: `fills_redundant` 81/s against 5.6/s (the origin's whole capacity), goodput 63 %, backlog 3.3 k → 4.0 k, waiting 1.1 k → 2.1 k, p99 2.5 s. f=4: goodput 88 %, `fills_redundant` 81/s against 1.6/s, backlog flat at 1450, waiting falling 32/s (~10 s from empty), p99 8.7 s |
| `coalesce` | RECOVERED | RECOVERED | `fetches` and `processed` 10/s in both windows (one per key per second), levels 0, goodput 100 %, p50 0.1 ms |

(`processed` reads 87/s where capacity is 80/s because records arriving in the 2 s grace period
were lumped into the last bucket; fixed after this run, see section 4.)

Readings:

- **All six verdicts agree with the labels and with the Cress prediction** (collapse iff
  multiplier > f) for the two hazardous configurations at both factors, and recover for the
  benign one.
- **The generalized verdict reads the collapse the id-based one could not.** Under
  `request_dated_no_coalesce` goodput is 100 % in the tail: every lookup is answered by a stale
  fill within 60–150 ms. The old verdict would have had median latency (62 ms against 0.1 ms,
  outside 5× + 20 ms) and a rising gauge to go on. The new one reads `fetches` at 9× and
  `fills_stale` from nothing to the origin's whole capacity, which is the mechanism in the
  program's own terms, and the reason line says so.
- **A disagreement with the simulator's trajectory, not with the label.** In the simulator
  `fill_dated_no_coalesce` recovers from the load trigger (its label rests on the hold
  experiment). Deployed at f = 2 it collapses: the 5 s trigger builds a queue of 3300 fetches,
  the redundant fills for a key arrive spread over the queueing delay and keep the key alive,
  but the lookups waiting behind them (2100 by the end) do not complete, and the origin spends
  all 80 fetches/s on redundant fills. At f = 4 the waiting set is falling at 32/s and the
  verdict is DRAINING, which a tail twice as long would decide. Sedge sees what the hold
  experiment predicted and the simulator's single prompt trajectory did not.
- **The trigger tipped the cache in one second.** At 160 rounds/s the origin's queue reached 536
  within the first second of the trigger and 2700 by the third, at which point every fill is
  older than the 1 s TTL; the hand estimate in the attribute's doc comment that a 2µ hot-key
  burst would stay under capacity was wrong by an order of magnitude, because once the queueing
  delay exceeds one cache tick every lookup on an expired key misses.

### Regression: `retry_service` through the new path

`retry_service` keeps its hand-written `Observed` and `load = requests`, so this exercises the
round generator with one record per round, the harness-driven timers (50 ms client clock, 10 ms
server clock), and the generalized verdict with explicit kinds (`backlog`, `pending` levels;
`sent`, `retried`, `served`, `denied` work). One trial per factor.

| configuration | f=2 | f=4 | 2026-10-13 result | tail detail |
|---|---|---|---|---|
| `no_governance` | COLLAPSED | DRAINING | COLLAPSED ×2 / DRAINING ×2 | f=2: goodput 0 %, 4695 abandoned, backlog 11.7 k → 14.4 k, `retried` 493/s against 0. f=4: goodput 97 %, 94 abandoned, backlog falling 163/s (~2 s from empty), p50 2.9 s, `retried` 145/s |

The verdicts and the numbers behind them are the ones the old harness produced; the only change
visible in the output is that the `retried` counter now appears in the reason line as the
amplified stream (493/s against 0 at f = 2 is 2λ, the cap-type retry rate section 3 of
`2026-10_cress_sedge_in_concert.md` predicts), and that the timers come over a port. The
budgeted configurations were not rerun for time.

### Capacity dip as the trigger

The same three configurations at f = 2 with no load burst (`--trigger-multiple 1`) and the
origin's clock stretched eightfold for the 5 s trigger window (`--dip origin_clock=8`): the
origin serves 10 fetches/s instead of 80 while the lookups keep arriving at 320/s. This is the
deployment's form of the hold experiment the `fill_dated_no_coalesce` label rests on.

| configuration | f=2, dip ×8 | tail detail |
|---|---|---|
| `request_dated_no_coalesce` | **COLLAPSED** | `fetches` 327/s against 35/s, `fills_stale` 80/s against 0 (= `processed`, the whole capacity), backlog 9.3 k → 12.1 k, goodput 100 %, p50 63 ms against 0.1 ms |
| `fill_dated_no_coalesce` | **COLLAPSED** | `fills_redundant` 74/s against 6.4/s, `fetches` 169/s against 22/s, backlog 2.1 k → 3.2 k rising, waiting 514 → 367, goodput 99.7 %, p99 10.3 s |
| `coalesce` | RECOVERED | `fetches` and `processed` 10/s in both windows, levels 0 |

A capacity dip of 5 s tips both non-coalescing configurations as surely as a load burst did, and
leaves the coalescing one untouched; the backlog it builds (2.1–9.3 k at the start of the tail)
is smaller than the burst's (3.3–10.6 k) and the verdicts are the same. The dip is the trigger
that matches Cress's perturbation (a hold on the origin's clock is a dip to zero), and it needs
no guess about how large a burst the program's hot keys can turn into fetches.

Raw rows for every trial above are in `design_docs/sedge_corpus/results_rounds.tsv` (copied from
`target/sedge/results.tsv`, with the new `by_id`, `relative_goodput`, `amplified_stream` and
`amplification` columns) and the cache's conservation rows in
`design_docs/sedge_corpus/cache_conservation.tsv`.

## 4. What broke or disagreed

- **Grace-period records in the last bucket.** The recorder clamped every arrival after the
  profile's end into bucket 59, so a 2 s grace period of a saturated origin added 160 records to
  one second and `processed` read 87/s against a capacity of 80/s. Records after `total` are now
  left out of the per-second rows (fates read from them still count). The cache verdicts above
  were produced before the fix; none depends on it.
- **Report file names.** The workload string for the split form (`cache_clock=1/round`) put a
  `/` into the report's file name and the first run wrote no report; fixed by escaping it.
- **`shed` under coalescing** is read as `amplified` on `fills_stale` by the count rule alone
  (section 2). The table is right about the count and wrong about the meaning; Cress's verdict
  is not affected, and Sedge maps `shed`/`silent` to unclassified work, but a stream whose
  amplification is a reclassification of conserved work should be marked as such, which needs
  the conservation of a *sum* of streams (`fills_applied + fills_stale + fills_redundant` is
  `processed`), not of each one.
- **The id overlay's choice is a heuristic.** It picks the single conserved stream whose records
  carry an integer `id` field (or are integers) *and* that Cress classified; a program with two
  such streams gets no overlay and is judged by rates alone. For the cache the lookup's arrival
  index is the harness's sequence number only because there is one data stream over one ordered
  connection; with several data streams the ids would have to be per stream.
- **Levels without a table.** When Cress has not run, a numeric stream is a level. A `Stream<u64>`
  of completion ids would then be read as a level and never as completions; the table is the
  fix, and the fallback is only for programs Cress has not seen.
- **Profile semantics changed.** `mu` and the rates in the summary are rounds per second. For
  `load = ...` programs a round is one request, so the old numbers carry over unchanged.

## 5. What is left

- **`Cluster` locations in `sedge_macro`.** Still rejected. `Observer::stream` would need a
  `Cluster` variant (`send_bincode_external` from a cluster, one column per member or a sum),
  and the macro `members(x = N)` and `with_cluster`. Cress's table already handles clusters
  (counts per member; a level is matched per member). `gossip_with_resend` and `election`, which
  have no conserved stream, would then exercise the fallback where RECOVERED means every
  amplified stream back within slack and no level rising.
- **The capacity dip** is implemented (`--dip timer=K`) and run once, on the cache at f = 2. It
  should be run across factors like the burst, and the two triggers' critical factors compared:
  the rate model says f_c is a property of the overloaded state and not of how it was entered,
  which two sweeps would test. A dip to zero (`K` very large) is Cress's hold exactly, and a
  sweep over `K` at fixed ρ is the deployment's version of Cress's hold-length sweep.
- **Calibrating µ.** `mu` is declared, in rounds per second, and for a program whose cost per
  round depends on its state (the cache: 0.06 fetches per round healthy, 8 in a herd) no single
  number is the capacity. What the headroom factor measures is then "ρ against the capacity in
  the state the trigger leaves the program in", which is the right quantity but should be said:
  a derived µ_herd = capacity / records-per-round-in-the-amplified-state, with the
  records-per-round read from Cress's table (`fetches` 1000 for 1000 lookups under the herd
  hold, so 8 per round), would give f_c = 80/(8ρ) directly and make "collapse iff A > f" a
  statement about rates rather than a coincidence of units.
- **Conservation of sums**, as above, so that reclassification is not read as amplification.
- **The `shed` reading in Sedge.** Cress's `shed` is mapped to unclassified work; a deployment
  should read a shed stream below baseline as shedding, not as goodput loss (it is not
  conserved) nor as amplification.
- **Sweeps and seeds.** One trial per factor here. The cache's predicted f_c = 8 is a direct test
  of the rate model that a run at f = 8 and 16 would settle in ten minutes.
