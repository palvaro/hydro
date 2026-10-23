# Sedge over the Cress witness corpus

**Date:** 2026-10-14. **Tree:** `feat/sedge-overload-harness` (`4de7c45e41`) with the corpus
commit `eff1be0086` (`cress-corpus`) applied on top, plus the Sedge annotations in this commit.
**Question:** which of the programs that were written to give Cress something to label can Sedge
deploy and overload, what does each one need, and what does Sedge say about the ones it can run?

Cress labels a program in the schedule domain: hold a batch, count the extra sends. Sedge runs the
program on localhost, offers it a Poisson workload at a fraction `1/F` of its declared capacity
`μ`, doubles that for five seconds, and reads the tail. The corpus was written for Cress. This
document is the record of pointing Sedge at it.

## 1. What Sedge requires of a program

The contract is fixed by `sedge_macro::overload_check` (the attribute), `sedge::program`
(`LoadItem`, `Observed`, `Observer`) and `sedge::verdict::judge`. Read together:

**Signature.** The annotated function must take only

- `&Process<'a, X>` parameters: each becomes one localhost process. `&Cluster` is rejected
  ("does not support cluster locations yet").
- Exactly one data stream, named by `load = <param>`, of type
  `Stream<T, Process<'a, X>, Unbounded, O, R>` where `T: sedge::LoadItem`. The harness connects an
  external bincode port to it and sends `T::from_id(0), T::from_id(1), …` at the profile's rate
  over one TCP connection, so the stream is in fact totally ordered and exactly-once and may be
  weakened to whatever the parameter asks for. `LoadItem` is implemented for `()`, `String` and
  the integer types; a program's own request type can implement it in the program's crate.
- Any number of `Stream<(), Process<'a, X>, Unbounded, ..>` parameters, each of which must be given
  a period in `timers(param = Duration-expr, …)`; the harness drives them with
  `source_interval`. A second *data* stream, or a `KeyedStream`, is an error.
- Plain parameters, each given a value in the attribute by name (`config = …`). Generic type
  parameters are given concrete types (`T = u64`).

**Capacity.** `mu = <rps>` declares the service rate the factors divide: at factor `F` the
baseline is `λ = μ/F` and the trigger is `2μ` (`--trigger-multiple`). Nothing checks that `μ` is
true; it is a claim the program's author makes about the deployed configuration, and the verdict
is read relative to the trial's own pre-trigger baseline, so a wrong `μ` shifts the factors rather
than the verdict. For capacity to be *real* on a deployment, the bottleneck must be a wall-clock
quantity: either CPU burned per request (`rpc_retry`'s `service_time`) or a per-tick cap that is
released by a timer stream (`server_capacity` per `server_clock` element). A cap that is released
per *dataflow tick* without a timer, as in `backoff_retry` and `bounded_queue_rejection`, is not a
capacity on a deployment: the tick runs when data arrives, so throughput scales with how arrivals
batch (this is why `rpc_retry` raised `max_per_tick` to 50 and let `service_time` set μ).

**Observation.** The function's return value is handed to `sedge::Observed::observe`, which the
program's crate implements by hand (its `q!` closures must live in the crate's own source):

- `completions(Stream<u64>)`: the id of each request that completed, where the id is the
  generator's sequence number. Every corpus program numbers its input with `enumerate()` over a
  single ordered connection, so this is free. Latency is offer-to-completion. Duplicates are
  ignored, so a stream of "every processing, including repeats" works.
- `abandoned(Stream<u64>)`: ids the program gave up on. Counted against Recovered.
- `gauge(name, Stream<f64>)`: a level (queue depth, pending table), read by the verdict: rising
  through the tail is a mark of collapse, falling of draining.
- `counter(name, Stream<f64>)`: increments summed per second into the trace; not read by the
  verdict (re-sends, serves, denied retries).

**Verdict** (`judge`): baseline = second half of the pre-trigger period (default `[5 s, 10 s)`),
tail = second half of the post-trigger period (`[37 s, 60 s)`), both by *offer* time.
RECOVERED if tail completion fraction ≥ 95 % of baseline's, no abandonments, median latency within
5× baseline + 20 ms, no gauge rising. DRAINING if some gauge is falling and none rising, whatever
the goodput (with a time-to-empty estimate). COLLAPSED if tail completes ≤ 50 % of baseline's
fraction, or a gauge is rising while goodput or latency is degraded. DRAINING otherwise.

**Running.** `./scripts/sedge [--factors 2,4 --trials N …] hydro_test <function-substring>`
builds a runner that links `hydro_test` with its `sedge` feature, deploys every registered check
on localhost with `hydro_deploy`, and appends to `target/sedge/results.tsv` and
`target/sedge/traces/`. The first configuration pays the full deploy build (about ten minutes on
this machine); later configurations of the same program rebuild in about a minute, and each
trial is `before + trigger + tail + grace` ≈ 62 s at the defaults.

## 2. Fit of every corpus program

Buckets: **(a)** fits as-is: Process-only, one data input, a completion signal, a timer-released
capacity, so the only work is the `Observed` impl and the attribute. **(b)** fits with a small
adapter to the program or to Sedge. **(c)** outside Sedge's model: no offered-request stream whose
completion can be read.

| program (`function`) | configurations | Cress label | fit | what's needed | notes |
|---|---|---|---|---|---|
| `rpc_retry` (`rpc_with_retries`) | `three_attempts`, `one_attempt` | hazardous 2.98×, stops / benign | **a** (done upstream) | – | μ from `service_time` 2 ms; already annotated on the Sedge branch |
| `blind_budget/service` (`retry_service`) | 7: `unbounded`, `no_governance`, `no_retries`, `time_budget`, `success_budget`, `hybrid_budget`, `circuit_breaker` | 155× grows / 2.98× stops / benign / 1.56× / 1.12× / 1.56× / 1.12× | **a** (done, `9cc7624a58`) | – | `server_capacity` per `server_clock`; budgets ×5 for the deployed tick rate |
| `backoff_retry` (`rpc_with_backoff`) | `backoff_on`, `backoff_off` | 2.98× grows-slowing / 2.98× stops | **b** | a real capacity: `service_time` on `ServerConfig` (as `rpc_retry`) or a `server_clock: Stream<()>` releasing `max_per_tick` | server serves `max_per_tick` per *arrival-driven* tick, so deployed throughput tracks arrival batching |
| `bounded_queue_rejection` (`rpc_with_bounded_queue`) | `bounded_100`, `unbounded` | 2.98× stops / 2.98× stops | **b** | same as `backoff_retry` | same server structure; `rejected` would be a natural `abandoned`-like counter |
| `cache_thundering_herd` (`cache_with_expiry`) | `request_dated_no_coalesce`, `fill_dated_no_coalesce`, `coalesce` | 10.20× / 13.51× / benign | **b** | a key space: `LoadItem for u32` gives every lookup a distinct key, so there are no hits and no herd. Either a `Lookup` newtype with `from_id(id) = id % 10` (changes the signature and the Cress round closure) or a Sedge attribute `load_map = |id| …` | origin capacity is per `origin_clock`, so μ is real; `completed.id` is the arrival index |
| `compaction_falls_behind` (`log_with_compaction`) | `reserve_0`, `reserve_8` | benign / benign (counter-modeled work, off the Cress scorecard) | **a** (done) | `LoadItem for Op` mirroring Cress's `InputValue` (one put per four gets) | the budget *is* released per clock element, so the counter-modeled cost throttles real throughput and Sedge can see what Cress by design cannot |
| `blind/batch_flush` (`batch_flush`) | default | hazardous (collapse exhibited; Cress attribute ignored for time) | **a** (done) | – | sink 5 per 10 ms; flush-after 4 × 50 ms |
| `blind/credit_flow` (`credit_flow`) | default | benign | **a** (done) | – | consumer 5 per 10 ms |
| `blind/log_catchup` (`replicated_log_catchup`) | default | hazardous 39.97× grows-slowing | **a** (done) | `unrelated_work` is a `()` stream, so it becomes a *timer* (steady 100 jobs/s); load is `appends`, completions are `applied` indices | the Cress trigger was unrelated work; Sedge's is appends. μ = leader reads/s |
| `blind/token_bucket` (`token_bucket`) | default | benign | **a** (done) | – | refill 5 per 10 ms |
| `blind/two_phase_commit` (`two_phase_commit`) | default | hazardous 116× grows | **a** (done) | – | participant 20 msgs per 10 ms = 500 tx/s |
| `blind/visibility_queue` (`visibility_queue`) | default | hazardous 155× grows | **a** (done) | – | worker 25 per 50 ms; visibility 6 × 50 ms |
| `blind/bounded_fanout` (`bounded_fanout`) | default | benign | **b** (harness) | `Cluster` support in `overload_check` (deploy N members, read completions from all) | shape is right: publications offered, `processed` at subscribers |
| `lease_renewal_storm` (`lease_renewal`) | `resend_after_4`, `one_outstanding` | 7.00× / benign | **b** (harness) | `Cluster` support; load would be `other_work` at the server (the Cress trigger), completions `served`; the renewals are timer-driven and their acks are the thing that storms | the server is a Process with a clock-released capacity, so μ is real |
| `election_stampede` (`election`) | `uniform_timeouts`, `spread_3` | grows from 0 / grows from 0 | **c** | a notion of offered work that is not a request: the hazard is triggered by a *late heartbeat* and the work is elections. `client_requests` exists but completing one needs a leader, which is the output, not the input | also `Cluster` |
| `gossip_resend` (`gossip_with_resend`) | `resend_on`, `resend_off` | 8.97× degree 2 / benign | **c** | offered work = `local_updates`, completion = "merged at every peer" (N completions per update, from N members); no single completion stream, and the amplification is in re-sends, not request latency | also `Cluster` |
| `rebalancing_ping_pong` (`rebalancing_workers`) | `no_cooldown`, `cooldown_8` | 3.35× / 1.55× | **c** | offered work = `tasks`, completion = `completed` would fit, but the trigger is report *staleness* (a timer period), not load, and the hazard is migrations, which Sedge can only see as a counter | also `Cluster` |
| `crdt_gossip_load` (`g_set_gossip`) | default | benign | **c** | no requests: work per round is a closed form of state size. A Sedge for this would offer *updates* and read *convergence*, which is a different verdict | also `Cluster` |
| `pure_heartbeat_load` (`pure_heartbeat`) | default | benign (Cress port: not explored) | **c** | no input stream at all; work is `n` messages per timer element | trivially stable under any load because it accepts none |
| `transitive_closure_load` (`productive_transitive_closure`) | default | benign | **c** | offered work = `graphs: Stream<Vec<(u32,u32)>>` cannot implement `LoadItem` (orphan rule on `Vec`), and a completion would be "closure of graph `i` emitted", which the program does not number | single `Process<'a>` is otherwise fine |

Tally: 9 programs / 17 configurations in (a), all annotated and run below; 5 programs / 10
configurations in (b), of which 2 programs need a capacity knob, 1 a key space, and 2 only
`Cluster` support in the macro; 6 programs / 9 configurations in (c).

## 3. Results

Sedge defaults: baseline 10 s, trigger 5 s at `2μ`, tail 45 s, grace 2 s, factors 2 and 4, one
trial per factor unless noted (`retry_service` rows are the 2026-10-13 run, two trials each,
copied from `target/sedge_report_2026-10-13/sedge_all.log`). Cress columns are from
`design_docs/cress_port/parity_table.md`. The Cress prediction for a plateau shape with multiplier
`A` is "recovers iff `μ/λ > A`", so `no_governance`-like 2.98× configurations are predicted to
collapse at factor 2 and recover at factor 4, and anything with a multiplier above 4 to collapse at
both.

| program | configuration | Cress verdict | Cress shape / multiplier | Cress prediction (F=2 / F=4) | Sedge F=2 | Sedge F=4 | tail detail |
|---|---|---|---|---|---|---|---|
| `rpc_with_retries` | `three_attempts` | hazardous | stops / 2.98× | collapse / recover | **COLLAPSED** | **DRAINING** | F=2: 0 % goodput, 4695 abandoned, backlog 11580 → 14321. F=4: 92 % goodput, 209 abandoned, backlog 2432 → 560 (falling 163/s, ~3 s from empty) |
| `rpc_with_retries` | `one_attempt` | benign | – | recover / recover | RECOVERED | RECOVERED | 100 % goodput, p50 3 ms vs 3 ms, backlog 0 |
| `retry_service` | `unbounded` | hazardous | grows / 155× | collapse / collapse | COLLAPSED ×2 | COLLAPSED ×2 | 2026-10-13: 0 % / 15–23 % goodput; backlog and pending rising |
| `retry_service` | `no_governance` | hazardous | stops / 2.98× | collapse / recover | COLLAPSED ×2 | DRAINING ×2 | 2026-10-13: F=4 goodput 83–96 %, backlog falling 143–164/s |
| `retry_service` | `no_retries` | benign | – | recover / recover | RECOVERED ×2 | RECOVERED ×2 | 2026-10-13 |
| `retry_service` | `time_budget` | hazardous | stops / 1.56× | recover / recover | RECOVERED ×2 | RECOVERED ×2 | 2026-10-13; zero abandonments |
| `retry_service` | `success_budget` | hazardous | stops / 1.12× | recover / recover | RECOVERED ×2 | RECOVERED ×2 | 2026-10-13 |
| `retry_service` | `hybrid_budget` | hazardous | stops / 1.56× | recover / recover | RECOVERED ×2 | RECOVERED ×2 | 2026-10-13; p50 8–11 ms vs 5–6 ms at F=2 |
| `retry_service` | `circuit_breaker` | hazardous | stops / 1.12× | recover / recover | RECOVERED ×2 | RECOVERED ×2 | 2026-10-13 |
| `two_phase_commit` | default | hazardous | grows / 116× | collapse / collapse | **COLLAPSED** | **COLLAPSED** | 0 % goodput at both; participant backlog 1.58 M → 2.71 M (F=2), 1.08 M → 1.71 M (F=4); pending 8360 → 11024 / 4792 → 5988; baseline p50 5.4 ms |
| `visibility_queue` | default | hazardous | grows / 155× | collapse / collapse | **COLLAPSED** | **COLLAPSED** | 0 % goodput at both; worker backlog 556 k → 917 k (F=2), 393 k → 591 k (F=4); pending 8237 → 10455 / 4806 → 5616; baseline p50 26 ms |
| `replicated_log_catchup` | default | hazardous | grows, slowing / 39.97× | collapse / collapse | **COLLAPSED** | **COLLAPSED** | 0 % goodput at both; leader backlog 78.9 k → 109 k (F=2), 79.1 k → 109 k (F=4); baseline p50 216 ms / 193 ms (the 300 ms poll period), backlog 12 / 0 |
| `batch_flush` | default | hazardous (exhibited collapse; Cress check ignored for time) | – | collapse / collapse | **COLLAPSED** | **COLLAPSED** | 0 % goodput at both; sink backlog 4.33 M → 6.81 M copies (F=2), 3.38 M → 5.15 M (F=4); pending 10.5 k → 13.1 k / 7.0 k → 8.2 k; baseline p50 43 ms / 55 ms |
| `credit_flow` | default | benign | – | recover / recover | RECOVERED | RECOVERED | 100 % goodput; p50 12 ms vs 11 ms (F=2), 10 ms vs 9 ms (F=4); consumer backlog ≤ 2 |
| `token_bucket` | default | benign | – | recover / recover | RECOVERED | RECOVERED | 100 % goodput; p50 0.1 ms vs 0.1 ms; backlog 0 in both windows |
| `log_with_compaction` | `reserve_0` | benign (counter-modeled work) | – | recover / recover | **COLLAPSED** | **COLLAPSED** | 0 % goodput at both; backlog 19.6 k → 23.8 k (F=2), 12.5 k → 13.9 k (F=4); uncompacted segment 842 → 1134 at both; baseline p50 5.3 ms, segment 0 |
| `log_with_compaction` | `reserve_8` | benign (counter-modeled work) | – | recover / recover | RECOVERED | RECOVERED | 100 % goodput; p50 5.3 ms vs 5.3 ms; backlog ≤ 4, segment 0 throughout |

Baselines were healthy in every trial: 100 % completion, no abandonments, gauges at or near zero,
so each verdict is read against a system that was in fact keeping up before the trigger. Wall
clock for the sweep: `rpc_with_retries` (2 configurations, first deploy build included) 9 min;
every later program 2.5–3 min for its two trials, with two programs running concurrently on an
8-core machine.


### Reading the results

- **Every verdict agrees with the corpus label, and with the Cress prediction where Cress has
  one.** The two 2.98× plateau programs (`rpc_with_retries three_attempts`, `retry_service
  no_governance`) collapse at factor 2 and drain at factor 4, as `μ/λ > A` says they should; the
  four programs whose Cress multiplier exceeds 4 (`unbounded` 155×, `visibility_queue` 155×,
  `two_phase_commit` 116×, `replicated_log_catchup` 40×) collapse at both factors; the budgeted
  policies, whose multipliers are below 2, recover at both; and the four benign programs
  (`one_attempt`, `no_retries`, `credit_flow`, `token_bucket`) recover at both with goodput at
  100 % and latency unchanged.
- **`log_with_compaction` is the row Cress cannot produce.** The corpus labels `reserve_0`
  hazardous by exhibited collapse and `reserve_8` as having no ground truth, but both are *off the
  Cress scorecard* because the program models a read's cost as an integer it subtracts from a
  budget rather than as records it moves, and Cress counts records. On a deployment that integer
  budget is released per clock element, so it throttles real throughput: Sedge reads `reserve_0`
  as COLLAPSED at both factors, with the uncompacted segment still growing through the tail (842
  → 1134) and the backlog with it, and `reserve_8` as RECOVERED with the segment at zero. This is
  the clearest instance in the corpus of the two tools being complementary: a work model that is
  invisible in the schedule domain is a capacity in the time domain.
- **`replicated_log_catchup` collapsed under a different trigger than the one its label rests
  on.** The simulator's collapse was induced by unrelated leader work; here `unrelated_work` is a
  steady timer and the trigger is a burst of appends. The mechanism is the same (a full 150-record
  chunk takes six leader ticks to begin answering against a four-tick retry deadline) and the
  outcome is the same, which is some evidence that the label is about the program and not about
  the particular disturbance.
- **`batch_flush` has the largest amplification in the corpus by Sedge's gauges**: 6.8 million
  copies queued at the sink against 14 310 items outstanding, i.e. each unacknowledged item has
  been flushed hundreds of times. Its Cress attribute is `ignore`d because the simulator run takes
  twenty minutes; Sedge produced the verdict in three.
- **DRAINING is doing its job.** Both 2.98× programs at factor 4 ended the tail with goodput of
  83–96 %, a few hundred abandonments and a backlog visibly falling; a tail of 45 s was a few
  seconds too short to call it RECOVERED, and the verdict says so rather than guessing.
- **Scaling from simulator units to wall clock was mechanical.** Every (a) program had a capacity
  released per clock element, so `μ` is `capacity × ticks/s` and the attribute's only judgement
  is the tick period. The one program that needed thought was `log_catchup`, where the bottleneck
  is the leader's reads but the follower's poll period also caps throughput; the attribute's doc
  comment records the arithmetic. `log_with_compaction`'s `μ = 1000` is an estimate (cost per
  operation depends on the segment), which is why its F=2 baseline (`λ = 500`) is the highest in
  the sweep; the verdict does not depend on it.
- One harness nit observed, not affecting verdicts: when the harness closes its external port at
  the end of a trial, the process that was still writing completions (`rpc_retry` Server,
  `visibility_queue` QueueWorker) panics with `BrokenPipe` on its way out. A graceful close of the
  observer ports before teardown would quiet it.

Raw rows for this sweep are in `design_docs/sedge_corpus/results.tsv` (copied from
`target/sedge/results.tsv`; per-second traces stayed under `target/sedge/traces/`).


## 4. What the bucket-(b) programs need

1. **A timer-released capacity** (`backoff_retry`, `bounded_queue_rejection`). Either add a
   `server_clock: Stream<()>` parameter and make the serve budget `clock.count() * max_per_tick`
   (the pattern every blind witness and `retry_service` already use; the Cress attributes then
   list `server_clock = 1` in their workload), or add `service_time: Duration` to `ServerConfig`
   and burn it per request as `rpc_retry` does. The first keeps the simulator's semantics exact
   and is the smaller diff; the second makes capacity depend on the machine, which is the more
   honest deployment but changes every test constant.
2. **A key space for the load** (`cache_thundering_herd`). The cheapest change is in Sedge, not
   the program: an optional `load_map = |id: u64| -> T` in the attribute, used instead of
   `LoadItem::from_id`, so `|id| (id % 10) as Key` reproduces the Cress round closure without a
   newtype. This would also let `token_bucket` offer the tick-stamped payload its Cress check
   does.
3. **`Cluster` locations in `overload_check`** (`bounded_fanout`, `lease_renewal_storm`). The
   macro needs a `members(param = N)` entry, `with_cluster` in the generated deploy, and the
   `Observer` to accept streams at a `Cluster` (merge members' completions; one gauge column per
   member or the sum). `lease_renewal` additionally wants the load to be the server's `other_work`
   while the clients' renewals stay timer-driven, which the contract already allows.

## 5. What Sedge would need for the bucket-(c) programs

These programs do not have a request that is offered and later completes, so "tail goodput
against baseline" has nothing to read. What they share is that *work is triggered by time* (a
heartbeat deadline, a gossip tick, a report period) and *amplified by delay*. A Sedge for them
would:

- **Offer non-request work.** Let `load` be a `()` timer whose *period* is the profile: a
  heartbeat that is late during the trigger, a report tick that is stale. The trigger is then a
  delay injection rather than a rate increase, which is exactly how the Cress labels for
  `election_stampede` and `rebalancing_ping_pong` were produced.
- **Read a program-defined "settled" signal instead of completions.** Leader elected and stable
  (`state_trace`), inboxes drained (`inbox_depth`), migrations zero (`migrated`), set converged
  (`g_set_gossip`'s singleton equal on every member). The present gauge machinery is close: a
  verdict rule of "every gauge back to its baseline band" would serve, with counters (elections,
  re-sends, migrations) as the measured amplification.
- **Accept `Cluster` locations** (as in §4.3), since every bucket-(c) program but
  `transitive_closure_load` is one.
- **Accept a program-supplied `LoadItem`** for foreign types (a `load_map`, as in §4.2), which
  would let `productive_transitive_closure` be offered graphs; it would still need to number its
  outputs by input to have completions.

`pure_heartbeat_load` stays out: it accepts no input, so there is nothing to overload, and its
Cress result ("not explored") is the right one for Sedge too.
