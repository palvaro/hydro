# Cress and Sedge in concert

Cress (`cress/`, `scripts/cress`, branch `cress-corpus`) runs a Hydro program in the simulator, holds one decision point at a time for 1, 2, 4, …, 512 rounds and to the end of the run, and counts the work the program does beyond the unheld run. Sedge (`sedge/`, `scripts/sedge`, branch `feat/sedge-overload-harness`) deploys the same program on localhost, offers it Poisson arrivals at λ = µ/f, drives a 5 s trigger at 1000 req/s through it, and reads the tail of the run against the pre-trigger baseline. One program has been run under both: `retry_service` in `hydro_test/src/cluster/witnesses/blind_budget/service.rs`, in seven retry-governance configurations, with the results collected in `target/sedge_report_2026-10-13/retry_service_report.pdf` and the 28 per-second traces under `traces/` beside it.

This document says what each tool measures and what it cannot, reads the seven configurations against a rate model that both tools turn out to be measuring from different sides, and proposes the changes that would make them one suite: Cress producing a cheap bound on amplification, Sedge measuring the critical headroom expensively, and one claim, that the two agree, that the corpus can test. It does not run anything new; a sibling document, `2026-10_sedge_on_corpus.md`, surveys which corpus programs Sedge can run at all.

## 1. What each tool measures

### Cress

Cress fixes the input and varies one scheduling decision. The unheld run follows the prompt schedule (every batch admits everything waiting, every snapshot sees the newest version) and discovers every `use::batch` or `use::snapshot` that ever had something buffered; for `retry_service` that is five points in `service.rs`: the client's clock (504), the client's new requests (505), replies arriving at the client (506), the server's clock (539), and requests arriving at the server (540). Each is then held for each length in the doubling sequence, and two counts are read against the unheld run: records admitted through batch points, summed over the program, and network messages sent, kept per sender. A hold delays records and cannot create them, so a positive difference is work the program derived from being delayed.

The attribute for these seven checks confines the data to the first 125 of 1000 rounds (`workload_rounds = 125`, two requests per round, one element per round into each clock), so the unheld run is 250 requests, 500 messages, 250 of them the client's. Three things are reported on top of the per-point curves, and it matters exactly what each is (`cress/src/check.rs`):

- The **verdict** is hazardous if any count rose above the unheld run under any hold of any point, at any length including the hold to the end of the run (`verdict_of`). Nothing about the size or the shape of the rise enters.
- The **multiplier** is `(reference + worst) / reference` for one count under one point: `worst` is the largest extra over every hold length including the one that never ends, and the label takes the largest such ratio over every reacting point and every count, admitted records and each sender's sends alike (`SeriesGrowth::multiplier`, `growth_label`). In all seven reports the winning count is the client's sends (`Process(loc2v1)`, reference 250). `no_governance` reads 2.98× because the client sent 496 extra messages under a 512-round hold of the server's arrivals: two re-sends for each of 250 requests, less the last few that the run's end cut short.
- The **shape** is fitted only over the finite holds that outlast the input, here 128, 256 and 512 (`Growth::fit`). It reads `stops` when the last interval rises by at most one record, `grows` otherwise (qualified `slowing` when the marginal rate falls across the doubling and with a degree when it at least doubles), `shedding` when every value is at or below zero, and it is taken from the worst point. Six of the seven configurations read `stops`; `unbounded` reads `grows` at 34.5 extra sends per round of delay.

A check costs 18 to 61 s including compilation, most of it in the long holds of the configurations that storm.

### Sedge

Sedge fixes the schedule, which is now the operating system's, and varies the load. The attribute deploys the client and server as processes, feeds the stream named `load` from an open-loop generator, and gives each `()` stream a wall-clock period: here a 50 ms client clock, so the 40-tick timeout is 2 s, and a 10 ms server clock serving 5 per tick, so µ = 500 req/s. A trial runs 10 s at λ = µ/f, 5 s at 1000 req/s, and 45 s at λ again. The verdict (`sedge/src/verdict.rs`, `judge`) compares the tail window [37 s, 60 s) with the baseline window [5 s, 10 s), both read by the time requests were *offered*, so a window's completion fraction and latency describe what a request offered then experienced:

- **RECOVERED** if the tail completes at least 95% of the baseline's fraction, abandons nothing, has a median latency within 5× the baseline's plus 20 ms, and no program-reported gauge rises (second-half mean above 1.05× the first-half mean plus one).
- **COLLAPSED** if the tail completes at most half the baseline's fraction, or a gauge rises while goodput is below 90% or the median latency is out of slack.
- **DRAINING** if a gauge is falling and none rising, whatever the goodput, and otherwise.

The `Observed` impl for `RetryOutputs` reports `backlog` (server queue) and `pending` (client table) as gauges and `sent`, `retried`, `served` and `denied` as counters, so the traces carry the program's own view of the storm. A trial is 60 s plus deployment; the report's 28 trials took about half an hour.

### The seven configurations side by side

| configuration | Cress verdict, shape, multiplier | sends/offered, whole run, f=2 / f=4 | Sedge f=2 | Sedge f=4 |
|---|---|---|---|---|
| `no_retries` | benign | 1.00 / 1.00 | RECOVERED | RECOVERED |
| `circuit_breaker` | hazardous, stops, 1.12× | 0.90 / 0.87 | RECOVERED | RECOVERED |
| `success_budget` | hazardous, stops, 1.12× | 1.06 / 1.07 | RECOVERED | RECOVERED |
| `time_budget` | hazardous, stops, 1.56× | 1.08 / 1.08 | RECOVERED | RECOVERED |
| `hybrid_budget` | hazardous, stops, 1.56× | 1.27–1.30 / 1.18–1.19 | RECOVERED | RECOVERED |
| `no_governance` | hazardous, stops, 2.98× | 2.38–2.39 / 2.04–2.15 | COLLAPSED | DRAINING |
| `unbounded` | hazardous, grows, 155.42× | 5.87–6.03 / 4.03–4.29 | COLLAPSED | COLLAPSED |

The sends/offered column is `sent` summed over the trace divided by `offered`, both trials. Every Sedge cell is two trials with the same verdict.

## 2. What the data says

**The hazardous verdict is a statement that the program retries.** Six of seven configurations are hazardous; two of those six collapse at any headroom tried. As a predictor of deployed collapse the gate has precision 2/6, and the overview document on `cress-corpus` already says why: the rule "any extra work at any hold" fires on the existence of a mechanism, and a retry policy is a mechanism. The verdict should carry no weight in the suite; the numbers beside it should.

**`grows` is the one signal that is right on its own.** It picks out `unbounded`, the only configuration that collapsed at both headrooms, and nothing else. Its basis is thin, since the shape is fitted on three points (128, 256, 512), but the thing it detects, extra work per round of delay with no ceiling, is the right thing.

**The multiplier works as a worst-case bound against the headroom factor.** "Collapse iff A* > f" is correct on all 14 runs: `unbounded` (155×) collapses at 2 and 4, `no_governance` (2.98×) collapses at 2 and drains at 4, and every configuration at or below 1.56× recovers at both. The rule is doing real work on `no_governance`, where it places the critical factor between the two headrooms tried. But A* is the configuration's constants read back: `max_attempts = 3` gives 2.98, and the 1.56 of the two clock-refilled budgets is, as section 3 shows, the refill rate divided by the simulator's request rate. It is also conservative against what was measured: whole-run sends/offered reached 2.38 where A* says 2.98, and 1.08 where A* says 1.56. And the evidence is seven configurations at two headrooms.

**Cress does not see what the deployed runs show most clearly.** Three cases:

- *Shedding.* `circuit_breaker` and `success_budget` both read 1.12×. Deployed, the breaker opens at t = 13 s, rejects every offered request for about 3.5 s (1811 and 1833 abandoned of 18547 and 18814 offered at f = 2, 9.8%; 12.8% at f = 4), lets the server drain its 1900-deep queue at 500/s, and is back at baseline latency by t = 17 s, the fastest recovery of any configuration. The success budget sheds nothing and takes until t = 28–30 s. Cress's own curves know this: under the breaker every point beyond a hold of 8 rounds is negative, −189 to −437 at the end, and the shape fitter prints "shedding, not amplifying" at three of four reacting points. The headline multiplier comes from a +31 at the one point whose dump of held requests slips under the breaker's threshold, and the verdict throws the rest away.
- *Dynamics.* `time_budget` and `hybrid_budget` tie at 1.56×, with identical curves at 506, 539 and 540. Deployed at f = 2 they are not alike: `time_budget`'s backlog is gone by t = 30–31 s; `hybrid_budget`'s lasts to t = 44–47 s, its tail window opens with a mean backlog of 459 and 835 and a p99 of 2242 and 2755 ms, and the sustained retry rate through the storm is 172–225/s against `time_budget`'s 68–100/s. The one place the two Cress reports differ is the client's new-request point (505), 196 against 122 extra records at a hold of 64, where replies keep flowing and the hybrid's reply-refilled half shows; the headline is the maximum over points and discards it.
- *A refill that a hold cannot exercise.* `success_budget` reads 1.12× in the simulator and retries at 100/s in the deployment, for a reason section 3 opens with.

**Sedge is thin where the margin question lives.** Two seeds, two headroom factors, one trigger shape. The verdict reads the median latency only, which is how `hybrid_budget` at f = 2 earns the same RECOVERED as the breaker with a 2.7 s p99 and a half-full tail window. The 45 s tail is barely long enough: `no_governance` at f = 4 empties its backlog at t = 53 and 58 s and is read DRAINING, which is correct about the window and uninformative about the program. And a verdict at f = 2 and f = 4 brackets `no_governance`'s critical factor to a factor of two and says nothing about where the budgets' critical factors lie, except that they are below 2.

## 3. One model, measured from two sides

The sharpest fact in the data is the one `success_budget` exposes. Cress reads it at 1.12×, and the overview on `cress-corpus` argues it is benign in principle: when replies stop, refills stop, so a stall can provoke at most a bucket's worth of re-sends. Deployed, replies never stop. The server is saturated at µ = 500/s and every reply is late, but each one refills a fifth of a token, so the client retries at 89–100/s through t = 14–19 s at f = 2, the same 100/s as the clock-refilled budget. Cress's perturbation is delay; the deployed failure is slowness. A hold-then-release represents a partition or a pause and cannot represent "delivered, late, at rate µ", and so it cannot see a refill that is driven by late deliveries. That is not a defect in the counting; it is a perturbation the simulator has not been asked to make, and section 4 asks for it.

The rest of the data reads with one equation. Once the server's queueing delay exceeds the client's timeout, every request offered at rate λ is re-sent at some rate r on top of its first send, so the server sees λ + r. The policy fixes r:

- A plain attempt cap A gives r = (A − 1)·λ: `no_governance` at f = 4 retries at 249–254/s against λ = 125/s, exactly 2λ.
- A clock-refilled budget gives r = b, the refill rate, independent of λ: `time_budget` retries at 100/s (5 tokens per 50 ms tick) while tokens are the constraint.
- A reply-refilled budget with one token per k replies gives r = µ/k while the server is saturated, because the server is completing at µ whether or not the completions are useful: `success_budget` retries at 89–100/s = 500/5.
- The hybrid gives the sum, 100 + 500/4 = 225/s; the trace reads 224–226/s at t = 14–16 s and 172 after completions fall.
- A breaker gives r = 0 and also removes λ for `open_ticks`; `unbounded` gives r = pending/timeout, which grows with pending.

In general r = min((A − 1)·λ, b) with A the cap and b the rate the policy's refill allows. The overloaded state sustains itself iff λ + r ≥ µ, and otherwise drains at m = µ − λ − r per second. The drain times this predicts are the observed ones: `time_budget` f = 2, 2573 of backlog at m ≈ 170/s is 15 s, and the backlog is empty 15–16 s after the trigger; `hybrid_budget` f = 2, 2705 at m ≈ 77/s is 35 s, observed 29–32 s as r falls with completions; `no_governance` f = 4, 5361 at m = 125/s is 43 s, observed 38–43 s; `no_retries` f = 2, 2423 at m = 250/s is 9.7 s, observed 9–10 s.

Setting λ + r = µ gives the critical headroom f_c = µ/λ_c:

| configuration | r in the overloaded state | f_c predicted | observed |
|---|---|---|---|
| `no_retries`, `circuit_breaker` | 0 | 1 | recovers at 2, 4 |
| `time_budget` | b = 100/s | µ/(µ − b) = 1.25 | recovers at 2, 4 |
| `success_budget` | µ/k = 100/s | k/(k − 1) = 1.25 | recovers at 2, 4 |
| `hybrid_budget` | b + µ/k = 225/s | µ/(µ − b − µ/k) = 1.82 | recovers at 2 with a 30 s drain, at 4 cleanly |
| `no_governance` | 2λ | A = 3 | collapses at 2, drains at 4 |
| `unbounded` | grows with pending | ∞ | collapses at 2, 4 |

The hybrid's slow recovery at f = 2 is what sitting 10% above one's critical factor looks like. Nothing in the two headroom factors tried contradicts the table; nothing in them confirms the three predictions between 1 and 2 either, which is the gap a sweep fills.

What Cress's multiplier is, in these terms: for a cap-type policy it is A, read from the plateau of extra sends per request, and it transfers to the deployment unchanged. For a rate-type policy it is 1 + b·T/(λ_sim·T) = 1 + b/λ_sim, the refill rate over the simulator's request rate: `time_budget`'s +139 sends is one token per tick for the 139 ticks during which requests were pending, over 250 requests, so 1.56. That number depends on the operating point the check was run at, λ_sim = 2 per round against µ_sim = 5 per round (f_sim = 2.5), which is neither of the two Sedge ran. At the deployed λ it would be 1 + 100/250 = 1.4 at f = 2 and 1.8 at f = 4; and 1 + b/λ > µ/λ is the same inequality as λ + b > µ, so "A* > f" is the right rule provided A* is evaluated at the deployed λ, which it was not. For the reply-refilled budget Cress reads b ≈ 0 because the hold stopped the replies, where the deployment has b = µ/k. The multiplier is the right kind of number and the wrong single number: the program has two constants, A and b, and one check at one rate can only report their combination.

The suite's claim is therefore: Cress reports A and b per sender and decision point from the simulator, cheaply; Sedge measures f_c from deployment, expensively; and f_c = min(A, µ/(µ − b)) within the resolution of the sweep, under the assumptions that the deployed server behaves as a single work-conserving queue at capacity µ, that the trigger is large enough to push its delay past the timeout, and that the policy's refill in the overloaded state is what the simulator can provoke. Where a program falls off that line, one of those assumptions is false of it, and that is the finding.

## 4. Changes

### Cress

1. **Replace the gate.** The verdict compares against a declared headroom rather than against zero. The attribute takes `headroom = f` (or inherits µ and f from the deployment half of a shared attribute, below), and the verdict is *within headroom* when min(A, µ/(µ − b)) < f by a margin to be chosen, *at the edge* when it is within that margin, and *beyond headroom* otherwise. `grows` on any sender's sends is a failure regardless of f, since it is A = ∞. `not explored` stays as it is.
2. **Report A and b separately.** The attribute already accepts rate lists (`workload(requests = [1, 2, 4])`). Running two data rates with the same `workload_rounds` and fitting the per-sender plateau as extra = (A − 1)·N + b·T, with N the requests and T the rounds during which requests were pending, separates a per-request cap from a per-round rate; a `stops` ceiling that scales with N is A, one that does not is b·T. Report both with the line each was read at, and the stall reaction (the hold that never ends) as a check on A.
3. **Add a throttle beside the hold.** A batch decision already decides how many waiting records to admit; a throttle admits a prefix of at most c per round for k rounds rather than none. Holding the server's arrivals models a pause; throttling them to c < λ_sim models a server that is slow and still replying, which is the deployed failure. Under a throttle the reply-refilled budget refills at c/k and its b becomes visible. This is the one change here that touches `cress/src/schedule.rs` rather than the report.
4. **Stop discarding what the curves know.** The one-line summary gains a shedding reading (the breaker's −189 to −437) and a per-point disagreement note when the worst point's shape or multiplier differs from the others' (the hybrid's 505). Multipliers are kept per sender, so a program in which one member's traffic rises while another's falls is read as such.
5. **Run at the deployment's operating point.** When the attribute carries µ and f, the check's workload is derived from them: `server_clock` fed at the server-to-client tick ratio per round (5 here) and `requests` at µ_sim/f. Today the simulator checks at f_sim = 2.5 and the deployment runs at 2 and 4; the 1.56 was never comparable to either.

### Sedge

1. **A headroom sweep.** `--sweep lo,hi` runs a coarse grid (1.25, 1.5, 2, 3, 4) at n seeds, then bisects between the largest collapsing and smallest recovering factor on a majority-of-seeds verdict, to a chosen resolution. f_c is reported as an interval. At one minute a trial, a sweep to a quarter of a factor with three seeds is about 15 minutes per configuration.
2. **Measure r and m from the trace.** The DRAINING branch already computes the backlog's slope; make it a reported quantity for every verdict, together with the sustained retry rate in the overloaded phase (`retried` per second while `backlog` exceeds the baseline) and sends/offered over that phase. From one trial that entered the overloaded state, m = µ − λ − r is a measurement and λ_c = µ − r (rate-type) or µ/A (cap-type) is a one-run estimate of f_c to check the sweep against.
3. **Read the tail's latency and duration, not only its median.** RECOVERED gains a p99 bound and a *time to recover*, the first second after the trigger from which the backlog stays at baseline; `hybrid_budget` f = 2 is RECOVERED in 29–32 s and `circuit_breaker` in 2 s, and the verdict should say so. A tail shorter than the estimated drain time should be flagged, and `--tail` scaled from a first trial's m.
4. **More seeds**, three at least, and the verdict per factor as a count, which the summary table already prints.

### Shared

1. **One attribute over one configuration.** Today `service.rs` carries seven `amplification_check` and seven `overload_check` attributes with separately written constants: timeout 6 against 40 ticks, budgets 10/1 against 50/5, `open_ticks` 12 against 80. The ×5 is the server-to-client tick ratio the simulator does not know about, and the 6-versus-40 is a different program. One attribute replaces each pair:

   ```rust
   #[cfg_attr(feature = "metastable", metastable::check(
       name = time_budget,
       config = RetryConfig { timeout_ticks: 40, server_capacity: 5, max_attempts: 3,
                              policy: Policy::TimeBudget { initial_tokens: 50, capacity: 50, refill_per_tick: 5 } },
       load = requests,                       // the stream the deployment's generator feeds and the simulator's data stream
       timers(client_clock = Duration::from_millis(50), server_clock = Duration::from_millis(10)),
       mu = 500,                              // req/s the deployment can sustain
       headroom = 2,                          // the f the program is meant to run at; the verdict is read against it
       sim(workload_rounds = 125, horizon = 1000),   // simulator-only settings
       deploy(trigger_multiple = 2, sweep = (1.25, 4), seeds = 3),  // deployment-only settings
   ))]
   ```

   Plain parameters (`config`), `load`, `timers`, `mu` and `headroom` are shared; `sim(...)` and `deploy(...)` carry what only one side needs, and are the only places a constant may appear once. The simulator's workload is derived, not written: a round is one element of the longest-period timer among the stream parameters (the 50 ms client clock, so a hold of k rounds is k × 50 ms and the 40-tick timeout is 40 rounds), every shorter timer is fed its period ratio of elements per round (the server clock 5, which is the ×5 the report had to apply by hand), and the `load` stream is fed at mu × round period / f per round (500 × 0.05 / 2 = 12.5 requests per round at f = 2, realised as the pair of rates [10, 15] that the (A, b) fit under Cress above needs anyway). Cress's macro today treats any stream named in `workload(...)` as data that stops after `workload_rounds`, so a timer rate needs a clause of its own; that is the one macro change this derivation requires. With it the simulator runs the deployed program at the deployed operating point with the deployed constants. The two macros already share their signature analysis (`sedge_macro` is adapted from `cress_config_macro`); the new one expands to both harnesses under their features and rejects a program either side cannot handle (clusters, keyed streams, a second data stream) with the same message both give today.
2. **One runner.** `scripts/metastable <crate> [function]` generates one runner crate with both features, runs Cress first, then Sedge at the factors Cress's A and b make interesting (around the predicted f_c, plus the declared headroom), and prints one table: program, configuration, A, b, shape, location, predicted f_c, measured f_c interval, r, m, time to recover, seeds.
3. **One record.** Both tools write TSV under `target/`; a common row schema with the fields above, one row per (program, configuration), and per-trial and per-hold detail beneath it, so the corpus evaluation in section 5 is a join and a plot rather than a transcription.

## 5. Evaluation on the corpus

The corpus on `cress-corpus` (`hydro_test/src/cluster/witnesses/mod.rs`, 21 labeled configurations plus `rpc_retry`, every one with a Cress attribute and a multiplier in `design_docs/cress_port/parity_table.md`) gives the suite about 25 points instead of 7. For every (program, configuration): Cress reports A, b and shape at the deployment's operating point; Sedge sweeps f_c; one point is plotted at (predicted f_c, measured f_c interval). *Agreement* is the interval containing the prediction, or both being at the bound (f_c = 1 for programs that shed or never re-send; ∞ for `grows`). *Disagreement* is anything else, and each one is read back to which of section 3's assumptions failed.

The retry family is the first test, because Cress reads the same 2.98× for `rpc_with_retries three_attempts`, `rpc_with_backoff` with and without backoff, and `rpc_with_bounded_queue` with and without the bound. If all four have f_c ≈ 3 the cap is the whole story and backoff and bounding are, as their corpus notes say, changes to the shape of the transient and not to the threshold. If `backoff_on` or `bounded_100` lands elsewhere, that is the first measurement of what the multiplier leaves out. Two of the four are not yet runnable: `2026-10_sedge_on_corpus.md` found that `rpc_with_backoff` and `rpc_with_bounded_queue` tick their server on arrivals (`max_per_tick` per arrival-driven tick), so on a deployment they have no capacity until they are given a server clock or a service time; only `rpc_with_retries` and `retry_service no_governance` have been measured, and both land between 2 and 4 as the cap predicts. `cache_with_expiry` (13.51× and 10.20×, `stops`) and `rebalancing_workers` (3.35× and 1.55×, `stops`) are the next, because their A is not a retry cap and the model's "requests offered" and "server capacity" need translating.

### Programs without a request/response shape

Sedge's generator feeds one `load` stream and reads completions and abandonments by id, which fits the four retry services and the cache and not, as written, gossip, election, lease renewal, rebalancing or the heartbeat. What "offered work" and "goodput" mean for each, which can be run with a different `Observed` impl and which need the harness to drive a timer or a membership change instead of a request stream, is the subject of `2026-10_sedge_on_corpus.md`; this section is a placeholder for its conclusions.

## 6. Open questions

**Is A* independent of the workload?** No, and section 3 says how: A is, b is not, and the multiplier is a combination of them at the rate the check happened to run. Reporting them separately is the fix for the policies seen so far. Whether every program decomposes into a per-request and a per-round constant is not known; `unbounded`'s r = pending/timeout is neither, and reads as `grows` only because pending grows.

**What is a hold in wall-clock terms?** Today nothing says. If a round is taken as one client tick, a hold of k rounds is k × 50 ms, and the first reaction at 8 rounds is 400 ms against a 6-tick timeout of 300 ms; the deployment's timeout is 2 s. The shared attribute removes the discrepancy in constants but not the question of what the simulator's round should be when the two clocks differ. Feeding the server clock at the tick ratio makes a round the client tick and makes the simulator's µ 25 per round; it does not make the simulator's prompt schedule look like a Poisson arrival process, and the first reaction will stay a function of the deterministic phase of two timers.

**Is the headroom factor the right single parameter?** It decides whether the overloaded state sustains itself; it does not decide whether the program gets there. Every configuration builds the same 2400–2900 backlog from the same 5 s trigger, a queueing delay of about 5 s against a 2 s timeout, and `unbounded` collapses at any f once there. A smaller trigger that leaves the queue under 2 s deep would show every policy recovering at f = 2, and a program's exposure is the pair (how large a transient tips it, how much headroom keeps it tipped). The sweep in section 4 varies the second; the trigger multiple should be swept too, at least at the predicted f_c.

**What does Sedge's verdict mean near f_c?** Arrivals are Poisson and the server is real, so near the threshold the verdict is a probability, and two seeds cannot estimate one. The sweep reports an interval; the proposal to read r and m from every trace gives a continuous quantity that does not need many seeds to be informative, and the two should be reported together until it is clear which is the better estimator of f_c.

**What does "benign" mean once the gate is a threshold?** `no_retries` has A = 1, b = 0 and f_c = 1; `circuit_breaker` has the same f_c and sheds 10% of its traffic to get it. A verdict that says both are within headroom at f = 2 is right and incomplete. The reading the suite owes the engineer is the pair (f_c, cost at the declared headroom), where the cost is shedding, time to recover, or tail latency, and the vocabulary for the second half is not yet fixed.
