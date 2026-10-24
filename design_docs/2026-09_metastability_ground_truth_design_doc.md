# Ground-Truth Experiments for Metastability Analysis in Hydro

Status: draft

2026-09

## 1. Summary

Hydro's dataflow makes feedback explicit, and its simulator makes many scheduling decisions explicit. This creates an opportunity to reason about metastability and, more generally, about feedback loops that sustain work after a transient trigger has disappeared.

We are not ready to design that analysis yet.

Before adding provenance, work effects, symbolic execution, or a new type discipline, we need a small experimental corpus that establishes the distinction the analysis is supposed to recover:

1. **Productive recursive query processing:** a computation such as transitive closure (TC) may contain a cycle, retain large state, and generate large and data-dependent intermediate results. Nevertheless, after finite logical input it reaches a fixed point and ceases doing work.
2. **A candidate hazardous pattern:** a timeout/retry cycle can create new physical work for an existing logical request in response to non-arrival. This is the program pattern a future analysis should flag. Its presence does **not** by itself establish overload, vulnerability, or metastability.
3. **A collapse experiment:** a separate warm-up/trigger/post-trigger/recovery workload and capacity schedule determines whether a particular configuration of the candidate program actually enters a self-sustaining bad regime.

The first milestone is therefore empirical, not analytical:

- implement the productive computation and candidate hazardous pattern as ordinary Hydro programs;
- independently subject the candidate program to a collapse experiment;
- make at least one configuration exhibit a reproducible self-sustaining failure;
- provide explicit, hand-written ground-truth instrumentation and controls;
- demonstrate stable, vulnerable, metastable, and recovered executions;
- only then ask whether an analysis flags the dangerous pattern and predicts its observed behavior.

This ordering is a hard requirement. We should not build provenance machinery around examples that have not yet exhibited the phenomenon of interest.

## 2. Motivation

The initial intuition is that, in Hydro, **data is data but ticks are work**. Ideally these are fused: a tick runs because new data arrived, and finite input eventually stops inducing work. A problematic feedback loop may instead keep scheduling work while producing no new logical data or retiring no new logical obligation.

This is not equivalent to distinguishing acyclic from cyclic programs, nor low amplification from high amplification. Semi-naive TC is the counterexample:

```text
new paths -> join with edges -> candidate paths -> distinct -> new paths
     ^                                                    |
     +----------------------------------------------------+
```

The cycle can produce a large frontier and a much larger candidate set. Its size is data-dependent and may be difficult to predict statically. Nevertheless, feedback is guarded by novelty: only newly established paths return as the next frontier. Once no new path is found, the recursive work stops.

A retry loop has a different shape at the level of executions:

```text
request -> attempt -> no timely response -> another attempt
                         ^                       |
                         +---- more load --------+
```

Each attempt may be a new physical datum, but it is not necessarily new logical information. The same logical request can repeatedly induce work because of timing or non-arrival. That makes the cycle a candidate hazardous pattern whether or not it ever overloads in a chosen deployment.

Whether the candidate actually produces a metastable failure is a different question. It requires a configured service plant and a workload experiment showing that a finite trigger moves the system into a bad regime which persists after the trigger is removed. The source pattern and the collapse scenario must remain distinct throughout this project.

The eventual analysis may therefore need to ask whether recursive work is repeatedly **funded by fresh logical novelty** or merely **reactivated by operational stimuli and stale lineage**. This document does not commit to a representation for that distinction. It first defines programs and independent ground-truth experiments against which any proposed representation must be judged.

## 3. Literature context

This project draws on several compatible views:

- Bronson et al., *Metastable Failures in Distributed Systems* (HotOS 2021), distinguish a transient trigger from the sustaining effect that persists after trigger removal. They emphasize hidden capacity, vulnerable operating regions, characteristic metrics, and feedback loops such as retries, cache collapse, and slow error handling.
- Huang et al., *Metastable Failures in the Wild* (OSDI 2022), model both workload amplification and capacity degradation and make trigger magnitude and duration explicit.
- Isaacs, Alvaro et al., *Analyzing Metastable Failures* (HotOS 2025), represent latent retry work as a retrial orbit and advocate an escalating toolchain from queueing models through discrete-event simulation and emulation.
- Farahbakhsh, Haeberlen, Lu, Alvisi et al., *Modeling Metastability* (HotNets 2025), formalize persistent history dependence in a finite work-flow model and prove that feedback is necessary but not sufficient.
- Flo gives Hydro a semantics of bounded and unbounded collections, per-iteration progress, and delayed feedback. Flo does not attempt to bound work magnitude across iterations.

The experiments below target the gap between Flo's per-iteration progress and metastability's cross-iteration persistence.

## 4. Research questions

The initial corpus should let us answer, before designing an analysis:

1. Can ordinary Hydro express both a productive fixed-point computation and a timeout/retry cycle that is a candidate hazardous pattern?
2. Can the same coarse structural features---cycles, ticks, state, large buffers, and data-dependent amplification---appear in both programs?
3. Can an independent collapse experiment make the candidate retry program enter a self-sustaining bad regime?
4. What dynamic observations separate the executions without relying on the compiler to recognize a construct named `retry`?
5. Can a triggered execution perform persistently more work than an untriggered execution under the same future logical input?
6. Can we identify a finite trigger prefix, a sustaining suffix or lasso, and an anti-trigger that restores the healthy regime?
7. Which relevant facts are already visible in Hydro IR and simulation, and which are hidden in quoted Rust or runtime behavior?

## 5. Methodological rules

### 5.1 Build phenomena before predictors

Work proceeds in this order:

1. Demo programs: productive TC and the candidate timeout/retry cycle.
2. A separate collapse experiment over the retry program.
3. Reproducible ground truth for stable, vulnerable, metastable, and recovered configurations.
4. Black-box characterization and parameter sweeps.
5. IR inspection and execution tracing.
6. Candidate prediction mechanism.
7. Evaluation against held-out configurations or examples.

No provenance or effect-system implementation belongs in stages 1--4.

### 5.2 Keep the oracle independent of the future analysis

The demos may expose application-specific ground-truth counters because the experiment author knows their semantics. For example, the retry demo may explicitly count unique request IDs, attempts, queued attempts, pending retry timers, and successful logical completions.

These counters are the oracle. They must not be presented as compiler-inferred provenance. A later analysis succeeds only if it predicts behavior using a more generic view of the program and agrees with these counters.

### 5.3 Compare paired collapse experiments

This rule applies to the ground-truth experiment over the retry program, not to the definition of the dangerous pattern itself. Whenever possible, run two executions with the same future logical workload:

- **control:** baseline workload from a healthy initial state;
- **triggered:** a finite trigger prefix followed by the same baseline workload.

The experiment should couple deterministic inputs and record nondeterministic decisions when applicable. Persistent work or low goodput in only the triggered run is stronger evidence than an isolated overloaded trace.

### 5.4 Separate logical output from physical work

Every demo reports at least:

- logical inputs admitted;
- logical outputs or newly established facts;
- physical work items processed;
- tick/operator activations;
- retained or queued state;
- elapsed logical or physical time.

The retry program's experimental oracle additionally distinguishes organic work from internally generated attempts.

## 6. Demo A: productive, high-amplification transitive closure

### 6.1 Program

Implement semi-naive transitive closure over a finite directed graph. Let `known` be all paths established so far and `frontier` be paths established in the previous iteration:

```text
frontier_0 = edges
known_0    = edges

candidates_t = frontier_t join edges
frontier_t+1 = distinct(candidates_t - known_t)
known_t+1    = known_t union frontier_t+1
```

Only `frontier_t+1` crosses the feedback boundary. The implementation should use ordinary Hydro recursion (`Tick::cycle` or `sliced!` state), joins, and explicit novelty suppression. The exact API shape will be chosen during implementation; it is important that this remain a normal Hydro program rather than a special benchmark evaluator.

### 6.2 Input families

Use multiple graph families because no single shape exercises both depth and intermediate amplification:

1. **Chain of length `n`:** produces `Theta(n^2)` reachable pairs over `Theta(n)` recursive iterations, with relatively little duplicate derivation.
2. **Layered diamond graph:** `d` layers of width `w`, with dense edges between adjacent layers. Many paths establish the same reachability facts, generating a large candidate set that `distinct` or `difference` suppresses.
3. **Cyclic strongly connected graph:** ensures graph cycles do not become computation cycles after all reachable pairs are known.

Each family has a small deterministic CI configuration and larger stress configurations.

### 6.3 Ground-truth instrumentation

Instrument the demo itself with counters for:

- input edge count;
- candidate tuples generated per iteration;
- candidate tuples rejected as already known or duplicated;
- newly established reachability facts per iteration;
- cumulative known facts;
- frontier size;
- ticks and operator work;
- peak retained state and peak frontier/candidate buffering;
- final output relation.

The instrumentation is intentionally aware of TC semantics. It is an experimental oracle, not the proposed generic solution.

### 6.4 Required behavior

For every finite input graph:

- the final relation equals a trusted reference transitive closure;
- the frontier eventually becomes empty;
- no further recursive tick/work is enabled after quiescence;
- all feedback after initialization is downstream of a non-empty novelty frontier;
- large stress cases exhibit substantial and data-dependent intermediate work;
- layered cases exhibit finite redundant work, demonstrating that “every work item immediately produces novelty” is too strong a criterion.

The key trace shape is:

```text
logical novelty rate falls to zero
=> recursive work falls to zero after finite drain
```

This program is the principal false-positive test for any future metastability analysis.

## 7. Demo B: candidate hazardous timeout/retry cycle

### 7.1 Program structure

Build a minimal request/response service in Hydro with explicit attempts:

```text
organic requests --> client attempt state --> server queue --> finite service capacity
                           ^                       |
                           |                       v
                           +-- timeout/retry <-- responses
```

Every organic request has a stable logical request ID. Every execution attempt also carries an attempt number. The server has finite processing capacity per round and an explicit queue. A client starts another attempt if the logical request has not completed before its timeout policy fires. Completion retires the logical request and, where modeled, cancels sibling attempts.

The retry mechanism should be authored from ordinary Hydro dataflow and state, not introduced as a new built-in retry combinator for the sake of the experiment.

### 7.2 Why this is the candidate dangerous pattern

The program contains a path on which non-arrival can create another physical attempt for an existing logical request. A future analysis should flag this possibility independently of configured rates, capacities, queue sizes, or trigger schedules.

This section makes **no claim that the program is metastable**. Depending on configuration, the same program may be safely stable, vulnerable to sufficiently strong triggers, or already overloaded. Pattern detection asks whether the source/IR admits operationally reactivated redundant work. Stability classification asks whether a particular dynamical system built from that program has a self-sustaining bad regime.

The distinction produces two separate evaluation targets:

- **Pattern target:** flag Demo B as potentially hazardous while avoiding Demo A as a false positive.
- **Behavior target:** predict which configurations and trigger schedules in section 8 actually cross into self-sustaining collapse.

### 7.3 Two implementations of the candidate program

We should separate deterministic reproducibility from runtime realism.

#### B1. Deterministic logical-time plant

Drive rounds using a simulation input. Each round carries the currently offered organic requests and available service tokens. Deadlines are represented in round numbers. This makes queueing, timeout, retry, cancellation, and capacity exact and reproducible in the existing simulator without depending on wall-clock scheduling.

This is still a Hydro program: attempts flow through queues and feedback state. It is not merely an external recurrence evaluated by the test harness. The harness controls only organic arrivals and capacity inputs.

B1 provides the controlled plant on which the separate collapse experiment runs. It should expose enough state to find and pin a small exact bad-state lasso, but the lasso belongs to an execution/configuration, not to the program definition.

#### B2. Asynchronous wall-clock service

After B1 is established, implement the same program topology using actual asynchronous delays and Hydro's existing timer/timeout facilities or a sidecar service. Run it as an integration or deployment test with finite concurrency and bounded queues.

B2 validates that behavior found by section 8 is not an artifact of round semantics. It need not initially be exhaustive or suitable for CI. Parameters should be derived from B1 and calibrated by measurement.

A future timed Hydro simulator may subsume B2, but building such a simulator is not a prerequisite for the first ground-truth result.

## 8. Ground-truth collapse experiment over Demo B

The warm-up/trigger/post-trigger/recovery scenario is an **experimental input to Demo B**, not part of the dangerous pattern. The analysis target in section 7 exists even in a run that never leaves the healthy regime. Conversely, only the experiment in this section can establish that the stability problem exists for a concrete configuration.

### 8.1 Workload and capacity phases

Every collapse experiment has four explicit phases:

1. **Warm-up:** baseline organic arrival rate below nominal service capacity; system reaches a healthy steady regime.
2. **Trigger:** for a finite duration, either raise organic arrivals or reduce server capacity.
3. **Post-trigger baseline:** restore the exact pre-trigger organic arrival and capacity conditions. This phase establishes whether the bad regime is self-sustaining.
4. **Anti-trigger/recovery:** if the system does not recover, temporarily shed organic load, disable/restrict retry work, drain the queue, or otherwise break the sustaining loop; then restore baseline.

The central ground-truth requirement is that some parameter configuration behaves as follows:

```text
baseline alone: healthy
finite trigger: enters bad regime
trigger removed: bad regime persists
anti-trigger: returns to healthy regime
baseline restored: remains healthy
```

This trace demonstrates a property of **Demo B + configuration + experimental schedule**. It must not be used to redefine the section 7 pattern as “whatever collapsed.”

### 8.2 Ground-truth instrumentation

The experimental oracle reports:

- organic logical requests admitted;
- unique logical requests completed (goodput);
- total attempts created and processed;
- first attempts versus retry/hedged attempts;
- queue depth and age distribution;
- attempts waiting in queues;
- pending timeout/retry events (the retrial orbit);
- expired or post-completion work;
- cancellations;
- timeout and error counts;
- service capacity offered and consumed;
- per-round/tick work;
- latency distribution for logical requests.

Request ID and attempt counters are application-aware ground truth. They are not inputs to the future generic analyzer.

### 8.3 Classification oracle

For the deterministic plant, define explicit healthy and bad sets using queue, orbit, and goodput over a window. Exact thresholds are benchmark configuration, not universal definitions.

A run is **healthy** when queue and orbit remain in their low basin and logical goodput keeps pace with baseline organic input.

A configured system is **vulnerable** when baseline alone stays healthy but at least one finite trigger in the declared trigger family reaches the bad set.

A run exhibits **metastable behavior** when:

1. it reaches the bad set under a finite trigger;
2. organic arrival and capacity return to the exact warm-up values;
3. it remains in the bad set for the declared recovery horizon or enters an exact repeated state/lasso;
4. the paired control run remains healthy under the same post-trigger logical workload; and
5. an anti-trigger that breaks the sustaining feedback returns it to the healthy set.

For a finite deterministic abstraction, prefer the stronger exact witness: a post-trigger state cycle with low goodput and no continuing trigger. For asynchronous experiments, use an operational persistence horizon and repeat the run enough times to estimate the transition probability.

### 8.4 Parameter search

Do not assume the first configuration of Demo B will be metastable. Sweep:

- baseline organic arrival rate;
- service capacity and worker concurrency;
- queue capacity and discipline;
- timeout;
- retry delay/backoff;
- maximum attempts or retry budget;
- cancellation semantics;
- trigger magnitude;
- trigger duration.

The result should include a small phase diagram or table identifying:

- stable configurations of the same candidate program;
- vulnerable configurations;
- self-sustaining executions;
- recovery thresholds.

Pin at least one representative from each category as a regression test. The fact that a single source program spans several regimes is important evidence that pattern detection and collapse prediction are different tasks.

## 9. Optional Demo C: cache-collapse feedback

A second operational example is desirable after the retry storm works. A look-aside cache demo would distinguish retry-specific behavior from the broader class:

```text
cache miss -> backend request -> backend queue/latency -> failed or delayed fill
     ^                                                   |
     +---------------- low hit rate ---------------------+
```

A finite cache invalidation or cold-start trigger should push a vulnerable configuration into a persistent low-hit-rate/high-backend-load regime. An anti-trigger such as admission reduction or protected cache-fill priority should recover it.

This demo is deferred until section 8 makes Demo B reproducibly self-sustaining under at least one configuration. It should not delay the first analysis experiment.

## 10. Initial comparative hypotheses

These are hypotheses to evaluate against the corpus, not baked-in definitions.

### H1: Work coupled to data novelty

In productive TC, work may be large and partially redundant, but finite logical input produces a finite sequence of novelty frontiers. Once the final novelty frontier drains, recursive work stops.

### H2: Stale-lineage reactivation

In the retry demo, a stable logical request may generate multiple physical attempts. A post-trigger suffix may continue generating attempts from old logical obligations plus operational events, without proportional logical completions.

### H3: Paired differential work

After future logical inputs are coupled, the control and triggered TC executions should coalesce in both logical state and work after finite drain. In a metastable retry configuration, the logical workloads may be identical while queue/orbit/work states and goodput remain different.

### H4: Redundancy is amortized, not forbidden

A useful predictor cannot reject every work item that fails to create novelty. Layered TC deliberately generates duplicate candidates. The candidate distinction is whether an unbounded or persistent suffix of work can occur after logical novelty has stopped, not whether every individual firing is productive.

### H5: Ticks are a useful first work measure, but not sufficient

Tick executions expose repeated work episodes in Hydro, but a single tick can process an arbitrarily large batch and top-level operators also perform work. Results should therefore report both tick activations and item/operator work.

## 11. What we will inspect after ground truth exists

Only after the acceptance criteria in section 13 are satisfied will we compare the demos at three levels.

### 11.1 Source and types

Record which distinctions are visible in collection types, locations, boundedness, ordering, and retry cardinality.

### 11.2 Hydro IR

Inspect concrete nodes and cycles, including `CycleSource`, `DeferTick`, joins, `Unique`, network boundaries, sources, and stateful operators. Determine which operational facts were erased into quoted expressions.

### 11.3 Execution traces

Record tick causes, input releases, feedback traversal, state changes, and operator/item counts. If necessary, add observation-only tracing that does not yet attempt to infer provenance.

This comparison will tell us whether the eventual predictor should be static, symbolic, concolic, dynamic, or hybrid.

## 12. Deferred analysis directions

The following ideas motivated the corpus but are explicitly out of scope until the demos work:

- shadow lineage or provenance DAGs;
- logical versus operational novelty classes;
- generational freshness across feedback boundaries;
- novelty gates and finite-fuel gates;
- redundant-work lasso detection;
- paired symbolic or concolic execution;
- work/resource effect systems;
- static amplification matrices;
- new IR metadata or semantic regions;
- metastability-related type parameters;
- automatic safe-region certificates.

The demos should constrain these designs. We should be willing to discard any proposed mechanism that cannot distinguish Demo A from Demo B or predict B's phase boundary.

## 13. Milestones and acceptance criteria

### M0: Specify programs, experiment, and metrics

- Fix small and stress graph families for TC.
- Specify the candidate retry-cycle program independently of any trigger schedule.
- Fix the collapse experiment's phase protocol and metric definitions.
- Implement a machine-readable trace schema shared where possible across demos.

### M1: Productive TC ground truth

- Hydro TC matches a trusted reference implementation.
- At least one case has large intermediate amplification and duplicate candidate work.
- Finite input always reaches quiescence in the tested configurations.
- The trace shows frontier novelty falling to zero followed by zero recursive work.

### M2: Candidate dangerous pattern

- Implement logical-time request attempts, finite service, timeouts, and the retry cycle as Hydro dataflow.
- Show directly that non-arrival can create another physical attempt for an existing logical request.
- Document the source and IR shape without claiming that the program is metastable.
- Establish the future analysis target: flag Demo B as a candidate while not flagging Demo A solely for having a cycle and amplification.

### M3: Deterministic collapse ground truth

- Run the independently specified warm-up/trigger/post-trigger/recovery experiment over B1.
- Exhibit a healthy control run of the same candidate program.
- Exhibit a finite trigger that enters a self-sustaining bad state after trigger removal.
- Prefer an exact post-trigger lasso; otherwise define and justify a bounded persistence witness.
- Demonstrate recovery via an explicit anti-trigger.
- Pin stable, vulnerable, metastable, and recovered configurations/traces.

### M4: Asynchronous reproduction

- Reproduce the qualitative transition with B2 using actual timers and finite runtime resources.
- Measure run-to-run variability and calibrate against the deterministic model.
- Document simulator/runtime gaps revealed by disagreement.

### M5: Prediction experiment

Only now:

- hide the application-aware oracle counters from the candidate analyzer;
- test whether it flags the dangerous timeout/retry pattern while avoiding productive TC;
- separately test whether it predicts stable versus vulnerable configurations and collapse boundaries;
- report pattern false positives/negatives separately from behavioral prediction errors and unknowns.

## 14. Risks

### 14.1 A recurrence masquerading as a system

A deterministic queue recurrence can trivially be made bistable. B1 must therefore model actual Hydro data movement: distinct attempt records, queue state, timeout events, capacity tokens, responses, and feedback. The test harness must not compute the transition relation on behalf of the program.

### 14.2 Defining the answer into the instrumentation

Application-aware counters are necessary for ground truth, but later analysis must not consume labels such as `is_retry` or `is_novel`. Preserve a raw trace view so the prediction experiment can be evaluated honestly.

### 14.3 TC is not automatically benign

Large TC state may trigger memory pressure, garbage collection, or capacity degradation in a real runtime. Demo A establishes productive logical recursion, not unconditional operational safety. If its resource use participates in a sustaining resource loop, that is a legitimate additional phenomenon and should be modeled separately.

### 14.4 Bounded retries can still be metastable

Finite attempts per request establish neither global stability nor recovery. Continuing baseline requests can continuously seed bounded retry families, and the aggregate retry workload can sustain overload.

### 14.5 Wall-clock tests can be flaky

The deterministic demo is the regression oracle. Asynchronous reproduction is corroborating evidence and should use generous timescale separation, repeated trials, and recorded traces.

## 15. Immediate next steps

1. Implement Demo A with the chain and layered-diamond inputs.
2. Implement B1's timeout/retry cycle without a trigger schedule and verify that non-arrival can re-attempt an existing logical request.
3. Build the warm-up/trigger/post-trigger/recovery driver as a separate experiment over B1.
4. Sweep experiment parameters until stable, vulnerable, and self-sustaining configurations are all observed.
5. Review the programs and traces before making any provenance or IR changes.
6. Decide whether B2 can use existing timer/timeout APIs or needs a small sidecar/runtime harness.
