# Stage-Level Dynamic Attribution for Feedback Hazards

Status: research checkpoint

This document describes the smallest methodology currently supported by evidence in Hydro. It is intentionally narrower than a general metastability analyzer.

## 1. Research question

Can Hydro distinguish fixed operational work from history-dependent feedback work using measurements at DFIR stage boundaries, without tuple-level lineage and without asking users to label hazards?

The current answer is provisionally **yes** for the heartbeat and timeout/retry ground-truth programs.

The method does not try to prove that a program is metastable from source code. It combines automatically derived graph structure with paired runtime measurements and asks whether a finite trigger changes the program's future work trajectory after external conditions return to baseline.

## 2. Core hypothesis

Consider two executions of the same program:

- **control:** remains under the baseline workload;
- **triggered:** receives a finite perturbation and then returns to exactly the same future positive and operational inputs as the control.

The hypothesis is:

> A feedback hazard can manifest as persistent divergence in stage-level physical work under identical future inputs.

This separates two demonstrated patterns.

### Pure heartbeat

```text
interval -> fixed heartbeat -> bounded fan-out
```

Each operational activation produces a fixed, membership-bounded amount of work. There is no acknowledgement path, retained outstanding-work population, or feedback dependency governing later heartbeat emission. After the timer schedules are coupled, control and triggered executions have the same gain.

### Timeout/retry

```text
organic request -> outstanding state -> physical attempt -> service
                       ^                                  |
                       +---- timeout / response ----------+
```

A timer activation can cause retained outstanding requests to be emitted again as physical attempts. A finite overload changes the outstanding population, so after baseline is restored the triggered execution may produce more physical work than the control under the same future arrivals and timer schedule.

## 3. Unit of observation

The unit is a **fused DFIR subgraph**, called a stage here.

A stage is not necessarily one Hydro operator. DFIR may fuse adjacent operators into one executable subgraph. Handoffs separate stages and are the observable data boundaries.

For each measurement window, the method records:

- stage run count;
- stage poll count;
- time spent polling;
- items consumed from each input handoff;
- items exposed at each output handoff;
- buffered items observed at each handoff;
- whether an input handoff is a tick/loop feedback boundary;
- compiler tags and operator names in the stage;
- number of retained-state references read and mutated by the stage.

The item count is the number of items actually drained by a receiving stage during the interval. It is not merely the number placed into a buffer.

## 4. Static inputs

The method reads two compiler-produced graph representations. Neither requires user hazard annotations.

### 4.1 Hydro IR

Hydro IR supplies semantic origin and topology that may be lost during fusion:

- source kind, including an automatically identified wall-clock interval;
- network boundaries;
- cycle source/sink identity;
- collection and location types;
- captured state references and their mutability.

`source_interval` now preserves `HydroSource::Interval` rather than appearing as an undifferentiated stream source. This is derived from the API constructor.

### 4.2 Runtime DFIR meta-graph

Each runtime DFIR instance retains its `DfirGraph`. The graph provides:

- `GraphNodeId` for operators and handoffs;
- subgraph membership;
- predecessor and successor edges;
- each subgraph's receive and send handoffs;
- delay type on feedback handoffs;
- resolved handoff/state references;
- operator names;
- compiler operator tags.

Hydro assigns statement IDs while lowering and carries them into DFIR as operator tags. Interval sources use an `interval:<statement-id>` tag. This provides an automatic bridge between Hydro origin information and the fused runtime graph.

## 5. Dynamic inputs

Runtime DFIR already maintains interval counters keyed by the same IDs used by the meta-graph:

```text
DfirMetrics.subgraphs[GraphSubgraphId]
DfirMetrics.handoffs[GraphNodeId]
```

`DfirMetricsIntervals::take_interval()` yields the difference since the previous measurement window.

The `DfirMetrics::by_stage(graph)` research view joins these counters with graph topology. Its output is descriptive; it does not classify a hazard.

The application experiment independently supplies the phase schedule:

1. baseline/warm-up;
2. finite trigger;
3. restored baseline;
4. optional zero-arrival or recovery phase.

The trigger schedule is experimental input, not part of the program-pattern definition.

## 6. Derived stage record

Conceptually, one record is:

```text
StageWindow {
    execution,                 // control or triggered
    phase,
    window,
    location,
    subgraph_id,
    operator_tags,
    operator_names,
    operational_input_items,
    ordinary_input_items,
    feedback_input_items,
    output_items,
    buffered_items,
    retained_state_reads,
    retained_state_writes,
    run_count,
    poll_count,
    poll_duration,
}
```

The current code exposes the graph/counter portion. Associating stages with execution phases is the next small integration step.

## 7. Paired comparison

For a selected work-producing stage, define windowed gain relative to operational activation:

```text
gain = output_items / operational_input_items
```

When there is no operational input in a window, gain is undefined rather than zero.

For coupled control and triggered windows:

```text
gain_delta = triggered_gain - control_gain
```

Interpretation:

- `gain_delta = 0` over the post-trigger horizon: stage work has coalesced at this resolution;
- persistent `gain_delta > 0`: the triggered execution continues producing more work under the same operational input;
- changing sign or noisy delta: inconclusive without a longer horizon or better work metric.

The existing `paired_gain_delta` helper calculates this descriptive quantity. It does not apply a hazard threshold.

## 8. Conservative structural screen

Before comparing gains, the graph can screen stages into broad shapes.

### Fixed operational source candidate

Evidence:

- stage ancestry contains an automatically identified interval source;
- stage reaches physical/network output;
- no feedback handoff governs the emission path;
- no retained-state reference governs the emission stage.

The pure heartbeat program satisfies this shape. Its exhaustive test additionally establishes fixed membership-bounded cardinality.

### Feedback-retained candidate

Evidence:

- stage ancestry contains an automatically identified interval source;
- the stage reads or mutates retained state;
- emitted work reaches a physical/network boundary;
- the surrounding program contains a return path by which completion or delay affects retained state or future work.

The timeout/retry program exposes an interval-dependent stage with a mutable retained-state reference and an outbound work path.

This screen is conservative evidence about dependencies. It does not by itself prove amplification or metastability. The paired dynamic comparison tests whether those dependencies produce persistent work divergence.

## 9. Outputs

The methodology produces three distinct outputs that must not be conflated.

### 9.1 Structural description

Example:

```text
interval-driven stage
retained-state writes: 1
feedback inputs: present
outbound network path: present
```

This is a graph fact.

### 9.2 Dynamic trajectory

Example:

```text
window  control gain  triggered gain  delta
  12        3.0            7.0         4.0
  13        3.0            6.5         3.5
```

This is an execution measurement.

### 9.3 Research classification

For now, classifications should remain modest:

- **fixed operational work observed**;
- **history-dependent work amplification observed**;
- **trajectories coalesced**;
- **inconclusive at stage/item granularity**.

The method should not yet emit “safe,” “metastable,” or “will collapse” based only on stage metrics.

## 10. Evidence at this checkpoint

The following focused evidence passes:

1. Runtime handoff counts correlate with their producer and consumer stages.
2. Wall-clock interval origin survives automatically in Hydro IR.
3. Interval origin survives lowering as an identifiable DFIR operator tag.
4. Pure heartbeat has interval-driven network work and no feedback handoff.
5. Pure heartbeat produces fixed membership-bounded work across exhaustive schedules.
6. Timeout/retry contains an interval-dependent stage with mutable retained state and an outbound work path.
7. The existing retry ground-truth deployment shows post-trigger physical attempts exceed logical arrivals under restored baseline and eventually drain when organic arrivals stop.
8. Paired gain arithmetic distinguishes equal fixed-gain windows from persistently amplified windows.

Together, these results justify collecting paired runtime stage windows from heartbeat and retry. They do not yet validate a general detector.

## 11. Known limitations

### 11.1 Stage fusion

Operators inside one fused stage are not individually counted. Attribution is to a subgraph boundary, not every Hydro operator.

### 11.2 Items are not universal work units

One request or TC fact is naturally one item. One CRDT gossip item may contain a large set, so item count can hide increasing payload cost. Poll duration partly captures CPU work, but network bytes are not currently part of `DfirMetrics`.

### 11.3 Mixed inputs

A stage can consume organic input, timer input, feedback, and retained state in one window. Stage-level counts cannot attribute each individual output to one cause.

### 11.4 Retained-state access is coarse

The graph knows that a closure reads or mutates a retained handoff. It does not know which fields were read, whether an output was cloned from state, or whether the state changed output cardinality.

### 11.5 Paired experiments require coupling

A gain difference is meaningful only when future external conditions are the same. Timer schedules, workload, membership, capacity, and observation windows must be coupled or controlled closely enough for comparison.

### 11.6 No semantic identity

Without tuple-level lineage, the method cannot prove that one physical attempt is a retry of a particular logical request. Application ground-truth IDs can validate the research result but must not silently become analyzer inputs.

## 12. Counterexamples and scope

A timer-driven output is not automatically hazardous:

- pure heartbeat emits fixed work;
- periodic monitoring may intentionally emit fresh samples;
- a clock may generate new Monte Carlo trials.

Likewise, a bounded message count does not imply bounded work:

- state-based CRDT gossip may send one growing payload;
- Raft's AppendEntries pump sends a bounded number of messages whose log suffixes vary in size.

These examples motivate measuring multiple gain notions eventually:

- item gain;
- byte-volume gain;
- CPU/poll-time gain;
- retained-state or outstanding-population gain.

The present checkpoint establishes only item- and stage-activity attribution.

## 13. Next experiment

The next justified step is small:

1. attach the existing metrics interval sidecar to pure heartbeat and timeout/retry;
2. emit `StageWindow` records for control and triggered executions;
3. automatically select interval-driven work stages using origin tags and graph topology;
4. compare post-trigger gain trajectories;
5. check that heartbeat coalesces to zero delta and retry retains positive delta for the declared persistence horizon.

Only after observing those traces should we decide whether stage granularity is sufficient or tuple-level lineage is necessary.
