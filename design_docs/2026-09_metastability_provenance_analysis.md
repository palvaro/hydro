# Provenance Analysis for Feedback Hazards in Hydro

Status: design proposal; no implementation approved

Companion: `2026-09_metastability_ground_truth_design_doc.md`

## 1. Purpose

This document proposes the next step after establishing ground truth with two ordinary Hydro programs:

- productive semi-naive transitive closure (Demo A); and
- a client/service timeout-retry loop with a finite-rate service (Demo B).

The first analysis should answer a deliberately narrower question than “will this deployment become metastable?”:

> Can an operational stimulus cause retained logical work to be emitted again as physical work through a feedback cycle, without requiring fresh logical novelty?

That is a program-hazard classification. Predicting whether a particular workload, timeout, and service rate crosses into collapse is a separate quantitative problem.

This proposal does **not** add user annotations such as “this is a retry” or “this is a novelty barrier.” Such annotations would encode the expected answer rather than discover it. When existing IR is insufficient, the compiler should derive additional summaries from program structure or report `unknown`.

## 2. Required outcomes

For the initial ground-truth demos, the intended classifications are:

| Program | Pattern classification | Behavioral classification |
|---|---|---|
| Demo A: TC | no hazardous reactivation found | productive and finite on finite input |
| Demo B: timeout/retry | candidate reactivation hazard | configuration-dependent; the pinned experiment is weakly metastable |

The pattern result must not depend on function names, source paths, request type names, IR node tags, or observed collapse traces.

The analysis result is tri-state:

- **hazard:** the analysis found a supported witness;
- **no hazard found:** all relevant feedback SCCs were understood and none matched;
- **unknown:** an opaque or unsupported operation prevents a justified conclusion.

`No hazard found` is not a universal stability proof. It is scoped to the analyzed hazard class.

## 3. Evidence from the current programs and IR

### 3.1 Demo A

The TC program exposes the productive recursion directly:

```text
frontier
  -> join(edges)
  -> unique candidates
  -> anti_join(known)
  -> novel frontier
  -> retained known/frontier state
```

The feedback cycle is real, but each fact crossing the recursive frontier has passed a relational novelty test against accumulated `known`. Once the anti-join is empty, recursive work drains.

### 3.2 Demo B

The timeout/retry program exposes:

```text
organic requests ---> outstanding state ----> attempts ----> network ----> service queue
                           ^                      |                           |
interval/timer ---------->|                      |                           v
responses ----------------+<---------------------+<--------------------- responses
```

In finalized IR, the following structure is visible:

- a `CycleSource`/`CycleSink` pair for attempts;
- request and response `Network` nodes;
- service and retry interval sources represented today as generic `HydroSource::Stream` values;
- batched state represented through `Reference` nodes captured by staged closures;
- a stateful `FlatMap` that reads the timer, responses, new requests, and outstanding map, and emits attempts.

Important limitation: staged Rust closure bodies are opaque to ordinary IR traversal. The IR records captured singleton references and whether they are mutable, but it does not currently summarize how an output depends on each capture or whether a value is newly constructed, retrieved from state, or suppressed by a set difference.

This limitation must produce `unknown` until compiler-derived summaries are available. The analyzer must not inspect formatted closure text or rely on hand-authored semantic labels.

## 4. Hazard model

### 4.1 Logical lineage and physical work

A logical lineage identifies application work across retransmission or reprocessing. The analysis does not need concrete request IDs. It needs an abstract fact such as:

```text
output may carry lineage already present in retained state
```

Physical work is an event that consumes a capacity-bearing resource. The first implementation recognizes existing Hydro boundaries conservatively:

- network sends;
- asynchronous future resolution;
- explicitly capacity-bearing library operators once such operators exist;
- stateful operators whose compiler summary declares an externally emitted work item.

The analysis must not equate every `map` or every tuple with costly work.

### 4.2 Operational stimuli

An operational stimulus can activate work without adding logical novelty. Examples include:

- interval and timeout events;
- non-arrival/deadline expiry;
- capacity or failure notifications;
- completion acknowledgements that release queued work.

`source_interval` and `Stream::timeout` should lower to distinguishable IR source kinds rather than undifferentiated `HydroSource::Stream` expressions. This is compiler-generated metadata derived from the API call, not a user assertion.

### 4.3 Reactivation hazard

A feedback SCC contains a candidate hazard when all of the following hold:

1. a stateful boundary can retain logical lineage;
2. an operational stimulus can activate that boundary;
3. an emitted value may preserve retained lineage rather than require fresh logical input;
4. the emission reaches a physical-work boundary;
5. the physical work or its delayed response can influence the retained state or future activation; and
6. no proven novelty-decreasing boundary dominates the re-emission path.

This identifies the retry shape without asserting that its rates are unstable.

### 4.4 Productive novelty

A novelty-decreasing boundary is stronger than deduplication in isolation. The initial recognized form is:

```text
candidate relation
  -> unique
  -> anti_join(accumulated known relation)
  -> feedback frontier
```

The proof obligation is structural:

- `known` is monotone across iterations;
- the anti-join key matches the lineage/fact key sent through feedback;
- only the anti-join result crosses the feedback boundary; and
- no bypass path reintroduces rejected candidates into that feedback SCC.

A plain `unique()` is not enough: retrying the same request once per tick can remain hazardous even if duplicates within one batch are removed.

## 5. Compiler-derived summaries

The analysis requires more information than the current IR exposes, but that information must be derived rather than manually labeled.

### 5.1 Source kinds

Extend `HydroSource` with semantic API-level kinds, initially:

```text
ExternalInput
FiniteCollection
Interval
Timeout
GeneralStream       // unsupported/unknown stimulus semantics
```

`source_interval` and `timeout` constructors emit the corresponding kind automatically.

### 5.2 Closure dependency summaries

For every staged operator closure, derive a conservative summary over inputs, captured references, state, and outputs:

```text
reads(input_i)
reads(capture_j)
writes(capture_j)
output_depends_on(input_i | capture_j)
output_preserves_lineage_from(input_i | capture_j)
may_filter
may_expand
```

The initial derivation need not understand arbitrary Rust. It can support a small staged expression subset and yield `unknown` otherwise. In particular, Demo B requires recognizing:

- insertion of fresh requests into the outstanding map;
- lookup/iteration over retained requests;
- removal on response;
- cloning a retained request into output; and
- a condition depending on time/timer state.

If deriving those facts from arbitrary `BTreeMap` mutation is too large for the first implementation, the correct response is not a manual “retry” annotation. Instead, factor the client logic into a reusable Hydro library operator with compiler-known semantics, or add a general compiler representation for state transitions whose dependency effects are generated by its builder.

### 5.3 Relational summaries

Existing relational operators provide compiler-known facts without closure-body interpretation:

- `unique`: duplicate suppression;
- `join`: output depends on both inputs;
- `anti_join`/`difference`: negative dependency and possible novelty barrier;
- `chain`/merge: union of provenance;
- network: physical-work boundary and asynchronous delay;
- cycle/state: retention across iterations.

These summaries should live alongside IR operator semantics, not in demo-specific code.

## 6. Analysis algorithm

### Phase 1: dependency graph

Build a normalized graph from finalized Hydro IR. Include:

- ordinary data edges;
- `CycleSink(c)` to `CycleSource(c)` feedback edges;
- captured-reference read/write dependencies from closure summaries;
- network send/receive as one logical edge annotated with physical work and asynchronous delay;
- state retention edges across ticks.

Preserve source locations and node IDs only for diagnostics.

### Phase 2: SCC classification

Compute strongly connected components over the normalized graph. Ignore acyclic work for this hazard class. For each SCC, record whether it contains:

- retained lineage;
- operational activation;
- physical work;
- delayed feedback;
- output expansion or re-emission; and
- unsupported semantics.

### Phase 3: abstract lineage propagation

Propagate a small abstract domain:

```text
Lineage = None | Fresh | Preserved | Mixed | Unknown
Activation = Logical | Operational | Mixed | Unknown
Multiplicity = NonExpanding | MayExpand | Unknown
```

The transfer functions are operator-defined. Stateful closures use compiler-derived summaries. Values coming from retained state are `Preserved`; values from organic input are `Fresh`; joins and merges combine domains.

### Phase 4: novelty proof

For each possible preserved-lineage re-emission path, attempt to prove it crosses a dominating novelty-decreasing subgraph. Initially support the TC pattern formed by monotone known state plus matching-key `anti_join`, with duplicate suppression as a supporting—not sufficient—fact.

### Phase 5: result and witness

Return:

```text
Hazard {
    scc,
    retained_lineage_path,
    operational_trigger_path,
    physical_work_path,
    feedback_path,
}
```

or a `NoHazardFound` result with discharged SCCs, or `Unknown` with the exact unsupported node/closure summary. Diagnostics should explain topology and inferred facts, not say merely “retry detected.”

## 7. Staged implementation plan

### M0: IR fixtures and inspection

- Add stable, compact IR fixtures for Demo A and Demo B.
- Preserve cycle IDs, operator kinds, source kinds, network edges, state captures, and relational keys.
- Exclude formatted closure token dumps from snapshots.

Acceptance: fixtures are readable and do not encode demo names into analysis logic.

### M1: structural feedback inventory

- Build the normalized dependency graph and SCC finder.
- Connect `CycleSink`/`CycleSource` and captured references.
- Identify network and state boundaries.
- Return `unknown` for opaque stateful closures.

Acceptance: both demos have feedback SCCs; neither is prematurely labeled safe or hazardous solely because it has a cycle.

### M2: automatic source and relational semantics

- Give interval/timeout sources explicit compiler-generated IR kinds.
- Add transfer functions for join, merge, unique, anti-join, network, and retained state.
- Prove Demo A’s feedback frontier is novelty-decreasing.

Acceptance: Demo A becomes `no hazard found`; variants that bypass `anti_join(known)` become `hazard` or `unknown`, never false-safe.

### M3: state-transition dependency summaries

- Derive or builder-generate dependency summaries for stateful staged closures.
- Support the outstanding-request transition used by Demo B without demo-specific matching.
- Trace retained request lineage from state to emitted attempt under interval activation.

Acceptance: Demo B yields a hazard witness; removing the timer-driven re-emission removes that witness; changing type/function names does not change the result.

### M4: quantitative behavior prediction

Only after M1–M3 agree with ground truth:

- consume measured offered/effective arrivals, service throughput, success, and latency distributions;
- estimate amplification and capacity headroom;
- predict stable versus vulnerable regions and report uncertainty;
- compare predictions with independent control/trigger traces.

Acceptance: pattern-classification errors and behavioral-prediction errors are reported separately.

## 8. Tests

Required tests include:

1. Demo A: productive TC is not flagged after its novelty proof succeeds.
2. TC without `anti_join(known)`: flagged or unknown.
3. TC with a bypass around the novelty frontier: flagged or unknown.
4. Demo B: timeout-driven retained request re-emission is flagged.
5. Demo B without retries: not flagged.
6. Demo B with retries but no physical-work path: not flagged for this hazard class.
7. A timer-driven generator producing genuinely fresh IDs: not classified as retained-lineage reactivation.
8. An opaque stateful closure: `unknown`, never silently safe.
9. Renamed/rearranged equivalent programs: same classification.
10. No test may consult execution traces to decide the pattern result.

## 9. Non-goals

The first analysis will not:

- prove global stability;
- infer service rates from source code;
- predict collapse from a cycle alone;
- parse debug-formatted Rust tokens to recognize retry code;
- require users to label hazards or safe barriers;
- treat every network cycle as dangerous;
- claim that all productive recursion uses anti-join.

## 10. Open decisions before implementation

1. Can stageleft expose a conservative expression dependency graph for arbitrary staged closures, or should Hydro introduce structured state-transition builders for analyzable state machines?
2. Should timeout/interval semantics be represented as `HydroSource` variants or operator metadata shared by source constructors?
3. How should key correspondence through `map` be represented so novelty proofs remain sound under projections?
4. What precise resource boundaries count as physical work beyond network sends and asynchronous tasks?
5. Should `unknown` be emitted per SCC, per potential witness path, or both?
6. How much of the TC novelty proof can be generalized to arbitrary monotone semilattice progress in the first version?

These decisions should be resolved with small IR prototypes before public APIs are changed.
