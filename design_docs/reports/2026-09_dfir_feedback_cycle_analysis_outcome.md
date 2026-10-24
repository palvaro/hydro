# DFIR Feedback-Cycle Analysis: Outcome

## What we attempted

We tried to distinguish two kinds of dataflow cycles using generic Hydro/DFIR signals:

- **Productive cycles:** repeated work is justified by logical progress or newly derived information and eventually drains on finite input.
- **Operationally reactivated cycles:** a timer, timeout, election, or similar stimulus repeatedly turns retained state back into physical work without requiring fresh logical progress.

The intended result was a generic, name-independent classifier—not one that recognizes “retry,” “gossip,” or particular operators by hand.

We added instrumentation for:

- DFIR stage activations and polling time;
- ordinary and delayed-feedback handoff traffic;
- retained-state reads and writes;
- interval-source provenance;
- serialized network messages and payload bytes;
- correlation between runtime counters and compiled graph structure.

We also created controls and measurements for productive transitive closure, timeout/retry, pure heartbeat, G-Set gossip, reliable broadcast, and Multi-Paxos-live.

## What we measured

- **Pure heartbeat:** fixed-size, membership-bounded work with no feedback or retained-state dependency.
- **Timeout/retry:** retained outstanding requests were re-emitted after timer activation without new application input. A finite overload entered a weakly metastable regime.
- **G-Set gossip:** increasing retained state from 4 to 400 elements left pump traffic at 63 messages but increased serialized bytes from 1,512 to 101,304; polling time also increased substantially.
- **Reliable broadcast:** eight finite inputs generated message windows such as `[24, 72, 0, ...]`, then drained.
- **Multi-Paxos-live:** one versus twenty pending commands produced 32 versus 450 messages and 782 versus 12,372 bytes in the selected runs.
- **Productive transitive closure:** application-level tracing showed candidate generation, duplicate rejection, novel facts, and eventual exhaustion of the novelty frontier.

These measurements are valid observations of the selected executions. They are not a general classification.

## What we learned

The instrumentation can measure **where and how much physical work occurs**, but it cannot generally determine **why that work occurred**.

Several initially promising signals are insufficient:

- Network traffic across multiple windows does not imply amplification; finite productive work can cross window boundaries and then drain.
- Network work with no ordinary input in the same window does not imply reactivation; a service may simply be draining previously buffered work.
- Growth in bytes with retained state does not imply instability; it may be normal full-state transmission.
- The presence of `unique` or `anti_join` does not prove that a cycle is productive. The operator may be outside the actual feedback path or may suppress only visible output while internal computation continues.
- The absence of those operators does not imply reactivation. Protocol progress is often encoded in arbitrary state-machine logic: ballot comparisons, advancing indices, set membership, quorum completion, or bounded phase transitions.
- An operational source is not sufficient evidence either. Timers also drive harmless heartbeats, batching, queue service, reporting, and checkpoints.

Real protocols frequently multiplex several causes over the same feedback edge. For example, a Raft network cycle carries fresh requests, productive responses, heartbeats, and retained-log replay. Aggregate stage or edge counters cannot attribute an observed send to one of those causes.

## Fundamental blockers

1. **No causal provenance**

   Current counters aggregate activity per stage and time window. They do not connect a particular output message to the ordinary input, feedback item, timer event, or retained-state value that caused it.

2. **No generic measure of logical progress**

   DFIR can count tuples and bytes, but it does not generally know whether a tuple represents a new fact, a completed phase, an advanced log index, an acknowledged request, or a duplicate. Productive progress is often encoded inside arbitrary user logic.

3. **Compiler fusion hides internal flow**

   Candidate generation, state access, novelty suppression, and network emission may be fused into one stage. Stage-level input and output totals then cannot measure flow immediately before and after the relevant gate.

4. **Sampling windows obscure temporal relationships**

   Input may arrive in one window, remain queued, and generate output in another. Therefore “no input in this window” does not mean “this output required no earlier input.”

5. **Cycles are interprocedural and heterogeneous**

   Wrappers inherit cycles from called libraries, higher-order functions can decide whether completions authorize fresh work, and one SCC may contain both productive and reactivated paths. Whole-function or whole-SCC labels are often too coarse.

6. **Reactivation is not the same as amplification**

   A cycle may repeatedly emit retained state at a fixed rate without growing, or it may have bounded fan-out and drain. Actual amplification depends on feedback gain, activation rate, service capacity, state growth, and workload—not merely graph structure.

## Why the classifier did not work

The attempted predicates converted correlated aggregate measurements into causal conclusions they could not support. They could identify interesting execution shapes, but those shapes had benign counterexamples. Hand-selected experiments demonstrated telemetry capabilities, not a repository-wide decision rule.

The reusable predicates were therefore quarantined. The underlying DFIR and network instrumentation remains useful as a measurement substrate.

A viable next analysis would need, at minimum:

- causal attribution from activation sources through retained-state reads to emitted work;
- per-edge or pre/post-gate flow measurements that survive fusion;
- an explicit or inferred logical-progress signal;
- recurrence analysis after finite ordinary input has drained;
- separate estimates of physical feedback gain and available capacity.

Without those additions, the honest result for complex cycles is generally **unknown**, rather than productive or amplifying.
