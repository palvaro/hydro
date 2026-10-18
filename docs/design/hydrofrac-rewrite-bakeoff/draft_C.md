# Draft C

# Hydrofrac

_Lineage-driven fault injection for Hydro_

Hydrofrac checks whether a Hydro program's eventual-consistency claims survive faults. It runs the program in the Hydro simulator, injects crashes and message omissions, and looks for an execution in which surviving cluster members fail to converge.

Hydro already checks an important part of eventual consistency: whether operators produce the same contents when given the same inputs. But that is not the whole problem. A sender can crash halfway through a broadcast. A lossy channel can drop a message to one recipient. The downstream computation may be perfectly deterministic and still leave different members with different contents, because they received different inputs.

Hydrofrac checks this missing part. It uses the program's existing `EventualConsistency` labels to generate correctness checks, and its existing transport declarations to determine which faults are allowed. The test author supplies the topology, workload, and fault bounds. No additional correctness property is needed for supported EC collections.

The search is based on Molly's lineage-driven fault injection, or LDFI (Alvaro, Rosen, Hellerstein, SIGMOD 2015). Rather than enumerate faults blindly, hydrofrac works **backwards from successful outcomes**. It records which deliveries contributed to convergence, asks which faults could interrupt those deliveries, and tries those faults next. If the program succeeds through another path, that run supplies another derivation for the search to defeat.

The intended result is a separation of existing Hydro code into two groups:

- **Certified within the test's bounds:** exhaustive search found no allowed fault and schedule that violates the generated EC check.
- **Incorrect under those bounds:** hydrofrac found a fault set and schedule that leave surviving members with different contents at the end of the run.

Not every search reaches one of these conclusions. Sampled schedules, exhausted search budgets, empty probe results, and unsupported collections are reported separately. A certificate is not a proof for all inputs, all cluster sizes, or arbitrarily long executions.

This document describes the current implementation. Development history is in `history-hydrofrac-development.md`.

## Why Hydro needs hydrofrac

A cluster location has a consistency parameter:

```rust
Cluster<'a, Tag, Con>
```

The default, `NoConsistency`, allows members to hold different contents. `EventualConsistency` says that members given the same inputs eventually hold the same contents. A collection can acquire that label from:

- A transport, such as `broadcast_closed` using a transport whose `ConsistencyGuarantee` is `EventualConsistency`.
- A manual proof, through `assert_has_consistency_of(manual_proof!(...))`.
- An operator that preserves consistency, such as a `map`, a `filter`, or a `fold` with the required commutativity and idempotence properties.

Collections at a single `Process` do not need this label: there is only one holder.

The type system reasons about how a computation transforms its inputs. It does not establish that a distributed protocol actually gets the necessary inputs to every surviving member.

For example, suppose a process broadcasts two items to three members. Each member inserts received items into a set. Set insertion has the right algebraic properties, but the sender can still die after sending the second item to only two members. No downstream algebra repairs that missing delivery. The protocol needs another dissemination path: an echo, gossip, retransmission, or some equivalent recovery mechanism.

In this document, **complete EC** means both parts of the argument:

1. **Consistent computation:** members that receive the same inputs compute the same contents.
2. **Fault-tolerant dissemination:** allowed faults do not leave surviving members with permanently different inputs to that computation.

Hydro's types address the first part. Hydrofrac tests the second, using the labelled collection's contents as the observable outcome.

“Complete EC” does **not** mean that every item from a failure-free run must survive every fault. If the only sender crashes before sending an item to anyone, EC does not require the system to recover that item. The requirement is convergence among surviving members, not loss-free delivery.

## What hydrofrac checks

The generated specification is `Spec::eventual_consistency`. It checks a set-valued collection at a cluster.

First, hydrofrac runs a failure-free probe. It collects every fact observed at any member into a fixed set, `FACTS`. Subsequent runs use that set as the domain of the check.

For each run with faults:

1. Exclude members that the fault hypothesis crashes.
2. For each fact in `FACTS`, determine which surviving members hold it.
3. If any survivor holds the fact, require every survivor to hold it.

For example:

| Final contents at three surviving members | Result |
|---|---|
| `{1, 2}`, `{1, 2}`, `{1, 2}` | Satisfied |
| `{1}`, `{1}`, `{1}` | Satisfied |
| `{1, 2}`, `{1, 2}`, `{1}` | Violated |
| `{}`, `{}`, `{}` | No disagreement; contributes no successful-delivery evidence |

The second row is allowed. A fault may have prevented item `2` from entering the system at all. The third row is the bug hydrofrac is looking for: some survivors received `2`, but another did not.

If the probe produces no facts, hydrofrac reports `Vacuous`, not a certificate.

### Scope of a certificate

A certificate applies to:

- **One tested collection.** Other EC-labelled collections are not implicitly covered.
- **One topology and workload.** Cluster sizes, input values, and timer firings come from the test.
- **The declared fault model.** Transport types determine available omissions; the test registers crashable nodes.
- **The declared fault bounds.** These include the crash budget and the end of finite failures, `EFF`.
- **The test's horizon.** The specification is evaluated when the test finishes driving the run.
- **The explored schedules.** A certificate requires `Exploration::Exhaustive`. `Exploration::Fuzz { instances }` produces a sampled result, not a certificate.

The horizon matters. Hydrofrac does not prove unbounded eventual convergence. If a protocol would converge after more retransmission rounds than the test provides, hydrofrac reports the disagreement still present at the test's end. The counterexample establishes failure of the bounded check; deciding whether the protocol is broken or the test is too short requires inspecting that execution.

The fact domain also matters. The generated check ranges over `FACTS`, not arbitrary new facts that appear only in faulty runs. It should not be described as an unrestricted equality check over everything a faulty execution might produce.

### Why the specification's form matters

LDFI needs more than an executable predicate. It needs successful derivations whose disruption is necessary for a counterexample. Otherwise the search could overlook a fault that changes the outcome without touching any recorded support.

For a fixed fact and fixed set of correct members, the positive obligation “every correct member holds this fact” is monotone in observed deliveries: adding another observed `(member, fact)` pair cannot undo it. Hydrofrac uses these positive delivery obligations to drive the generated EC search.

This is also why the probe fixes the fact domain. An open-ended requirement such as “every member holds everything that any member holds” can acquire a new obligation when a faulty run produces a new fact. Successful lineage from an earlier run need not explain that fact.

Fixing `FACTS` does not make arbitrary program logic monotone. The certification argument still depends on conservative lineage and sound handling of the generated specification's preconditions and delivery obligations. Hydrofrac does not perform a general syntactic analysis that establishes these properties for arbitrary user-written specifications. Certificates for those specifications are therefore marked **RELATIVE**.

## Using hydrofrac

The program does not need fault-injection hooks. Hydrofrac instruments the network operators already present in its flow.

The test instantiates the program, selects a labelled collection, registers the relevant members, and drives an execution:

```rust
let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

// This output is labelled EventualConsistency.
let delivered = reliable_broadcast_closed(items, &members);

let mut spec = Spec::new(&mut flow);
let correct = spec.correct_members(&members, 0..3);
let observed = spec.observe_cluster(delivered.weaken_consistency());
spec.eventual_consistency(observed, correct);

let compiled = flow.sim()
    .skip_consistency_assertions() // The simulator does not yet check them.
    .with_cluster_size(&members, 3)
    .with_provenance()
    .compiled();

Ldfi::new()
    .level(Level::Record)
    .crashable("source", &source)
    .crashable_cluster("member", &members, 0..3)
    .crash_budget(1)
    .spec(&spec)
    .search(&compiled, async |_run| {
        send.send(1);
        send.send(2);
        hydro_lang::sim::quiesce().await;
    })
```

The body determines which inputs arrive, which timers fire, and how long the protocol has to converge. For protocols that depend on retransmission, gossip, or leader changes, the body must fire the appropriate timers after `run.end_of_finite_failures()`. Ending faults without giving the protocol a recovery window is not a useful convergence test.

The result includes either the faults and failing schedule, or the successful search's scope, grade, hypothesis count, and exploration mode.

### Coverage of existing labels

Today, each test observes one collection through `spec.observe_cluster`. A program may have many labelled collections—multi-Paxos has thirteen—and certifying one does not certify the others.

At the start of each search, hydrofrac lists the places where collections acquire the EC label, with one of these statuses:

- `checked by this test`
- `UNCHECKED (fed by a network channel)`
- `nothing to check (local inputs only)`

This makes omissions visible, but unchecked labels do not currently fail the test.

Automatic coverage is in progress. The intended implementation uses an IR rewrite at flow finalization to observe every supported, network-fed EC collection and run a separate search for each, sharing the test body. Each collection will receive a verdict or an explicit unsupported reason. `spec.observe_cluster` will remain available for selecting one collection manually.

That coverage is necessary before hydrofrac can classify an entire program's EC claims rather than just the claims selected by its tests.

## How hydrofrac works

### The simulator supplies schedules

The Hydro simulator compiles the distributed flow into a single-process executable. Network channels become queues, ticks execute over explicitly scheduled batches, and the test body controls external inputs and timers.

There are two distinct searches:

- Hydrofrac chooses **fault hypotheses**.
- The simulator explores **batchings and orderings** under each hypothesis.

Exhaustive schedule exploration is needed for a certificate. Sampling can find a counterexample, but a successful sample cannot establish that no failing schedule exists.

### Transports supply the fault model

Hydrofrac inserts a filter before every network send. The filter can omit messages or simulate a crash.

**Omission** drops a message identified by its channel, sender, destination, and wire bytes. While finite failures last, every byte-identical copy on that channel is dropped. Retransmitting an identical payload therefore does not create a new omission identity.

Omissions are available on lossy channels such as `lossy_delayed_forever`. A `fail_stop` channel does not permit independent message omissions; delivery failures arise from sender crashes.

**Crash** is represented as:

```text
crash(node after message)
```

The node performs sends through the named message. Later sends are dropped, and its external outputs are suppressed. Naming the crash point by a message rather than a send count makes the hypothesis usable across schedules. It also allows hydrofrac to crash a sender between two sends in a single broadcast fan-out.

Only nodes registered by the test are crashable. Crashes count against the crash budget. Faults take effect before `EFF`; the remaining execution gives the protocol a chance to recover.

The program's transport declarations therefore determine what can go wrong, while the test determines how much can go wrong and when faults must stop.

### Lineage explains successful outcomes

Each run records:

- Executions of asynchronous code and ticks.
- Sends and their fates.
- Receives and the deliveries they consumed.
- The executions that produced observed outputs.

Starting from a successful specification output, hydrofrac follows these dependencies backwards to the deliveries that supported it. That backward closure is its derivation.

This is the central LDFI idea. A forward search asks, “What happens if I drop this message?” A lineage-driven search first asks, “Why did this run succeed?” It then chooses faults that could remove the explanation.

Suppose the probe succeeds because a source sends directly to every member. Hydrofrac tries a crash that interrupts that fan-out. If the remaining members repair the missing delivery through an echo, the next successful run records the echo path. A breaking hypothesis must now defeat both known ways of succeeding.

Hydrofrac supports four lineage levels:

| Level | Dependencies recorded |
|---|---|
| `Exec` | Every input to the execution and to earlier executions of the same tick. |
| `Slice` | The same execution-level information, narrowed by a static slice for each sink. This is the default. |
| `Record` | Record-level provenance, added by an IR rewrite through `SimFlow::with_provenance`. |
| `Counterfactual` | Node-local replay with deliveries removed one at a time, to test whether the output depends on them. |

More precise lineage can reduce the number of hypotheses, but it is not always available. The record rewrite cannot handle every operator or see through every closure. Counterfactual replay may fail to reproduce a node's original output even when all deliveries are retained; it may also encounter code that panics when a delivery is removed.

When a more precise level cannot produce a usable result, hydrofrac falls back: `Counterfactual` to `Record`, and `Record` to `Slice`.

The direction of this fallback is important. Including an unnecessary dependency gives the search extra faults to try. Omitting a necessary dependency can hide a counterexample. Conservative over-approximation costs search time; under-approximation threatens certification.

### The search defeats known derivations

The search follows Molly's loop:

1. Run the failure-free probe, establish `FACTS`, and record the first successful derivation.
2. Convert each derivation into a constraint: a breaking fault set must interrupt at least one delivery supporting it.
3. Find a fault hypothesis that hits every derivation collected so far, subject to the fault bounds.
4. Run the hypothesis under the configured schedule exploration.
5. If a schedule violates the specification, return it as a counterexample.
6. Otherwise, add the successful run's derivation and repeat.

An omission interrupts the matching message. A crash interrupts sends after its crash point. The solver accounts for those effects when constructing a hitting set.

The search stops when it finds a counterexample, when no hitting set remains within the bounds, or when the hypothesis budget runs out. Budget exhaustion is `Outcome::Budget`, not certification.

`Strategy::Blind` searches the same fault universe without lineage, trying untried fault sets in increasing weight order. It is the ablation for measuring what lineage buys. `Ldfi::fuzz_faults_with` instead samples fault sets randomly.

### The specification is itself a Hydro flow

`Spec` builds an observer at a dedicated process and compiles it with the program. Before each run, hydrofrac supplies the observer with the probe's `FACTS` and the members not crashed by the hypothesis.

The observer collects distinct `(member, fact)` pairs for correct members, restricted to `FACTS`. Let `PRESENT` be the subset of `FACTS` seen at any correct member. The EC condition is:

```text
number of distinct observed (member, fact) pairs
    = number of correct members × number of PRESENT facts
```

This checks that every present fact occurs at every correct member. A fact absent everywhere creates no obligation.

Because the observer is a Hydro flow, its outputs have lineage just like the program's outputs. Hydrofrac uses that lineage to build the search constraints.

## Judgements

The distinction between a certificate and a successful test run should be explicit.

| Result | Meaning |
|---|---|
| **Certified** | Exhaustive exploration completed without a violation, and no further fault hypothesis remains within the declared bounds. |
| **Violated** | An allowed fault set and explored schedule failed the generated check. The result includes the counterexample. |
| **Fuzz result: no violation found** | Sampled schedules passed. This is evidence, not a certificate. |
| **Budget exhausted** | The search stopped before reaching a conclusion. |
| **Vacuous** | The probe supplied no facts to check. |
| **Unsupported or unchecked** | No conclusion for this collection. |

A counterexample does not require exhaustive exploration: one failing execution is enough. A certificate does.

For complete EC, certification combines with the existing type-level reasoning. The type system establishes consistency given the same inputs; hydrofrac supplies bounded evidence that the protocol's fault handling preserves the required dissemination. Neither result substitutes for the other.

## Results on existing Hydro code

The existing tests cover every producer of the `EventualConsistency` label in the codebase and the set-valued, cluster-resident labelled outputs of `hydro_std::ec_inference_demos`. Tests are in:

- `hydro_test/src/cluster/ec_ldfi.rs`
- `hydro_test/src/cluster/ec_demos_ldfi.rs`

The searches below allow one crash. Hypothesis counts include the probe. The multi-Paxos test keeps its proposer correct.

| Program | Result |
|---|---|
| `broadcast_closed`, process → cluster, `fail_stop` | **Violated**, 5 hypotheses. `crash(source after bcast[→1]:Item(2))` interrupts a fan-out. |
| `broadcast_closed`, process → cluster, `lossy_delayed_forever` | **Violated**, 2 hypotheses. `omit(bcast[→0]:Item(2))` loses an item that is never retransmitted. |
| `broadcast_closed`, cluster → cluster, `fail_stop` | **Violated**, 5 hypotheses. `crash(source@0 after bcast[0→1]:Item(10))`. |
| `broadcast_live_from_process` | **Violated**, 5 hypotheses. A source crash interrupts the one-hop broadcast. |
| `leader_merge_broadcast` | **Violated**, 2 hypotheses. A leader crash interrupts dissemination of `(writer 0, 100)`. The demo documents leader survival as an assumption. |
| Echo reliable broadcast, `hydro_test` version | **Certified**, 10 hypotheses, exhaustive schedules. |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | **Certified**, 10 hypotheses each, exhaustive schedules. |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | **No violation found**, 28 hypotheses each, 8 sampled schedules per hypothesis. |
| `g_set_gossip`, `g_set_gossip_live` | **No violation found**, 13 hypotheses each, 8 sampled schedules per hypothesis. |
| `multi_paxos`, learners' `learned` output; 1 proposer, 3 acceptors, 3 learners | **No violation found**, 24 hypotheses, 8 sampled schedules per hypothesis. Learners rebroadcast learned values; this check exercises that dissemination path. |

The distinction is straightforward in this corpus. Single-hop dissemination fails when one crash or omission interrupts delivery. Protocols with echo or gossip mechanisms pass the searches performed so far, with exhaustive certification for the smaller schedule spaces.

This exposes a gap in transport-level justifications for EC. Treating a failed connection as a failed recipient does not account for a sender that dies halfway through a fan-out. Treating a dropped message as indefinitely delayed does not make surviving recipients converge within a recovery window. A transport label alone cannot establish complete EC for the program using it.

Some other labels do not introduce an independent network fault question. Algebraically justified folds preserve consistency given their inputs. `quorum` and `epoch_splice` are EC-preserving combinators whose dissemination assumptions come from their upstream inputs. `synod` has no `EventualConsistency` label. ABD's replica register does have an EC claim, but its lattice `Singleton` output needs a different observer.

## Hand-written specifications

Hydrofrac can also check properties that Hydro's types do not express.

A hand-written `Spec` has two Hydro views:

- A **precondition**: if empty, the run is ignored.
- A **postcondition**: if empty while the precondition is nonempty, the run is a counterexample.

`Ldfi` also accepts simpler output goals through `goal`, `goal_all_members`, and `goal_witnessed`.

These interfaces support checks for committed-entry durability in Raft and Paxos, two-phase commit atomicity, ISR replication agreement, quorum counting, and retry-until-ack delivery. Their certificates are marked **RELATIVE**, because hydrofrac does not establish that an arbitrary specification has the form required by the lineage search's completeness argument.

These tests have found substantive defects:

- An injected Raft election-restriction bug, using index-only log comparison, was found in 90 hypotheses with four omissions and no crash. The scenario had not been written down by the author. Correct Raft completed a relative certification search in 235 hypotheses under the same bounds.
- Production Paxos counted P1b replies rather than distinct acceptors. The defect became visible after the test moved `EFF` past the first ballot. Correct Paxos, the Paxos key-value store, and compartmentalized Paxos completed relative certification searches in 372, 301, and 568 hypotheses.
- Simulator exploration also exposed two non-fault defects: `collect_quorum_with_response` was not a function of its input despite its `nondet!` justification, and an idempotence-justified `fold` lacked a simulator implementation of its retry-only cast. Both were fixed.

The larger tests are ignored by default for runtime. The Raft bug and correct-version searches take roughly thirty minutes and an hour; the three corrected Paxos searches take about forty-five minutes together.

## Limitations

**A certificate covers a test, not all executions.** Changing the workload, cluster size, timers, or horizon requires another test. Exhaustive fault search does not generalize those choices.

**Sampled schedules do not certify.** Uniform broadcast, gossip, and multi-Paxos currently use schedule sampling because their schedule spaces are large.

**Convergence is checked at a finite horizon.** Hydrofrac cannot distinguish “never converges” from “has not converged yet” using the final contents alone. Test bodies must provide a realistic recovery window.

**The generated observer handles set-valued collections.** A lattice `Singleton`, such as ABD's max-by-timestamp register, needs an observer that compares final values rather than accumulated facts. That observer is not implemented.

**Membership is static in these tests.** The simulator starts with all members joined. Tests of `_live` protocols exercise faults, not join timing.

**Precise lineage has gaps.** The provenance rewrite does not handle `entries_partially_ordered` on keyed streams, so `leader_merge` uses `Level::Slice`. Counterfactual replay failures are treated as inconclusive for the affected node and trigger fallback.

**Unchecked labels remain unchecked.** The coverage report is informational. Enforcing coverage across all tests needs a per-program registry or automatic observation.

**The fault vocabulary is limited.** Byzantine behavior and duplication are not modelled. There is no separate partition primitive, although omission sets on lossy channels can represent partitions. The Raft counterexample uses four omissions on two links.

## Next steps

1. **Observe every supported EC-labelled collection automatically.** This turns per-collection tests into a program-wide report of certified, violated, unsupported, and inconclusive claims.
2. **Add a lattice-aware observer.** Use it to check ABD's `Singleton` register.
3. **Support explicit convergence witnesses.** Protocols that report convergence should be able to use that signal in the specification rather than rely only on a fixed horizon.
4. **Add decision-trace replay.** Record nondeterministic decisions and replay a prefix with one decision changed. This would provide finer counterfactual analysis than node-local replay.
5. **Run Raft's `committed` output under the generated specification.** The harness exists; the search takes about an hour.
