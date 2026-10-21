# Hydrofrac

_Lineage-driven fault injection for Hydro_

Hydrofrac checks whether a Hydro program's eventual-consistency claims survive faults. It runs the program in the Hydro simulator, injects crashes and message omissions, and looks for an execution in which surviving cluster members fail to converge.

Hydro already checks an important part of eventual consistency: whether operators produce the same contents when given the same inputs. But that is not the whole problem. A sender can crash halfway through a broadcast. A lossy channel can drop a message to one recipient. The downstream computation may be perfectly deterministic and still leave different members with different contents, because they received different inputs.

Hydrofrac checks this missing part. It finds every collection the program labels `EventualConsistency`, generates a correctness check for each, and uses the program's existing transport declarations to determine which faults are allowed. The test author supplies the topology, workload, and fault bounds. No collection is named and no correctness property is written.

The search is based on Molly's lineage-driven fault injection, or LDFI (Alvaro, Rosen, Hellerstein, SIGMOD 2015). Rather than enumerate faults blindly, hydrofrac works **backwards from successful outcomes**. It records which deliveries contributed to convergence, asks which faults could interrupt those deliveries, and tries those faults next. If the program succeeds through another path, that run supplies another derivation for the search to defeat.

The intended result is a separation of existing Hydro code into two groups:

- **Certified within the test's bounds:** exhaustive search found no allowed fault and schedule that violates the generated EC check.
- **Incorrect under those bounds:** hydrofrac found a fault set and schedule that leave surviving members with different contents at the end of the run.

Not every search reaches one of these conclusions. Sampled schedules, exhausted search budgets, empty probe results, and collections outside the generated specification's class are reported separately. A certificate is not a proof for all inputs, all cluster sizes, or arbitrarily long executions.

This document describes the current implementation and is the user guide. The per-example gallery of the EC corpus is `hydrofrac-ec-corpus.md`; the design retrospective is `hydrofrac-retrospective.md`; development history is in `history-hydrofrac-development.md`.

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

- **One collection.** Every labelled collection gets its own search and its own verdict. Certifying one does not certify another, even one derived from it.
- **One topology and workload.** Cluster sizes, input values, and timer firings come from the test.
- **The declared fault model.** Transport types determine available omissions; the test registers crashable nodes.
- **The declared fault bounds.** The end of finite failures, `EFF`, and the crash budget if the test sets one (by default every combination of crashes of the registered nodes is admissible).
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

The program does not need fault-injection hooks or observation points. Hydrofrac instruments the network operators already present in its flow and taps the collections it already labels.

Hydrofrac is driven from a test that builds the flow, fixes cluster sizes, declares which nodes may crash, and writes a body that drives inputs and timers. It does not say which collections to check or what to check about them: `Spec::all_labels` plus `Ldfi::search_all_labels` finds every labelled collection and runs one search per collection.

### The simplest example

One process broadcasts two items to a cluster of three over `fail_stop`, and every node is crashable. This is `ec_broadcast_closed_all_labels` in `hydro_test/src/cluster/ec_ldfi.rs`, trimmed to the lines that matter:

```rust
let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<Item, TotalOrder, ExactlyOnce>();

// This collection is labelled EventualConsistency by the transport.
let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
let _out = at_members.weaken_consistency().sim_cluster_output();

let spec = Spec::all_labels(&mut flow); // Names no collection.

let compiled: CompiledSim = flow.sim()
    .skip_consistency_assertions() // The simulator does not yet check them.
    .with_cluster_size(&members, 3)
    .with_provenance()
    .compiled();

let results = Ldfi::new()
    .level(Level::Precise)
    .crashable("source", &source)
    .crashable_cluster("member", &members, 0..3)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| {
        send.send(Item(1));
        send.send(Item(2));
        hydro_lang::sim::quiesce().await;
    });
println!("{results}");
```

`all_labels` finds the one labelled collection, the members' side of the `broadcast_closed`, which is both where the label is minted and (through `weaken_consistency`) where it exits. The test registers four crashable nodes and sets no `.crash_budget`, so every combination of their crashes is admissible except the ones that cannot say anything. The search logs the bounds before the first hypothesis, then the verdict:

```text
ldfi: crash bounds: crashes unbounded; not all 4 nodes at once; at most 1 of the checked cluster's 3 crashable member(s)
```

```text
EventualConsistency at ec_ldfi.rs:270 (Cluster(loc2v1), AssertIsConsistent, minted and delivered): VIOLATED by {crash(source after bcast[→1]:Item(2))}, 5 hypotheses
```

![Lamport diagram of the broadcast_closed counterexample: the source crashes after its second item to member 1; its second item to member 2 is lost](molly-viz-samples/ec/ec_broadcast_closed_all_labels/failing.png)

*The failing run, one lane per node, time downward. The source delivers `Item(1)` and `Item(2)` to members 0 and 1, then `☠ crashed` ends its lane; its `Item(2)` to member 2 is drawn gray dashed to a `✗`, lost because the sender had crashed. Members 0 and 1 hold `Item(2)` and member 2 never does.*

The picture shows the premise the type system made. `FailStop`'s `ConsistencyGuarantee` (`hydro_lang/src/networking/mod.rs`) reasons that a failed connection stops all future deliveries to that recipient, which models the recipient as having failed. That says nothing about the sender. One crash of the sender between two sends of one fan-out leaves two members with the item and one without, and nothing retransmits it. Every single-hop `broadcast_closed` in the corpus fails the same way.

### A protocol with an echo

Whether the failure matters depends on what the program builds on top of the hop, and that is what `all_labels` separates. The second harness, `ec_demo_reliable_broadcast_all_labels` in `hydro_test/src/cluster/ec_demos_ldfi.rs`, checks `reliable_broadcast_closed`:

![Dataflow of reliable_broadcast_closed with the EventualConsistency-labelled region shaded: two bold broadcast_closed nodes where the label enters, a double-bordered weaken_consistency where it exits](molly-viz-samples/figures/reliable_broadcast_regions.png)

*`reliable_broadcast_closed` as `Spec::all_labels` sees it. The shaded region is the collections whose type carries `EventualConsistency`. The source's `broadcast_closed` (`:130`) and the members' echo `broadcast_closed` (`:156`) are its two minting nodes, bold. `forward_ref`, `merge_unordered` and `unique` inherit the label. The delivered set leaves the region at the test's `weaken_consistency`, double-bordered: the exit.*

```rust
let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

let delivered = reliable_broadcast_closed(items, &members); // labelled EventualConsistency
// Drop the label and make the stream a program output: this is the *exit*
// of the labelled region, the collection all_labels reports as "delivered".
delivered.weaken_consistency().sim_cluster_output();

let spec = Spec::all_labels(&mut flow);
let compiled = flow.sim()
    .skip_consistency_assertions()
    .with_cluster_size(&members, 3)
    .with_provenance()
    .compiled();

let results = Ldfi::new()
    .level(Level::Precise)
    .crashable("source", &source)
    .crashable_cluster("member", &members, 0..3)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| {
        send.send(1);
        send.send(2);
        hydro_lang::sim::quiesce().await;
    });
println!("{results}");
```

It prints one line per labelled collection:

```text
EventualConsistency at reliable_broadcast.rs:130 (Cluster(loc2v1), AssertIsConsistent, minted): VIOLATED by {crash(source after reliable_broadcast.rs:130[→1]:2)}, 5 hypotheses
EventualConsistency at reliable_broadcast.rs:156 (Cluster(loc2v1), AssertIsConsistent, minted): VIOLATED by {crash(member@0 after reliable_broadcast.rs:156[0→1]:2)}, 3 hypotheses
EventualConsistency at reliable_broadcast.rs:149 (Cluster(loc2v1), Unique, delivered): CERTIFICATE, 36 hypotheses, exhaustive
```

The first is the source's initial broadcast, violated by the source's crash between two sends: the same failing run as the first example, except that member 2 now receives `2` through the echoes, so only the hop itself disagrees. The second is the members' echo broadcast, violated by an echoing member's crash. The third is the delivered set the program exposes, certified under exhaustive schedules: the echo repairs what the sender's crash leaves missing. The two violations are not defects. Reliable broadcast builds a reliable delivered set out of an unreliable single-hop broadcast and an unreliable echo, and "inner collections violated, delivered set certified" is the search showing the protocol doing its job.

![Lamport diagram: member 0 crashes after echoing to member 1; its echo of 2 to member 2 is lost](molly-viz-samples/ec/ec_reliable_broadcast_inner_echo/failing.png)

*`reliable_broadcast.rs:156`, the members' echo, keyed by echoer. Member 0 echoes `1` and `2` to member 1, dies, and its echo of `2` to member 2 is lost. Member 1 holds (member 0, `2`) and member 2 does not, while the merged delivered set is complete at both. The echo repairs the merged set, not the keyed record of who echoed.*

The third line exists because of the `weaken_consistency().sim_cluster_output()` call, which strips the label from the type and registers the stream as a per-member output of the simulated cluster. Without it only the two minting nodes are reported. The certificate was 10 hypotheses when this test carried `.crash_budget(1)`; under the default bounds the search also runs every admissible pair (a crash point of the source together with one member's crash), and each pair survives and adds a hypothesis.

`results.verdicts` holds the same information as values, one `LabelVerdict` per collection: `Searched(outcome)` or `NotChecked(reason)`. A test asserts on it per site (`results.at("file.rs:149")`), overall (`results.all_certified_or_not_checked()`), or by iterating `results.counterexamples()`. Each verdict includes either the faults and failing schedule, or the successful search's scope, grade, hypothesis count, and exploration mode.

To check one collection by hand, build the specification with `Spec::new`, name the collection with `spec.observe_cluster(collection)` and the members with `spec.correct_members(&cluster, 0..n)`, call `spec.eventual_consistency(observed, correct)`, and run `Ldfi::search`. The search is the same.

The remaining examples of the corpus, one per violated collection with its failing run, are in `hydrofrac-ec-corpus.md`.

### What the test sets

Besides the flow and the body, an `all_labels` test sets four things on `Ldfi::new()`. The builder has other methods; they are tuning and measurement knobs, described in `hydrofrac-retrospective.md`.

- **Which nodes may crash.** `.crashable("name", &process)` and `.crashable_cluster("name", &cluster, 0..n)` register the nodes a hypothesis may crash. The default, nothing crashable, is the absence of a declaration: the search is omissions-only until the test says otherwise. The test does not say how many registered nodes may crash at once; by default the search tries every combination, minus the ones that cannot violate anything. `.crash_budget(k)` exists for a protocol with a stated tolerance where the unbounded search is impractical; its only use in the corpus is multi-Paxos, with `f = 1`. See "Crash bounds" below.

- **Which schedules.** `.exploration(Exploration::Exhaustive)`, the default, explores every schedule the simulator can produce under each hypothesis; a certificate from it is a proof over that finite space. `Exploration::Fuzz { instances }` samples `instances` schedules per hypothesis, and the result is then "no violation found", never a certificate. Multi-Paxos, ISR and ABD use `Fuzz { instances: 8 }` because exhaustive exploration of a leader-election run is out of reach; reliable broadcast and 2PC are exhaustive.

- **Which lineage.** Compile the program `.with_provenance()` and leave `.level()` at its default, `Level::Precise`, which reads the deliveries an output depended on from provenance recorded on each record. `Level::Execution` attributes every output to every input of the execution that produced it: coarser, more hypotheses, same verdicts. Without `.with_provenance()` the search still runs at `Precise`, recovering the same information by replaying a node with a delivery removed, which is slower.

- **How long to search.** `.max_hypotheses(n)`, default 256. The search ends with a counterexample, an infeasible solver (certificate), or this budget; the third is `Outcome::Budget`, which says nothing about the protocol and means "raise it or shrink the test". In the corpus it was hit only by Raft and by noisy single-schedule Paxos runs.

Building with the `ldfi` feature needs cmake and a C++ toolchain for the HiGHS solver bindings (`good_lp`, built from source).

The body determines which inputs arrive, which timers fire, and how long the protocol has to converge. For protocols that depend on retransmission, gossip, or leader changes, the body must fire the appropriate timers after `run.end_of_finite_failures()`; see "When faults may happen" below.

### Which collections are checked

A **labelled collection** is a node in the program's IR where the `EventualConsistency` label is *minted* or where it *exits* the labelled region. The label is a type parameter of the live collection, part of `Stream<T, L, B, O, R>`'s type, and the compiler records each node's resulting type in its IR metadata (`metadata().consistency`), so the search can walk the IR and pick out the nodes whose output carries the label while none of their inputs do (`sim::observe`).

A label is minted at a node with no labelled input. It enters the type at two kinds of operator:

- A network fan-out whose output location is `Cluster<_, N::ConsistencyGuarantee>`, over a transport that grants the label:
  - `broadcast_closed` in `hydro_lang` (process → cluster and cluster → cluster; `live_collections/stream/networking.rs`);
  - `fan_out` and `fan_out_from_process` in `hydro_std::ec_inference_demos`, and their clients `broadcast_live` and `broadcast_live_from_process`.

  The granting transports are `TCP.fail_stop()`, `TCP.lossy_delayed_forever()` and `UDP.lossy_delayed_forever()`. Plain `lossy` grants `NoConsistency`; `send`, `demux`, `round_robin` and the membership-snapshotting `broadcast` grant nothing (`hydro_lang/src/networking/mod.rs`).
- A programmer's `assert_has_consistency_of` with a proof. Each fan-out is this underneath: a `demux` followed by a library `manual_proof!`.

The minting point is where the type system's assumption about faults lives.

A collection exits the labelled region at a consumer that does not carry the label (`weaken_consistency`, a `batch` into an unlabelled tick) or at a program output. A send to another location is not an exit: the collection continues there under the transport's own rule. The exit is what the program delivers under the label.

Both are reported because they can disagree, as reliable broadcast's minted single hop and certified delivered set show above. Only the search distinguishes the two.

The operators between a mint and an exit inherit the label and are not listed. A `Tee` and a label-preserving `Cast` stand for the collection they share. Nodes at the same source site, of the same operator, on the same cluster are one collection; when there is one node per cluster member, the collection is their union.

The generated specification applies to an unbounded stream (or keyed stream, taken as pairs) at a cluster's top level that a network channel feeds. Everything else is reported with a reason and not searched:

- **Local inputs only.** No fault in the model can withhold an input, so there is nothing to search.
- **Not a stream.** A `Singleton`, `Optional`, or `KeyedSingleton` is reported, not checked, today; the planned treatment checks its input stream rather than its value (next steps, item 1).
- **Inside a tick** or an atomic region. The per-tick contents are not the collection's.
- **Excluded by the test** through `Spec::all_labels_except`. Observing a collection shares and serializes its elements, so its element type must be `Clone + Serialize`. Labelled cluster collections nearly always are, being built from network sends; one that is not fails the generated crate's compilation and is excluded by hand.

`Ldfi::search_all_labels` runs one search per checked collection against the shared body. Before each search it tells the observer which collection the search is about and which members of that collection's cluster the hypothesis leaves correct, then clears the fact domain and runs the loop below unchanged. The result is `LabelResults`: one `LabelVerdict` per collection.

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

The node performs sends through the named message. Later sends are dropped, and its external outputs are suppressed. Naming the crash point by a message rather than a send count makes the hypothesis usable across schedules. It also allows hydrofrac to crash a sender between two sends in a single broadcast fan-out, which is how every single-hop `broadcast_closed` in the corpus is violated.

Only nodes registered with `.crashable` / `.crashable_cluster` may crash. Crashes are declared by the test and omissions are not because of where the information lives: a `lossy_*` transport is the program's statement that messages on that channel may be dropped, while nothing in the program says whether the client, ZooKeeper or the coordinator is part of what is being tested. The corpus harnesses hold such senders correct so the search is about the protocol, then run them crashable separately to watch the premise fail.

#### Crash bounds

How many registered nodes crash in one hypothesis is not the test's to say. The specification says what the protocol must uphold; the nodes that may fail are the test's declaration; how many fail at once is something the protocol survives or does not, and the search's job is to find out. `Ldfi::new()` therefore sets no crash budget (`crash_budget` is an `Option<usize>`, `None` by default) and tries fault sets weakest first: fewest faults, then a crash after a later message before one after an earlier message. Before the crash-budget change (1538508755) every corpus harness set `.crash_budget(1)` by convention, which can hide bugs: 2PC with cooperative termination certifies under one crash and breaks under two.

Two families of fault sets cannot violate anything and are excluded as constraints of the hitting-set program rather than run:

- **Every node of the program crashing.** `Σ crashes ≤ N − 1` over the program's `N` nodes, counting every process and cluster member but not the specification's observer (`CompiledSim::node_count`). It binds only when everything is crashable; with a non-crashable node left, crashing all the crashable ones is admissible.
- **All or all but one of the checked cluster's members crashing.** For the generated specification over a cluster `C`, `Σ crashes of C's crashable members ≤ |C| − 2`. One survivor agrees with itself. The bound applies only to the cluster the checked collection lives on (the collection's cluster under `search_all_labels`, the `correct_members` cluster under `Spec::eventual_consistency`); a crash anywhere else withholds inputs, which is what the specification is about. Hand-written specifications get no such bound.

Every other vacuous combination is found by running it: a hypothesis whose every schedule is vacuous is recorded, and none of its supersets is proposed. The search prints the bounds it works under as one `ldfi: crash bounds:` line before the first hypothesis.

The vacuity rule had a gap that the unbounded search exposed: the vacuous set was computed before derivations, so it saw a failed hand-written `PRE` but not the generated specification's vacuity (no correct member holds any fact of the domain), and every superset of "the proposer crashed before anything was chosen" was enumerated. The vacuous set is now taken from the schedules after derivation ([ldfi-level-ablation.md](ldfi-level-ablation.md#unbounded-crash-budget-with-vacuity-constraints)).

The cost of removing the default budget, measured once per test: no verdict changed and every counterexample is still a single fault at the same hypothesis count, while certificates grew where the protocol survives two crashes (reliable broadcast's delivered set from 10 hypotheses to 36 under `all_labels`, uniform broadcast from 28 to 105). Multi-Paxos's `learned` set, with seven crashable nodes, did not finish within 2,000 hypotheses unbounded; its tests keep `.crash_budget(1)`, the protocol's own stated fault model (`f = 1` of three acceptors), which is the kind of claim `.crash_budget` is for.

#### When faults may happen

**Every failure happens at a time `≤ EFF`.** This is Molly's fault model, and the simulator's filter enforces it for both kinds of fault. The body may declare the end of finite failures once, with `run.end_of_finite_failures()`; from then on omissions stop applying and no crash point fires. The execution after `EFF` is failure-free recovery, the window in which the protocol is expected to converge.

A crash point is named by a message that was sent before `EFF` in the run that proposed it. In another run the same message may be sent only after `EFF`, or never; the crash then does not happen in that run (a node already dead stays dead: crashes are permanent). The search's account of what a crash kills (`Dependency::killed_by`) follows the simulator exactly: a crash after `m` kills every later send of the node, before or after `EFF`, provided `m` was sent before `EFF` in that run.

The call is optional, and neither harness in "Using hydrofrac" makes it. Without it every send of the run is a fault candidate, which is right for a closed broadcast, since it has nothing to recover with. A protocol that converges by retrying needs the cut: with none, the adversary may drop every retransmission within the run, and no retry-based protocol could certify. Where the body places it decides which messages the search can lose.

The ISR harness's body (`isr_replication.rs`, the `body` function) is the shape to copy. The `run` it receives is the `Run` handle every body gets as its argument, the `_run` the earlier examples ignore:

```rust
async |run| {
    for w in 1..=writes {
        h.write_send.send(Write(w));           // the client's writes
    }
    hydro_lang::sim::quiesce().await;
    for round in 0..rounds {
        h.replica_timer.send_many((0..members).map(|m| (m, ())));   // replicas heartbeat
        hydro_lang::sim::quiesce().await;
        h.zk_timer.send(());                   // ZooKeeper recomputes the view
        hydro_lang::sim::quiesce().await;
        if round == 0 {
            run.end_of_finite_failures();      // faults may fall anywhere up to here
        }
    }
}
```

Everything before the call, the writes and the first round of replica and ZooKeeper timers, is where crashes and omissions may land; the remaining `rounds − 1` rounds run failure-free and are the protocol's chance to recover. A harness has to place `EFF` late enough for the failures it means to admit. With `EFF` right after the client's write, the leader's crash after the acknowledgement is not an admissible failure, and the unclean-election bug is outside the bounds.

The program's transport declarations therefore determine what can go wrong, while the test determines which nodes may crash and when faults must stop.

### Lineage explains successful outcomes

Each run records:

- Executions of asynchronous code and ticks.
- Sends and their fates.
- Receives and the deliveries they consumed.
- The executions that produced observed outputs.

Starting from a successful specification output, hydrofrac follows these dependencies backwards to the deliveries that supported it. That backward closure is its derivation.

This is the central LDFI idea. A forward search asks, “What happens if I drop this message?” A lineage-driven search first asks, “Why did this run succeed?” It then chooses faults that could remove the explanation.

Suppose the probe succeeds because a source sends directly to every member. Hydrofrac tries a crash that interrupts that fan-out. If the remaining members repair the missing delivery through an echo, the next successful run records the echo path. A breaking hypothesis must now defeat both known ways of succeeding.

Hydrofrac has two lineage oracles, selected by `Level`:

| Level | Dependencies recorded |
|---|---|
| `Execution` | Every input to the execution that produced the output, and to earlier executions of the same tick (persisted state). Sound but coarse; available for any program. |
| `Precise` | Record provenance where the program carries it, counterfactual replay where it does not; falls back to execution lineage only when replay is inconclusive. The default. |

Record provenance is added by an IR rewrite through `SimFlow::with_provenance`: each element carries the delivery ids it was computed from. An element that came out of a closure the rewrite cannot see into carries an opaque mark instead; at such a record hydrofrac derives the step by **counterfactual replay** of the node alone (its recorded deliveries removed one at a time, keeping those without which the node no longer produces the record).

A program compiled without `with_provenance()` (because the rewrite does not support one of its operators, or by choice) has no tags at all, and `Precise` treats every record of it as opaque: every send and output is derived by replay. Replay is thus an oracle inside `Precise`, not a level of its own: measured as a standalone level it produced the same derivations as record provenance wherever record provenance applied, at two to ten times the cost (`ldfi-level-ablation.md`). A static-slice narrowing of the execution closure was also measured and removed: it never separated a record from the inputs of its execution on any protocol in the corpus.

When replay cannot produce a usable result, hydrofrac falls back: a replay that does not reproduce the node's output (or panics), a node with no scripted inputs to replay against, or a search with no replay oracle goes to the record provenance where there is any, and from there to the execution closure; the search counts these fallbacks per schedule. `Ldfi::new()` defaults to `Precise`; on a program compiled without `with_provenance()` the search logs that it is replaying every record.

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

`Spec::all_labels` leaves the observer's input as a slot to be filled at compile time. When the flow is compiled for the simulator, a pass over the IR (`sim::observe`) lists the labelled collections and taps each one in the specification's class. The labelled node becomes a `Tee`, as a `.clone()` would make it. The second branch is mapped to `(collection index, bincode of the element)` and sent to the observer over a `fail_stop` channel, exactly the IR a `.send()` to the observer would build. The slot becomes the union of the taps.

The pass runs before network compilation and before the provenance rewrite, so the taps are compiled and traced like any other operator and lineage flows through them. A `Tee` branch is invisible to the rest of the program, whose own behaviour is unchanged.

## Judgements

The distinction between a certificate and a successful test run should be explicit.

| Verdict | What it is worth | What it does not imply |
|---|---|---|
| **Certified** (`Outcome::Certificate`, `Grade::Generated`, `Exploration::Exhaustive`) | No admissible fault set, under any schedule, breaks the generated check within this test's topology, workload, horizon, crash bounds and `EFF`. | Anything about other inputs, cluster sizes or longer horizons; convergence beyond the horizon. |
| **Violated** (`Outcome::Counterexample`) | A concrete fault set and schedule under which some fact of `FACTS` is present at one correct member and absent at another at the end of the run. One failing run suffices; exhaustive exploration is not required. | That the protocol is wrong rather than the horizon too short; the trace must be read. |
| **No violation under Fuzz** | `instances` sampled schedules per hypothesis passed and no hitting set remained. Evidence. | A certificate; a violation in an unsampled schedule is missed. |
| **Budget** (`Outcome::Budget`) | The hypothesis cap was reached. | Anything about the protocol. |
| **Vacuous** (`Outcome::Vacuous`) | The failure-free probe collected no facts: the harness protects nothing. | Anything about the protocol; it is a statement about the body. |
| **Not checked** (`LabelVerdict::NotChecked(reason)`) | The collection is outside the generated specification's class: local inputs only, not a stream, inside a tick, excluded by the test. | Silence: the reason is printed, and nothing fails for a label the test does not assert on. |

Every certificate on the `all_labels` path carries `Grade::Generated`. The grade follows the specification, not the search: `Spec::eventual_consistency` has by construction the form the hitting-set inference is sound for, a positive obligation over a fixed fact domain that no extra observed fact can undo. `Grade::HandWritten`, printed RELATIVE, is the grade of specifications the test writes itself; `all_labels` never produces it, and "Hand-written specifications" below says what it is worth. `Grade::Goal` belongs to the superseded goal API. `Strategy::Blind` ends in `Outcome::Exhausted` rather than a certificate.

For complete EC, certification combines with the existing type-level reasoning. The type system establishes consistency given the same inputs; hydrofrac supplies bounded evidence that the protocol's fault handling preserves the required dissemination. Neither result substitutes for the other.

## Results on existing Hydro code

The existing tests cover every producer of the `EventualConsistency` label in the codebase and the set-valued, cluster-resident labelled outputs of `hydro_std::ec_inference_demos`. Tests are in:

- `hydro_test/src/cluster/ec_ldfi.rs`
- `hydro_test/src/cluster/ec_demos_ldfi.rs`

The searches below run under the default crash bounds (every combination of crashes of the registered nodes, minus the two excluded families: every node dead, and fewer than two correct members of the checked cluster), except multi-Paxos, whose test states the protocol's own fault model, one crash, because the unbounded search does not finish within 2,000 hypotheses ([ldfi-level-ablation.md](ldfi-level-ablation.md#unbounded-crash-budget-with-vacuity-constraints)). Hypothesis counts include the probe. Under `Fuzz`, the count varies by one between runs; where two runs differed, both are given. The per-example gallery, with each violated collection's failing run, is `hydrofrac-ec-corpus.md`.

| Program | Result |
|---|---|
| `broadcast_closed`, process → cluster, `fail_stop` | **Violated**, 5 hypotheses. `crash(source after bcast[→1]:Item(2))` interrupts a fan-out. |
| `broadcast_closed`, process → cluster, `lossy_delayed_forever` | **Violated**, 2 hypotheses. `omit(bcast[→0]:Item(1))` loses an item that is never retransmitted. |
| `broadcast_closed`, cluster → cluster, `fail_stop` | **Violated**, 5 hypotheses. `crash(source@0 after bcast[0→1]:Item(10))`. |
| `broadcast_live_from_process` | **Violated**, 5 hypotheses. A source crash interrupts the one-hop broadcast. |
| `leader_merge_broadcast` | **Violated**, 2 hypotheses. A leader crash interrupts dissemination of `(writer 0, 100)`. The demo documents leader survival as an assumption. |
| Echo reliable broadcast, `hydro_test` version | **Certified**, 21 hypotheses, exhaustive schedules (10 under one crash). |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | **Certified**, 36 hypotheses each, exhaustive schedules (10 under one crash). |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | **No violation found**, 105 hypotheses each, 8 sampled schedules per hypothesis (28 under one crash). |
| `g_set_gossip`, `g_set_gossip_live` | **No violation found**, 13 hypotheses each, 8 sampled schedules per hypothesis. |
| `multi_paxos`, learners' `learned` output; 1 proposer, 3 acceptors, 3 learners, one crash | **No violation found**, 24–25 hypotheses, 8 sampled schedules per hypothesis. Learners rebroadcast learned values; this check exercises that dissemination path. |

The distinction is straightforward in this corpus. Single-hop dissemination fails when one crash or omission interrupts delivery. Protocols with echo or gossip mechanisms pass the searches performed so far, with exhaustive certification for the smaller schedule spaces.

This exposes a gap in transport-level justifications for EC. Treating a failed connection as a failed recipient does not account for a sender that dies halfway through a fan-out. Treating a dropped message as indefinitely delayed does not make surviving recipients converge within a recovery window. A transport label alone cannot establish complete EC for the program using it.

Some other labels do not introduce an independent network fault question. Algebraically justified folds preserve consistency given their inputs. `quorum` and `epoch_splice` are EC-preserving combinators whose dissemination assumptions come from their upstream inputs. `synod` has no `EventualConsistency` label. ABD's replica register does have an EC claim, but it is a `Singleton`, reported and not checked today; it is to be checked at its input stream (next steps, item 1).

### Every label of multi-Paxos

`Spec::all_labels` over `multi_paxos` reports ten collections. The run has 1 proposer, 3 acceptors, and 3 learners, all crashable, with one crash allowed (the protocol's fault model, stated by the test; see above) and `Fuzz { instances: 8 }`. The body tells proposer 0 to lead round 1, quiesces, sends commands 7 and 8, and quiesces. Line numbers are into `hydro_std/src/ec_inference_demos/multi_paxos.rs` unless named otherwise.

| Site | Collection | Result |
|---|---|---|
| `:281`, acceptors, minted and delivered | `broadcast_closed` of prepares, proposer → acceptors. Its `.entries()` goes straight into the acceptor's tick batch (`:303`), so the mint is also the exit. | **Violated**, 5 hypotheses. `crash(proposer@0 after :281[0→1]:Ts { round: 1, writer: 0 })`: the proposer dies between the prepares to acceptors 0 and 1. |
| `:549`, acceptors, minted | `broadcast_closed` of phase-2 accepts, proposer → acceptors. | **Violated**, 8 hypotheses. `crash(proposer@0 after :549[0→1]:(Ts { round: 1, writer: 0 }, 0, 1, Some(8)))`: the accept for slot 1 reaches acceptor 0 only. |
| `:286`, acceptors, delivered | The `forward_ref` that closes the accept cycle; the same stream as `:549`'s entries, exiting at `use::batch(accepts_fwd, ..)` (`:308`). | **Violated**, 8 hypotheses, by the same crash. Nothing between the broadcast and the batch retransmits. |
| `:569`, proposers, minted | `broadcast_closed` of chosen slots, proposer → proposers. | **No violation found**, 5–6 hypotheses, 8 sampled schedules per hypothesis. The proposer cluster has one member, so agreement holds trivially, and the crash bounds never crash it (all but one member of a one-member cluster is none); the acceptor crashes that remain are tried once each. |
| `:570`, proposers, delivered | The `.values()` of `:569`, exiting at `weaken_consistency()` (`:571`). | **No violation found**, 5–6 hypotheses, 8 sampled schedules per hypothesis; trivial for the same reason. |
| `:582`, learners, minted | `broadcast_closed` of chosen decrees, proposer → learners: the first hop of the learner echo. | **Violated**, 10–11 hypotheses. `crash(proposer@0 after :582[0→1]:(1, 0, 0, Some(7)))`: slot 0 reaches learner 0 only. This is the premise the echo repairs. |
| `:591`, learners, minted | `broadcast_closed` of the echo, learners → learners, keyed by echoer. | **Violated**, 9–10 hypotheses. `crash(learner@0 after :591[0→1]:(1, 0, 0, Some(7)))`: learner 0 dies partway through echoing slot 0, and under its own key that echo is present at some correct learners and absent at others. |
| `:588`, learners, delivered | `learned = initial.merge_unordered(echo_fwd).unique()`, exiting at the test's `weaken_consistency()`. | **No violation found**, 25–26 hypotheses, 8 sampled schedules per hypothesis. Every learner that receives a decree echoes it to every learner; one crash leaves at least one echoer per decree. |
| `:608`, learners, delivered | The `SpliceFact` `flat_map_unordered` of `learned`, exiting into the fold at `epoch_splice.rs:190`. | **No violation found**, 25–26 hypotheses, 8 sampled schedules per hypothesis. A function of `learned`, element by element. |
| `epoch_splice.rs:190`, learners, minted | The `fold` with `commutative = manual_proof!` into `Singleton<SpliceState>`. | **Not checked**: not a stream. |

The `.values()` directly after `:281` is a `Cast` and shares `:281`'s collection, so it is not listed. The automatic search of `learned` took 25–26 hypotheses against the manual test's 24–25: under `all_labels` the observer carries every label's facts and selects by index, so the probe's derivation is larger.

The pattern is the one reliable broadcast shows above. A minted single-hop `broadcast_closed` is violated by the sender's crash mid-fan-out (`reliable_broadcast.rs:130`; here `:281`, `:549`, `:582`), because the transport's grant assumes the sender finishes. The delivered set after an echo cycle passes (`reliable_broadcast.rs:149`; here `:588`, `:608`). The receiving side of the echo itself, a cluster → cluster broadcast keyed by echoer (`reliable_broadcast.rs:156`; here `:591`), is violated by an echoer's crash: the echo repairs the merged set, not the keyed record of who echoed. One verdict per label separates the transport's assumption from what the program delivers under it.

### Every label of ISR replication, two-phase commit, and ABD

The hand-written harnesses described below also run under `Spec::all_labels` (`isr_replication.rs` and `two_pc_ldfi.rs`, `*_all_labels` tests; `abd.rs` in `hydro_std`), under the default crash bounds. The senders those harnesses hold correct (the client and ZooKeeper; the coordinator) are also run crashable, since a `broadcast_closed` from a process has no premise other than the sender's survival.

| Program, fault model | Site | Result |
|---|---|---|
| ISR (3 replicas, 4 rounds, `Fuzz { instances: 8 }`), replicas crashable | `:343` write broadcast, client → replicas (minted and delivered); `:360` replication `forward_ref` at the replicas (delivered); `:564` view broadcast, ZK → replicas (minted) | **No violation found**, 4 (`:343`) and 7 (`:360`, `:564`) hypotheses. No crashable node sends on these channels. |
| ISR, client and ZK crashable too, `EFF` right after the write | `:343` | **Violated**, 2 hypotheses. `crash(client after write[→1]:Write(1))`. |
| | `:564`, `:360` | **No violation found**, 5 hypotheses each. Every ZK send lies after `EFF`, so no ZK crash point is admissible within the bounds the body sets. |
| ISR, client and ZK crashable, `EFF` after the first round | `:343` | **Violated**, 2 hypotheses, by the client's crash as above. |
| | `:564`, `:360` | **Violated**, 8 hypotheses each. `crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })`: ZK dies between two sends of one view, and the replication cycle carries the difference. |
| 2PC with cooperative termination (2 participants, 1 transaction, exhaustive), coordinator and participants crashable | `:86` prepare broadcast (minted and delivered) | **Violated**, 2 hypotheses. `crash(coordinator after prepare[→0]:Txn(1))`. |
| | `:92` commit broadcast (minted and delivered) | **Violated**, 2 hypotheses. `crash(coordinator after commit[→0]:Txn(1))`. |
| | `:98` peer `forward_ref`, `:161` peer broadcast | **Vacuous.** The failure-free run carries no peer traffic: the termination timers fire after every decision is known, so no participant queries a peer. |
| 2PC, coordinator held correct | `:86`, `:92` | **Certified**, 1 hypothesis each: with two participants, any crash leaves a single correct member, so no fault set is admissible and the failure-free run is the certificate. `:98` and `:161` vacuous as above. |
| ABD (3 replicas, 1 client, one write, `Fuzz { instances: 8 }`), replicas and client crashable | `:141` query broadcast, `:150` phase-2 `forward_ref`, `:161` the writes filtered from it, `:314` write broadcast | **Violated**, 2 hypotheses each, by the client's crash between two sends of its query or its write. Every stream the replicas label is a single hop from the client. |
| | `:165` the register `fold` | **Not checked**: not a stream. |

The vacuous rows are statements about the body. A termination protocol whose premise is never exercised in the failure-free run gives the specification nothing to protect, and the search says so rather than certifying. A collection minted inside a tick (`hydro_test`'s `ec_all_labels_classes.rs`: a `batch` of a broadcast into the members' tick, labelled there by `assert_has_consistency_of`) is reported `not checked: inside a tick`, while the broadcast before the tick and the `all_ticks` after it are searched.

## Hand-written specifications

Hydrofrac can also check properties that Hydro's types do not express: agreement, durability, "the leader has every acknowledged write". The test writes the property as a Hydro flow, and `Ldfi::search` runs the same search over it. Because hydrofrac is then checking a view it did not construct, three things change: the grade on the certificate, the soundness caveat behind the grade, and the weight that falls on where the body ends finite failures.

### Writing one

`Spec::new(&mut flow)` adds an observer process to the program. The streams the property mentions are shipped to that observer with `spec.observe(stream)` (from a process), `spec.observe_cluster(stream)` (from every member, tagged with the member) or `spec.observe_sequenced(stream)` (keeping arrival order). Over those streams the test builds two bounded `Optional`s in the observer's tick and registers them as `spec.pre(..)` and `spec.post(..)`:

- **`PRE`** is the precondition that says the run is one the property speaks about. If it is empty, the run is ignored. A `PRE` that never holds in the failure-free run is `Outcome::Vacuous`, a verdict about the harness.
- **`POST`** is the conclusion. A run in which `PRE` is non-empty and `POST` is empty is a counterexample.

A view holds if it is non-empty at the end of the run, as a Datalog goal holds at fixpoint. `spec.correct_members(&cluster, 0..n)` gives the set of members the current hypothesis did not crash, for properties quantified over survivors.

The ISR durability check (`hydro_test/src/cluster/isr_replication.rs`) is the pattern in full. `PRE`: the client's first write was acknowledged and a final leader exists (the leader of `observe_sequenced(views).max()`). `POST`: that leader is among the replicas that appended the write. If a crash lets a replica that never saw the write win the election, `PRE` holds, `POST` is empty, and the search reports the crash.

The older goal API (`goal`, `goal_all_members`, `goal_witnessed`) still compiles but is superseded by `pre`/`post`.

### What a hand-written certificate is worth

A certificate from a `pre`/`post` specification is `Grade::HandWritten` and prints as **Certified, RELATIVE**. What it says: no hitting set remained; the recorded derivations persisted under every excluded fault set. What it does not say: that the inference was sound for this view.

The hitting-set inference ("a fault set that leaves this derivation intact cannot break the view") is valid only for views monotone in the facts they observe. The generated specification has that form by construction (see "Why the specification's form matters"). Hand-written agreement and durability do not: agreement is the *absence* of a conflict, and durability of a write rests on the *final* leader view. A hypothesis can break either by adding a fact, removing nothing any clause names. A hand-written `POST` falsified by an extra fact can therefore be missed, and the grade says so.

The concrete lesson was a false certificate. When the observer-level counterfactual was first used alone, ISR's durability `POST` read the final leader off `observe_sequenced(views).max()`, and deleting views latest-first never changed the value, since the earlier view names the same leader. The derivation held the appended write and the initial view, with no heartbeat in its lineage. The breaking hypothesis (lose a replication, crash the leader after the acknowledgement, let ZK promote the stale follower) kills neither; it *adds* a later view. Six hypotheses, certificate, bug present.

Record provenance, which folds every contribution of an aggregate into its value, kept the views and the heartbeats behind them, and the observer's derivation is now the **union** of the counterfactual and recorded derivations: sound where either is, minimal only where both agree (`history-hydrofrac-negation.md`). The search finds the corpus's non-monotone bugs because the breaking hypothesis also displaces a recorded fact in `POST`'s derivation; a conflict that appeared without displacing one would not be proposed.

### Bounding failures

The `EFF` rule of "When faults may happen" carries more weight here. Finite failures is the fairness assumption that makes the convergence question answerable at all, and the body has to fire the protocol's timers after the cut or it has not asked the question: four Raft bodies failed correct Raft this way before the fifth ([history-hydrofrac-development.md](history-hydrofrac-development.md#9-what-it-has-found)).

Where the cut falls decides which messages the search can lose. The ISR harness's cut was moved past the first round so the acknowledgement lies inside the window in every world. The Paxos harness's cut was moved from before the new leader's first ballot to after it, which made the P1a/P1b exchange faultable, grew the correct variant's certificate from 196 to 372 hypotheses, and exposed a production defect the old cut had hidden ([history-hydrofrac-development.md](history-hydrofrac-development.md#92-paxos-and-two-defects-the-simulator-found-on-the-way)). The harness that cannot lose a message cannot test the protocol's handling of its loss.

Whether a point on the body's timeline is the right form of the fairness assumption is an open question, discussed in `hydrofrac-retrospective.md`.

### What these harnesses have found

These interfaces support checks for committed-entry durability in Raft and Paxos, two-phase commit atomicity, ISR replication agreement, quorum counting, and retry-until-ack delivery. They have found substantive defects:

- An injected Raft election-restriction bug, using index-only log comparison, was found in 90 hypotheses with four omissions and no crash. The scenario had not been written down by the author. Correct Raft completed a relative certification search in 235 hypotheses under the same bounds.
- Production Paxos counted P1b replies rather than distinct acceptors. The defect became visible after the test moved `EFF` past the first ballot. Correct Paxos, the Paxos key-value store, and compartmentalized Paxos completed relative certification searches in 372, 301, and 568 hypotheses.
- Simulator exploration also exposed two non-fault defects: `collect_quorum_with_response` was not a function of its input despite its `nondet!` justification, and an idempotence-justified `fold` lacked a simulator implementation of its retry-only cast. Both were fixed.

The larger tests are ignored by default for runtime. The Raft bug and correct-version searches take roughly thirty minutes and an hour; the three corrected Paxos searches take about forty-five minutes together. The failing runs of the 2PC, ISR, Paxos and reliable-delivery harnesses are drawn in the gallery under `molly-viz-samples/` (see `molly-viz.md`).

## Limitations

**Hydrofrac runs only in the simulator.** The fault filter is a simulator artefact, not an IR node, so nothing exists under `deploy`. Embedded channels are not fault points.

**A certificate covers a test, not all executions.** Changing the workload, cluster size, timers, or horizon requires another test. Exhaustive fault search does not generalize those choices.

**Sampled schedules do not certify.** Uniform broadcast, gossip, and multi-Paxos currently use schedule sampling because their schedule spaces are large.

**Convergence is checked at a finite horizon.** Hydrofrac cannot distinguish “never converges” from “has not converged yet” using the final contents alone. Test bodies must provide a realistic recovery window.

**Scalar collections are reported, not checked.** A `Singleton`, `Optional`, or `KeyedSingleton`, such as ABD's max-by-timestamp register, is reported with the reason and not searched today. The planned treatment checks the stream the scalar is computed from, not its value (next steps, item 1).

**Membership is static in these tests.** The simulator starts with all members joined. Tests of `_live` protocols exercise faults, not join timing.

**Precise lineage has gaps.** The provenance rewrite does not handle `entries_partially_ordered` on keyed streams, so `leader_merge` is compiled without provenance and searched at `Level::Execution`. A counterfactual replay that does not reproduce the node's output is treated as inconclusive for that step and falls back to the recorded lineage.

**Not every labelled collection is in the class.** The automatic specification checks unbounded streams at a cluster's top level that a network channel feeds. Lattice values, collections inside a tick, and collections with local inputs only are reported with the reason and not searched. Observing a collection requires its element type to be `Clone + Serialize`; one that is not must be excluded with `Spec::all_labels_except`. The verdicts do not fail a test by themselves: `LabelResults` is what a test asserts on, and nothing fails for a label the test does not look at.

**The fault vocabulary is limited.** Omission and crash only: no delay, no reordering (that is the schedule explorer's dimension), no Byzantine behavior, no duplication. There is no separate partition primitive, although omission sets on lossy channels can represent partitions. The Raft counterexample uses four omissions on two links.

**Cost.** On an 8-core debug build, ISR unit searches take seconds to half a minute, exhaustive echo broadcast at `n = 3` about a minute, and the hand-written Raft and Paxos searches half an hour to an hour each. The regression suites are `cargo test -p hydro_test ldfi -- --test-threads=1` (about 12 min, long certifications ignored) and `cargo test -p hydro_lang --features ldfi --lib sim`.

## Next steps

1. **Check scalar collections at their inputs.** A `Singleton`, `Optional`, or `KeyedSingleton` is checked at its input, not its value. A labelled scalar is a function of the stream it is computed from, and the type system has already vouched for the function; the only premise left to hydrofrac is that every correct member receives the same inputs. So the tap goes on the labelling operator's input stream, and the verdict is reported under the scalar's site. When that input is itself labelled it is already a row of the table, and the scalar's row says so. ABD's register (`abd.rs:165`, asserted over an unlabelled stream) is the first case. Checking the value itself ("final values equal") is not built and is not planned: it re-proves the type system's conclusion, which the certificate does not claim.
2. **Support explicit convergence witnesses.** Protocols that report convergence should be able to use that signal in the specification rather than rely only on a fixed horizon.
3. **Add decision-trace replay.** Record nondeterministic decisions and replay a prefix with one decision changed. This would provide finer counterfactual analysis than node-local replay.
4. **Run Raft's `committed` output under the generated specification.** The harness exists; the search takes about an hour.
