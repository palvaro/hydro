# Hydrofrac

(_Lineage-driven fault injection for Hydro. The name is a nod to hydrofracturing: pump faults in under pressure and see whether anything cracks._)

Hydrofrac is a tool for checking whether a Hydro program keeps its eventual-consistency promises when things go wrong. It runs the program in the Hydro simulator, injects crashes and lost messages, and searches systematically for a combination of faults — within bounds the test declares — under which the program's correctness property fails. The search ends in one of two ways: a counterexample, which is a specific set of faults and a specific schedule in which the property fails; or a certificate, which says that no fault set within the bounds falsified the property in any run the search explored.

The word "lineage" is meant literally. Every run is recorded in enough detail to say, for every output the program produced, which earlier events that output depended on: which execution produced it, which deliveries fed that execution, which sends those deliveries came from. The search uses that record to choose the next faults to try, instead of guessing.

The search itself is Molly's (Alvaro, Rosen, Hellerstein, SIGMOD 2015), with the Hydro simulator in place of Dedalus. The difference is who writes the correctness property. Molly's user wrote it by hand. Hydrofrac's user, for eventual consistency, does not: a Hydro programmer who labels a cluster collection `EventualConsistency` has already declared that every member of the cluster ends up holding the same contents. Hydrofrac takes that declaration as the specification, generates the checker from it, and checks it under the faults the program's own transports admit. The programmer writes no checker.

This document describes what exists today. The development record, with the reasoning behind each decision, is `history-hydrofrac-development.md`.

## Why hydrofrac exists

A collection placed at a Hydro cluster carries a consistency level in its location type, `Cluster<'a, Tag, Con>`. The default, `NoConsistency`, says members of the cluster may hold arbitrarily different values. `EventualConsistency` says: every member of the cluster that is given the same inputs ends up holding the same contents.

A collection acquires the stronger label in one of three ways:

- a transport promises it: `broadcast_closed` over a transport whose `ConsistencyGuarantee` is `EventualConsistency`;
- a programmer proves it: `assert_has_consistency_of(manual_proof!(...))`;
- an operator preserves it: a `map`, a `filter`, or a `fold` with a commutative and idempotent combiner, applied to an already-labelled input.

Collections at a single `Process` carry no label; there is only one holder.

The type system takes the label seriously and proves what it can: that given the same inputs, the operators between input and collection produce the same contents. It proves this over the shape of every execution. What the type system cannot see is whether every member *is* given the same inputs. Some executions are missing events: a sender crashes between two sends of one fan-out, or a message on a lossy channel is dropped. In those executions the label's premise is false, and a conditional with a false premise says nothing — the member that missed an input is, as far as the label is concerned, entitled to hold anything at all.

This is not a corner case. As the Results section shows, every single-hop broadcast in the codebase loses the premise to one crash or one omission.

So the premise, when it matters, can only be tested by running the program under faults. That is hydrofrac's job. It takes one concrete execution — a topology and an input workload fixed by the test — and searches the faults the program's own transports admit, within bounds the test declares, for one that leaves a correct member without an input the others have. Whether a missing input turns into a broken promise depends on the program: a protocol that retransmits repairs the gap, a protocol that sends once and moves on does not. Which of the two a program is, the type system cannot see. Hydrofrac runs it and finds out.

## What hydrofrac checks

The specification is generated from the label (`Spec::eventual_consistency`). In words:

1. Run the program once with no faults. Write down every element that ends up in the labelled collection, at any member. Call that set `FACTS`: it is what the program delivers when nothing goes wrong. The restriction to `FACTS` is what "should have been delivered" means — and the test author does not write it down; the probe run does.
2. Run it again with faults. Ignore the members that crashed. The surviving members must agree: no element of `FACTS` may be held by one survivor and missing from another.

That is all. Two things the specification deliberately does not say are worth spelling out.

It does not say that every fact reaches every survivor. A fault can keep an element from entering the system at all — the only sender died before sending it — and then nobody has it. That is not disagreement. The property is that survivors converge on the same set, not that the set is complete.

And it is not phrased on the premise. "No correct member may ever miss an input" would be violated by the first message lost on the first lossy channel, and would say nothing about the program, because losses are exactly what the fault model admits as possible. The specification is phrased on the conclusion — survivors agree — because the answer depends on the program: a protocol that retransmits, echoes, or gossips repairs a lost delivery and converges; a protocol that does not leaves the disagreement there to observe at the end of the run.

Two degenerate cases get their own handling. If the failure-free run delivers nothing (`FACTS` is empty), there is nothing to check, and the search reports `Vacuous` rather than a certificate. If a fault set leaves the survivors holding nothing at all, the specification is trivially satisfied, and that run contributes nothing to the search; it counts neither for nor against the program.

### Certified or incorrect

Every search sorts the labelled collection into one of two piles:

- **Violated.** Some fault set within the bounds, in some explored schedule, left one correct member holding a fact that another correct member never got. The fault set and the failing schedule are printed. This verdict says the program is incorrect with respect to the label: the label on that collection is a promise the program does not keep under the faults its own transports admit. A counterexample is a statement about the program.
- **Certified.** The search ran out of things to try — no fault set within the bounds remains that could break the specification — without finding one. In every run explored, every correct member ended up holding the same contents. A certificate is a statement about the execution: this topology, this workload, these bounds, the schedules explored. (What makes running out of things to try a proof rather than a give-up is the argument under "What a certificate claims".)

Searches can also end without a verdict: `Vacuous` when the probe delivered nothing, and `Outcome::Budget` when the hypothesis budget runs out first (see "How it works").

The complete eventual-consistency obligation on a labelled collection is thus split between two tools, each with the half it can actually discharge:

- The **type system** proves the shape half: given the same inputs, the operators produce the same contents — for every execution, from the program's structure.
- **Hydrofrac** searches the fault half: under the faults the transports admit, within bounds, do the members that stay up still end up agreeing — exhaustively, for one execution.

The type system quantifies over all executions and speaks to shape. Hydrofrac quantifies over all faults within bounds and speaks to one execution. Neither subsumes the other: whether a program retransmits is behavior under faults, invisible to the type system, and executions the test did not run are invisible to hydrofrac. The confidence that a certificate generalizes to other inputs and larger clusters is the confidence a test gives, sharpened by one fact: within the bounds, the search over fault sets was exhaustive rather than random. Generalizing beyond the test's execution is the test author's induction. A counterexample needs no induction.

Taken together — the type system's theorem for all executions, and hydrofrac's search for the test's execution — a certified collection has the complete eventual-consistency story: nothing shape-level can diverge its members, and no fault within the declared bounds and explored schedules made the correct ones disagree.

## Using it

Nothing is required of the program. The test fixes an execution, names the collection, and drives the run:

```rust
let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

let delivered = reliable_broadcast_closed(items, &members);   // labelled EventualConsistency

let mut spec = Spec::new(&mut flow);
let correct = spec.correct_members(&members, 0..3);
let observed = spec.observe_cluster(delivered.weaken_consistency());
spec.eventual_consistency(observed, correct);

let compiled = flow.sim()
    .skip_consistency_assertions()      // the simulator does not yet check them
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

Reading it top to bottom: the test builds a flow with a source process and a three-member cluster, and broadcasts two items with `reliable_broadcast_closed`, whose output carries the `EventualConsistency` label. The `Spec` declares members `0..3` correct and names the labelled collection to observe. `Ldfi` takes the lineage level (`Record`), the nodes that may crash (the source and all three members), the crash budget (one), the specification, and the body. The body is the execution: which inputs, which timers, how long. Here it sends two items and waits for quiescence.

For a protocol whose convergence needs time — retransmission timers, gossip rounds, leader change — the body fires those timers after `run.end_of_finite_failures()`, so the protocol has its window after faults stop taking effect. The result prints either as a counterexample with its faults and failing schedule, or as a certificate with its grade, hypothesis count and exploration.

`Strategy::Blind` runs the same loop over the same fault universe with the lineage switched off (hypotheses are simply the untried fault sets, in increasing order of weight). It exists as the ablation that measures what lineage buys. `Ldfi::fuzz_faults_with` skips the loop entirely and samples fault sets at random, for a baseline on the other side.

## How it works

### The simulator, and the faults it admits

The Hydro simulator compiles a flow into a single-process executable in which every network channel is a queue, every tick is an explicitly scheduled batch, and external inputs and timers are driven by the test body. The simulator's nondeterminism is the batching and ordering of those queues, which it explores either exhaustively or by sampling. Hydrofrac adds a second kind of nondeterminism on top: which sends happen at all. It places a **filter** in front of every network send, with two verbs:

- **Omission** drops a message, identified by channel, sender, destination and wire bytes. Every byte-identical copy on that channel is dropped while finite failures last, so a retransmission carrying the same payload counts as the same message and is dropped too. Available on lossy channels.
- **