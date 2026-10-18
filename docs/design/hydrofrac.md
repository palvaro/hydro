# Hydrofrac: lineage-driven fault injection for Hydro

Hydrofrac is a fault-injection search for Hydro programs. It runs a program in the Hydro
simulator, records why each run produced what it produced, and uses that record to choose the
next faults to try. It ends in one of two ways: a set of faults within declared bounds under
which the program's specification fails, with the schedule that shows it; or a certificate that
no fault set within those bounds breaks any run it observed.

The search is Molly's (Alvaro, Rosen, Hellerstein, SIGMOD 2015), with the Hydro simulator in
place of Dedalus. The difference is who writes the correctness property. Molly's user wrote it
by hand. Hydrofrac's user, for eventual consistency, does not: a Hydro programmer who labels a
cluster collection `EventualConsistency` has already declared that every member of the cluster
ends up holding the same contents, and hydrofrac takes that declaration as the specification
and checks it under the faults the program's transports admit. The programmer writes nothing
further.

This document describes what exists. The development record, with the reasoning behind each
decision, is `history-hydrofrac-development.md`.

## 1. The property

A collection at a Hydro cluster carries one of two consistency levels in its location type,
`Cluster<'a, Tag, Con>`. The default, `NoConsistency`, says that members of the cluster may
hold arbitrarily different values. `EventualConsistency` says that *every member of the
cluster that is given the same inputs ends up holding the same contents.* A collection gets the
stronger label in one of three ways: a transport promises it (`broadcast_closed` over a
transport whose `ConsistencyGuarantee` is `EventualConsistency`), a programmer asserts it with
a proof (`assert_has_consistency_of(manual_proof!(...))`), or an operator whose algebra
preserves it (a `fold` with a commutative, idempotent combiner; a `map`; a `filter`) is
applied to a labelled input. Collections at a single `Process` carry no label; there is only
one holder.

The type system checks the label over the shape of every execution: given the same inputs,
the operators between them and the collection produce the same contents. What it cannot see is
whether every member *is* given the same inputs. Some executions are missing events: a sender
crashed between two sends of one fan-out, a message on a lossy channel was dropped. In those
executions the label's premise is false, and the label says nothing.

Hydrofrac tests the premise. It takes one concrete execution of the program (a fixed topology
and a fixed input workload, chosen by the test), and searches the faults the program's own
transports admit, within declared bounds, for one that leaves a correct member without an
input the others have. If it finds none, every correct member in every explored run held
every element of the labelled collection; if it finds one, it names the fault and the schedule.

The division of labour is this. The type system quantifies over all executions and speaks to
shape. Hydrofrac quantifies over all faults within bounds and speaks to one execution. A
certificate is a statement about that execution; the confidence that it generalizes to other
inputs and larger clusters is the confidence a test gives, sharpened by the fact that the
search over faults is exhaustive within the bounds rather than random. A counterexample is a
statement about the program.

## 2. What the certificate says, exactly

The specification is generated from the label (`Spec::eventual_consistency`). In words:

1. Run the program once with no faults. Write down every element that ends up in `C` at any
   member of the cluster. Call that set `FACTS`: it is what the program delivers when nothing
   goes wrong.
2. Run it again with faults. Ignore the members that crashed. The surviving members must
   agree: no element of `FACTS` may be held by one survivor and missing from another.

That is all. The restriction to `FACTS` says what "should have been delivered" means. The
clause "held by one survivor" is there because a fault can keep an element from entering the
system at all (the only sender died before sending it); then no one has it, and that is not
disagreement. The property is that survivors converge on the same set, not that the set is
complete.

Two boundary cases. If the failure-free run delivers nothing (`FACTS` is empty) there is
nothing to check, and the search reports `Vacuous` rather than a certificate. If a fault set
leaves the survivors holding nothing at all, the specification is trivially satisfied and that
run contributes nothing to the search; it does not count for or against the program.

A certificate is:

- **Within the fault model the program declares.** Every `Network` operator is a fault point;
  the transport it names is its fault vocabulary. A `lossy_delayed_forever` channel admits
  omission of any message sent before the end of finite failures; a `fail_stop` channel admits
  only its sender's crash. Crashes are available for the nodes the test registers as
  crashable and are counted against a crash budget.
- **Within bounds the test declares.** The crash budget; the end of finite failures (`EFF`),
  after which no new fault takes effect and the protocol has the rest of the run to converge;
  the horizon, which is however long the test body drives the run.
- **For the execution the test fixes.** The cluster sizes, the inputs, the timer firings.
- **Over the schedules explored.** Under `Exploration::Exhaustive` every batching and
  ordering of the simulator is explored for every hypothesis, and the certificate is a proof
  within the bounds. Under `Exploration::Fuzz { instances }` a sample is explored, and the
  result is reported as a fuzz result, not a certificate.

A certificate is *sound by the form of the specification*. The search's completeness argument
is that a fault set which leaves every delivery in the derivation of the specification intact
cannot falsify the specification. That is true of a view that is monotone in the deliveries,
and "every correct member holds every fact of a fixed set" is monotone in the deliveries by
construction. This is why `FACTS` is fixed from a probe run rather than quantified over
whatever each run produces: "every member holds everything any member holds" over an open
domain is *not* monotone (a fault that makes one member derive a fresh fact falsifies it
without touching any derivation), and would need a syntactic analysis of the view to grade.
No such analysis exists in hydrofrac; the generated specification does not need one.

Specifications the user writes by hand (section 7) do not get this guarantee. Their
certificates are printed as **RELATIVE**: within the fault model, relative to the
specification's form.

## 3. What it is not

The certificate is not an agreement claim, not a liveness claim, and not a claim about
inputs or topologies other than the test's.

Agreement ("members hold the *same* contents") is the type system's, given the premise;
hydrofrac discharges the premise and does not re-prove the conclusion. Liveness ("every
member *eventually* holds every fact") is made vacuous by design: the run has a finite
horizon, and the specification is evaluated at its end. A protocol that would have converged
with more time is reported as violated under the body that gave it less, which is a statement
about the body as much as the protocol; the test author chooses the horizon, and the
counterexample shows what was still missing. Interleavings are the simulator's: hydrofrac
chooses fault sets; the simulator chooses schedules under each.

## 4. How it works

### 4.1 The simulator and the fault point

The Hydro simulator compiles a flow into a single-process executable with every network
channel modelled as a queue, every tick as an explicitly scheduled batch, and every external
input and timer driven by the test body. It explores batchings and orderings either
exhaustively or by sampling. Hydrofrac places a **filter** in front of every network send. The
filter has two verbs:

- **Omission.** Drop this message, identified by channel, sender, destination and wire bytes.
  Every byte-identical copy on that channel is dropped while finite failures last, so a
  retransmission with the same payload is the same message. Available on lossy channels.
- **Crash.** `crash(node after m)`: the node performs every send up to and including `m`, and
  every later send is dropped; its external outputs are suppressed. Named by a message rather
  than a send count, so the fault means the same thing in every schedule, and so that it can
  split the fan-out of one broadcast (the sender dies between two sends of the same item).
  Available on nodes the test registers.

Both kinds lie before `EFF`. Nothing in the program registers fault points; the transports it
already declares are the fault model.

### 4.2 Lineage

Every run records every execution (an async run or one execution of a tick), every send with
its fate, every receive by the delivery it took, and which execution produced each output.
The **derivation** of a specification's view is the backward closure from the executions that
produced it. Four levels of precision are available:

| Level | What a tick execution depends on |
|---|---|
| `Exec` | every input of this execution and of every earlier execution of the same tick |
| `Slice` (default) | as above, narrowed by a static slice of the program per sink |
| `Record` | record-level provenance, by an IR rewrite that threads a provenance set through every operator (`SimFlow::with_provenance`) |
| `Counterfactual` | counterfactual replay: each node is re-executed alone with one delivery removed at a time; a delivery is in the derivation iff the node stops producing the output without it |

The levels trade precision for applicability. `Exec` and `Slice` always apply and may
over-approximate: they can name deliveries the output did not actually need. `Record` and
`Counterfactual` are more precise but can fail on a node: the provenance rewrite does not see
through every closure, and a replay with every delivery kept sometimes does not reproduce the
node's output, in which case removing deliveries one at a time proves nothing. When that
happens, the derivation for that node is taken from the next less precise level:
`Counterfactual` uses `Record`'s answer, `Record` uses `Slice`'s.

The direction matters. An over-approximated derivation makes a clause too large, so the search
tries fault sets it did not need to; that costs hypotheses, not correctness. An
under-approximated derivation makes a clause too small, and could hide the fault set that
breaks the program behind a certificate. Falling back toward the levels that can only
over-approximate keeps every certificate sound.

### 4.3 The search

Molly's loop. The failure-free run (the probe) fixes `FACTS` and yields the first derivation.
Each derivation is a clause: a set of deliveries at least one of which any breaking fault set
must kill, where omission kills a message and `crash(node after m)` kills every later send of
that node. A hypothesis is a hitting set of the clauses so far, within the crash budget. Each
hypothesis is run under the configured exploration; if the specification fails in some
schedule, that is the counterexample; if it holds, the run's derivation is a new clause (the
retransmission, the redundant path, the echo). The search ends when no hitting set exists
within the bounds (a certificate), a counterexample is found, or the hypothesis budget is
exhausted (`Outcome::Budget`).

### 4.4 The generated specification

`Spec` is a Hydro flow at a dedicated observer process, compiled with the program. The test
names the collection the label is on (`spec.observe_cluster(collection)`) and the cluster's
members (`spec.correct_members(&cluster, 0..n)`); hydrofrac feeds the observer, before each
run, the members the hypothesis does not crash and the facts `FACTS` learned from the probe.
`spec.eventual_consistency(observed, correct)` builds the view: the distinct `(member, fact)`
pairs at correct members over `FACTS` are as many as the `FACTS` facts present times the
correct members. The view is evaluated at the end of each run.

The observer is part of the compiled flow, so the specification's derivation is recorded like
any other output's, and the same lineage machinery produces the clauses.

### 4.5 Which collections are checked

Every one the program labels, without the test naming any. A `Spec::all_labels` leaves an
*observation slot* at the observer; when the flow is compiled for the simulator, a pass over
the IR (`sim::observe`) lists the labelled collections and taps each one the specification
applies to: the labelling node becomes a `Tee` (as a `.clone()` would make it), and a second
branch is mapped to `(collection index, bincode of the element)` and sent to the observer over
a `fail_stop` channel, exactly the IR a `.send(..)` to the observer would build. The slot
becomes the union of the taps. The pass runs before network compilation and before the
provenance rewrite, so the taps are compiled and traced like any other operator; a `Tee`
branch is invisible to the rest of the program, whose own outputs are unchanged.

A **labelled collection** is a labelled node where the label is *minted* (none of its inputs
is labelled: a `broadcast_closed` over a transport that grants it, an
`assert_has_consistency_of`) or where the collection *exits* the labelled region (a consumer
drops the label, as `weaken_consistency` and `batch` into an unlabelled tick do, or it is an
output of the program; a send to another location is not an exit, since the collection
continues there under the transport's own rule). The operators in between inherit the label
and are not listed; a `Tee` and a label-preserving `Cast` stand for what they share. A minting
point is where the type system's assumption about faults lives; an exit is what the program
delivers under the label. Reliable broadcast shows why both are reported: its initial
single-hop broadcast is minted `EventualConsistency` and is violated by the sender's crash, and
its delivered set, derived from that label by the type system, certifies anyway, because of the
echo the type system cannot see. Nodes at the same source line, of the same operator, on the
same cluster (one per cluster member, or one per instantiation) are one collection.

The set-based specification applies to an unbounded stream (or keyed stream, as pairs) at a
cluster's top level that a network channel feeds. The rest is reported, not checked: a
collection with local inputs only (no fault in the model can withhold an input), a lattice
value (`Singleton`, `Optional`, `KeyedSingleton`: the specification needs an equality variant,
not built), a collection inside a tick (its per-tick contents are not the collection's), and
whatever the test excludes (`Spec::all_labels_except`, for an element type that is not
`Clone + Serialize`).

`Ldfi::search_all_labels` runs one search per checked collection against the shared body: it
tells the observer which collection the search is about and which members of *that*
collection's cluster the hypothesis leaves correct, clears the fact domain, and runs the loop
of section 4.3 unchanged. The result is a verdict per collection (`LabelResults`): the search's
outcome, or the reason it was not checked. `spec.observe_cluster` remains for checking one
collection by hand.

## 5. Using it

Nothing is required of the program. The test fixes an execution and drives the run; the
labelled collections are found in the program:

```rust
let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

let delivered = reliable_broadcast_closed(items, &members);   // labelled EventualConsistency
delivered.weaken_consistency().sim_cluster_output();

let spec = Spec::all_labels(&mut flow);                       // names nothing

let compiled = flow.sim()
    .skip_consistency_assertions()      // the simulator does not yet check them
    .with_cluster_size(&members, 3)
    .with_provenance()
    .compiled();

let results = Ldfi::new()
    .level(Level::Record)
    .crashable("source", &source)
    .crashable_cluster("member", &members, 0..3)
    .crash_budget(1)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| {
        send.send(1);
        send.send(2);
        hydro_lang::sim::quiesce().await;
    });
println!("{results}");
```

prints, for this program,

```
EventualConsistency at reliable_broadcast.rs:130 (Cluster(loc2v1), AssertIsConsistent, minted): VIOLATED by {crash(source after reliable_broadcast.rs:130[→1]:2)}, 5 hypotheses
EventualConsistency at reliable_broadcast.rs:156 (Cluster(loc2v1), AssertIsConsistent, minted): VIOLATED by {crash(member@0 after reliable_broadcast.rs:156[0→1]:2)}, 3 hypotheses
EventualConsistency at reliable_broadcast.rs:149 (Cluster(loc2v1), Unique, delivered): CERTIFICATE, 10 hypotheses, exhaustive
```

and `results.verdicts` holds the same as values (`LabelVerdict::Searched(outcome)` or
`LabelVerdict::NotChecked(reason)`), for assertions per site (`results.at("file.rs:149")`) or
overall (`results.all_certified_or_not_checked()`). To check one collection by hand, build the
specification with `Spec::new`, `spec.observe_cluster(collection)`,
`spec.correct_members(&cluster, 0..n)` and `spec.eventual_consistency(observed, correct)`, and
run `Ldfi::search`.

The body is the execution: which inputs, which timers, how long. For a protocol whose
convergence needs time (retransmission timers, gossip rounds, leader change), the body fires
those timers after `run.end_of_finite_failures()` so the protocol has its window. Each result
prints as a counterexample with its faults and failing schedule, or as a certificate with its
grade, hypothesis count and exploration.

`Strategy::Blind` runs the same loop over the same fault universe without lineage (hypotheses
are the untried fault sets by increasing weight), as the ablation that measures what lineage
buys; `Ldfi::fuzz_faults_with` samples fault sets at random.

## 6. Results

Every producer of the `EventualConsistency` label in the codebase, and every demonstration
protocol of `hydro_std::ec_inference_demos` whose labelled output is a cluster-resident set,
has been run under the generated specification with one crash allowed and every registered
node crashable (`hydro_test/src/cluster/ec_ldfi.rs`, `ec_demos_ldfi.rs`). Hypothesis counts
include the probe run.

| Program | Result |
|---|---|
| `broadcast_closed`, process → cluster, `fail_stop` | **Violated**, 5 hypotheses: `crash(source after bcast[→1]:Item(2))`. The sender dies between two sends of one item. |
| `broadcast_closed`, process → cluster, `lossy_delayed_forever` | **Violated**, 2 hypotheses: `omit(bcast[→0]:Item(2))`. One lost message; nothing retransmits. |
| `broadcast_closed`, cluster → cluster, `fail_stop` | **Violated**, 5 hypotheses: `crash(source@0 after bcast[0→1]:Item(10))`. |
| `broadcast_live_from_process` (one hop over live membership) | **Violated**, 5 hypotheses: `crash(source after bcast[→1]:2)`. |
| `leader_merge_broadcast` | **Violated**, 2 hypotheses: `crash(leader after merged[→1]:(writer 0, 100))`. The demo's own documentation says its label assumes the leader survives; this is that sentence as a counterexample. |
| Echo reliable broadcast (`hydro_test` version) | **Certificate**, 10 hypotheses, exhaustive. |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | **Certificate**, 10 each, exhaustive. |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | **Certificate**, 28 each, 8 sampled schedules per hypothesis. |
| `g_set_gossip`, `g_set_gossip_live` | **Certificate**, 13 each, 8 sampled schedules. |
| `multi_paxos`, the learners' `learned` output (1 proposer, 3 acceptors, 3 learners) | **Certificate**, 24 hypotheses (25 on another run; fuzz sampling moves the count by one between runs), 8 sampled schedules. The learners re-broadcast what they learn; the certificate is that echo cycle's. The proposer is held correct. |

Every single-hop dissemination fails under one crash or one omission; every protocol with an
echo or gossip cycle holds. The transports themselves (`networking/mod.rs`) justify granting
the label by reasoning about the recipient ("a failed connection models the recipient as
failed") or about delay ("dropped messages are indefinitely delayed"), and neither reasoning
survives one sender crash or one lost message. The label is a promise about the program that
uses the transport, and only a program that retransmits keeps it. That is the right division:
the type system cannot know whether the program retransmits, and hydrofrac can.

Other labelled collections in the tree are not fault questions: `source_interval` on a
cluster (a stream of `()`), keyed folds with algebraic proofs (preserve the label given the
same inputs), and `collect_quorum` (a function of the response set). Of the remaining demos,
`quorum` and `epoch_splice` are combinators with no transport of their own (EC in, EC out;
their premise is whichever broadcast fed them), `synod` labels nothing `EventualConsistency`,
and `abd`'s replica register is in the class but is a lattice `Singleton` the set-based
specification cannot observe correctly (section 8).

### Every label of multi-Paxos

`Spec::all_labels` over `multi_paxos` (1 proposer, 3 acceptors, 3 learners; proposer, acceptors
and learners all crashable, one crash; `Fuzz { instances: 8 }`; the body tells proposer 0 to
lead round 1, quiesces, sends commands 7 and 8, quiesces) reports ten collections. Hypothesis
counts include the probe run and move by one between runs with the fuzz sampling. Line numbers
are into `hydro_std/src/ec_inference_demos/multi_paxos.rs` unless named otherwise.

| Site | Collection | Result |
|---|---|---|
| `:281`, acceptors, minted and delivered | `broadcast_closed` of prepares, proposer → acceptors; its `.entries()` goes straight into the acceptor's tick batch (`:303`), so the mint is also the exit. | **Violated**, 5 hypotheses: `crash(proposer@0 after :281[0→1]:Ts { round: 1, writer: 0 })`. The proposer dies between the prepare to acceptor 0 and the prepare to acceptor 1; acceptor 0 holds a prepare no other acceptor receives. |
| `:549`, acceptors, minted | `broadcast_closed` of phase-2 accepts, proposer → acceptors. | **Violated**, 8 hypotheses: `crash(proposer@0 after :549[0→1]:(Ts { round: 1, writer: 0 }, 0, 1, Some(8)))`. The proposer dies after sending the accept for slot 1 (command 8) to acceptor 0 only. |
| `:286`, acceptors, delivered | The `forward_ref` that closes the accept cycle; the same stream as `:549`'s entries, exiting the labelled region at `use::batch(accepts_fwd, ..)` (`:308`). | **Violated**, 8 hypotheses, by the same crash as `:549`. Nothing between the broadcast and the batch retransmits. |
| `:569`, proposers, minted | `broadcast_closed` of chosen slots, proposer → proposers. | **Certificate**, 17–18 hypotheses, 8 sampled schedules. The proposer cluster has one member in this run, so agreement is among one member and holds trivially. |
| `:570`, proposers, delivered | The `.values()` of `:569`, exiting at `weaken_consistency()` (`:571`). | **Certificate**, 19 hypotheses, 8 sampled schedules; trivial for the same reason. |
| `:582`, learners, minted | `broadcast_closed` of chosen decrees, proposer → learners: the initial hop of the learner echo. | **Violated**, 9 hypotheses: `crash(proposer@0 after :582[0→1]:(1, 0, 0, Some(7)))`. The proposer dies after shipping slot 0 (command 7) to learner 0 only. This is the premise the echo repairs. |
| `:591`, learners, minted | `broadcast_closed` of the echo, learners → learners, keyed by echoer. | **Violated**, 10 hypotheses: `crash(learner@0 after :591[0→1]:(1, 0, 0, Some(7)))`. Learner 0 dies partway through echoing slot 0; its echo, under its own key, is present at some correct learners and absent at others. |
| `:588`, learners, delivered | `learned = initial.merge_unordered(echo_fwd).unique()`, exiting at the test's `weaken_consistency()`. | **Certificate**, 25–26 hypotheses, 8 sampled schedules. Every learner that receives a decree echoes it to every learner; one crash leaves at least one echoer per decree that a correct learner received. |
| `:608`, learners, delivered | The `SpliceFact` `flat_map_unordered` of `learned`, exiting into the fold at `epoch_splice.rs:190`. | **Certificate**, 25–26 hypotheses, 8 sampled schedules. A function of `learned`, element by element. |
| `epoch_splice.rs:190`, learners, minted | The `fold` with a `commutative = manual_proof!` into `Singleton<SpliceState>`. | **Not checked**: a `Singleton`, not a set-valued collection (section 8). |

The `.values()` directly after `:281` (a `Cast`, delivered) is the same collection as its mint
and is not listed separately. The automatic search of `learned` took 25–26 hypotheses against
the manual search's 24–25 in the main table: the observer's dataflow is larger under
`all_labels` (it carries every label's facts and selects by index), so the probe run's lineage
has more to cover.

The pattern is the one section 5 shows for reliable broadcast. A minted single-hop
`broadcast_closed` is violated by the sender's crash mid-fan-out (`reliable_broadcast.rs:130`,
and here `:281`, `:549`, `:582`), because the transport's grant assumes the sender finishes.
The delivered set after an echo cycle certifies (`reliable_broadcast.rs:149`, and here `:588`,
`:608`). The receiving side of the echo itself, a cluster → cluster broadcast keyed by echoer
(`reliable_broadcast.rs:156`, and here `:591`), is violated by an echoer's crash: the echo
repairs the merged set, not the keyed collection of who echoed. A verdict per label separates
the transport's assumption from what the program delivers under it.

<!-- ISR / 2PC per-label results: pending -->

## 7. Hand-written specifications

A `Spec` can also be written by hand, as two Hydro views over the program's outputs: a
pre-condition (when it is empty the run is ignored) and a post-condition (when it is empty and
the pre-condition is not, the run is a counterexample). `Ldfi` also accepts simpler goals over
plain outputs (`goal`, `goal_all_members`, `goal_witnessed`). These are how hydrofrac is run
against properties the type system does not state: Raft's and Paxos's durability of committed
entries, two-phase commit's atomicity, ISR replication's agreement, quorum's claim counting,
retry-until-ack delivery (`raft_ldfi.rs`, `paxos_ldfi.rs`, `two_pc_ldfi.rs`,
`isr_replication.rs`, `quorum_ldfi.rs`, `reliable_delivery.rs`). Their certificates are graded
RELATIVE: no form argument grades them, and the user is told so.

They have found real bugs. An injected Raft election-restriction bug (index-only log
comparison) is found in 90 hypotheses with four omissions and no crash, a scenario the author
had not written down; correct Raft certifies in 235 under the same bounds (both tests
`#[ignore]`d for running time: thirty minutes and an hour). Paxos's P1b quorum counted replies
rather than acceptors, a defect of the production protocol exposed once the body's `EFF` moved
past the first ballot; correct Paxos, the Paxos key-value store and compartmentalized Paxos
certify in 372, 301 and 568 hypotheses (ignored, 45 minutes together). The simulator found
two defects on the way that are not fault questions: `collect_quorum_with_response` was not a
function of its input (its `nondet!` justification was false; fixed, with a test), and a
`fold` with an idempotence proof had no simulator implementation of its retry-only cast (now
the identity, since the simulated network never duplicates).

## 8. Limitations

- **One execution.** The certificate is for the test's topology and inputs. Nothing in
  hydrofrac generalizes it; that is the test author's induction.
- **Sampled schedules are not certificates.** Protocols with large schedule spaces (uniform
  broadcast, gossip, multi-Paxos) are run under `Fuzz`, and reported as such.
- **The horizon is the body's.** A protocol is given the time the body gives it. An
  unconverged run is a counterexample, not vacuous; a convergence witness that the protocol
  reports (every correct member has quiesced on the same view) would make it vacuous, and is
  not built. Raft's hand-written pre-condition ("some member committed") is the case where
  this cost effort: its body was tuned until correct Raft converged.
- **Set-valued collections only.** The generated specification observes a stream of facts. A
  lattice `Singleton` (`abd`'s max-by-timestamp register) needs a variant that observes the
  final value per member and checks equality; not built.
- **Static membership.** The simulator has everyone joined from the start; the `_live` demos
  are tested for faults, not for join timing.
- **Lineage gaps.** The provenance rewrite does not handle `entries_partially_ordered` on a
  keyed stream (`leader_merge` runs on `Level::Slice`). Counterfactual replay can panic inside
  a program that assumes a delivery it is being denied; the panic is contained on the
  generated side of the dynamic-library boundary and treated as inconclusive for that node.
- **Not every labelled collection is in the class.** The automatic specification checks
  unbounded streams at a cluster's top level that a network channel feeds. A lattice value
  (`Singleton`, `Optional`, `KeyedSingleton`), a collection inside a tick, and a collection with
  local inputs only are reported with the reason and not searched. Observing a collection
  shares and serializes its elements, so its element type must be `Clone + serde::Serialize`
  (labelled cluster collections nearly always are, being built from network sends); one that is
  not fails the generated crate's compilation and is excluded with `Spec::all_labels_except`.
- **The verdicts do not fail a test by themselves.** `LabelResults` is what a test asserts on;
  nothing fails for a label the test does not look at.
- **Byzantine, partition, and duplication faults** are outside the vocabulary. Partitions are
  expressible as omission sets on lossy channels and are found that way (the Raft
  counterexample is four omissions on two links); duplication has no fault point.

## 9. Next

In order of what the argument of section 1 needs:

1. The lattice-aware specification for `Singleton` outputs, and `abd` under it.
2. A convergence witness in the generated specification for protocols that report one, so
   that horizon is a parameter rather than a verdict.
3. Decision-trace replay: record every nondeterministic decision of a run and replay a prefix
   with one decision overridden. This is the general counterfactual oracle of which node-level
   replay is the coarse instance.
4. Raft's `committed` under the generated specification (the harness is written; the run is
   an hour).
