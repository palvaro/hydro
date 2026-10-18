# Blind comparison

## Task

**System prompt:**

```
You are rewriting a technical design document for an internal engineering audience. Output only the rewritten document, in Markdown, with no preamble, no commentary on the task, and no closing remarks.
```

**User prompt:**

```
I do not like the way hydrofrac.md is written. Please rewrite it for a Hydro-familiar audience. The goal is to explain why hydrofrac is needed and why as well as how it works. The reader will have some familiarity with LDFI, but should be reminded about the idea of working "backwards" from outcomes. Make sure to clearly explain how hydrofrac separates existing Hydro code into certified or incorrect w.r.t. complete EC.

The current draft's writing is extremely precious and hence often confusing, ambiguous, sometimes incomprehensible. Here is an example of MY OWN technical writing; try to write more like this:

=============================================================================
WRITING SAMPLE (my own technical writing, about a different tool)
=============================================================================

Cress

(_A hydrophilic plant, or *C*ounterfactual *R*edundancy *E*stimation under *S*imulated *S*talls_)

Cress is a tool for reasoning about a system's potential for amplifying work. It can be used to feel out the safety margin of a distributed system and help make it robust against metastable failures.

Cress is built atop the Hydro simulator, which systematically explores potential nondeterminism in program runs. The typical use case for the simulator is to support property-based testing: it explores schedules in an attempt to find violations of correctness properties. Cress subverts that simulator in an attempt to find and measure work amplification. At a very high level, cress asks whether a program can be induced to do work not only when data arrives (i.e., productive work) but also when data does NOT arrive. If a program does work in response to time passing, we can ask HOW MUCH of this unproductive work it can be made to do. The nondeterminism that we are interested in is hence _delay_ -- manifested both in the form of streams that stop making progress (a simulator use::batch hook) and singletons that stop changing (a use::snapshot) hook.

Using cress

To use cress, a user will annotate the source code of their hydro program with a closure that provides constants for the program's inputs that will be used in the tests. For example, a user trying to explore the amplification hazard of a client and service under a particular configuration will provide an `#[amplification_check(...)]` annotation that provides parameters such as queue bound, client timeout, retry and throttling policy tunables, etc. They then run cress at the command line.

```
#[amplification_check(
    name = no_governance,
    workload(requests = 2),
    workload_rounds = 125,
    config = RetryConfig { timeout_ticks: 6, server_capacity: 5, max_attempts: 3,
             policy: Policy::None },
)]
#[amplification_check(
    name = circuit_breaker,
    workload(requests = 2),
    workload_rounds = 125,
    config = RetryConfig { timeout_ticks: 6, server_capacity: 5, max_attempts: 3,
             policy: Policy::CircuitBreaker { timeout_threshold: 8, open_ticks: 12 } },
)]

pub fn service<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<u64, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    server_clock: Stream<(), Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RetryConfig,
) -> Responses<'a> {
[.....]
$ ./cress hydro_test service_no_governance

amplification check summary (hydro_test, functions matching "retry_service_no_governance")

function       configuration  workload                                                           verdict    growth  multiplier  location                                                             first reaction  extra work               horizon  seconds
retry_service  no_governance  requests=2, client_clock=1, server_clock=1, data in rounds 0..125  hazardous  stops   2.98x       src/cluster/witnesses/blind_budget/service.rs:533, batch of Request  8               992 in admitted records  999      9.0

$ ./cress hydro_test service_circuit_breaker

amplification check summary (hydro_test, functions matching "retry_service_circuit_breaker")

function       configuration    workload                                                           verdict    growth  multiplier  location                                                             first reaction  extra work              horizon  seconds
retry_service  circuit_breaker  requests=2, client_clock=1, server_clock=1, data in rounds 0..125  hazardous  stops   1.12x       src/cluster/witnesses/blind_budget/service.rs:533, batch of Request  8               16 in admitted records  999      6.0
```

How cress works

Cress first instantiates the user's program under the given input, wires it up to the simulator, and runs a single "prompt" execution in which no delays are simulated. In that run, it counts how many records are admitted into each dataflow "tick" at each use::batch point, and also how many network messages were sent on a per-sender basis. It then begins exploring schedules in which it places a "hold" on a single source of nondeterminism (e.g., the stream of client requests arriving at the server) for increasing amounts of time. In these exploratory runs, it releases the input data following the same schedule as the prompt run, and pays special attention to the holds that last longer than the input. In all of these cases, ANY increase in either counter versus the prompt run can be attributed counterfactually to the hold. The counters are a proxy for "work" and a positive delta against the prompt run is a measure of "amplification." Cress then exhaustively searches the family of schedules with a specific shape: a single hold point, with the hold starting at the beginning of the program execution and lasting for 1, 2, 4, [...] ticks, up to the whole run of the program (there are many deficiencies here! but this is a prototype, and this makes the search tractable). It searches for the schedule that maximizes the delta.

In the case of the client/server program, the hold that matters is on the stream of requests arriving at the server. If it is held for 8 rounds (just past the client's 6-tick timeout), the client starts re-sending. If it is held for 128 rounds or more, long enough that every request the test will ever offer is stuck behind the hold, the client has re-sent every one of them twice, which with three attempts is all it is allowed to do. The prompt run had the client send 250 messages, while under those long holds it sends 746, a delta of 496. Holding for 256 or 512 rounds gives the same 496. Holding forever also gives 496.

Judgements

When the search is complete, cress returns a judgement. For some programs and configurations (e.g., the client/server program with retries disabled) it will report 'benign': it was unable to stimulate amplification of any kind. For the others it will report 'hazardous', and then say two more things about the hazard.

*Locus* identifies a program location sensitive to the delay. Cress identifies the hold that produced a reaction at the shortest delay, by the file and line of the `use::batch` (or `use::snapshot`) in the source. For the client/server program that is the server's inbox, reacting at a hold of 8 rounds.

*Shape* describes how the work changes with the passage of time. For runs with hold durations that outlast the input, the only thing that differs is how long the program waited. Cress looks at the deltas across those runs and answers one question: does the extra work stop, or does it keep growing? If it stops, cress reports the ceiling, which for the three-attempt client is 496 extra messages, the same at 128, 256 and 512 rounds. It also reports the multiplication factor as 2.8x; this is very close to the fixed number of retries.

If the work keeps growing, cress reports the rate, in extra messages per additional round of delay; the same client with no attempt cap reads about 35. Cress also notices if the rate is itself growing (it will say "degree 2"), and if the program does LESS work under a long delay (it will say "shedding", which is what a circuit breaker does because it refuses input while open). Cress does not know why the ceiling is 496; matching it to two re-sends for each of the 248 held requests is the reader's job, and the source line of the hold is printed to help.

Validation

To kick the tires on cress, I had a collection of agents (running GPT5.6 Sol) implement a corpus of labeled examples of systems with and without work amplification. Without any knowledge of cress, each "author" was asked to implement a system in Hydro, and to label it with ground truth regarding its stability.

Benign

Benign systems are written so as to never produce work amplification, and are accompanied by an assurance argument (since I do not know how to prove that a program is robust to collapse!) that the system cannot amplify work. One example is non-distributed transitive closure computation that produces substantial intermediate results but always performs productive work. Another is a system that heartbeats at a fixed interval; although it produces "useless" work that increases with time, it is invariant across schedules.

Hazardous

Hazardous systems are supposed to amplify work in certain regimes. Examples include the running client/server example under a wide range of configurations, read-through caches with TTL-based expiry, leader election, replicated log catchup. Even for such systems, certain configurations (e.g., a correctly-parameterized token bucket or circuit breaker that rate-limits a client when success signals from the server degrade) minimize amplification.

The systems labeled "hazardous" have stronger ground truth than those labeled benign. The hazardous label was only permitted when the author was able to write a stress test that caused the system to collapse using the standard pattern: after a short burst of load beyond system capacity, the system remained in a degraded state with a low success rate.

Full results

Every row is one program in one configuration, run for 1000 rounds.

• *verdict* — `hazardous` if any hold at any decision point made any count rise, `benign` if none did. This is the sound part: a single extra message is hazardous, because cress cannot know what a message is worth.
• *shape* — what the extra work did across the holds long enough to cover the whole input (128, 256 and 512 rounds), where the only thing that differs between runs is how long the program waited. `stops`: the extra work reached a ceiling and a longer delay adds nothing. `grows`: every further round of delay costs about the same additional amount. `grows, slowing`: it still rises but each round costs less than the last. `grows, degree 2`: each round costs more than the last, with the rate doubling when the hold doubles. `shedding`: the program did less than the reference run under long delays (it refused input). `not read`: the data could not be confined, so the question could not be asked.
• *ceiling or rate* — the one number that goes with the shape. When the shape stops, the ceiling: the most extra work a delay of any length can cause. When it grows, the rate: extra messages per additional round of delay.
• *multiplier* — the worst count found divided by the reference count, for the sender where that ratio was largest. `2.98×` means that sender sent almost three times what the input required under the worst delay. When the shape stops, this is a property of the program. When it grows, it is a property of how long cress ran, and the rate is the number to compare instead. "followers send from 0" means the sender sent nothing at all in the reference run, so there is no ratio.
• *stall* — the extra work under the hold that never releases, in which the held records never arrive: what the program does while waiting for something that never comes. For growing shapes the per-round figure is given, since it is an independent estimate of the rate. A negative stall means the program sent fewer messages than the reference run.

=============================================================================
THE DOCUMENT TO REWRITE: docs/design/hydrofrac.md (current draft)
=============================================================================

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

Today a test checks one labelled collection, the one it passes to `spec.observe_cluster`. A
program may label several (multi-Paxos labels thirteen), and a test says nothing about the
ones it does not name. Every search therefore begins by listing every place in the program
where a collection acquires the label, marked `checked by this test`, `UNCHECKED (fed by a
network channel)`, or `nothing to check (local inputs only)`, so the scope of a certificate is
visible in the log.

**In the works.** Since the specification is a function of the collection alone and the
labelled collections are known from the program's IR, nothing about this needs the test author.
The next version observes every labelled, network-fed cluster collection automatically (by an
IR rewrite at flow finalization, as the provenance rewrite already inserts operators) and runs
one search per collection against the shared test body, reporting a verdict for each: certified,
violated, or not in class with the reason (a lattice `Singleton`, a collection inside a tick).
The report above is then unnecessary and goes away; `spec.observe_cluster` remains for checking
one collection by hand.

## 5. Using it

Nothing is required of the program. The test fixes an execution, names the collection, and
drives the run:

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

The body is the execution: which inputs, which timers, how long. For a protocol whose
convergence needs time (retransmission timers, gossip rounds, leader change), the body fires
those timers after `run.end_of_finite_failures()` so the protocol has its window. The result
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
| `multi_paxos`, the learners' `learned` output (1 proposer, 3 acceptors, 3 learners) | **Certificate**, 24 hypotheses, 8 sampled schedules. The learners re-broadcast what they learn; the certificate is that echo cycle's. The proposer is held correct. |

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
- **The report of unchecked labels is a report.** It does not fail a test. Making it one needs a notion of
  "every search over this program", which a per-program registry of specifications could
  provide.
- **Byzantine, partition, and duplication faults** are outside the vocabulary. Partitions are
  expressible as omission sets on lossy channels and are found that way (the Raft
  counterexample is four omissions on two links); duplication has no fault point.

## 9. Next

In order of what the argument of section 1 needs:

1. Automatic coverage of every labelled collection (section 4.5), in progress.
2. The lattice-aware specification for `Singleton` outputs, and `abd` under it.
3. A convergence witness in the generated specification for protocols that report one, so
   that horizon is a parameter rather than a verdict.
4. Decision-trace replay: record every nondeterministic decision of a run and replay a prefix
   with one decision overridden. This is the general counterfactual oracle of which node-level
   replay is the coarse instance.
5. Raft's `committed` under the generated specification (the harness is written; the run is
   an hour).
```


---

## Response A

*(4309 words)*

# Hydrofrac

_Lineage-driven fault injection for Hydro: pump in faults until a labelled collection cracks._

Hydrofrac checks whether a Hydro program's `EventualConsistency` labels survive the faults its own transports admit. It runs the program in the Hydro simulator, records why each run produced its outputs, and uses that record to decide which faults to try next. The search ends in one of two ways:

- A **counterexample**: a set of faults, within declared bounds, under which the surviving members of a cluster disagree about a labelled collection. Hydrofrac prints the faults and the schedule.
- A **certificate**: no fault set within those bounds caused disagreement in any run it explored.

The programmer does not write a specification. If a cluster collection is labelled `EventualConsistency`, the label is the specification.

The development record, with the reasoning behind each decision, is in `history-hydrofrac-development.md`.

## Why hydrofrac is needed

A collection at a Hydro cluster carries a consistency level in its location type, `Cluster<'a, Tag, Con>`. The default, `NoConsistency`, makes no promise. `EventualConsistency` promises that every member of the cluster that is given the same inputs ends up holding the same contents.

A collection gets the `EventualConsistency` label in one of three ways:

1. **A transport promises it.** For example, `broadcast_closed` over a transport whose `ConsistencyGuarantee` is `EventualConsistency`.
2. **A programmer asserts it.** They call `assert_has_consistency_of(manual_proof!(...))`.
3. **An operator preserves it.** Operators whose algebra carries the label from input to output do this when applied to a labelled input. Examples are a `fold` with a commutative, idempotent combiner, a `map`, or a `filter`.

Collections at a single `Process` carry no label, because there is only one holder.

The type system checks the label conditionally. It verifies that the operators between the inputs and the collection are deterministic enough that members with the same inputs compute the same contents. It cannot check the condition itself: that every member actually receives the same inputs. That condition is false in exactly the executions we worry about in distributed systems:

- A sender crashed partway through a broadcast, so some members got the message and others did not.
- A message on a lossy channel was dropped and never resent.

In those executions the label promises nothing, even though the type system accepted it.

The transports grant the label on informal grounds. The comments in `networking/mod.rs` reason about the recipient ("a failed connection models the recipient as failed") or about delay ("dropped messages are indefinitely delayed"). Neither argument holds once the sender crashes or a message is lost for good. Whether the label is true is a property of the program built on the transport, and specifically of whether that program retransmits or re-disseminates. The type system cannot see that. Hydrofrac can.

In this document, **complete EC** means the label's conclusion together with its premise. The type system supplies the conclusion: same inputs give the same contents. Hydrofrac checks the premise: under the admitted faults, every surviving member gets every input that any other survivor got. A collection that passes both has complete EC within hydrofrac's bounds.

## A reminder about LDFI

Hydrofrac is Molly (Alvaro, Rosen and Hellerstein, SIGMOD 2015) with the Hydro simulator in place of Dedalus.

The central idea of lineage-driven fault injection is to work backwards from a good outcome instead of forwards from guessed faults. A random fault injector picks faults and hopes one of them matters. LDFI does the following instead:

1. Run the program once with no faults and observe a correct outcome.
2. Ask why the outcome happened. Lineage gives the answer as the set of message deliveries the outcome depended on. Any fault set that breaks the outcome must kill at least one of those deliveries. Any other fault set leaves the support for the outcome intact.
3. Choose a fault set that kills at least one delivery from each "why" collected so far. Formally, this is a hitting set of the derivations.
4. Run the program under that fault set. There are two possible results:
   - The outcome breaks. That is a counterexample.
   - The outcome survives. The program found another way to produce it, through a retransmission, a redundant path, or an echo. Its lineage is a new "why", and the next fault set must hit that one as well.
5. Repeat until either a fault set breaks the outcome, or no fault set within the bounds can hit every "why". The second case is a certificate.

Lineage keeps the search small. Faults that cannot affect the outcome are never tried, and each surviving run shows the search exactly which redundancy it must defeat next.

Molly's users wrote the correctness property by hand. For eventual consistency, hydrofrac takes the property from the label.

## Using hydrofrac

Nothing is required of the program under test. The test does three things:

- Fixes one concrete execution: the topology, the cluster sizes, and the inputs.
- Names the labelled collection to check.
- Drives the run.

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

The test body defines the execution: which inputs arrive, which timers fire, and how long the run lasts. Some protocols need time after the faults stop in order to converge, for example those that use retransmission timers, gossip rounds, or leader change. For those, the body should fire the relevant timers after `run.end_of_finite_failures()`.

The result prints as one of the following:

- A counterexample, with its faults and the failing schedule.
- A certificate, with its grade, the number of hypotheses tried, and the exploration mode.
- `Vacuous`: the failure-free run delivered nothing, so there was nothing to check.
- `Budget`: the hypothesis budget ran out before the search finished.

Two alternative strategies use the same fault universe and serve as ablations that measure what lineage contributes:

- `Strategy::Blind` runs the same loop without lineage. It tries untried fault sets in order of increasing size.
- `Ldfi::fuzz_faults_with` samples fault sets at random.

## How hydrofrac works

### The probe run and the specification

Hydrofrac first runs the program with no faults. This is the **probe**. It records every element that ends up in the labelled collection `C` at any member. That set is `FACTS`: what the program delivers when nothing goes wrong.

In every later run, which includes faults, hydrofrac ignores the members that crashed and checks one condition:

> No element of `FACTS` is held by one surviving member and missing from another.

Survivors must agree. They do not need to hold all of `FACTS`. Some faults keep an element out of the system entirely, for example when its only sender dies before sending it to anyone. Then no survivor has the element, and that is not a disagreement.

Two edge cases:

- If `FACTS` is empty, there is nothing to check, and the search reports `Vacuous`.
- If a fault set leaves the survivors holding nothing at all, the condition holds trivially. That run counts neither for nor against the program.

The specification is itself a Hydro flow, `Spec::eventual_consistency`. It runs at a dedicated observer process that is compiled into the program:

1. `spec.observe_cluster` connects the labelled collection to the observer.
2. `spec.correct_members` names the members of the cluster.
3. Before each run, hydrofrac tells the observer which members the current hypothesis does not crash, and gives it `FACTS` from the probe.
4. The observer computes the distinct `(member, fact)` pairs over `FACTS` at correct members.
5. At the end of the run, the specification holds if that count equals the number of facts present times the number of correct members.

The observer is part of the compiled flow. Its output therefore has lineage like any other output, and the same machinery that explains program outputs also explains why the specification held.

### Fault points

Every `Network` operator in the program is a fault point, and the transport it names determines which faults it admits. Programs do not register fault points. The transports they already declare are the fault model. Hydrofrac places a filter in front of every network send. The filter can do two things:

- **Omit a message.** Omission is available only on lossy channels such as `lossy_delayed_forever`. A message is identified by channel, sender, destination, and wire bytes. While failures are active, every byte-identical copy on that channel is dropped, so a retransmission of the same payload counts as the same message.
- **Crash a node.** Crashes are available only on nodes the test registers as crashable, and each one counts against the crash budget. The fault `crash(node after m)` means the node performs every send up to and including message `m`, and every later send is dropped. The node's external outputs are also suppressed.
  - The crash point is named by a message, not by a send count, so the fault means the same thing in every schedule.
  - Naming it by message also lets a crash fall between two sends of a single broadcast. This is the case that breaks naive dissemination.
  - A `fail_stop` channel admits only crashes of its sender.

All faults take effect before the **end of finite failures** (`EFF`). After `EFF`, no new fault starts, and the protocol has the rest of the run to converge.

### Lineage

Every run records the following:

- Every execution: an async run, or one execution of a tick.
- Every send, and what happened to it.
- Every receive, and which delivery it consumed.
- Which execution produced each output.

The **derivation** of the specification's result is the set of deliveries reached by walking backward from the executions that produced it. Hydrofrac can compute derivations at four levels of precision:

| Level | What a tick execution is taken to depend on |
|---|---|
| `Exec` | Every input of this execution and of every earlier execution of the same tick. |
| `Slice` (default) | The same, narrowed by a static slice of the program for each sink. |
| `Record` | Record-level provenance. An IR rewrite (`SimFlow::with_provenance`) threads a provenance set through every operator. |
| `Counterfactual` | Each node is re-executed alone, with one delivery removed at a time. A delivery is in the derivation if and only if the node stops producing the output without it. |

`Exec` and `Slice` always work, but they can over-approximate: they can include deliveries the output did not actually need. `Record` and `Counterfactual` are more precise, but they sometimes fail on a node:

- The provenance rewrite cannot see through every closure.
- A counterfactual replay that keeps every delivery sometimes fails to reproduce the node's output. In that case, removing deliveries one at a time proves nothing.

When a level fails on a node, hydrofrac uses the next less precise level for that node. `Counterfactual` falls back to `Record`, and `Record` falls back to `Slice`.

The fallback always moves toward over-approximation, and the direction matters:

- A derivation that is too large costs extra hypotheses, because the search tries fault sets it did not need to. The verdict is still correct.
- A derivation that is too small could leave out the delivery whose loss breaks the program, which would produce a false certificate.

Because every fallback moves toward the safe direction, certificates remain sound.

### The search loop

The search is Molly's loop, in Hydro terms:

1. The probe fixes `FACTS` and produces the first derivation.
2. Each derivation becomes a **clause**: a set of deliveries, at least one of which any breaking fault set must kill. An omission kills one message. `crash(node after m)` kills every send by that node after `m`.
3. A **hypothesis** is a fault set that hits every clause so far and stays within the crash budget.
4. Hydrofrac runs each hypothesis in the simulator, which explores schedules under it. There are two possible results:
   - The specification fails in some schedule. That schedule is the counterexample.
   - The specification holds. The run's derivation becomes a new clause.
5. The search stops when one of three things happens:
   - No hypothesis exists within the bounds. This produces a certificate.
   - A counterexample is found.
   - The hypothesis budget runs out (`Outcome::Budget`).

Hydrofrac chooses fault sets, and the simulator chooses interleavings under each one. The simulator compiles the flow into a single-process executable. In that executable, every channel is a queue, every tick is an explicitly scheduled batch, and every input and timer is driven by the test body.

### A worked example

Consider `broadcast_closed` from a process to a three-member cluster over `fail_stop`, with inputs `1` and `2`:

1. The probe delivers both items to every member, so `FACTS = {1, 2}`.
2. The probe's derivation names the source's sends to each member.
3. Each member's copy of item `2` exists only because the source sent it directly. One crash that falls after the send to member 1 and before the sends to the others leaves the survivors in disagreement.
4. Hydrofrac reaches that crash on its fifth hypothesis: `crash(source after bcast[→1]:Item(2))`.

Now consider echo reliable broadcast under the same bounds:

1. The same crash is tried.
2. Member 1 re-broadcasts what it received, so the other members still get item `2`.
3. The specification holds, and the run's derivation, which goes through member 1's echo, becomes a new clause. To break the protocol, a fault set must now kill both the source's send and the echo.
4. With one crash allowed and no omissions available on `fail_stop`, no single fault hits every clause.
5. The search runs out of hypotheses after 10 and reports a certificate.

## What a verdict means

A certificate holds under four conditions:

- **It assumes the program's declared fault model.** It covers omissions on lossy channels, and crashes of the nodes the test registered as crashable.
- **It assumes the test's bounds.** These are the crash budget, `EFF`, and the horizon. The horizon is however long the test body drives the run.
- **It covers one execution.** That execution is the cluster sizes, inputs, and timer firings the test fixes.
- **It covers only the schedules explored.**
  - Under `Exploration::Exhaustive`, every batching and ordering is explored for every hypothesis, and the certificate is a proof within the bounds.
  - Under `Exploration::Fuzz { instances }`, a sample of schedules is explored, and the result is reported as a fuzz result rather than a certificate.

A certificate is a statement about one execution. Confidence that it generalizes to other inputs and larger clusters is the confidence any test gives you. That confidence is stronger here than for an ordinary test, because the search over faults is exhaustive within the bounds rather than random.

A counterexample is a statement about the program: there exists a run in which the label is false.

### Why certificates from the generated specification are sound

The argument for completeness is the standard LDFI argument. If a fault set leaves every delivery in the specification's derivation intact, it cannot falsify the specification. That argument holds only for properties that are monotone in deliveries, meaning that extra deliveries can never turn a true result into a false one. "Every correct member holds every fact of a fixed set" is monotone by construction.

This is why `FACTS` is fixed by the probe rather than recomputed in each run. Consider the alternative property, "every member holds everything any member holds", evaluated over whatever each run happens to produce. That property is not monotone. A fault could cause one member to derive a new fact, which would falsify the property without touching any recorded derivation. Grading such a property would require a syntactic analysis of the specification, which hydrofrac does not have. The generated specification does not need one.

Hand-written specifications, described below, carry no such argument. Their certificates print as **RELATIVE**: valid within the fault model, relative to the form of the specification.

### What a verdict does not claim

A verdict makes no claim about agreement, liveness, or other inputs and topologies.

- **Agreement.** That members hold the same contents is the type system's conclusion, given the premise. Hydrofrac checks the premise and does not re-prove the conclusion.
- **Liveness.** That every member eventually holds every fact is not checked. The specification is evaluated at the end of a finite run.
  - A protocol that would have converged with more time is reported as violated. That says as much about the test body as about the protocol.
  - The counterexample shows what was still missing when the run ended.
- **Other inputs and topologies.** A verdict covers only the execution the test fixes.

## Sorting existing Hydro code

With one crash allowed and every registered node crashable, I ran hydrofrac under the generated specification against two sets of programs:

- Every producer of the `EventualConsistency` label in the codebase.
- Every demonstration protocol in `hydro_std::ec_inference_demos` whose labelled output is a set held at a cluster.

The tests are `hydro_test/src/cluster/ec_ldfi.rs` and `ec_demos_ldfi.rs`. Hypothesis counts include the probe run.

### Violated

These programs carry the label but do not have complete EC.

| Program | Hypotheses | Counterexample |
|---|---|---|
| `broadcast_closed`, process → cluster, `fail_stop` | 5 | `crash(source after bcast[→1]:Item(2))`. The sender dies between two sends of one item. |
| `broadcast_closed`, process → cluster, `lossy_delayed_forever` | 2 | `omit(bcast[→0]:Item(2))`. One message is lost and nothing retransmits it. |
| `broadcast_closed`, cluster → cluster, `fail_stop` | 5 | `crash(source@0 after bcast[0→1]:Item(10))` |
| `broadcast_live_from_process` (one hop over live membership) | 5 | `crash(source after bcast[→1]:2)` |
| `leader_merge_broadcast` | 2 | `crash(leader after merged[→1]:(writer 0, 100))`. The demo's documentation says its label assumes the leader survives. This is that assumption turned into a counterexample. |

### Certified

These programs have complete EC within the bounds.

| Program | Hypotheses | Exploration |
|---|---|---|
| Echo reliable broadcast (`hydro_test` version) | 10 | exhaustive |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | 10 each | exhaustive |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | 28 each | 8 sampled schedules per hypothesis |
| `g_set_gossip`, `g_set_gossip_live` | 13 each | 8 sampled schedules |
| `multi_paxos`, the learners' `learned` output (1 proposer, 3 acceptors, 3 learners) | 24 | 8 sampled schedules |

Three of these rows are fuzz results, not proofs: uniform broadcast, gossip, and multi-Paxos have schedule spaces too large to explore exhaustively.

In `multi_paxos`, the learners re-broadcast what they learn, so the certificate is really a certificate for that echo cycle. The proposer is held correct.

### The pattern

Every single-hop dissemination fails under one crash or one lost message. Every protocol with an echo or gossip cycle holds. The label is a promise about the program that uses the transport, and only a program that re-disseminates keeps it.

### Not in scope

The remaining labelled collections are not questions about faults:

- `source_interval` on a cluster produces a stream of `()`.
- Keyed folds with algebraic proofs preserve the label given the same inputs.
- `collect_quorum` is a function of the response set.

The remaining demos are out of scope for different reasons:

- `quorum` and `epoch_splice` are combinators with no transport of their own. They take EC in and give EC out, so their premise belongs to whichever broadcast feeds them.
- `synod` labels nothing `EventualConsistency`.
- `abd`'s replica register is in scope in principle. However, it is a lattice `Singleton`, which the set-based specification cannot observe correctly. See the limitations below.

## Which collections a test checks

Today a test checks one labelled collection: the one it passes to `spec.observe_cluster`. A program may label many collections (multi-Paxos labels thirteen), and a test says nothing about the ones it does not name.

To make the scope of each certificate visible, every search begins by printing a list of every place in the program where a collection acquires the label. Each entry is marked with one of the following:

- `checked by this test`
- `UNCHECKED (fed by a network channel)`
- `nothing to check (local inputs only)`

**In progress: automatic coverage.** The specification depends only on the collection, and the labelled collections are visible in the program's IR. Checking them therefore does not need the test author. The next version will do the following:

1. Observe every labelled cluster collection that is fed by the network. This is done by an IR rewrite at flow finalization, in the same way the provenance rewrite already inserts operators.
2. Run one search per collection against the shared test body.
3. Report a verdict for each collection:
   - certified;
   - violated;
   - not in scope, with the reason, such as a lattice `Singleton` or a collection inside a tick.

The printed list then goes away. `spec.observe_cluster` remains for checking a single collection by hand.

## Hand-written specifications

A `Spec` can also be written by hand, as two Hydro views over the program's outputs:

- A **pre-condition**. If it is empty, the run is ignored.
- A **post-condition**. If it is empty while the pre-condition is not, the run is a counterexample.

`Ldfi` also accepts simpler goals over plain outputs: `goal`, `goal_all_members`, and `goal_witnessed`.

Hand-written specifications let hydrofrac check properties the type system does not state. The current tests cover the following:

| Property | Test file |
|---|---|
| Durability of committed entries in Raft | `raft_ldfi.rs` |
| Durability of committed entries in Paxos | `paxos_ldfi.rs` |
| Atomicity in two-phase commit | `two_pc_ldfi.rs` |
| Agreement in ISR replication | `isr_replication.rs` |
| Claim counting in quorum | `quorum_ldfi.rs` |
| Retry-until-ack delivery | `reliable_delivery.rs` |

Certificates from these tests are graded RELATIVE, and hydrofrac says so.

### Bugs found

These tests have found real bugs.

- **Raft.** An injected bug in Raft's election restriction compared logs by index only. Hydrofrac found it in 90 hypotheses, using four omissions on two links and no crash. That scenario was not one the author had anticipated. Correct Raft certifies in 235 hypotheses under the same bounds. Both tests are `#[ignore]`d because they take thirty minutes and an hour.
- **Paxos.** The P1b quorum counted replies rather than distinct acceptors. This is a defect in the production protocol. It became visible once the body's `EFF` moved past the first ballot. The following now certify, and the three tests together take 45 minutes and are `#[ignore]`d:

  | Protocol | Hypotheses |
  |---|---|
  | Correct Paxos | 372 |
  | The Paxos key-value store | 301 |
  | Compartmentalized Paxos | 568 |

The simulator also found two defects that are not about faults:

- `collect_quorum_with_response` was not a function of its input, so its `nondet!` justification was false. It is fixed, and a test covers it.
- A `fold` with an idempotence proof had no simulator implementation of its retry-only cast. The cast is now the identity, since the simulated network never duplicates messages.

## Limitations

- **One execution.** A certificate covers the test's topology and inputs. Generalizing it is the test author's job.
- **Sampled schedules are not certificates.** Protocols with large schedule spaces run under `Fuzz`, and their results are reported that way.
- **The test body sets the horizon.** A protocol that has not converged by the end of the run is reported as a counterexample, not as vacuous.
  - A convergence witness reported by the protocol would fix this. Such a witness would say that every correct member has settled on the same view. It is not built.
  - Raft's hand-written pre-condition ("some member committed") shows the cost: its test body was tuned until correct Raft converged.
- **Set-valued collections only.** The generated specification observes a stream of facts. A lattice `Singleton`, such as `abd`'s max-by-timestamp register, needs a variant that observes each member's final value and checks the values for equality. That variant is not built.
- **Static membership.** In the simulator, every member is joined from the start. The `_live` demos are tested for faults, not for join timing.
- **Lineage gaps.**
  - The provenance rewrite does not handle `entries_partially_ordered` on a keyed stream, so `leader_merge` runs at `Level::Slice`.
  - Counterfactual replay can panic inside a program that assumes a delivery it is being denied. The panic is contained on the generated side of the dynamic-library boundary, and the result for that node is treated as inconclusive.
- **The list of unchecked labels does not fail a test.** Making it fail a test requires a notion of "every search over this program", which a per-program registry of specifications could provide.
- **Some fault types are not modelled.**
  - Byzantine faults are not modelled at all.
  - Partitions can be expressed as sets of omissions on lossy channels, and are found that way. The Raft counterexample is four omissions on two links.
  - Duplication has no fault point.

## Next steps

These are ordered by how much they contribute to the goal of checking every label automatically:

1. Automatic coverage of every labelled collection (in progress).
2. A lattice-aware specification for `Singleton` outputs, and `abd` checked under it.
3. A convergence witness in the generated specification for protocols that can report one. The horizon then becomes a parameter rather than a source of false verdicts.
4. Decision-trace replay: record every nondeterministic decision in a run, then replay a prefix with one decision changed. This is the general form of counterfactual lineage, of which node-level replay is a coarse special case.
5. Raft's `committed` collection checked under the generated specification. The test harness is written; the run takes an hour.

---

## Response B

*(4015 words)*

# Hydrofrac

(_Hydraulic fracturing for distributed protocols, or lineage-driven fault injection for Hydro_)

Hydrofrac is a tool for finding out whether a Hydro program keeps the promise made by its `EventualConsistency` label when messages are lost and nodes crash. It runs the program in the Hydro simulator, records why each run produced the outputs it did, and uses that record to decide which faults to try next. It ends in one of two ways: a concrete set of faults, within bounds the test declares, under which the cluster's members disagree, together with the schedule that shows it; or a certificate that no fault set within those bounds can make them disagree.

The search is Molly's (Alvaro, Rosen, Hellerstein, SIGMOD 2015), with the Hydro simulator standing in for Dedalus. The important difference is who writes the correctness property. Molly's user wrote it by hand. For eventual consistency, hydrofrac's user writes nothing: a Hydro programmer who labels a cluster collection `EventualConsistency` has already stated that every member of the cluster ends up holding the same contents, and hydrofrac takes that statement as the specification.

This document describes what exists today. The development record, with the reasoning behind each decision, is `history-hydrofrac-development.md`.

## Why hydrofrac is needed

A collection living at a Hydro cluster carries one of two consistency levels in its location type, `Cluster<'a, Tag, Con>`. The default, `NoConsistency`, says members may hold arbitrarily different values. `EventualConsistency` says that every member of the cluster that is given the same inputs ends up holding the same contents. A collection gets the stronger label in one of three ways:

- a transport promises it (`broadcast_closed` over a transport whose `ConsistencyGuarantee` is `EventualConsistency`);
- a programmer asserts it, with a proof (`assert_has_consistency_of(manual_proof!(...))`);
- an operator whose algebra preserves it is applied to a labelled input (a `fold` with a commutative, idempotent combiner; a `map`; a `filter`).

Collections at a single `Process` carry no label, because there is only one holder and nothing to disagree with.

The type system checks this label over the *shape* of every execution: given the same inputs, the operators between the inputs and the collection produce the same contents at every member. What the type system cannot see is whether every member actually IS given the same inputs. In a real execution a sender may crash between two sends of one fan-out, or a message on a lossy channel may be dropped. In those executions the label's premise ("given the same inputs") is false, and the label says nothing. A `broadcast_closed` over a `fail_stop` transport carries the label, and it is also true that if the sender dies halfway through its fan-out, member 0 has the item and member 1 does not.

So the label covers half of eventual consistency: agreement, given equal inputs. Hydrofrac covers the other half: whether the program, under the faults its own transports admit, actually delivers equal inputs to every correct member. Together they are the complete property. Hydrofrac does this by taking one concrete execution (a fixed topology and input workload, chosen by the test) and searching exhaustively over fault sets within declared bounds for one that leaves a surviving member missing something another survivor has. If it finds none, the certificate says that in every explored run, every correct member held every element of the labelled collection. If it finds one, it names the fault and the schedule.

The division of labour is worth stating plainly. The type system quantifies over all executions and speaks to shape. Hydrofrac quantifies over all faults within bounds and speaks to one execution. A certificate is a statement about that execution; confidence that it generalizes to other inputs and larger clusters is the kind of confidence a test gives, sharpened by the fact that the search over faults is exhaustive within its bounds rather than random. A counterexample, on the other hand, is a statement about the program.

## Working backwards from a good outcome

Hydrofrac is an LDFI tool, and it helps to recall what that means. Ordinary fault injection works forwards: pick some faults, run the system, see what happens. The space of possible fault sets is enormous and most of them are harmless, so this is a slow way to find the ones that matter.

LDFI works backwards. Start with a run in which nothing goes wrong and the outcome is good. Ask: what had to go right for this outcome? The answer is the outcome's *lineage*: the particular messages whose delivery the outcome depended on. Any fault set that leaves every one of those deliveries intact cannot change the outcome, so there is no point trying it. The only fault sets worth trying are those that kill at least one delivery in the lineage.

Now try one of those. Two things can happen. Either the outcome breaks, and we have a counterexample. Or the outcome survives anyway, which means the program had another way to produce it (a retransmission, a redundant path, an echo from a peer), and the new run's lineage shows what that other way was. That lineage becomes a second constraint: a breaking fault set must now kill something in the first lineage AND something in the second. Keep going. Each successful run teaches the search one more way the program can succeed, and the set of candidate fault sets shrinks. The search ends when no fault set within bounds can hit every lineage seen so far (a certificate), or when one of them breaks the program (a counterexample).

That is the entire idea. What hydrofrac adds to it is a Hydro-specific fault model, Hydro-specific lineage, and a specification that comes from the program's own types rather than from the user.

## What hydrofrac checks, exactly

The specification is generated from the label (`Spec::eventual_consistency`). In words:

1. Run the program once with no faults. Write down every element that ends up in the labelled collection `C` at any member of the cluster. Call that set `FACTS`. It is what the program delivers when nothing goes wrong.
2. Run it again with faults. Ignore the members that crashed. The survivors must agree: no element of `FACTS` may be held by one survivor and missing from another.

That is all. The restriction to `FACTS` is what "should have been delivered" means. The phrase "held by one survivor" is there because a fault can keep an element from entering the system at all (the only sender died before sending it); then nobody has it, and that is not disagreement. The property is that survivors converge on the same set, not that the set is complete.

Two boundary cases. If the failure-free run delivers nothing (`FACTS` is empty) there is nothing to check, and the search reports `Vacuous` rather than a certificate. If a particular fault set leaves the survivors holding nothing at all, the specification is trivially satisfied and that run contributes nothing; it counts neither for nor against the program.

A certificate is bounded in four ways, and the bounds are printed with it.

- **The fault model is the one the program declares.** Every `Network` operator is a fault point, and the transport it names is its vocabulary. A `lossy_delayed_forever` channel admits omission of any message sent before the end of finite failures. A `fail_stop` channel admits only its sender's crash. Crashes are available for the nodes the test registers as crashable, and are counted against a crash budget.
- **The bounds are the test's.** The crash budget; the end of finite failures (`EFF`), after which no new fault takes effect and the protocol has the rest of the run to converge; and the horizon, which is however long the test body drives the run.
- **The execution is the test's.** Cluster sizes, inputs, timer firings.
- **The schedules are the simulator's.** Under `Exploration::Exhaustive`, every batching and ordering the simulator can produce is explored for every hypothesis, and the certificate is a proof within the bounds. Under `Exploration::Fuzz { instances }`, a sample of schedules is explored, and the result is reported as a fuzz result, not a certificate.

### Why the certificate is sound

The backwards argument of the previous section has a hidden requirement. "A fault set that leaves every delivery in the lineage intact cannot falsify the specification" is only true if the specification is *monotone* in deliveries: adding deliveries can never make a true specification false. "Every correct member holds every fact in a fixed set `FACTS`" is monotone by construction, which is exactly why `FACTS` is fixed from the probe run rather than quantified over whatever each run happens to produce. The tempting alternative, "every member holds everything any member holds," is NOT monotone over an open domain: a fault that causes one member to derive a fresh fact (a timeout firing, say) falsifies it without removing anything from any derivation, so lineage could never point the search at it. Checking that property soundly would need a syntactic analysis of the view. Hydrofrac has no such analysis, and the generated specification does not need one.

Specifications the user writes by hand (see below) do not get this guarantee. Their certificates are printed as **RELATIVE**: sound within the fault model, relative to the form of the specification the user wrote.

### What a certificate is not

It is not an agreement claim: agreement given equal inputs is the type system's, and hydrofrac discharges the premise without re-proving the conclusion. It is not a liveness claim: the run has a finite horizon and the specification is evaluated at its end. A protocol that would have converged given more time is reported as violated under a body that gave it less; this says as much about the body as about the protocol, and the counterexample shows what was still missing so the author can tell which. It is not a claim about inputs or topologies other than the test's. And hydrofrac chooses fault sets, not interleavings; interleavings are the simulator's.

## Using hydrofrac

Nothing is required of the program under test. The test fixes an execution, names the labelled collection, and drives the run:

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

The body is the execution: which inputs, which timers, how long. For a protocol whose convergence needs time (retransmission timers, gossip rounds, leader change), the body fires those timers after `run.end_of_finite_failures()` so the protocol gets its window. The result prints either as a counterexample, with its faults and the failing schedule, or as a certificate, with its grade, hypothesis count and exploration mode.

Two variants exist for comparison. `Strategy::Blind` runs the same loop over the same fault universe without lineage (hypotheses are simply the untried fault sets in order of increasing weight); it is the ablation that measures what lineage buys. `Ldfi::fuzz_faults_with` samples fault sets at random.

## How hydrofrac works

### Fault points

The Hydro simulator compiles a flow into a single-process executable in which every network channel is a queue, every tick is an explicitly scheduled batch, and every external input and timer is driven by the test body. It can explore batchings and orderings exhaustively or by sampling. Hydrofrac places a filter in front of every network send. The filter knows two verbs:

- **Omission.** Drop this message, identified by channel, sender, destination and wire bytes. Every byte-identical copy on that channel is dropped while finite failures last, so a retransmission with the same payload counts as the same message. Available on lossy channels.
- **Crash.** `crash(node after m)`: the node performs every send up to and including `m`; every later send is dropped and its external outputs are suppressed. The crash is named by a message rather than a send count so that it means the same thing in every schedule, and so that it can split the fan-out of a single broadcast (the sender dies between two sends of the same item). Available on nodes the test registers.

Both kinds of fault lie before `EFF`. The program registers nothing; the transports it already declares are the fault model.

### Lineage

Every run records every execution (an async run, or one execution of a tick), every send with its fate, every receive with the delivery that satisfied it, and which execution produced each output. The *derivation* of a specification's view is the backward closure from the executions that produced it. Four levels of precision are available:

| Level | What a tick execution is taken to depend on |
|---|---|
| `Exec` | every input of this execution and of every earlier execution of the same tick |
| `Slice` (default) | as above, narrowed by a static slice of the program per sink |
| `Record` | record-level provenance, by an IR rewrite that threads a provenance set through every operator (`SimFlow::with_provenance`) |
| `Counterfactual` | counterfactual replay: each node is re-executed alone with one delivery removed at a time; a delivery is in the derivation iff the node stops producing the output without it |

These trade precision for applicability. `Exec` and `Slice` always apply and may over-approximate: they can name deliveries the output did not actually need. `Record` and `Counterfactual` are more precise but can fail on a node. The provenance rewrite does not see through every closure, and a replay with every delivery kept sometimes does not reproduce the node's output, in which case removing deliveries one at a time proves nothing. When that happens, the derivation for that node is taken from the next less precise level: `Counterfactual` falls back to `Record`'s answer, `Record` to `Slice`'s.

The direction of the fallback matters. An over-approximated derivation makes a clause too large, so the search tries fault sets it did not need to; that costs hypotheses, not correctness. An under-approximated derivation makes a clause too small, and could hide the one fault set that breaks the program behind a certificate. Falling back toward the levels that can only over-approximate keeps every certificate sound.

### The search

This is Molly's loop. The failure-free run (the probe) fixes `FACTS` and yields the first derivation. Each derivation becomes a clause: a set of deliveries, at least one of which any breaking fault set must kill, where omission kills a message and `crash(node after m)` kills every later send of that node. A hypothesis is a hitting set of all clauses so far, within the crash budget. Each hypothesis is run under the configured exploration. If the specification fails in some schedule, that is the counterexample. If it holds, the run's derivation is a new clause, which is the search learning about the retransmission, the redundant path or the echo that saved the outcome. The search ends when no hitting set exists within the bounds (a certificate), when a counterexample is found, or when the hypothesis budget runs out (`Outcome::Budget`).

### The generated specification

`Spec` is itself a Hydro flow, placed at a dedicated observer process and compiled together with the program. The test names the collection the label is on (`spec.observe_cluster(collection)`) and the cluster's members (`spec.correct_members(&cluster, 0..n)`). Before each run, hydrofrac feeds the observer the members the hypothesis does not crash and the `FACTS` learned from the probe. `spec.eventual_consistency(observed, correct)` builds the view: the distinct `(member, fact)` pairs at correct members over `FACTS` must number as many as the `FACTS` facts present times the number of correct members. The view is evaluated at the end of each run.

Because the observer is part of the compiled flow, the specification's derivation is recorded like any other output's, and the same lineage machinery produces the clauses.

### Which collections are checked

Today a test checks one labelled collection, the one it passes to `spec.observe_cluster`. A program may label several (multi-Paxos labels thirteen), and a test says nothing about the ones it does not name. Every search therefore begins by listing every place in the program where a collection acquires the label, marked `checked by this test`, `UNCHECKED (fed by a network channel)`, or `nothing to check (local inputs only)`, so the scope of a certificate is visible in the log.

This is interim. Since the specification is a function of the collection alone, and the labelled collections are known from the program's IR, nothing about this step actually needs the test author. The next version observes every labelled, network-fed cluster collection automatically (by an IR rewrite at flow finalization, the same way the provenance rewrite already inserts operators) and runs one search per collection against the shared test body, reporting a verdict for each: certified, violated, or not in class with the reason (a lattice `Singleton`, a collection inside a tick). The report above then goes away; `spec.observe_cluster` remains for checking one collection by hand.

## Sorting the existing code: certified or incorrect

Every producer of the `EventualConsistency` label in the codebase, and every demonstration protocol in `hydro_std::ec_inference_demos` whose labelled output is a cluster-resident set, has been run under the generated specification with one crash allowed and every registered node crashable (`hydro_test/src/cluster/ec_ldfi.rs`, `ec_demos_ldfi.rs`). Each falls into one of three bins. Hypothesis counts include the probe run.

**Incorrect: the label is violated by one fault.**

| Program | Result |
|---|---|
| `broadcast_closed`, process → cluster, `fail_stop` | **Violated**, 5 hypotheses: `crash(source after bcast[→1]:Item(2))`. The sender dies between two sends of one item. |
| `broadcast_closed`, process → cluster, `lossy_delayed_forever` | **Violated**, 2 hypotheses: `omit(bcast[→0]:Item(2))`. One lost message; nothing retransmits. |
| `broadcast_closed`, cluster → cluster, `fail_stop` | **Violated**, 5 hypotheses: `crash(source@0 after bcast[0→1]:Item(10))`. |
| `broadcast_live_from_process` (one hop over live membership) | **Violated**, 5 hypotheses: `crash(source after bcast[→1]:2)`. |
| `leader_merge_broadcast` | **Violated**, 2 hypotheses: `crash(leader after merged[→1]:(writer 0, 100))`. The demo's own documentation says its label assumes the leader survives; this is that sentence as a counterexample. |

**Certified: no fault set within bounds breaks the label.**

| Program | Result |
|---|---|
| Echo reliable broadcast (`hydro_test` version) | **Certificate**, 10 hypotheses, exhaustive. |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | **Certificate**, 10 each, exhaustive. |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | **Certificate**, 28 each, 8 sampled schedules per hypothesis. |
| `g_set_gossip`, `g_set_gossip_live` | **Certificate**, 13 each, 8 sampled schedules. |
| `multi_paxos`, the learners' `learned` output (1 proposer, 3 acceptors, 3 learners) | **Certificate**, 24 hypotheses, 8 sampled schedules. The learners re-broadcast what they learn; the certificate belongs to that echo cycle. The proposer is held correct. |

The pattern is clean. Every single-hop dissemination fails under one crash or one omission; every protocol with an echo or gossip cycle holds. The transports themselves (`networking/mod.rs`) justify granting the label by reasoning about the recipient ("a failed connection models the recipient as failed") or about delay ("dropped messages are indefinitely delayed"), and neither argument survives one sender crash or one lost message. The label is really a promise about the program that uses the transport, and only a program that retransmits keeps it. This is the right division of labour: the type system cannot know whether the program retransmits, and hydrofrac can.

**Not a fault question, or not in class.** The remaining labelled collections in the tree do not need hydrofrac, or cannot yet use it. `source_interval` on a cluster is a stream of `()`. Keyed folds with algebraic proofs preserve the label given the same inputs, which is exactly what the type system already checks. `collect_quorum` is a function of its response set. Of the demos, `quorum` and `epoch_splice` are combinators with no transport of their own (EC in, EC out; their premise is whichever broadcast fed them), `synod` labels nothing `EventualConsistency`, and `abd`'s replica register is in the class but is a lattice `Singleton`, which the set-based specification cannot observe correctly (see Limitations).

## Hand-written specifications

A `Spec` can also be written by hand, as two Hydro views over the program's outputs: a pre-condition (when it is empty the run is ignored) and a post-condition (when it is empty and the pre-condition is not, the run is a counterexample). `Ldfi` also accepts simpler goals over plain outputs (`goal`, `goal_all_members`, `goal_witnessed`). This is how hydrofrac is run against properties the type system does not state: Raft's and Paxos's durability of committed entries, two-phase commit's atomicity, ISR replication's agreement, quorum's claim counting, retry-until-ack delivery (`raft_ldfi.rs`, `paxos_ldfi.rs`, `two_pc_ldfi.rs`, `isr_replication.rs`, `quorum_ldfi.rs`, `reliable_delivery.rs`). Their certificates are graded RELATIVE, since no form argument grades them, and the user is told so.

They have found real bugs. An injected Raft election-restriction bug (comparing logs by index only) is found in 90 hypotheses with four omissions and no crash, a scenario the author had not written down; correct Raft certifies in 235 hypotheses under the same bounds. (Both tests are `#[ignore]`d for running time: thirty minutes and an hour.) Paxos's P1b quorum counted replies rather than acceptors, a defect in the production protocol that surfaced once the body's `EFF` was moved past the first ballot; correct Paxos, the Paxos key-value store and compartmentalized Paxos certify in 372, 301 and 568 hypotheses (ignored, 45 minutes together). The simulator also turned up two defects along the way that are not fault questions: `collect_quorum_with_response` was not a function of its input (its `nondet!` justification was false; fixed, with a test), and a `fold` with an idempotence proof had no simulator implementation of its retry-only cast (now the identity, since the simulated network never duplicates).

## Limitations

- **One execution.** The certificate is for the test's topology and inputs. Nothing in hydrofrac generalizes it; that induction is the test author's.
- **Sampled schedules are not certificates.** Protocols with large schedule spaces (uniform broadcast, gossip, multi-Paxos) run under `Fuzz` and are reported as such.
- **The horizon is the body's.** A protocol gets the time the body gives it. An unconverged run is a counterexample, not vacuous. A convergence witness reported by the protocol itself (every correct member has quiesced on the same view) would make such runs vacuous instead; it is not built. Raft's hand-written pre-condition ("some member committed") is where this cost effort: its body was tuned until correct Raft converged.
- **Set-valued collections only.** The generated specification observes a stream of facts. A lattice `Singleton` (`abd`'s max-by-timestamp register) needs a variant that observes the final value per member and checks equality; not built.
- **Static membership.** The simulator has everyone joined from the start; the `_live` demos are tested for faults, not for join timing.
- **Lineage gaps.** The provenance rewrite does not handle `entries_partially_ordered` on a keyed stream (`leader_merge` runs at `Level::Slice`). Counterfactual replay can panic inside a program that assumes a delivery it is being denied; the panic is contained on the generated side of the dynamic-library boundary and treated as inconclusive for that node.
- **The report of unchecked labels is only a report.** It does not fail a test. Making it fail one needs a notion of "every search over this program", which a per-program registry of specifications could provide.
- **Byzantine, partition and duplication faults** are outside the vocabulary. Partitions are expressible as omission sets on lossy channels and are found that way (the Raft counterexample is four omissions on two links); duplication has no fault point.

## Next

In the order the argument of this document needs them:

1. Automatic coverage of every labelled collection, in progress.
2. The lattice-aware specification for `Singleton` outputs, and `abd` under it.
3. A convergence witness in the generated specification for protocols that report one, so that horizon becomes a parameter rather than a verdict.
4. Decision-trace replay: record every nondeterministic decision of a run and replay a prefix with one decision overridden. This is the general counterfactual oracle of which node-level replay is the coarse instance.
5. Raft's `committed` under the generated specification. The harness is written; the run is an hour.

---

## Response C

*(3604 words)*

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
