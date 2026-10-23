> **History.** This is the development record of hydrofrac as it was written, round by round;
> it is kept for the reasoning behind decisions that the current description (`hydrofrac.md`)
> states without argument. Specifics here (the polarity analysis, the generated
> specification's `PRE`, hypothesis counts) may no longer match the code.

# Hydrofrac: lineage-driven fault injection on the Hydro simulator

Status: working prototype (feature `ldfi` of `hydro_lang`, tests in `hydro_test`), after five
rounds. The first built the search on the simulator's scripted hooks; the second moved the fault
point to a filter on every network send (section 5, with section 11 recording why); the third
replaced hand-named goals by specifications written as Hydro views (section 6.5) and worked out
what counterfactual lineage does and does not buy for them (`history-hydrofrac-negation.md`); the fourth
graded certificates by a syntactic polarity check, since removed; the fifth corrected the Raft harness
and, in doing so, settled what hydrofrac is *for*: discharging the premise of the
`EventualConsistency` label, which section 1.1 argues it is both necessary and sufficient for,
and section 1.2 demonstrates on every producer of the label in the codebase and on the
`eventual_consistency` branch's protocols. That is narrower than the earlier rounds implied and
more useful. This document is written for the Hydro core team. It explains what hydrofrac is, how it relates to
LDFI as published, exactly how it uses the simulator to inject faults and to collect lineage,
what a user writes to run it on a protocol, what it has found, and what remains. The research
log with every experiment and every soundness bug along the way is `ldfi-on-sim-hooks.md`; this
document is the summary. Section 12 is the roadmap.

## 1. What it is, in one paragraph

Hydrofrac is the tool that discharges the premise of Hydro's `EventualConsistency` label: the
label says replicas compute the same contents given the same inputs, and hydrofrac certifies
that the program's declared faults cannot withhold an input from a correct replica (section
1.1). Mechanically, it is a test harness that takes a Hydro program, a correctness
specification, and a fault
model (which nodes may crash and how many; which channels may lose messages follows from the
transports the program declares), and searches for a combination of faults that makes the
specification fail. The specification is two views written in Hydro over the program's outputs,
`PRE` and `POST`, evaluated on an observer the simulator adds: a run in which `PRE` holds and
`POST` does not is a counterexample, exactly as in LDFI as published, and because the views are
dataflow they have lineage. It does this with the Hydro simulator: every candidate fault set is
one simulator run, and the simulator's own schedule explorer runs underneath, so a counterexample
is a fault set plus an interleaving. The search is lineage-driven: after each run in which `POST`
held, hydrofrac computes which message deliveries that outcome depended on, and the next fault
set is chosen so that it breaks every explanation seen so far. The search ends either with a
counterexample or with a certificate that no fault set within the model can break the
specification, and in both cases every hypothesis it tried comes with the derivation that
motivated it. The certificate is a proof only under exhaustive schedule exploration and only for
specifications that are monotone in the facts they observe; section 6.5 and
`history-hydrofrac-negation.md` say what that means and how often it holds.

### 1.1 What hydrofrac does, where it fits, and why it is necessary and sufficient

"What bugs does LDFI prevent; what properties does it guarantee; safety or liveness?" is a
question the method's authors have struggled with in public. In Hydro the answer is one
sentence:

> **Hydrofrac discharges the premise of the `EventualConsistency` label.** The label promises
> that replicas compute the same contents *given the same inputs*; hydrofrac certifies that the
> program's declared fault model cannot withhold an input from a correct replica. The label and
> the certificate together are convergence under faults. Neither alone is.

Everything below defends that sentence: what the property class is, why the gap exists, why
nothing else in the toolchain can close it (necessity), why a certificate closes it completely
within its bounds (sufficiency), and what the certificate does not say.

**The property class.** Given a bounded execution (a test body with a horizon, faults admitted
only before the end of finite failures), hydrofrac decides whether an admissible fault set can
make a fact that some correct node holds at the end of the run absent at another correct node at
the end of the run. One class: *fault-tolerant convergence of derived facts within a
finite-failures window*. In the classical taxonomy it is a safety property of bounded executions
("by the end, every correct node has it") standing in for the liveness property "eventually every
correct node has it"; the finite-failures assumption is the device that makes the liveness
question finitely decidable, and the certificate is a completeness claim over that finite space.
Not safety in general, not liveness in general: the bounded, monotone shadow of eventual delivery.
Within the class, under exhaustive schedule exploration, a certificate is a proof (section 7); the
hitting-set inference behind it is sound for a view whose derivation implies it, which "every
correct node holds every fact of a fixed set" is by construction (section 6.5).

**Why that class is the gap in Hydro.** The `EventualConsistency` label on a live collection says
that the collection's contents are a deterministic function of the *set* of inputs each replica
receives: insensitive to arrival order and to retries. It says nothing about the input sets being
the same at every replica. `broadcast_closed` is the clean case: a stream replicated that way is
labelled `EventualConsistency`, each member's input set is "the broadcasts I received", a crash
of the broadcaster between two sends of one fan-out gives two members different sets, and each
member's contents are perfectly consistent with its own set. The type system's guarantee is
"same inputs ⇒ same contents"; the unstated premise is "same inputs", and only a program that
works around faults makes it true. Hydrofrac checks the premise: for every fact at any correct
replica, every correct replica has it by the end. That is the division of labour among the three
tools Hydro has. The **type system** guarantees determinism given the inputs (agreement among
replicas is its property, by construction). The **simulator** explores the nondeterminism the
types admit (interleavings, batchings, unsafe operators). **Hydrofrac** checks that faults cannot
withhold the inputs. Together: an `EventualConsistency` label plus a hydrofrac certificate is
convergence under the fault model; the same holds for the whole-program eventual-determinism
property of a purely deterministic program. Any program whose correctness rests on those labels
needs the certificate in its test suite, and section 12 proposes that the specification be
*generated* from the labels rather than written: observe the labelled collection at every
replica, `PRE` a convergence witness, `POST` the same contents everywhere.

**`PRE` is a convergence witness.** A specification of this shape has a precondition for one
reason: an execution that has not converged is a trivially correct one, and must be vacuous
rather than a counterexample. The fifth round learned this the slow way. The Raft harness's
`PRE` was "some member committed an entry", which is true as soon as the first leader commits,
so a run too short for the cluster to catch up satisfied `PRE`, failed `POST`, and was reported
as a violation of correct Raft; the body was then lengthened four times (section 9) until the
protocol converged in time, which is treating a specification error as a scheduling problem. The
right `PRE` is a view that is true only once the system reports itself settled (for Raft: a
correct leader whose view every correct member shares and whose latest heartbeat every correct
member has acknowledged; for ISR, the final view names a leader), and the right `POST` is that
the settled state is complete (the leader's log contains every committed entry). With that
`PRE`, a leaderless cluster, a leader that cannot yet commit, a follower still catching up are
all vacuous; a body too short for *any* hypothesis to converge makes the failure-free run vacuous
and the search returns `Outcome::Vacuous` ("the harness protects nothing") in one run. It also
states the boundary of the class honestly: a protocol that never converges is vacuous, not
wrong. Hydrofrac does not check liveness.

**Necessary.** Three tools could be asked to discharge the premise, and two cannot. The *type
system* reasons about the program's functions of their inputs; it does not know which inputs
arrive. Every mint of the label in the codebase shows where its reasoning stops:
`broadcast_closed` justifies `EventualConsistency` on `fail_stop` by "a failed connection models
the *recipient* as having failed" and on `lossy_delayed_forever` by "dropped messages are
indefinitely delayed", and `fan_out` mints it from membership completeness, holder persistence
and channel delivery. All of these are premises about delivery, taken as given; none is about
the sender crashing between two sends, which is the one fault that breaks every one of them
(section 1.2). Types cannot know whether the program retransmits; that is a property of the
protocol's reaction to a fault, which needs an execution. The *simulator* has executions, and
explores the nondeterminism the types admit (interleavings, batchings), but faults are not in
its state space: its model of `lossy_delayed_forever` is "the message arrives at the end", its
model of a crash is nothing. It explores the failure-free world completely and the faulty world
not at all. A *random fault injector* (Maelstrom, Jepsen's nemeses) has both executions and
faults, and cannot discharge anything: its output is absence of evidence after a run of some
length, and a premise is discharged by a completeness claim, not by having looked for a while.
So the tool that is missing has to (a) inject the declared faults into the simulator's
executions, (b) check the convergence property, and (c) terminate with a claim of the form "no
admissible fault set within these bounds breaks it". That is a hitting-set search over the
lineage of the property, and it is what hydrofrac is. Nothing else in the toolchain has the
output shape the premise requires.

**Sufficient.** Within its bounds, the certificate is complete: it says that *no* fault set
over the declared fault model, within the crash budget and the finite-failures window, makes
a fact present at a correct replica absent at another correct replica by the end of the run.
Under exhaustive schedule exploration that is a proof over a finite space (section 7), and the
hitting-set inference it rests on is sound for the generated view by its form: a universal over
a fact domain and a member domain the hypothesis fixes, whose derivation is the deliveries behind
every pair and whose truth those deliveries imply (section 6.5). No analysis of the view is
involved; the fourth round's syntactic polarity check, which tried to establish the same thing
for arbitrary hand-written views, was removed once there was one specification to establish it
for. Composed with the
label: the label gives *same inputs ⇒ same contents*, the certificate gives *same inputs*, and
the conclusion is the convergence the label advertises, under the fault model the program
declared. Nothing further is needed from hydrofrac, and nothing further should be asked of it:
agreement among replicas is the type system's by construction, the interleavings are the
simulator's, liveness proper (a protocol that never converges) is vacuous by design, and
safety properties falsified by an *extra* fact are outside the class, and a hand-written
specification that asks about one gets a certificate graded RELATIVE with no soundness claim
(section 9's Raft and Paxos, kept as curiosities). The specification is
also generated, not written (`Spec::eventual_consistency`, section 1.2): a labelled collection
determines its own test, so the obligation can be discharged mechanically wherever the label
appears, which is what makes "every EC collection must have a certificate" a lint rather than
a chore. The inventory in section 1.2 is the evidence that the task is well posed: every
producer of the label in the codebase, and every demonstration protocol on the
`eventual_consistency` branch, falls on one side of a single line, single-hop or single-point
dissemination fails under one crash and every echo or gossip cycle certifies, and the search
found that line without being told where it was.

**What the certificate does not say.** It is relative to the body's horizon and heal window; to
the sampled schedules unless exploration is exhaustive; to the fault model (omissions on lossy
channels, permanent crashes before the end of finite failures, a crash budget; no partitions
with recovery, no Byzantine behaviour); and to the class: a property falsified by an *extra*
fact (agreement, uniqueness, no duplicates) is outside it, the inference is unsound for it, and
only a hand-written specification can ask about it, whose certificate is graded RELATIVE and
says so.

**Why hydrofrac and not Maelstrom.** Substitute Maelstrom into the formula, `EventualConsistency`
+ Maelstrom ⇒ convergence, and it does not type-check: the right-hand side is a guarantee and
Maelstrom's output is "ran 29 minutes, no anomaly", which is absence of evidence. A premise is
discharged by a completeness claim over a finite space, which is what a certificate is and what
no random fault injector can produce, however long it runs. That is the whole case. On bug
finding alone the case is weaker and should be stated weakly: within the class, random injection
finds most bugs eventually (Maelstrom's broadcast workload does check convergence, reading every
node at the end, and its `kill` nemesis lands mid-fan-out given enough runs; Jepsen has found
such bugs in real systems; every bug in the Molly paper was a known one). What the measurements
here say (section 9) is that lineage finds the in-class bugs in 3–32 hypotheses, every time, and
then *stops* with a certificate, while random injection with the same vocabulary misses 2PC bugs
in 2–4 of 10 repeats at five or more participants and never stops. Directed, cheap, terminating,
and the right output shape; not unique. The two are different instruments besides: Maelstrom
runs the real deployment path (its first run found a real bug in Hydro's Maelstrom backend that no
simulator test could), real timing, client-level linearizability; hydrofrac runs in the
simulator in seconds against a derived specification and sits in `cargo test` next to the type
checker.

**Raft, as a curiosity.** The headline of earlier versions of this document was that hydrofrac
found an injected Raft election bug that thirteen simulator tests and twelve Maelstrom runs did
not. It did (section 9, with the correction of how). But the bug is outside the class: it is a
*safety* bug, a violation of the promise the `EventualConsistency` label makes (replicas agree),
not of the premise hydrofrac checks. Hydrofrac caught it because this bug's symptom includes a
convergence failure, a stale node elected and never receiving the committed entry, so the
cluster converges on a leader whose log lacks a fact every other correct node holds. A safety
bug whose only symptom is a conflict or a retraction would be invisible to the sound half of the
method; the Paxos P1b defect of section 9.2 is one, found under an agreement specification graded
RELATIVE. Both were reached by the *fault exploration*, which is a decent way to reach states
where safety bugs manifest, with no guarantee attached. The in-class results are the
unglamorous ones: reliable delivery, reliable broadcast, 2PC completeness, ISR durability, the
quorum library. The Raft and Paxos results stay in section 9 as what they are.

So the answer to the question. Safety or liveness? Neither in general: the bounded, monotone
shadow of eventual delivery, which is exactly the shape of the one premise Hydro's type system
leaves open. What does it guarantee? That the convergence the `EventualConsistency` label
promises is not withheld by admissible faults. What bugs does it prevent? The ones where a
program relies on a label whose premise it never discharged: a broadcast with no echo, a leader
with no successor, a fan-out whose holder can die. Nothing else; and in Hydro, nothing else is
needed from it. Narrow, necessary, and complete for its class.

### 1.2 The inventory: every producer of the label, tested

If that is what hydrofrac is for, then every piece of Hydro that *produces* a collection
labelled `EventualConsistency` owes it a test. `Spec::eventual_consistency` is the generated
specification (`hydro_lang/src/spec_views.rs`; nothing written by hand). It is Molly's "the good
outcome persists" for the one good outcome the label is about: `FACTS` is the set of facts
correct members held at the end of the failure-free run, which the search learns from a probe
run and feeds to the observer before every run after it, as it feeds the correct members; then
`PRE` is "some correct member holds some fact of `FACTS`" and `POST` is "every correct member
holds every fact of `FACTS` that some correct member holds" (as a count: the `(member, fact)`
pairs at correct members over `FACTS` are as many as the `FACTS` facts present times the correct
members). Fixing the fact domain is what makes the certificate sound by construction (section
6.5): quantifying over whatever facts a run produces would let a fault that creates a fresh fact
at one replica falsify the view without touching any derivation, and a fresh fact is not the
label's premise about the given inputs anyway. `hydro_test/src/cluster/ec_ldfi.rs` applies it.
The producers in the codebase, and what the search says about each (the counts below are
unchanged by the fixed domain; what changed is that every certificate in this table is graded
*within the fault model*):

| Producer of `EventualConsistency` | Where the label comes from | Result |
|---|---|---|
| `Stream::broadcast_closed`, process → cluster, over `fail_stop` | `TcpFailPolicy::ConsistencyGuarantee`: "a failed connection stops all deliveries to that recipient, which models the *recipient* as having failed" | **Violated**, 4 hypotheses: `crash(source after bcast[→1]:Item(2))`. The sender is the one that fails, between two sends of one fan-out. |
| The same over `lossy_delayed_forever` | "dropped messages are modeled as indefinitely delayed, so the output is a lower bound … eventually delivered in full" | **Violated**, 2 hypotheses: `omit(bcast[→0]:Item(2))`. One lost message, no crash. A closed broadcast sends once; nothing retransmits; indefinitely delayed is never. |
| `Stream::broadcast_closed`, cluster → cluster, over `fail_stop` | as above, plus "every source member sends to every destination member" | **Violated**, 4 hypotheses: `crash(source@0 after bcast[0→1]:Item(10))`. |
| The echoing reliable broadcast (`reliable_broadcast.rs`), the same generated spec | the program | **Certificate**, 10 hypotheses, exhaustive schedules; plain broadcast under the same spec violated in 2. The generated spec separates the protocols exactly as the hand-written one did. |
| ISR's `writes.broadcast_closed(replicas, fail_stop)` and `views.broadcast_closed(..)` | as the first row | The ISR harness declares the client and ZK non-crashable, under which the premise holds trivially; with a crashable sender it is the first row again. Not run separately. |
| Raft's `committed` output (`assert_has_consistency_of::<EventualConsistency>`) | the protocol | The durability specification of section 9 is this shape (every correct member holds every committed entry); certificate in 235 hypotheses on correct Raft, violated in 90 with the injected bug. |
| `hydro_std::quorum::collect_quorum` | `assert_has_consistency_of(manual_proof!(/** TODO */))` | The output (keys with at least `min` successes, each once) is a function of the set of responses; the proof now says so. |
| `hydro_std::quorum::collect_quorum_with_response` | the same `TODO` | **Not a function of its input.** Three participants, quorum of two, all three reply: one batching emits `{a, b}`, another `{a, b, c}`; the `nondet!` justification on the batch ("we always produce deterministic quorum results") was false. Found by the simulator's exhaustive exploration in two schedules once a test asked. Fixed: once a key reaches quorum every response for it is emitted exactly once, before or after; the proof is written. Paxos's `p_p1b` consumes it through `fold_early_stop` and is unaffected. |
| `source_interval*` on a cluster | `assert_has_consistency_of_trusted` | A stream of `()`: trivially the same contents everywhere. |
| Keyed `fold` / `reduce` with algebraic proofs | `assert_has_consistency_of(manual_proof!(/** algebraic properties */))` | Preserve the label given the same inputs; not a fault question. |

The `eventual_consistency` branch's `ec_inference_demos` (`hydro_std/src/ec_inference_demos`,
brought over verbatim; their simulator tests, written against that branch's own crash model,
are compiled out) are the protocols this framing was waiting for: EC *inferred* from a few
trusted premises, with the fault dependency written in prose. `fan_out` mints the label from
membership completeness, holder persistence and channel delivery, none of which is the data
holder's survival; `leader_merge` says of itself that its label "silently assumes the leader's
survival, `F = {leader}`". Under the generated specification, one crash
(`hydro_test/src/cluster/ec_demos_ldfi.rs`):

| Demo | Result |
|---|---|
| `broadcast_live` (one hop over live membership, EC minted by `fan_out`) | **Violated**, 4 hypotheses: `crash(source after bcast[→1]:2)`. |
| `leader_merge_broadcast` | **Violated**, 2 hypotheses: `crash(leader after merged[→1]:(writer 0, 100))`. The prose caveat, as a counterexample. |
| `reliable_broadcast_closed`, `reliable_broadcast_live` | Certificate, 10 each, exhaustive schedules. |
| `uniform_reliable_broadcast_closed`, thresholds 1 and 2 | Certificate, 28 each, 8 sampled schedules (exhaustive passed 240,000 interleavings). |
| `g_set_gossip`, `g_set_gossip_live` | Certificate, 13 each, 8 sampled schedules. |
| `multi_paxos`, the learners' `learned` output (one proposer, three acceptors, three learners) | Certificate, 26 hypotheses, 8 sampled schedules. The learners re-broadcast what they learn (`learned.broadcast_closed(learners)` feeding back into `learned`), which is an echo cycle, and the certificate is the cycle's, not consensus's: a crashed acceptor or learner cannot withhold a learned value from a correct learner. The proposer is the single non-replicated party and is held correct. |

Every single-hop or single-point dissemination fails; every echo or gossip cycle holds. Two
simulator gaps on the way: `fold` with an idempotence proof casts its at-least-once input to
exactly-once and the simulator had no implementation of a retry-only cast on a top-level
stream (it is now the identity there, since the simulated network never duplicates a
delivery; a hook that injects duplicates to exercise idempotence proofs does not exist), and
the provenance rewrite does not handle `entries_partially_ordered` on a keyed stream (the
`leader_merge` search runs on the recorded lineage levels). Membership in the simulator is
static, so the `_live` variants are tested for faults, not for join timing. The remaining
demos were not run, for three different reasons worth separating. `quorum` and `epoch_splice`
are combinators, not protocols: each takes an EC-labelled stream and derives an EC-labelled
output from it locally (EC in, EC out), with no transport of their own to fault; the premise
they inherit is whichever broadcast fed them, which is the first rows of the table. `synod`
labels nothing `EventualConsistency`; its output is an agreement claim at the proposer, outside
the class. `abd` *is* in the class, and is the interesting gap: its replica `register`
(`abd.rs:158`) is a cluster-resident labelled collection fed by the client's write broadcast,
exactly the shape the certificate is for, but it is a lattice `Singleton` (max by timestamp),
and the generated specification observes a *set of facts*: its probe would collect every
intermediate value of the register as a "fact" and then demand every replica hold every one,
which is not what convergence of a lattice value means. A lattice-aware variant (observe the
final value per correct member; POST is equality) is a small addition and is the next thing to
build; by the first rows of the table one would expect it to be violated by a client crash
between two sends of a write.

**The lint.** Every search under a `Spec` now begins by listing every *source* of an
`EventualConsistency` label in the program (`Slices::ec_collections`, from the IR's consistency
metadata: a labelled operator none of whose inputs is labelled, so the point where the label
was minted or re-minted rather than inherited; one line per source line and location, with
cycles followed so an echo feeding its own input counts as network-fed), whether a network
channel feeds it, and whether a channel into the specification's observer carries it. A fed,
unobserved source is printed as `NOT OBSERVED: an undischarged premise`; a fed source the spec
observes as `observed by this specification`; a source with no network upstream as `nothing
to discharge`. On `reliable_broadcast` that is five lines, on `multi_paxos` thirteen (the
first version printed one line per downstream operator and repeated them per cluster
instantiation, forty for `multi_paxos`, and was unreadable).
On the existing hand-written harnesses it has teeth at once: the 2PC harness labels the
coordinator's `prepare` and `commit` broadcasts to the participants (`two_pc_ldfi.rs:86, 92,
161, 162`) and discharges none of them, and the ISR harness labels the client's write broadcast
and ZK's view broadcast (`isr_replication.rs:343, 564`, and their dataflow-encoding twins) and
discharges neither; both harnesses check other properties over other outputs. Those are exactly
the `broadcast_closed` premises of the first rows of the table, carried by senders those
harnesses declare non-crashable, which is the one circumstance under which the premise holds
trivially. The lint does not know that; it reports the premise, and the harness author decides.
(A labelled collection with no network upstream, such as an ISR replica's local fold, is
reported as having nothing to discharge.)

Two readings of the table. First, the label is undischarged at its source: both transports that
`networking/mod.rs` grants `EventualConsistency` to justify it by reasoning about the recipient
or about delay, and neither reasoning survives one sender crash or one lost message. The label
is a promise about the program that uses the transport, and only reliable broadcast keeps it.
That is the right division: the type system cannot know whether the program retransmits, and
hydrofrac can. Second, the `TODO` proofs were hiding a determinism bug that has nothing to do
with faults, and the simulator found it in two schedules once someone wrote the test the proof
should have been. The generated specification makes the first kind of test free; the second
kind still needs a person to notice a `TODO`.

## 2. LDFI as published, briefly

Lineage-driven fault injection (Alvaro, Rosen, Hellerstein, SIGMOD 2015; the Molly prototype)
starts from a simple observation. If a distributed program produced a correct outcome, and we can
see which messages that outcome was computed from, then any fault set that leaves all of those
messages intact cannot break that outcome. So the only fault sets worth trying are those that
remove at least one message from every known explanation. Formally: the lineage of the outcome
is a disjunction of derivations (each a set of deliveries); negate it and you get a CNF formula
whose satisfying assignments are the fault sets that disable every known derivation. Run one
such fault set. If the outcome still holds, the program had another way to get there (a retry, a
redundant path); that new derivation becomes a new clause, and the loop repeats. If the formula
becomes unsatisfiable, no fault set within the bounds can break the outcome.

Molly needed four things from its substrate: a fault vocabulary (message omission and permanent
crash, with a horizon `EOT`, an end of finite failures `EFF` after which messages are delivered,
and a crash budget); lineage of the outcome; deterministic re-execution under a fault set; and
stable message identity across runs. Molly got lineage from Dedalus, a Datalog, where provenance
is a property of the language. It fixed a synchronous schedule (deliver at `t+1` or never) and
did not explore message reordering.

## 3. How hydrofrac is LDFI, and how it is not

It is LDFI in the ways that matter for the method.

- The fault vocabulary is Molly's: omission of a message on a lossy channel, permanent crash of
  a node, a crash budget, and a horizon set by the test body (Molly's `EFF` survives as an
  optional bound; section 10).
- The specification is Molly's: `PRE` and `POST` as views in the program's own language over
  derived facts, so the search negates the lineage of a *view*, not of one concrete message
  (section 6.5).
- The loop is Molly's: run, derive, negate, solve, run again, until counterexample or
  certificate. Hypotheses are minimal hitting sets over the derivations, chosen weakest-first.
- Over-approximate lineage is accepted as sound, under-approximate lineage is a bug, and every
  false certificate during development (four so far) was traced to under-approximation and
  fixed.
- Certificates are the distinctive product: random and coverage-guided fuzzers cannot produce
  one.

It departs from Molly in five ways.

1. **Lineage is not a language feature.** Hydro programs are Rust closures over dataflow, not
   Datalog rules. Hydrofrac reconstructs lineage from the simulator's own event log at four
   levels of precision (section 6), the finest of which re-executes a node with deliveries
   removed to ask the counterfactual question directly. Molly's lineage was exact
   why-provenance; hydrofrac's is a sound approximation at levels 0 to 2 and a 1-minimal
   sufficient set at level 3. A *sufficient set* is a set of deliveries into a node that, replayed
   alone with every other delivery dropped, still reproduces the output; *1-minimal* (the
   delta-debugging sense, as in Zeller's `ddmin`) means no single delivery can be removed from it
   and keep it sufficient. It is one derivation of the output, not necessarily the smallest and
   not necessarily the only one.
2. **The schedule is explored, not fixed.** Each hypothesis runs under the simulator's
   exhaustive or sampled schedule exploration. A counterexample is a (fault set, schedule) pair.
   The staged Raft scenario of an earlier version of section 9 needed a specific interleaving as
   well as specific lost messages; the counterexample the generic body yields does not depend on
   the schedule, but the certificate on correct Raft is still relative to the schedules sampled.
3. **The fault point is a filter on every network send, inserted by the simulator.** Every
   `Network` node of the program is a fault point; the transport the program declared
   (`lossy_delayed_forever` or `fail_stop`) is its fault model (section 5). The first version
   put the fault point on the receiving `batch` instead, and section 11 explains why that was
   wrong.
4. **Crash points are named by messages, not by time.** Molly's `crash(node, t)` uses a logical
   clock; hydrofrac's `crash(node after sending m)` names the last message the node sends, so
   the fault means the same thing in every run even when the schedule changes.
5. **Hypotheses are solved as an integer program (HiGHS), not SAT.** Same problem, different
   solver; the ILP lets the search express "weakest first" as an objective and the crash budget
   and pre-condition constraints as linear constraints.

## 4. The simulator, as hydrofrac uses it

The Hydro simulator (`hydro_lang/src/sim/`) compiles a flow to one deterministic DFIR per
location (per cluster member), and one DFIR per tick. The simulator's scheduler runs the async
DFIRs to a fixed point and then picks one runnable tick to execute, using fuzzer entropy to
choose. Everything that is nondeterministic in the program (which buffered messages go into a
tick's batch, the order of an unordered fold) is a decision taken at a **hook**. The scheduler
owns the hooks; the program never sees its own entropy.

The scripted-hook layer (`hydro_lang/src/sim_hooks.rs`, `sim/hooks.rs`,
`sim/runtime/scripted.rs`) lets a test take over one specific hook. The test creates a handle
with `flow.sim_hook()` and binds it to one operator with `nondet!(/** reason */ hook = h)`:

```rust
// hydro_lang/src/live_collections/stream/mod.rs:2274
pub fn batch<L2>(self, tick: &Tick<L2>,
    mut nondet: NonDet<Option<BatchHook<T, O, R, L::SimHookScope>>>) -> ...
{
    ...
    metadata.op.sim_hook_id = nondet.take_hook().map(|h| h.id);
```

The hook id is stored on the IR node. When the simulator builds the program, a batch with a hook
id gets a `Scripted` wrapper around its buffer, registered under `(hook_id, member)`. On a
cluster, one hook handle controls one instance per member (`hook.on(m)`). The test side of the
handle talks to the instance inside the compiled dylib through a bincode-serialized control
blob, because `TypeId`s and thread-locals do not cross the dylib boundary.

Originally the scripted layer offered only per-execution decisions (`release_values`,
`release_all`, `pause`). Hydrofrac added a second mode to the same hooks: a **standing policy**,
a filter applied autonomously at every execution with no entropy and no scripting. Hooks are
optional in hydrofrac: a hooked batch gives lineage an exact record of which tick execution
consumed which message and is what counterfactual replay drives, but the fault point is
elsewhere.

## 5. Fault injection: the filter on every send

### 5.1 The filter

The simulator's compilation of a `Network` node (`sim/builder.rs`, `create_network`) emits a
send half and a receive half joined by an in-memory channel. Every send half calls
`LineageHandles::record_sent` (`sim/runtime/lineage.rs`) before putting anything on the wire.
That call is the filter: it consults the instance's `FaultState` and returns the send's fate.

```rust
pub enum SendFate { Delivered, Omitted, Crashed }

pub struct SendKey {            // the identity of a send
    pub channel: usize,         // the Network node's sink id
    pub from: NodeKey,          // sending location and member
    pub to_member: Option<u32>, // destination member
    pub bytes: Vec<u8>,         // wire bytes
}

pub struct FaultState {
    pub lossy: HashSet<usize>,            // channels whose transport admits loss
    pub omissions: HashSet<SendKey>,      // sends to drop while finite failures last
    pub finite_failures_ended: bool,      // the EFF switch
    pub crash_points: HashMap<NodeKey, CrashPoint>,
    pub dead: HashSet<NodeKey>,
    ...
}
```

The filter is there whether or not the test asked for it; the program does not mention it, and
the harness registers no channels. The `lossy` set is installed by the host from the program's
declared networking before the instance runs: a `lossy_delayed_forever` channel admits omission,
a `fail_stop` channel admits only its sender's crash. The transport type is the program's own
statement of its fault model, and the filter enforces it at the sender, where the model is about
the channel rather than about one consumer of it.

A send that is delivered carries a fresh **delivery id** on the wire (`(u64, Bytes)`); the
receive half records it (`Recv`) before deserialization. An omitted or crashed send produces no
`Recv`.

### 5.2 Omission

`Fault::Omit(message)` names a `SendKey`. `Run::install` puts it in `omissions`; the filter drops
every send whose key matches while the channel is lossy and finite failures last. Identity is by
channel, sender, destination and bytes, not by send ordinal, so the same fault means the same
thing in every run even when the sender's send sequence shifts, and a byte-identical
retransmission is omitted too for as long as the fault stands; a retransmission with a different
payload (Raft's heartbeat with a higher `leader_commit`) is a different message. This is Molly's
omission.

What a message *is* to the user: `Message { channel: label, from_member, to_member, payload }`,
rendered as `label[from→to]:payload` (`rpc[0→1]:AppendEntries(..)`, `initial[→1]:Payload(42)`,
`msg:Msg { attempt: 1 }`). The label is the channel's name if the program gave it one
(`TCP.lossy_delayed_forever().name("rpc").bincode()`), else the `file:line` of the `send`, else
`ch#id`.

### 5.3 Crash: a pattern of applications of the same filter

`CrashPoint::AfterSend { channel, to_member, bytes }` and `CrashPoint::BeforeAnySend` are as in
the first version. The filter counts the send; if the node is already dead the send is
`Crashed`; if this send matches the node's crash point the node is marked dead afterwards (the
node performed the send; whether the network also lost it is the omission's business). Every
`sim_output` sink of a dead node is suppressed the same way (`LineageOutputSender`), so the
test body does not see outputs from a node that has died, which is what `run.crashed_member(..)`
is for.

So `crash(node after sending m)` means: the node runs normally, performs every send up to and
including `m`, and then every later send on every channel is lost and every later external
output is suppressed. It never restarts. Because the cut is at the sender, one crash point can
split the fan-out of a single broadcast: in the plain-broadcast counterexample, the broadcaster
dies after its second of three sends, and members 0 and 1 receive while member 2 does not.

Why the crash point is a message and not a count: the first version used `crash(node after k
sends)`. Under a different hypothesis the node's send sequence shifted (a background transaction
it would have decided was not there), so `send #7` named a different message, the solver's
constraints meant something else in the bug's world, and the search certified a program with a
known bug. With a message as the identity, if the named message is never sent the node simply
survives.

### 5.4 End of finite failures and the horizon

`Run::end_of_finite_failures()` flips `finite_failures_ended` and records the current length of
the event log. From then on omissions are ignored, and only sends recorded before that index are
omission candidates in later hypotheses. Crash points lie before it too: `crash(node after m)` is
proposed only for sends `m` performed before `EFF` (and `crash(node before any send)` always); a
crash is permanent, as in Molly, but it happens inside the finite-failures window. The reason is
what an end-of-run verdict means. The window after `EFF` is where the protocol is expected to
converge with no interference; a crash inside that window would need a convergence window of
its own after it, which the body cannot supply without knowing where the crash fell. (The first
version allowed crashes at any send. On Raft that produced a "violation" of durability in which
the leader crashed in the heal phase, after the body's last election timer had fired, and the
cluster, leaderless with no timer left to fire, was blamed for the entry it never got to
replicate.) Every dependency on a post-`EFF` send is killed by exactly the crashes that kill the
sender's last pre-`EFF` send, which is how the solver sees it. The horizon `EOT` is whatever the
test body drives: timers in Hydro are `sim_input`s the body fires, and
`hydro_lang::sim::quiesce().await` is a phase barrier.

### 5.5 What the hooks do now

A hooked `batch` is no longer a fault point. At the start of every run hydrofrac walks the
instance's registry of scripted hooks and places every batch hook under its traced
pass-through policy (`trace_all_hooks`, a type-erased `install_passthrough` on the hook shell),
so a hooked batch draws no entropy and its releases are logged against the tick execution they
fed. That is optional precision for lineage (section 6), and what counterfactual replay drives
(section 6.4). A program with no hooks at all is faultable and traceable; a program with hooks
needs nothing from the harness to use them.

Because a hook under a policy draws no entropy, the schedule explorer's decision space under a
hypothesis consists only of the program's remaining nondeterminism (which runnable tick goes
next, and what an unhooked batch releases). In the measurement grid this had a practical
consequence: at one schedule per hypothesis, every verdict and counterexample matched the
eight-schedule run on the protocols without schedule-sensitive bugs, so the fair cost of a
hypothesis is one simulator instance.

Raft is the exception. Every other harness runs one schedule per hypothesis
(`Exploration::Fuzz { instances: 1 }`, `ldfi_grid.rs`); the Raft harness runs four (`SCHEDULES`
in `raft_ldfi.rs`, passed to both `unit_test_fuzz_iterations` and `Exploration::Fuzz`), so a
Raft hypothesis costs four instances, which is the rate behind the instance counts in section 9.
The reason is that Raft's outcome under a fixed fault set can depend on the schedule (section 3,
departure 2): which member's tick runs next decides, for instance, whether a candidate's
`RequestVote` reaches a member before or after the old leader's `AppendEntries`, and so who holds
what when a timer fires. The staged scenario of the earlier harness lived on exactly such a race;
under one schedule the search could land on the harmless interleaving for the critical fault set,
see the goal hold, and certify a program with a known bug. The generic body's counterexample
(section 9) happens not to depend on the schedule, but the certificate on correct Raft does: with
sampled schedules it is relative to the sampled schedules (the doc comment on `Exploration::Fuzz`
and the `ldfi_certifies_raft_durability_under_one_crash` docstring both say so), and Raft is the
one protocol where that caveat bites; the other protocols' outcomes do not depend on tick order
under a fixed fault set, which is what the eight-vs-one measurement showed.
`Exploration::Exhaustive` is the sound alternative; the Raft harness does not use it, for cost.

### 5.6 What this fault point cannot do

- Embedded (raw-element) channels carry no serialized payload, no delivery id, and are not
  fault points.
- There is no `cut_after(n)` for a `fail_stop` connection dying on its own; the only fault on a
  reliable channel is the sender's crash.
- Delay and reordering (hold instead of drop) and partitions (a predicate on the destination
  set) are expressible at this filter and are not built.

## 6. Lineage collection

### 6.1 The event log

One simulator instance produces one `Vec<LineageEvent>` (`sim/runtime/lineage.rs`). The host
scheduler writes two kinds of event and the generated code writes three.

| Event | Written by | When | Contents |
|---|---|---|---|
| `Exec` | host | before each async run or tick execution | `ExecId`, location, member, tick-or-async |
| `Released` | host | right after a policy hook's trace grows during a tick execution | `exec`, `hook_id`, `member`, released items as bytes |
| `Sent` | the filter, at the send half | every network send | `exec`, delivery id, channel (sink id), from/to node, bytes, `Debug` rendering, `fate`, record provenance ids |
| `Recv` | generated receive half | every delivered arrival, before deserialization | `exec`, channel, to node, delivery id |
| `Output` | generated `sim_output` sink | every external output | `exec`, sink, port, member, bytes, `dropped`, record provenance ids |

The host sets `current_exec` in a shared `Rc<Cell<ExecId>>` before each execution and compares
each policy hook's `delivered` trace length before and after the hooks run to produce
`Released`. Generated code reads `current_exec` when it records `Sent`, `Recv`, `Output`. All
of this state is created in the dylib and handed to the host as `Rc`s, following the existing
hook pattern.

A receive names its send by delivery id, exactly. A hooked batch's release is the deserialized
item, so it is matched to a send by bytes: the earliest unmatched delivered `Sent` with the
same bytes, to the hook's node, on a channel the static slice says feeds the hook. (Matching by
bytes alone was wrong in the first version, where 2PC's `commit` was attributed to the
`prepare` of the same transaction, which carried the same payload; channel-scoped matching fixed
it.) A release that matches no send is a local or external item and is not a dependency.

### 6.2 Levels 0 and 1: execution lineage and the static slice

The support of an output is the backward closure from the execution that produced it. At
**Level 0** (`Level::Execution`) the rules are: a tick execution depends on every message its
hooks released into it, on every arrival at its node before it (an unhooked batch may have
released any of them), and, as an over-approximation of persisted state, on every earlier
execution of every tick at the node; an async run depends on what it received and on the tick
executions at its location since its previous run; a message depends on the execution that sent
it. Everything a goal output transitively reaches is in its derivation.

**Level 1** (`Level::Slice`, the default) narrows the first rule with a static slice of the
Hydro IR (`sim/slice.rs`), computed once per compiled program with the same traversal that emits
the simulator. For each network send and each `sim_output` sink, the slice walks upstream and
collects the inputs it can reach, split into `direct` (same execution) and `carried` (through
at least one `defer_tick`, so earlier executions): hooked batches, channels consumed at the top
level or through an unhooked batch (for the latter, every arrival on the channel at the node
before the execution, since the log does not say which execution released it), and the yields
of local ticks. Closures that read state through `Reference` captures add the slice of every
closure that writes the same cell. Only a multi-version network makes a sink `opaque`, which
falls back to the Level 0 rule. The LDFI log prints the channels and slices at the start of
every search:

```
ldfi: channels and static slices:
  channel#29 "initial": Process(loc1v1) → Cluster(loc2v1), reliable
  channel#51 "echo": Cluster(loc2v1) → Cluster(loc2v1), reliable
  sink#51 (async): direct {hook#0, hook#1, yield(...)} carried {hook#0, hook#1}
```

Both levels need nothing from the program. Both have the same weakness: a protocol written as
one step closure over a state struct is one IR node, so the slice says every output depends on
every input across all executions, which is Level 0 again.

### 6.3 Level 2: record-level provenance by IR rewrite

`SimFlow::with_provenance()` runs a sim-only rewrite (`sim/provenance.rs`) that wraps every
collection element as `Tagged<T>`: the value plus the set of delivery ids it was computed from.
`Tagged<T>` compares, hashes, orders, prints and serializes as `T`, so deduplication, joins,
set difference and the hook policies are unchanged. The receive half of every channel tags
each deserialized record with the delivery id of the send that produced it (the id travelled
with the message). `map`/`filter` pass the set through; `join`/`cross` union both sides;
`fold`/`reduce`/`scan` accumulate every contribution; cycles carry it; a network send or
`sim_output` strips the set from the wire payload and records it in the `Sent`/`Output` event's
`prov` field. The closure then follows record-to-record edges: delivery id → the `Sent` that
produced the record → its `prov` ids, and so on. An empty set is exact: the record was computed
from nothing that came off a channel (a constant, a timer, an external input).

The rewrite supports the operators the test protocols use (about 26 `HydroNode` variants) and
panics at compile time, by variant name, on anything else. Closures that capture state through
`Reference` (the `use::state` / `by_ref` / `by_mut` style, which is how `raft_step` and
`replica_step` are written) are compiled by `wrap_opaque`: the user's closure runs over plain
views of the tagged slots, and everything it emits is tagged `Prov::OPAQUE`, which means "the
rewrite cannot tell you what this came from". Level 2 then answers that one step by replay.

### 6.4 Level 3: counterfactual lineage by replay

The simulator is deterministic and the event log records exactly what every hook released into
every execution. That makes it possible to ask the lineage question directly: *if this delivery
had not happened, would the node still have produced this output?*

The mechanism is the `replay` field on the policies. A `ReplayScript<T>` (`tick_input.rs`) is
a queue of per-execution release lists. A hook under a replay script ignores its buffer and
releases `execs[k]` into the `k`-th execution of its tick, for exactly `execs.len()` executions,
including executions that release nothing. `CompiledSim::replay` runs a fresh instance with
every hook of node *N* under its recorded script (minus the deliveries being tested) and every
other node's hooks under an empty script (`replay_all_hooks`, installed type-erased from the
recorded bytes), driven by an all-zero entropy source so any unhooked in-tick choice resolves
the same way every time, with no body and no logging. Replay therefore needs the node's batches
to be hooked: it is the one place where hooks are still load-bearing, because only a hook
records what each tick execution consumed in a form the simulator can feed back.
The instance runs *N* alone with its inputs held fixed, in a few milliseconds, and its own event
log says whether the output appeared.

`Lineage::counterfactual` (`sim/ldfi.rs`) does greedy deletion, latest delivery first, one
replay per step: delete a delivery; if the output is still produced, leave it deleted; otherwise
put it back. The result is a 1-minimal sufficient set (defined in section 3). Each kept delivery
is a dependency and is followed to the send that produced it, whose own derivation is computed
the same way, memoized per run. Three details that each cost a false certificate before they were
right: the candidate set must include deliveries *after* the output's execution (a later echo can
substitute for the original); a replayed tick may legitimately have nothing to do once its only
input is deleted; and a node that does not reproduce the output with every delivery kept (one
fed through a hook-free path) falls back to the recorded lineage for that step. A fourth, found
on Raft: a replay feeds recorded inputs to a state that has diverged from the one they were
recorded against, and the program may not survive that (a leader asked to acknowledge an entry it
never appended in the replayed world indexed past its log). The generated crate catches the
panic on its own side of the dynamic-library boundary (`sim::runtime::erase_guarded`; unwinding
across it aborts the process, which it did), the host re-raises it as a test failure in a real
run, and a replay that panics counts as "target not produced", so the delivery whose removal
provoked it stays in the derivation. That is the conservative direction, a weaker clause; the
Raft searches of section 9 hit it 624 times in 365,000 replays.

`Level::Record` uses replay only at opaque records; a program written in dataflow style never
replays. `Level::Counterfactual` (`LDFI_LEVEL=replay`) replays every step and exists for
comparison; it costs about 5× the wall time of the recorded levels and 300–435 replays per
search on ISR.

### 6.5 Specifications: `PRE` and `POST` as views

In LDFI as published, the program and its specification are written in the same language, so
the specification has lineage: the search is not for a run that fails to deliver one concrete
message, but for a run in which a view `PRE` holds and a view `POST` does not, and the
derivations of `POST` are what the search negates. Hydrofrac's first version did not have this.
A goal was a shape (`goal`, `goal_all_members`) or a set of facts the body named by hand
(`witness`, `witness_final`, `require`) alongside a Rust verdict, and the two could disagree
(section 8.4 of the first version listed this as the main source of false certificates).

A [`Spec`](../../hydro_lang/src/sim/spec.rs) is the published formulation for a Hydro program.
It is a small Hydro dataflow of its own, on an **observer** process the simulator adds to the
program, that receives the outputs the specification is about and derives two views from them.
Reliable broadcast's agreement, in full:

```rust
let mut spec = Spec::new(&mut flow);
let delivered = spec.observe_cluster(deliveries);        // (member, payload) facts so far
let correct = spec.correct_members(&members, 0..3);      // the members that do not crash

// PRE: some member delivered m.
let delivered_m = delivered.filter(q!(|(_, p)| p.0 == 42));
spec.pre(delivered_m.clone().first());
// POST: every correct member delivered m.
let delivering = delivered_m.map(q!(|(m, _)| (m, ()))).unique();
let correct_delivering = correct.clone().map(q!(|m| (m, ()))).join(delivering).count();
spec.post(correct_delivering.zip(correct.count()).filter(q!(|(d, n)| d == n)));

Ldfi::new().crashable(..).crashable_cluster(..).crash_budget(1).spec(spec)
    .search(&compiled, async |_| { source.send(Payload(42)); quiesce().await; })
```

The same specification separates the protocols: with `echo = false` the search finds the
mid-fan-out crash in two runs; with `echo = true` it certifies under one and two crashes. The
body only drives the run; the verdict is the views' final values.

What the pieces are:

- **Observation.** `observe` / `observe_cluster` send an output stream to the observer over an
  ordinary channel and present it in the observer's tick as the *set of facts so far*: a
  bounded, sorted stream accumulated across the tick's executions. Views are ordinary Hydro over
  bounded collections (`filter`, `map`, `join`, `unique`, `count`, `first`, `zip`). The tick runs
  whenever a fact arrives; its last execution sees every fact. The scheduler runs the observer's
  ticks *eagerly*, as soon as they are runnable and outside the schedule choice: the observer
  only consumes, so its interleaving is unobservable to the program, and exploring it would
  multiply schedules (it did, by 64×, before this rule).
- **The fault model's facts.** `correct_members(cluster, members)` is the complement of Molly's
  `crash` relation: the members the hypothesis under test does not crash, fed by the search
  before each run through a `sim_input` on the observer.
- **`PRE` and `POST`.** Each is a bounded `Optional` in the observer's tick; the view holds if
  it is non-empty at the end of the run, as a Datalog goal holds if it is derivable at fixpoint.
  Each is published once per tick execution as an `Option<T>` on an external port, and the
  search reads the last value. `PRE` false makes the run vacuous; `PRE` true and `POST` false is
  the counterexample; both true yields `POST`'s derivation as the clause and `PRE`'s as the
  requirement to preserve (the first version's `require`).
- **Lineage through observations.** The observation channels are network sends through the
  filter, so a view's lineage runs through them to the executions that produced the outputs,
  but they are not fault points: the search never omits them (they are `fail_stop`) and never
  names a crash point by one of them (they are excluded from the send sequences). A view's
  dependency on an observed fact is rendered `observed[m→]:v from member@m through send #k`,
  killed by a crash of `m` before its `k`-th protocol send, plus whatever the fact itself was
  derived from. Under record-level provenance the derivation of a view is exactly the set of
  deliveries its value was computed from, which is why the broadcast harness compiles with
  provenance by default.

**Negation.** A view derived through negation (`filter_not_in`, an empty `count`) holds by
the *absence* of facts, which has no record provenance: under Level 2 alone the negated
agreement `POST` ("no correct member is missing") has an empty derivation, and an empty
derivation certified plain broadcast after one run. Molly struggled here too (bounded walks of
De Morgan-rewritten programs). The simulator has a better tool: it is a counterfactual oracle at
the run's nondeterministic decision points, and "what the observer's batch released" is one of
them. The derivation of a view is therefore always computed counterfactually at the observer:
the observer is replayed alone with observed facts removed one at a time (its batches are
hooked, so the simulator feeds it exactly what it saw), a fact is kept iff the view's final
value changes without it, and each kept fact continues into the observed node's lineage at the
configured level. That asks the same question of both formulations. The broadcast harness
states agreement positively and through negation and gets identical derivations
(`{initial[→0], initial[→1], initial[→2], observed[0→], observed[1→], observed[2→]}`), the same
two-run counterexample, and the same certificate; the observer replays cost about 8 per
schedule. When it landed, the intervention also found the *minimal* set where the record
provenance of the positive view had accumulated more (the one-crash certificate went from 15
hypotheses to 10 and the two-crash from 89 to 32); the record provenance has since been tightened
to the point that on these views the two derivations coincide, and both certificates are 10 and
32 whichever derivation is used.

What the observer replay cannot intervene on is the limit of this. It cannot alter a decision
inside an observed node (which arrival a member's deduplication picked is the record
provenance's business, and it is exact there), and it cannot alter the fault model's own facts:
`correct_members` comes from the hypothesis, not from a replayed decision. So a view that is
only falsified by *adding* a fault without removing any observed fact gets an empty derivation.
"Every member that delivered `m` is correct" is such a view: under no faults every deliverer is
correct, no fact's removal changes that, the derivation is `{}` and the search certifies after
one run. On this protocol that certificate happens to be right (the delivery is a member's last
act, so no crash point in the vocabulary follows it); on a protocol where a node sends anything
after delivering it would be wrong, and the search cannot tell the difference. The same
monotonicity gap is why the hitting-set inference ("a fault set that leaves the derivation
intact cannot break `POST`") is only an argument for views monotone in their support; the
greedy deletion finds a 1-minimal sufficient set of the facts, and for a non-monotone view a
superset of a sufficient set need not be sufficient. The search warns whenever `POST` holds with
an empty derivation.

The everyday non-monotone view is a **final value**. ISR's durability is "the *final* leader's
log holds the acknowledged write", with the final leader read off `observe_sequenced(views).max()`.
Deriving that counterfactually alone certified the unclean-election bug in six hypotheses: every
view in the failure-free run names the same leader, so deleting views latest-first never changes
the value, the sufficient set is the appended write plus the *initial* view, and the hypothesis
that breaks the property (lose a replication, crash the leader after it acknowledged, let ZK
promote the stale follower) kills neither — it adds a later view. Record provenance folds every
view into the aggregate and so keeps the heartbeats that kept the leader, and with them the
crash after the acknowledgement. `spec_view` therefore takes the **union** of the counterfactual
and the recorded derivation at the observer: sound where either is, minimal only where both
agree. `history-hydrofrac-negation.md` has the analysis.

**The grade, by construction (fifth round).** What the hitting-set inference needs is that
a derivation of the view implies the view: a fault set that leaves every delivery of the
derivation intact cannot break it. The fourth round tried to establish that per program with a
syntactic walk over the observer's dataflow (`polarity.rs`, grading each view monotone,
non-monotone or unknown and labelling the certificate accordingly). That was a hack, and it
existed because hand-written views can have any shape. It is gone. The one specification
hydrofrac is for is generated (`Spec::eventual_consistency`, section 1.2), and its shape makes
the implication hold by construction: a universal over a fact domain and a member domain the
hypothesis fixes, both fed to the observer by the search (`FACTS`, the facts of the failure-free
run, learned from a probe run; `CORRECT`, the members the hypothesis does not crash). Its
derivation is the record provenance of every `(member, fact)` pair, and keeping every one of
those deliveries keeps the view. So a certificate under the generated specification is graded
*within the fault model and the explored schedules*, as a property of the specification's form,
not of an analysis; and it takes the recorded derivation alone, with no observer replay. A
hand-written `PRE`/`POST` (`Spec::pre` / `Spec::post`, still available as an escape hatch) gets
the union of the counterfactual and recorded derivations and the grade *RELATIVE to the recorded
derivations persisting under the excluded fault sets (hand-written specification: no soundness
claim for the inference)*, because that is the truth about it: the view may be falsified by an
extra fact that no derivation names. The control is integrity, "no member delivered `m` twice":
true of the broadcast, with an empty derivation (no delivery's removal changes it, and the
absence of a duplicate has no record provenance), so the search certifies it after the
failure-free run having checked nothing, and the grade says so. `history-hydrofrac-negation.md` records
the fourth round's analysis of negation and polarity as history.

A deduplicating view (`unique`, `first`) keeps one derivation per run, so alternatives surface
one hypothesis at a time; the complete answer is lineage that carries alternatives (a set of
derivations per record), which is not built.

Every harness runs on a specification: agreement for broadcast and 2PC (`PRE`: someone
delivered / committed; `POST`: every correct member did), durability for ISR (`PRE`: the client
was acknowledged and the final view names a leader; `POST`: that leader's log holds the write),
and durability for Raft (`PRE`: some member committed some entry `(i, r)`; `POST`: every correct
member committed `(i, r)`), the terminating form of Leader Completeness. An earlier Raft harness
used state-machine safety instead (`POST`: no two committed entries disagree at an index), the
other everyday non-monotone view: agreement is the *absence* of a conflict, and a hypothesis
breaks it by making a member commit something else. The search still found the bug (in the
staged scenario) because the same hypothesis removed that member's recorded commit (the lost
`AppendEntries`), which was in the derivation; a conflict that appeared without displacing a
recorded commit would not be proposed. Agreement is not the property to search for with LDFI; it
is well covered by the simulator's own tests and by Maelstrom, and the Raft harness now asks the
question lineage is built for, whether a committed entry survives. The first version's `witness` / `witness_final` / `require` API remains available;
no harness uses it.

## 7. The search loop

`Ldfi::run` (`sim/ldfi.rs:2691`) is the loop. For each hypothesis:

1. Run the body under the fault set, via `CompiledSim::exhaustive` or `fuzz_bounded(n)`
   (`Exploration`). Each simulator instance yields one `RunRecord`: the event log, the body's
   verdict, its witnesses and requirements, the hooks' delivered and dropped traces, and the
   EFF cut.
2. Outside the instance (replays are instances of their own), turn each record into a
   `Schedule`: the goal's derivations, the requirements' derivations, every crashable node's
   send sequence in this run, and the set of all observed dependencies.
3. If any schedule violated the goal, return `Outcome::Counterexample { faults, schedule, runs }`.
4. Otherwise add every derivation of every held schedule as a `Clause { deps, sends, premises }`.
   `sends` is the run's send sequences, which give crash points their meaning in this run.
   `premises` is the set of crashes in this hypothesis: a clause observed in a world where a node
   was dead does not necessarily hold in a world where that node is alive (agreement in Raft is
   not monotone in a member's presence), so a hypothesis that does not include the premise is
   treated as hitting the clause.
5. Build the candidate universe: for every dependency in every clause, its omission (if the
   channel is lossy) and the crash of its sender right before that send (if the sender is
   crashable); plus the preserve points from requirements. Sort weakest first: omissions before
   crashes, and within a node a crash after a later message before one after an earlier message.
6. Solve for the next hypothesis (`next_hypothesis` → `solve_hypothesis`, HiGHS via `good_lp`):
   one binary per candidate fault; minimize a weighted count whose tie-break prefers weaker
   faults; one cover constraint per clause (hits + missing premises ≥ 1); the crash budget and
   at most one crash per node; a no-good per tried set; no superset of a hypothesis whose every
   schedule was vacuous; and, when feasible, at least one required derivation left intact.
7. If the ILP is infeasible, return `Outcome::Certificate`. If the hypothesis budget (default
   256) is exhausted, `Outcome::Budget`. If the failure-free run itself was vacuous in every
   schedule (`PRE` never held: the body protects nothing), `Outcome::Vacuous`, a verdict about
   the harness; before it existed such a search certified at once, with an empty derivation.

`Strategy::Blind` is the same loop with the cover constraints removed: it enumerates every
untried admissible fault set over the observed universe, weakest first. It is the control that
separates the value of lineage from the value of weakest-first order.

The log for plain broadcast, which is the whole search:

```
ldfi: run #1 {}: 6 schedule(s), goal held; derivations
  {{initial@0:Payload(42)←broadcaster#0, initial@1:Payload(42)←broadcaster#1, initial@2:Payload(42)←broadcaster#2}}
ldfi: universe {crash(broadcaster after initial@1:Payload(42)), crash(broadcaster after initial@0:Payload(42)),
  crash(broadcaster before any send)}; preserve points {}; requirements {}
ldfi: run #2 {crash(broadcaster after initial@1:Payload(42))}: 2 schedule(s), goal VIOLATED
LDFI: goal violated after 2 hypothesis run(s) by {crash(broadcaster after initial@1:Payload(42))}
  delivered in the failing schedule: {initial@0:Payload(42), initial@1:Payload(42)}
  sends lost to crashes: {broadcaster#2}
```

The failure-free run's derivation is the three fan-out deliveries. The weakest crash that kills
one of them is the broadcaster dying after its second send. That hypothesis violates agreement.

## 8. Using hydrofrac, and what it costs the user

There is no specification language. A hydrofrac test is ordinary Rust in three parts, and the
user writes all three.

### 8.1 Program side: nothing required, two things optional

Every network channel of the program is a fault point and is traced, by construction. The
program's transport declarations are its fault model: a `lossy_delayed_forever` channel admits
omission, a `fail_stop` channel admits only its sender's crash. The soundness rule of the first
version, *every channel a goal can depend on must be registered*, is discharged by the compiler.

Two things help and are optional.

- **Name the channels.** `TCP.lossy_delayed_forever().name("rpc").bincode()` makes the label in
  reports `rpc[0→1]:AppendEntries(..)` instead of `raft.rs:1151[0→1]:..`. The name is the same
  one multi-version deployments use.
- **Hook the batches**, for Level 3. A batch bound to a scripted hook (`use::batch(stream,
  nondet)` with a `NonDet<Option<BatchHook<..>>>` parameter the caller fills in with
  `nondet!(hook = h)`) records exactly which tick execution consumed which message, and is what
  counterfactual replay feeds. Raft (`hydro_test/src/cluster/raft.rs`) bundles four into
  `RaftHooks`, about 50 lines of declarations plus four `nondet!(..)` arguments; every other
  caller passes `nondet!(/** .. */)` and gets `None` everywhere. Without hooks, Levels 0 to 2
  work unchanged and Level 3 falls back to the recorded lineage at every step.

For Level 2, the program is compiled with `.with_provenance()`. The rewrite is automatic but
supports a fixed set of operators; using one it does not support fails at compile time by name.
Adding an operator is a `syn` closure-wrapping rule in `sim/provenance.rs`.

### 8.2 Test side: the specification and the body

The harness builds the flow, writes the specification, compiles the simulator, declares the
crash model, and writes the body. The broadcast harness (`reliable_broadcast.rs`) is the
listing in section 6.5, about 45 lines. The parts, and what each demands of the author:

- **Specification.** Which outputs to observe, which cluster's correct members the views need,
  and `PRE` / `POST` as Hydro over the observed facts. This is where the thinking is: the views
  must say what the property means ("every correct member delivered `m`", not "member 2
  delivered `m`"); whether `POST` is stated positively or through negation does not matter
  (section 6.5), but a view only falsifiable by adding a fault has no derivation.
- **Crash model.** Which nodes are `crashable` (processes) or `crashable_cluster` (members),
  and the budget. Omission needs no declaration: it is available on every lossy channel.
- **Body.** Drives the run: sends inputs, fires timers, awaits `quiesce()`. It returns nothing;
  the verdict is the views'.

The first version's API is still available and no harness uses it: `goal(&out)` ("some value
appears"), `goal_all_members(&out, cluster, members)`, and `goal_witnessed()` with the body
naming facts (`witness`, `witness_final`, `require`) and returning the verdict. Its hazards
(section 10) are the reason for the specification layer. The Raft harness (`raft_ldfi.rs`) is
about 170 lines on a `Spec`; its body is 25 lines, all scenario (which timers fire when), where
the first version's was 60 with the agreement check and the witness bookkeeping written by hand.

### 8.3 Running it

```
cargo test -p hydro_test --lib -- reliable_broadcast::tests::ldfi_breaks_plain_broadcast --nocapture
```

Environment knobs, if the harness opts into them: `LDFI_LEVEL=exec|slice|record|replay`
(`level_from_env()`), `LDFI_PROVENANCE=1` (the harness checks it before `with_provenance()`),
`LDFI_LINEAGE_DEBUG`, `LDFI_SOLVER_DEBUG`, `LDFI_REPLAY_DEBUG` for per-run detail. The result is
an `Outcome` with a `Display` that prints the verdict, the fault set, the failing schedule's
deliveries, and the derivations.

Build dependencies beyond Hydro's: HiGHS via `good_lp`, built from source, needs `cmake`, a C++
toolchain and `libclang`. The `ldfi` feature of `hydro_lang` gates everything.

### 8.4 Effort, honestly

| Protocol | Program change | Test module | Notes |
|---|---|---|---|
| Reliable delivery | none required; channels named | ~130 lines (two harness variants) | first example; run with and without hooks |
| Broadcast | none required; channels named | ~130 lines (harness ~45) | positive, negated and control `POST`s |
| 2PC + CTP | none required; channels named | ~200 lines + a comparison harness | goal conditioned on a commit |
| ISR replication | `Hooks` struct with 9 hooks, kept for Level 3; channels named | ~180 lines + the measurement grid | `witness_final` on the view |
| Raft | `RaftHooks` struct, 4 batch sites, ~50 lines, kept for Level 3 | ~180 lines, body ~60 | 4 schedules/hypothesis |
| `collect_quorum` (library) | none: an ~80-line program around the unchanged library call | ~130 lines | the first library-level target |
| Paxos (`paxos.rs`, 1,200 lines, never simulated before) | `LeaderTiming::Driven` (the wall-clock heartbeat and expiry check as input streams, ~110 lines beside the existing `WallClock` branch); `PaxosTestVariant` (the injected bug, lossy acceptor channels); channels named; one root-level `max().into_singleton()` moved into the tick. Two defects fixed (section 9.2). | `paxos_ldfi.rs`, ~600 lines for three harnesses; the core harness ~150, body 15 | no hooks; 4 schedules/hypothesis |
| Paxos + client + KV replicas | none beyond the above (`CorePaxosWith`, a `PaxosLike` carrying timing and variant) | ~150 lines in the same module | prefix agreement over `enumerate`d applied writes |
| Compartmentalized Paxos | `_with` variants threading timing and variant (~60 lines); the read quorum made a column (section 9.2) | ~130 lines in the same module | 8 nodes, 5 channels |

The hook structs predate the filter; they stay because counterfactual replay needs them (section
6.4), and because the protocols' other tests script them. Nothing else in the protocols exists
for hydrofrac, except timing: a protocol whose liveness is on the wall clock (`sample_every`,
`timeout`, `source_interval_delayed`) has to take its timers as inputs to be simulated at all,
which is the one retrofit Paxos needed and Raft was written with. Getting Paxos to *run* in the
simulator also cost four simulator gaps (a top-level `reduce`, snapshots of top-level optionals,
`Optional::or`'s tick on an atomic location, and `reduce_watermark` / keyed batches under the
provenance rewrite), none specific to fault injection.

The recurring cost is not the lines. It is writing the property down as views, which is the
same judgement as writing it in Datalog: what the facts are, what `PRE` conditions on, and
stating `POST` without negation. (The first version had further hazards of its own, whether a
fact was "ever" or "final" and whether every channel was registered, and its false certificates
came from them.) Users should expect to read the first run's derivation and check that it names
the messages they expected.

## 9. What it has found

On the paper's examples it reproduces the paper: tries-twice reliable delivery breaks under two
omissions in 3 runs; plain broadcast breaks under one mid-fan-out crash in 2 runs and reliable
broadcast certifies under one and two crashes (10 and 32 hypotheses), both under the one
agreement specification of section 6.5 (hand-written, so graded RELATIVE; the generated
specification of section 1.2 certifies the same protocol within the fault model); 2PC breaks
under one coordinator crash, 2PC with
cooperative termination certifies under one crash and breaks under two (coordinator after one
commit, then the participant that got it); ISR's unclean election reproduces the Kafka bug.

It also works at the library level. `hydro_std::quorum::collect_quorum` counts *replies*, not
*responders*, and leaves deduplication to the caller; `hydro_test/src/distributed/quorum_ldfi.rs`
puts it between a coordinator and three retransmitting responders over lossy channels (~80
lines of program, ~130 of harness, no changes to the library) under two specifications. The
quorum claim ("a declared quorum rests on replies from at least two distinct responders")
certifies for the deduplicating caller in 28 hypotheses and breaks for the caller that feeds the
library every reply in 5: silence two responders (`{omit req[→0], omit req[→1]}`) and the
third's retransmitted reply is counted twice. Availability of a one-shot request breaks in 2
hypotheses under the same two omissions, the paper's "try once" shape. Every operator
`collect_quorum` uses is in the provenance rewrite; the harness runs on sampled schedules
(eight per hypothesis) because three responders answering twice each give ~5,600
interleavings under exhaustive exploration.

Raft is next, as a curiosity (section 1.1: the bug is a safety bug outside the class, reached
through its convergence symptom), and it needs a correction first. `raft.rs` has thirteen behavioural tests
including unit tests of the §5.4.1 election restriction and two fuzz tests of concurrent
elections; none exercises message loss. An injected bug that compares last-log index only,
ignoring term (a mistake shipped in real implementations), passes all thirteen. An earlier
version of this section reported that hydrofrac finds it in 17 hypotheses. That number was
obtained with a test body that **staged the bug's scenario by hand**: elect member 1, land a
request at 1 while firing 0's election timer so the schedule decides who wins term 2, land a
request at 0, heartbeats from 0, end of finite failures, then fire 1's timer twice and request at
1. Everything the bug needs except the two lost messages was scripted by someone who knew the
bug; the search only had to find the two omissions inside that scenario, and the comparison with
Maelstrom in section 9.1 credited lineage for conditions the body had arranged. The author of
this document wrote that body and reported the result without saying so. The numbers below
replace it.

The body now (`hydro_test/src/cluster/raft_ldfi.rs`) is generic and was shaped only by what the
*correct* protocol needs to converge: `ROUNDS = 3` rounds of one member's election timer
(rotating), a client request at that member, and every member's heartbeat timer; then the end of
finite failures and a heal phase of two cycles in which every member's election timer fires, a
request lands everywhere, and three heartbeat rounds follow. The specification is durability of
a committed entry, in Molly's terminating form: `PRE`, some member committed some entry `(i, r)`
(the first commit the observer sees); `POST`, every correct member committed `(i, r)` by the end
of the run. Agreement proper (no index holds two entries) is the absence of a conflict and is
left to the simulator's own tests and to Maelstrom. Four versions of this body were shorter and
"violated" durability on *correct* Raft before the fifth did not, each a liveness artefact of the
harness: no election timer after the failures (leaderless cluster); no request of the new
leader's own term (this Raft appends no no-op on election, and §5.4.2 forbids committing a
prior-term entry otherwise); one post-`EFF` timer fire per member, spent on the suppression a
late pre-`EFF` heartbeat re-armed; and too few heartbeats for a follower to catch up under the
protocol's linear `next_index` back-off, which was replaced by §5.3's conflict hint in the
rejection (a protocol improvement made for the harness, since a heal phase long enough for the
linear back-off made every counterfactual replay a second long and a hypothesis an hour). The
body was never touched in response to anything the *buggy* protocol did.

Under that body, hydrofrac finds the bug in **90 hypotheses** (360 simulator instances at 4
schedules each, about thirty minutes on the sampled schedules), with four omissions and no crash:
`{omit rpc[0→1]:AppendEntries(r0), omit rpc[0→2]:AppendEntries(r0), omit rpc[1→0]:AppendEntries(r1)
×2}`. Member 0 wins term 1 and appends `r0`, whose replication is lost on both links; member 1
wins term 2 with 2's vote (0 denies, its log is longer), appends `r1` at the same index, replicates
it to 2 and commits it, while its replication to 0 is lost twice (the entry and the heartbeat
retransmission); in the heal phase 0's timer fires and it campaigns for term 3 with last entry
`(index 1, term 1)`: under the index-only check, 1 and 2 (index 1, term 2) grant, 0 leads, and
its heartbeats overwrite the committed `r1` at both. The scenario is cleaner than the staged one
(no crash, no race on the schedule) and it is one the author had not written down. Correct Raft
under the same body and bounds (one crash, omissions on the intra-cluster channel) **certifies in
235 hypotheses** (940 instances, about an hour; the two searches together ran 365,000
counterfactual replays, 624 of which the program did not survive, see section 6.4). The
certificate is graded RELATIVE, as every hand-written specification's is (section 6.5); Raft's
`committed` output under the generated specification is the in-class form of this test and has
not been run (an hour per search). The earlier staged harness, and the counts it produced, are kept in this
section's history only; both tests are `#[ignore]`d for their running time and record their
measurements in their doc comments.

What lineage contributed is now measurable honestly: 89 hypotheses held before the ninetieth,
each proposed from the derivations of the good outcomes seen so far, climbing from one omission
to four. The vocabulary in the failure-free run is about thirty pre-`EFF` messages and a dozen
crash points; a lineage-blind enumeration reaches four-omission sets after tens of thousands of
smaller ones. The earlier comparison (25 vs 160 vs random 0/5) measured the same question
*inside* a scenario that had already decided most of it, and is withdrawn. The blind and random
columns have not been re-measured on the generic body; at three to four minutes per hypothesis
at the end of the search, that is a day of compute per column and has not been spent.

Two things the search substrate learned on the way deserve their own mention, since each would
have produced a wrong verdict silently. A body whose `PRE` never holds in the failure-free run
(the first generic body sent requests to members that were not the leader, and this Raft drops
them) used to certify immediately with an empty derivation; it now returns `Outcome::Vacuous`.
And a counterfactual replay that feeds recorded inputs into a diverged member state can drive the
program into conditions no real execution reaches: Raft's leader indexed past its own log on an
acknowledgement for an entry it had never sent in the replayed world, and the panic, raised
inside the generated crate, aborted the whole process at the dynamic-library boundary. The tick
closure is now guarded on the generated side (`sim::runtime::erase_guarded`), the host reports a
program panic as a test failure in a real run, and in a replay treats it as inconclusive, keeping
the removed delivery as a dependency (the conservative direction: a weaker clause, never a
dropped one).

Measurement grid (ISR, 2PC at n = 2..6, workload 1..8; all in `ldfi-on-sim-hooks.md`, measured
on the first substrate; the per-protocol unit tests reproduce their counts on the filter, except
that ISR's dataflow encoding now certifies in 33 hypotheses rather than 20 because channels the
first harness never hooked, the client acknowledgements and the views, are fault points): lineage
beats blind enumeration everywhere, by 2–7× on certificates; against uninformed random it wins
on 2PC at every size by a margin that grows with size and never misses, while random misses in
2–4 of 10 repeats from n = 5; on ISR at small sizes random is competitive because the fault
space is small. Record-level provenance keeps the hypothesis count flat as workload grows (2PC
load 8: 11 hypotheses vs 73 for the slice levels); the slice is not a substitute. On the
closure-encoded ISR, replay turns 14/27 (bug/certificate) into 6/14, better than the dataflow
encoding under record provenance (10/18).

### 9.1 The same bug under Maelstrom

This comparison is no longer part of the argument for hydrofrac (section 1.1: the bug is outside
the class, and the case for hydrofrac is the shape of its output, not a race on a bug); it stays
because the runs were done and because what they show about fault vocabularies is true. The
Maelstrom runs predate the §5.3 conflict-hint change to `raft.rs`; the election path they
exercised is unchanged. To place the Raft result against an established tool, the same injected
bug was run under
Maelstrom (Jepsen's workbench), through a Raft-backed `lin-kv` workload written for the purpose
(`hydro_test/src/maelstrom/lin_kv_raft.rs`; there was none in the repository). Twelve runs, 29
minutes of test time, three and five nodes, partitions every 1–10 s, uniform and exponential
latency, rates 3–100: ~1,900 leader elections, no linearizability anomaly, and an in-node oracle
that prints whenever a committed index is overwritten never fired. The correct configuration
passes every run, so the adapter is not the problem.

The bug never *occurred* under Maelstrom, which is a statement about search and fault model, not
about the checker. The trigger is a conjunction: a node holding an unreplicated entry at an index
where a different entry has been committed, campaigning while its index is still at least the
majority's. The earlier version of this section listed four reasons Maelstrom stays away from it
and credited hydrofrac with all four; two of them (load not outpacing the stale log, and the
stale node's timer firing at the right moment) were conditions the staged body had arranged, not
things lineage found. What stands, against the generic body:

1. The search is directed. Nothing about Maelstrom's run `n` informs run `n + 1`. Hydrofrac's
   lineage of the committed entry names the `AppendEntries` whose delivery the good outcome
   rested on, and each hypothesis is the smallest set that kills every derivation seen so far;
   the four-omission counterexample was the ninetieth hypothesis, not one of `C(30, 4)`.
2. The fault vocabulary has the right shape. The trigger is *selective* loss: drop `r0` to both
   followers and `r1` to the old leader, deliver everything else, so logs stay the same length
   while their contents diverge. A partition drops every message on a link for an interval, and
   under load the isolated node's log falls behind, which the index-only check then correctly
   denies. Hydrofrac's omission is a standing filter on one message identity.

The timer point is now the body's: the generic body fires each member's timer twice in the heal
phase because that is what correct Raft needed, and the stale node's campaign falls out of it.
This is a result about one bug class (selective loss) and about Maelstrom's built-in nemeses; a
nemesis that drops individual messages would narrow the second point. Maelstrom earned its keep
regardless: its first run exposed a real bug in Hydro's Maelstrom backend (every
cluster-to-cluster channel shared one untagged message type, so a program with two such channels
deserialized each other's bytes), a code path no simulator test exercises. Different tools find
different failure classes.

### 9.2 Paxos, and two defects the simulator found on the way

Paxos (`hydro_test/src/cluster/paxos.rs`, the production consensus core behind the benchmarks)
had never run in the simulator: its leader liveness is on the wall clock. With the timers as
inputs, two proposers, three acceptors, lossy acceptor channels and the agreement specification
of section 6.5 over the chosen `(slot, value)` pairs, the injected bug (a commit quorum of `f`
acknowledgements instead of `f + 1`, which every unit test passes) was first found in 12–43
hypotheses across eight runs, 8–50 s, with the counterexample `{omit p2a(x)→acceptor 1, omit
p2a(x)→acceptor 2}` *under the schedule in which* the new leader's P1b quorum misses acceptor 0;
the spread came from that schedule, since the body's end of finite failures preceded the new
leader's first ballot and acceptor 0's P1b was not a fault point. The body now ends finite
failures after that first ballot, the counterexample is the three omissions `{p2a(x)→1,
p2a(x)→2, p1b(acceptor 0)→proposer 1}` and does not depend on the schedule, and correct Paxos
(with the P1b fix below) certifies under one crash in 372 hypotheses (196 before the move; the
first ballot's P1a/P1b exchange is now inside the fault window). Through the replicated key-value
store (one client, two replicas, prefix agreement over each replica's applied writes with their
position) the store certifies in 301; its bug test is ignored, honestly: the client forwards a
write to the leader it last heard of through a snapshot the simulator may leave stale, so whether
`x` is sequenced by proposer 0 at all is the schedule's, and the earlier "found in 42" was one
draw of that lottery (the failure-free run's own derivation shows `x` sequenced by proposer 1).
Compartmentalized Paxos (two proposers, two proxy leaders, a 2 × 2 acceptor grid, eight nodes)
certifies in 568 hypotheses (124 before) and its injected bug (a write quorum one acceptor short
of a row) in 3. The three certificates together take about 45 minutes at four sampled schedules
per hypothesis.

Three findings were not injected.

- **A message loop in Paxos's leader change.** The values a new leader recommits are derived from
  its P1b quorum, a fixed snapshot, and were re-sent on every later tick of the leader; each round
  of P2bs triggered the next tick, so once a recommit was needed the proposer and the acceptors
  exchanged the same P2as and P2bs forever. No deployment test exercises a leader change with a
  partially replicated slot. The simulator surfaced it as the first faulty hypothesis never
  quiescing; the fix gates the recommit on the tick the proposer becomes leader.
- **Compartmentalized Paxos's read quorum was not a column.** The leader election counted any
  `rows` P1bs of the `rows × cols` grid, so a proposer could be elected on a row's worth of P1bs
  (the default deployment is a 2 × 2 grid), which does not intersect the other row, the write
  quorum of half the slots. Hydrofrac reported a violation of agreement on the *correct* variant
  in 8 hypotheses, with a failing schedule in which proposer 1's quorum was the acceptors of row 1
  while `x` had been chosen on row 0; no fault was needed, only that P1b order. The fix has P1bs
  name their acceptor and the proposer count them per column, which is the paper's quorum
  intersection argument made explicit; plain Paxos is the one-column case.
- **A P1b quorum of two replies from one acceptor.** A proposer whose check finds no leader
  re-sends the P1a of the *same* ballot, and an acceptor answers every P1a it receives, so a
  proposer can hold two P1bs from one acceptor for one ballot. `collect_quorum` counts replies,
  not responders (section 9, the quorum harness), and Paxos's leader election counted them per
  `(ballot, column)` without naming the acceptor, so two replies from one acceptor made a quorum
  of two. The earlier harness never saw it because its end of finite failures preceded proposer
  1's first check, so the first round of P1bs always arrived and the quorum was distinct
  acceptors' by luck of ordering. The body now ends finite failures after that first check (so
  that acceptor 0's P1b is a fault point and the injected bug no longer depends on the sampled
  schedule), and the search reported a violation of agreement on *correct* Paxos in 48 hypotheses
  and on correct compartmentalized Paxos in 18: lose the first-round P1bs of the two acceptors
  holding `x`, let the third's first and second replies make the quorum, and `y` is proposed at
  the slot `x` was chosen in. Loss is not even needed: two slow acceptors and one fast one, a
  timeout and a retry, and the fast one's two replies are a quorum. The fix keeps one P1b per
  `(ballot, acceptor)` before counting (any of them is a sound view of the acceptor's log: having
  promised the ballot, it accepts only that ballot's own proposals afterwards).

All three are defects of the kind the simulator finds given a scenario that drives a leader
change; what hydrofrac added is the scenario's faults and, for the second and third, the report
that named the schedule or the lost messages. The third is also a lesson about harness bodies:
where the end of finite failures falls decides which messages are fault points, and the harness
that cannot lose a message cannot test the protocol's handling of its loss.

## 10. Limitations and what is not built

- **The filter is a simulator artefact.** It lives in the simulator's compilation of `Network`
  nodes, not in the IR, so it does not exist under `deploy` (section 11.3). Embedded channels
  are not fault points. Delay, reordering and partitions are not built.
- **Level 3 needs hooks.** Counterfactual replay feeds a node its recorded per-execution inputs,
  which only a hooked batch records; a protocol without hooks gets the recorded lineage levels.
  The receive-side "source" of section 11.3 would remove this.
- **Properties are checked at quiesce.** "Eventually" means "by the end of the run the body
  drove". Properties that need an unbounded future are out of scope, as in Molly.
- **`EFF` is a leftover.** Molly's end of finite failures and horizon `EOT` were how a search
  over logical time was bounded. In the simulator the horizon is what the body drives, and
  omission candidates are the sends the run observed, so `end_of_finite_failures` is only an
  optional bound a body may impose (reliable delivery uses it to make "try twice" fail in bounded
  rounds). It should not shape the design further.
- **Certificates assume monotonicity, and the interesting properties are not monotone.** The
  hitting-set inference ("a fault set that leaves this derivation intact cannot break `POST`")
  is valid only for views monotone in the facts they observe. Agreement is the *absence* of a
  conflict; durability of a write rests on the *final* leader view; both are non-monotone, and
  `history-hydrofrac-negation.md` shows a false certificate from exactly this on ISR before the observer
  derivation was made the union of counterfactual and recorded lineage. The search now finds
  these bugs because the breaking hypothesis also displaces a recorded fact in `POST`'s
  derivation; a conflict that appeared without displacing one would not be proposed. The
  certificate for such a property is relative to that, and the grade says so (section 6.5):
  *within the fault model* only for the generated specification, whose form guarantees the
  inference; *RELATIVE, no soundness claim* for every hand-written view. The fourth round's
  syntactic polarity check, which tried to grade hand-written views one by one, was removed:
  it could only approximate the property, and with one specification to certify there is
  nothing left for it to classify. The first version's hand-named goal API (`goal`, `witness`, `require`) is
  still present and unused; it had the same gap and more (a forgotten requirement or an "ever"
  fact that should have been "final" was a wrong certificate with no warning).
- **One derivation per record.** Deduplicating operators keep the first provenance seen, so a
  view's alternatives surface one hypothesis at a time (section 6.5).
- **Replay assumes monotone steps.** A 1-minimal sufficient set found by greedy deletion is one
  derivation, not all of them, and the deletion order can matter for a step that is not monotone
  in its inputs. Several derivations per output (ddmin) are not implemented.
- **Vacuous-superset pruning is a heuristic.** Vacuity is not monotone in the fault set.
- **Scale.** Tested to a few hundred fault points, eight records of workload, seven members.
  Exhaustive schedule exploration is out of reach past the smallest configurations; sampled
  exploration makes the certificate relative to the sampled schedules.
- **Not measured.** A coverage-guided fuzzer (libFuzzer under `cargo sim`) as a baseline; blind
  and random to the certificate on correct Raft; Raft under load; a per-node replay driver that
  does not instantiate the whole dylib.

## 11. The redesign: sender-side filters

The first version put the fault point on the receiving `batch`: a hook on a `NoOrder` stream
accepted an `OmissionPolicy` (values to discard on arrival), a hook on a `TotalOrder` stream a
`PassthroughPolicy`, and the harness registered each hook as a `lossy_channel`,
`reliable_channel` or `input` with a label. That conflated two things the simulator provides: a
place to interpose, the `BatchHook`, and deterministic control of the schedule. LDFI needs the
second. The fault model was built on the first because interposition there was free: the
scripted-hook layer already existed, a standing policy was a small addition to it, and the hooks
were already the place where lineage could see what a tick consumed. The cost was a list of
limitations. A channel consumed without a hooked `batch` (a `recv.map(f).send(..)` forwarding
path, a `sim_output` straight from the channel) could not be faulted and was invisible to lineage
except through a special rule for `Recv` events; a `tee`d channel output omitted at one consumer
was still delivered at the others; every faultable batch was a hook parameter threaded through
the protocol's signature; crashes needed a separate mechanism in the generated send half because
a receiving hook cannot stop a node from sending; omission was a standing policy on bytes because
a receiving hook has no notion of a message instance; and the soundness of every certificate
rested on the harness author having registered every channel. All of these followed from one
choice: the fault point was a receiver-side test seam, not a model of the network.

Section 5 describes what replaced it. This section records the proposal as it was made, what
of it is built, and what is not.

### 11.1 The proposal

The compiler inserts a **filter** node in front of every `Network` send and every external output
sink. The filter belongs to the fault model, not to the program: it is there whether or not the
author asked for it, and the author's code does not mention it.

Omission is one application of the filter: drop the sends that match a predicate keyed by `(send
site, destination, bytes)`. This is the identity `CrashPoint::AfterSend` already uses, and the
lesson of section 5.3 stands: do not key on send ordinals, because they shift between hypotheses
and a constraint written against one world then means something else in another.

Crash is a pattern of applications: after send `m`, drop every send on every channel from the
node and suppress every external output. One mechanism, two patterns; this is Molly's own
formulation of its fault vocabulary, which section 5.3 reached by a different route.

`end_of_finite_failures` flips a runtime switch on every filter rather than replacing every hook's
policy with `deliver_all()`.

The transport type keeps its role. A `lossy` connection permits omission at its
filter; a `fail_stop` connection permits only the sender's crash. The program still states its own
fault model, and the filter enforces it at the sender, where the model is about the channel rather
than about one consumer of it.

### 11.2 What it fixes, and what it adds

Against the limitations listed at the top of this section, item by item:

- Hook-free consumption paths and `tee`d outputs: every send passes through the filter, so what
  the receiver does with the channel is irrelevant.
- Program intrusion: no `nondet!(hook = ..)` threading, no `RaftHooks` struct. Every `Network`
  node is faultable by default, and the first version's soundness rule ("every channel a goal can
  depend on must be registered") is discharged by construction.
- Crash as a separate mechanism in the send half: subsumed.
- Omission as a standing policy, and EFF as policy replacement: a predicate and a switch.

Beyond the current vocabulary, the same node gives the faults the hooks could not. Hold instead of
drop is delay and reordering; a predicate on the destination set is a partition; and because the
filter is an IR node rather than a simulator artefact, it exists under `deploy` as well as `sim`.

### 11.3 What the simulator must still provide

Moving the fault point out of the simulator does not make the simulator optional. Four things
stay on its side.

Deterministic re-execution of a fault set. This is Molly's third requirement, and the search loop
depends on it: "the outcome still held, so the program has another derivation" is only a valid
inference if the run differed from the last one by the fault set alone.

Schedule exploration. A Raft outcome under a fixed fault set can depend on the interleaving
(section 4); a filter under `deploy` yields one interleaving per run, chosen by the operating
system, which is Molly's fixed schedule or worse.

Level 3 replay. Counterfactual lineage re-runs one node with a recorded set of deliveries fed into
it and everything else withheld. That is inherently receiver-side. Its natural form in this
design is the mirror image of the filter: a compiler-inserted **source** at every `Network`
receive that, under replay, feeds recorded bytes instead of the wire. With the filter on the send
side and the source on the receive side, `BatchHook` drops out of both fault injection and
lineage and goes back to being what it was built for.

Phase barriers and timers. `quiesce()` and the `sim_input` timers are how the body defines the
horizon; a deployment would need wall-clock equivalents.

The division of labour this leaves is the one the prototype should have started from: the
simulator's job is determinism, schedules and replay; the fault model's job is the IR.

### 11.4 Migration points for lineage

Two things in the first version's lineage change.

Delivery ids are minted at the `Sent` event, not at the hooked batch's `Tag`. One id per send,
carried to the receiver as the record's provenance; the `Tag` event goes away, and the
delivery-to-send matching (earliest unmatched `Sent` with the same bytes on a channel the slice
says feeds this hook) becomes unnecessary for receives because the id travels with the message
(it remains for the releases of a hooked batch, whose items are deserialized). Level 1's slice
becomes "network receives reachable from the sink" where there is no hook, instead of marking
the sink opaque.

An omitted send is recorded as `Sent { fate: Omitted }`, exactly as a crashed node's send is,
and produces no `Recv`. The receiver-side drop recorded a `Recv` for a message that was never
delivered, which was wrong. The first version's rule that a `Recv` on a channel with no hook
still yields a dependency on its send was a workaround for the gap the filter closes; it is now
simply the rule for every receive.

### 11.5 Status

Built, in the simulator rather than in the IR, with the following correspondence to the proposal.

- The filter (11.1) is `FaultState::filter` behind `LineageHandles::record_sent`, which the
  simulator's `create_network` emits at every send half. Omission is a predicate on `(send site,
  sender, destination, bytes)`; crash is the pattern of applications; `end_of_finite_failures`
  is a switch. External output sinks are filtered for crashes only (there is no omission of an
  output).
- Channels are fault points by construction (11.2): `Ldfi::lossy_channel`, `reliable_channel`
  and `input` are gone; the transport type decides what the filter admits; labels come from the
  channel's name or the `send`'s source line.
- Delivery ids are minted at the send and travel on the wire (11.4); `Recv` names its send by
  id; the `Tag` event and the batch-side tagging are gone; the receive half tags records under
  provenance; an omitted send is `Sent { fate: Omitted }` and produces no `Recv`; the special
  `Recv` rule is gone, replaced by the ordinary rule that a receive depends on its send.
- Hooks are optional precision (5.5). The search drives every scripted batch hook of the
  program itself, type-erased, so the harness never scripts them; Level 3 still needs them.

Not built: the filter as an IR node, so that it exists under `deploy`; delay, reordering and
partitions at the filter; the receive-side source that would let replay feed recorded bytes
into a node without hooks, which is what would let `BatchHook` drop out of lineage entirely.
The simulator-side obligations of 11.3 (determinism, schedule exploration, replay, barriers)
are unchanged.

## 12. Where it stands, and what next

Built, across the five rounds: the search loop with an ILP hitting-set solver; the sender-side
filter as the fault point, with every channel a fault point by construction and the transport
type as the fault model; message-named crash points; four lineage levels, with record provenance
exact to the send and counterfactual replay at node granularity; specifications as Hydro views on
an observer, derived by intervention and by record provenance together; five protocols on the
same harness shape (reliable delivery, broadcast, 2PC with cooperative termination, ISR
replication, Raft), each reproducing its published or injected bug and certifying the correct
variant; and a comparison against Maelstrom on the Raft bug (section 9.1).

Built in the fourth round and removed in the fifth: a syntactic polarity check of hand-written
specifications (a walk over the observer's dataflow grading each view monotone, non-monotone or
unknown, to label its certificate). It was a hack born of accepting arbitrary views; once the
specification was generated and its soundness established by its form (section 6.5), the walk
had nothing to classify. Certificates are now graded by what produced the specification: within
the fault model for the generated one, RELATIVE with no soundness claim for a hand-written one.

The fifth round was a correction (section 9) that ended in a definition (section 1.1):
hydrofrac discharges the premise of the `EventualConsistency` label, it is the only tool in the
chain with the output shape that task needs, and within its bounds its certificate is complete
for it. The Raft harness had staged its bug's scenario; it
now runs a generic body shaped only by what correct Raft needs to converge, under a durability
specification, and the search finds the bug in 90 hypotheses and certifies correct Raft in 235.
Getting there fixed four things in the substrate that each would have produced a wrong verdict
silently: a search whose failure-free run never satisfies `PRE` returns `Outcome::Vacuous`
instead of certifying; a program panic inside a counterfactual replay is contained on the
generated side of the dynamic-library boundary and treated as inconclusive instead of aborting
the process; crash points lie before the end of finite failures, like omissions, so that the
convergence window is one; and Paxos's P1b quorum counts acceptors, not replies, a defect of
the production protocol that the Paxos body exposed once its end of finite failures moved past
the first ballot (section 9.2). The Raft bug test and certificate are `#[ignore]`d for their
running time (half an hour and an hour); the Paxos key-value bug test is ignored because the
sampled schedules decide whether its scenario happens at all (its doc comment says why).

Next, in the order that the argument of section 1.1 requires.

0. **Specifications generated from `EventualConsistency` labels.** Built
   (`Spec::eventual_consistency`, sections 1.2 and 6.5): the fact domain is learned from a
   probe run, the view is sound by its form, and every producer of the label in the codebase
   has been run under it, as has every demo whose labelled output is a cluster-resident set
   (section 1.2). Left: a lattice-aware variant of the generated specification for `Singleton`
   outputs such as `abd`'s register (observe the final value per correct member, POST is
   equality); a convergence witness in `PRE` for protocols that can report
   one (every correct member has quiesced on the same view; what "view" is per protocol is the
   one thing a user may have to name), so that an unconverged run is vacuous rather than a
   counterexample. The lint is built (section 1.2) as a report at the start of every search;
   making it a failing check would need a notion of "every search over this program", which a
   test-suite-level registry of specifications per compiled program could provide. The Raft
   harness should be rewritten
   on the same pattern (its `PRE` today is "some member committed", which is why its body had
   to be tuned until the protocol converged; section 1.1), which needs Raft to expose the
   acknowledgement of the leader's latest heartbeat as an output.
1. **Decision-trace replay.** Record every nondeterministic decision of a run (the scheduler's
   draws, the hooks' releases, the filter's fates) and replay a prefix exactly with one decision
   overridden. This is the general counterfactual oracle of which node-level replay is the
   coarse instance: why and why-not for any fact at any step, without hooks, and a decision
   sequence rather than entropy bytes as the counterexample's schedule. It is worth building only
   if the graded certificates show the ones it would strengthen are the ones users want; it does
   not by itself repair the inference for non-monotone views.
2. **More protocols.** Section 9 covers the paper's examples, Raft, and the quorum building
   block. Candidates in the repository: the Paxos family (`paxos.rs`,
   `compartmentalized_paxos.rs`, `paxos_with_client.rs`) with agreement and validity as views,
   and the replicated key-value store (`kv_replica`) with a linearizability-shaped
   specification; each is a protocol written without hydrofrac in mind, which is the retrofit
   the effort table (section 8.4) has not yet measured.

   **Membership change** is the richest vein historically (Raft's joint consensus, the
   ZooKeeper reconfiguration anomalies) and there is nothing on this branch to run it on yet.
   `hydro_std::membership::track_membership` is a 19-line fold over `MembershipEvent`s;
   `source_cluster_membership_stream` is a deployment-time input (ECS discovery) that the
   simulator renders as a static "everyone joined" stream (`sim/graph.rs`); no protocol in
   `hydro_test` changes its configuration while running (`dyn_raft` lives on `main`, not here).
   When one exists, the shape is: the specification is agreement *across an epoch boundary*
   ("no index is committed with two values by members of the old or the new configuration, and
   every entry committed under the old configuration is in the logs of the new one"), with the
   configuration-change entries observed as sequenced facts so "old" and "new" are views; the
   fault model is a crash *during* reconfiguration (the leader after sending the joint
   configuration to some members but not others, which the message-named crash points express
   directly) plus omission of the configuration entries themselves; and the simulator needs
   membership events as a driven input (a `sim_input` of `MembershipEvent`, so the body can
   stage a join or a leave) and a decision about whether a crashed member is also `Left`
   (today a crash is silent; `correct_members` would have to reflect the epoch). None of that
   is hard; the protocol is the work.
3. **Raft's `committed` under the generated specification.** The durability harness of
   section 9 is hand-written and graded RELATIVE; the same question under
   `Spec::eventual_consistency` is the in-class form and would carry the sound grade. It has
   not been run because a search takes an hour; it should be, once, for the record.
4. **Replay cost on long runs.** A hypothesis late in the Raft certificate search costs three to
   four minutes, almost all of it counterfactual replays (about a thousand per hypothesis, each a
   full re-execution of one member). Two things would cut it: not minimizing over deliveries
   after `EFF` (they are not intervenable; a first attempt kept them all as dependencies and made
   the recursion worse, since minimization is also what prunes redundant retransmissions), and
   caching replays across hypotheses whose scripts share a prefix. Neither changes a verdict.
