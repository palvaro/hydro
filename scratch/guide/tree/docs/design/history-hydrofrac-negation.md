> **History.** This is the fourth round's analysis of negation and polarity in hand-written
> specifications. The polarity check it led to (`polarity.rs`) was removed in the fifth round:
> hydrofrac's specification is now generated from the `EventualConsistency` label and sound by
> its form (`hydrofrac.md` §1.1, §6.5), so there is no view left to grade. The analysis stands
> as the reason the hand-written path is labelled "no soundness claim".

# Negation in hydrofrac specifications

Status: analysis, written alongside the `Spec` layer (`hydro_lang/src/sim/spec.rs`, `hydrofrac.md`
§6.5). It asks whether a `POST` written through negation can be given lineage, and if so under what
conditions the search that negates that lineage remains sound. Code references are to
`hydro_lang/src/sim/ldfi.rs` and `sim/provenance.rs` as of this branch; claims about Molly are
marked as such.

## 1. The problem

The natural statement of broadcast agreement is through negation: no correct member is missing.

```rust
let missing = correct.filter_not_in(delivering);   // correct members that did not deliver m
spec.post(missing.count().filter(q!(|n| *n == 0)));
```

Under record-level provenance this `POST` has an empty derivation. The reason is mechanical.
`filter_not_in` is a `Difference` node and `anti_join` an `AntiJoin`; the provenance rewrite
(`sim/provenance.rs`) treats both as operators whose output records are the records of the
*positive* side: `Difference` is in the tag-pass-through group, and `AntiJoin` strips the tags of
the negative side before the operator (`|__x: Tagged<_>| __x.0`) and re-attaches the positive side's
tag after (`key_in`). That is the correct why-provenance for an element that *survives* the
difference: it is there because it was in `pos`, and nothing in `neg` matched it. But `POST` is not
about the survivors. It holds because `missing` is *empty*, and an empty collection's count has no
contributing records. `count` is a fold; a fold's provenance is the union of its inputs' tags;
there are no inputs; `Some(0)` is tagged with nothing. `correct` is a `sim_input` and carries no
tags either, so even the surviving-element path would have contributed nothing.

The search then sees a derivation `{}` for `POST`; `solve_hypothesis` builds `Σ hits ≥ 1` over an
empty sum, the ILP is infeasible, and the outcome is `Certificate` after the failure-free run: a
false certificate, silent except for the warning `Ldfi::run` prints when `POST` holds with an empty
derivation. The positive formulation in the broadcast harness (count the correct members that *did*
deliver, compare with the count of correct members) avoids this because every delivery is folded
into the first count; the other operand (`correct.count()`) is untagged, rightly, since it is not a
fact a fault can remove.

## 2. What Molly did

What the LDFI paper (Alvaro, Rosen, Hellerstein, SIGMOD 2015) establishes, as I read it: the
specification is a pair of Dedalus/Datalog views `pre` and `post`; the search is for executions in
which `pre` holds and `post` does not; hypotheses are hitting sets over the derivations of the goal;
negation is permitted in the program and the specification subject to stratification; and the
search is bounded in logical time by `EOT` (the horizon) and `EFF` (after which no more omissions
occur), which is what makes the set of possible derivations finite.

What I am reconstructing rather than citing: Molly computed provenance by rewriting the Dedalus
program so that each rule's firings were recorded as facts, and for rules with negated subgoals it
produced provenance for the *absence* by enumerating, within the finite horizon, the derivations of
the negated goal that did *not* fire (a De Morgan rewrite of the rule body into a disjunction of
"the positive subgoal held and some premise of every derivation of the negated subgoal failed"),
bounded by the horizon's logical-time walk. The user's recollection of "bounded walks of
De-Morgan-rewritten Dedalus programs" matches this shape. I am not confident of the exact
mechanism or of how complete it was, and this note does not depend on it. The relevant point is
that Molly treated negation as a provenance problem to be solved by rewriting the program within a
finite logical-time horizon. Hydrofrac has a different instrument, a simulator that can re-run a
recorded run with one decision changed, and the next section argues that this makes the rewrite
unnecessary.

## 3. Lineage as intervention on nondeterministic decisions

A simulated run is a function of a finite sequence of **nondeterministic decisions**, and nothing
else. Between decisions the program is deterministic: a tick execution is a function of what was
released into it, a send's bytes are a function of the execution that performed it. The decisions
are of two kinds:

- **The scheduler's.** Which runnable tick executes next (`compiled.rs`, the `(0..candidate_count).any()`
  draw in the tick-selection loop); what a batch releases into that execution and in what order
  (`autonomous_decision` of each unhooked `StreamHook`, drawing a prefix length or an index per
  element from the bolero driver, in `run_hooks`); which version a snapshot reveals
  (`SingletonHook`, `OptionalInitNoneHook`). Under `exhaustive` the bolero driver enumerates these
  draws; under `fuzz` it samples them; a hooked batch under a pass-through policy makes no draw.
- **The environment's.** The filter's fate for every send: delivered, omitted, or lost to a crash
  (`FaultState::filter`). These are not entropy; they are installed before the run as the
  hypothesis, and they are the decisions fault injection intervenes on.

Every question hydrofrac asks is a question about these decisions. Fault injection varies the
environment's decisions and holds the schedule to exploration. Schedule exploration varies the
scheduler's. And *lineage*, properly stated, is: **which decisions does the presence of this fact
depend on?** The simulator is a counterfactual oracle for exactly that question, because it can
re-run the recorded decision sequence with one decision altered and report whether the fact is still
there. Why-provenance of a positive fact is the set of decisions whose alteration removes it.
Why-not provenance of a negative fact (`missing(N)` holds, i.e. `delivered(N, m)` is absent) is the
set of decisions whose alteration *derives* `delivered(N, m)`: deliver the omitted send, let the
crashed node live one send longer, run the tick that held the message. Both are the same operation
from the oracle's point of view: reproduce the recorded trace up to decision `k`, alter `k`, continue
from there under some rule for the remaining decisions, look at the result. There is no De Morgan
rewrite because there is no rule body to rewrite; the oracle does not know how `POST` was computed.

What hydrofrac has today is the coarse special case. `Lineage::counterfactual` intervenes at node
granularity: it replays one node alone with the recorded releases into its hooked batches held
fixed (`replay_scripts`, the per-execution release lists), deletes candidate releases one at a time,
latest first, and keeps a deletion if the target fact (`produced`, for `POST` "the last output on
this port equals the recorded bytes") is still there. The result is a 1-minimal sufficient set of
the node's inputs; each kept input is a delivery whose send is then followed into the sender's own
lineage. "Delete this release" is one particular intervention: it stands for "the decision that put
this message into this execution went the other way", and it is only expressible because the batch
is hooked and its releases are recorded. Everything else about the replay is held to zero entropy
(`CompiledSim::replay`'s all-zero driver), which is why the other nodes are silenced rather than
re-run.

For the negated agreement `POST`, this coarse oracle already gives the right answer. The observer
is the node; its inputs are the observations and the `correct` facts, all hooked (the `Spec` creates
the hooks). Deleting any correct member's delivery observation makes `missing` non-empty, the count
becomes `1`, the `filter` drops it, the last output is `None`, `produced` is false, the deletion is
undone. The `correct` releases are not candidates (`matters` requires a release matched to a send,
and `correct` comes from a `sim_input`) and are always kept. The surviving set is every correct
member's delivery, identical to the positive formulation's derivation under record provenance and to
what Molly's rewrite would have produced. The replay is cheap: one process with one tick, nothing
else active, a few milliseconds per deletion. What is missing is only routing: `Goal::Spec` derives
`POST` through `witnessed_final` → `derive_final`, which replays only under `Level::Counterfactual`
or for opaque records. The sibling implementation thread is adding "always replay at the observer".

The general oracle is a **decision trace** and a replayer that reproduces a prefix of it. It would
need, from the scheduler:

1. *Indexing.* Every draw gets a sequence number and its outcome is recorded: `(k, TickChoice i)`,
   `(k, BatchRelease { hook, items })`, `(k, Reveal { hook, version })`, and for the environment
   `(k, Fate { send id, fate })`. Today the scheduler records executions and hook releases
   (`Exec`, `Released`) and the filter records fates (`Sent { fate }`), but tick choices and
   unhooked batch decisions are not in the log; they live only in the entropy stream.
2. *Reproduction.* A driver that answers draw `k` with the recorded outcome rather than from
   bytes. The bolero `Driver` interface already allows this (the replay driver is a `Driver::new`
   over a byte vector; a trace-backed driver would implement the same trait), and `FaultState`
   already takes installed outcomes rather than drawing them.
3. *Override and continuation.* Alter decision `k` (deliver instead of omit; release this item now
   instead of later; pick that tick), then continue under a policy for decisions after `k`: hold
   them to the recorded outcomes where the recorded choice is still available, fall back to a fixed
   rule (first alternative, as the all-zero driver does) where it is not. Deciding when a later
   recorded decision is "still available" after an earlier alteration is the one genuinely new
   piece of design.

With that, why and why-not are the same loop over the same trace, at whatever granularity the
question needs, and node-level replay through hooked batches is the first instance of it. The
`Spec`'s observer is where the question is asked; the trace is where it is answered.

## 4. The soundness catch: monotonicity

The search's use of a derivation is a constraint. For every clause `D` observed in a held run,
`solve_hypothesis` requires `Σ { x_f : f kills some d ∈ D } ≥ 1` (minus premises): the next
hypothesis must kill at least one delivery of every known derivation. In the terms of section 3:
*to break `POST`, change at least one of the decisions in `S`*, where `S` is the set of decisions a
single-decision counterfactual found necessary. The justification is the sentence from §2 of
`hydrofrac.md`: *any fault set that leaves all of `S` intact cannot break that outcome*. That
sentence is true only if the outcome is **monotone** in the intervened facts: if `S` suffices for
`POST`, then any superset of `S` also makes `POST` hold. A fault set that leaves `S` intact produces
some superset of `S` (plus whatever else survives), and the inference needs that superset to still
satisfy `POST`.

A single-decision counterfactual establishes exactly what it checked: *`S` alone, with every other
intervenable input removed, reproduces `POST`*, and *removing any one element of `S` does not*. It
says nothing about joint interventions elsewhere. Positive Datalog is monotone, so for a positively
stated view the gap closes by itself: more facts cannot un-derive anything. With negation it does
not: a joint change of decisions outside `S` can flip `POST` without touching `S`, and the clause
constraint never proposes it.

Two concrete non-monotone `POST`s over the broadcast facts:

- **"Some correct member has not delivered."** `post :- missing.count() > 0`. In a run where the
  broadcaster crashed mid fan-out, `POST` holds; its counterfactual derivation at the observer is
  `{}` (deleting deliveries only enlarges `missing`). The clause is then either infeasible (false
  certificate) or vacuous (no constraint), and both are wrong: a hypothesis that *adds* a delivery
  relative to the observed run (one fewer omission) breaks this `POST`, and no hitting-set
  constraint expresses "deliver more".
- **"Exactly one member delivered."** `post :- delivering.count() == 1`. In a run where only member
  0 delivered, the counterfactual derivation is `{delivery@0}`: sufficient and 1-minimal. The clause
  says the next hypothesis must kill `delivery@0`. But the hypothesis that kills nothing and lets
  member 1 deliver too breaks `POST`, and the constraint excludes it. The search can certify a
  property that fails in the failure-free run of a slightly different schedule.

A third one *is* a property someone wants, and it produced a false certificate the day the
observer-level counterfactual landed. ISR's durability `POST` is "the **final** leader's log holds
the acknowledged write", with the final leader read off `observe_sequenced(views).max()`. In the
failure-free run every view ZK published names replica 0. The single-fact counterfactual at the
observer deletes views latest-first and keeps `POST`'s value (`(replica 0, leader 0)`) unchanged
each time, because the earlier view it falls back to names the same leader; the 1-minimal
sufficient set is `{the leader's appended write, the initial view}`, whose lineage is empty of
heartbeats. The clause then says "to break `POST`, kill the appended write or the initial view",
and the hypothesis that actually breaks it — lose one replication, crash the leader after it
acknowledged, let ZK promote the stale follower — kills neither: it *adds* a later view. Six
hypotheses, certificate, bug present. `max` over a sequence is not monotone in the facts (a later
fact changes it), and the counterfactual's minimal sufficient set is exactly the wrong answer for
it: it is sufficient only in a world where no later view exists. Record provenance, which folds
every contribution of an aggregate into its value, keeps all the views and through them the
heartbeats that kept replica 0 the leader, and the crash after the acknowledgement is back in the
universe (found in 9 hypotheses, as before). This is why `spec_view` now takes the **union** of the
two derivations: sound where either is, minimal only where both agree.

So the failure mode is not that negation is forbidden, it is that a view which can become *true*
by *removing* facts, or *false* by *adding* them, is outside the inference the search performs —
and a final-value view is the everyday instance. The agreement `POST` written with
`filter_not_in` is not such a view. Adding a delivery can only remove an element from `missing`,
never add one; `missing.count() == 0` is therefore monotone in the deliveries. The negation is over
`correct`, a domain relation the fault model fixes before the run, not a fact a decision can remove,
and the operand that varies with decisions appears on the *negated* side of the difference, where
more facts mean fewer survivors. Negating an intervenable operand and then asking for *emptiness*
(a count of zero, `filter_if_none`) is antitone composed with antitone, which is monotone.

A rule that captures this and is checkable by hand:

> A `POST` view is sound for the hitting-set search if it is monotone in the intervenable facts.
> Positive operators preserve monotonicity. A difference `pos ∖ neg` is monotone in `neg`'s
> intervenable facts only through a subsequent *emptiness* test (or an aggregate antitone in the
> collection, compared downward), and is monotone in `pos` only if `pos`'s facts are not
> intervenable (a domain relation: `correct_members`, a `sim_input`, a constant). `PRE` has the
> dual requirement: the search *preserves* its derivation, which is sound if `PRE` is monotone too
> (a sufficient set for `PRE` stays sufficient when more facts arrive).

Checking it mechanically is a polarity walk over the observer's dataflow. Stratum 0 is every
collection that carries no tags: `correct_members`, other `sim_input`s, constants (`SingletonSource`
is `Tagged::empty`), and anything derived only from those. Every observed stream is tagged and
monotone-positive (polarity `+`). Walk the IR from the `POST` sink upward assigning polarities:
`map`/`filter`/`join`/`cross`/`union`/`unique`/`first`/`count` (as a collection) preserve polarity;
`Difference`/`AntiJoin` give the positive input the parent's polarity and the negative input the
opposite; a comparison fixes direction (`== 0`, `<= k`, `filter_if_none` flip; `>= k`,
`filter_if_some` keep); `zip` passes polarity to both sides. The view is admissible if every tagged
leaf is reached with polarity `+` and every leaf reached with polarity `−` is in stratum 0. The
agreement `POST` passes (`correct` reached with `+` and in stratum 0; `delivering` reached with `−`
then flipped to `+` by `== 0`). "Exactly one" fails (`delivering` is reached with both polarities
through the two sides of the equality). The information needed is all in the IR the `Spec` builds;
the provenance rewrite already walks it with a representation per node. The check is independent of
which oracle computes the derivation: the decision-trace oracle of section 3 answers "which
decisions matter" exactly, and still cannot make the clause constraint mean anything for a
non-monotone view.

## 5. Options

**(a) Derive spec views counterfactually at the observer, always.** This is what the sibling
thread is implementing: `POST` and `PRE` are derived by replaying the observer with observations
deleted one at a time, and each kept observation continues into the observed node's lineage at the
configured level. It fixes the empty-derivation problem for every view that is monotone, whatever
operators it is written with, at a cost of one observer replay per deleted candidate (a few
milliseconds each for the broadcast observer). It does not fix monotonicity: a non-monotone `POST`
still produces a derivation the clause constraint then misuses. It keeps the single-derivation
limitation (section 6). It is the right default now: the alternative is asking authors to encode
every universal as a count comparison, which is what §6.5 of `hydrofrac.md` currently asks.

**(b) A polarity check on the observer's dataflow**, as in section 4, run once when the `Spec` is
compiled, warning (or refusing) when a tagged leaf is reached with negative polarity or with both.
This is the piece that makes (a) safe to offer: without it the warning on an empty derivation is
the only guard, and a non-monotone view with a non-empty derivation (the "exactly one" case) gets
no warning at all. It is a static IR walk of a small graph; the polarity rules above cover every
operator the `Spec` examples use. Cost: a day; risk: operators with no obvious polarity (`fold`
with an arbitrary closure, `reduce`) must be treated as unknown and warned about.

*Built* (`hydro_lang/src/sim/polarity.rs`), as a grade rather than a gate: the search prints each
view's grade before its first run and every certificate under a `Spec` says whether it is within
the fault model (`POST` monotone) or relative to the recorded derivations persisting. The walk
runs on the IR before the provenance rewrite and reads the user's closures from their staged
tokens. It classifies: the leaves (a `Network` receive at the observer is tagged; `sim_input`,
constants and `source_iter` are stratum 0); the sign-preserving operators of section 4;
`Difference` / `AntiJoin` (negative side flipped); a `Filter` / `FilterMap` on a singleton or
optional whose predicate is a comparison of the parameter with an integer literal (`== 0`,
`<= k`, `< k` flip; `!= 0`, `>= k`, `> k` keep; `== k`, `!= k` for `k > 0` both ways);
`Fold` whose accumulator is `*n += 1` (a count, grows with its input); `Reduce` downstream of
`enumerate` (a final value: non-monotone). It cannot classify, and grades unknown with the
reason: a predicate on a `zip` of two aggregates (`delivering == correct`), a `fold` with any
other accumulator, a `reduce` not over a sequence (which element wins is not syntactic), a
`scan` (so `first()` and `last()`, which are generators), a `flat_map` on an aggregate's value.
Both-ways signs that arise only through an unclassified operator are reported as unknown, not as
non-monotone; `NonMonotone` is reserved for a tagged leaf reached negatively or both ways along a
path the walk understood, or for a final value. On the harnesses: the agreement `POST` written
with `filter_not_in` grades monotone, exactly as the rule above predicts, and takes the
counterfactual derivation alone; the positive agreements grade unknown; ISR's final view grades
non-monotone. The "exactly one" and "some correct member has not delivered" views of section 4
grade non-monotone (`== 1` both ways; `> 0` after a difference whose positive side is the tagged
one). The control in the broadcast harness is integrity, "no member delivered `m` twice", which
is true, non-monotone, and certified after one run with an empty derivation: the grade is the
only thing that distinguishes that certificate from an earned one.

**(c) Why-not provenance by rewriting.** Reconstructing, for a fact that holds through absence, the
derivations of the absent facts that would have had to exist, Molly-style. This needs a bounded
enumeration of what *could* have been derived, which in Molly came from the logical-time horizon.
Not recommended: (d) gives the same answer by asking the oracle instead of rewriting the program.

**(d) Decision-trace time travel as the general oracle.** Record every nondeterministic decision
with its outcome (section 3, items 1–3), replay a prefix exactly, alter one decision, continue.
Why and why-not become the same query at any granularity: "would member 2 have delivered if the
filter had delivered `initial[→2]`" is one altered fate; "would it have delivered if its tick had
run before the broadcaster's crash took effect" is one altered tick choice. It subsumes (a), which is
the instance where the only decisions intervened on are "which recorded releases this node's hooked
batches made", and it removes the dependence of Level 3 on hooks (an unhooked batch's release is a
recorded decision like any other). It also gives the search a tool it does not have today: when a
hypothesis holds, the trace says which schedule decisions the outcome rested on, which is the
information a schedule-sensitive bug (Raft, `hydrofrac.md` §5.5) needs. Cost: the recorder is a
scheduler change (two draw sites plus the hooks' `autonomous_decision`s), the replayer is a bolero
`Driver` backed by the trace, and the continuation policy after an override is new design. It does
not change section 4.

Recommendation: (a) and (b) are built, with the empty-derivation warning kept as a backstop; (d)
is the next substrate step, with (a) as its first instance. Document the rule of
section 4 in `spec.rs` in place of "write `POST` positively": the requirement is monotonicity in the
intervenable facts, not the absence of negation, and the agreement `POST` may be written the natural
way.

## 6. One derivation per record, and what replay does not change

`hydrofrac.md` §6.5 notes that deduplicating operators (`unique`, `first`) keep the first
provenance seen for a record, so a view's alternative supports surface one hypothesis at a time
(reliable broadcast under one crash: 10 hypotheses, each member's relevant crash point refuted by
its own run). Counterfactual lineage at the observer does not change this: greedy deletion returns *one*
1-minimal sufficient set, the one its deletion order (latest first) leaves standing; the derivation
through an echo rather than the broadcaster's send is not enumerated. Several derivations per fact
would need repeated deletion under different orders or a ddmin-style enumeration of minimal
sufficient sets, exponential in general. The search is complete without it (every alternative does
surface, one run later), so this is a cost question, not a soundness one. The decision-trace oracle
of section 3 does not change it either; it changes what can be intervened on, not how many
sufficient sets are reported.
