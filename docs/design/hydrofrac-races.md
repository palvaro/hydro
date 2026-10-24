# hydrofrac-races: certificates for programs with `nondet!`

Status: design note, stage 1 (before implementation). Branch `hydrofrac-races`, from `hydrofrac-lite-lean`
@ `58d0b3bcbf`.

## 0. Summary

- **The question.** Which schedule choices can change whether a correct member ends up holding a fact, under a
  given fault set?
- **The answer.** A choice can matter in exactly two ways:
  1. the fact's derivation passes through a *fragile step*: an operator inside a tick whose output exists or not
     depending on how its inputs were split into ticks or ordered;
  2. the fault set crashes a node, and the order of that node's sends decides which of them the crash cuts.
- **Why (1) is visible in lineage.** Hydro allows negation and "is it here yet?" tests only inside a tick, and
  the record they produce is computed in that same tick. So a fragile step always lies on the record's own
  *positive* lineage, even when what it tests is the *absence* of something.
- **What we build (v1).**
  - Records carry tags for the fragile steps they passed through.
  - A derivation with no such tag is *robust*: it holds in every schedule.
  - Crashes are judged by a rule that does not depend on send order.
  - Only robust derivations are used to skip fault sets. Every fault set the search does run is explored over
    its schedules until robust derivations cover it, or until its schedules run out.
- **What it buys.**
  - Programs whose only `nondet!` choices feed set-preserving operators get real certificates, cheaply.
  - Programs whose facts are derived through fragile steps (2PC, quorums, gossip snapshots) still get sound
    answers, but v1 can only enumerate their fault sets. That costs about what brute force costs.
- **v2 (proposed, §8).** Turn the commonest fragile step, "trigger meets stored state", into an ordering premise
  ("x arrived before y at node n"). A premise is discharged by a quiescence barrier in the harness, or flipped by
  a directed schedule. That is where savings on 2PC, gossip and quorums would come from.

## 1. Terms

- **Schedule.** Every choice the simulator makes in one run:
  - which ready tick runs next;
  - what each `batch`/`snapshot` hook releases into that tick;
  - the order an `observe_nondet`/`merge_ordered` hook picks.

  Between choices, top-level (non-tick) dataflow runs deterministically to a fixpoint (`sim/compiled.rs`
  `LaunchedSim::step`).
- **Fault set**, or hypothesis. A set of omissions (`omit(m)`: every copy of message `m` on a lossy channel is
  lost) and crashes, at most one per node. `crash(n after m)` means `n` dies right after it sends `m`.
  `crash(n)` means it dies before its first send. Messages are named by content: channel, sender, destination
  and bytes (`fault::MessageId`). So a fault set means the same thing in every schedule.
- **Pair.** A (member, fact) of the checked label.
- **Derivation of a pair.** The deliveries the member's record of the fact was computed from, closed backwards
  through the records those deliveries carried (record provenance, `lineage`).
- **Fragile step.** An operator whose output records can exist or not depending on how the simulator batched or
  ordered its inputs (§3.1 lists them). A batch itself is *not* fragile: it only decides *when* an element enters
  a tick.
- **Robust derivation.** A derivation in whose backward closure no record passed a fragile step.
- **Domain.** The facts some correct member holds in some failure-free schedule.

## 2. What a certificate claims

For a label, a crash bound `k`, and the harness as written (inputs fixed, and the `quiesce()` barriers it
contains), a certificate says:

- **Claim A (domain agreement).** For every fault set with at most `k` crashes, every schedule the simulator can
  produce for it, and every fact of the domain: at the end of the run, either every correct member holds the
  fact, or none does.
- **Claim B (no new disagreement).** The same holds for facts outside the domain. Faults can create new facts only
  through a fragile step, e.g. a timeout that fires only when nothing has arrived. v1 certifies Claim B in two
  cases:
  - the program has no fragile step at all;
  - the search ran every schedule of every admissible fault set for that label, so nothing was skipped.

  Otherwise Claim B is checked only in the runs that were explored, and the report says so. This is a real limit
  of lineage. LDFI kills derivations that exist; it cannot rule out derivations that exist only in runs it never
  made. Molly's goal-based specifications had the same limit. Lite does not hit it because a choice-free Hydro
  program is monotone, so faults only remove facts.

The certificate covers what the harness lets happen, nothing more. If the harness quiesces between two inputs,
the race between them is not covered. The report prints the harness's barrier count.

## 3. Where schedules can matter

### 3.1 Fragile steps

The simulator branches only at hooks. Top-level dataflow is deterministic and, in Hydro, monotone. A
non-monotone operator needs a `Bounded` input, which only a tick provides. So a record's existence can depend on
the schedule only through operators that run inside a tick on what the tick happened to receive, or that depend
on an order the schedule picked.

**Fragile:**

| step | why the output can vanish in another schedule |
|---|---|
| `anti_join`, `filter_not_in`, `difference` (negation) | the negative side may hold more or less at that tick |
| `cross_singleton`, `filter_if`, `zip` with a snapshot or with this tick's batch | "trigger meets state": the state may not yet hold the element when the trigger is released (`ask_once`) |
| `join`/`cross_product` inside a tick | the two elements may be released in different ticks |
| `fold`/`reduce`/`count`/`scan` inside a tick, and every snapshot of a top-level fold | the value seen depends on how much has been folded so far |
| `enumerate`, `first`, ordered `fold`/`scan` on a `TotalOrder` stream whose order a choice can change | the order is the schedule's |
| closures that read state by reference | the state's contents at that moment |
| timers and other non-iterator sources | how many records exist |

**Not fragile:**
- `map`, `filter`, `flat_map`, `filter_map`, `inspect`, `chain`, casts, order-only `observe_nondet`;
- `batch` itself, `all_ticks`, `defer_tick`;
- network send and receive;
- `unique`;
- the dedup idiom `x.filter_not_in(own past outputs)`.

The set of records these produce over a whole run is a function of the set of records they receive, however
those were split into ticks. This list is the "tolerant" list the research branch's CALM analysis argued for
(`origin/hydrofrac-sensitivity`, `sensitivity.rs` `walk_calm`). Here it is applied per record, at run time, not
per label over a static cone.

### 3.2 Crashes and send order

The simulator can change the order in which a node sends (it changes the order in which the node's inputs
arrive). `crash(n after m)` cuts what `n` sends after `m`, so which sends that is depends on the schedule. Lite
decides "does this crash kill this derivation?" from the send order of the run the derivation was seen in
(`search/clause.rs` `Dependency::killed_by`). That is valid only if every run has that order.

### 3.3 The user's hypothesis, checked

> *"Choice sites absent from every derivation of the label's facts can be fixed arbitrarily."*

**Not sound as stated, and also weaker than it needs to be.**

- **Not sound.** In 2PC with cooperative termination, a participant commits because no Abort was in its view
  when it decided. Whether that Abort exists is decided by a timer race at *another* participant. That choice
  site appears in no derivation of `committed`, yet it decides it. The negation step at the deciding
  participant *is* in the derivation, though. So the right test is "does the derivation pass a fragile step?",
  not "which choice sites does it name?".
- **Weaker than needed.** A choice site that does appear in a derivation is harmless when only set-preserving
  operators follow it. For example, `batch → map → send` passes a choice and is still robust.

> *"What about choices that create or destroy a derivation without appearing in its positive lineage
> (`ask_once`)?"*

In Hydro they cannot, by the argument above.
- In `ask_once`, member 1 builds its answer with `known_now.filter_if(queried)` (`ec_bruteforce.rs:183`). That
  step is fragile (trigger meets state) and is on the answer's own lineage.
- The case without a positive witness is the empty answer: no record exists at all. But then member 2 holds no
  derivation of the fact, so the search does not count it as covered (§4.3), and it must explore.

## 4. Mechanism (v1)

### 4.1 Fragile tags in provenance

- **Tagging.** The provenance rewrite (`provenance/rewrite.rs`) already wraps every record as `Tagged<T>` with the
  set of deliveries it came from. After each fragile step (§3.1) it adds one more tag to every output record:
  `site(i)`, a reserved id naming that step's IR node.
- **Propagation.** Tags flow like delivery ids: unions at joins, carried across sends in the send's metadata, so
  the lineage closure collects them.
- **Classification.** `lineage::derivation` returns the deliveries plus the fragile sites. A derivation is
  robust iff that set of sites is empty.
- **Static summary.** The program-wide list of fragile steps is computed when the rewrite runs. "None" makes the
  label class *fragile-free*.

### 4.2 Crash rule that does not depend on send order

`crash(n after m)` kills a derivation iff the derivation uses a send of `n` **other than `m`**. `crash(n)` kills it
iff it uses any send of `n`.

- **Sound in every schedule.** If the derivation's only send from `n` is `m`, then in every schedule where `m` is
  sent, it is sent before the crash, since the crash comes right after it. If `m` is never sent, the crash never
  fires.
- **Conservative.** It ignores orders that every schedule happens to share, e.g. two sends from one `flat_map`.
  So it may propose a few more hypotheses than lite on choice-free programs. That is measured in §7.
- **Completion carries over.** Lite's argument (§4.5 of the lite doc) needs one property: `crash(n)` kills
  everything any other crash of `n` kills. That still holds.
- **Firing is unchanged.** It is lite's `FaultHook`: the crash fires when `m` is sent before the heal.

**Refinement: closed-broadcast fan-out order.** `crash(n after m)` also spares a send `d` of `n`
that is always sent before `m`. One case is known for sure: `broadcast_closed`.
- **The shape.** It lowers to `cross_product(data, ClusterIds)` → map → `demux`. The cross
  product is a `join_multiset_half` with key `()`. Its build side is the cluster's member ids,
  which the simulator lists as `0..n`, all pushed by one `source_iter`. It is read in full
  before the probe side. Each probe record `v` comes out as `(v → 0), …, (v → n−1)`, in that
  order. The maps and the cast before the network are one to one, and the network sends its
  items in order.
- **The claim.** On such a channel, if `n` sends `m = (v → k)`, it has already sent `(v → j)`
  for every `j < k`: in every run, under every fault set. So `crash(n after (v → k))` spares
  them.
- **Recognised strictly.** `provenance::fanout::mark` matches the exact IR, closures by their
  text in `hydro_lang`'s source, and the id source by the destination cluster's key. It marks
  the `Network` node, and the engine reads the mark from `CompiledSim::channels`. A
  hand-written fan-out, or anything else in between, is not matched, and keeps the
  conservative rule.
- **Checked.** `check/fanout_tests.rs` runs a process broadcast and a batched cluster broadcast
  under every schedule (1,332 of them for one test). On each, it asserts that every payload
  goes to members in increasing id order.
- **Completion.** Each `(channel, sender, payload)` is one totally ordered chain, and chains are
  disjoint.
  - A crash after a message `x` that no clause names has an image: the crash after the last
    named message that precedes `x` in its chain. Both spare the same named sends, so both
    kill the same named deliveries.
  - If no named message precedes `x`, the image is `crash(n)`, as before.
  - The stand-in for `crash(n)` used with exemptions must be a crash after an unnamed message
    that no named message precedes.
- **Exact crash points.** Suppose the program is fragile-free, so the probe has seen every
  message any node can send, and every message `n` can send lies in `m`'s chain. Then
  `crash(n after m)` cuts exactly the chain after `m`, in every run.
  - Such a set's facts are the same in every schedule. It is run on one schedule.
  - If `n` is not a member of the label's cluster, a fact the set loses everywhere is dead
    (`Sweep::mark_dead`). The argument is the start-crash one: a derivation under a superset
    sends from `n` only messages up to `m`, which the rule spares.
  - For completion, every crash point of such an `n` is a candidate. Mapping a crash after an
    unnamed message onto the dead set's crash point would excuse a pair the original crash
    does not.

### 4.3 Clauses per pair

Lite learns one clause per schedule: the union of the derivations of every pair required in that run. A
hypothesis must hit every such clause.
- That is sound only if every domain fact was held in that run.
- A run in which some fact vanished at every member contributes a clause without that fact's deliveries. A later
  hypothesis that splits the members on that fact could be skipped. This is suspicion S1, being tested now (§9).

v1 keeps clauses per pair, as Molly kept them per goal:

- **Admissible.** A hypothesis `H` is admissible for pair `p` iff it kills every robust derivation of `p` seen so
  far (in any run, under any fault set).
- **Skipped.** `H` is skipped (pruned) iff it is admissible for no pair. By Lemma R (§5), every correct member
  then holds every domain fact in every schedule.
- **ILP change.** One indicator per pair, at least one indicator set, and the pair's cover rows conditional on
  its indicator. The rest of lite's model (bounds, vacuity, no-goods, tie-breaks) is unchanged.

### 4.4 The loop

1. **Probe** (no faults).
   - If the program is fragile-free: one schedule. All its facts are robust, hence the same in every schedule.
   - Otherwise: every schedule, which yields the domain. A sampled probe gives no certificate.
2. **Propose.**
   - First, as a counterexample heuristic, the smallest admissible `H` that also kills every *fragile* derivation
     seen for the chosen pair. This is Molly's move: it aims where a counterexample is likeliest.
   - Then any admissible `H`.
3. **Explore `H`.** Run its schedules one at a time:
   - stop with a counterexample if a schedule breaks Claim A or B;
   - stop early once the derivations learned make `H` admissible for no pair;
   - otherwise run every schedule.

   `H` is then a no-good. Early stopping is where most of the saving is. A hypothesis whose first schedule shows
   every pair robustly re-derived and spared costs one schedule, not thousands.
4. **Conclude.**
   - When no admissible `H` is left over the completed universe (lite's completion, with §4.2's rule), the label
     is certified up to the bound, and the crash sweep goes on as in lite.
   - **Universe for pairs with no robust derivation.** Such a pair makes *every* fault set admissible, so the
     candidate faults its robust clauses would propose are not enough. For these pairs the universe is the
     brute-force enumerator's (`check/brute_force.rs` `harvest`): every lossy message and every crash point seen
     in any run, grown to a fixpoint. The ILP then proposes every fault set over it that is not skipped. So
     these labels cost about what brute force costs, minus the skipped sets.

### 4.5 Which choices are explored, per spec

The checker never has to ask which choice *sites* matter. It asks, per hypothesis and per pair, whether a
derivation that is robust and spared by the crash rule covers the pair.
- **Covered:** no schedule can take the fact away, so the hypothesis' other schedules are not run.
- **Not covered:** some choice may matter, so the schedules are run.

The question becomes one of lineage, which is what the user asked for. The price is that inside an explored
hypothesis every choice is still varied (§8.3 explains why fixing the "irrelevant" ones is not yet safe).

## 5. Why it is sound

**Lemma R.** Let `D` be a robust derivation of pair (m, f), seen in any run. Let `H` be any fault set that kills
no delivery of `D`: no omission of a lossy message of `D`, and no crash that kills `D` under §4.2. Then in every
schedule of `H`, `m` holds `f` at the end of the run, or `m` has crashed.

*Argument* (by induction along `D`, from its sources to `m`):
- **Sources.** Each source record is a harness input, which every schedule delivers.
- **Steps.** Every step on `D` is set-preserving (§3.1). So once its input records exist in a run, its output
  record exists by the end of the run, whatever the batching. At quiescence every batch and snapshot hook has
  released everything pending (`SimTick::can_run`; quiescence means no tick can run). Records are compared by
  content, and set-preserving steps are deterministic functions of their inputs.
- **Sends.** Each send `s` of `D` by a node `n`:
  - is not omitted (`H` has no omission of it);
  - is not cut by a crash, since by §4.2 the only send of `n` in `D` is the crash message itself, or `n` is not
    crashed.
- **The end.** If `m` is still up, it receives every delivery of `D` and derives `f`. Taps emit the first time a
  record exists, and a member's view only grows.

*Corollary (pruning).* If `H` is admissible for no pair, then in every schedule of `H` every pair is held or its
member crashed. So Claim A holds for `H`, without running it.

*Theorem.* A certificate at bound `k` implies Claim A for every fault set with at most `k` crashes.
- Every such fault set is one of three kinds:
  - skipped (corollary);
  - explored and stopped early (its later schedules fall under the corollary);
  - explored in full.
- Completion (§4.4, step 4) shows that "no admissible set over the completed universe" covers every fault set,
  not only the candidates.
- Claim B: in a fragile-free program every derivation is robust, so faults only remove facts (by Lemma R, facts
  under `H` are a subset of the domain). Otherwise Claim B holds only for the fault sets explored in full.

**Where it is not proved.**
1. **The fragile-step table (§3.1) is trusted.** It is a table of operator semantics, with the same status as the
   research branch's CALM list. Brute force is the check, and every certificate in §7 is cross-checked. User
   closures are assumed pure, as in lite.
2. **The harness is assumed deterministic:** it sends fixed inputs and does not branch on outputs.
3. **Sampled schedules are never certificates.** A label that needs sampling (CTP-3) gets `Exhausted` at best.
4. **The probe must be exhaustive** unless the program is fragile-free.

## 6. On paper: the known false certificates

### 6.1 `ask_once`

The program has three members. Member 0 gets 7 and announces it to members 1 and 2 (`fail_stop`). Member 2's
timer fires at the same time; if 2 knows nothing, it asks member 1, which answers with what it knows *in that
tick*.

- **Probe.** The program has fragile steps (`filter_if` at `:176` and `:184`), so every failure-free schedule
  runs.
  - (1, 7) and (2, 7) have robust derivations: the direct announcements.
  - (2, 7) sometimes also has a fragile one: via the answer.
- **Proposal.** To break (2, 7), kill its robust derivation `{0→2}`: `crash(member 0 after announce→1)`.
  - Under §4.2 this spares (1, 7), whose only send of member 0 is the crash message.
  - It kills (2, 7).
- **Explore that set.** In the schedules where 2's query reaches member 1 before the announcement does, member 1
  answers with nothing, so member 1 holds 7 and member 2 does not. That is a counterexample: the same set, and
  the same race, that `brute_force_finds_the_ask_once_race` finds.
- **Lite's false certificate is not possible here.** Lite skipped this set because some schedule's derivation of
  (2, 7) avoided `0→2` (it went through the answer). That derivation is fragile, so v1 does not use it to skip.

### 6.2 CTP with three participants, label `committed`

- **Every derivation is fragile.** A participant commits through an in-tick `JoinHalf` (`two_pc_ctp.rs:156`, fed
  by the batch at `:105`), so every derivation of `committed` carries a fragile tag.
- **So nothing is skipped.** Every fault set within the bound is explored. The breaking set
  `{crash(coordinator after commit→P0), crash(P0 after Decided→P1)}` has two crashes, so it is reached after
  bounds 0 and 1 are exhausted.
- **Cost.** One crash already gives more than 66,000 schedules, so the search must sample. The verdict is
  therefore a counterexample (found in about 40% of sampled schedules, as brute force finds it), or `Exhausted`
  if sampling misses it. Never a certificate.
- **Lite's pruning is not possible here.** Lite skipped this set using a derivation from schedules in which
  P0's `Decided` reached P1 first. That derivation is fragile.

### 6.3 CTP with two participants and racing timers

- `committed` is fragile in the same way.
- `crash(coordinator after commit→P0)` is one crash, proposed at bound 1.
- It splits the participants in about 80% of schedules (`ctp2_racing_timers_split_by_direct_injection`). With
  sampling the search finds it, as `ctp2_racing_timers_search_finds_the_crash` does today.

## 7. Expected cost (to be measured)

- **Fragile-free programs.**
  - **What it costs.** One schedule for the probe and about one per hypothesis, the same as lite. A hypothesis
    needs more only when a crash rule (§4.2) leaves a pair uncovered, i.e. a derivation that uses two sends of the
    crashed node.
  - **What's new.** These programs have `nondet!` choices, so lite gives them no certificate. Here they get one.
- **Labels whose pairs all have robust derivations, in programs with fragile steps elsewhere.**
  - The probe runs every schedule; hypotheses usually take one.
  - Claim A is certified; Claim B only if nothing was skipped.
- **Labels with fragile-only pairs** (2PC `committed`, quorum outputs, gossip state).
  - v1 enumerates their fault sets, every schedule each: brute-force cost.
  - It is sound, but saves nothing. This is what v2 is for.

**Per-target prediction.** This comes from reading the code (`races-target-steps.md`); nothing here is measured
yet. "Semantically fragile in its harness" means a fact's presence at a correct member can differ between two
schedules with the same faults. "v1 sees" is what the step table of §3.1 says.

| target | semantically fragile in its harness? | what v1 sees | v1 outcome |
|---|---|---|---|
| `hydro_test` broadcast, plain | no | dedup only, so robust | certified, about lite's cost |
| `hydro_test` broadcast, echo | no | robust | certified, but the probe alone is >160k schedules unless the program counts as fragile-free (it does: dedup only), in which case 1 schedule |
| URB closed | no (threshold over cumulative state) | a closure over `by_mut` state, so fragile | enumeration; the probe alone is >240k schedules, too big |
| G-Set gossip | no (barriers between rounds) | snapshot × pump, so fragile | enumeration, sampled: no certificate |
| multi-Paxos, ABD | no, but only because the harness has one ballot / one writer | closures over state, a max over the threshold batch | enumeration, too big |
| CTP-2, quiescing harness | no (the barrier orders decision before query) | trigger × state, so fragile | enumeration, sampled |
| CTP-3, CTP-2 racing, `ask_once` | **yes** | fragile | counterexample (as brute force) |

The lesson: in this corpus every semantically fragile step is **a one-shot trigger (a query, a timeout) meeting
stored state**. Everything v1 wrongly calls fragile is either a threshold over cumulative state hidden inside a
closure, or a trigger × state whose race a barrier removes. v2 (§8.1) addresses the second. The first needs a rule
for closures over state, which syntax alone cannot give soundly (§8.2).

## 8. v2 (proposed): ordering premises, barriers, directed schedules

### 8.1 Trigger meets state

The commonest fragile step joins this tick's batch (the *trigger*: a query, a timeout, a gossip pump) with
persisted state or a snapshot. Its output record exists iff the state already held the element when the trigger
was released.

- **The premise.** Record that as an ordering premise on the derivation: `x ≺_n y` ("delivery `x` reached node
  `n`'s state before trigger `y` was released"). A derivation then holds in every schedule that keeps its
  premises.
- **Discharged by a barrier.** Suppose `x`'s own derivation is robust, uses only harness inputs sent before some
  `quiesce()`, and `y` comes from an input sent after it. Then every schedule keeps the premise: at quiescence
  every hook has released everything, and a snapshot then shows the latest version (`SingletonHook`: pending
  versions keep the tick runnable). The derivation is then as good as robust.
  - This is the precise form of "harness discipline": it says which races a barrier removes.
- **Flipped by a directed schedule.** An undischarged premise is the adversary's second kind of move, besides a
  fault: release `y` at `n` before `x`. The hypothesis becomes a fault set plus premise flips. The ILP kills a
  derivation by hitting a delivery *or* flipping a premise.
  - The simulator can realize a flip: every batch hook may release less than it has, so a hook can hold back
    `x` until `y` has been released.
  - That needs one extension point: a release filter consulted by the hooks, behind the `hydrofrac` feature.
  - If the held-back run never releases `y`, then `y` depends on `x` and the flip is impossible.
- **What v2 would buy (to verify with `races-target-steps.md`):**
  - `ask_once`'s answer and CTP's peer queries are this shape;
  - the gossip snapshot at the pump is this shape;
  - with the quiescing harnesses, their premises are discharged, so pairs become prunable.

  The quorum step (`collect_quorum`: count over retained responses, then a threshold) is a different shape. It
  needs its own rule ("state retained until the record it supports is emitted"), which is not designed yet.

### 8.2 Thresholds over state hidden in closures (open)

The uniform broadcast, multi-Paxos and ABD mints all have the same form: one closure over `by_mut` state that adds
each response to a pending set and emits once the set reaches a threshold.

- **Why they are robust.** The pending set is cumulative and the threshold test is monotone. Batching therefore
  changes only *when* the record is emitted.
- **Why provenance cannot tell.** The rewrite sees only "a closure that reads state". Whether that closure is
  monotone in its state is a property of user code. The Paxos acceptor shows the danger: its closure over state
  refuses accepts below its promise, which is not monotone.

There are three ways out, none of them in v1:
1. **Trust a small vetted combinator.** A library threshold (`quorum` / `collect_quorum`) declared monotone, like
   `unique` is today. The certificate would then name every combinator it trusts.
2. **Ask programs to express the mint with structural operators** whose semantics the table covers.
3. **Pay brute-force cost** for these labels.

I recommend (1). It moves the trust from arbitrary closures to one audited function.

### 8.3 Fixing irrelevant choices inside an explored hypothesis (not proposed for v1)

- **The argument.** If every step after a choice is set-preserving, the choice only shifts *when* records arrive
  downstream. Downstream hooks can always hold records back, so "release everything at once" upstream reaches
  every view downstream can have.
- **The problem: crashes.** The same choice changes a node's send order. With upstream fixed, crash points
  between sends that some other schedule would have ordered differently are never tried. Under §4.2's rule the
  search does not *need* those orders to skip a set. But it needs them to run one honestly. Fixing choices would
  therefore require crashes that cut an arbitrary set of a node's sends rather than a suffix, and checking that
  each such cut is a real run. Deferred.

## 9. Side findings (being checked)

- **S1.** Lite's per-run union clause, after a run that loses a fact at every member (§4.3). A worker is building
  a choice-free program on which lite and brute force would disagree.
- **S2.** Lite's positional kill relation, when a different fault set changes a node's send order in a
  choice-free program (§3.2). Same worker.

If either is real, it is a bug in lite's certified class, independent of `nondet!`, and §4.2 / §4.3 fix it.

## 10. Plan

1. **Prototype, as a separate module `hydrofrac::races`, not in lite's path.**
   - Fragile tags: rewrite and lineage.
   - Per-pair clauses with §4.2's rule.
   - The exploration policy.
   - An entry `RaceCheck` with lite's harness shape.
   - Brute force is reused unchanged.
2. **Validate:**
   - `ask_once`, CTP-3 (sampled) and CTP-2 racing (sampled) give their counterexamples;
   - certificates on small configurations, each cross-checked against brute force;
   - planted bugs;
   - cost against brute force.
3. **v2 (§8.1)** only if v1 lands and the root agrees.

## 11. Results (stage 3, measured)

Branch `hydrofrac-races` @ `fbff7b5955`. "Races" is `EcCheck::races()`. "Brute force" is lite's enumerator:
every fault set of at most 2 faults, each run over every schedule. "Cost" is fault sets run / schedules run. Every
row where brute force ran agrees with it (`assert_agrees`). There is no disagreement anywhere.

**How to read the certificates.** In a 2-member cluster the vacuity rule (never crash all, or all but one, of the
checked cluster) admits no member crash. A certificate there covers only crashes of the one node outside the
cluster: the source, the broadcaster or the coordinator.

| program, configuration | label | verdict (plain English) | proven? | races cost | lite | brute force | test |
|---|---|---|---|---|---|---|---|
| `ask_once`, 3 members, value ∥ timer | `known` | a single crash of member 0, right after its announcement to member 1, makes member 2 miss 7 in the schedules where 2's query reaches member 1 first | counterexample | 4 / 74 | finds it (heuristic) | 8 / 132 | `races_agree_on_ask_once` |
| 2PC cooperative termination, 3 participants, 64 sampled schedules per set | `committed` | two crashes (coordinator after commit→P0, P0 after Decided→P1) split the participants | counterexample; sampled, so no certificate | 133 / 8,451 | — | 200 / 12,800 | `races_agree_on_two_pc_cooperative_termination` |
| 2PC-CTP, 2 participants, racing timers, 64 sampled | `committed` | a single crash of the coordinator right after its commit to P0 makes P1 never commit | counterexample; sampled | 5 / 258 | 2 / 128 | 6 / 384 | `races_targets_ctp2_racing_sampled_races` |
| 2PC-CTP, 2 participants, quiescing harness, exhaustive | `committed` | every admissible fault set (coordinator crashes only), every schedule | **certified** | 6 / 2,460 (= brute force) | `Exhausted`, 5 / 2,428 | 6 / 2,460 | `races_targets_ctp2_quiescing_races` |
| uniform reliable broadcast, 2 members, 1 send, threshold 1 and 2 | output, `certified` | every admissible set (source crashes only) | **certified** | 4 / 169 (= brute force) | `Exhausted`, 3 / 113 | 4 / 169 | `races_targets_urb2_threshold{1,2}_races` |
| same | `echo`, `distinct_acks` | same | **certified** | 3 / 58 | `Exhausted` | 4 / 169 | same |
| `hydro_test` echo reliable broadcast, 2 members, 1 payload (204 failure-free schedules) | `deliveries`, `echo`, `echoes` | same | **certified** | **2 / 2** | `Exhausted`, 3 / 215 | 4 / 419 | `both_agree_on_an_echo_broadcast_over_two_members` |
| same, **3 members** (over 160,000 failure-free schedules) | `deliveries` | every admissible set: the broadcaster and one member may crash, anywhere | **certified**, not cross-checked (brute force cannot run) | **6 / 6**, 0.1 s | `Exhausted` | infeasible | `races_alone_on_an_echo_broadcast_over_three_members` |
| `reliable_broadcast_closed` (1 failure-free schedule: its batch never branches) | `new_messages`, 1 / 2 / 3 payloads | every admissible set | **certified** | 8 / 8, 134 / 134, 281 / 281 | certified, 25 / 25 and 46 / 46 for 2 and 3 payloads | 65 / 65, 176 / 176, 341 / 341 | `races_agree_on_reliable_broadcast_closed`, `both_agree_on_reliable_broadcast_closed_with_*` |
| `reliable_broadcast` plain, 1 / 2 / 3 payloads | `deliveries` | a single crash of the broadcaster partway through its broadcast leaves some members without the payload | counterexample | 2 / 2, 4 / 225, 4 / 9,921 | 2 / 282, 2 / 12,524 for 2 and 3 payloads | 20 / 38, 32 / 668, 44 / 24,484 | `races_agree_on_a_plain_broadcast`, `both_agree_on_a_plain_broadcast_with_*` |
| planted bug: members do not re-forward | `delivered` | a single crash of the source partway through its broadcast | counterexample | 2 / 2 | — | agrees | `hydrofrac` `brute_tests` |
| the S1 program (no `nondet!`) | `known` | omitting member 0's forward to member 1 makes member 1 miss f | counterexample (lite falsely certifies) | 3 / 3 | false certificate | agrees with races | `suspected_total_loss_*`, races variant |

**Not cross-checkable:** brute force cannot even finish the failure-free run on these.
- uniform broadcast, 3 members: over 215,000 schedules;
- G-Set gossip, 3 members, 1 round: over 44,000;
- multi-Paxos, 1 proposer, 3 acceptors: over 21,000;
- ABD, 1 write: over 131,000.

In those programs v1 also falls back to enumeration, because their derivations pass through fragile steps. So v1
cannot certify them at any size worth having.

### What this shows

- **Soundness.** No disagreement with brute force in any of the configurations above. The known false certificates
  are gone: `ask_once`, CTP-3, and lite's own S1.
- **Where races wins.** Where a label's derivations are robust, a fault set costs one schedule instead of all of
  them. Examples:
  - the echo broadcast: 2 schedules where brute force needs 419, and 6 schedules for 3 members, where brute force
    is impossible;
  - the plain broadcast's counterexample: 2 schedules where brute force needs 38.
- **Where it does not.** Labels whose derivations are fragile cost exactly brute force: 2PC `committed`, the
  uniform broadcast's quorum output. That is sound, but no saving.
- **A regression against lite.** On `reliable_broadcast_closed` with several payloads, lite costs 25 / 46 fault
  sets and races costs 134 / 281.
  - The program has no `nondet!` branching at all, so lite's positional rule applies there.
  - Races' crash rule knows only the order inside one broadcast, not the order between two broadcasts.
  - The proposed fix is to recognise that a source's `TotalOrder` stream broadcasts payload 1 before payload 2.
    It is not done yet.

### Next

1. Order between successive broadcasts of a `TotalOrder` stream (fixes the regression).
2. v2 (§8.1): "trigger meets state" premises with barrier discharge. This is what would let G-Set gossip and CTP-2
   with the quiescing harness be certified without enumerating.
3. §8.2: a trusted quorum combinator, for uniform broadcast, multi-Paxos and ABD.

## 12. Order in the formula (design for review, before implementation)

The user's direction: put order into the hitting-set formula instead of falling back to an enumeration. A
certificate is then "the formula has no solution". This section gives the encoding, the soundness argument, what
it deletes, and the one place where I think the direction needs a correction.

### 12.1 Encoding

- **Ordering literals.** A literal is a yes/no statement about a run, with a definite truth value in every run.
  - **Crash literals:** `after(d, m)` = "node n sent `d` after `m`" (two sends of one node).
  - **Premise literals of fragile steps** (§3.1):
    - for "trigger meets stored state": `before_n(x, y)` = "delivery `x` was in node n's state when trigger `y`
      was released";
    - for a join of two batches: `same_tick_n(a, b)`;
    - for a negation: `clear_n(k, y)` = "no record with key `k` was on the negative side when `y` was released";
    - for a fold or count read mid-way: `before_n(x, y)` for each folded input.
- **Clauses.** A derivation `D` of pair p becomes the row
  `Σ x_f (faults f that kill a delivery of D) + Σ (1 − ℓ) (premise literals ℓ of D) ≥ z_p`.
  - A crash `crash(n after m)` kills a delivery `d` sent by n only together with `after(d, m)`: the row gets the
    product `x_crash · after(d, m)`, linearised as usual.
  - Robust derivations have no premise literals, so their rows are unchanged.
- **Free and fixed literals.** A literal is a free variable of the adversary unless it is **proved constant**,
  i.e. it has the same value in every run of every fault set. Two proofs exist today:
  - **fan-out order:** `after(d, m)` is false when `d` precedes `m` in one `broadcast_closed` fan-out (§4.2,
    verified at the IR level);
  - **send-free nodes:** a node that never sent `m` never fires `crash(n after m)`.

  Barrier order (v2, §8.1) would be a third proof: `before_n(x, y)` is true when `x`'s derivation is robust and
  finishes before a `quiesce()` that precedes `y`'s harness input.
- **The solver** proposes a fault set plus a value for every free literal. The engine runs the fault set.
- **Certificate.** The formula is infeasible over the completed universe, with every tried fault set excluded by a
  no-good. There is no size bound beyond the crash bound.

### 12.2 Why it is sound

Take any fault set H that was neither tried nor ruled out, and any schedule σ of H. Let L(σ) be the truth values
the literals actually take in that run. Then (H, L(σ)) satisfies every row it hits, so it is not a solution: some
pair p has a row it misses.
- That row's deliveries survive H in σ.
- Its premise literals are true in σ.
- **Premise lemma** (one per fragile-step kind, extending Lemma R): if a derivation's deliveries all occur and its
  premises hold in a run, its record is produced in that run.

So every pair holds in every schedule of every untried fault set.

**Completion, with literals.**
- A crash after a message that no clause names is mapped to `crash(n)`. Its crash literals are free, so it kills
  every send of n, as `crash(n)` does.
- An omission of a message no clause names is dropped.
- **The gap.** Both mappings are sound only if the image has been run, or the extra fault cannot change the run.
  For an omission this holds: a message never sent in any schedule of the image is never sent in H either. For a
  crash after an unseen message it does not hold in a program with fragile steps, because the image may never have
  been run. This is worker B's "stuck set" finding.
- **The fix.** The universe holds one symbolic candidate per node, "crash n after a message not seen yet". If the
  solver picks it, the engine first runs the set without it, to learn which messages n sends there, then names them.
  This replaces both the enumeration and the `Unnamed` candidate.

### 12.3 Directed schedules: not needed for soundness

The soundness argument above never assumes that a proposed literal value can be realized. The engine only has to
check whether H's schedules break the spec, and it runs every schedule of H unless H is already ruled out by robust
derivations alone. So:
- **No literal value is ever concluded impossible because a directed run failed.** That would be unsound unless
  the director were complete.
- **Directed runs are only an optimisation:** they find the schedule that realizes the proposed flips first. That is
  how a counterexample is found after one schedule instead of thousands.
- **Every directed run is a legitimate schedule by construction.** The director only picks among the decisions each
  hook is already allowed to make (`SimHook::autonomous_decision`: how many pending items a batch releases, which
  snapshot version, which ready tick runs next). The run is exactly the run the simulator would make with that byte
  sequence.
- **The minimal hook:** a driver that replays the bytes of a run and overrides the decisions at the named hook so
  as to hold back `x` until `y` is released.

### 12.4 Where I disagree: negation, and the fan-out rule

1. **Negation literals cannot be fixed, so their derivations never prune.**
   - `clear_n(k, y)` is about records that may exist only under faults: an Abort, a timeout's query. The checker can
     never prove that no such record arrives.
   - So the literal stays free, and a derivation through a negation protects nothing.
   - For the labels that need such derivations (2PC `committed` through peer answers), the solver ends up proposing
     every fault set over the universe and the engine runs each one. That is still a certificate when the formula
     becomes infeasible, and it needs no size bound for crash-only programs. But it costs what brute force costs.
   - With lossy channels the number of omission sets is exponential, though finite over the observed universe.
2. **Deleting the fan-out rule would multiply cost, without gaining soundness.**
   - The rule is not a special case of the encoding. It is the proof that makes one literal constant. Without it,
     every `after(d, m)` is free.
   - Every crash set in a fragile-free program would then have to run all its schedules. For the echo broadcast
     over 3 members, that means more than 160,000 schedules per set, instead of 1.
   - I propose to keep it, as the first entry in a short list of proved constant literals (`literals.rs`: fan-out,
     never-sent; later, barriers).

### 12.5 What is deleted, and what changes

- **Deleted:**
  - `races/enumerate.rs`, `Next::Unconstrained` and the fallback path in `check/races_engine.rs`;
  - the `Unnamed` candidate;
  - `mark_dead` and dead sets: the solver now proposes supersets itself;
  - the crashed-at-start excusal: it becomes a literal-free row, "a pair of a member crashed at start needs
    nothing";
  - the planned multi-payload fix: crash literals between two payload chains are free, so those crash sets are
    run. In a program with no choices that is one schedule each.
- **Kept, as proved literals:** the fan-out order (`provenance/fanout.rs`) and "one schedule when every literal H
  touches is proved and the program has no fragile step". The latter is `is_exact`, restated as "all of H's crash
  literals are fixed".
- **Size:** `races/` plus `check/races_engine.rs` are about 1,900 lines of product code today (tests excluded). I
  expect about −250 from the deletions and +150 for literals. I'll report exact numbers.

### 12.6 Predicted gate outcomes (to be measured)

| case | prediction |
|---|---|
| `ask_once`, CTP-3, CTP-2 racing, S1 | found, as now |
| `reliable_broadcast_closed` `new_messages`, 1–3 payloads | certified with no size bound (crash-only, finite universe), at about the cost of today's enumeration (8 / 134 / 281 sets, one schedule each) |
| echo broadcasts (2 and 3 members) | certified, same cost as now, because fan-out literals are proved |
| CTP-2 quiescing | certified, at brute-force cost (6 / 2,460) |
| lossy programs (echo over a lossy first hop, relay chain) | certificates without a size bound; cost grows with the number of omission sets the robust clauses can't rule out |
