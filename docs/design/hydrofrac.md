# Hydrofrac: lineage-driven fault injection on the Hydro simulator

Status: working prototype (feature `ldfi` of `hydro_lang`, tests in `hydro_test`), after three
rounds. The first built the search on the simulator's scripted hooks; the second moved the fault
point to a filter on every network send (section 5, with section 11 recording why); the third
replaced hand-named goals by specifications written as Hydro views (section 6.5) and worked out
what counterfactual lineage does and does not buy for them (`hydrofrac-negation.md`). This
document is written for the Hydro core team. It explains what hydrofrac is, how it relates to
LDFI as published, exactly how it uses the simulator to inject faults and to collect lineage,
what a user writes to run it on a protocol, what it has found, and what remains. The research
log with every experiment and every soundness bug along the way is `ldfi-on-sim-hooks.md`; this
document is the summary. Section 12 is the roadmap.

## 1. What it is, in one paragraph

Hydrofrac is a test harness that takes a Hydro program, a correctness specification, and a fault
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
`hydrofrac-negation.md` say what that means and how often it holds.

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
   The Raft bug in section 9 needs a specific interleaving as well as a specific lost message,
   and only the combination finds it.
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
the event log. From then on omissions are ignored (crashes are permanent, as in Molly), and only
sends recorded before that index are omission candidates in later hypotheses. The horizon `EOT`
is whatever the test body drives: timers in Hydro are `sim_input`s the body fires, and
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
The reason is that the Raft bug is schedule-sensitive (section 3, departure 2): the fault set is
necessary but not sufficient. The body lands request `b` at member 1 and member 0's election
timer in the same phase, so the explorer's remaining nondeterminism, which member's tick runs
next, decides whether 1 broadcasts `AppendEntries(b)` before it sees `RequestVote(term 2)` and
keeps the leadership (nothing happens), or steps down first holding an unreplicated `b@1` so that
0 wins term 2. Only in the second interleaving does the lost `AppendEntries(a)` leave 1 with a
stale log as long as the others', which the index-only check fails to reject. Under one schedule
the search can land on the harmless interleaving for the critical fault set, see the goal hold,
and certify a program with a known bug; the other protocols' outcomes do not depend on tick order
under a fixed fault set, which is what the eight-vs-one measurement showed. The cost is in the
certificate: with sampled schedules it is relative to the sampled schedules (the doc comment on
`Exploration::Fuzz` and the `ldfi_certifies_raft_agreement_under_one_crash` docstring both say
so), and Raft is the one protocol where that caveat bites. `Exploration::Exhaustive` is the sound
alternative; the Raft harness does not use it, for cost.

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
fed through a hook-free path) falls back to the recorded lineage for that step.

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
schedule. The intervention also found the *minimal* set where the record provenance of the
positive view had accumulated more: the one-crash certificate went from 15 hypotheses to 10 and
the two-crash from 89 to 32.

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
agree. That costs the minimality gain above (broadcast certificates are back to 15 and 89
hypotheses) until the observer's dataflow is checked for monotonicity, which is what would
license the counterfactual alone. `hydrofrac-negation.md` has the analysis and the proposed
check.

A deduplicating view (`unique`, `first`) keeps one derivation per run, so alternatives surface
one hypothesis at a time; the complete answer is lineage that carries alternatives (a set of
derivations per record), which is not built.

Every harness runs on a specification: agreement for broadcast and 2PC (`PRE`: someone
delivered / committed; `POST`: every correct member did), durability for ISR (`PRE`: the client
was acknowledged and the final view names a leader; `POST`: that leader's log holds the write),
and state-machine safety for Raft (`PRE`: member 0 committed its request `a`; `POST`: among the
correct members' commits and member 0's own, the distinct `(index, message)` pairs are as many
as the distinct indices, i.e. no two committed entries disagree at an index). Raft's `POST` is
the other everyday non-monotone view: agreement is the *absence* of a conflict, and a hypothesis
breaks it by making a member commit something else. The search still finds the bug because the
same hypothesis removes that member's recorded commit of `a` (the lost `AppendEntries(a)`), which
is in the derivation; a conflict that appeared without displacing a recorded commit would not
be proposed. The first version's `witness` / `witness_final` / `require` API remains available;
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
   256) is exhausted, `Outcome::Budget`.

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
| Broadcast | none required; channels named | ~130 lines (harness ~45) | the only harness on a `Spec` so far |
| 2PC + CTP | none required; channels named | ~200 lines + a comparison harness | goal conditioned on a commit |
| ISR replication | `Hooks` struct with 9 hooks, kept for Level 3; channels named | ~180 lines + the measurement grid | `witness_final` on the view |
| Raft | `RaftHooks` struct, 4 batch sites, ~50 lines, kept for Level 3 | ~180 lines, body ~60 | 4 schedules/hypothesis |
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
broadcast certifies under one and two crashes (15 and 89 hypotheses with the union derivation of
section 6.5; 10 and 32 with the counterfactual alone), both under the one
agreement specification of section 6.5; 2PC breaks under one coordinator crash, 2PC with
cooperative termination certifies under one crash and breaks under two (coordinator after one
commit, then the participant that got it); ISR's unclean election reproduces the Kafka bug.

The headline is Raft. `raft.rs` has thirteen behavioural tests including unit tests of the
§5.4.1 election restriction and two fuzz tests of concurrent elections; none exercises message
loss. An injected bug that compares last-log index only, ignoring term (a mistake shipped in
real implementations), passes all thirteen. Hydrofrac finds it in 17 hypotheses (68 simulator
instances at 4 schedules each; 25 on the first substrate with its hand-named goal, 20 on the
filter) with the counterexample `{omit rpc[0→1]:AppendEntries(a, leader_commit 0), omit
rpc[0→1]:AppendEntries(a, leader_commit 1)}` under the schedule in which member 0 takes term 2.
Blind enumeration needs 160 hypotheses (640 instances). Random fault injection with the same
vocabulary found it in 0 of 5 repeats within 640 instances each (two thirds of its instances
were vacuous). On correct Raft, hydrofrac certifies in 89 hypotheses (175 on the first
substrate, 133 on the filter with the hand-named goal); blind exhausted its budget at 256. The
improvements come from exact delivery ids on the receive side (the record provenance of a Raft
step names its sends precisely where the first version matched releases to sends by bytes) and
from the specification's views deriving one lineage for the whole agreement fact rather than one
per hand-named witness.

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

To place the Raft result against an established tool, the same injected bug was run under
Maelstrom (Jepsen's workbench), through a Raft-backed `lin-kv` workload written for the purpose
(`hydro_test/src/maelstrom/lin_kv_raft.rs`; there was none in the repository). Twelve runs, 29
minutes of test time, three and five nodes, partitions every 1–10 s, uniform and exponential
latency, rates 3–100: ~1,900 leader elections, no linearizability anomaly, and an in-node oracle
that prints whenever a committed index is overwritten never fired. The correct configuration
passes every run, so the adapter is not the problem. Hydrofrac finds the bug in 17 hypotheses, 68
instances of ~30 ms: about two seconds of simulation.

The bug never *occurred* under Maelstrom, which is a statement about search and fault model, not
about the checker. The trigger is a conjunction: a node holding an unreplicated entry at an index
where a different entry has been committed, campaigning while its index is still at least the
majority's, before a heartbeat reaches it. Four things keep Maelstrom away from it.

1. The search is undirected: faults are drawn from a fixed distribution and nothing about run
   `n` informs run `n + 1`. Hydrofrac's lineage of the committed entry names
   `AppendEntries(a)→1` as something the good outcome rested on, so its omission is a weakest-first
   hypothesis within the first runs; the explanation of the good outcome contains the recipe for
   the bad one.
2. The fault vocabulary has the wrong shape. The trigger is *selective* loss (drop `a` to member
   1, deliver what follows, so 1's index keeps pace while its content diverges). A partition drops
   every message on a link for an interval; the only "lose this, deliver the next" it expresses is
   its own boundary. Hydrofrac's omission is a standing filter on one message identity.
3. Load heals the bug. Under a partition the majority keeps committing client operations while
   the isolated node's clients time out, so the isolated node's log falls *behind*, and the
   index-only check then correctly denies it the vote. Continuous traffic, which makes the
   workload realistic, masks the defect; hydrofrac's body issues two requests and the stale log is
   as long as the others' by construction of the hypothesis.
4. Timers are not the test's to fire. The deposed node must campaign before the new leader's
   heartbeat reaches it; after a heal the heartbeat almost always wins that race. The simulator
   body fires the election timer in the phase where it matters and the explorer enumerates the
   remaining order.

This is a result about one bug class (interleaving plus selective loss) and about Maelstrom's
built-in nemeses; a nemesis that drops individual messages would help with the second point and
none of the others. Maelstrom earned its keep regardless: its first run exposed a real bug in
Hydro's Maelstrom backend (every cluster-to-cluster channel shared one untagged message type, so
a program with two such channels deserialized each other's bytes), a code path no simulator test
exercises. Different tools find different failure classes.

### 9.2 Paxos, and two defects the simulator found on the way

Paxos (`hydro_test/src/cluster/paxos.rs`, the production consensus core behind the benchmarks)
had never run in the simulator: its leader liveness is on the wall clock. With the timers as
inputs, two proposers, three acceptors, lossy acceptor channels and the agreement specification
of section 6.5 over the chosen `(slot, value)` pairs, the injected bug (a commit quorum of `f`
acknowledgements instead of `f + 1`, which every unit test passes) is found in 12–43 hypotheses
across eight runs, 8–50 s, with the counterexample `{omit p2a(x)→acceptor 1, omit
p2a(x)→acceptor 2}` under the schedule in which the new leader's P1b quorum misses acceptor 0: the
predicted minimal set. Correct Paxos certifies under one crash in 196 hypotheses (184 s, 1,632
replays). Through the replicated key-value store (one client, two replicas, prefix agreement over
each replica's applied writes with their position) the same bug is three omissions away and is
found in 42 hypotheses; the store certifies in 280 (the hitting sets grow with the extra channel
and with the second leader's crash points, past the default budget of 256). Compartmentalized
Paxos (two proposers, two proxy leaders, a 2 × 2 acceptor grid, eight nodes) certifies in 124
hypotheses, 71 s, and its injected bug (a write quorum one acceptor short of a row) in 3.

Two findings were not injected.

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

Both are defects of the kind the simulator alone finds, given a scenario that drives a leader
change; what hydrofrac added is the scenario's faults and, for the second, the report that named
the schedule.

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
  `hydrofrac-negation.md` shows a false certificate from exactly this on ISR before the observer
  derivation was made the union of counterfactual and recorded lineage. The search now finds
  these bugs because the breaking hypothesis also displaces a recorded fact in `POST`'s
  derivation; a conflict that appeared without displacing one would not be proposed. The
  certificate for such a property is relative to that, and nothing in the tool yet says so: a
  polarity check of the observer's dataflow that grades the certificate is the next piece of
  work (section 12). The first version's hand-named goal API (`goal`, `witness`, `require`) is
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

Schedule exploration. The Raft counterexample of section 9 is a (fault set, interleaving) pair; a
filter under `deploy` yields one interleaving per run, chosen by the operating system, which is
Molly's fixed schedule or worse.

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

Built, across the three rounds: the search loop with an ILP hitting-set solver; the sender-side
filter as the fault point, with every channel a fault point by construction and the transport
type as the fault model; message-named crash points; four lineage levels, with record provenance
exact to the send and counterfactual replay at node granularity; specifications as Hydro views on
an observer, derived by intervention and by record provenance together; five protocols on the
same harness shape (reliable delivery, broadcast, 2PC with cooperative termination, ISR
replication, Raft), each reproducing its published or injected bug and certifying the correct
variant; and a comparison against Maelstrom on the Raft bug (section 9.1).

Next, in the order that soundness requires.

1. **Polarity check of the specification.** A walk over the observer's dataflow assigning
   polarities from the `POST` sink to the observed leaves (`hydrofrac-negation.md` §4 has the
   per-operator rules). Its output is a *grade* on the certificate, not a gate on the search:
   "monotone view, certificate within the fault model and the explored schedules" versus
   "non-monotone view (operators: ...), certificate relative to the recorded derivations
   persisting". Where the view is monotone it also licenses the counterfactual derivation alone,
   recovering the minimal derivations (broadcast 10 / 32 hypotheses instead of 15 / 89).
2. **Decision-trace replay.** Record every nondeterministic decision of a run (the scheduler's
   draws, the hooks' releases, the filter's fates) and replay a prefix exactly with one decision
   overridden. This is the general counterfactual oracle of which node-level replay is the
   coarse instance: why and why-not for any fact at any step, without hooks, and a decision
   sequence rather than entropy bytes as the counterexample's schedule. It is worth building only
   if (1) shows the certificates it would strengthen are the ones users want; it does not by
   itself repair the inference for non-monotone views.
3. **More protocols.** Section 9 covers the paper's examples plus Raft. Candidates in the
   repository: the Paxos family (`paxos.rs`, `compartmentalized_paxos.rs`, `paxos_with_client.rs`)
   with agreement and validity as views, and the replicated key-value store (`kv_replica`) with
   a linearizability-shaped specification; each is a protocol written without hydrofrac in mind,
   which is the retrofit the effort table (section 8.4) has not yet measured.
