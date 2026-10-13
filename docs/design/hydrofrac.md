# Hydrofrac: lineage-driven fault injection on the Hydro simulator

Status: working prototype on the `ldfi-level2` branch (feature `ldfi` of `hydro_lang`, tests in
`hydro_test`). This document is written for the Hydro core team. It explains what hydrofrac is,
how it relates to LDFI as published, exactly how it uses the simulator's hooks to inject faults
and to collect lineage, and what a user has to write to run it on a protocol. The research log
with every experiment and every soundness bug along the way is `ldfi-on-sim-hooks.md`; this
document is the summary.

## 1. What it is, in one paragraph

Hydrofrac is a test harness that takes a Hydro program, a correctness property written as a Rust
predicate, and a fault model (which channels may lose messages, which nodes may crash, how many
crashes), and searches for a combination of faults that makes the property fail. It does this
with the Hydro simulator: every candidate fault set is one simulator run, and the simulator's own
schedule explorer runs underneath, so a counterexample is a fault set plus an interleaving. The
search is lineage-driven: after each run in which the property held, hydrofrac computes which
message deliveries that outcome depended on, and the next fault set is chosen so that it breaks
every explanation seen so far. The search ends either with a counterexample or with a proof that
no fault set within the model can break the property (a certificate), and in both cases every
hypothesis it tried comes with the derivation that motivated it.

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
  a node, `EFF`, a crash budget, and a horizon set by the test body.
- The loop is Molly's: run, derive, negate, solve, run again, until counterexample or
  certificate. Hypotheses are minimal hitting sets over the derivations, chosen weakest-first.
- Over-approximate lineage is accepted as sound, under-approximate lineage is a bug, and three
  false certificates during development were traced to under-approximation and fixed.
- Certificates are the distinctive product: random and coverage-guided fuzzers cannot produce
  one.

It departs from Molly in five ways.

1. **Lineage is not a language feature.** Hydro programs are Rust closures over dataflow, not
   Datalog rules. Hydrofrac reconstructs lineage from the simulator's own event log at four
   levels of precision (section 6), the finest of which re-executes a node with deliveries
   removed to ask the counterfactual question directly. Molly's lineage was exact
   why-provenance; hydrofrac's is a sound approximation at levels 0 to 2 and a 1-minimal
   sufficient set at level 3.
2. **The schedule is explored, not fixed.** Each hypothesis runs under the simulator's
   exhaustive or sampled schedule exploration. A counterexample is a (fault set, schedule) pair.
   The Raft bug in section 9 needs a specific interleaving as well as a specific lost message,
   and only the combination finds it.
3. **The fault point is the receiving `batch`, not the network.** This is the main structural
   choice and the main limitation (section 5).
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
`release_all`, `pause`). Hydrofrac added a second mode to the same hooks: a **standing policy**.

## 5. Fault injection through the hooks

### 5.1 Omission: a standing policy on the receiving batch

A `BatchHook` on a `NoOrder` stream accepts an `OmissionPolicy`
(`hydro_lang/src/sim/runtime/tick_input.rs:252`):

```rust
pub struct OmissionPolicy<T> {
    pub omit: Vec<T>,                   // values to discard on arrival
    pub replay: Option<ReplayScript<T>>, // section 6.4
}
```

Installing it (`hook.omit(values)` on the test side) changes the hook's behaviour in three ways.
It stops drawing entropy and stops needing per-execution decisions. At every scheduling query,
the hook sweeps its buffer: any buffered element equal to one of `omit` is discarded and
recorded in the hook's `dropped` trace (`policy_sweep`, `tick_input.rs:454`). The tick becomes
runnable as soon as the buffer is non-empty after the sweep, and when it runs, the hook releases
everything left and records each item in its `delivered` trace (`apply_policy`,
`tick_input.rs:471`). Omission is a real drop, not a hold: a message in `omit` never reaches the
program, and later byte-identical copies on that channel are also dropped for as long as the
policy stands. The empty omission policy is `deliver_all()`.

A `BatchHook` on a `TotalOrder` stream only accepts a `PassthroughPolicy`
(`tick_input.rs:279`), which delivers everything and traces it. A totally ordered stream only
comes out of a `fail_stop` transport, which promises no loss, so omission is not in its
vocabulary. This is deliberate: the transport type the program declares is the program's own
statement of its fault model, and the fault point enforces it.

On the hydrofrac side, each registered channel is a `ChannelPoint` (`sim/ldfi.rs:372`) wrapping
one hook instance. `Ldfi::lossy_channel(label, hook)` registers it with omission allowed;
`Ldfi::reliable_channel(label, hook)` and `Ldfi::input(label, hook)` register it with omission
disallowed. At the start of each run, `Run::install` (`ldfi.rs:920`) asks every point to install
its share of the hypothesis: the point filters the fault set for `Omit` faults with its label,
deserializes their payloads, and calls `hook.omit(those)`; a reliable point calls
`deliver_all()`. At the end of the run, each point's `delivered()` and `dropped()` traces are
read back.

What a message *is*: `Message { channel: label, payload: bincode bytes }`. Two messages are the
same if they are on the same labelled channel and serialize to the same bytes. A retransmission
with an identical payload is the same message for omission purposes; a retransmission with a
different payload (Raft's heartbeat with a higher `leader_commit`) is a different one.

### 5.2 Crash: a cut at the send half, in generated code

Crashes are not implemented through hooks. The simulator's `create_network` emits a send half
for every `Network` node, and the lineage build adds a call to `LineageHandles::record_sent`
(`sim/runtime/lineage.rs:203`) before every send. That function consults a `CrashState`:

```rust
pub enum CrashPoint {
    BeforeAnySend,
    AfterSend { channel: usize, to_member: Option<u32>, bytes: Vec<u8> },
}
```

`note_send` (`lineage.rs:158`) counts the send; if the node is already dead, the send is dropped
(nothing is put on the wire, and the `Sent` event is recorded with `dropped: true`); if this
send matches the node's crash point, the node is marked dead afterwards. Every `sim_output`
sink of a dead node is suppressed the same way (`LineageOutputSender::try_send_with`,
`lineage.rs:314`), so the test body does not see outputs from a node that has died, which is
what the body's `run.crashed_member(..)` check is for.

So `crash(node after sending m)` means: the node runs normally, performs every send up to and
including `m`, and then every later send on every channel is lost and every later external
output is suppressed. It never restarts. This is Molly's crash. Because the cut is at the send
half, one crash point can split the fan-out of a single broadcast: in the plain-broadcast
counterexample, the broadcaster dies after its second of three sends, and members 0 and 1
receive while member 2 does not.

Why the crash point is a message and not a count: the first version used `crash(node after k
sends)`. Under a different hypothesis the node's send sequence shifted (a background transaction
it would have decided was not there), so `send #7` named a different message, the solver's
constraints meant something else in the bug's world, and the search certified a program with a
known bug. With `AfterSend { channel, to_member, bytes }`, if the named message is never sent,
the node simply survives.

### 5.3 End of finite failures and the horizon

`Run::end_of_finite_failures()` (`ldfi.rs:908`) does two things: it calls `deliver_all()` on
every registered point, replacing any omission policy, and it records the current length of the
event log. Only deliveries recorded before that index are omission candidates in later
hypotheses. The horizon `EOT` is whatever the test body drives: timers in Hydro are `sim_input`s
the body fires, and `hydro_lang::sim::quiesce().await` is a phase barrier.

### 5.4 What the hooks give for free

Because a hook under a policy draws no entropy, the schedule explorer's decision space under a
hypothesis consists only of the program's remaining nondeterminism (which runnable tick goes
next). In the measurement grid this had a practical consequence: at one schedule per hypothesis,
every verdict and counterexample matched the eight-schedule run on the protocols without
schedule-sensitive bugs, so the fair cost of a hypothesis is one simulator instance. Raft is the
exception, and the harness samples four schedules per hypothesis there.

### 5.5 What this fault point cannot do

The fault point is the consumer's batch, not the channel. Three consequences:

- A channel whose output is consumed without a hooked `batch` (a `recv.map(f).send(..)`
  forwarding path, a `sim_output` straight from the channel) cannot be faulted and is invisible
  to lineage except through the `Recv` event (section 6.1). The design calls for a hook on the
  `Network` node itself to close this gap; it is not built.
- If a channel's output is `tee`d to several consumers, an omission at one consumer's hook does
  not remove the message at the others. None of the test protocols do this.
- There is no `cut_after(n)` for a `fail_stop` connection dying on its own; the only fault on a
  reliable channel is the sender's crash.

### 5.6 Why a crash is not a set of omissions

Semantically a crash *is* an omission pattern: `crash(n, t)` is "omit everything `n` sends
after `t`", and the first draft of this design planned to build it that way, as `omit_from(n)`
on every channel out of `n`. The prototype nevertheless implements the two faults through
different mechanisms, omission as a policy on the receiver's hook (5.1) and crash as a cut at
the send half in generated code (5.2). The reasons are worth stating, because the split looks
like an accident and is not.

**The omission fault point cannot express a crash.** The hook is the consumer's batch, not the
channel, and 5.5 lists what it cannot see: hook-free forwarding paths, `sim_output`s straight
from a channel, tees. For a single omission those are gaps in the fault *model*. For a crash
assembled from omissions they would be gaps in the *fault*: the node would be dead on its hooked
channels and alive on every other path, an under-approximation of the fault, which is the
direction that produces false certificates. Three further things a receiver-side omission
cannot do at all: it cannot see the sender's timeline (the hook sees arrival order, which is not
send order under a different schedule, and across channels there is no common order at the
receivers at all, so "after `m`" is only well-defined at the send half); it is forbidden on
`fail_stop` channels by type, deliberately, although a node on a `fail_stop` channel can still
die and a prefix cut is exactly the one fault `fail_stop` admits; and it does not suppress the
dead node's external outputs, which `run.crashed_member(..)` and the `alive(N through send k)`
dependency (section 7) rely on.

**The solver needs a crash to be one literal, interpreted against lineage.** The ILP has one
binary per candidate fault, a crash budget, and at most one crash per node. If a crash were a
conjunction of omission literals, the search would have to know which messages the node sends
after `m`, and that set depends on the hypothesis: it is not known until the world in which the
node crashes has been run. So a crash cannot be extensional (a known set of `omit`s); it is an
intensional literal, `crash(node after m)`, whose extension is computed per run. That is what
`Clause { deps, sends, premises }` and `Dependency::killed_by(fault, sends)` do: whether
`crash(n after m)` kills a dependency is evaluated against the send sequence of the run in which
the dependency was observed, and a clause carries its run's send sequences because crash
literals have no run-independent meaning otherwise. The send-count version of this literal
certified a program with a known bug (5.2); naming the crash point by a message is Molly's
logical-time indexing in a substrate without a logical clock. Three constructs in the search
are crash-specific for the same reason and have no omission analogue: preserve points (the
crash right after a send in the pre-condition's lineage, 6.5), the `alive` dependency, and
crash premises (section 7, step 4).

**What remains of the objection.** After the fact, a crash is an omission pattern here too:
dead-node sends are logged as `Sent { dropped: true }` and surface as `crashed_sends`, so the
two faults are unified at the lineage level and differ only at the injection level. The design
in 5.5 that would close the gap, a hook on the `Network` node itself, would also unify the
injection: omission would be "drop these payloads at the send half" and crash "drop everything
after this payload at the send half", one mechanism with two policies. The present split is in
part historical, omission having been built on the existing scripted hooks (which already named
messages by payload equality) and crash in codegen because the hooks lack the sender's timeline.
Two asymmetries are open questions rather than settled design. Molly's omission is
`omit(from, to, payload, t)`; ours dropped `t` (identity is channel and bytes, and a standing
policy drops every identical copy, so an identical retransmission is the same fault) while crash
kept it, so the fault that is conceptually composite is the temporally indexed one. And a
derivation observed under `omit(m)` is, like one observed under a crash, only known to hold in
that world, yet only crashes are recorded as clause premises.

## 6. Lineage collection through the hooks

### 6.1 The event log

One simulator instance produces one `Vec<LineageEvent>` (`sim/runtime/lineage.rs:45`). The host
scheduler writes two kinds of event and the generated code writes four.

| Event | Written by | When | Contents |
|---|---|---|---|
| `Exec` | host | before each async run or tick execution | `ExecId`, location, member, tick-or-async |
| `Released` | host | right after a policy hook's trace grows during a tick execution | `exec`, `hook_id`, `member`, released items as bytes |
| `Sent` | generated send half | every network send | `exec`, channel (sink id), from/to node, bytes, `dropped`, record provenance ids |
| `Recv` | generated receive half | every network receive, before deserialization | `exec`, channel, to node, bytes |
| `Output` | generated `sim_output` sink | every external output | `exec`, sink, port, member, bytes, `dropped`, record provenance ids |
| `Tag` | generated code after a hooked batch (provenance build only) | every item a hooked batch releases | `exec`, fresh delivery id, `hook_id`, member, bytes |

The host sets `current_exec` in a shared `Rc<Cell<ExecId>>` before each execution
(`compiled.rs:2935`) and compares each hook's `delivered` trace length before and after the
hooks run to produce `Released` (`compiled.rs:2954`). Generated code reads `current_exec` when it
records `Sent`, `Recv`, `Output`, `Tag`. All of this state is created in the dylib and handed to
the host as `Rc`s, following the existing hook pattern.

A delivery is attributed to its send by matching: a `Released` item is matched to the earliest
unmatched `Sent` with the same bytes, to the same node, on a channel that the static slice says
feeds this hook. Matching by bytes alone was wrong (2PC's `commit` was attributed to the
`prepare` of the same transaction, which carried the same payload); channel-scoped matching
fixed it. A `Recv` on a channel with no hook still yields a dependency on its send, killable by
the sender's crash.

### 6.2 Levels 0 and 1: execution lineage and the static slice

The support of an output is the backward closure from the execution that produced it. At
**Level 0** (`Level::Execution`) the rules are: a tick execution depends on every message its
hooks released into it, and, as an over-approximation of persisted state, on every earlier
execution of the same tick; an async run depends on the tick executions at its location since
its previous run; a message depends on the execution that sent it. Everything a goal output
transitively reaches is in its derivation.

**Level 1** (`Level::Slice`, the default) narrows the first rule with a static slice of the
Hydro IR (`sim/slice.rs`), computed once per compiled program with the same traversal that emits
the simulator. For each network send and each `sim_output` sink, the slice walks upstream and
collects the hooked batches it can reach, split into `direct` (same execution) and `carried`
(through at least one `defer_tick`, so earlier executions). Closures that read state through
`Reference` captures add the slice of every closure that writes the same cell. A sink whose
slice reaches an unhooked batch is `opaque` and falls back to the Level 0 rule. The LDFI log
prints the slices at the start of every search:

```
ldfi: static slices:
  sink#51 (async): direct {initial@0, echo@0, yield(...)} carried {initial@0, echo@0}
```

Both levels need nothing from the program beyond the hooks. Both have the same weakness: a
protocol written as one step closure over a state struct is one IR node, so the slice says every
output depends on every input across all executions, which is Level 0 again.

### 6.3 Level 2: record-level provenance by IR rewrite

`SimFlow::with_provenance()` runs a sim-only rewrite (`sim/provenance.rs`) that wraps every
collection element as `Tagged<T>`: the value plus the set of delivery ids it was computed from.
`Tagged<T>` compares, hashes, orders, prints and serializes as `T`, so deduplication, joins,
set difference and the hook policies are unchanged. Each item a hooked batch releases gets a
fresh delivery id (the `Tag` event). `map`/`filter` pass the set through; `join`/`cross` union
both sides; `fold`/`reduce`/`scan` accumulate every contribution; cycles carry it; a network send
or `sim_output` strips the set from the wire payload and records it in the `Sent`/`Output`
event's `prov` field. The closure then follows record-to-record edges: delivery id → the `Tag`
that assigned it → the `Sent` that produced that record → its `prov` ids, and so on. Where a
record carries no provenance (a top-level path with no hook), the closure falls back to Level 1.

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

The mechanism is the `replay` field on the policies. A `ReplayScript<T>` (`tick_input.rs:310`)
is a queue of per-execution release lists. A hook under a replay script ignores its buffer and
releases `execs[k]` into the `k`-th execution of its tick, for exactly `execs.len()` executions,
including executions that release nothing. `CompiledSim::replay` (`compiled.rs:1234`) runs a
fresh instance with every hook of node *N* under its recorded script (minus the deliveries being
tested) and every other node's hooks under an empty script, driven by an all-zero entropy source
so any unhooked in-tick choice resolves the same way every time, with no body and no logging.
The instance runs *N* alone with its inputs held fixed, in a few milliseconds, and its own event
log says whether the output appeared.

`Lineage::counterfactual` (`sim/ldfi.rs`) does greedy deletion, latest delivery first, one
replay per step: delete a delivery; if the output is still produced, leave it deleted; otherwise
put it back. The result is a 1-minimal sufficient set. Each kept delivery is a dependency and
is followed to the send that produced it, whose own derivation is computed the same way,
memoized per run. Three details that each cost a false certificate before they were right: the
candidate set must include deliveries *after* the output's execution (a later echo can
substitute for the original); a replayed tick may legitimately have nothing to do once its only
input is deleted; and a node that does not reproduce the output with every delivery kept (one
fed through a hook-free path) falls back to the recorded lineage for that step.

`Level::Record` uses replay only at opaque records; a program written in dataflow style never
replays. `Level::Counterfactual` (`LDFI_LEVEL=replay`) replays every step and exists for
comparison; it costs about 5× the wall time of the recorded levels and 300–435 replays per
search on ISR.

### 6.5 Two kinds of fact

A derivation is computed for a *fact* the body names. `Run::witness(output, value)` means "this
value was output at some point"; the derivation is taken from the latest output equal to it, and
under replay the predicate is "the value still appears". `Run::witness_final(output, value)`
means "this value was the *last* output"; under replay the predicate is "the node's last output
on this port equals the value", so a deleted heartbeat that would have changed the final view
stays in the derivation. ISR's final leader view had to be declared `witness_final` to avoid a
false certificate: two heartbeats are enough to *publish* a view, but the goal was about the
view that *stood* at the end, which rests on every later heartbeat too.

`Run::require(output, value)` names a fact the goal is conditioned on ("if the write was
acknowledged, then..."). Its derivation is not negated; instead the search tries to *preserve*
it, and the crash right after each send in it joins the candidate universe, because the weakest
crash that keeps the pre-condition and kills what follows is often the bug (the leader dies
between acknowledging and replicating).

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

### 8.1 Program side: expose the batches

Every channel that may be faulted, and every channel the goal can depend on, must be consumed
through a `batch` that carries a hook. That means the protocol function takes a
`NonDet<Option<BatchHook<..>>>` parameter per such batch (or a struct of them) and passes it to
`use::batch(stream, nondet)`. Callers that do not care pass `nondet!(/** reason */)` and the
program's behaviour is unchanged.

Broadcast (`hydro_test/src/cluster/reliable_broadcast.rs:49`) takes two:

```rust
pub fn broadcast<'a>(members, source, echo: bool,
    nondet_initial: NonDet<Option<InitialHook>>,
    nondet_echo: NonDet<Option<EchoHook>>) -> ... {
    ...
    let from_broadcaster = use::batch(initial, nondet_initial);
    let from_members     = use::batch(echoes,  nondet_echo);
```

Raft (`hydro_test/src/cluster/raft.rs:894`) bundles four into `RaftHooks { requests,
election_timer, heartbeat_timer, traffic }`, each `Option`, with a `SimHook` impl so
`flow.sim_hook()` creates all four at once; `raft_server` takes `NonDet<RaftHooks<..>>` and
`take_hook()`s it. That is about 50 lines of declarations plus changing four `nondet!(..)`
arguments at the batch sites. Every other caller of `raft_server` passes `nondet!(/** .. */)`
and gets `None` everywhere.

This is a requirement on code structure, not on behaviour. The soundness rule is: *every channel
a goal can depend on must be registered*, because an unhooked path is invisible to lineage and a
missing dependency can manufacture a certificate. In practice that means hooking every batch in
the protocol.

For Level 2, the program is compiled with `.with_provenance()`. The rewrite is automatic but
supports a fixed set of operators; using one it does not support fails at compile time by name.
Adding an operator is a `syn` closure-wrapping rule in `sim/provenance.rs`.

### 8.2 Test side: the harness

The harness builds the flow with hooks, compiles the simulator, declares the fault model and the
goal, and writes the body. The broadcast harness (`reliable_broadcast.rs:108`) is about 55 lines:

```rust
let initial_hook: InitialHook = flow.sim_hook();
let echo_hook: EchoHook = flow.sim_hook();
let deliveries = broadcast(&members, source, echo,
    nondet!(/** LDFI delivery point */ hook = initial_hook),
    nondet!(/** LDFI delivery point */ hook = echo_hook));
let out = deliveries.sim_cluster_output();
let compiled = flow.sim().skip_consistency_assertions()
    .with_cluster_size(&members, 3).compiled();

let mut ldfi = Ldfi::new()
    .crashable("broadcaster", &broadcaster)
    .crashable_cluster("member", &members, 0..3)
    .crash_budget(crashes)
    .goal_all_members(&out, "member", 0..3);
for m in 0..3 {
    ldfi = ldfi.reliable_channel(&format!("initial@{m}"), initial_hook.on(m))
               .reliable_channel(&format!("echo@{m}"), echo_hook.on(m));
}
ldfi.run(&compiled, async |run| {
    source_send.send(Payload(42));
    hydro_lang::sim::quiesce().await;
    let delivered: Vec<bool> = (0..3)
        .filter(|m| !run.crashed_member("member", *m))
        .map(|m| out.collect_sorted(m).await.contains(&Payload(42)))
        .collect();
    delivered.iter().all(|d| *d) || delivered.iter().all(|d| !*d)   // agreement
})
```

The parts, and what each demands of the author:

- **Fault model.** Which hooks are `lossy_channel` (omission allowed; must be `NoOrder`), which
  are `reliable_channel` (traced only), which are `input` (external, never a fault; timers and
  client requests). Which nodes are `crashable`. The budget. Labels are free strings used in
  reports and fault names.
- **Goal shape.** This tells lineage what to trace. `goal(&out)` is "some value appears";
  `goal_all_members(&out, cluster, members)` is "every non-crashed member produced a value";
  `goal_witnessed()` defers to the body, which names the facts with `witness`,
  `witness_member`, `witness_final`, `witness_member_final`, and the pre-conditions with
  `require`, `require_member`. Choosing between `witness` and `witness_final` is a judgement
  about the property (is it "ever" or "at the end") and getting it wrong produced a false
  certificate once.
- **Body.** Drives the run: sends inputs, fires timers, awaits `quiesce()`, calls
  `end_of_finite_failures()` at the right point, skips crashed nodes, and returns the verdict.
  The verdict is the ground truth; the goal shape is only lineage's hint. The two must agree,
  and hydrofrac does not check that they do. A body whose pre-condition fails should call
  `mark_vacuous()` and return `true`.

The Raft harness (`raft_ldfi.rs`) is about 200 lines including the body, the comparison test
and the shared `compile` helper; the body alone is 60 lines, most of it the scenario (which
timers fire when) and the agreement check.

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
| Reliable delivery | 2 hook parameters (messages, acks) | ~110 lines | first example |
| Broadcast | 2 hook parameters | ~115 lines (harness ~55) | the harness above |
| 2PC + CTP | 5 hook parameters (prepares, commits, peers, votes, timeouts) | ~210 lines + a comparison harness | goal conditioned on a commit |
| ISR replication | `Hooks` struct with 9 hooks; protocol written in two encodings | ~190 lines + the measurement grid | `witness_final` on the view |
| Raft | `RaftHooks` struct, 4 batch sites, ~50 lines | ~200 lines, body ~60 | 4 schedules/hypothesis |

The protocols themselves were written or ported for this work, so the "program change" column
is what a protocol author adds, not what a retrofit on existing code cost; no retrofit has been
tried except Raft, where the four batches already existed and only their `nondet!` arguments
changed.

The recurring cost is not the lines. It is three judgement calls the tool cannot make: which
facts witness the property and whether they are "ever" or "final" facts; which facts are
pre-conditions; and whether every channel the property could depend on is registered. Each of
the three false certificates found during development was one of these judgements made wrong,
or a vocabulary bug in the tool (count-indexed crashes) that the same kind of judgement
exposed. Users should expect to read the first run's derivation and check that it names the
messages they expected.

## 9. What it has found

On the paper's examples it reproduces the paper: tries-twice reliable delivery breaks under two
omissions in 3 runs; plain broadcast breaks under one mid-fan-out crash in 2 runs; reliable
broadcast certifies under one and two crashes; 2PC breaks under one coordinator crash, 2PC with
cooperative termination certifies under one crash and breaks under two (coordinator after one
commit, then the participant that got it); ISR's unclean election reproduces the Kafka bug.

The headline is Raft. `raft.rs` has thirteen behavioural tests including unit tests of the
§5.4.1 election restriction and two fuzz tests of concurrent elections; none exercises message
loss. An injected bug that compares last-log index only, ignoring term (a mistake shipped in
real implementations), passes all thirteen. Hydrofrac finds it in 25 hypotheses (100 simulator
instances at 4 schedules each) with the counterexample `{omit AppendEntries(a, leader_commit
0)→1, omit AppendEntries(a, leader_commit 1)→1}` under the schedule in which member 0 takes term
2. Blind enumeration needs 160 hypotheses (640 instances). Random fault injection with the same
vocabulary found it in 0 of 5 repeats within 640 instances each (two thirds of its instances
were vacuous). On correct Raft, hydrofrac certifies in 175 hypotheses; blind exhausted its
budget at 256.

Measurement grid (ISR, 2PC at n = 2..6, workload 1..8; all in `ldfi-on-sim-hooks.md`): lineage
beats blind enumeration everywhere, by 2–7× on certificates; against uninformed random it wins
on 2PC at every size by a margin that grows with size and never misses, while random misses in
2–4 of 10 repeats from n = 5; on ISR at small sizes random is competitive because the fault
space is small. Record-level provenance keeps the hypothesis count flat as workload grows (2PC
load 8: 11 hypotheses vs 73 for the slice levels); the slice is not a substitute. On the
closure-encoded ISR, replay turns 14/27 (bug/certificate) into 6/14, better than the dataflow
encoding under record provenance (10/18).

## 10. Limitations and what is not built

- **Fault point.** Hook-free consumption paths and `tee`d channel outputs are outside the fault
  model (section 5.5). A `Network`-node hook would close this.
- **Program intrusion.** Every faultable or traced batch needs a hook parameter. There is no
  way to point hydrofrac at a protocol that was not written with this in mind.
- **Properties are checked at quiesce.** "Eventually" means "by the end of the run after EFF".
  Properties that need an unbounded future are out of scope, as in Molly.
- **The spec is unchecked.** The goal shape and the body's verdict can disagree. The body can
  forget a requirement, declare a final fact as an "ever" fact, or omit a channel from
  registration, and the result is a wrong certificate with no warning.
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
