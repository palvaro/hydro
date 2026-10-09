# Lineage-driven fault injection on the Hydro simulator hooks

Status: proposal, October 2026. Companion to [`sim-hooks-1875-v2.md`](./sim-hooks-1875-v2.md),
whose §8 lists "network-level hooks: message drop and delivery-order control on specific
channels" as a follow-on; this document is the LDFI-shaped version of that follow-on.

## Verdict in one paragraph

The scripted simulator hooks (`hydro_lang/src/sim_hooks.rs`, `sim/hooks.rs`,
`sim/runtime/scripted.rs`) are a good substrate for the *mechanics* LDFI needs: a per-operator,
per-cluster-member handle that can withhold or release individual messages by value, shared across
the host/dylib boundary, composable with fuzzing and exhaustive search of the remaining
non-determinism. They are not, by themselves, the right *fault point*: hooks sit on consumers
(`batch`, `snapshot`, `assume_ordering`, NoOrder `fold`), not on channels, and their decision model
is a schedule (one queued decision per tick execution) rather than a standing policy ("omit these,
deliver the rest"). Both gaps are small and local: one new hook kind on `Network` nodes with policy
semantics, and one execution-context tag on messages. With those, the hook registry gives
fault injection for free and *tick-granular* lineage nearly for free. Record-level (Molly-grade)
lineage would need an IR rewrite; I argue it is a second phase and that its marginal value for
fold-heavy Hydro programs is smaller than it first appears, because Molly's own aggregate rule is
equally coarse.

## What LDFI needs from a substrate

From the SIGMOD 2015 paper (Alvaro, Rosen, Hellerstein) and Molly:

1. A **fault vocabulary** over a bounded execution: message omission `omit(from, to, payload, t)` and
   permanent crash `crash(node, t)` (a crash is "omit everything `node` sends after `t`"), with bounds
   `EOT` (horizon), `EFF` (no omissions after this time; lets eventual properties be checked), and a
   crash budget.
2. **Lineage** of the correctness outcome in a run: a disjunction of derivations, each a conjunction
   of delivery events. Negated, it is a CNF whose models are the fault sets that disable every
   derivation seen so far. Over-approximate derivations (a derivation listing messages it did not
   really need) are *sound*: pruning a hypothesis only happens when a superset of some real support
   survives. Under-approximation is unsound.
3. A **deterministic re-execution** of the program under a hypothesis, plus **collection of new
   lineage** (redundant paths, retries) from that run, iterating to UNSAT (certificate within bounds)
   or an invariant violation (bug).
4. **Stable message identity across runs**: the hypothesis "omit m" must refer to the same m in the
   next run.

Molly fixed a synchronous schedule (deliver at `t+1` or never) and ignored reordering. That is
precisely the dimension the Hydro simulator enumerates, so the two compose rather than overlap.

## What the simulator is, in LDFI terms

Reading `sim/builder.rs`, `sim/compiled.rs`, `sim/runtime/*`:

- **Where non-determinism lives.** One async DFIR per location (per cluster member) runs
  deterministically; every tick gets its own DFIR, fed only through hooks. A hook buffers input
  across scheduler steps and decides what enters the next tick execution (`TickInputHook`), or is
  its own scheduling unit (`ObservationHook`: top-level `assume_ordering`, NoOrder `fold`), or is
  resolved mid-tick (`InlineHook`). The scheduler (`LaunchedSim::step`, `compiled.rs:2427`) runs all
  async DFIRs to a fixpoint, then picks one runnable tick/observation using fuzzer entropy.
- **There is no network fault primitive.** `create_network` (`builder.rs:1914`) is an unbounded
  in-memory channel; `fail_stop` is reliable and ordered. `lossy_delayed_forever` is accepted only
  with `test_safety_only()`, and is "modeled" purely by the type system: the channel output is
  `NoOrder`, so consumers must pass through a subset-selecting hook (`TopLevelFoldHook` picks a
  non-empty subset per release; NoOrder `batch` picks a multiset), and a message never released
  before the test stops looking *is* a dropped message. Plain `lossy` is a `todo!`.
- **Scripted hooks** (`Scripted<H>`): a handle created by `flow.sim_hook()` is bound with
  `nondet!(... hook = h)`; the test body issues decisions (`release_values`, `reveal`, `next`,
  `order`) that form a schedule of groups; a hook with buffered input and no decision is a hard
  error unless `pause()`d. Scripted hooks consume no entropy; unscripted siblings are still fuzzed,
  so a test can pin some decisions and explore the rest (`fuzz`/`exhaustive`), or pin everything
  (`deterministic`).
- **Per-member scoping**: a hook on a cluster operator has one instance per member
  (`OnCluster` → `.on(member)`), registered as `(hook_id, Some(member))` in
  `ScriptedHookRegistry`.
- **Cross-boundary plumbing**: hook objects are `Rc<RefCell<dyn ...>>` created in the dylib and
  handed to the host; payloads cross as bincode blobs because `TypeId`s and thread-locals are not
  shared with the dylib (`scripted.rs` module docs). Any new state the host needs from generated
  code must take the same route.
- **Replay**: `fuzz_repro(bytes)` re-runs a schedule from recorded entropy; `HYDRO_SIM_LOG=1`
  prints every release with the operator's source location.
- **Time is external.** Timers are `sim_input`s the test fires (see `hydro_test/src/cluster/raft.rs`
  tests); `sim::quiesce()` is a phase barrier.

## Fit: fault injection

### What already works

Omission on a `lossy_delayed_forever` channel whose receiver consumes through a scripted
NoOrder hook can be expressed today: `hook.on(r).release_values(all_but_m)` then `pause()`. Crash of a
member is "never release anything it sent" at every receiver's hook, filtering on the
`MemberId` that cluster-sourced payloads carry. That is enough for a hand-rolled experiment on a
small program, and it is how I would validate the solver loop before touching codegen.

### Why that is the wrong fault point in general

1. **Fan-out.** A channel output can be `Tee`d to several consumers, each with its own hook.
   An omission must be all-or-nothing, so the harness would have to coordinate the drop across every
   downstream hook of the same message, and know statically which hooks those are. A hook *on the
   channel* drops once, before the tee.
2. **Hook-free paths.** `recv.map(f).send(&next, ...)` and `recv.sim_output()` have no hook; loss on
   such a channel is unrepresentable. Any pipeline that forwards is a hole in the fault model.
3. **Hold is not drop.** `BatchDecision`/`UnorderedBatchDecision` offer `Prefix`/`Values`/`All`;
   nothing discards. "Delayed forever" equals omission only if the test never asks the simulator to
   drain. To check eventual properties after `EFF` the harness must release *everything else* and
   quiesce, and a held message would then be delivered, which is a retry-free re-delivery the real
   system never performs. We need `drop(values)`, which is type-legal exactly on `NoOrder` /
   `Lossy*` channels.
4. **Schedule vs. policy.** The scripted layer is group-scheduled: one queued decision per hook per
   tick execution, forgotten-hook errors at each boundary, `pause()` declarations. LDFI wants a
   standing filter ("deliver everything except {m1, m2}; everything from node 3 after its 4th send is
   gone") that costs no entropy and no per-step scripting. Driving LDFI through `release_values`
   would force the harness to predict arrival order and sizes it does not care about.
5. **Fault model mismatch by transport.** Under `fail_stop` typing, arbitrary omission is outside the
   declared contract; the only legal fault is a prefix cut (connection death = crash of one side).
   Under `lossy`/`lossy_delayed_forever`, arbitrary omission is legal. The transport's fault policy
   type is, usefully, the program's own declaration of its LDFI fault model; the fault point should
   enforce it rather than let the harness inject whatever it wants.

### Proposal: a `ChannelHook` with policy semantics

A new hook kind bound to a `Network` node, instantiated in `create_network` at the receive half
(one instance per receiving member), kept out of the fuzzer's decision space.

```rust
// handle (sim_hooks.rs); S is OnProcess / OnCluster of the *receiver*
pub struct ChannelHook<T, F: FailPolicyKind, S = OnProcess> { id, member, .. }

// binding: reuse the NonDet payload mechanism already on `lossy`
.send(&replicas, TCP.lossy(nondet!(/** omissions */ hook = ch)).bincode())
// `lossy_delayed_forever()` takes no NonDet; add `.sim_hook(ch)` on NetworkingConfig,
// or accept an optional payload the same way.

// test side (sim/hooks.rs)
ch.on(r).omit(values)          // Lossy, LossyDelayedForever: discard these payloads (multiset, by value)
ch.on(r).omit_from(sender)     // cluster sources: discard everything from this member from now on
ch.on(r).cut_after(n)          // FailStop: deliver the first n from `sender`, then nothing (connection death)
ch.on(r).deliver_rest()        // explicit "no more faults" for EFF; default policy is deliver-all
ch.on(r).delivered()           // Vec<(sender, payload, exec_tag)> observed so far (lineage, below)
```

Runtime: a `Policy<H>` shell beside `Scripted<H>` in `runtime/scripted.rs` that shares
`ScriptedHookControl` (registry, status blob, location meta) but not the group/boundary machinery;
its `RuntimeHook::only_one_possible_decision` is always `true` and it never draws entropy, so an
unbound or default-policy `ChannelHook` is invisible to `fuzz`/`exhaustive`/`deterministic`. The
decision set is gated by the fail policy type at the handle, so `omit` does not even exist on a
`FailStop` channel. A side benefit: this is also the missing implementation of plain `lossy` in the
simulator (`builder.rs:1941`), with the fuzzer allowed to drive the policy if someone wants that
later.

Crash of node `n` at "time" `t` becomes: `omit_from(n)` on every channel out of `n` after `n`'s
`t`-th send, plus the harness ignoring `n`'s external outputs after that point. There is no
restart in Hydro's model, so permanent crash is the only crash, as in Molly.

### Message identity and time

Molly's `t` is a logical clock. Here, use `(channel_id, from_member, to_member, payload, k)` where
`k` is the k-th occurrence of that payload on that channel (retransmissions are distinct faults, as
they should be). Payload equality is what the existing hooks already use to name messages, and the
`Serialize + DeserializeOwned + PartialEq` bounds the `SimHook` impls demand already guarantee it.
Occurrence counting replaces the clock; crash time is "after the sender's `t`-th send on any
channel", also a count. Both are stable across runs as long as the schedule is pinned (next
section).

## Fit: lineage

Three levels, increasing cost.

### Level 0: what exists today

The release log plus `delivered()` give the set of messages delivered before the outcome. Treating
that whole set as the single derivation is sound (maximally over-approximate) and reduces LDFI to
bounded search over subsets of *messages that actually flowed*. That is already better than blind
injection over the message space, and it exercises the whole loop (fault vocabulary, hypothesis
runner, solver, convergence). Build this first.

### Level 1: execution-granular lineage, from the hooks (proposed)

The scheduler knows every unit of execution: an async DFIR run at `(loc, member)`, or a tick
execution at `(tick_loc, member)`. Each hook knows what it released into which tick execution.
Each `ChannelHook` knows what arrived and what it delivered. The one missing link is which
execution *produced* a message. Add an execution tag:

- The scheduler assigns `ExecId = (SimLocation, kind, seq)` before every `run_tick()` call and
  stores it in an `Rc<Cell<ExecId>>` created by generated code and returned in `DylibResult`
  (thread-locals do not cross the dylib boundary; this follows the existing `Rc<RefCell<hook>>`
  pattern).
- The sim channel element type becomes `(ExecId, payload)` (sim-only; stripped at the receiver's
  deserialize map, cf. `channel_elem_ty` at `builder.rs:293`). `sim_output` sinks get the same tag.

Then the lineage graph is built on the host with no provenance inside user closures:

```
message m   <- produced-by  ExecId(async run at loc L, round r)
async run   <- inputs       {tick executions at L yielded since L's previous async run}
                            ∪ {channel deliveries to L since then} ∪ {external inputs to L since then}
tick exec e <- inputs       {releases of e's hooks} ∪ {earlier executions of the same tick}  // state carry
```

A derivation of an output is the backward closure to channel deliveries. The "earlier executions of
the same tick" edge over-approximates persisted state (`fold` over `'static`, `DeferTick` cycles),
and a tick that batches several unrelated requests attributes all of them to each output. Both are
sound. Keyed hooks (`KeyedBatchHook`, `KeyedSnapshotHook`) release per key, so for keyed ticks the
closure can be taken per key at no extra cost; that recovers most of the precision that sharded or
per-client state would otherwise lose.

Precision relative to Molly: Molly's provenance for an aggregate is *all* contributing tuples, the
same as this level's per-execution rule. Hydro programs are fold/reduce/join heavy (quorum counts,
log folds, `cross_singleton` with snapshots), so for the derivations that matter in consensus and
replication code, Level 1 is close to what Molly would report. Redundancy that Level 1 cannot see
inside one execution (a quorum fold over three acks, where any two suffice) is discovered by
iteration, exactly as Molly discovers it through an aggregate.

### Level 2: record-level provenance via IR rewrite (deferred)

A pass in the `SimFlow::compiled` pipeline (next to `splice_versioned_networks`) that rewrites every
collection to carry `(T, Prov)` with per-node rules: `Map`/`Filter`/`FlatMap` wrap `f`; `Join`,
`CrossProduct`, `CrossSingleton` union; `Fold`/`Reduce`/`Scan` union over all inputs; keyed variants
per key; `Unique` becomes an or-provenance fold; `Difference`/`AntiJoin` strip the negative side;
`Network` serializes `Prov` (serde, already bincode); `ResolveFutures*` wrap the future. About
forty-five node kinds, each a `syn` closure-wrapping rule plus a `CollectionKind` adjustment. It is
the same kind of surgery as the versioned-network splice, so it is feasible, but it changes the
element types of every operator and every user closure's calling convention in the sim build, and it
buys precision only where Level 1 is coarse: non-aggregate joins and filters inside a tick that
processes several independent requests. Decide after seeing Level 1 on real examples.

## Composition with the fuzzer (the new thing)

Molly fixed the schedule. Here a hypothesis run is `flow.sim()...fuzz(...)` or `exhaustive(...)`
with the `ChannelHook` policies installed: the fuzzer explores batchings and orderings *under the
injected faults*, and the invariant is checked in every explored schedule. Two consequences:

- A counterexample is a (fault set, schedule) pair, and the reproducer is the existing bolero
  bytes plus the policy list. `fuzz_repro` already replays the bytes; the policy list is plain data.
- Lineage is per schedule. The CNF for "break the goal in some schedule" should be built per
  schedule (a hypothesis need only kill every derivation of *one* schedule), with the hypothesis
  then re-run under fuzzing to find such a schedule. A simple v1: collect derivations from N
  failure-free fuzz instances, solve each schedule's formula separately, dedupe hypotheses.

Fragility to note: bolero entropy is consumed by hook decisions, so an injected omission that
changes buffer contents shifts downstream decisions and the same bytes no longer mean the same
schedule. For attribution we pin with `deterministic`-style scripting of the hooks that matter, or
accept that the hypothesis run explores afresh. Channel policies never consume entropy, so the
failure-free run and its hypothesis runs at least start from the same prefix.

## EOT / EFF in this setting

- `EOT`: the number of external timer rounds the harness fires (timers are `sim_input`s) plus a
  final `quiesce()`.
- `EFF`: after round `EFF`, the harness calls `deliver_rest()` on every channel hook so no new
  omissions occur; dropped messages stay dropped. Eventual invariants are checked after the final
  quiesce. This is the piece `lossy_delayed_forever` + `test_safety_only` explicitly cannot do
  today (networking docs: "cannot test liveness"), and it falls out of `omit` being a real drop.

## Harness sketch

```rust
let mut flow = FlowBuilder::new();
let ch_vote: ChannelHook<(MemberId<Acceptor>, Vote), Lossy, OnCluster<Proposer>> = flow.sim_hook();
let ch_p2a:  ChannelHook<P2a, Lossy, OnCluster<Acceptor>> = flow.sim_hook();
/* build program with TCP.lossy(nondet!(hook = ch_vote)), ... */

flow.sim()
    .ldfi(Ldfi { eot: 4, eff: 2, crashes: 1 })
    .run(async |faults: &FaultSet| {
        faults.install();             // applies omit/omit_from/cut_after to the ChannelHooks
        for round in 0..4 {
            timers.send_many(...);
            if round == 2 { faults.end_of_finite_failures(); }
            hydro_lang::sim::quiesce().await;
        }
        let committed: Vec<_> = out.collect_with_lineage().await;   // (value, ExecId)
        ldfi::goal(committed.iter().all(agree))        // records the outcome and its support
    });
```

`.ldfi(..).run` drives the loop: failure-free run, lineage from the trace, CNF, SAT or minimal
hitting set (formulas are tiny; a few hundred clauses), re-run, until UNSAT or a failing run, which
it reports as `(FaultSet, bolero bytes)` and writes beside the existing `sim-failures`
reproducers.

## Touch points, in dependency order

1. `sim_hooks.rs`: `ChannelHook<T, F, S>` handle; `SimHook` impl with the serde/PartialEq bounds.
2. `networking/mod.rs`: `lossy(nondet)` accepts a hook payload (`NonDet<Option<ChannelHook<..>>>`
   via the existing `NonDet<H>` mechanism); `lossy_delayed_forever()`/`fail_stop()` get
   `.sim_hook(h)`; the hook id lands in `HydroNode::Network`'s existing
   `metadata.op.sim_hook_id`, so `create_network` needs to receive `op_meta` (today it gets only
   `networking_info` and `tag_id`).
3. `sim/runtime/channel.rs` (new): `ChannelPolicyHook<T>` (buffer, policy, delivered trace) and
   `Policy<H>` shell; register in `ScriptedHookRegistry`; add a `channel_hooks` list to
   `DylibResult`.
4. `sim/builder.rs::create_network`: interpose the hook at the receive half for all four
   location-pair cases; lift the `todo!` on `Lossy`; keep `test_safety_only` required unless an
   LDFI config with `eff` is present.
5. `sim/compiled.rs`: `ExecId` cell, tag sends/outputs, a structured `SimTrace` sink next to the
   release log (releases, deliveries, executions), exposed to the test body.
6. `sim/ldfi/` (new, or a separate crate): lineage graph from `SimTrace`, CNF, solver, loop,
   reproducer output.
7. Later: Level 2 IR rewrite in `SimFlow::compiled`.

## Resolved: the fault vocabulary follows the transport type

The decision set of a `ChannelHook` is fixed by the channel's fail-policy type: `FailStop` admits
only `cut_after` (connection death) and crashes; `Lossy` / `LossyDelayedForever` admit arbitrary
omission. The type encodes the program's assumptions about its environment, and LDFI's job is to
find where redundancy falls short *within* those assumptions, not to change them. A program that
should survive omissions declares `lossy` and takes the `NoOrder` consequences downstream; that
pressure is the point of the typing. Two consequences: under `fail_stop` the hypothesis space is
small (prefix cuts per sender/receiver pair, plus crashes), so the loop will often reach UNSAT and
produce a real certificate for the declared model; and a counterexample can never be dismissed
as outside the assumptions.

## Open questions

- **Goal expression.** Molly's invariants are rules; here they are assertions in the test body.
  `collect_with_lineage` plus `ldfi::goal(bool)` attributes the outcome to the outputs the test
  read, which over-approximates (all read outputs support the goal). A `Hydro`-side invariant stream
  (`assert_is_consistent` is a precedent, currently skipped in sim) would give exact goal lineage.
- **Snapshots and state.** A `snapshot` reveals one buffered version; lineage of a tick that read
  version `v` should include the executions that produced versions `≤ v`, not all. `SnapshotHook`
  knows the revealed version; wire that into the Level 1 edge rule.
- **Clusters that change size** (`next_version` chains) and `VersionedNetwork`: the splice already
  rewrites `Network` nodes; the `ChannelHook` must be attached after the splice.
- **At-least-once streams**: `batch` on `AtLeastOnce` is unimplemented in the sim; duplication as a
  fault is out of scope until then.

## First experiment

`hydro_test`: the paper's three delivery protocols (`simple-deliv`, `retry-deliv`, `redun-deliv`)
as a client, a broadcaster, and a 3-member replica cluster over `TCP.lossy(...)`, timers as
`sim_input`. Expected: Level 0 finds the single-omission bug in `simple-deliv` in one hypothesis;
`retry-deliv` needs the crash budget; `redun-deliv` reaches UNSAT with `crashes: 0`. Then Raft from
`hydro_test/src/cluster/raft.rs` with `eot` = 3 election rounds, which is where the comparison with
Molly-era results gets interesting.

## Prototype status

A first cut exists on this branch (feature `ldfi` of `hydro_lang`, on top of `sim`), validated on
the paper's first three examples. It follows the staging above with these deviations.

**Fault points.** Instead of scripting `release_values` + `pause()` from the harness (which
would have needed the harness to predict arrivals), *policy semantics* were added to the
existing scripted hook shell (`ScriptableHook::Policy`, `policy_sweep`, `apply_policy` in
`sim/runtime/scripted.rs`): a hook under a standing policy draws no entropy, is never forgotten,
triggers its tick exactly when something deliverable is buffered, and traces deliveries and
discards. `NoOrder` batch hooks take an `OmissionPolicy` (`omit` / `deliver_all`, a real drop,
applied on arrival); `TotalOrder` batch hooks take only the traced `PassthroughPolicy`, since a
reliable channel admits no omission. That is the `Policy<H>` shell of §"Proposal", hosted inside
`Scripted<H>` and bound at the consumer rather than at the `Network` node.

**Crashes** are implemented at the *send half*, in generated code (`sim/runtime/lineage.rs`,
`create_network`): `crash(node after sending m)` (or `before any send`) lets the node send up
to and including `m`; later sends are recorded and dropped, and once dead the node's
`sim_output`s are suppressed. This is the paper's definition, it works over `fail_stop`, and
it splits the fan-out of one broadcast. The crash time is named by a message, not a send count,
for the reason given under "Two soundness problems" below (the first cut used counts).

**Lineage** is Level 1. Level 0 was built first and discarded: on reliable delivery it put the
acks into every support although they contribute to no derivation. Now generated code records
`Sent { exec, from, to, bytes, dropped }` at every network send and `Output { exec, port, bytes,
dropped }` at every `sim_output`; the scheduler assigns an `ExecId` per async run and tick
execution and records `Released { exec, hook, items }` from each policy hook's trace. Releases
are matched to sends one-to-one in event order (earliest unmatched send on a channel feeding
the hook, to the same node, with the same bytes; see "static slices" below): a pure byte match
attributed 2PC's `commit` to the `prepare` of the same transaction and produced a false
certificate. The support of a goal output is the backward
closure (tick ← released messages ← sending execution; tick ← earlier executions of the same
tick; async run ← ticks at its location since its previous run; both refined by the static
slices below). Dependencies carry the sender
and its send ordinal, so a crash literal kills every later send. Goals are existential (`goal`)
or a conjunction over the correct members of a cluster (`goal_all_members`), and the body
decides the invariant itself with `run.crashed(..)`. Channels through unregistered paths are
invisible to the closure, so every channel a goal can depend on must be registered.

**Hypotheses** are minimal hitting sets, solved as an integer program with HiGHS (`good_lp`,
built from source: needs `cmake` and a C++ toolchain, and `libclang` for the bindings). A binary
per candidate fault; minimize a weighted count whose tie-break prefers weaker faults (a crash
after more sends), so a hypothesis leaves as much of the protocol running as a minimum hitting
set allows; one cover constraint per derivation under the `kills` relation; the crash budget and
one crash per node as constraints; a no-good per tried set. Infeasible is the certificate. The
MHS/ILP formulation is this document's, not the paper's (which hands the negated lineage, as
CNF, to a SAT solver).

**Results.**

- Reliable delivery (`hydro_test/src/distributed/reliable_delivery.rs`; "if a message is sent and
  not all messages are dropped, it is eventually delivered"; goal at the receiver; EOT 4, EFF 2):
  tries-twice → `{}` ⊢ `{msg_1}`, `{omit msg_1}` ⊢ `{msg_2}`, `{omit msg_1, omit msg_2}`
  violated; retry-until-ack → same two runs, then support `{}` → certificate. Three runs each.
- Broadcast (`hydro_test/src/cluster/reliable_broadcast.rs`; the echo RB from the
  `eventual_consistency` branch ported to a per-member tick; agreement over correct members;
  reliable links, 3 members): plain broadcast, budget 1 → `crash(b after 2)` violated on run 2.
  Reliable broadcast, budget 1 → certificate in 4 runs; budget 2 → certificate in 13.
- Two-phase commit (`hydro_test/src/cluster/two_pc_ldfi.rs`; agreement: a commit anywhere, even
  on a node that later dies, means every correct participant committed; 2 participants):
  plain 2PC, budget 1 → `crash(coordinator after 3)` (both prepares and one commit) violated on
  run 2. With cooperative termination (uncertain participants query peers on a timer), budget 1
  → certificate in 7 runs; budget 2 → `{crash(coordinator after 3), crash(participant@0 after
  2)}` on run 8: the coordinator dies after one commit, the participant that got it dies before
  its answer reaches its peer, and the transaction is committed only on a dead node.

**Against lineage-blind search** (`compare_search_strategies` in `two_pc_ldfi.rs`). Same fault
space, same simulator, same goal. *ldfi/exhaustive* explores every schedule per hypothesis (its
certificate is a proof); *ldfi/fuzz 4* samples four schedules per hypothesis
(`Exploration::Fuzz`, certificate "not a proof"); *blind* is the same loop with the cover
constraints removed (every untried admissible fault set over the observed universe, by weight);
*fuzz* draws a crash set per instance from the simulator's own entropy (uniform under
`cargo test`, 2000 instances, crash points bounded at 6; libFuzzer-guided under `cargo sim`, not
measured). "runs" are hypotheses; "sched"/"inst" are simulator instances, the comparable cost.

```
protocol    n cr | ldfi/exhaustive runs  sched | ldfi/fuzz4  runs sched | blind      runs  sched | fuzz   inst
2pc         2  1 | violated         2      24 | violated      2     8 | violated     2     24 | violated   7
2pc + ctp   2  1 | certificate      7     126 | certificate   7    28 | exhausted   13    222 | none    2000
2pc + ctp   2  2 | violated         8     150 | violated      8    32 | violated    14    246 | violated  59
2pc         3  1 | violated         2    1152 | violated      2     8 | violated     2   1152 | violated   2
2pc + ctp   3  1 | certificate     10  139764 | certificate  10    40 | exhausted   34 160500 | none    2000
2pc + ctp   3  2 | violated        13  209652 | violated     13    52 | cancelled: >37 runs, ~10 min | violated 33
```

(The exhaustive and blind cells of the last two rows were measured once, outside the default
test: the budget-2 LDFI run took 5m15s; `ldfi_ctp_three_participants_two_crashes` reproduces
it.) Reading it straight: at equal cost in instances, the random fuzzer is competitive with
sampled LDFI on these shallow bugs (7 vs 8, 59 vs 32, 2 vs 8, 33 vs 52, and its numbers swing
2–3× between seeds); blind enumeration ties LDFI wherever one crash suffices, because the
weakest-first order alone finds it. Lineage pays for proving absence (7 vs 13, 10 vs 34 hypotheses,
and the fuzzer cannot certify) and for the explanation attached to every verdict. Whether it
also wins instances-to-bug is a question for fault spaces where the violating combination is
rare (the Kafka-style replication bug), not these. Under `exhaustive` the dominant cost is
schedules per hypothesis, paid identically by every searcher; `Exploration::Fuzz` trades the
proof for tractability (row 6: 52 instances instead of 209,652).

**ISR replication** (`hydro_test/src/cluster/isr_replication.rs`; the paper's Kafka bug; 3
replicas, ZK, one write, four timer rounds; omission on the leader→follower channel in round 1,
replicas crashable; goal "every acknowledged write is in the final leader's log", with the
post-condition's witnesses named by the body: the final view and the leader's log entry).
Exhaustive exploration is out of reach (>45k schedules for the failure-free run), so everything
samples 8 schedules per hypothesis; the fuzzer draws from the same vocabulary (observed lossy
messages plus crashes).

```
protocol     cr | ldfi/fuzz 8  runs sched | blind/fuzz 8  runs sched | fuzz   inst
isr unclean   1 | violated      19   152 | violated       25   200 | violated  32
isr clean     1 | certificate   42   336 | exhausted      88   704 | none    704
```

LDFI finds the paper's scenario, `{omit(repl 0→1), crash(replica@0 after 7 sends)}`: follower 1
misses the replication and is shrunk out of the ISR, the leader acknowledges on the shrunken ISR,
dies after its last heartbeat, and ZK promotes the stale follower. Clean election certifies.
But random search with the same vocabulary wins on instances (32 vs 152): the fault space is
still small enough to hit by chance. Two lessons. (1) A run in which the pre-condition fails
(nothing acknowledged) must contribute *no* derivation; registering the final view as a witness
anyway produced a false certificate until the body was fixed. (2) The "earlier executions of
the same tick" rule is costing precision here: every replica's final heartbeat inherits that
replica's whole history, so every single crash is a hitting set and 17 of LDFI's 19 hypotheses
are singletons. Carrying only the state an execution actually read is the next lineage
precision item.

**Lineage precision: static slices** (`hydro_lang/src/sim/slice.rs`). The coarseness
above ("every output of a tick execution depends on every input of it and of every earlier
execution of the same tick") is replaced by a per-sink backward slice of the Hydro IR, computed
once per compiled program with the same traversal (and therefore the same statement ids) that
emits the simulator. For each network send and each `sim_output`, the slice walks upstream
through the top-level send path, the `YieldConcat` into the tick, and the tick's operators,
and collects the hook-bound batches it reaches: `direct` (reached without crossing a
`defer_tick`: deliveries in the same execution) and `carried` (through at least one
`defer_tick`: deliveries in earlier executions of the same tick). `forward_ref` cycles are
resolved to a fixpoint; in-tick cycles without `defer_tick` stay `direct`. Values read through
`Reference` captures in closures (the `use::state` / singleton-ref style) add the slice of
every closure that captures the same cell mutably, as both `direct` and `carried`, since the
writes may have run in this or any earlier execution. A sink whose slice reaches an unscripted
batch or a multi-version network is `opaque`, and the closure falls back to the old rule for
it. `Sent` and `Output` events now carry the sink id, releases are matched to sends on the
channel(s) the slice says feed the hook (not merely destination and bytes), and every receive
half records a `Recv { exec, channel }` so that top-level consumers appear in the closure
(arrivals on one channel at one node are in send order, so the k-th receive is the k-th
non-dropped send). `HYDRO_SLICE_DEBUG=1` prints the slices and the IR upstream of each sink;
the LDFI log prints the resolved slices once per search.

Two soundness bugs surfaced while making the slice *complete* (an incomplete slice drops
dependencies and manufactures certificates), and both affected the old lineage silently:

1. A hook fed through a top-level `forward_ref` (2PC's peer channel is declared before the
   tick that produces it) had no channel attribution; with channel-scoped matching its
   deliveries went unmatched and the CTP two-crash counterexample became a false certificate
   until the hook→channel map resolved cycles. (Under the old destination-and-bytes matching
   the same deliveries were attributed by luck of payload shape.)
2. ISR's `replica_step` reads and mutates its state through `Reference` captures that the IR
   child walk does not see; the slice came back empty, every derivation was `{}`, and the
   search certified the known-buggy unclean election. Following `ClosureExpr::singleton_refs`
   and adding writers' inputs fixed it.

With the sound version the 2PC comparison table above reproduces to the run, with one change:
2PC+CTP, n=3, two crashes, under `ldfi/fuzz 4`, 12 hypotheses instead of 13. Derivations are
tighter (2PC's decision no longer carries the `timeout` input; the ack channel never enters
reliable delivery's support), but the solver's path does not change: on 2PC the next hitting
set was already determined by the crash points. ISR as originally written was unchanged too
(19 / 42), and for a reason that is the real finding of this round: it was written as
`replica_step` / `zk_step`, one closure per tick over a state struct, the same style as
`raft.rs`. To a dataflow slice that closure is one node, so every output depends on every
input across all executions, which *is* the old rule. Slicing cannot help a program that
hides its dataflow in a closure.

**Encoding experiment.** ISR was therefore rewritten in dataflow style, keeping the closure
version as a control (`Encoding::{Closure, Dataflow}` in `isr_replication.rs`): one stream
per message type, state as `use::state` cycles, and a separate output path per kind of
message a replica sends (heartbeats and shrink requests go to ZK on separate channels; the
closure version multiplexes them on one). Same goal body, same registered channels otherwise,
same fault model, same sampling. Resulting slices: replicate ← {write, view}; acks ← {repl};
heartbeat ← {timer}; shrink ← {write, repl, ack, view, timer}; client ack ← {write, repl,
ack, view}; log ← {write, repl, view}.

```
protocol     encoding  cr | ldfi/fuzz 8  runs sched | blind/fuzz 8  runs sched | fuzz   inst
isr unclean  closure    1 | violated      19   152  | violated       25   200  | violated  31
isr unclean  dataflow   1 | violated      15   120  | violated       25   200  | violated  10
isr clean    closure    1 | certificate   42   336  | exhausted      88   704  | none     704
isr clean    dataflow   1 | certificate   28   224  | exhausted      88   704  | none     704
```

Encoding moves the LDFI numbers (19 → 15 hypotheses to the bug, 42 → 28 to the certificate),
blind is unaffected as it should be, and against blind the lineage is worth 3× on the
certificate. Random fuzz still reaches the bug in fewer instances (10 here, 31–32 in earlier
runs; it swings with the seed): the fault space is small enough to hit by chance. What keeps
LDFI at 15 is visible in the derivations and is the protocol, not the lineage: the final-view
witness depends on every heartbeat, a crash kills later heartbeats, so every single crash is a
size-1 hitting set and weakest-first order tries all 13 of them before any pair. Lesson (2)
above is therefore closed with a corrected diagnosis: the singleton-crash hypotheses were
partly coarse lineage (fixed by slicing, once the program exposes its dataflow) and partly the
goal's dependence on liveness signals, which no lineage precision removes.

Summary of the slicing round: slicing is a correctness requirement for Level 1 lineage (two
false certificates found); it improves explanations everywhere; it improves search only when
the protocol is written as dataflow. Closure-style protocols (Raft as written) are out of
reach for Level 1 lineage regardless; making them visible is Level 2 work.

**Measurement grid** (`hydro_test/src/cluster/ldfi_grid.rs`, `#[ignore]`d; ~30 min). Three
things the tables above got wrong or left out, fixed here.

1. *Accounting.* Those tables charged LDFI 4–8 sampled schedules per hypothesis and random one
   instance per fault set. Every hook is under a policy, so a schedule only varies tick
   interleaving; at one schedule per hypothesis (`ldfi/1`) every verdict and counterexample in
   the grid matches the eight-schedule run (`ldfi/8`), so the fair LDFI cost is one instance
   per hypothesis. On ISR the hypothesis sequence is schedule-independent (zero spread over
   repeats); on 2PC the schedule decides which derivations are observed and the count varies
   (n=5: 20..106 over five repeats), so `ldfi/1` is reported as median [min..max].
2. *Scale.* 2PC+CTP at n ∈ {2..6} participants (crash points grow linearly, crash pairs
   quadratically; the fault space is crashes only), ISR at m ∈ {3,5,7} replicas × r ∈ {4,8}
   rounds. Workload stays at one, so slices stay precise; that is the half of the paper's
   regime this substrate can test.
3. *Baselines with spread.* Blind enumeration at one schedule per hypothesis. Random with a
   controlled draw: `bern` (each omittable message w.p. ½, each node w.p. ½ in random visit
   order while the budget allows; about 1.5 faults per instance) and `k` (up to k faults
   uniformly from the vocabulary). 10 repeats for 2PC (budget 2000 instances), 5 for ISR
   (budget 1000). A first version of `bern` visited nodes in registration order, which spent
   the crash budget on the coordinator / replica 0 with probability ½ on every instance;
   those were the critical nodes in both protocols, so that baseline was lucky, not
   uninformed, and was rerun with random visit order. "vac" is the fraction of random
   instances whose pre-condition failed (nothing committed / acknowledged): the masking rate.

```
2PC+CTP, two crashes, instances to first violation (median [min..max] over repeats; misses = repeats with none in 2000)
n | ldfi/1          | blind/1 | random bern             | random k2                | random k3                | vac
2 |   8 [8..8]      |    14   |  38 [3..427]    0 miss  |  24 [6..87]     0 miss   |  54 [2..120]    0 miss   | 42%
3 |  18 [11..21]    |    48   | 165 [10..301]   0       | 100 [5..840]    0        |  95 [2..697]    0        | 39%
4 |  21 [18..27]    |    96   | 204 [14..798]   0       | 106 [27..981]   0        | 353 [24..650]   0        | 33%
5 |  82 [20..106]   |   168   | 496 [17..1693]  0       | 316 [13..1009]  2        | 624 [149..1818] 2        | 28%
6 | 119 [72..179]   |   275   | 103 [41..1722]  2       | 374 [90..1361]  4        | 718 [94..1097]  4        | 25%

2PC+CTP, one crash (certificate): ldfi/1 7, 10, 13, 16, 19 hypotheses (3n+1); blind exhausted at 13, 25, 37, 81, 139;
random: no violation in 2000 instances at any n (it cannot certify).

ISR unclean election, budget 1 (omission + crash)
config             | ldfi/1 | blind/1 | random bern     | random k2        | vac
closure  m=3 r=4   |   19   |   25    |   3 [1..37]     |  63 [12..153]    | 6%
dataflow m=3 r=4   |   15   |   25    |  16 [5..78]     |  61 [1..82]      | 5%
dataflow m=5 r=4   |   23   |   42    |   4 [1..9]      | 104 [40..185]    | 4%
dataflow m=3 r=8   |   27   |   37    |  17 [5..31]     |  33 [12..190]    | 2%
dataflow m=5 r=8   |   43   |   63    |  25 [13..74]    |  62 [3..77]      | 1%
dataflow m=7 r=4   |   31   |   62    |  50 [29..309]   | 219 [117..796]   | 6%
ISR clean (certificate): dataflow m=3 r=4 ldfi 28, blind exhausted 88; m=5 r=4 ldfi 94, blind over budget (400); random none.
```

Reading it straight.

- *2PC.* With fair accounting and a growing fault space, this is the regime the paper
  describes. LDFI's instances to the bug grow slowly (8 → 119) and it never misses;
  uninformed random's median grows from tens to hundreds, its range reaches the budget, and
  from n=5 it misses within 2000 instances in 2–4 of 10 repeats. Blind enumeration separates
  the lineage from the weakest-first order: it tracks LDFI at n=2 (14 vs 8) and is 2–3× worse
  by n=5–6, and on certificates it is 2–7× worse and exhausts the budget first on ISR. The one
  cell where random's median beats LDFI (n=6 `bern`, 103 vs 119) comes with two misses and a
  range of 41..1722; the distributions overlap, LDFI's does not reach the budget.
- *ISR.* Random is competitive, and `bern` wins at small sizes, because the fault space never
  grows: the omittable vocabulary is the leader's replication fan-out (2–6 messages) and the
  nodes are 3–7, so a 1.5-fault draw hits a 2-fault bug in a handful of instances. Uniform
  `k2` random loses at every size but one and the gap widens with m (219 vs 31 at m=7). LDFI's
  cost grows with the heartbeat count, for the reason given above (every heartbeat is a
  dependency of the final view).
- *Certificates* remain LDFI-only, and cheap: 3n+1 hypotheses on 2PC.
- *Masking.* A quarter to a half of random's 2PC instances kill the coordinator before any
  commit and test nothing; the rate falls with n as the coordinator becomes a smaller share
  of the vocabulary. This is the mechanism behind random's advantage on shallow bugs (many
  faults per instance, each instance covering many singleton hypotheses) and behind its
  decay as the fault space grows.

**Level 2: record-level provenance** (`hydro_lang/src/sim/provenance.rs`,
`sim/runtime/provenance.rs`; `SimFlow::with_provenance()`, `Ldfi::level(Level::Record)`).
A sim-only IR rewrite wraps every collection element in `Tagged<T>`: the value plus the set of
*delivery ids* it was computed from. `Tagged<T>` compares, hashes, orders, prints, and
serializes as `T`, so deduplication, joins, set difference and hook policies are unchanged.
Each item a scripted batch releases gets a fresh id (`Tag` event); `map`/`filter` pass the set
through, `join`/`cross` union both sides, `fold`/`reduce`/`scan` accumulate every
contribution, cycles carry it; a network send or `sim_output` strips the set from the wire
payload and records it in the `Sent`/`Output` event. Keyed collections carry `(K, Tagged<V>)`
so keyed DFIR operators still see the key; adapters are inserted where a `Cast` changes
representation or a DFIR operator destructures pairs. The LDFI closure then follows
record-to-record edges (delivery id → the send that produced it → that record's ids) and
falls back to Level 1 where a record carries no provenance (a top-level path with no hook).
Supported for the operators the examples use (~26 variants); anything else panics at compile
time by name. `Reference` captures (the closure encoding) are not supported, by design: a
closure's reads would have to inherit the whole cell's provenance, which is Level 1 again.

**Two soundness problems the ablation surfaced**, both in the fault model rather than the
lineage, both fixed before the numbers below.

1. *Crash points must be named by messages, not send counts.* The first ablation pass gave
   false certificates for Levels 0 and 1 at load 2 (the bug exists; blind and Level 2 found
   it). The coarser lineage put a background transaction's vote into the derivation, the
   search tried `crash(p0 after 1 send)`, and in that world the coordinator never decided the
   background transaction, so its send sequence shifted: `coordinator#7` meant a different
   message. The clause observed there constrained the solver with literals that mean
   something else in the bug's world; the bug set stopped being a hitting set and was never
   proposed. Molly's crash literals are indexed by logical time, which is stable; a send count
   is not. Crashes are now `crash(node after sending m)` (kills every send after `m`; if `m`
   is never sent the node survives) or `crash(node before any send)`, and a dependency's
   killers are the crashes after any message preceding it *in that run's send sequence*
   (`Clause` = derivation + the run's `SendSequences`). With this, Levels 0/1 find the load-2
   bug (21–22 hypotheses) instead of certifying after 260.
2. *The pre-condition's lineage must widen the universe.* With message-indexed crashes and a
   universe of "crash right before a dependency send", Level 2 certified ISR at load 2 while
   Levels 0/1 found the bug: the bug needs the leader to die *between* acknowledging write 1
   and acknowledging write 2, two sends that are not in the goal's lineage (the goal is
   conditioned on the acknowledgement, not derived from it), so under precise lineage neither
   crash point was a candidate; the coarse lineage had them by accident. Enlarging the universe
   to every observed send fixes it but erases most of Level 2's advantage (the number of
   singleton hypotheses becomes the number of sends, independent of lineage: 2PC certificate
   at load 8 drops from 73 to 46 instead of to 11). The principled fix is Molly's: the body
   names the outputs the goal is *conditioned on* (`Run::require`), and the crash right after
   each send in a requirement's lineage, the weakest crash that preserves the pre-condition,
   joins the universe. Universe = clause candidates ∪ preserve points. Two smaller gaps
   went with it: a receive on a channel with no fault point (the client's acknowledgement)
   now yields a dependency on its send, killable by the sender's crash; and a record with no
   provenance at the root of a Level 2 closure (an output on a hook-free path) falls back to
   the slice-level closure instead of deriving from nothing, which had silently emptied the
   pre-condition's lineage.

**Ablation** (`ldfi_ablation` in `ldfi_grid.rs`): Level 0 (execution), Level 1 (static
slice), Level 2 (record provenance) on the same provenance-compiled program, one schedule per
hypothesis, five repeats (median [min..max]); blind and uniform random k2 for reference. The
workload axis adds background transactions / writes; the goal is checked on the first.

```
2PC+CTP n=3, hypotheses to certificate (cr=1) / to the two-crash bug (cr=2); L* median [min..max] of 5
load | L0/exec        L1/slice        L2/record           | blind     | random k2 (10×2000)
  1  | 10 / 15        10 / 16         10 / 13 [12..15]    |  27 /  36 | none
  2  | 19 / 22        19 / 22         11 / 15 [13..15]    |  50 /  60 | none
  4  | 37 / 38        37 / 39         11 / 16 [14..17]    | 102 / 180 | none
  8  | 73 / 74        73 / 74         11 / 13 [13..17]    | 201 / 364 | none

ISR dataflow m=3 r=4, budget 1: certificate (clean) / bug (unclean)
load | L0/exec        L1/slice        L2/record           | blind     | random k2 (5×1000)
  1  |  18 / 10        18 / 10         18 / 10            |  72 /  23 | 71 [18..237]
  2  |  87 / 14        87 / 14         24 / 11            | 384 /  34 | 410 [82..853]
  4  | >400 / 18      >400 / 18        36 / 13            |>400 /  67 | 287 [123..370], 1 miss
```

Level 2 is load-invariant on 2PC (10–11 hypotheses to certify, 13–16 to the bug at every
load) and nearly so on ISR (18 → 36 to certify, 10 → 13 to the bug). Levels 0 and 1 grow
linearly on 2PC, about nine hypotheses per background transaction (each adds the
coordinator's sends for it as crash candidates), and worse than linearly on ISR's certificate
(18 → 87 → past the 400-hypothesis budget); they coincide throughout: the slice cannot
separate records that share a hook, only record provenance can. Blind grows faster still, and
uniform random never finds the 2PC pair within 2000 instances once the crash vocabulary is
message-indexed (the pair it needs is one of hundreds); on ISR it still finds the bug, with a
median that moves from 71 to 287–410 instances and a miss at load 4, and it cannot certify.
At load 8 on 2PC, Level 2 is 6.6× cheaper than Level 0/1 on the certificate, 5.7× on the bug,
and 18–28× cheaper than blind; at load 4 on ISR it certifies where every other searcher ran
out of budget.

The hypothesis the ablation tests, *finer-grained provenance ⇒ fewer executions to find bugs
and to certify*, holds on this corpus with one precise boundary: it holds exactly when there
are several records in flight per hook. At workload one the three levels are identical, and
the earlier rounds' "no effect" was this boundary, not a property of the method. The gain is
larger on certificates than on bugs (a certificate must exhaust the universe, which coarse
lineage inflates; a bug is found as soon as the right pair is proposed, and weakest-first
order gets there in a dozen steps either way at these sizes).

So the earlier "random wins instances-to-bug" conclusion was an artifact of three things:
charging LDFI for schedules that did not change its verdicts, a random baseline whose node
order happened to favor the bug, and fault spaces too small for any search to matter. With
those corrected, lineage-guided search beats uninformed random on 2PC at every size, by a
margin that grows with size, and the honest statement about ISR is that it is not yet a
haystack. What remains untested is workload (many concurrent records), which is where static
slices degrade toward blind and record-level lineage would be the operative variable.

Message identity is `(channel, payload)`; every copy of an omitted payload is dropped while the
policy stands. Not done: the `Network`-node fault point (fan-out, hook-free paths),
`cut_after`, reproducer output, occurrence counting, a libFuzzer-guided measurement of the fuzz
baseline under `cargo sim`, `Reference` captures under the provenance rewrite (Level 2 for the
closure encoding), and workloads beyond eight records.

**Where this leaves the question.** Commit `ldfi-level2` is the checkpoint for everything
above. What it establishes: on a general-purpose dataflow substrate, lineage-driven fault
injection beats blind enumeration everywhere and uninformed random on every configuration that
has a fault space worth searching; record-level provenance is what keeps its cost flat as the
workload grows, and the static slice is not a substitute; certificates are the method's
distinctive product and the place where precision pays most; and the fault vocabulary has to
be constructed with care (message-indexed crash points, pre-condition lineage) or the solver's
constraints lie, which three false certificates demonstrated along the way. What it does not
establish: anything about protocols written as step closures over a state struct (Raft as
written, ISR's control encoding), which Level 2 cannot see into; scaling beyond a few hundred
fault points and eight records; and the comparison against a coverage-guided fuzzer. The
closure problem is the next fork: either make the provenance rewrite see through `Reference`
captures (an open design question, likely a longshot) or accept dataflow style as the price of
lineage.

## Appendix: comparison with the earlier proposal (`f32054f9`, `docs/design/lfdi-on-sim-hooks.md`)

Read after the above was written. Where the two agree, I omit it (both: a channel-level drop
hook is the one real substrate gap; lineage is observation, not a decision; over-approximate
lineage is sound; reuse the registry and bincode-across-the-dylib plumbing; stage the work).
The differences that matter:

1. **What counts as a fault.** The earlier doc calls orderings and timings "the easy 80%" of
   LDFI cuts and makes "ordering-cut front-end" its stage 1. LDFI's fault vocabulary is omission
   and crash; orderings are schedule non-determinism, which Molly deliberately fixed and which
   the Hydro fuzzer already explores. Emitting ordering scripts is not LDFI, and the solver has
   nothing to say about them. The useful relationship is composition (§"Composition with the
   fuzzer"): a hypothesis is a fault set, and the fuzzer searches schedules under it.

2. **Policy vs. schedule semantics for the channel hook.** The earlier doc has the
   `ChannelHook` "reuse the script-as-schedule group model verbatim"
   (`net.deliver(a).await; net.drop(b).await`). Under that model every delivery on a hooked
   channel must be scripted or the boundary scan reports a forgotten hook, and the harness must
   know arrival order. An LDFI run wants a standing filter with deliver-all as the default and no
   entropy. That is why I propose a separate `Policy<H>` shell rather than `Scripted<H>`.

3. **Crash.** The earlier doc proposes a `CrashHook` that flips a member's async dataflows dead,
   fired `.at_quiescence()`. Two issues: crash timing at phase barriers is too coarse (2PC's
   blocking case is "crashed after sending prepare to A, before B", which is between two sends
   in one async run); and it needs new scheduler state. Omission-of-all-sends-after-the-k-th
   at the channel hooks is Molly's definition, needs no scheduler change, and can split sends
   inside one run. The one thing the dead-flag version does better is silencing the crashed
   node's own external outputs; the harness can filter those by `ExecId` instead.

4. **Fault model from the transport type.** Not addressed there. `FailStop` channels admit only
   prefix cuts; `Lossy*` admit arbitrary omission. Gating the decision set on the fail-policy
   type parameter makes the program's declared fault model the one LDFI tests, and gives the
   simulator its missing implementation of plain `lossy` (`builder.rs:1941`) as a side effect.

5. **How to get lineage cheaply.** The earlier doc's "coarse" tap is a provenance set riding on
   every value, unioned at every operator. That *is* the IR rewrite (my Level 2) with the
   weakest rule; it changes every element type and closure calling convention from day one,
   and the doc underestimates it as "a one-line codegen addition". It misses that the hooks
   already partition execution into units with known inputs: hook releases are lineage edges,
   and tagging messages with the producing `ExecId` closes the graph (Level 1) without touching
   a single user closure. That is the actual sense in which the hooks are a lineage substrate.

6. **Precision expectations.** The earlier doc treats per-operator precision as the main future
   effort. Molly's aggregate rule is "all inputs", the same as execution-level attribution, so
   for fold/quorum-heavy protocols Level 1 is already at Molly's precision; keyed hooks add
   per-key precision for free. Level 2 pays only for non-aggregate joins and filters inside
   ticks that process several independent requests.

7. **Not covered there at all**: message identity across runs (occurrence counting replaces the
   clock), EOT/EFF (timers are `sim_input`s; `deliver_rest()` at EFF; eventual properties
   become checkable, which `lossy_delayed_forever` + `test_safety_only` cannot do today),
   per-schedule formulas when the hypothesis run is fuzzed, snapshot-version edges, and the
   entropy-shift fragility of replaying bolero bytes under an injected fault.

Minor: `lattices::Point<T, Provenance>` is a phantom-type marker and not a precedent for runtime
provenance; `ChannelKey` ids exist only for multi-version flows, ordinary networks are identified by
`StmtId`.
