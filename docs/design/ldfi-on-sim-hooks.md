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
