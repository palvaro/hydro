# Hydrofrac: inventory

_What exists, what a result is worth, and the handful of ideas that make it work_

Hydro's type system certifies **consistent computation**: a cluster collection labelled
`EventualConsistency` (`Cluster<'a, Tag, EventualConsistency>`) is one whose members compute the
same contents when given the same inputs, because every operator on the path is a deterministic
function of its input *set* (order- and retry-insensitive). That guarantee has an unstated premise:
*same inputs*. A `broadcast_closed` over `fail_stop` mints the label on the grounds that "a failed
connection models the recipient as having failed"; a sender that dies between the second and third
send of one fan-out leaves two members with different input sets, and every downstream operator is
perfectly consistent with its own, wrong, input. **Hydrofrac discharges that premise**: it searches
the program's declared fault model (crashes of registered nodes, omissions on lossy channels) for a
fault set and schedule under which correct members end the run holding different facts. The label
and the certificate together are convergence under faults; neither alone is. Nothing else in the
toolchain has the output shape this needs: the type system cannot see which inputs arrive, the
simulator explores interleavings but has no faults in its state space, and a random fault injector
(Maelstrom, Jepsen) produces "ran for a while, saw nothing", which is not a completeness claim.

This document is an inventory written after five rounds of development. Part I is the artifact:
how to run it and exactly what each verdict is worth. Part II is the load-bearing ideas. The
authoritative description is `docs/design/hydrofrac.md`; the reasoning behind its choices is in
`docs/design/history-hydrofrac-development.md` (five rounds; §9 findings, §10 limits, §12 status)
and `docs/design/history-hydrofrac-negation.md`. Where the history and the design doc disagree,
this document follows the design doc and says so.

---

## PART I — The artifact

### What it checks

For every collection the program labels `EventualConsistency`, hydrofrac generates one
specification (`Spec::eventual_consistency`, view in `hydro_lang/src/spec_views.rs`) and runs one
lineage-driven search for a fault set that breaks it. The specification is:

1. **Probe.** Run the body with no faults. Collect every fact any member of the collection holds
   at the end into a fixed set, `FACTS`. If `FACTS` is empty, the verdict is `Vacuous`.
2. **Per faulty run.** Exclude the members the hypothesis crashes. For each fact in `FACTS`, if any
   surviving member holds it, every surviving member must hold it. As a count:
   `|distinct (member, fact) pairs at correct members over FACTS| = |correct| × |FACTS present|`.

Three correct members ending with `{1,2}`, `{1,2}`, `{1}` is the bug. `{1}`, `{1}`, `{1}` is fine:
a fault may have kept item `2` out of the system altogether; EC requires convergence among
survivors, not loss-free delivery. New facts that appear only in faulty runs are outside the
domain and create no obligation (Part II, idea 5, says why that is essential rather than lazy).

### How to run it

**Automatic path** (`Spec::all_labels` + `Ldfi::search_all_labels`). The program needs no hooks and
no observation points; the test supplies topology, workload, crashable nodes, crash budget and a
body. This is `hydro_test/src/cluster/ec_all_labels_classes.rs`, lightly trimmed:

```rust
use hydro_lang::sim::ldfi::{LabelVerdict, Ldfi, Level, NotCheckedReason, Outcome};
use hydro_lang::sim::spec::Spec;

let mut flow = FlowBuilder::new();
let source = flow.process::<Source>();
let members = flow.cluster::<Member>();
let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

// Minted EventualConsistency by the transport; nothing here mentions hydrofrac.
let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
let _out = at_members.weaken_consistency().sim_cluster_output();

let spec = Spec::all_labels(&mut flow);          // names no collection

let compiled = flow.sim()
    .skip_consistency_assertions()               // the simulator does not check the labels itself
    .with_cluster_size(&members, 3)
    .with_provenance()                           // needed for Level::Record
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
eprintln!("{results}");
assert!(results.all_certified_or_not_checked());
```

`results` is a `LabelResults`: one `LabelResult { collection, verdict }` per labelled collection,
`verdict: LabelVerdict = Searched(Outcome) | NotChecked(NotCheckedReason)`. Query per site with
`results.at("file.rs:149")`, or `results.counterexamples()`. The default exploration is
`Exploration::Exhaustive`; use `.exploration(Exploration::Fuzz { instances: n })` when the schedule
space is too big, at the cost described below. `Level::Slice` is the default lineage level;
`Level::Record` needs `.with_provenance()` and is what every EC harness uses. Knobs:
`LDFI_LEVEL=exec|slice|record|replay` (via `.level_from_env()`), `LDFI_LINEAGE_DEBUG`,
`LDFI_SOLVER_DEBUG`, `LDFI_REPLAY_DEBUG`. Everything is behind the `ldfi` feature of `hydro_lang`;
the ILP solver (HiGHS via `good_lp`) builds from source and needs `cmake`, a C++ toolchain and
`libclang`.

Which collections get a row: a node where the label is **minted** (a `broadcast_closed` over a
granting transport, an `assert_has_consistency_of`) or where it **exits** the labelled region
(`weaken_consistency`, a `batch` into an unlabelled tick, a program output). Both are reported
because they disagree in exactly the interesting cases: reliable broadcast's first hop is minted
and *violated*; its delivered set, derived from that label, *certifies* because of the echo the
type system cannot see.

**Hand-written path.** Build `Spec::new(&mut flow)`, name what you observe
(`spec.observe(..)`, `spec.observe_cluster(collection)`, `spec.observe_sequenced(..)`), name the
correct members (`spec.correct_members(&cluster, 0..n)`), and either call
`spec.eventual_consistency(observed, correct)` for the generated view on one collection, or write
your own `spec.pre(view)` / `spec.post(view)` as Hydro over the observed facts. Then `Ldfi::search`.
Harnesses: `hydro_test/src/cluster/{raft_ldfi,paxos_ldfi,two_pc_ldfi,ec_ldfi,ec_demos_ldfi}.rs`,
`hydro_test/src/distributed/quorum_ldfi.rs`. The older goal API (`goal`, `goal_all_members`,
`goal_witnessed` with `Run::witness` / `require`) still exists; no harness uses it.

**The body is part of the specification.** It decides which inputs arrive, which timers fire, and
how long the protocol has to converge. Faults are admitted only before the body calls
`run.end_of_finite_failures()`; the body must fire the protocol's recovery timers *after* that
call, or a correct protocol "fails" (Part II, idea 6).

### The verdicts

| Verdict | What it is | What it is worth | What it does NOT imply |
|---|---|---|---|
| **Certified** (`Outcome::Certificate`, `Grade::Generated`, `Exploration::Exhaustive`) | The ILP found no fault set within the bounds that hits every recorded derivation, and every hypothesis was run under every schedule. | A proof, within the fault model and bounds, that no admissible fault set and schedule leaves correct members with different `FACTS` at the end of this body. Composed with the label: convergence under the declared faults. | Nothing about other inputs, cluster sizes, timer patterns or horizons; nothing about delay, reordering, duplication, partition-with-recovery or Byzantine behaviour; nothing about facts outside `FACTS`; nothing about any *other* label, even one derived from this one. |
| **Violated** (`Outcome::Counterexample`) | One admissible fault set and one explored schedule under which the check failed. Carries the faults, the failing schedule's deliveries, and every hypothesis tried. | A concrete reproduction. Needs no exhaustive exploration: one failing run is enough. | That the *protocol* is wrong rather than the *body* too short: a disagreement still present at the horizon may be "not yet converged". Inspect the execution. |
| **Fuzz, no violation** (`Certificate` with `Exploration::Fuzz { instances }`) | Same search, but each hypothesis was run under `instances` sampled schedules. | Evidence that the lineage search exhausted its hypotheses without finding a failing *sampled* schedule. For protocols whose outcome under a fixed fault set does not depend on tick order, this is as good as exhaustive in practice (measured: 1 vs 8 schedules agreed on every protocol except Raft). | A certificate. A schedule-sensitive bug (Raft's elections are the known case) can hide in an unsampled interleaving. The `Display` says so. |
| **Budget** (`Outcome::Budget`) | Hypothesis budget (default 256, `.max_hypotheses(n)`) ran out. | Nothing. The hypotheses tried are available for inspection. | Not a certificate, not a violation. |
| **Vacuous** (`Outcome::Vacuous`) | The failure-free probe produced no facts (generated spec) or `PRE` never held (hand-written). | A verdict about the *harness*: the body protects nothing. Before this existed, such a search certified at once with an empty derivation. | Anything about the protocol. |
| **Not checked** (`LabelVerdict::NotChecked`) | The collection is outside the generated spec's class: `LocalInputsOnly`, `NotAStream(kind)`, `InsideTick`, `Excluded`. | The label is enumerated and the reason stated, so nothing is silently skipped. | That the premise is discharged. `LocalInputsOnly` means there is genuinely nothing to search; `NotAStream` means the check is not built yet (open doors). |
| **RELATIVE** (`Grade::HandWritten`, also `Grade::Goal`) | A `Certificate` under a hand-written `PRE`/`POST`. | "No fault set that hits every *recorded* derivation breaks the view." Useful as a directed bug-finder; the hypotheses it climbs through are real executions. | Soundness of the inference. A view that can be falsified by an *extra* fact (agreement, uniqueness, a final value) can fail under a fault set that leaves every recorded derivation intact; the search never proposes it (Part II, idea 5). |

A test fails only on what it asserts: `LabelResults` is data, and nothing fails for a label the
test does not look at.

### What it has found

**Real defects in production code**, none injected (history §9.2, §9.1):

- **Paxos leader-change recommit loop** (`hydro_test/src/cluster/paxos.rs`). A new leader's
  recommit values came from a fixed P1b snapshot and were re-sent every tick; each P2b round
  triggered the next tick, so proposer and acceptors exchanged the same P2a/P2b forever. Surfaced as
  the first faulty hypothesis never quiescing. Fixed by gating the recommit on the leader's first tick.
- **Compartmentalized Paxos read quorum was not a column.** Election accepted any `rows` P1bs of
  the `rows × cols` grid, so a proposer could be elected on one row, which does not intersect the
  other row's write quorum. Agreement violation on the *correct* variant in 8 hypotheses, no fault
  needed, only a P1b ordering. Fixed: P1bs name their acceptor and are counted per column.
- **P1b quorum counted replies, not distinct acceptors.** A proposer that finds no leader re-sends
  the same ballot's P1a, acceptors answer every P1a, `collect_quorum` counts replies, so two replies
  from one acceptor made a quorum of two. Invisible until the body's `EFF` moved past the first
  ballot; then violated on correct Paxos in 48 hypotheses and correct compartmentalized Paxos in 18.
  Fixed by keeping one P1b per `(ballot, acceptor)`.
- **`hydro_std::quorum::collect_quorum_with_response` was not a function of its input** despite its
  `nondet!` justification (one batching emits `{a, b}`, another `{a, b, c}`). Found by exhaustive
  schedule exploration in two schedules once a test asked. Fixed, along with an idempotence-justified
  `fold` whose retry-only cast had no simulator implementation.
- **Maelstrom backend: untagged shared channel type.** Every cluster-to-cluster channel shared one
  message type, so two such channels deserialized each other's bytes. Found by the *first Maelstrom
  run* of the Raft comparison, not by hydrofrac; a code path no simulator test reaches.

**Injected bugs, as calibration.** Raft's election restriction comparing last-log index only
(ignoring term; shipped in real implementations; passes all thirteen `raft.rs` tests) was found in
**90 hypotheses**, four omissions and no crash, under a *generic* body shaped only by what correct
Raft needs to converge: `{omit rpc[0→1]:AppendEntries(r0), omit rpc[0→2]:AppendEntries(r0),
omit rpc[1→0]:AppendEntries(r1) ×2}`. Member 0 leads term 1 with `r0` unreplicated; member 1
leads term 2 and commits `r1` at the same index while 0 never hears of it; in the heal phase 0
campaigns with `(index 1, term 1)`, wins under the index-only check, and overwrites the committed
`r1`. The author had not written this scenario down. **Honesty correction:** an earlier version
reported "17 hypotheses" and a 25-vs-160-vs-random comparison. That body staged the bug's scenario
by hand (who leads which term, when the stale node's timer fires), leaving the search two omissions
to find. It is withdrawn; blind and random have not been re-measured on the generic body (a day of
compute each). Under Maelstrom (12 runs, 29 min, partition and latency nemeses) the bug never
occurred: its trigger is *selective* loss that keeps logs the same length while contents diverge,
which a partition does not produce. Smaller calibrations: Paxos commit quorum of `f` instead of
`f + 1`, three omissions, schedule-independent once `EFF` followed the first ballot;
compartmentalized Paxos write quorum one acceptor short, 3 hypotheses; `collect_quorum` with a
non-deduplicating caller, 5; tries-twice reliable delivery, 3 runs and two omissions (the paper's
example).

**Every producer of the label in the codebase** (`hydrofrac.md` "Results"; one crash allowed; counts
include the probe):

| Program | Result |
|---|---|
| `broadcast_closed` process→cluster, `fail_stop` | **Violated**, 5 hyp. `crash(source after bcast[→1]:Item(2))`: sender dies mid-fan-out. |
| `broadcast_closed` process→cluster, `lossy_delayed_forever` | **Violated**, 2 hyp. `omit(bcast[→0]:Item(2))`: nothing retransmits; "indefinitely delayed" is never. |
| `broadcast_closed` cluster→cluster, `fail_stop` | **Violated**, 5 hyp. |
| `broadcast_live_from_process`, `leader_merge_broadcast` (ec_inference_demos) | **Violated**, 5 and 2 hyp. The demo's own prose caveat ("assumes the leader survives"), as a counterexample. |
| Echo reliable broadcast (`hydro_test`), `reliable_broadcast_closed`, `reliable_broadcast_live` | **Certified**, 10 hyp. each, exhaustive. |
| `uniform_reliable_broadcast_closed` (thresholds 1, 2), `g_set_gossip`, `g_set_gossip_live` | **Fuzz, no violation**, 28 / 13 hyp., 8 schedules per hyp. |
| `multi_paxos`, learners' `learned`, proposer held correct | **Fuzz, no violation**, 24–25 hyp., 8 schedules. The learner echo cycle's result, not consensus's. |
| `multi_paxos`, all ten labels under `Spec::all_labels`, proposer crashable | **5 violated** (single-hop mints `:281`, `:549`, `:582`, the exit `:286`, the keyed echo `:591`), **4 fuzz-no-violation** (`:569`, `:570` trivially, one proposer; `learned` `:588` and its `flat_map` `:608`), **1 not checked** (`epoch_splice.rs:190`, a `Singleton`). |
| ISR, client and ZK crashable, `EFF` after the first round | `:343` write broadcast **Violated** by the client's mid-fan-out crash; `:564` view broadcast and `:360` replication **Violated**, 8 hyp.: ZK dies between two sends of one view. |
| 2PC with cooperative termination, exhaustive | Coordinator crashable: `:86`, `:92` **Violated**, 2 hyp. each. Coordinator held correct: **Certified**, 3 and 5 hyp. Peer channels `:98`, `:161` **Vacuous**: the failure-free run never exercises termination. |
| ABD (3 replicas, 1 client) | Every replica-labelled stream **Violated**, 2 hyp., by the client's crash between two sends. The register fold `:165` **Not checked**. |

The line the search drew without being told where it was: *every single-hop or single-point
dissemination fails under one crash; every echo or gossip cycle holds.* Both transports that
`networking/mod.rs` grants `EventualConsistency` justify it by reasoning about the recipient or
about delay; neither survives one sender crash or one lost message. The label is a promise about the
program using the transport, and only a program with a second dissemination path keeps it.

Hand-written harnesses, graded RELATIVE: correct Raft certified in 235 hypotheses (durability of a
committed entry); correct Paxos, Paxos+KV store, compartmentalized Paxos in 372, 301, 568; 2PC with
cooperative termination certified under one crash, broken under two; `collect_quorum` with a
deduplicating caller in 28; ISR's unclean election reproduces the Kafka bug.

Note on counts: the history (§1.2) reports "4 hypotheses" where the design doc reports "5" for the
same `broadcast_closed` violations, and "Certificate, 26" for multi-Paxos where the design doc says
"No violation found, 24–25, 8 sampled schedules". The design doc is current (counts include the
probe; Fuzz is not a certificate) and is used above.

### Explicit non-coverage

- **Simulator only.** The fault filter lives in the simulator's compilation of `Network` nodes
  (`sim/builder.rs`), not in the IR; nothing exists under `deploy`. Embedded (raw-element) channels
  carry no delivery id and are not fault points.
- **Fault vocabulary.** Omission on lossy channels and permanent crash of registered nodes, both
  before `EFF`. No delay, reordering, duplication, partition-with-recovery or Byzantine behaviour. A
  `fail_stop` connection cannot fail on its own. A partition can be approximated by an omission set
  (Raft's counterexample is four omissions on two links).
- **Out of class, reported not checked:** scalar labels (`Singleton`, `Optional`,
  `KeyedSingleton`: ABD's register, `epoch_splice`'s fold); labels inside a tick or atomic region;
  collections with local inputs only; element types that are not `Clone + Serialize` (exclude with
  `Spec::all_labels_except`).
- **One test, one topology, one horizon per certificate.** Cluster sizes, inputs, timers and the
  recovery window are the body's. **Fuzz is not a certificate**; uniform broadcast, gossip,
  multi-Paxos, Raft and Paxos all sample schedules today. **Finite horizon**: never-converges and
  not-yet-converged look the same; liveness is not checked.
- **Static membership.** `_live` variants are tested for faults, not join timing; no
  membership-change protocol exists on the branch.
- **Precise lineage has gaps.** The provenance rewrite covers ~26 `HydroNode` variants and panics
  by name on others (`entries_partially_ordered` on keyed streams; `leader_merge` runs at
  `Level::Slice`). Replay needs hooked batches. Deduplicating operators keep one derivation per
  record, so alternatives surface one hypothesis at a time.
- **Tested scale.** A few hundred fault points, eight records of workload, up to eight nodes.

### Cost

| Search | Reported cost |
|---|---|
| Plain / reliable broadcast, 2PC (small, exhaustive) | Seconds. Observer replays ~8 per schedule, each a few ms. |
| EC suite (`ec_ldfi`, `ec_demos_ldfi`) | ~1.5 min. Full `hydro_test` LDFI set ~7 min. |
| Paxos injected bug | 8–50 s, 12–43 hypotheses (before `EFF` was moved); schedule-independent after. |
| Raft injected bug (generic body) | **~30 min**: 90 hypotheses × 4 sampled schedules = 360 instances. |
| Correct Raft, relative certificate | **~1 h**: 235 hypotheses, 940 instances, 365,000 counterfactual replays (624 panicked and were kept conservatively). Late hypotheses cost 3–4 min each, almost all replay. |
| Correct Paxos ×3 (core, +KV, compartmentalized) | **~45 min** together, 4 sampled schedules per hypothesis. |
| `Level::Counterfactual` vs recorded levels | ~5× wall time; 300–435 replays per search on ISR. |
| Uniform reliable broadcast, exhaustive | 240,000 interleavings passed once; the test uses `Fuzz { instances: 8 }`. |

The Raft and Paxos tests are `#[ignore]`d for runtime. Raft's `committed` under the generated
specification has not been run (an hour).

---

## PART II — What is going on: the load-bearing ideas

### 1. The fault point is a sender-side filter on every `Network` send, and the transport type is the fault model

Every send half the simulator emits (`sim/builder.rs`, `create_network`) calls
`LineageHandles::record_sent` before anything goes on the wire; that call consults a `FaultState`
and returns `SendFate::{Delivered, Omitted, Crashed}`. A delivered send carries a fresh delivery id;
an omitted or crashed one produces no `Recv`. The filter is there whether or not the test asked.

The identity of a send is `SendKey { channel, from, to_member, bytes }`. An omission names one; it
drops every byte-identical copy on that channel while finite failures last, so a retransmission of
the same payload does not acquire a new identity (Molly's omission), while a retransmission with a
different payload (Raft's heartbeat with a higher `leader_commit`) is a different message. The
transport the program declared decides what the filter admits: `lossy_*` admits omission;
`fail_stop` admits only the sender's crash. The program's networking declarations *are* its fault
model, and the harness registers no channels.

A crash is a pattern of applications of the same filter: `crash(node after message)` means the
node performs every send up to and including the named one, then every later send on every channel
is `Crashed` and every external output is suppressed; it never restarts. `CrashPoint::BeforeAnySend`
is the degenerate case. Because the cut is at the sender, one crash point splits a single
broadcast's fan-out, which is the counterexample for every single-hop broadcast in the table.

**Why not the obvious alternatives.** The first version put the fault point on the receiving
`batch` hook, because interposition there was free (the scripted-hook layer existed). That
conflated "a place to interpose" with "a model of the network": a channel consumed without a hooked
batch could not be faulted; a `tee`d channel omitted at one consumer was delivered at others;
crashes needed a separate mechanism because a receiver cannot stop a sender; and the soundness of
every certificate rested on the author having registered every channel. The sender-side filter
discharges that rule by construction (history §11). And why name a crash point by a message rather
than a send count: under a different hypothesis the node's send sequence shifts, so `send #7` names
a different message, the ILP's constraint means something else in the bug's world, and the first
version certified a program with a known bug this way. With a message as identity, the hypothesis
means the same thing in every schedule, and if the message is never sent the node simply survives.

### 2. Two nested searches

Hydrofrac searches **fault hypotheses**; under each hypothesis the simulator searches
**schedules** (which runnable tick executes next, what an unhooked batch releases, which version a
snapshot reveals). A counterexample is a (fault set, schedule) pair. The outer loop is Molly's
(`Ldfi::run`, `sim/ldfi.rs`):

1. Run the probe; establish `FACTS`; record the first successful derivation.
2. Each derivation becomes a clause: a breaking fault set must interrupt at least one delivery in it.
   A clause also records the crashes that were *premises* of the run it came from (a derivation seen
   in a world where a node was dead need not hold where it is alive), and the run's send sequences,
   which give crash points their meaning.
3. Build the candidate universe: for every dependency, its omission (if the channel is lossy) and
   the crash of its sender right before that send (if the sender is crashable). Order weakest first:
   omissions before crashes; within a node, later crash points before earlier ones.
4. Solve an ILP (HiGHS via `good_lp`): one binary per candidate; minimise weighted count; one cover
   constraint per clause; crash budget and one crash per node; a no-good per tried set; no superset
   of a hypothesis whose every schedule was vacuous; when feasible, leave one `PRE` derivation intact.
5. Run the hypothesis under `Exploration`. A violating schedule ⇒ `Counterexample`. Otherwise add
   the new derivations and repeat. Infeasible ILP ⇒ `Certificate`; budget ⇒ `Budget`.

**Why nested, and why ILP.** Molly fixed a synchronous schedule and did not explore reordering;
Hydro protocols' outcomes can depend on tick order (Raft: whether a `RequestVote` lands before or
after the old leader's `AppendEntries`), so a certificate on one schedule per hypothesis could land
on the harmless interleaving and certify a known bug. Hence `Exploration::Exhaustive` is required
for a certificate and `Fuzz` is reported as such. The ILP rather than SAT because "weakest first" is
an objective and the budget and premise constraints are linear; same problem, more convenient
solver. `Strategy::Blind` is the same loop with the cover constraints removed, the ablation that
measures what lineage buys (2–7× fewer hypotheses on certificates; random injection misses 2PC bugs
in 2–4 of 10 repeats at five or more participants and never terminates with a claim).

### 3. Four lineage levels, and every fallback points toward over-approximation

Lineage is reconstructed from the simulator's event log (`sim/runtime/lineage.rs`: `Exec`,
`Released`, `Sent`, `Recv`, `Output`), not from the language, because Hydro programs are Rust
closures over dataflow, not Datalog rules. The derivation of an output is the backward closure
from the execution that produced it to the deliveries that supported it.

| `Level` | What it records | Cost / what it buys |
|---|---|---|
| `Execution` | A tick execution depends on everything released into it, every arrival at its node before it, and every earlier execution of every tick at the node (persisted state, over-approximated). | Free; needs nothing from the program. Coarse: a step-closure protocol is one IR node, so everything depends on everything. |
| `Slice` (default) | Same, narrowed by a static slice of the IR per sink (`sim/slice.rs`): which hooks, channels and local ticks a send or output can reach, split into same-execution and carried-through-`defer_tick`. | Free. Same weakness for closure-encoded protocols. |
| `Record` | Record-level provenance by an IR rewrite (`sim/provenance.rs`, `SimFlow::with_provenance`): every element becomes `Tagged<T>` carrying the delivery ids it was computed from; `join` unions, `fold` accumulates, a send strips the tags into the `Sent` event. `Reference`-capturing closures emit `Prov::OPAQUE`. | Compile-time rewrite; keeps hypothesis counts flat as workload grows (2PC load 8: 11 hypotheses vs 73 under `Slice`). Panics by variant name on unsupported operators. |
| `Counterfactual` | Node-local replay (`CompiledSim::replay`): re-run one node alone with its hooked batches fed their recorded releases, delete deliveries one at a time latest-first, keep a deletion if the output survives. Result: a 1-minimal sufficient set (ddmin sense). | ~5× wall time; a replay is a fresh instance of the dylib, ms each but thousands per hypothesis on Raft. Needs hooked batches. Sees through closures nothing else can; on closure-encoded ISR turns 14/27 into 6/14. |

**Why the fallback direction is the whole soundness story.** An extra dependency in a derivation
gives the solver one more fault to try (cost). A missing dependency removes a constraint the
breaking hypothesis would otherwise have to satisfy, so the search can certify a program with a
bug (unsoundness). Every one of the four false certificates during development was an
under-approximation: a byte-only match that attributed 2PC's `commit` to its `prepare`; a replay
candidate set that excluded deliveries *after* the output's execution (a later echo can substitute);
a replayed tick that legitimately had nothing to do; a node that would not reproduce its output
even with everything kept. So: `Counterfactual` falls back to `Record`, `Record` to `Slice`, and a
replay that panics (Raft's leader indexed past its log when fed recorded inputs against diverged
state, 624 times in 365,000 replays) counts as "target not produced", keeping the delivery. The
panic is caught on the generated side of the dylib boundary (`sim::runtime::erase_guarded`),
because unwinding across it aborts the process, which it did.

### 4. The specification is a Hydro flow at an observer process

A `Spec` (`hydro_lang/src/sim/spec.rs`) adds an **observer** process to the program. Observed
collections are sent to it over an ordinary `fail_stop` channel and presented in the observer's
tick as the accumulated set of facts so far. `PRE` and `POST` are bounded `Optional`s in that tick:
a view holds if non-empty at the end of the run. The observer's ticks run eagerly and outside the
schedule choice (it only consumes, so its interleaving is unobservable to the program; exploring it
multiplied schedules by 64× before this rule). The fault model's own facts are fed before each run:
`correct_members` (the complement of Molly's `crash` relation) and, for the generated view, the
fact domain `FACTS`.

Under `Spec::all_labels`, the observer's input is a placeholder *slot*. When the flow is compiled
for the simulator, an IR pass (`sim/observe.rs`) enumerates labelled collections, decides class and
`NotCheckedReason`, and taps each in-class one: the node becomes a `Tee` (as `.clone()` would), the
new branch is mapped to `(collection index, bincode of the element)` and sent to the observer with
exactly the IR a `.send()` would build. The slot becomes the union of the taps. The pass runs
*before* network compilation and *before* the provenance rewrite, so the taps are traced like any
other operator and lineage flows through them. `Ldfi::search_all_labels` then runs the ordinary
loop once per in-class collection, telling the observer which index is current and which members of
that collection's cluster are correct, and clearing the fact domain between searches.

**Why a flow and not a Rust predicate.** LDFI needs the specification to have *lineage*: the
derivation it negates is the derivation of `POST`, not of some concrete message the author named.
The first version's goals were a Rust verdict alongside hand-named witness facts, and the two could
disagree (a fact that was "ever" when it should have been "final"; a channel left unregistered);
that was the main source of false certificates. Making the view a Hydro dataflow means record
provenance and replay apply to it unchanged. Why fix `FACTS` from a probe: an open-ended "every
member holds whatever any member holds" acquires a new obligation whenever a faulty run produces a
new fact, which no earlier derivation explains, and a fresh fact is not the label's premise about
the *given* inputs anyway. (The history §1.2 describes a generated `PRE` "some correct member holds
some fact of `FACTS`"; the code, `spec_views::eventual_consistency`, has no `PRE`: non-vacuity is
the probe having a non-empty domain. The design doc and code agree; the history is stale.)

### 5. Monotonicity decides what a certificate is worth

The hitting-set inference is: *a fault set that leaves every delivery of a derivation intact
cannot break `POST`*. That is true only if `POST` is **monotone** in the observed facts: if a set
`S` of deliveries suffices for `POST`, any superset does too. A fault set that leaves `S` intact
produces some superset of `S`, and the inference needs that superset to still satisfy the view.

The generated EC view is monotone by its form: `FACTS` and `correct` are fixed by the search before
the run, so the view is a universal over fixed domains whose derivation is the record provenance of
every `(member, fact)` pair; keep them all and the view holds. No analysis is involved, which is
why it carries `Grade::Generated` and why the certificate is "within the fault model and the
explored schedules". Hand-written views can have any shape, and the everyday ones are not
monotone: *agreement* is the absence of a conflict (falsified by adding a fact); *durability of the
final leader's log* reads a `max()` over views (a later fact changes it); *integrity* ("nobody
delivered twice") holds with an empty derivation and certifies after one run having checked nothing.
The search finds bugs in these when the breaking hypothesis also happens to displace a recorded
fact; a conflict that appeared without displacing one would never be proposed. Hence
`Grade::HandWritten`, RELATIVE, no soundness claim.

**The false ISR certificate** is the concrete lesson (`history-hydrofrac-negation.md` §4). When
observer-level counterfactual derivation landed, ISR's durability view, with the final leader read
off `observe_sequenced(views).max()`, deleted views latest-first and never changed value (every
view named replica 0), so the 1-minimal sufficient set was `{the appended write, the initial view}`
with no heartbeat in it. The breaking hypothesis (lose a replication, crash the leader after the
ack, let ZK promote the stale follower) kills neither; it *adds* a later view. Six hypotheses,
certificate, bug present. Record provenance folds every view into the aggregate and so keeps the
heartbeats that kept replica 0 the leader, and with them the crash point after the acknowledgement.
So for hand-written views `spec_view` takes the **union** of the counterfactual and recorded
derivations at the observer: sound where either is, minimal where both agree. (The generated view
takes the recorded derivation alone, with no observer replay; its form makes that sufficient.) Why
counterfactual at all: a view derived through
negation (`filter_not_in`, `count() == 0`) has *no* record provenance for its emptiness, and under
record provenance alone the negated agreement view certified plain broadcast after one run; replaying
the observer with facts removed asks the same question of both formulations and gets identical
derivations. The fourth round's syntactic polarity walk (`polarity.rs`), which tried to grade
arbitrary views monotone/non-monotone/unknown, was removed in the fifth: once the one specification
hydrofrac is *for* is generated and sound by form, there is nothing left to classify, and the walk
could only approximate the property anyway.

### 6. Where the end of finite failures falls decides what can be tested

`Run::end_of_finite_failures()` flips a switch on every filter and records the event-log index.
From then on omissions are ignored, and only sends recorded *before* that index are omission
candidates in later hypotheses. Crash points lie before it too: `crash(node after m)` is proposed
only for pre-`EFF` sends `m`. Every post-`EFF` dependency is killed by exactly the crashes that kill
its sender's last pre-`EFF` send, which is how the solver sees it.

Two consequences, both learned the hard way. First, **what is a fault point is the body's
decision.** Paxos's P1b-counting defect was invisible for as long as the body's `EFF` preceded the
new leader's first ballot: the first round of P1bs always arrived, and the quorum was distinct
acceptors' by luck of ordering. Moving `EFF` past the first check made acceptor 0's P1b a fault
point, made the injected bug's counterexample schedule-independent, and exposed the real defect. The
ISR table shows the same thing in miniature: with `EFF` right after the write, no ZK send is a
crash point and the view broadcast "passes"; with `EFF` after the first round, it is violated. A
harness that cannot lose a message cannot test the protocol's handling of its loss. Second, **the
window after `EFF` must be a real recovery window.** The first version allowed crashes at any send
and produced a "violation" of correct Raft in which the leader crashed in the heal phase after the
last election timer had fired, and the leaderless cluster was blamed. Raft's generic body went
through four versions that "violated" correct Raft before the fifth gave it enough post-`EFF`
election timers, same-term requests and heartbeat rounds to converge; none of those changes was in
response to the buggy protocol. The history's §10 calls `EFF` "a leftover that should not shape the
design further"; the design doc lists it among the declared fault bounds a certificate is relative
to. The latter is the current position, and §9.2 of the same history is the reason.
`[NEEDS AUTHOR REASONING: the long-term intent for EFF: a body-supplied bound forever, or something
the convergence witness (open door 2) would replace?]`

### 7. The simulator is a counterfactual oracle, and that is why hooks still matter

A simulated run is a function of a finite sequence of nondeterministic decisions, the scheduler's
(which tick, what a batch releases) and the environment's (each send's fate); everything between is
deterministic. Lineage, properly stated, is "which decisions does this fact depend on", and the
simulator answers by re-running with one decision changed. Node-level replay (idea 3, and the
observer replay of idea 5) is the coarse instance: the only decision it can alter is "which recorded
releases this node's hooked batches made", which is why it needs hooks and why `RaftHooks` survive
the move to the sender-side filter. The same determinism is what makes the outer loop valid: "the
outcome still held, so the program has another derivation" is only an inference if the run differed
from the last by the fault set alone. The decision-trace open door generalises this.

---

## Open doors

1. **Scalar labels checked at their input stream.** A labelled `Singleton`/`Optional`/
   `KeyedSingleton` is a function of a stream the type system already vouches for; the only premise
   left is that every correct member receives the same inputs. Tap the labelling operator's input
   and report under the scalar's site. ABD's register (`abd.rs:165`) is the first case; expected to
   be violated by a client crash between two sends. "Final values equal" is explicitly *not*
   planned: it re-proves the type system's conclusion. (The history §12 proposed exactly that
   lattice-equality variant; the design doc's later commit `7d58ad20e0` supersedes it.)
2. **Convergence witnesses in `PRE`.** Let a protocol that can report "settled" (a leader whose
   view every correct member shares and whose latest heartbeat all have acknowledged) make
   unconverged runs vacuous rather than counterexamples, instead of tuning body length. Raft would
   need to expose the heartbeat acknowledgement as an output.
3. **Decision-trace replay** as the general counterfactual oracle: record every draw with its
   outcome, replay a prefix, override one decision, continue under a policy for the rest. Gives
   why and why-not at any granularity, removes Level 3's dependence on hooks, and yields a decision
   sequence rather than entropy bytes as a counterexample's schedule. The continuation policy after
   an override is the one new piece of design. Does not repair the inference for non-monotone views.
4. **Raft's `committed` under the generated specification.** The in-class form of the durability
   harness; carries the sound grade; the harness exists; about an hour; has not been run.
5. **Membership change.** Historically the richest vein (joint consensus, ZooKeeper
   reconfiguration anomalies); nothing on the branch changes configuration while running. Needs
   membership events as a driven `sim_input`, a decision whether a crashed member is also `Left`,
   and a specification of agreement across an epoch boundary.
6. **Smaller items from the roadmap**: replay cost on long runs (do not minimise over post-`EFF`
   deliveries; cache replays across hypotheses sharing a script prefix; neither changes a verdict);
   the filter as an IR node so the fault model exists under `deploy`, with delay, reordering and
   partition at the same filter, and a receive-side source so replay needs no hooks; unmeasured
   baselines (blind and random to the certificate on correct Raft, a coverage-guided fuzzer, Raft
   under load); making "every label has a certificate" a failing check, which needs a registry of
   specifications per compiled program.

## Appendix: Closed doors

<!-- APPENDIX-PLACEHOLDER -->
