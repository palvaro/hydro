# Inferring Distributed Consistency Types

Goals and Progress

2026-08

## 1. Goals

Hydro claims to do for distributed systems what Rust did for memory safety and concurrency: make the hard cases tractable by pushing their invariants into the type system. For that claim to hold, the type system must **make it hard to write the wrong program** and give **compile-time confidence** that a correct one is correct — without the author reaching for escape hatches at every step.

The escape hatch is `manual_proof!` — an unchecked human claim that the compiler builds guarantees on — most dangerous in `assert_has_consistency_of(manual_proof!(…))`, which mints a consistency label that no mechanical oracle checks. The current ecosystem leans on it heavily: the reference Raft (`hydro_test/src/cluster/raft.rs`) asserts EC on its committed log with one monolithic `manual_proof!` bundling the election restriction, log matching, and majority commit into a single human argument — and there are few examples of **inferred** EventualConsistency (EC).

This project asks how feasible it is to move that boundary: derive consistency labels from how a protocol is built, and shrink the manual obligations down to the places where a genuine, non-inferable fact actually lives. As in Rust, the endgame is sequestration: user protocols free of assertions, with trust concentrated in a few audited library mints. The goals are:

1. **Inference** — when possible, EC appears on a protocol's output because the compiler derived it from the composition, with zero consistency assertions in the protocol body.
2. **Residue localization** — where an obligation is genuinely irreducible, it is a single, named, greppable seam rather than a proof smeared through the protocol.

The work targets two protocol classes: **EC, NoOrder** systems (exemplified by broadcast and gossip, where agreement is on a set/bag) and **EC, TotalOrder** systems (exemplified by replicated logs, where positions must be assigned). Each is studied under both static and dynamic cluster membership, and under crash-stop faults---both dynamic membership and crash faults are experimental simulator features described in more detail below.

## 2. Background

### 2.1 Static and dynamic cluster membership

The protocols below are studied under two membership regimes. Under **static membership**, the member set of a cluster is known at deploy time (`ClusterIds`): it is the same at every location and complete before the first message flows, so membership is common knowledge by construction. Under **dynamic membership**, members join over time and each location learns of joins by observing a stream of membership events. The work so far restricts dynamism to **joins only**, because a join is a *stable fact* (once true, it stays true) and the membership relation is therefore monotone; leaves are a claim about the run's future and are deferred explicitly (§4 item 4).

Where do membership events come from? Any real deployment has an **orchestrator** — Kubernetes, ECS, ZooKeeper, a service mesh, etc. — that is already in the business of tracking membership. Recognizing this factors the dynamic-membership problem cleanly in two:

1. **The common case: an oracle feeds the cluster its membership events.** In a real deployment the orchestrator provides membership. The property our protocols actually consume from it is **completeness**, not consistency: every join fact is eventually delivered to every data-holder (an *eventually complete* view of a growing set). Observers may see joins at different times, in different orders, forever; completeness of the join-event stream is exactly sufficient. This is the case that matters for anything anyone would actually ship.
2. **Inventing the universe: bootstrapping the oracle in bare Hydro.** The other branch is building such a membership oracle *from scratch* inside Hydro, with no external orchestrator — deriving eventual agreement on the member set from first principles. This is genuinely interesting and touches the hardest parts of the theory (agreeing on a mutating set is not a stable-fact problem), but it is **not urgent**: in practice the orchestrator already exists, so this is a "fun, later" branch rather than a blocker.

### 2.2 The hydro simulator

The simulator is this project's test oracle. It compiles a flow into a single deterministic executable in which every source of runtime nondeterminism — batch boundaries, message interleavings, retries — becomes an explicit decision point, and it explores the decision space either exhaustively (when finite) or by fuzzing. The division of labor the whole project rests on: **the types assume the delivery axioms and do the induction; the simulator refutes finite counterexamples.** The sim deliberately distrusts the human witnesses on both sides — it explores the choices `nondet!` admits, and it permutes elements even when a fold claims commutativity via `manual_proof!` (`sim/runtime.rs`).

Both dynamic membership and crash-stop faults required experimental extensions to the simulator. The two extensions share one design discipline, learned the hard way: fork the search only on decisions a protocol could actually observe, so that exhaustive exploration does not dilate.

**The membership hook** makes join timing a search dimension. When a cluster opts in via `with_dynamic_membership`, its `Joined` events are delivered one at a time, at nondeterministically explored moments, instead of all up front. To break symmetry, the hook forks on join *timing* only, never on unobservable member order; an earlier version that forked on order multiplied the genuine interleavings by every permutation of the members, and the diagnosis and fix are recorded in `membership_hook_blowup_findings.md`.

**Crash-fault injection** was the last missing oracle: without it, every guarantee that turns on process failure lived only in types and prose, and a fault-tolerant protocol was observationally indistinguishable from its fault-intolerant counterpart in every execution the sim could run. The mechanism (`2026-08_crash_injection_sim.md`):

- **Staged sends + `CrashHook`**: a crashable location's sends are buffered per-recipient and delivered by a hook that, at each *send boundary*, forks crash-vs-flush; a crash delivers an independently chosen per-recipient **prefix** of the in-flight sends (the classical partial-broadcast state, previously unrepresentable because sends were atomic within a DFIR tick) and halts the location permanently. Inbound channels stay open — fail-stop semantics. Crash points exist only at send boundaries, following the membership-hook lesson.
- **Fault domains with budgets**: `with_crashable_process(&p)` and `with_crashable_cluster(&c, F)`. For clusters, *which* member dies, *when*, and *which* cut survives are all untargeted search dimensions; `F` bounds total crashes via a budget shared across the members' hooks.
- Crash-free flows compile and schedule byte-identically to before; the whole pre-existing sim suite passes unmodified.

## 3. Concrete progress

I have reached a few modest milestones.  The first are a handful of demo protocols in which the EC label is derived by construction rather than asserted by fiat.  These are intended to represent *protocols that a hydro user could have written* as opposed to trusted libraries.

In each, when EC (and, where noted, TotalOrder) appears on an output, it is because the compiler derived it. Similarly, when a fault-tolerance claim is made, it is because the simulator found no counterexample (and found the counterexample for the protocols that lack it).

| Construction | Consistency (how) | Order | Membership | Safety @ F=1 crash | Progress @ F=1 crash |
| --- | --- | --- | --- | --- | --- |
| `broadcast` (snapshot, legacy) | NC — honest | NO | dynamic | vacuous (no claim) | — |
| `broadcast_closed` / `fan_out` / `broadcast_live` | EC — **minted** | per-sender TO / NO | both | **✗ shown** (sender-crash divergence; `fan_out` refutation) | — |
| `reliable_broadcast_closed` | EC — **inferred** | NO | static | **✓ shown** (all-or-nothing, exhaustive); **✗ uniformity shown** (deliver-then-crash hole) | — |
| `reliable_broadcast_live` | EC — inferred | NO | dyn-joins | *not yet shown* | — |
| `uniform_reliable_broadcast_closed` | EC — inferred | NO | static | **✓ shown, incl. faulty deliverers** (uniform agreement, fuzz) | — |
| G-Set gossip (static & dyn) | EC — inferred | NO (set) | both | **✓ shown** (survivors converge, fuzz) | — |
| ABD register (quorums only) | replica state EC — **inferred**; linearizability — untyped (sim) | TO (one cell) | static | **✓ shown** (reads see latest write; ts-monotone under client crash; fuzz) | **✓ shown** (no leader, fuzz) |
| leader_merge (Process leader) | TO,EC — inferred | TO | static | **✗ shown** (replicas diverge) | **✗ shown** (blocks) |
| leader_merge + RB, order-as-data | EC — inferred | NO + slots | static | **✓ shown** (agreement ∀, exhaustive) | **✗ shown** (dead state ∃) |
| member-leader merge (slots) | EC — inferred | NO + slots | static | — | **✗ shown** (blocks, same fault model as Raft) |
| single-decree synod (quorums + adopt-highest) | chosen = irrefutable — untyped (sim) | one decision | static | **✓ shown** (agreement ∀ under dueling ±crash; both red variants refuted, fuzz) | **✓ shown** (Ω discipline, fuzz) |
| Raft | TO,EC — **asserted** (`manual_proof!`) | TO | static | **✓ shown** (prefix-consistent ∀, fuzz) | **✓ shown** (∀ fuzz) |

Each non-blank cell above cites a mechanical witness: a compile-time type fact, or a sim result ("shown" = exhaustive where feasible; fuzz for Raft and the gossip cycle, whose snapshot-fork × echo state space defeats exhaustive search even at n=2). Blank fault cells are untested, not believed-false — they are the cheap next work items.

(A knowledge-theoretic reading — EC as Halpern & Moses's eventual common knowledge — guided several early choices: why `lossy → NoConsistency`, why EC dies at snapshots, why joins are tractable and leaves are not.)

Code can be found here: [https://github.com/palvaro/hydro/tree/eventual_consistency/hydro_std/src/ec_inference_demos](https://github.com/palvaro/hydro/tree/eventual_consistency/hydro_std/src/ec_inference_demos)

### 3.1 Static-membership demos

- **Static reliable broadcast** (`hydro_std/src/ec_inference_demos/reliable_broadcast.rs`) — echo-based reliable broadcast over a static cluster. EC is inferred around the re-broadcast cycle, with zero consistency assertions in the protocol.
- **Uniform reliable broadcast** (`.../uniform_broadcast.rs`) — RB's echo cycle with delivery gated on a quorum certificate (§3.3) at threshold F + 1: deliver only facts that are already crash-durable. Zero assertions in the protocol body. Closes RB's deliver-then-crash hole — a member that delivers and dies cannot take the fact with it — and the red/green sim pair pins the separation under an identical crashable-sender-plus-crashable-member fault model (`2026-08_quorum_certificates.md`).
- **CRDT (G-Set) gossip** (`.../crdt_gossip.rs`) — state-based grow-only-set gossip over a static cluster. EC is inferred around the folded-state cycle; the only `manual_proof!`s are the fold's associativity/commutativity/ idempotence obligations, which are honest lattice-merge facts, not consistency assertions. A companion soundness file (`crdt_gossip_soundness.rs`) pins the negative cases: swapping `fail_stop` for a lossy policy must fail to compile.
- **Leader/merge "fake consensus"** (`.../leader_merge.rs`) — multi-writer, total-order, EC output produced by a single merging leader (reminiscent of core primary/backup, and capturing the "single writer principle" in the type system). Its honest witness ledger: exactly one `nondet!` (the authored interleaving choice — irreducible, since the merge order is a fact that does not exist until the leader manufactures it) and zero consistency assertions. It also pins where the type system *refuses* the naive port (a cluster member as leader), which surfaces the exact spot the guarantee is load-bearing. What the type system does **not** see — the single point of failure inherent in this design — is now exhibited by the crash demos below.

### 3.2 Dynamic-membership demos

The dynamic demos are the §3.1 protocols with dissemination swapped from the static roster onto a **live membership view** (§3.3). In each, EC is still inferred, and the protocol body still carries zero consistency assertions.

- **Live reliable broadcast** (`.../reliable_broadcast.rs`, `reliable_broadcast_live`) — the same echo cycle, with both the initial send and the echo fanned out over live membership. A member that joins after the initial send genuinely misses it and is caught up by a peer's echo re-broadcast; join timing is explored exhaustively at n=3 through the membership hook (§2.2).
- **Dynamic G-Set gossip** (`.../crdt_gossip.rs`, `g_set_gossip_live`) — the same folded-state cycle over the live view; the only `manual_proof!`s remain the fold's lattice-merge obligations. Late-joiner catch-up and crash-healing (the gossip pump re-offering merged state after a member crash) are behavior-tested (fuzz; the n=1 case exhaustively).

### 3.3 Supporting primitives

- **`broadcast_closed`** (in `hydro_lang`'s trusted base) — the static dissemination mint: fan out over deploy-time `ClusterIds` via an EC-preserving network policy, with EC minted by a single audited assertion. The static demos of §3.1 rest on it.
- The **`EventuallyComplete` membership view** and the **`fan_out` minting rule** (`.../fan_out.rs`) — the general form: fanning out over any eventually-complete view of the (monotone, joins-only) membership relation re-earns EC at delivery. `broadcast_live` is a thin client; static `broadcast_closed` is conceptually its degenerate instance.

- The **`quorum` certificate mint** (`.../quorum.rs`) — counts *distinct* attestors per fact and mints a sealed `Durable` certificate exactly once at the threshold crossing: a fact attested by k > F members survives any in-budget crash. Quorum claims are deliberately proof territory, not type territory (value-dependent, cardinality arithmetic, relational across certificates), so this lands on the residue-localization criterion: one audited mint, protocols above it assertion-free, the crash sim as its oracle. It is also the concrete forcing function for versioned membership views (§4 item 4): there is intentionally no quorum over a live, still-growing view. The consensus roadmap it anchors (Durable ∘ Covering ∘ Splice) is `2026-08_quorum_certificates.md`.

Honest caveats, tracked in §4's checklist: the `fan_out` mint lives in `hydro_std` as a research artifact behind a user-callable assert (not in `hydro_lang`'s trusted base), completeness borrows `ConsistencyProof` instead of having its own typed premise, no deploy-mode oracle anchors the `live()` axiom — and the mint's crash-hole is now sim-refuted (§3.4).

### 3.4 The separation demos crash injection unlocked

Each sim test runs the *same* fault against two protocols; the quantifiers do the work (∃-witness that the weak protocol breaks; ∀ over explored fault configurations that the strong one doesn't):

- **Broadcast is not reliable broadcast.** Under an explored sender crash, `broadcast_closed` has an execution where one member delivered and another never will; `reliable_broadcast_closed` delivers all-or-nothing in every explored execution. The simulator crash capability has produced the first executions in which the echo cycle is load-bearing.
- **leader_merge is not consensus, in two precise senses.** (a) Its *dissemination* hole: plain broadcast diverges under a leader crash — repairable without consensus by shipping order as data over reliable broadcast, after which replicas agree in every execution. (b) Its *succession* hole, which survives the repair: the search finds a reachable **dead state** — blocking at F = 1 in the Skeen sense — where a write submitted after the crash is permanently uncommittable. Both the Process-leader and distinguished-member forms are pinned; the latter under the identical fault model as the Raft test below.
- **Raft is indeed consensus: no single crash blocks progress.** Under `with_crashable_cluster(_, 1)` and a crash-agnostic driver (round-robin election oracle, client retrying to a different member each round), the write commits on ≥ N−F members within bounded rounds in every explored execution, committed logs prefix-consistent throughout — including mid-tenure crashes with partially replicated entries. (Fuzz, 8192 executions/run, not exhaustive: Raft's schedule space at n=3 is too large. Same evidentiary standard as the existing Raft safety nets.)
- **`fan_out`'s premise 2 is refuted.** Crash a cluster-source member mid-fan-out and *live* destinations permanently diverge: the EC label minted by single-hop fan-out is false under crash faults, exactly as the epistemic post-mortem predicted. This upgrades the planned Tier-1 restructure (§4 item 3) from taste to a mechanical red/green pair.

Progress claims are phrased as **non-blockingness** (a possibility property with a finite witness — the dead state — decidable at the sim's controlled quiescence), never as FLP-forbidden untimed liveness; the framing and driver discipline are recorded in `2026-08_crash_injection_sim.md` §3.

## 4. What it takes to really fix dynamic membership

In dependency order; items 1–3 make the simplest dynamic examples sound, and the testing story (formerly item 4 here) is **done** — join timing is exhaustively explored through the compiled dataflow (§3.2):

1. **Make completeness a first-class typed premise in `hydro_lang`.** Today the `EventuallyComplete` view type and the `fan_out` minting rule live in `hydro_std` as research artifacts, reusing `ConsistencyProof` for a property that isn't consistency. The fix: a `CompletenessProof` trait of its own, the audited mint promoted into `hydro_lang`, and `broadcast_closed` re-derived as its degenerate static instance (deploy-time `ClusterIds` = the trivially complete view).
2. **Wire a real oracle behind `live()`.** The completeness axiom must be a property of an actual deploy-mode membership substrate (the orchestrator feed behind `source_cluster_membership_stream`), not an assertion satisfied by construction in the sim — the deleted experiment's unresolved gap ("deploy backing never wired"). Until an orchestrator integration exists, the axiom is honest but unanchored.
3. **Crash-honest delivery — now sim-refuted, restructure pending.** Single-hop fan-out yields EC only if the data holder does not crash mid-fan-out; the sim now *exhibits* the diverging execution (`fan_out_ec_mint_refuted_under_source_crash`), and the RB crash demos confirm the echo cycle survives the same fault. The crash-honest EC mint therefore attaches to the replicate-cycle (reliable-broadcast) pattern, with bare `fan_out` demoted to a mechanical primitive carrying no consistency claim. This item and item 1 are the same restructuring, and it should also settle `broadcast_closed`'s honesty (its trusted mint has the same crash-hole: track `F = {sender}` in the label, or stop minting EC where F-independence is not structural).
4. **Leaves are a separate tier — defer them explicitly.** Joins-only is the tractable cut (joins are stable facts). `Left` is a claim about the run's future, with exactly two doors: reclassify leave as crash (the indexical live-group already covers it) or epochs/versioned views. Note the upside of the orchestrator here: a ZK-style oracle *has* an internal total order, so it can serve **versioned** membership views — exactly the epoch structure that quorum-style protocols will eventually need for an agreed current set. Crash injection gives this tier its first concrete work item: crash → eventual `Left` in the membership hook, modeling the orchestrator's failure *detection* (`2026-08_crash_injection_sim.md` §6).

## 5. Near-term direction

- **The Tier-1 restructure** (§4 items 1 + 3, one piece of work): typed completeness premise in `hydro_lang`, the crash-honest EC mint attached to the replicate-cycle, `fan_out` demoted, `broadcast_closed`'s label made honest. Now motivated by a refutation rather than an argument, with the crash demos as its standing regression net.
- **M-check: convergence-at-quiescence in the sim.** Replaces `skip_consistency_assertions` with an actual end-state equality check across live members — gives the remaining EC axioms an oracle, and gives crash tests the survivor-identification their positive counterparts need (e.g. RB's "any *correct* member" quantifier under echoing-member crashes).
- **A real oracle substrate behind `live()`** (§4 item 2).

Deferred (interesting, not urgent): leaves/versioned views and crash → `Left` (§4 item 4); bootstrapping a membership oracle in bare Hydro (§2.1 case 2); the typed fault-dependency and epoch-splice program (`2026-08_ordering_consistency_taxonomy.md`, `2026-08_epoch_keyed_consensus_splice.md`); **static fault-tolerance refutation from location types** — capability-set cardinality as a compile-time necessary condition ("there must not be just one"), making the portfolio table's ✗ cells compiler verdicts (`2026-08_quorum_certificates.md` §3c).
