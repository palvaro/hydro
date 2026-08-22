# Orchestrated Membership and EC-by-Construction Dissemination

**Status:** proposed (design agreed in discussion; implementation not started)

## Problem

Protocols over dynamic clusters need `EventualConsistency` (EC) outputs, but the
only membership primitive available in-system,
`source_cluster_membership_stream`, is honestly `NoConsistency`: observers can
permanently miss event *prefixes* (and, under churn, entire join+leave pairs),
so no projection of the raw stream converges across observers — not the event
sets, not even joins-only. Every EC claim over dynamic membership therefore
bottoms out in an unchecked `manual_proof!`, and today those axioms are
scattered and (in some cases) located in layers that cannot justify them.

Two prior findings motivate this design (see the note *"Why we might need a
fixpoint result to bootstrap EC live clusters"* and its appendix):

1. **EC is a limit predicate.** Convergence claims over dynamic membership are
   statements about the *limit* of a monotone process, unobservable at any
   finite instant. There is no purely structural, finite witness; something
   must sign the limit. The design goal is therefore not "eliminate trust" but
   "minimize, locate, and machine-check the premises of the trusted claims."

2. **The fan-out layer cannot justify the delivery axiom.** A single-hop
   broadcast cannot promise "every element reaches every member that ever
   joins" if the sender crashes mid-broadcast; that guarantee is what reliable
   broadcast's echo cycle *adds*. Accordingly, `broadcast_live` has been demoted
   to a `NoConsistency` mechanical fan-out and the single EC axiom moved into
   `reliable_broadcast_live` (see that module's docs). This design generalizes
   that relocation into a reusable pattern.

## New capability from the deployment substrate

In typical deployments (e.g. ECS), clusters are overseen by an **orchestrator**
(think ZooKeeper-backed). The orchestrator guarantees an **eventually
consistent view of the live member set**:

- Each member consumes its *own* view-delta stream; streams may differ between
  observers not only in ordering/prefix but in *content* (a join+leave pair
  completing before an observer subscribes is invisible to it — history
  compression, as with ZK ephemeral nodes).
- However, each observer's *folded* view — the derived live-member set —
  converges: once churn stops, all observers eventually agree on the set
  (call it `V∞`).

Consequences that bound what we may build on this:

- EC attaches to the **set**, never the stream, and only to **state-based**
  projections. History-dependent derivations ("ever joined", join counts) do
  **not** converge and never will.
- The set is **non-monotone** (members leave), so it cannot directly drive the
  delta-re-firing join that `broadcast_live` relies on for late-joiner
  catch-up.

## Key move: the monotone envelope

Fan-out does not need the EC set itself; it needs a **monotone
over-approximation** of it. Each observer folds its own view stream into

```
U = union of every member id ever present in my view   (the "envelope")
```

- `U` is **monotone by construction** (unions only grow) — it drives the
  existing symmetric-hash-join catch-up machinery unchanged.
- `U` is **not EC** and never claimed to be: observers' envelopes differ
  forever. This is fine because of a fundamental asymmetry: for delivery,
  *over*-approximation is safe (sends to departed members are wasted, not
  wrong); *under*-approximation is fatal.
- The property that matters is: **every observer's `U` eventually contains
  `V∞`** — which follows directly from the view's EC guarantee. Hence every
  node holding the message log eventually fans out to every stable member.

This *derives* what was previously the assumed "coverage" axiom, and
strengthens it from existential ("each stable member known to *some*
log-holder") to universal ("...to *every* log-holder"). The bottom axiom does
not vanish — it moves into the orchestrator, where it is backed by actual
consensus.

Note the pitfall this dodges: accumulating joins from the **raw** membership
stream is the *same code* (filter to joins, accumulate) but lacks any
convergence guarantee to lean on. The envelope must be built over the
**orchestrator's** view stream; its correctness rests entirely on the
substrate.

## Why the output cannot be *inferred* EC

The pipeline is:

```
V (EC, non-monotone set)  →  U (envelope: NoConsistency, monotone)  →  join/demux/echo  →  delivered (EC)
```

`U` is a fold over the *history* of `V`, and histories are observer-dependent —
the type system is **correct** to drop consistency there. So the pipeline is
EC-in → NoConsistency-middle → EC-out, and the final step is a limit theorem,
not a local dataflow fact. No compositional inference can carry EC through.
The goal is instead to make the signature **one reusable axiom with
machine-checked premises**.

## Architecture: two trusted points, everything else checked

### Trusted point 1: `source_orchestrator_view()` (hydro_lang)

A new membership source, typed `EventualConsistency`, blessed inside
`hydro_lang` with `assert_has_consistency_of_trusted`.

Precedent: `ClusterIds` is already EC-blessed exactly this way ("deploy-time
metadata, identical on every cluster member", `stream/networking.rs`). The
orchestrator view is its dynamic generalization, and the justification is the
same *kind*: a substrate guarantee (orchestrator consensus), not a protocol
argument. This is where "the only way to have an EC stream is to start with
one" is resolved — by starting with one that a real system actually provides.

Proposed shape (to be refined during implementation):

```rust
/// EC view of the live member set of `cluster`, backed by the deployment
/// orchestrator. State-based: emits the current set (or deltas tagged with
/// enough structure to fold into it); no history guarantee.
fn source_orchestrator_view<L2>(&self, cluster: &Cluster<'a, L2>, ...)
    -> Singleton<HashSet<MemberId<L2>>, Cluster<'a, L, EventualConsistency>, ...>
```

### The convergence theorem (what any blessing must ultimately rest on)

For the family of dissemination protocols (reliable broadcast variants, gossip,
anti-entropy — any protocol whose goal is "all stable members end up with the
same accumulated state"):

> **Premises.**
> - **(a) Order/duplicate-insensitive accumulation.** Each node's delivered
>   state is a deterministic function of a commutative + idempotent
>   accumulation of contributions (for RB: the grow-only *set* of messages —
>   `unique()` is its stream form; for CRDT gossip: the lattice state). This is
>   forced by the family itself: redundant, unordered dissemination delivers
>   duplicates in arbitrary interleavings, so anything order- or
>   count-sensitive diverges.
> - **(b) Eventual transitive flow.** From every holder, held state eventually
>   flows — directly or transitively through intermediaries' *merged*
>   summaries — to every member of `V∞`, infinitely often. (Transitive relay is
>   only sound *because* of (a).) All-to-envelope re-offer is merely the
>   simplest sufficient instance; pairwise periodic reconciliation satisfies
>   (b) via transitivity **plus a fairness assumption** on peer selection
>   (random hits everyone w.p. 1; round-robin by construction) — a liveness
>   fact invisible to the type system.
> - **(c) Envelope coverage.** Every node's envelope eventually contains `V∞`
>   (from the orchestrator axiom + monotone accumulation). Nothing inside the
>   protocol can manufacture this; it is what the orchestrator uniquely
>   supplies.
> - **(d) Per-offer eventual delivery.** Each offer to a live peer eventually
>   succeeds. Free under `fail_stop` (deliver-or-crash); under lossy transports
>   it additionally requires **sufficient retry** — protocol structure, not
>   transport (gossip's periodic rounds are its retry loop; a retained join
>   re-fires on member deltas but *not* on silent drops). The sim is honest
>   about the gap: `lossy_delayed_forever` self-labels as safety-only.
>
> **Conclusion.** All members of `V∞` converge to the same delivered state:
> the accumulation of every contribution that ever reached at least one `V∞`
> member.

The conclusion's quantifier does real work: a member that received `m`,
delivered it, and crashed before relaying is outside `V∞` — precisely the
classical "correct process" exclusion in RB's agreement property, falling out
of the `V∞` framing for free. Members that join and leave mid-flight get best
effort — the only expressible obligation, since some are invisible to every
observer.

### Trusted point 2: a two-tier pattern, not a library routine

"Reliable broadcast" is a *family* of protocols achieving a distributed
property, not one algorithm. Users will implement their own echo strategies,
gossip topologies, and anti-entropy schemes; a single library implementation
that is manually blessed helps none of them. What users need is a **pattern**
by which *their* implementation earns EC. Sorting the theorem's premises by
what the compiler can check:

- (a) — **checkable** today via the algebra-proof machinery
  (`commutative = ...`, `idempotent = ...`).
- (c) — **checkable by provenance**: make the envelope a type
  (`MonotoneCover`), constructible *only* by monotone accumulation from an
  EC-typed view. Must live in `hydro_lang` so user code cannot forge it.
- (d) — transport half **checkable** (`NetworkFor::ConsistencyGuarantee`);
  retry half is structural under `fail_stop`, an obligation otherwise.
- (b) — the crux: a liveness property of the user's dataflow shape. For the
  canonical cyclic shape (merged state is itself what gets disseminated over
  the cover via a retained join) it is **structural**. For arbitrary
  topologies it is not type-visible, period.

Hence two tiers:

**Tier 1 — an inference rule in `hydro_lang`.** A small primitive — the
replicate cycle `merge(local ∪ received) → disseminate over MonotoneCover →
deliver` — whose EC output is a *typing rule* signed once inside the
compiler's trusted base, exactly like `broadcast_closed`'s trusted assert for
the static case (which is the degenerate constant-cover instance). Any user
protocol expressible in this shape — most RB variants, all state-based CRDT
gossip — earns EC with **zero** manual proofs, whatever its batching, retry,
or dedup choices.

**Tier 2 — a named obligation for everything else.** A user with a custom
topology does not assert "output is EC" wholesale; they discharge one
specific, minimal premise — *"from any holder, your dissemination relation
eventually covers `V∞` (b), with sufficient retry for your transport (d)"* —
via a dedicated proof parameter, exactly as `commutative = manual_proof!(...)`
names what is left to the human on folds. The value of the pattern is not
eliminating the trusted step (it is a limit claim; nothing structural can
witness it) but making it **small, uniform, and located at a named hole**.

`reliable_broadcast_live` is accordingly a *test/example* of Tier 1, not a
trust anchor. Protocols that cannot use the pattern at all (quorum counts,
majority membership — anything needing the agreed *set*) consume the
orchestrator view itself (Trusted point 1) directly.

## Giving the axiom teeth: convergence checking in the simulator

EC-as-limit is unobservable at any finite instant in production, but the
simulator *controls* quiescence. For each explored interleaving it can run to
quiescence and then check that all members' delivered/merged states are equal
— a decidable, per-execution check of exactly the property the combinator's
`manual_proof!` claims.

Today the sim panics on consistency assertions unless
`skip_consistency_assertions()` is set (`sim/flow.rs`). Implementing
convergence-at-quiescence checking upgrades both the Tier-1 rule and any
Tier-2 obligation from "human said so" to "human said so, and exhaustive/fuzz
search found no diverging execution." For Tier 2 it is especially valuable:
the sim can empirically test a fairness/coverage claim (b) that no one can
statically check.

## Implementation plan

Milestones, roughly independent:

1. **M-view: orchestrator view in the sim.** A sim hook (analogous to
   `MembershipHook`) that maintains an authoritative member set, delivers
   per-observer view-delta streams with nondeterministic timing and history
   compression (a join+leave pair may be elided for late subscribers), but
   guarantees per-observer convergence to the authoritative set. This is the
   *model* of the orchestrator; deploy-mode backing (ZK/ECS) can come much
   later.
2. **M-source: `source_orchestrator_view()` in hydro_lang.** EC-typed via
   `assert_has_consistency_of_trusted`, wired to the M-view hook in sim mode.
3. **M-envelope: monotone envelope in hydro_std.** Fold the view
   into `U` with a `monotone` proof; feed the existing live cross-product.
   `broadcast_live` gains a variant taking the view instead of the raw stream.
4. **M-tier1: the replicate-cycle typing rule in hydro_lang.** The
   `MonotoneCover` provenance type and the replicate-cycle primitive whose EC
   output is signed once in the compiler's trusted base. Rewire
   `reliable_broadcast_live` as a test/example instance. Behavior tests:
   late-joiner catch-up, join-leave-join under fresh incarnation ids, sender
   crash mid-broadcast (fail-stop) with echo completing delivery.
5. **M-tier2: the named obligation.** A proof parameter (à la
   `commutative = manual_proof!(...)`) for custom-topology protocols to
   discharge premises (b) and (d)'s retry half; a pairwise-gossip example
   discharging it.
6. **M-check: convergence-at-quiescence in the sim.** Replace
   `skip_consistency_assertions` for these constructs with an actual end-state
   equality check across members of the final view.

## Pinned / open questions

- **Wasted sends** (pinned by request): `U` never shrinks, so fan-out to
  departed ids continues forever. Candidate patches, not yet designed:
  (a) unique incarnation ids (ZK-session-style; departure permanent per-id)
  make suppressing sends to departed ids safe — needs its own small axiom
  ("departed ids never return"); rejoin = fresh id = normal catch-up.
  (b) ask the orchestrator for totally ordered view changes → epochs →
  view-synchronous `broadcast_closed` per epoch + state transfer. Bigger
  machine; uses strictly more than the EC-view guarantee.
- **Retention**: the append-only premise means unbounded payload retention.
  Log compaction interacts with late-joiner catch-up; out of scope here, but
  the theorem's premise list is where a snapshot/checkpoint story would
  attach.
- **Knows-about connectivity, for the record**: without the orchestrator, the
  implemented (push-only) echo requires every stable member to be
  *directed-reachable from the message origin* in the knows-about graph —
  strictly stronger than weak connectivity, which suffices only for protocols
  that symmetrize edges (e.g. gossip that sends its view to everyone in it, or
  an RB variant that learns senders on receipt). The envelope-over-orchestrator
  design supersedes this by making every log-holder eventually know every
  stable member; recorded here because it constrains any future
  orchestrator-free fallback.
- **Left events / non-monotone consumption**: Tier 1 deliberately
  ignores `Left` for fan-out (envelope semantics). Protocols needing the
  *current* set consume the EC view directly instead.

## Relation to existing docs

- `2026-08_consistency_inference_proposal.md`: this design adds two trusted
  boundaries to that inference story; everything between them remains inferred.
- `2026-08_m0_wiring_plan.md` / `MembershipHook`: M-view generalizes the M0
  dynamic-membership hook from "release pre-seeded joins at nondeterministic
  times" to "authoritative set with compressed, converging per-observer views."
- The note *"Why we might need a fixpoint result to bootstrap EC live
  clusters"* (Obsidian) and its appendix: the problem statement and the
  option analysis this design resolves (Option 2, "certify a construction,"
  with the orchestrator supplying the view).
