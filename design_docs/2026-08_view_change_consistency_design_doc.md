# Compositional Consistency Across View Changes

> **Design doc (August 2026).** Proposes a mechanism for inferring cluster
> consistency labels (e.g. `EventualConsistency`) for long-lived protocols that
> run over *dynamic* cluster membership, by composing an inner-loop protocol
> (which upholds a property under fixed membership) with a view-change protocol
> (which preserves that property across adjacent membership views).

## Table of Contents

- [Motivation](#motivation)
- [Background: how consistency enters the type system today](#background-how-consistency-enters-the-type-system-today)
- [The core pattern: inner loop + view change](#the-core-pattern-inner-loop--view-change)
- [The Datalog reframing: the join *is* the view change](#the-datalog-reframing-the-join-is-the-view-change)
- [The central insight: demux-to-everyone *is* `broadcast_closed`](#the-central-insight-demux-to-everyone-is-broadcast_closed)
- [The two options considered](#the-two-options-considered)
- [Option B: compositional consistency over view changes](#option-b-compositional-consistency-over-view-changes)
- [Worked example: dynamic reliable broadcast](#worked-example-dynamic-reliable-broadcast)
- [The three variants as one combinator](#the-three-variants-as-one-combinator)
- [Blocker: the simulator has no dynamic membership](#blocker-the-simulator-has-no-dynamic-membership)
- [Open questions](#open-questions)
- [Incremental plan](#incremental-plan)

## Motivation

Hydro represents cluster membership as a *live, asynchronous stream of events*
(`MembershipEvent::{Joined, Left}`, keyed by `MemberId`) obtained from
`Location::source_cluster_membership_stream`. Each node's view of "who is in the
cluster right now" evolves independently and non-deterministically; there is no
cross-node agreement on *when* a join or leave is observed.

Against this backdrop, Hydro's strongest consistency proofs for cluster
collections rely on the opposite assumption — *closed*, deploy-time-fixed
membership:

- `Stream::broadcast_closed` earns `EventualConsistency` (EC) precisely because
  it fans out over `ClusterIds`, which is deploy-time metadata *identical on
  every member*. The proof is discharged with a single
  `assert_has_consistency_of_trusted(manual_proof!(...))` grounded in that fact.
- The two "EC fully inferred" protocols in `hydro_std`
  (`reliable_broadcast_closed`, `crdt_gossip::g_set_gossip`) do not infer EC
  through any dynamic-membership operator. They infer it because
  `broadcast_closed` *hands them* an EC-typed location, and their `forward_ref`
  cycle stays pinned to that EC location type through consistency-neutral ops
  (`merge_unordered`, `unique`, `fold`, `clone`).

Meanwhile, every dynamic-membership fanout path bottoms out at `Stream::demux`,
which **unconditionally** returns `Cluster<'a, L2, NoConsistency>`. This is
correct: demux targets a specific member ID computed from a point-in-time
membership snapshot, which is inherently per-member-divergent.

The consequence: **there is currently no way to infer EC for any protocol built
over dynamic membership.** EC only ever enters the type system via (a)
`broadcast_closed`'s trusted assertion grounded in static `ClusterIds`, or (b) a
network policy's `NetworkFor::ConsistencyGuarantee` associated type. Neither
applies to a `demux`-over-dynamic-membership protocol.

We want to build long-lived services (starting with a *dynamic* reliable
broadcast that supports nodes joining and leaving) and still have the type
system *infer* a meaningful consistency label for their outputs — not force
every such protocol to re-assert consistency with a bespoke `manual_proof!`.

## Background: how consistency enters the type system today

Relevant machinery (all in `hydro_lang`):

- `location::cluster::{Consistency, NoConsistency, EventualConsistency}` — the
  marker trait and its two impls. `Cluster<'a, Tag, Con: Consistency = NoConsistency>`.
- `location::dynamic::ClusterConsistency` — the runtime enum
  (`NoConsistency | EventualConsistency`), `#[derive(PartialOrd, Ord)]`, so there
  is already a lattice hook for "at least as strong as."
- `networking::NetworkFor::ConsistencyGuarantee: Consistency` — the transport's
  associated type. `fail_stop` and `lossy_delayed_forever` map to
  `EventualConsistency`; plain `lossy` maps to `NoConsistency`. **This is the
  existing precedent for consistency-as-an-associated-type derived from a
  policy.**
- `assert_has_consistency_of_trusted::<L2>(manual_proof!(...))` — the trusted
  escape hatch that lets an operator claim a stronger consistency than the type
  system can derive, discharged by a human-checked proof note. Used exactly
  twice today, both inside `broadcast_closed`.

The pattern worth internalizing: **EC is established *once*, at a trusted
boundary, and then *preserved* (inferred) through consistency-neutral dataflow.**
The design below generalizes *where* that trusted boundary can legitimately sit.

## The core pattern: inner loop + view change

A large class of long-lived distributed services decompose into two levels:

1. **Inner loop (fixed membership).** Assuming the set of participants is fixed
   (a "view" `V`), the protocol upholds some *arbitrary* property `P` (its
   specification). `P` is whatever the protocol guarantees — for reliable
   broadcast, `P` is the RB spec (Validity + Agreement over *delivery events*);
   for a quorum read it is quorum intersection; and so on. `P` is **not** the
   same thing as a consistency label like `EventualConsistency` (EC). EC is a
   *specific* label the type system tracks for replicated-collection
   convergence; for collection-valued protocols EC is a *consequence* of `P`,
   derived once, not a synonym for `P`.

2. **View change.** Hooks that ensure `P` continues to hold *across adjacent
   views* `V → V'` (a member joins or leaves).

The goal — the "think big" framing — is to let protocol authors implement and
*study* their protocols under **fixed membership** (the tractable regime, which
Hydro already supports via `broadcast_closed` / `ClusterIds`), and then get the
dynamic-membership version *and its guarantees* for free, by showing that the
view-change hooks make the dynamic execution behave *as though membership were
fixed*. Concretely: every time a view change happens, the hooks make it as
though a fresh fixed-membership world begins *now*, with the new view, seeded so
the protocol is in a legal state of that fixed world. If that holds, the
protocol never "sees" a membership change — from its own vantage it only ever
ran in (a sequence of) fixed-membership worlds it was already proven correct
for. This is a **refinement / simulation** argument, and it is generic over both
the protocol and its guarantee.

The crucial observation is that **the correctness argument at level 2 is not the
same argument as at level 1.** Level 1 rests on "every member computes the same
membership set." Level 2 rests on "the state carried from `V` into `V'` is a
complete hand-off." Because they are different proof obligations, they deserve

The crucial observation is that **the correctness argument at level 2 is not the
same argument as at level 1.** Level 1 rests on "every member computes the same
membership set." Level 2 rests on "the state carried from `V` into `V'` is a
complete hand-off." Because they are different proof obligations, they deserve
different type-level treatment rather than being collapsed into a single flat EC
assertion.

Level 1 is *already solved* by the existing machinery: `broadcast_closed` + a
`forward_ref` cycle infers EC within a fixed view. The gap is that level 2 — the
across-view guarantee — has no representation in the type system at all.

## The Datalog reframing: the join *is* the view change

The cleanest way to see why "as though membership were fixed" is achievable —
and often *free* — is to think in Datalog. Many over-all-members operations are
joins against a `member` relation. Broadcast is the archetype:

```
deliver(m, p) :- message(m), member(p).
```

Under fixed membership you implement this incrementally (semi-naïve / delta):
a new `message(m)` fires the rule against all current members. But incremental
maintenance of a join is **symmetric in its inputs** — it *also* fires on deltas
to `member`. A new `member(p)` fires the rule against all accumulated
`message(m)`, producing exactly the `deliver(m, p)` events that catch the joiner
up. **You did not write a view-change protocol at all** — you wrote the join,
and correct delta maintenance handles membership deltas for free, because
`member` is just another relation in the rule.

So "make it as though membership were fixed" is not, for this class, a
simulation theorem to discharge — it is simply: *express the over-members
operation as a join against the live `member` relation, and rely on the
framework maintaining that join incrementally under deltas to either input.* The
fixed- and dynamic-membership implementations are the **same rule**; the only
obligation is complete delta propagation on the `member` input. Monotonicity
does the rest: while `member` and `message` only grow, the join is monotone,
incremental maintenance is exact, and the fixed-membership guarantee is
preserved by construction.

This reframing sharpens everything:

- **The "hooks" collapse to one requirement:** the protocol must express its
  over-all-members operations as joins against the `member` *relation*, not
  against a point-in-time *snapshot* of it. That is the entire interface. There
  is no per-protocol reseed/retire hook to hand-write — there is the relation,
  and the obligation to join against it rather than sample it.
- **`broadcast`'s current `NoConsistency` is explained precisely.** It takes a
  `use::snapshot` of the membership singleton and demuxes against that frozen
  set (see `Stream::broadcast` in `live_collections/stream/networking.rs`).
  Snapshotting *breaks* the join's delta symmetry: a `member` delta after the
  snapshot never re-fires the rule, so joiners are missed, so it is
  `NoConsistency`. The dynamic-EC version is the *same operation* joined against
  the *live* monotone `member` relation instead of a snapshot, so member-deltas
  fire, so joiners are covered.
- **The atomicity knob re-reads as the lattice structure of `member`.** If
  `member` only grows (joins-only, append-only), the join is monotone and
  refinement is exact by construction. `Left` events make `member`
  non-monotone (a retraction), which is exactly where either
  retraction/multiplicity machinery or an atomic-view-change protocol becomes
  necessary, and exactly where the guarantee degrades. **Joins are trivial;
  leaves are the hard part** — and for reliable broadcast specifically, leaves
  need no handling at all (see the worked example).
- **The `sent_to` dedup dissolves.** An earlier state-focused sketch proposed a
  `KeyedSingleton<(MsgId, MemberId), ()>` to track "have I sent m to p." In the
  relational view `deliver(m, p)` is a *set*, so re-derivation is idempotent and
  dedup is automatic; there is nothing to suppress. The bookkeeping was an
  artifact of thinking imperatively about "sends" rather than relationally about
  a derived relation.

### Empirical confirmation (prototype)

A minimal prototype validates the mechanism against the real API. Using
`Stream::cross_product` (which, per its implementation, maps both sides to a
unit key and `join`s them; with both sides `Unbounded` this compiles to a
symmetric-hash `HydroNode::Join`) to cross the message stream with the *live,
unbounded* stream of joined member IDs:

```rust,ignore
let joined_member_ids = node
    .source_cluster_membership_stream(&cluster, nondet!(...))
    .entries()
    .filter_map(q!(|(id, ev)| match ev {
        MembershipEvent::Joined => Some(id),
        MembershipEvent::Left => None, // leaves need no handling for RB
    }));

let delivered = messages
    .cross_product(joined_member_ids)      // symmetric-hash join, delta on either input
    .map(q!(|(msg, member_id)| (member_id, msg)))
    .into_keyed()
    .demux(&cluster, TCP.fail_stop().bincode());
```

This compiles and passes a `sim` test that delivers all messages to all members.
It confirms: (a) the unit-key cross-product stays a symmetric-hash join, (b) no
dedup state is needed, (c) leaves are handled by `filter_map` keeping the member
relation monotone. **Caveat:** the output type is still `NoConsistency` (demux
hard-codes it) — the prototype proves the *mechanism*, not the EC *inference*.
And critically, see the simulator limitation below: the sim cannot yet produce a
member that joins *mid-execution*, so the prototype exercised only the
fixed-membership fanout, **not** the view-change (delta-on-`member`) path.

## The central insight: demux-to-everyone *is* `broadcast_closed`

Everything above still leaves the delivery join typed `NoConsistency`, because
`Stream::demux` hard-codes `NoConsistency`. But this is where the real
observation lands, and it dissolves the whole `NoConsistency`-vs-EC tension:

> **`demux`-to-everyone against a monotone member relation is the same operation
> as `broadcast_closed`, and should carry the same consistency
> (`N::ConsistencyGuarantee`), not `NoConsistency`.**

The reasoning:

- `demux` is `NoConsistency` *in general* because it routes each element `e` to
  a single member `f(e)` — a per-element choice that can differ across members,
  hence divergent.
- But the delivery join does not route to *a* member; it keys **every** element
  to **every** member (the full cross-product against the member relation). So
  `demux` here is not "route to someone," it is "send to everyone." And
  send-to-everyone against the member set is *definitionally* `broadcast_closed`.
- **Monotonicity is what makes "everyone" well-defined over time.** The member
  relation is a lattice (join-only growth, join = set union). "Demux to
  everyone against the *live* monotone relation" means: as the member set grows,
  the cross-product grows, and every element eventually reaches every member
  that ever exists. A late joiner is just a later `member` delta, and the
  monotone join re-fires every accumulated element to them. There is no
  point-in-time snapshot to be stale against — so over time this delivers
  *exactly* what `broadcast_closed` delivers against the static set. **The
  static `ClusterIds` set is simply the limit of the monotone live relation.**

This reframes the entire design. It is not, fundamentally, "add a view-change
layer with a refinement theorem on top of a `NoConsistency` demux." It is: **a
demux whose keying is the full cross-product against a monotone member relation
is a broadcast, and its consistency should be hoisted to
`N::ConsistencyGuarantee`** — the very fact `broadcast_closed` already discharges
with a `manual_proof!` grounded in *static* `ClusterIds`. The dynamic version is
the *same witness* with `ClusterIds` generalized from "static set" to "the
running join of a monotone member lattice."

### The general hoisting rule and how the type system recognizes it

The trigger for the hoist is structural: *the demux's key source is the full
cross-product against a monotone (join-only) member relation*, rather than an
arbitrary per-element routing function. Three ways to recognize it, increasingly
principled and invasive:

1. **By construction (a combinator).** Offer `broadcast_live` (or similar) that
   internally does `cross_product(monotone_members) → demux` and, because *it*
   built the total cross-product, legitimately asserts `N::ConsistencyGuarantee`
   with one `manual_proof!` inside the combinator — exactly like
   `broadcast_closed` today, but with the member source being the live monotone
   relation instead of `ClusterIds`. **This is the direct generalization of
   `broadcast_closed` and is buildable now**, with no `NoConsistency` detour at
   all. It is the correct *first* instantiation: it proves the rule is sound
   before generalizing it.

2. **By a typed monotone-member witness.** Give the member relation a type-level
   marker that it is the *complete, monotone* membership (not a snapshot, not a
   filtered subset), and gate `demux` so that when its key source is that
   witnessed relation, the output consistency is `N::ConsistencyGuarantee`. This
   is the "hoist demux in general" version — it applies to *any* demux whose
   keys provably cover the monotone member set, not just a blessed combinator.

3. **By lattice/monotonicity inference in the IR.** The compiler recognizes that
   the keying stream is monotone and covers the member lattice, and infers the
   hoist with no annotation. This is the "truly infer EC" endgame and the most
   invasive (it is Option B's combinator generalized into an inference rule over
   lattice structure).

The recommended ordering is **1 → 2 → (maybe) 3**: ship the combinator that
generalizes `broadcast_closed` to a live monotone member relation, then, once
the soundness is trusted, promote it to a recognized rule so plain `demux`-to-a-
monotone-cover can be hoisted generally.

## The two options considered


**Option A — composition, no new types (works today).**
Build the dynamic protocol with `source_cluster_membership_stream` +
`track_membership` + `demux`, accept the resulting `NoConsistency`, then
re-establish EC at the protocol boundary with
`assert_has_consistency_of_trusted(manual_proof!(...))`, exactly as
`broadcast_closed` does. The proof note carries the real argument (append-only
log + monotone membership + retry-until-delivered + EC-preserving transport ⇒
every live member eventually converges). EC then propagates through the
surrounding cycle by inference.

- **Pro:** buildable now, no changes to `demux` / `Consistency` /
  `ClusterConsistency`, faithful to how the codebase already handles its one
  legitimately-trusted EC step.
- **Con:** *every* dynamic protocol must re-assert EC with its own bespoke
  `manual_proof!`. The trust is scattered and un-reused. It does not capture the
  inner-loop/view-change structure — it flattens it.

**Option B — compositional consistency over view changes (this proposal).**
Introduce a *view-change primitive* whose associated type records the strength of
its hand-off guarantee, and a *combinator* that composes an EC-within-view inner
loop with such a view change to produce an EC-across-views output — with the
trusted proof living *once*, inside the combinator. Every protocol built on top
gets its consistency label **inferred by composition**, no per-protocol
`manual_proof!`.

This document specifies Option B. Option A remains a valid fallback / first
increment (see [Incremental plan](#incremental-plan)).

## Option B: compositional consistency over view changes

### The composition rule

Informally:

```
   (inner loop upholds P within a view, EC inferred by existing machinery)
 ⨯ (view change transfers P's state from V to V', with hand-off guarantee H)
 ⟹ (P holds across the whole view sequence, consistency = H::AcrossView inferred)
```

Both inputs are *typed*, which is what makes the output *inferred* rather than
asserted:

- The inner loop's within-view EC is already inferred (level 1, existing).
- The view change's hand-off strength becomes an **associated type on a
  view-change trait**, directly analogous to
  `NetworkFor::ConsistencyGuarantee`. Just as `fail_stop ⟹ EventualConsistency`
  is baked into the transport, `<handoff>::AcrossViewConsistency` is baked into
  the view-change protocol.

The single trusted proof ("EC-within-view + complete hand-off ⇒ EC-across-views")
lives inside the combinator's definition — one `manual_proof!`, discharged once,
reused by every client.

### Proposed trait (sketch)

```rust,ignore
/// A view-change / membership hand-off protocol. Its associated type records
/// the consistency that is *preserved across adjacent views* given an inner
/// loop that is EC within each view.
pub trait ViewChange<'a, State> {
    /// The consistency guarantee preserved across view boundaries, given a
    /// complete-within-view inner loop. Mirrors `NetworkFor::ConsistencyGuarantee`.
    type AcrossViewConsistency: Consistency;

    // ... method(s) that route hand-off state into the inner loop's cycle ...
}
```

Candidate impls:

- **`EventuallyCompleteHandoff`** (the join-triggered append-only log; see worked
  example). `AcrossViewConsistency = EventualConsistency`. No consensus required;
  the guarantee is purely eventual.
- **`AtomicViewChange`** (consensus-backed membership log, virtual-synchrony
  style) — *future*. Could justify a guarantee stronger than EC (e.g. no message
  observed out of view order). Requires consensus for the membership sequence
  itself; deferred.
- **`LossyHandoff`** (pruned / bounded-retention log; the practical Variant 3) —
  `AcrossViewConsistency = NoConsistency` today (honest under-approximation),
  or a new intermediate tier once we are willing to be invasive.

### Where the trusted boundary moves

In Option A the trusted `manual_proof!` sits at *each protocol's* `demux`
boundary. In Option B it sits *once* inside the combinator, and its statement is
the general lemma rather than a protocol-specific fact:

> Given (i) an inner collection that is EC within each fixed view (already
> type-inferred), and (ii) a view change whose hand-off transfers the complete
> P-state into every new view before that view participates, the composed
> collection is EC across the entire view sequence.

Client protocols inherit EC by *instantiating* the combinator with a hand-off
impl whose `AcrossViewConsistency = EventualConsistency` — no local proof.

### Relationship to `demux`'s `NoConsistency`

`demux` stays `NoConsistency`; we do **not** change it. The combinator is what
re-establishes consistency, exactly as `broadcast_closed` re-establishes it after
its own `demux`. The difference is that the re-establishment is now a reusable,
policy-parameterized combinator instead of an inline per-protocol assertion.

## Worked example: dynamic reliable broadcast

This is the concrete protocol that motivated the design, and the concrete witness
that makes `EventuallyCompleteHandoff::AcrossViewConsistency = EventualConsistency`
inhabitable.

### Property P (within a view)

If any correct member delivers a message `m`, all correct members of the view
eventually deliver `m`.

### Inner loop (fixed membership)

The existing `reliable_broadcast_closed` shape: initial broadcast, then a
`forward_ref` cycle that re-broadcasts newly-seen messages, deduplicated. EC
inferred by existing machinery.

### View change (eventually-complete hand-off)

Each member maintains:

1. `log: KeyedSingleton<MsgId, T>` — an **append-only** delivered-message log
   (the "burden of history"), built by fold/insert-if-absent, never pruned.
2. `sent_to: KeyedSingleton<(MsgId, MemberId), ()>` — which messages have already
   been pushed to which peer.

The delivery obligation is expressed as a **single** rule, not two:

> For every `(m, peer)` where `peer` is a *current* member and `m ∈ log`, have I
> sent `m` to `peer`? If not, send it and record it in `sent_to`.

```
outstanding = (log.keys() ⨯ current_members) \ sent_to.keys()
```

Two event kinds enlarge one side of the cross product, and *both are the same
rule*:

- **New message arrives** → new key in `log` → crossed with all current members →
  send to everyone (this is the normal within-view broadcast).
- **New member joins** (`V → V'`) → new key in `current_members` → crossed with
  all messages in `log` → send the *entire log prefix* to just that joiner (this
  is the view-change hand-off).

A joiner is simply a member who is "owed" every message that existed before they
joined — the same relation as an old message being "owed" to every member that
existed before it arrived. The join-triggered catch-up is therefore *not* a
separate mechanism; it is a special case of the general per-`(m, peer)` delivery
obligation. This is exactly why one echo round (per pair) suffices and no
periodic full-log re-broadcast is needed: correctness comes from the
`(m, peer)`-keyed dedup being re-evaluated as either side grows, not from
repetition.

### Why this is a *complete* hand-off (the level-2 proof)

Consider member `p` joining concurrently with a broadcast of `m` from member `q`:

- If `p` joins *before* `q` receives `m`: `q` receives `m`, and `p` is in `q`'s
  current-members view, so `q`'s normal broadcast of `m` targets `p`. Covered.
- If `p` joins *after* `q` receives `m`: `q`'s join-triggered push (full log,
  including `m`) covers `p`. Covered.

There is no third case: "concurrent" means one of these happens-before relations
holds from each `q`'s local point of view, and we need only *one* correct `q` to
land on either side. Because the log is append-only and pushes are idempotent
(dedup by content/ID), replay and duplication are harmless. Given an
EC-preserving transport (`fail_stop` / `lossy_delayed_forever`), every live
member eventually receives a complete prefix ⇒ P holds across views ⇒ EC.

### Why the combinator, not a bespoke assertion

All of the above is the *body* of the `EventuallyCompleteHandoff` impl's proof.
The dynamic reliable broadcast function becomes a thin client:

```rust,ignore
// pseudo-API
pub fn reliable_broadcast_dynamic<'a, T, L2>(
    source: Stream<T, Process<'a, L>, ...>,
    cluster: &Cluster<'a, L2>,
) -> Stream<T, Cluster<'a, L2, EventualConsistency>, ...> {
    with_view_change(cluster, EventuallyCompleteHandoff, |inner_loop| {
        // the fixed-view echo protocol, EC inferred within-view
    })
    // AcrossViewConsistency = EventualConsistency, inferred — no manual_proof! here
}
```

## The three variants as one combinator

An earlier discussion identified three variants of dynamic reliable broadcast:

1. **Static closed membership** (`broadcast_closed`, existing). Bounded state,
   bounded-round proof, real EC.
2. **Dynamic membership, unbounded log** (this doc's worked example). Real EC,
   at the cost of unbounded "burden of history."
3. **Dynamic membership, pruned / bounded-retention log.** Practical, but a
   joiner arriving after an entry is pruned may never receive it — a genuine
   semantic weakening, not merely a cost trade-off.

Option B reveals these are **not three protocols — they are one combinator
instantiated with three hand-off associated types**:

| Variant | Hand-off impl | `AcrossViewConsistency` |
|---|---|---|
| 1 | (static; degenerate — no view changes) | `EventualConsistency` |
| 2 | `EventuallyCompleteHandoff` (unbounded log) | `EventualConsistency` |
| 3 | `LossyHandoff` (pruned log) | `NoConsistency` today; a new intermediate tier later |

The three consistency labels fall out as the *codomain of the hand-off type*.
This is the deeper payoff and the reason Option B is worth its invasiveness: it
makes consistency **compositional over view changes**, rather than adding ad-hoc
enum variants. The atomicity/completeness of the view-change protocol *is* the
knob that selects the output consistency — a strongly-atomic view change yields
strong across-view consistency, an eventually-complete one yields EC, a
lossy/pruned one yields the weaker label.

## Blocker: the simulator has no dynamic membership

This is the largest practical finding and it gates everything downstream. The
Hydro simulator explores executions by forking on nondeterministic decisions
drawn from a fuzz driver (batch boundaries, message orderings, snapshot
versions; see the module comment in `hydro_lang/src/sim/compiled.rs`). But
**cluster membership is not one of those decision points.** In both sim and
legacy deploy, `deploy_runtime::cluster_membership_stream` emits *all* members
as `MembershipEvent::Joined` up front from the compile-time-fixed `ClusterIds`
list (a plain `futures::stream::iter`), and the sim uses that same function via
`hydro_lang/src/sim/graph.rs`. The member set is baked at compile time
(`hydro_lang/src/sim/flow.rs` computes `cluster_member_ids` into
`__hydro_lang_cluster_ids_{key}`).

Consequences:

- The simulator **cannot produce a member that joins partway through
  execution**, interleaved with message flow. It also cannot produce a `Left`.
- Therefore *every consistency guarantee Hydro can currently test is implicitly
  under static membership.* This is why `broadcast_closed` (static) is the
  well-trodden, tested path and `broadcast` (dynamic) is `NoConsistency` and
  essentially untested for its dynamic behavior.
- The green prototype `sim` test above delivered to all members, but only
  exercised fixed-membership fanout — the exhaustive search interleaved network
  delivery order, **not** membership timing. The delta-on-`member` (view-change)
  path was never taken. So "the join handles joiners" remains *empirically
  unproven* despite a passing test.

Making membership dynamic in the sim means turning the membership stream into a
**source of nondeterministic decisions** the exhaustive engine can fork on:
"does member k's `Joined` event arrive now, or later, interleaved with other
work?" (and eventually, inject `Left`). This is squarely what the sim's
exhaustive engine is built for — join timing becomes another choice point.

### Root cause and the recommended fix

Investigation of the sim internals pins the cause and a clean fix:

- **Root cause (structural, not a hard limit).** The scheduler loop
  `LaunchedSim::step()` (`hydro_lang/src/sim/compiled.rs`) first drains *all*
  async DFIRs to quiescence and returns immediately if any made progress —
  **before** reaching the nondeterministic fork point (`(0..ready_ticks +
  ready_obs).any()`). Source streams live inside async DFIRs, so
  `source_stream(futures::stream::iter([...Joined]))` emits every element as
  deterministic up-front work with zero forking. That is exactly why all
  `Joined`s arrive at once and cannot interleave with message flow. The member
  *universe* is compile-time-fixed (`__hydro_lang_cluster_ids_{key}`, from
  `with_cluster_size`), but that is fine — dynamic membership is about the
  *timing* of `Joined`/`Left` emission, not the universe.

- **Recommended fix: a dedicated membership `SimHook`.** Instead of codegen'ing
  the sim membership stream as an eager `source_stream(stream::iter(...))`,
  route it through a hook (like the existing batch/fold hooks in
  `hydro_lang/src/sim/runtime.rs`) whose queued "inputs" are the pending
  `Joined` (later `Left`) events, and whose `autonomous_decision(driver, ...)`
  draws `(0..=remaining).generate(driver)` to choose how many membership events
  to release this scheduler round. Because hooks fire only in the
  nondeterministic phase of `step()` (after async work quiesces), this naturally
  interleaves membership changes with message flow and forks the exhaustive
  search over join timing. This mirrors `TopLevelFoldHook` almost exactly.

- **Constraint.** `__hydro_lang_cluster_ids_{key}` must keep listing the *full*
  member set, because it also backs `broadcast_closed` / `ClusterIds`. The
  dynamic-membership change lives in the membership stream's *emission
  schedule*, not by shrinking that array.

- **Files.** A new hook type in `hydro_lang/src/sim/runtime.rs`; a sim-specific
  membership codegen path (today the sim reuses
  `deploy_runtime::cluster_membership_stream` via `hydro_lang/src/sim/graph.rs`)
  to emit a hook-backed source; and hook registration in `graph.rs` codegen.
  `Left` events are a clean v2 increment on top of the join-timing hook.

**No hard blockers.** This is prerequisite work for verifying *any*
dynamic-membership protocol, and hence gates the empirical validation of the
whole design.

## Open questions

1. **Reified epoch vs. implicit views.** Does a member's collection carry a
   runtime view-number / epoch (so we can detect "this state is from an older
   view and needs hand-off"), or is the boundary implicit in membership-stream
   events? The unbounded-log variant needs *no* epoch — content-addressing of the
   log substitutes for it. The atomic/consensus variant essentially *forces* a
   reified epoch. Whether the trait must accommodate a reified epoch decides
   whether it is one trait or two.

2. **Re-runnable inner loop vs. state-into-long-lived-cycle.** `broadcast_closed`'s
   cycle is one long-lived dataflow; there is no "restart the inner loop per
   view." For level 2 to compose with level 1, the view change is most naturally
   expressed as *hand-off state flowing into the existing long-lived cycle* (the
   join-push merges into the same `forward_ref` cycle), **not** as spawning a
   fresh inner loop per view. If so, the combinator's real job is "route hand-off
   state into the inner loop's cycle with the right consistency type" — a modest
   change expressible with existing `forward_ref` / `merge` primitives, *provided*
   the merged-in hand-off stream's type can carry the across-view EC witness. If
   instead we want per-view inner-loop restart, this becomes a much larger
   epoch-orchestration subsystem.

3. **Expressibility (largely resolved by the prototype).** The relational view
   removes the hardest piece: `deliver(m, p)` is a set-valued join, so the
   `(m, peer)` dedup dissolves (idempotent re-derivation), and
   `Stream::cross_product` over two `Unbounded` sides already compiles to a
   symmetric-hash `HydroNode::Join` that is delta-driven on both inputs. What
   remains unverified is whether the *view-change* (delta-on-`member`) path
   actually delivers to late joiners — which cannot be tested until the
   simulator supports dynamic membership (see the Blocker section).

4. **Liveness caveat for the EC claim.** Even the "real EC" variant 2 is EC
   *given* at least one reachable holder of the complete log whenever a joiner
   catches up (the cluster is never totally empty mid-transfer). This must be
   documented alongside the combinator's proof, the same way `broadcast_closed`
   documents its own assumption.

5. **Intermediate consistency tier (deferred, invasive).** Variant 3's honest
   label is weaker than EC but stronger than `NoConsistency`. Introducing a third
   `ClusterConsistency` variant touches the `Consistency` trait, the runtime
   enum, and the `is_none_or(|c| c == NoConsistency)` checks in `singleton.rs`,
   `stream/mod.rs`, `keyed_singleton.rs`, `optional.rs`, `keyed_stream/mod.rs`.
   The derived `PartialOrd`/`Ord` on `ClusterConsistency` already provides the
   ordering hook. Explicitly out of scope for the first cut; variant 3 reports
   `NoConsistency` in the interim.

## Incremental plan

0. **Unblock the simulator (prerequisite).** Add a membership `SimHook` so the
   exhaustive engine can fork on the *timing* of `Joined` events (and, as a v2,
   `Left`), interleaving membership changes with message flow. Without this, no
   dynamic-membership protocol can be verified. See the Blocker section for the
   concrete mechanism (`runtime.rs` hook + sim membership codegen in `graph.rs`).

1. **Prototype the protocol as a live join, hoisting to EC by construction.**
   Implement dynamic broadcast as `broadcast_live` — the direct generalization
   of `broadcast_closed` — doing `messages.cross_product(monotone_member_stream)
   → demux` against the *live, monotone* member relation, and asserting
   `N::ConsistencyGuarantee` inside the combinator with one `manual_proof!`
   (hoisting rule instantiation #1; see "The central insight"). This is EC-by-
   construction, **no `NoConsistency` detour** — the static-membership prototype
   already compiles; once step 0 lands, extend the `sim` test to exercise late
   joiners and confirm the delta-on-`member` path delivers. Reliable broadcast
   is then a thin client of `broadcast_live` (no dedup state, no leave-handling
   for RB).

2. **Extract the combinator (Option B).** Factor the inline EC assertion into a
   `ViewChange` trait + `with_view_change` combinator with
   `EventuallyCompleteHandoff` as its first impl. The `manual_proof!` moves from
   the protocol into the combinator body. Reliable broadcast becomes a thin
   client with EC *inferred*. Resolves open questions 1 and 2 by forcing a
   concrete trait shape.

3. **Second client, to validate reuse.** Port `crdt_gossip::g_set_gossip` (or a
   dynamic-membership variant of it) onto the same combinator, confirming the EC
   inference genuinely composes across more than one protocol.

4. **(Deferred) Atomic view change + intermediate tier.** Consensus-backed
   `AtomicViewChange` and the sub-EC consistency tier for pruned logs, per open
   questions 1 and 5.
