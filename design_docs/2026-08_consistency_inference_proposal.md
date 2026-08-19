# Proposal: Inferring Consistency Types for Non-Trivial Protocols

2026-08

## Goal

Extend Hydro's consistency-type *inference* from toy, fixed-membership protocols
to non-trivial ones — ending at protocols over *dynamic* membership (nodes
joining and leaving). "Inference" means the compiler derives a protocol's
output consistency from how it is built, instead of the author asserting it by
hand.

## Background (self-contained)

Hydro tags every cluster collection with a consistency label on its type:
`Cluster<Tag, Con>`, where `Con` is `NoConsistency` (default) or
`EventualConsistency` (EC). EC means every live member eventually converges to
the same value.

EC gets into the type system at one trusted boundary — `broadcast_closed` —
and only from the *conjunction* of two facts, not either alone:

1. **Closed membership**: it fans out over *static*, deploy-time-fixed
   `ClusterIds`, identical on every member.
2. **An EC-preserving network policy**: its output consistency is
   `N::ConsistencyGuarantee`, so `fail_stop`/`lossy_delayed_forever` yield EC
   while plain `lossy` yields `NoConsistency`.

Both are required: the same fixed member set *and* a policy that delivers the
same messages to every live member. Drop either and the output is not EC. (A
network policy's guarantee is a general type-level knob, but on its own — e.g.
via `demux` — it does not yield EC, precisely because membership is not closed.)

Once established, EC *propagates by inference* through **monotone** operators —
those whose output only grows as their input grows, so as members' inputs
converge to the same limit their outputs do too. Per-element maps (`map`,
`filter`, `unique`, `merge`) are monotone unconditionally; a `fold` is monotone
only when its combiner is a lattice merge (commutative + idempotent), which is
the one place a `manual_proof!` is still required. Feedback cycles
(`forward_ref`) preserve it. Operators that take a *point-in-time cut* of a
growing input — `batch`, `snapshot`, `sample_every` — are non-monotone by
nature, so they mechanically downgrade to `NoConsistency` and demand a `nondet!`
witness. Monotonicity, not mere determinism, is the dividing line: an
order-sensitive `fold` is perfectly deterministic yet diverges across members.

The gap: every dynamic-membership fanout bottoms out at `demux`, which is
unconditionally `NoConsistency` (it routes each element to one member, a
per-member-divergent choice). So today **there is no path to inferred EC over
changing membership** — the regime real long-lived services need.

## Status

- **Two EC-inferring libraries** (`hydro_std`): `reliable_broadcast_closed` and
  `crdt_gossip::g_set_gossip` earn EC on their output with *zero* consistency
  assertions in the protocol — EC is inferred entirely from `broadcast_closed`
  plus a `forward_ref` cycle. They prove inference works for real protocols, but
  both rely on closed membership, so neither is yet practical.

- **`broadcast_transcript_consensus`** (`hydro_test`): a Paxos protocol where
  every message is broadcast to all members and each member folds the shared EC
  transcript to extract the committed log. It *mostly* earns EC on the log:
  convergence is inferred from the transcript's EC, but the Paxos safety
  invariant (at most one value per slot) is not something transport-level EC can
  provide, so exactly one `assert_has_consistency_of` carries it. This pins the
  boundary — convergence inferred and free, protocol safety a single explicit
  obligation — and is backed by property tests, a deterministic simulation
  harness, and external Jepsen/Knossos linearizability checks. Still
  closed-membership.

## Next steps

### 1. Dynamic membership as a monotone join (not inner-loop + view-change)

Background exploration in `2026-08_view_change_consistency_design_doc.md`. That
doc frames dynamic membership as an "inner loop" (fixed-view protocol) composed
with a "view change" (hand-off across views). **We are dropping that framing.**
Its own Datalog section undercuts it: broadcasting to all members is the join

```
deliver(m, p) :- message(m), member(p).
```

Maintained incrementally, this fires on a `member` delta exactly as it fires on
a `message` delta — a late joiner is caught up on the accumulated messages by
the *same rule*, with no separate hand-off protocol and no per-view restart. So
there is no "view change" to compose in: the protocol is written *once* as a
description of how messages flow under deltas to *either* `message` or `member`,
and the framework's incremental maintenance does the rest. This also matches
Hydro's execution model — one long-lived `forward_ref` dataflow, not a sequence
of per-view worlds — which the two-level framing does not.

The only real distinction left is **live relation vs. snapshot**, and it is not
a combinator: join against the live monotone `member` relation rather than
`use::snapshot`-ing it. Snapshotting freezes one side of the join, so `member`
deltas never re-fire and joiners are missed — which is exactly why today's
`broadcast` is `NoConsistency`. Joining against the live relation makes
"demux to everyone" equal to `broadcast_closed` generalized from a static set to
the limit of a growing one, carrying the same EC.

Proposed path:

- **`broadcast_live`**: fan out over the live monotone member relation, asserting
  EC by construction with one `manual_proof!` — the direct generalization of
  `broadcast_closed` from static `ClusterIds` to the running join of the member
  lattice. Buildable now, no new types, no `ViewChange`/`AcrossViewConsistency`
  machinery.
- **Reliable broadcast and gossip as thin clients** of `broadcast_live`, with EC
  inferred — validating that "write it once against the live relation" composes
  across more than one protocol.

What this framing does *not* dissolve — the genuinely hard part — is
non-monotonicity. EC-for-free rests on the join being monotone (joins-only
membership, append-only state). Two things break that and neither is a
view-change question: (a) `Left` events that must *remove* state make `member`
non-monotone — a retraction problem (reliable broadcast happens to need no leave
handling, but general protocols do); (b) bounded/pruned logs, where a joiner
arriving after a prune misses data — a real consistency weakening, i.e. a
sub-EC tier. These are deferred and are the actual open research, framed as
retraction/consistency-tier questions rather than hand-off ones.

### 2. Simulator support for dynamic membership (parallel, not a prerequisite)

`broadcast_live` (step 1) can be built, type-checked, and even sim-tested under
*static* membership without any simulator change — the prototype already does
exactly this. What the simulator cannot yet do is exercise the *dynamic* path: a
member joining mid-execution, interleaved with message flow, so the late-joiner
catch-up is actually tested. It bakes membership at compile time and emits all
`Joined` events up front. Fix: route the membership stream through a `SimHook`
(like the existing batch/fold hooks) so the exhaustive engine forks on join
*timing* (and later `Left`).

This is not a prerequisite for step 1 — the two proceed in parallel. Late-join
*convergence* can already be tested today, deterministically, with a pure
step-harness (the committed `StepBroadcastCluster` pattern: drive the decision
function by hand and start feeding a member the transcript at a later tick). So
M0 is scoped narrowly: exhaustive exploration of join-vs-message *timing*
through the real compiled dataflow — the one thing neither the type system nor a
hand-written harness can certify on its own.

## Milestones

Two independent tracks converge at M2.

- **M1** (main track) — `broadcast_live`: EC by construction over the live
  monotone member relation. Buildable and type-checkable now; sim-testable under
  static membership. No simulator work.
- **M0** (parallel track) — simulator membership hook, so joins interleave with
  message flow. Separable from M1; unblocks dynamic-membership *testing*, not
  M1's construction.
- **M2** (needs M1 + M0) — reliable broadcast and gossip rewritten as thin
  clients of `broadcast_live`, EC inferred; dynamic-join test confirms
  late-joiner catch-up and that "write it once" composes.
- **M3** (research) — non-monotone membership (`Left`/retraction) and a sub-EC
  tier for bounded-retention logs.

## Realization

**M1 — `broadcast_live` (small, no new types).** `broadcast_closed` today is:
`ClusterIds` (static) → `source_iter` → `cross_product(members)` →
`.into_keyed().demux(to, via)` → one `assert_has_consistency_of_trusted`. The
open `broadcast` differs in exactly the one way that kills EC: it
`use::snapshot`s the member set inside a `sliced!` block (freezing one side of
the join) and never re-asserts, so it stays `NoConsistency`. `broadcast_live` is
`broadcast_closed` with the static source swapped for the *live* membership
stream, kept as a growing relation:

```rust,ignore
let members = self.location
    .source_cluster_membership_stream(to, nondet!(/* late joiners covered by the join */))
    .entries()
    .filter_map(q!(|(id, ev)| matches!(ev, MembershipEvent::Joined).then_some(id))); // monotone
self.cross_product(members)               // two Unbounded sides ⇒ symmetric-hash join
    .map(q!(|(data, id)| (id, data))).into_keyed().demux(to, via)
    .assert_has_consistency_of_trusted(manual_proof!(
        /* live monotone member relation + EC-preserving policy ⇒ every element
           eventually crosses every member that ever joins — ClusterIds is just
           the limit of this relation */))
```

This is the prototype the design doc already compiled, plus the one trusted
assertion (a strict generalization of `broadcast_closed`'s). No trait, no
`demux` change. Soundness needs *both* join sides retained; the symmetric-hash
join keeps them, so it holds for append-only data — a *pruned* data side breaks
it, which is exactly the M3 boundary.

**M0 — sim membership hook.** Sim codegens membership as an eager
`source_stream(stream::iter([...Joined]))`, which drains before the scheduler's
nondeterministic fork point, so joins can't interleave with message flow. The
fix is contained — one new hook plus a sim-specific source-codegen branch:

1. **`MembershipHook` (`sim/runtime.rs`), ~40 lines, mostly copied from
   `TopLevelStreamOrderHook`.** Same `SimHook` impl (release one queued event or
   none per round, under a fuzz decision — that release logic *is* the
   join-timing exploration). The one difference: its `VecDeque` is *pre-seeded*
   at construction from the cluster's member set, since a membership source has
   no upstream DFIR feeding it (the existing top-level hooks are fed by an
   upstream `for_each(push_back)`).
2. **A sim-specific `cluster_membership_stream` codegen path.** Today
   `sim/graph.rs` reuses the eager `deploy_runtime` source; instead emit a
   hook-backed `source_stream` (create the `Rc<RefCell<VecDeque>>` + mpsc
   channel, register the hook), mirroring the wiring in `builder.rs`. **This is
   where the effort concentrates:** that hook-registration code is structured
   around `CollectionKind` *transforms*, but membership is a top-level *source*,
   so wiring a pre-seeded hook into a `source_stream` cleanly is the part needing
   fluency in the sim codegen internals — not the hook logic, which is trivial.
3. **Seeding from cluster size.** `SimFlow::with_cluster_size` already records
   the max size and `flow.rs` computes member ids; the hook constructor reads
   that to build the `Joined` queue. Keep `__hydro_lang_cluster_ids_{key}`
   listing the full set (it backs `broadcast_closed`/`ClusterIds`); only the
   emission *schedule* becomes nondeterministic.
4. **An opt-in knob** (e.g. `SimFlow::with_dynamic_membership(&cluster)`) so the
   default stays eager and the existing sim tests don't all suddenly fork on
   join order. Near-zero blast radius.

`Left`/leave is deliberately out of M0's first cut (non-monotone; ties into M3).
No scheduler-loop change: it already forks on hook decisions, so M0 only *adds*
a participating hook.

**M2 — clients + payoff test.** Swap `reliable_broadcast` and `g_set_gossip`
onto `broadcast_live`; their `forward_ref` cycles keep EC inferred unchanged —
the only edit is the fan-out call. Then a sim test (enabled by M0) that joins a
member mid-run and asserts the joiner converges, exercising the delta-on-`member`
path the prototype never could.

**Sequencing.** M0 and M1 are independent — M1 compiles and type-checks without
the sim; M0 unblocks *validating* it. Run them in parallel; converge at M2.
