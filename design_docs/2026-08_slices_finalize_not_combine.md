# Slices Are for Finalizing, Not Combining

2026-08

**Status:** programming maxim, one page. Fell out of a discussion of when
`sliced!` is actually needed. Companion to
`2026-08_nondet_vs_manual_proof.md` (where nondeterminism enters) and the
consistency-inference docs (where EC dies).

## The maxim

> Use `sliced!` to **finalize** — to emit a sealed, one-shot answer based on a
> chosen version of state. Never use it merely to **combine** live collections;
> monotone operators do that without a cut.

## Why combining doesn't need a slice

`join` accepts **any boundedness on both sides** (`stream/mod.rs:2929`) — an
unbounded×unbounded join is a safe, deterministic operator (symmetric hash
join, both sides retained). Its semantics are monotone: each element pairs with
every matching element that *ever* arrives, retroactively, as either side
grows. Map, filter, union, and lattice folds likewise. If your logic is
expressible as monotone algebra over the live collections, it needs no slice,
no `nondet!`, and it preserves `EventualConsistency`.

What monotone combination cannot give you is a **sealed answer**: "pair this
request with *the* value of the state" — one version, emitted once, never
revised. Choosing a version is inherently non-monotone (the answer at time t is
invalidated by an update at t+1). That is the one thing `sliced!` is for, and
the type system agrees: `cross_singleton` demands a **`Bounded`** singleton
(`stream/mod.rs:1014`), so reading "the current value" forces you through
`use::snapshot` — the version-pick, guarded by the `nondet!` that justifies it.

Finalization points look like: get-responses (reply once, one value),
threshold decisions ("do we have a quorum *now*?"), emitting anything a client
will act on and that must not be extended or retracted afterward.

Note the corollary even for monotone state: `count`/fold *derivation* is
monotone and sliceless, but *reading* the count is version-picking and needs
the snapshot. Derive outside slices; observe inside them; keep the slice body
minimal.

## Combining via slice is "wasting time" — in three literal senses

1. **Runtime waiting.** A monotone join is fully incremental — elements flow
   through on arrival. A slice imposes a cut: elements wait for a batch
   boundary and the body runs tick-at-a-time. That is a local synchronization
   barrier erected for no semantic gain. CALM, concretely: coordination is
   waiting, and monotone logic is exactly the logic that never waits.

2. **Simulation time.** Every `use::batch`/`use::snapshot` hook is a fork
   point for the exhaustive simulator; needless cuts multiply the explored
   state space combinatorially (see the measured blowup in
   `2026-08_membership_hook_blowup_findings.md`). A gratuitous slice inflates
   the cost of *testing*, not just running.

3. **Downstream time — the escalation.** The cut is the non-monotone step
   that downgrades `EventualConsistency`, so downstream code that needed EC
   must re-earn it, potentially with real distributed work (a broadcast round
   or worse). A local shortcut that creates distributed coordination is the
   most expensive waste of all.

## And it can be worse than waste: wrong

The repo's own case study: the old open-membership `broadcast` combined
data × members by `use::snapshot`-ing the member set inside a slice — freezing
one side of what should have been a join — so late joiners were never
retroactively matched with accumulated messages. `broadcast_live` replaced the
snapshot with a **join against the live monotone membership relation**, and the
retroactive matching of ordinary join semantics is precisely what catches
joiners up and earns EC (`2026-08_consistency_inference_proposal.md`,
`ec_inference_demos/broadcast_live.rs`). Same inputs, same intent: the join
version is incremental, EC, and correct; the slice version waits, forks the
sim, drops the label, and silently loses matches.

## Rule of thumb

Before writing `sliced!`, ask: **is any output of this block a final answer?**

- **No** — every output may keep growing/updating as inputs grow: you are
  combining. Use `join`/`cross_product`/monotone folds; delete the slice.
- **Yes** — something is emitted once and never revised: you are finalizing.
  Take the slice, keep it minimal (derive state outside, observe via
  `use::snapshot`/`use::atomic` inside), and let each `nondet!` explain why
  the version-pick is unobservable or acceptable.
