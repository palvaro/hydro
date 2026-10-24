# Completeness vs. Consistency

2026-08

**Status:** explanatory note. Pins down why `EventuallyComplete` (the premise
type on membership views, `hydro_std/src/ec_inference_demos/fan_out.rs`) is a
different property from `EventualConsistency + NoOrder`, not a respelling of
it. Written because the two are easy to conflate — an eventually-complete
view *is* tolerant of cross-observer disorder and delay, which sounds exactly
like EC,NO — and because the retired orchestrated-membership experiment
failed precisely by conflating them (epistemic doc §5).

## 1. The objects

Fix a cluster with a **true join relation** `J`: the set of members that ever
actually joined, as a fact about the run. `J` is monotone (joins-only;
`joined(p)` is a stable fact — once true, stays true).

Each observer `o` (a process, or an individual member of an observing
cluster) receives a membership feed and materializes a **view** `V_o(t) ⊆ J`
at each time `t` — a growing set of join facts it has learned. Views only
contain true facts (the substrate does not fabricate joins); the question is
only what they *omit* and *when*.

## 2. The two properties, side by side

**EventualConsistency (EC), as Hydro defines it,** is a property of a *family*
of replicated outputs: all live members eventually materialize the same value.
Applied to the views themselves, EC,NO says:

> ∀ observers o, o′:  lim V_o = lim V_o′        (some common limit `L`)

Note what it does **not** say: nothing relates `L` to `J`. Agreement is the
entire content; ground truth is never mentioned.

**EventuallyComplete** is a property of *one observer's* view, relative to
ground truth:

> for this observer o:  lim V_o = J             (no true join fact withheld)

Note what *it* does not say: nothing relates `V_o` to any other observer's
view. Two complete views may disagree at every finite time, in content and in
order, forever.

## 3. The lattice of claims

On a joins-only feed, the implication runs strictly one way:

- **All observers complete ⇒ views are EC,NO.** Every view converges to the
  same limit, namely `J` itself. (This is why the conflation is tempting:
  completeness *implies* the agreement property, so complete views look
  EC,NO from the outside.)
- **EC,NO does not imply complete.** The separating example, and it is not
  exotic: every observer snapshots membership at deploy time. All views are
  identical forever — perfect agreement, `L = the snapshot` — and a member
  joining later is omitted by everyone, *consistently*. `L ⊊ J`. This is
  exactly the legacy snapshotting `broadcast`, whose output is honestly
  `NoConsistency` for exactly this reason.

|  | agree with each other | agree with the truth |
|---|---|---|
| orchestrator feeds (per-subscriber delivery contract) | eventually ✓ | ✓ complete |
| deploy-time snapshot shared by all | ✓ always | ✗ (stale, forever) |
| one good feed among garbage feeds | ✗ | ✓ for that one observer — and that is *usable* (see §5) |

So `EventuallyComplete` = EC,NO **plus** "the common limit is `J`" — and that
extra conjunct is the entire soundness content, as the next section shows.

## 4. Why the consumer needs the truth-anchor: the quantifier chase

`fan_out`'s conclusion (the EC it stamps on *delivery*) quantifies over the
true membership: *every element eventually reaches every member that ever
joins* — every member of `J`, because those members are live destinations
whose convergence the EC label promises.

Suppose the premise were merely EC,NO on the views: all data-holders agree on
some limit `L ⊊ J`, and let `m ∈ J ∖ L`. Then:

- no holder's view ever contains `m`, so no holder ever sends to `m`;
- `m` is nevertheless a live member of the destination cluster;
- `m` materializes nothing while its peers materialize everything;
- the delivered output is **not** EC. Premise satisfied, conclusion false.

The premise must reach the same referent the conclusion quantifies over.
Mutual agreement among the senders cannot reach a member the agreement
forgot. Slogan: **you cannot deliver to a member your consensus omitted.**

With completeness the argument closes: each holder's view eventually contains
every `m ∈ J`; the fan-out join retains the data side; the arrival of `m` in
the view re-fires the accumulated data; delivery follows from the channel
policy. Each step is local to one holder — which brings us to the second
difference.

## 5. Pointwise vs. family-wide

EC is irreducibly a relation over a *family* of replicas; it is not even
well-formed for a single stream in isolation. Completeness is **pointwise**:
a property one observer's view has or lacks, alone. This matters three ways:

1. **Consumption is pointwise.** `fan_out`'s argument (§4) uses only *this
   holder's* view covering `J`; it never compares two holders' views. In
   principle one holder with a complete view delivers everything even if
   every other observer's feed is garbage.
2. **Discharge is pointwise.** The orchestrator's actual contract is
   per-subscriber ("every join event is eventually delivered on *your*
   watch/feed") — a coverage claim about one subscription, not an agreement
   claim about all of them. The escape hatch
   (`MembershipView::from_stream_asserted`) accordingly names a pointwise
   obligation: *no join fact carried by this stream's relation is permanently
   withheld from this observer* — no cross-member clause to discharge.
3. **The static case is the degenerate instance.** Deploy-time `ClusterIds`
   is complete at time zero because `J` is compile-time known — a statement
   about one view and the truth, no family involved.

## 6. Why not overload the existing label

Even granting that complete views happen to satisfy EC,NO (§3), reusing the
consistency label would be wrong twice over:

- **Semantically:** every rule that mints or propagates EC is
  mutual-agreement-shaped. No compositional dataflow rule can strengthen
  "the replicas agree" into "the replicas agree *on the truth*" — the
  truth-anchor is a fact about the deployment substrate, precisely the kind
  of non-inferable premise the agenda wants at a single named seam, not
  smuggled through a label whose propagation rules don't preserve it.
- **Operationally:** the premise gates a *different operation*. Fan-out is
  defined over `EventuallyComplete` views and deliberately undefined over
  `Sampled` ones — the "never fan out over a snapshot" guardrail as
  unrepresentability. A consistency label on the view couldn't express that
  gate: the snapshot case is the one with the *best* consistency.

The raw membership stream, meanwhile, keeps its honest `NoConsistency`: per-
observer timing genuinely diverges, and nothing needs to pretend otherwise.

## 7. Naming (a real complaint)

Both names begin with "Eventually," which invites exactly the conflation this
note exists to dispel, and "complete" collides with logic's other uses.
Candidates if a rename is ever worth the churn: `EventualCoverage`,
`ConvergesToTruth`, `NoFactWithheld`. The concept split is load-bearing
regardless of the spelling; it is arguably the one clean thing the retired
orchestrated-membership experiment taught (its post-mortem: the EC label it
minted on the member set "had no consumers" — every fan-out re-earns EC at
delivery — while the property fan-out *does* consume is coverage).

## 8. Pointers

- `hydro_std/src/ec_inference_demos/fan_out.rs` — the `Completeness`
  type-state, the two trusted discharge sites, the fan-out rule.
- `2026-08_epistemic_foundations_ec_inference.md` §5 — the post-mortem of
  minting consistency where coverage was needed.
- `2026-08_orchestrated_membership_ec_dissemination.md` — the substrate
  contract this premise is designed to be discharged by.
- `2026-08_crash_injection_sim.md` §4 — the orthogonal hole in the same rule
  (premise 2, holder persistence, refuted under crash faults): completeness
  covers *who must be reached*; it says nothing about *the holder surviving
  to reach them*.
