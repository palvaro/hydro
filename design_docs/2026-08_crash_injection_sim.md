# Crash-Fault Injection in the Simulator

2026-08

**Status:** implemented and demonstrated. Records the mechanism, the framing
that makes crash results honest (blocking vs. liveness; quantifier discipline),
the demo suite, and the deliberately-deferred follow-ups.

## 1. What this closes

The research agenda (§2) recorded one oracle we deliberately did not have:

> the sim explores message, batch, and join timing but never kills a process —
> so any guarantee that turns on process failure currently lives only in the
> types and prose, not in the sim.

That gap was not cosmetic. In every execution the simulator could run,
`broadcast_closed` and `reliable_broadcast_closed` were observationally
identical — the echo cycle had never been load-bearing in any explored
execution — and every claim about what consensus adds over `leader_merge` was
prose. Crash injection makes the *distinguishing* content of these protocols
mechanically checkable.

## 2. Mechanism

### The atomic-sends problem

In the sim, a send compiles to a direct `try_send` into the recipient's queue,
executed inside the sender's DFIR run. A broadcast's demux therefore reaches
all recipients atomically within one scheduler step, and the classical
partial-broadcast crash state — reached A but not B — was unrepresentable by
merely halting a location between steps.

### Staged sends + CrashHook (`sim/runtime.rs`, `sim/builder.rs`)

For locations opted into crash injection, `create_network` routes sends through
per-recipient FIFO **staging buffers** (`StagedChannel`) instead of sending
directly. A per-location **`CrashHook`** delivers them:

- **Flush** (the default): every staged message is delivered, promptly and
  deterministically. Staging adds no nondeterminism to crash-free executions.
- **Crash**: the search draws, independently per outgoing channel and
  recipient, a *prefix* of the staged FIFO to deliver — the messages a real
  crash would have gotten onto the wire — then the location halts permanently.

Fork discipline (the `MembershipHook` blowup lesson, applied from day one):
crash points exist **only at send boundaries**. A crash between two points at
which the location produced no output is observationally equivalent to
crashing at the next boundary with an empty cut, so forking anywhere else only
dilates the search. The hook is fork-eligible exactly while staged sends are
pending; it drains and drops out of the scheduler's ready set like any
ordinary stream hook.

### Halting (`sim/compiled.rs`)

A crashed location's DFIRs are skipped **but kept alive**: channels *into* it
stay open, so senders to a dead member are unaffected — messages to failed
members are wasted, not wrong, matching `fail_stop`'s contract. Its ticks and
observations are removed from scheduling; its external output streams end at
quiescence.

### Fault domains and budgets

- `SimFlow::with_crashable_process(&p)` — the process may crash (budget 1).
- `SimFlow::with_crashable_cluster(&c, max_crashes)` — **which** member
  crashes, **when** (at which of its send boundaries), and **which**
  per-recipient cut survives are all search dimensions. `max_crashes` is the
  fault model's `F`, enforced by one budget cell shared across all members'
  hooks. A spent budget also stops the crash coin, so exhausted domains stop
  forking the search.

A scripted-crash API (`Cluster::sim_crasher`, test-side targeted kills at
phase barriers) was built and then **removed**: targeting "the leader" smuggles
in knowledge the theorems don't grant, and tests one point of a universal
claim (see §3). Explored crashes subsume it.

### The sibling mechanism: dynamic membership (`MembershipHook`)

Crash injection is the *second* environment-nondeterminism source added to the
simulator; the first — dynamic membership — deserves description here both
because the demos compose them and because the crash design inherited its
discipline wholesale.

**Default behavior.** In the sim, `source_cluster_membership_stream` is backed
by an eager iterator of `(member, Joined)` events that the scheduler drains to
completion before reaching any fork point: every member appears to join at
time zero, and joins can never interleave with message flow. Correct for
static tests; useless for anything about late joiners.

**`with_dynamic_membership(&cluster)`.** A post-`compile_network` IR rewrite
(`sim/graph.rs::apply_dynamic_membership`) swaps each `ClusterMembers` source
for that cluster with a hook-backed channel: a queue pre-seeded with one
`(member, Joined)` per member, registered as a `MembershipHook` keyed by the
**observing** location. Each observer — each process, and each individual
member of an observing cluster — gets its *own* hook instance with its own
copy of the join queue. This is a semantic commitment, not an implementation
convenience: observers have independent join timelines and may disagree
forever about when (and in what order) members joined. There is no global
membership clock to disagree with.

**Scheduling.** The hook is a direct analog of the top-level stream-order
hook: each time the scheduler services it, it either releases the front
pending join into the observer's dataflow or defers it. That forks the search
on exactly one thing — *this observer's join timing relative to its own
message processing* — which is what explores "does this member join before or
after this element flowed?", the load-bearing question for late-joiner
catch-up. Once its queue drains, `can_make_nontrivial_decision()` goes false
and the hook drops out of the scheduler's ready set like any ordinary stream
hook.

**The blowup, and the rule it produced.** The first implementation
additionally forked on *which* queued member to release — but member order is
unobservable for a symmetric fan-out (releasing `{m0, m1}` vs. `{m1, m0}` is
the same execution), so the search multiplied every genuine timing
interleaving by every permutation of members, compounded across the several
observers of an echo cycle: 294 → 12,400,000 executions going from n=2 to n=3,
for a genuine state space of a couple dozen behaviors
(`membership_hook_blowup_findings.md` has the full autopsy, including the
scheduler-level mechanism: an always-ready, pre-seeded hook co-located with a
feedback cycle re-enters the fork set every fixpoint round). The fix — release
in fixed front order, fork on timing only, drain like a stock hook — preserves
every *observable* interleaving and makes exhaustive search at n=3 tractable.

The general rule both hooks now embody, and which any future environment hook
(e.g. crash → `Left`, §6) should follow: model environment events as
**draining** queues whose hooks are fork-eligible only while items are
pending, and fork **only at observably-distinct points** — an observer's own
action boundaries for joins, a sender's send boundaries for crashes. Never
fork on choices no node can distinguish, and never leave a hook perpetually
ready next to a feedback cycle.

## 3. Framing: what crash results may honestly claim

### Blocking, not liveness

"≤ F crashes ⇒ eventual commit" is a liveness property in the Alpern–Schneider
sense, and by FLP no asynchronous consensus has it — so it cannot be what a
crash test asserts. The property that actually separates consensus from
single-author constructions, given connected fail-stop channels, is
**non-blockingness** (Skeen): under ≤ F crashes, no reachable state exists
from which commit is unreachable. Its violation has a *finite witness* — a
reachable dead state — and the sim's controlled quiescence is precisely a
dead-state detector: "quiescent + fault budget respected + submitted write
uncommitted + no pending nondeterministic work" is decidable per execution.
This is the same move as convergence-at-quiescence for EC-as-limit: a claim
unobservable in production becomes a finite check where the search controls
quiescence.

Progress assertions in the demos are therefore **bounded** (commit within
MAX_ROUNDS of a scripted, fair failure-detector oracle), never untimed. The
test plays Ω; the claim is that the commit path exists and is taken when the
oracle fires.

### Quantifier discipline

The two sides of a separation result need opposite quantifiers, and the fault
API must match:

- **∃-witness** (protocol X is broken/blocking): let the search *find* the
  crash configuration. Works with any crashable domain; the search does the
  targeting.
- **∀-over-fault-configurations** (protocol Y tolerates F): the crash must be
  an untargeted search dimension over the *whole* fault domain
  (`with_crashable_cluster(_, F)`), and the driver must be **crash-agnostic**
  — it cannot know who died, exactly like a real client and failure detector.
  Scripting the victim tests one point of the universal and proves nothing.

A practical driver lesson (found by a budget-0 bisection when the first Raft
driver failed with zero crashes): keep each driver round race-free with
`sim::quiesce()` barriers between a candidacy and the request that depends on
it, and fire at most one candidacy per round. Un-barriered bursts let every
request race its own round's leadership change; simultaneous candidacies
re-create the dueling-elections livelock that randomized timeouts solve in
production.

## 4. The demo suite

All under fail-stop channels; static membership unless noted.

| Test | Fault | Quantifier / result |
|---|---|---|
| `sim_crashable_sender_explores_partial_broadcasts` (hydro_lang) | process, explored | ∀: per-recipient FIFO prefixes (a crash cuts, never reorders); ∃ divergence and ∃ full delivery. Pins the fault model itself. |
| `broadcast_closed_violates_agreement_under_sender_crash` (hydro_std) | sender process, explored | ∃ execution where one member delivered and another never will. Plain broadcast is not RB. |
| `reliable_broadcast_closed_agreement_under_sender_crash` (hydro_std) | sender process, explored | ∀: all-or-nothing delivery (agreement), both outcomes witnessed. First execution ever in which the echo cycle is load-bearing. |
| `leader_merge_dissemination_hole_leader_crash_diverges_replicas` (hydro_std) | leader process, explored | ∃ permanent replica divergence — the *dissemination* hole, repairable without consensus. |
| `leader_merge_plus_reliable_broadcast_agrees_but_blocks_at_f1` (hydro_std) | leader process, explored | ∀ agreement (RB + order-as-data repairs dissemination); ∃ dead state — **blocking at F = 1**. |
| `member_leader_single_crash_can_block_progress` (hydro_std) | log cluster, `F=1` explored | ∃ execution where a retrying client's write never reaches any member — the sole author died; no retry policy helps. |
| `any_single_crash_cannot_block_progress` (hydro_test, Raft) | cluster, `F=1` explored | ∀ (by fuzz): the write commits on ≥ N−F members within bounded rounds, prefix-consistent throughout, under every explored crash choice/timing/cut — including mid-tenure crashes with partially replicated entries. |

The last two run under the *identical* fault model and client discipline:
∃-blocked for the member-leader merge vs. ∀-progress for Raft is the exact
content of "what consensus adds is author succession."

### fan_out's premise 2, refuted on schedule

The epistemic doc's correction predicted that single-hop `fan_out`'s EC mint
tacitly assumes the holder does not crash, and could not be tested. It now is:
`fan_out_ec_mint_refuted_under_source_crash` (hydro_std) crashes a
cluster-source member mid-fan-out and the search finds live destination
members that permanently disagree — **the EC label on single-hop `fan_out` is
refuted under crash faults**, mechanically justifying the planned Tier-1 move
(demote `fan_out` to a mechanical primitive; attach the crash-honest EC mint
to the replicate cycle, whose echo is exactly what `reliable_broadcast_*`
adds and whose crash-tolerance the RB demos confirm).

## 5. Deliberate limits (current)

- **Fuzz, not exhaustive, for Raft.** The schedule space at N=3 is too large;
  the ∀ claims there are "no counterexample in 8192 executions/run," the same
  standard as the repo's existing Raft safety nets.
- **Crash-stop only.** No recovery/rejoin; nothing in the type story claims
  anything about crash-recovery either.
- **External outputs are unstaged.** A crash cuts network sends; test-visible
  outputs emitted in the same DFIR run as cut sends are still observed (local
  deliveries before the crash are legitimate observations).
- **Fail-stop channels only.** Crash × lossy compounds fault models with no
  consumer; lossy already degrades consistency labels to `NoConsistency`.

## 6. Written down, not built: crashes × dynamic membership

A crashed member never emits a `Left` event: crash injection and the
membership machinery are currently disjoint. Wiring them together is the
natural next step and touches the part of the theory the type system has
deliberately avoided (leaves are not stable facts; see the epistemic doc and
the orchestrated-membership design):

1. **Crash → eventual `Left`** in `MembershipHook` streams models an
   orchestrator's failure detection (ZK session expiry, ECS health checks).
   Per-observer timing of the `Left` must be nondeterministic, joins-vs-leaves
   interleavings explored — with the blowup lessons applied (fork on timing
   only, per-observer).
2. **The interesting checks it unlocks:** does `broadcast_live`'s monotone
   join relation stay sound when joined members die (envelope semantics say
   yes — sends to departed ids are wasted, not wrong — but that is currently
   an argument, not a test)? Does a `fan_out` over an `EventuallyComplete`
   view + crash of the *only* member that held an element reproduce the
   premise-2 refutation in the dynamic setting? Does late-joiner catch-up
   survive the death of every member that had caught the joiner up so far?
3. **What it must not claim:** a `Left` is failure-*detection*, not failure —
   the sim would be modeling the orchestrator's suspicion, which is exactly
   the boundary the orchestrated-membership design places the axiom on.

Also deferred: the RB "any correct member" positive counterpart (crash an
*echoing* member and assert survivor convergence) wants survivor-identification
in assertions — either a `crashed_members()` test-side accessor or
convergence-at-quiescence over members with live output streams (M-check
subsumes this).

## 7. Relation to existing docs

- `2026-08_research_agenda.md` §2: the "no crash injection" caveat is
  superseded by this doc.
- `2026-08_epistemic_foundations_ec_inference.md` §5: the premise-2 correction
  is now a sim-refuted fact rather than a prediction (§4 above).
- `2026-08_ordering_consistency_taxonomy.md` §5–§8: the `F` parameter and
  "cost of consensus is author succession" now have mechanical witnesses.
- `2026-08_orchestrated_membership_ec_dissemination.md`: M-check
  (convergence-at-quiescence) remains unimplemented; §6's survivor
  identification need is one more consumer for it.
