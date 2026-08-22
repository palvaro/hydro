# MembershipHook exhaustive-search blowup: findings

Status: root cause proven by measurement. Fix NOT yet implemented.

## The symptom

`reliable_broadcast_live` (dynamic membership) under `.exhaustive()`:

| n (cluster size) | executions | wall clock |
|---|---|---|
| 2 | 294 | ~1.5s |
| 3 | 12,400,584 | 3424s (57 min) |

This is NOT intractable in the "can't finish" sense — n=3 *did* finish at 12.4M
executions in 57 minutes. But 12.4M is absurdly more than the genuine number of
distinct behaviors, which is a couple dozen (message vs. join interleavings).

## What the genuine state space actually is

1 process, 1 cluster of N, 1 message. Genuine distinct executions =
interleavings of {N join events, 1 message send, resulting echoes} that differ
in some observable outcome. That is a small combinatorial number over N, on the
order of tens — NOT millions.

## Measurements (all via temporary `zzz_measure_*` tests in reliable_broadcast.rs)

- **Static RB (closed membership), single message, exhaustive: 1 execution.**
  Fail-stop delivery is deterministic; the echo cycle runs to fixpoint with
  nothing forking.
- **Static RB, ordered multi-message (3 msgs): 1 execution.** In-order stream,
  no forking.
- **Static RB, UNORDERED multi-message via `send_many_unordered`: 1 execution.**
  (External input installs no stream-order hook, so not a true analog.)
- **Stock `TopLevelStreamOrderHook` (assume_ordering) with 3 elements up front,
  released one-at-a-time into the SAME echo cycle: 6 = 3! executions.**
  DECISIVE: the exact hook MembershipHook was copied from, same one-at-a-time
  release, same cycle — explores exactly the genuine orderings, NO dilation.
- **Pure unordered stream (no cycle, no membership), 3 elements: 1 execution.**
- **MembershipHook one-per-round (committed): n=2=294, n=3=12.4M.**
- **MembershipHook, `produce()` wait-coin removed: still 294.** => the internal
  wait-coin is NOT the cause.
- **MembershipHook, drain-whole-queue-in-one-consultation: n=2=26, n=3=9186.**
  Collapses the blowup but CHANGES SEMANTICS (makes all joins atomic at an
  observer, losing "member 0 joins, observer acts, member 1 joins").

## Instrumentation result (the smoking gun)

Added logging at the scheduler fork point (compiled.rs:1952, the
`(0..n_ticks+n_obs).any()`), printing which observations are ready. For n=2:

```
916  FORK ticks=0 obs=3 :: ["Process(loc1v1)#None", "Cluster(loc2v1)#Some(0)", "Cluster(loc2v1)#Some(1)"]
154  FORK ticks=0 obs=2 :: ["Process(loc1v1)#None", "Cluster(loc2v1)#Some(1)"]
154  FORK ticks=0 obs=2 :: ["Process(loc1v1)#None", "Cluster(loc2v1)#Some(0)"]
```

Every fork is between **three independent membership observers**:
- `Process(sender)` — from `broadcast_live_from_process` (initial broadcast
  observes membership at the sender)
- `Cluster#0`, `Cluster#1` — from `broadcast_live` (the echo observes membership
  at EACH cluster member)

So `reliable_broadcast_live` installs a MembershipHook at O(observers) locations,
each pre-seeded with its own copy of the join queue.

## Root cause (proven)

The simulator is normally tractable because ALL ordinary nondeterminism is
**data-driven and self-draining**:
- A hook's `input` queue is fed incrementally by an upstream DFIR.
- `can_make_nontrivial_decision()` = `!input.is_empty()`, so a hook is
  fork-eligible only while it actually has pending data.
- The scheduler only re-adds an observation to `possibly_ready_observations`
  when its async DFIR made progress (compiled.rs:1908-1911), and re-filters by
  `can_run()` (lines 1926-1928), pushing non-ready ones back to `not_ready`.
- Once a hook drains, `can_run()` goes false and it DROPS OUT of the fork set.
- Between data arrivals, a feedback cycle runs to fixpoint with nothing forking.
- => fork count scales with genuine data events, not scheduler rounds.

`MembershipHook` violates this in TWO compounding ways:
1. **Pre-seeded + `is_ready()` hardcoded `true`.** Its queue is full from t=0
   and stays non-empty until fully drained, so `can_make_nontrivial_decision()`
   is true continuously — never gated by data flow the way stock hooks are.
2. **Co-located with the echo-cycle DFIRs.** The three observers ARE the
   locations running the echo feedback loop. Their async DFIRs make progress
   every cycle round, which re-adds their observations to `possibly_ready` every
   round (compiled.rs:1908-1911). A stock drained hook gets filtered straight
   back out by `can_run()`; the always-ready MembershipHook passes the filter
   EVERY cycle round and re-enters `(0..N).any()`.

So MembershipHook forks scale as (observers × cycle-rounds), and cycle-rounds is
a fixpoint-iteration artifact, not a behavior. Replicated across
(members × observers) always-ready sources that re-ready each other → 294 → 12.4M.

This is why the stock `TopLevelStreamOrderHook` into the same cycle gives 6, not
294: it drains and drops out; mine persists and re-forks every round.

## Why cross-observer interleaving is not a real execution

No node observes another node's membership-view timing. What is observable (=
what makes an execution genuinely distinct) is each observer's OWN membership
view at the moment IT acts (broadcasts / echoes). Two schedules that agree on
every observer's action-time view but differ in the global interleaving of their
join events are the SAME actual execution. The one-per-round scheduling
enumerates all those indistinguishable cross-observer/cross-round interleavings.

## Fix direction (per user: "do not treat this stream different than any
## unordered unbounded stream"; "we still need to explore all actual executions")

Membership must flow through the ordinary draining stream machinery instead of
being a bespoke perpetually-ready hook co-located with the cycle. The fix must:
- preserve each observer's join-vs-its-own-action timing (all actual executions),
- stop enumerating cross-observer / cross-cycle-round interleavings (the artifact).

Candidate: make the membership source behave like a real upstream-fed,
self-draining stream (so `can_run()` goes false once its finite joins are
released and it drops out of the per-round fork), rather than an always-ready
(`is_ready()==true`, pre-seeded, never-draining) observation.

DECISION LOCKED (Peter): per-observer independent timelines. Nodes learn
membership at different times and CAN disagree on join timing. NO global join
order. This is a hard requirement, not up for further debate.

Consequence for the mechanism: the redundancy to eliminate is the GLOBAL
cross-observer interleaving (observer A's "sees join-0" vs observer B's
"sees join-0" — unobservable by anyone), NOT the per-observer timeline. Each
observer's own timeline (when IT sees each join relative to IT processing each
message/echo) must be fully explored. Neither drain-all (loses within-observer
join-vs-action timing) nor one-per-cycle-round (dilates + cross-interleaves) is
correct. The correct fork is per-observer, indexed by that observer's own
message/tick progress, and must NOT create a global scheduling choice across
observers.

## REFINED mechanism (why produce()-coin removal didn't help; the true lever)

The dilation is NOT primarily the hook's internal produce() coin. Removing it
kept 294. The dominant multiplier is the SCHEDULER's per-round choice
`next_tick_or_obs = (0..n_ticks + n_obs).any()` (compiled.rs:1952), which forks
on WHICH ready observation to service each round.

Chain of reasoning, reconciled with all measurements:
- The scheduler only forks at these round boundaries, choosing among currently
  READY ticks/observations.
- A stock ordering hook (sender-side, 3!=6 result) sits on a QUIET location. It
  releases its finite items over a few rounds, its input drains, `can_run()`
  goes false, and it LEAVES the ready set. The echo cycle then runs with it
  gone. So the only forks are the orderings of its releases = 3! = 6.
- The membership hooks sit on the ECHO OBSERVERS (cluster members) — the very
  locations running the feedback cycle. Their async DFIRs make progress every
  cycle round, re-adding their observations to `possibly_ready` every round
  (compiled.rs:1908-1911). Because each membership hook is pre-seeded and
  `is_ready()==true`, it stays in the ready set across EVERY cycle round.
- => every cycle round, the scheduler's `(0..N).any()` forks on the ordering of
  {sender-obs, member0-obs, member1-obs} — even though the cycle round itself is
  a fixpoint-iteration artifact and releasing a join at round k vs k+1 (with no
  intervening observable event) is the SAME execution.
- Cross-multiplied over 3 observers × many cycle rounds => 294 (n=2), 12.4M (n=3).

So the true lever is: keep membership observations OUT of the scheduler's
per-round ready set except at genuine per-observer decision points. A genuine
decision point for an observer is when it has NEW data (a message/echo to fan
out) whose membership-crossing outcome depends on whether a not-yet-released
join is visible. Between such points the observation must be non-ready so it
drops out of `(0..N).any()`, exactly like a drained stream hook.

## The faithful fix (matches "treat like any unordered unbounded stream")

Route each observer's join events through the SAME draining delivery
nondeterminism the simulator already handles tractably for ordinary streams,
rather than a bespoke always-ready observation co-located with the cycle. Two
implementation shapes considered:

(A) Deliver joins like network messages to the observer, so join-vs-message
    ordering at that observer is explored by the existing (tractable, draining)
    network machinery. Most faithful; biggest rewire.
(B) Gate the membership observation's readiness on the observer's own pending
    data (non-ready between the observer's data events), so it forks per-observer
    against that observer's message/echo progress and drains. Smaller change,
    but couples the hook to the observer's data queue.

Both preserve per-observer disagreement (LOCKED requirement) and drain (no
cross-round dilation). drain-all (26 / 9186) is REJECTED: it makes all joins at
an observer atomic, losing within-observer "join, message, join" staggering =
loses actual executions.

DECISION NEEDED FROM PETER: shape (A) vs (B). Everything else is settled.

## Cleanup owed

Temporary artifacts still in the tree (all reversible):
- `zzz_measure_*` tests in `hydro_std/src/reliable_broadcast.rs`
- (instrumentation in `hydro_lang/src/sim/compiled.rs` already reverted)
- MembershipHook currently reverted to committed one-per-round form.
