# Brainstorming Session: Building EC from Composable Pieces

## Date: 2026-08-07

## Summary

We attempted to build `EventualConsistency` up from small, typed building blocks
rather than asserting it by fiat on a monolithic consensus implementation. Along
the way, the Hydro simulator caught a real safety violation in our naive protocol,
demonstrating the power of combining type-level guarantees with exhaustive/fuzz
testing.

## The Journey

### Starting point

Hydro's type system gives `EventualConsistency` in two ways:
1. **Inferred** — via `broadcast_closed` + `fail_stop` (trivial, single sender to fixed cluster)
2. **Asserted** — via `manual_proof!` on complex protocols like Raft (opaque, unverifiable)

We wanted a middle path: build EC from composable pieces where each piece's EC
is inferred, and the composition gives global EC.

### New primitive: `broadcast_from_member`

Added to `hydro_lang/src/live_collections/stream/networking.rs`. A cluster member
broadcasts to all members of its own cluster. EC is inferred via
`assert_has_consistency_of_trusted` (same justification as `broadcast_closed` —
one source, same data to all members, fail_stop transport).

### Building blocks (all EC inferred)

1. **`propose_in_view`** — leader sequences requests, broadcasts proposals. EC inferred.
2. **`commit_decisions`** — leader counts acks, broadcasts commit notifications. EC inferred.
3. **`quorum_read`** — new leader gathers max committed slot from f+1 members.

### The composition argument

- Per-view proposals: EC (inferred)
- Per-view commits: EC (inferred)
- Committed log = proposals ∩ commits = deterministic join of EC streams → EC
- Cross-view safety: new view starts beyond max committed (quorum read guarantees)

### The naive fencing bug

We built `fenced_proposals` — a filter that drops proposals whose view < max_view_seen,
where max_view_seen is derived from the proposals stream itself.

**THE FUZZER FOUND A SAFETY VIOLATION:**

```
Got: [CommitNotification { view: 0, slot: 2 }, CommitNotification { view: 1, slot: 2 }]
```

Both views committed slot 2! The race: view 0's proposals arrive and get acked in
one tick, before view 1's proposals arrive to trigger the fence. The fence was
reactive (updated by seeing proposals) rather than preemptive (established before
proposals can be acked).

### The fix: Paxos Phase 1

You need PREPARE/PROMISE before proposing. The new leader must get f+1 members to
promise "I won't ack anything from a lower view" BEFORE it starts proposing. This
locks out the old leader — even if its proposals are in flight, at most f members
will ack them (not enough for quorum).

This is literally why Paxos has two phases. Phase 1 isn't ceremony — it's the
thing that makes safety work under concurrency. The Hydro simulator proved this
to us empirically.

## What This Demonstrates About Hydro

1. **The type system gives per-piece guarantees for free.** Each `broadcast_from_member`
   call produces an EC-typed stream with zero manual proofs on consistency.

2. **The type system CANNOT verify composition safety.** EC on individual broadcasts
   doesn't mean the protocol is safe overall. Slot conflicts are a semantic property
   that depends on protocol logic, not just network delivery guarantees.

3. **The simulator catches composition bugs.** The fuzzer explored an interleaving
   where the naive protocol violated safety — something no amount of type-level
   reasoning would catch.

4. **Types + simulator = powerful combination.** Types give you confidence about
   individual pieces. The simulator gives you confidence about their interaction.
   Neither alone is sufficient.

## Current State of the Code

### Files
- `hydro_lang/src/live_collections/stream/networking.rs` — `broadcast_from_member` primitive
- `hydro_test/src/cluster/primary_backup.rs` — protocol building blocks + tests
- `hydro_test/src/cluster/ISSUE_EC_SENDER_FAILURE.md` — open question about EC under sender failure
- `hydro_std/src/quorum.rs` — fixed NoTick → TopLevel

### Tests (8 passing before phase-1 rewrite)
1. `test_single_view_proposals_are_ec` — compile-time EC type check
2. `test_commit_notifications_are_ec` — compile-time EC type check
3. `test_proposals_arrive_at_cluster_members` — sim: all members get proposals
4. `test_two_views_compose_correctly` — sim: sequential views, gap-free
5. `test_committed_log_is_ec` — sim: full commit path, single view
6. `test_two_view_committed_log_is_ec` — sim: full commit path, two views
7. `test_concurrent_leaders_fencing` — sim: naive fencing (FOUND THE BUG!)
8. `test_quorum_read_recovers_correct_slot` — sim: quorum read dataflow

### What's broken / in progress
- `fenced_proposals` was deleted and replaced by `phase1_prepare` + `fenced_ack_filter`
- The concurrent/failure tests need rewriting to use prepare/promise
- `two_view_protocol` function references deleted code
- File doesn't currently compile due to stale references

### Next steps
1. Fix compilation — remove stale references to `fenced_proposals`
2. Rewrite `test_leader_failure_mid_view` with proper phase-1 fencing
3. Verify the fuzzer NO LONGER finds the safety violation
4. This would demonstrate: types give EC per piece, phase 1 gives safety across
   pieces, simulator validates the composition

## Manual Proofs in the Final Protocol

- **Commutativity of ack counting** (trivial)
- **Commutativity of max** (trivial)
- **"Deterministic filter of EC is EC"** on the fenced ack filter (small, auditable)
- **Quorum intersection** — "f+1 responses capture full committed prefix" (the one semantic claim)

Everything else is inferred by the type system.

## Open Issues

1. **EC under sender failure** — see `ISSUE_EC_SENDER_FAILURE.md`. Does `fail_stop`
   EC hold when the sender crashes mid-broadcast? The protocol handles this via
   quorum recovery, but the raw primitive's claim may be too strong.

2. **Exhaustive exploration** — the concurrent-views scenario is too large for
   exhaustive simulation. Fuzz testing gives high confidence but not completeness.

3. **Dynamic start_slot** — `propose_in_view` takes a compile-time `usize` for
   start_slot. A real protocol needs this to come from the quorum read (a runtime
   `Singleton<usize>`). Requires restructuring to pass a Singleton.
