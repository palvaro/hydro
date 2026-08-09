# HANDOFF: Building EC from Composable Pieces

## Repo
`/Users/palvarox/code/hydro-clean`

## Read These First
1. `hydro_test/src/cluster/BRAINSTORM_EC_COMPOSITION.md` — full session history
2. `hydro_test/src/cluster/primary_backup.rs` — the code
3. `hydro_lang/src/live_collections/stream/networking.rs` — search for `broadcast_from_member`
4. `hydro_test/src/cluster/ISSUE_EC_SENDER_FAILURE.md` — open question for Shadaj

## What This Project Is

Building `EventualConsistency` up from composable typed building blocks rather
than asserting it by fiat on a monolithic consensus protocol. Each piece uses
`broadcast_from_member` (a new primitive we added) which gives EC inferred by
the type system. The composition of these EC pieces gives a full consensus
protocol whose committed log is EC.

## What Works (7 tests passing)

```bash
cargo test -p hydro_test --lib primary_backup
```

Sequential views, full commit path, quorum read, promise production all pass.

## What's Broken

`test_phase1_prevents_concurrent_commit` fails.

### The Bug

The promise quorum collection emits `max_committed + 1` as the start_slot for
view 1. In the test, `max_committed_per_member` is 0 for all members, so
start_slot = 1. But the test sends view 0 and view 1 both competing for slot 0.
View 1 actually proposes for slot 1 (not slot 0), so there's no conflict to
test. The assertion `commits[0].slot == 0` fails because the commit is for
slot 1.

### The Fix

Either:
- Change `max_committed_per_member` to send values that produce start_slot=0
  (but that doesn't make sense — if nothing is committed, start at 0)
- OR: the promise collection should NOT add +1. The start_slot should be 0
  when max_committed is 0 (nothing committed yet, start at beginning).
  Fix is in the test's `start_signal` sliced block: change `Some(max_s + 1)`
  to `Some(max_s)` — then both views compete for slot 0.
- OR: change the test to have view 0 commit slot 0 first, THEN view 1
  starts at slot 1, and test that view 0 can't commit slot 1 after the
  prepare. This is more realistic anyway.

### After Fixing the Start Slot

The REAL test is: does the fuzzer find a safety violation (both views committing
the same slot)? If phase 1 is wired correctly (proposals causally downstream of
promise quorum), it should NOT find one. If it does, the causal wiring is wrong.

## Key Architectural Decisions

1. `broadcast_from_member` — intra-cluster broadcast, EC inferred. Lives in
   `hydro_lang/src/live_collections/stream/networking.rs`.

2. Three EC broadcasts per view: Prepare, Proposals, Commits.

3. `fenced_ack_filter` — driven by Prepare stream (EC), not by proposals.
   Members only ack proposals with view >= max_promised_view.

4. `propose_in_view_gated` — buffers requests until start_signal fires
   (promise quorum reached). Ensures proposals are causally after phase 1.

5. Safety argument: once f+1 members promise view V, at most f will ack
   view < V. Old leader can't reach quorum. New leader's proposals only
   exist after promises are in. Therefore at most one view commits each slot.

## The Big Discovery

The Hydro fuzzer found a REAL safety violation when we tried naive fencing
(filtering proposals by max view seen in the proposal stream itself). Both
views committed the same slot. This proved that Paxos phase 1 is necessary —
you can't skip it. The fencing must be preemptive (established by promises)
not reactive (triggered by seeing proposals).

## Files Modified

- `hydro_lang/src/live_collections/stream/networking.rs` — `broadcast_from_member`
- `hydro_test/src/cluster/primary_backup.rs` — protocol + tests
- `hydro_test/src/cluster/mod.rs` — added `pub mod primary_backup`
- `hydro_std/src/quorum.rs` — fixed `NoTick` → `TopLevel<'a>`
- `lego_replicate/Cargo.toml` — version bump
- `hydro_transparent_replicate/Cargo.toml` — version bump
