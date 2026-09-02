# Report: the Hydro simulator discovering Ongaro's 2015 membership-change bug

This document describes, precisely, what was tested, what the simulator was and was not
told, what it found, and how the finding was verified. It also corrects an earlier
overclaim made in conversation about this work (see "A correction" below).

Raw trace artifact referenced throughout: [`dyn_raft_bootstrapped_trace.txt`](./dyn_raft_bootstrapped_trace.txt).

## Background: the bug being sought

Diego Ongaro's 2015 dissertation describes a single-server membership-change algorithm,
and a safety bug in its original (pre-barrier) form: if a leader is allowed to append a
configuration-change log entry before it has committed an entry from its own current
term, two leaders in different terms can each append a *different* configuration change,
and each can get that change committed under a *different* quorum (the old configuration's
majority vs. the new configuration's majority). The result is two members permanently
disagreeing about what occupies the same physical log index — a violation of Raft's State
Machine Safety property. The fix ("the 2015 correction") is a barrier: a leader may not
append a configuration change until it has committed at least one entry from its current
term.

`hydro_test/src/cluster/dyn_raft.rs` implements this protocol twice from one shared step
function, `dyn_raft_step_with_policy`, gated by a `StepPolicy::require_current_term_commit`
flag:

- `dyn_raft_step` (`require_current_term_commit: true`) — the production path, always
  used by the public `dyn_raft`/`dyn_raft_server` functions. This keeps the barrier.
- `dyn_raft_step_unpatched_for_simulation` (`require_current_term_commit: false`,
  `#[doc(hidden)]`) — omits the barrier, and (as of this work) truncates conflicting log
  suffixes unconditionally, matching the dissertation's original, unbarricaded behavior
  verbatim. This exists only so simulation can demonstrate the counterexample; it is never
  reachable from the production API.

## Three tests, three different amounts of staging

All three tests drive the same doc-hidden simulation server,
`dyn_raft_server_unpatched_for_simulation`, over a **static** 4-member physical Hydro
cluster (`{0,1,2,3}`), through `flow.sim().fuzz(...)`. All three install the same oracle
(below). They differ only in how much of the scenario is pre-arranged by the test author
versus left to the simulator's schedule search.

| test | what is fixed by the test | what is left to the simulator | has it found the bug? |
|---|---|---|---|
| `simulator_discovers_ongaro_membership_bug` | Two full setup phases, each ended by a quiesce: (1) member 0 wins an uncontested election; (2) member 0 is sent `Remove(3)`. Only after that does a single un-quiesced burst run with member 1's election, `Remove(2)`, and heartbeats for both 0 and 1. | Within the final burst only: batching, message delivery order/timing, and vote outcomes. | Yes — found by coverage-guided fuzzing (`cargo sim`) after roughly half a million executions. |
| `simulator_discovers_ongaro_membership_bug_bootstrapped` | Only that *some* first leader exists: one election tick to member 0, one quiesce. Nothing else. | Everything after the bootstrap: which member leads which subsequent term, whether/when each of 24 copies of `Remove(2)`/`Remove(3)` (sent to *all four* members) is accepted or rejected, all replication and vote-message ordering and batching, across 2 further election waves and 6 heartbeat waves — all sent concurrently, with no quiescing until the very end. | **Yes** — found by coverage-guided fuzzing at **315,218 executions**. This is the strongest tier that has actually produced a reproducer. |
| `simulator_discovers_ongaro_membership_bug_unstaged` | Nothing protocol-specific. All four members receive symmetric inputs (election ticks, heartbeat ticks, copies of both removals) sent entirely up front, with **no quiescing at all** until a single final drain. | Everything, including which member(s) ever become leader at all. | Not yet. Plain `cargo test` (random, 8,192 iterations) does not find it. A coverage-guided `cargo sim` run reached 8.6 million executions and then hit libFuzzer's out-of-memory limit (2 GB) without finding it. This is marked `#[ignore]` and is not part of the default test suite. |

The bootstrapped test is the one worth trusting as the primary artifact, and is discussed
in detail below. The fully-staged test remains in the suite as a faster, more targeted
regression once the bug's shape is known. The unstaged test remains as an honest negative
result — see "What the simulator did *not* do."

## The oracle (identical in all three tests)

Hydro's simulator does not itself validate `assert_has_consistency_of` (the type-level
consistency claim on the production `committed` stream) — so this is an explicit,
hand-written runtime safety check in the test closure, evaluated after collecting every
member's committed-entries output stream:

```rust
let mut committed_at: HashMap<usize, DynLogEntry<String, Replica>> = HashMap::new();
for member in 0..N as u32 {
    for entry in committed_recv.collect::<Vec<_>>(member).await {
        if let Some(previous) = committed_at.get(&entry.index) {
            assert_eq!(
                previous, &entry,
                "committed log forked: physical index {} committed two different entries",
                entry.index
            );
        } else {
            committed_at.insert(entry.index, entry);
        }
    }
}
```

For every physical log index, every member that committed *anything* at that index must
have committed the *same* `DynLogEntry` (same term and payload) as every other member.
This is Raft's State Machine Safety property, checked directly against the physical log
index (not the application-visible command index), which is what the counterexample
actually violates. The test is written as `#[should_panic(expected = "committed log
forked")]`: the test "passing" means this assertion fired.

## What the bootstrapped test actually found

Verified reproducer replay (`cargo test --lib
cluster::dyn_raft::tests::simulator_discovers_ongaro_membership_bug_bootstrapped --
--ignored`, deterministic, ~2.4s):

```
assertion `left == right` failed: committed log forked: physical index 1 committed two different entries
  left: DynLogEntry { payload: Noop, term: 1, index: 1 }
 right: DynLogEntry { payload: Noop, term: 4, index: 1 }
```

Two members committed different entries at physical log index 1: one member's committed
history has a no-op from term 1 at that position; another member's committed history has a
*different* no-op, from term 4, at the same position. That is the fork — two members
permanently disagree about what is in slot 1 of the log, which is exactly the safety
violation the barrier exists to prevent. The cluster went through at least four terms to
get there, i.e. this took more than the minimal two-term counterexample from the
dissertation; the simulator found a longer path than the textbook one.

### Lineage: the scheduler trace

`HYDRO_SIM_LOG=1 cargo test --lib ... -- --ignored --nocapture` replays the same saved
reproducer with the simulator's built-in tick/release logging turned on. The full log is
committed at [`docs/dyn_raft_bootstrapped_trace.txt`](./dyn_raft_bootstrapped_trace.txt);
it is 955 lines and ends with the panic above. Two things about what this trace is and is
not:

- **What it is:** a literal, line-by-line record of the scheduler's decisions: 49
  `Running Tick (Cluster Member N)` entries (16 for member 0, 12 for member 1, 11 for
  member 2, 10 for member 3), each followed by which dataflow edges in the compiled
  program released values on that tick (e.g. "releasing items: [()]" for an election-timer
  tick, "releasing no items" when a batch was empty). This is a real, mechanical
  lineage — it is what the simulator did, in the order it did it, and it is what makes the
  reproducer byte string meaningful rather than opaque.
- **What it is not:** a semantic Raft trace. It does not print "member 1 became leader of
  term 2" or "member 0 truncated index 1" — those facts have to be inferred from which
  edges fired, and several of the multiset/unordered releases are printed redacted as
  `[?]` by the sim runtime itself (this is the runtime's own logging code, not a
  redaction I applied). Reconstructing the full leader/term timeline from this trace alone
  is possible in principle but was not done as part of this report; the report's claims
  are limited to what the trace and the final assertion directly show.

## A correction

Earlier in this work, the first (fully-staged) test's doc comment and my own summary
described it as letting the simulator discover the bug "on its own." That was not
accurate: two setup phases in that test (elect member 0; send it `Remove(3)`) are the
first two steps of Ongaro's counterexample, written by someone who already knew the
target shape. That test's doc comment has since been corrected to say exactly that.

The bootstrapped test is the honest response to that criticism: the *only* fixed step is
"some leader exists," which is not specific to the membership bug at all (every Raft
cluster needs a first leader before anything else can happen), and every subsequent
choice — who leads which term, when each of many redundant reconfiguration requests lands,
all message and batch ordering — was left to the simulator. It found the fork without
being told the two-configuration mechanism.

## What the simulator did *not* do

- It did not find the counterexample from a fully symmetric, zero-staging workload within
  the budgets tried here: 8,192 random schedules (`cargo test`, no reproducer) and 8.6
  million coverage-guided schedules (`cargo sim`, ~2 hours before hitting libFuzzer's
  memory ceiling) both came up empty on the `_unstaged` test. This is not evidence the bug
  is unreachable from that workload — it is evidence that, at the budgets available in
  this environment, blind search did not get there, while a one-step bootstrap did (at
  315k executions). The gap between "one trivial bootstrap step" and "nothing" suggests
  the reachable-but-rare regime, not an artifact of over-fitting the test to the known bug.
- It did not need any message loss, crash-fault injection, or lossy transport: all three
  tests use `TCP.fail_stop().bincode()`. Interleaving and batching alone are sufficient.
- It did not validate `assert_has_consistency_of` — Hydro's simulator does not check that
  annotation at all (hence the hand-written oracle above; the unpatched simulation path
  also does not attach that annotation to its output, since it would be false).
- Coverage-guided fuzzing (`cargo sim`) requires sanitizer-coverage/libFuzzer linking that
  was not available in every environment this work ran in; where unavailable, `cargo test`
  alone cannot discover new failures (see the `_unstaged` result), only replay previously
  saved ones.

## User story: how the bootstrapped test was actually written

The fully-staged test was written first, by deliberately scripting the first two steps of
the known Ongaro counterexample (elect member 0; give it `Remove(3)`) and letting the
simulator improvise only the racy remainder. When that framing was challenged — could this
test have been written by someone who *didn't* already know the bug? — the honest answer
was no.

The bootstrapped test was then written to close that gap, under a self-imposed rule: the
only thing the test is allowed to fix is whatever is needed for the cluster to do anything
at all (a first leader), and every input that follows must be symmetric across all four
members, so that no line of the test encodes which member ends up doing what. Concretely,
the intent going in was just "throw a first election, then throw a pile of removals and
timer ticks at everyone and let the fuzzer sort out the schedule" — an attempt to cover a
large swath of schedules cheaply, not a targeted reproduction. Two further election waves,
three redundant copies of each removal (addressed to every member, to survive whichever
member happens to be leader when they land), and six heartbeat waves were chosen simply to
give the search room, without regard for how the counterexample mechanism actually works.

It was launched under `cargo sim -p hydro_test -- \
simulator_discovers_ongaro_membership_bug_semi_staged --ignored` (the test's working name
before it was renamed to `_bootstrapped`) with a fresh, empty corpus. Coverage climbed from
2,505 edges at initialization to 2,885–2,886 edges over the run. It found the fork at
execution **#315,218**, about **86 seconds** of wall-clock fuzzing after the sancov-instrumented
binary started running (build/link time before that was several minutes and is not
counted). The resulting 144-byte reproducer was written automatically to
`hydro_test/src/cluster/sim-failures/simulator_discovers_ongaro_membership_bug_bootstrapped.bin`,
and every subsequent `cargo test` run replays it deterministically in about 2.4 seconds —
no fuzzing required after the fact.

## How to reproduce this yourself

```bash
# Deterministic replay of the saved failure (fast, no fuzzing):
cargo test --lib \
  cluster::dyn_raft::tests::simulator_discovers_ongaro_membership_bug_bootstrapped \
  -- --ignored

# The same, with the simulator's scheduler trace on stderr
# (produces output like docs/dyn_raft_bootstrapped_trace.txt):
HYDRO_SIM_LOG=1 cargo test --lib \
  cluster::dyn_raft::tests::simulator_discovers_ongaro_membership_bug_bootstrapped \
  -- --ignored --nocapture

# Re-run coverage-guided discovery from scratch (deletes the saved reproducer's
# effect by fuzzing fresh; requires a libFuzzer/sancov-capable toolchain):
rm hydro_test/src/cluster/sim-failures/simulator_discovers_ongaro_membership_bug_bootstrapped.bin
./cargo-sim sim -p hydro_test -- \
  simulator_discovers_ongaro_membership_bug_bootstrapped --ignored
```
