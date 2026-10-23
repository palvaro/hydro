# Cress on Raft: a first schedule-amplification check of `raft_server`

**Date:** 2026-10-15. **Tree:** branch `cress_n_sedge` plus the changes below. **Question:** does
the schedule alone make the Raft implementation in `hydro_test/src/cluster/raft.rs` do work its
input did not require, and if so at which of its four `use::batch` hooks? Two mechanisms were
named before the run as candidates: (a) `AppendEntries` is re-sent on every heartbeat until
acked, so a slow follower makes the leader send more; (b) election timers that fire while a live
leader's heartbeats are delayed start elections. This document records what was annotated, the
verdicts verbatim, the curves and the conservation table, and the reading the numbers support.

## 1. What was added

Nothing in the protocol changed. `raft_step`, `raft_server` and `raft` are as they were.

- `RaftOutputs` derives `cress::SimOutputs` under `cfg(feature = "cress")`. Its `committed`
  field lives on `Atomic<Cluster<'a, Tag, EventualConsistency>>`, which the derive's field
  bound (`Stream<..>: SimOutputs`) did not cover; `cress/src/harness.rs` gains one impl for
  `Stream<T, Atomic<L>, B, O, R>` that calls `end_atomic()` and sinks the result, so the derive
  handles the struct unchanged. `redirected` on `Cluster<.., NoConsistency>` and `leader_views`
  on `Cluster<..>` were already covered (the cluster impl is generic over the consistency
  parameter).
- `RaftRpc`, `AppendEntriesRequest`, `RequestVoteDto` and `RequestVoteResponseDto` gain
  `PartialEq`/`Eq`. The simulator's scripted hooks, which Cress binds to every batch, require
  `T: PartialEq` on the item (to honour a `Values(..)` decision), and the intra-cluster traffic
  item is `(MemberId<Replica>, RaftRpc<String, Replica>)`. The two generic types get manual impls
  in the style of their existing `Clone`/`Debug`, so `ClusterTag` carries no bound.
- A wrapper `raft_check` (`hydro_test/src/cluster/raft.rs:1169`, `cfg(feature = "cress")`) fixes
  what the attribute cannot name: `T = String`, `O = TotalOrder`, the `Replica` tag,
  `net = TCP.fail_stop().bincode()`, and the `nondet!` proof. It calls `raft_server` and
  returns its `RaftOutputs`. Three `amplification_check` configurations sit on it, all with
  `cluster = 3`, `cluster_size = 3`, `workload_rounds = 125`, `skip_consistency_assertions`
  (the `committed` stream carries `assert_has_consistency_of`), and a `round` closure. A round
  is one element of the heartbeat clock on every member.

  | configuration | requests | election timer |
  |---|---|---|
  | `steady_leader` | one per round to member 0, rounds 1..124 | member 0 in round 0, never again |
  | `contested` | one per round to member 0, rounds 1..124 | every member, period 6, member `m` in rounds `r` with `(r + 2m) % 6 == 0` |
  | `contested_broadcast` | one per round to **every** member, rounds 1..124 | as `contested` |

  Member 0's timer fires in round 0 in every configuration (in `contested` because `0 % 6 == 0`),
  which is how the leader is chosen, before any request. Requests start in round 1 so that in
  the unheld run every request reaches a leader. The `round` closure can say "fire once", so
  `steady_leader` has no further elections at all rather than a long period. The 2-round stagger
  in `contested` stands in for Raft's randomized timeouts: with equal timers every election is a
  three-way split and nobody is ever elected. `contested_broadcast` was added after the first
  run, to separate what a leader change does to the protocol from what it does to a client that
  keeps sending to the deposed leader.

Command: `./scripts/cress hydro_test raft_check`. Each configuration takes 50–85 s after the
build. Horizon 1000 rounds; holds begin at round 1; finite holds that outlast the input are 128,
256 and 512.

The decision points Cress found, by line (`hydro_test/src/cluster/raft.rs`):

| line | hook |
|---|---|
| 939 | `request_batch`, batch of `String` |
| 944 | `election_batch`, batch of `()` |
| 950 | `heartbeat_batch`, batch of `()` |
| 955 | `traffic_batch`, batch of `(MemberId<Replica>, RaftRpc<String, Replica>)` |
| 1083 | the `.atomic()` on `committed`, batch of `LogEntry<String>` |

## 2. Verdicts

```
✓ BENIGN  raft_check (steady_leader)  growth: -  multiplier: -  location: -
    outputs: conserved: committed, leader_views; silent: redirected
! HAZARDOUS  raft_check (contested)  growth: grows  multiplier: 2.00x  location: hydro_test/src/cluster/raft.rs:955, batch of (MemberId<Replica>, RaftRpc<String, Replica>)
    outputs: amplified: redirected, leader_views; shed: committed
! HAZARDOUS  raft_check (contested_broadcast)  growth: grows  multiplier: 2.00x  location: hydro_test/src/cluster/raft.rs:955, batch of (MemberId<Replica>, RaftRpc<String, Replica>)
    outputs: amplified: redirected, leader_views; shed: committed
```

Unheld run, all three configurations: 4000 network messages over 1000 rounds (2 `AppendEntries`
from the leader and 2 replies per round; member 0 sends 2000, members 1 and 2 send 1000 each),
`committed` = 372 = 124 requests × 3 members, `leader_views` = 6 (one view per member for the
candidacy and one for the result). `redirected` is 0 when requests go to member 0 only and 248 =
124 × 2 when they are broadcast.

## 3. Curves

### `steady_leader`

Every hook: "no extra work at any hold length". Sends stay at 4000 under every hold of every
hook, including the hold of the traffic batch (955) to the end of the run. `committed` is 372
under every finite hold of every hook and 0 only under the never-ending holds of the request
batch, the heartbeat batch, the traffic batch and the committed batch itself (records never
delivered cannot commit; the election batch held forever changes nothing, 372).

### `contested`

Extra work over the unheld run. The request batch (939), the election batch (944) and the
committed batch (1083) show no extra work at any hold length.

```
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
950, batch of ()  (heartbeat timer)
    extra records admitted               -4   -377   -382   -391   -410   -447   -522   -671   -970  -1567  -5702
    extra messages, busiest sender       -1    997    994    989    978    957    914    829    658    317   -333
    extra messages, all senders          -4     -5    -10    -19    -38    -75   -150   -299   -598  -1195  -2333
    beyond the input (128, 256, 512): sends from @2 keep growing, rate 1.57 per round of delay
    multiplier: 2.00× the unheld run's 1000 sends from @2 (+997 at a hold of 2 rounds)
955, batch of (MemberId, RaftRpc)  (intra-cluster traffic)
    extra records admitted                0   -369     -6   -365      2   -349     34   -285   -204    -29  -4368
    extra messages, busiest sender        0    999      1    995      5    979     21    915     85    659      0
    extra messages, all senders           0      3      0      7      8     23     40     87    168    343  -1332
    beyond the input (128, 256, 512): sends from @2 keep growing, rate 2.24 per round of delay
    multiplier: 2.00× the unheld run's 1000 sends from @2 (+999 at a hold of 2 rounds)
```

Busiest sender is member 2 (`Cluster(loc1v1) @2`) throughout.

### `contested_broadcast`

The traffic-batch rows are identical to `contested` to the record. The heartbeat-batch rows:

```
950, batch of ()  (heartbeat timer)
    extra records admitted               -4    -11    -28    -42    -84   -166   -308   -712  -1038  -1672  -5888
    extra messages, busiest sender       -1    997     -2    987     -6    947    -22    787    -86    147   -334
    extra messages, all senders          -4     -5    -16    -21    -48    -85   -176   -340   -666  -1300  -2519
```

## 4. Conservation

| configuration | stream | kind | unheld | finite holds | hold to the end | largest at |
|---|---|---|---|---|---|---|
| `steady_leader` | committed | conserved | 372 | 372 | 0..372 | - |
| | redirected | silent | 0 | 0 | 0 | - |
| | leader_views | conserved | 6 | 6 | 4..6 | - |
| `contested` | committed | **shed** | 372 | 0..372 | 0..372 | - |
| | redirected | amplified | 0 | 0..122 | 0..82 | 950 at a hold of 2 |
| | leader_views | amplified | 6 | 6..1030 | 6..2000 | 950 at a hold of 512 |
| `contested_broadcast` | committed | **shed** | 372 | 0..372 | 0..372 | - |
| | redirected | amplified | 248 | 248..309 | 0..309 | 950 at a hold of 128 |
| | leader_views | amplified | 6 | 6..945 | 6..1834 | 950 at a hold of 512 |

`committed` by hold length, the two hooks that move it (the other three keep it at 372 under
every finite hold):

```
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
contested            950 heartbeat       372      0      0      0      0      0      0      0      0      0      0
                     955 traffic         372      0    366      0    366      0    366      0      0      0      0
contested_broadcast  950 heartbeat       372    366    360    351    336    291    240      0      0      0      0
                     955 traffic         372    366    366    348    366    276    366      0      0      0      0
```

`leader_views` under the heartbeat hold (950): 6, 10, 14, 22, 38, 70, 134, 262, 518, 1030, 2000
in `contested`; under the traffic hold (955): 6, 10, 13, 12, 17, 20, 33, 52, 97, 180, 337.

## 5. Reading

Numbers first; the mechanism named at each point is the one the code and the curve agree on.

**Mechanism (a), `AppendEntries` re-sends, is not visible to Cress, and `steady_leader` is
benign for that reason.** The leader sends exactly 2 `AppendEntries` per heartbeat whatever is
acked: under a hold of the traffic batch for k rounds the message count is the unheld run's at
every k (0 extra, 4000 sends, all holds). What grows is the `entries` field of each message:
`next_index` does not advance while the replies are held, so the message in round r of the
hold carries every entry appended since the hold began, r of them (`raft.rs:752–777`). Cress
counts records where they enter a tick and cross the network; the payload is "work represented
as a number inside a record" and is outside its measure (crate docs, "What this does not
see"). The benign verdict for `steady_leader` is therefore the statement that no *count*
reacts, which is true, and says nothing about bytes. Reading the count as the cost of
mechanism (a) would be a mistake.

**The headline multiplier is a change of sender, not extra work.** Both `contested`
configurations report 2.00× at a hold of 2 rounds on the heartbeat batch (+997 sends from
member 2) and on the traffic batch (+999). At the same holds the all-senders row is −5 and +3.
Member 2 became the leader and sends the 2 `AppendEntries` per round that member 0 sent in the
unheld run; member 0's 2000 sends became 1000 replies. Cress reads sends per sender by design
(a cluster total can hide one member's rise), and here that design produces a 2.00× whose total
is flat. The alternation in the traffic-batch row (999, 1, 995, 5, 979, 21, 915, 85, 659) is the
same fact: whether member 2 or member 0 holds the lead when the hold ends flips with the hold
length, and the row reports member 2 either way.

**Why a 2-round delay changes the leader at all.** A hold of 1 round on either hook does
nothing (−1 send). A hold of 2 rounds, beginning at round 1 as every Cress hold does, deposes
member 0 in both `contested` configurations. Member 0 wins the election in round 0 but sends no
heartbeat in that round: its heartbeat element of round 0 is consumed by the same tick as the
election element, before the votes return (`raft_step` evaluates (e) with `role == Candidate`).
So the followers have seen no `AppendEntries` when member 2's timer fires in round 2, and a
heartbeat (or traffic) delay covering rounds 1 and 2 is enough for `heartbeat_seen` to be false
(`raft.rs:693–719`). In steady state the same timer would tolerate a delay of up to 5 rounds.
Cress measures the delay the program tolerates at round 1; for this program that is the
start-up window, and it is 1 round.

**Mechanism (b), election churn, is what grows, and it grows linearly.** Under a hold of the
heartbeat batch the leader sends no `AppendEntries`, so every election timer that fires during
the hold is unsuppressed. Each firing raises the term, and every member records the new term as
a view transition when the `RequestVote` reaches it, so `leader_views` rises by about 2 per
round of hold, about 4 per firing (134 at 64, 262 at 128, 518 at 256, 1030 at 512, 2000 under
the hold that never ends), and the extra sends from member 2 beyond the input fit at 1.57 per
round of delay. Total traffic *falls* under this hold, 2.33 messages
per round of hold (−299 at 128, −598 at 256, −1195 at 512): 4 replication messages per round
are replaced by about 1.7 vote messages. Under a hold of the traffic batch the members keep
sending but nothing is delivered: the leader's 2 `AppendEntries` per round continue, the
followers' replies stop, and each follower's timer fires every 6 rounds with 2 `RequestVote`s;
after release the backlog is processed at once and a new term is settled. The all-senders row
grows with the hold at about 0.67 per round (87 at 128, 168 at 256, 343 at 512), and Cress fits
the busiest sender at 2.24 per round beyond the input with "no ceiling within the run". That
is the shape of a protocol that keeps electing for as long as heartbeats do not arrive, which
is the behaviour Raft specifies; the cost per round of delay is bounded by the timer period
and is small next to the replication it displaces.

**`committed` is not conserved, and this is the important line.** `steady_leader` conserves
it: 372 under every finite hold of every hook. Both `contested` configurations shed it, and the
two configurations separate three causes.

1. *A client pinned to a deposed leader.* In `contested`, every hold that leaves member 2 as
   leader gives `committed` = 0 and `redirected` = 122 (hold 2) or less: the remaining requests
   reach a follower and are redirected with the hint `Some(member 2)`. In `contested_broadcast`
   the same holds give 366 and 360. This is the harness's client, which does not follow the
   hint; it is not Raft.
2. *Entries appended by a leader deposed before replicating them.* In `contested_broadcast`
   under the heartbeat hold, `committed` is 366, 360, 351, 336, 291, 240 at holds 2, 4, 8, 16,
   32, 64: 2, 4, 7, 12, 27, 44 requests fewer than 124, on all three members alike. These are
   the requests the leader of the moment appended to its own log while its heartbeats were
   held; the next leader's log wins and they are truncated (`raft.rs:605–628`). They were
   never committed and the client was never told they were; `redirected` rises by only 0 to 33
   over the same holds (248, 251, 251, 257, 263, 281), so most of them were not redirected
   either, because at the time of arrival some member did lead. This is Raft's stated
   semantics for uncommitted entries.
3. *Nothing commits after a leader change once the input is over.* Under every hold of the
   heartbeat batch or the traffic batch of 128 rounds or more, in both `contested`
   configurations, `committed` is 0 on every member for the remaining ~870 rounds, although
   the leader of the final term heartbeats every round after release and its followers reply.
   The same holds in `steady_leader` give 372, and the shorter holds that changed the leader
   while data still flowed give 366. The commit rule (`raft.rs:725–748`, §5.4.2) advances
   `commit_index` only through an entry of the leader's *current* term; the leader at release
   was elected after round 124 and has no such entry; no request arrives to supply one; so the
   entries it does hold, replicated and acknowledged, never commit. The Raft paper's remedy
   (§8) is a no-op entry appended on election; this implementation has none. The effect is a
   liveness gap, not a safety fault: Cress counts records and cannot see whether the 372
   records agree, but every member reaches the same count under every finite hold in
   `steady_leader`, and in the shed cases all three members fall together.

**The location Cress names is right for what it measures, and the reader should move it.**
The traffic batch (955) is reported because it ties the heartbeat batch (950) at a first
reaction of 2 rounds and has 999 extra against 997. Both hooks are the same edge of the same
loop: a held heartbeat timer stops the leader's `AppendEntries` at the source, a held traffic
batch stops them at the receiver, and both unsuppress the election timer at `raft.rs:693`.
That line, not either batch, is where the program's response to lateness begins.

## 6. What is left

- Mechanism (a) wants a measure Cress does not have: bytes on the wire, or entries per
  `AppendEntries`. Exposing `entries.len()` per message as a numeric output stream would make
  it a column in the conservation table (as a "level" it would be read per tick, which is not
  the right shape; a sum per run is what is wanted).
- The start-up sensitivity (a 1-round tolerance at round 1 against 5 in steady state) is a
  property of `hold_start = 1`. A second sweep with holds beginning after the first heartbeat
  has landed would measure the steady-state tolerance; Cress deliberately has no such knob.
- Point 3 of the `committed` reading should be confirmed in the simulator with a direct test:
  elect, append, depose, re-elect, stop sending, and assert that `commit_index` stays where it
  was until the next request. If confirmed, the no-op entry is a protocol change and belongs
  to whoever owns `raft_step`, not to this check.
