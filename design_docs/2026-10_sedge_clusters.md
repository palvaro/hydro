# Sedge on clusters: the relay, `gossip_with_resend`, and a first run on Raft

**Date:** 2026-10-15. **Tree:** branch `cress_n_sedge` plus `2026-10_sedge_rounds.md`'s changes
plus the changes below. **Question:** can Sedge check a program whose locations are
`Cluster<'a, X>` rather than `Process<'a, X>`, and does the deployed `gossip_with_resend` behave
as Cress's 8.97× multiplier and its own doc comment predict? Then, with whatever time was left, a
first Sedge run on `raft_server`.

Sedge as left by `2026-10_sedge_rounds.md` rejected clusters at the macro (section 5 there).
This document records what was blocking, what was changed, the conservation table Cress writes
for the gossip program, what Sedge then reads on localhost at factors 2 and 4, and the Raft run.

## 1. What was blocking, and what was changed

Three things stood between Sedge and a cluster, one of them in `hydro_lang` and left there.

1. **The macro.** `sedge_macro`'s `as_process` returned a hard error on `Cluster`. It is now
   `as_location`, classifying `Process` and `Cluster` the way `cress_config_macro` does: a cluster
   parameter `x` is created with `flow.cluster::<X>()`, deployed with `.with_cluster(&x, N
   localhost hosts)`, and takes its size from the attribute as `x = N`, the same entry Cress's
   attribute takes, so one attribute body (minus `mu`) serves both tools. The `round_data`
   closure's `inputs` has a `sedge::ClusterRoundSender<T>` field per cluster data stream, with
   `send(member, item)` like Cress's `SimClusterSender`, and a `<cluster>_members: u32` field per
   cluster. With `load = param` on a cluster stream, every member gets one `LoadItem` per round.
2. **No channel between an external process and a cluster, deployed.** `hydro_lang` has
   `e2m`/`m2e` IR nodes (`source_external_bincode` on a cluster, and the reverse), but the
   `HydroDeploy` backend's `e2m_source`, `e2m_connect` and `m2e_sink` are `todo!()`
   (`hydro_lang/src/compile/deploy_provider.rs:199–239`); only the simulator implements them.
   Sedge therefore deploys **one relay process** (`sedge::SedgeRelay`, a marker type in
   `sedge/src/program.rs`) whenever some stream lives on a cluster, and routes through it:
   - *into* a cluster: the harness's port is `relay.source_external_bincode::<(u32, T)>`, and
     the relay maps the raw id to a `MemberId` and `demux`es over TCP
     (`sedge::cluster_load_input`). Per member the stream is `TotalOrder`/`ExactlyOnce`, as the
     process form is, and a program that asks for less weakens it.
   - *out of* a cluster: `Observer::stream` now takes any `Observable`, a trait with three impls:
     `Stream<T, Process<..>>` as before, `Stream<T, Cluster<'a, L, C>>` for any consistency `C`,
     which does `.send(&relay, TCP.fail_stop().bincode())` (the relay receives a
     `KeyedStream<MemberId<L>, T>`), tags each record with `member.get_raw_id()` and sends the
     `(u32, T)` pair to the external port, and `Stream<T, Atomic<L>>` for any observable `L`,
     which calls `end_atomic()` first (Raft's `committed` output). `Observation` and the
     recorder's `Sample` gain a `member: Option<u32>`; a **level from a cluster is kept per member
     and reported as the sum** of the members' latest values, which is the simulator tests'
     `inbox_total`. Counts are over all members.

   The relay was chosen over implementing `e2m`/`m2e` in the deploy backend because it needs no
   change to `hydro_lang` or `hydro_deploy`, the deploy backend's external-port plumbing is
   per-process (one TCP server port per external connection, registered on the external node),
   and the question here was whether Sedge can read a cluster at all. The cost is one more
   process and one more TCP hop on every observed record and every input record; at the rates
   below (≤ 1700 records/s on the wire stream, ≤ 400 input records/s) it did not show.
3. **Timers on a cluster.** A timer is a `()` stream the harness drives over a port; on a cluster
   it goes through the relay like data, so the port takes `(member, ())`. `TimerDrive` now
   carries `members`, and `drive_timer` fires each member once per period with the members
   **staggered by `period / members`** (a process is a cluster of one, phase zero). This is the
   deployment's deterministic form of independent per-member clocks, and for Raft the form of
   its randomized election timeouts: with three members and a 300 ms election timer, member `m`
   fires at `100·m + 300·k` ms, which is `raft_check`'s `contested` closure (`(r + 2m) % 6 == 0`)
   in wall-clock time. The `--dip` stretch applies per member as before. The gossip trials in
   section 3 ran before the stagger existed, with all five members ticked at the same instant;
   the Raft trials in section 4 ran with it.

Two smaller changes came out of the gossip run and are not cluster-specific:

- **Conserved goodput is read per offered round.** Without ids, goodput was the tail/baseline
  ratio of a conserved stream's *rate*. The baseline window is 5 s; at ρ = 12.5 rounds/s that
  is ~62 Poisson arrivals, a 13 % standard deviation, and at ρ = 6.25 it is 18 %, so a healthy
  program could read as DRAINING on arrival noise alone. `StreamWindow` gains `per_round`
  (records / rounds offered in the window), and the conserved ratio is now
  `tail per round / baseline per round`, which the workload fixes. Displayed as
  `wire (conserved): 241.7/s, 20.00 per round`. The rate is still what the verdict text shows for
  amplified streams, whose thresholds (1.5×, 3×) are wide enough.
- **`derive(SedgeOutputs)` on a generic struct.** The derive now emits a `where` bound
  `FieldType: Observable<'a>` per field (as Cress's derive does), so `RaftOutputs<'a, T,
  ClusterTag>` derives without `T: Serialize` on the struct itself.

A dip run's trace file now carries the dip in its name (`...trial0.dip-heartbeat_timer_interrupts
x8.tsv`); before this the Raft dip run overwrote the burst run's traces at the same factor, which
cost one reading below.

`cargo check -p hydro_test --features cress,sedge` is clean; `cargo test -p sedge` passes
(11 tests; two new: cluster levels summed over members, conserved goodput read per round).

## 2. The conservation table for `gossip_with_resend`

The Cress attributes on `gossip_with_resend` were converted to the split form so that the Sedge
attributes could carry the same text: `timers(timer = Duration::from_millis(50))` and

```rust
round_data = |round, inputs| {
    for m in 0..inputs.cluster_members {
        inputs.local_updates.send(m, round as u64 * inputs.cluster_members as u64 + m as u64);
    }
},
```

Cress's results are unchanged by the conversion: `resend_on` HAZARDOUS, grows degree 2, 8.97× at
the clock batch (`gossip_resend.rs:190`); `resend_off` BENIGN. Five members, data in rounds
0..125, horizon 1000. Columns: unheld count; range over finite holds; range under the hold that
never ends (`design_docs/sedge_corpus/gossip_conservation.tsv`).

| stream | `resend_off` | `resend_on` |
|---|---|---|
| `wire` | **conserved** 2500; 2500; 20..2500 | **amplified** 2500; 2500..18917; 20..22430 |
| `merged` | conserved 2500; 2500; 10..2500 | amplified 2500; 2500..12690; 10..22420 |
| `acks_sent` | conserved 2500; 2500; 10..2500 | amplified 2500; 2500..12690; 10..22420 |
| `inbox_depth` | level (one per tick execution per member) | level |
| `outstanding_depth` | level | level |

The brief for this work expected no conserved stream ("an update propagates; it doesn't
complete"). The table says otherwise for `resend_off`: 125 updates × 5 members × 4 peers = 2500
deltas on the wire, 2500 merges and 2500 acknowledgements under *every* finite hold, which is
the program's doc comment ("every delta is sent once per peer and merged once per member under
any schedule") as a measurement. Turning re-sending on makes the same three streams amplified
(up to 7.6× on the wire under a 512-round hold of the clock). So `resend_off` is judged by
conserved goodput and `resend_on` by amplification, both from the same five output fields, with
no `Observed` impl written: `Outputs` just derives `SedgeOutputs`.

## 3. Sedge on `gossip_with_resend`

Deployed constants (doc comment on the function): 50 ms ticks, `max_merges_per_tick = 5`
→ 100 merges/s per member; a round is one update at every member = 4 deltas received per member
→ **µ = 25 rounds/s**; `ack_timeout_ticks = 3` = 150 ms against a healthy acknowledgement of one
or two ticks. The rate model, written before the run: once the trigger has built inboxes deeper
than the timeout, every member re-sends one delta per peer per tick (80/s into each member) on
top of `4ρ` new deltas; the inboxes keep growing while `4ρ + 80 > 100`, i.e. while ρ > 5
rounds/s, so **f_c = 5**: factors 2 and 4 should collapse, 8 should recover. Cress's 8.97× > f
predicts the same for 2 and 4. Default profile (10 s at ρ, 5 s at 2µ = 50 rounds/s, 45 s at ρ),
one trial per factor, seed 1, kinds from the table above. Six processes on localhost: five
members and the relay. First deploy build 9 min; each trial 60 s + 2 s grace.

| configuration | f=2 (ρ = 12.5) | f=4 (ρ = 6.25) |
|---|---|---|
| `resend_off` | **RECOVERED** | **RECOVERED** |
| `resend_on` | **COLLAPSED** | **COLLAPSED** |

The verdicts' reason lines, verbatim:

- `resend_off` f=2: `RECOVERED: tail goodput is 97% of baseline, no abandonments, no amplified
  stream`. Baseline [5, 10) s: `wire` 216/s, 20.00 per round; `acks_sent`/`merged` 223/s, 20.65
  per round; levels 5 → 0. Tail [37, 60) s: `wire`, `merged`, `acks_sent` all 241.7/s, 20.00 per
  round; inbox total 13 → 10.
- `resend_off` f=4: `RECOVERED: tail goodput is 100% of baseline, no abandonments, no amplified
  stream`. 20.00 per round on all three in both windows; inbox total 10 → 7.
- `resend_on` f=2: `COLLAPSED: inbox_depth, outstanding_depth still rising through the tail with
  goodput at 100% of baseline, wire at 641.7/s against 225.4/s`. Tail: `merged` and `acks_sent`
  500.0/s (= 5 × 100, the whole merge capacity), inbox total 8899 → 10483, outstanding total
  3168 → 3864.
- `resend_on` f=4: `COLLAPSED: wire at 524.3/s against 136.0/s through the tail, goodput at 100%
  of baseline, inbox_depth, outstanding_depth still rising`. Tail: `merged` 500.0/s, inbox total
  5624 → 5932, outstanding 1361 → 1435.

Per-second traces (`target/sedge/traces/`, columns `wire_n`, `merged_n`, `inbox_depth_v`):

| configuration, f | trigger end (s 14) | s 19 | s 29 | s 39 | s 59 | inbox slope in the tail |
|---|---|---|---|---|---|---|
| `resend_off`, 2 | inbox 3365, merged 500/s | 1785 | 0 | 30 | 0 | drained at ~300/s (500 − 4ρ·5), empty by s 29 |
| `resend_off`, 4 | 2885 | 1245 | 0 | 20 | 0 | drained at ~375/s, empty by s 24 |
| `resend_on`, 2 | 5301, wire 1660/s | 5721, wire 560/s | 7361 | 8561 | 11361 | **+141/s** (model: (4·12.5 + 80 − 100)·5 = 150/s) |
| `resend_on`, 4 | 4795 | 5155, wire 660/s | 5315 | 5555 | 5975 | **+20/s** (model: (4·6.25 + 80 − 100)·5 = 25/s) |

Readings:

- **All four verdicts agree with the labels, with Cress's multiplier and with the rate model.**
  With re-sending on, `merged` sits at exactly 500/s through the tail at both factors (every
  member merging its full budget every tick), the wire carries 4ρ·5 new deltas plus ~400/s of
  re-sends (the program's cap of one per peer per tick: 5 × 4 × 20), and the inbox total grows at
  the rate the model gives within 10–20 %. At f = 4 the margin is 5 merges/s per member and the
  trace shows it: the inbox total wanders (5355 at s 24, 5295 at s 34) before resuming its rise;
  the verdict's halves (5624 → 5932) still read it as rising. A run at f = 8 (ρ = 3.1,
  4ρ + 80 = 92.5 < 100) is the direct test of f_c = 5 and was not run for time.
- **The amplification and the level agree on the mechanism.** At f = 2 the reason line leads
  with the rising levels (the amplification ratio of `wire`, 641.7 / (225.4 + 2) = 2.82, is just
  under the collapse factor of 3), at f = 4 with the wire (524.3 / 138 = 3.80). Both are the same
  state: merges at capacity, half of them redundant, acknowledgements never catching up.
- **Conserved goodput without ids works, per round.** `wire` reads 20.00 per round in every window
  of both `resend_off` trials (5 members × 4 peers, exactly). `acks_sent`/`merged` read 20.65 in
  the f = 2 baseline: a round's merges happen a tick after its sends, so a 5 s window picks up
  the previous second's merges at its start, and the 97 % is that edge effect, not lost work.
  Reading these by rate would have given 216 / 241.7 = 0.89 → DRAINING; this is why the per-round
  normalization was added.
- **The trigger tipped the system in one second**, as the program's doc predicts for "any
  trigger longer than about 5 rounds": at 50 rounds/s the inbox total was 821 one second in,
  and re-sends (`wire` 1296/s against 960/s of new deltas) had already begun.
- **Shutdown noise.** Every trial's log shows two or three cluster members panicking with
  `BrokenPipe` at the deployment's stop: a member killed first closes the TCP sinks its peers
  are writing to. The verdict is computed before `stop()` and the traces are flat to the end,
  so nothing was lost; the process-only programs did not show it because their only
  process-to-process channels were one-directional.

## 4. Raft

`raft_server` (`hydro_test/src/cluster/raft.rs`) is the second cluster program, and the first
whose outputs include an `Atomic` stream and a generic output struct. The parent's sibling run
(`2026-10_cress_on_raft.md`) had added `derive(SimOutputs)` and a `raft_check` wrapper with three
Cress configurations; this run adds `derive(sedge::SedgeOutputs)` on `RaftOutputs` and a
`raft_sedge_check` wrapper (`cfg(feature = "sedge")`) with the same pinning (`T = String`,
`TotalOrder`, `Replica`, `TCP.fail_stop().bincode()`), three members, `cluster_size = 3`, and two
configurations: **`pinned`** sends every request to member 0 (the pinned client of Cress's
`contested`), **`broadcast`** sends every request to every member (`contested_broadcast`), so the
leader commits each once, three commits per round over the members, and the two others redirect
it. Timers: heartbeats every 50 ms at every member; the election timer every 300 ms, the members
staggered by 100 ms (section 1, item 3): member 0 fires at t = 0, is the first leader, and a live
leader's heartbeats suppress the others. `mu = 200` rounds/s is nominal: the leader's cost per
request is a log entry, there is no per-tick cap, and the factor only scales the offered rate.

No Cress rows exist for `raft_sedge_check` (the table is keyed by function and configuration, and
Cress's are on `raft_check`), so all three outputs were read as unclassified work: the verdict
has only amplification and the levels to go on, and "goodput at 100 %" below means *no conserved
stream was known*, not that requests completed. Six trials, one each. Build 6 min.

**Load burst** (default profile, 2µ = 400 rounds/s for 5 s; `broadcast` offers 1200 requests/s
during it):

| configuration | f=2 (ρ = 100) | f=4 (ρ = 50) | tail detail |
|---|---|---|---|
| `broadcast` | RECOVERED | RECOVERED | `committed` 293/s against 281/s (3.0 per round), `redirected` 196/s (2.0 per round), `leader_views` 0 in both windows; f=4: 144/s against 161/s, 3.00 per round over the run |
| `pinned` | RECOVERED | RECOVERED | `committed` 293/s against 281/s, `redirected` 0, `leader_views` 0; f=4 trace: 14182 committed for 4730 rounds = 3.00, 6 view transitions in the whole run (the startup election) |

A load burst does not touch the heartbeats, which the harness drives, so no election happens and
the protocol commits every request. This is the result the mechanism predicts, and says nothing
about it.

**Capacity dip** (`--trigger-multiple 1 --dip heartbeat_timer_interrupts=8`, f = 2): for the 5 s
trigger window the heartbeat period is 400 ms, longer than the 300 ms election timeout, which is
Cress's hold on the heartbeat path in wall-clock form.

| configuration | verdict | reason line (verbatim) |
|---|---|---|
| `broadcast` | **DRAINING** | `goodput at 100% of baseline; committed at 635.5/s against 281.4/s` |
| `pinned` | **COLLAPSED** | `redirected at 98.5/s against 0.0/s through the tail, goodput at 100% of baseline` |

The traces (`design_docs/sedge_corpus/traces_clusters/raft_sedge_check.*.dip-heartbeat_x8.tsv`,
per second: rounds, `committed_n`, `redirected_n`, `leader_views_n`):

| s | `broadcast` | `pinned` |
|---|---|---|
| 9 | 97 rounds, committed 283, redirected 194, views 0 | 97, 283, 0, 0 |
| 10 (dip starts) | 186, **12**, 397, **13** | 186, 12, 98, 14 |
| 11–14 | ~205 rounds, **0 committed**, ~460 redirected, 26–29 views per second | ~205, 0, ~165, 20–28 |
| 15 (dip ends) | 98, 0, 203, 13 | 98, 0, 98, 6 |
| 16–36 | ~95 rounds, **0 committed**, 2.0 redirected per round, 0 views | ~95, **0**, 1.0 redirected per round (= every request), 0 views |
| 37 | 93 rounds, **committed 8094** | 93, 0 |
| 38–59 | 3.0 committed per round, 2.0 redirected | 0 committed, every request redirected, to the end |
| whole run | 6457 rounds; committed 17608 = 2.73 per round (**1763 never committed**); 143 view transitions | 6457 rounds; committed **2991** = 0.46 per round (**3466 of 6457 requests never committed**); 124 view transitions |

Readings:

- **The dip starts elections, as Cress said it would**: 13–29 view transitions per second for
  the six seconds the heartbeats are slow (a new term every ~100 ms, the staggered timers taking
  turns), then a stable leader from s 16 (`leader_views` 0 for the rest of the run). Cress's
  `contested` multiplier (2.00× at the traffic batch) is this: elections as long as the heartbeat
  path is held.
- **Then nothing commits for 27 seconds under a stable leader, and the whole backlog commits in one
  second.** In `broadcast`, requests keep arriving at the new leader (exactly one member accepts
  and two redirect from s 16 on), the leader commits none of them until s 37, and at s 37 it emits
  8094 entries (27 s × ~100 rounds/s × 3 members). The burst is what the verdict's `committed at
  635.5/s against 281.4/s` reads, and why it says DRAINING: the amplified stream is at 2.24×,
  under the collapse factor, no level is known. The shape — a long stall ending in a burst equal
  to the backlog — is what RAFT §5.3's back-off produces when the new leader's `nextIndex` for a
  follower is decremented one entry per rejected `AppendEntries` and an `AppendEntries` goes out
  once per heartbeat: ~540 heartbeats of 50 ms to walk back over the entries the deposed leader
  appended during the dip but never replicated, then one accepted `AppendEntries` carrying
  everything, and the commit index jumping with it. This is a hypothesis from the trace's shape;
  the member-level traces (section 5) would confirm it. Either way the deployment shows what the
  simulator's `committed = 0 under holds ≥ 128` hinted at (`2026-10_cress_on_raft.md`): a leader
  change makes the committed stream stall, there for the rest of the horizon, here for 27 s.
- **The 1763 entries that never commit** are the requests the deposed leaders accepted during the
  dip (s 10–15: ~1000 rounds, of which a third reached a leader) and the entries the back-off
  truncated. With `committed` known as conserved, this would read as 9 % lost work by count
  (Cress's `shed`) — the verdict did not see it because the kind was unknown and the tail window
  starts at s 37, after the burst.
- **`pinned` collapses the way its Cress label says it does**: once member 0 is deposed, every
  request to it is redirected and no request is ever committed again; 54 % of the run's requests
  are lost. The verdict says COLLAPSED for the right event (`redirected` from 0 to one per round,
  49×) and the wrong reason ("goodput at 100 %"), because `committed` was unclassified; with
  the Cress kinds the tail's conserved goodput would be 0 %. The pinned client's hint
  (`Option<MemberId>`) is in the redirected records and was not read.
- **Burst against dip.** On the gossip program the two triggers would tip the same state; on Raft
  only the dip reaches the mechanism, because load does not delay a harness-driven heartbeat. A
  deployment whose heartbeat is a `source_interval` in the leader's own process would be delayed
  by load; the harness's timer is a measurement choice (it is what makes the dip possible) and
  this is its cost.

## 5. What is left

- **Factor 8 on gossip**, the direct test of f_c = 5 (ten minutes). And the capacity dip
  (`--dip timer=K`) as the trigger, which is Cress's hold on the clock.
- **Cress's table for a Sedge wrapper.** `cress_kinds` matches rows by function and configuration
  name, so `raft_sedge_check`'s configurations do not find `raft_check`'s rows and Raft's outputs
  fall back to unclassified work; both Raft dip verdicts say "goodput at 100 %" over a 27 s and a
  50 s commit stall. Either the Sedge attribute should name the Cress function and configuration
  it corresponds to (`kinds_from = raft_check::contested_broadcast`), or the two tools should
  share one wrapper with both attributes, as the gossip program does. The Raft wrapper can be
  shared once Cress's `round` closures are rewritten in the `timers`/`round_data` form.
- **Lost work by count.** The rate verdict reads the tail window only. A conserved stream's total
  over the whole post-trigger period against the rounds offered (`committed` 2.73 per round
  against 3.00 here) is the deployment's version of Cress's stall column and would have caught the
  1763 lost entries whichever second the burst landed in. The id overlay does this already when
  records carry ids; `LogEntry` has an `index`, not the request's number.
- **Raft's 27 s commit stall after an election** deserves its own run: with per-member columns and
  a shorter dip (one or two missed heartbeats, not twelve) the back-off hypothesis above is a
  ten-minute test, and if it holds, the fix (the §5.3 fast back-off, or a no-op entry on election
  for the simulator's finding) is in the protocol, not the harness.
- **The relay is a cost and a hop.** Implementing `e2m_source`/`e2m_connect`/`m2e_sink` in the
  `HydroDeploy` backend would remove it; the external node's port registry is per-port, so an
  external-to-cluster connection would have to open one connection per member and the
  `ExternalBincodeSink` would need to take `(MemberId, T)`, which is the shape the relay already
  presents to the harness.
- **Per-member columns.** Levels are summed and counts totalled over members. The trace could
  carry `<stream>@<member>` columns beside the sum, which is what would show one member falling
  behind the others (a slow follower in Raft, a member whose inbox alone is growing).
- **The stagger is deterministic.** Cress's `contested` configuration and the harness's timer
  agree on it, but a real deployment's election timeouts are random, and a run with randomized
  phases would say whether the stagger hides or shows anything.
- **Edge effects in the baseline window.** The 20.65-per-round reading above is a one-tick lag
  across a window edge; a longer baseline (`--before 20`) or windows aligned to rounds would
  remove it.
