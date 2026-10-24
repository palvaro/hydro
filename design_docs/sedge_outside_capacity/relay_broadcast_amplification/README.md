# `relay_broadcast`: does Cress's re-send amplification sustain a metastable collapse under Sedge?

This document has two parts.

**Part 1: the per-peer `ReliableResend` loop cannot sustain a collapse.** The program was fixed
so that a tick no longer costs O(unacked) (W1), and Sedge now reads the members' I/O from
outside them (W3).

- After that, the configuration that used to collapse at f ≤ 2 recovers from the same burst
  at every f tried: 1.25, 1.5, 2 and 4.
- It also recovers from a burst four times as long, which builds a 250 k-entry backlog.
- The re-sends Cress flags are real, and they stay high while a backlog exists. But they are
  capped at one per peer per timer tick, so they add a bounded, additive load: about
  270 re-sends/s across the cluster, about 4 % of the first relays at f = 1.5. That is far below
  the headroom.
- The earlier collapse (`../relay_broadcast/README.md`) was therefore sustained by the O(table)
  per-tick rebuild, as that document inferred. Removing the rebuild removes the collapse.

**Part 2: the per-entry variant, which re-sends every overdue relay on every timer tick, does
fall into a self-sustaining re-send storm.**

- Without a trigger it is stable at f = 1.5–4.
- After a 2μ, 5 s burst, in-band re-sends jump to 6–10 k/s at every f from 1.5 to 4, and the
  storm does not end.
- The storm is not visible on the wire. Member CPU stays pinned while the members' socket
  writes fall to about zero, and the re-sends pile up in unbounded user-space send buffers.
  RSS grows about 12 MB/s per member until the kernel OOM-kills a member at its 400 MB limit,
  30–45 s after the burst.
- An OOM kill is a **crash** outcome, not a metastable collapse, so Sedge now detects it and
  labels the trial INVALID.
- No recovery was seen at any f ≤ 4. The runs that would have looked for recovery, and for a
  bounded-buffer version that keeps the storm on the wire, were not run before the host was
  retired (see "Not run").

# Part 1: persistent table, out-of-band I/O, per-peer re-sends

## What changed

### W1: the outstanding table is persistent state

Before, the table was a tick-scoped `KeyedSingleton<(peer, id), (last_sent, body)>` carried
across ticks with `defer_tick`. Every tick rebuilt all of it (`kept.chain(resent).chain(new)
.first()`) and ran `filter_key_not_in(acks)` over all of it.

Now the members' tick emits only an ordered stream of `TableEvent`s:

- the tick's acks (`Ack`), sorted;
- then its first relays (`Send`, carrying `now`; Part 2 replaced these with `Receipt` events and moved the dedup and fan-out into the table);
- then, on ticks that saw a timer element, one `Pump { now }`.

A top-level `scan` applies these events to an `OutstandingTable` (`peer → id → (last_sent,
body)`, in `BTreeMap`s) in place. The scan is persistent, `'static` state, so nothing is
copied between ticks.

- **Costs:** an ack is one `remove`, and a relay is one `insert`. A pump walks each peer's map
  from its smallest id to the first entry that is due. The only entries it skips are ones sent
  in the last `ack_timeout_ticks` timer ticks.
- **Protocol: unchanged.** Per peer, the smallest-id entry that has waited ≥ `ack_timeout_ticks`
  timer ticks is re-sent, once per timer tick. `wire` is first sends followed by re-sends
  ordered by peer, and depth is reported on timer ticks.
- **HydroFrac** (`cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi`): 6/6 pass.
  - `Echo` keeps its counterexample: omit `relay[0→1]` of id 2, plus a crash of the source
    after it reaches only member 0.
  - `EchoResend` diverges under two crashes.
  - `ReliableResend`: no violation found, under one crash (29 fault sets) and under unbounded
    crashes (107 fault sets), with 8 sampled schedules each. The fault universe changed with the
    program's operators, but the verdicts did not.
- **Cress** (`./scripts/cress --config reliable_resend_n9_4k hydro_test relay_broadcast`): the
  same verdict as before.
  ```
  ! HAZARDOUS  relay_broadcast (reliable_resend_n9_4k)  growth: grows  multiplier: 25.95x  location: hydro_test/src/cluster/relay_broadcast.rs:528, batch of Arrival
      outputs: amplified: wire, merged, acks_sent, delivered; shed: outstanding_depth, metrics
  ```

### W3: observing the members from outside

The members' once-a-second metric reports travel the same send queues that starve in an
overload. Sedge now also reads each node's kernel accounting itself:

- `rchar`, `wchar` and `syscw` from `/proc/<pid>/io`;
- `VmRSS` from `/proc/<pid>/status`.

These are summed over each container's processes and become per-node trace columns:
`wchar_kb_*`, `rchar_kb_*`, `syscw_*` and `rss_mb_*`.

- **Why `/proc` I/O counters, and not network counters?**
  - The nodes run with `--network host`, so the container has no network interface of its own
    to count.
  - Node-to-node links are Unix sockets in `$TMPDIR`, so per-socket TCP counters (`ss -ti`)
    don't exist.
  - `wchar` counts every byte a node passes to `write`/`writev`, which hydro_deploy uses for
    sockets. It measures what really left the process, as opposed to what the dataflow queued.
- **Why a sampler container?** `/proc/<pid>/io` is guarded by a ptrace check, and the check
  fails across user namespaces, as from the sandbox. `DockerBackend::proc_sampler` therefore
  starts one small container per trial:
  - `--pid host --network none`, running as the invoking user, pinned to the harness cpuset;
  - the harness writes the pids of each member container (read from its `cgroup.procs`) to a
    file;
  - the container rewrites a snapshot of their counters four times a second, using bash
    builtins only.

  It costs the nodes' cores nothing.
- **Interpreting the counters.** `wire_x = wchar / 4120 B` is the number of relays the members
  wrote to the kernel. A relay is 4 + 8 + 8 + 4096 bytes plus framing, and acks and reports add
  under 1 %.
  - `wire_x / (72 × rounds)` is wire per offered request. 72 = 9 members × 8 peers, the first
    relays one request causes.
  - At baseline the ratio is 0.97–1.05, and `wire_x` agrees with the in-band `wire` to within
    about 3 %.
  - The remaining in-band reports also kept arriving in these runs (9 per second), because the
    members no longer starve.

### W2: CPU headroom at baseline

μ was re-measured with the new program: **136.3 rounds/s**. 136.3/s keeps up, at 94 % of
periods throttled. 148.7/s falls behind. The earlier value was 123.4 on a different core.

Under this cap, though, the members use nearly the whole quota at every f ≤ 3, because a
member's cost per round falls with load: at higher load each tick handles a larger batch.

| λ (rounds/s) | f | member cpu (quota 0.010) | throttled fraction | CPU per round |
|---|---|---|---|---|
| 23 | 6 | 0.0066–0.0081 | 0.15–0.23 | 0.30 ms |
| 34 | 4 | 0.0089–0.0097 | 0.38–0.86 (0.38 in the post-burst tail) | 0.26 ms |
| 45 | 3 | 0.0097–0.0102 | 0.59–0.87 | 0.22 ms |
| 91 | 1.5 | 0.0100 | 0.99–1.00 | 0.11 ms |
| 136 | 1 (μ) | 0.0100 | 0.94 | 0.074 ms |

So the throttled fraction measures how fully the quota is used, not how much headroom there
is. It is below 0.6 only at f ≥ 4. Raising the cap does not change this, because the ratio is
set by f. The runs below cover both regimes: f ≤ 2, saturated at baseline, and f = 4, with
real slack after warm-up.

## Results (per-peer `ReliableResend`)

Setup:

- `reliable_resend_n9_4k`: 9 members, `ReliableResend { ack_timeout_ticks: 3 }`, 4 KiB bodies.
- Members capped at 0.01 cpu per 100 ms; the source uncapped.
- Release build (`profile`), open-loop feed, unbounded send buffers.
- Nodes on cpus 4–5, harness on cpu 3.
- Burst at 2μ starting at 10 s; 60 s tail.

```sh
./scripts/sedge --config reliable_resend_n9_4k --factors F --trials 1 --tail 60 \
  --harness-cpus 3 --node-cpus 4-5 hydro_test relay_broadcast                   # burst_w1
./scripts/sedge ... --factors 1.5 --trigger 20 ...                               # burst_long_20s
./scripts/sedge ... --factors F --trigger-multiple $(python3 -c "print(1/F)") ... # ctl_fF
python3 analyze.py --period-us=100000 traces/*/*.tsv
python3 work_per_cpu.py LABEL=trace.tsv ...
```

| run | f | λ | Sedge | peak unacked | re-sends/s, tail [45,75) | unacked at 75 s |
|---|---|---|---|---|---|---|
| burst 5 s | 1.25 | 109 | RECOVERED | 44 k | 7–9 | 604 |
| burst 5 s | 1.5 | 91 | RECOVERED | 42 k | 9 | 551 |
| burst 5 s | 2 | 68 | DRAINING¹ | 50 k | 3–5 | 435 |
| burst 5 s | 4 | 34 | RECOVERED | 43 k | 0 | 57 |
| burst **20 s** | 1.5 | 91 | DRAINING² | 252 k | 253–274 | 119 k, falling |
| no burst | 1.5 | 91 | RECOVERED | 719 | 3–6 | 570 |
| no burst | 2 | 68 | INVALID³ | 527 | 1–4 | 401 |

1. "Unacked rising" is slope noise on a flat level of about 400–450. The trace is flat from
   30 s on.
2. Still draining, at 3.3 k/s and accelerating. The level read 252 k at 30–45 s, then 221 k,
   174 k and 119 k at 75–90 s. Sedge estimated 45 s to empty.
3. The open-loop check fired: 6 of 5084 requests entered more than 50 ms late, the worst
   81 ms. The program's own reading was RECOVERED, and the trace is flat.

Before W1, the same configuration and burst **COLLAPSED** at f = 1.25, 1.5, 1.75 and 2 (10/10
valid trials). There, unacked was still rising at 140–170 k at 75 s, and the member reports
stopped by 45 s.

The 5 s burst at f = 1.5 (`traces/burst_w1/factor1.5.trial0.tsv`):

```
window   rounds/s wire_in resent_in rep unacked wchar(MB/s) wire_x wire_x/off cpu    thr  rss(MB)
[5,10)    86.4    5987     4.8     8.8   509    24.70     5994   0.96      0.0100 0.99  9.5
[10,15)  272.4   17530   200.6     8.4 42478    39.24     9525   0.49      0.0103 1.00 24.1
[15,20)   92.6    9358   299.2     9.4 37389    43.72    10611   1.59      0.0099 1.00 40.3
[20,30)   91.6    7006   273.1     9.1  8139    40.47     9823   1.49      0.0100 1.00 40.3
[30,45)   87.5    6287    47.2     9.0   664    27.97     6788   1.08      0.0099 0.99 38.6
[45,60)   90.4    6516     8.7     9.1   608    26.58     6452   0.99      0.0101 1.00 38.6
[60,75)   89.4    6428     9.1     9.0   551    26.69     6478   1.01      0.0100 0.99 38.8
```

The 20 s burst at f = 1.5 (`traces/burst_long_20s/factor1.5.trial0.tsv`):

```
window   rounds/s wire_in resent_in rep unacked wchar(MB/s) wire_x wire_x/off cpu    thr  rss(MB)
[5,10)    86.4    6260     0.4     9.2    736   25.78     6256   1.01      0.0102 1.00   9.6
[10,15)  272.4   11441   190.0     7.4  22866   31.10     7548   0.38      0.0106 1.00  20.8
[15,20)  275.6   18856   241.6     9.0  83577   26.13     6342   0.32      0.0098 1.00  50.8
[20,30)  266.8   13299   232.8     8.4 145630   28.96     7028   0.37      0.0099 1.00  82.3
[30,45)   89.3   13706   252.8     8.1 251735   26.90     6528   1.01      0.0101 1.00 125.8
[45,60)   90.9    7144   253.3     9.1 221252   36.95     8969   1.37      0.0100 1.00 155.2
[60,75)   89.3    6650   264.0     9.0 174201   40.40     9805   1.52      0.0100 1.00 155.2
[75,90)   86.5    6461   273.6     8.9 119230   43.02    10441   1.68      0.0100 1.00 155.2
```

Column definitions are in `analyze.py`. `wire_in` is the relays the dataflow emitted (in band),
and `wire_x` is the relays the kernel actually received (out of band).

Reading the 20 s burst:

- **Re-sends do not decay.** They hold at about 250–275/s for as long as the backlog lasts,
  against 0.4/s at baseline. This is the amplification Cress predicts: late acks cause
  re-sends, for as long as the delay lasts.
- **But they are capped by the pump.** The pump re-sends at most one entry per peer per timer
  tick. Under the cap a member gets only about 4 effective timer ticks a second, not 20, so the
  whole cluster sends about 9 × 8 × 4 ≈ 290 re-sends/s at most. Against 72λ ≈ 6.5 k first
  relays/s that is about 4 % extra wire: a bounded, additive load. Headroom at f = 1.5 is 33 %
  of capacity, and even f = 1.25 has 20 %.
- **So the backlog drains**, faster as it shrinks.
- **Where `wire_x` is above `wire_in` (ratio 1.4–1.7), the excess is old data.** It is not new
  amplification. It is the member writers flushing relays that the dataflow emitted during the
  burst, when `wire_in` was 11–19 k/s but the kernel took only 6–7.5 k/s. Those relays were
  queued in the unbounded user-space send buffers, which is the RSS rise to 155 MB. Only the
  out-of-band counter shows this queue.

### Per-tick cost no longer grows with the table

`work_per_cpu.py` bins each second by unacked entries per member and gives the members' work
per CPU-millisecond in each bin, as a median:

- in band: merged arrivals plus wire sends;
- out of band: `rchar + wchar`, in KiB.

```
trace          unacked/member  seconds  work/cpu-ms (in band)  io KiB/cpu-ms (outside)
old_f1.5_seed7 [0,100)         10       141.0                  -
old_f1.5_seed7 [3000,10000)    16       155.8                  -
old_f1.5_seed7 [10000,30000)   39       - (no complete reports)
new_f1.5_20s   [0,100)          9       146.3                  301.9
new_f1.5_20s   [1000,3000)      4       286.7                  352.7
new_f1.5_20s   [3000,10000)     6       318.4                  257.2
new_f1.5_20s   [10000,30000)   69       213.0                  397.8
```

With up to 28 k entries per member, a member does as much work per CPU-millisecond as it does
at baseline, or more, because it is batching. Before W1, the old runs went blind at exactly
this table size. This comparison is the measurement W1 asked for. It is coarse: the members
are pinned at their quota throughout, so only throughput per CPU, not tick latency, can be
seen.

## What Part 1 means

- **Cress's hazard is real and visible in deployment.** A burst makes the late acks cause
  re-sends, and the re-sends persist as long as the backlog does, so the "grows" shape is
  there. The amplified streams are the ones Cress lists: wire, merged (each re-send is merged
  again), acks_sent and delivered.
- **It is not self-sustaining here.** In this protocol, re-sends are paced by the pump (one per
  peer per timer tick), so the extra load is a constant rate, not proportional to the backlog.
  A collapse sustained by re-sends alone would need that constant to exceed the headroom. That
  happens only at f below about 1.05–1.15 here, which is too close to μ to test cleanly with a
  2μ burst.
- **The earlier collapse was W1.** It came from the O(unacked) per-tick rebuild combined with
  send-queue starvation, which is a cost of how the state was written, not the re-send policy.
  Cress does not model it, since it counts messages, not per-tick CPU.
- **What would make re-sends sustain a collapse** is a retry policy whose rate grows with the
  backlog: re-send every timed-out entry, not one per peer per tick, optionally with a retry
  limit. Part 2 builds that variant.

## Part 1 caveats

- **One trial per point, one host.** The parent asked for cheap confirmations only, so the
  extended sweep was not run.
- **RSS grew about 0.3 MB/s even in the no-burst controls** (7.6 → 31 MB over 75 s at
  f = 1.5). The dedup set (`first_receipts.unique()`) keyed on `(id, body)`. The body is
  excluded from comparisons, but its `Bytes` were still held, so every body ever received stayed
  alive. This cost memory, not CPU (the controls' CPU and work rates are flat). Part 2 fixes it:
  dedup is now by id only, and the per-entry controls' RSS stays flat at about 7 MB.
- **Code version.** The Part 1 Sedge runs used the program before Part 2's dedup change.
  - HydroFrac was re-run after that change, and `ReliableResend` still certifies: 163 fault
    sets under one crash and 314 under unbounded crashes, the latter with a raised budget, see
    Part 2.
  - Cress for `reliable_resend_n9_4k` was not re-run after it. `cress/reliable_resend_n9_4k.txt`
    is the report from before it.
- **The 20 s burst did not finish draining** within the run. It was falling at 3.3 k/s, and
  faster as the table shrank.
- **The "about 4 effective pumps/s" figure is inferred** from the re-send ceiling, at 8 peers ×
  9 members. The pump rate itself is not reported.

# Part 2: per-entry retries (`ReliableResendPerEntry`)

## The variant

`RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks, max_attempts: Option<u32> }` relays
like `ReliableResend`, on first receipt from any sender, but it retries each relay on its own
timer.

- **Per-entry deadlines.** Each unacked `(peer, id)` entry has its own deadline,
  `last sent + ack_timeout_ticks`.
- **Every overdue entry is re-sent.** On each timer tick, every entry whose deadline has passed
  is re-sent, with no per-peer or per-tick cap, and its deadline restarts.
- **Retry limit.** After `max_attempts` sends in all (the first included), the entry is given
  up. `None` never gives up.
- **Cost.** The table keeps a deadline-ordered `BTreeSet` next to the entries, so a timer tick
  costs O(due entries + changes).
- **Catch-up.** Deadlines are counted in timer elements (`now` is the timer's index). The
  deployed timer is a Tokio interval with the default `Burst` behaviour, which delivers missed
  ticks late instead of dropping them. So a starved member's clock catches up, and everything
  overdue is re-sent on its next timer tick.
- **Duplicates cost the full price.** They go through the same receive, merge and ack path as
  any arrival.
- **Dedup is by id only.** It now uses a `HashSet<u64>` inside the table's `scan`. Before, a
  top-level `unique` over `(id, body)` held every received body (the RSS creep in Part 1). The
  fan-out to peers moved into the same `scan`. This applies to every policy.

## HydroFrac

```sh
cargo test -p hydro_test --lib cluster::relay_broadcast -- --nocapture --test-threads=3
```

All 12 `relay_broadcast_ldfi` tests and 4 new `OutstandingTable` unit tests pass. The trimmed
log is in `hydrofrac.txt`.

| test | timer rounds before the cut | result |
|---|---|---|
| `Echo` | 0 | DIVERGES: omit relay 0→1 of id 2, plus a source crash (unchanged) |
| `EchoResend`, unbounded crashes | 0 | DIVERGES (unchanged) |
| `ReliableResend`, 1 crash / unbounded | 0 | no violation (163 / 314 fault sets) |
| `PerEntry` max 3 / 5 / unbounded, unbounded crashes | 0 | no violation (256 / 241 / 247) |
| `PerEntry` **max 3** | 10 | **DIVERGES** |
| `PerEntry` **max 5** | 16 | **DIVERGES** |
| `PerEntry` **unbounded** | 10 | no violation (239) |

What the two setups test:

- **The standard setup** fires timers only after the cut. HydroFrac's omissions are all before
  the cut ("finite failures"), so every retry is delivered and any limit ≥ 2 certifies.
- **The stricter setup** fires `3 · max + 1` timer rounds *before* the cut, so HydroFrac can omit
  the retries too. Re-sends are identical bytes, so one omission covers every copy.
- **The bounded limits break in the stricter setup.** The source crashes after reaching only
  member 0, and member 0's relays of id 2 to members 1 and 2 are omitted on every attempt.
  Once member 0 has used up its attempts it gives the relays up, and members 1 and 2 never get
  id 2.
- **So only `max_attempts: None` certifies under HydroFrac's omission model**, and it is the one
  deployed (`per_entry_n9_4k`).

**The fault-set counts went up**, to about 250 from 29–107. The table and the dedup now live in
one persistent `scan`, which provenance sees as one opaque state: each relay's lineage includes
every earlier receipt at that member. `ReliableResend` with unbounded crashes hit the default
budget of 256 hypotheses (UNDECIDED). The searches with unbounded crashes now use
`max_hypotheses(1024)`.

## Cress

```
! HAZARDOUS  relay_broadcast (per_entry_n9_4k)  growth: grows  multiplier: 175.60x  location: hydro_test/src/cluster/relay_broadcast.rs:616, batch of Arrival
    outputs: amplified: wire, merged, acks_sent, delivered; shed: outstanding_depth, metrics
```

These are the same amplified streams as `ReliableResend`. The difference is the rate:

- the busiest sender gains **144 extra sends per round of delay**, against 16 for the per-peer
  policy;
- the per-peer policy's rate is fixed (two per peer per round);
- the per-entry rate grows with the number of relays outstanding at the held member.

The largest multiplier, 175.60×, is at the ack batch (`:618`). The full report is in
`cress/per_entry_n9_4k.txt`.

**The Cress workload was reduced.** `per_entry_n9_4k`'s `amplification_check` uses 10 workload
rounds, a horizon of 200 rounds and **empty bodies**. The `ReliableResend` checks use 40 rounds,
a horizon of 1000 and 4 KiB bodies.

- Under a hold, every outstanding relay is re-sent every three timer elements, and the
  simulator keeps every buffered re-send.
- The first run, with 40 rounds, horizon 1000 and 4 KiB bodies, grew to 10.5 GB and was killed
  by the kernel's global OOM killer on this 15.7 GB host.
- With 10 rounds and horizon 200, but still 4 KiB bodies, it passed 3.5 GB and a watchdog
  killed it.
- With empty bodies it peaks at about 400 MB.
- Body size does not change Cress's counts, since Cress counts messages, not bytes. The
  deployed Sedge config keeps 4 KiB bodies.

## Sedge

```sh
./scripts/sedge --config per_entry_n9_4k --harness-cpus 3 --node-cpus 4-5 --memory-mb 400 \
  --factors F --trials 1 --tail 60 hydro_test relay_broadcast                      # bursts
./scripts/sedge ... --trigger-multiple $(python3 -c "print(1/F)")                    # controls
```

The setup is the same as Part 1 (members at 0.01 cpu per 100 ms, the source uncapped, release
build, open-loop feed, unbounded send buffers), plus a memory limit of **400 MB per node**:

- per-entry re-sends queue in unbounded buffers, and the host has 15.7 GB;
- the worst healthy recovery seen (Part 1, the 20 s burst) peaked at 155 MB per member.

**μ = 126.1 rounds/s.** 126.1/s keeps up, at 96 % of periods throttled, and 137.6/s falls
behind.

**Dead nodes are now detected.** Member containers no longer run with `--rm`. After the grace
period, before teardown, the harness reads each node's `State.Running`, `State.OOMKilled` and
`State.ExitCode` with `docker inspect`. Any node that died makes the trial **INVALID**, with
"OOM-killed at its memory limit" (or "exited with code N") and the program's own reading kept
in the reason.

### Bursts (2μ for 5 s)

| f | λ | before the burst: re-sends/s | burst window: re-sends/s | unacked | after: wchar, MB/s | member RSS | outcome |
|---|---|---|---|---|---|---|---|
| 1.25 | 101 | 860 → 8 150 (already unstable) | 9 840 | 19.8 k | 4.5 → 0.06 | → 400 MB | INVALID: OOM |
| 1.5 | 84 | 0–56 | 9 910 | 24.5 k | 24 → 4.8 → 0.4 → 0 | 7 → 85 → 188 → 354 → 399 MB | INVALID: OOM (Member:2) |
| 2 | 63 | 33–42 | 6 390 | 19.4 k | 20 → 0.9 → 0.4 → 0 | → 383–399 MB | INVALID: OOM (Member:3) |
| 3 | 42 | 2–12 | 7 520 | 17.8 k | 20 → 0.6 → 0.8 → 0 | → 387–399 MB | INVALID: OOM (Member:4) |
| 4 | 32 | 1–5 | 8 440 | 18.8 k | 17 → 2.0 → 0.7 → 0 | → 380–400 MB | INVALID: OOM (Member:1) |

The traces are in `traces/pe_burst/`. f = 1.5 (`traces/pe_burst/factor1.5.trial0.tsv`):

```
window   rounds/s wire_in resent_in rep unacked wchar(MB/s) wire_x wire_x/off cpu    thr  rss(MB)
[5,10)    79.0    5648    55.8     9.0    464   23.21     5633   0.99      0.0100 1.00   7.2
[10,15)  254.4   17541  9912.0     6.8  16575   24.08     5844   0.32      0.0109 1.00  24.2
[15,20)   84.4   11806  9863.2     2.6  24504    4.79     1162   0.19      0.0100 1.00  84.7
[20,30)   87.1       0     0.0     0.0  24504    0.40       97   0.02      0.0099 1.00 188.1
[30,45)   82.0       0     0.0     0.0  24504    0.13       33   0.01      0.0088 0.75 353.9
[45,60)   80.8       0     0.0     0.0  24504    0.00        0   0.00      0.0000 0.11 399.2
```

Reading:

- **The burst starts a re-send storm.**
  - In-band re-sends go from tens per second to 6–10 k/s at every f, up to f = 4.
  - With every ack late, every entry is re-sent every 3 timer ticks. That is about 2 k
    re-sends per member per timer tick, against 8λ ≈ 250–700 first relays/s per member.
- **The storm does not reach the wire.**
  - Member CPU stays at the quota.
  - The kernel writes (`wchar`) fall to about zero within 10 s, member reports included, so
    `rep` = 0 and the in-band series go dark.
  - Each member is a single-threaded Tokio runtime: its tick serializes thousands of re-sends
    into the unbounded send buffers, and the writer tasks that would flush them barely run.
- **The out-of-band RSS slope measures the storm.**
  - It is about 12 MB/s per member, ≈ 3 k queued relays/s at 4.1 KB each.
  - The amplified work is therefore real and grows with the backlog, as Cress predicts. In this
    deployment it is spent on serializing and queueing, not on the wire, so outside wire/offered
    falls to 0 rather than staying ≫ 1.
- **The end is a crash.** About 30–45 s after the burst a member reaches 400 MB and the kernel
  kills it. Sedge reports INVALID.
  - The in-band verdict without that check would be wrong: it read RECOVERED, because the
    reports had stopped.
  - The collapse is metastable up to the point where the OOM turns it into a crash.
- **No recovery at f ≤ 4.** A 2μ, 5 s burst leaves about 18 k unacked relays whose acks are all
  past the 150 ms timeout. From then on each member's re-send work grows with that backlog.

### No-burst controls

| f | λ | unacked | re-sends/s | wire_x/off | member RSS | throttled | Sedge's label |
|---|---|---|---|---|---|---|---|
| 1.25 | 101 | 3.5 k → 21 k | 1.8 k → 12.3 k | → 0 | → 400 MB | 0.96–1.0 | INVALID: OOM (Member:0) |
| 1.5 | 84 | 494–648 | 6–78 | 0.97–1.04 | 7.0–7.6 MB | 0.98–1.0 | DRAINING¹ |
| 2 | 63 | 194–525 | 1–57 | 0.98–1.04 | 6.8–7.4 MB | 0.97–1.0 | DRAINING¹ |
| 3 | 42 | 65–290 | 0–8 | 0.96–1.05 | 6.5–7.2 MB | 0.80–0.94 | COLLAPSED¹ |
| 4 | 32 | 43–113 | 0–10 | 0.98–1.05 | 6.4–7.1 MB | 0.60–0.85 | RECOVERED |

1. These are slope-noise labels ("unacked rising") on traces that are flat for all 75 s. Sedge's
   slope test does not cope with a small, noisy level. The traces are in `traces/pe_ctl_f*/`.

What the controls show:

- **f = 1.25 is unstable on its own.** Spurious re-sends from the baseline's ack latency are
  enough to start the storm, so this is overload, not metastability.
- **f = 1.5–4 are stable without a trigger.** Together with the bursts, that makes this a
  metastable failure for f = 1.5–4, ending in a crash.
- **Member RSS stays flat at about 7 MB**, which confirms that the id-only dedup fixed the creep.

### Baseline throttling (W2)

- **Throttled fraction:** 0.98–1.0 at f = 1.5–2, 0.80–0.94 at f = 3 and 0.60–0.85 at f = 4.
  Part 1 measured 0.15–0.23 at f = 6.
- **Getting it below 0.6** therefore takes f ≥ 4–6 under this cap.
- **Why it stays high:** the member's cost per round falls about 4× as load rises from λ = 23 to
  μ, because of batching. So it fills its quota at almost any load, and the throttled fraction
  measures how fully the quota is used, not how much headroom there is.

## Not run

These were stopped when the host was retired.

- **Bursts at f = 6 and 8**, to look for recovery. The node rebuild for them was OOM-killed, see
  "Host memory".
- **A weaker trigger**: a 1.5μ, 5 s burst at f = 2–4, to find a recovery boundary.
- **Bounded member-to-member send buffers with the source unbounded:**
  `--send-buffer bounded:Member`.
  - The aim was to make the storm show up as wire bytes and CPU instead of RSS, with no OOM,
    so that it could be compared directly with Cress's amplified wire count.
  - Bounded sends push back on the members' own dataflows, while the source's feed stays
    open-loop. The feed-lateness check is measured where records enter the source, so it would
    still mean what it should.
  - **The code is in this tree but untested in a run.** It compiles, and `cargo test -p sedge`
    passes (18 tests).
- **More trials at the boundary**, and Cress on `per_entry5`/`per_entry3`. Those two configs
  exist, but only `per_entry_n9_4k` was checked.

**Also untested in a run:** the per-trial `remove_prefix` sweep. It was added after an
OOM-killed container survived `docker rm -f`; the leaked containers were all this worker's and
were removed by hand. The OOM detection and the `/proc` sampler *were* exercised in the runs
above.

## Host memory

The deployed nodes are built with the `profile` profile and the harness's rustflags
(`-C opt-level=3 -C codegen-units=1 -C debuginfo=2 -C lto=off`). The `hydro_test` lib's rustc
peaks at about **8.3 GB** under those flags.

- On the shared 15.7 GB host, several such builds were OOM-killed by the kernel (five in
  half an hour, one of them this worker's).
- The Sedge runner pins itself to the harness cpuset, and the build inherits the pin, so one
  rebuild took about 30–40 minutes on one core.
- On the new host, give these builds room. Pre-building outside the pinned runner would help.

## Files

- `results.tsv`: Sedge's row for each trial, with the run group in the first column.
  - `ctl_quick_f3`/`ctl_quick_f6` are 15 s-tail controls whose traces were not kept.
  - `pe_*` rows are the per-entry runs.
- `calibration.tsv`, `probes.tsv`: the μ probes (`reliable_resend_n9_4k` 136.3, `per_entry_n9_4k`
  126.1).
- `traces/<group>/factor<f>.trial0.tsv`: per-second traces, with the out-of-band columns
  (`wchar_kb_*`, `rchar_kb_*`, `syscw_*`, `rss_mb_*`).
  - Part 1: `burst_w1`, `burst_long_20s`, `ctl_f1.5`, `ctl_f2`.
  - Part 2: `pe_burst`, `pe_ctl_f*`.
- `cress/`: the Cress reports for `per_entry_n9_4k` and for `reliable_resend_n9_4k` (Part 1's
  program).
- `hydrofrac.txt`: the HydroFrac verdict lines for all 12 tests.
- `analyze.py`: window summaries.
- `work_per_cpu.py`: work per CPU-ms against table size.
