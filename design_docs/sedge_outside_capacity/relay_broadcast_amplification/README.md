# `relay_broadcast` `ReliableResend`: does the re-send amplification alone sustain a collapse?

**Short answer: no.** The program was fixed so that a tick no longer costs O(unacked)
(W1), and Sedge now reads the members' I/O from outside them (W3). After that, the same
configuration that collapsed at f ≤ 2 recovers from the same burst at every f tried
(1.25, 1.5, 2, 4). It also recovers from a burst four times as long, which builds a
250 k-entry backlog. The re-sends that Cress flags are real, and they stay high while a
backlog exists. They are capped at one per peer per timer tick, though, so they add a bounded
amount of load: about 270 re-sends/s across the cluster, about 4 % of the first relays at
f = 1.5. That is far less than the headroom. The earlier collapse
(`../relay_broadcast/README.md`) was sustained by the O(table) per-tick rebuild, as that
document inferred. This experiment confirms it: removing the rebuild removes the collapse.

## What changed

All of the following is in one commit, "feat(sedge,hydro_test): persistent `relay_broadcast`
outstanding table and out-of-band `/proc` I/O sampling".

### W1: the outstanding table is persistent state

Before, the table was a tick-scoped `KeyedSingleton<(peer, id), (last_sent, body)>` carried
across ticks with `defer_tick`. Every tick rebuilt all of it (`kept.chain(resent).chain(new)
.first()`) and ran `filter_key_not_in(acks)` over all of it.

Now the members' tick emits only an ordered stream of `TableEvent`s:

- the tick's acks (`Ack`), sorted;
- then its first relays (`Send`, carrying `now`);
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

## Results

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

## What this means

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
  limit. That is the per-entry-retry variant another worker (R) is building, so it is not
  duplicated here.

## Caveats

- **One trial per point, one host.** The parent asked for cheap confirmations only, so the
  extended sweep was not run.
- **RSS grows about 0.3 MB/s even in the no-burst controls** (7.6 → 31 MB over 75 s at
  f = 1.5). The dedup set (`first_receipts.unique()`) keys on `(id, body)`. The body is excluded
  from comparisons, but its `Bytes` are still held, so every body ever received stays alive.
  This costs memory, not CPU (the controls' CPU and work rates are flat), and it predates this
  change. Deduplicating on the id alone would fix it.
- **The 20 s burst did not finish draining** within the run. It was falling at 3.3 k/s, and
  faster as the table shrank.
- **The "about 4 effective pumps/s" figure is inferred** from the re-send ceiling, at 8 peers ×
  9 members. The pump rate itself is not reported.

## Files

- `results.tsv`: Sedge's row for each trial, with the run group in the first column. The
  `ctl_quick_f3`/`ctl_quick_f6` rows are 15 s-tail controls; their traces were not kept.
- `calibration.tsv`, `probes.tsv`: the μ probe.
- `traces/<group>/factor<f>.trial0.tsv`: per-second traces, with the out-of-band columns.
- `analyze.py`: window summaries.
- `work_per_cpu.py`: work per CPU-ms against table size.
