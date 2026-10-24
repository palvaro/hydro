# `relay_broadcast` `ReliableResend`: metastable collapse, with capacity applied from outside the program

This is the search along the workload/topology dimension: cluster size and request payload. It
looks for a configuration of `ReliableResend` that is stable below μ, tips into a lasting degraded
state after a transient burst, and recovers when there is enough headroom. None of the capacity
code is in the program. Members are capped by Docker CFS quotas, and every timer runs in-process.

## Configuration

`reliable_resend_n9_4k` in `hydro_test/src/cluster/relay_broadcast.rs`:

- **Cluster:** 9 members, `ReliableResend { ack_timeout_ticks: 3 }`, relay channel `FailStop`.
- **Workload:** each request carries a 4 KiB body (`Request::with_body(round, 4096)`). The body
  is part of the offered load, since members relay it, keep it for re-sending, and re-send it.
- **Capacity:**
  - Each member gets 0.01 cpu, with a 100 ms CFS period.
  - The source is uncapped.
  - The build is release (`--deploy-profile profile`).
  - The harness runs on cpu 6 and the nodes on cpu 7.
  - Feed is open-loop with an unbounded send buffer.
- **Timers:** 50 ms in-process timer and a 1 s report tick.
- **Trigger:** a load burst at 2μ for 5 s, from 10 s to 15 s, followed by a 60 s tail. There is
  no CPU dip.

μ = **123.4 rounds/s**, measured on this host, slot, and build: 123.4 keeps up, 134.5 falls behind.

Commands:

```sh
./scripts/cress --config reliable_resend_n9_4k hydro_test relay_broadcast
./scripts/sedge --config reliable_resend_n9_4k --factors F --trials N --tail 60 \
  --harness-cpus 6 --node-cpus 7 hydro_test relay_broadcast
# no-burst control: the trigger window runs at the baseline rate λ = μ/F
./scripts/sedge ... --factors F --trigger-multiple $(python3 -c "print(1/F)") ...
```

## Results

Each cell gives cluster-wide unacked relays (window means). Verdicts are Sedge's.

| f | λ | burst runs | unacked: base → 60–75 s | no-burst control | control unacked over 75 s |
|---|---|---|---|---|---|
| 1.25 | 98.7 | COLLAPSED ×1 | 578 → 148k | RECOVERED | 540–590, flat |
| 1.5 | 82.3 | COLLAPSED ×3, INVALID ×1¹ | ~430 → 139–170k | flat² | 404–473, flat |
| 1.75 | 70.5 | COLLAPSED ×3, INVALID ×1¹ | ~330 → 146–166k | RECOVERED ×2 | 165–371, flat |
| 2 | 61.7 | COLLAPSED ×1 | 259 → 136k | RECOVERED | 286–363, flat |
| 2.5 | 49.4 | DRAINING ×3³ | peak 41–55k → empty by 45 s (1 trial) or ~75 s (2 trials) | RECOVERED | ≤ 60 |
| 3 | 41.1 | RECOVERED ×2, DRAINING ×1³ | peak 37–53k → empty by 30–45 s (2) or ~75 s (1) | RECOVERED | ≤ 11 |
| 4 | 30.8 | RECOVERED | peak 21.5k → 9 by 30–45 s | — | — |

1. INVALID means the open-loop check fired: a few records entered more than 50 ms late. The
   program's own reading was COLLAPSED in both cases.
2. Sedge labels this control DRAINING ("unacked rising") because of slope noise at the end of
   the run. The trace is flat, see `traces/ctl_f1.5_a`.
3. At f=2.5 and f=3 every trial drains to empty within the run. DRAINING only means the drain
   had not finished inside the verdict window.

The collapse is **metastable**:

- **Steady below μ:** every no-burst control from f=1.25 to f=3 is flat for 75 s. Unacked stays
  bounded, re-sends are ~0, and throttling does not trend.
- **A 5 s burst tips it:** at f ≤ 2, unacked climbs by ~2.5k/s through the whole tail, and
  members stay pinned at ~98% throttled even though the load is back to λ < μ.
- **Headroom recovers:** at f ≥ 2.5 the backlog (40–55k) drains in 30–60 s, and f=4 is clean by
  30 s. The boundary lies between f=2 and f=2.5. No single factor produced mixed outcomes across
  trials.

Checks on this program:

- **Cress:** HAZARDOUS, grows, 25.95× @ `relay_broadcast.rs:526` (batch of Arrival).
- **HydroFrac:** `cluster::relay_broadcast_ldfi` passes all 6 tests. `None`, `Echo` and the
  unbounded `EchoResend` test still produce counterexamples, and `ReliableResend` holds under
  one crash and under unbounded crashes.

## What sustains the degraded state

The data comes from `mechanism/`: two more f=1.5 burst runs (seeds 7 and 8, both COLLAPSED).
`sampler.py` sampled each member's RSS once a second.

- **Re-sends start it but don't sustain it.** Re-sends peak at 150–250/s during the burst, then
  *decay* through the tail (to ~4–40/s) while unacked keeps growing. The re-send pump is
  timer-driven, and once the members are starved the timers fire less often.
- **Member RSS grows steadily after the burst.** It goes from 8 MB to 190–233 MB per member by
  95 s, about 2.5–3 MB/s. The unacked table accounts for only ~10–20 MB of that: ~17k entries
  per member, with the 4 KiB bodies shared through `Bytes`. Kernel socket buffers are not part
  of RSS, and the inbound path is pull-based. The growth is therefore outgoing messages queued
  in the unbounded send buffers, which the starved writer tasks never flush.
- **Observed rates collapse.** Cluster `delivered` falls from ~6,500/s to ~250/s, and `wire`
  from ~5,800/s to ~1,000/s (45–75 s). The metric reports travel through the same starved send
  path, so these rates lag as well.
- **The sustaining load is per-tick work that grows with the unacked table.**
  - Every tick rebuilds `outstanding` (a tick-scoped `KeyedSingleton`, carried across ticks with
    `defer_tick`) and runs `filter_key_not_in(acks)` over it.
  - At ~17k entries per member, that costs an estimated 2–4 ms per tick against a quota of 1 ms
    per 100 ms. So once the table passes a few thousand entries per member, a tick eats more
    than a whole period's quota.
  - The send writers then get no CPU, acks stop arriving, and the tables keep growing.
  - At f ≥ 2.5 the burst's backlog does not cross that line for long enough, and it drains.

So the trigger is the retry storm that Cress flags, but what keeps the system stuck is an
O(unacked) per-tick state rebuild combined with send-queue starvation. That cost belongs to the
program as written (its state lives in tick-scoped collections). It is not capacity code. A
production implementation would keep the table per peer, or in a timer wheel, and would not pay
O(table) on every tick.

This was not measured directly. There is no `perf` on the host, and the members' fds are not
readable from outside their containers, so per-socket queue depths and per-tick CPU could not be
sampled. The attribution above rests on the RSS growth, the decaying re-send rate, and the
per-tick cost estimate.

## Program changes made during the search

All of these are protocol-neutral. HydroFrac, Cress and Sedge were re-run after each one.

1. **`Request { id, body: Body }`** replaces the bare `u64` request. `Relay` and `Arrival` carry
   the body too. Every earlier configuration sends an empty body.
2. **Dedup without drift.** The `relayed` set was a tick-scoped `KeyedSingleton` rebuilt every
   tick, which cost O(ids seen so far) per tick. Members slowed down as a run went on, and an
   f=1.25/1.5 no-burst control degraded on its own. Dedup is now a top-level `.unique()` on the
   first-receipt stream. Hydro compiles that to `unique::<'static>`, a persistent set that costs
   O(1) per arrival.
3. **Shared bodies.** `Body` wraps `bytes::Bytes` (the hydro_test `bytes` dependency now has
   `serde` enabled), so the per-peer copies in the re-send table cost a refcount, not 4 KiB. The
   type's `Eq`/`Ord`/`Hash` ignore the bytes, because the id determines the body.
4. **The re-send scan runs on timer ticks only.** `due` is gated with `filter_if(pump)` before the
   table is scanned, and the depth count is gated the same way and held between timer ticks. The
   `outstanding_depth` output therefore emits on timer ticks only, and Cress now lists it as
   "shed". This change alone raised μ from 56.6 to 123.4 rounds/s.

## Exploratory runs (`exploratory/`)

These are debug builds of the program before the fixes, kept for the record:

- **n=13, bare ids: no metastability window.** μ = 493.5 rounds/s, about the same as n=5. The
  re-send cap of 20/s per peer is ≤ 8% of the per-peer relay load λ at any n, so cluster size
  alone cannot make re-sends dominate. The payload, by lowering μ, is what does.
- **n9_4k before the drift fix.** The no-burst controls at f=1.25 and f=1.5 drifted, with
  throttling climbing from ~300 to ~850 ms/s over 75 s. That run was the evidence for the
  `relayed` fix.

## Files

- `results.tsv`: every trial's Sedge row; the first column is the run group.
- `traces/<group>/factor<f>.trial<n>.tsv`: per-second Sedge traces. The groups are `burst_*`
  (2μ for 5 s) and `ctl_f<f>` (no-burst controls).
- `mechanism/`: the two sampled f=1.5 runs, with traces and per-member RSS (`*.members.jsonl`),
  plus the sampler.
