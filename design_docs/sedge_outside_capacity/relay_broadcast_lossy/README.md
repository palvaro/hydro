# `relay_broadcast` under a lossy send buffer: data for the demo

This directory holds the runs behind the rewritten `demo_relay_broadcast.md`: the Cress reports, the μ probes, every
Sedge run of the final build with its trace, and the earlier runs that ruled out the other two deployments. The
scripts that read the traces are in `../relay_broadcast_amplification/` (`analyze2.py`, `summarize.py`).

## Setup of the final runs

- Program: `hydro_test/src/cluster/relay_broadcast.rs`, unchanged protocol. Configurations `per_entry_n9_4k`
  (`ReliableResendPerEntry`, no retry limit), `per_entry3_n9_4k` (at most 3 sends per relay) and
  `reliable_resend_n9_4k` (per-peer: one re-send per peer per timer tick). 9 members, 4 KiB bodies, 50 ms timer,
  ack timeout 3 ticks.
- Members capped at **0.05 cpu per 20 ms**, 400 MB each. The source is uncapped, with 2 GB. Release build
  (`--deploy-profile profile`).
- **Send buffers: `--send-buffer lossy:1024:Member`.** Each member's outgoing connection queues at most 1024
  messages in front of its writer, and drops a send that finds the queue full. The source and the harness's relay
  keep unbounded queues, so the feed stays open-loop.
- μ = **565.7 rounds/s**, measured on this build (`probes.tsv`, `calibration.tsv`). 616.9/s falls behind with
  `unacked` rising.
- Every trial: 10 s baseline, a trigger, a 60 s tail. A burst is 2μ for 5 s. Its control has the same length, with
  the "trigger" at the baseline rate (`--trigger-multiple 1/f`).
- One trial per point (seed 1).

## Why the lossy buffer

Each of the other two deployments ended the runs early in a way that hides the question.

- **Unbounded buffers** (Sedge's default; `earlier/n_*`, same cap). A 2μ, 5 s burst puts about 200 MB of first relays
  into each member's queues while their writers starve. A member is OOM-killed at 400 MB within about 10 s, at every
  f from 1.5 to 4. That makes the run a crash (INVALID), and the memory comes from the burst itself, not from
  re-sends.
- **`bounded:Member`** (Hydro's default for member links; `earlier/e1_bounded_ctl_f2`,
  `earlier/bounded_member_calibration_old_cap.txt`). Below μ it behaves like the unbounded run. Above μ every member
  stops at 0 % CPU and 0 bytes written: each one is blocked sending to a peer that is blocked sending, so no one
  reads. The source's queue then grows until it is OOM-killed. That is a write deadlock with no work done at all, so
  there is no amplification to see.

A lossy queue keeps the members' memory bounded and never blocks their dataflow. A dropped relay or ack is a
message omission, which is the fault HydroFrac certifies the retrying policies against.

## Cress and the lossy buffer

Cress models channel *holds*, that is, delays (`cress/src/check.rs`). It has no loss model, and the send buffer is
outside the program. Under these policies, a drop followed by a re-send reaches the receiver like a message held
until the re-send. So the verdict applies unchanged. Cress was re-run on the final tree (`cress/console.txt`):

| configuration | Cress |
|---|---|
| `per_entry_n9_4k` | HAZARDOUS, grows, 175.60× (reduced workload: 10 rounds, horizon 200, empty bodies) |
| `per_entry3_n9_4k` | HAZARDOUS, **stops**, 2.80× (same reduced workload) |
| `reliable_resend_n9_4k` | HAZARDOUS, grows, 25.95× (40 rounds, horizon 1000, 4 KiB bodies) |

## Results (`runs/p_*`)

`python3 ../relay_broadcast_amplification/summarize.py --quota=0.05 runs/p_*`:

- "tail" is from 15 s after the trigger ends to the end of the run; "base" is the second half of the pre-trigger
  baseline.
- `wire_x` is the relays the members wrote to the kernel (out of band, `wchar / 4120 B`).
- `att_x` is the relay-sized sends the lossy queues were handed (out of band, counted in the transport), with the
  share dropped next to it.

| config | f | burst | its control | tail re-sends/s (in band) | wire_x base → tail | att_x tail (dropped) | member CPU / quota, throttled |
|---|---|---|---|---|---|---|---|
| per_entry | 1.5 | COLLAPSED | DRAINING¹ | silent | 28.0 k → 189 | 346 k (99.9 %) | 0.99, 0.99 |
| per_entry | 2 | COLLAPSED | DRAINING¹ | silent | 23.6 k → 239 | 368 k (99.9 %) | 0.99, 0.99 |
| per_entry | 3 | COLLAPSED | RECOVERED | silent | 13.9 k → 219 | 367 k (99.9 %) | 0.99, 0.99 |
| per_entry | 4 | COLLAPSED | RECOVERED | 22.8 k when a report gets out | 10.0 k → 265 | 385 k (99.9 %) | 1.00, 0.99 |
| per_entry | 6 | COLLAPSED | RECOVERED | silent | 6.6 k → 264 | 386 k (99.9 %) | 1.00, 0.99 |
| per_entry | 8 | COLLAPSED | RECOVERED | 32.2 k when a report gets out | 4.96 k → 258 | 395 k (99.9 %) | 1.00, 0.99 |
| per_entry3 | 2 | DRAINING² | RECOVERED | 38.7 k | 21.2 k → 47.1 k | 58.9 k (21 %) | 0.99, 0.99 |
| per_entry3 | 3 | RECOVERED | RECOVERED | 2 | 13.9 k → 13.5 k | 13.4 k (0) | 0.87, 0.33 |
| per_entry3 | 4 | RECOVERED | RECOVERED | 2 | 9.9 k → 10.2 k | 10.1 k (0) | 0.71, 0.16 |
| reliable_resend | 2 | DRAINING | DRAINING¹ | 1.40 k | 21.3 k → 21.7 k | 21.7 k (0) | 0.98, 0.76 |
| reliable_resend | 4 | DRAINING | DRAINING¹ | 1.42 k | 9.9 k → 11.5 k | 11.5 k (0) | 0.83, 0.25 |

1. A slope-noise label on a flat trace: wire_x / offered stays at 1.00, there are no drops and no re-sends to speak
   of, and `unacked` moves between a few hundred and about a thousand.
2. `unacked` falls slowly, because entries are given up after 3 sends, so Sedge reads the run as draining. The storm
   itself lasts the whole tail: 38.7 k re-sends/s, and the members write 2.2× their baseline bytes to the kernel.

All 22 runs pass the open-loop check (no record entered more than 50 ms late), and no node died. "COLLAPSED" for the
per-entry bursts is Sedge's out-of-band reading. The in-band reports stop for 54–60 of the 60 tail seconds. The
capped members write 1–7 % of their baseline bytes while using 99–100 % of their quota, and every node is still
running.

What the numbers say:

- **No-burst controls are stable at every f**, from 1.5 to 8. Member CPU against quota is 1.00, 0.97, 0.82, 0.66,
  0.51 and 0.39 at f = 1.5, 2, 3, 4, 6 and 8, the throttled fraction falls from 0.86 to 0.03, and member RSS stays
  at 7–8 MB.
- **`per_entry` stays collapsed after the burst at every f, up to f = 8**, which is λ = 71, an eighth of μ.
  - The members hand their send queues 346–395 k relay-sized sends per second. That is 12–80× the baseline wire of
    5–28 k/s, measured in the transport, outside the program's own reports.
  - 99.9 % of those sends are dropped at the full queues. The kernel receives 1–5 % of the baseline wire, so
    **outside-measured wire does not stay above baseline. It collapses.**
  - The source's writes to the members fall to 18–49 % of what it is offered at f ≤ 4, and its own queue grows
    (to 720 MB at f = 1.5).
  - The amplified work is the per-entry pump. Every timer tick it re-sends every overdue entry, about 8 k per member
    after the burst. That work uses the whole quota, so ticks run long, sockets are read rarely, acks do not get out,
    and every entry stays overdue.
- **`per_entry3`** recovers at f = 3 and 4. At f = 2 it keeps a re-send storm on the wire for the whole tail.
  That is the classic condition: up to 3× retry amplification of λ = 283 exceeds μ = 566.
- **`reliable_resend`** re-sends at a fixed rate of about 1.4 k/s and drains.

## Files

- `runs/<run>/results.tsv`, `runs/<run>/*.tsv`: Sedge's row and per-second trace for each run, with the
  out-of-band columns `wchar_kb_*`, `rchar_kb_*`, `syscw_*`, `rss_mb_*`, `cpu_*`, `nr_throttled_*`,
  `throttled_ms_*`, and the transport's `sendq_attempted[_kb]_*`, `sendq_dropped[_kb]_*`.
- `probes.tsv`, `calibration.tsv`: the μ probes on the final build.
- `cress/`: the three Cress reports, and `console.txt` with their verdict lines.
- `earlier/`: the unbounded and `bounded:Member` runs at the same cap, and the calibrations that came out invalid
  before Sedge's probe fix (`invalid_calibrations_before_probe_fix.txt`).
