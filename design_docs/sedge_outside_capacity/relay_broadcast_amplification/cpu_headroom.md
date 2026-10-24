# Can members get real CPU headroom at baseline?

This analysis uses only the existing traces in `traces/`. No new runs were made. To reproduce it,
run `python3 design_docs/sedge_outside_capacity/relay_broadcast_amplification/headroom.py` from
the repo root.

## Short answer

**Not with the current cap.** The cap is `cpus(Member = 0.01)` with `cpu_period_us = 100_000`, so
each member gets **1 ms of CPU, then is frozen for the rest of the 100 ms period**. At every
f ≤ 2 a member is frozen for most of every period. The kernel's minimum quota is 1 ms, so 0.01 cpu
can't be expressed with a shorter period. A different cap should give real headroom. It is
predicted below, not yet measured.

## Table (baseline windows; controls split into thirds)

`thr` is the share of CFS periods in which the member was throttled. `throttled_ms/s` is cgroup v1
`throttled_time`, which is summed over the cpuset's 2 CPUs, so it can exceed 1000. `ms/stall` is
`throttled_ms / nr_throttled`. `load/μ` uses the measured rounds/s.

| run | f | window | rounds/s | load/μ | member cpu | thr | throttled ms/s | ms/stall | cpu ms/round | re-sends/s |
|---|---|---|---|---|---|---|---|---|---|---|
| pe_ctl | 1.5 | [2,75) | 82–86 | 0.65–0.68 | 0.0100 | 1.00 | 930–960 | 93–96 | 0.12 | 31–54 |
| pe_ctl | 2 | [2,75) | 62–66 | 0.49–0.52 | 0.0100 | 0.98–1.00 | 800–820 | 82 | 0.15–0.16 | 8–37 |
| pe_ctl | 3 | [2,75) | 41–45 | 0.33–0.35 | 0.0100 | 0.87–0.93 | 560–620 | 63–66 | 0.22–0.24 | 2–6 |
| pe_ctl | 4 | [2,75) | 30–34 | 0.24–0.27 | 0.0097–0.0100 | 0.64–0.79 | 350–470 | 52–58 | 0.30–0.32 | 1–4 |
| pe_ctl | 1.25 | [2,10) | 101 | 0.80 | 0.0105 | 1.00 | 1100 | 110 | 0.10 | **8 800** (unstable on its own) |
| rr ctl / pre-burst | 1.25–2 | [2,10) | 68–107 | 0.50–0.78 | 0.0100 | 0.98–1.00 | 800–940 | 81–95 | 0.09–0.15 | 0–11 |
| rr burst tail | 4 | [45,75) | 34 | 0.25 | 0.0090 | 0.38 | 171 | 43 | 0.26 | 0 |
| rr (Part 1 README) | 6 | — | 23 | 0.17 | 0.0066–0.0081 | 0.15–0.23 | — | — | 0.30 | — |

## Why members are pinned at low f

1. **There is a fixed cost that doesn't depend on load.** Fitting member CPU against load over the
   65 member-seconds with at most 1 throttled period (λ = 20–44) gives about **3.2 ms/s + 0.18 ms
   per round**. The fixed 3.2 ms/s is about a third of the whole 10 ms/s quota, and it comes from
   the in-program 20 Hz timer, the 1 Hz report and the runtime wake-ups. The fit covers a narrow
   range, so treat it as approximate.
2. **Capacity without batching is only about f ≈ 3.5–4.** Without batching, the quota covers
   (10 − 3.2) / 0.18 ≈ 38 rounds/s, about μ/3.3. Below f ≈ 4 a member can keep up only by
   batching. Batching happens because the member is frozen for most of each period and arrivals
   pile up, so cost per round falls 0.30 → 0.10 ms as load rises. μ = 126 is this batched
   capacity. This is why `thr` → 1 for every f below about 4: the throttled fraction measures how
   fully the quota is used, not headroom. It matches the cutoff the README reports (`thr` < 0.6
   only at f ≥ 4).
3. **The 100 ms period is too coarse for this protocol.** A throttled member freezes for about
   80–95 ms per period at f ≤ 2, against a 50 ms timer and a 150 ms ack timeout (3 ticks). An ack
   crosses two members, each of which can be frozen for up to about 99 ms, so baseline ack latency
   can exceed the timeout with no overload at all. This explains the steady baseline re-sends of
   tens per second at f = 1.5–2. It is also the likely reason f = 1.25 storms with no trigger.
   Sedge's own `capacity.rs` warns against exactly this: keep the period at 5–10 ms, because long
   freezes show up as "a burst of missed timeouts rather than as a slower server".

## Recommendations

These follow the rules: only the members are capped, the source and clients stay uncapped and
open-loop, and the program is unchanged. They are predictions from the fit above, and each
needs a μ calibration and no-burst controls before it is trusted. Each is a new configuration,
so **Cress must run before Sedge**. Cress counts messages, not CPU, so its verdict should not
change, but the rule still applies.

| option | cap | freeze when throttled | fixed cost / quota | predicted capacity without batching | notes |
|---|---|---|---|---|---|
| **A (recommended)** | `cpus(Member = 0.05)`, `cpu_period_us = 20_000` (1 ms / 20 ms) | ≤ 19 ms, under one 50 ms tick | about 6 % | about (50 − 3.2) / 0.18 ≈ 260 rounds/s | f = 2 should run at about half the quota, with `thr` clearly below 1. Baseline wire is about 72 × 130 × 4 KiB ≈ 38 MB/s, and a 2μ burst is about 4× the current rates. |
| B | `cpus(Member = 0.1)`, `cpu_period_us = 10_000` (Sedge's default period) | ≤ 9 ms | about 3 % | about 540 rounds/s | Most headroom and the cleanest timeouts. A 2μ burst means the source drives about 1 000+ rounds/s with 4 KiB bodies (≈ 300 MB/s of member wire), so watch the 2-core node cpuset. |
| C | `cpus(Member = 0.02)`, `cpu_period_us = 50_000` | ≤ 49 ms, about one tick | about 16 % | about (20 − 3.2) / 0.18 ≈ 93 rounds/s | Borderline: two frozen hops can still reach the 150 ms timeout. Not recommended. |

Also:

- **Report utilization as `cpu / quota` and the freeze length, not just `thr`.** `thr` reads
  about 1 whenever demand within one period beats the quota, even when the member has spare
  throughput.
- **Smaller bodies** (for example 512 B) would cut the per-round cost and the wire volume under
  options A and B. That changes the deployed workload, so it is a new config.
- **No retuning inside the program.** Lengthening `ack_timeout_ticks` would also hide the freeze,
  but it is a protocol change (HydroFrac and Cress re-run) and doesn't add any headroom.
- **Under A or B the storm should still form after a burst.** A 2μ burst for 5 s still builds a
  backlog whose queueing delay is far past 150 ms. What should go away is the baseline's
  spurious re-sends. f = 1.25 then becomes a real test of metastability, where today it is a
  cap artifact.

## Caveats

- The fit uses member-seconds at λ = 20–44 only. No uncapped member was ever measured, so the
  per-round cost at high load without throttling is extrapolated.
- `cpu` is sampled at 1 ms resolution per second, which quantizes the fit.
- `throttled_time` is summed across CPUs, so `ms/stall` is an upper bound on the freeze length
  seen by any one thread.
