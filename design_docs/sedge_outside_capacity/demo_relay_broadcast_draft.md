<!--
DRAFT. This is the skeleton for the rewrite of `demo_relay_broadcast.md`. It replaces that file (and
its PDF) once the Sedge results it is waiting on are in. Every `TODO(sedge)` is a placeholder
for a result that has not been measured yet. Every number outside a TODO is taken from a
committed file, named where it is used. The old `demo_relay_broadcast.md` stays until then: its
results are what §5 of this draft corrects.
-->

# Demo: `relay_broadcast` under HydroFrac, Cress and Sedge, with capacity imposed from outside

This demo runs one program, `hydro_test/src/cluster/relay_broadcast.rs`, through the three tools.
The program contains **no capacity code**. Each tool answers a different question:

- **HydroFrac** asks whether the members can end up disagreeing under crashes and lost messages.
  The `Echo` policy can, because it has too little redundancy: one lost echo plus a source crash
  is enough. The redundant fix, `ReliableResendPerEntry`, which re-sends every relay until it
  is acknowledged, is certified by HydroFrac, but only when retries are unlimited.
- **Cress** asks whether delay makes the program do extra work. It finds that the fix's
  re-sends are a *retry amplifier*: late acknowledgements cause re-sends, and each re-send is
  more wire, merge and acknowledgement work. The fix is HAZARDOUS, with a 175.60× multiplier.
- **Sedge** deploys the program with each member's CPU capped from outside, and offers it an
  open-loop load with a short burst.
  - TODO(sedge): one sentence on the outcome. For example: stable without the burst at
    f = …; after the burst it collapses and stays collapsed at f ≤ …, and recovers at f ≥ ….
  - TODO(sedge): one sentence on the evidence that the *amplification* sustains the collapse:
    in the collapsed tail, re-sends run at …× and outside-measured wire traffic at …× their
    pre-trigger baseline. These are the same streams Cress reports.

**What changed since the previous version of this demo.** The previous version reported a
collapse of the per-peer `ReliableResend` policy at f ≤ 2. That collapse was **not** sustained
by retries. It was sustained by the program rebuilding its whole table of unacknowledged
relays on every tick, which costs work proportional to the backlog. With that table made
persistent state, the per-peer policy recovers from the same burst at every f tried. Its
re-sends are about 4 % of normal traffic, a bounded, additive load
(`relay_broadcast_amplification/README.md`, Part 1). This version therefore uses
`ReliableResendPerEntry`, whose retry load grows with the backlog, and asks whether *that*
amplification sustains a collapse.

Contents:

1. [The program](#1-the-program)
2. [HydroFrac](#2-hydrofrac)
3. [Cress](#3-cress)
4. [Sedge](#4-sedge)
5. [Discussion](#5-discussion)

Everything here was produced on the branch `feat/hydrofrac-cress-sedge-e2e`.
TODO(sedge): host (cores, CPU model, cgroup version, Docker version) of the runs below.

Raw data:

- [`relay_broadcast_amplification/results.tsv`](relay_broadcast_amplification/results.tsv): one
  Sedge row per trial. TODO(sedge): run groups added for this demo.
- [`relay_broadcast_amplification/traces/`](relay_broadcast_amplification/traces/): per-second
  traces, with the out-of-band columns (`wchar_kb_*`, `rss_mb_*`, …).
- [`relay_broadcast_amplification/cress/`](relay_broadcast_amplification/cress/) and
  [`relay_broadcast_amplification/hydrofrac.txt`](relay_broadcast_amplification/hydrofrac.txt):
  the Cress reports and the HydroFrac verdicts.

The plots are made by [`plot_amplification.py`](demo_relay_broadcast/plot_amplification.py),
which reads only committed traces. The PDF is rendered with the `rpc_retry` demo's
[`render_pdf.py`](demo_rpc_retry/render_pdf.py):

```sh
python3 -m pip install --target $TMPDIR/py matplotlib markdown-it-py "fpdf2<2.8"   # Python 3.7
export PYTHONPATH=$TMPDIR/py
# One call per burst run, each with the no-burst control of the same factor and length.
python3 demo_relay_broadcast/plot_amplification.py \
  --burst relay_broadcast_amplification/traces/TODO/factorF.trial0.tsv \
  --control relay_broadcast_amplification/traces/TODO/factorF.trial0.tsv \
  --out demo_relay_broadcast --prefix TODO --memory-mb 400
python3 demo_rpc_retry/render_pdf.py demo_relay_broadcast.md demo_relay_broadcast.pdf \
  $TMPDIR/py/matplotlib/mpl-data/fonts/ttf
```

All commands in this document run from `design_docs/sedge_outside_capacity/`, except the
`cargo`, `./scripts/cress` and `./scripts/sedge` commands, which run from the repository root.

**Terms used below.**

- **Tick:** one step of a Hydro location's dataflow. In one tick the location takes whatever
  input has arrived and processes it as a batch.
- **Timer tick:** a tick that saw an element of the members' 50 ms protocol timer. Re-sends
  are judged only on timer ticks.
- **μ** (mu): the highest request rate the deployed system keeps up with.
- **λ** (lambda): the offered rate.
- **f = μ/λ:** the *headroom*. f = 2 means the system is offered half of what it can handle.
- **Open loop:** requests arrive on a fixed schedule, whether or not the system keeps up.
- **Throttled:** the kernel has stopped a capped container because it used up its CPU quota for
  the current period.
- **In band / out of band:** the members' own once-a-second reports, which travel the same
  queues as their traffic (in band), against kernel counters that Sedge reads from outside the
  members (out of band).

## 1. The program

`relay_broadcast` has one `Source` process and a cluster of `Member`s.

- The **source** broadcasts each request (an id and a 4 KiB body) to every member.
- If the source crashes part-way through a broadcast, some members have the request and others
  never will. So the **members relay** what they receive to each other. `RelayPolicy` chooses
  how:
  - `None`: no relaying.
  - `Echo`: a member that gets a request *from the source* sends it on (echoes it) to every
    peer, once.
  - `EchoResend`: `Echo`, and each echo is re-sent until the peer acknowledges it.
  - `ReliableResend`: a member relays every request on its *first receipt from anyone*. It keeps
    each relay in an *outstanding* table until the peer acknowledges it, and re-sends at most
    one overdue relay per peer per timer tick.
  - `ReliableResendPerEntry { ack_timeout_ticks, max_attempts }`: the same relaying, but
    *every* relay whose acknowledgement is `ack_timeout_ticks` timer ticks overdue is re-sent
    on each timer tick, with no per-peer or per-tick cap. After `max_attempts` sends (`None`:
    never) the relay is given up.

**The correctness claim.** `delivered`, the set of ids each member has received, is labelled
*eventually consistent*: in the end, every live member has the same set. The label is an
`assert_has_consistency_of` with a hand-written argument. HydroFrac's job is to check that
argument.

TODO(doc): quote the `assert_has_consistency_of` block with its current line numbers.

**Why `Echo` is not enough.** An echo is sent once. If it is lost, nothing repairs it. If the
source also crashed before reaching that member, the member never learns the id.

**The fix: `ReliableResendPerEntry`.** Two kinds of redundancy are added:

- A member relays an id the first time it hears it *from anyone*. So if a relaying member
  crashes part-way through, any peer it did reach relays the id onward.
- Each relay has its own retry timer and is re-sent until acknowledged. So a lost relay is
  repaired, however many times it is lost.

The re-send loop is the outstanding table's `Pump` event (`relay_broadcast.rs:933-961`). On a
timer tick, every relay whose deadline has passed is re-sent, in deadline order, and its
deadline restarts:

```rust
TableEvent::Pump { now } => {
    let mut out = Vec::new();
    if ack_timeout_ticks > 0 && per_entry {
        // Every relay whose deadline has passed, in deadline order.
        while let Some(&(deadline, peer, id)) = self.due.first() {
            if deadline > now {
                break;
            }
            self.due.pop_first();
            let e = self
                .entries
                .get_mut(&(peer, id))
                .expect("every deadline has its entry");
            e.attempts += 1;
            out.push(TableOut::Wire {
                peer,
                id,
                body: e.body.clone(),
                resend: true,
            });
            if max_attempts > 0 && e.attempts >= max_attempts {
                // The last attempt: give the relay up.
                self.entries.remove(&(peer, id));
                self.len -= 1;
            } else {
                e.deadline = now + ack_timeout_ticks;
                self.due.insert((e.deadline, peer, id));
            }
        }
    }
    // ...
}
```

The number of re-sends on a timer tick is the number of overdue relays. When acks are late
across the board, that is the whole backlog, so the retry load grows with the backlog. The
per-peer policy's load, by contrast, is capped at one per peer.

**The table is persistent state.** It is one top-level `scan` over a stream of table events
(acks, first receipts, timer ticks), and each event is applied in place:

- an ack is one `remove`;
- a first receipt is one `insert` per peer;
- a timer tick costs O(due entries).

Nothing is copied or rescanned between ticks. The dedup of first receipts (a `HashSet` of ids)
lives in the same `scan`.

TODO(doc): line numbers for the `scan` and for `TableEvent`.

The Sedge configuration (`relay_broadcast.rs:487-498`):

```rust
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = per_entry_n9_4k,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: None }, channel: RelayChannel::FailStop },
    mu = 150,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
```

- `mu = 150` is only where the capacity probe starts. μ is measured.
- `cpus(Member = 0.01)` with `cpu_period_us = 100_000` gives each member 1 ms of CPU in every
  100 ms. The source has no cap and is fed open-loop.
- The timers run inside the program. The 50 ms `timer` is the protocol's re-send clock, and
  `report_tick` is the members' once-a-second report.

**No capacity code.** A member merges whatever has arrived. Its capacity is whatever the code
achieves on 1 % of a core. There are no per-tick budgets, filler load or harness-driven timers.

**Program changes since the previous version of this demo.** HydroFrac, Cress and Sedge were
re-run after each one.

1. **Persistent outstanding table.** It replaces a tick-scoped `KeyedSingleton` that was rebuilt
   and filtered in full on every tick. This removed the per-tick cost proportional to the
   backlog that sustained the previous version's collapse (§5).
2. **Dedup by id only, inside the table's `scan`.** The old top-level `unique` over
   `(id, body)` kept every received body alive, a slow RSS creep of about 0.3 MB/s even
   without a burst.
3. **`ReliableResendPerEntry`**, the policy described above.

## 2. HydroFrac

HydroFrac (lineage-driven fault injection) searches for a set of faults (crashes and lost
messages) under which a collection labelled eventually consistent ends up different at different
members. When it finds one, it reports it as a **counterexample**. When its search runs out
without finding one, it reports "no violation found".

The setup comes from `hydro_test/src/cluster/relay_broadcast_ldfi.rs`:

- **Size and faults:** 3 members and 2 request ids. The source and every member can crash.
- **Channel:** `lossy_delayed_forever`, so any single relay may be lost.
- **Two schedules for the timer.**
  - *Standard:* the test sends both requests, waits for quiet, declares the end of finite
    failures (the *cut*: no new crash or loss after it) and then fires the timers. Every retry
    after the cut is delivered.
  - *Stricter:* `3 · max_attempts + 1` timer rounds fire *before* the cut, so HydroFrac can omit
    the retries as well as the first sends.
- **Schedules are sampled:** 8 per fault set (`Exploration::Fuzz { instances: 8 }`).

TODO(hydrofrac): replace with fresh output from this tree once the re-run is in. The lines
below are excerpts from `relay_broadcast_amplification/hydrofrac.txt`, with `...` marking cuts:

```
$ cargo test -p hydro_test --lib cluster::relay_broadcast -- --nocapture --test-threads=3
  delivered  :576                    DIVERGES under omit(relay Relay { from: 0, id: 2, body: Body(b"") } member 0→member 1) + crash(source after sending bcast Request { id: 2, body: Body(b"") } to member 0)
relay_broadcast Echo over LossyDelayedForever, 0 timer rounds before the cut: 2.6s
...
  delivered  :576                    DIVERGES under omit(relay Relay { from: 0, id: 2, body: Body(b"") } member 0→member 1) + omit(relay Relay { from: 0, id: 2, body: Body(b"") } member 0→member 2) + crash(source after sending bcast Request { id: 2, body: Body(b"") } to member 0)
relay_broadcast ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(3) } over LossyDelayedForever, 10 timer rounds before the cut: 12.2s
relay_broadcast ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(5) } over LossyDelayedForever, 16 timer rounds before the cut: 16.1s
...
  delivered  :576                    no violation found (247 fault sets, 8 sampled schedules each)
relay_broadcast ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: None } over LossyDelayedForever, 0 timer rounds before the cut: 154.2s
...
  delivered  :576                    no violation found (239 fault sets, 8 sampled schedules each)
relay_broadcast ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: None } over LossyDelayedForever, 10 timer rounds before the cut: 191.5s
...
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 244 filtered out; finished in 716.55s
```

Each verdict line is printed just before its test's header line. The trimmed log does not
keep the verdict line for `max_attempts: Some(5)` with 16 rounds before the cut. That test's
name (`relay_per_entry_5_diverges_when_every_attempt_is_omitted`) asserts the divergence, and
it passed. TODO(hydrofrac): keep the line in the fresh output.

| policy | timer rounds before the cut | result |
|---|---|---|
| `Echo` | 0 | counterexample: omit relay 0→1 of id 2, plus a source crash |
| PerEntry, max_attempts 3 / 5 / none, unbounded crashes | 0 | no violation (256 / 241 / 247 fault sets) |
| PerEntry, max_attempts 3 | 10 | counterexample |
| PerEntry, max_attempts 5 | 16 | counterexample |
| PerEntry, max_attempts none | 10 | no violation (239 fault sets) |

**Reading.**

- **`Echo`: counterexample.** The source sends id 2 to member 0 only, then crashes. Member 0
  echoes id 2 to its peers, but the echo to member 1 is lost. `Echo` never re-sends, so member 1
  never gets id 2, while members 0 and 2 have it. Two faults are enough: one crash and one loss.
- **A retry limit is a hole.** In the stricter setup the source again crashes after reaching
  only member 0, and every attempt of member 0's relays of id 2 to members 1 and 2 is omitted.
  Once member 0 has used up its attempts it gives the relays up. Re-sends are identical bytes,
  so one omission covers every copy.
- **Only unlimited retries are certified.** `max_attempts: None` holds in both setups, and it
  is the configuration deployed below. "No violation found" covers the sampled schedules, at 3
  members and 2 ids. It does not prove that no violation exists.
- **The redundancy is the hazard.** HydroFrac's argument for unlimited retries is exactly
  "keep re-sending until acknowledged". §3 shows that this is what Cress flags.

## 3. Cress

Cress runs the program in the simulator and asks one question: **does delay cause extra work?**
It *holds* one decision point at a time (a `use::batch` or `use::snapshot`) for a range of
lengths, and counts every output stream and every message against the unheld run.

```sh
./scripts/cress --config per_entry_n9_4k hydro_test relay_broadcast
```

```
! HAZARDOUS  relay_broadcast (per_entry_n9_4k)  growth: grows  multiplier: 175.60x  location: hydro_test/src/cluster/relay_broadcast.rs:616, batch of Arrival
    outputs: amplified: wire, merged, acks_sent, delivered; shed: outstanding_depth, metrics
```

The full report is in
[`relay_broadcast_amplification/cress/per_entry_n9_4k.txt`](relay_broadcast_amplification/cress/per_entry_n9_4k.txt).
The two decision points that react are the members' batch of arrivals (`:616`) and their batch
of acks (`:618`):

```
hold length (rounds)                      1      2      4      8     16     32     64    128    199
hydro_test/src/cluster/relay_broadcast.rs:618, batch of Ack
    extra records admitted                0    432   2592  12096  42768 104976 229392 478224 627912
    extra messages, busiest sender        0     16     96    448   1584   3888   8496  17712  27936
    extra messages, all senders           0    144    864   4032  14256  34992  76464 159408 251424
    first reacted at a hold of 2 rounds with 432 extra; largest extra work 627912 in admitted records
    beyond the input (holds 16, 32, 64, 128): extra sends from Cluster(loc2v1) @8 keeps growing: rate 144.00 per round of delay, no ceiling within the run (rates 144.000, 144.000, 144.000)
    beyond the input (holds 16, 32, 64, 128): extra admitted records keeps growing: rate 3888.00 per round of delay, no ceiling within the run (rates 3888.000, 3888.000, 3888.000)
    multiplier: 175.60× the unheld run's 160 sends from Cluster(loc2v1) @8 (+27936 at a hold of 199 rounds)
    under a hold that never ends: +627912 admitted records (3155.3 per round of the run)
```

**Reading.**

- **Late acks cause re-sends.** Holding a member's acks makes every relay it sent look
  overdue, so it re-sends all of them every three timer ticks. Holding its arrivals has the same
  effect on its peers, because the acks for those arrivals are not sent.
- **The amplified streams** are `wire` (the re-sends themselves), `merged` and `delivered`
  (each re-send is received and merged again) and `acks_sent` (each merged duplicate is
  acknowledged again). §4 measures the same four streams in deployment.
- **It grows with the backlog.** The busiest member gains 144 extra sends per round of delay,
  with no ceiling within the run. The per-peer `ReliableResend` gains 16 at a 25.95×
  multiplier, because it re-sends at most one relay per peer per timer tick.
- **The workload was reduced** to 10 rounds, a horizon of 200 and *empty bodies*. With
  40 rounds, a horizon of 1000 and 4 KiB bodies the simulator, which keeps every buffered
  re-send, grew to 10.5 GB and was killed. Cress counts messages, not bytes, so body size does
  not change its counts. The Sedge configuration keeps 4 KiB bodies.
- TODO(cress): the verdict for every new Sedge configuration run in §4. Cress runs first. In
  particular: does `--send-buffer bounded:Member` need its own check, or is it a deployment
  option outside Cress's model? Also `per_entry5_n9_4k` and `per_entry3_n9_4k` if they are
  deployed.

## 4. Sedge

### Configuration

- `per_entry_n9_4k`: 9 members, 4 KiB bodies, `ReliableResendPerEntry { ack_timeout_ticks: 3,
  max_attempts: None }`.
- Members capped at 0.01 cpu per 100 ms, each in its own container. The source is uncapped
  and open-loop.
- Release build (`--deploy-profile profile`). Nodes and harness on separate cpusets.
- Memory limit of 400 MB per node. A node that dies makes the trial **INVALID**: Sedge reads
  `State.OOMKilled` and the exit code of every node before teardown. An OOM kill is a crash,
  not a collapse.
- Out-of-band counters: Sedge reads `/proc/<pid>/io` and `VmRSS` of every member's processes
  from a sampler container, four times a second. `wchar / 4120 B` is the number of relays a
  member actually wrote to its sockets.
- **Send buffers.** TODO(sedge): `--send-buffer bounded:Member`, and what it does to the
  members' own dataflows. The source's feed stays open-loop. State whether the feed-lateness
  check held (`feed_late`, `feed_max_lag_ms` in `results.tsv`).
- **μ.** TODO(sedge): μ for this configuration and send-buffer mode, from `probes.tsv`. The
  unbounded-buffer μ was 126.1 rounds/s.
- **Trigger.** A load burst at 2μ for 5 s starting at 10 s, then a 60 s tail. Sedge's verdict
  window is the second half of the tail, [45, 75) s. TODO(sedge): the 1.5μ trigger runs.

### Commands

```sh
# Cress first, on every new configuration.
./scripts/cress --config per_entry_n9_4k hydro_test relay_broadcast
# Burst runs.
./scripts/sedge --config per_entry_n9_4k --deploy-profile profile --send-buffer bounded:Member \
  --memory-mb 400 --harness-cpus H --node-cpus N --factors F --trials T --tail 60 \
  hydro_test relay_broadcast
# The no-burst control at factor F: the trigger window runs at λ = μ/F, so the load never changes.
./scripts/sedge ... --trigger-multiple $(python3 -c "print(1/F)") ...
```

TODO(sedge): the exact commands used, with cpusets and seeds.

### Results

TODO(sedge): the outcome table, one row per (factor, trigger), each burst run paired with its
no-burst control of the same length.

| f | λ | trigger | trials | burst outcomes | control outcomes | tail re-sends/s | tail wire_x / baseline | max member RSS |
|---|---|---|---|---|---|---|---|---|
| TODO | – | 2μ, 5 s | – | – | – | – | – | – |
| TODO | – | 1.5μ, 5 s | – | – | – | – | – | – |

What to state under the table:

- TODO(sedge): where the controls are stable, and where the load alone is too much (with
  unbounded buffers, f = 1.25 was unstable on its own).
- TODO(sedge): where the burst runs collapse and where they recover. At the boundary, list the
  mixed outcomes across trials.
- TODO(sedge): every INVALID trial and its reason. A crash is not counted as a collapse.

### Is the collapse sustained by the amplification?

The question this demo has to answer is whether the collapsed tail runs on the streams Cress
reports, not just whether something collapsed. For each collapsed run, the tail is compared
with the same run's pre-trigger baseline and with its no-burst control
(`plot_amplification.py`'s table):

TODO(sedge): paste the table for the showcase collapsed run, and for a recovered run at higher f
for contrast.

| stream | burst: pre-trigger /s | burst: tail /s | burst: ratio | control: pre-trigger /s | control: tail /s | control: ratio |
|---|---|---|---|---|---|---|
| re-sends | – | – | – | – | – | – |
| wire (in band) | – | – | – | – | – | – |
| merged = delivered (in band) | – | – | – | – | – | – |
| acks_sent (derived) | – | – | – | – | – | – |
| wire, outside (wchar) | – | – | – | – | – | – |

What would show it, and what would not:

- **It would show it** if, in the collapsed tail:
  - re-sends stay far above baseline;
  - wire traffic measured from *outside* stays far above the 72 first relays per offered
    request;
  - merged and acks_sent rise with it;
  - member CPU stays at the quota and RSS stays below the limit.

  The members would then be busy with the amplified work and not with new requests.
- **It would not show it** if outside wire traffic falls to about zero while RSS climbs, which
  is what happened with unbounded buffers (the work queued in memory, not on the wire, and
  ended in an OOM). It would also not show it if the controls at the same f are unstable
  (that is overload), or if the in-band reports stop and only Sedge's reading of a gauge says
  "collapsed".

### Time series

TODO(sedge): the four figures from `plot_amplification.py` for the showcase collapsed run, and
the re-send and wire figures for a recovered run.

<!--
![re-sends and unacked relays, collapsed](demo_relay_broadcast/TODO_resends.png)
![wire measured outside, RSS and CPU, collapsed](demo_relay_broadcast/TODO_wire.png)
![Cress's amplified streams normalized to the control](demo_relay_broadcast/TODO_streams.png)
![tail against baseline](demo_relay_broadcast/TODO_tail.png)
-->

### CPU headroom at baseline

A member's cost per round falls as load rises, because each tick handles a larger batch. So
under a fixed quota it uses nearly all of the quota at almost any load. With unbounded buffers
the mean throttled fraction at baseline was 0.98–1.0 at f = 1.5–2, 0.80–0.94 at f = 3 and
0.60–0.85 at f = 4. Below 0.6 was reached only at f ≥ 4–6.

TODO(sedge): what was tried in this round to give the members real headroom at baseline, and
what was achievable (a table of f against member CPU and throttled fraction). If real headroom
at f ≤ 2 was not achievable under this cap, say so.

## 5. Discussion

**The previous collapse was not caused by retries.** The previous version of this demo found
`ReliableResend` collapsing at f ≤ 2 after a burst, and its own analysis already saw the
re-sends die down while the backlog kept growing. The cause was the program's state, not its
retry policy. The outstanding table was tick-scoped state, rebuilt and filtered in full on
every tick, so every tick cost work proportional to the backlog, and the members' outgoing
messages starved behind it. With the table made persistent:

- the same configuration and burst recover at every f tried (1.25, 1.5, 2 and 4);
- they also recover from a burst four times as long, which builds a 250 k-entry backlog;
- re-sends, capped at one per peer per timer tick, add about 270/s across the cluster, about
  4 % of normal traffic, far below the headroom.

Cress does not model per-tick cost, since it counts messages, so it could not have seen this.
Data: `relay_broadcast_amplification/README.md`, Part 1.

**Retries that grow with the backlog.** `ReliableResendPerEntry` re-sends every overdue relay.
After a burst, with unbounded send buffers, it fell into a re-send storm of 6–10 k/s at every
f from 1.5 to 4. But the storm piled up in user-space send buffers instead of on the wire, and
members were OOM-killed at 400 MB 30–45 s after the burst. Those runs are crashes, not
collapses, and are reported as INVALID.

TODO(sedge): what bounded member send buffers changed. Is the storm now on the wire, and is the
collapsed state stable without a crash?

TODO(sedge): the boundary. Where it recovers, and the mixed outcomes at the boundary.

TODO(sedge): the conclusion on whether the amplification Cress identifies sustains the
collapse, with the evidence above. If it does not hold, say so and say what does sustain it.

**Caveats.**

- Cress's verdict was computed on a reduced workload with empty bodies (§3).
- HydroFrac's "no violation found" covers sampled schedules at 3 members and 2 ids.
- TODO(sedge): number of trials per point, number of hosts, and anything run only once.
