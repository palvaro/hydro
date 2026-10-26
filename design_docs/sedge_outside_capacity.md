# Sedge, rethought: capacity and triggers from the outside

**Status:** design, for the next agent to implement.
**Context:** this follows the end-to-end `relay_broadcast` example (`design_docs/relay_broadcast/README.md`), in which HydroFrac, Cress and Sedge were run on one program.

## 1. Principle

> The tools exercise **real code**. The harness may control what is *outside* the program: the load it offers, the resources it gets, and the faults injected into its environment. It may not change what is *inside*.

Sedge already does this for load: an open-loop generator attached to the program's external ports. It does **not** do it for capacity:

- **Capacity knobs in business logic.** Most example programs model their own capacity with knobs such as `max_per_tick`, `server_capacity`, `max_merges_per_tick`, `budget_per_tick` and `service_time` spin-loops. These knobs exist only to make overload reachable.
- **Harness-owned timers.** Sedge ticks the programs' timers itself, through external `()` ports driven by `harness::drive_timer`. In budget-gated programs the timer *is* the capacity: μ equals the per-tick budget times the timer frequency, so it is set by the harness's sleep loop.
- **`--dip` stretches those timers.** On budget-gated programs it removes the emulated capacity. On Raft it delays heartbeats. Either way it manipulates the program's internals.
- **μ is declared, not measured.** `run.mu.or(meta.mu)`, at `sedge/src/harness.rs:462-467`. In `design_docs/2026-10_sedge_on_corpus.md` the docs admit: "Nothing checks that μ is true".

The redesign moves capacity and capacity-dip triggers to the outside, using containers or cgroups. It then rewrites every example to remove the fake capacity modeling.

## 2. Audit: how much of the corpus emulates capacity

This audit is of the integrated tree, which matches `wip/sedge-cress-2026-10-15` for the corpus. Classes:
- **a**: the knob only emulates capacity and is removable.
- **b**: the knob is genuine protocol logic: admission control, retry budgets, backoff, shedding, flow control.
- **c**: no capacity knob.

### 2.1 Programs Sedge deployed

| # | Program (path under `hydro_test/src/cluster/`) | Knob(s) | Class | Sedge verdicts | Trigger |
|---|---|---|---|---|---|
| 1 | `rpc_retry.rs` `rpc_with_retries` | `ServerConfig::max_per_tick` (:92), `service_time` busy-wait (:402-405) | a (retries are b) | three_attempts COLL/DRAIN, one_attempt REC/REC | burst |
| 2 | `witnesses/blind_budget/service.rs` `retry_service` | `RetryConfig::server_capacity` (:53, :659), released by `server_clock` | a (client budgets b) | unbounded COLL/COLL, no_governance COLL/DRAIN, budgets REC | burst |
| 3 | `witnesses/blind/two_phase_commit.rs` | `Config::participant_capacity` (:56, :280) | a | COLL/COLL | burst |
| 4 | `witnesses/blind/visibility_queue.rs` | `WorkerConfig::max_per_tick` (:50, :254) | a | COLL/COLL | burst |
| 5 | `witnesses/blind/log_catchup.rs` | `CatchupConfig::leader_capacity` (:44, :289), filler `unrelated_work` (:153) | a | COLL/COLL | burst |
| 6 | `witnesses/blind/batch_flush.rs` | `SinkConfig::max_per_tick` (:69, :209) | a | COLL/COLL | burst |
| 7 | `witnesses/blind/credit_flow.rs` | `consumer_capacity` (:56), `producer_capacity` (:54); `credit_window` is b | a + b | REC/REC | burst |
| 8 | `witnesses/blind/token_bucket.rs` | `refill_tokens`, `bucket_capacity` (the rate limiter *is* the program) | b | REC/REC | burst |
| 9 | `witnesses/compaction_falls_behind.rs` `log_with_compaction` | `StoreConfig::budget_per_tick` (:173); read cost modeled as a counter (:150, :259-279); `compaction_reserve` (:176) | a | reserve_0 COLL/COLL, reserve_8 REC/REC | burst |
| 10 | `witnesses/cache_thundering_herd.rs` `cache_with_expiry` | `OriginConfig::max_fetch_per_tick` (:139, :401) | a (stands in for a slow origin) | fill_dated / request_dated COLL, coalesce REC | burst + `--dip origin_clock` |
| 11 | `witnesses/gossip_resend.rs` | `GossipConfig::max_merges_per_tick` (:95, :212) | a | resend_on COLL/COLL, resend_off REC | burst |
| 12 | `relay_broadcast.rs` | `RelayConfig::max_merges_per_tick` (:118, :295) | a | resend variants COLL/DRAIN/REC, echo_only REC | burst |
| 13 | `raft.rs` `raft_server` | none ("the protocol has no per-tick cap") | c | burst: REC ×4. `--dip heartbeat_timer_interrupts=8`: pinned COLL, broadcast DRAIN | burst, dip |

### 2.2 Programs only Cress ran

| # | Program | Class |
|---|---|---|
| 14 | `witnesses/backoff_retry.rs` | a + b |
| 15 | `witnesses/bounded_queue_rejection.rs` | a + b |
| 16 | `witnesses/election_stampede.rs` (with a filler `Msg::Client`) | a |
| 17 | `witnesses/lease_renewal_storm.rs` (with filler `other_work`) | a |
| 18 | `witnesses/rebalancing_ping_pong.rs` | a + b |
| 19–23 | `bounded_fanout`, `pure_heartbeat`, `productive_tc`, `two_pc`, `cress_inputs_check` | c (the honest controls) |

### 2.3 Totals

- **16 of 23** programs emulate capacity internally.
- **11 of the 13** programs Sedge deployed emulate capacity internally.
- **Every** COLLAPSED or DRAINING verdict under a load burst is in a class-a program. Every class-b and class-c program that Sedge ran under a burst RECOVERED.
- **Every** Cress HAZARDOUS verdict involving real extra work is in a class-a program. Raft `contested` reports 2.00×, but the cluster total is flat: "a change of sender, not extra work".
- The only collapse without a knob, Raft, needed `--dip`. That stretches the harness-driven heartbeat timer, which makes it a **timing fault**, not overload. Under a load burst Raft recovered in all 4 trials, because load cannot delay a heartbeat that the harness drives.
- **Only one program does any real work per request**, and that work is a spin-wait (`rpc_retry`). Every other server does an echo, an ack, or integer arithmetic.

### 2.4 Would collapse happen without the knobs?

Mostly **not at the rates tested**. With the knobs gone, each request costs microseconds, queues never outgrow the 150 ms–2 s timeouts, and the retry or resend mechanisms never fire. The mechanisms are real, though, and should amplify given a real bottleneck. Supplying that bottleneck honestly is the job of this redesign.

## 3. Design

### 3.1 What the harness may control

| Lever | Today | New |
|---|---|---|
| Offered load | open-loop Poisson on external ports | unchanged |
| Capacity | knobs in the program, μ declared | **resource limits on each deployed process** (container or cgroup), with **μ measured** |
| Capacity-dip trigger | `--dip` stretches program timers | **lower the CPU quota mid-run**, then restore it |
| Protocol timers | external ports ticked by the harness | **in-process** `source_interval`, so real CPU starvation delays them, as in production |
| Timing-fault trigger (Raft-style) | `--dip` | kept, but **labelled as a timing fault** and reported separately from overload |

Capacity is then whatever the real code achieves under the given resources. Per-message costs such as serialization, syscalls, Tokio scheduling and the actual merge or handler all count.

### 3.2 Mechanism: containers or cgroups

The infrastructure survey found these facts about the current host:
- Kernel 5.10, **cgroup v1**, systemd 219.
- **No user cgroup delegation**: the cgroup mount is read-only, `systemd-run --user` has no bus, and `systemd-run` without `--user` requires authentication.
- **Docker works** (25.0, cgroupfs driver, 8 CPUs).
  - `docker run --cpus 0.5` held a spin loop at 49.8% CPU.
  - A live `docker update --cpus 0.2` took effect within seconds.
  - `docker update --cpu-period` fails once NanoCPUs has been set. To change the period, start containers with `--cpu-period`/`--cpu-quota` instead of `--cpus`.
- `taskset` works.

**Recommendation:** use Docker as the primary backend and a raw cgroup as an optional backend for hosts with v2 delegation. Put both behind one trait:

```rust
trait CapacityBackend {
    async fn launch(&self, node: &NodeSpec, limits: &Limits, real_bin: &Path, args: &[String]) -> Handle;
    async fn set_cpu(&self, h: &Handle, quota: CpuQuota);   // dip / restore
    async fn cpu_stat(&self, h: &Handle) -> CpuStat;          // nr_throttled, throttled_time, usage
}
struct Limits { cpu: CpuQuota /* quota_us, period_us */, cpuset: Option<CpuSet>, memory: Option<u64> }
```

**Where to inject it.** Every localhost node is spawned in `LocalhostHost::launch_binary` (`hydro_deploy/core/src/localhost/mod.rs:143-281`), using `Command::new(&binary.bin_path)` (:204-206). That function already supports a `perf`/`samply` prefix (:158-196), so wrapping the command with a launcher prefix is a known-good shape. Two options:

1. **Sedge-local (no upstream change).** A `CgroupHost`/`ContainerHost` wraps `LocalhostHost`. `Host` and `LaunchedHost` are public traits, and the wrapper is added with `deployment.add_host`. It delegates everything except `launch_binary`. There it clones `BuildOutput` (all fields are `pub`, `rust_crate/build.rs:101-108`), sets `bin_path` to a shim, and passes `SEDGE_REAL_BIN` / `SEDGE_LIMITS` through the environment. The shim then runs `exec docker run --rm --network host --cpu-period P --cpu-quota Q -v <bin>:<bin> -v <libdir> … <img> <real_bin> "$@"`, or joins a cgroup and execs the binary.
2. **Upstream (cleaner).** Add `launcher_prefix: Vec<String>` to `TrybuildHost`/`RustCrate`, mirroring how `pin_to_core` becomes `taskset` on SSH hosts (`ssh.rs:395-396`). Also add an optional `pid()` to `LaunchedBinary`, which today keeps its `child` private (`launched_binary.rs:46-67`).

Start with option 1 and propose option 2 upstream once it works.

**Notes:**
- `--network host` keeps hydro_deploy's localhost TCP ports working unchanged.
- Use a minimal base image with a matching glibc. `amazonlinux:latest` is already present, but check its glibc against the host build.
- Name containers deterministically (`sedge-<run>-<node>`) and remove them on exit, including on panic, by registering a guard in `run_trial`. Expect about 1 s of startup per node.
- hydro_lang has a separate `DockerDeploy` backend (`deploy_graph_containerized.rs`, feature `docker_deploy`). It sets no CPU limits today, although bollard supports them. Moving Sedge to it would require making the harness and macro generic over `Deploy`, plus a per-program image build. This is **not** recommended as the first step.

### 3.3 CPU limit semantics: important details

- **Use a short CFS period.** With the default 100 ms period, a 20% quota means a process runs for 20 ms and is then throttled for up to 80 ms. That is longer than a 50 ms Hydro tick, so it shows up as a burst of missed timeouts rather than as a slower server. Use `--cpu-period` of 5–10 ms (the minimum is 1 ms). Also record `nr_throttled` and `throttled_time` from `cpu.stat` in the trace, as ground truth.
- **Quota is summed across all threads.** Tokio worker threads can use up the quota in parallel. Optionally fix the worker count for deployed binaries, or say in the report that it isn't fixed.
- **Isolate the harness.** The load generator, observer and Sedge process must not compete with the capped nodes for CPU. Give the nodes a `cpuset` (for example cores 4–7) and pin the harness to the rest (`taskset`).
- **What to avoid:** `cpu.shares`/`cpu.weight` only take effect under contention, so they are not a cap. `cpuset` alone only offers whole-core steps.
- **Programs with trivial per-message work need a tight cap**, perhaps a few percent of a core. That is legitimate: it is the real code on a small machine. It does make noise from a shared host matter more, so use multiple trials.

### 3.4 Calibrating μ

Delete `mu` from the attributes, or keep it only as a hint for the probe's starting point. Before each sweep, and for each set of limits, run a probe:

1. Deploy with the target limits.
2. Ramp the offered load open-loop in steps, e.g. 10 s each, increasing geometrically.
3. Take μ = the highest offered rate at which goodput ≥ 0.95 × offered and the backlog levels are flat. Bisect between the last good step and the first bad one.
4. Record μ, the CPU usage and the throttling statistics in `results.tsv`. The sweep factors f = μ/λ now mean what they claim.

The definition of "goodput" here comes from the stream kinds in Cress's `conservation.tsv`, as today. Calibration should be cached per (program, config, limits, host fingerprint).

### 3.5 Triggers

- **Load spike:** unchanged. The trigger runs at `trigger_multiple` × μ, with μ now measured.
- **Capacity dip (new, external):** at `trigger_start` the harness calls `set_cpu(h, quota / K)` on the selected nodes, and at `trigger_end` restores the quota. This uses `docker update --cpu-quota`, or a write to `cpu.cfs_quota_us` (v1) or `cpu.max` (v2). Allowed targets are all nodes, one role, or one member. Example CLI: `--cpu-dip member=4 --cpu-dip-nodes member:0`.
- **Timing fault (old `--dip`):** keep it only for programs whose timers really are external inputs. Report it in a separate verdict category, `TIMING_FAULT`, not as overload. Since protocol timers now run in-process (§3.6), a CPU dip may reproduce Raft's heartbeat collapse honestly, by starving the leader. That is a good experiment to run.

### 3.6 Timers

Today a `timers(x = period)` parameter becomes an external port ticked by `drive_timer` (`harness.rs:375-400`). In the new design the **Sedge macro binds timer parameters to an in-process `source_interval(period)`** on the parameter's location. The program's signature still takes `timer: Stream<(), L, …>`, so Cress (which feeds timers as sim inputs) is unaffected. Only the binding in the Sedge harness changes. This makes timers part of the program's real execution, so CPU starvation and queueing delay them as they would in production.

The old staggering of cluster members (`period/members`) is unnecessary with in-process timers, because members start at different moments anyway. A random phase can be added as an option.

### 3.7 Verdicts and traces

- Keep RECOVERED / DRAINING / COLLAPSED (`verdict.rs::judge_with_kinds`).
- Add per-node `cpu_usage`, `nr_throttled` and `throttled_ms` columns per second, read from `cpu_stat`.
- Add a sanity gate: a run where the nodes **never throttle** and the generator fell behind (`late` arrivals, `generator.rs`) is **INVALID**. That case shows the harness is the bottleneck, not the program.
- In the summary, report μ_measured, the limits and the host.

## 4. Rewriting the examples

**Rule:** remove every class-a knob. Keep class-b logic: retry budgets, backoff, admission control, shedding (`max_backlog`) and credit windows are the subjects under test. Process every arrival. Timers drive only protocol timeouts, and they run in-process.

Steps for every class-a program:
1. Delete the capacity field (`*_capacity`, `max_per_tick`, `budget_per_tick`, `max_merges_per_tick`, `max_fetch_per_tick`, `work_per_tick`), the `clock.count() * K` budget, and the `enumerate().cross_singleton(budget)` served/backlog split.
2. Delete filler-load channels (`unrelated_work`, `Msg::Client` in election, `other_work`) unless they become real work.
3. Delete `service_time` spin-loops.
4. Where the protocol's realistic work is non-trivial, implement **real** work: a real KV operation, a real log scan, a real lattice merge over real state, a real file append + `fsync`. Otherwise leave the work trivial and let the external CPU cap supply the bottleneck.

Per-program notes:

| Program | Remove | Real work left / to add | Keep (class b) |
|---|---|---|---|
| `rpc_retry`, `retry_service`, `backoff_retry`, `bounded_queue_rejection` | `max_per_tick`, `server_capacity`, `service_time` spin | a real handler (e.g. KV get/put or hashing) | `max_attempts`, token budgets, circuit breaker, backoff, `max_backlog` |
| `two_phase_commit` (witness) | `participant_capacity` | prepare = append to a log file + `fsync` | prepare timeout |
| `visibility_queue` | `max_per_tick` | real per-task work | visibility timeout |
| `log_catchup` | `leader_capacity`, `unrelated_work` | serve chunks from a stored log | `chunk_size`, `retry_after_ticks` |
| `batch_flush` | `max_per_tick` | real per-copy processing | — |
| `credit_flow` | both capacities | — | `credit_window` (expected to stay benign) |
| `log_with_compaction` | **full rewrite**: the counter-based cost model | real segments; gets that scan uncompacted records; compaction that really merges; "reserve" becomes a real compaction schedule (class b) | — |
| `cache_with_expiry` | `max_fetch_per_tick` | the origin becomes a separate process doing real work, CPU-capped from outside | `coalesce`, `request_dated`, TTL |
| `gossip_resend`, `relay_broadcast` | `max_merges_per_tick` | real lattice merge; cap members externally | `ack_timeout_ticks`, resend policy, relay policy |
| `election_stampede` | `budget_per_tick`, `Msg::Client` | — | timeout spread |
| `lease_renewal_storm` | `max_per_tick`, `other_work` | — | resend-after policy |
| `rebalancing_ping_pong` | `work_per_tick` | real task execution | `threshold`, `cooldown_ticks` |
| `token_bucket` | — (class b) | an in-process refill timer; add a real downstream, since without one "RECOVERED" is trivially true | bucket |
| `raft` | — | in-process timers (check what `raft_sedge_check` binds) | everything |

`relay_broadcast` specifics:
- `max_merges_per_tick` is gone and the inbox becomes a plain merge.
- `RelayChannel` stays. It is a real deployment choice, not capacity modeling.
- HydroFrac results should not change, because the budget never affected HydroFrac. Re-run the HydroFrac tests anyway; the label line constants in `relay_broadcast_ldfi.rs` (:237/:250) will move.

### 4.1 Effect on Cress

Cress runs in the simulator and counts work, not time, so it needs no capacity model to detect "delay causes extra sends". Some of its growth shapes ("grows", "stops"), and the multipliers it feeds to Sedge, were measured *with* per-round budgets, though. After the rewrite:
- Re-run Cress on every rewritten program, recording before and after verdicts and multipliers.
- Expect HAZARDOUS/BENIGN to mostly hold, since the mechanisms are still there. Expect the shapes and multipliers to change.
- The Cress→Sedge prediction (`μ/λ > A`, `μ > λ + b`) must be revalidated against the measured μ.

## 5. Plan

Milestones 1 and 2 can run in parallel with the first program rewrites.

1. **M1: backend and spike.**
   - Implement `CapacityBackend` (Docker), the `ContainerHost` wrapper, the shim, cleanup, the `cpuset` split and `cpu_stat` collection.
   - Validate on `raft` (class c, no rewrite needed): deploy it under a cap and confirm that throttling stats appear and that the run is not INVALID.
2. **M2: calibration and in-process timers.**
   - Implement the μ probe and cache, and bind timers to `source_interval` in `sedge_macro`.
   - Validate μ reproducibility (±10% across 3 runs) on one program.
3. **M3: rewrite one witness end to end.** Take `rpc_retry`: remove `max_per_tick` and the spin, add a real handler.
   - Run Cress before and after.
   - Run a Sedge sweep with f ∈ {1.5, 2, 4, 8} and 3 trials.
   - **Success criterion:** `three_attempts` COLLAPSES at low f and RECOVERS at high f, and `one_attempt` RECOVERS, all with **no internal capacity code**.
4. **M4: CPU-dip trigger.** Run it on `rpc_retry` and `raft`. For Raft, test whether starving the leader's CPU reproduces the heartbeat-driven collapse that previously needed `--dip`.
5. **M5: rewrite the rest of the corpus and `relay_broadcast`**, following §4. Publish a before/after table of Cress and Sedge verdicts. Programs that stop collapsing are a **finding**, not a failure: they mean the old collapse was produced by the emulated capacity.
6. **M6: docs.** Update the Sedge README and attribute docs, `design_docs/relay_broadcast/README.md`, and the "how they fit" overview.

## 6. Risks and open questions

- **The generator may be the bottleneck.** With trivial handlers and loose caps, the single-connection TCP generator may saturate first. The INVALID gate (§3.7) catches this. Fixes are tighter caps or multiple generator connections.
- **Host noise.** The host is shared with other cargo jobs. Use `cpuset` isolation, run multiple trials, and avoid sweeping during builds.
- **cgroup v1 vs v2.** The semantics are the same but the file names differ. Docker hides the difference.
- **Container overhead.** `--network host` avoids the bridge. Startup costs about 1 s per node. Check that the container image's glibc matches the host build.
- **Tokio threads and the quota.** Consider a fixed worker count for deployed binaries.
- **Memory limits** (`--memory`) are a natural second lever (backlog causing OOM is a collapse mode), but are out of scope for M1–M4.
- **Network constraints** (bandwidth, latency, loss via `tc netem` inside the container's network namespace) are another external lever and a natural follow-up. They need non-host networking, so they are out of scope for now.
- **Open:** does Raft collapse under an external CPU dip on its leader once its timers are in-process? A yes would be a strong demonstration of the new design.
- **Open:** without budgets, do Cress's self-sustaining growth shapes survive, or are they artifacts of the per-round budget?
