# Work amplification as adversarial scheduling

Status: design note, step 2 of the metastability project. E0 (canonical program + deterministic
sim collapse) and E2 (per-edge counting under two schedules, with controls) are done; no
lineage code exists yet. Ground truth:
`hydro_test/src/cluster/rpc_retry.rs`, a Hydro request/response service whose client has a
timeout-and-retry policy, driven to a metastable collapse by the test harness both on localhost
and, deterministically, in the simulator (step 1: commit `bbd3ca02c5`; E0: `92bd747dc7`; the
program was called `retry_storm` until the restructuring described under "E2: results / Program
hygiene").

A note on process: the previous branch's history and this project's own first E0 attempt both
show the same failure mode — an agent works for hours without compiling or running anything
and reports progress it cannot demonstrate. Every experiment here ends with a command that was
actually run and whose output is recorded. Claims without output are not results.

## The question

A metastable failure needs a positive feedback loop: a briefly overloaded system generates
more work in response to the overload, and the extra work keeps it overloaded after the
trigger ends. We want to ask of an arbitrary Hydro program: *can it be made to amplify
work, by how much, and for how long?*

The previous attempt (branch `metastable`) tried to answer this by labelling emissions.
It tagged every record with the set of source events it descended from, split sources
into "data" and "operational" (timers), and classified an emission as productive or
reactivated by whether its data ancestry was novel. The label was load-bearing and had to
be declared per program; the program's own timers had to be rewritten by hand; anti-joins
dropped the negative side's lineage and `unique` erased the second derivation's, so the
instrument was blind at exactly the operators where amplification is born. Its documents
admit that metastability was never demonstrated and that "reactivated" is not the same as
"amplifying" (fixed-rate replay and backlog-proportional replay both look reactivated).

## The reframing

Do not classify emissions. Ask instead whether the *scheduler* can make the program do
more work on the same inputs.

The Hydro simulator already explores nondeterminism: given fixed inputs, it enumerates or
fuzzes the decisions at every declared nondeterminism point (which buffered items a
`batch` releases into a tick, in what order, when a member crashes, when membership
changes). Today that search looks for safety violations. The amplification checker runs
the same search with a different objective: **find a decision sequence that maximizes the
number of records crossing the program's edges, per input record, and report whether that
maximum is bounded or grows with the length of the run.**

Definition. Fix an input set (every source, timers included, is just an input). Let
$W_e(\sigma)$ be the number of records that cross edge $e$ under decision sequence
$\sigma$. The program **amplifies on $e$** if $W_e$ is not constant over $\sigma$. Its
**gain** on $e$ is $\max_\sigma W_e(\sigma) / \min_\sigma W_e(\sigma)$. It is
**unboundedly amplifying** if the gain grows with the length of the run (the adversary
always has another move).

This is FLP's adversary with the opposite objective. FLP's scheduler uses only legal
delivery choices to keep a system from ever finishing; ours uses only legal delivery
choices to keep it from ever stopping. Both are existence arguments over schedules, and
both are answered by the same machinery: a search over decision sequences.

Why this needs no data/timer distinction. Timers are inputs and are held fixed across
schedules. Whatever changes between two runs is due to decisions, not to what arrived.

Why it separates the false positives without knowing what they are:

- Heartbeats: every decision sequence yields the same work. The adversary has no moves.
- Productive recursion (transitive closure): confluent under set semantics; the derivation
  set is fixed by the data. No moves.
- Timeout/retry: holding a response past the timeout produces one more derivation of the
  same request onto the client→server edge, up to `max_attempts`. Gain 3, bounded. With
  unbounded retries, gain grows with run length.
- Gossip: flagged iff some decision (e.g. holding acks) widens what gets re-sent. Whether
  the CRDT gossip library amplifies is an empirical question the checker can answer.
- Election storms (Paxos/Raft): holding heartbeats past the election timeout re-derives
  the leader and replays the uncheckpointed log to every acceptor. Existing Hydro code.

Delay is not the only move. Each hook dimension is a real feedback edge:

| scheduler decision           | feedback edge it exercises                                  |
|------------------------------|-------------------------------------------------------------|
| release timing (hold / never)| timeout-driven retry; loss-driven retransmit                |
| release size                 | arrival rate vs. capacity; the collapse itself is this      |
| release order                | gap repair, retransmit, stale-ballot rejection              |
| crash / membership           | state transfer, log replay, full resync on rejoin           |
| interleaving of conflicting inputs | OCC / 2PC aborts and retries under contention         |

## What lineage is for

Lineage is not the verdict. It is the adversary's gradient and the report's explanation.

- *Attribution.* When schedule $\sigma'$ produces more records than $\sigma$ on edge $e$,
  the extra records that share ancestry with a held delivery are re-derivations of that
  delivery's goal. This identifies "the same goal" dynamically, by the perturbation, rather
  than by a rule about which sources are identity-bearing.
- *Search guidance.* A delivery that appears as a leaf in existing derivations, and whose
  absence the program tests (anti-join, timeout), is a candidate move. Molly removes support
  to make a goal fail; we withhold support to see how many new derivations the program
  manufactures in response.
- *Explanation.* deriv1: "I tried." deriv2: "I timed out and retried." Two derivations of
  the same goal, differing in delivery and clock leaves rather than data leaves. In LDFI's
  terms that difference is redundancy over deliveries, i.e. fault tolerance; here it is
  amplification. Same mechanism, seen from the cost side.

Representation constraints learned from the previous branch: derivation records as a
side log keyed by record id, not root sets carried in payloads (fold accumulation swamped
those); anti-join and `unique` must *record* the dropped derivation, not erase it;
cross-tick state carry (`use::state` re-deriving every entry each tick) must be reported
separately from location-crossing edges or it dominates every count.

## What the simulator gives us (at `bbd3ca02c5`)

- No virtual time. `source_interval` cannot run (no tokio time driver); `Instant::now()`
  is wall clock. The repo's own sim tests thread timers as stream parameters
  (`raft.rs` `election_timer_interrupts`, `multi_paxos_live.rs`) and feed them with
  `sim_input`. Time in the sim is decisions, not seconds.
- Per-item holding exists: every `batch` buffers into a typed `VecDeque` and a `StreamHook`
  decides per tick what to release; unreleased items wait indefinitely. This is the "hold a
  delivery" move. Hooks are a plain trait (`SimHook`), so an adversarial policy is a new
  hook implementation or a custom bolero driver.
- Hooks already see the concrete `Vec<T>` released per edge per tick with the operator's
  source location: the natural counting point for $W_e$.
- IR rewrite before codegen has precedent (`apply_dynamic_membership`,
  `splice_versioned_networks`): a pass that inserts observation on intra-tick edges or wraps
  items has a mechanical home.

## Honesty notes

- An unconstrained adversary that holds every response forever makes any retry program do
  "unbounded" work, and that is correct behavior against a dead server. The report must be
  about the *shape* of the win: gain as a function of how much perturbation the adversary
  had to inject. A program that needs infinite delay to reach 3× is different from one where
  a small delay past a threshold triggers self-sustaining work. The ground truth is the
  second kind; the sim reproduction should show the threshold.
- Amplification is not collapse. Collapse is amplification × offered load > capacity, which
  needs a load and capacity model. The checker owns amplification; the step 1 experiment
  supplies the rest for one program.
- Programs whose work depends on input *shape* but not on schedule (a chain vs. a star for
  TC) are not flagged, deliberately. Contention-driven amplification (aborts) needs inputs
  that make conflict possible, but the signal is still that a serial interleaving does less
  work than a contended one.

## Feasibility experiments, in order

- **E0. Canonical program.** One program, not two: time enters as stream parameters (the
  Raft pattern); deployment passes `source_interval`, the sim passes `sim_input`. `last_sent`
  becomes a logical clock value; the busy-wait stays for deployment and is a no-op in the sim,
  where `max_per_tick` is the capacity. Re-run the deployed test to confirm it still collapses.
  Then reproduce the collapse deterministically in the sim under a round-robin tick policy
  with every hook releasing everything: backlog and abandon rate never return to baseline
  after the trigger window.
- **E1.** Hand-compute expected per-edge counts under a prompt schedule and under
  "hold responses past the timeout" before measuring anything. Done against the canonical
  program (see "E1: expected counts" below).
- **E2. Counting only, no lineage.** Per-edge record counts at the hooks, two schedules, same
  inputs. Expected: client→server and server-processed rise 1× → 3× per request in
  retry_storm; unchanged with `max_attempts = 1`; unchanged for Raft driven with heartbeats
  only; unchanged for a monotone loop. If this fails, lineage will not rescue it.
- **E3. Lineage as side derivation records**, used to attribute E2's extra records to the held
  deliveries, with anti-join and `unique` recording drops.
- **E4. The adversary as a `SimHook`**, choosing moves from E3's records; show it finds the
  loop unaided, that gain saturates at `max_attempts`, and grows without bound at
  `max_attempts = ∞`.

Constraints carried over from the previous branch: no per-program role labels; no
quiesce-per-firing harness that smuggles semantics in; the pattern examples (gossip,
heartbeats, TC, elections) stay in the mental model and are drawn from existing Hydro code
where possible rather than encoded as bespoke tests.

## E0: results

(Names below are those of the E0 commit; see "E2: results / Program hygiene" for the current
ones: `rpc_retry.rs`, `rpc_with_retries`, `Workload`, `RetryPolicy`, `ServerConfig`.)

Done. `hydro_test/src/cluster/retry_storm.rs` is now one program: `retry_storm(...)` takes
three clock streams (`client_clock`, `client_report_tick`, `server_report_tick`);
`retry_storm_deployed` (behind the `tokio` feature) wires `source_interval`s into them; the
sim tests wire `sim_input`s. `Instant::now()` is gone: the client keeps a logical clock (a
`use::state` singleton holding the highest clock index seen), `InFlight.first_sent/last_sent`
and `Completion.latency_ticks` are tick numbers, and the timeout is `timeout_ticks`. The
busy-wait remains for deployment and is skipped when `service_time` is zero (sim);
`max_per_tick` is the capacity in both worlds. The dataflow itself (the client `sliced!` block
with `outstanding`, the server FIFO backlog, `forward_ref` responses) is unchanged from
step 1; `outgoing` was made `TotalOrder` by sorting retries so the sim has no
`assume_ordering` shuffle hook.

Simulator support added (`hydro_lang/src/sim/prompt_schedule.rs`, `compiled.rs`, `flow.rs`):
`PromptScheduleDriver`, a bolero `Driver` that answers every "how many to release" with the
maximum, every "which one" with the first (round-robin ticks, FIFO items), and every boolean
with `false`; `CompiledSim::run_with_driver(driver, thunk)` mirroring `fuzz_repro`; and
`SimFlow::run_prompt(thunk)`. This turns a simulation into a deterministic discrete-time
execution. The E4 adversary is the same shape with a different policy.

Deployed (`retry_storm::deployed_tests`, `--features tokio`, 222 s, assertions unchanged
except 20 ms → 4 ticks at the 5 ms clock; BASE `timeout_ticks: 40`): backlog 0 through
t = 11 s; 376 at 12 s; 2560 at 13 s; 5236 at 14 s; from 14 s on the client completes 0/s,
sends 800 retries/s and abandons 400/s while the server processes ~900/s of which two thirds
are retries; backlog 7426 at 20 s, 10286 at 30 s, 13288 at 40 s. No trigger: 400/s, zero
retries, zero backlog. `max_attempts = 1`: tail latency 0, tail backlog 0. Same shape as the
step 1 run.

Simulator (`retry_storm::sim_tests`, 33 s, deterministic): a round is one `client_clock`
element followed by `quiesce()`; per round exactly one client tick, one server tick, one
observation tick. Config: baseline 2/round, `max_per_tick` 5, trigger 12/round over rounds
[100, 160), `timeout_ticks` 40, 3 attempts, 800 rounds. Backlog: 0 through round 99; 217 at
130; 420 at 159 (= 60 × 7, as hand-computed); 680 at 200; 838 at 400; 1038 at 600; 1237 at
799, i.e. +1 per round indefinitely (offered 2 new + 2 second + 2 third attempts = 6 > 5).
Rounds 600–800: 0 completions against a baseline of 400, 400 abandoned, server served 333
first attempts vs. 667 retries. No trigger: no retry ever, backlog 0. `max_attempts = 1`:
peak backlog 420, drains at 3/round, 0 by round 300.

The sim collapse contains no held deliveries. Every hook released everything on every tick;
the delay that trips the timeouts is emergent from the queue (the release-size dimension in the
table above). That is the first piece of evidence that delay is one move among several and
not the definition.

Not done in E0: the full `hydro_lang` sim test suite was only `cargo check`ed, not run; the
report `sliced!` blocks still exist in the program and run idle ticks in the sim (harmless).

## E1: expected counts for the canonical retry storm

Fixed input: $T$ clock ticks, $b$ requests minted per tick ($N = Tb$), no trigger window,
server capacity $c \ge b$ per server tick, timeout $\tau$ ticks, `max_attempts` $A = 3$.
From the client slice: a retry for request $k$ fires at client tick `now` iff $k$'s response
is not visible in that tick (`filter_key_not_in(responded_ids)`) and
`now - last_sent >= τ`. A response visible in the same tick as the judgement suppresses the
retry. Rounds are: clock element → client tick → server tick → response delivered to the
client's `batch` buffer.

Prompt schedule (round-robin ticks, every hook releases everything):

| edge                         | records | per request |
|------------------------------|---------|-------------|
| client → server (`outgoing`) | $N$     | 1           |
| server `processed`           | $N$     | 1           |
| server → client (responses)  | $N$     | 1           |
| `completions`                | $N$     | 1           |
| `abandoned`                  | 0       | 0           |
| `outstanding` state carry    | $N$     | 1 tick      |

Adversary holds every response in the client's `use::batch(responses)` buffer for $d$
client ticks before releasing it:

| edge                         | per request                                   |
|------------------------------|-----------------------------------------------|
| client → server, `processed`, responses | $1 + \min(A-1,\ \lceil d/\tau \rceil - 1)$ |
| `completions`                | 1 (first visible response)                    |
| duplicates dropped at `responded_ids` / join | $\min(A-1, \lceil d/\tau\rceil - 1)$ |
| `abandoned`                  | 0 (1 if held forever)                         |
| `outstanding` state carry    | $d$ ticks ($A\tau$ if held forever)           |

So the gain on the location-crossing edges is a step function of the perturbation: 1 for
$d \le \tau$, 2 for $\tau < d \le 2\tau$, 3 for $d > 2\tau$, saturating at $A$. Held forever:
client→server $3N$, processed $3N$, completions 0, abandoned $N$. Three consequences for the
instrument:

- The extra records are re-derivations of the *same* requests; the duplicates they produce
  are dropped at `responded_ids.unique()` and the join against `outstanding`. Those drops are
  where E3 must record, not erase.
- The `outstanding` state edge has gain $d$, not 3. State carry must be reported separately
  or it dominates every count.
- The collapse reproduction (E0 part B) is the other perturbation axis: same prompt policy,
  different *input* (trigger $6$/round against $c = 5$ for 600 rounds builds a backlog of
  ~600, i.e. ~120 rounds of queueing, well past $\tau = 40$), after which per-request
  derivations reach 3 with no hold decisions at all. If E2 measures the same gain on the same
  edges under both perturbations, that is evidence the delay-vs-load distinction is not
  fundamental: both are just schedules under which the server's answer arrives late.

## Next: E2

Counting only, no lineage. Per-edge record counts at the hooks (each hook already holds the
`Vec<T>` it releases and the operator's source location). Same inputs, two schedules under
`run_with_driver`: the prompt driver, and a driver that holds items in the client's
`use::batch(responses)` buffer for $d$ ticks. Expected (E1): client→server and server
`processed` per-request counts follow $1 + \min(2, \lceil d/\tau \rceil - 1)$; unchanged with
`max_attempts = 1`; unchanged for Raft driven with heartbeats only; unchanged for a monotone
loop. Then the same measurement under the trigger with the prompt driver, to check that the
load perturbation shows the same gain on the same edges as the hold perturbation.

## E2: results

Done. Every number below is from a test that runs under `cargo test` and asserts it:
`rpc_retry::amplification_tests` (three tests), `raft::amplification_control`, and
`hydro_std::ec_inference_demos::reliable_broadcast::amplification_control`.

### Program hygiene

Reviewing E2 surfaced that the ground-truth program did not read as a benign service that
happens to be vulnerable: the overload script (`trigger_*`) was part of the program's config
and the client minted its own load from it; the wire protocol carried `attempt` so that the
*server* could split its work into "first attempts" and "retries"; every name announced the
outcome. A checker that is meant to look at arbitrary programs cannot be validated on one that
labels its own failure. The program was therefore restructured, with every E0 and E2 number
re-measured and unchanged:

- `rpc_with_retries(client, server, requests, clocks…, RetryPolicy, ServerConfig)`: the client
  takes an application request stream, assigns ids, and retries per
  `RetryPolicy { timeout_ticks, max_attempts }`; the server has a FIFO and a per-tick budget and
  answers everything it serves. The wire carries `Request { id, body }` / `Response { id, body }`
  and nothing about attempts; the server has no notion of a retry.
- The workload (`Workload { baseline_per_tick, trigger_per_tick, trigger_start_tick,
  trigger_end_tick }`) lives in the harness: the sim tests feed the request stream per round,
  the deployment harness (`deploy_with_workload`) builds a generator from the same
  `source_interval` that drives the clock. Request bodies carry the tick they were issued in,
  which is how the tests know a request's issue tick.
- Metrics are what either side can legitimately know: the client reports completed, latency,
  sent, re-sent and abandoned; the server reports processed and backlog. The tests' "retry"
  numbers are the client's own re-send count, or (in the sim) the harness noticing that an id
  it has already seen served is served again.

Deployed after the restructuring: baseline 400/s at 0.03 ticks; tail (t ≥ 25 s) goodput 0/s,
6400 abandoned, client sent 19200 of which 12800 re-sends, server processed 15120, backlog
8056 → 11876. Simulator: the E0 trajectory below to the record (217, 420, 680, 838, 1038, 1237;
tail 0 completed, 400 abandoned, server served 333 ids for the first time and 667 again).

### Instrument

- **Counting** (`hydro_lang/src/sim/edge_counts.rs`). `SimHook` gained `edge_location()`,
  `pending_release_len()` and `buffered_len()`, implemented by the four `batch` hooks
  (`StreamHook`/`KeyedStreamHook` × `TotalOrder`/`NoOrder`). The scheduler's `run_hooks` records
  the size of each release into a thread-local `EdgeCounts` when the run is wrapped in
  `count_edges`; `SimFlow::run_with_driver(driver, thunk)` does the wrapping and returns the
  counts. Nothing in generated code or the IR changed; the dylib's statics are not shared with
  the host, so all recording is host-side.
- **Edge identity.** The hook's source location is the `sliced!` invocation, not the
  `use::batch` line, so every batch in a block shares a location. Hooks now also carry
  `std::any::type_name` of their element, and an edge is keyed `file:line:col#index <type>`
  (E2b added the hook's index within its tick, because `raft_server`'s two timer batches are
  both `()`). A real fix is an operator id in the IR metadata (E3's rewrite pass can assign one).
- **The hold driver** (`hydro_lang/src/sim/hold_schedule.rs`). `HoldScheduleDriver` is the
  prompt driver with a per-edge policy: `Prompt`, `Metered` (release one item per decision) or
  `Hold(d)` (an item first seen at the hook's $k$-th decision is released at its $(k+d)$-th).
  The scheduler tells the driver which hook is asking (`edge_counts::current_hook`: edge,
  cluster member, buffered length, decision serial), which is how a context-free bolero `Driver`
  gets a per-edge policy. `Hold` handles the totally-ordered protocol (one count question) and
  the unordered one (stop?/which? per item); keyed batches and `TopLevelFoldHook` are not
  supported.
- **Time must be a driver decision.** The E0 harness (one clock element, then `quiesce()`, per
  round) cannot hold anything: `run_hooks` forces the last undecided hook to release at least
  one item when no other hook released, and `quiesce()` runs the client tick until its buffers
  are empty, so a held response is forced out within its own round. (Measured: with the clock
  un-metered by mistake, all 300 clock elements were released in one tick, `now` jumped to 299,
  and 510 of 600 requests were retried immediately — the degenerate case the doc's "time
  collapses" warning describes.) So the E2 harness sends the whole clock up front and the
  driver **meters** the client's clock batch (one element per client tick) and its request
  batch ($b$ per tick): the tick count is logical time, and the `responses` hook can be held
  across ticks because the clock hook is the one that satisfies the forcing rule. When the clock runs dry the held hook is forced and the
  driver flushes everything (end-of-run flush, visible below).
- Under this harness the scheduler's round-robin runs the server tick once per two client
  ticks (150 server decisions for 300 client ticks), so baseline latency is 1 or 2 ticks
  (300 and 298 requests; the two minted last see 0). E1 assumed a constant $l$; the hand
  computation below uses each request's measured baseline latency $l_{id}$.

### Hold perturbation (`hold_schedule_amplifies_by_the_e1_step_function`)

Config: $b = 2$, no trigger, $c = 20$ (so that the 12 arrivals per server tick at gain 3 never
queue), $\tau = 40$, $A = 3$, $T = 300$ ticks, $N = 600$ requests. Baseline (metered clock,
$d = 0$) reproduces E1's prompt table exactly: 600 on client→server, `processed`, responses and
`completions`; 0 abandoned. Hand computation per request: visible at $t + l_{id} + d$; judged at
$t + k\tau$ while not visible and $t + k\tau \le T - 1$; the first $A - 1$ judgements re-send,
the $A$-th abandons; anything still held when the clock runs dry completes at $T - 1$. The test
compares this per request (sends and completion latency), not just in aggregate.

| $d$ | sends/request (measured = hand-computed) | client→server = processed = responses | completions | abandoned |
|-----|------------------------------------------|---------------------------------------|-------------|-----------|
| 0   | {1: 600}                                  | 600  | 600 | 0 |
| 20  | {1: 600}                                  | 600  | 600 | 0 |
| 38  | {1: 600}                                  | 600  | 600 | 0 |
| 39  | {1: 340, 2: 260}                          | 860  | 600 | 0 |
| 40  | {1: 80, 2: 520}                           | 1120 | 600 | 0 |
| 60  | {1: 80, 2: 520}                           | 1120 | 600 | 0 |
| 78  | {1: 80, 2: 520}                           | 1120 | 600 | 0 |
| 79  | {1: 80, 2: 300, 3: 220}                   | 1340 | 600 | 0 |
| 80  | {1: 80, 2: 80, 3: 440}                    | 1560 | 600 | 0 |
| 118 | {1: 80, 2: 80, 3: 440}                    | 1560 | 600 | 0 |
| 120 | {1: 80, 2: 80, 3: 440}                    | 1560 | 240 | 360 |

Reading: the step function of E1, with the threshold at $l + d > k\tau$. At $d = 39$ only the
requests with $l = 2$ cross $\tau$ (260 of them; the rest of the $l = 2$ requests are minted too
late for a retry to fire before the clock ends); at $d = 40$ both latencies cross. The 80
requests stuck at 1 send are those minted in the last $\tau$ ticks (no judgement before the
clock ends). At $d = 79$ the $l = 2$ requests cross $2\tau$; at $d = 80$ all do; gain saturates
at $A = 3$. At $d = 120 = 3\tau$, $l + d > 3\tau$ and the third judgement abandons: 360
abandoned, 240 complete via the end-of-run flush — E1's "held forever" row. Completion latency
under hold equals baseline $+ d$ for every request whose release tick exists, and $T - 1 - t$
for the ones flushed. Extra records are re-derivations of the same ids: `completions` stays at
600 while the location-crossing edges go to 1560.

### Controls

- **`max_attempts = 1`** (`hold_without_retries_changes_no_edge`), $d \in \{41, 120\}$:
  client→server, `processed`, responses and `outgoing` all stay at 600, equal to $d = 0$. What
  changes is the verdict, not the work: `completions` 80 / `abandoned` 520 (every request whose
  judgement tick exists is abandoned before its response is visible; the 80 minted in the last
  40 ticks complete in the flush).
- **Raft, heartbeats only** (`raft::amplification_control`, 3 members): one uncontested
  election, then 50 heartbeat-timer interrupts per member sent up front and metered (followers
  ignore theirs). The traffic batch (`(MemberId, RaftRpc)`, unordered) is held $d \in
  \{1, 5, 20\}$ on every member. Traffic records: 204 = 4 (RequestVote + votes) + 2 · 2 · 50
  (AppendEntries + acks), identical at every $d$; timer records 151 at every $d$; leader-view
  histories identical. The only thing that grows with $d$ is the number of decisions (155 →
  159 → 175 → 235), i.e. ticks that ran while items were held.
- **Monotone loop** (`reliable_broadcast::amplification_control`): `reliable_broadcast_closed`
  (echo once, `unique()` closes the cycle), 20 messages to 3 members. All members deliver all
  20. The counter sees **zero** edges: the whole cycle is top-level dataflow with no `batch`,
  so there is no hook, no scheduling decision and nothing an adversary could vary. That is the
  right verdict ("no moves"), but it also shows the instrument's blind spot: edges outside
  ticks are invisible to hook counting. `g_set_gossip` would have been the alternative, but its
  network deliveries land in a `TopLevelFoldHook`, whose question protocol (include/exclude per
  element, then a permutation) `Hold` does not implement; also, under the prompt driver that
  hook releases one element (the newest) per decision, not everything. Both are recorded here
  rather than worked around.

### Load perturbation (`load_perturbation_shows_the_same_gain_on_the_same_edges`)

E0's config and harness (one clock element, `quiesce()`, 800 rounds; $c = 5$; trigger 12/round
over [100, 160)), `PromptScheduleDriver`, counted. The run is E0's run: backlog 1237 at round
799. Counts: client→server 4937 = `processed` 3700 + backlog 1237; responses 3700; `completions`
982; `abandoned` 978; sends per request {1: 619, 2: 425, 3: 1156}. Requests minted before the
trigger's queue exists ([0, 60)): all 1 send. Requests minted in [200, 720): all 3 sends. So
the load perturbation, with no held delivery at all, produces the same gain (3, saturated at
$A$) on the same three edges as the hold perturbation at $d > 2\tau$ — the evidence E1's last
bullet asked for: both are schedules under which the server's answer arrives late.

## E2b: a program not written for this (Raft election storms)

Every amplifying result in E2 was on `rpc_retry`, which we built. E2b asks whether the same
instrument sees amplification in existing code with no changes: `hydro_test/src/cluster/raft.rs`,
whose election timer is a timeout-and-retry loop on the leader (a follower that sees no
`AppendEntries` between two election-timer interrupts starts a new term). Test:
`raft::amplification_tests` (two tests). Nothing in `raft_server` changed.

Two small instrument additions were needed and are recorded here because they were foreseeable
from E2: the two timer batches in `raft_server` are both `()` streams in one block, so the hook's
position within its tick joined the edge key (`file:line:col#index <type>`; which index is which
timer is settled by the heartbeats-only control, where the election hook releases exactly one
interrupt); and a bursty policy `Periodic { period, count }` (release `count` or everything at
every `period`-th decision, nothing between) joined `Hold(d)`, because a constant delay only
shifts a periodic heartbeat while a gap is what a timeout notices. `Periodic { period: E,
count: Some(1) }` also paces the election timer at one interrupt per `E` ticks.

Setup: 3 members, one uncontested election, then 200 ticks with a heartbeat-timer interrupt
per tick on every member (followers ignore theirs) and an election-timer interrupt every
$E = 4$ ticks on every member, all sent up front and paced by the driver. The intra-cluster
traffic batch (the only edge deliveries cross: RequestVote, votes, AppendEntries and acks share
it) is scheduled promptly, with a constant delay `Hold(d)`, and in bursts `Periodic { period:
d }`, for $d \in \{1, 2, 3, 4, 5, 6, 12, 24\}$. Reported per run: traffic records on that edge,
the highest term reached (terms beyond 1 are elections after the first), and each member's
final leader view.

| $d$ | constant: traffic / terms | bursty: traffic / terms |
|-----|---------------------------|-------------------------|
| prompt | 804 / 1 | — |
| 1, 2, 3 | 804 / 1 | 804 / 1 |
| 4   | 334 / 51 | 804 / 1 |
| 5   | 338 / 51 | 633 / 75 |
| 6   | 342 / 51 | 701 / 69 |
| 12  | 364 / 51 | 366 / 53 |
| 24  | 409 / 51 | 411 / 53 |

Hand computation for the baseline: $4 + 2 \cdot 2 \cdot 200 = 804$ (election, then two
`AppendEntries` and two acks per heartbeat), measured 804; term 1 throughout. Heartbeats-only
control (no election timer): 804 and term 1 at every $d$.

Three findings:

- **The threshold is sharp and is the protocol's own liveness condition.** A constant delay of
  $d \ge E$ (round trip $2d$ longer than the election period) re-elects every period until the
  run ends: term 51 after 50 periods, and at the end two of three members have no leader at all.
  Bursty delivery with period $d$ leaves a gap of $d - 1$, so it storms from $d = E + 1$; at
  $d = E$ it is harmless. Below the threshold nothing changes, at any shape of delay. This is the
  "shape of the win" the Honesty notes asked for: gain as a step function of the perturbation,
  with the step where Raft's own analysis puts it (broadcast time $\ll$ election timeout).
- **The re-derived goal is the leader, 51 times instead of once, but the per-edge record count
  goes down.** Under the storm the traffic edge carries 334–409 records against 804 promptly:
  the leader's heartbeats (2 per tick, the productive steady-state work) disappear and a few
  election messages per period replace them. A single count on an edge that mixes both cannot
  see the storm; it reports *less* work. In `rpc_retry` counting sufficed because retries add
  records to an edge without removing any. So "more records on an edge" is not the general
  signal. "More derivations of the same goal" is, and that requires knowing which records derive
  which goal, i.e. E3's attribution. E4's objective should be derivations per goal (here: terms,
  or RequestVote records), not records per edge.
- **The storm is the metastable failure of Raft, found by a schedule alone.** No message was
  lost, no member crashed, the inputs were fixed; a delivery delay past a threshold left the
  cluster without a leader for the remainder of the run (the bursty $d = 5, 6$ cases churn even
  faster, 74 and 68 elections, with every member's final view leaderless). Whether it would
  recover if the delay were removed is the E4 question (the E0 sim collapse did not).

### What E2 says about the next steps

- Counting at hooks is sufficient to see the gain and its threshold in `rpc_retry`, and to
  see the two controls stay flat. But E2b shows it is not sufficient in general: when the
  amplified work displaces other work on the same edge (Raft's election storm), the count falls.
  E3 (lineage) is therefore needed for the verdict itself, not only for explanation: the
  quantity to maximize is derivations of a goal, and only attribution gives it.
- The instrument's coverage is exactly the set of `batch` hooks. Top-level edges
  (`reliable_broadcast`, the network sends themselves) and intra-tick edges are not counted;
  E3's IR pass is the place to add observation there.
- The hold is only expressible when something else in the same tick can satisfy the scheduler's
  forcing rule. A metered clock is the natural such thing; the E4 adversary will need the same
  structure (or a scheduler change that lets a tick run empty under a policy that asks for it).
- The `sliced!`-level location and the `()` collision in Raft argue for an operator id in the
  hook metadata before E4 names moves by edge.

## E3: design

Status: design. All three steps below are done and measured ("E3: results"): boundary ids, the
lineage pass, and the negative leaves. The negative-leaves result changed what lineage is for;
see "What lineage is for, revised" and "A third program: the redo queue in multi_paxos_live".

### The verdict is derivations per goal

E2 and E2b together fix what the checker must compute. A **goal** is a data item the program
exists to produce: an output record, identified by the input data it answers (the completion for
request $k$; the committed entry for client request $r$). Everything else the program derives
along the way (a response in flight, a leader, a vote, a heartbeat) is a **sub-derivation**: work
done toward some goal. A leader is not a goal; the protocol needs one in order to commit an
entry, so an election is work attributed to whatever entries were waiting for a leader.

The **derivation count** of goal $g$ under schedule $\sigma$, $D_g(\sigma)$, is the number of
records, over the edges of interest, that were derived toward $g$. The program amplifies on $g$
if $D_g$ varies with $\sigma$; gain is $\max_\sigma D_g / \min_\sigma D_g$; it is unbounded if
the gain grows with run length. This replaces E2's $W_e$, which E2b showed can *fall* while the
program storms (heartbeats displaced by election traffic on the same edge). A goal that is never
reached (Raft under the storm never commits) still accumulates derivations; that is the case the
checker most needs to report.

Expected on the two ground truths, to be hand-computed before measuring:

- `rpc_retry`, hold $d$ on responses: $D_k = 1$ under prompt; $1 + \min(2, \lceil (l+d)/\tau
  \rceil - 1)$ sends toward $k$ under hold, i.e. the E2 table, now per goal, with the second and
  third response to $k$ *recorded* as dropped at `responded_ids.unique()` and at the join against
  `outstanding`, not lost. E2's edge counts and the per-goal counts agree here because retries
  add records without displacing any.
- Raft with $R$ client requests submitted before the delay begins, election timer every $E$:
  under prompt each entry commits with one election (shared) and one `AppendEntries` per
  follower; under a constant delay $d \ge E$ nothing commits and every period adds an election
  attributed to the $R$ pending entries, so $D_r$ grows with the run while $W_e$ on the traffic
  edge falls (E2b: 804 → 334). This is the case where the two metrics must disagree; if the E3
  instrument does not show it, E3 has failed.

### Goals are named by the perturbation, not by labels

The previous branch labelled sources (data vs timer) to decide what counts as identity. That is
rejected here, as before. Instead: run $\sigma$ (prompt) and $\sigma'$ (adversarial) on the
same inputs; a record in $\sigma'$ is a re-derivation toward $g$ if its ancestry contains $g$'s
input record *and* a delivery the adversary held (as a negative leaf, see below). The adversary's
move names the goal: a held response names its request; a held heartbeat names the entries
waiting on the leader it would have confirmed. No program declares anything.

### Representation: a side log of derivation records

Constraints carried from the previous branch and confirmed by E2:

- Derivation records live in a **side log keyed by record id**, never as root sets carried in
  payloads (fold accumulation swamped those).
- **State carry is reported separately.** `use::state` re-derives every entry every tick;
  `outstanding` has gain $d$ under hold. Ancestry through state is followed at analysis time
  but state-carry edges are not counted as work.
- **Anti-join, `unique` and `first` record what they drop.** `filter_key_not_in(responded_ids)`
  is where a retry is born; `unique()` and the join against `outstanding` are where the
  duplicate answers die. Both must appear in the log.
- **The negative leaf is the held delivery.** When an anti-join fires at a tick, the records
  that would have suppressed it are sitting in a hook's buffer, unreleased. The scheduler knows
  them (`buffered_len` is already in `HookContext`; their ids are one step further). Recording
  them as the negative support of the retry's derivation record is what links the extra work to
  the adversary's move without any label.

The log is produced inside the compiled dylib (it is the program's operators that know their
parents) and must be shipped to the host: the dylib's statics are not shared (E2 note). Follow
the existing `println_handler` pattern: a host-provided sink function passed through
`__hydro_runtime`, or a dedicated output channel.

### Mechanism: an IR pass, staged

Parents cannot be correlated through a `q!` closure without wrapping the value, so E3 needs an
IR rewrite before codegen (precedent: `apply_dynamic_membership`, `splice_versioned_networks`)
that turns `Stream<T>` into `Stream<(RecordId, T)>` and each operator into one that computes the
payload as before and appends `(child id, operator id, tick, parents)` to the log. Rules:

| operator | derivation record |
|----------|-------------------|
| source, `sim_input`, timer | fresh id, no parents (a leaf) |
| map, filter, flat_map | child ← input |
| join, cross | child ← both inputs |
| fold, reduce, `use::state` | state version ← previous version + inputs this tick (state-carry edge, flagged) |
| `filter_key_not_in`, anti-join | child ← positive input, plus negative: the buffer contents of the hook feeding the negative side (held deliveries) |
| `unique`, `first`, keyed dedup | the surviving record as usual, plus a **drop record** for each dropped input, pointing at the survivor |
| `batch` (hook) | child ← input, plus the tick and the release decision (already counted by E2) |
| `send` / network | child ← input, crossing locations (the edges E2 counted) |

Do it in three steps, each ending with a run:

1. **E3.1, ids at the boundary only.** Wrap records at `batch` and at network receive
   (both already have hooks), assign ids there, and log (edge, tick, member, id, held ids). No
   intra-tick lineage. Attribution by the harness supplying an extractor for the goal key
   (`|r: &Request<T>| r.id`), as the E2 tests already do from `sim_output`. This gives per-goal
   counts for `rpc_retry` and for Raft's `AppendEntries` (they carry entries) but not for
   election traffic, and it is not generic. It is a week's insurance that the analysis and the
   Raft harness are right before the pass is written.
2. **E3.2, the pass.** Rules above; verify on `rpc_retry` that per-goal counts equal E2's
   per-request sends at every $d$ and that the drops at `unique`/join are recorded; verify on
   Raft that pending entries accumulate elections under the storm.
3. **E3.3, the perturbation link.** Negative leaves from held buffers; show that in `rpc_retry`
   every extra record's ancestry contains a held response, and in Raft every election's
   ancestry contains a held heartbeat. That is the report E4 will use as its gradient.

### Open questions for E3

- Operator ids: hooks are keyed `file:line:col#index <type>` (E2b). The pass should assign a
  stable operator id in `HydroIrOpMetadata` and the hooks should carry it; retire the index.
- Coverage: E2's counting sees `batch` hooks only. The pass observes every edge, which also
  closes E2's blind spot (reliable broadcast, network sends, `TopLevelFoldHook`).
- Cost: the previous branch's instrument was unusable at scale. Log per firing, not per
  ancestor; compute closure at analysis time; measure the slowdown on the 800-round E0 run.
- The forcing rule (E2): the adversary can only hold when another hook in the tick releases.
  E4 will need either the metered-clock structure or a scheduler option that lets a tick run
  empty under an explicit policy. Decide during E3.3, when the negative leaves make the
  interaction concrete.


## E3: results

### E3.1: ids at the boundary

Done. Tests: `rpc_retry::amplification_tests::boundary_ids_attribute_every_extra_record_to_its_request`
and `tracing_the_e0_collapse_attributes_every_send_and_costs_little`,
`raft::amplification_tests::boundary_ids_attribute_append_entries_to_their_entries`. Every number
below is asserted by one of them.

**Instrument** (`hydro_lang/src/sim/lineage.rs`). The ids are kept on the host, not in the program.
A `batch` buffer only grows at its back (the DFIR `for_each` that feeds it) and only shrinks
through a decision the scheduler sees, so the host keeps, per hook, a list of `(id, arrival
tick)` parallel to the buffer: before each decision it extends the list with fresh ids for
whatever arrived since the last one; after the decision it asks the hook which *positions* of the
pre-decision buffer it is releasing (`SimHook::pending_release_positions`, new; the unordered
hook's `remove(idx)` sequence is monotone, so a position is `idx` plus the removals so far) and
removes those. What remains is the held set. Record contents come across as bincode when the
element type is `Serialize` and as a `Debug` string when it is `Debug`, through two function
pointers the generated code stores in the hook (`__maybe_serialize__!`, the `__maybe_debug__!`
shadowing trick with a `Serialize` bound); the host decodes with its own copy of the type, as
`sim_output` does. This is the "function pointer passed in" pattern the E2 note prescribes, with
nothing stored on the dylib side. `SimFlow::run_traced` (and `CompiledSim::run_traced`) return
`(EdgeCounts, Lineage)`; the log is a `Vec<Release>`, one per hook decision: edge key, member,
the hook's tick (its decision index, the same count `Hold` uses), a run-wide serial, the released
records (`id`, `arrived`, `payload`, `debug`) and the ids still held. Attribution is by the
harness: `Lineage::per_goal::<T, G>(edge, |record: T| goals)`. Keyed batches report no positions
and are not logged (the hold driver does not support them either). Nothing is recorded unless the
run is wrapped in `trace_lineage`, and the hooks are only asked to serialize when it is.

**`rpc_retry`, hold $d$ on responses** ($b = 2$, $\tau = 40$, $A = 3$, $T = 300$, $N = 600$; the
E2 configuration). $D_k$ is the number of records carrying request $k$ on an edge. On all four
work edges (client→server, `processed`, responses, `outgoing`) $D_k$ equals, request by request,
the hand-computed sends of E2's step function *and* the client's own count of sends of $k$ from
the `outgoing` output; the goal edge (`completions`) has $D_k = 1$ for every completed $k$ and no
record for an abandoned one; `abandoned` has one record per abandoned $k$. The log's record count
equals the E2 counter on every edge. Every record on every edge came across as bincode.

| $d$ | $D_k$ histogram, measured = hand-computed | completions / abandoned | responses held exactly $d$ / flushed at end |
|-----|-------------------------------------------|-------------------------|---------------------------------------------|
| 0   | {1: 600}                                   | 600 / 0 | — (no hold to check) |
| 39  | {1: 340, 2: 260}                           | 600 / 0 | 740 / 120 |
| 40  | {1: 80, 2: 520}                            | 600 / 0 | 960 / 160 |
| 80  | {1: 80, 2: 80, 3: 440}                     | 600 / 0 | 1080 / 480 |
| 120 | {1: 80, 2: 80, 3: 440}                     | 240 / 360 | 840 / 720 |

The per-record arrival ticks show the hold itself: every response released by the client's
`responses` hook was released exactly $d$ ticks after it arrived, except those in the hook's last
release (the end-of-run flush, when the clock runs dry and the forcing rule fires), which were
held less. And at every release before the flush, the ids the release reports as *held* are
exactly (by count and by id) the responses that arrived within the last $d$ ticks. That is the
negative-leaf information E3.3 needs, already in the log; what is missing is the link from the
retry's derivation to it, which needs intra-tick lineage (E3.2).

**`rpc_retry`, the E0 collapse** (800 rounds, E0 harness, prompt driver; 24,971 records over
12,570 releases). Per request, $D_k$ on client→server and `outgoing` equals the client's own send
count for every one of the 2200 requests (the load-perturbation gain of E2, now per goal);
`processed` carries each id as often as the server served it, and client→server minus `processed`
is the final backlog (1237). Cost: 775 ms untraced, 811 ms traced, 1.05×, so the "log per firing,
compute closure at analysis time" rule holds at this scale.

**Raft, $R = 5$ entries** appended at the leader after the first election and before the timers
start, election timer every $E = 4$, 200 ticks (the E2b configuration; `raft_server` unchanged).
Attribution: a traffic record decodes as `(MemberId, RaftRpc)`; an `AppendEntries` is a derivation
toward each entry it carries; nothing else carries an entry.

| schedule | traffic $W_e$ | `AppendEntries` per entry $D_r$ | elections | committed per member | records carrying no entry |
|----------|---------------|---------------------------------|-----------|----------------------|---------------------------|
| prompt | 804 | 2 (all five entries) | 0 | 5, 5, 5 | 4 (the first election) + 398 heartbeats + 400 acks |
| constant delay $d = 4$ | 332 | 16 (all five entries) | 50 | 0, 0, 0 | 298 `RequestVote` + 2 votes + 16 acks |

Hand computations, both matched: under prompt the leader's first heartbeat carries all five
entries to each follower and the acks are processed before its next heartbeat (visible in the
log: `AppendEntries(5 entries)` at follower tick 1, both acks at the leader's next decision, then
`AppendEntries(0 entries, commit 5)`), so $D_r = C - 1 = 2$, one per follower, and every entry
commits once on every member; $804 = 4 + 2 + 398 + 400$. Under the delay the term-1 leader
re-sends all five entries at every heartbeat until it is deposed, which takes $E$ ticks (the
followers' first timeout finds no heartbeat, since the first one is delivered at tick $1 + d$)
plus $d$ ticks for their `RequestVote` to reach it: $D_r = (C - 1)(E + d) = 16$; no leader lives
long enough to send an empty heartbeat (0), nothing commits.

So the disagreement the design demands is visible already at the boundary: $W_e$ falls 804 → 332
while $D_r$ rises 2 → 16. But E3.1 sees only the part of the storm's work that carries an entry.
The 16 re-sends stop when the leader is deposed; the 300 election records that follow, which
*are* the storm and grow with the run (E2b: 50 elections in 200 ticks), carry no entry and are
attributed to nothing. In E3.1's accounting $D_r$ saturates at 16 while the program keeps working
toward the five pending entries for another 190 ticks. Attributing an election to the entries
waiting for a leader needs the election's ancestry (timer, plus the *absence* of a heartbeat,
whose would-be suppressor is a held `AppendEntries` carrying those entries), i.e. E3.2's
derivation records and E3.3's negative leaves. That is the gap E3.1 was meant to make concrete,
and it is the case E3.2 must be measured on.

Decisions taken in E3.1 that E3.2 inherits: the edge key is still E2's `file:line:col#index
<type>` (the operator id is E3.2's, where the pass has the IR); the log is host-side and per
release, with the held set per release rather than per record; `Record::decode` panics on a type
mismatch rather than returning `None`, so a harness that names the wrong type for an edge fails
loudly.

### E3.2: the pass

Done. Tests: `rpc_retry::amplification_tests::lineage_pass_attributes_by_ancestry_what_e31_attributes_by_payload`,
`tracing_the_e0_collapse_attributes_every_send_and_costs_little` (now also instrumented),
`raft::amplification_tests::lineage_pass_attributes_elections_through_state`. Every number below
is asserted by one of them, except where marked "printed".

**The pass** (`hydro_lang/src/sim/lineage_pass.rs`, on by `SimFlow::with_lineage()`). An IR
rewrite between `apply_dynamic_membership` and codegen. `Stream<T>` (and singletons, optionals)
become `(u64, T)`; keyed collections `(K, V)` become `(K, (u64, V))` so the keyed operators still
key on `K`, and a `Cast` between the two layouts re-nests. Every staged closure is *wrapped*, not
rewritten: `{ let __f = <staged closure>; move |__w| { unwrap; __y = __f(__x); derive; wrap } }`,
so the `q!` code and its type hints are untouched. The rules are the design's table:

| operator | as implemented |
|----------|----------------|
| source, `sim_input`, timer, `singleton(..)` | a `Map` after the leaf assigns a fresh id, no parents |
| `map`, `filter_map`, `flat_map`, `enumerate` | child ← input |
| `filter`, `inspect`, `batch`, `chain`, `defer_tick`/cycle (state carry), the positive side of `anti_join` | pass-through, same id |
| `join`, `join_keyed_singleton`, `cross_singleton`, `cross_product` | child ← both inputs; **and** a `Tee` on both inputs feeds an `anti_join` beside the join whose `for_each` reports every probe-side record with no partner as dropped |
| `fold`, `fold_keyed`, `reduce`, `count`, `max` | the accumulator becomes `(Vec<u64>, A)` and collects every input id of the tick; a `Map` after the aggregate derives the output from them (`reduce` becomes a fold over `Option<T>`) |
| `scan` (behind `KeyedStream::first`) | keyed input and `Option<(K, U)>` output: parents are the inputs of that key alone (any other scan: every input so far in the tick) |
| `unique` | a `scan` over a `HashMap<T, id>`: the first record of each payload survives under its id, every later one is reported dropped naming the survivor, then flattened |
| `sort` | sorts `(T, id)`, so by payload, id along |
| `anti_join` negative side | stripped to keys (`filter_key_not_in`); `difference` becomes an anti-join keyed by the payload |
| network (`send`, `broadcast`, `demux`) | the serializer's output gets the id in front (8 bytes, little-endian; `PrependId` for `Bytes` and `(member, Bytes)`), the deserializer splits it off (`SplitId`); the record keeps its id across locations |
| `sim_output` | reports `Output { record, op }` and strips the id |
| a closure with `by_ref`/`by_mut` handles (`raft_step`) | the closure is constructed per input with the handles shadowed: a singleton by a reference into its payload, a `by_mut` stream by a `LineageVec` that ids every pushed record, a `by_ref` stream by a clone of the payloads; parents of everything it produces = the input + every referenced singleton's id; and every `by_mut` singleton gets a **new version** after the call, derived from those parents (`StateVersion`, flagged) |

The program reports through `hydro_lang::sim::lineage_rt` (its own copy in the dylib) to a sink
function pointer the host passes as a new `__hydro_runtime` parameter, like `println_handler`;
the host appends `Derivation { record, op, parents, after_release, state_version }`, `Drop` and
`Output` records to the same `Lineage` as E3.1's boundary log. The boundary hooks read the id out
of the wrapped element (`format_item_id`) and report the held set's ids too, so the E3.1 log and
the derivation records share one id space; the E3.1 host ids are only used when the pass is off
(they start at $2^{63}$). Operators are numbered in traversal order and the table `(id, kind,
file:line:col, source line, element type)` is stored in the log (`Lineage::operators`; the
operator id the E2b open question asked for, though the hooks' edge keys are still E2's). The
edge keys are unchanged under the pass (the hook names its type by `T`, not `(u64, T)`), so every
E2/E3.1 predicate and every hold policy works unmodified on an instrumented program.
Attribution: `Lineage::per_goal_by_ancestry(edge, goals)` counts, per goal (named by the id of
its input record, e.g. the record carrying request $k$ on the requests edge), the records on the
edge whose ancestry contains it, by reachability over a children index; `children_with(false)`
cuts the state chain (a state version is then not a child of the previous version). Anything the
pass cannot wrap panics at compile time with the operator's location.

**`rpc_retry`** (E2 configuration; hold $d \in \{0, 40, 80, 120\}$). On every work edge and for
every one of the 600 requests, $D_k$ by ancestry equals $D_k$ by payload (E3.1) equals the
hand-computed sends; the completion edge has exactly one record per completed request by
ancestry as well; every E2 edge count is unchanged by the instrumentation. The duplicate answers
are now *recorded*, not inferred: every response record is a completion, or a drop at `unique()`
(the survivor named), or a drop at the join against `outstanding`, with nothing else dropped
anywhere; and every dropped record descends from a request.

| $d$ | responses | completions | dropped at `unique` | dropped at the join | derivation records |
|-----|-----------|-------------|---------------------|---------------------|--------------------|
| 0   | 600  | 600 | 0   | 0    | 17,566 |
| 40  | 1120 | 600 | 0   | 520  | 178,526 |
| 80  | 1560 | 600 | 160 | 800  | 316,286 |
| 120 | 1560 | 240 | 320 | 1000 | 424,706 |

The derivation count grows with $d$ because `outstanding` is re-derived every tick a request
waits (the design's "`use::state` re-derives every entry every tick"): 268 operators, 17.5k
derivations at $d = 0$, 425k at $d = 120$. On the E0 collapse (800 rounds, backlog up to 1237
re-enumerated every server tick): 3,614,402 derivations, 2,718 drops (= 3700 responses − 982
completions), 11,397 outputs; by ancestry the sends per request are E2's {1: 619, 2: 425, 3:
1156} exactly; wall time 1.73 s against 0.77 s untraced (2.26×; E3.1 alone 1.06×). Usable, but
the state re-derivation is where the log's volume comes from.

**Raft** ($R = 5$ entries, $E = 4$, 200 ticks; 80 operators, ~7k derivations, 606–611 state
versions). `raft_step` is one staged closure over the member's whole state through `by_mut`, so
every message a member sends descends from its state, and the state from everything the member
ever absorbed. Per entry:

| schedule | traffic $W_e$ | elections | $D_r$ by payload (E3.1) | $D_r$ by ancestry | by ancestry, state chain cut | what descends from $r_0$ (printed) |
|----------|---------------|-----------|--------------------------|-------------------|------------------------------|------------------------------------|
| prompt | 804 | 0 | 2 | 800 | 800 | 2 `AppendEntries` with entries, 398 heartbeats, 400 acks |
| constant delay $d = 4$ | 332 | 50 | 16 | 324 | 8 | 292 `RequestVote`, 16 `AppendEntries`, 16 acks |

Reading. The pass does what E3.1 could not: under the storm the 292 election records (all but
the 6 `RequestVote`s sent before any follower had received an entry, and the initial election)
are attributed to the five pending entries, and $D_r$ grows with the run (324 in 200 ticks, and
every further period adds 6). But the same attribution gives 800 under prompt: the leader's
heartbeats and their acks also descend from the entries, through the state, so by ancestry
alone the storm is *less* work toward the entries than the steady state, the same wrong sign as
$W_e$. This is not a defect of the pass; it is what "derived from" means for a state machine.
Cutting the state chain does not help: it gives 8 under the storm and still 800 under prompt,
because under prompt the lineage does not need the chain: each heartbeat's parents include this
tick's messages, i.e. the previous heartbeat's acks, whose parents include that heartbeat, and so
on back to the first `AppendEntries` that carried the entries. The exchange itself carries the
ancestry forward. So the discriminator the design named is necessary: a record is a re-derivation toward
$g$ if its ancestry contains $g$'s input **and** a delivery the adversary held. Under prompt
nothing is held, so the 800 are steady-state work by construction; under the storm the question
E3.3 must answer is which of the 324 have a held `AppendEntries` (the one that would have
suppressed the timeout) as a negative ancestor. The information is in the log already: the
`held` set of every release (E3.1) and the tick association of every derivation
(`after_release`).

Decisions taken in E3.2 that E3.3 inherits: the join-drop observer reports the probe side only;
a `scan` other than `first()` gets cumulative parents; a `by_mut` singleton is assumed mutated on
every call (a new version per tick, parents = the closure's inputs); the negative side of an
anti-join contributes nothing to the positive record's lineage (E3.3's negative leaf goes there);
the E2 edge key is kept for the hooks and the operator table carries the operator id.

### Negative leaves: linking a re-derivation to the delivery that was held

Done. Tests: `rpc_retry::amplification_tests::negative_leaves_mark_exactly_the_records_the_hold_caused`,
`raft::amplification_tests::negative_leaves_attribute_the_storm_but_not_the_steady_state`. Every
number below is asserted by one of them except where marked "printed".

**Instrument.** The scheduler now brackets each run of a tick (`begin_tick`/`end_tick` around
`run_hooks` and the tick's DFIR run), so every `Derivation` and `Drop` carries the index of its
`TickRun` in `Lineage::ticks`, and the held sets of that run's releases are the derivation's
negative support. The pass marks operators as **negative** (`OperatorInfo::negative`): anti-joins
(`filter_key_not_in`, `difference`), which now emit a derivation of their own for each surviving
record instead of passing it through (a retry's birth needs a record to hang the negative leaf
on), and any closure that reads state or aggregates through `by_ref`/`by_mut` handles (Raft's
`raft_step`: the absence tests are inside it, so the whole step is the best available
approximation). The analysis, `Lineage::re_derivations(edge, goals)`: a record $r$ on the edge is
a **re-derivation toward $g$** iff (1) $g$'s input is an ancestor of $r$, and (2) some record $v$
in $r$'s ancestry (or $r$ itself) was derived at a negative operator in a tick run during which a
hook of that tick held records descending from $g$ — $v$'s *witnesses* — none of which is an
ancestor of $r$. The last clause is what keeps a late completion from being marked: the
completion of request $k$ descends from `kept` entries derived while $k$'s response sat in the
buffer, but it also descends from that response, so the absence was filled and the record is the
goal arriving late, not extra work. Without the clause 520 completions are marked at $d = 40$
(printed, the "every derivation of the tick" variant the design sketched).

**`rpc_retry`, hold $d$ on responses** (E2 configuration). Per request, on all four work edges,
the re-derivations are exactly the records after the first in release order — `sends − 1` of the
E2 step function — and every witness is a response *to that request*, held by the `responses`
hook of the tick in which the marked derivation happened. The completion edge has 0
re-derivations for every request at every $d$; the `abandoned` edge marks every abandonment (a
verdict that exists only because the response was held). Nothing is marked at $d = 0$.

| $d$ | re-derivations per request on each work edge (measured = hand-computed) | completions marked | abandoned marked | witnesses checked |
|-----|-------------------------------------------------------------------------|--------------------|------------------|-------------------|
| 0   | {0: 600} | 0 / 600 | 0 / 0 | 0 |
| 40  | {0: 80, 1: 520} | 0 / 600 | 0 / 0 | 2080 |
| 80  | {0: 80, 1: 80, 2: 440} | 0 / 600 | 0 / 0 | 3840 |
| 120 | {0: 80, 1: 80, 2: 440} | 0 / 240 | 360 / 360 | 4200 |

Every one of the 4200 marked derivations happened in a tick that released a clock element. That
settles the forcing-rule question the design left open, at least for this program: absence only
produces work when something observes it, and what observes it is a timer release, so an
adversary that paces the timers and holds the deliveries needs no "run empty" option here. (The
third program below shows the other half of the answer.) Cost, printed: the analysis walks
ancestors per record on the edge and takes 3.6 s at $d = 0$ but 103 s at $d = 120$ (482k
derivations, six edges); a top-down formulation would fix it and was not done.

**Raft** ($R = 5$ entries, $E = 4$, 200 ticks). The design's bar was: prompt $= 0$, storm $> 0$
and growing. Both hold. Under prompt nothing on the traffic edge is ever held and the timer hooks
hold only `()` leaves, so 0 of the 800 records descending from an entry are marked. Under the
storm ($d = 4$) 314 of the 324 are: all 292 `RequestVote`s (witnesses: 274 held `RequestVote`s,
27 held `AppendEntries`, 13 held acks), all 16 acks, and 6 of the 16 `AppendEntries` re-sends —
the ones at the leader's heartbeat ticks 6–8, after the first acks exist to be held; the 10 at
heartbeat ticks 1–5 have no negative leaf *at the leader* (the acks that would have stopped them
do not exist yet, because the `AppendEntries` that would produce them is held at the followers),
exactly as hand-computed. Per 40-tick window: 64, 60, 60, 60, 60, i.e. 6 per election period,
growing with the run.

**The contradiction.** A control the design did not ask for: the same rule under $d = 1, 2, 3$,
where E2b measured the same 804 records and no election. It marks 796, 794 and 792 of the 800.
`raft_step` is one closure, so every in-flight message descends from the entries through the
state and counts as a witness, and the rule cannot tell a heartbeat sent while acks are in flight
from a `RequestVote` sent while an `AppendEntries` is held. Flipping the polarity does not help:
treating the closure as positive gives 0 on the storm too. The information (which output answers
which absence) is inside the closure; the IR sees a blob. So on Raft the negative-leaf count says
a harmless one-tick delay is *worse* than the storm.

| schedule | traffic | elections | derived toward $r_0$ | re-derived toward $r_0$ |
|----------|---------|-----------|----------------------|-------------------------|
| prompt | 804 | 0 | 800 | 0 |
| constant delay 1 / 2 / 3 | 804 | 0 | 800 | 796 / 794 / 792 |
| constant delay 4 (storm) | 332 | 50 | 324 | 314 |

### What lineage is for, revised

The negative-leaf rule is exact where the program's absence test is a dataflow operator
(`rpc_retry`'s anti-join) and brackets — 0 or nearly everything — where it is inside a Rust
closure (Raft, and every protocol in this repository written as one `by_mut` step: `paxos_ec`,
`dyn_raft`, `broadcast_transcript_consensus`, `multi_paxos`). Its precision is exactly how much
of the program's logic lives in the dataflow rather than in Rust, which is Hydro's own argument,
but it means that on the programs we have, a single-run lineage rule is not the verdict. What has
discriminated every case so far — retry storm, election storm, and the redo queue below — is the
design's first principle applied per goal: run two schedules on the same inputs and compare
derivations per goal (E3.1's payload attribution at the hooks suffices; no pass needed), swept
over the perturbation to get the threshold and the shape. Lineage's remaining jobs are the
explanation of each extra record where the dataflow exposes the absence test, and the proposal of
moves (which held record to try next). Making it exact for opaque steps needs a counterfactual
(fork the deterministic run at the tick with the held record released, LDFI's move), which is the
search loop's business, not the log's.

### A third program: the redo queue in `multi_paxos_live`

Test: `hydro_std::ec_inference_demos::multi_paxos_live::amplification_tests::redo_queue_re_proposes_the_whole_backlog_per_stall`.
Nothing in `multi_paxos_live` or `multi_paxos` changed. This is the first program not written for
this project and not a leader-election theorem: its election kernel keeps a *redo queue* of
admitted commands not yet observed chosen; a timer interrupt that finds the queue non-empty and no
completion since the previous interrupt campaigns, and on establishment the kernel **releases the
whole queue** to the core again.

Harness: 3 acceptors, 2 proposers (only proposer 0 gets commands and interrupts, so no duel), 1
learner. Phase 1 establishes an epoch. Phase 2: 200 commands, one per kernel tick (the metered
clock), a timer interrupt every $P = 10$ ticks, and the proposer's own chosen notifications (the
self-hop back to the kernel) scheduled promptly, with a constant delay `Hold(d)`, or in bursts
`Periodic { period: d }`. Goals: commands; derivations toward a command: accept records carrying
it at the acceptors (payload attribution); the goal edge: learned slots per command.

| $d$ | constant: accepts / campaigns / decrees per command | bursty: accepts / campaigns / decrees per command |
|-----|-----------------------------------------------------|----------------------------------------------------|
| prompt | 603 / 0 / {1: 201} | — |
| 1, 2, 5, 8 | 603 / 0 / {1: 201} | 603 / 0 / {1: 201} |
| 10 | 603 / 0 / {1: 201} | 615 / 1 / {1: 199, 2: 2} |
| 12 | 645 / 1 / {1: 188, 2: 13} | 639 / 4 / {1: 193, 2: 8} |
| 15 | 654 / 1 / {1: 185, 2: 16} | 720 / 7 / {1: 173, 2: 28} |
| 20 | 660 / 1 / {1: 183, 2: 18} | 1146 / 10 / {1: 30, 2: 171} |
| 30 | 744 / 2 / {1: 175, 2: 8, 3: 18} | 1434 / 13 / {1: 34, 2: 73, 3: 94} |
| 50 | 1005 / 4 / {1: 155, 2: 8, 3: 12, 4: 8, 5: 18} | 2139 / 16 / {1: 19, 2: 40, 3: 40, 4: 38, 5: 64} |

Hand computation, matched: prompt is 3 accepts per command (one per acceptor), one decree, no
campaign. A constant delay is a transient: only the interrupts at $kP < l + d$ find no completion
(campaigns $\le \lceil (l+d)/P \rceil$), then a completion arrives every tick and the stall
detector is quiet. Bursts every $d > P$ stall in a $1 - P/d$ fraction of the periods for the whole
run: predicted 3.3 / 6.7 / 10 / 13.3 / 16 campaigns for $d = 12 / 15 / 20 / 30 / 50$, measured
4 / 7 / 10 / 13 / 16. Each stall re-proposes every pending command to every acceptor, so the work
per stall grows with the backlog, accept traffic reaches 3.5× and commands are decreed up to five
times each (duplicate log entries; the module docs assign deduplication to the state machine, and
the rate had never been measured). Two boundary deviations, recorded rather than tuned: at $d = P$
one campaign where the hand computation said none (one period straddled a burst boundary), and at
$d = 12$ bursty did slightly less accept work than constant (639 vs 645: four small re-releases
against one large one; the crossover is at $d = 15$). Same shape as E2b — threshold at the
protocol's own timer, constant delay bounded, bursty delay sustained — in code nobody wrote for
this, with fixed inputs and no message lost.

**The forcing rule decides which moves exist.** The natural move, holding the acceptors' acks,
was impossible: that hook is alone in its tick (`quorum`'s slice), so the scheduler forces it to
release whenever the tick runs, and every policy on it gave the prompt numbers (measured, then
the move was changed to the completion self-hop, which shares a tick with the metered command
clock). So the open question is answered in two halves: an adversary that paces timers and holds
deliveries never needs a tick to run empty (rpc_retry), but it does need to hold on edges that
have no metered co-hook, which today's scheduler does not allow. A scheduler option that lets a
tick run with nothing released under an explicit policy is the first piece of the search.

### The scheduler option: ticks may run empty

Done. `hydro_lang::sim::edge_counts::with_empty_ticks_allowed(|| run)` switches the forcing rule
off for a run: when every hook of a tick decides to hold, the tick runs with no new input (state
carry only) and stays runnable, so an edge that is alone in its tick can be held or paced. The
default is unchanged (every existing test runs under the forcing rule). First use, in the same
multi_paxos_live test: the acks move that was impossible above, at $d \in \{5, 12, 20, 50\}$.

| $d$ | constant: accepts / campaigns / decrees per command | bursty: accepts / campaigns / decrees per command |
|-----|-----------------------------------------------------|----------------------------------------------------|
| 5 | 603 / 0 / {1: 201} | 603 / 0 / {1: 201} |
| 12 | 603 / 0 / {1: 201} | 720 / 5 / {1: 172, 2: 29} |
| 20 | 708 / 1 / {1: 184, 3: 16, …} | 1575 / 10 / {1: 47, 2: 27, 3: 106, 4: 20, …} |
| 50 | 1827 / 4 / {1: 153, 6: 8, 10: 10, 15: 16, …} | 4845 / 16 / {1: 21, 3: 40, 6: 32, 10: 50, 15: 42, …} |

Hand computation, matched: the kernel sees completions late by the same $d$, so the campaign
staircase is the completion move's within one at every $d$ (asserted). What differs is the work
per stall: with the delay on the acks the re-proposals' own acks are delayed too, so the pending
set the next stall re-releases is larger — 4845 accepts against 2139 at bursty $d = 50$, 8× the
prompt run, and commands decreed up to 15 times. Where the delay sits changes the gain by a
factor of two on the same program with the same threshold; the sweep has to try every edge.

### The sweep

Done: `hydro_lang/src/sim/sweep.rs`. Tests, one per program, each a harness of the program's
existing sim inputs and one goal extractor:
`rpc_retry::amplification_tests::sweep_finds_the_retry_storm_without_being_told_the_edge`,
`raft::amplification_tests::sweep_finds_the_election_storm_without_being_told_the_edge`,
`multi_paxos_live::amplification_tests::sweep_finds_the_redo_queue_without_being_told_the_edge`,
and the corpus controls in `hydro_std/src/ec_inference_demos/sweep_controls.rs`. Every number
below is printed by one of them and the ones marked "asserted" are checked against a hand
computation there.

**The tool.** `sweep::Program { run, clocks, time, goals, reached }` is what a harness declares:
`run` wires the flow and runs the fixed inputs under a driver the sweep builds; `clocks` are the
edges the driver meters or paces in every run (a metered input stream, a periodic timer) and are
never moved; `time` is the clock whose non-empty releases are logical time; `goals` is the one
declared line of program knowledge, `(edge key, record) -> goals`, decoding the payload with the
host's type for that edge. The sweep runs prompt once, takes every other counted hook edge with a
record under prompt as a candidate, and for each candidate × {constant `Hold(d)`, bursty
`Periodic { period: d }`} × a geometric grid of `d` runs the same inputs and compares
derivations per goal ([`Lineage::per_goal`], payload attribution, no pass). Per cell it reports
the total and per-goal histogram of derivations toward goals, the largest per-goal ratio against
prompt, records no goal accounts for, the harness's progress signals against prompt's, and the
excess over prompt in four windows of logical time — a record's time is the clock count at the
decision it *arrived* at its hook, which a hold cannot move — from which the gain is labelled
transient (at least three quarters of the excess in the first half of the run) or sustained.
Three thresholds are located by bisection between grid points: the first `d` with any finding,
the first with a sustained gain (the knee), and the first with a progress change. Every run is
under `with_empty_ticks_allowed`, so an edge alone in its tick is a candidate like any other; the
forcing rule is an option (`Options::empty_ticks = false`), used once below. Keyed `batch` edges
are counted but not in the lineage log (no per-record positions), so they get the bursty shape
only (`HoldScheduleDriver` now answers a keyed hook's per-key count questions under `Periodic`,
all keys or none) and a note. Two scheduler fixes were needed: a tick whose hooks all held may
find its DFIR has nothing to do (`run_tick()` false) — an error under the forcing rule, allowed
under empty ticks — and the driver's keyed-hook panic became `Periodic`-only.

**Cost.** One run is one compiled simulation with the harness's inputs: ~3 s on rpc_retry
(300 ticks) and multi_paxos_live (200 commands), ~4 s on Raft (200 ticks × 3 members). The
sweeps below took 102, 55 (× 2 settings) and 239 runs: 5, 7 and 12 minutes.

**rpc_retry** (E2's inputs: 300 ticks, 2 requests per tick, τ = 40, A = 3; clocks: the client
clock and the request stream; goals: the request id every record carries, a `Completion`
reaches its request). 11 hook edges, 3 idle under prompt (`retried`, `abandoned`, the server's
report tick), 2 clocks, 6 candidates. Prompt: 4 derivations per request (arrival, processed,
response, outgoing), 600 reached. Findings, all asserted per request against E1's step function
(`expected(t, l, delay)`, sends per request), constant and bursty:

| edge | shape | first gain | knee | derivations at d = 40 / 64 / 128 (prompt 2400) | progress at 128 |
|------|-------|------------|------|-------------------------------------------------|-----------------|
| responses (server → client) | constant | 39 | 39 | 5000 / 5000 / 7560 | abandoned 360, completions 240 |
| responses | bursty | 41 | 41 | 2400 / 3400 / 5192 | abandoned 32, completions 568 |
| arrivals (client → server) | constant | 39 | 39 | 5000 / 5000 / 7560 | abandoned 360, completions 240 |
| arrivals | bursty | 37 (progress only) | 40 | 2466 / 3422 / 5614 | abandoned 46, completions 482, backlog 136 |
| processed, outgoing, completions (metrics tees) | both | — | — | 2400 at every d | — |

The retry storm, re-found on both edges of the wire without being told either. Constant delay:
the threshold is τ − 1 (the requests with baseline latency 2 cross the timeout first), the gain
is sustained (every request minted early enough pays it) and saturates at A: 5000 = 2400 + 520 ×
5 (four work edges and a `retried` record per extra send), 7560 at 3τ = the same plus 360
abandon records; per-goal max ratio 2.25 then 3.75. Bursty: a response arriving at hook decision
`a` is released at the next multiple of `d`, so its delay is `(−a mod d)`, at most `d − 1`; the
threshold is **41**, not the τ = 40 a first hand computation says, because the arrivals'
parity against the server's every-other-tick cadence decides which delays occur — the test
derives the threshold from the prompt log's arrival decisions and matches every bursty cell per
request (asserted). Holding the arrivals instead of the responses gives the same step function
from τ on (asserted equal to the responses move at every d ≥ 40) and 2 requests instead of 260
at τ − 1: a server whose buffer holds records runs every round, so the baseline latencies lose
their parity (recorded, not hand-computed). The bursty arrivals move also shows the sweep's one
scheduler artifact: a burst of 2d requests against a capacity of 20 per tick leaves a backlog,
and the last burst's remainder is stranded when the clock runs dry, because a tick with state but
no input is not runnable — completions 556/570/584/598 at d = 37–40 with *fewer* derivations
than prompt and no retry yet. In deployment the server tick is timer-driven; the sweep labels it
"progress only" and the knee (40) is where the retries start.

**Raft** (E3.1's inputs: 5 entries, election timer every 4, 200 heartbeat ticks × 3 members;
clocks: the heartbeat hook metered, the election hook paced; goals: the entries an
`AppendEntries` carries). 5 hook edges: the two timers, the requests, the committed output
slice, the traffic. Prompt: 2 derivations per entry (one `AppendEntries` per follower), 0
elections, 15 commits. Requests and committed: no moves at any d (asserted). Traffic:

| setting | shape | first gain | first storm (progress) | derivations per entry, below / at the storm | elections at the largest d |
|---------|-------|------------|------------------------|---------------------------------------------|----------------------------|
| ticks may run empty | constant | 1 | **2** | 6 at d = 1; 8, 8, 16, 16, 24, 32, 40, 56 at d = 2 … 24 | 50 |
| ticks may run empty | bursty | 2 | 5 | 2(2d − 1) below; 2(d + 3) from d = 6 | 52 (82 at d = 5, 75 at d = 6) |
| forcing rule (E2b's) | constant | 1 | 4 | 6, 10, 14 at d = 1, 2, 3; 16, 20, … from 4 | 50 |
| forcing rule | bursty | 2 | 5 | as above | 52 |

The election storm, re-found: from the storm threshold on, a term per period and nothing
commits, for every larger d (asserted), while the per-goal derivations by payload see only the
leader's re-sends before it is deposed — 2 per follower per heartbeat tick of the leader's life,
which the storm quantizes to the election period (4⌈(d+1)/4⌉ per follower under a constant
delay) — a transient in every cell. So on Raft the sweep's verdict is carried by the progress
signals, exactly as E2b said a single count could not see the storm; the derivation count says
"bounded", the terms say "loop", and the report prints both. **A contradiction with E2b, worth
recording:** under the sweep's default the constant-delay storm starts at d = 2, not at the
election period. The timer phases are printed per run: under the forcing rule the followers'
first phase-2 heartbeat decision is 1 and their election timer fires at 4 at every d, because
phase 1's election traffic — the only thing in a follower's tick — is flushed by the forcing
rule and phase 1 costs the same number of decisions whatever the policy; under empty ticks
phase 1's traffic is really held d, phase 1 takes longer, and at d = 2 the followers reach
phase 2 at decision 3 with the timer at 4 while the leader's first heartbeat, two decisions
later plus d, arrives just after it. Same program, same inputs, same delay: the storm's threshold
is the election period minus the phase offset between the followers' timers and the leader's
heartbeats, and E2b measured the offset-zero case. The bursty threshold (5, E2b's "above the
period") is the same in both settings; at d = 5 and 6 there are 82 and 75 terms in 200 ticks,
more than one candidate per period.

**multi_paxos_live** (the redo-queue inputs: 200 commands at one per kernel tick, timer every
10; clocks: the command hook metered, the kernel timer paced; goals: the command an accept
record at the acceptors carries). 12 hook edges, 2 clocks, 10 candidates, none idle. Prompt:
603 derivations (3 per command), 0 campaigns, 201 learned slots.

| edge | shape | first gain | knee | campaigns / accepts / max decrees per command at d = 12 / 16 / 32 / 48 |
|------|-------|------------|------|-------------------------------------------------------------------------|
| completions → kernel (the edge held by hand before) | constant | 12 | — (transient) | 1/648/2 · 1/642/2 · 3/843/4 · 4/1041/5 |
| completions → kernel | bursty | 10 | 11 | 3/639/2 · 8/819/2 · 14/1542/4 · 15/1947/5 |
| acks → quorum mint (the edge needing empty ticks) | constant | 13 | — (transient) | 0/603/1 · 1/708/3 · 2/930/6 · 4/1827/15 |
| acks → quorum mint | bursty | 9 | 9 | 5/720/2 · 8/1227/4 · 14/3045/11 · 16/7527/19 |
| **phase-2 proposals → acceptors** (new) | constant | 13 | **15** | 0/603/1 · **19/6816/2** · 19/6816/2 · 19/6816/2 |
| phase-2 proposals → acceptors | bursty | 9 | 9 | 6/819/2 · 10/1047/2 · 14/1737/2 · 16/5691/2 |
| **kernel releases → proposer core** (new) | constant | 13 | 13 | 0/603/1 · 1/660/2 · 2/747/3 · 4/1011/5 |
| kernel releases → proposer core | bursty | 10 | 10 | 4/645/2 · 9/927/3 · 15/1821/5 · 17/2517/6 |
| prepares, promises, the learner's and the kernel's other edges (6) | both | — | — | 603 at every d |

The redo queue, re-found on both edges that were held by hand (thresholds within three ticks of
the period, asserted; constant transient and bounded by ⌈(l + d)/P⌉ campaigns, asserted;
bursty sustained), with the same numbers where the grids meet (acks bursty d = 12: 720 / 5, as
in the table above). And two edges nobody had pointed the instrument at. Holding the kernel's
releases on their way into the proposer core is the completion move from the other side. Holding
the **phase-2 proposals themselves** is the strongest move on this program and a different
shape: from a constant delay of 15 the run saturates — every interrupt after the first campaigns
(19 of 20), the pending set is re-proposed in full at every campaign, and *nothing completes
while the timer runs*: the re-proposals of a campaign land 15 ticks later, after the next
interrupt has campaigned again and its prepares (not delayed) have raised the acceptors'
promise, so they are refused. Accept records 6816 = 3 × (201 + Σ_{k=1}^{19} (19 + 10(k − 1))):
19 commands pending at the first campaign, then 10 more per period, each proposed once per
campaign after its admission, a staircase {20: 19, 19: 10, …, 2: 10, 1: 2} of proposals per
command (asserted, at d = 15, 16, 24, 32, 48: identical). All 201 commands are chosen once the
timer stops (learned slots 203). That is the Raft storm's shape in Paxos with one proposer and no
duel — a redo timer shorter than the proposal path's delay fences its own proposals — 11× the
accept work at zero goodput for the length of the run, and a constant delay does it. Where the
delay sits changes the outcome by an order of magnitude on the same program: the completion move
at d = 48 is 1041 accepts and a transient; the proposal move at d = 15 is 6816 and a loop.

**Corpus controls** (`sweep_controls.rs`, five tests; these programs have no timer, so the
sweep runs them single-shot: no clock, no shape, a gain would be reported as "extra"; grid 1–16,
bisection on). Measured, all asserted:

| program | inputs | hook edges | runs | prompt | under every move |
|---------|--------|------------|------|--------|------------------|
| uniform broadcast (20 messages, 3 members, threshold 2) | up front | 1 (the certificate mint) | 17 | 180 attestations, 60 deliveries | 180, 60 |
| synod, sole proposer | up front | 4 (prepares, accepts, promises, acks) | 65 | 12 records, 1 chosen | 12, 1 |
| synod, duel (rounds 1 and 2) | up front | 4 | 65 | 21 records (the lower ballot's accepts refused), 1 value | 21, agreement holds |
| ABD, write then read (1 client) | closed loop | 4 (2 replica batches of one private type, 2 mints) | 65 | 12 attributed + 12 unattributed, read returns 10 | 12 + 12, 10 |
| g-set gossip (2 members, 8 pump ticks) | up front | 1 (the pump, which is the clock) | 1 | converged | no candidate edge |

No moves anywhere, as the design predicted: fixed work per input, no retry, no timer. The gossip
row is the honest statement rather than a measurement — its deliveries land in a top-level fold,
which has no hook, so once the pump is the clock there is nothing for the sweep to hold.

**Incompleteness, to state in every report (the tool prints it):** the moves cover one of the
design's five hook dimensions, release timing. Release *size* produced the sim collapse with no
hold at all; order, crash and membership moves do not exist yet. And what the sweep's inputs
are: the test body of the program's existing sim tests, sent up front and metered (all three
programs here; the corpus controls are single-shot or closed loop); round-by-round harnesses
have not been tried.

### Per-decision counterfactual forks

Done, first experiments: `hydro_lang/src/sim/counterfactual.rs`, `HoldScheduleDriver::with_fork`.
Tests: `raft::amplification_tests::counterfactual_forks_attribute_the_hold_per_delivery` (the kill
criterion and the storm, 3 min) and
`rpc_retry::amplification_tests::counterfactual_forks_remove_exactly_the_flushed_requests_retries`
(1.5 min). Every number below is printed by them; the ones marked asserted are checked against a
hand computation.

**Mechanism.** No snapshot: a fork is a replay of the held run under the same policy with one
edit, `Fork { edge, member, decision, arrived }`: at that hook decision the records that arrived at
`arrived` are released although `Hold(d)` would keep them. On an unordered hook the driver
answers the "which item" questions with the group's positions, so exactly those records go; on a
totally ordered hook the hook releases a prefix, so everything older goes with them (the report
says which happened, read off the fork run's log). Everything else — clocks, inputs, the policy at
every other decision — is the held run's. A fork is taken at the group's own arrival ("this
delivery was not held"). The diff against the held run is taken three ways: per edge (record
counts, no program knowledge), per (edge, payload shape) (the `Debug` rendering with digits and
string contents removed, cut at 72 characters; no program knowledge, a heuristic), and per goal
(the harness's extractor, as in the sweep). "Caused" is the positive part of held − fork, the work
the hold made; "displaced" the reverse. One replay per fork, ~3–4 s; 18 forks per experiment here.

**rpc_retry** (E2's inputs, responses held d = τ = 40, 12 forks at every 8th arrival group; the
responses hook is totally ordered, so a fork flushes the whole held buffer at that decision).
Hand computation per request: a response flushed at decision a after arriving at a′ is delayed
a − a′ instead of d, so its sends are `expected(t, l, a − a′)` instead of `expected(t, l, d)`; the
records caused are 4 work records plus one `retried` per send removed, and no other request
changes. Asserted at every fork, per request: caused 20 at decision 2 (4 requests), 180 at 18, 340
at 34, then 400 at every later fork (80 requests × 5 = the whole buffer's retries); displaced 0
throughout; the per-goal column gives the same 5 per request. `max_attempts = 1`: 0 work records
changed at every fork (asserted); the metrics edges show 80 abandons turned into 80 completions
per fork — the verdict, not the work. So on the program with the dataflow absence test the fork
reproduces E3.3's exact result with no lineage at all, and its granularity is the hook's: a
prefix, not a record.

**Raft** (E3.1's inputs, traffic held d = 4, the first 18 arrival groups, i.e. phase 1's election
messages and the first heartbeat ticks at every member). The kill criterion first, heartbeats only:

| fork (member, decision, what) | per edge caused / displaced | per shape | per goal (5 entries) |
|---|---|---|---|
| phase-1 votes and `RequestVote`s (decisions 0–1) | 0 / 0 | none | 0 |
| a follower's first `AppendEntries`, decision 5 | 0 / 0 | none | 4 per entry |
| its 2nd, 3rd, 4th `AppendEntries`, decisions 6, 7, 8 | 0 / 0 | none | 3, 2, 1 per entry |
| decisions 9–12 | 0 / 0 | none | 0 |

Per edge and per shape the diff is 0 at every fork (asserted): a fork moves a message, it never
changes how many are sent. Per goal it is `max(0, a₀ + d − a)` re-sends per entry for an
`AppendEntries` arriving at a follower at a (asserted), where a₀ + d is when the held run's first
`AppendEntries` was released: the leader re-sends the pending entries until an ack is processed,
and the earliest ack in the fork answers the message released at a. This is the bounded
transient the sweep sees at d = 1..3, now attributed to the delivery that caused it; and it is
present in the control, because a hold *does* cause it.

The storm (election timer every 4, same d, same 18 forks):

| fork | per edge caused / displaced | per shape | per goal | progress |
|---|---|---|---|---|
| phase-1 messages | 0 / 0 | none | 0 | — |
| follower 1's `AppendEntries` at 5, 6, 7 | 4 / 5 | `RequestVote` 298 → 294; committed `LogEntry` 0 → 5 | 3, 2, 1 per entry | committed 0 → 5 |
| follower 2's `AppendEntries` at 5, 6, 7 | 3 / 5 | `RequestVote` 298 → 294, one more vote; committed 0 → 5 | 3, 2, 1 per entry | committed 0 → 5 |
| follower 1's at 8 | 4 / 0 | `RequestVote` 298 → 294 | 0 | — |
| follower 2's two records at 8 | 0 / 2 | two more votes | 0 | elections 50 → 51 |
| decisions 9–11 (election traffic) | 0 / 0 | none | 0 | — |

Reading, column by column, which is the point of the experiment:

- **Per edge**, the storm's per-delivery signal is 3–4 records caused against 5 displaced: net
  negative. Releasing one follower's first `AppendEntries` in time removes that follower's
  campaign in that period (4 `RequestVote` records) and lets the leader commit the 5 entries
  before it is deposed (5 records on the committed edge). Totals over 18 forks: caused 25,
  displaced 32. A per-edge total, even per delivery, is blind to this storm for the same reason
  E2b's was: what the hold causes is small and what it displaces is larger.
- **Per shape**, the storm is legible with no program knowledge: the caused records are all
  `RequestVote`s (28 over the forks), the displaced ones are commits and votes, and the control
  has no shape change at all. This is the spike's stated expectation ("the diff is the
  RequestVote/vote path") met, and it is the first program-knowledge-free measure in this project
  that separates the storm from the control. Its cost is that the shape is a heuristic over a
  `Debug` string.
- **Per goal**, the storm and the control look alike: 3, 2, 1 re-sends per entry removed in both
  (asserted: the storm's total toward goals, 60, does not exceed the control's 100). The goal
  column measures the leader's bounded re-sends, which is real work the hold caused, but it is
  not the storm. On Raft the goal is the leader, not the entries, and no extractor over the
  traffic payloads names it.

**The records themselves (provenance for opaque closures).** The counts above are aggregates of
a record-level difference the fork also produces: every record that crossed a hook in the held
run and has no counterpart in the fork, matched by edge, member and payload
(`ForkOutcome::caused_records`, and `displaced_records` for the reverse). This is the link E3.3's
rule could not compute inside `raft_step`: for one held delivery, the concrete records that exist
because it was held. On rpc_retry it is exact, asserted per record at every fork: the caused
records are the retry records (arrival, processed, response, outgoing, `retried`) of exactly the
flushed requests whose early release beats their timeout, plus one `Completion` per flushed
request, which differs between the branches only in its latency field — the goal reached at a
different time, matched against its displaced twin. On Raft the raw lists are useless and the
reason is instructive: releasing follower 1's first `AppendEntries` gives ~110 caused records and
~111 displaced, because once the follower holds the five entries every `RequestVote` it ever
sends carries `last_log_index: 5` instead of `0`, and the terms shift by one, so payload matching
pairs almost nothing across the branches and the whole storm shows up twice, renumbered.
Collapsing the two lists by *role* — (RPC kind, sender, receiver), dropping the payload — recovers
the answer exactly: for that fork, net caused `RequestVote 1 → 0` × 2, `RequestVote 1 → 2` × 2,
`AppendEntries 0 → 1` × 3 (the entry-carrying re-sends), net displaced `heartbeat 0 → 1` × 3 (the
same messages, empty) and `commit at 0` × 5; the role-collapsed `RequestVote` total over the 18
forks equals the per-shape count (28, asserted). For the two records the fork releases at
decision 8 the net is only two votes exchanged, and from decision 9 on the raw lists are one ack
each way (the same ack, with a different term) and the net is empty.

So the fork makes the *set difference* exact for a closure, and the remaining problem moves to
record identity across runs: which fields of a record are its identity (kind, sender, receiver, a
request id) and which are state stamped on it (term, `last_log_index`, latency). rpc_retry's
records are all identity, so payload matching is exact there; Raft's carry state, and the role
key that fixes it is the goal extractor's knowledge in another form. The `Debug`-shape heuristic
(digits stripped) is a program-knowledge-free approximation of the role key that happened to be
exact on both programs here.

**What this settles and what it does not.** The fork is exact where E3.3's rule was exact
(rpc_retry, asserted per record) and it is exact where the rule bracketed once records are
matched by role (Raft: the suppressed campaign's four `RequestVote`s and the three entry-carrying
re-sends per early `AppendEntries`, nothing in the control), with no lineage, no pass and no
negative-operator marking. It also shows the storm is a system condition that no single delivery
owns: each fork removes one follower's campaign in one period and the run storms on (elections
50 in every fork but one), so summing "caused" over forks measures how many deliveries each
individually matter, not how much the storm costs. The per-fork granularity is the hook's
(prefix on ordered edges). Cost is one replay per fork; the sweep's finding says which edge and
which d, so an attribution pass is tens of replays, not thousands.

### Identity is the idempotency key

The residue of program knowledge, located by the fork's record lists, is one fact per message
type: which fields say what the message is *for* and which are stamps the protocol puts on it at
send time. `Request { id, body }`: both identity. `Completion { id, latency_ticks }`: id is
identity, latency a stamp. `RequestVote { term, candidate, last_log_index, last_log_term }`:
candidate is identity (the message is work toward "candidate becomes leader"), the rest are
stamps. `AppendEntries { term, leader, prev_log_*, entries, leader_commit }`: each entry's index
is identity, the rest are stamps.

**The identity fields are the idempotency key.** A retried RPC carries the same request id with a
new attempt number and timestamp; a re-proposed Paxos value carries the same slot under a higher
ballot; a re-sent Raft entry carries the same index under the current term. "Same key, different
stamp" is what the receiver uses to recognise a duplicate and what the checker needs to recognise
"the same goal, derived again". They are the same fact.

**Consequently the residue is already in the program.** A receiver that deduplicates has an
operator that does it, and that operator names the key: `unique()` (identity = the whole
payload), `filter_key_not_in` / `anti_join` (identity = the key), `fold_keyed` and `join` (the
key), `use::state` keyed by an id (rpc_retry's `outstanding`, keyed by request id). The lineage
pass already walks the IR and marks the absence-testing operators as negative
(`OperatorInfo::negative`); their key expressions are the goal extractor, derived rather than
declared. Where that works it removes the declared line entirely. Where the dedup lives inside a
Rust closure (Raft's `raft_step` keeps `match_index` and the log in HashMaps the IR cannot see),
the key is still in the code, in the closure's map types, but not in the dataflow; the fallback is
a derive on the wire type (`#[goal(...)]` per variant, generating the extractor and the fork's
cross-run matching key), which is written once on the type, not per test, and is the same
knowledge the programmer wrote into the closure's HashMap keys.

Experiment to run next, cheap because the operator table exists: for each of the three
programs, list the keyed and negative operators the pass sees with their key expressions, and
compare with the hand-written goal extractors. Prediction: rpc_retry's `filter_key_not_in` on
responses by `id` and the `unique()` on responses give exactly `Response::id` / `Request::id`;
multi_paxos_live's acceptor `fold` keyed by slot gives the slot (a finer goal than the command:
the redo queue re-proposes the same command at a *new* slot, so the command is the identity and
the slot is a stamp — the inference would get this wrong, and that is worth knowing); Raft gives
nothing, the closure case.

### Next

- **Forks on multi_paxos_live's proposal path**, where the sweep found the zero-goodput loop:
  does releasing one campaign's proposals in time end the loop (one delivery owns it, the
  redo-queue shape) or only skip one period (the Raft shape)? The accepts edge is unordered
  (exact forks). The hand computation: releasing the proposals of campaign k at their arrival
  should let them be chosen before interrupt k + 1, so that interrupt finds a completion and does
  not campaign; whether the *following* interrupt stalls again depends on whether the next
  batch's proposals are still delayed 15.
- **The shape heuristic as a sweep column.** Per (edge, shape) counts against prompt would give
  the sweep a program-knowledge-free verdict for displacement-type storms; try it on the three
  validation programs and the controls before trusting it.
- **The redo queue under load:** give the acceptors a per-tick capacity (`rpc_retry`'s
  `max_per_tick`) and ask whether bursty completions plus re-proposals run the backlog away, i.e.
  whether this is a metastable failure and not only amplification. The proposal-path finding
  above says a constant delay already suffices for a loop with zero goodput; capacity is the way
  the delay arises on its own.
- Dropped: further precision work on lineage for opaque closures (the fork above replaces it),
  the analysis-time cost of the negative-leaf walk, and the "every derivation of the tick"
  variant. The PerfFuzz spike (`2026-09_perffuzz_spike.md`) answered the coverage-guided
  alternative: a generic search, yes; a generic score, no — the fork is its one untested path,
  now tested.
