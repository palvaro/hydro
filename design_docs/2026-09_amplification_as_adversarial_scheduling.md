# Work amplification as adversarial scheduling

Status: design note, step 2 of the metastability project. E0 (canonical program + deterministic
sim collapse) is done; no checker code exists yet. Ground truth:
`hydro_test/src/cluster/retry_storm.rs`, a Hydro client/server program with timeout/retry that
is driven to a metastable collapse both on localhost and, deterministically, in the simulator
(step 1: commit `bbd3ca02c5`; E0: the commit that adds this file).

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
