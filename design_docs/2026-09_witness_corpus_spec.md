# Witness corpus specification: programs that may or may not amplify work

This document specifies a self-contained sub-project: a corpus of roughly ten small Hydro programs, each of which either does or does not exhibit schedule-dependent work amplification, each labeled, and each label confirmed by a stress test in the Hydro simulator. The corpus is ground truth for tools that this document deliberately does not describe, and the author must not learn how those tools work. The author should read this file, the two existing witnesses it names, and the simulator source as needed, and nothing else about the surrounding project. In particular, the author must not read other files under `design_docs/`, the `AI_WORK_STATUS.md` file, or any notes outside the repository.

## 1. Goal and definitions

A program does *productive* work when it processes an input it has not processed before. It does *amplified* work when the same input causes extra processing that a different, equally legal execution would not have caused. The extra work is *schedule-dependent* when the schedule, rather than the input, selects between the two executions: which messages land in which batch, how long a receiver holds an item before a tick observes it, and in what order ready ticks run. Retries fired because a response was late, and cache refills issued by many clients who all saw the same expiry, are examples.

Amplification becomes *metastability* when the extra work feeds back into the cause of the delay. A transient overload builds a backlog, the backlog delays responses past a timeout, the timeouts create retries, and the retries keep the backlog from draining after the overload has ended. The system stays in the bad state under a load it handled before the trigger.

The operational label criteria are as follows.

An **amplifying** witness satisfies all of these under a fixed harness: capacity is strictly above baseline offered load; a bounded trigger (a burst of extra input, or an interval during which delivery is held) is applied and then removed; after the trigger ends, some measure of pending work (backlog depth, outstanding table size, in-flight count) keeps growing or stays flat for many rounds, or the completion rate stays far below baseline; and a control run recovers. The control either omits the trigger or disables the mechanism through a program parameter while keeping the trigger, and in it the backlog returns to zero and completions return to baseline within a bounded number of rounds.

A **benign** witness satisfies these under the same shape of experiment: pending work returns to its pre-trigger level within a bounded number of rounds after the trigger ends; total work (messages sent, items processed) over the run is bounded by a small constant multiple of the input, and the author states the constant; and the bound holds under schedule exploration (`fuzz` or `exhaustive`), not only under the fixed schedule.

A program with an amplifying variant and a benign variant selected by a parameter counts as two witnesses and is the preferred shape, because the two share everything except the mechanism under study.

## 2. Simulator facts the author needs

The simulator lives in `hydro_lang/src/sim/`.

**There is no clock.** The simulator's runtime has no time driver, so `source_interval` and `sample_every` cannot run under it. Every timer must be a stream parameter of type `Stream<(), L, Unbounded, TotalOrder, ExactlyOnce>`, where `L` is the process or cluster that owns the timer, and each element is one tick of that timer. A deployment wires `location.source_interval(period)` into the parameter; a simulation creates it with `location.sim_input::<(), TotalOrder, ExactlyOnce>()`, which returns a `(SimSender, Stream)` pair, and sends elements from the harness. `rpc_retry.rs` takes three such parameters and documents the pattern; `crdt_gossip.rs` takes one (`gossip_ticks`).

**Logical time is a fold over the timer.** A program that compares times (a timeout, an expiry) keeps a counter in Hydro state that advances once per timer element and stamps items with its current value; `rpc_retry.rs` does this with `use::state(|l| l.singleton(q!(0u64)))` and `enumerate()` on the clock stream. Wall-clock reads are not allowed in the dataflow.

**State lives in Hydro operators.** All mutable state must be `use::state`, `use::state_null`, folds, `KeyedSingleton`s, or other Hydro live collections carried across ticks. A program that keeps its tables in Rust cells captured by `q!` closures is not a valid witness even if it runs.

**Each `use::batch` is a scheduling decision.** Inside a `sliced!` block, `use::batch(stream, nondet!(/** ... */))` releases into the tick some prefix (for `TotalOrder` streams) or subset (for `NoOrder` streams) of what the stream has delivered so far, and the simulator chooses which. `use::snapshot(singleton, nondet!(...))` chooses which version of a singleton the tick observes. These are the only points where the schedule enters the program, so a hazard that depends on delay must have the delayed items pass through a `use::batch`. The `nondet!` comment should say what varying the batch can and cannot change.

**Sends are immediate; delay is holding.** A `send` or `broadcast_closed` over `TCP.fail_stop().bincode()` delivers each item into the receiver's top-level channel as soon as the sender emits it. Network delay is modeled entirely by the receiver's `use::batch` not releasing the item yet; there is no separate delay knob. Only `fail_stop` and (under `test_safety_only()`) `lossy_delayed_forever` transports are supported.

**Ticks fire only when they have input.** A `sliced!` block whose batch hooks hold nothing is not runnable, so a periodic job must be pumped by a timer element.

**`quiesce().await` runs to a fixpoint.** `hydro_lang::sim::quiesce()` (in `hydro_lang/src/sim/compiled.rs`) advances the scheduler until nothing can make progress. Inputs sent after it never interleave with work before it.

**The round-based harness.** A stress test loops over rounds. Each round, the harness sends one element on each timer that should advance, sends this round's data, awaits `quiesce()`, and drains every output with `SimReceiver::try_next`, `collect`, or `collect_sorted` (the `SimClusterReceiver` equivalents take a member id, and cluster inputs are sent with `send(member_id, item)`). The test asserts on the resulting per-round trace. `rpc_retry.rs::sim_tests::run` is the reference implementation.

**Two ways to run.** `flow.sim().run_prompt(async move || { ... })` (in `hydro_lang/src/sim/flow.rs`, driver in `hydro_lang/src/sim/prompt_schedule.rs`) runs one deterministic execution in which every batch releases everything it holds and ready ticks run round-robin; use it for the collapse and recovery measurements, whose numbers must be reproducible. `flow.sim().exhaustive(async || { ... })` enumerates every schedule choice and is feasible only for short runs; `flow.sim().fuzz(async || { ... })` samples schedules and is bounded with `unit_test_fuzz_iterations(k)`. Use those for the benign bound. `run_with_driver` accepts a custom bolero `Driver`. Cluster programs need `.with_cluster_size(&cluster, n)`, and programs that call `assert_has_consistency` need `.skip_consistency_assertions()`.

**Observability.** Outputs are exported with `stream.sim_output()` on a process or `stream.sim_cluster_output()` on a cluster. Unordered outputs are read with `collect_sorted`, so map them to `Ord` types.

## 3. Design rules for witnesses

Each witness is a `pub fn` that takes its locations, data input streams, timer streams, and a small config struct, and returns a struct of output streams. The body should be under about 300 lines of idiomatic Hydro dataflow, with the hazard visible in the dataflow rather than inside a large `q!` closure, so that a reader can point at the operator or `sliced!` block where the mechanism lives.

Each witness has a **capacity knob** (items served per tick, fan-out bound), a **load shape** that lives in the harness rather than the program (requests per round, trigger window), and a **mechanism knob** that switches the hazard off (`max_attempts = 1`, `backoff = true`, `coalesce = true`). Setting the mechanism knob to its safe value must yield the benign twin with nothing else changed.

Each witness exports enough outputs that work can be counted from outside: every message put on the wire, every item processed, completions with latency in ticks, and a per-tick pending-work measure such as backlog depth. The harness distinguishes first-time from repeated occurrences by id, as `rpc_retry.rs` does with `sent_first`/`sent_retry` and `served_first`/`served_again`. Ids must therefore be assigned inside the dataflow, for example with `enumerate()` on an arrival-ordered stream. The program must not be edited to help any external tool.

Each witness must run identically under `run_prompt` on every invocation; anything that would make runs differ, such as `HashMap` iteration order leaking into an emitted order, must be removed. The program must not depend on simulator-only APIs, and the module docs should say how a deployment would wire `source_interval` into each timer parameter. A deploy harness like `rpc_retry.rs`'s `deploy_with_workload` is welcome but not required.

## 4. Candidate witnesses

The list below is a starting point with an expected label for each. The author confirms or overturns each label by measurement; a candidate with the other label is still a valid witness, and the doc table records what was found. Aim for about half amplifying and half benign.

**W1. `rpc_retry` (exists at `hydro_test/src/cluster/rpc_retry.rs`). Amplifying, measured.** A client keeps an outstanding table and re-sends after `timeout_ticks`, up to `max_attempts`; the server serves `max_per_tick` per tick from a FIFO. A burst builds a backlog whose queueing delay exceeds the timeout, so every request is sent three times and offered load exceeds capacity forever. Mechanism knob: `max_attempts = 1` is the benign twin, already measured. This is the model; nothing needs doing.

**W2. `crdt_gossip` (exists at `hydro_std/src/ec_inference_demos/crdt_gossip.rs::g_set_gossip`). Expected benign; needs a load harness.** Each member folds updates into a G-Set and, on every `gossip_ticks` element, re-broadcasts its full state to all peers. It has only been tested for convergence. The harness should offer `u` updates per round to each member, pump gossip every round, and measure elements on the wire per round. The expectation is that per-round work is `n × (n−1) × |state|`, which grows with state size but not with delay or reordering; the author should confirm that holding gossip messages in a batch does not cause extra re-broadcasts.

**W3. Gossip with resend-until-acknowledged. Expected amplifying.** Extend W2 so that a member re-sends its state to a peer until the peer acknowledges having merged it, with a bound on merges per tick per member. Delayed acknowledgements cause re-sends, re-sends consume the peer's merge budget, and the peer's acknowledgements are delayed further. Mechanism knob: `ack_timeout_ticks = None` disables re-send.

**W4. Retry with exponential backoff and jitter. Expected benign.** The W1 client with the timeout for attempt `k` equal to `base × 2^k`, jittered by a deterministic hash of the request id. Retry load decays geometrically, so the backlog should drain after the trigger. The author should report whether some trigger size defeats backoff. Mechanism knob: `backoff = false` recovers W1.

**W5. Heartbeat-timeout leader election with follower stampede. Expected amplifying.** One member sends heartbeats on a timer; a follower that has seen no heartbeat within `election_timeout_ticks` broadcasts a vote request, and every member handles vote requests and heartbeats from the same per-tick budget. Heartbeats held past the timeout make every follower start an election at once, the election traffic delays heartbeat processing, and the next timeout fires before the election completes. Mechanism knob: `randomized_timeouts = true` staggers followers. Write this as small dataflow with no log replication.

**W6. Pure heartbeat (exists at `hydro_test/src/cluster/pure_heartbeat.rs`). Benign, negative control.** One message per timer element per member, no acknowledgements; work per round is exactly `n × (n−1)` under every schedule. Needs a round-based harness and a doc table.

**W7. Cache with expiry: thundering-herd refill versus coalescing. Amplifying without coalescing, benign with.** A cache serves lookups from keyed state, and on a miss sends a fetch to an origin with capacity `max_fetch_per_tick`; entries expire after `ttl_ticks`. Without coalescing, every lookup that arrives while an entry is missing produces its own fetch, so a hot key that expires while the origin is slow keeps the origin slow. With coalescing, the cache keeps a pending set and sends one fetch per missing key. Mechanism knob: `coalesce`. Trigger: a burst of lookups on a small key set.

**W8. Rebalancing that migrates the same items repeatedly. Expected amplifying.** Workers each hold a queue and exchange queue-length reports; on a rebalance timer, a worker that sees a peer shorter than itself by more than `threshold` sends it half the difference. Reports are messages and can be stale, so under held reports two workers trade the same items back and forth every rebalance tick, and each migration costs processing budget. Mechanism knob: `hysteresis_ticks` (an item that migrated within the last `h` ticks stays put).

**W9. Bounded server queue with early rejection. Expected benign.** The W1 server drops the oldest queued request when its backlog exceeds `max_backlog` and immediately sends a rejection, so the client's timeout never fires for dropped requests and the client retries on rejection with backoff. Mechanism knob: `max_backlog = None` recovers W1.

**W10. Compaction that falls behind. Expected amplifying.** A single log process appends records and, on a compaction timer, compacts if the uncompacted segment exceeds `segment_size`, spending one unit of its per-tick budget per uncompacted record scanned while appends queue up. A burst makes compaction longer, more appends queue, and the next compaction is larger. Mechanism knob: `incremental = true` caps records compacted per tick. This one has no network and tests whether the definition covers a single process.

**W11. Lease renewal storm. Expected amplifying.** Clients renew leases every `renew_every_ticks`; a renewal unacknowledged within `grace_ticks` is re-sent every tick, and a lapsed lease triggers a re-acquire that costs the server more than a renewal. Held acknowledgements produce a renewal per client per tick, delaying acknowledgements further. Mechanism knob: a cap of one outstanding renewal per client.

**W12. Transitive closure (exists at `hydro_test/src/local/productive_tc.rs`). Benign, negative control.** Recursive dataflow that generates many intermediate tuples, including duplicates, and terminates when novelty runs out; work is bounded by the size of the closure under every schedule. Needs a round-based harness that feeds edges over rounds and a doc table.

The author may substitute candidates as long as the corpus ends with at least four confirmed amplifying and four confirmed benign witnesses.

## 5. Labeling protocol

Every witness file ends with a `#[cfg(test)] mod sim_tests` structured like `rpc_retry.rs::sim_tests`, containing the following.

A `Round` record struct and a `run(config..., rounds) -> Vec<Round>` function that builds the flow, wires `sim_input`s for every parameter, exports every output, and executes the round loop under `run_prompt`.

A doc comment on the workload constants that states the **hand-computed expectation before measurement**: baseline load, capacity, trigger size and duration, the backlog the trigger should build, the delay that backlog implies against the timeout or expiry, and the offered load the mechanism will therefore produce after the trigger. `rpc_retry.rs` states that a 60-round trigger of 12 per round against a capacity of 5 builds a backlog of about 420, a queueing delay of about 84 rounds against a 40-round timeout, so every request is sent three times and offered load becomes 6 per round against a capacity of 5. Write the expectation first; if the measurement disagrees, keep both and explain.

A **main run** long enough to separate the trigger from the tail. `rpc_retry.rs` uses 800 rounds with the trigger in rounds 100 to 160 and asserts on rounds 600 to 800. For an amplifying witness the assertions are that the pre-trigger window is healthy, that tail completions are below a fraction of baseline (rpc_retry uses one quarter), that repeated work in the tail exceeds first-time work, and that the pending-work measure is non-decreasing at the end. For a benign witness the assertions are that the trigger did build pending work, that the tail is healthy, and that total work is at most the stated multiple of total input.

At least one **control**. For an amplifying witness, the two controls from `rpc_retry.rs` are the standard: no trigger stays healthy, and the same trigger with the mechanism knob set to safe recovers. For a benign witness, the control is the amplifying twin if one exists, and otherwise a schedule exploration (`fuzz` with a stated iteration count, or `exhaustive` over a short run) showing the work bound holds across schedules.

A **summary table in the module docs** with the measured numbers for each run: baseline per round, capacity, trigger, timeout or expiry, tail completions against baseline, tail repeated work against first-time work, backlog at tail start and end, and the label. The table must reflect the numbers the tests assert on.

Each test prints its trajectory at fixed round indices, as `print_trajectory` does.

## 6. Layout and process

New witnesses go under `hydro_test/src/cluster/witnesses/<name>.rs`, declared in `witnesses/mod.rs` and by a `pub mod witnesses;` line in `hydro_test/src/cluster/mod.rs`. Single-process witnesses such as W10 belong there too, for the sake of one directory. `rpc_retry.rs` stays where it is. Harnesses for the existing `crdt_gossip`, `pure_heartbeat`, and `productive_tc` programs should be new files under `witnesses/` that import them, leaving the existing files unmodified.

Each module doc begins with a paragraph describing the program, a paragraph naming the mechanism and the knob, the list of timer parameters, and the summary table from section 5.

One commit per witness file. Commit messages follow Conventional Commits with a Markdown body, as `AGENTS.md` requires, and carry the measured tail numbers in the body. Commit only when the tests pass.

Run tests with output redirected and grepped, never streamed: `cargo test -p hydro_test <name> > "$TMPDIR/<name>.log" 2>&1; grep -E "test result|round |tail|panicked" "$TMPDIR/<name>.log"`. Do not rerun a long test to re-read output that is already in the log. macOS has no `timeout` command. Building may need `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`. Do not run stable `cargo fmt`; `rustfmt.toml` uses nightly options. Never edit crate sources while a simulator test is running. Escape `!` when typing macro names such as `nondet!` into a shell.

Write all prose, in code comments and in commit messages, in complete sentences.

At the end, the author reports the table of labels with their numbers, the candidates whose expected label was overturned and why, and any candidate that could not be made to run in the simulator.
