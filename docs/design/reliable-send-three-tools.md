# Reliable delivery under hydrofrac, Cress and Sedge

This document follows one small protocol, a sender that resends until its messages are acknowledged, through three tools that each ask a different question about it. Hydrofrac asks whether the protocol is correct when messages are lost and nodes crash. Cress asks whether delay makes the protocol do work that its input did not require. Sedge asks whether the deployed protocol recovers from a burst of load when its receiver has a fixed, externally imposed CPU budget. The naive protocol passes the first question and fails the other two. We then add a token bucket to its resends in three different ways and let the tools judge the candidates. Each tool rejects or ranks something that the other two cannot see.

Every number below comes from a named test or run. Claims are marked in the style of the hydrofrac guide:

- **DEMONSTRATED** means that a test or a run shows it, and the test or run is named.
- **ARGUED** means that it follows from the code or from a model, and nothing here measures it directly.
- **GUESSED** means that it is our reading of the data, and we have not checked it.

Contents:

1. [The question](#1-the-question)
2. [The three tools](#2-the-three-tools)
3. [The protocol](#3-the-protocol)
4. [Act 1: the naive protocol is correct and fragile](#4-act-1-the-naive-protocol-is-correct-and-fragile)
5. [The diagnosis](#5-the-diagnosis)
6. [Act 2: three token buckets](#6-act-2-three-token-buckets)
7. [Act 3: real loss in deployment](#7-act-3-real-loss-in-deployment)
8. [Limits of what we showed](#8-limits-of-what-we-showed)
9. [How to reproduce](#9-how-to-reproduce)

The code is on the local branch `demo/budget-runs` (commit `4f8b9ebbc5`), which sits on `integ/frac-cress-sedge` (`ebd4272032`), which in turn sits on upstream `hydro-project/hydro` `main` at `858ba1a6b2`. The Sedge runs used a 16-core host with Docker. The containers ran on cores 8–15, the harness ran on cores 0–7, and each node had a 6000 MB memory limit. A background Java process (`TPSGeneratorWorker`) was running on the host throughout.

## 1. The question

A reliable sender over a lossy link must keep resending until it hears an acknowledgement, because otherwise a single lost message is lost forever. Resending is also how such a sender hurts itself: when acknowledgements are late, the sender cannot tell a lost message from a slow one, so it resends messages that have already arrived, and those resends compete with new work for the receiver's capacity. The protocol must therefore be correct under loss and also well behaved under delay and overload. These are different properties, and no single tool checks all of them.

## 2. The three tools

**Hydrofrac** is lineage-driven fault injection in the Hydro simulator. It takes a rule of the form "if a record is in collection `A` at the nodes that the A-side chooses, then a record with the same key is in collection `B` at the nodes that the B-side chooses," and it searches for a set of lost messages and crashes that breaks the rule. It either returns a counterexample or certifies that no number of crashes or lost messages within its bounds breaks the rule. It knows nothing about time or load.

**Cress** checks schedule amplification in the simulator. It holds each batch of a program for longer and longer and counts the messages that the program sends compared with the run in which nothing is held. When the extra work grows with the length of the hold, the program turns delay into load. Cress reports the shape of that growth (a ceiling, or a rate per round of delay) and a multiplier. It never loses a message, so it knows nothing about faults.

**Sedge** deploys the program, imposes capacity from outside with Docker CPU quotas, and drives it with an open-loop Poisson load that has a baseline, a burst, and a tail. It reads the tail against the baseline and says whether the program recovered, is still draining, or collapsed. It measures wire traffic from outside the program, from each container's `/proc` write counters. Its deployments run over plain TCP, so it knows nothing about loss. Sedge reads Cress's classification of each output stream from Cress's result table, so Cress runs first on the same function and configuration.

## 3. The protocol

`reliable_send` in `hydro_std/src/reliable_send.rs` is `Stream::send` for a lossy network. The sender puts each new value on the wire at once and adds it to an outstanding set. An acknowledgement removes its value from the set. On every element of the timer stream `resend_ticks`, the sender resends every value that is still outstanding. The receiver acknowledges every arrival, including retransmissions, and delivers arrivals with duplicates removed. The value is its own identity, so the protocol needs no sequence numbers. Both channels are `TCP.lossy_delayed_forever()`. The whole sender is one `sliced!` block, and its state lives in Hydro collections:

```rust
let wire = sliced! {
    let new = use::batch(source, nondet!(/** ... */));
    let acked = use::batch(acks, nondet!(/** ... */));
    let ticks = use::batch(resend_ticks, nondet!(/** ... */));
    let mut outstanding = use::state_null::<Stream<T, Tick<_>, Bounded, NoOrder>>();

    let still = outstanding.filter_not_in(acked);
    let resent = still.clone().filter_if(ticks.first().is_some());
    outstanding = still.chain(new.clone()).unique();

    new.chain(resent)
};
```

All three tools run the same function, `hydro_test::cluster::reliable_send_check::reliable_send_pair`, which wires one sender to one receiver, numbers the input so that a load generator can match deliveries to requests, and returns the delivered stream. The function carries one attribute for each simulation or deployment tool, and both attributes use the configuration name `two_per_round` so that Sedge finds Cress's results:

```rust
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = two_per_round,
    T = super::rpc_retry::Payload<4096>,
    load = values,
    mu = 500,
    cpus(Sender = none, Receiver = 0.01),
    cpu_period_us = 100_000,
    timers(resend_ticks = std::time::Duration::from_secs(1)),
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = two_per_round,
    T = u64,
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
pub fn reliable_send_pair<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> ReliableSendOutputs<'a, T>
```

The Cress configuration offers two new values per round for the first 125 rounds of a 1000-round run, and the timer fires once per round for the whole run. The Sedge configuration gives the receiver one percent of a core and gives the sender no limit, feeds `values` open loop with 4 KiB payloads that are distinct per sequence number, and wires `resend_ticks` to a one-second `source_interval`. The receiver's work per arrival is real decoding, deduplication and acknowledgement, and the program contains no capacity code.

The hydrofrac rule is validity: every value that a correct sender sends, a correct receiver delivers. It is stated in `hydro_test/src/cluster/ec_reliable_send.rs` as a rule from the sender's input to the receiver's output:

```rust
vals.implies(
    "delivery",
    &delivered,
    q!(|v| v),
    q!(|v| v),
    ASide::Correct,
    BSide::AllCorrect,
);
```

The hydrofrac harness models a sender's life. In the fault phase the sender sends its values, the timer may fire, and the network may drop any message. After the network heals, the timer keeps firing once per round and each round settles before the next one starts (`EcCheck::run_then_recover`).

## 4. Act 1: the naive protocol is correct and fragile

### Hydrofrac: correct under loss and crashes

| test | what it checks | verdict | cost |
|---|---|---|---|
| `delivers_each_value_once_without_faults` | Two values, a tick after each, no faults, every schedule. | Each value is delivered exactly once. | 14,580 schedules |
| `reliable_send_delivers_despite_losses` | One value, with a resend before the heal that may itself be lost. | `delivery` holds under any number of crashes and lost messages. | 112 fault sets, 1.0 s |
| `reliable_send_delivers_three_values_despite_losses` | Three values, with any of their first copies and acknowledgements lost in any interleaving, and resends only after the heal. | `delivery` holds under any number of crashes and lost messages. | 576 fault sets, 8,544 schedules, 74 s |
| `plain_send_loses_a_message` | The same harness over plain `send`, as a negative control. | `delivery` is broken by one lost message and no crash. | 2 fault sets |

All four results are DEMONSTRATED by the named tests. The control shows that the harness can find a loss when there is one to find, so the certificates are not vacuous.

### Cress: delay turns into unbounded work

Holding the acknowledgement batch makes the sender's extra work grow without a ceiling. At a hold of 999 rounds the sender sends 232,252 more messages than in the unheld run, which is 248 extra messages per round of delay and a multiplier of 930. Holding the timer batch or the new-values batch adds no work. These results are DEMONSTRATED by `./scripts/cress hydro_test reliable_send_pair` and pinned by the test `holding_acks_amplifies_and_holding_ticks_does_not`. The rate of 248 is the size of the outstanding set, since every value that was sent after the hold began is resent on every tick (ARGUED from the code and matched by the count).

### Sedge: collapse at every load we tried

Sedge calibrated the receiver's capacity at μ = 500 values per second under the one-percent quota: the receiver keeps up at 500 per second and falls behind at 545. We then swept the steady load λ = μ/f over the factors f below, with two trials per factor and a one-second resend timer.

| f | λ (values/s) | verdicts | tail goodput | sender wire messages/s, baseline to tail | sender memory at end |
|---|---|---|---|---|---|
| 2 | 250 | 2 COLLAPSED | 0% | 250 to about 770 | 0.84–0.90 GB |
| 4 | 125 | 2 COLLAPSED | 0% | 120 to about 750 | 0.38–0.43 GB |
| 8 | 62 | 2 COLLAPSED | 0% | 65 to about 820 | 0.21 GB |
| 16 | 31 | 2 COLLAPSED | 0% and 17% | 33 to about 850 | 0.10–0.13 GB |
| 32 | 16 | 2 COLLAPSED | 0% | 15 to about 800 | 0.11–0.12 GB |
| 64 | 8 | 1 COLLAPSED and 1 DRAINING | 0% and 100% | 7 to about 850 | 0.06–0.11 GB |

Eleven of twelve trials collapsed, and no trial was invalid (DEMONSTRATED by the Sedge sweep of `reliable_send_pair`). After the burst the receiver was throttled in 85–100% of CPU periods, while the sender used 0.37–0.79 cores and was never throttled. The sender wrote about 800 messages per second in the tail at every load, while the receiver delivered only 2–11% of that as new values. The rest of the traffic was resends.

![V0 at f = 8 with a one-second resend timer. The burst is the pink band, and the baseline and tail windows are green. Completions fall to almost nothing after the burst, latency climbs past 10 s, the sender keeps writing about 800 messages per second, and the receiver stays throttled.](reliable-send-three-tools-img/v0-f8-resend-1s.png)

The one trial that escaped, at f = 64, did so about 50 s into the run, when a catch-up of 3,590 acknowledgement messages per second cleared the backlog and latency fell back to 1.3 ms. The figure below sets that escape beside a trial that stayed collapsed.

![V0 with a one-second resend timer, at f = 8 (left, collapsed) and f = 64 (right, which escapes about 50 s into the run). The sender writes about the same number of messages per second in both runs although the steady load differs by a factor of eight.](reliable-send-three-tools-img/v0-f8-vs-f64.png)

<!-- PENDING: extra Sedge trials for V0 (more than 2 per factor), to turn "11 of 12" into a collapse probability per factor. -->

## 5. The diagnosis

The sender writes about the same number of messages per second after the burst whether the steady load is 250 or 8 values per second, so the steady load does not set the storm. The burst does. During the five-second burst at twice the receiver's capacity, values arrive faster than the receiver can acknowledge them, and the outstanding set grows to several thousand values. Every one-second tick then resends the whole set, which is a demand of roughly the backlog divided by the resend period, far above the 500 values per second that the receiver can process. While the receiver works through resends, acknowledgements stay late, the set stays large, and the next tick resends it again. This account is ARGUED from the protocol and from the traces, and the following experiment supports it.

If the account is right, stretching the resend period should shrink the demand by the same factor. With a 20-second period, which brings the backlog's demand below the receiver's capacity, f = 2 and f = 8 both recovered with full goodput and a median latency back at 1 ms (DEMONSTRATED by one trial at each factor; the committed attribute still uses one second).

![V0 at f = 8, with the same load and the same receiver quota. With a one-second resend timer (left) the run collapses, and with a 20-second timer (right) it recovers within about three seconds of the burst.](reliable-send-three-tools-img/v0-f8-period-1s-vs-20s.png)

This also explains a mismatch between the tools. Sedge's documentation predicts recovery when μ > λ + b for a protocol whose extra work grows at a rate b, and Cress measured b as the size of the outstanding set per tick. In the simulator that set is the steady input that arrived during the hold. In deployment it is whatever the burst left behind, so b is not a constant, and the threshold becomes a threshold on the backlog. Cress identified the right mechanism and the right shape, and Sedge showed how large the mechanism becomes in deployment. Our own first two predictions of the collapse threshold, f < 6 and then f < 31, were both wrong because they used the steady load, and the f = 32 runs refuted the second one.

A longer resend period is a blunt fix, because it also makes recovery from every real loss twenty times slower. What the protocol lacks is a limit on resends that does not depend on the backlog. A token bucket provides exactly that.

## 6. Act 2: three token buckets

### The variants

Each budgeted variant leaves first sends free and makes each resend spend one token. The bucket starts full with B = 10 tokens and never holds more than B. The variants differ only in where tokens come from.

| variant | function | tokens come from |
|---|---|---|
| V0 none | `reliable_send_pair` | No bucket; every tick resends everything outstanding. |
| V1 time | `reliable_send_pair_time_budget` | One token per timer tick. |
| V2 success | `reliable_send_pair_success_budget` | One token per four useful acknowledgements, that is, acknowledgements that clear an outstanding value. |
| V3 hybrid | `reliable_send_pair_hybrid_budget` | Both sources. |

The parameters (B = 10, one token per tick, four acknowledgements per token) are the constants of the retry-governance corpus in `hydro_test/src/cluster/witnesses/blind_budget/`. That corpus keeps its token state in a hand-written map inside a `fold`, which these variants do not do. The bucket here follows the pure-Hydro token bucket in `witnesses/blind/token_bucket.rs`, and it lives inside the same `sliced!` block as V0's sender. Tokens and the remainder of acknowledgement credit are `use::state` singletons. On a tick, the first `available` outstanding values are resent and move to the back of the outstanding FIFO, so resends go round robin and no value waits forever:

```rust
let fired = ticks.count();
let tick_grant = fired.clone().map(q!(move |n| n as u64 * tokens_per_tick));
let available = tokens
    .zip(tick_grant)
    .zip(ack_grant)
    .map(q!(move |((saved, by_tick), by_ack)| (saved + by_tick + by_ack).min(capacity)));

let placed = still
    .enumerate()
    .cross_singleton(available.clone())
    .cross_singleton(fired)
    .map(q!(|(((position, value), budget), fired)| {
        (fired > 0 && (position as u64) < budget, value)
    }));
let resent = placed.clone().filter_map(q!(|(resend, value)| resend.then_some(value)));
let waiting = placed.filter_map(q!(|(resend, value)| (!resend).then_some(value)));
let spent = resent.clone().count().map(q!(|n| n as u64));
tokens = available.zip(spent).map(q!(|(available, spent)| available - spent));
outstanding = waiting.chain(resent.clone()).chain(new.clone()).unique();
```

The FIFO needs an ordered input, so the budgeted variants take a `TotalOrder` source while V0 accepts any order. This matters for reliable broadcast, whose echo stream is unordered. The choice adds no `nondet!`, so it adds no schedule choices for hydrofrac to explore.

### What we predicted

Before running anything we wrote down what we expected from each tool. For V1 we expected hydrofrac to hold, because the timer always refills the bucket, and we expected Cress to report a rate of about one extra message per round, which is the refill rate. For V2 we expected hydrofrac to fail, because losses that drain the bucket leave no acknowledgement to refill it, and we expected Cress to report a ceiling. For V3 we expected hydrofrac to hold and Cress to look like V1. We expected all three budgeted variants to recover in Sedge whenever μ exceeds λ plus the refill rate.

### What the tools found

| | hydrofrac | Cress, holding acknowledgements | Sedge, at μ = 500 | calibrated μ under the same quota |
|---|---|---|---|---|
| V0 none | Holds (T2, T2b). | Grows at 248 per round; +232,252 at a hold of 999; 930×. | 11 of 12 trials collapsed, f = 2–64. | 500 |
| V1 time | Holds (T2, T2b). | Grows at 1.00 per round; +1,007 at a hold of 999; 5.03×. | 12 of 12 trials recovered, f = 1.1–16, settling in 3–6 s. | 841 |
| V2 success | **Broken** by starvation (see below). | **Stops at a ceiling of 10**; 1.04×. | 12 of 12 trials recovered, and it is the slowest near capacity, settling in 6–8 s. | 771 |
| V3 hybrid | Holds (T2, T2b). | Grows at 1.00 per round, the same as V1. | 12 of 12 trials recovered, settling in 3–6 s. | 771 |

The hydrofrac results are DEMONSTRATED by the tests in `ec_reliable_send.rs`. The single-value checks cost 112 fault sets and about 1.1 s for each variant, the three-value checks cost 576 fault sets and about 80 s for V1 and V3, and the fault-set counts match V0's exactly, so the bucket costs hydrofrac nothing. The Cress results are DEMONSTRATED by one run of `./scripts/cress hydro_test reliable_send_pair` over all four functions, which took 2.5 minutes, and every prediction held. The Sedge results are DEMONSTRATED by sweeps with μ pinned to V0's value of 500, so every variant saw the same absolute load, and the seeded generator produced identical arrivals across variants. "Settle" is the number of seconds after the burst until the median latency stays within five times its baseline (or 50 ms) for five consecutive seconds.

**Only hydrofrac sees that V2 can lose data.** With a test-only bucket of one token that only acknowledgements refill, hydrofrac reports that "the loss of the message 1 … on data and the loss of the 2nd copy of the message 1 … leave Receiver@0 without 1," with no crash and two lost messages, after exploring three fault sets in 15 ms (DEMONSTRATED by `success_budget_starves_when_losses_drain_the_bucket`). The first tick spends the only token on a resend, the network drops both copies, and no acknowledgement can ever arrive to earn another token. The same bucket with one extra token per tick delivers (DEMONSTRATED by `hybrid_bucket_of_one_delivers`). With the corpus bucket of ten tokens, starvation needs more than ten lost resends, which is beyond what hydrofrac can explore at its cost of about 2^k fault sets for k lossy messages, so for B = 10 we argue the failure but do not demonstrate it. Cress ranks V2 best of the four, and Sedge sees nothing wrong with it, because neither tool ever loses a message.

<!-- PENDING: V2 starvation as a function of B (B = 1, 2, 3, ...): the number of lost messages hydrofrac needs, and the cost of finding them. -->

![All four variants at f = 2 (λ = 250 per second) on identical arrivals. V0 (top left) collapses, and V1, V2 and V3 recover within seconds of the burst.](reliable-send-three-tools-img/compare-f2-all-variants.png)

![The three budgeted variants at f = 1.1 (λ = 455 per second), the closest to capacity that we tried. All three recover.](reliable-send-three-tools-img/compare-f1.1-budgeted.png)

Three more observations come from the Sedge runs. First, under the same one-percent quota the budgeted variants sustain 1.5–1.7 times V0's rate, because V0's resends consume receiver capacity even at steady load (DEMONSTRATED by one calibration per variant). The difference between V1's 841 and V2's and V3's 771 is a single probe step from a single calibration each, so we do not claim it. Second, in the tail the budgeted senders write about the offered load and end the run with about 20 MB of memory, while V0's sender writes 760–880 messages per second at every load and ends with 0.1–0.9 GB. Third, V2 drains slowest near capacity, which fits its rule, since its tokens come back only after acknowledgements clear values (GUESSED as the cause).

<!-- PENDING: the V2 throttling oddity. At f = 2 the V2 receiver is throttled in about 97% of CPU periods before and after the burst, against 43–62% for V1 and V3 at the same load and the same sender message rate. -->

<!-- PENDING: Cress rate sweeps (several workload rates and bucket parameters), to show how the measured rate tracks the refill rate. -->

### What the comparison settles

V2 is ruled out, because it can lose a value forever, and only hydrofrac shows it. V1 and V3 both remove the storm while keeping the delivery guarantee. Our experiments cannot yet tell V1 from V3. Cress holds acknowledgements, so the hybrid's acknowledgement refill has nothing to count, and Sedge's deployments lose nothing, so the hybrid has no losses to recover from. The difference between them can only show up when acknowledgements flow while values are being lost, which is the subject of Act 3.

## 7. Act 3: real loss in deployment

Sedge's deployment layer can make a sender's outgoing buffer lossy (`HYDRO_DEPLOY_SEND_BUFFER=lossy:N`), so the same receiver quota can be combined with real message loss. Our hypothesis is that V3 recovers from loss faster than V1 at the same protection against storms, because acknowledgements that do arrive buy extra resends. If V3 shows no advantage, V1 is the better choice because it is simpler.

<!-- PENDING: Act 3, the lossy-link Sedge experiment comparing V1 and V3 (and V0 as a reference), with verdicts, recovery times and figures. -->

## 8. Limits of what we showed

- Sedge ran two trials per factor, and one calibration per variant, on a host that also ran a background Java process.
- V2's starvation is demonstrated only with a bucket of one token, and for the corpus bucket of ten tokens it is argued.
- Hydrofrac cannot yet check the loss of resends of several values in one run. Its cost grows as about 2^k in the number k of lossy messages sent before the network heals, because the protocol's resend path passes through steps that hydrofrac treats as fragile. A possible remedy, treating `filter_not_in` as robust per key, is recorded as an open research item beside the soundness item R1 in `docs/design/hydrofrac-soundness.md` §18. Like every races certificate, the certificates here are subject to the open review items R1–R6, and R1 has this protocol's shape.
- Cress labels every variant HAZARDOUS, because by design it labels any extra work at all as hazardous (`cress/src/lib.rs`). The severity is in the growth shape and the multiplier.
- Cress and Sedge never lose a message: the simulator does not drop on lossy channels, and the deployment runs over plain TCP. Loss is exercised only by hydrofrac, and in deployment only by the pending Act 3.
- Deploying `reliable_send` required a change to `hydro_lang` (commit `2ad28e7c5f`), because the deployment backend refused every TCP channel other than `fail_stop`. The change deploys `LossyDelayedForever` as plain TCP, which is sound because in-order delivery that stops at a failure is one of the behaviors that a delayed-forever, unordered channel allows. It still refuses plain `Lossy`.

## 9. How to reproduce

All commands run from the repository root on `demo/budget-runs`, with Rust 1.98.1.

```sh
# hydrofrac: every reliable_send test, including the variants and the starvation probe.
cargo test -p hydro_test --lib cluster::ec_reliable_send -- --test-threads=4 --nocapture

# Cress: the name is a substring match, so this runs all four functions and writes all four
# rows of target/cress/conservation.tsv. Run it immediately before Sedge, because each run
# clears the table.
./scripts/cress hydro_test reliable_send_pair

# Sedge, one variant at a time, with nothing else heavy on the machine. Pin --mu so that every
# variant sees the same absolute load; without it a variant calibrates its own capacity.
./scripts/sedge --backend docker --config two_per_round --mu 500 \
    --factors 1.1,1.25,2,4,8,16 --trials 2 \
    --memory-mb 6000 --node-cpus 8-15 --harness-cpus 0-7 \
    hydro_test reliable_send_pair_time_budget
```

The Sedge filter is also a substring match, so the plain name `reliable_send_pair` deploys all four functions. The per-variant traces, calibration tables and plots of the runs in this document are under `target/sedge-demo-reliable-send/` and `target/sedge-demo-budget/` on the host where they were produced, and they are not committed.
