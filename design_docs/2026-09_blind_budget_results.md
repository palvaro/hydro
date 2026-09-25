# The checker on the independently written retry-governance programs

The five configurations under `hydro_test/src/cluster/witnesses/blind_budget/` were written by an author who had read only the blind-author brief and the seven earlier blind programs, and who knew nothing about the checker. This document records what the checker said about them before their labels were read, and then compares.

The operator (the thread that wrote this document and `hydro_test/src/cluster/witnesses/blind_budget_check.rs`) read, before Phase 1, only these things: the signature of the shared constructor `retry_service` in `blind_budget/service.rs`; the definitions of the `Policy` enum and the `RetryConfig` struct, which are needed to construct a configuration; the `steady_state_inputs` helper's signature; and, in each configuration file, the single `const CONFIG: RetryConfig = ...` line and the line that sets the steady-state per-round request count. Those lines were found with grep and read without their surroundings. The module doc comments, doc tables, test bodies, and `blind_budget/mod.rs` were not read until Phase 2. The grep output also showed the names of two test functions in each file, `run` and `steady_state_control`, and one assertion in a control test that 120 requests produced 120 sends, 120 serves, and 120 completions; none of these reveals a label.

## Phase 1: verdicts recorded before reading labels

All five checks ran in 106 s of test time (`cargo test -p hydro_test --lib blind_budget_check -- --nocapture --test-threads=1`, about five and a half minutes of wall clock including the build), each at the checker's default horizon of 1000 rounds. Every configuration shares the same program, `retry_service`, with `timeout_ticks: 6` and `server_capacity: 5`, and the same steady-state input: two requests, one client clock element, and one server clock element per round. Only the `policy` field differs. The checker found five decision points in the program, all in `service.rs`: line 415 admits the client's clock, line 416 admits the client's new requests, line 417 admits replies arriving at the client, line 450 admits the server's clock, and line 451 admits requests arriving at the server. The tables below are the checker's own output; each curve gives extra work over the unheld run at hold lengths of 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, and 999 rounds, and the 999-round hold lasts to the end of the run, so its numbers include work pushed past the horizon and are read separately.

### `no_governance`, policy `Policy::None { max_attempts: 3 }`

```
unheld run: 8000 records admitted at decision points, 4000 network messages sent, 5 decision points had buffered input
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
src/cluster/witnesses/blind_budget/service.rs:415, batch of (usize, ()): no extra work at any hold length
src/cluster/witnesses/blind_budget/service.rs:416, batch of (usize, u64)
    extra records admitted                0      0      0      0     10   6697   6547   6227   5587   4307  -5994
    extra messages, busiest sender        0      0      0      0      5   3860   3870   3870   3870   3870  -1998
    first reacted at a hold of 16 rounds with 10 extra; largest extra work 6697 in admitted records
src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply
    extra records admitted                0      0      0      8     56    184    558   6933   6933   6933   1962
    extra messages, busiest sender        0      0      0      4     28     92    279   3960   3960   3960   3960
    first reacted at a hold of 8 rounds with 8 extra; largest extra work 6933 in admitted records
src/cluster/witnesses/blind_budget/service.rs:450, batch of ()
    extra records admitted                0      0      0     12     64    192    450   6957   6957   6957    963
    extra messages, busiest sender        0      0      0      6     32     96    225   3960   3960   3960   3960
    first reacted at a hold of 8 rounds with 12 extra; largest extra work 6957 in admitted records
src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
    extra records admitted                0      0      0     16   6865   6797   6637   6317   5677   4397  -3996
    extra messages, busiest sender        0      0      0      8   3948   3960   3960   3960   3960   3960   3960
    first reacted at a hold of 8 rounds with 16 extra; largest extra work 6865 in admitted records
verdict: hazardous
verdict: hazardous
  decision point: src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
  first reacted at a hold of 8 rounds, with 16 extra admitted records there
  largest extra work over the curve: 6865 in admitted records
  decision points that admitted more records under that hold, at its peak:
    src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply +2917
    src/cluster/witnesses/blind_budget/service.rs:451, batch of Request +3948
```

### `time_budget`, policy `Policy::TimeBudget { initial_tokens: 0, capacity: 40, refill_per_tick: 4 }`

```
unheld run: 8000 records admitted at decision points, 4000 network messages sent, 5 decision points had buffered input
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
src/cluster/witnesses/blind_budget/service.rs:415, batch of (usize, ()): no extra work at any hold length
src/cluster/witnesses/blind_budget/service.rs:416, batch of (usize, u64)
    extra records admitted                0      0      0      0     10   6717   6429   5853   4701   2397  -5994
    extra messages, busiest sender        0      0      0      0      5   3880   3752   3496   2984   1960  -1998
    first reacted at a hold of 16 rounds with 10 extra; largest extra work 6717 in admitted records
src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply
    extra records admitted                0      0      0      8     56    390   6969   6969   6969   6969   1998
    extra messages, busiest sender        0      0      0      4     28    195   3996   3996   3996   3996   3996
    first reacted at a hold of 8 rounds with 8 extra; largest extra work 6969 in admitted records
src/cluster/witnesses/blind_budget/service.rs:450, batch of ()
    extra records admitted                0      0      0     12     64    284   6993   6993   6993   6993    999
    extra messages, busiest sender        0      0      0      6     32    142   3996   3996   3996   3996   3996
    first reacted at a hold of 8 rounds with 12 extra; largest extra work 6993 in admitted records
src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
    extra records admitted                0      0      0     16   6901   6833   6673   6353   5713   4433  -3996
    extra messages, busiest sender        0      0      0      8   3984   3996   3996   3996   3996   3996   3996
    first reacted at a hold of 8 rounds with 16 extra; largest extra work 6901 in admitted records
verdict: hazardous
verdict: hazardous
  decision point: src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
  first reacted at a hold of 8 rounds, with 16 extra admitted records there
  largest extra work over the curve: 6901 in admitted records
  decision points that admitted more records under that hold, at its peak:
    src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply +2917
    src/cluster/witnesses/blind_budget/service.rs:451, batch of Request +3984
```

### `success_budget`, policy `Policy::SuccessBudget { initial_tokens: 10, capacity: 40, replies_per_token: 4 }`

```
unheld run: 8000 records admitted at decision points, 4000 network messages sent, 5 decision points had buffered input
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
src/cluster/witnesses/blind_budget/service.rs:415, batch of (usize, ()): no extra work at any hold length
src/cluster/witnesses/blind_budget/service.rs:416, batch of (usize, u64)
    extra records admitted                0      0      0      0     10     74    138    266    522    935  -5994
    extra messages, busiest sender        0      0      0      0      5     37     69    133    261    498  -1998
    first reacted at a hold of 16 rounds with 10 extra; largest extra work 935 in admitted records
src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply
    extra records admitted                0      0      0      8     20     20     20     20     20     20  -1988
    extra messages, busiest sender        0      0      0      4     10     10     10     10     10     10     10
    first reacted at a hold of 8 rounds with 8 extra; largest extra work 20 in admitted records
src/cluster/witnesses/blind_budget/service.rs:450, batch of ()
    extra records admitted                0      0      0     12     20     20     20     20     20     20  -2987
    extra messages, busiest sender        0      0      0      6     10     10     10     10     10     10     10
    first reacted at a hold of 8 rounds with 12 extra; largest extra work 20 in admitted records
src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
    extra records admitted                0      0      0     16     42     74    138    266    522    935  -3996
    extra messages, busiest sender        0      0      0      8     21     37     69    133    261    498     10
    first reacted at a hold of 8 rounds with 16 extra; largest extra work 935 in admitted records
verdict: hazardous
verdict: hazardous
  decision point: src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
  first reacted at a hold of 8 rounds, with 16 extra admitted records there
  largest extra work over the curve: 935 in admitted records
  decision points that admitted more records under that hold, at its peak:
    src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply +437
    src/cluster/witnesses/blind_budget/service.rs:451, batch of Request +498
```

### `hybrid_budget`, policy `Policy::HybridBudget { initial_tokens: 0, capacity: 40, refill_per_tick: 3, replies_per_token: 4 }`

```
unheld run: 8000 records admitted at decision points, 4000 network messages sent, 5 decision points had buffered input
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
src/cluster/witnesses/blind_budget/service.rs:415, batch of (usize, ()): no extra work at any hold length
src/cluster/witnesses/blind_budget/service.rs:416, batch of (usize, u64)
    extra records admitted                0      0      0      0     10   6200   5939   5416   4371   2278  -5994
    extra messages, busiest sender        0      0      0      0      5   3363   3262   3059   2654   1841  -1998
    first reacted at a hold of 16 rounds with 10 extra; largest extra work 6200 in admitted records
src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply
    extra records admitted                0      0      0      8     56    198    390    774   1542   3078   1002
    extra messages, busiest sender        0      0      0      4     28     99    195    387    771   1539   3000
    first reacted at a hold of 8 rounds with 8 extra; largest extra work 3078 in admitted records
src/cluster/witnesses/blind_budget/service.rs:450, batch of ()
    extra records admitted                0      0      0     12     64    204    396    780   1548   3084      3
    extra messages, busiest sender        0      0      0      6     32    102    198    390    774   1542   3000
    first reacted at a hold of 8 rounds with 12 extra; largest extra work 3084 in admitted records
src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
    extra records admitted                0      0      0     16   6368   6281   6107   5761   5067   3680  -3996
    extra messages, busiest sender        0      0      0      8   3451   3444   3430   3404   3350   3243   3000
    first reacted at a hold of 8 rounds with 16 extra; largest extra work 6368 in admitted records
verdict: hazardous
verdict: hazardous
  decision point: src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
  first reacted at a hold of 8 rounds, with 16 extra admitted records there
  largest extra work over the curve: 6368 in admitted records
  decision points that admitted more records under that hold, at its peak:
    src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply +2917
    src/cluster/witnesses/blind_budget/service.rs:451, batch of Request +3451
```

### `circuit_breaker`, policy `Policy::CircuitBreaker { max_attempts: 3, timeout_threshold: 8, open_ticks: 12 }`

```
unheld run: 8000 records admitted at decision points, 4000 network messages sent, 5 decision points had buffered input
hold length (rounds)                      1      2      4      8     16     32     64    128    256    512    999
src/cluster/witnesses/blind_budget/service.rs:415, batch of (usize, ()): no extra work at any hold length
src/cluster/witnesses/blind_budget/service.rs:416, batch of (usize, u64)
    extra records admitted                0      0      0      0     10    -34    -66   -130   -290   -620  -5994
    extra messages, busiest sender        0      0      0      0      5    -17    -33    -65   -145   -310  -1998
    first reacted at a hold of 16 rounds with 10 extra; largest extra work 10 in admitted records
src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply
    extra records admitted                0      0      0      8    -34   -104   -244   -454   -942  -1938  -3908
    extra messages, busiest sender        0      0      0      4    -17    -52   -122   -227   -471   -969  -1910
    first reacted at a hold of 8 rounds with 8 extra; largest extra work 8 in admitted records
src/cluster/witnesses/blind_budget/service.rs:450, batch of ()
    extra records admitted                0      0      0     12    -34   -104   -244   -454   -946  -1942  -4907
    extra messages, busiest sender        0      0      0      6    -17    -52   -122   -227   -473   -971  -1910
    first reacted at a hold of 8 rounds with 12 extra; largest extra work 12 in admitted records
src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
    extra records admitted                0      0      0     16    -32   -104   -244   -486   -936  -1994  -3996
    extra messages, busiest sender        0      0      0      8    -16    -52   -122   -243   -468   -997  -1910
    first reacted at a hold of 8 rounds with 16 extra; largest extra work 16 in admitted records
verdict: hazardous
verdict: hazardous
  decision point: src/cluster/witnesses/blind_budget/service.rs:451, batch of Request
  first reacted at a hold of 8 rounds, with 16 extra admitted records there
  largest extra work over the curve: 16 in admitted records
  decision points that admitted more records under that hold, at its peak:
    src/cluster/witnesses/blind_budget/service.rs:417, batch of Reply +8
    src/cluster/witnesses/blind_budget/service.rs:451, batch of Request +8
```

### Shapes of the reacting curves, classified by eye before reading labels

Every configuration reacts first at a hold of 8 rounds, the first doubling above the 6-tick timeout, and every configuration's named decision point is line 451, the server's arrivals edge, because it is one of three points that react at 8 rounds and it has the most extra work at that length (16 against 12 and 8). The shapes differ, and the shape is what the present verdict rule does not read. In what follows the 999-round hold is excluded, since a hold to the end of the run pushes work past the horizon and its negative numbers describe postponement.

`no_governance`. The arrivals curve (line 451) jumps from 16 at 8 rounds to 6865 at 16 rounds and then declines slowly (6797, 6637, 6317, 5677, 4397) while the busiest sender's extra messages sit at a ceiling of 3960 from 32 rounds onward. The ceiling is the cap the policy imposes: with three attempts each of the roughly 2000 requests can be re-sent at most twice, and 3960 extra messages is two extra sends for nearly every request. The replies curve (line 417) and the server-clock curve (line 450) rise across the doublings (8, 56, 184, 558 and 12, 64, 192, 450) and then reach the same ceiling at 128 rounds. This is a curve that rises with the hold until the policy's cap stops it.

`time_budget`. The curves are the same shape as `no_governance` and nearly the same numbers: the arrivals curve jumps to 6901 at 16 rounds and the message ceiling is 3996; the replies curve rises 8, 56, 390 and reaches its ceiling of 3996 messages at 64 rounds, one doubling earlier than without governance. A refill of 4 tokens per tick against 2 requests per round never runs the bucket dry, so the budget does not bind and the reaction is the ungoverned one.

`success_budget`. The three reacting points split into two shapes. The replies curve (line 417) and the server-clock curve (line 450) step to 20 extra admitted records and 10 extra messages at 16 rounds and stay at exactly 20 and 10 through 512 rounds: a step that holds flat. The arrivals curve (line 451) does something else: 16, 42, 74, 138, 266, 522, 935 in admitted records and 8, 21, 37, 69, 133, 261, 498 in messages, which is close to doubling at every doubling of the hold, so it rises roughly in proportion to the hold length and shows no sign of a ceiling by 512 rounds. Its magnitude is small, about one extra message per round of hold, against about eight per round for the ungoverned program, but it rises.

`hybrid_budget`. The arrivals curve (line 451) steps to 6368 at 16 rounds and then declines slowly, with the busiest sender's messages at a ceiling near 3450 (3451, 3444, 3430, 3404, 3350, 3243), the same shape as `no_governance` at a slightly lower level. The replies curve (line 417) and the server-clock curve (line 450) rise steadily across every doubling, 8, 56, 198, 390, 774, 1542, 3078 in admitted records and 4, 28, 99, 195, 387, 771, 1539 in messages, reaching 3000 messages at the 999-round hold; that is a rise in proportion to the hold length with no ceiling reached.

`circuit_breaker`. Every reacting curve is positive at exactly one hold length, 8 rounds (16, 12, and 8 extra admitted records at the three points), and negative at every longer hold, falling further as the hold grows: the arrivals curve reads -32, -104, -244, -486, -936, -1994 from 16 to 512 rounds. A positive step of at most 16 followed by a fall below the unheld run is a shape neither of the two descriptions above fits: after a short reaction the program does less work than it would have done with no delay at all, and the deficit grows with the hold.

Under the present verdict rule, which says hazardous on any positive extra work at any hold length, all five configurations are hazardous, all named at line 451, and all first reacting at 8 rounds. Under a rule that reads the curve, the five would not all get the same verdict: `no_governance`, `time_budget`, and `hybrid_budget` rise to a ceiling or rise without one; `success_budget` rises without a ceiling on one edge and steps flat on two; and `circuit_breaker` steps once and then falls.

## Phase 2: comparison

Only after the section above was written did the operator read `blind_budget/mod.rs`, each configuration's module docs and assurance arguments, and the body of `service.rs`.

The author's labels are: `no_governance` hazardous by exhibited collapse (the burst test's tail has 800 repeated sends and 666 repeated serves and the backlog grows from 2119 to 2318); `time_budget` hazardous by exhibited collapse (800 repeated sends in the tail, backlog 1100 to 1299, "timer refills sustain four retries per round after the input burst"); `hybrid_budget` hazardous by exhibited collapse (691 repeated sends, backlog 914 to 1005); `success_budget` benign by assurance argument (with `N` input requests, `B = 10` initial tokens, and one token per `K = 4` useful replies, each id yields at most one useful reply, so retries are at most `B + floor(N/K)` and sends and serves are each at most `N + B + floor(N/K)` under every schedule, asserted under 256 random schedules); and `circuit_breaker` benign by assurance argument (each accepted input carries an attempt counter that a probe also increments, so sends and serves are each at most `N × A` with `A = 3`, asserted under 256 random schedules; new input is dropped while the breaker is open or probing).

| configuration | author's label | basis | present rule (any extra work) | agree | candidate rule (extra work keeps rising with hold length) | agree |
|---|---|---|---|---|---|---|
| `no_governance` | hazardous | exhibited collapse | hazardous, line 451, first at 8 | yes | hazardous: the replies and server-clock curves rise across five doublings before reaching the cap | yes |
| `time_budget` | hazardous | exhibited collapse | hazardous, line 451, first at 8 | yes | hazardous: same shape as `no_governance` | yes |
| `success_budget` | benign | assurance argument | hazardous, line 451, first at 8 | no | hazardous: the arrivals curve doubles at every doubling through 512 rounds | no |
| `hybrid_budget` | hazardous | exhibited collapse | hazardous, line 451, first at 8 | yes | hazardous: the replies and server-clock curves rise linearly to the horizon | yes |
| `circuit_breaker` | benign | assurance argument | hazardous, line 451, first at 8 | no | bounded reaction: one positive step of 16 at 8 rounds, then below the unheld run | yes |

The present rule agrees with three of five labels and is wrong on both benign ones. The candidate rule, read as "some curve rises across several doublings", agrees with four of five; it recovers `circuit_breaker` and still disagrees on `success_budget`. Read more strictly, as "a curve that steps once and holds is a bounded reaction", the candidate rule would also call `no_governance` and `time_budget` bounded reactions, because their arrivals curves step from 16 to about 6900 in one doubling and then hold at the cap, and that reading would agree with only two of five. The rule's outcome therefore depends on how "keeps rising" is defined, and no reading of the shape alone separates every configuration correctly.

### The success-refilled budget did not behave as predicted, and the reason is the release

The prediction was that a budget refilled only by successes would go flat: when replies stop, refills stop, retries stop, and extra work is bounded by the bucket however long the stall lasts. That is exactly what the checker measured when it held the replies edge (line 417) or the server's clock (line 450): extra work steps to 10 messages, which is the 10 initial tokens spent on 10 retries, and stays there through 512 rounds. On those two edges the mechanism turns itself off as predicted.

Holding the server's arrivals edge (line 451) produces a different curve because of what happens when the hold ends. During a hold of `k` rounds the client sends `2k` requests that queue at the held edge, and it spends its 10 initial tokens on retries that queue there too; nothing completes, so no tokens are earned. When the hold is released, about `2k` requests are delivered to the server at once and drain at 5 per tick while 2 new requests arrive per round, so the backlog shrinks by about 3 per round and lasts about `2k / 3` rounds. Throughout the drain the queueing delay exceeds the 6-tick timeout, so every request that completes has already timed out at the client, and each completion is a useful reply that earns a quarter of a token. The client therefore issues about one retry for every four completions for the whole drain, and every one of those retries is a duplicate whose original is still in the queue. Extra work is about half a retry per round of hold, which is what the curve shows: 21, 37, 69, 133, 261, 498 extra messages at 16, 32, 64, 128, 256, 512 rounds, close to `k` messages at hold `k` once sends and serves are added. The 999-round hold reads 10 because nothing is ever released and nothing ever completes.

The author's bound holds. With `N = 2000` requests over the run, the bound on retries is `10 + 500 = 510`, and the largest extra sends the checker measured is 498 at the 512-round hold, so the curve was approaching the bound rather than passing it; a longer horizon at the same input rate would show it saturating near 510. The two statements are consistent: total work is bounded by a function of the input alone (`1.25N + 10` sends), which is the author's benign label, and a delay of `k` rounds costs about `k` extra messages up to that bound, which is the checker's rising curve. The definition in the study brief contains both clauses, "the excess grows the longer delivery is delayed" and "work is bounded by a function of the input under every schedule", and this program satisfies both, so the definition itself does not decide the label. What decides it in practice is the constant: this policy caps extra work at a quarter of the input, and 2 requests per round times 1.25 is 2.5 against a capacity of 5, so the burst test recovers, whereas `no_governance` caps extra work at twice the input, and 2 times 3 is 6 against a capacity of 5, so the burst test collapses. Both caps are functions of the input alone; the difference between recovery and collapse is the multiplier against the workload, which is the workload dependence the owner has identified before.

### The time-refilled budget did not bind

The prediction was that a time-refilled budget would grow with a smaller slope than no governance. It did not, because the author's parameters (4 tokens per tick against 2 requests per round) never let the bucket run dry: the `time_budget` curves are the `no_governance` curves with slightly higher ceilings (3996 against 3960 messages), and the author's own table says the refills sustain four retries per round after the burst. The prediction is about a budget that binds, and this configuration does not test it.

### The circuit breaker sheds load, which neither rule reads

The breaker's curves are positive only at the 8-round hold (16, 12, and 8 extra records at the three reacting points, the retries issued during the 8 consecutive timeouts before the breaker opens) and negative at every longer hold, falling to -1994 extra records at 512 rounds. The negative numbers are the program dropping new input while open or probing, so it does less work than the unheld run, and the deficit grows with the hold. The present rule flags the program hazardous on the 16 extra records; the candidate rule calls it a bounded reaction because the step does not repeat; neither rule describes what the program actually does, which is to stop working. Load shedding is benign for amplification and costly in dropped requests, and the checker counts the first and not the second: the `dropped` output stream carries that cost and no count in the report reads it.

The author's assurance argument for the breaker deserves a note. The bound offered, sends at most `N × A`, is the attempt cap, and `no_governance` has the same cap with the same `A = 3`; the argument as written would prove `no_governance` benign too, and that configuration collapsed. What separates the two programs is not the bound but the shedding: while open, the breaker refuses new input, so the queue drains and the cap is never approached. The label is defensible on the burst test's behaviour, but the written argument does not carry it.

### Two observations about the checker on these programs

The named decision point is line 451 in every configuration, because three points react at the same shortest hold (8 rounds, the first doubling above the 6-tick timeout) and the tie goes to the largest extra work at that length, which the arrivals edge wins with 16 against 12 and 8. The program's retry logic lives in one function, `client_step`, applied inside a single step; the checker did not need to see inside it, because the five batch points at the step's boundaries were enough to hold and count.

### Firewall

Before Phase 1 the operator's grep for the steady-state input matched, in each configuration file, the names of two test functions (`run` and `steady_state_control`), a control assertion that 120 requests gave 120 sends, 120 serves, and 120 completions, a burst size of 12 per round, and fragments of harness bodies that feed the clocks and quiesce. None of these names a label or a basis. Nothing else outside the constructor signature, the type definitions, and the `CONFIG` constants was read before Phase 1 was written.
