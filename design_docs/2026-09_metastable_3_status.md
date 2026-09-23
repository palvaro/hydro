# metastable-3: status

## What this branch is

`metastable-3` starts from `main` at `90d9d26736`. The earlier `metastable-2` branch (tip `87e0853aff`, pushed to the fork) is abandoned; its code may be read and copied but nothing here builds on it. The goal is a deterministic checker that, pointed at a Hydro program, decides whether the schedule alone can make it do work its input did not require, and names where in the program that hazard arises. The first deliverable, now complete, is a corpus of labeled witness programs that the checker will be judged against.

## Commits so far

```
177f3d259f feat(hydro_test): witnesses W6, W8, W10, W11, W12: heartbeat and TC controls, rebalancing, compaction, lease renewal
93a970ae9b docs(design): verdict rule follows the mechanism-based labels
484fd4e109 fix(hydro_test,design): label witnesses by mechanism, not by outcome under one harness
086c9bfdfb feat(hydro_test): witnesses W3, W4, W5: gossip resend-until-ack, backoff retry, election stampede
bc5c42c6b6 docs(design): the checker searches schedules under fixed input; twins are incidental
4b07732d63 docs(design): what carries over from metastable-2 under the stricter constraints
60abaf0a28 feat(hydro_test): witnesses W2, W7, W9: crdt_gossip load harness, cache with expiry, bounded queue with rejection
9953dba28e chore(hydro_test): add empty witnesses module for the corpus
7deaa01d9b docs(design): witness corpus specification
23a5014705 feat(hydro_lang,hydro_test): port deterministic prompt-schedule runner and rpc_retry witness from metastable-2
```

## The corpus

Twelve programs and twenty-two labeled configurations live in `hydro_test/src/cluster/witnesses/` and `hydro_test/src/cluster/rpc_retry.rs`. The full table with headline numbers and confirming test names is in `witnesses/mod.rs`. A configuration is hazardous when some schedule makes the same input cost more work, hazardous, mitigated when a parameter keeps that from collapsing the program under load, and benign when work is bounded by the input under every schedule.

| file | configuration | label |
|---|---|---|
| `rpc_retry` | `max_attempts = 3` / `1` | hazardous / benign |
| `backoff_retry` | `backoff = true` / `false` | hazardous, mitigated / hazardous |
| `bounded_queue_rejection` | `max_backlog = Some(100)` / `None` | hazardous, mitigated / hazardous |
| `cache_thundering_herd` | no coalescing, request dated / fill dated; coalescing | hazardous / hazardous, mitigated; benign |
| `compaction_falls_behind` | `compaction_reserve = 0` / `8` | hazardous / hazardous, mitigated |
| `crdt_gossip_load` | `g_set_gossip` unmodified | benign |
| `election_stampede` | `timeout_spread_ticks = 0` / `3` | hazardous / hazardous |
| `gossip_resend` | `ack_timeout_ticks > 0` / `= 0` | hazardous / benign |
| `lease_renewal_storm` | `resend_after_ticks = Some(4)` / `None` | hazardous / benign |
| `pure_heartbeat_load` | `pure_heartbeat` unmodified | benign |
| `rebalancing_ping_pong` | `cooldown_ticks = 0` / `8` | hazardous / hazardous, mitigated |
| `transitive_closure_load` | `productive_tc` unmodified | benign |

All 45 corpus tests pass together (`cargo test -p hydro_test --lib witnesses::`, 92 s of test time, about 5 minutes wall clock including the build), and the 3 `rpc_retry::sim_tests` pass in 8 s. Every program takes time as a stream parameter, keeps its state in Hydro operators, and was labeled by a hand-computed expectation written before measurement.

## Guiding documents

`design_docs/2026-09_witness_corpus_spec.md` defines the labels and the protocol every witness follows, and was written so that an author could work from it without knowing anything about the checker. `design_docs/2026-09_what_carries_over.md` reviews every technique from `metastable-2` against the new constraints and ends with the ordered experiments below.

## Next

The checker work starts with section 5 of `2026-09_what_carries_over.md`. Every experiment searches schedules under the witness's baseline workload with no load trigger, and its pass criterion is agreement with the corpus labels.

1. E1: port `edge_counts`, `hold_schedule` and `with_empty_ticks_allowed`, and show that a held response edge on `rpc_retry` produces excess sends while `max_attempts = 1` sends exactly the input. Nothing later is trusted until this passes.
2. E2: hold responses on a delay grid and show the excess rises with delay above the timeout, with `backoff_retry` rising more slowly.
3. E3: release-size moves on the arrivals edge, to reach server-side bounds in `bounded_queue_rejection`.
4. E4: the goal-free sweep over every labeled configuration, judged by the rule that a rising excess on some edge is hazardous and a flat one is benign.
5. E5 and E6: the static identity pass for localization candidates, and the payload-shape column on `election_stampede`.
6. E7: localization by negative leaves and forks on the cells E4 finds.

## Open bug, out of scope

`transitive_closure_load` found that the unmodified `hydro_test/src/local/productive_tc.rs` yields an incomplete closure under 212 of 256 fuzzed schedules. The program replaces its frontier with each tick's novel facts, so a tick that admits a graph without a step token discards the pending frontier. Completeness of that program depends on the schedule. It is recorded here and left alone.

## First checker experiment: random schedules on `rpc_retry`, no simulator changes

`hydro_test/src/cluster/witnesses/checker_experiments.rs` asks whether `flow.sim().fuzz(..)` alone, on the baseline workload with no trigger, recovers the label of `rpc_retry`. It does: over 512 random schedules with a timeout of 2 ticks and six clock elements per round, every schedule at three attempts cost more than the 48 distinct requests (mean 25 extra sends, max 55, and the server redid exactly that work), and no schedule at one attempt did, although the delays the schedules imposed were the same (1 to 6 ticks). Raising the timeout to 4 and 6 shows extra work rising with the observed delay within a run (4.8 to 8.1 at timeout 4) and falling across timeouts (25.1, 7.7, 2.3). Each 512-schedule run takes about 10 s once built. Two limits: the timeout had to be small for a uniform random draw to reach it, so the checker will need to direct the search or meter time; and a passing schedule's bytes are not exposed by `fuzz`, so although the per-tick trace names the batch decision that held a reply, it can only be replayed for schedules the fuzzer saved.
