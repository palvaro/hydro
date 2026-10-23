# Corpus parity: metastable-3 vs the port

Port: `./scripts/cress hydro_test` on this branch (sequential, debug build). The run that produced the `port` columns below took 46 minutes of checks after the runner build, on 2026-10-09; a rerun after the fix pass of 2026-10-09 (atomic-context decision points, `NoOrder` and `KeyedStream` inputs, the all-senders messages row) took 66 minutes on a busier machine and reproduced every one of these 34 rows figure for figure (verdict, location, first reaction, extra work, growth, multiplier; `design_docs/cress_port/sweep_results.tsv` is that rerun), and added two benign controls that are not corpus rows: `two_pc (three_participants)` from `hydro_test/src/cluster/two_pc.rs` and `keyed_counter_with_unordered_gets` from `hydro_test/src/cluster/cress_inputs_check.rs`. An earlier run on the pre-review tree gave the same figures with every line number one lower (the `#[cfg(feature = "cress")]` line above each `use cress::...` import shifted them). m3 reference: verdict, location and first reaction from the sweep table in `design_docs/2026-09_amplification_checker_overview.md` (captured before commit dc4ea76756 added `workload_rounds = 125` and shifted line numbers; the shift per file is noted), shape and multiplier from `design_docs/2026-09_growth_label.md` section 3c (data confined, as the ported attributes are), and for `retry_service` from section 3b plus m3's own CLI reports. The m3 sweep's extra-work column is not comparable (data in every round) and is omitted.

| function | configuration | verdict m3 / port | location m3 / port | first reaction m3 / port | shape m3 / port | multiplier m3 / port | port s |
|---|---|---|---|---|---|---|---|
| bounded_fanout | default | benign / benign | - | - / - | - / - | - / - | 26.0 |
| cache_with_expiry | coalesce | benign / benign | - | - / - | - / - | - / - | 11.2 |
| cache_with_expiry | fill_dated_no_coalesce | hazardous / hazardous | `cache_thundering_herd.rs:340, batch of ()` / `cache_thundering_herd.rs:350, batch of ()` (m3 line +10 = 350) | 1 / 1 | stops / stops | 13.51x / 13.51x | 11.9 |
| cache_with_expiry | request_dated_no_coalesce | hazardous / hazardous | `cache_thundering_herd.rs:340, batch of ()` / `cache_thundering_herd.rs:350, batch of ()` (m3 line +10 = 350) | 1 / 1 | stops / stops | 10.20x / 10.20x | 12.0 |
| credit_flow | default | benign / benign | - | - / - | - / - | - / - | 12.9 |
| election | spread_3 | hazardous / hazardous | `election_stampede.rs:239, batch of (usize, ())` / `election_stampede.rs:244, batch of (usize, ())` (m3 line +5 = 244) | 8 / 8 | grows / grows | from 0 / from 0 | 23.4 |
| election | uniform_timeouts | hazardous / hazardous | `election_stampede.rs:239, batch of (usize, ())` / `election_stampede.rs:244, batch of (usize, ())` (m3 line +5 = 244) | 8 / 8 | grows, slowing / grows, slowing | from 0 / from 0 | 24.5 |
| gossip_with_resend | resend_off | benign / benign | - | - / - | - / - | - / - | 61.1 |
| gossip_with_resend | resend_on | hazardous / hazardous | `gossip_resend.rs:134, batch of (usize, ())` / `gossip_resend.rs:139, batch of (usize, ())` (m3 line +5 = 139) | 2 / 2 | grows, degree 2 / grows, degree 2 | 8.97x / 8.97x | 111.7 |
| lease_renewal | one_outstanding | benign / benign | - | - / - | - / - | - / - | 223.9 |
| lease_renewal | resend_after_4 | hazardous / hazardous | `lease_renewal_storm.rs:203, batch of Ack` / `lease_renewal_storm.rs:288, batch of ()` (m3 line +4 = 207) | 4 / 4 | not read / not read | 7.00x / 7.00x | 1033.4 |
| log_with_compaction | reserve_0 | benign / benign | - | - / - | - / - | - / - | 7.4 |
| log_with_compaction | reserve_8 | benign / benign | - | - / - | - / - | - / - | 5.4 |
| productive_transitive_closure | default | benign / benign | - | - / - | - / - | - / - | 8.7 |
| pure_heartbeat | default | benign / **not explored** | - | - / - | - / - | - / - | 3.1 |
| rebalancing_workers | cooldown_8 | hazardous / hazardous | `rebalancing_ping_pong.rs:198, batch of u64` / `rebalancing_ping_pong.rs:205, batch of u64` (m3 line +7 = 205) | 8 / 8 | stops / stops | 1.55x / 1.55x | 18.1 |
| rebalancing_workers | no_cooldown | hazardous / hazardous | `rebalancing_ping_pong.rs:198, batch of u64` / `rebalancing_ping_pong.rs:205, batch of u64` (m3 line +7 = 205) | 8 / 8 | stops / stops | 3.35x / 3.35x | 18.4 |
| replicated_log_catchup | default | hazardous / hazardous | `log_catchup.rs:226, batch of ()` / `log_catchup.rs:228, batch of ()` (m3 line +2 = 228) | 8 / 8 | grows, slowing / grows, slowing | 39.97x / 39.97x | 19.2 |
| retry_service | circuit_breaker | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | stops / stops | 1.12x / 1.12x | 11.3 |
| retry_service | hybrid_budget | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | stops / stops | 1.56x / 1.56x | 11.4 |
| retry_service | no_governance | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | stops / stops | 2.98x / 2.98x | 11.6 |
| retry_service | no_retries | benign / benign | - | - / - | - / - | - / - | 11.2 |
| retry_service | success_budget | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | stops / stops | 1.12x (CLI) / 1.14x (test) / 1.12x | 11.4 |
| retry_service | time_budget | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | stops / stops | 1.56x / 1.56x | 11.6 |
| retry_service | unbounded | hazardous / hazardous | `service.rs:539, batch of Request` / `service.rs:540, batch of Request` | 8 / 8 | grows / grows | 155x / 155.42x | 44.0 |
| rpc_with_backoff | backoff_off | hazardous / hazardous | `backoff_retry.rs:279, batch of Request<u64>` / `backoff_retry.rs:282, batch of Request<u64>` (m3 line +3 = 282) | 64 / 64 | stops / stops | 2.98x / 2.98x | 8.4 |
| rpc_with_backoff | backoff_on | hazardous / hazardous | `backoff_retry.rs:279, batch of Request<u64>` / `backoff_retry.rs:282, batch of Request<u64>` (m3 line +3 = 282) | 64 / 64 | grows, slowing / grows, slowing | 2.98x / 2.98x | 8.9 |
| rpc_with_bounded_queue | bounded_100 | hazardous / hazardous | `bounded_queue_rejection.rs:322, batch of Request` / `bounded_queue_rejection.rs:325, batch of Request` (m3 line +3 = 325) | 64 / 64 | stops / stops | 2.98x / 2.98x | 9.3 |
| rpc_with_bounded_queue | unbounded | hazardous / hazardous | `bounded_queue_rejection.rs:322, batch of Request` / `bounded_queue_rejection.rs:325, batch of Request` (m3 line +3 = 325) | 64 / 64 | stops / stops | 2.98x / 2.98x | 9.5 |
| rpc_with_retries | one_attempt | benign / benign | - | - / - | - / - | - / - | 30.5 |
| rpc_with_retries | three_attempts | hazardous / hazardous | `rpc_retry.rs:315, batch of Request<u64>` / `rpc_retry.rs:325, batch of Request<u64>` (same `use::batch(incoming, ...)`; this file also gained the `cfg_attr` gating and a clippy `expect`, so the shift is not a constant; `rpc_retry_check` locates the line from the source) | 64 / 64 | stops / stops | 2.98x / 2.98x | 30.1 |
| token_bucket | default | benign / benign | - | - / - | - / - | - / - | 5.0 |
| two_phase_commit | default | hazardous / hazardous | `two_phase_commit.rs:247, batch of ()` / `two_phase_commit.rs:249, batch of ()` (m3 line +2 = 249) | 4 / 4 | grows / grows | 116x / 116.44x | 79.1 |
| visibility_queue | default | hazardous / hazardous | `visibility_queue.rs:207, batch of Delivery` / `visibility_queue.rs:209, batch of Delivery` (m3 line +2 = 209) | 8 / 8 | grows / grows | 155x / 155.42x | 46.0 |

## The one verdict that differs

`pure_heartbeat`: m3 read benign; the port reads *not explored*. The program has no `use::batch` or `use::snapshot` that ever has input (its heartbeats go straight to the network, 9000 messages in the unheld run, 0 records admitted), so the checker had nothing to hold and ran no schedule other than the unheld one. The port's `Verdict::NotExplored` exists for exactly this case, so that a run that explored nothing is never printed as a pass; m3 had no such verdict and called it benign. Everything else about the row is the same (no decision point, no curve).

## The one location that differs

`lease_renewal resend_after_4`: m3 named the clients' `Ack` batch (`:207` after the shift); the port names the server's clock batch (`:288`). Both are edges of the same renewal loop and both first react at a hold of 4 rounds with 4 extra records, so the location rule falls through to the largest extra work over the curve: 14906 for `Ack` (the port's `Ack` curve is m3's exactly, 14906 records and 2984 sends) against 14919 for the clock batch, which in m3 was at most 14906. The 13-record difference (0.09%) comes from how the server tick interleaves with the twenty client ticks within a round: the port's schedule runs ready ticks in a fixed (location, member) order, one tick per settle step, where m3's driver followed the simulator's own ready order. Verdict, first reaction, shape (not read) and multiplier (7.00x) agree.

## Other notes

- The m3 overview rows were run with data in every round; the port's attributes (from the m3 working tree) confine data to rounds 0..125, which is why the m3 extra-work column is not reproduced here and why `two_phase_commit` and `visibility_queue` run in 79 s and 44 s rather than the minutes the all-rounds `blind_check` tests take (those reproduce m3's all-rounds curves figure for figure: see PORT_NOTES.md).
- `retry_service success_budget`: m3's hand-wired `blind_budget_growth` test reads 1.14x and m3's own CLI report read 1.12x (the attribute harness also feeds the server clock the test leaves to the program); the port reproduces both readings in the corresponding place.
- `election` and `gossip_with_resend` rows matched only after the keyed-stream send-counting fix recorded in PORT_NOTES.md; before it they ran with zero messages counted.
- No corpus program has a snapshot the checker can hold (the two `use::snapshot` calls are an `Optional`, reported as not holdable, and the `crdt_gossip` program that upstream's simulator cannot run), so the review-driven change to snapshot holds (reveal the newest version; reveal a never-revealed snapshot rather than abort) cannot move any row here. It is pinned by the unit tests in `cress/src/tests.rs`.
