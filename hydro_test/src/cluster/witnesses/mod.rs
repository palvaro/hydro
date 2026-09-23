//! The witness corpus: small Hydro programs labeled hazardous (some schedule makes the same input
//! cost more work, and more the longer delivery is delayed) or benign (work is bounded by the
//! input under every schedule), each with a stress-test harness that confirms its label. See
//! `design_docs/2026-09_witness_corpus_spec.md`.
//!
//! # Corpus
//!
//! A configuration is labeled **hazardous** when the schedule alone can make the program do work
//! its input did not require, **hazardous, mitigated** when that is so but a parameter keeps the
//! program from collapsing under the load trigger, and **benign** when its work is bounded by a
//! function of the input under every schedule. Every label rests on a test named in the last
//! column; the headline number is taken from that module's own doc table. `rpc_retry` lives one
//! directory up because it is also the deployed ground truth.
//!
//! | file | configuration | label | mechanism | headline number | confirming test |
//! |---|---|---|---|---|---|
//! | `super::rpc_retry` | `max_attempts = 3` | hazardous | timeout resends make the server serve requests again | tail completions 0 of 400, backlog 1038 -> 1237 | `rpc_retry::sim_tests::retries_cause_metastable_collapse` |
//! | `super::rpc_retry` | `max_attempts = 1` | benign | no resend, one serve per request | backlog peaks at 420, drains to 0 by round 300 | `rpc_retry::sim_tests` (`max_attempts = 1` control) |
//! | `backoff_retry` | `backoff = true` | hazardous, mitigated | timeout resends, spaced geometrically | 1044 re-sends for 2600 requests; 2 / 1044 / 3524 re-sends for 30 / 60 / 90-round holds | `redundant_work_grows_with_the_hold` |
//! | `backoff_retry` | `backoff = false` | hazardous | same as `rpc_retry` | tail completions 0 of 400, backlog 1038 -> 1237 | `without_backoff_the_system_collapses` |
//! | `bounded_queue_rejection` | `max_backlog = Some(100)` | hazardous, mitigated | timeout resends; the bound caps the server's own delay | 511 of 512 fuzzed schedules re-send, worst 41 sends for 24 requests, mean re-sends 5.00 / 5.78 / 8.20 at reply delay 2 / 3 / 4 | `held_replies_make_the_server_redo_work_under_the_bound` |
//! | `bounded_queue_rejection` | `max_backlog = None` | hazardous | same as `rpc_retry` | tail completions 0 of 400, queue 1038 -> 1237 | `without_the_bound_the_system_collapses` |
//! | `cache_thundering_herd` | `coalesce = false, request_dated = true` | hazardous | every miss during a slow fetch is another fetch, and stale fills are discarded | origin offered 1600 fetches per 200 rounds against capacity 800, queue 2462 -> 3258 | `herd_with_request_dating_keeps_the_origin_saturated` |
//! | `cache_thundering_herd` | `coalesce = false, request_dated = false` | hazardous, mitigated | every miss during a slow fetch is another fetch; redundant fills refresh the TTL | 124 / 160 / 200 / 272 / 498 fetches for a 0 / 5 / 10 / 20 / 50-round hold of the origin clock | `fill_dated_herd_does_more_work_the_longer_fills_are_held` |
//! | `cache_thundering_herd` | `coalesce = true` | benign | one outstanding fetch per key | fetches equal distinct misses under 256 fuzzed schedules | `coalescing_bound_holds_across_schedules` |
//! | `compaction_falls_behind` | `compaction_reserve = 0` | hazardous | gets pay per uncompacted record and starve compaction | tail 205 ops served against 1000, backlog 3432 -> 4223 | `background_compaction_starves_and_reads_collapse` |
//! | `compaction_falls_behind` | `compaction_reserve = 8` | hazardous, mitigated | same coupling, reserve keeps the segment short | extra units 0 / 4 / 52 / 102 / 195 for holds of 0 / 4 / 8 / 16 / 32 rounds | `held_arrivals_cost_extra_units_growing_with_the_hold` |
//! | `crdt_gossip_load` | `g_set_gossip` unmodified | benign | full-state rebroadcast is idempotent | wire work exactly `(n-1)(u + n\|set\|)` per round, tail growth a constant 6 | `gossip_converges_every_round_and_work_tracks_state_size` |
//! | `election_stampede` | `timeout_spread_ticks = 0` | hazardous | a late heartbeat makes every follower start an election from the shared budget | 353 elections, 1412 vote requests, leaderless until round 579; 173 / 353 / 537 elections for 10 / 20 / 30-round holds | `wasted_work_grows_with_the_hold` |
//! | `election_stampede` | `timeout_spread_ticks = 3` | hazardous | same mechanism, staggered | 128 elections, settled by round 497 | `spread_timeouts_still_stampede_but_settle_sooner` |
//! | `gossip_resend` | `ack_timeout_ticks > 0` | hazardous | deltas are re-sent until acknowledged and re-sends consume merge budget | 13858 re-sends, inbox 8584 -> 11569, half of tail merges redundant | `resends_keep_the_inboxes_from_draining` |
//! | `gossip_resend` | `ack_timeout_ticks = 0` | benign | each delta sent once per peer | 0 re-sends, inbox peak 911 back to 10 by round 338 | `without_resends_the_inboxes_drain` |
//! | `lease_renewal_storm` | `resend_after_ticks = Some(4)` | hazardous | an unacknowledged renewal is re-sent every tick | tail acked 0 of 400, 2400 re-sends, backlog 5117 -> 6908; extra sends 0 / 0 / 2 / 30 / 2311 / 2334 for holds 0 / 2 / 4 / 8 / 16 / 32 | `resends_cause_a_renewal_storm_that_never_ends` |
//! | `lease_renewal_storm` | `resend_after_ticks = None` | benign | one renewal message per period | exactly 1600 sends for 1600 periods, 0 extra at every hold | `with_one_outstanding_renewal_the_system_recovers` |
//! | `pure_heartbeat_load` | `pure_heartbeat` unmodified | benign | one message per peer per timer element | 9180 messages for 3060 elements, unchanged by holds up to 64 rounds | `every_timer_element_costs_exactly_n_messages` |
//! | `rebalancing_ping_pong` | `cooldown_ticks = 0`, report period 4 | hazardous | stale length reports move the same task back and forth | 537 migrations and 126 bounced tasks against 195 and 0 with fresh reports; 195 / 196 / 391 / 537 / 366 / 620 migrations for report periods 1 / 2 / 3 / 4 / 6 / 8 | `migrations_grow_with_report_staleness_unless_cooled_down` |
//! | `rebalancing_ping_pong` | `cooldown_ticks = 8` | hazardous, mitigated | same mechanism, cooldown blocks the return trip | 246 migrations and 0 bounced at every report period | `with_a_cooldown_the_cluster_recovers` |
//! | `transitive_closure_load` | `productive_tc` unmodified | benign | recursion stops when novelty runs out | 5748 candidates and 4720 facts, every number hand-computed; no fuzzed schedule exceeded the bound | `the_totals_hold_across_schedules` |
//!
//! The whole corpus runs with `cargo test -p hydro_test --lib witnesses::` (45 tests, 92 s of
//! test time, about 5 minutes wall clock including the test build) followed by
//! `cargo test -p hydro_test --lib rpc_retry::sim_tests` (3 tests, 8 s).

pub mod backoff_retry;
pub mod bounded_queue_rejection;
pub mod cache_thundering_herd;
pub mod checker_experiments;
pub mod compaction_falls_behind;
pub mod crdt_gossip_load;
pub mod election_stampede;
pub mod gossip_resend;
pub mod lease_renewal_storm;
pub mod pure_heartbeat_load;
pub mod rebalancing_ping_pong;
pub mod transitive_closure_load;
