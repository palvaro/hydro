//! The witness corpus: small Hydro programs labeled hazardous (some schedule makes the same input
//! cost more work, and more the longer delivery is delayed) or benign (work is bounded by the
//! input under every schedule), each with a stress-test harness that confirms its label. See
//! `design_docs/2026-09_witness_corpus_spec.md`.
//!
//! # Corpus
//!
//! Every label carries its basis, and there are two bases for ground truth. A configuration is
//! **hazardous, by exhibited collapse** when a test drove the program into collapse, which is a
//! constructive proof that the program can be driven there. It is **benign, by assurance
//! argument** when no collapse was found and there is a written argument that work is a function
//! of the input alone under every schedule; the argument is either a closed form for the work
//! that held under hundreds of random schedules, or the structural absence of any operator that
//! can emit a second send. Failing to collapse a program is not evidence that it is robust, so an
//! assurance argument is the strongest basis available for a negative label, and the owner
//! accepts it as such.
//!
//! Five configurations have **no ground truth**. Each differs from an exhibited-collapse
//! configuration only in a tuning parameter (backoff, a queue bound, a compaction reserve, a
//! cooldown), no test collapsed it, and no assurance argument exists for it. For these the tool
//! is expected to find amplification, because the mechanism of the collapsing configuration is
//! present and a tuning parameter can only move the threshold at which it runs away, not remove
//! it; whether the parameter suffices depends on a workload unknown when the program is written.
//! Agreement with an expectation is recorded separately from agreement with ground truth.
//!
//! Every row rests on a test named in the last column; the headline number is taken from that
//! module's own doc table. `rpc_retry` lives one directory up because it is also the deployed
//! ground truth.
//!
//! | file | configuration | label and basis | mechanism | headline number | confirming test |
//! |---|---|---|---|---|---|
//! | `super::rpc_retry` | `max_attempts = 3` | hazardous, by exhibited collapse | timeout resends make the server serve requests again | tail completions 0 of 400, backlog 1038 -> 1237 | `rpc_retry::sim_tests::retries_cause_metastable_collapse` |
//! | `super::rpc_retry` | `max_attempts = 1` | benign, by assurance argument: no operator emits a second send, so the server serves each id once | no resend, one serve per request | backlog peaks at 420, drains to 0 by round 300 | `rpc_retry::sim_tests` (`max_attempts = 1` control) |
//! | `backoff_retry` | `backoff = true` | no ground truth; tool expected to find amplification | timeout resends, spaced geometrically | 1044 re-sends for 2600 requests; 2 / 1044 / 3524 re-sends for 30 / 60 / 90-round holds | `redundant_work_grows_with_the_hold` |
//! | `backoff_retry` | `backoff = false` | hazardous, by exhibited collapse | same as `rpc_retry` | tail completions 0 of 400, backlog 1038 -> 1237 | `without_backoff_the_system_collapses` |
//! | `bounded_queue_rejection` | `max_backlog = Some(100)` | no ground truth; tool expected to find amplification | timeout resends; the bound caps the server's own delay | 511 of 512 fuzzed schedules re-send, worst 41 sends for 24 requests, mean re-sends 5.00 / 5.78 / 8.20 at reply delay 2 / 3 / 4 | `held_replies_make_the_server_redo_work_under_the_bound` |
//! | `bounded_queue_rejection` | `max_backlog = None` | hazardous, by exhibited collapse | same as `rpc_retry` | tail completions 0 of 400, queue 1038 -> 1237 | `without_the_bound_the_system_collapses` |
//! | `cache_thundering_herd` | `coalesce = false, request_dated = true` | hazardous, by exhibited collapse | every miss during a slow fetch is another fetch, and stale fills are discarded | origin offered 1600 fetches per 200 rounds against capacity 800, queue 2462 -> 3258 | `herd_with_request_dating_keeps_the_origin_saturated` |
//! | `cache_thundering_herd` | `coalesce = false, request_dated = false` | no ground truth; tool expected to find amplification | every miss during a slow fetch is another fetch; redundant fills refresh the TTL | 124 / 160 / 200 / 272 / 498 fetches for a 0 / 5 / 10 / 20 / 50-round hold of the origin clock | `fill_dated_herd_does_more_work_the_longer_fills_are_held` |
//! | `cache_thundering_herd` | `coalesce = true` | benign, by assurance argument: the outstanding set admits one fetch per missing key, so fetches equal distinct misses; held under 256 random schedules | one outstanding fetch per key | fetches equal distinct misses under 256 fuzzed schedules | `coalescing_bound_holds_across_schedules` |
//! | `compaction_falls_behind` | `compaction_reserve = 0` | hazardous, by exhibited collapse | gets pay per uncompacted record and starve compaction | tail 205 ops served against 1000, backlog 3432 -> 4223 | `background_compaction_starves_and_reads_collapse` |
//! | `compaction_falls_behind` | `compaction_reserve = 8` | no ground truth; tool expected to find amplification | same coupling, reserve keeps the segment short | extra units 0 / 4 / 52 / 102 / 195 for holds of 0 / 4 / 8 / 16 / 32 rounds | `held_arrivals_cost_extra_units_growing_with_the_hold` |
//! | `crdt_gossip_load` | `g_set_gossip` unmodified | benign, by assurance argument: wire work per round is the closed form `(n-1)(u + n\|set\|)` and merges are `3\|set\| - 2`, both fixed by the input; held under 256 random schedules | full-state rebroadcast is idempotent | wire work exactly `(n-1)(u + n\|set\|)` per round, tail growth a constant 6 | `gossip_converges_every_round_and_work_tracks_state_size` |
//! | `election_stampede` | `timeout_spread_ticks = 0` | hazardous, by exhibited collapse | a late heartbeat makes every follower start an election from the shared budget | 353 elections, 1412 vote requests, leaderless until round 579; 173 / 353 / 537 elections for 10 / 20 / 30-round holds | `wasted_work_grows_with_the_hold` |
//! | `election_stampede` | `timeout_spread_ticks = 3` | hazardous, by exhibited collapse | same mechanism, staggered | 128 elections, settled by round 497 | `spread_timeouts_still_stampede_but_settle_sooner` |
//! | `gossip_resend` | `ack_timeout_ticks > 0` | hazardous, by exhibited collapse | deltas are re-sent until acknowledged and re-sends consume merge budget | 13858 re-sends, inbox 8584 -> 11569, half of tail merges redundant | `resends_keep_the_inboxes_from_draining` |
//! | `gossip_resend` | `ack_timeout_ticks = 0` | benign, by assurance argument: the re-send operator never fires, so each delta is sent once per peer and merged once | each delta sent once per peer | 0 re-sends, inbox peak 911 back to 10 by round 338 | `without_resends_the_inboxes_drain` |
//! | `lease_renewal_storm` | `resend_after_ticks = Some(4)` | hazardous, by exhibited collapse | an unacknowledged renewal is re-sent every tick | tail acked 0 of 400, 2400 re-sends, backlog 5117 -> 6908; extra sends 0 / 0 / 2 / 30 / 2311 / 2334 for holds 0 / 2 / 4 / 8 / 16 / 32 | `resends_cause_a_renewal_storm_that_never_ends` |
//! | `lease_renewal_storm` | `resend_after_ticks = None` | benign, by assurance argument: the re-send operator never fires, so sends equal renewal periods exactly | one renewal message per period | exactly 1600 sends for 1600 periods, 0 extra at every hold | `with_one_outstanding_renewal_the_system_recovers` |
//! | `pure_heartbeat_load` | `pure_heartbeat` unmodified | benign, by assurance argument: work is exactly `n` messages per timer element, with no state, acknowledgement or retry on the path; held under 256 random schedules | one message per peer per timer element | 9180 messages for 3060 elements, unchanged by holds up to 64 rounds | `every_timer_element_costs_exactly_n_messages` |
//! | `rebalancing_ping_pong` | `cooldown_ticks = 0`, report period 4 | hazardous, by exhibited collapse | stale length reports move the same task back and forth | 537 migrations and 126 bounced tasks against 195 and 0 with fresh reports; 195 / 196 / 391 / 537 / 366 / 620 migrations for report periods 1 / 2 / 3 / 4 / 6 / 8 | `migrations_grow_with_report_staleness_unless_cooled_down` |
//! | `rebalancing_ping_pong` | `cooldown_ticks = 8` | no ground truth; tool expected to find amplification | same mechanism, cooldown blocks the return trip | 246 migrations and 0 bounced at every report period | `with_a_cooldown_the_cluster_recovers` |
//! | `transitive_closure_load` | `productive_tc` unmodified | benign, by assurance argument: every candidate is a path in the admitted graph, so candidates are bounded by the closure; held under 256 random schedules | recursion stops when novelty runs out | 5748 candidates and 4720 facts, every number hand-computed; no fuzzed schedule exceeded the bound | `the_totals_hold_across_schedules` |
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
