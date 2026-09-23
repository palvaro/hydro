//! Checker experiments: can schedule exploration alone, with no load trigger and no simulator
//! changes, recover the corpus label of `rpc_retry`?
//!
//! The question the checker must answer is whether there exists a schedule under which a fixed
//! input costs the program more work than the input requires, and more of it the longer delivery
//! is delayed. This module asks that question of [`super::super::rpc_retry`] using only the
//! existing `flow.sim().fuzz(..)` API: the fuzzer chooses, at every tick of every slice, how many
//! buffered items each `use::batch` releases. The harness offers the witness's baseline workload
//! (two requests per round against a capacity of five) and never a trigger. For every schedule
//! the fuzzer draws, the harness records the total work (client sends and server serves), the
//! number of distinct requests, and the longest reply delay any request saw, measured in client
//! clock ticks from first send to completion. Extra work is total work minus what the distinct
//! requests require, bucketed by that longest delay.
//!
//! # How a random schedule reaches a delay
//!
//! A reply can only be delayed relative to the client's logical clock while clock elements are
//! released and the reply is held. Under the round-based harness, a tick whose only loaded hook is
//! the replies hook is forced to release, so a reply cannot survive past the round's last clock
//! element. The harness therefore offers several clock elements per round, and a schedule that
//! releases them one at a time while holding a reply advances the client's clock past the timeout.
//! Because every release count is a uniform random draw, the chance that a reply survives `k`
//! consecutive clock releases falls geometrically in `k`, so the timeout has to be small for a
//! random search to reach it. The program is unchanged; timeout and clock density are harness
//! parameters, as they are for the bounded-queue hold experiment in
//! [`super::bounded_queue_rejection`].
//!
//! # Hand-computed expectation, written before measuring
//!
//! Twenty-four rounds, two requests per round (48 distinct requests), six clock elements per
//! round, timeout 2 ticks, capacity 5 per server tick, 512 schedules per configuration.
//!
//! - Under the prompt schedule, every reply is observed before the next clock element, latency is
//!   0, and the run costs 48 sends and 48 serves: extra work 0.
//! - With `max_attempts = 3`, a reply held while two clock elements are released makes the client
//!   re-send that id and the server, whose queue is otherwise empty, serve it a second time; held
//!   while four are released, a third time. So every schedule whose longest delay is below 2 has
//!   extra work exactly 0; schedules whose longest delay is 2 or 3 have positive mean extra work;
//!   schedules whose longest delay is 4 or more have a higher mean than those, and no request is
//!   sent more than three times, so extra sends never exceed 96. The curve rises with delay and
//!   plateaus at the attempt cap.
//! - With `max_attempts = 1`, the client never re-sends, so extra work is exactly 0 in every
//!   delay bucket, although the delay buckets themselves are populated the same way.
//!
//! The relation is statistical rather than exact because a clock jump and the reply can land in
//! the same client tick, which gives a long latency with no re-send.
//!
//! # Measured
//!
//! 512 schedules per configuration, about 10 s of wall clock per 512 once the staged crate is
//! built (the first configuration in a process takes about 25 s). Extra work is sends minus the 48
//! distinct requests; extra serves were equal to extra sends in every schedule, so the server redid
//! exactly the work the client re-sent.
//!
//! | configuration | schedules with extra work | extra sends by longest observed delay (mean over schedules; count) | mean / max extra sends |
//! |---|---|---|---|
//! | `max_attempts = 3`, timeout 2 | 512 of 512 | delay 4: 25.3 (7); delay 5: 23.1 (75); delay 6: 25.3 (430) | 24.96 / 55 |
//! | `max_attempts = 1`, timeout 2 | 0 of 512 | delay 1: 0 (8); 2: 0 (45); 3: 0 (110); 4: 0 (128); 5: 0 (140); 6: 0 (81) | 0 / 0 |
//! | `max_attempts = 3`, timeout 4 | 512 of 512 | delay 4: 4.8 (5); delay 5: 5.8 (69); delay 6: 8.1 (438) | 7.73 / 22 |
//! | `max_attempts = 3`, timeout 6 | 416 of 512 | delay 4: 0 (7); delay 5: 0 (45); delay 6: 2.5 (460) | 2.28 / 9 |
//!
//! Both halves of the expectation held: with three attempts every schedule cost more than the
//! input, with one attempt none did, and the delays a schedule imposed were the same either way
//! (1 to 6 ticks). One thing the expectation got wrong: with retries on, no schedule had a longest
//! delay below 4, so the low buckets of the timeout-2 run are empty and its curve is flat at the
//! attempt cap. The re-sends themselves lengthen the schedule: each re-send produces another reply,
//! which keeps the client tick runnable for more sub-ticks in the round, each of which can release
//! another clock element while a reply is held. The rising part of the curve shows once the
//! timeout is raised so that only the longest delays reach it: at timeout 4 extra sends rise from
//! 4.8 to 8.1 across the delay buckets and no request is sent more than twice; at timeout 6 only
//! the delay-6 schedules re-send. Across the three timeouts the mean falls from 25.1 to 7.7 to 2.3,
//! which is the same curve read the other way: extra work rises with the delay a schedule can impose
//! relative to the timeout.
//!
//! # What this does and does not give the checker
//!
//! Random schedule exploration recovers the label of `rpc_retry` at one attempt and three
//! attempts without a load trigger and without touching the simulator, at a cost of about 10 s per
//! 512 schedules. It does so only because the timeout was set small enough for a random draw to
//! reach it; at the witness's own timeout of 40 the same search would need a reply to survive 40
//! consecutive single-element clock releases, which a uniform draw will not produce. A checker that
//! uses the fuzzer as its search needs a way to bias or direct the draws toward holding one edge,
//! or a harness that meters time so that a hold of a chosen length is a single decision.
//!
//! On localization: the simulator's per-tick trace (`run_with_scheduler_and_logger`) prints, for
//! every decision, the `use::batch` source location and the items released, and `fuzz_repro` replays
//! a saved schedule with that trace. So for any one schedule the information to say which batch
//! decision held the reply is available. Inside `fuzz`, however, the bytes that identify the
//! current schedule are not exposed to the harness unless the run fails, so a passing schedule
//! that showed extra work cannot be replayed with the trace on. Making the fuzzer save the
//! schedules the harness flags would close that gap.

#[cfg(test)]
mod sim_tests {
    use std::collections::{BTreeMap, HashSet};
    use std::time::{Duration, Instant};

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use crate::cluster::rpc_retry::{
        Client, RetryPolicy, Server, ServerConfig, rpc_with_retries,
    };

    const ROUNDS: usize = 24;
    const REQUESTS_PER_ROUND: usize = 2;
    const CLOCKS_PER_ROUND: usize = 6;
    const SCHEDULES: usize = 512;
    const TIMEOUT_TICKS: u64 = 2;

    /// One fuzzed schedule's outcome.
    #[derive(Debug, Clone, Copy)]
    struct RunOutcome {
        longest_delay: u64,
        distinct: u64,
        sends: u64,
        serves: u64,
    }

    // Free functions rather than methods: `impl` blocks inside this module are dropped when the
    // module is staged into the trybuild crate.
    fn extra_sends(r: &RunOutcome) -> u64 {
        r.sends - r.distinct
    }
    fn extra_serves(r: &RunOutcome) -> u64 {
        r.serves - r.distinct
    }

    /// Runs `SCHEDULES` fuzzed schedules of the baseline workload and returns one outcome per
    /// schedule plus the wall-clock time.
    fn fuzz_baseline(max_attempts: u32, timeout_ticks: u64) -> (Vec<RunOutcome>, Duration) {
        let policy = RetryPolicy {
            timeout_ticks,
            max_attempts,
        };
        let server_config = ServerConfig {
            max_per_tick: 5,
            service_time: Duration::ZERO,
        };

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        // Metrics windows are a deployment concern; these never fire.
        let (_client_report_send, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report_send, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            policy,
            server_config,
        );
        let completed = outputs
            .completed
            .map(q!(|c| (c.id, c.latency_ticks)))
            .sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();

        let mut runs: Vec<RunOutcome> = Vec::new();
        let runs_ref = &mut runs;
        let started = Instant::now();

        flow.sim()
            .unit_test_fuzz_iterations(SCHEDULES)
            .fuzz(async || {
                let mut sent_ids: HashSet<u64> = HashSet::new();
                let mut sends = 0u64;
                let mut serves = 0u64;
                let mut longest_delay = 0u64;
                for tick in 0..ROUNDS as u64 {
                    for _ in 0..CLOCKS_PER_ROUND {
                        clock_send.send(());
                    }
                    for _ in 0..REQUESTS_PER_ROUND {
                        request_send.send(tick);
                    }
                    quiesce().await;
                    for (_, latency) in completed.collect_sorted::<Vec<_>>().await {
                        longest_delay = longest_delay.max(latency);
                    }
                    while let Some(req) = outgoing.try_next().await {
                        sent_ids.insert(req.id);
                        sends += 1;
                    }
                    while processed.try_next().await.is_some() {
                        serves += 1;
                    }
                }
                runs_ref.push(RunOutcome {
                    longest_delay,
                    distinct: sent_ids.len() as u64,
                    sends,
                    serves,
                });
            });

        (runs, started.elapsed())
    }

    /// Mean extra sends over the schedules whose longest delay lies in `[lo, hi)`, with the count.
    fn bucket(runs: &[RunOutcome], lo: u64, hi: u64) -> (usize, f64) {
        let sel: Vec<&RunOutcome> = runs
            .iter()
            .filter(|r| r.longest_delay >= lo && r.longest_delay < hi)
            .collect();
        let mean = sel.iter().map(|r| extra_sends(r)).sum::<u64>() as f64 / sel.len().max(1) as f64;
        (sel.len(), mean)
    }

    fn report(label: &str, runs: &[RunOutcome], elapsed: Duration) {
        report_with_timeout(label, runs, elapsed, TIMEOUT_TICKS)
    }

    fn report_with_timeout(label: &str, runs: &[RunOutcome], elapsed: Duration, timeout: u64) {
        let with_extra = runs.iter().filter(|r| extra_sends(r) > 0).count();
        let max_sends = runs.iter().map(|r| extra_sends(r)).max().unwrap_or(0);
        let max_serves = runs.iter().map(|r| extra_serves(r)).max().unwrap_or(0);
        println!(
            "[{label}] {} schedules in {:.1} s; {} with extra work; max extra sends {max_sends}, max extra serves {max_serves}",
            runs.len(),
            elapsed.as_secs_f64(),
            with_extra
        );
        let mut by_delay: BTreeMap<u64, (usize, u64, u64)> = BTreeMap::new();
        for r in runs {
            let e = by_delay.entry(r.longest_delay).or_default();
            e.0 += 1;
            e.1 += extra_sends(r);
            e.2 += extra_serves(r);
        }
        for (delay, (n, sends, serves)) in &by_delay {
            println!(
                "[{label}] longest delay {delay}: {n} schedules, mean extra sends {:.2}, mean extra serves {:.2}",
                *sends as f64 / *n as f64,
                *serves as f64 / *n as f64
            );
        }
        let (n0, m0) = bucket(runs, 0, timeout);
        let (n1, m1) = bucket(runs, timeout, 2 * timeout);
        let (n2, m2) = bucket(runs, 2 * timeout, u64::MAX);
        println!(
            "[{label}] buckets: below timeout {n0} schedules mean {m0:.2}; one to two timeouts {n1} schedules mean {m1:.2}; two timeouts or more {n2} schedules mean {m2:.2}"
        );
    }

    /// With three attempts, extra work appears only when some reply was delayed past the timeout
    /// and grows with the longest delay a schedule imposed.
    #[test]
    fn fuzzed_schedules_recover_the_hazardous_label_of_rpc_retry() {
        let (runs, elapsed) = fuzz_baseline(3, TIMEOUT_TICKS);
        report("max_attempts = 3", &runs, elapsed);

        assert!(runs.iter().all(|r| r.distinct == (ROUNDS * REQUESTS_PER_ROUND) as u64));
        // No delay, no redundant work: the mechanism is delay-driven.
        assert!(
            runs.iter()
                .filter(|r| r.longest_delay < TIMEOUT_TICKS)
                .all(|r| extra_sends(r) == 0 && extra_serves(r) == 0),
            "a schedule that never delayed a reply past the timeout should not have re-sent or re-served"
        );
        // Some schedule makes the same 48 requests cost more than 48 sends and 48 serves.
        assert!(runs.iter().any(|r| extra_sends(r) > 0), "the fuzzer should find a schedule that re-sends");
        assert!(runs.iter().any(|r| extra_serves(r) > 0), "a re-send should make the server serve an id again");
        // Never more than the attempt cap.
        assert!(runs.iter().all(|r| extra_sends(r) <= 2 * r.distinct));
        // Extra work grows with delay.
        let (n1, m1) = bucket(&runs, TIMEOUT_TICKS, 2 * TIMEOUT_TICKS);
        let (n2, m2) = bucket(&runs, 2 * TIMEOUT_TICKS, u64::MAX);
        if n1 > 0 && n2 > 0 {
            assert!(m2 >= m1, "extra sends should grow with the delay the schedule imposed");
        }
    }

    /// With one attempt, the same schedules produce the same delays and no extra work at all.
    #[test]
    fn fuzzed_schedules_find_no_extra_work_without_retries() {
        let (runs, elapsed) = fuzz_baseline(1, TIMEOUT_TICKS);
        report("max_attempts = 1", &runs, elapsed);

        assert!(runs.iter().all(|r| r.distinct == (ROUNDS * REQUESTS_PER_ROUND) as u64));
        assert!(
            runs.iter().all(|r| extra_sends(r) == 0 && extra_serves(r) == 0),
            "without retries no schedule can add work"
        );
        // The delays are still there; only the response to them is gone.
        assert!(
            runs.iter().any(|r| r.longest_delay >= TIMEOUT_TICKS),
            "the fuzzer should still find schedules that delay a reply past the timeout"
        );
    }

    /// The delay a schedule can impose is bounded by the clock density (six elements per round),
    /// so raising the timeout leaves fewer schedules able to reach it, and past half the clock
    /// density no schedule can reach it twice. Expected before measuring: mean extra sends fall
    /// from timeout 2 to timeout 4 to timeout 6, and at timeout 4 and 6 no request is sent more
    /// than twice (extra sends at most 48).
    #[test]
    fn extra_work_falls_as_the_timeout_rises_past_the_reachable_delay() {
        let mut means = Vec::new();
        for timeout in [2u64, 4, 6] {
            let (runs, elapsed) = fuzz_baseline(3, timeout);
            report_with_timeout(&format!("max_attempts = 3, timeout = {timeout}"), &runs, elapsed, timeout);
            let mean = runs.iter().map(extra_sends).sum::<u64>() as f64 / runs.len() as f64;
            let max = runs.iter().map(extra_sends).max().unwrap_or(0);
            println!("[timeout = {timeout}] mean extra sends {mean:.2}, max {max}");
            if timeout >= 4 {
                assert!(max <= (ROUNDS * REQUESTS_PER_ROUND) as u64, "a delay of at most 6 can trigger at most one re-send when the timeout is 4 or more");
            }
            means.push(mean);
        }
        assert!(means[0] > means[1] && means[1] > means[2], "extra work should fall as the timeout rises: {means:?}");
    }
}
