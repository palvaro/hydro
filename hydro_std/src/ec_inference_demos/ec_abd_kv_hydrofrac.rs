//! The keyed ABD register ([`abd_kv_register`]) as a blind target for hydrofrac's `EcCheck`:
//! three replicas, one client, and the client issues two writes to different keys at once,
//! with no `quiesce()` between them.
//!
//! [`abd_kv_register`]'s documented contract allows at most one outstanding operation per
//! client member; this harness deliberately breaks it, to see what each checker (and plain
//! failure-free schedule exploration) reports.
//!
//! All tests here are `#[ignore]`d measurements: run them with `--ignored --nocapture`.

#[cfg(stageleft_runtime)]
use std::sync::atomic::{AtomicUsize, Ordering};

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;

use super::abd::{Ts, abd_kv_register};

pub struct Replica;
pub struct Client;

#[cfg(stageleft_runtime)]
const REPLICAS: usize = 3;

/// The two requests: `(request_id, (key, value))`, sent back to back.
#[cfg(stageleft_runtime)]
const REQUESTS: [(u64, (u32, u32)); 2] = [(1, (1, 10)), (2, (2, 20))];

/// Builds the flow and runs `f` with the compiled-sim builder and the write sender / output
/// receiver.
#[cfg(stageleft_runtime)]
macro_rules! build {
    ($flow:ident, $replicas:ident, $clients:ident, $w_send:ident, $done:ident) => {
        let mut $flow = FlowBuilder::new();
        let $replicas = $flow.cluster::<Replica>();
        let $clients = $flow.cluster::<Client>();
        let ($w_send, writes) = $clients.sim_input::<(u64, (u32, u32)), TotalOrder, ExactlyOnce>();
        let (_r_send, reads) = $clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let outs = abd_kv_register(&$replicas, REPLICAS / 2 + 1, writes, reads);
        let $done = outs.write_done.sim_cluster_output();
        let _read = outs.read_result.sim_cluster_output();
    };
}

#[cfg(stageleft_runtime)]
fn ec_check(mut check: hydrofrac::EcCheck, mode: &str) -> hydrofrac::Results {
    build!(flow, replicas, clients, w_send, _done);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&replicas, REPLICAS)
                .with_cluster_size(&clients, 1),
        )
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        for r in REQUESTS {
            w_send.send(0, r);
        }
    });
    let schedules: usize = results.results().iter().map(|l| l.stats.schedules).sum();
    eprintln!(
        "ABD-KV, two concurrent writes, EcCheck::{mode}: {:.1}s, {schedules} schedules summed over labels\n{results}",
        start.elapsed().as_secs_f64()
    );
    results
}

/// `EcCheck::new()` (lite) with sampled schedules.
/// `EcCheck::new()`, exhaustive per hypothesis unless `ABD_KV_SAMPLES` is set.
#[cfg(stageleft_runtime)]
#[test]
#[ignore = "measurement"]
fn abd_kv_two_writes() {
    let mut check = hydrofrac::EcCheck::new();
    if let Some(n) = std::env::var("ABD_KV_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        check = check.sample_schedules(n);
    }
    if let Some(n) = std::env::var("ABD_KV_MAX_CRASHES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        check = check.max_crashes(n);
    }
    ec_check(check, "new()");
}

#[cfg(stageleft_runtime)]
static RUNS: AtomicUsize = AtomicUsize::new(0);
#[cfg(stageleft_runtime)]
static INCOMPLETE: AtomicUsize = AtomicUsize::new(0);
#[cfg(stageleft_runtime)]
static MISSING_1: AtomicUsize = AtomicUsize::new(0);
#[cfg(stageleft_runtime)]
static MISSING_2: AtomicUsize = AtomicUsize::new(0);

/// Plain failure-free exploration (no hydrofrac): send both writes, let the simulation
/// quiesce, then count which write completions reached the client. Exhaustive when
/// `ABD_KV_FUZZ` is unset, otherwise `ABD_KV_FUZZ` random schedules. With `ABD_KV_PANIC` set,
/// the first incomplete run panics, so the simulator replays it with its schedule log.
#[cfg(stageleft_runtime)]
#[test]
#[ignore = "measurement"]
fn abd_kv_two_writes_failure_free_completion() {
    build!(flow, replicas, clients, w_send, done);
    let sim = flow
        .sim()
        .skip_consistency_assertions()
        .with_cluster_size(&replicas, REPLICAS)
        .with_cluster_size(&clients, 1)
        .compiled();
    let body = async || {
        for r in REQUESTS {
            w_send.send(0, r);
        }
        hydro_lang::sim::compiled::quiesce().await;
        let got: Vec<(u64, (u32, Ts))> = done.collect_sorted(0).await;
        let run = RUNS.fetch_add(1, Ordering::Relaxed) + 1;
        if run % 20_000 == 0 {
            eprintln!(
                "progress: {run} schedules, incomplete {}, write 1 missing {}, write 2 missing {}",
                INCOMPLETE.load(Ordering::Relaxed),
                MISSING_1.load(Ordering::Relaxed),
                MISSING_2.load(Ordering::Relaxed),
            );
        }
        let has = |rid| got.iter().any(|(r, _)| *r == rid);
        if !has(1) {
            MISSING_1.fetch_add(1, Ordering::Relaxed);
        }
        if !has(2) {
            MISSING_2.fetch_add(1, Ordering::Relaxed);
        }
        if !(has(1) && has(2)) {
            if INCOMPLETE.fetch_add(1, Ordering::Relaxed) == 0 {
                eprintln!("first incomplete schedule (run {run}): got {got:?}");
            }
            // Fail the run so the simulator replays it with its schedule log.
            assert!(
                std::env::var("ABD_KV_PANIC").is_err(),
                "a write never completed: got {got:?}"
            );
        }
    };
    let start = std::time::Instant::now();
    let explored = match std::env::var("ABD_KV_FUZZ")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => {
            sim.fuzz_bounded(n, body);
            n
        }
        None => sim.exhaustive(body),
    };
    eprintln!(
        "ABD-KV, two concurrent writes, failure-free: {explored} schedules explored ({} bodies finished) in {:.1}s; \
         incomplete (some write never acked): {}; write 1 missing: {}; write 2 missing: {}",
        RUNS.load(Ordering::Relaxed),
        start.elapsed().as_secs_f64(),
        INCOMPLETE.load(Ordering::Relaxed),
        MISSING_1.load(Ordering::Relaxed),
        MISSING_2.load(Ordering::Relaxed),
    );
}
