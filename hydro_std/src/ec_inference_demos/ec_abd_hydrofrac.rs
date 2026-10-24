//! The ABD register under hydrofrac's `EcCheck` (docs/design/hydrofrac-lite.md §7): every
//! `EventualConsistency` label of [`abd_register`] and [`abd_register_reliable_phase2`], one
//! client write, every node (client and replicas) crashable, crash tolerance swept from zero
//! upwards.
//!
//! The register is a `fold` into a `Singleton`, so it is not tapped itself: it is checked at its
//! input, the phase-2 stream it folds (docs/design/hydrofrac-lite.md §4.7). With phase 2 over a
//! single `broadcast_closed`, that input breaks under one crash: the client dies between two of
//! its phase-2 sends, so one replica stores the write and the others never will. Over reliable
//! broadcast, every replica that stores the write echoes it, and the register holds.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;

use super::abd::{abd_register, abd_register_reliable_phase2};

pub struct Replica;
pub struct Client;

#[cfg(stageleft_runtime)]
const REPLICAS: usize = 3;

/// Every label of the register after one client write, phase 2 over `broadcast_closed` or, if
/// `reliable`, over reliable broadcast; `samples` sampled schedules per hypothesis.
#[cfg(stageleft_runtime)]
fn check(reliable: bool, samples: usize) -> hydrofrac::Results {
    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<Replica>();
    let clients = flow.cluster::<Client>();
    let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
    let (_r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let outs = if reliable {
        abd_register_reliable_phase2(&replicas, REPLICAS / 2 + 1, writes, reads)
    } else {
        abd_register(&replicas, REPLICAS / 2 + 1, writes, reads)
    };
    let _done = outs.write_done.sim_cluster_output();
    let _read = outs.read_result.sim_cluster_output();

    let mut check = hydrofrac::EcCheck::new().sample_schedules(samples);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&replicas, REPLICAS)
                .with_cluster_size(&clients, 1),
        )
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        w_send.send(0, (1, 10u32));
    });
    eprintln!(
        "abd (reliable phase 2: {reliable}): {:.1}s\n{results}",
        start.elapsed().as_secs_f64()
    );
    results
}

/// The register: the labelled `fold` into a `Singleton`. It is not checked itself (members that
/// agree on a stream's facts can disagree on a fold of it), so this returns the result of the
/// stream it folds, which is.
#[cfg(stageleft_runtime)]
fn register(results: &hydrofrac::Results) -> &hydrofrac::check::LabelResult {
    let folds: Vec<_> = results
        .labels_named("abd_register_state")
        .filter(|l| l.plan.collection.covered_by.is_some())
        .collect();
    let [register] = folds[..] else {
        panic!("one register label folding a labelled stream\n{results}");
    };
    let hydrofrac::Verdict::NotChecked(_) = register.verdict else {
        panic!("the register itself is not checked\n{results}");
    };
    &results.labels[register.plan.collection.covered_by.expect("filtered")]
}

/// The one searched label named `name` (other labels of that name are covered or not checked).
#[cfg(stageleft_runtime)]
fn searched<'r>(
    results: &'r hydrofrac::Results,
    name: &'r str,
) -> &'r hydrofrac::check::LabelResult {
    let found: Vec<_> = results
        .labels_named(name)
        .filter(|l| l.plan.is_searched())
        .collect();
    let [label] = found[..] else {
        panic!("one searched label `{name}`\n{results}");
    };
    label
}

/// `label` tolerates no crash and breaks with one crash of a node whose name starts with
/// `node`.
#[cfg(stageleft_runtime)]
fn breaks_at_one_crash_of(
    results: &hydrofrac::Results,
    label: &hydrofrac::check::LabelResult,
    node: &str,
) {
    use hydrofrac::search::Fault;
    let hydrofrac::Verdict::Counterexample(cex) = &label.verdict else {
        panic!("{}: expected a counterexample\n{results}", label.name());
    };
    assert_eq!(cex.tolerates, Some(0), "{}\n{results}", label.name());
    let [Fault::Crash { node: crashed, .. }] = cex.faults.iter().collect::<Vec<_>>()[..] else {
        panic!("{}: one crash\n{results}", label.name());
    };
    assert!(crashed.starts_with(node), "{}\n{results}", label.name());
}

/// No counterexample under any admissible crash set. ABD's replicas batch (its quorums), so
/// the program has schedule choices and the search, which finds none, cannot certify its labels
/// (docs/design/hydrofrac-lite.md §4.9).
#[cfg(stageleft_runtime)]
fn no_counterexample(results: &hydrofrac::Results, label: &hydrofrac::check::LabelResult) {
    assert!(!results.choices.is_empty(), "{}\n{results}", label.name());
    assert!(
        matches!(
            label.verdict,
            hydrofrac::Verdict::Exhausted {
                tolerates: hydrofrac::search::Tolerance::All,
                ..
            }
        ),
        "{}\n{results}",
        label.name()
    );
}

/// With phase 2 over a single `broadcast_closed`, the client crashing between two of its
/// phase-2 sends leaves one replica storing the write and the others never: the register
/// (through its input) breaks at one crash, as do the query and phase-2 hops.
#[test]
fn the_register_breaks_when_the_client_crashes_mid_phase_two() {
    let results = check(false, 8);
    breaks_at_one_crash_of(&results, register(&results), "Client");
    for name in ["queries", "phase2", "phase2_fwd"] {
        breaks_at_one_crash_of(&results, searched(&results, name), "Client");
    }
    assert!(
        !results
            .results()
            .iter()
            .any(|l| matches!(l.verdict, hydrofrac::Verdict::Budget { .. })),
        "{results}"
    );
}

/// With phase 2 over reliable broadcast, every replica that stores a write echoes it, so the
/// client's crash no longer splits the replicas: the register (through its input) has no
/// counterexample under any admissible crash set (searched, not certified: see
/// [`no_counterexample`]). The phase-1 query broadcast and reliable broadcast's own hops
/// are still single hops and still break: a lost query only delays the write, and a hop's
/// verdict is informational once the hops merge.
#[test]
fn reliable_phase_two_certifies_the_register() {
    let results = check(true, 8);
    no_counterexample(&results, register(&results));
    no_counterexample(&results, searched(&results, "phase2_fwd"));
    breaks_at_one_crash_of(&results, searched(&results, "queries"), "Client");
    breaks_at_one_crash_of(&results, searched(&results, "initial"), "Client");
    breaks_at_one_crash_of(&results, searched(&results, "echo"), "Replica");
    assert!(
        !results
            .results()
            .iter()
            .any(|l| matches!(l.verdict, hydrofrac::Verdict::Budget { .. })),
        "{results}"
    );
}

/// Measurement (docs/design/hydrofrac-races.md §7): the failure-free schedule space of the
/// register after one client write, phase 2 over `broadcast_closed`, explored exhaustively by
/// the races mode capped at zero crashes. Decides whether the brute-force cross-check of
/// `hydro_test`'s `ec_races_targets` can include ABD. Measured: 131,476 schedules (about 222 per
/// second) in 10 minutes without finishing, so it cannot.
#[cfg(stageleft_runtime)]
#[test]
#[ignore = "measurement"]
fn races_targets_probe_abd() {
    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<Replica>();
    let clients = flow.cluster::<Client>();
    let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
    let (_r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let outs = abd_register(&replicas, REPLICAS / 2 + 1, writes, reads);
    let _done = outs.write_done.sim_cluster_output();
    let _read = outs.read_result.sim_cluster_output();
    let mut check = hydrofrac::EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&replicas, REPLICAS)
                .with_cluster_size(&clients, 1),
        )
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        w_send.send(0, (1, 10u32));
    });
    let schedules = results
        .results()
        .iter()
        .map(|l| l.stats.schedules)
        .max()
        .unwrap_or(0);
    eprintln!(
        "PROBE abd (plain phase 2): {schedules} failure-free schedules in {:.1}s\n{results}",
        start.elapsed().as_secs_f64()
    );
}
