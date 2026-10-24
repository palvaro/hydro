//! The races mode (docs/design/hydrofrac-races.md) cross-checked by brute force on the smallest
//! configurations of the races-target corpus (docs/design/races-target-steps.md) that the
//! enumeration can still exhaust: uniform reliable broadcast, G-Set gossip, multi-Paxos, ABD and
//! two-phase commit with cooperative termination.
//!
//! Every agreement test explores **every** schedule of every admissible fault set of at most
//! [`MAX_FAULTS`] faults (all channels here are `fail_stop`, so every fault is a crash). The
//! search is capped at the same number of crashes ([`EcCheck::max_crashes`]), so its sweep and
//! the enumeration cover the same sets. Each program is checked by the search ([`EcCheck::new`]),
//! which must agree with the enumeration, and the cost lines (`COST ...`, with `--nocapture`) put
//! the search's sets and schedules next to the enumeration's.
//!
//! Skipped, because brute force cannot exhaust even their failure-free run (measured by the probes
//! below): uniform reliable broadcast over three members (checked by the races mode alone in
//! `ec_quorum_trust`), G-Set gossip, multi-Paxos and ABD.
//! Uniform reliable broadcast runs over two members instead, where only the source may crash.
//! Cooperative termination with racing timers is sampled; it is a counterexample-finding
//! contrast only. With its timers firing only after the faults stop, it is exhaustive.
//!
//! The `races_targets_probe_*` tests only measure the failure-free schedule space of a
//! configuration (search and enumeration both capped at zero crashes); they are `#[ignore]`d.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;
use hydrofrac::check::{CrossCheck, Verdict};


pub struct Source;
pub struct Member;
pub struct Proposer;
pub struct Acceptor;
pub struct Learner;

/// The largest fault set the enumeration covers, and the search's crash cap.
const MAX_FAULTS: usize = 2;

/// The check, capped at `max_crashes` crashes.
fn capped(max_crashes: usize) -> EcCheck {
    EcCheck::new().max_crashes(max_crashes)
}

/// One line per verdict.
fn verdict(cross: &CrossCheck, v: &Verdict) -> String {
    let names = cross.results.report_names();
    match v {
        Verdict::Certified { tolerates } => format!("CERTIFIED (tolerates {tolerates:?})"),
        Verdict::Counterexample(c) => format!(
            "COUNTEREXAMPLE {} ({} crash(es), tolerated {:?}, certified {}, missing {:?}, divergent {:?})",
            names.fault_set(&c.faults),
            c.crashes(),
            c.tolerates,
            c.tolerance_certified,
            c.missing,
            c.divergent,
        ),
        Verdict::Exhausted { tolerates, sites } => format!(
            "EXHAUSTED up to {tolerates:?} (not a certificate; {} choice sites)",
            sites.len()
        ),
        Verdict::Budget { tolerates } => format!("BUDGET (tolerates {tolerates:?})"),
        Verdict::Vacuous(r) => format!("vacuous ({r:?})"),
        Verdict::NotChecked(r) => format!("not checked ({r:?})"),
        Verdict::CoveredBy(i) => format!("covered by label #{i}"),
    }
}

/// Prints the search's report, the enumeration's, and one `COST` line per searched label: the
/// verdict, the search's sets and schedules, and the enumeration's.
fn costs(name: &str, cross: &CrossCheck, wall: std::time::Duration) {
    eprintln!(
        "=== {name} ({:.1}s)\nsearch:\n{}\n{cross}",
        wall.as_secs_f64(),
        cross.results
    );
    for l in cross.results.results() {
        let brute = cross.brute.label(l.index);
        eprintln!(
            "COST {name} / {}: {} | search {} sets, {} schedules, {:.1}s, races {:?} | \
             brute force: {} | brute force total {} sets, {} schedules, {:.1}s",
            l.name(),
            verdict(cross, &l.verdict),
            l.stats.hypotheses,
            l.stats.schedules,
            l.stats.wall.as_secs_f64(),
            l.stats.races,
            match brute {
                None => "not enumerated".to_owned(),
                Some(b) => format!(
                    "{} breaking of {} admissible{}",
                    b.breaking.len(),
                    b.admissible,
                    b.breaking
                        .first()
                        .map(|s| format!(", first {}", cross.results.report_names().fault_set(s)))
                        .unwrap_or_default()
                ),
            },
            cross.brute.sets,
            cross.brute.schedules,
            cross.brute.wall.as_secs_f64(),
        );
    }
    if cross.disagreements.is_empty() {
        eprintln!("AGREE {name}");
    } else {
        for d in &cross.disagreements {
            eprintln!("DISAGREEMENT {name}: {d}");
        }
    }
}

/// Runs `build` (a cross-check of `max_faults` faults), capped at `max_faults` crashes, prints
/// its costs, and returns it.
fn cross(
    name: &str,
    max_faults: usize,
    build: impl FnOnce(EcCheck, usize) -> CrossCheck,
) -> CrossCheck {
    let start = std::time::Instant::now();
    let cross = build(capped(max_faults), max_faults);
    costs(name, &cross, start.elapsed());
    cross
}

/// The failure-free schedule space of a configuration (both sides capped at zero crashes).
fn probe(name: &str, build: impl FnOnce(EcCheck, usize) -> CrossCheck) {
    let cross = cross(name, 0, build);
    eprintln!(
        "PROBE {name}: {} failure-free schedules in {:.1}s",
        cross.brute.schedules,
        cross.brute.wall.as_secs_f64()
    );
}

// --- 1. Uniform reliable broadcast (closed) ---------------------------------------------------

/// `uniform_reliable_broadcast_closed` from one process to `members` members, with echo
/// threshold `threshold`, one item sent.
fn urb(members_count: usize, threshold: usize) -> impl FnOnce(EcCheck, usize) -> CrossCheck {
    move |mut check, max_faults| {
        use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let _out =
            uniform_reliable_broadcast_closed(items, &members, threshold).sim_cluster_output();
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, members_count))
            .compiled();
        check.cross_check(
            &sim,
            async || {
                send.send(1);
            },
            max_faults,
        )
    }
}

/// Measured: passed 215,452 schedules (about 340 per second) in 15 minutes without finishing.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_urb3() {
    probe("urb (3 members, threshold 1)", urb(3, 1));
}

/// Measured: 56 schedules.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_urb2() {
    probe("urb (2 members, threshold 1)", urb(2, 1));
}

// --- 2. G-Set gossip (closed) -----------------------------------------------------------------

/// `g_set_gossip` among `members` members; `updates` members (0, 1, ...) each contribute one
/// element in the fault phase, then the recovery phase runs `rounds` × (a gossip tick at each
/// member of `tickers`, quiesce). Its failure-free runs are those of the barrier harness this
/// replaces (updates, quiesce, rounds), so the measurements below stand.
fn gossip(
    members_count: u32,
    updates: u32,
    rounds: u32,
    tickers: &'static [u32],
) -> impl FnOnce(EcCheck, usize) -> CrossCheck {
    move |mut check, max_faults| {
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;
        let mut flow = FlowBuilder::new();
        let members = flow.cluster::<Member>();
        let (update, ups) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
        let _set = g_set_gossip(&members, ups.weaken_ordering::<NoOrder>(), ticks);
        let sim = check
            .instrument(
                flow.sim()
                    .with_cluster_size(&members, members_count as usize),
            )
            .compiled();
        check.cross_check_then_recover(
            &sim,
            async || {
                for m in 0..updates {
                    update.send(m, 10 + m);
                }
            },
            async || {
                for _ in 0..rounds {
                    tick.send_many(tickers.iter().map(|m| (*m, ())));
                    hydro_lang::sim::compiled::quiesce().await;
                }
            },
            max_faults,
        )
    }
}

/// Measured: passed 88,889 schedules in 10 minutes without finishing. (Two members admit no
/// member crash, so the configuration would be vacuous anyway.)
#[test]
#[ignore = "measurement"]
fn races_targets_probe_gossip2() {
    probe(
        "gossip (2 members, 1 update, 1 round)",
        gossip(2, 1, 1, &[0, 1]),
    );
}

/// Measured: passed 44,412 schedules in 10 minutes without finishing.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_gossip3() {
    probe(
        "gossip (3 members, 1 update, 1 round)",
        gossip(3, 1, 1, &[0, 1, 2]),
    );
}

/// Measured: passed 325,645 schedules in 15 minutes, then the process was killed (memory).
#[test]
#[ignore = "measurement"]
fn races_targets_probe_gossip3_one_ticker() {
    probe(
        "gossip (3 members, 1 update at 0, 1 round, tick at 1 only)",
        gossip(3, 1, 1, &[1]),
    );
}

// --- 3. Multi-Paxos ---------------------------------------------------------------------------

/// `multi_paxos`: one proposer leads round 1 and gets `commands` commands, together; three
/// acceptors (majority two), `learners` learners.
fn paxos(commands: u32, learners_count: usize) -> impl FnOnce(EcCheck, usize) -> CrossCheck {
    move |mut check, max_faults| {
        use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;
        let mut flow = FlowBuilder::new();
        let proposers = flow.cluster::<Proposer>();
        let acceptors = flow.cluster::<Acceptor>();
        let learners = flow.cluster::<Learner>();
        let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (command, cmds) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let outputs = multi_paxos(&acceptors, &learners, 2, leads, cmds);
        let _learned = outputs.learned.weaken_consistency().sim_cluster_output();
        let sim = check
            .instrument(
                flow.sim()
                    .with_cluster_size(&proposers, 1)
                    .with_cluster_size(&acceptors, 3)
                    .with_cluster_size(&learners, learners_count),
            )
            .compiled();
        check.cross_check(
            &sim,
            async || {
                lead.send(0, 1);
                for c in 0..commands {
                    command.send(0, 7 + c);
                }
            },
            max_faults,
        )
    }
}

/// Measured with a barrier between the lead and the commands (now dropped, which only adds
/// schedules): passed 21,861 schedules (about 106 per second) in 10 minutes without finishing.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_paxos() {
    probe("multi_paxos (1 command, 2 learners)", paxos(1, 2));
}

// --- 4. ABD -----------------------------------------------------------------------------------
//
// Not here: the register's message types are private to `hydro_std`, so a flow built from this
// crate does not stage. Its probe is `hydro_std`'s `ec_abd_hydrofrac::races_targets_probe_abd`
// (measured: 131,476 failure-free schedules, about 222 per second, in 10 minutes without
// finishing), so it is not brute-forceable.

// --- 5. Two-phase commit with cooperative termination -----------------------------------------

/// Two-phase commit with cooperative termination, coordinator + `n` participants, one
/// transaction (the harness of `ec_two_pc_hydrofrac`). `racing`: every participant's timer
/// fires with the transaction, in the fault phase; otherwise the timers fire only in the
/// recovery phase. `ticks`: the recovery ticks.
#[cfg(stageleft_runtime)]
fn ctp(n: u32, racing: bool, ticks: usize) -> impl FnOnce(EcCheck, usize) -> CrossCheck {
    move |check, max_faults| {
        super::ec_two_pc_hydrofrac::TwoPc::with(check, true, n).cross_check(
            racing, ticks, max_faults,
        )
    }
}

/// Measured: 32 schedules.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_ctp_timers_after_the_faults() {
    probe("ctp (2 participants, timers after the faults)", ctp(2, false, 1));
}

/// Measured (with the barrier harness's racing variant, the same failure-free runs): passed
/// 31,957 schedules in 10 minutes without finishing.
#[test]
#[ignore = "measurement"]
fn races_targets_probe_ctp_racing() {
    probe("ctp (2 participants, racing timers)", ctp(2, true, 0));
}

// --- Agreement tests ----------------------------------------------------------------------------

/// `build`, on `n` sampled schedules per fault set (for counterexample-finding contrasts only:
/// a sampled run certifies nothing).
fn sampled(
    n: usize,
    build: impl FnOnce(EcCheck, usize) -> CrossCheck,
) -> impl FnOnce(EcCheck, usize) -> CrossCheck {
    move |check, max_faults| build(check.sample_schedules(n), max_faults)
}

/// The label `name` (by display name) among the searched labels.
fn label<'c>(cross: &'c CrossCheck, name: &str) -> &'c hydrofrac::check::LabelResult {
    cross
        .results
        .results()
        .iter()
        .find(|l| l.plan.collection.display_name() == name && l.stats.hypotheses > 0)
        .unwrap_or_else(|| panic!("no searched label `{name}`:\n{}", cross.results))
}

/// The searched label `name` is certified (up to [`MAX_FAULTS`] crashes, or for every admissible
/// set when no admissible set can crash more nodes than that), Claim B included.
fn assert_certified(cross: &CrossCheck, name: &str) {
    let l = label(cross, name);
    assert!(
        matches!(
            l.verdict,
            Verdict::Certified {
                tolerates: hydrofrac::search::Tolerance::UpTo(MAX_FAULTS)
                    | hydrofrac::search::Tolerance::All
            }
        ),
        "{name}: {}\n{}",
        verdict(cross, &l.verdict),
        cross.results
    );
    assert!(
        l.stats.races.is_some_and(|r| r.claim_b),
        "{name}: Claim B\n{}",
        cross.results
    );
}

/// The trusted `quorum` (docs/design/hydrofrac-races.md §8.2) leaves the label no fragile step:
/// the races mode runs one schedule per set (measured: 2 sets at threshold 1, 3 at threshold 2,
/// against brute force's 4 sets and 169 schedules).
fn assert_one_schedule_per_set(cross: &CrossCheck, name: &str) {
    let l = label(cross, name);
    assert_eq!(
        l.stats.schedules, l.stats.hypotheses,
        "{name}: one schedule per set\n{}",
        cross.results
    );
}

/// Uniform reliable broadcast, two members (so no member may crash: only the source), one item.
#[test]
fn races_targets_urb2_threshold1() {
    let c = cross("urb (2 members, threshold 1)", MAX_FAULTS, urb(2, 1));
    c.assert_agrees();
    assert_certified(&c, "uniform_reliable_broadcast_closed");
    assert_one_schedule_per_set(&c, "uniform_reliable_broadcast_closed");
}

#[test]
fn races_targets_urb2_threshold2() {
    let c = cross("urb (2 members, threshold 2)", MAX_FAULTS, urb(2, 2));
    c.assert_agrees();
    assert_certified(&c, "uniform_reliable_broadcast_closed");
    assert_one_schedule_per_set(&c, "uniform_reliable_broadcast_closed");
}

/// Two-phase commit with cooperative termination, two participants, the timers firing only in
/// the recovery phase (transaction; heal; timeouts), every schedule of every set of at most two
/// faults.
#[test]
fn races_targets_ctp2_timers_after_the_faults() {
    let c = cross(
        "ctp (2 participants, timers after the faults)",
        MAX_FAULTS,
        ctp(2, false, 1),
    );
    c.assert_agrees();
    assert_certified(&c, "committed");
}

/// The contrast: the same program with one-shot timers racing the transaction. Its
/// failure-free run alone passed 31,957 schedules in 10 minutes without finishing, so this is
/// sampled (64 schedules per set, one fault): it can find the one-crash counterexample, never
/// certify.
const RACING_SAMPLES: usize = 64;

#[test]
fn races_targets_ctp2_racing_sampled() {
    let c = cross(
        "ctp (2 participants, racing timers, sampled)",
        1,
        sampled(RACING_SAMPLES, ctp(2, true, 0)),
    );
    c.assert_agrees();
    let cex = label(&c, "committed")
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the races mode finds the crash:\n{}", c.results));
    assert_eq!(cex.crashes(), 1, "{}", c.results);
}
