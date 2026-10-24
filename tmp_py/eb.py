p='ec_bruteforce.rs'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:70]
    s=s.replace(a,b)
rep('''//! - [`ask_once`], a miniature of cooperative termination's race, small enough to explore every
//!   schedule of every fault set. The enumeration finds a single crash that breaks its claim
//!   (the members' known values). Molly's pruning rule would prune that crash with derivations
//!   from schedules in which the peer had already learned the value when the query arrived,
//!   and certify the claim; the search no longer certifies labels of programs with schedule
//!   choices, and its heuristic for them finds the crash.''','''//! - [`ask_once`], a miniature of cooperative termination's race, small enough to explore every
//!   schedule of every fault set. The enumeration finds a single crash that breaks its claim
//!   (the members' known values). Molly's pruning rule would prune that crash with derivations
//!   from schedules in which the peer had already learned the value when the query arrived,
//!   and certify the claim. The search marks those derivations fragile (they pass a "trigger
//!   meets state" step), so they protect nothing, and it finds the crash.''')
rep('''//! Each agreement test on an order-sensitive program has a companion that asserts only the
//! enumeration's side (that it finds the breaking set), so the oracle stays guarded. For these
//! labels, agreement means the search found every counterexample the enumeration did, within
//! the crash bounds it swept, and certified none of them (docs/design/hydrofrac-lite.md §4.9).''','''//! Each agreement test on an order-sensitive program has a companion that asserts only the
//! enumeration's side (that it finds the breaking set), so the oracle stays guarded.''')
rep('''#[test]
fn brute_force_agrees_on_a_plain_broadcast() {
    let cross = plain_reliable_broadcast(EcCheck::new());
    agree("reliable_broadcast::broadcast (plain)", &cross);
    // Without the echo, a broadcaster crash mid fan-out breaks every searched label.
    assert!(!cross.brute.labels.is_empty(), "{cross}");
    for b in &cross.brute.labels {
        assert_eq!(b.min_crashes(), Some(1), "{}: {cross}", b.name);
    }
}''','''#[test]
fn brute_force_agrees_on_a_plain_broadcast() {
    let cross = plain_reliable_broadcast(EcCheck::new());
    costs("reliable_broadcast::broadcast (plain)", &cross);
    agree("reliable_broadcast::broadcast (plain)", &cross);
    // Without the echo, a broadcaster crash mid fan-out breaks every searched label.
    assert!(!cross.brute.labels.is_empty(), "{cross}");
    for b in &cross.brute.labels {
        assert_eq!(b.min_crashes(), Some(1), "{}: {cross}", b.name);
        let cex = cross.results.results()[b.index]
            .verdict
            .counterexample()
            .unwrap_or_else(|| panic!("{}: a mid fan-out crash breaks it: {cross}", b.name));
        assert_eq!(cex.crashes(), 1, "{cross}");
    }
}''')
rep('''fn brute_force_agrees_on_reliable_broadcast_closed() {
    let cross = reliable_broadcast_closed_cross_check(EcCheck::new());
    agree("reliable_broadcast_closed", &cross);
}''','''fn brute_force_agrees_on_reliable_broadcast_closed() {
    let cross = reliable_broadcast_closed_cross_check(EcCheck::new());
    costs("reliable_broadcast_closed", &cross);
    agree("reliable_broadcast_closed", &cross);
}''')
rep('''/// The search agrees with the enumeration on [`ask_once`]. Molly's rule certified the claim
/// after 2 hypotheses, pruning the breaking crash with a derivation (member 1 answering member
/// 2's query) that only occurs in the schedules where member 1 already knew the value. The
/// program has schedule choices, so the search uses its heuristic (a hypothesis need only kill
/// one schedule's derivations) and finds the crash at its first hypothesis.
#[test]
fn brute_force_agrees_on_ask_once() {
    let cross = ask_once_cross_check(EcCheck::new());
    cross.assert_agrees();''','''/// The search agrees with the enumeration on [`ask_once`]. Molly's rule certified the claim
/// after 2 hypotheses, pruning the breaking crash with a derivation (member 1 answering member
/// 2's query) that only occurs in the schedules where member 1 already knew the value. Here that
/// derivation is fragile, and the crash of member 0 right after its announcement to member 1
/// spares (1, 7) under the order-free crash rule and kills (2, 7)'s only robust derivation; one of
/// its schedules splits the members.
#[test]
fn brute_force_agrees_on_ask_once() {
    let cross = ask_once_cross_check(EcCheck::new());
    costs("ask_once", &cross);
    cross.assert_agrees();''')
rep('''        .unwrap_or_else(|| panic!("the search finds the race: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(!cex.tolerance_certified, "{cross}");
}''','''        .unwrap_or_else(|| panic!("the search finds the race: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    // Every schedule was explored, so no counterexample with fewer crashes was missed.
    assert!(cex.tolerance_certified, "{cross}");
}''')
rep('''/// harness of `ec_two_pc_hydrofrac`). `samples`: schedules per fault set, or every schedule.
/// `races`: checked by [`EcCheck::races`].
fn two_pc_cross_check(
    ctp: bool,
    participants_count: u32,
    samples: Option<usize>,
    races: bool,
) -> CrossCheck {''','''/// harness of `ec_two_pc_hydrofrac`). `samples`: schedules per fault set, or every schedule.
fn two_pc_cross_check(ctp: bool, participants_count: u32, samples: Option<usize>) -> CrossCheck {''')
rep('''    let check = if races {
        EcCheck::races()
    } else {
        EcCheck::new()
    };
    let mut check = match samples {
        Some(n) => check.sample_schedules(n),
        None => check,
    };''','''    let mut check = match samples {
        Some(n) => EcCheck::new().sample_schedules(n),
        None => EcCheck::new(),
    };''')
rep('''        "=== two_pc (ctp: {ctp}, {participants_count} participants, samples: {samples:?}, races: {races})\\nsearch:\\n{}\\n{cross}",''','''        "=== two_pc (ctp: {ctp}, {participants_count} participants, samples: {samples:?})\\nsearch:\\n{}\\n{cross}",''')
rep('''    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES), false);
    let label = claim(&cross, "committed");
    let b = cross''','''    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES));
    let label = claim(&cross, "committed");
    let b = cross''')
rep('''/// The search agrees with the enumeration on two-phase commit with cooperative termination
/// and three participants. Molly's rule certified `committed` (after 12 hypotheses), which two
/// crashes break; the heuristic for programs with schedule choices finds the two crashes.
#[test]
fn brute_force_agrees_on_two_pc_cooperative_termination() {
    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES), false);
    cross.assert_agrees();
    let cex = claim(&cross, "committed")
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the search finds the two-crash split: {cross}"));
    assert_eq!(cex.crashes(), 2, "{cross}");
}''','''/// The search agrees with the enumeration on two-phase commit with cooperative termination
/// and three participants. Molly's rule certified `committed` (after 12 hypotheses), which two
/// crashes break: every derivation of it passes a negation, so none protects a pair, and the
/// search finds the two crashes. Sampled, so the tolerance is not a certificate.
#[test]
fn brute_force_agrees_on_two_pc_cooperative_termination() {
    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES));
    costs("two_pc ctp (3 participants, sampled)", &cross);
    cross.assert_agrees();
    let cex = claim(&cross, "committed")
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the search finds the two-crash split: {cross}"));
    assert_eq!(cex.crashes(), 2, "{cross}");
    assert!(!cex.tolerance_certified, "sampled schedules never certify: {cross}");
}''')
a=s.index('''// The races mode (docs/design/hydrofrac-races.md §4.4) on the same programs. Each test prints
// the search's cost next to the enumeration's.''')
b=s.index('''// Several payloads sent together''')
costs_fn=s[s.index('/// Prints the races search\'s and the enumeration\'s cost'):s.index('#[test]\nfn races_agree_on_a_plain_broadcast()')]
costs_fn=costs_fn.replace("/// Prints the races search's and the enumeration's cost","/// Prints the search's and the enumeration's cost").replace('"COST {name} / {}: races {} sets','"COST {name} / {}: search {} sets')
s=s[:a]+costs_fn+s[b:]
rep('''// Each test runs lite's check and the races mode, each cross-checked, and prints a `TABLE` line
// per label (run with `--nocapture`).''','''// Each test runs the check, cross-checked, and prints a `TABLE` line per label (run with
// `--nocapture`).''')
a=s.index('''/// Runs `build` with lite's check and with the races mode''')
b=s.index('''/// Every failure-free schedule of `sim` under `body`''')
s=s[:a]+'''/// Runs `build` with the check, asserts it agrees with brute force, and prints one line per
/// searched label. `build` returns the cross-check and the program's failure-free schedule count.
fn compare(name: &str, build: impl Fn(EcCheck) -> (CrossCheck, usize)) {
    let start = std::time::Instant::now();
    let (cross, schedules) = build(EcCheck::new());
    let wall = start.elapsed();
    eprintln!(
        "=== {name}: {schedules} failure-free schedules\\nsearch:\\n{}\\n{cross}",
        cross.results
    );
    for r in cross.results.results() {
        let Some(b) = cross.brute.label(r.index) else {
            continue;
        };
        let notes = r.stats.races.unwrap_or_default();
        eprintln!(
            "TABLE {name} | {} | search: {} — {} sets / {} schedules (one-schedule {}, early stops \\
             {}) | brute force: {} sets / {} schedules, min crashes {:?} | failure-free schedules \\
             {schedules}",
            r.name(),
            verdict_summary(&r.verdict),
            r.stats.hypotheses,
            r.stats.schedules,
            notes.single_schedule_sets,
            notes.early_stops,
            cross.brute.sets,
            cross.brute.schedules,
            b.min_crashes(),
        );
    }
    eprintln!("TIME {name}: {wall:.1?} (with its brute force)");
    cross.assert_agrees();
}

'''+s[b:]
s=s.replace('fn both_agree_on_','fn brute_force_agrees_on_')
rep('''/// Too large to cross-check: lite's exhaustive search and the enumeration together did not
/// finish within 30 minutes.''','''/// Too large to cross-check: lite's exhaustive search and the enumeration together did not
/// finish within 30 minutes (the enumeration alone is most of it).''')
rep('''/// Races mode alone (no enumeration, which cannot finish here: the failure-free run alone has
/// more than 160,000 schedules) on the echo broadcast over three members with one payload. The
/// program is fragile-free, so each fault set runs on one schedule. Prints the verdicts and
/// costs; asserts that `deliveries` is certified.
#[test]
fn races_alone_on_an_echo_broadcast_over_three_members() {''','''/// The search alone (no enumeration, which cannot finish here: the failure-free run alone has
/// more than 160,000 schedules) on the echo broadcast over three members with one payload. The
/// program is fragile-free, so each fault set runs on one schedule. Prints the verdicts and
/// costs; asserts that `deliveries` is certified.
#[test]
fn search_alone_on_an_echo_broadcast_over_three_members() {''')
rep('''    let mut check = EcCheck::races();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))''','''    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))''')
rep('''"=== races alone, echo broadcast''','''"=== search alone, echo broadcast''')
rep('''"RACES-ALONE {} |''','''"ALONE {} |''')
open(p,'w').write(s)
