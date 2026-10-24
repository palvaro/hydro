//! The hydrofrac brute-force cross-check (docs/design/hydrofrac-lite.md §7, "Honesty check") on
//! corpus programs: every admissible fault set of at most two faults is run without lineage
//! pruning, over every schedule, and `EcCheck`'s verdicts must agree with what the enumeration
//! finds. These programs batch in ticks, so unlike the inline programs in
//! `hydrofrac::check::brute_tests` their fault sets run over many schedules.
//!
//! The tables print with `--nocapture`.
//!
//! `reliable_broadcast::broadcast` *with* the echo is not here: one exhaustive run of it over
//! three members exceeds 160k schedules, and the check holds every schedule's record until the
//! run ends, so even single faults do not finish (the process was killed after 31 minutes). The
//! echo logic is cross-checked on the inline echo broadcasts in `hydrofrac::check::brute_tests`
//! and on `hydro_std`'s `reliable_broadcast_closed` below.
//!
//! # Order-sensitive programs
//!
//! Lineage-driven pruning assumes that a derivation whose deliveries a fault set leaves intact
//! still derives its fact under that set, in *every* schedule. That holds when the derivation is
//! order-insensitive. It fails when an operator on it reads state that the schedule may or may
//! not have filled yet: a peer that answers a query only if it already knows the answer. The
//! second half of this file cross-checks such programs:
//!
//! - [`ask_once`], a miniature of cooperative termination's race, small enough to explore every
//!   schedule of every fault set. The enumeration finds a single crash that breaks its claim
//!   (the members' known values). Molly's pruning rule would prune that crash with derivations
//!   from schedules in which the peer had already learned the value when the query arrived,
//!   and certify the claim; the search no longer certifies labels of programs with schedule
//!   choices, and its heuristic for them finds the crash.
//! - two-phase commit with cooperative termination and three participants (the program the
//!   bug was found on), on sampled schedules: one coordinator crash alone takes more than
//!   100,000 schedules to explore (see [`TWO_PC_SCHEDULES`]).
//!
//! Each agreement test on an order-sensitive program has a companion that asserts only the
//! enumeration's side (that it finds the breaking set), so the oracle stays guarded. For these
//! labels, agreement means the search found every counterexample the enumeration did, within
//! the crash bounds it swept, and certified none of them (docs/design/hydrofrac-lite.md §4.9).
//!
//! ABD (`hydro_std`'s `abd_register`, one client write, three replicas) is order-sensitive too,
//! but too large to enumerate: its failure-free probe alone passed 21,000 schedules at about
//! 100 per second without finishing. It is searched on sampled schedules in `hydro_std`.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;
use hydrofrac::check::CrossCheck;

use super::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};
use super::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

/// The largest fault set the enumeration covers.
const MAX_FAULTS: usize = 2;

fn agree(name: &str, cross: &CrossCheck) {
    eprintln!("=== {name}\nsearch:\n{}\n{cross}", cross.results);
    cross.assert_agrees();
}

/// `reliable_broadcast::broadcast` to three members, without the echo, checked by `check`.
fn plain_reliable_broadcast(mut check: EcCheck) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        false,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(Payload(42));
        },
        MAX_FAULTS,
    )
}

#[test]
fn brute_force_agrees_on_a_plain_broadcast() {
    let cross = plain_reliable_broadcast(EcCheck::new());
    agree("reliable_broadcast::broadcast (plain)", &cross);
    // Without the echo, a broadcaster crash mid fan-out breaks every searched label.
    assert!(!cross.brute.labels.is_empty(), "{cross}");
    for b in &cross.brute.labels {
        assert_eq!(b.min_crashes(), Some(1), "{}: {cross}", b.name);
    }
}

/// `hydro_std`'s `reliable_broadcast_closed`: echo-based reliable broadcast over `fail_stop`.
#[test]
fn brute_force_agrees_on_reliable_broadcast_closed() {
    let cross = reliable_broadcast_closed_cross_check(EcCheck::new());
    agree("reliable_broadcast_closed", &cross);
}

/// `hydro_std`'s `reliable_broadcast_closed` over three members, checked by `check`.
fn reliable_broadcast_closed_cross_check(mut check: EcCheck) -> CrossCheck {
    use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = reliable_broadcast_closed(input, &members).sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    )
}

/// A miniature of cooperative termination's race, over three members. Member 0 learns a
/// value from its input and announces it to members 1 and 2, in that order. Member 2, if its
/// timer fires while it knows nothing, asks member 1, once; member 1 answers a query with what
/// it knows *when the query arrives*, and never again. Messages are `Some(value)` (an
/// announcement or an answer) or `None` (a query), sent to one member each. The members' known
/// values are labelled `EventualConsistency` as a claim, like two-phase commit's `committed`.
///
/// The claim fails under one crash: member 0 announces to member 1 and crashes before member
/// 2. Member 2's timer fires and it asks member 1; if the query reaches member 1 before the
/// announcement does, member 1 has nothing to answer, and member 2 never learns the value
/// member 1 holds.
fn ask_once<'a>(
    peers: &Cluster<'a, Member>,
    values: Stream<u32, Cluster<'a, Member>, Unbounded, TotalOrder>,
    timeouts: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder>,
    nondet_values: NonDet,
    nondet_net: NonDet,
    nondet_timeouts: NonDet,
) -> Stream<u32, Cluster<'a, Member, EventualConsistency>, Unbounded, NoOrder> {
    let (net_handle, net_in) =
        peers.forward_ref::<Stream<(MemberId<Member>, Option<u32>), _, Unbounded, NoOrder>>();

    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (out, known): (
        Stream<(MemberId<Member>, Option<u32>), Cluster<'a, Member>, Unbounded, NoOrder>,
        Stream<u32, Cluster<'a, Member>, Unbounded, NoOrder>,
    ) = sliced! {
        let value_batch = use::batch(values, nondet_values);
        let net_batch = use::batch(net_in, nondet_net);
        let timeout_batch = use::batch(timeouts, nondet_timeouts);
        let mut known = use::state(|l| {
            l.source_iter(q!(Vec::<u32>::new())).weaken_ordering::<NoOrder>()
        });

        let own = value_batch.weaken_ordering::<NoOrder>();
        let heard = net_batch.clone().filter_map(q!(|(_, message)| message));
        let newly = own.clone().chain(heard).unique().filter_not_in(known.clone());
        let known_now = known.chain(newly.clone());
        known = known_now.clone();

        // Announce what this member learned from its own input, to members 1 and 2.
        let announce = own.flat_map_ordered(q!(|value| [
            (hydro_lang::location::MemberId::from_raw_id(1), Some(value)),
            (hydro_lang::location::MemberId::from_raw_id(2), Some(value)),
        ]));
        // Ask member 1, once, if the timer fires while nothing is known.
        let knows_nothing = known_now.clone().count().map(q!(|n| n == 0));
        let ask = timeout_batch
            .weaken_ordering::<NoOrder>()
            .filter_if(knows_nothing)
            .map(q!(|()| (hydro_lang::location::MemberId::from_raw_id(1), None::<u32>)));
        // Answer this tick's queries (only member 2 asks) with what is known now.
        let queried = net_batch
            .filter(q!(|(_, message)| message.is_none()))
            .count()
            .map(q!(|n| n > 0));
        let answers = known_now
            .filter_if(queried)
            .map(q!(|value| (hydro_lang::location::MemberId::from_raw_id(2), Some(value))));

        (announce.chain(ask).chain(answers), newly)
    };

    net_handle.complete(
        out.demux(peers, TCP.fail_stop().name("net").bincode())
            .entries(),
    );

    known.assert_has_consistency_of::<Cluster<'a, Member, EventualConsistency>>(manual_proof!(
        /** claimed, not proven: one crash refutes it when a query outruns the announcement
        it asks about. This label is the target of the cross-check. */
    ))
}

/// The label of a program's output claim: the `<name>.assert_has_consistency_of(..)` the
/// function that builds the program returns (the report names it by its receiver, `name`).
fn claim<'c>(cross: &'c CrossCheck, name: &str) -> &'c hydrofrac::check::LabelResult {
    let mut found = cross.results.results().iter().filter(|l| {
        l.plan.collection.display_name() == name && l.plan.collection.op == "AssertIsConsistent"
    });
    let label = found
        .next()
        .unwrap_or_else(|| panic!("no claim `{name}` in\n{cross}"));
    assert!(found.next().is_none(), "{cross}");
    label
}

/// [`ask_once`] over three members: member 0 gets the value and member 2's timer fires at the
/// same time, so the query and the announcement race. Every schedule of every fault set is
/// explored. Checked by `check`.
fn ask_once_cross_check(mut check: EcCheck) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let peers = flow.cluster::<Member>();
    let (value_send, values) = peers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = peers.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _known = ask_once(
        &peers,
        values,
        timeouts,
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
    )
    .sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&peers, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            value_send.send(0, 7);
            timeout_send.send(2, ());
        },
        MAX_FAULTS,
    );
    eprintln!("=== ask_once\nsearch:\n{}\n{cross}", cross.results);
    cross
}

/// The enumeration finds the race: one crash of member 0, between its announcements to
/// members 1 and 2, breaks the `known` claim in the schedules where member 2's query reaches
/// member 1 first. This is the enumeration's side alone, so it guards the oracle the agreement
/// test below relies on. About 0.2 s of enumeration (8 sets, 132 schedules).
#[test]
fn brute_force_finds_the_ask_once_race() {
    use hydrofrac::search::{CrashAt, Fault};

    let cross = ask_once_cross_check(EcCheck::new());
    let label = claim(&cross, "known");
    let b = cross
        .brute
        .label(label.index)
        .unwrap_or_else(|| panic!("the claim was not searched:\n{cross}"));
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
    let set = b.breaking_within(1).expect("a one-crash breaking set");
    let [
        Fault::Crash {
            node,
            at: CrashAt::After(m),
        },
    ] = set.iter().collect::<Vec<_>>()[..]
    else {
        panic!("expected one positional crash, got {set:?}\n{cross}");
    };
    assert!(node.ends_with("@0"), "member 0 crashes: {node}\n{cross}");
    assert_eq!(
        m.to.member,
        Some(1),
        "after its announcement to member 1\n{cross}"
    );
}

/// The search agrees with the enumeration on [`ask_once`]. Molly's rule certified the claim
/// after 2 hypotheses, pruning the breaking crash with a derivation (member 1 answering member
/// 2's query) that only occurs in the schedules where member 1 already knew the value. The
/// program has schedule choices, so the search uses its heuristic (a hypothesis need only kill
/// one schedule's derivations) and finds the crash at its first hypothesis.
#[test]
fn brute_force_agrees_on_ask_once() {
    let cross = ask_once_cross_check(EcCheck::new());
    cross.assert_agrees();
    let label = claim(&cross, "known");
    assert!(
        !cross.results.choices.is_empty(),
        "the program has schedule choices: {cross}"
    );
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the search finds the race: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(!cex.tolerance_certified, "{cross}");
}

/// Schedules sampled per fault set for two-phase commit with cooperative termination. Its
/// schedule space is too large to enumerate: with three participants, one transaction and only
/// participant 2's timer, the failure-free run has 936 schedules, but under one coordinator
/// crash (after its `commit` to participant 0) the exploration passed 104,000 schedules at
/// about 400 per second without finishing; with participants 1 and 2's timers, it passed
/// 66,000 (the failure-free run: 1,872). Bolero's random engine draws fresh seeds from the OS
/// for every iteration (`BOLERO_RANDOM_SEED` only adds one fixed input), so the sample is not
/// reproducible; the breaking set splits the participants in about 40% of schedules, so 64
/// samples miss it with probability about 10^-14.
const TWO_PC_SCHEDULES: usize = 64;

/// Two-phase commit (`ctp`: with cooperative termination) over `participants` participants,
/// one transaction, every participant's timer fired once the transaction settles (the
/// harness of `ec_two_pc_hydrofrac`). `samples`: schedules per fault set, or every schedule.
/// `races`: checked by [`EcCheck::races`].
fn two_pc_cross_check(
    ctp: bool,
    participants_count: u32,
    samples: Option<usize>,
    races: bool,
) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        participants_count as usize,
        txns,
        timeouts,
        ctp,
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** external input, consumed as it arrives */),
    )
    .sim_cluster_output();

    let check = if races {
        EcCheck::races()
    } else {
        EcCheck::new()
    };
    let mut check = match samples {
        Some(n) => check.sample_schedules(n),
        None => check,
    };
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&participants, participants_count as usize),
        )
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            txn_send.send(Txn(1));
            hydro_lang::sim::compiled::quiesce().await;
            timeout_send.send_many((0..participants_count).map(|p| (p, ())));
        },
        MAX_FAULTS,
    );
    eprintln!(
        "=== two_pc (ctp: {ctp}, {participants_count} participants, samples: {samples:?}, races: {races})\nsearch:\n{}\n{cross}",
        cross.results
    );
    cross
}

/// The enumeration finds the two-crash violation of cooperative termination with three
/// participants (the coordinator after its `commit` to participant 0, and participant 0 after
/// answering participant 1) on sampled schedules. The enumeration's side alone, as above.
/// About 36 s of enumeration (200 sets, 12,800 schedules).
#[test]
fn brute_force_finds_the_two_pc_cooperative_termination_violation() {
    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES), false);
    let label = claim(&cross, "committed");
    let b = cross
        .brute
        .label(label.index)
        .unwrap_or_else(|| panic!("committed was not searched:\n{cross}"));
    assert_eq!(b.min_crashes(), Some(2), "{cross}");
    let set = b.breaking_within(2).expect("a two-crash breaking set");
    let nodes: Vec<&String> = set
        .iter()
        .filter_map(hydrofrac::search::Fault::crashed_node)
        .collect();
    assert!(
        nodes.iter().any(|n| n.starts_with("Coordinator")),
        "the coordinator is one of the crashes: {set:?}\n{cross}"
    );
}

/// The search agrees with the enumeration on two-phase commit with cooperative termination
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
}

// The races mode (docs/design/hydrofrac-races.md §4.4) on the same programs. Each test prints
// the search's cost next to the enumeration's.

/// Prints the races search's and the enumeration's cost (sets, schedules) for every label.
fn costs(name: &str, cross: &CrossCheck) {
    for l in cross.results.results() {
        if l.stats.hypotheses == 0 {
            continue;
        }
        eprintln!(
            "COST {name} / {}: races {} sets, {} schedules ({:?}); brute force {} sets, {} \
             schedules",
            l.name(),
            l.stats.hypotheses,
            l.stats.schedules,
            l.stats.races,
            cross.brute.sets,
            cross.brute.schedules,
        );
    }
}

#[test]
fn races_agree_on_a_plain_broadcast() {
    let cross = plain_reliable_broadcast(EcCheck::races());
    costs("reliable_broadcast::broadcast (plain)", &cross);
    agree("races: reliable_broadcast::broadcast (plain)", &cross);
    for b in &cross.brute.labels {
        let verdict = &cross.results.results()[b.index].verdict;
        let cex = verdict
            .counterexample()
            .unwrap_or_else(|| panic!("{}: a mid fan-out crash breaks it: {cross}", b.name));
        assert_eq!(cex.crashes(), 1, "{cross}");
    }
}

#[test]
fn races_agree_on_reliable_broadcast_closed() {
    let cross = reliable_broadcast_closed_cross_check(EcCheck::races());
    costs("reliable_broadcast_closed", &cross);
    agree("races: reliable_broadcast_closed", &cross);
}

/// The races mode finds the `ask_once` race: the crash of member 0 right after its announcement
/// to member 1 spares (1, 7) under the order-free crash rule and kills (2, 7)'s only robust
/// derivation, and one of its schedules splits the members.
#[test]
fn races_agree_on_ask_once() {
    let cross = ask_once_cross_check(EcCheck::races());
    costs("ask_once", &cross);
    cross.assert_agrees();
    let label = claim(&cross, "known");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the races mode finds the race: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
}

/// Two-phase commit with cooperative termination, three participants, sampled: every
/// derivation of `committed` is fragile, so nothing is certified.
#[test]
fn races_agree_on_two_pc_cooperative_termination() {
    let cross = two_pc_cross_check(true, 3, Some(TWO_PC_SCHEDULES), true);
    costs("two_pc ctp (3 participants, sampled)", &cross);
    cross.assert_agrees();
    let label = claim(&cross, "committed");
    assert!(
        !matches!(label.verdict, hydrofrac::Verdict::Certified { .. }),
        "sampled schedules never certify: {cross}"
    );
}

// Several payloads sent together, so the `nondet!` batches have something to split: one payload
// gives a single failure-free schedule, and brute force then runs one schedule per set too.
// `hydro_std`'s `reliable_broadcast_closed` has no schedule choice at all (one failure-free
// schedule however many payloads); `reliable_broadcast::broadcast` batches, and branches.
// Each test runs lite's check and the races mode, each cross-checked, and prints a `TABLE` line
// per label (run with `--nocapture`).

/// A short rendering of a verdict, for the comparison tables.
fn verdict_summary(verdict: &hydrofrac::Verdict) -> String {
    use hydrofrac::Verdict;
    use hydrofrac::search::Tolerance;
    let tolerance = |t: &Tolerance| match t {
        Tolerance::All => "all".to_owned(),
        Tolerance::UpTo(k) => format!("up to {k}"),
    };
    match verdict {
        Verdict::Certified { tolerates } => format!("certified ({})", tolerance(tolerates)),
        Verdict::Counterexample(cex) => format!(
            "counterexample, {} crash(es), {} fault(s)",
            cex.crashes(),
            cex.faults.len()
        ),
        Verdict::Exhausted { tolerates, .. } => format!("exhausted ({})", tolerance(tolerates)),
        Verdict::Budget { .. } => "budget".to_owned(),
        Verdict::Vacuous(_) => "vacuous".to_owned(),
        other => format!("{other:?}"),
    }
}

/// Runs `build` with lite's check and with the races mode, asserts both agree with brute force,
/// and prints one comparison line per searched label. `build` returns the cross-check and the
/// program's failure-free schedule count.
fn compare(name: &str, build: impl Fn(EcCheck) -> (CrossCheck, usize)) {
    let start = std::time::Instant::now();
    let (lite, schedules) = build(EcCheck::new());
    let lite_wall = start.elapsed();
    let start = std::time::Instant::now();
    let (races, _) = build(EcCheck::races());
    let races_wall = start.elapsed();
    eprintln!(
        "=== {name}: {schedules} failure-free schedules\nlite:\n{}\nraces:\n{}\n{races}",
        lite.results, races.results
    );
    for (l, r) in lite.results.results().iter().zip(races.results.results()) {
        let Some(b) = races.brute.label(r.index) else {
            continue;
        };
        let notes = r.stats.races.unwrap_or_default();
        eprintln!(
            "TABLE {name} | {} | races: {} — {} sets / {} schedules (one-schedule {}, early stops \
             {}, enumerated {}) | lite: {} — {} sets / {} schedules | brute force: {} sets / {} \
             schedules, min crashes {:?} | failure-free schedules {schedules}",
            r.name(),
            verdict_summary(&r.verdict),
            r.stats.hypotheses,
            r.stats.schedules,
            notes.single_schedule_sets,
            notes.early_stops,
            notes.enumerated_sets,
            verdict_summary(&l.verdict),
            l.stats.hypotheses,
            l.stats.schedules,
            races.brute.sets,
            races.brute.schedules,
            b.min_crashes(),
        );
    }
    eprintln!(
        "TIME {name}: lite {lite_wall:.1?}, races {races_wall:.1?} (each with its brute force)"
    );
    lite.assert_agrees();
    races.assert_agrees();
}

/// Every failure-free schedule of `sim` under `body` (the harness then quiesces), counted.
fn failure_free_schedules(
    sim: &hydro_lang::sim::compiled::CompiledSim,
    body: &mut impl AsyncFnMut(),
) -> usize {
    let mut body = std::panic::AssertUnwindSafe(body);
    sim.exhaustive(async || {
        (*body)().await;
        hydro_lang::sim::compiled::quiesce().await;
    })
}

/// `reliable_broadcast_closed` over three members, with `payloads` sent together.
fn reliable_broadcast_closed_with(mut check: EcCheck, payloads: &[u32]) -> (CrossCheck, usize) {
    use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = reliable_broadcast_closed(input, &members).sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let mut body = async || {
        for p in payloads {
            send.send(*p);
        }
    };
    let schedules = failure_free_schedules(&sim, &mut body);
    (check.cross_check(&sim, &mut body, MAX_FAULTS), schedules)
}

/// `reliable_broadcast::broadcast` over three members, with `payloads` sent together.
fn reliable_broadcast_with(
    mut check: EcCheck,
    echo: bool,
    members_count: usize,
    payloads: &[u32],
) -> (CrossCheck, usize) {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        echo,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, members_count))
        .compiled();
    let mut body = async || {
        for p in payloads {
            send.send(Payload(*p));
        }
    };
    let schedules = failure_free_schedules(&sim, &mut body);
    (check.cross_check(&sim, &mut body, MAX_FAULTS), schedules)
}

#[test]
fn both_agree_on_reliable_broadcast_closed_with_two_payloads() {
    compare("reliable_broadcast_closed, 2 payloads", |check| {
        reliable_broadcast_closed_with(check, &[1, 2])
    });
}

#[test]
fn both_agree_on_a_plain_broadcast_with_two_payloads() {
    compare(
        "reliable_broadcast::broadcast (plain), 2 payloads",
        |check| reliable_broadcast_with(check, false, 3, &[1, 2]),
    );
}

#[test]
fn both_agree_on_reliable_broadcast_closed_with_three_payloads() {
    compare("reliable_broadcast_closed, 3 payloads", |check| {
        reliable_broadcast_closed_with(check, &[1, 2, 3])
    });
}

#[test]
fn both_agree_on_a_plain_broadcast_with_three_payloads() {
    compare(
        "reliable_broadcast::broadcast (plain), 3 payloads",
        |check| reliable_broadcast_with(check, false, 3, &[1, 2, 3]),
    );
}

#[test]
fn both_agree_on_an_echo_broadcast_over_two_members() {
    compare(
        "reliable_broadcast::broadcast (echo), 2 members, 1 payload",
        |check| reliable_broadcast_with(check, true, 2, &[1]),
    );
}

/// Too large to cross-check: lite's exhaustive search and the enumeration together did not
/// finish within 30 minutes.
#[test]
#[ignore = "did not finish within 30 minutes"]
fn both_agree_on_an_echo_broadcast_over_two_members_with_two_payloads() {
    compare(
        "reliable_broadcast::broadcast (echo), 2 members, 2 payloads",
        |check| reliable_broadcast_with(check, true, 2, &[1, 2]),
    );
}

/// Races mode alone (no enumeration, which cannot finish here: the failure-free run alone has
/// more than 160,000 schedules) on the echo broadcast over three members with one payload. The
/// program is fragile-free, so each fault set runs on one schedule. Prints the verdicts and
/// costs; asserts that `deliveries` is certified.
#[test]
fn races_alone_on_an_echo_broadcast_over_three_members() {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        true,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    let mut check = EcCheck::races();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        send.send(Payload(42));
    });
    eprintln!("=== races alone, echo broadcast, 3 members, 1 payload ({:?})\n{results}", start.elapsed());
    for l in results.results() {
        eprintln!(
            "RACES-ALONE {} | certified: {} | {} sets / {} schedules | {:?}",
            l.plan.collection.display_name(),
            matches!(l.verdict, hydrofrac::check::Verdict::Certified { .. }),
            l.stats.hypotheses,
            l.stats.schedules,
            l.stats.races
        );
    }
    let deliveries = results
        .results()
        .iter()
        .find(|l| l.plan.collection.display_name() == "deliveries")
        .expect("a deliveries label");
    assert!(
        matches!(deliveries.verdict, hydrofrac::check::Verdict::Certified { .. }),
        "{results}"
    );
}
