use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::*;

/// A message: its channel and payload.
type M = (&'static str, u32);

fn lossy(channel: &'static str, payload: u32) -> Dependency<M> {
    Dependency {
        message: (channel, payload),
        sender: None,
        lossy: true,
    }
}

fn reliable(channel: &'static str, payload: u32, sender: &str, ordinal: u64) -> Dependency<M> {
    Dependency {
        message: (channel, payload),
        sender: Some((sender.to_owned(), ordinal)),
        lossy: false,
    }
}

fn no_sends() -> Rc<SendSequences<M>> {
    Rc::new(SendSequences::new())
}

fn crash(node: &str) -> Fault<M> {
    Fault::crash_after(node, None)
}

fn set<const N: usize>(faults: [Fault<M>; N]) -> FaultSet<M> {
    faults.into_iter().collect()
}

fn solve(
    clauses: &[Clause<M>],
    tried: &BTreeSet<FaultSet<M>>,
    bounds: &CrashBounds,
) -> Option<FaultSet<M>> {
    hitting_set(
        &universe(clauses, []),
        clauses,
        tried,
        &[],
        bounds,
        Backend::Microlp,
    )
}

/// Every hypothesis the solver proposes under `bounds`, in order, until none is left.
fn all_hypotheses(clauses: &[Clause<M>], bounds: &CrashBounds) -> Vec<FaultSet<M>> {
    let mut tried = BTreeSet::new();
    let mut out = vec![];
    while let Some(h) = solve(clauses, &tried, bounds) {
        assert!(tried.insert(h.clone()), "a hypothesis is proposed once");
        out.push(h);
        assert!(out.len() < 1000, "the enumeration terminates");
    }
    out
}

fn crashed_nodes(h: &FaultSet<M>) -> BTreeSet<&str> {
    h.iter()
        .filter_map(Fault::crashed_node)
        .map(String::as_str)
        .collect()
}

#[test]
fn hitting_sets_grow_by_size_and_skip_tried() {
    let clauses = vec![
        Clause::new([lossy("req", 1), lossy("ack", 1)], no_sends()),
        Clause::new([lossy("req", 2), lossy("ack", 2)], no_sends()),
    ];
    let mut tried = BTreeSet::new();
    // Every minimal hitting set (one literal per clause) comes first, each exactly once...
    for _ in 0..4 {
        let h = solve(&clauses, &tried, &CrashBounds::cap(0)).unwrap();
        assert_eq!(h.len(), 2);
        assert!(tried.insert(h));
    }
    // ...and only then the non-minimal ones.
    let h = solve(&clauses, &tried, &CrashBounds::cap(0)).unwrap();
    assert_eq!(h.len(), 3);
}

#[test]
fn empty_clause_has_no_hitting_set() {
    let clauses = vec![
        Clause::new([lossy("req", 1)], no_sends()),
        Clause::new([], no_sends()),
    ];
    assert_eq!(
        solve(&clauses, &BTreeSet::new(), &CrashBounds::cap(0)),
        None
    );
}

/// An empty derivation observed under a crash says only that the fact holds in worlds with that
/// crash: any hypothesis without it counts as hitting the clause.
#[test]
fn an_empty_derivation_under_a_crash_binds_only_worlds_with_that_crash() {
    let sends = Rc::new(SendSequences::from([("s".to_owned(), vec![("to", 1)])]));
    let mut under_crash = Clause::new([], sends.clone());
    under_crash.premises = set([crash("s")]);
    let clauses = vec![Clause::new([reliable("to", 1, "s", 0)], sends), under_crash];
    // The only candidate is `crash(s)`, which is the premise: no hypothesis remains.
    assert_eq!(
        solve(&clauses, &BTreeSet::new(), &CrashBounds::default()),
        None
    );
    // With an omission available as well, the omission alone is proposed.
    let mut clauses = clauses;
    clauses[0].deps.insert(lossy("other", 2));
    assert_eq!(
        solve(&clauses, &BTreeSet::new(), &CrashBounds::default()),
        Some(set([Fault::Omit(("other", 2))]))
    );
}

/// A premise that is not a candidate can never be selected: the clause applies to no hypothesis.
#[test]
fn a_clause_whose_premise_is_not_a_candidate_is_dropped() {
    let mut clause = Clause::new([lossy("req", 1)], no_sends());
    clause.premises = set([crash("elsewhere")]);
    let clauses = vec![clause];
    // Nothing binds, so the empty set is the minimum; once tried, the omission follows.
    assert_eq!(
        solve(&clauses, &BTreeSet::new(), &CrashBounds::default()),
        Some(FaultSet::new())
    );
}

/// A crash after message `m` kills every send after `m`'s position in the run, so one crash can
/// hit a clause of several messages from the same sender, and the weakest such crash (after the
/// latest message) is tried first.
#[test]
fn crash_kills_later_sends_and_respects_the_cap() {
    // The sender's fan-out to two members: sends `m0` then `m1`.
    let (m0, m1) = (("to@0", 42), ("to@1", 42));
    let sends = Rc::new(SendSequences::from([("s".to_owned(), vec![m0, m1])]));
    let clauses = vec![Clause::new(
        [reliable("to@0", 42, "s", 0), reliable("to@1", 42, "s", 1)],
        sends.clone(),
    )];
    assert_eq!(
        solve(&clauses, &BTreeSet::new(), &CrashBounds::cap(0)),
        None,
        "a reliable channel admits no omission; with no crashes allowed nothing can be done"
    );
    // Weakest first: the crash after `m0` (killing only `m1`) is tried before the crash before
    // any send (killing both).
    let first = solve(&clauses, &BTreeSet::new(), &CrashBounds::cap(1)).unwrap();
    let later = Fault::crash_after("s", Some(m0));
    assert_eq!(first, set([later.clone()]));
    let pre_heal = &clauses[0].pre_heal_sends;
    let killed: Vec<bool> = clauses[0]
        .deps
        .iter()
        .map(|d| d.killed_by(&later, &sends, pre_heal))
        .collect();
    assert_eq!(killed, [false, true]);
    assert!(
        clauses[0]
            .deps
            .iter()
            .all(|d| d.killed_by(&crash("s"), &sends, pre_heal))
    );
    // In a run where `m0` was never sent, a crash after it never fires.
    let other_run = SendSequences::from([("s".to_owned(), vec![m1])]);
    let other_pre_heal = PreHealSends::from([("s".to_owned(), 1)]);
    assert!(!reliable("to@1", 42, "s", 0).killed_by(&later, &other_run, &other_pre_heal));
}

/// Every failure happens before the network heals: a crash point named by a message that this
/// run sends only after the heal point does not fire here and kills nothing, while a pre-heal
/// crash point kills every later send, including the post-heal ones. The clause must say both.
/// Only pre-heal crash points are proposed.
#[test]
fn crash_points_fire_only_before_the_heal_and_kill_every_later_send() {
    // The sender's run: `a`, `b` before the heal; `c`, `d` after it.
    let (a, b, c, d) = (("to", 1), ("to", 2), ("to", 3), ("to", 4));
    let sends = Rc::new(SendSequences::from([("s".to_owned(), vec![a, b, c, d])]));
    let pre_heal = PreHealSends::from([("s".to_owned(), 2)]);
    let dep_b = reliable("to", 2, "s", 1);
    let dep_d = reliable("to", 4, "s", 3);
    let after = |m: M| Fault::crash_after("s", Some(m));
    // A pre-heal crash point kills every later send, post-heal ones included.
    assert!(dep_d.killed_by(&after(a), &sends, &pre_heal));
    assert!(dep_d.killed_by(&after(b), &sends, &pre_heal));
    assert!(dep_b.killed_by(&after(a), &sends, &pre_heal));
    assert!(!dep_b.killed_by(&after(b), &sends, &pre_heal));
    // A crash point named by a post-heal send of this run (minted in some other run where `c`
    // was pre-heal) does not fire here.
    assert!(!dep_d.killed_by(&after(c), &sends, &pre_heal));
    assert!(!dep_d.killed_by(&after(d), &sends, &pre_heal));
    // Had `c` been pre-heal, the crash after it would kill `d`.
    let later_heal = PreHealSends::from([("s".to_owned(), 3)]);
    assert!(dep_d.killed_by(&after(c), &sends, &later_heal));
    // Proposal: the weakest crash point that kills `d` is the one right after the last pre-heal
    // send, `b`; neither `c` nor `d` is minted as a crash point.
    assert_eq!(dep_d.candidate_faults(&sends, &pre_heal), vec![after(b)]);
    let clause = Clause {
        deps: [dep_d].into_iter().collect(),
        sends,
        pre_heal_sends: Rc::new(pre_heal),
        premises: BTreeSet::new(),
    };
    assert!(clause.hit_by(&after(a)));
    assert!(clause.hit_by(&after(b)));
    assert!(!clause.hit_by(&after(c)));
    assert!(!clause.hit_by(&after(d)));
}

/// Three members each sending one message over reliable channels: a clause of three
/// dependencies, each killable only by its sender's crash before any send.
fn three_senders() -> Vec<Clause<M>> {
    let sends = Rc::new(SendSequences::from([
        ("m@0".to_owned(), vec![("to", 1)]),
        ("m@1".to_owned(), vec![("to", 2)]),
        ("m@2".to_owned(), vec![("to", 3)]),
    ]));
    vec![Clause::new(
        [
            reliable("to", 1, "m@0", 0),
            reliable("to", 2, "m@1", 0),
            reliable("to", 3, "m@2", 0),
        ],
        sends,
    )]
}

fn program_of(nodes: usize) -> CrashBounds {
    CrashBounds {
        nodes,
        ..CrashBounds::default()
    }
}

/// Without a cap the enumeration is every admissible crash set by size, except the one that
/// crashes every node of the program (here the three senders are the whole program): with
/// every node dead, nothing is left to check.
#[test]
fn unbounded_never_proposes_crashing_every_node() {
    let proposed = all_hypotheses(&three_senders(), &program_of(3));
    // Singles first, then pairs; the triple is never proposed.
    assert_eq!(proposed.len(), 6, "{proposed:?}");
    assert!(proposed[..3].iter().all(|h| h.len() == 1), "{proposed:?}");
    assert!(proposed[3..].iter().all(|h| h.len() == 2), "{proposed:?}");
    assert!(
        proposed.iter().all(|h| crashed_nodes(h).len() < 3),
        "{proposed:?}"
    );
}

/// Crashing every node a candidate names is proposed when another node remains: the bound is
/// over the program's nodes.
#[test]
fn crashing_every_candidate_node_is_proposed_when_another_node_remains() {
    // A fourth node (a receiver that no derivation depends on, say) is part of the program.
    let proposed = all_hypotheses(&three_senders(), &program_of(4));
    assert_eq!(proposed.len(), 7, "{proposed:?}");
    assert_eq!(crashed_nodes(&proposed[6]).len(), 3, "{proposed:?}");
    // A single sender among others: its crash is admissible.
    let sends = Rc::new(SendSequences::from([("s".to_owned(), vec![("to", 1)])]));
    let clauses = vec![Clause::new([reliable("to", 1, "s", 0)], sends)];
    let proposed = all_hypotheses(&clauses, &program_of(2));
    assert_eq!(proposed.len(), 1, "{proposed:?}");
    assert_eq!(crashed_nodes(&proposed[0]), BTreeSet::from(["s"]));
}

/// Over a three-member checked cluster, no hypothesis crashes two of its members: one survivor
/// agrees with itself. Crashes outside the cluster do not count against that cap.
#[test]
fn never_crashes_all_but_one_member_of_the_checked_cluster() {
    let mut clauses = three_senders();
    // A source outside the cluster whose send the clause also depends on.
    let sends = Rc::new(SendSequences::from([
        ("src".to_owned(), vec![("in", 9)]),
        ("m@0".to_owned(), vec![("to", 1)]),
    ]));
    clauses.push(Clause::new(
        [reliable("in", 9, "src", 0), reliable("to", 1, "m@0", 0)],
        sends,
    ));
    let cluster = CheckedCluster::new(["m@0", "m@1", "m@2"].map(String::from));
    assert_eq!(cluster.cap, 1);
    let bounds = CrashBounds {
        cap: None,
        nodes: 4,
        checked: Some(cluster),
    };
    let proposed = all_hypotheses(&clauses, &bounds);
    assert!(!proposed.is_empty());
    for h in &proposed {
        let members = crashed_nodes(h)
            .into_iter()
            .filter(|n| n.starts_with("m@"))
            .count();
        assert!(
            members <= 1,
            "{h:?} crashes {members} members of the checked cluster"
        );
    }
    // `src` is outside the cluster: a member's crash together with it is proposed.
    assert!(
        proposed
            .iter()
            .any(|h| crashed_nodes(h).contains("src") && crashed_nodes(h).len() == 2),
        "{proposed:?}"
    );
}

/// An explicit cap still bounds, on top of the vacuity constraints.
#[test]
fn an_explicit_crash_cap_still_bounds() {
    let clauses = three_senders();
    let one = CrashBounds {
        cap: Some(1),
        ..program_of(3)
    };
    let proposed = all_hypotheses(&clauses, &one);
    assert_eq!(proposed.len(), 3, "{proposed:?}");
    assert!(proposed.iter().all(|h| h.len() == 1), "{proposed:?}");
    assert!(all_hypotheses(&clauses, &CrashBounds::cap(0)).is_empty());
    // A cap above the vacuity bound changes nothing.
    let two = CrashBounds {
        cap: Some(2),
        ..program_of(3)
    };
    assert_eq!(
        all_hypotheses(&clauses, &two),
        all_hypotheses(&clauses, &program_of(3))
    );
}

#[test]
fn no_superset_of_a_vacuous_set_is_proposed() {
    let clauses = vec![
        Clause::new([lossy("req", 1), lossy("ack", 1)], no_sends()),
        Clause::new([lossy("req", 2), lossy("ack", 2)], no_sends()),
    ];
    let vacuous = vec![set([Fault::Omit(("req", 1))])];
    let mut tried = BTreeSet::new();
    while let Some(h) = hitting_set(
        &universe(&clauses, []),
        &clauses,
        &tried,
        &vacuous,
        &CrashBounds::default(),
        Backend::Microlp,
    ) {
        assert!(!h.contains(&Fault::Omit(("req", 1))), "{h:?}");
        tried.insert(h);
    }
    // Every subset of the three other omissions that hits both clauses.
    assert_eq!(tried.len(), 3, "{tried:?}");
}

/// The tie-break never trades cardinality: every `k`-set costs less than every `(k+1)`-set. With
/// the research branch's `0.5 / n` per position, a 3-set of the latest positions in a universe of
/// 100 costs `3 + 0.005 · (97 + 98 + 99) = 4.47`, more than the 4-set of the earliest positions,
/// `4 + 0.005 · 6 = 4.03`.
#[test]
fn the_tie_break_never_trades_cardinality() {
    for n in 1..=300 {
        let w: Vec<f64> = weights(n).collect();
        for k in 0..n {
            // The heaviest k-set against the lightest (k+1)-set.
            let heaviest_k: f64 = w[n - k..].iter().sum();
            let lightest_k1: f64 = w[..=k].iter().sum();
            assert!(heaviest_k < lightest_k1, "n = {n}, k = {k}");
        }
    }
}

#[test]
fn the_minimum_cardinality_set_wins_over_cheaper_positions() {
    // A universe of 100 omissions, ordered by payload. The unique 3-set hitting every clause is
    // {97, 98, 99}; {0, 1, 2, 3} is a 4-set of early positions.
    let ch = "c";
    let clauses: Vec<Clause<M>> = [(0, 97), (1, 98), (2, 99), (3, 97), (3, 98), (3, 99)]
        .into_iter()
        .map(|(a, b)| Clause::new([lossy(ch, a), lossy(ch, b)], no_sends()))
        .collect();
    let extra: Vec<Fault<M>> = (0..100).map(|p| Fault::Omit((ch, p))).collect();
    let universe = universe(&clauses, &extra);
    assert_eq!(universe.len(), 100);
    let h = hitting_set(
        &universe,
        &clauses,
        &BTreeSet::new(),
        &[],
        &CrashBounds::default(),
        Backend::Microlp,
    )
    .unwrap();
    assert_eq!(
        h,
        set([
            Fault::Omit((ch, 97)),
            Fault::Omit((ch, 98)),
            Fault::Omit((ch, 99))
        ])
    );
}

#[test]
fn a_tried_set_outside_the_universe_constrains_nothing() {
    let clauses = vec![Clause::new([lossy("req", 1)], no_sends())];
    let tried = BTreeSet::from([set([Fault::Omit(("req", 1)), Fault::Omit(("gone", 7))])]);
    assert_eq!(
        solve(&clauses, &tried, &CrashBounds::default()),
        Some(set([Fault::Omit(("req", 1))]))
    );
}

// The sweep.

/// Drives `sweep` as if no hypothesis broke the label or taught anything new, recording each
/// proposal with the bound it was proposed at.
fn drain(sweep: &mut Sweep<M>, clauses: &[Clause<M>]) -> (Vec<(usize, FaultSet<M>)>, Next<M>) {
    let mut proposed = vec![];
    loop {
        match sweep.next(clauses) {
            Next::Try(h) => {
                proposed.push((sweep.crash_bound(), h));
                assert!(proposed.len() < 1000);
            }
            done => return (proposed, done),
        }
    }
}

#[test]
fn the_sweep_raises_the_bound_one_crash_at_a_time() {
    let mut sweep = Sweep::new(program_of(3));
    let (proposed, done) = drain(&mut sweep, &three_senders());
    assert_eq!(done, Next::Certified(Tolerance::All));
    // Nothing at zero crashes (no omissions), then three singles, then three pairs; the
    // vacuity bound stops it at 2.
    let bounds: Vec<usize> = proposed.iter().map(|(b, _)| *b).collect();
    assert_eq!(bounds, [1, 1, 1, 2, 2, 2]);
    // Every set proposed at bound `k` holds exactly `k` crashes.
    assert!(
        proposed.iter().all(|(b, h)| crash_count(h) == *b),
        "{proposed:?}"
    );
    assert_eq!(sweep.tolerated(), Some(2));
    assert_eq!(sweep.hypotheses(), 7, "the probe and six hypotheses");
}

#[test]
fn the_sweep_stops_at_the_cap() {
    let mut sweep = Sweep::new(CrashBounds {
        cap: Some(1),
        ..program_of(5)
    });
    let (proposed, done) = drain(&mut sweep, &three_senders());
    assert_eq!(proposed.len(), 3);
    assert_eq!(done, Next::Certified(Tolerance::UpTo(1)));
    // A cap at or above the vacuity bound is no cap.
    let mut sweep = Sweep::new(CrashBounds {
        cap: Some(2),
        ..program_of(3)
    });
    assert_eq!(
        drain(&mut sweep, &three_senders()).1,
        Next::Certified(Tolerance::All)
    );
}

/// Once the bound covers every crash the candidates can hold, a higher bound admits nothing new.
#[test]
fn the_sweep_stops_when_more_crashes_cannot_kill_more() {
    // An unbounded program (no every-node bound): the three senders saturate at 3.
    let mut sweep = Sweep::new(CrashBounds::default());
    let (proposed, done) = drain(&mut sweep, &three_senders());
    assert_eq!(done, Next::Certified(Tolerance::All));
    assert_eq!(proposed.len(), 7);
    assert_eq!(sweep.tolerated(), Some(3));
    // The checked-cluster cap saturates it earlier: one member at most.
    let mut sweep = Sweep::new(CrashBounds {
        checked: Some(CheckedCluster::new(["m@0", "m@1", "m@2"].map(String::from))),
        ..CrashBounds::default()
    });
    let (proposed, done) = drain(&mut sweep, &three_senders());
    assert_eq!(done, Next::Certified(Tolerance::All));
    assert_eq!(proposed.len(), 3);
    assert_eq!(sweep.tolerated(), Some(1));
}

/// Learned clauses carry over a raise. At bound 0 only an omission can be tried; once the run
/// under it shows a derivation only a crash kills, the sweep raises to 1 and the next hypothesis
/// covers both clauses.
#[test]
fn clauses_learned_at_one_bound_carry_over_to_the_next() {
    let sends = Rc::new(SendSequences::from([("s".to_owned(), vec![("to", 1)])]));
    let mut clauses = vec![Clause::new([lossy("in", 1)], sends.clone())];
    let mut sweep = Sweep::new(program_of(3));
    assert_eq!(
        sweep.next(&clauses),
        Next::Try(set([Fault::Omit(("in", 1))]))
    );
    assert_eq!(sweep.crash_bound(), 0);
    // Under that omission the fact was derived anyway, from a reliable delivery.
    clauses.push(Clause::new([reliable("to", 1, "s", 0)], sends));
    assert_eq!(
        sweep.next(&clauses),
        Next::Try(set([Fault::Omit(("in", 1)), crash("s")]))
    );
    assert_eq!(sweep.crash_bound(), 1);
    assert_eq!(sweep.tolerated(), Some(0));
}

#[test]
fn an_unkillable_derivation_certifies_at_once() {
    let mut sweep = Sweep::new(program_of(3));
    let clauses = vec![Clause::new([], no_sends())];
    assert_eq!(sweep.next(&clauses), Next::Certified(Tolerance::All));
    assert_eq!(sweep.hypotheses(), 1);
}

#[test]
fn the_budget_counts_the_probe() {
    let mut sweep = Sweep::new(program_of(3)).with_budget(3);
    let (proposed, done) = drain(&mut sweep, &three_senders());
    assert_eq!(proposed.len(), 2);
    assert_eq!(done, Next::Budget);
}

#[test]
fn a_vacuous_hypothesis_excludes_its_supersets_across_bounds() {
    let mut sweep = Sweep::new(program_of(4));
    sweep.mark_vacuous(set([crash("m@0")]));
    let (proposed, _) = drain(&mut sweep, &three_senders());
    assert!(
        proposed.iter().all(|(_, h)| !h.contains(&crash("m@0"))),
        "{proposed:?}"
    );
    // {m@1}, {m@2}, {m@1, m@2}.
    assert_eq!(proposed.len(), 3);
}

/// With no clauses and explicit candidates, the sweep is a blind enumeration of every admissible
/// set, by crash count and then by size: the brute-force cross-check's engine.
#[test]
fn with_no_clauses_the_sweep_enumerates_every_admissible_set() {
    let mut sweep = Sweep::new(program_of(3));
    sweep.add_candidates([
        Fault::Omit(("a", 1)),
        Fault::Omit(("b", 1)),
        crash("n@0"),
        crash("n@1"),
    ]);
    let (proposed, done) = drain(&mut sweep, &[]);
    assert_eq!(done, Next::Certified(Tolerance::All));
    // 2^4 subsets, minus the probe's empty set.
    assert_eq!(proposed.len(), 15, "{proposed:?}");
    let mut counts: Vec<usize> = proposed.iter().map(|(_, h)| crash_count(h)).collect();
    assert!(counts.is_sorted(), "{counts:?}");
    counts.dedup();
    assert_eq!(counts, [0, 1, 2]);
}

// Benchmarks.

/// A small deterministic PRNG (xorshift64), so the benchmark instance needs no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A synthetic instance shaped like the largest research searches: `nodes` senders each with
/// `sends` messages, half of them on lossy channels, and `clauses` derivations of 4 to 12
/// deliveries each.
fn synthetic(nodes: usize, sends: usize, clauses: usize, seed: u64) -> Vec<Clause<M>> {
    let mut rng = Rng(seed);
    let names: Vec<String> = (0..nodes).map(|n| format!("n@{n}")).collect();
    let sequences: Rc<SendSequences<M>> = Rc::new(
        names
            .iter()
            .enumerate()
            .map(|(n, name)| {
                (
                    name.clone(),
                    (0..sends).map(|i| ("m", (n * sends + i) as u32)).collect(),
                )
            })
            .collect(),
    );
    (0..clauses)
        .map(|_| {
            let deps = (0..4 + rng.below(9)).map(|_| {
                let n = rng.below(nodes);
                let i = rng.below(sends);
                Dependency {
                    message: ("m", (n * sends + i) as u32),
                    sender: Some((names[n].clone(), i as u64)),
                    lossy: i.is_multiple_of(2),
                }
            });
            Clause::new(deps, sequences.clone())
        })
        .collect()
}

/// Runs a sweep on `clauses` for up to `hypotheses` proposals, as if none broke the label; returns
/// the proposals' sizes and the time spent.
fn time_sweep(
    clauses: &[Clause<M>],
    hypotheses: usize,
    backend: Backend,
) -> (Vec<usize>, Duration) {
    let mut sweep = Sweep::new(CrashBounds {
        cap: Some(2),
        nodes: 6,
        checked: None,
    })
    .with_backend(backend)
    .with_budget(hypotheses + 1);
    let start = Instant::now();
    let mut sizes = vec![];
    while let Next::Try(h) = sweep.next(clauses) {
        sizes.push(h.len());
    }
    (sizes, start.elapsed())
}

/// The worst case: the clauses never grow, so every tried set needs an explicit no-good. A real
/// search does not look like this (see [`bench_lean_vs_full_model`]).
///
/// `cargo test -p hydrofrac --release -- --ignored --nocapture --test-threads 1 bench_`
#[test]
#[ignore = "benchmark"]
fn bench_microlp() {
    for (clauses, hypotheses) in [(20, 100), (40, 200), (60, 300)] {
        let instance = synthetic(6, 20, clauses, 0x9e37_79b9_7f4a_7c15);
        let universe = universe(&instance, []).len();
        let (sizes, took) = time_sweep(&instance, hypotheses, Backend::Microlp);
        println!(
            "microlp: {clauses} clauses, {universe} candidates, {} hypotheses: {took:?} ({:?}/hypothesis)",
            sizes.len(),
            took / sizes.len().max(1) as u32
        );
    }
}

/// The solver check (docs/design/hydrofrac-lite.md §7): both backends propose sets of the same
/// size, step by step.
#[cfg(feature = "highs")]
#[test]
fn microlp_and_highs_agree_on_sizes() {
    let instance = synthetic(6, 10, 15, 7);
    let (micro, _) = time_sweep(&instance, 60, Backend::Microlp);
    let (highs, _) = time_sweep(&instance, 60, Backend::Highs);
    assert_eq!(micro, highs);
}

#[cfg(feature = "highs")]
#[test]
#[ignore = "benchmark"]
fn bench_microlp_vs_highs() {
    for (clauses, hypotheses) in [(20, 100), (40, 200), (60, 300)] {
        let instance = synthetic(6, 20, clauses, 0x9e37_79b9_7f4a_7c15);
        let universe = universe(&instance, []).len();
        let (micro, micro_took) = time_sweep(&instance, hypotheses, Backend::Microlp);
        let (highs, highs_took) = time_sweep(&instance, hypotheses, Backend::Highs);
        println!(
            "{clauses} clauses, {universe} candidates, {} hypotheses: microlp {micro_took:?}, highs {highs_took:?}",
            micro.len()
        );
        assert_eq!(micro, highs);
    }
}

// Lazy no-goods.

/// The cost of `set` under the solver's weights for `universe`.
fn cost(universe: &[Fault<M>], set: &FaultSet<M>) -> f64 {
    let weights: Vec<f64> = weights(universe.len()).collect();
    set.iter()
        .map(|f| weights[universe.iter().position(|u| u == f).expect("a candidate")])
        .sum()
}

/// A derivation the run of `tried` could have observed: deliveries that no fault of `tried`
/// kills, chosen by `rng`, observed under `tried`'s crashes. `None` when the pool has nothing
/// `tried` spares.
fn surviving_clause(
    pool: &[Dependency<M>],
    sends: &Rc<SendSequences<M>>,
    tried: &FaultSet<M>,
    rng: &mut Rng,
) -> Option<Clause<M>> {
    let pre_heal: PreHealSends = sends.iter().map(|(n, s)| (n.clone(), s.len())).collect();
    let spared: Vec<&Dependency<M>> = pool
        .iter()
        .filter(|d| !tried.iter().any(|f| d.killed_by(f, sends, &pre_heal)))
        .collect();
    if spared.is_empty() {
        return None;
    }
    let deps = (0..1 + rng.below(3)).map(|_| spared[rng.below(spared.len())].clone());
    let mut clause = Clause::new(deps, sends.clone());
    clause.premises = tried
        .iter()
        .filter(|f| f.crashed_node().is_some())
        .cloned()
        .collect();
    Some(clause)
}

/// A random program: `nodes` senders with `sends` messages each, half of them lossy.
fn random_pool(
    rng: &mut Rng,
    nodes: usize,
    sends: usize,
) -> (Vec<Dependency<M>>, Rc<SendSequences<M>>) {
    let names: Vec<String> = (0..nodes).map(|n| format!("n@{n}")).collect();
    let sequences: Rc<SendSequences<M>> = Rc::new(
        names
            .iter()
            .enumerate()
            .map(|(n, name)| {
                (
                    name.clone(),
                    (0..sends).map(|i| ("m", (n * sends + i) as u32)).collect(),
                )
            })
            .collect(),
    );
    let pool = (0..nodes * sends)
        .map(|k| {
            let (n, i) = (k / sends, k % sends);
            Dependency {
                message: ("m", k as u32),
                sender: Some((names[n].clone(), i as u64)),
                lossy: rng.below(2) == 0,
            }
        })
        .collect();
    (pool, sequences)
}

/// The lean and the full model have the same feasible region, so at every step of a search they
/// agree on feasibility and on the optimal cost, and the lean solver never re-proposes a tried
/// set. The search is driven in lockstep by the full model's choice, on many small random
/// instances where runs sometimes teach nothing new (no clause), sometimes are vacuous,
/// sometimes yield a derivation under premises that are also hits (a row that always holds), and
/// otherwise yield a derivation the hypothesis does not kill (often a repeat of an earlier one).
#[test]
fn the_lean_model_is_exact() {
    for seed in 1..=80u64 {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let nodes = 2 + rng.below(3);
        let per_node = 2 + rng.below(3);
        let (pool, sends) = random_pool(&mut rng, nodes, per_node);
        let bounds = CrashBounds {
            cap: Some(1 + rng.below(2)),
            nodes: nodes + 1,
            checked: None,
        };
        let mut clauses: Vec<Clause<M>> = (0..1 + rng.below(3))
            .filter_map(|_| surviving_clause(&pool, &sends, &FaultSet::new(), &mut rng))
            .collect();
        let mut tried = BTreeSet::from([FaultSet::new()]);
        let mut vacuous = vec![];
        for step in 0..40 {
            let universe = universe(&clauses, []);
            let solve = |model| {
                ilp::solve(
                    &universe,
                    &clauses,
                    &tried,
                    &vacuous,
                    &bounds,
                    Backend::Microlp,
                    model,
                )
            };
            let (full, lean) = (solve(ilp::Model::Full), solve(ilp::Model::Lean));
            match (&full, &lean) {
                (None, None) => break,
                (Some(f), Some(l)) => {
                    assert!(
                        !tried.contains(l),
                        "seed {seed} step {step}: re-proposed {l:?}"
                    );
                    let (cf, cl) = (cost(&universe, f), cost(&universe, l));
                    assert!(
                        (cf - cl).abs() < 1e-9,
                        "seed {seed} step {step}: costs {cf} vs {cl}"
                    );
                }
                _ => panic!("seed {seed} step {step}: full {full:?}, lean {lean:?}"),
            }
            let chosen = full.unwrap();
            tried.insert(chosen.clone());
            match rng.below(6) {
                0 => {}
                1 => vacuous.push(chosen),
                2 => {
                    // A derivation observed under a crash that also kills one of its deliveries.
                    if let Some(mut clause) =
                        surviving_clause(&pool, &sends, &FaultSet::new(), &mut rng)
                    {
                        let dep = clause.deps.first().unwrap();
                        if let Some((node, _)) = &dep.sender {
                            clause.premises = set([crash(node)]);
                            clauses.push(clause);
                        }
                    }
                }
                _ => clauses.extend(surviving_clause(&pool, &sends, &chosen, &mut rng)),
            }
        }
    }
}

#[test]
fn implied_and_always_satisfied_cover_rows_are_dropped() {
    let sends: Rc<SendSequences<M>> =
        Rc::new(SendSequences::from([("n".to_owned(), vec![("r", 1)])]));
    let a = lossy("a", 1);
    let b = lossy("b", 2);
    let r = reliable("r", 1, "n", 0);
    let narrow = Clause::new([a.clone()], sends.clone());
    let wide = Clause::new([a.clone(), b.clone()], sends.clone());
    let mut under_crash = Clause::new([a.clone()], sends.clone());
    under_crash.premises = set([crash("n")]);
    let mut always = Clause::new([r], sends.clone());
    always.premises = set([crash("n")]);
    let clauses = [
        wide.clone(),
        narrow.clone(),
        narrow.clone(),
        under_crash,
        always,
    ];
    let universe = universe(&clauses, []);
    assert_eq!(ilp::all_cover_rows(&universe, &clauses).len(), 5);
    // `narrow` implies `wide`, its copy and `under_crash`; `always` always holds.
    let lean = ilp::cover_rows(&universe, &clauses);
    assert_eq!(lean, ilp::cover_rows(&universe, &[narrow]));
}

#[test]
fn a_tried_set_that_a_clause_excludes_needs_no_no_good() {
    let sends = no_sends();
    let a = lossy("a", 1);
    let b = lossy("b", 2);
    let tried = set([Fault::Omit(a.message)]);
    // The run under `tried` observed a derivation through `b`, which `tried` does not kill.
    let clause = Clause::new([b], sends.clone());
    assert!(ilp::already_excluded(
        &tried,
        std::slice::from_ref(&clause),
        &[]
    ));
    // A derivation `tried` kills does not exclude it.
    let killed = Clause::new([a], sends.clone());
    assert!(!ilp::already_excluded(&tried, &[killed], &[]));
    // Nor does one observed under a crash `tried` lacks.
    let mut under_crash = clause;
    under_crash.premises = set([crash("n")]);
    assert!(!ilp::already_excluded(&tried, &[under_crash], &[]));
    // A vacuous subset does.
    assert!(ilp::already_excluded(
        &tried,
        &[],
        std::slice::from_ref(&tried)
    ));
}

/// A search shaped like a real one: every hypothesis's run yields a derivation it does not kill
/// (so explicit no-goods are rarely needed, and derivations repeat). Returns the proposals'
/// sizes, the time spent, and the final model's rows as (cover rows, no-goods).
fn time_learning_search(
    nodes: usize,
    sends: usize,
    hypotheses: usize,
    model: ilp::Model,
) -> (Vec<usize>, Duration, (usize, usize)) {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let (pool, sequences) = random_pool(&mut rng, nodes, sends);
    let bounds = CrashBounds {
        cap: Some(2),
        nodes,
        checked: None,
    };
    let mut clauses: Vec<Clause<M>> = (0..3)
        .filter_map(|_| surviving_clause(&pool, &sequences, &FaultSet::new(), &mut rng))
        .collect();
    let mut tried = BTreeSet::from([FaultSet::new()]);
    let mut sizes = vec![];
    let start = Instant::now();
    while sizes.len() < hypotheses {
        let universe = universe(&clauses, []);
        let Some(h) = ilp::solve(
            &universe,
            &clauses,
            &tried,
            &[],
            &bounds,
            Backend::Microlp,
            model,
        ) else {
            break;
        };
        sizes.push(h.len());
        clauses.extend(surviving_clause(&pool, &sequences, &h, &mut rng));
        tried.insert(h);
    }
    let took = start.elapsed();
    let universe = universe(&clauses, []);
    let rows = match model {
        ilp::Model::Full => (ilp::all_cover_rows(&universe, &clauses).len(), tried.len()),
        ilp::Model::Lean => (
            ilp::cover_rows(&universe, &clauses).len(),
            tried
                .iter()
                .filter(|t| !ilp::already_excluded(t, &clauses, &[]))
                .count(),
        ),
    };
    (sizes, took, rows)
}

/// `cargo test -p hydrofrac --release -- --ignored --nocapture --test-threads 1 bench_`
#[test]
#[ignore = "benchmark"]
fn bench_lean_vs_full_model() {
    for hypotheses in [100, 200, 300] {
        let (full, full_took, full_rows) =
            time_learning_search(6, 20, hypotheses, ilp::Model::Full);
        let (lean, lean_took, lean_rows) =
            time_learning_search(6, 20, hypotheses, ilp::Model::Lean);
        println!(
            "learning, {} hypotheses: full {full_took:?} ({:?}/h; {} covers, {} no-goods), \
             lean {lean_took:?} ({:?}/h; {} covers, {} no-goods)",
            lean.len(),
            full_took / full.len().max(1) as u32,
            full_rows.0,
            full_rows.1,
            lean_took / lean.len().max(1) as u32,
            lean_rows.0,
            lean_rows.1,
        );
    }
}
