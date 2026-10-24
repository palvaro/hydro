use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::search::{
    Backend, CheckedCluster, CrashAt, CrashBounds, Dependency, Fault, FaultSet, HYPOTHESIS_BUDGET,
    Node, Tolerance,
};

type M = String;
/// Facts are named by a string in these tests.
type F = &'static str;
type P = (Node, F);

fn pair(member: &str, fact: F) -> P {
    (member.to_owned(), fact)
}

/// No dead sets.
const ALIVE: &Dead<M, F> = &[];

fn reliable(message: &str, sender: &str) -> Dependency<M> {
    Dependency {
        message: message.to_owned(),
        sender: Some((sender.to_owned(), 0)),
        lossy: false,
    }
}

fn lossy(message: &str, sender: &str) -> Dependency<M> {
    Dependency {
        lossy: true,
        ..reliable(message, sender)
    }
}

/// A lossy delivery whose sender is unknown: only its omission kills it.
fn lost(message: &str) -> Dependency<M> {
    Dependency {
        sender: None,
        ..lossy(message, "")
    }
}

fn omit(message: &str) -> Fault<M> {
    Fault::Omit(message.to_owned())
}

fn crash(node: &str) -> Fault<M> {
    Fault::crash_after(node, None)
}

fn crash_after(node: &str, message: &str) -> Fault<M> {
    Fault::crash_after(node, Some(message.to_owned()))
}

fn set(faults: impl IntoIterator<Item = Fault<M>>) -> FaultSet<M> {
    faults.into_iter().collect()
}

fn unbounded() -> CrashBounds {
    CrashBounds::default()
}

fn cap(crashes: usize) -> CrashBounds {
    CrashBounds {
        cap: Some(crashes),
        ..CrashBounds::default()
    }
}

/// [`propose`] over a universe of plain faults, with no dead sets.
fn propose_faults(
    universe: &[Fault<M>],
    pairs: &Pairs<P, M>,
    tried: &BTreeSet<FaultSet<M>>,
    bounds: &CrashBounds,
    scope: Scope,
) -> Option<FaultSet<M>> {
    let universe: Vec<Candidate<M>> = universe.iter().cloned().map(Candidate::Fault).collect();
    let proposal = propose(
        &universe,
        pairs,
        ALIVE,
        tried,
        bounds,
        scope,
        Backend::default(),
    )?;
    Some(
        proposal
            .into_iter()
            .map(|c| match c {
                Candidate::Fault(f) => f,
                Candidate::Unnamed(n) => panic!("unnamed crash of {n}"),
            })
            .collect(),
    )
}

/// Every set `propose` gives at `scope`, each added to the tried sets, until it gives none.
fn exhaust(
    universe: &[Fault<M>],
    pairs: &Pairs<P, M>,
    bounds: &CrashBounds,
    scope: Scope,
) -> Vec<FaultSet<M>> {
    let mut tried = BTreeSet::new();
    let mut out = vec![];
    while let Some(set) = propose_faults(universe, pairs, &tried, bounds, scope) {
        assert!(tried.insert(set.clone()), "re-proposed {set:?}");
        out.push(set);
        assert!(out.len() < 10_000, "runaway");
    }
    out
}

/// Every subset of `universe` with at most `size` faults.
fn subsets(universe: &[Fault<M>], size: usize) -> Vec<FaultSet<M>> {
    let mut out = vec![FaultSet::new()];
    for fault in universe {
        let grown: Vec<FaultSet<M>> = out
            .iter()
            .filter(|s| s.len() < size)
            .map(|s| {
                let mut s = s.clone();
                s.insert(fault.clone());
                s
            })
            .collect();
        out.extend(grown);
    }
    out
}

#[test]
fn omission_kills_only_its_own_lossy_message() {
    assert!(kills(&omit("a"), &lossy("a", "n")));
    assert!(!kills(&omit("a"), &reliable("a", "n")));
    assert!(!kills(&omit("b"), &lossy("a", "n")));
}

#[test]
fn crash_before_any_send_kills_every_send_of_its_node() {
    assert!(kills(&crash("n"), &reliable("a", "n")));
    assert!(kills(&crash("n"), &lossy("a", "n")));
    assert!(!kills(&crash("k"), &reliable("a", "n")));
    let unknown_sender = Dependency {
        sender: None,
        ..lossy("a", "n")
    };
    assert!(!kills(&crash("n"), &unknown_sender));
}

#[test]
fn crash_after_a_message_spares_that_message_only() {
    assert!(!kills(&crash_after("n", "a"), &reliable("a", "n")));
    assert!(kills(&crash_after("n", "a"), &reliable("b", "n")));
    assert!(!kills(&crash_after("n", "a"), &reliable("b", "k")));
    // The observed position is ignored: `b` was seen sent before `a`, but another schedule may
    // send it after.
    let early_b = Dependency {
        sender: Some(("n".to_owned(), 0)),
        ..reliable("b", "n")
    };
    let late_a = Dependency {
        sender: Some(("n".to_owned(), 5)),
        ..reliable("a", "n")
    };
    assert!(kills(&crash_after("n", "a"), &early_b));
    assert!(!kills(&crash_after("n", "a"), &late_a));

    // A clause whose only send of `n` is `a` survives `crash(n after a)`; one with two sends of
    // `n` does not.
    let one = PairClause::robust([reliable("a", "n"), reliable("x", "k")]);
    let two = PairClause::robust([reliable("a", "n"), reliable("b", "n")]);
    assert!(!one.hit_by(&crash_after("n", "a")));
    assert!(two.hit_by(&crash_after("n", "a")));
    assert!(two.hit_by(&crash_after("n", "b")));
    assert!(one.hit_by(&crash("n")));
}

/// `ask_once` (§6.1): member 0 announces to 1 and 2. Crashing it right after the announcement
/// to 1 spares (1, 7) and kills (2, 7)'s robust derivation, in every schedule.
#[test]
fn ask_once_crash_splits_the_members() {
    let to1 = PairClause::robust([reliable("ann->1", "m0")]);
    let to2 = PairClause::robust([reliable("ann->2", "m0")]);
    let h = set([crash_after("m0", "ann->1")]);
    assert!(!to1.hit_by_set(&h));
    assert!(to2.hit_by_set(&h));
    let pairs: Pairs<P, M> =
        BTreeMap::from([(pair("m1", "7"), vec![to1]), (pair("m2", "7"), vec![to2])]);
    assert!(!pruned(&h, &pairs, ALIVE));
    let (two, one) = (pair("m2", "7"), pair("m1", "7"));
    assert!(admissible_for(&h, &two, &pairs[&two], ALIVE));
    assert!(!admissible_for(&h, &one, &pairs[&one], ALIVE));
}

#[test]
fn universe_orders_omissions_then_later_crash_points_then_crash() {
    let mut late = reliable("late", "n");
    late.sender = Some(("n".to_owned(), 3));
    let clauses = [
        PairClause::robust([reliable("early", "n"), lossy("drop", "k")]),
        PairClause::fragile([late]),
    ];
    assert_eq!(
        universe(&clauses),
        vec![
            omit("drop"),
            crash_after("k", "drop"),
            crash("k"),
            crash_after("n", "late"),
            crash_after("n", "early"),
            crash("n"),
        ]
    );
}

/// A set is proposed iff it kills every robust clause of some pair: exactly the admissible
/// subsets of the universe, each once.
#[test]
fn proposals_are_the_sets_admissible_for_some_pair() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (
            pair("mA", "A"),
            vec![
                PairClause::robust([lost("a1"), lost("x")]),
                PairClause::robust([lost("a2"), reliable("c", "k")]),
                PairClause::fragile([lost("f")]),
            ],
        ),
        (pair("mB", "B"), vec![PairClause::robust([lost("b")])]),
    ]);
    let universe = universe(pairs.values().flatten());
    let bounds = cap(1);
    let proposed = exhaust(&universe, &pairs, &bounds, Scope::RobustOnly);
    // The smallest first: `omit(b)` alone.
    assert_eq!(proposed[0], set([omit("b")]));
    for w in proposed.windows(2) {
        assert!(w[0].len() <= w[1].len(), "{proposed:?}");
    }
    let proposed: BTreeSet<FaultSet<M>> = proposed.into_iter().collect();
    let expected: BTreeSet<FaultSet<M>> = subsets(&universe, universe.len())
        .into_iter()
        .filter(|s| within_bounds(s, &bounds))
        .filter(|s| pairs.iter().any(|(p, c)| admissible_for(s, p, c, ALIVE)))
        .collect();
    assert_eq!(proposed, expected);
    assert!(expected.iter().all(|s| !pruned(s, &pairs, ALIVE)));
}

/// S1 (§4.3): a run under which `f` vanished at every member contributes no clause for `f`.
/// Per-pair clauses keep the probe's derivation of (m1, f), so the set that omits its only
/// delivery is still proposed. Lite's union clause for that run (only `g`'s deliveries) would
/// have been missed by that set, which would then have been skipped.
#[test]
fn a_run_that_lost_a_fact_does_not_hide_its_breaking_set() {
    let mut pairs: Pairs<P, M> = BTreeMap::from([
        (pair("m0", "f"), vec![PairClause::robust([])]),
        (
            pair("m1", "f"),
            vec![PairClause::robust([lossy("z fwd f", "m0")])],
        ),
        (
            pair("m1", "g"),
            vec![PairClause::robust([lossy("fwd g", "m0")])],
        ),
    ]);
    let mut sweep = Sweep::new(cap(0));
    assert_eq!(sweep.next(&pairs), Next::Try(set([omit("fwd g")])));
    // Its run lost `f` at every member (no clause for `f`), and re-derived `g` another way.
    pairs
        .get_mut(&pair("m1", "g"))
        .unwrap()
        .push(PairClause::robust([lossy("retry g", "m0")]));
    assert_eq!(sweep.next(&pairs), Next::Try(set([omit("z fwd f")])));
}

/// Fragile clauses never prune, but they do shape the heuristic's proposals.
#[test]
fn fragile_clauses_never_prune_but_steer_all_known() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (
            pair("mp", "p"),
            vec![
                PairClause::robust([lossy("a", "n")]),
                PairClause::fragile([lossy("b", "n")]),
            ],
        ),
        (
            pair("mq", "q"),
            vec![
                PairClause::robust([lossy("c", "n")]),
                PairClause::robust([lossy("d", "n")]),
            ],
        ),
    ]);
    // Killing the fragile clause does not make a set admissible, nor does it stop one being so.
    assert!(pruned(&set([omit("b")]), &pairs, ALIVE));
    assert!(!pruned(&set([omit("a")]), &pairs, ALIVE));
    assert!(!pruned(&set([omit("a"), omit("b")]), &pairs, ALIVE));
    // A pair with only fragile clauses keeps every set unpruned, even the empty one.
    let only_fragile: Pairs<P, M> = BTreeMap::from([(
        pair("mr", "r"),
        vec![PairClause::fragile([lossy("a", "n")])],
    )]);
    assert!(!pruned(&FaultSet::new(), &only_fragile, ALIVE));
    assert_eq!(unconstrained_pairs(&only_fragile), vec![pair("mr", "r")]);
    assert_eq!(unconstrained_pairs(&pairs), Vec::<P>::new());

    let universe = universe(pairs.values().flatten());
    let tried = BTreeSet::from([FaultSet::new()]);
    let robust = propose_faults(&universe, &pairs, &tried, &cap(0), Scope::RobustOnly);
    assert_eq!(robust, Some(set([omit("a")])));
    // All known: (p) needs `a` and `b`, (q) needs `c` and `d`; the tie-break prefers the
    // earlier candidates.
    let all = propose_faults(&universe, &pairs, &tried, &cap(0), Scope::AllKnown);
    assert_eq!(all, Some(set([omit("a"), omit("b")])));
}

#[test]
fn a_pair_whose_clause_cannot_be_killed_is_never_chosen() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (pair("ml", "local"), vec![PairClause::robust([])]),
        (
            pair("mr", "remote"),
            vec![PairClause::robust([reliable("a", "n")])],
        ),
    ]);
    let universe = universe(pairs.values().flatten());
    let proposed = exhaust(&universe, &pairs, &cap(0), Scope::RobustOnly);
    assert!(proposed.is_empty(), "{proposed:?}");
    let proposed = exhaust(&universe, &pairs, &cap(1), Scope::RobustOnly);
    // `crash(n after a)` spares `a`, so only `crash(n)` kills it.
    assert_eq!(proposed, vec![set([crash("n")])]);
}

#[test]
fn bounds_hold_in_proposals() {
    let pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![
            PairClause::robust([reliable("a", "n0")]),
            PairClause::robust([reliable("b", "n1")]),
            PairClause::robust([reliable("c", "n2")]),
        ],
    )]);
    let universe = universe(pairs.values().flatten());
    let bounds = CrashBounds {
        cap: None,
        nodes: 4,
        checked: Some(CheckedCluster::new([
            "n0".to_owned(),
            "n1".to_owned(),
            "n2".to_owned(),
        ])),
    };
    // Killing all three needs three crashes in the checked cluster, which allows one.
    assert!(exhaust(&universe, &pairs, &bounds, Scope::RobustOnly).is_empty());
    let loose = CrashBounds {
        checked: None,
        ..bounds
    };
    let proposed = exhaust(&universe, &pairs, &loose, Scope::RobustOnly);
    assert_eq!(proposed, vec![set([crash("n0"), crash("n1"), crash("n2")])]);
}

#[test]
fn sweep_switches_to_enumeration_when_a_pair_is_unconstrained() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (
            pair("m1", "robust"),
            vec![PairClause::robust([lossy("a", "n")])],
        ),
        (
            pair("m2", "fragile"),
            vec![PairClause::fragile([lossy("b", "n")])],
        ),
    ]);
    let mut sweep = Sweep::new(cap(1));
    assert_eq!(
        sweep.next(&pairs),
        Next::Unconstrained(vec![pair("m2", "fragile")])
    );
}

/// A set that was run and is still admissible (its run gave the pair only fragile derivations)
/// makes every superset admissible: the sweep hands the label to the enumerator.
#[test]
fn sweep_switches_to_enumeration_when_a_tried_set_stays_admissible() {
    let mut pairs: Pairs<P, M> =
        BTreeMap::from([(pair("mp", "p"), vec![PairClause::robust([lossy("a", "n")])])]);
    let mut sweep = Sweep::new(cap(0));
    assert_eq!(sweep.next(&pairs), Next::Try(set([omit("a")])));
    pairs
        .get_mut(&pair("mp", "p"))
        .unwrap()
        .push(PairClause::fragile([lossy("b", "n")]));
    assert_eq!(
        sweep.next(&pairs),
        Next::Unconstrained(vec![pair("mp", "p")])
    );
}

#[test]
fn sweep_certifies_up_to_the_cap_and_stops_at_the_budget() {
    let mut pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![PairClause::robust([reliable("a", "n")])],
    )]);
    let mut sweep = Sweep::new(cap(1));
    // Bound 0 admits nothing (only a crash kills `a`), so the sweep moves to bound 1.
    assert_eq!(sweep.next(&pairs), Next::Try(set([crash("n")])));
    assert_eq!(sweep.tolerated(), Some(0));
    assert_eq!(sweep.bound(), 1);
    pairs
        .get_mut(&pair("mp", "p"))
        .unwrap()
        .push(PairClause::robust([]));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::All));

    let mut sweep = Sweep::new(cap(1));
    sweep.budget = 1;
    assert_eq!(sweep.next(&pairs), Next::Budget);
    assert_eq!(HYPOTHESIS_BUDGET, Sweep::<M, F>::new(cap(0)).budget);

    // Two nodes to crash, a cap of one: certified up to one.
    let pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![
            PairClause::robust([reliable("a", "n0")]),
            PairClause::robust([reliable("b", "n1")]),
        ],
    )]);
    let mut sweep = Sweep::new(cap(1));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::UpTo(1)));
}

/// A small deterministic PRNG (xorshift64).
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

/// Completion, checked against a larger fault space: random robust (and some fragile) clauses
/// of pairs whose members also send; the sweep runs, and every set it proposes is answered,
/// for every pair it was admissible for, by a robust re-derivation it spares (a run that held)
/// or, sometimes, by marking the pair's fact dead under it. When it certifies, no fault set
/// within the certified bound, over omissions of every message and every crash point (after
/// messages no clause names, and of nodes no clause names), is admissible for any pair, with
/// the exemptions honoured. Even seeds tell the sweep every message each node sends, so it has
/// a crash point after an unnamed message for each; odd seeds tell it nothing, so it may hand
/// the label to the enumerator instead.
#[test]
fn completion_covers_every_fault_set() {
    let nodes: Vec<String> = (0..4).map(|n| format!("n{n}")).collect();
    let messages: Vec<(String, String, bool)> = nodes
        .iter()
        .flat_map(|n| (0..5).map(move |i| (format!("{n}:{i}"), n.clone(), i % 2 == 0)))
        .collect();
    let dep = |(m, n, l): &(String, String, bool)| Dependency {
        message: m.clone(),
        sender: Some((n.clone(), 0)),
        lossy: *l,
    };
    // Messages 3 and 4 of each node are never named by a clause, and the sweep is only ever told
    // of 3; node `n4` sends nothing.
    let named: Vec<&(String, String, bool)> = messages
        .iter()
        .filter(|(m, _, _)| !m.ends_with(":3") && !m.ends_with(":4"))
        .collect();
    let mut space: Vec<Fault<M>> = vec![];
    for (m, n, _) in &messages {
        space.push(Fault::Omit(m.clone()));
        space.push(Fault::crash_after(n.clone(), Some(m.clone())));
    }
    for n in nodes.iter().chain([&"n4".to_owned()]) {
        space.push(Fault::crash_after(n.clone(), None));
    }
    let candidates = subsets(&space, 3);

    let (mut certified, mut handed_over, mut deaths) = (0, 0, 0);
    for seed in 1..=60u64 {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let random_deps = |rng: &mut Rng, avoid: &FaultSet<M>| -> Vec<Dependency<M>> {
            let len = 1 + rng.below(3);
            (0..len)
                .map(|_| dep(named[rng.below(named.len())]))
                .filter(|d| !avoid.iter().any(|f| kills(f, d)))
                .collect()
        };
        let mut pairs: Pairs<P, M> = BTreeMap::new();
        for _ in 0..1 + rng.below(3) {
            let key = pair(&nodes[rng.below(nodes.len())], ["f0", "f1"][rng.below(2)]);
            let mut clauses = vec![];
            for _ in 0..1 + rng.below(3) {
                clauses.push(PairClause::robust(random_deps(&mut rng, &FaultSet::new())));
            }
            if rng.below(2) == 0 {
                clauses.push(PairClause::fragile(random_deps(&mut rng, &FaultSet::new())));
            }
            pairs.insert(key, clauses);
        }
        let bounds = cap(rng.below(3));
        let mut sweep = Sweep::new(bounds.clone());
        let told = seed % 2 == 0;
        if told {
            for (m, n, _) in messages.iter().filter(|(m, _, _)| !m.ends_with(":4")) {
                sweep.saw_sends(n, [m.clone()]);
            }
        }
        let tolerance = loop {
            match sweep.next(&pairs) {
                Next::Try(set) => {
                    assert!(within_bounds(&set, &sweep_bounds(&bounds, &sweep)));
                    assert!(
                        !pruned(&set, &pairs, sweep.dead()),
                        "seed {seed}: proposed a pruned set"
                    );
                    let deadly = set.iter().all(|f| {
                        matches!(
                            f,
                            Fault::Omit(_)
                                | Fault::Crash {
                                    at: CrashAt::BeforeAnySend,
                                    ..
                                }
                        )
                    });
                    let admitted: Vec<P> = pairs
                        .iter()
                        .filter(|(p, c)| admissible_for(&set, *p, c, sweep.dead()))
                        .map(|(p, _)| p.clone())
                        .collect();
                    for p in admitted {
                        if deadly && rng.below(3) == 0 {
                            sweep.mark_dead(set.clone(), p.1);
                            deaths += 1;
                        } else {
                            let spared = PairClause::robust(random_deps(&mut rng, &set));
                            pairs.get_mut(&p).unwrap().push(spared);
                        }
                    }
                }
                Next::Certified(tolerance) => break Some(tolerance),
                Next::Unconstrained(stuck) if !told => {
                    assert!(!stuck.is_empty());
                    break None;
                }
                other => panic!("seed {seed}: {other:?}"),
            }
        };
        let Some(tolerance) = tolerance else {
            handed_over += 1;
            continue;
        };
        certified += 1;
        let within = match tolerance {
            Tolerance::All => unbounded(),
            Tolerance::UpTo(k) => cap(k),
        };
        for h in &candidates {
            if within_bounds(h, &within) {
                assert!(
                    pruned(h, &pairs, sweep.dead()),
                    "seed {seed}: {h:?} is admissible after {tolerance:?}, pairs {pairs:?}, \
                     dead {:?}",
                    sweep.dead()
                );
            }
        }
    }
    assert_eq!(certified + handed_over, 60);
    assert!(certified >= 30, "{certified} certified");
    assert!(deaths > 0);
}

/// The bounds the sweep is proposing under: the cap lowered to its current bound.
fn sweep_bounds(bounds: &CrashBounds, sweep: &Sweep<M, F>) -> CrashBounds {
    bounds.capped_at(sweep.bound())
}

#[test]
fn enumeration_goes_by_size_then_position() {
    let pairs: Pairs<P, M> = BTreeMap::from([(pair("mp", "p"), vec![PairClause::fragile([])])]);
    let mut e = Enumerator::new([omit("a"), omit("b"), omit("c"), omit("a")], 2);
    assert_eq!(e.universe(), &[omit("a"), omit("b"), omit("c")]);
    let tried = BTreeSet::from([FaultSet::new()]);
    let mut out = vec![];
    while let Some(s) = e.next(&unbounded(), &tried, &pairs, ALIVE) {
        out.push(s);
    }
    assert_eq!(
        out,
        vec![
            set([omit("a")]),
            set([omit("b")]),
            set([omit("c")]),
            set([omit("a"), omit("b")]),
            set([omit("a"), omit("c")]),
            set([omit("b"), omit("c")]),
        ]
    );
    assert_eq!(e.next(&unbounded(), &tried, &pairs, ALIVE), None);
}

#[test]
fn enumeration_respects_bounds_and_skips_tried_and_pruned_sets() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (pair("mp", "p"), vec![PairClause::robust([lossy("a", "n")])]),
        (
            pair("mq", "q"),
            vec![PairClause::robust([reliable("b", "k")])],
        ),
    ]);
    let universe = [omit("a"), crash_after("n", "a"), crash("n"), crash("k")];
    let mut tried = BTreeSet::from([FaultSet::new(), set([omit("a")])]);
    let mut e = Enumerator::new(universe, 3);
    let mut out = vec![];
    while let Some(s) = e.next(&cap(1), &tried, &pairs, ALIVE) {
        out.push(s);
    }
    // Admissible: kills `a` (`omit(a)` or `crash(n)`, for p) or `b` (`crash(k)`, for q); at most
    // one crash, and never two of one node. `crash(n after a)` spares `a`.
    assert_eq!(
        out,
        vec![
            set([crash("n")]),
            set([crash("k")]),
            set([omit("a"), crash_after("n", "a")]),
            set([omit("a"), crash("n")]),
            set([omit("a"), crash("k")]),
        ]
    );
    // Growing the universe restarts the cursor; tried sets stay skipped.
    tried.extend(out);
    assert!(e.extend([omit("z")]));
    assert!(!e.extend([omit("z")]));
    let mut again = vec![];
    while let Some(s) = e.next(&cap(1), &tried, &pairs, ALIVE) {
        again.push(s);
    }
    assert_eq!(
        again,
        vec![
            set([omit("a"), omit("z")]),
            set([crash("n"), omit("z")]),
            set([crash("k"), omit("z")]),
            set([omit("a"), crash_after("n", "a"), omit("z")]),
            set([omit("a"), crash("n"), omit("z")]),
            set([omit("a"), crash("k"), omit("z")]),
        ]
    );
}

/// A start crash of a pair's member excuses the pair: the program never proposes it for that
/// pair, and a crash after a message no clause names takes its place, or, when no run has shown
/// one, the label goes to the enumerator.
#[test]
fn a_start_crash_of_the_member_excuses_its_pairs() {
    let p = pair("m1", "f");
    let mut pairs: Pairs<P, M> = BTreeMap::from([(
        p.clone(),
        vec![
            PairClause::robust([reliable("a", "m1")]),
            PairClause::robust([reliable("b", "m1")]),
        ],
    )]);
    assert!(excused(&set([crash("m1")]), &p, ALIVE));
    assert!(!excused(&set([crash_after("m1", "a")]), &p, ALIVE));
    assert!(!admissible_for(&set([crash("m1")]), &p, &pairs[&p], ALIVE));
    assert!(pruned(&set([crash("m1"), omit("x")]), &pairs, ALIVE));
    // Over the plain universe only `crash(m1)` kills both, and it excuses the pair.
    let plain = universe(pairs.values().flatten());
    assert!(plain.contains(&crash("m1")));
    assert_eq!(
        exhaust(&plain, &pairs, &cap(1), Scope::RobustOnly),
        Vec::<FaultSet<M>>::new()
    );

    let mut blind = Sweep::new(cap(1));
    assert_eq!(blind.next(&pairs), Next::Unconstrained(vec![p.clone()]));

    let mut sweep = Sweep::new(cap(1));
    sweep.saw_sends(&"m1".to_owned(), ["a", "b", "w"].map(str::to_owned));
    assert_eq!(sweep.next(&pairs), Next::Try(set([crash_after("m1", "w")])));
    // Its run held: a derivation it spares.
    pairs
        .get_mut(&p)
        .unwrap()
        .push(PairClause::robust([reliable("c", "m0")]));
    assert!(matches!(
        sweep.next(&pairs),
        Next::Try(_) | Next::Certified(_)
    ));
}

/// A dead fact: once marked, the set and every superset are admissible for no pair of the fact.
/// Without the mark the sweep keeps handing the label to the enumerator.
#[test]
fn a_dead_fact_excuses_its_pairs_under_supersets() {
    let (p1, p2, g) = (pair("m1", "f"), pair("m2", "f"), pair("m1", "g"));
    let mut pairs: Pairs<P, M> = BTreeMap::from([
        (p1.clone(), vec![PairClause::robust([reliable("a", "o")])]),
        (p2.clone(), vec![PairClause::robust([reliable("b", "o")])]),
        (g.clone(), vec![PairClause::robust([lossy("c", "k")])]),
    ]);
    let origin = set([crash("o")]);
    let mut sweep = Sweep::new(cap(1));
    sweep.mark_tried(origin.clone());
    // `crash(o)` was run: `f` vanished at every member, so it is still admissible for both.
    assert_eq!(
        sweep.next(&pairs),
        Next::Unconstrained(vec![p1.clone(), p2.clone()])
    );
    assert_eq!(
        sweep.next(&pairs),
        Next::Unconstrained(vec![p1.clone(), p2.clone()])
    );

    sweep.mark_dead(origin.clone(), "f");
    let dead = sweep.dead().to_vec();
    let bigger = set([crash("o"), omit("c")]);
    for p in [&p1, &p2] {
        assert!(excused(&origin, p, &dead));
        assert!(excused(&bigger, p, &dead));
        assert!(!excused(&set([crash_after("o", "a")]), p, &dead));
    }
    // Not for other facts.
    assert!(!excused(&bigger, &g, &dead));
    assert!(admissible_for(&bigger, &g, &pairs[&g], &dead));
    // The sweep moves on: first `g`'s omission, then crashes of `o` after one of its sends, and
    // never `crash(o)` again (it can only matter for `f`).
    assert_eq!(sweep.next(&pairs), Next::Try(set([omit("c")])));
    let mut proposed = vec![];
    loop {
        // Every run held: the pairs it was admissible for were re-derived out of its reach.
        let last = proposed.last().cloned().unwrap_or_else(|| set([omit("c")]));
        for (p, clauses) in pairs.iter_mut() {
            if admissible_for(&last, p, clauses, &dead) {
                clauses.push(PairClause::robust([]));
            }
        }
        match sweep.next(&pairs) {
            Next::Try(set) => proposed.push(set),
            Next::Certified(_) => break,
            other => panic!("{other:?} after {proposed:?}"),
        }
        assert!(proposed.len() < 50);
    }
    assert_eq!(proposed[0], set([crash_after("o", "a")]));
    assert!(
        proposed.iter().all(|s| !s.contains(&crash("o"))),
        "{proposed:?}"
    );
}

#[test]
#[should_panic(expected = "only omissions and start crashes")]
fn a_dead_set_with_a_crash_point_is_refused() {
    let mut sweep: Sweep<M, F> = Sweep::new(cap(1));
    sweep.mark_dead(set([crash_after("o", "a")]), "f");
}

#[test]
fn enumeration_skips_supersets_of_dead_sets() {
    let pairs: Pairs<P, M> = BTreeMap::from([(pair("m1", "f"), vec![PairClause::fragile([])])]);
    let dead = [(set([omit("a")]), "f")];
    let mut e = Enumerator::new([omit("a"), omit("b"), crash("m1")], 2);
    let mut out = vec![];
    while let Some(s) = e.next(&unbounded(), &BTreeSet::new(), &pairs, &dead) {
        out.push(s);
    }
    // Not `{omit(a)}` nor its supersets (dead), nor sets crashing the member at start.
    assert_eq!(out, vec![FaultSet::new(), set([omit("b")])]);
}
