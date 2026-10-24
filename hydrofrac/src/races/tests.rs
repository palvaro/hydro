use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use super::*;
use crate::search::{
    Backend, CheckedCluster, CrashBounds, Dependency, Fault, FaultSet, HYPOTHESIS_BUDGET, Node,
    Tolerance,
};

type M = String;
/// Facts are named by a string in these tests.
type F = &'static str;
type P = (Node, F);

fn pair(member: &str, fact: F) -> P {
    (member.to_owned(), fact)
}

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

/// A test send order: chains of messages, each totally ordered (one sender per chain).
#[derive(Debug, Default)]
struct Chains(BTreeMap<M, (usize, usize)>);

impl Chains {
    fn new<S: AsRef<str>>(chains: &[impl AsRef<[S]>]) -> Self {
        let mut at = BTreeMap::new();
        for (c, chain) in chains.iter().enumerate() {
            for (i, m) in chain.as_ref().iter().enumerate() {
                assert!(
                    at.insert(m.as_ref().to_owned(), (c, i)).is_none(),
                    "disjoint chains"
                );
            }
        }
        Chains(at)
    }
}

impl SendOrder<M> for Chains {
    fn precedes(&self, earlier: &M, later: &M) -> bool {
        match (self.0.get(earlier), self.0.get(later)) {
            (Some((c, i)), Some((d, j))) => c == d && i < j,
            _ => false,
        }
    }
}

fn cap(crashes: usize) -> CrashBounds {
    CrashBounds {
        cap: Some(crashes),
        ..CrashBounds::default()
    }
}

/// [`propose`] over a universe of plain faults.
fn propose_faults(
    universe: &[Fault<M>],
    pairs: &Pairs<P, M>,
    tried: &BTreeSet<FaultSet<M>>,
    bounds: &CrashBounds,
    scope: Scope,
) -> Option<FaultSet<M>> {
    let universe: Vec<Candidate<M>> = universe.iter().cloned().map(Candidate::Fault).collect();
    let tried: BTreeSet<Proposal<M>> = tried.iter().map(proposal_of).collect();
    let proposal = propose(
        &universe,
        pairs,
        &tried,
        bounds,
        scope,
        Backend::default(),
        &(),
    )?;
    assert!(proposal.iter().all(|c| c.fault().is_some()));
    Some(faults_of(&proposal))
}

/// The faults the clauses of `pairs` reveal: `omit(m)` for each lossy delivery, and for each
/// sender `crash(n)` and `crash(n after m)` for each of its deliveries.
fn revealed(pairs: &Pairs<P, M>) -> Vec<Fault<M>> {
    let mut out = BTreeSet::new();
    for dep in pairs.values().flatten().flat_map(|c| c.deps.iter()) {
        if dep.lossy {
            out.insert(Fault::Omit(dep.message.clone()));
        }
        if let Some((n, _)) = &dep.sender {
            out.insert(crash(n));
            out.insert(Fault::crash_after(n.clone(), Some(dep.message.clone())));
        }
    }
    out.into_iter().collect()
}

/// The candidates over [`revealed`] faults, with every send complete (no unseen crash).
fn universe_of(pairs: &Pairs<P, M>) -> Vec<Fault<M>> {
    let known: BTreeSet<Fault<M>> = revealed(pairs).into_iter().collect();
    candidates(&known, &BTreeMap::new(), false)
        .into_iter()
        .map(|c| c.fault().cloned().expect("no unseen crash"))
        .collect()
}

/// A sweep under `bounds` that has seen the faults of `pairs`, and every send.
fn sweep_of(bounds: CrashBounds, pairs: &Pairs<P, M>) -> Sweep<M> {
    let mut sweep = Sweep::new(bounds);
    for f in revealed(pairs) {
        sweep.saw_fault(f);
    }
    sweep.sends_complete();
    sweep
}

/// `Next::Try` of a plain run.
fn run(set: FaultSet<M>) -> Next<M> {
    Next::Try {
        set,
        in_full: false,
    }
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
    assert!(kills(&omit("a"), &lossy("a", "n"), &()));
    assert!(!kills(&omit("a"), &reliable("a", "n"), &()));
    assert!(!kills(&omit("b"), &lossy("a", "n"), &()));
}

#[test]
fn crash_before_any_send_kills_every_send_of_its_node() {
    assert!(kills(&crash("n"), &reliable("a", "n"), &()));
    assert!(kills(&crash("n"), &lossy("a", "n"), &()));
    assert!(!kills(&crash("k"), &reliable("a", "n"), &()));
    let unknown_sender = Dependency {
        sender: None,
        ..lossy("a", "n")
    };
    assert!(!kills(&crash("n"), &unknown_sender, &()));
}

#[test]
fn crash_after_a_message_spares_that_message_only() {
    assert!(!kills(&crash_after("n", "a"), &reliable("a", "n"), &()));
    assert!(kills(&crash_after("n", "a"), &reliable("b", "n"), &()));
    assert!(!kills(&crash_after("n", "a"), &reliable("b", "k"), &()));
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
    assert!(kills(&crash_after("n", "a"), &early_b, &()));
    assert!(!kills(&crash_after("n", "a"), &late_a, &()));

    // A clause whose only send of `n` is `a` survives `crash(n after a)`; one with two sends of
    // `n` does not.
    let one = PairClause::robust([reliable("a", "n"), reliable("x", "k")]);
    let two = PairClause::robust([reliable("a", "n"), reliable("b", "n")]);
    assert!(!one.hit_by(&crash_after("n", "a"), &()));
    assert!(two.hit_by(&crash_after("n", "a"), &()));
    assert!(two.hit_by(&crash_after("n", "b"), &()));
    assert!(one.hit_by(&crash("n"), &()));
}

/// `after(d, m)`: fixed false when `d` precedes `m`, fixed true when it follows, free
/// otherwise.
#[test]
fn crash_literals_are_fixed_only_by_a_proved_order() {
    let order = Chains::new(&[["a", "b"]]);
    let (a, b, x) = ("a".to_owned(), "b".to_owned(), "x".to_owned());
    assert_eq!(after(&order, &a, &b), Literal::Fixed(false));
    assert_eq!(after(&order, &b, &a), Literal::Fixed(true));
    assert_eq!(after(&order, &x, &a), Literal::Free);
    assert_eq!(after(&(), &a, &b), Literal::Free);
}

/// `ask_once` (§6.1): member 0 announces to 1 and 2. Crashing it right after the announcement
/// to 1 spares (1, 7) and kills (2, 7)'s robust derivation, in every schedule.
#[test]
fn ask_once_crash_splits_the_members() {
    let to1 = PairClause::robust([reliable("ann->1", "m0")]);
    let to2 = PairClause::robust([reliable("ann->2", "m0")]);
    let h = set([crash_after("m0", "ann->1")]);
    assert!(!to1.hit_by_set(&h, &()));
    assert!(to2.hit_by_set(&h, &()));
    let pairs: Pairs<P, M> =
        BTreeMap::from([(pair("m1", "7"), vec![to1]), (pair("m2", "7"), vec![to2])]);
    assert!(!pruned(&h, &pairs, &()));
    let (two, one) = (pair("m2", "7"), pair("m1", "7"));
    assert!(admissible_for(&h, &two, &pairs[&two], &()));
    assert!(!admissible_for(&h, &one, &pairs[&one], &()));
}

#[test]
fn candidates_order_omissions_then_later_crash_points_then_unseen_then_crash() {
    let known: BTreeSet<Fault<M>> = [
        omit("drop"),
        crash("k"),
        crash_after("k", "drop"),
        crash("n"),
        crash_after("n", "early"),
        crash_after("n", "late"),
    ]
    .into();
    let latest: BTreeMap<Node, BTreeMap<M, u64>> = BTreeMap::from([(
        "n".to_owned(),
        BTreeMap::from([("early".to_owned(), 0), ("late".to_owned(), 3)]),
    )]);
    let f = Candidate::Fault;
    assert_eq!(
        candidates(&known, &latest, true),
        vec![
            f(omit("drop")),
            f(crash_after("k", "drop")),
            Candidate::Unseen("k".to_owned()),
            f(crash("k")),
            f(crash_after("n", "late")),
            f(crash_after("n", "early")),
            Candidate::Unseen("n".to_owned()),
            f(crash("n")),
        ]
    );
    assert_eq!(candidates(&known, &latest, false).len(), 6);
}

/// The image of a fault set: known faults stay, an omission of an unknown message goes, a
/// crash after an unknown message becomes the unseen crash (or goes, when sends are complete).
#[test]
fn the_image_keeps_known_faults_and_maps_the_rest() {
    let known: BTreeSet<Fault<M>> = [omit("a"), crash("n"), crash_after("n", "a")].into();
    let h = set([omit("a"), omit("u"), crash_after("n", "u"), crash("k")]);
    let f = Candidate::Fault;
    assert_eq!(
        image(&h, &known, true),
        Proposal::from([
            f(omit("a")),
            Candidate::Unseen("n".to_owned()),
            f(crash("k"))
        ])
    );
    assert_eq!(
        image(&h, &known, false),
        Proposal::from([f(omit("a")), f(crash("k"))])
    );
    // The unseen crash kills every delivery of its node; it excuses nothing.
    let c = PairClause::robust([reliable("b", "n")]);
    assert!(Candidate::<M>::Unseen("n".to_owned()).hits(&c, &()));
    assert!(!Candidate::<M>::Unseen("k".to_owned()).hits(&c, &()));
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
    let universe = universe_of(&pairs);
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
        .filter(|s| pairs.iter().any(|(p, c)| admissible_for(s, p, c, &())))
        .collect();
    assert_eq!(proposed, expected);
    assert!(expected.iter().all(|s| !pruned(s, &pairs, &())));
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
    let mut sweep = sweep_of(cap(0), &pairs);
    assert_eq!(sweep.next(&pairs), run(set([omit("fwd g")])));
    // Its run lost `f` at every member (no clause for `f`), and re-derived `g` another way.
    pairs
        .get_mut(&pair("m1", "g"))
        .unwrap()
        .push(PairClause::robust([lossy("retry g", "m0")]));
    assert_eq!(sweep.next(&pairs), run(set([omit("z fwd f")])));
}

/// Fragile clauses never prune, but they do shape the heuristic's proposals.
#[test]
fn fragile_clauses_never_prune_but_steer_all_known() {
    let pairs: Pairs<P, M> = BTreeMap::from([
        (
            pair("mp", "p"),
            vec![
                PairClause::robust([lossy("a", "n")]),
                PairClause::with_premises([lossy("b", "n")], [3, 5]),
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
    assert!(pruned(&set([omit("b")]), &pairs, &()));
    assert!(!pruned(&set([omit("a")]), &pairs, &()));
    assert!(!pruned(&set([omit("a"), omit("b")]), &pairs, &()));
    // A pair with only fragile clauses keeps every set unpruned, even the empty one.
    let only_fragile: Pairs<P, M> = BTreeMap::from([(
        pair("mr", "r"),
        vec![PairClause::fragile([lossy("a", "n")])],
    )]);
    assert!(!pruned(&FaultSet::new(), &only_fragile, &()));

    let universe = universe_of(&pairs);
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
    let universe = universe_of(&pairs);
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
    let universe = universe_of(&pairs);
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

/// A start crash of a pair's member excuses the pair: the program never proposes it for that
/// pair, but a crash of the member after one of its messages is a candidate.
#[test]
fn a_start_crash_of_the_member_excuses_its_pairs() {
    let p = pair("m1", "f");
    let pairs: Pairs<P, M> =
        BTreeMap::from([(p.clone(), vec![PairClause::robust([reliable("a", "m1")])])]);
    assert!(excused(&set([crash("m1")]), &p));
    assert!(!excused(&set([crash_after("m1", "b")]), &p));
    let mut sweep = sweep_of(cap(1), &pairs);
    sweep.saw_fault(crash_after("m1", "b"));
    // `crash(m1 after a)` spares `a`, `crash(m1)` excuses: `crash(m1 after b)` is the one.
    assert_eq!(sweep.next(&pairs), run(set([crash_after("m1", "b")])));
}

/// A pair no robust clause protects keeps every set admissible: the program proposes every set
/// over the candidates (heuristic first), then concludes. There is no other fallback.
#[test]
fn an_unprotected_pair_gets_every_set_then_a_certificate() {
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
    let mut sweep = Sweep::new(cap(0));
    sweep.saw_fault(omit("a"));
    sweep.saw_fault(omit("b"));
    sweep.sends_complete();
    let mut proposed = BTreeSet::new();
    let verdict = loop {
        match sweep.next(&pairs) {
            Next::Try { set, in_full } => {
                assert!(!in_full);
                assert!(proposed.insert(set));
            }
            other => break other,
        }
    };
    // No node can crash: every bound is the same.
    assert_eq!(verdict, Next::Certified(Tolerance::All));
    assert_eq!(
        proposed,
        BTreeSet::from([
            set([omit("a")]),
            set([omit("b")]),
            set([omit("a"), omit("b")])
        ])
    );
    assert!(sweep.covers_everything(&pairs));
}

/// A set that was run and is still admissible (its run gave the pair only fragile derivations)
/// makes its supersets admissible: the program proposes them itself.
#[test]
fn a_tried_set_that_stays_admissible_gets_its_supersets_proposed() {
    let mut pairs: Pairs<P, M> =
        BTreeMap::from([(pair("mp", "p"), vec![PairClause::robust([lossy("a", "n")])])]);
    let mut sweep = sweep_of(cap(0), &pairs);
    sweep.saw_fault(omit("b"));
    assert_eq!(sweep.next(&pairs), run(set([omit("a")])));
    pairs
        .get_mut(&pair("mp", "p"))
        .unwrap()
        .push(PairClause::fragile([lossy("b", "n")]));
    assert_eq!(sweep.next(&pairs), run(set([omit("a"), omit("b")])));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::UpTo(0)));
}

#[test]
fn sweep_certifies_up_to_the_cap_and_stops_at_the_budget() {
    let mut pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![PairClause::robust([reliable("a", "n")])],
    )]);
    let mut sweep = sweep_of(cap(1), &pairs);
    // Bound 0 admits nothing (only a crash kills `a`), so the sweep moves to bound 1.
    assert_eq!(sweep.next(&pairs), run(set([crash("n")])));
    assert_eq!(sweep.tolerated(), Some(0));
    assert_eq!(sweep.bound(), 1);
    pairs
        .get_mut(&pair("mp", "p"))
        .unwrap()
        .push(PairClause::robust([]));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::All));

    let mut sweep = sweep_of(cap(1), &pairs);
    sweep.budget = 1;
    assert_eq!(sweep.next(&pairs), Next::Budget);
    assert_eq!(HYPOTHESIS_BUDGET, Sweep::<M>::new(cap(0)).budget);

    // Two nodes to crash, a cap of one: certified up to one.
    let pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![
            PairClause::robust([reliable("a", "n0")]),
            PairClause::robust([reliable("b", "n1")]),
        ],
    )]);
    let mut sweep = sweep_of(cap(1), &pairs);
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::UpTo(1)));
}

/// New candidates void the bounds concluded over the old ones: the sweep starts again from zero
/// crashes, keeping its tried sets.
#[test]
fn new_candidates_restart_the_sweep() {
    let pairs: Pairs<P, M> = BTreeMap::from([(
        pair("mp", "p"),
        vec![PairClause::fragile([lossy("a", "n")])],
    )]);
    let mut sweep = Sweep::new(cap(1));
    sweep.saw_fault(omit("a"));
    sweep.sends_complete();
    assert_eq!(sweep.next(&pairs), run(set([omit("a")])));
    // No node can crash: every bound is the same.
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::All));
    assert_eq!(sweep.tolerated(), Some(0));
    // A later run shows a lossy `b`: `{omit(a), omit(b)}` (the heuristic's, which also kills
    // the fragile clause) and `omit(b)` are new, at bound 0.
    sweep.saw_fault(omit("b"));
    assert_eq!(sweep.next(&pairs), run(set([omit("a"), omit("b")])));
    assert_eq!(sweep.tolerated(), None);
    assert_eq!(sweep.bound(), 0);
    assert_eq!(sweep.next(&pairs), run(set([omit("b")])));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::All));
}

/// An unseen crash (sends not complete) stands for every crash after a message no run showed.
/// It is resolved by running the rest of its set in full, once; after that, a proposal holding
/// it with that set is answered without a run.
#[test]
fn an_unseen_crash_is_resolved_by_a_run_in_full() {
    let mut pairs: Pairs<P, M> = BTreeMap::from([(
        pair("m1", "f"),
        vec![PairClause::robust([reliable("a", "n")])],
    )]);
    let mut sweep = Sweep::new(cap(1));
    for f in revealed(&pairs) {
        sweep.saw_fault(f);
    }
    // `crash(n after a)` spares `a`; the unseen crash kills it but excuses nothing, and comes
    // before `crash(n)`.
    assert_eq!(
        sweep.next(&pairs),
        Next::Try {
            set: FaultSet::new(),
            in_full: true
        }
    );
    sweep.explored_in_full(FaultSet::new());
    assert_eq!(sweep.next(&pairs), run(set([crash("n")])));
    pairs
        .get_mut(&pair("m1", "f"))
        .unwrap()
        .push(PairClause::robust([]));
    assert_eq!(sweep.next(&pairs), Next::Certified(Tolerance::All));

    // With the probe already explored in full, the unseen crash needs no run at all.
    let pairs: Pairs<P, M> = BTreeMap::from([(
        pair("m1", "f"),
        vec![PairClause::robust([reliable("a", "n")])],
    )]);
    let mut sweep = Sweep::new(cap(1));
    for f in revealed(&pairs) {
        sweep.saw_fault(f);
    }
    sweep.explored_in_full(FaultSet::new());
    assert_eq!(sweep.next(&pairs), run(set([crash("n")])));
}

/// With a known order (`a` before `b` before `c`, all of `n`), `crash(n after b)` spares `a`
/// and `b` and kills `c` and any other send of `n`.
#[test]
fn a_known_send_order_spares_what_precedes_the_crash_point() {
    let order = Chains::new(&[["a", "b", "c"]]);
    let after_b = crash_after("n", "b");
    assert!(!kills(&after_b, &reliable("a", "n"), &order));
    assert!(!kills(&after_b, &reliable("b", "n"), &order));
    assert!(kills(&after_b, &reliable("c", "n"), &order));
    assert!(kills(&after_b, &reliable("x", "n"), &order));
    // Without the order, `a` counts as killed.
    assert!(kills(&after_b, &reliable("a", "n"), &()));
    let ab = PairClause::robust([reliable("a", "n"), reliable("b", "n")]);
    let ac = PairClause::robust([reliable("a", "n"), reliable("c", "n")]);
    assert!(!ab.hit_by(&after_b, &order));
    assert!(ac.hit_by(&after_b, &order));
}

/// A closed broadcast `d0, d1, d2` from `s` to three members: with the fan-out order, crashing
/// `s` after `d2` cuts nothing, and after `d1` cuts only `d2`.
#[test]
fn a_fan_out_order_narrows_admissible_crash_points() {
    let order = Chains::new(&[["d0", "d1", "d2"]]);
    let pairs: Pairs<P, M> = BTreeMap::from([
        (
            pair("m0", "f"),
            vec![PairClause::robust([reliable("d0", "s")])],
        ),
        (
            pair("m1", "f"),
            vec![PairClause::robust([reliable("d1", "s")])],
        ),
        (
            pair("m2", "f"),
            vec![PairClause::robust([reliable("d2", "s")])],
        ),
    ]);
    let last = set([crash_after("s", "d2")]);
    assert!(pruned(&last, &pairs, &order));
    assert!(!pruned(&last, &pairs, &()));
    let middle = set([crash_after("s", "d1")]);
    let admitting = |order: &dyn SendOrder<M>| -> Vec<P> {
        pairs
            .iter()
            .filter(|(p, c)| admissible_for(&middle, *p, c, order))
            .map(|(p, _)| p.clone())
            .collect()
    };
    assert_eq!(admitting(&order), vec![pair("m2", "f")]);
    assert_eq!(admitting(&()), vec![pair("m0", "f"), pair("m2", "f")]);
}

/// An exact crash point: every message of `s` lies in one chain, and the runs have shown them
/// all. Then `crash(s after d1)` touches only proved literals.
#[test]
fn a_crash_point_is_exact_when_every_send_lies_in_its_chain() {
    let mut sweep: Sweep<M> =
        Sweep::new(cap(1)).with_order(Rc::new(Chains::new(&[["d0", "d1", "d2"]])));
    let s = "s".to_owned();
    for (i, d) in ["d0", "d1", "d2"].into_iter().enumerate() {
        sweep.saw_send(&s, d.to_owned(), i as u64);
    }
    // Not before the sends are known complete.
    assert!(!sweep.is_exact(&s, &"d1".to_owned()));
    sweep.sends_complete();
    assert!(sweep.is_exact(&s, &"d1".to_owned()));
    // A send outside the chain makes its literal free.
    sweep.saw_send(&s, "x".to_owned(), 3);
    assert!(!sweep.is_exact(&s, &"d1".to_owned()));
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

/// Completion, checked against a larger fault space. Random clauses (robust, and some fragile)
/// of pairs whose members also send; the sweep runs, and the test answers each run: for every
/// pair the set was admissible for, a robust re-derivation the set spares (a run that held), or
/// only a fragile one (the pair stays unprotected). Runs sometimes reveal a message no clause
/// names. When the sweep certifies, every fault set within the certified bound, over omissions
/// of every message and every crash point (after messages no run showed, too), is either
/// skipped by Lemma R, or its image was tried. Even seeds have complete sends (a crash after a
/// message no run showed never fires, so the space omits it); odd seeds have unseen crashes.
#[test]
fn completion_covers_every_fault_set() {
    let nodes: Vec<String> = (0..3).map(|n| format!("n{n}")).collect();
    let messages: Vec<(String, String, bool)> = nodes
        .iter()
        .flat_map(|n| (0..4).map(move |i| (format!("{n}:{i}"), n.clone(), i % 2 == 0)))
        .collect();
    let dep = |(m, n, l): &(String, String, bool)| Dependency {
        message: m.clone(),
        sender: Some((n.clone(), 0)),
        lossy: *l,
    };
    let reveal = |sweep: &mut Sweep<M>, (m, n, l): &(String, String, bool)| {
        if *l {
            sweep.saw_fault(Fault::Omit(m.clone()));
        }
        sweep.saw_fault(Fault::crash_after(n.clone(), Some(m.clone())));
    };
    for seed in 1..=24u64 {
        let complete = seed % 2 == 0;
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        // Messages `:3` are never named by a clause; a run reveals some of them.
        let named: Vec<&(String, String, bool)> = messages
            .iter()
            .filter(|(m, _, _)| !m.ends_with(":3"))
            .collect();
        let hidden: Vec<&(String, String, bool)> = messages
            .iter()
            .filter(|(m, _, _)| m.ends_with(":3"))
            .collect();
        let mut pairs: Pairs<P, M> = BTreeMap::new();
        for p in 0..3 {
            let member = nodes[rng.below(nodes.len())].clone();
            let clauses = (0..1 + rng.below(2))
                .map(|_| {
                    let deps: Vec<Dependency<M>> = (0..1 + rng.below(2))
                        .map(|_| dep(named[rng.below(named.len())]))
                        .collect();
                    if rng.below(4) == 0 {
                        PairClause::fragile(deps)
                    } else {
                        PairClause::robust(deps)
                    }
                })
                .collect();
            pairs.insert((member, ["f", "g", "h"][p]), clauses);
        }
        let bounds = cap(1 + rng.below(2));
        let mut sweep = Sweep::new(bounds.clone());
        for n in &nodes {
            sweep.saw_fault(crash(n));
        }
        for c in pairs.values().flatten() {
            for d in &c.deps {
                let (n, _) = d.sender.clone().unwrap();
                sweep.saw_fault(Fault::crash_after(n, Some(d.message.clone())));
                if d.lossy {
                    sweep.saw_fault(Fault::Omit(d.message.clone()));
                }
            }
        }
        if complete {
            sweep.sends_complete();
            // Complete sends: every message a run can send was shown.
            for m in &hidden {
                reveal(&mut sweep, m);
            }
        }
        sweep.explored_in_full(FaultSet::new());
        let verdict = loop {
            match sweep.next(&pairs) {
                Next::Try { set, in_full } => {
                    // The runs answer every pair the set is admissible for.
                    let admitting: Vec<P> = pairs
                        .iter()
                        .filter(|(p, c)| admissible_for(&set, *p, c, &()))
                        .map(|(p, _)| p.clone())
                        .collect();
                    for p in admitting {
                        let spared: Vec<Dependency<M>> = (0..rng.below(2))
                            .map(|_| dep(named[rng.below(named.len())]))
                            .filter(|d| !set.iter().any(|f| kills(f, d, &())))
                            .collect();
                        // The run that showed them revealed their faults.
                        for d in &spared {
                            let (n, _) = d.sender.clone().unwrap();
                            sweep.saw_fault(Fault::crash_after(n, Some(d.message.clone())));
                            if d.lossy {
                                sweep.saw_fault(Fault::Omit(d.message.clone()));
                            }
                        }
                        let clause = if rng.below(3) == 0 {
                            PairClause::fragile(spared)
                        } else {
                            PairClause::robust(spared)
                        };
                        pairs.get_mut(&p).unwrap().push(clause);
                    }
                    if !complete && rng.below(3) == 0 {
                        reveal(&mut sweep, hidden[rng.below(hidden.len())]);
                    }
                    if in_full || rng.below(2) == 0 {
                        sweep.explored_in_full(set);
                    }
                }
                other => break other,
            }
        };
        let Next::Certified(tolerance) = verdict else {
            panic!("seed {seed}: {verdict:?}");
        };
        let within = match tolerance {
            Tolerance::All => bounds.clone(),
            Tolerance::UpTo(k) => bounds.capped_at(k),
        };
        // The larger space: every omission and every crash point, after any message.
        let mut space: Vec<Fault<M>> = vec![];
        for (m, n, l) in &messages {
            if *l {
                space.push(Fault::Omit(m.clone()));
            }
            space.push(Fault::crash_after(n.clone(), Some(m.clone())));
        }
        space.extend(nodes.iter().map(|n| crash(n)));
        for _ in 0..400 {
            let h: FaultSet<M> = (0..1 + rng.below(3))
                .map(|_| space[rng.below(space.len())].clone())
                .collect();
            if !within_bounds(&h, &within) {
                continue;
            }
            let img = sweep.image(&h);
            let admissible = pairs.iter().any(|(p, clauses)| {
                !img.contains(&Candidate::Fault(crash(&p.0)))
                    && clauses
                        .iter()
                        .filter(|c| c.is_robust())
                        .all(|c| img.iter().any(|x| x.hits(c, &())))
            });
            if admissible {
                assert!(
                    sweep.tried().contains(&img),
                    "seed {seed}: {h:?} (image {img:?}) admissible, untried"
                );
            } else {
                assert!(pruned(&h, &pairs, &()), "seed {seed}: {h:?} not pruned");
            }
        }
    }
}
