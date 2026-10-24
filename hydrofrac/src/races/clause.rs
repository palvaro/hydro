//! Per-pair clauses, the crash rule, and the candidates of the hitting-set program
//! (docs/design/hydrofrac-races.md §4.2, §4.3, §12).
//!
//! A clause is one derivation of one pair (a member and a fact): the deliveries the member's
//! record of the fact was computed from, and the premise literals of the fragile steps it passed
//! ([`super::literals`]). A clause with no premise is *robust*: it holds in every schedule in which
//! its deliveries occur (Lemma R, §5). Only robust clauses protect a pair; a premise is never
//! proved, so the adversary can always make a fragile clause fail.

use std::collections::{BTreeMap, BTreeSet};

use super::literals::{Literal, SendOrder, after};
use crate::search::{CrashAt, CrashBounds, Dependency, Derivation, Fault, FaultSet, Node};

/// One derivation of one pair, with the premises it needs.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PairClause<M> {
    /// The deliveries the fact was derived from.
    pub deps: Derivation<M>,
    /// The fragile steps (by site index) whose premise literal the derivation needs. Empty iff
    /// the derivation is robust.
    pub premises: BTreeSet<usize>,
}

/// The clauses learned so far, by pair.
pub type Pairs<P, M> = BTreeMap<P, Vec<PairClause<M>>>;

/// A pair's key: which member it is about, and which fact. Implemented for `(member, fact)`.
pub trait PairKey: Ord + Clone {
    /// The fact's type.
    type Fact: Ord + Clone;
    /// The node of the member that should hold the fact.
    fn member(&self) -> &Node;
    /// The fact.
    fn fact(&self) -> &Self::Fact;
}

impl<F: Ord + Clone> PairKey for (Node, F) {
    type Fact = F;
    fn member(&self) -> &Node {
        &self.0
    }
    fn fact(&self) -> &F {
        &self.1
    }
}

/// Whether `fault` kills the delivery `dep`, once every free literal is set the adversary's way
/// ([`super::literals`]):
/// - `omit(m)` kills it iff `dep` is `m` on a lossy channel;
/// - `crash(n)` kills it iff `n` sent it;
/// - `crash(n after m)` kills it iff `n` sent it, it is not `m` itself, and the literal
///   `after(dep, m)` is not proved false.
///
/// The send's position in the run it was observed in (`dep.sender`'s ordinal) is ignored on
/// purpose: another schedule may order `n`'s sends differently. Only proved literals are trusted.
///
/// **Why "not killed" is true in every schedule.** A delivery from a node the fault does not crash
/// is never cut by it. Under `crash(n after m)`, `m` itself is sent before `n` dies in every
/// schedule in which it is sent at all, and if `m` is never sent the crash never fires. A `d` whose
/// `after(d, m)` is proved false is sent before `m`, so before the crash.
pub fn kills<M: PartialEq>(
    fault: &Fault<M>,
    dep: &Dependency<M>,
    order: &dyn SendOrder<M>,
) -> bool {
    match fault {
        Fault::Omit(m) => dep.lossy && dep.message == *m,
        Fault::Crash { node, at } => {
            let Some((sender, _ordinal)) = &dep.sender else {
                return false;
            };
            sender == node
                && match at {
                    CrashAt::BeforeAnySend => true,
                    CrashAt::After(m) => {
                        dep.message != *m && after(order, &dep.message, m) != Literal::Fixed(false)
                    }
                }
        }
    }
}

impl<M> PairClause<M> {
    /// Whether the clause needs no premise.
    pub fn is_robust(&self) -> bool {
        self.premises.is_empty()
    }
}

impl<M: Ord> PairClause<M> {
    /// A robust clause.
    pub fn robust(deps: impl IntoIterator<Item = Dependency<M>>) -> Self {
        PairClause {
            deps: deps.into_iter().collect(),
            premises: BTreeSet::new(),
        }
    }

    /// A clause that passed the fragile steps `sites`.
    pub fn with_premises(
        deps: impl IntoIterator<Item = Dependency<M>>,
        sites: impl IntoIterator<Item = usize>,
    ) -> Self {
        PairClause {
            deps: deps.into_iter().collect(),
            premises: sites.into_iter().collect(),
        }
    }

    /// A clause that passed one (unnamed) fragile step.
    pub fn fragile(deps: impl IntoIterator<Item = Dependency<M>>) -> Self {
        Self::with_premises(deps, [0])
    }

    /// Whether `fault` kills some delivery of this clause.
    pub fn hit_by(&self, fault: &Fault<M>, order: &dyn SendOrder<M>) -> bool {
        self.deps.iter().any(|dep| kills(fault, dep, order))
    }

    /// Whether some fault of `set` kills some delivery of this clause.
    pub fn hit_by_set(&self, set: &FaultSet<M>, order: &dyn SendOrder<M>) -> bool {
        set.iter().any(|fault| self.hit_by(fault, order))
    }
}

/// Facts verified lost under a fault set, hence under every superset of it
/// ([`super::Sweep::mark_dead`]): each entry is a fault set and a fact that no correct member
/// holds under it, in any schedule, nor under any superset.
pub type Dead<M, F> = [(FaultSet<M>, F)];

/// Whether `pair` needs nothing under `set`, two facts with no literal:
/// - **crashed at start:** `set` holds `crash(member)` before any send, which fires at instance
///   start in every schedule, so the member is never correct;
/// - **dead fact:** `set` is a superset of a set under which the pair's fact is dead
///   ([`super::Sweep::mark_dead`]), so no correct member holds it under `set` either.
pub fn excused<P: PairKey, M: Ord + Clone>(
    set: &FaultSet<M>,
    pair: &P,
    dead: &Dead<M, P::Fact>,
) -> bool {
    set.contains(&Fault::crash_after(pair.member().clone(), None))
        || dead
            .iter()
            .any(|(lost, fact)| fact == pair.fact() && lost.is_subset(set))
}

/// Whether `set` is admissible for `pair`: it does not [excuse](excused) the pair, and it kills
/// every robust clause of it. A pair with no robust clause makes every set that does not excuse
/// it admissible.
pub fn admissible_for<P: PairKey, M: Ord + Clone>(
    set: &FaultSet<M>,
    pair: &P,
    clauses: &[PairClause<M>],
    dead: &Dead<M, P::Fact>,
    order: &dyn SendOrder<M>,
) -> bool {
    !excused(set, pair, dead)
        && clauses
            .iter()
            .filter(|c| c.is_robust())
            .all(|c| c.hit_by_set(set, order))
}

/// Whether `set` can be skipped (§4.3, the corollary of Lemma R): it is admissible for no pair.
/// For each pair, either some robust clause survives it, so in every schedule of `set` the pair
/// is held or its member crashed, or `set` [excuses](excused) the pair.
pub fn pruned<P: PairKey, M: Ord + Clone>(
    set: &FaultSet<M>,
    pairs: &Pairs<P, M>,
    dead: &Dead<M, P::Fact>,
    order: &dyn SendOrder<M>,
) -> bool {
    pairs
        .iter()
        .all(|(pair, clauses)| !admissible_for(set, pair, clauses, dead, order))
}

/// Whether `set` is within `bounds`: at most one crash per node, the crash cap, the every-node
/// bound and the checked cluster's cap (as `check/brute_force.rs` `admissible`).
pub fn within_bounds<M>(set: &FaultSet<M>, bounds: &CrashBounds) -> bool {
    let crashed: Vec<&Node> = set.iter().filter_map(Fault::crashed_node).collect();
    let distinct: BTreeSet<&Node> = crashed.iter().copied().collect();
    if distinct.len() != crashed.len() {
        return false;
    }
    if bounds.max_crashes().is_some_and(|max| crashed.len() > max) {
        return false;
    }
    bounds.checked.as_ref().is_none_or(|checked| {
        crashed
            .iter()
            .filter(|n| checked.nodes.contains(**n))
            .count()
            <= checked.cap
    })
}

/// One candidate of the hitting-set program.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Candidate<M> {
    /// A fault some run revealed.
    Fault(Fault<M>),
    /// "Crash `n` right after a message no run has shown `n` sending" (§12.2). Its crash literals
    /// are all free, so it kills every delivery from `n`, as `crash(n)` does, but it excuses
    /// nothing. It cannot be run as such: the sweep resolves it by running the rest of the set in
    /// full, which shows every message `n` sends there ([`super::Next::Try`]).
    Unseen(Node),
}

/// A proposed set of candidates.
pub type Proposal<M> = BTreeSet<Candidate<M>>;

impl<M: Ord + Clone> Candidate<M> {
    /// Whether this candidate kills some delivery of `clause`.
    pub fn hits(&self, clause: &PairClause<M>, order: &dyn SendOrder<M>) -> bool {
        match self {
            Candidate::Fault(fault) => clause.hit_by(fault, order),
            Candidate::Unseen(node) => clause
                .deps
                .iter()
                .any(|d| d.sender.as_ref().is_some_and(|(sender, _)| sender == node)),
        }
    }

    /// The node this candidate crashes, if it is a crash.
    pub fn crashed_node(&self) -> Option<&Node> {
        match self {
            Candidate::Fault(fault) => fault.crashed_node(),
            Candidate::Unseen(node) => Some(node),
        }
    }

    /// The fault, if this is one.
    pub fn fault(&self) -> Option<&Fault<M>> {
        match self {
            Candidate::Fault(fault) => Some(fault),
            Candidate::Unseen(_) => None,
        }
    }
}

/// The faults of `proposal`, without its [`Candidate::Unseen`] crashes.
pub fn faults_of<M: Ord + Clone>(proposal: &Proposal<M>) -> FaultSet<M> {
    proposal
        .iter()
        .filter_map(Candidate::fault)
        .cloned()
        .collect()
}

/// A fault set as a proposal.
pub fn proposal_of<M: Ord + Clone>(set: &FaultSet<M>) -> Proposal<M> {
    set.iter().cloned().map(Candidate::Fault).collect()
}

/// The candidates over the faults the runs revealed (`known`: the brute-force enumerator's
/// harvest, `check/brute_force.rs`), weakest first, since the solver's tie-break prefers earlier
/// candidates and a hypothesis should leave as much of the protocol running as it can:
/// omissions, then per crashable node its crash points after later messages before earlier
/// ones, then (if `unseen`) [`Candidate::Unseen`], then `crash(n)`. "Later" is the latest
/// position (`latest`) any run sent the message at; it is only a tie-break.
pub fn candidates<M: Ord + Clone>(
    known: &BTreeSet<Fault<M>>,
    latest: &BTreeMap<Node, BTreeMap<M, u64>>,
    unseen: bool,
) -> Vec<Candidate<M>> {
    let mut out: Vec<Candidate<M>> = known
        .iter()
        .filter(|f| matches!(f, Fault::Omit(_)))
        .cloned()
        .map(Candidate::Fault)
        .collect();
    let crashable: BTreeSet<&Node> = known.iter().filter_map(Fault::crashed_node).collect();
    for node in crashable {
        let mut points: Vec<(u64, &M)> = known
            .iter()
            .filter_map(|f| match f {
                Fault::Crash {
                    node: n,
                    at: CrashAt::After(m),
                } if n == node => Some(m),
                _ => None,
            })
            .map(|m| {
                let at = latest.get(node).and_then(|l| l.get(m)).copied();
                (at.unwrap_or(0), m)
            })
            .collect();
        points.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        out.extend(
            points
                .into_iter()
                .map(|(_, m)| Candidate::Fault(Fault::crash_after(node.clone(), Some(m.clone())))),
        );
        if unseen {
            out.push(Candidate::Unseen(node.clone()));
        }
        if known.contains(&Fault::crash_after(node.clone(), None)) {
            out.push(Candidate::Fault(Fault::crash_after(node.clone(), None)));
        }
    }
    out
}

/// The image of any fault set `set` among the [candidates] (the completion, §12.2):
/// - a known fault is itself;
/// - an omission of a message no run showed is dropped;
/// - a crash after a message no run showed becomes [`Candidate::Unseen`] if `unseen`, and is
///   dropped otherwise (then every message any run of any fault set sends was shown: the
///   never-sent proof, [`super::literals::after`]).
///
/// What the sweep's soundness argument ([`super::sweep`]) uses of the image:
/// - it crashes no more nodes than `set`, so it is within every bound `set` is;
/// - it excuses a pair only if `set` does: it is a subset of `set` but for its unseen crashes,
///   which excuse nothing, and keeps every `crash(n)`;
/// - a dropped omission names a message no run showed, so it kills no clause's delivery (every
///   delivery of a clause was shown by the run it was read from);
/// - [`Candidate::Unseen`] kills every delivery of its node, so at least what the crash it
///   replaces kills;
/// - a crash dropped when `unseen` is false never fires, so it changes no run.
pub fn image<M: Ord + Clone>(
    set: &FaultSet<M>,
    known: &BTreeSet<Fault<M>>,
    unseen: bool,
) -> Proposal<M> {
    set.iter()
        .filter_map(|f| {
            if known.contains(f) {
                return Some(Candidate::Fault(f.clone()));
            }
            match f {
                Fault::Omit(_) => None,
                Fault::Crash {
                    at: CrashAt::BeforeAnySend,
                    ..
                } => Some(Candidate::Fault(f.clone())),
                Fault::Crash {
                    node,
                    at: CrashAt::After(_),
                } => unseen.then(|| Candidate::Unseen(node.clone())),
            }
        })
        .collect()
}
