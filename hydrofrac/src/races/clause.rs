//! Per-pair clauses and the crash rule that does not depend on send order
//! (docs/design/hydrofrac-races.md §4.2, §4.3).
//!
//! A clause is one derivation of one pair (a member and a fact): the deliveries the member's
//! record of the fact was computed from. It is *robust* when no record in its backward closure
//! passed a fragile step, so it holds in every schedule (Lemma R, §5). Only robust clauses are
//! used to skip fault sets; fragile ones only steer the counterexample heuristic.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use crate::search::{CrashAt, Dependency, Derivation, Fault, FaultSet, Node};

/// One derivation of one pair, and whether it holds in every schedule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PairClause<M> {
    /// The deliveries the fact was derived from.
    pub deps: Derivation<M>,
    /// Whether no record of the derivation passed a fragile step.
    pub robust: bool,
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

/// Facts verified lost under a fault set, hence under every superset of it
/// ([`super::Sweep::mark_dead`]): each entry is a set of omissions and start crashes, and a
/// fact that no correct member holds under it, in any schedule.
pub type Dead<M, F> = [(FaultSet<M>, F)];

/// What is known, in every run, of the order in which a node sends its messages.
///
/// `precedes(d, m)` claims: in every run (any fault set, any schedule) in which `m`'s sender
/// sends `m`, it has already sent `d`. It must be false unless `d` and `m` have the same sender.
/// The completion argument ([`universe`], [`candidates`]) also needs it to be a union of
/// disjoint chains: each message lies in at most one chain, and the chain is totally ordered.
pub trait SendOrder<M>: Debug {
    /// Whether every run that sends `later` has sent `earlier` before it (see the trait docs).
    fn precedes(&self, earlier: &M, later: &M) -> bool;
}

/// No order known: every pair of sends may come in either order.
impl<M> SendOrder<M> for () {
    fn precedes(&self, _: &M, _: &M) -> bool {
        false
    }
}

/// Whether `fault` kills the delivery `dep`, in every schedule (§4.2):
/// - `omit(m)` kills it iff `dep` is `m` on a lossy channel;
/// - `crash(n)` kills it iff `n` sent it;
/// - `crash(n after m)` kills it iff `n` sent it, it is not `m` itself, and `order` does not
///   say it is always sent before `m`.
///
/// The send's position in the run it was observed in (`dep.sender`'s ordinal) is ignored on
/// purpose: another schedule may order `n`'s sends differently. Only `order`, which holds in
/// every run, is trusted.
///
/// **Why this is sound in every schedule.** The rule only claims that a delivery *survives*
/// when the crash cannot cut it whatever the order:
/// - a delivery from a node the fault does not crash is never cut by it;
/// - under `crash(n after m)`, the delivery `m` itself is sent before `n` dies in every
///   schedule in which it is sent at all (the crash fires right after it), and if `m` is never
///   sent the crash never fires.
///
/// - a delivery `d` that `order` says always precedes `m` is sent before `m`, so before the
///   crash, in every schedule in which the crash fires.
///
/// So if a clause's only sends from `n` are `m` and messages that always precede it,
/// `crash(n after m)` spares it in every schedule. Every other delivery from `n` may come after
/// `m` in some schedule, so it counts as killed.
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
                    CrashAt::After(m) => dep.message != *m && !order.precedes(&dep.message, m),
                }
        }
    }
}

impl<M: Ord> PairClause<M> {
    /// A robust clause.
    pub fn robust(deps: impl IntoIterator<Item = Dependency<M>>) -> Self {
        PairClause {
            deps: deps.into_iter().collect(),
            robust: true,
        }
    }

    /// A fragile clause.
    pub fn fragile(deps: impl IntoIterator<Item = Dependency<M>>) -> Self {
        PairClause {
            deps: deps.into_iter().collect(),
            robust: false,
        }
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

/// Whether `pair` cannot be broken by `set`, whatever its clauses say:
/// - **crashed at start:** `set` holds `crash(member)` before any send, which fires at instance
///   start in every schedule, so the member is never correct;
/// - **dead fact:** `set` is a superset of a set under which the pair's fact was verified lost
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
/// every robust clause of it. A pair with no robust clause makes every set that does not
/// excuse it admissible.
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
            .filter(|c| c.robust)
            .all(|c| c.hit_by_set(set, order))
}

/// Whether `set` can be skipped (§4.3, the corollary of Lemma R): it is admissible for no pair.
/// For each pair, either some robust clause survives it, so in every schedule of `set` the pair
/// is held or its member crashed, or `set` [excuses](excused) the pair. Fragile clauses are
/// ignored, and a pair with no robust clause keeps every set that does not excuse it unpruned.
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

/// The pairs with no robust clause. Each makes every fault set that does not excuse it
/// admissible, so the hitting-set program cannot finish the label and the engine must
/// enumerate (§4.4, step 4).
pub fn unconstrained_pairs<P: Clone, M: Ord>(pairs: &Pairs<P, M>) -> Vec<P> {
    pairs
        .iter()
        .filter(|(_, clauses)| !clauses.iter().any(|c| c.robust))
        .map(|(p, _)| p.clone())
        .collect()
}

/// The pairs for which some set of `tried` is still admissible. The empty set (the probe) is
/// admissible exactly for the [unconstrained](unconstrained_pairs) pairs. A set that was run
/// and stays admissible for a pair gave that pair no robust derivation it spares, so every
/// superset of it that does not excuse the pair is admissible for it too, including supersets
/// adding faults that no clause names.
pub fn stuck_pairs<P: PairKey, M: Ord + Clone>(
    tried: &BTreeSet<FaultSet<M>>,
    pairs: &Pairs<P, M>,
    dead: &Dead<M, P::Fact>,
    order: &dyn SendOrder<M>,
) -> Vec<P> {
    pairs
        .iter()
        .filter(|(pair, clauses)| {
            tried
                .iter()
                .any(|t| admissible_for(t, *pair, clauses, dead, order))
        })
        .map(|(p, _)| p.clone())
        .collect()
}

/// The candidate faults for `clauses`:
/// - `omit(m)` for every lossy delivery `m`;
/// - for every sender `n` of a delivery: `crash(n after m)` for every message `m` of `n` that is
///   a delivery of some clause, and `crash(n)`.
///
/// **Completion.** Every fault set `H` maps into this universe without losing a hit on any
/// clause: drop its faults that kill no delivery of any clause, and replace each crash of `n`
/// outside the universe, `crash(n after x)` for an `x` no clause names, by:
/// - `crash(n after y)`, where `y` is the last message some clause names that precedes `x` in
///   its chain of the [`SendOrder`], if there is one. Both spare exactly the named messages of
///   the chain up to `y`: a named `d` precedes `x` iff it is `y` or precedes `y` (no named
///   message lies between `y` and `x`), and a message outside the chain precedes neither. So
///   both kill the same named deliveries;
/// - `crash(n)` otherwise: no named message precedes `x`, so `crash(n after x)` kills every
///   named delivery from `n`, exactly what `crash(n)` kills.
///
/// The image crashes the same nodes as `H` (after dropping), so it is within every crash bound
/// `H` is. Hence "no admissible set
/// over the universe" is a statement about every fault set, as long as no pair is [excused]:
/// `crash(n)` can excuse a pair where the crash it replaces did not, so [`candidates`] adds a
/// replacement that excuses nothing.
///
/// Order, weakest first (the solver's tie-break prefers earlier candidates, so a hypothesis
/// leaves as much of the protocol running as it can): omissions, then per node the crash points
/// after later messages before earlier ones, `crash(n)` last. "Later" is the largest position
/// any observed run sent the message at; it is only a tie-break, never used to judge a kill.
pub fn universe<'a, M: Ord + Clone + 'a>(
    clauses: impl IntoIterator<Item = &'a PairClause<M>>,
) -> Vec<Fault<M>> {
    let mut omissions: BTreeSet<Fault<M>> = BTreeSet::new();
    // Per node, per message, the latest position it was seen sent at.
    let mut crash_points: BTreeMap<Node, BTreeMap<M, u64>> = BTreeMap::new();
    for dep in clauses.into_iter().flat_map(|c| c.deps.iter()) {
        if dep.lossy {
            omissions.insert(Fault::Omit(dep.message.clone()));
        }
        if let Some((sender, ordinal)) = &dep.sender {
            let latest = crash_points
                .entry(sender.clone())
                .or_default()
                .entry(dep.message.clone())
                .or_insert(*ordinal);
            *latest = (*latest).max(*ordinal);
        }
    }
    let mut out: Vec<Fault<M>> = omissions.into_iter().collect();
    for (node, messages) in crash_points {
        let mut points: Vec<(u64, M)> = messages.into_iter().map(|(m, at)| (at, m)).collect();
        points.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        out.extend(
            points
                .into_iter()
                .map(|(_, m)| Fault::crash_after(node.clone(), Some(m))),
        );
        out.push(Fault::crash_after(node, None));
    }
    out
}

/// One candidate of the hitting-set program.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Candidate<M> {
    /// A fault.
    Fault(Fault<M>),
    /// `crash(n after m)` for a message `m` of `n` that no clause names and no named message
    /// precedes, when no run has shown such a message: it kills every delivery from `n`, as
    /// `crash(n)` does, but excuses nothing. It cannot be run; a proposal that needs it goes to
    /// the enumerator.
    Unnamed(Node),
}

impl<M: Ord> Candidate<M> {
    /// Whether this candidate kills some delivery of `clause`.
    pub fn hits(&self, clause: &PairClause<M>, order: &dyn SendOrder<M>) -> bool {
        match self {
            Candidate::Fault(fault) => clause.hit_by(fault, order),
            Candidate::Unnamed(node) => clause
                .deps
                .iter()
                .any(|d| d.sender.as_ref().is_some_and(|(sender, _)| sender == node)),
        }
    }

    /// The node this candidate crashes, if it is a crash.
    pub fn crashed_node(&self) -> Option<&Node> {
        match self {
            Candidate::Fault(fault) => fault.crashed_node(),
            Candidate::Unnamed(node) => Some(node),
        }
    }
}

/// The [universe] of `pairs`, completed for the exemptions of [excused]: for every node `n`
/// whose `crash(n)` can excuse a pair (`n` is a pair's member, or `crash(n)` is part of a dead
/// set), a crash of `n` right after a message no clause names, placed right after `crash(n)`.
/// That is `crash(n after w)` for the first such `w` in `sends[n]` (every message the runs
/// showed `n` sending) that no named message precedes ([`SendOrder`]), or
/// [`Candidate::Unnamed`] if there is none. With `sends_complete`
/// (`sends` holds every message any node sends under any fault set, in any schedule) there is
/// no replacement when there is no such `w`: a crash after a message `n` never sends never
/// fires, so a set holding it runs exactly as the set without it.
///
/// For every node `n` whose crash after a message is part of a dead set, also every crash of
/// `n` after a message in `sends[n]` (and [`Candidate::Unnamed`] unless `sends_complete`):
/// mapping a crash of `n` to another crash point could land on the dead set's and excuse a
/// pair the original crash does not, so these crashes map to themselves.
///
/// **Completion with exemptions.** Map a fault set `H` admissible for a pair `p` as in
/// [universe], but where [universe] would replace a crash of an exempting node `n` after an
/// unnamed message by `crash(n)` (no named message precedes it), use this candidate instead. It
/// kills every named delivery from `n`, as the crash it replaces does. And keep a crash of a
/// node with a crash point in a dead set as it is: it is a candidate if `n` can send its
/// message, and otherwise it never fires (`sends_complete`), so `H` runs as `H` without it. The image holds `crash(n)` only where `H` does, and dead sets hold only
/// omissions and start crashes, so the image excuses `p` only if `H` does: it is still
/// admissible for `p`.
pub fn candidates<P: PairKey, M: Ord + Clone>(
    pairs: &Pairs<P, M>,
    dead: &Dead<M, P::Fact>,
    sends: &BTreeMap<Node, BTreeSet<M>>,
    sends_complete: bool,
    order: &dyn SendOrder<M>,
) -> Vec<Candidate<M>> {
    let named: BTreeSet<&M> = pairs
        .values()
        .flatten()
        .flat_map(|c| c.deps.iter().map(|d| &d.message))
        .collect();
    let exempting: BTreeSet<&Node> = pairs
        .keys()
        .map(PairKey::member)
        .chain(
            dead.iter()
                .flat_map(|(set, _)| set.iter().filter_map(Fault::crashed_node)),
        )
        .collect();
    // The nodes with a crash point (after a message) in a dead set.
    let pinned: BTreeSet<&Node> = dead
        .iter()
        .flat_map(|(set, _)| set.iter())
        .filter_map(|f| match f {
            Fault::Crash {
                node,
                at: CrashAt::After(_),
            } => Some(node),
            _ => None,
        })
        .collect();
    let mut out = vec![];
    for fault in universe(pairs.values().flatten()) {
        let replacement = match &fault {
            Fault::Crash {
                node,
                at: CrashAt::BeforeAnySend,
            } if exempting.contains(node) => match sends.get(node).and_then(|sent| {
                sent.iter()
                    .find(|w| !named.contains(w) && !named.iter().any(|d| order.precedes(d, w)))
            }) {
                Some(w) => Some(Candidate::Fault(Fault::crash_after(
                    node.clone(),
                    Some(w.clone()),
                ))),
                None if sends_complete => None,
                None => Some(Candidate::Unnamed(node.clone())),
            },
            _ => None,
        };
        let every_point = match &fault {
            Fault::Crash {
                node,
                at: CrashAt::BeforeAnySend,
            } if pinned.contains(node) => {
                let mut points: Vec<Candidate<M>> = sends
                    .get(node)
                    .into_iter()
                    .flatten()
                    .map(|w| Candidate::Fault(Fault::crash_after(node.clone(), Some(w.clone()))))
                    .collect();
                if !sends_complete {
                    points.push(Candidate::Unnamed(node.clone()));
                }
                points
            }
            _ => vec![],
        };
        out.push(Candidate::Fault(fault));
        out.extend(replacement);
        out.extend(every_point);
    }
    // A point listed twice (named, and again as one of every point) is one candidate.
    let mut seen = BTreeSet::new();
    out.retain(|c| seen.insert(c.clone()));
    out
}
