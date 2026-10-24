//! Per-pair clauses and the crash rule that does not depend on send order
//! (docs/design/hydrofrac-races.md §4.2, §4.3).
//!
//! A clause is one derivation of one pair (a member and a fact): the deliveries the member's
//! record of the fact was computed from. It is *robust* when no record in its backward closure
//! passed a fragile step, so it holds in every schedule (Lemma R, §5). Only robust clauses are
//! used to skip fault sets; fragile ones only steer the counterexample heuristic.

use std::collections::{BTreeMap, BTreeSet};

use crate::search::{CrashAt, Dependency, Derivation, Fault, FaultSet, Node};

/// One derivation of one pair, and whether it holds in every schedule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PairClause<M> {
    /// The deliveries the fact was derived from.
    pub deps: Derivation<M>,
    /// Whether no record of the derivation passed a fragile step.
    pub robust: bool,
}

/// The clauses learned so far, by pair. The engine keys pairs by `(member, fact)`.
pub type Pairs<P, M> = BTreeMap<P, Vec<PairClause<M>>>;

/// Whether `fault` kills the delivery `dep`, in every schedule (§4.2):
/// - `omit(m)` kills it iff `dep` is `m` on a lossy channel;
/// - `crash(n)` kills it iff `n` sent it;
/// - `crash(n after m)` kills it iff `n` sent it and it is not `m` itself.
///
/// The send's position in the run it was observed in (`dep.sender`'s ordinal) is ignored on
/// purpose: another schedule may order `n`'s sends differently.
///
/// **Why this is sound in every schedule.** The rule only claims that a delivery *survives*
/// when the crash cannot cut it whatever the order:
/// - a delivery from a node the fault does not crash is never cut by it;
/// - under `crash(n after m)`, the delivery `m` itself is sent before `n` dies in every
///   schedule in which it is sent at all (the crash fires right after it), and if `m` is never
///   sent the crash never fires.
///
/// So if a clause's only send from `n` is `m`, `crash(n after m)` spares it in every schedule.
/// Every other delivery from `n` may come after `m` in some schedule, so it counts as killed.
pub fn kills<M: PartialEq>(fault: &Fault<M>, dep: &Dependency<M>) -> bool {
    match fault {
        Fault::Omit(m) => dep.lossy && dep.message == *m,
        Fault::Crash { node, at } => {
            let Some((sender, _ordinal)) = &dep.sender else {
                return false;
            };
            sender == node
                && match at {
                    CrashAt::BeforeAnySend => true,
                    CrashAt::After(m) => dep.message != *m,
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
    pub fn hit_by(&self, fault: &Fault<M>) -> bool {
        self.deps.iter().any(|dep| kills(fault, dep))
    }

    /// Whether some fault of `set` kills some delivery of this clause.
    pub fn hit_by_set(&self, set: &FaultSet<M>) -> bool {
        set.iter().any(|fault| self.hit_by(fault))
    }
}

/// Whether `set` is admissible for a pair with these clauses: it kills every robust one. A pair
/// with no robust clause makes every set admissible.
pub fn admissible_for<M: Ord>(set: &FaultSet<M>, clauses: &[PairClause<M>]) -> bool {
    clauses
        .iter()
        .filter(|c| c.robust)
        .all(|c| c.hit_by_set(set))
}

/// Whether `set` can be skipped (§4.3, the corollary of Lemma R): for **every** pair, some
/// robust clause survives it, so in every schedule of `set` every pair is held or its member
/// crashed. Fragile clauses are ignored, and a pair with no robust clause is never satisfied,
/// so it keeps every set unpruned.
pub fn pruned<P, M: Ord>(set: &FaultSet<M>, pairs: &Pairs<P, M>) -> bool {
    pairs.values().all(|clauses| !admissible_for(set, clauses))
}

/// The pairs with no robust clause. Each makes every fault set admissible, so the hitting-set
/// program cannot finish the label and the engine must enumerate (§4.4, step 4).
pub fn unconstrained_pairs<P: Clone, M: Ord>(pairs: &Pairs<P, M>) -> Vec<P> {
    pairs
        .iter()
        .filter(|(_, clauses)| !clauses.iter().any(|c| c.robust))
        .map(|(p, _)| p.clone())
        .collect()
}

/// The pairs for which some set of `tried` is still admissible. The empty set (the probe) is
/// admissible exactly for the [unconstrained](unconstrained_pairs) pairs; a set that was run
/// and stays admissible for a pair gave that pair no robust derivation it spares, so every
/// superset of it is admissible for the pair too, including supersets adding faults that no
/// clause names.
pub fn stuck_pairs<P: Clone, M: Ord>(tried: &BTreeSet<FaultSet<M>>, pairs: &Pairs<P, M>) -> Vec<P> {
    pairs
        .iter()
        .filter(|(_, clauses)| tried.iter().any(|t| admissible_for(t, clauses)))
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
/// outside the universe by `crash(n)`. Such a crash is `crash(n after m)` for an `m` no clause
/// names, so it kills every delivery from `n`: exactly what `crash(n)` kills. The image crashes
/// a subset of `H`'s nodes, so it is within every crash bound `H` is. Hence "no admissible set
/// over the universe" is a statement about every fault set.
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
