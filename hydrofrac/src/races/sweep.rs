//! The crash sweep over pairs (docs/design/hydrofrac-races.md §4.4): one label's search from
//! zero crashes upwards, as lite's (`search/sweep.rs`), keeping tried sets across bounds.
//!
//! At each bound it proposes first by Molly's heuristic ([`Scope::AllKnown`]), then by the
//! admissibility rule ([`Scope::RobustOnly`]), and concludes at the bound only when the rule
//! admits nothing.
//!
//! **Why concluding is sound.** Say the rule admits no untried set over the [universe] at
//! bound `k`, and no tried set is still admissible for any pair (else the sweep returns
//! [`Next::Unconstrained`]). Take any fault set `H` with at most `k` crashes, admissible for
//! some pair `p`. Its completion image `H'` ([universe]) lies in the universe, is within the
//! same bounds, and is still admissible for `p`. So `H'` was tried, yet it is admissible: a
//! contradiction. Hence every such `H` is [pruned](super::pruned), and by Lemma R (§5) every
//! pair is held or its member crashed in every schedule of `H`.

use std::collections::BTreeSet;

use super::clause::{Pairs, stuck_pairs, universe};
use super::ilp::{Scope, propose};
use crate::search::{Backend, CrashBounds, Fault, FaultSet, HYPOTHESIS_BUDGET, Tolerance};

/// What the sweep says to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next<M, P> {
    /// Run this hypothesis.
    Try(FaultSet<M>),
    /// No fault set within the bounds is admissible: the label holds (Claim A).
    Certified(Tolerance),
    /// The hypothesis budget is spent: not a certificate.
    Budget,
    /// For these pairs some tried set (perhaps the empty probe) is still admissible, so every
    /// superset of it is too, over faults no clause names. The hitting-set program cannot
    /// finish the label; the engine must [enumerate](super::Enumerator) (§4.4, step 4).
    Unconstrained(Vec<P>),
}

/// One label's search state across crash bounds.
#[derive(Clone, Debug)]
pub struct Sweep<M> {
    bounds: CrashBounds,
    bound: usize,
    tolerated: Option<usize>,
    tried: BTreeSet<FaultSet<M>>,
    /// The hypothesis budget, counting the probe.
    pub budget: usize,
    /// The solver.
    pub backend: Backend,
}

impl<M: Ord + Clone> Sweep<M> {
    /// A sweep under `bounds`, at zero crashes; the probe (the empty set) counts as tried.
    pub fn new(bounds: CrashBounds) -> Self {
        Sweep {
            bounds,
            bound: 0,
            tolerated: None,
            tried: BTreeSet::from([FaultSet::new()]),
            budget: HYPOTHESIS_BUDGET,
            backend: Backend::default(),
        }
    }

    /// The current crash bound.
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// The largest crash bound certified so far (`None`: not even omissions alone).
    pub fn tolerated(&self) -> Option<usize> {
        self.tolerated
    }

    /// The sets tried so far, counting the probe.
    pub fn tried(&self) -> &BTreeSet<FaultSet<M>> {
        &self.tried
    }

    /// Records that `set` was run some other way (e.g. by the [enumerator](super::Enumerator)).
    pub fn mark_tried(&mut self, set: FaultSet<M>) {
        self.tried.insert(set);
    }

    /// The next step given every clause learned so far. A proposed set counts as tried.
    pub fn next<P: Clone>(&mut self, pairs: &Pairs<P, M>) -> Next<M, P> {
        let stuck = stuck_pairs(&self.tried, pairs);
        if !stuck.is_empty() {
            return Next::Unconstrained(stuck);
        }
        let universe = universe(pairs.values().flatten());
        // The most crashes a set over this universe can hold: one per node, and at most the
        // checked cluster's cap among its members. Past it, a higher bound admits nothing new.
        let crash_nodes: BTreeSet<_> = universe.iter().filter_map(Fault::crashed_node).collect();
        let crash_nodes = match &self.bounds.checked {
            None => crash_nodes.len(),
            Some(checked) => {
                let members = crash_nodes
                    .iter()
                    .filter(|n| checked.nodes.contains(**n))
                    .count();
                crash_nodes.len() - members + members.min(checked.cap)
            }
        };
        loop {
            if self.tried.len() >= self.budget {
                return Next::Budget;
            }
            let bounds = self.bounds.capped_at(self.bound);
            for scope in [Scope::AllKnown, Scope::RobustOnly] {
                if let Some(set) =
                    propose(&universe, pairs, &self.tried, &bounds, scope, self.backend)
                {
                    self.tried.insert(set.clone());
                    return Next::Try(set);
                }
            }
            self.tolerated = Some(self.bound);
            let saturated = crash_nodes <= self.bound;
            let vacuity = (self.bounds.nodes >= 1).then(|| self.bounds.nodes - 1);
            if saturated || vacuity.is_some_and(|v| self.bound >= v) {
                return Next::Certified(Tolerance::All);
            }
            if self.bounds.cap.is_some_and(|cap| self.bound >= cap) {
                return Next::Certified(Tolerance::UpTo(self.bound));
            }
            self.bound += 1;
        }
    }
}
