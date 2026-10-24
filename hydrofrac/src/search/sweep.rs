//! The crash sweep (docs/design/hydrofrac-lite.md §4.5): one label's search from zero crashes
//! upwards, keeping clauses, tried sets and vacuous sets across bounds.
//!
//! Every hypothesis proposed at bound `k + 1` holds exactly `k + 1` crashes, because bound `k`
//! was exhausted first. That needs "exhausted" to cover every positional fault set, not only
//! the weakest candidates the clauses propose, so before concluding at a bound the sweep asks
//! once more over the universe extended by [`completion`].

use std::collections::BTreeSet;

use super::clause::{Clause, Fault, FaultSet, completion, universe};
use super::ilp::{Backend, CrashBounds, group_is_unkillable, hitting_set_any};

/// The hypothesis budget, counting the failure-free probe (§2).
pub const HYPOTHESIS_BUDGET: usize = 1024;

/// What the sweep says to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next<M> {
    /// Run this hypothesis.
    Try(FaultSet<M>),
    /// No admissible hypothesis is left: the label holds.
    Certified(Tolerance),
    /// The hypothesis budget is spent: not a certificate.
    Budget,
}

/// How many crashes a certified label tolerates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tolerance {
    /// Every admissible fault set (the vacuity bounds, or saturation, stopped the sweep).
    All,
    /// Every fault set with at most this many crashes (the `max_crashes` cap stopped it).
    UpTo(usize),
}

/// One label's search state across crash bounds.
#[derive(Clone, Debug)]
pub struct Sweep<M> {
    bounds: CrashBounds,
    /// The current crash bound.
    pub(super) bound: usize,
    tolerated: Option<usize>,
    tried: BTreeSet<FaultSet<M>>,
    vacuous: Vec<FaultSet<M>>,
    /// The hypothesis budget, counting the probe.
    pub(super) budget: usize,
    pub(super) backend: Backend,
}

impl<M: Ord + Clone> Sweep<M> {
    /// A sweep under `bounds`, at zero crashes; the probe (the empty set) counts as tried.
    pub fn new(bounds: CrashBounds) -> Self {
        Sweep {
            bounds,
            bound: 0,
            tolerated: None,
            tried: BTreeSet::from([FaultSet::new()]),
            vacuous: vec![],
            budget: HYPOTHESIS_BUDGET,
            backend: Backend::default(),
        }
    }

    /// Records that `set` gave a vacuous run: neither it nor a superset is proposed again.
    pub fn mark_vacuous(&mut self, set: FaultSet<M>) {
        self.vacuous.push(set);
    }

    /// The largest crash bound certified so far (`None`: not even omissions alone).
    pub fn tolerated(&self) -> Option<usize> {
        self.tolerated
    }

    /// The hypotheses tried so far, counting the probe.
    pub fn hypotheses(&self) -> usize {
        self.tried.len()
    }

    /// The next step given every clause learned so far. A proposed set counts as tried.
    pub fn next(&mut self, clauses: &[Clause<M>]) -> Next<M> {
        let groups: Vec<&[Clause<M>]> = clauses.iter().map(std::slice::from_ref).collect();
        self.next_any(&groups)
    }

    /// [`Self::next`] with disjunctive clauses ([`super::hitting_set_any`]): the heuristic for
    /// programs with schedule choices, whose [`Next::Certified`] the caller does not trust (§4.9).
    pub fn next_any(&mut self, groups: &[&[Clause<M>]]) -> Next<M> {
        if groups.iter().any(|g| group_is_unkillable(g)) {
            return Next::Certified(Tolerance::All);
        }
        let clauses: Vec<Clause<M>> = groups.iter().flat_map(|g| g.iter().cloned()).collect();
        let universe = universe(&clauses, []);
        // The most crashes any admissible set over this universe can hold: one per node, and
        // at most the checked cluster's cap among its members.
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
        // The completed universe, computed the first time the weakest candidates admit nothing:
        // only then is "no hitting set" true of every fault set.
        let mut complete: Option<Option<Vec<Fault<M>>>> = None;
        loop {
            if self.tried.len() >= self.budget {
                return Next::Budget;
            }
            let bounds = self.bounds.capped_at(self.bound);
            if let Some(set) = hitting_set_any(
                &universe,
                groups,
                &self.tried,
                &self.vacuous,
                &bounds,
                self.backend,
            ) {
                self.tried.insert(set.clone());
                return Next::Try(set);
            }
            let complete = complete.get_or_insert_with(|| {
                let extra = completion(&clauses, &self.vacuous);
                if extra.iter().all(|f| universe.contains(f)) {
                    None
                } else {
                    Some(super::clause::universe(&clauses, &extra))
                }
            });
            if let Some(complete) = complete
                && let Some(set) = hitting_set_any(
                    complete,
                    groups,
                    &self.tried,
                    &self.vacuous,
                    &bounds,
                    self.backend,
                )
            {
                self.tried.insert(set.clone());
                return Next::Try(set);
            }
            self.tolerated = Some(self.bound);
            // A higher bound admits no new set once it covers every crash the universe can
            // hold, or once it reaches the vacuity bound.
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
