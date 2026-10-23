//! The crash sweep: one label's search, from zero crashes upwards.
//!
//! The search starts with a crash bound of 0, so only omissions. When no admissible hypothesis
//! is left at bound `k`, the label is certified for every fault set with at most `k` crashes.
//! The sweep then raises the bound to `k + 1` and keeps everything it learned. A derivation is a
//! fact about a run, not about the bound, so clauses, tried sets and vacuous sets all carry
//! over. The caller stops the sweep at the first counterexample. The label then tolerates
//! [`Sweep::tolerated`] crashes and breaks at [`Sweep::crash_bound`].
//!
//! Every hypothesis proposed at bound `k + 1` holds exactly `k + 1` crashes. Every set with at
//! most `k` crashes was already excluded at bound `k`, and constraints only accumulate. So a
//! counterexample found at bound `k + 1` really needs `k + 1` crashes, given what the search
//! has seen.

use std::collections::BTreeSet;

use super::clause::{Clause, Fault, FaultSet, universe};
use super::ilp::{Backend, CrashBounds, group_is_unkillable, hitting_set_any};

/// The default hypothesis budget, counting the failure-free probe. Once it is spent the verdict
/// is [`Next::Budget`], not a certificate (docs/design/hydrofrac-lite.md §2).
pub const HYPOTHESIS_BUDGET: usize = 1024;

/// What the sweep says to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next<M> {
    /// Run this hypothesis.
    Try(FaultSet<M>),
    /// No admissible hypothesis is left: the label holds.
    Certified(Tolerance),
    /// The hypothesis budget is spent. The label holds with at most [`Sweep::tolerated`]
    /// crashes, if that is `Some`.
    Budget,
}

/// How many crashes a certified label tolerates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tolerance {
    /// Every admissible fault set: any omissions, and any number of crashes short of the
    /// vacuity bounds. Either the vacuity bounds stopped the sweep, or no crash beyond the
    /// last bound could kill anything the search has seen.
    All,
    /// Every fault set with at most this many crashes. The explicit cap stopped the sweep.
    UpTo(usize),
}

/// One label's search state across crash bounds.
#[derive(Clone, Debug)]
pub struct Sweep<M> {
    bounds: CrashBounds,
    bound: usize,
    tolerated: Option<usize>,
    tried: BTreeSet<FaultSet<M>>,
    vacuous: Vec<FaultSet<M>>,
    extra: BTreeSet<Fault<M>>,
    budget: usize,
    backend: Backend,
}

impl<M: Ord + Clone> Sweep<M> {
    /// A sweep under `bounds`, starting at zero crashes. The failure-free probe counts as
    /// tried: it is hypothesis 0.
    pub fn new(bounds: CrashBounds) -> Self {
        Sweep {
            bounds,
            bound: 0,
            tolerated: None,
            tried: BTreeSet::from([FaultSet::new()]),
            vacuous: vec![],
            extra: BTreeSet::new(),
            budget: HYPOTHESIS_BUDGET,
            backend: Backend::default(),
        }
    }

    /// Sets the hypothesis budget, counting the probe.
    pub fn with_budget(mut self, budget: usize) -> Self {
        self.budget = budget;
        self
    }

    /// Sets the ILP backend.
    pub fn with_backend(mut self, backend: Backend) -> Self {
        self.backend = backend;
        self
    }

    /// Adds candidate faults beyond those the clauses propose. With no clauses, these alone
    /// make a blind enumeration of every admissible set by size.
    pub fn add_candidates(&mut self, faults: impl IntoIterator<Item = Fault<M>>) {
        self.extra.extend(faults);
    }

    /// Records that `set` gave a vacuous run, so neither it nor any superset is proposed again.
    pub fn mark_vacuous(&mut self, set: FaultSet<M>) {
        self.vacuous.push(set);
    }

    /// The current crash bound.
    pub fn crash_bound(&self) -> usize {
        self.bound
    }

    /// The largest crash bound certified so far: the label holds under every fault set with at
    /// most this many crashes. `None` while nothing is certified, not even omissions alone.
    pub fn tolerated(&self) -> Option<usize> {
        self.tolerated
    }

    /// The hypotheses tried so far, counting the probe.
    pub fn hypotheses(&self) -> usize {
        self.tried.len()
    }

    /// The bounds.
    pub fn bounds(&self) -> &CrashBounds {
        &self.bounds
    }

    /// The next step given every clause learned so far. A proposed set counts as tried.
    pub fn next(&mut self, clauses: &[Clause<M>]) -> Next<M> {
        let groups: Vec<&[Clause<M>]> = clauses.iter().map(std::slice::from_ref).collect();
        self.next_any(&groups)
    }

    /// [`Self::next`] with disjunctive clauses (see [`super::hitting_set_any`]): a hypothesis
    /// must satisfy some clause of every group. With one clause per group this is [`Self::next`].
    ///
    /// Used as a heuristic for schedule-sensitive labels, whose "certificates" are not proofs:
    /// the caller decides what [`Next::Certified`] means.
    pub fn next_any(&mut self, groups: &[&[Clause<M>]]) -> Next<M> {
        if groups.iter().any(|g| group_is_unkillable(g)) {
            return Next::Certified(Tolerance::All);
        }
        let clauses: Vec<Clause<M>> = groups.iter().flat_map(|g| g.iter().cloned()).collect();
        let universe = universe(&clauses, &self.extra);
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
