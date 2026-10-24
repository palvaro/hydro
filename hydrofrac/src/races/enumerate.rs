//! The fallback for labels the hitting-set program cannot finish (docs/design/hydrofrac-races.md
//! §4.4, step 4): every fault set over a harvested universe, smallest first, skipping those
//! already tried or [pruned].
//!
//! The universe is the brute-force enumerator's (`check/brute_force.rs` `harvest`): every lossy
//! message and every crash point seen in any run. The engine grows it as runs reveal more.

use std::collections::BTreeSet;

use super::clause::{Dead, PairKey, Pairs, SendOrder, pruned};
use crate::search::{CrashBounds, Fault, FaultSet, Node};

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

/// A cursor over the subsets of a universe, by increasing size, then lexicographically by
/// position in the universe (the order faults were first added). Deterministic.
///
/// A skipped set is not revisited: the caller's clauses and dead sets only grow, and a pruned
/// set stays pruned when they do. Growing the universe restarts the cursor (tried sets are
/// still skipped).
#[derive(Clone, Debug)]
pub struct Enumerator<M> {
    universe: Vec<Fault<M>>,
    max_faults: usize,
    /// The positions of the next subset to look at; `None` once every size is done.
    cursor: Option<Vec<usize>>,
}

impl<M: Ord + Clone> Enumerator<M> {
    /// Subsets of `universe` (deduplicated, first occurrence kept) of up to `max_faults` faults.
    pub fn new(universe: impl IntoIterator<Item = Fault<M>>, max_faults: usize) -> Self {
        let mut this = Enumerator {
            universe: vec![],
            max_faults,
            cursor: Some(vec![]),
        };
        this.extend(universe);
        this
    }

    /// The candidate faults, in enumeration order.
    pub fn universe(&self) -> &[Fault<M>] {
        &self.universe
    }

    /// Adds faults the runs revealed. Returns whether any was new; if so, the cursor restarts.
    pub fn extend(&mut self, faults: impl IntoIterator<Item = Fault<M>>) -> bool {
        let mut grew = false;
        for fault in faults {
            if !self.universe.contains(&fault) {
                self.universe.push(fault);
                grew = true;
            }
        }
        if grew {
            self.cursor = Some(vec![]);
        }
        grew
    }

    /// The next set within `bounds` that is neither in `tried` nor [pruned] by `pairs` and
    /// `dead`, or `None` once every set of up to `max_faults` faults has been passed.
    pub fn next<P: PairKey>(
        &mut self,
        bounds: &CrashBounds,
        tried: &BTreeSet<FaultSet<M>>,
        pairs: &Pairs<P, M>,
        dead: &Dead<M, P::Fact>,
        order: &dyn SendOrder<M>,
    ) -> Option<FaultSet<M>> {
        while let Some(positions) = self.cursor.take() {
            self.cursor = self.advance(&positions);
            let set: FaultSet<M> = positions
                .iter()
                .map(|&i| self.universe[i].clone())
                .collect();
            if within_bounds(&set, bounds)
                && !tried.contains(&set)
                && !pruned(&set, pairs, dead, order)
            {
                return Some(set);
            }
        }
        None
    }

    /// The subset after `positions`: the next combination of the same size, or the first of
    /// the next size.
    fn advance(&self, positions: &[usize]) -> Option<Vec<usize>> {
        let n = self.universe.len();
        let k = positions.len();
        let mut next = positions.to_vec();
        // The rightmost position that can still move right.
        if let Some(i) = (0..k).rev().find(|&i| next[i] < n - k + i) {
            next[i] += 1;
            for j in i + 1..k {
                next[j] = next[j - 1] + 1;
            }
            return Some(next);
        }
        (k < self.max_faults.min(n)).then(|| (0..k + 1).collect())
    }
}
