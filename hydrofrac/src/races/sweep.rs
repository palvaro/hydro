//! The crash sweep over pairs (docs/design/hydrofrac-races.md §4.4): one label's search from
//! zero crashes upwards, as lite's (`search/sweep.rs`), keeping tried sets across bounds.
//!
//! At each bound it proposes first by Molly's heuristic ([`Scope::AllKnown`]), then by the
//! admissibility rule ([`Scope::RobustOnly`]), and concludes at the bound only when the rule
//! admits nothing.
//!
//! **Why concluding is sound.** Say the rule admits no untried set over the
//! [candidates] at bound `k`, and no tried set is still admissible for any pair (else the sweep
//! returns [`Next::Unconstrained`]). Take any fault set `H` with at most `k` crashes, admissible
//! for some pair `p`. Its completion image `H'` ([candidates]) lies in the candidates, is within
//! the same bounds, and is still admissible for `p`. So `H'` was tried, yet it is admissible: a
//! contradiction. (If `H'` needs a [`Candidate::Unnamed`], the sweep returns
//! [`Next::Unconstrained`] instead of concluding.) Hence every such `H` is
//! [pruned](super::pruned): by Lemma R (§5), or because it [excuses](super::excused) the pair,
//! every pair is held, or its member crashed, or its fact is held by no correct member, in
//! every schedule of `H`.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use super::clause::{Candidate, PairKey, Pairs, SendOrder, candidates, excused, stuck_pairs};
use super::ilp::{Scope, propose};
use crate::search::{
    Backend, CrashAt, CrashBounds, Fault, FaultSet, HYPOTHESIS_BUDGET, Node, Tolerance,
};

/// What the sweep says to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next<M, P> {
    /// Run this hypothesis.
    Try(FaultSet<M>),
    /// No fault set within the bounds is admissible: the label holds (Claim A).
    Certified(Tolerance),
    /// The hypothesis budget is spent: not a certificate.
    Budget,
    /// The hitting-set program cannot finish the label; the engine must
    /// [enumerate](super::Enumerator) (§4.4, step 4). Either some tried set (perhaps the empty
    /// probe) is still admissible for these pairs, so every superset of it that does not
    /// excuse them is too, over faults no clause names; or the next set admissible for them
    /// needs a crash after a message no run has shown ([`Candidate::Unnamed`]).
    Unconstrained(Vec<P>),
}

/// One label's search state across crash bounds, for pairs whose facts are `F`.
#[derive(Clone, Debug)]
pub struct Sweep<M, F> {
    bounds: CrashBounds,
    bound: usize,
    tolerated: Option<usize>,
    tried: BTreeSet<FaultSet<M>>,
    dead: Vec<(FaultSet<M>, F)>,
    sends: BTreeMap<Node, BTreeSet<M>>,
    sends_complete: bool,
    order: Rc<dyn SendOrder<M>>,
    /// The hypothesis budget, counting the probe.
    pub budget: usize,
    /// The solver.
    pub backend: Backend,
}

impl<M: Ord + Clone + 'static, F: Ord + Clone> Sweep<M, F> {
    /// A sweep under `bounds`, at zero crashes; the probe (the empty set) counts as tried.
    pub fn new(bounds: CrashBounds) -> Self {
        Sweep {
            bounds,
            bound: 0,
            tolerated: None,
            tried: BTreeSet::from([FaultSet::new()]),
            dead: vec![],
            sends: BTreeMap::new(),
            sends_complete: false,
            order: Rc::new(()),
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

    /// Whether `crash(node after m)` is *exact*: the runs have shown every message `node` can
    /// send ([`Self::sends_complete`]), and every one of them lies in `m`'s chain of the send
    /// order (it is `m`, precedes `m`, or follows `m`). Then in every run the crash cuts exactly
    /// the messages that follow `m` in the chain: those before `m` are sent before it, and one
    /// after it is only ever sent after it.
    pub fn is_exact(&self, node: &Node, m: &M) -> bool {
        self.sends_complete
            && self.sends.get(node).is_none_or(|sent| {
                sent.iter()
                    .all(|w| w == m || self.order.precedes(w, m) || self.order.precedes(m, w))
            })
    }

    /// Records that `fact` is dead under `set`: no correct member holds it, in any schedule
    /// of `set`. From then on `set` and every superset of it are admissible for no pair of
    /// `fact` ([`excused`](super::excused)).
    ///
    /// **Preconditions** (the engine checks them; only the second is checked here):
    /// - it has verified the loss in every schedule of `set`;
    /// - every crash of `set` is a start crash (`crash(n)`, before any send), or an
    ///   [exact](Self::is_exact) `crash(n after m)` of a node `n` that is not a member of the
    ///   label's cluster;
    /// - the program is fragile-free.
    ///
    /// **Why sound.** Suppose a superset `H ⊇ set` gave some correct member `c` a derivation
    /// `D` of `fact` in some schedule. In a fragile-free program `D` is robust. It survives
    /// `set` under the crash rule (§4.2):
    /// - an omission of `set` is in `H`, and `D`'s deliveries were not omitted;
    /// - a start crash of `set` is in `H`, so its node sent nothing, and `D` has no delivery
    ///   from it;
    /// - for an exact `crash(n after m)` of `set` (also in `H`), every delivery `d` of `D` from
    ///   `n` lies in `m`'s chain. If `d` followed `m`, sending it would have sent `m` first,
    ///   and the crash would have cut `d`. So `d` is `m` or precedes it, and the rule spares
    ///   it.
    ///
    /// `c` is not crashed by `set` in any schedule: its start crashes are in `H` too, where `c`
    /// is correct, and its other crashes are of nodes outside the cluster. So by Lemma R `c`
    /// holds `fact` in every schedule of `set`: a contradiction. In a program with fragile
    /// steps a superset can create `fact`, so the engine must not call this there.
    pub fn mark_dead(&mut self, set: FaultSet<M>, fact: F) {
        assert!(
            set.iter().all(|f| match f {
                Fault::Omit(_)
                | Fault::Crash {
                    at: CrashAt::BeforeAnySend,
                    ..
                } => true,
                Fault::Crash {
                    node,
                    at: CrashAt::After(m),
                } => self.is_exact(node, m),
            }),
            "a dead set holds only omissions, start crashes and exact crash points"
        );
        self.dead.push((set, fact));
    }

    /// The dead sets recorded so far, with their facts.
    pub fn dead(&self) -> &[(FaultSet<M>, F)] {
        &self.dead
    }

    /// Records messages a run showed `node` sending. They give crash points after messages no
    /// clause names ([`candidates`]), which the completion needs once a start crash can excuse
    /// a pair.
    pub fn saw_sends(&mut self, node: &Node, messages: impl IntoIterator<Item = M>) {
        self.sends.entry(node.clone()).or_default().extend(messages);
    }

    /// Uses `order` (what is known of every run's send order) in the crash rule.
    pub fn with_order(mut self, order: Rc<dyn SendOrder<M>>) -> Self {
        self.order = order;
        self
    }

    /// The send order the crash rule uses.
    pub fn order(&self) -> &dyn SendOrder<M> {
        &*self.order
    }

    /// Records that [`Self::saw_sends`] has been told every message any node sends under any
    /// fault set, in any schedule. True of a fragile-free program once its failure-free run was
    /// read: every record under a fault set is computed from a derivation the fault set leaves
    /// intact, which exists without faults too, so every send under faults is a send without
    /// them. Then a crash after a message no run showed never fires, and needs no candidate
    /// ([`candidates`]).
    pub fn sends_complete(&mut self) {
        self.sends_complete = true;
    }

    /// The next step given every clause learned so far. A proposed set counts as tried.
    pub fn next<P: PairKey<Fact = F>>(&mut self, pairs: &Pairs<P, M>) -> Next<M, P> {
        let stuck = stuck_pairs(&self.tried, pairs, &self.dead, &*self.order);
        if !stuck.is_empty() {
            return Next::Unconstrained(stuck);
        }
        let universe = candidates(
            pairs,
            &self.dead,
            &self.sends,
            self.sends_complete,
            &*self.order,
        );
        // The most crashes a set over this universe can hold: one per node, and at most the
        // checked cluster's cap among its members. Past it, a higher bound admits nothing new.
        let crash_nodes: BTreeSet<_> = universe
            .iter()
            .filter_map(Candidate::crashed_node)
            .collect();
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
                let Some(proposal) = propose(
                    &universe,
                    pairs,
                    &self.dead,
                    &self.tried,
                    &bounds,
                    scope,
                    self.backend,
                    &*self.order,
                ) else {
                    continue;
                };
                let Some(set) = proposal
                    .iter()
                    .map(|c| match c {
                        Candidate::Fault(f) => Some(f.clone()),
                        Candidate::Unnamed(_) => None,
                    })
                    .collect::<Option<FaultSet<M>>>()
                else {
                    return Next::Unconstrained(self.admitting(&proposal, pairs, scope));
                };
                self.tried.insert(set.clone());
                return Next::Try(set);
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

impl<M: Ord + Clone + 'static, F: Ord + Clone> Sweep<M, F> {
    /// The pairs `proposal` is admissible for at `scope`.
    fn admitting<P: PairKey<Fact = F>>(
        &self,
        proposal: &BTreeSet<Candidate<M>>,
        pairs: &Pairs<P, M>,
        scope: Scope,
    ) -> Vec<P> {
        let faults: FaultSet<M> = proposal
            .iter()
            .filter_map(|c| match c {
                Candidate::Fault(f) => Some(f.clone()),
                Candidate::Unnamed(_) => None,
            })
            .collect();
        pairs
            .iter()
            .filter(|(pair, clauses)| {
                !excused(&faults, *pair, &self.dead)
                    && clauses
                        .iter()
                        .filter(|c| scope == Scope::AllKnown || c.robust)
                        .all(|c| proposal.iter().any(|f| f.hits(c, &*self.order)))
            })
            .map(|(p, _)| p.clone())
            .collect()
    }
}
