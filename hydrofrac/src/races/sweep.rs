//! The crash sweep over pairs (docs/design/hydrofrac-races.md §4.4, §12): one label's search
//! from zero crashes upwards, keeping tried sets across bounds.
//!
//! At each bound it proposes first by Molly's heuristic ([`Scope::AllKnown`]), then by the
//! admissibility rule ([`Scope::RobustOnly`]), and concludes at the bound only when the rule
//! admits nothing untried. There is no other way to conclude: no size bound, no fallback. A pair
//! no robust derivation protects keeps every set admissible, so the program proposes every set
//! over the candidates, one by one, and that is the cost.
//!
//! **Why concluding is sound.** The candidates are the faults the runs revealed, plus, unless
//! sends are complete, [`Candidate::Unseen`] for each crashable node ([`candidates`]). Say the
//! rule admits no untried proposal over them at bound `k`. Take any fault set `H` with at most `k`
//! crashes, any schedule `σ` of it, and its [image] `H°`.
//! - **`H°` is admissible for no pair.** For each pair `p`, either `H°` excuses `p`, or some
//!   robust clause `D` of `p` is killed by no candidate of `H°`. If `H°` excuses `p`, so does `H`
//!   ([image]): `H` crashes `p`'s member at start, so it is not correct in `σ`, or `H` holds a
//!   set under which `p`'s fact is dead, so no correct member holds it in `σ`
//!   ([`Sweep::mark_dead`]). Otherwise no fault of `H` kills `D`, except crashes that never fire
//!   ([image]), and by Lemma R (§5) `p` is held in `σ`, or its member crashed.
//! - **`H°` is admissible for some pair.** It is within the bounds, so it was tried. A tried
//!   proposal that is still admissible was explored without stopping early (an exploration stops
//!   early only once its set is pruned, and clauses only grow, so it stays pruned), and every
//!   schedule held, or the search would have ended. Let `S` be `H°` without its
//!   [`Candidate::Unseen`] crashes. `S` was *settled*: every schedule was run, or its facts are
//!   the same in every schedule (one schedule of a fragile-free program, with exact crash points).
//!   If `H°` has an unseen crash, `S` was moreover explored in full ([`Next::Try`]). The faults of
//!   `H` not in `S` are omissions and crash points after messages no run showed. No run of `S`
//!   sends them: `S`'s runs were all read, or, in a fragile-free program, every message any run
//!   sends is shown by the probe (`sends_complete`). So none of these faults fires, and `σ` is a
//!   schedule of `S`, which held.
//!
//! Either way the label holds in `σ` (Claim A). If the candidates grow after a bound concluded,
//! the conclusion is about the old candidates, so the sweep restarts from zero crashes (the tried
//! sets are kept, so this costs only solver calls).

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use super::clause::{
    Candidate, PairKey, Pairs, Proposal, candidates, faults_of, image, proposal_of,
};
use super::ilp::{Hits, Scope, propose};
use super::literals::{Literal, SendOrder, after};
use crate::search::{CrashAt, CrashBounds, Fault, FaultSet, HYPOTHESIS_BUDGET, Node, Tolerance};

/// What the sweep says to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next<M> {
    /// Run this set. With `in_full`, run every schedule, without stopping early, and report it
    /// with [`Sweep::explored_in_full`]: the proposal held a [`Candidate::Unseen`] crash, which
    /// stands for every crash after a message the set's runs do not send, and those are all the
    /// set itself once its runs have shown every message they send; or the label has no pair, so
    /// every set is run in full for Claim B.
    Try {
        /// The fault set.
        set: FaultSet<M>,
        /// Whether to run every schedule.
        in_full: bool,
    },
    /// No fault set within the bounds is admissible: the label holds (Claim A). With no pair,
    /// every set within the bounds was tried, in full (Claim B).
    Certified(Tolerance),
    /// The hypothesis budget is spent: not a certificate.
    Budget,
}

/// One label's search state across crash bounds, for pairs whose facts are `F`.
#[derive(Clone, Debug)]
pub struct Sweep<M, F> {
    bounds: CrashBounds,
    bound: usize,
    tolerated: Option<usize>,
    /// How many candidates there were when the current bound's search began.
    universe_len: usize,
    tried: BTreeSet<Proposal<M>>,
    in_full: BTreeSet<FaultSet<M>>,
    dead: Vec<(FaultSet<M>, F)>,
    known: BTreeSet<Fault<M>>,
    latest: BTreeMap<Node, BTreeMap<M, u64>>,
    sends_complete: bool,
    order: Rc<dyn SendOrder<M>>,
    hits: Hits<M, F>,
    runs: usize,
    /// The hypothesis budget: how many sets may be run, counting the probe.
    pub budget: usize,
}

impl<M: Ord + Clone + 'static, F: Ord + Clone> Sweep<M, F> {
    /// A sweep under `bounds`, at zero crashes; the probe (the empty set) counts as tried.
    pub fn new(bounds: CrashBounds) -> Self {
        Sweep {
            bounds,
            bound: 0,
            tolerated: None,
            universe_len: 0,
            tried: BTreeSet::from([Proposal::new()]),
            in_full: BTreeSet::new(),
            dead: vec![],
            known: BTreeSet::new(),
            latest: BTreeMap::new(),
            sends_complete: false,
            order: Rc::new(()),
            hits: Hits::default(),
            runs: 1,
            budget: HYPOTHESIS_BUDGET,
        }
    }

    /// The current crash bound.
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// The largest crash bound certified over the current candidates (`None`: not even omissions
    /// alone).
    pub fn tolerated(&self) -> Option<usize> {
        self.tolerated
    }

    /// The proposals tried so far, counting the probe.
    pub fn tried(&self) -> &BTreeSet<Proposal<M>> {
        &self.tried
    }

    /// Records a fault a run revealed: `omit(m)` for a lossy message sent before the heal,
    /// `crash(n)` for a crashable node, `crash(n after m)` for a message it sent before the heal.
    pub fn saw_fault(&mut self, fault: Fault<M>) {
        self.known.insert(fault);
    }

    /// Records that a run showed `node` sending `m` before the heal, as its `position`-th send.
    pub fn saw_send(&mut self, node: &Node, m: M, position: u64) {
        let at = self
            .latest
            .entry(node.clone())
            .or_default()
            .entry(m)
            .or_insert(position);
        *at = (*at).max(position);
    }

    /// Records that every schedule of `set` was run, without stopping early, and every message
    /// its runs sent was reported ([`Self::saw_fault`]).
    pub fn explored_in_full(&mut self, set: FaultSet<M>) {
        self.in_full.insert(set);
    }

    /// Records that [`Self::saw_fault`] has been told every message any node sends under any
    /// fault set, in any schedule. True of a fragile-free program once its failure-free run was
    /// read: every record under a fault set is computed from a derivation the fault set leaves
    /// intact, which exists without faults too, so every send under faults is a send without
    /// them. Then a crash after a message no run showed never fires (the never-sent proof,
    /// [`super::literals`]), and no [`Candidate::Unseen`] is needed.
    pub fn sends_complete(&mut self) {
        self.sends_complete = true;
    }

    /// Uses `order` (what is proved of every run's send order) for the crash literals.
    pub fn with_order(mut self, order: Rc<dyn SendOrder<M>>) -> Self {
        self.order = order;
        self.hits = Hits::default();
        self
    }

    /// The send order the crash literals use.
    pub fn order(&self) -> &dyn SendOrder<M> {
        &*self.order
    }

    /// Whether `crash(node after m)` touches only proved literals: the runs have shown every
    /// message `node` can send ([`Self::sends_complete`]), and for each of them `w ≠ m` the literal
    /// `after(w, m)` is fixed. Then in every run the crash cuts exactly the sends `w` whose literal
    /// is true.
    pub fn is_exact(&self, node: &Node, m: &M) -> bool {
        self.sends_complete
            && self.latest.get(node).is_none_or(|sent| {
                sent.keys()
                    .all(|w| w == m || after(&*self.order, w, m) != Literal::Free)
            })
    }

    /// Records that `fact` is dead under `set`: no correct member holds it, in any schedule of
    /// `set`. From then on `set` and every superset of it excuse every pair of `fact`
    /// ([`super::excused`]), a row with no literal.
    ///
    /// **Preconditions** (the engine checks them; only the second is checked here):
    /// - it has verified the loss in every schedule of `set`;
    /// - every crash of `set` is a start crash (`crash(n)`, before any send), or an
    ///   [exact](Self::is_exact) `crash(n after m)` of a node `n` that is not a member of the
    ///   label's cluster;
    /// - the program is fragile-free.
    ///
    /// **Why sound.** Suppose a superset `H ⊇ set` gave some correct member `c` a derivation `D`
    /// of `fact` in some schedule. In a fragile-free program `D` is robust. No fault of `set`
    /// kills it:
    /// - an omission of `set` is in `H`, and `D`'s deliveries were not omitted;
    /// - a start crash of `set` is in `H`, so its node sent nothing, and `D` has no delivery from
    ///   it;
    /// - for an exact `crash(n after m)` of `set` (also in `H`), every message `n` can send has a
    ///   proved literal `after(·, m)`. A delivery `d` of `D` from `n` whose literal is true would
    ///   be sent after `m`, which the crash in `H` cuts; so its literal is false, and the crash
    ///   rule spares it.
    ///
    /// `c` is correct under `set` too: its start crashes are in `H`, where `c` is correct, and its
    /// other crashes are of nodes outside the cluster. So by Lemma R, `c` holds `fact` in every
    /// schedule of `set`: a contradiction. The argument uses only Lemma R and proved literals,
    /// never the send order a run happened to show, so a crash point whose meaning moves with the
    /// send order (S2) cannot reach it. In a program with fragile steps a superset can create
    /// `fact`, so the engine must not call this there.
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

    /// The candidates of the program, as [candidates] gives them.
    pub fn universe(&self) -> Vec<Candidate<M>> {
        candidates(&self.known, &self.latest, !self.sends_complete)
    }

    /// The image of `set` among the candidates ([image]).
    pub fn image(&self, set: &FaultSet<M>) -> Proposal<M> {
        image(set, &self.known, !self.sends_complete)
    }

    /// Whether every set over the candidates within the full bounds was tried (for Claim B, §2).
    pub fn covers_everything<P: PairKey<Fact = F>>(&self, pairs: &Pairs<P, M>) -> bool {
        propose(
            &self.universe(),
            pairs,
            &self.dead,
            &self.tried,
            &self.bounds,
            Scope::Any,
            &*self.order,
            &mut Hits::default(),
        )
        .is_none()
    }

    /// The next step given every clause learned so far. A proposed set counts as tried.
    pub fn next<P: PairKey<Fact = F>>(&mut self, pairs: &Pairs<P, M>) -> Next<M> {
        let universe = self.universe();
        if universe.len() != self.universe_len {
            self.universe_len = universe.len();
            self.bound = 0;
            self.tolerated = None;
        }
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
        'bound: loop {
            if self.runs >= self.budget {
                return Next::Budget;
            }
            let bounds = self.bounds.capped_at(self.bound);
            // No pair (an empty domain): Claim A holds of every set, so the only claim left is
            // Claim B, which needs every set within the bounds run in full.
            let everything = pairs.is_empty();
            let scopes: &[Scope] = if everything {
                &[Scope::Any]
            } else {
                &[Scope::AllKnown, Scope::RobustOnly]
            };
            for &scope in scopes {
                let Some(proposal) = propose(
                    &universe,
                    pairs,
                    &self.dead,
                    &self.tried,
                    &bounds,
                    scope,
                    &*self.order,
                    &mut self.hits,
                ) else {
                    continue;
                };
                self.tried.insert(proposal.clone());
                let set = faults_of(&proposal);
                let unseen = set.len() != proposal.len();
                if unseen {
                    if self.in_full.contains(&set) {
                        // Resolved: its unseen crashes never fire in `set`'s runs.
                        continue 'bound;
                    }
                    self.tried.insert(proposal_of(&set));
                }
                self.runs += 1;
                return Next::Try {
                    set,
                    in_full: unseen || everything,
                };
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
