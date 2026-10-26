//! The rule hydrofrac checks, evaluated on the host (docs/design/hydrofrac-implies.md).
//!
//! For collections `A` and `B` (each a tapped collection, after its key function): **if a fact
//! is in `A` at a member the A-side chooses ([`ASide`]), it is in `B` at the members the B-side
//! chooses ([`BSide`])**. The facts of `A` held by the chosen members are the run's
//! *obligations*: they are read from each run, never from another one. A collection labelled
//! `EventualConsistency` is the instance `A = B =` that collection, `Correct → AllCorrect`:
//! every fact a correct member holds, every correct member holds.
//!
//! The view a member's tap gives is the union of what it emitted ([`MemberView`]), and a tap
//! emits each element of a stream once it exists. A crashed member's view is what it emitted
//! before its crash fired: the fault hook drops only what it emits after.
//!
//! The module is plain data: it takes observations in program order, whatever produced them,
//! and is generic over `S`, the support of an observation (the provenance record lineage reads
//! the observation's derivation from).

use std::collections::{BTreeMap, BTreeSet};

use crate::observe::Fact;

/// One element a member's tap emitted.
#[derive(Clone, Debug)]
pub struct Observation<S> {
    /// The label (index into [`crate::observe::Labels::plans`]).
    pub label: usize,
    /// The cluster member that emitted it.
    pub member: u32,
    /// The element.
    pub fact: Fact,
    /// What it was computed from (its provenance record).
    pub support: S,
}

/// What the members of one label's cluster held at the end of one run: for each member, each
/// fact with the supports of every observation of it, in program order.
///
/// It is a union over the run: an observation is never retracted, so the view at the end of
/// the run contains the view at every earlier point (monotone by construction).
#[derive(Clone, Debug)]
pub struct MemberView<S> {
    held: BTreeMap<u32, BTreeMap<Fact, Vec<S>>>,
}

impl<S> Default for MemberView<S> {
    fn default() -> Self {
        MemberView {
            held: BTreeMap::new(),
        }
    }
}

impl<S: Clone> MemberView<S> {
    /// The view of `label` from a run's observations (observations of other labels are
    /// ignored).
    pub fn of<'o>(label: usize, observations: impl IntoIterator<Item = &'o Observation<S>>) -> Self
    where
        S: 'o,
    {
        let mut view = MemberView::default();
        for o in observations {
            if o.label == label {
                view.add(o.member, o.fact.clone(), o.support.clone());
            }
        }
        view
    }

    /// Records that `member` emitted `fact` computed from `support`.
    pub fn add(&mut self, member: u32, fact: Fact, support: S) {
        self.held
            .entry(member)
            .or_default()
            .entry(fact)
            .or_default()
            .push(support);
    }

    /// The facts `member` held.
    pub fn facts_of(&self, member: u32) -> impl Iterator<Item = &Fact> {
        self.held.get(&member).into_iter().flat_map(|f| f.keys())
    }

    /// Whether `member` held `fact`.
    pub fn holds(&self, member: u32, fact: &Fact) -> bool {
        self.held.get(&member).is_some_and(|f| f.contains_key(fact))
    }

    /// The supports of every observation of `fact` at `member`, in program order (empty if it
    /// was not held).
    pub fn supports(&self, member: u32, fact: &Fact) -> &[S] {
        self.held
            .get(&member)
            .and_then(|f| f.get(fact))
            .map_or(&[], Vec::as_slice)
    }

    /// Every fact some member of `members` held.
    pub fn facts_among(&self, members: &BTreeSet<u32>) -> BTreeSet<Fact> {
        members
            .iter()
            .flat_map(|m| self.facts_of(*m).cloned())
            .collect()
    }
}

/// Which records of `A` are obligations: those held by members chosen by this side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ASide {
    /// Only members that never crash in the run.
    Correct,
    /// Any member, a crashed one counting what it emitted before its crash fired.
    Any,
}

/// Which members of `B`'s location must hold a match of each obligation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BSide {
    /// Every member that never crashes in the run.
    AllCorrect,
    /// At least `k` members that never crash (a write stored at `k` replicas that survive).
    AtLeast(usize),
    /// At least one member, crashed or not: a crashed member counts what it emitted before its
    /// crash fired (a message some sender sent).
    Some,
}

impl BSide {
    /// How many members must hold a match: `k` for `AtLeast(k)`, 1 for `Some`, `None` for
    /// `AllCorrect` (all the correct ones).
    pub fn at_least(self) -> Option<usize> {
        match self {
            BSide::AllCorrect => None,
            BSide::AtLeast(k) => Some(k),
            BSide::Some => Some(1),
        }
    }

    /// Whether the side counts what crashed members held (`Some`).
    pub fn counts_crashed(self) -> bool {
        self == BSide::Some
    }
}

/// The members of one side's location in one run: all of them, and those whose crash did not
/// fire (a crash point that never fired leaves its member correct).
#[derive(Clone, Debug, Default)]
pub struct Members {
    pub all: BTreeSet<u32>,
    pub correct: BTreeSet<u32>,
}

/// The obligation keys of a run: the facts of `a` held by the members `side` chooses.
pub fn obligations<S: Clone>(a: &MemberView<S>, side: ASide, members: &Members) -> BTreeSet<Fact> {
    a.facts_among(match side {
        ASide::Correct => &members.correct,
        ASide::Any => &members.all,
    })
}

/// A `(member of B, fact)` pair the rule counts and the run delivered, with what it was
/// computed from.
#[derive(Clone, Debug)]
pub struct HeldPair<S> {
    /// The member.
    pub member: u32,
    /// The fact.
    pub fact: Fact,
    /// The supports of every observation of the fact at the member, in program order. The
    /// pair holds in any run in which one of them is recomputed.
    pub supports: Vec<S>,
}

/// The rule, evaluated on one run.
#[derive(Clone, Debug)]
pub struct Evaluation<S> {
    /// The run's obligations ([`obligations`]).
    pub obligations: BTreeSet<Fact>,
    /// Every `(member of B, obligation)` pair the rule counts that the run delivered, by member
    /// then fact: at correct members, or at any member under `Some`.
    pub held: Vec<HeldPair<S>>,
    /// The pairs that break the rule, by member then fact: under `AllCorrect`, a correct member
    /// without the obligation; under `AtLeast(k)` and `Some`, every counted member without it,
    /// for an obligation fewer than `k` counted members held.
    pub missing: Vec<(u32, Fact)>,
}

impl<S> Evaluation<S> {
    /// Whether the rule holds on the run.
    pub fn holds(&self) -> bool {
        self.missing.is_empty()
    }
}

/// Evaluates the rule `A ⇒ B` on a run: `a` and `b` are what the members of each side's
/// location held over the run (keys, for a rule with key functions: the taps sit after them),
/// `sides` the rule's settings, and `in_a`, `in_b` the members of each side's location.
pub fn evaluate<S: Clone>(
    a: &MemberView<S>,
    b: &MemberView<S>,
    (a_side, b_side): (ASide, BSide),
    in_a: &Members,
    in_b: &Members,
) -> Evaluation<S> {
    let obligations = obligations(a, a_side, in_a);
    let (mut held, mut missing) = (vec![], vec![]);
    let counted = if b_side.counts_crashed() {
        &in_b.all
    } else {
        &in_b.correct
    };
    for &member in counted {
        for fact in &obligations {
            let supports = b.supports(member, fact);
            if supports.is_empty() {
                missing.push((member, fact.clone()));
            } else {
                held.push(HeldPair {
                    member,
                    fact: fact.clone(),
                    supports: supports.to_vec(),
                });
            }
        }
    }
    if let Some(k) = b_side.at_least() {
        // An obligation fewer than `k` counted members hold breaks the rule, even when every
        // member that lacks it crashed: it is missing at every member that is not a counted
        // holder (at every member, if all of them are).
        missing = vec![];
        for fact in &obligations {
            let holders: BTreeSet<u32> = held
                .iter()
                .filter(|p| &p.fact == fact)
                .map(|p| p.member)
                .collect();
            if holders.len() < k {
                let mut lacking: Vec<u32> = in_b.all.difference(&holders).copied().collect();
                if lacking.is_empty() {
                    lacking = holders.into_iter().collect();
                }
                missing.extend(lacking.into_iter().map(|m| (m, fact.clone())));
            }
        }
        missing.sort();
    }
    Evaluation {
        obligations,
        held,
        missing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(b: u8) -> Fact {
        Fact::from_bytes(vec![b])
    }

    /// Members `all`, of which `correct` never crashed.
    fn members(all: &[u32], correct: &[u32]) -> Members {
        Members {
            all: all.iter().copied().collect(),
            correct: correct.iter().copied().collect(),
        }
    }

    fn obs(member: u32, f: u8, support: &'static str) -> Observation<&'static str> {
        Observation {
            label: 0,
            member,
            fact: fact(f),
            support,
        }
    }

    const EC: (ASide, BSide) = (ASide::Correct, BSide::AllCorrect);

    /// The rule with `A = B =` the view of `o`.
    fn agreement(
        o: &[Observation<&'static str>],
        sides: (ASide, BSide),
        m: &Members,
    ) -> Evaluation<&'static str> {
        let view = MemberView::of(0, o);
        evaluate(&view, &view, sides, m, m)
    }

    #[test]
    fn holds_when_every_correct_member_has_every_fact() {
        let o = [
            obs(0, 1, "a"),
            obs(1, 1, "b"),
            obs(0, 2, "c"),
            obs(1, 2, "d"),
        ];
        let e = agreement(&o, EC, &members(&[0, 1], &[0, 1]));
        assert!(e.holds());
        assert_eq!(e.obligations, [fact(1), fact(2)].into());
        assert_eq!(e.held.len(), 4);
        assert_eq!(e.held[0].supports, vec!["a"]);
    }

    #[test]
    fn a_missing_fact_is_a_violation() {
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(0, 2, "c")];
        let e = agreement(&o, EC, &members(&[0, 1], &[0, 1]));
        assert_eq!(e.missing, vec![(1, fact(2))]);
    }

    #[test]
    fn a_crashed_member_owes_nothing_and_its_facts_oblige_nothing_under_correct() {
        // Member 2 crashed after deriving fact 2; no correct member has it.
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(2, 2, "c")];
        let e = agreement(&o, EC, &members(&[0, 1, 2], &[0, 1]));
        assert!(e.holds());
        assert_eq!(e.obligations, [fact(1)].into());
    }

    #[test]
    fn under_any_a_crashed_members_facts_oblige_the_correct_ones() {
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(2, 2, "c")];
        let e = agreement(
            &o,
            (ASide::Any, BSide::AllCorrect),
            &members(&[0, 1, 2], &[0, 1]),
        );
        assert_eq!(e.missing, vec![(0, fact(2)), (1, fact(2))]);
    }

    #[test]
    fn a_fact_only_some_correct_members_hold_is_required_of_all() {
        let o = [obs(0, 1, "a")];
        let e = agreement(&o, EC, &members(&[0, 1, 2], &[0, 1, 2]));
        assert_eq!(e.missing, vec![(1, fact(1)), (2, fact(1))]);
    }

    #[test]
    fn at_least_counts_correct_holders_and_some_counts_crashed_ones() {
        // A: member 0 of a client acked keys 1 and 2. B: replica 0 (crashed) and 1 stored 1;
        // replica 2 stored 2.
        let acks = [obs(0, 1, "x"), obs(0, 2, "y")];
        let stored = [obs(0, 1, "a"), obs(1, 1, "b"), obs(2, 2, "c")];
        let (a, b) = (MemberView::of(0, &acks), MemberView::of(0, &stored));
        let client = members(&[0], &[0]);
        let replicas = members(&[0, 1, 2], &[1, 2]);
        let e = evaluate(&a, &b, (ASide::Correct, BSide::AtLeast(1)), &client, &replicas);
        assert!(e.holds());
        let e = evaluate(&a, &b, (ASide::Correct, BSide::AtLeast(2)), &client, &replicas);
        // Two replicas are correct: key 1 at one of them, key 2 at the other.
        assert_eq!(e.missing, vec![(1, fact(2)), (2, fact(1))]);
        assert_eq!(e.held.len(), 2);
        let e = evaluate(&a, &b, (ASide::Correct, BSide::AllCorrect), &client, &replicas);
        assert_eq!(e.missing, vec![(1, fact(2)), (2, fact(1))]);
        // `Some` counts the crashed replica 0's key 1; `AtLeast(1)` does not.
        let one_correct = members(&[0, 1, 2], &[2]);
        let e = evaluate(&a, &b, (ASide::Correct, BSide::Some), &client, &one_correct);
        assert!(e.holds());
        let e = evaluate(&a, &b, (ASide::Correct, BSide::AtLeast(1)), &client, &one_correct);
        assert_eq!(e.missing, vec![(0, fact(1)), (1, fact(1)), (2, fact(1))]);
        // Short of `k` with every counted member holding it: still broken.
        let none_correct = members(&[0, 1, 2], &[]);
        let e = evaluate(&a, &b, (ASide::Any, BSide::AtLeast(1)), &client, &none_correct);
        assert!(!e.holds());
        let e = evaluate(&a, &b, (ASide::Any, BSide::AtLeast(4)), &client, &replicas);
        assert!(!e.holds());
    }

    #[test]
    fn no_obligation_is_vacuous() {
        let o: [Observation<&str>; 0] = [];
        assert!(
            agreement(&o, EC, &members(&[0, 1], &[0, 1]))
                .obligations
                .is_empty()
        );
    }

    #[test]
    fn the_view_is_a_union_and_keeps_every_support_in_order() {
        let o = [obs(0, 1, "first"), obs(0, 1, "second"), obs(1, 1, "b")];
        let view = MemberView::of(0, &o);
        assert_eq!(view.supports(0, &fact(1)), &["first", "second"]);
        // Every prefix of the run's observations gives a view contained in the final one.
        for n in 0..=o.len() {
            let prefix = MemberView::of(0, &o[..n]);
            for m in [0, 1] {
                for f in prefix.facts_of(m) {
                    assert!(view.holds(m, f));
                }
            }
        }
    }

    #[test]
    fn other_labels_are_ignored() {
        let mut o = vec![obs(0, 1, "a")];
        o.push(Observation {
            label: 1,
            member: 1,
            fact: fact(1),
            support: "other",
        });
        let view = MemberView::of(0, &o);
        assert!(!view.holds(1, &fact(1)));
    }
}
