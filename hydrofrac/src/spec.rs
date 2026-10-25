//! The generated `EventualConsistency` specification, evaluated on the host
//! (docs/design/hydrofrac-lite.md §4.7).
//!
//! For one labelled collection on a cluster, let `FACTS` be the facts correct members held at
//! the end of the failure-free run (the **fact domain**, learned from the probe,
//! [`learn_domain`]). In a run, `POST` is: every correct member holds every fact of `FACTS`
//! that some correct member holds. A fact of `FACTS` that no correct member holds (its
//! producers crashed before producing it) is required of nobody, as in Molly. There is no
//! `PRE`: if the probe holds no facts, the label is vacuous.
//!
//! This is the research branch's observer view (`spec_views::eventual_consistency`), computed
//! by the host from the taps' outputs ([`crate::observe`]) instead of by a Hydro dataflow on an
//! observer process. The view only grows during a run: a member's facts are the union of what
//! its tap emitted ([`MemberView`]), and a tap emits each element of a stream once it exists.
//! Why that is as sound as the observer, including crashes mid-run, heal and the correct-member
//! set, is argued in the design doc.
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

/// The fact domain of a label: the facts its correct members held in the failure-free run.
/// Empty means the label is vacuous (nothing to agree on).
pub fn learn_domain<S: Clone>(view: &MemberView<S>, correct: &BTreeSet<u32>) -> BTreeSet<Fact> {
    view.facts_among(correct)
}

/// A `(correct member, fact)` pair the specification requires and the run delivered, with
/// what it was computed from.
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

/// The specification of one label, evaluated on one run.
#[derive(Clone, Debug)]
pub struct Evaluation<S> {
    /// The domain facts some correct member held: what every correct member owes.
    pub required: BTreeSet<Fact>,
    /// Every required `(member, fact)` pair the run delivered, by member then fact. When
    /// [`Self::holds`], this is the derivation of `POST`: a run that recomputes one support
    /// of every pair (with the same correct members) satisfies `POST` again.
    pub held: Vec<HeldPair<S>>,
    /// Every required `(member, fact)` pair the run did not deliver, by member then fact.
    pub missing: Vec<(u32, Fact)>,
    /// Facts correct members held that are not in the domain (a fault made some member derive
    /// something the failure-free run never did). Not required of anyone; for reports.
    pub outside_domain: BTreeSet<Fact>,
}

impl<S> Evaluation<S> {
    /// Whether `POST` holds: every correct member holds every required fact.
    pub fn holds(&self) -> bool {
        self.missing.is_empty()
    }
}

/// Evaluates the label's `POST` on a run: `view` is what the run's members held, `domain` the
/// label's fact domain, and `correct` the members of the label's cluster that are not crashed
/// at the end of the run (a crash point that never fired leaves its member correct).
pub fn evaluate<S: Clone>(
    view: &MemberView<S>,
    domain: &BTreeSet<Fact>,
    correct: &BTreeSet<u32>,
) -> Evaluation<S> {
    let present = view.facts_among(correct);
    let required: BTreeSet<Fact> = present.intersection(domain).cloned().collect();
    let outside_domain = present.difference(domain).cloned().collect();
    let mut held = vec![];
    let mut missing = vec![];
    for &member in correct {
        for fact in &required {
            let supports = view.supports(member, fact);
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
    Evaluation {
        required,
        held,
        missing,
        outside_domain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(b: u8) -> Fact {
        Fact::from_bytes(vec![b])
    }

    fn members(m: &[u32]) -> BTreeSet<u32> {
        m.iter().copied().collect()
    }

    fn obs(member: u32, f: u8, support: &'static str) -> Observation<&'static str> {
        Observation {
            label: 0,
            member,
            fact: fact(f),
            support,
        }
    }

    #[test]
    fn holds_when_every_correct_member_has_every_fact() {
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(0, 2, "c"), obs(1, 2, "d")];
        let view = MemberView::of(0, &o);
        let domain = learn_domain(&view, &members(&[0, 1]));
        assert_eq!(domain, [fact(1), fact(2)].into());
        let e = evaluate(&view, &domain, &members(&[0, 1]));
        assert!(e.holds());
        assert_eq!(e.held.len(), 4);
        assert_eq!(e.held[0].supports, vec!["a"]);
    }

    #[test]
    fn a_missing_fact_is_a_violation() {
        let domain: BTreeSet<Fact> = [fact(1), fact(2)].into();
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(0, 2, "c")];
        let e = evaluate(&MemberView::of(0, &o), &domain, &members(&[0, 1]));
        assert!(!e.holds());
        assert_eq!(e.missing, vec![(1, fact(2))]);
    }

    #[test]
    fn a_crashed_member_owes_nothing_and_its_facts_require_nothing() {
        let domain: BTreeSet<Fact> = [fact(1), fact(2)].into();
        // Member 2 crashed after deriving fact 2; no correct member has it.
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(2, 2, "c")];
        let e = evaluate(&MemberView::of(0, &o), &domain, &members(&[0, 1]));
        assert!(e.holds());
        assert_eq!(e.required, [fact(1)].into());
        // Member 2 lacks fact 1 too, but it is not correct.
        assert!(e.missing.is_empty());
    }

    #[test]
    fn a_fact_only_some_correct_members_hold_is_required_of_all() {
        let domain: BTreeSet<Fact> = [fact(1)].into();
        let o = [obs(0, 1, "a")];
        let e = evaluate(&MemberView::of(0, &o), &domain, &members(&[0, 1, 2]));
        assert_eq!(e.missing, vec![(1, fact(1)), (2, fact(1))]);
    }

    #[test]
    fn facts_outside_the_domain_are_not_required() {
        let domain: BTreeSet<Fact> = [fact(1)].into();
        let o = [obs(0, 1, "a"), obs(1, 1, "b"), obs(0, 9, "x")];
        let e = evaluate(&MemberView::of(0, &o), &domain, &members(&[0, 1]));
        assert!(e.holds());
        assert_eq!(e.outside_domain, [fact(9)].into());
    }

    #[test]
    fn an_empty_probe_is_vacuous() {
        let o: [Observation<&str>; 0] = [];
        assert!(learn_domain(&MemberView::of(0, &o), &members(&[0, 1])).is_empty());
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
