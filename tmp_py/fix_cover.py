p='hydrofrac/src/observe/tests.rs'; s=open(p).read()
o="""fn gossip_state_is_checked_at_the_stream_it_folds() {"""
assert s.count(o)==1
s=s.replace("""#[cfg(stageleft_runtime)]
#[test]
fn gossip_state_is_checked_at_the_stream_it_folds() {""","""/// The gossip state (a fold of `all_elements`) is not checked: agreement on the stream's set of
/// facts does not fix a fold's value (a count over duplicates differs), so a scalar is never
/// covered by its stream.
#[cfg(stageleft_runtime)]
#[test]
fn gossip_state_is_not_covered_by_the_stream_it_folds() {""")
o="""    let (all, all_elements) = find(&plans, "all_elements");
    assert!(all_elements.is_searched());
    let (_, state) = find(&plans, "gset_state");
    assert_eq!(state.covered_by, Some(all));
    assert!(!state.is_searched());"""
assert s.count(o)==1
s=s.replace(o,"""    let (all, all_elements) = find(&plans, "all_elements");
    assert!(all_elements.is_searched());
    let (_, state) = find(&plans, "gset_state");
    assert_eq!(state.collection.covered_by, Some(all));
    assert_eq!(state.covered_by, None);
    assert!(matches!(state.not_checked, Some(NotCheckedReason::NotAStream(_))), "{state:?}");
    assert!(!state.is_searched());""")
open(p,'w').write(s)

p='hydrofrac/src/observe/plan.rs'; s=open(p).read()
o="""/// The plan for every collection of the enumeration: `classify`, then a scalar whose input
/// stream is checked is covered by it.
pub fn plan(collections: Vec<EcCollection>) -> Vec<LabelPlan> {
    let mut plans: Vec<LabelPlan> = collections
        .into_iter()
        .map(|collection| LabelPlan {
            not_checked: classify(&collection),
            collection,
            covered_by: None,
        })
        .collect();
    for i in 0..plans.len() {
        if let Some(input) = plans[i].collection.covered_by
            && matches!(plans[i].not_checked, Some(NotCheckedReason::NotAStream(_)))
            && plans[input].not_checked.is_none()
        {
            plans[i].not_checked = None;
            plans[i].covered_by = Some(input);
        }
    }
    plans
}"""
assert s.count(o)==1
s=s.replace(o,"""/// The plan for every collection of the enumeration: `classify`. A scalar is never covered by
/// the stream it aggregates (`covered_by` stays `None`): the specification compares sets of
/// facts, and members that agree on a stream's set can still disagree on a fold of it (a count
/// or a sum over a duplicate, or any fold that is not idempotent). The IR does not record
/// whether a fold is idempotent, so such a scalar is reported not checked.
pub fn plan(collections: Vec<EcCollection>) -> Vec<LabelPlan> {
    collections
        .into_iter()
        .map(|collection| LabelPlan {
            not_checked: classify(&collection),
            collection,
            covered_by: None,
        })
        .collect()
}""")
o="""    /// For a labelled scalar checked at its input stream: that stream's plan index.
    pub covered_by: Option<usize>,"""
assert s.count(o)==1
s=s.replace(o,"""    /// For a labelled scalar checked at its input stream: that stream's plan index. Always
    /// `None` ([`plan`] no longer covers scalars).
    pub covered_by: Option<usize>,""")
open(p,'w').write(s)
print('ok')
