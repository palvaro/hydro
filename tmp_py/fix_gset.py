p='hydro_test/src/cluster/ec_demos_hydrofrac.rs'; s=open(p).read()
def rep(o,n,c=1):
    global s
    assert s.count(o)==c,(o[:80],s.count(o))
    s=s.replace(o,n)
rep("""/// The verdict a label is checked by: its own, or for a covered scalar the verdict of the
/// labelled stream it aggregates.
#[cfg(stageleft_runtime)]
fn effective<'r>(results: &'r Results, label: &'r LabelResult) -> &'r Verdict {
    match label.verdict {
        Verdict::CoveredBy(index) => &results.results()[index].verdict,
        _ => &label.verdict,
    }
}""","""/// The verdict a label is judged by here: its own, or for a scalar folding a labelled stream
/// (which hydrofrac does not check: agreeing streams can still fold to different values) the
/// verdict of that stream. For an idempotent fold such as a G-Set's union, agreeing streams do
/// fold to the same value, by reading.
#[cfg(stageleft_runtime)]
fn effective<'r>(results: &'r Results, label: &'r LabelResult) -> &'r Verdict {
    match (&label.verdict, label.plan.collection.covered_by) {
        (Verdict::NotChecked(_), Some(index)) => &results.results()[index].verdict,
        _ => &label.verdict,
    }
}""")
rep(""".filter(|l| matches!(l.verdict, Verdict::CoveredBy(_)))""",""".filter(|l| l.plan.collection.covered_by.is_some())""",2)
s=s.replace(""".filter(|l| matches!(l.verdict, Verdict::CoveredBy(_)))""",""".filter(|l| l.plan.collection.covered_by.is_some())""")
rep("""/// State-based G-Set gossip, closed and live: the G-Set, checked at the stream it folds, has
/// no counterexample under any admissible crash set.""","""/// State-based G-Set gossip, closed and live: the stream the G-Set folds (the G-Set itself is
/// not checked, see [`effective`]) has no counterexample under any admissible crash set.""")
open(p,'w').write(s)
print('ok')
