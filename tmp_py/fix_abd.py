p='hydro_std/src/ec_inference_demos/ec_abd_hydrofrac.rs'; s=open(p).read()
o=s[s.index("/// The register: the labelled `fold` into a `Singleton`, checked at its input stream. Returns"):s.index("/// The one searched label named")]
n='''/// The register: the labelled `fold` into a `Singleton`. It is not checked itself (members that
/// agree on a stream's facts can disagree on a fold of it), so this returns the result of the
/// stream it folds, which is.
#[cfg(stageleft_runtime)]
fn register(results: &hydrofrac::Results) -> &hydrofrac::check::LabelResult {
    let folds: Vec<_> = results
        .labels_named("abd_register_state")
        .filter(|l| l.plan.collection.covered_by.is_some())
        .collect();
    let [register] = folds[..] else {
        panic!("one register label folding a labelled stream\\n{results}");
    };
    let hydrofrac::Verdict::NotChecked(_) = register.verdict else {
        panic!("the register itself is not checked\\n{results}");
    };
    &results.labels[register.plan.collection.covered_by.expect("filtered")]
}

'''
s=s.replace(o,n)
open(p,'w').write(s)
print('ok')
