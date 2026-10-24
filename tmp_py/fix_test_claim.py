p='hydrofrac/src/check/brute_tests.rs'; s=open(p).read()
old=s[s.index("    let claim = |s: Stream<u32, Cluster<'_, ()>, Unbounded, TotalOrder>| {"):s.index("    let mut check = EcCheck::new().max_crashes(0);\n", s.index("    let claim = |s: Stream<u32"))]
new='''    let claim = |s: Stream<u32, Cluster<'_, ()>, Unbounded, NoOrder>| {
        s.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */))
    };
    let _a = claim(a.weaken_consistency().weaken_ordering()).sim_cluster_output();
    let _b = claim(b.weaken_consistency().weaken_ordering()).sim_cluster_output();
'''
s=s.replace(old,new)
old2='''    agree("one labelling line twice", &cross);
    let searched = cross.results.results().iter().filter(|l| l.plan.is_searched()).count();
    assert_eq!(searched, 2, "{cross}");
    assert!(
        breaks_under(&cross, |f| !f.is_empty()),
        "omitting `b` leaves a member without b's 1: {cross}"
    );'''
new2='''    agree("one labelling line twice", &cross);
    // The two broadcasts, and the two collections `claim` labels.
    let searched: Vec<_> = cross.results.results().iter().filter(|l| l.plan.is_searched()).collect();
    assert_eq!(searched.len(), 4, "{cross}");
    let claimed: Vec<_> = searched
        .iter()
        .filter(|l| l.plan.collection.label == searched[searched.len() - 1].plan.collection.label)
        .collect();
    assert_eq!(claimed.len(), 2, "{cross}");
    // `b`'s broadcast and `claim(b)` break under omissions; `a`'s and `claim(a)` hold.
    let breaking = searched.iter().filter(|l| l.verdict.counterexample().is_some()).count();
    assert_eq!(breaking, 2, "{cross}");
    assert!(
        claimed.iter().any(|l| l.verdict.counterexample().is_some()),
        "omitting `b` leaves a member without claim(b)'s 1: {cross}"
    );'''
assert s.count(old2)==1
s=s.replace(old2,new2)
open(p,'w').write(s)
print('ok')
