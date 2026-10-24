p='hydrofrac/src/check/brute_tests.rs'; s=open(p).read()
old='''    let claim = |s: Stream<u32, Cluster<'_, ()>, Unbounded, NoOrder>| {
        s.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */))
    };
'''
new='''    fn claim<'a>(
        s: Stream<u32, Cluster<'a, ()>, Unbounded, NoOrder>,
    ) -> Stream<u32, Cluster<'a, (), EventualConsistency>, Unbounded, NoOrder> {
        s.assert_has_consistency_of::<Cluster<'a, (), EventualConsistency>>(manual_proof!(/** */))
    }
'''
assert s.count(old)==1
s=s.replace(old,new)
open(p,'w').write(s)
print('ok')
