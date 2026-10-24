p='hydrofrac/src/provenance/fragile.rs'; s=open(p).read()
o="""fn impure(node: &HydroNode) -> bool {"""
assert s.count(o)==1
s=s.replace(o,"""pub(crate) fn impure(node: &HydroNode) -> bool {""")
open(p,'w').write(s)
p='hydrofrac/src/provenance/choices.rs'; s=open(p).read()
o="""        _ if observed => "observe_for_mut","""
assert s.count(o)==1
s=s.replace(o,"""        _ if observed => "observe_for_mut",
        // A closure that may give different results for one input: the one schedule of a
        // choice-free program would not stand for the others (`super::fragile::names_impure`).
        _ if super::fragile::impure(node) => "impure_closure",""")
open(p,'w').write(s)
print('ok')
