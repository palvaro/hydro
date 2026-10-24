p='hydrofrac/src/observe/tap.rs'; s=open(p).read()
o="""    });
    ir.extend(taps);
"""
assert s.count(o)==1
s=s.replace(o,"""    });
    // One label, one node: a label tapped at no node (or at two) would check the wrong facts.
    for (plan, &count) in plans.iter().zip(&tapped) {
        assert!(
            !plan.is_searched() || count == 1,
            "hydrofrac: label `{}` ({}) was tapped at {count} nodes, not one",
            plan.collection.display_name(),
            plan.collection.label
        );
    }
    ir.extend(taps);
""")
open(p,'w').write(s)
print('ok')
