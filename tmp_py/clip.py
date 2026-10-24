import re
p='hydrofrac/src/check/brute_force.rs'
s=open(p).read()
a='''                let failed = match runs.iter().find(|(set, _)| *set == cex.faults) {
                    Some((_, run)) => run.labels[&label.index].failure.is_some(),
                    None => {
                        let run = self.execute(&cex.faults, &targets, false, |_, _| {});
                        run.labels[&label.index].failure.is_some()
                    }
                };'''
b='''                let failed = if let Some((_, run)) = runs.iter().find(|(set, _)| *set == cex.faults) {
                    run.labels[&label.index].failure.is_some()
                } else {
                    let run = self.execute(&cex.faults, &targets, false, |_, _| {});
                    run.labels[&label.index].failure.is_some()
                };'''
assert a in s; s=s.replace(a,b); open(p,'w').write(s)
p='hydrofrac/src/lineage/tests.rs'
s=open(p).read()
s=s.replace('assert!(clause.hit_by(&Fault::crash_after("A", Some(m1.clone())), &()));','assert!(clause.hit_by(&Fault::crash_after("A", Some(m1)), &()));')
s=s.replace('assert!(!clause.hit_by(&Fault::crash_after("A", Some(m2.clone())), &()));','assert!(!clause.hit_by(&Fault::crash_after("A", Some(m2)), &()));')
open(p,'w').write(s)
p='hydrofrac/src/search/tests.rs'
s=open(p).read()
s=s.replace('''    let capped = CrashBounds {
        cap: Some(5),
        ..unbounded.clone()
    };''','''    let capped = CrashBounds {
        cap: Some(5),
        ..unbounded
    };''')
open(p,'w').write(s)
