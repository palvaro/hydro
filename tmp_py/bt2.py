p='brute_tests.rs'
s=open(p).read()
# merge extra assertions into the brute_force_agrees tests
s=s.replace('''    agree("plain broadcast (fail_stop)", &cross);
    // The enumeration itself finds the mid fan-out crash of the source.
    let b = &cross.brute.labels[0];
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
}''','''    agree("plain broadcast (fail_stop)", &cross);
    // The enumeration itself finds the mid fan-out crash of the source.
    let b = &cross.brute.labels[0];
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
    let cex = cross.results.label("delivered").verdict.counterexample().expect("a mid fan-out crash");
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
}''')
s=s.replace('''    assert!(
        b.breaking[0].iter().all(|f| matches!(f, Fault::Omit(_))),
        "{cross}"
    );
}''','''    assert!(
        b.breaking[0].iter().all(|f| matches!(f, Fault::Omit(_))),
        "{cross}"
    );
    let cex = cross.results.label("delivered").verdict.counterexample().expect("an omission");
    assert_eq!(cex.crashes(), 0, "{cross}");
}''')
s=s.replace('''    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(2), "{cross}");
}''','''    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(2), "{cross}");
    let cex = label.verdict.counterexample().expect("two crashes break it");
    assert_eq!(cex.crashes(), 2, "{cross}");
}''')
a=s.index('// The races mode (docs/design/hydrofrac-races.md §4.4), cross-checked on the same programs.')
b=s.index('/// An echo broadcast whose members batch what they receive')
s=s[:a]+'''/// The verdict on `label`, and its races notes.
#[cfg(stageleft_runtime)]
fn races_label<'c>(cross: &'c CrossCheck, label: &str) -> (&'c Verdict, super::RaceNotes) {
    let l = cross.results.label(label);
    let notes = l
        .stats
        .races
        .unwrap_or_else(|| panic!("no races notes: {cross}"));
    (&l.verdict, notes)
}

'''+s[b:]
s=s.replace('races_agree(','agree(')
open(p,'w').write(s)
