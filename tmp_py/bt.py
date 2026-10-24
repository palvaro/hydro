p='brute_tests.rs'
s=open(p).read()
# S1
a=s.index('/// S1 with the search capped at zero crashes: a false certificate.')
b=s.index('// The races mode (docs/design/hydrofrac-races.md §4.4), cross-checked on the same programs.')
s=s[:a]+'''/// S1 with the search capped at zero crashes. Lite's union clauses certified it falsely; the
/// per-pair clauses keep (m1, f)'s derivation from the probe, and `omit(gossip m0→m1)` alone
/// breaks the label, as the enumeration finds.
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_certificate() {
    let cross = total_loss_masks_a_partial_loss(Some(0));
    eprintln!("=== S1 (max_crashes 0)\\n{cross}\\n{}", cross.results);
    assert!(
        cross.results.choices.is_empty(),
        "not choice-free: {:?}",
        cross.results.choices
    );
    assert_breaks_by_the_gossip_omission(&cross);
}

/// S1 uncapped: the counterexample is the zero-crash omission (lite claimed to tolerate every
/// zero-crash fault set).
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_tolerance() {
    let cross = total_loss_masks_a_partial_loss(None);
    eprintln!("=== S1 (uncapped)\\n{cross}\\n{}", cross.results);
    assert_breaks_by_the_gossip_omission(&cross);
}

/// The search agrees with the enumeration on S1, and breaks `known` with no crash.
#[cfg(stageleft_runtime)]
fn assert_breaks_by_the_gossip_omission(cross: &CrossCheck) {
    cross.assert_agrees();
    let label = cross.results.label("known");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("omissions alone break it: {cross}"));
    assert_eq!(cex.crashes(), 0, "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(0), "{cross}");
}

'''+s[b:]
# delete races duplicate of S1
a=s.index('/// S1: where lite certifies zero crashes (or claims to tolerate them) and `omit(gossip m0→m1)`')
b=s.index('#[test]\n',a)
b=s.index('\n}\n',b)+3
s=s[:a]+s[b:].lstrip('\n')
# S2
a=s.index('#[cfg(stageleft_runtime)]\n#[test]\nfn suspected_positional_crash_order_tolerance()')
b=s.index('/// The races mode on the S2 relay agrees with the enumeration')
c=s.index('\n}\n',b)+3
s=s[:a]+'''#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_tolerance() {
    for max_faults in [2, 3, 4] {
        let cross = positional_crash_order_relay(EcCheck::new(), max_faults);
        eprintln!(
            "=== S2 relay (uncapped, {max_faults} faults)\\n{cross}\\nsearch:\\n{}",
            cross.results
        );
        assert!(
            cross.results.choices.is_empty(),
            "not choice-free: {:?}",
            cross.results.choices
        );
        assert_breaks_at_the_relay(&cross);
    }
}

/// S2 with the search capped at one crash: lite certified "holds with up to 1 crash"; the search
/// finds `{crash(n after →m0)}`. See [`suspected_positional_crash_order_tolerance`].
#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_certificate() {
    let cross = positional_crash_order_relay_in(EcCheck::new(), 3, Some(1));
    eprintln!(
        "=== S2 relay (max_crashes 1, 3 faults)\\n{cross}\\nsearch:\\n{}",
        cross.results
    );
    assert_breaks_at_the_relay(&cross);
}

/// The search agrees with the enumeration on the S2 relay, and breaks `known` with the one crash
/// `{crash(n after →m0)}`, its tolerance of none certified.
#[cfg(stageleft_runtime)]
fn assert_breaks_at_the_relay(cross: &CrossCheck) {
    cross.assert_agrees();
    let label = cross.results.label("known");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert_eq!(cex.tolerates, Some(0), "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
    assert!(
        cross
            .results
            .report_names()
            .fault_set(&b.breaking[0])
            .starts_with(&format!("{{{RELAY_BREAK}")),
        "{cross}"
    );
}
'''+s[c:]
s=s.replace('''/// Suspected soundness bug S2, uncapped: the search reports a counterexample with two crashes and
/// claims (as certified) that the label tolerates one, while one crash breaks it.''','''/// Suspected soundness bug S2 (demonstrated in lite, whose positional crash rule is deleted):
/// lite reported a counterexample with two crashes and claimed (as certified) that the label
/// tolerates one, while one crash breaks it. The order-free crash rule finds the one crash.''')
s=s.replace('''///   (`Dependency::killed_by`, read in the clause's own run) says `H` kills nothing there: `m1`'s
///   old derivation "survives" `H`, and `H` is never proposed.''','''///   (lite's `Dependency::killed_by`, read in the clause's own run) said `H` kills nothing there:
///   `m1`'s old derivation "survived" `H`, and `H` was never proposed. The order-free rule says
///   `crash(n after →m0)` kills `→m1` (their order is not proved), so `H` is proposed.''')
s=s.replace('''/// - Every other one-crash set either holds (the echo covers `{omit(direct), crash(n after
///   →m1)}`; the redundant paths cover `r1`, `r2` and the sources) or hits no clause. So bound 1
///   is declared exhausted (the completion adds only `crash(n)`, which is vacuous), and the search
///   breaks at 2 with `{crash(s1), crash(s2)}`.''','''/// - Lite: every other one-crash set either holds (the echo covers `{omit(direct), crash(n after
///   →m1)}`; the redundant paths cover `r1`, `r2` and the sources) or hits no clause. So bound 1
///   was declared exhausted, and lite broke at 2 with `{crash(s1), crash(s2)}`.''')
open(p,'w').write(s)
