p='results.rs'
s=open(p).read()
rep=[
('''    /// Whether [`Self::tolerates`] is a certificate. It is in a program whose fault sets each
    /// have one execution. In a program with schedule choices (docs/design/hydrofrac-lite.md
    /// §4.9) it says only that the search found no counterexample with fewer crashes.''',
'''    /// Whether [`Self::tolerates`] is a certificate. It is unless the search sampled schedules
    /// ([`crate::EcCheck::sample_schedules`]): then it says only that the sample showed no
    /// counterexample with fewer crashes.'''),
('''        /// The largest crash bound cleared before the budget ran out, if any (a certificate only
        /// in a program with no schedule choices).''',
'''        /// The largest crash bound cleared before the budget ran out, if any (a certificate
        /// unless schedules were sampled).'''),
('''    /// A label of a program with schedule choices (docs/design/hydrofrac-lite.md §4.9) whose
    /// search ran out of hypotheses without a counterexample: **not a certificate**. Lineage
    /// pruning is sound only where each fault set has one execution. Here a derivation seen in
    /// one schedule need not recur in another, so the fault sets the search skipped were not
    /// ruled out.''',
'''    /// A label whose search sampled schedules ([`crate::EcCheck::sample_schedules`]) and found
    /// no counterexample: **not a certificate**, as a schedule outside the sample may break it.'''),
('''    /// Whether the verdict is not a certificate of a label that needs checking: a
    /// counterexample, an exhausted budget, or an exhausted search in a program with schedule
    /// choices (what [`Results::assert_holds`] rejects).''',
'''    /// Whether the verdict is not a certificate of a label that needs checking: a
    /// counterexample, an exhausted budget, or a sampled search (what [`Results::assert_holds`]
    /// rejects).'''),
('''    /// Whether every label is certified (or needs no check): no counterexample, no exhausted
    /// budget, and no label whose search merely found nothing in a program with schedule
    /// choices ([`Verdict::Exhausted`]).''',
'''    /// Whether every label is certified (or needs no check): no counterexample, no exhausted
    /// budget, and no label whose sampled search merely found nothing ([`Verdict::Exhausted`]).'''),
('''    /// Panics with the report unless every label holds (see [`Self::holds`]). No label of a
    /// program with schedule choices can be certified, so for such a program use
    /// [`Self::assert_no_counterexample`].''',
'''    /// Panics with the report unless every label holds (see [`Self::holds`]). A sampled search
    /// certifies nothing, so with [`crate::EcCheck::sample_schedules`] use
    /// [`Self::assert_no_counterexample`].'''),
('''                "hydrofrac: the program has schedule choices, so the search of label `{}` is not a \\
                 certificate (`assert_no_counterexample` accepts it)\\n{self}",''',
'''                "hydrofrac: schedules were sampled, so the search of label `{}` is not a \\
                 certificate (`assert_no_counterexample` accepts it)\\n{self}",'''),
('''    /// Panics with the report if some label has a counterexample. Weaker than
    /// [`Self::assert_holds`]: it also accepts a label whose search found nothing in a program
    /// with schedule choices ([`Verdict::Exhausted`]) and an exhausted budget, neither of which
    /// is a certificate.''',
'''    /// Panics with the report if some label has a counterexample. Weaker than
    /// [`Self::assert_holds`]: it also accepts a sampled search that found nothing
    /// ([`Verdict::Exhausted`]) and an exhausted budget, neither of which is a certificate.'''),
('''                "{searched}; not a certificate (program has schedule choices: {})",''',
'''                "{searched} on sampled schedules; not a certificate (schedule choices: {})",'''),
]
for a,b in rep:
    assert a in s, a[:60]
    s=s.replace(a,b)
open(p,'w').write(s)

p='tests.rs'
s=open(p).read()
a=s.index('''#[cfg(stageleft_runtime)]
#[test]
fn a_program_with_a_schedule_choice_is_exhausted_not_certified()''')
b=s.index('''/// A message type with `Debug`''')
s=s[:a]+'''/// A schedule choice that no derivation needs to win: the batch is a choice but not a fragile
/// step, so every derivation is robust, and the label is certified (Claim B too). The plain
/// `initial` hop still breaks at one crash, and its tolerance of none is a certificate.
#[cfg(stageleft_runtime)]
#[test]
fn a_schedule_choice_with_robust_derivations_is_certified() {
    let results = batched_exchange(EcCheck::new());
    assert_eq!(
        results.choices.iter().map(|c| c.op.as_str()).collect::<Vec<_>>(),
        ["batch"],
        "{results}"
    );
    let label = results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "{results}"
    );
    assert!(label.stats.races.is_some_and(|r| r.claim_b), "{results}");
    let initial = results.label("initial");
    let cex = initial.verdict.counterexample().expect("the hop breaks");
    assert!(cex.tolerance_certified, "{results}");
    assert_eq!(cex.tolerates, Some(0), "{results}");
}

/// A sampled search certifies nothing: `assert_holds` rejects it, and says why;
/// `assert_no_counterexample` accepts it. Capped at zero crashes, the exchange has no
/// counterexample.
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "schedules were sampled, so the search of label")]
fn assert_holds_rejects_a_sampled_search() {
    let results = batched_exchange(EcCheck::new().max_crashes(0).sample_schedules(4));
    let label = results.label("delivered");
    let Verdict::Exhausted { tolerates, sites } = &label.verdict else {
        panic!("expected a sampled verdict:\\n{results}");
    };
    assert_eq!(*tolerates, Tolerance::UpTo(0), "{results}");
    assert_eq!(sites, &results.choices, "{results}");
    assert!(
        results
            .to_string()
            .contains("on sampled schedules; not a certificate (schedule choices: batch at tests.rs:"),
        "{results}"
    );
    results.assert_no_counterexample();
    results.assert_holds();
}

'''+s[b:]
open(p,'w').write(s)
