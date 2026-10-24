p='tests.rs'
s=open(p).read()
a=s.index('/// A sampled search certifies nothing: `assert_holds` rejects it, and says why;')
b=s.index('/// A message type with `Debug`')
s=s[:a]+'''/// A sampled search certifies nothing: `assert_holds` rejects it, and says why;
/// `assert_no_counterexample` accepts it. (A fragile-free program like the exchange is certified
/// even when sampled, since one schedule of a set shows all its facts; so the verdict here is set
/// by hand. The sampled CTP targets in `hydro_test` reach it through the engine.)
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "schedules were sampled, so the search of label")]
fn assert_holds_rejects_a_sampled_search() {
    let mut results = batched_exchange(EcCheck::new().max_crashes(0).sample_schedules(4));
    let sites = results.choices.clone();
    let index = results.label("delivered").index;
    results.labels[index].verdict = Verdict::Exhausted {
        tolerates: Tolerance::UpTo(0),
        sites,
    };
    assert!(
        results
            .to_string()
            .contains("on sampled schedules; not a certificate (schedule choices: batch at tests.rs:"),
        "{results}"
    );
    assert!(!results.holds(), "{results}");
    results.assert_no_counterexample();
    results.assert_holds();
}

'''+s[b:]
open(p,'w').write(s)
