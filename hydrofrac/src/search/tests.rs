use super::*;

/// The crash bounds: the cap, never every node, and the checked cluster's cap of `|members| − 2`.
#[test]
fn crash_bounds_cap_and_never_crash_every_node() {
    let unbounded = CrashBounds {
        cap: None,
        nodes: 4,
        checked: None,
    };
    assert_eq!(unbounded.max_crashes(), Some(3));
    assert_eq!(unbounded.capped_at(1).max_crashes(), Some(1));
    let capped = CrashBounds {
        cap: Some(5),
        ..unbounded
    };
    assert_eq!(capped.max_crashes(), Some(3));
    assert_eq!(CrashBounds::default().max_crashes(), None);
    let checked = CheckedCluster::new(["m0", "m1", "m2"].map(str::to_owned));
    assert_eq!(checked.cap, 1);
}

/// The tie-break prefers earlier candidates but never trades cardinality: the heaviest set of
/// `k` weighs less than the lightest set of `k + 1`.
#[test]
fn the_tie_break_never_trades_cardinality() {
    for n in [1, 2, 5, 40] {
        let w: Vec<f64> = weights(n).collect();
        for k in 0..n {
            let heaviest: f64 = w[n - k..].iter().sum();
            let lightest: f64 = w[..(k + 1).min(n)].iter().sum();
            assert!(k + 1 > n || heaviest < lightest, "n {n}, k {k}");
        }
        assert!(w.windows(2).all(|p| p[0] < p[1]));
    }
}

#[test]
fn faults_render_as_in_the_report() {
    let omit: Fault<&str> = Fault::Omit("m");
    assert_eq!(omit.to_string(), "omit(m)");
    assert_eq!(
        Fault::<&str>::crash_after("n", None).to_string(),
        "crash(n)"
    );
    assert_eq!(
        Fault::crash_after("n", Some("m")).to_string(),
        "crash(n after m)"
    );
    let set: FaultSet<&str> = [omit, Fault::crash_after("n", None)].into();
    assert_eq!(crash_count(&set), 1);
}
