p='hydrofrac/src/check/brute_tests.rs'
s=open(p).read()
a=s.index('/// PROBE (soundness review): a top-level `scan` over an order the schedule picks')
s=s[:a]+'''/// A top-level `scan` over an order the schedule picks (`assume_ordering` at top level is a
/// simulator hook): two sources each send one value to two members, and each member keeps the
/// first value it sees. The members can disagree with no fault at all. Before `fragile.rs` became
/// an allowlist, folds and scans were fragile only inside a tick, so this program counted as
/// fragile-free, ran on one schedule, and was certified; brute force broke it with the empty set.
#[cfg(stageleft_runtime)]
#[test]
fn a_top_level_scan_over_a_chosen_order_is_fragile() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = b.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let from_a = in_a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .weaken_ordering::<NoOrder>();
    let from_b = in_b
        .broadcast_closed(&members, TCP.fail_stop().name("b").bincode())
        .weaken_ordering::<NoOrder>();
    let firsts = from_a
        .merge_unordered(from_b)
        .assume_ordering::<TotalOrder>(nondet!(/** the order a member sees */))
        .scan(
            q!(|| false),
            q!(|seen, x| if *seen {
                None
            } else {
                *seen = true;
                Some(x)
            }),
        )
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member sees the same first value */
        ));
    let _first = firsts.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "scan"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(2);
        },
        MAX_FAULTS,
    );
    agree("top-level scan over a chosen order", &cross);
    let label = cross.results.label("a_top_level_scan_over_a_chosen_order_is_fragile");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the members disagree with no fault: {cross}"));
    assert!(cex.faults.is_empty(), "{cross}");
}
'''
open(p,'w').write(s)
