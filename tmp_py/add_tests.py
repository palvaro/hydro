p='hydrofrac/src/check/brute_tests.rs'; s=open(p).read()
s = s.rstrip() + '''

/// Whether some label of `cross` breaks under a fault set satisfying `faults`.
#[cfg(stageleft_runtime)]
fn breaks_under(cross: &CrossCheck, faults: impl Fn(&FaultSet<crate::fault::MessageId>) -> bool) -> bool {
    cross
        .results
        .results()
        .iter()
        .filter_map(|l| l.verdict.counterexample())
        .any(|c| faults(&c.faults))
}

/// A constant inside a tick (`tick.singleton`) is emitted in every tick that runs, and only if
/// one runs: here the tick runs only on a lossy delivery, so omitting it leaves the member
/// without the constant. The label also merges a `fail_stop` stream at top level, so it is fed
/// by the network and checked. Before the fix the constant had no lineage (no fault could kill
/// it) and the search certified the label.
#[cfg(stageleft_runtime)]
#[test]
fn tick_constants_are_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_x, in_x) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    in_x.broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .batch(&tick, nondet!(/** the tick runs only when `x` arrives */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** a no-op */)));
    let y = in_y.broadcast_closed(&members, TCP.fail_stop().name("y").bincode());
    let nine = tick
        .singleton(q!(9u32))
        .into_stream()
        .all_ticks()
        .weaken_ordering::<NoOrder>()
        .merge_unordered(y.weaken_consistency().weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member emits 9 */
        ));
    let _out = nine.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "tick_constant"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send_y.send(5);
        },
        MAX_FAULTS,
    );
    agree("tick constants", &cross);
    assert!(
        breaks_under(&cross, |f| !f.is_empty()),
        "omitting `x` leaves a member without 9: {cross}"
    );
}

/// The dedup idiom's state edge is robust only if nothing but the negation and the cycle read
/// it. Here `seen` also leaves the tick (`all_ticks`): a record carried to the next tick is
/// emitted only if a next tick runs, so the members disagree with no fault.
#[cfg(stageleft_runtime)]
#[test]
fn a_dedup_state_read_elsewhere_is_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let arrived = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .weaken_ordering::<NoOrder>();
    let (handle, prev) = tick.cycle::<Stream<u32, _, Bounded, NoOrder>, _>();
    let seen = prev.chain(tick.singleton(q!(0u32)).into_stream().weaken_ordering::<NoOrder>());
    let fresh = arrived.filter_not_in(seen.clone());
    handle.complete_next_tick(seen.clone().chain(fresh.clone()));
    fresh
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** a no-op */)));
    let leaked = seen
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member's history is the same */
        ));
    let _out = leaked.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "defer_tick"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    agree("dedup state read elsewhere", &cross);
    assert!(
        breaks_under(&cross, |f| f.is_empty()),
        "the members disagree with no fault: {cross}"
    );
}

/// Reviewer #1's program: a batch in a tick that also snapshots a singleton (`s`) that has a
/// value only once its producer tick ran. Omitting `x` to a member leaves its snapshot without
/// a value, so the simulator never runs its consumer tick, and that member never emits 7. Before
/// the fix the batch was robust, the search certified `facts`, and the simulator aborted the
/// process on the stuck tick.
#[cfg(stageleft_runtime)]
#[test]
fn review_batch_beside_an_unready_snapshot() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_x, in_x) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let y = in_y
        .broadcast_closed(&members, TCP.fail_stop().name("y").bincode())
        .weaken_consistency();
    let consumer = members.tick();
    let facts = y
        .batch(&consumer, nondet!(/** */))
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = facts.sim_cluster_output();
    let x = in_x
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .weaken_consistency();
    let producer = members.tick();
    let s = x.batch(&producer, nondet!(/** */)).count().latest();
    s.snapshot(&consumer, nondet!(/** */))
        .into_stream()
        .all_ticks()
        .for_each(q!(|_| {}));
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "batch_beside_snapshot"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send_y.send(7);
        },
        MAX_FAULTS,
    );
    cross.assert_agrees();
    let facts = cross
        .results
        .results()
        .iter()
        .find(|l| l.name() == "facts")
        .expect("the `facts` label");
    assert!(
        facts.verdict.counterexample().is_some(),
        "omitting `x` leaves a member without 7: {cross}"
    );
}

/// Two collections labelled by one line (a helper called twice) are two labels. Merged, the
/// `fail_stop` collection's 1 masked the lossy one's missing 1, and both engines certified the
/// union.
#[cfg(stageleft_runtime)]
#[test]
fn one_labelling_line_twice_is_two_labels() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let a = in_a.broadcast_closed(&members, TCP.fail_stop().name("a").bincode());
    let b = in_b.broadcast_closed(&members, TCP.lossy_delayed_forever().name("b").bincode());
    let claim = |s: Stream<u32, Cluster<'_, ()>, Unbounded, TotalOrder>| {
        s.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */))
    };
    let _a = claim(a).sim_cluster_output();
    let _b = claim(b).sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(1);
        },
        MAX_FAULTS,
    );
    agree("one labelling line twice", &cross);
    let searched = cross.results.results().iter().filter(|l| l.plan.is_searched()).count();
    assert_eq!(searched, 2, "{cross}");
    assert!(
        breaks_under(&cross, |f| !f.is_empty()),
        "omitting `b` leaves a member without b's 1: {cross}"
    );
}

/// A closure that names outside state is fragile, and a choice (its one schedule would not
/// stand for the others).
#[cfg(stageleft_runtime)]
#[test]
fn impure_closures_are_fragile_choices() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .map(q!(|x: u32| x + std::process::id() * 0))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = out.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let _sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "impure_closure"), "{sites:?}");
    let impure = |text: &str| crate::provenance::fragile::names_impure(text.parse().unwrap());
    assert!(impure("|x| { static N: u32 = 0; x + N }"));
    assert!(impure("|x| x + COUNT.fetch_add(1, Ordering::Relaxed) as AtomicU32"));
    assert!(impure("|x| (x, std::time::SystemTime::now())"));
    assert!(impure("|x| x.borrow_mut().replace(RefCell::new(0))"));
    assert!(!impure("|x: &'static str| x.len()"));
    assert!(!impure("|(k, v)| (k, v + 1)"));
}

/// The simulator's own crash injection and dynamic membership are refused: hydrofrac would
/// count the members the simulator crashed as correct.
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "does not support the simulator's own crash injection")]
fn the_simulators_own_crashes_are_refused() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = out.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let _sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .with_crashable_cluster(&members, 1)
        .compiled();
}

/// With `BOLERO_RANDOM_TEST_TIME_MS` set, bolero ends an exhaustive exploration at the time limit
/// without saying so: the check refuses to run (in a child process, so the variable does not
/// leak into other tests).
#[cfg(stageleft_runtime)]
#[test]
fn a_time_bounded_exhaustive_run_is_refused() {
    const CHILD: &str = "HYDROFRAC_TIME_BOUND_CHILD";
    if std::env::var(CHILD).is_ok() {
        let cross = plain_broadcast(true);
        eprintln!("{cross}");
        return;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "check::brute_tests::a_time_bounded_exhaustive_run_is_refused",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env("BOLERO_RANDOM_TEST_TIME_MS", "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "the child ran: {stdout}\\n{stderr}");
    assert!(
        stderr.contains("would silently cut the exhaustive exploration short")
            || stdout.contains("would silently cut the exhaustive exploration short"),
        "{stdout}\\n{stderr}"
    );
}
'''
open(p,'w').write(s)
print('ok')
