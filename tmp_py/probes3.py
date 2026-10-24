p='hydrofrac/src/check/rev2_probes.rs'; s=open(p).read()
s=s.replace("if n == 3 { x + 1000 } else { x }","if n >= 2 && n % 2 == 0 { x + 1000 } else { x }")
s=s.replace('''/// C2: `optional_first_tick''','''/// C1b: a tick constant beside a lossy batch, chained at top level with a fail_stop stream.
#[cfg(stageleft_runtime)]
#[test]
fn c1b_tick_constant_fed() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    input
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let y = in_y.broadcast_closed(&members, TCP.fail_stop().name("y").bincode());
    let nine = tick
        .singleton(q!(9u32))
        .into_stream()
        .all_ticks()
        .weaken_ordering::<NoOrder>()
        .merge_unordered(y.weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = nine.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send_y.send(5);
        },
        MAX_FAULTS,
    );
    report("C1b", &check, &cross);
}

/// C2: `optional_first_tick''',1)
open(p,'w').write(s)
print('ok')
