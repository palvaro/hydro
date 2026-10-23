//! Tests of the schedule-sensitivity diagnostic, and a demonstration that positional crash
//! points make pruning schedule-dependent even in a cone with no non-deterministic operator.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::cell::RefCell;
#[cfg(stageleft_runtime)]
use std::collections::BTreeSet;
#[cfg(stageleft_runtime)]
use std::panic::AssertUnwindSafe;
#[cfg(stageleft_runtime)]
use std::rc::Rc;

#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::{CompiledSim, quiesce};

#[cfg(stageleft_runtime)]
use crate::check::{EcCheck, Verdict};
#[cfg(stageleft_runtime)]
use crate::fault::{Event, Fate, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
#[cfg(stageleft_runtime)]
use crate::provenance::sensitivity::label_cones;
#[cfg(stageleft_runtime)]
use crate::search::{CrashAt, Fault, FaultSet};

/// Runs `faults` directly (no search) over every schedule, with the check's driving: an empty
/// body, quiesce, heal, quiesce.
#[cfg(stageleft_runtime)]
fn run_directly(sim: &CompiledSim, faults: FaultSet<MessageId>) -> Vec<InstanceRecord> {
    let hook = Rc::new(FaultHook::new(&FaultPlan { faults }, sim));
    sim.set_network_hook(hook.clone());
    let records: RefCell<Vec<InstanceRecord>> = RefCell::new(vec![]);
    let healer = AssertUnwindSafe(hook.clone());
    sim.exhaustive_then(
        async || {
            quiesce().await;
            healer.heal();
            quiesce().await;
        },
        || records.borrow_mut().extend(hook.drain_instances()),
    );
    sim.clear_network_hook();
    records.into_inner()
}

/// The members of the reply channel's deliveries in one schedule.
#[cfg(stageleft_runtime)]
fn replied(record: &InstanceRecord, reply: usize) -> BTreeSet<u32> {
    record
        .log
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::Sent {
                msg,
                fate: Fate::Delivered,
                ..
            } if msg.channel == reply => msg.to.member,
            _ => None,
        })
        .collect()
}

/// Two members each ask a server once; the server answers every request with the same fact.
/// The label is the members' answers. Nothing in the cone is non-deterministic: the server maps
/// its (unordered) requests one to one. But the server's two replies go out in the order the
/// requests arrive, which the schedule decides.
#[cfg(stageleft_runtime)]
fn request_reply() -> (FlowBuilder<'static>, Cluster<'static, ()>) {
    let mut flow = FlowBuilder::new();
    let server = flow.process::<()>();
    let members = flow.cluster::<()>();
    let replies = members
        .source_iter(q!([()]))
        .send(&server, TCP.fail_stop().name("request").bincode())
        .entries()
        .map(q!(|(member, ())| (member, 7u32)))
        .demux(&members, TCP.fail_stop().name("reply").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: the server answers every member */
        ));
    let _out = replies.sim_cluster_output();
    (flow, members)
}

#[cfg(stageleft_runtime)]
#[test]
fn the_request_reply_cone_has_no_nondeterministic_operator() {
    let (flow, _members) = request_reply();
    let built = flow.finalize();
    let cones = label_cones(built.ir());
    assert!(!cones.is_empty());
    assert!(cones.iter().all(|c| !c.schedule_sensitive()), "{cones:?}");
}

/// Positional crashes in a cone with no non-deterministic operator. The simulator runs
/// top-level (async) dataflow deterministically and branches only at ticks and observations, the
/// `nondet!` sites. So a program whose label cone has no site has exactly one schedule per fault
/// set: the order of the server's replies cannot differ between schedules, the positional kill
/// relation means the same thing in every clause, and the search finds the crash that splits the
/// members (`crash(server after reply→member 0)`) at its first hypothesis.
///
/// (What can still vary is the order across *runs*: a different fault set can make a node send
/// in a different order, so a crash point named in one run cuts elsewhere in another. Nothing in
/// this program exhibits it.)
#[cfg(stageleft_runtime)]
#[test]
fn a_site_free_program_has_one_schedule_and_its_positional_crash_is_found() {
    let (flow, members) = request_reply();
    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let results = check.run(&sim, async || {});
    println!("{results}");

    // Ground truth, without the search.
    let reply = sim
        .channels()
        .iter()
        .find(|c| c.name.as_deref() == Some("reply"))
        .expect("the reply channel")
        .id;
    let server = sim
        .locations()
        .iter()
        .find(|l| l.members.is_none() && !l.eager)
        .expect("the server")
        .id
        .clone();
    let server = NodeId::new(&server, None);
    let probe = run_directly(&sim, FaultSet::new());
    let to_member_0: MessageId = probe
        .iter()
        .flat_map(|r| r.log.events().iter())
        .find_map(|e| match e {
            Event::Sent { msg, .. }
                if msg.channel == reply && msg.from == server && msg.to.member == Some(0) =>
            {
                Some(msg.clone())
            }
            _ => None,
        })
        .expect("a reply to member 0");
    let orders: BTreeSet<Vec<u32>> = probe
        .iter()
        .map(|r| {
            r.log
                .events()
                .iter()
                .filter_map(|e| match e {
                    Event::Sent { msg, .. } if msg.channel == reply => msg.to.member,
                    _ => None,
                })
                .collect()
        })
        .collect();
    println!("reply orders across the probe's schedules: {orders:?}");

    let hook = FaultHook::new(
        &FaultPlan {
            faults: FaultSet::new(),
        },
        &sim,
    );
    let crash = Fault::Crash {
        node: hook.names().name(&server),
        at: CrashAt::After(to_member_0),
    };
    let records = run_directly(&sim, [crash.clone()].into_iter().collect());
    let split = records
        .iter()
        .filter(|r| replied(r, reply).len() == 1)
        .count();
    println!(
        "{crash}: {split} of {} schedules leave exactly one member answered",
        records.len()
    );

    let label = results
        .results()
        .iter()
        .find(|l| l.plan.is_searched())
        .expect("a searched label");
    let verdict = format!("{:?}", label.verdict);
    let caught = matches!(label.verdict, Verdict::Counterexample(_));
    println!("EcCheck verdict: {verdict}; ground truth: {split} splitting schedules");
    assert_eq!(orders.len(), 1, "one schedule, one reply order: {orders:?}");
    assert_eq!(probe.len(), 1, "a site-free program has one schedule");
    assert!(split > 0, "the crash splits the members");
    assert!(caught, "the search must find it: {results}");
    let Verdict::Counterexample(cex) = &label.verdict else {
        unreachable!()
    };
    assert_eq!(cex.tolerates, Some(0), "{results}");
    assert_eq!(cex.hypothesis, 1, "{results}");
}

/// The walk's default is "sensitive": a node it does not know is opaque. Every IR node kind is
/// classified explicitly (a new one does not compile until it is); `Placeholder`, the only node
/// with no meaning, stands in for an unknown operator.
#[cfg(stageleft_runtime)]
#[test]
fn an_unknown_operator_is_opaque() {
    use hydro_lang::compile::ir::HydroNode;

    use crate::provenance::sensitivity::{NodeClass, classify, nondet_op};

    assert_eq!(
        classify(&HydroNode::Placeholder),
        NodeClass::Opaque("unknown_operator")
    );
    assert_eq!(
        nondet_op(&HydroNode::Placeholder),
        Some(("unknown_operator", true))
    );
}

/// A captured reference the walk cannot resolve to its state cell (anything but a
/// `HydroNode::Reference`) is reported as such, so the closure's node counts as opaque.
#[cfg(stageleft_runtime)]
#[test]
fn an_unresolved_captured_reference_is_opaque() {
    use hydro_lang::compile::ir::{ClosureExpr, DebugExpr, HydroNode};

    use crate::provenance::sensitivity::captures;

    let expr: syn::Expr = syn::parse_quote!(|x| x);
    let f = ClosureExpr::new(DebugExpr::from(expr), vec![(HydroNode::Placeholder, true)]);
    assert_eq!(captures(&f), vec![None]);
}

/// A process broadcasts its input to a cluster (the label is what the members receive); the
/// input's order is `O`.
#[cfg(stageleft_runtime)]
fn broadcast_input<O: hydro_lang::live_collections::stream::Ordering>() -> FlowBuilder<'static> {
    use hydro_lang::live_collections::stream::ExactlyOnce;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, O, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().name("to_members").bincode());
    let _out = delivered.sim_cluster_output();
    flow
}

/// An external input whose order the program leaves unspecified is something the walk cannot
/// see through (in a deployment it arrives in any order): its label is sensitive. The same
/// program over a totally ordered input is not. The broadcast is insensitive to the order of
/// what it carries, so under the order-insensitivity analysis the input counts only for the
/// order of the process's sends (U2); the conservative walk counts it in the cone.
#[cfg(stageleft_runtime)]
#[test]
fn an_unordered_external_input_makes_its_label_sensitive() {
    use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};

    let ordered = label_cones(broadcast_input::<TotalOrder>().finalize().ir());
    assert!(!ordered.is_empty());
    assert!(
        ordered.iter().all(|c| !c.schedule_sensitive()),
        "{ordered:?}"
    );

    let built = broadcast_input::<NoOrder>().finalize();
    let unordered = label_cones(built.ir());
    assert!(!unordered.is_empty());
    for cone in &unordered {
        assert!(
            cone.sites
                .iter()
                .any(|s| s.op == "unordered_external_input" && s.opaque && s.send_order),
            "{cone:?}"
        );
    }
    let conservative = crate::provenance::sensitivity::label_cones_conservative(built.ir());
    for cone in &conservative {
        assert!(
            cone.sites
                .iter()
                .any(|s| s.op == "unordered_external_input" && s.opaque && !s.send_order),
            "{cone:?}"
        );
    }
}

/// U2: a label whose own cone has no site, sent by a process that also batches something
/// else. The batch can reorder the process's sends, and so move a positional crash point
/// across the label's delivery, so the label is sensitive through the sender (`send_order`).
#[cfg(stageleft_runtime)]
#[test]
fn a_choice_at_a_sender_reorders_its_sends() {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send_a, a) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (_send_b, b) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = source.tick();
    let at_members = a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed */
        ));
    at_members.for_each(q!(|_| {}));
    b.batch(&tick, nondet!(/** test */))
        .all_ticks()
        .broadcast_closed(&members, TCP.fail_stop().name("b").bincode())
        .weaken_consistency()
        .for_each(q!(|_| {}));

    let cones = label_cones(flow.finalize().ir());
    let label = cones
        .iter()
        .find(|c| c.collection.name.as_deref() == Some("at_members"))
        .unwrap_or_else(|| panic!("the label: {cones:?}"));
    assert!(label.schedule_sensitive(), "{label:?}");
    assert!(
        label.sites.iter().all(|s| s.send_order),
        "only through the sender: {label:?}"
    );
    assert!(
        label.sites.iter().any(|s| s.op == "batch"),
        "the batch: {label:?}"
    );
}

/// A process broadcasts its input to a cluster; each member batches what it receives, and the
/// label is what comes out of the batches: mapped (`snapshot` false), or kept only in a tick
/// in which more than one record arrived (`snapshot` true, a `filter_if` on the tick's count).
#[cfg(stageleft_runtime)]
fn batched_at_members(snapshot: bool) -> FlowBuilder<'static> {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_members = input.broadcast_closed(&members, TCP.fail_stop().name("to_members").bincode());
    let tick = members.tick();
    let batch = at_members.batch(&tick, nondet!(/** test */));
    let out = if snapshot {
        let crowded = batch.clone().count().map(q!(|n| n > 1));
        batch.filter_if(crowded).all_ticks()
    } else {
        batch.map(q!(|x| x + 1)).all_ticks()
    };
    let _out = out
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed */
        ))
        .sim_cluster_output();
    flow
}

/// The claim `name` the test program makes (named after the binding of its `assert_has_consistency_of`;
/// `broadcast_closed` makes one of its own on what it delivers).
#[cfg(stageleft_runtime)]
fn the_claim<'c>(
    cones: &'c [crate::provenance::sensitivity::LabelCone],
    name: &str,
) -> &'c crate::provenance::sensitivity::LabelCone {
    cones
        .iter()
        .find(|c| {
            c.collection.op == "AssertIsConsistent" && c.collection.name.as_deref() == Some(name)
        })
        .unwrap_or_else(|| panic!("no claim `{name}` among {cones:?}"))
}

/// A batch whose every path to the label is order- and batch-insensitive (a `map`, then
/// `all_ticks`) cannot change what the label holds: the order-insensitivity analysis discharges
/// it, where the conservative walk counts it. The members send nothing, so no send order runs
/// through it either.
#[cfg(stageleft_runtime)]
#[test]
fn a_batch_with_a_monotone_downstream_is_discharged() {
    use crate::provenance::sensitivity::label_cones_conservative;

    let built = batched_at_members(false).finalize();
    let calm = label_cones(built.ir());
    let label = the_claim(&calm, "out");
    assert!(!label.schedule_sensitive(), "{label:?}");
    assert!(
        label.harmless.iter().any(|s| s.op == "batch"),
        "the batch is discharged, not missed: {label:?}"
    );
    let old = label_cones_conservative(built.ir());
    let old = the_claim(&old, "out");
    assert!(
        old.sites.iter().any(|s| s.op == "batch" && !s.send_order),
        "the conservative walk counts it: {old:?}"
    );
}

/// The same batch read through a snapshot of the tick (`filter_if` on its count): which
/// records pass depends on how the schedule splits them into ticks, so the batch counts, and
/// the site names the operator that observes the tick.
#[cfg(stageleft_runtime)]
#[test]
fn a_batch_read_through_a_snapshot_counts() {
    let built = batched_at_members(true).finalize();
    let calm = label_cones(built.ir());
    let label = the_claim(&calm, "out");
    assert!(label.existence_sensitive(), "{label:?}");
    let batch = label
        .sites
        .iter()
        .find(|s| s.op == "batch")
        .unwrap_or_else(|| panic!("the batch: {label:?}"));
    assert!(!batch.send_order, "{label:?}");
    let via = batch.via.as_deref().unwrap_or_else(|| panic!("{label:?}"));
    assert!(
        via.contains("CrossSingleton") || via.contains("Fold"),
        "the snapshot (`filter_if` is a `cross_singleton` with the tick's count): {via}"
    );
}

/// `filter_not_in` against its own history (the `use::state` idiom of
/// `hydro_test::cluster::reliable_broadcast`): over the run, the output is every record that
/// arrived, whatever ticks they arrived in, so the batch before it is discharged.
#[cfg(stageleft_runtime)]
#[test]
fn filter_not_in_against_its_own_history_is_discharged() {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_members = input.broadcast_closed(&members, TCP.fail_stop().name("to_members").bincode());
    let fresh: Stream<u32, Cluster<'_, ()>, Unbounded, NoOrder> = sliced! {
        let arrived = use::batch(at_members, nondet!(/** test */));
        let mut seen = use::state(|l| {
            l.source_iter(q!(Vec::<u32>::new())).weaken_ordering::<NoOrder>()
        });
        let fresh = arrived
            .weaken_ordering::<NoOrder>()
            .unique()
            .filter_not_in(seen.clone());
        seen = seen.chain(fresh.clone());
        fresh
    };
    let _out = fresh
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed */
        ))
        .sim_cluster_output();
    let cones = label_cones(flow.finalize().ir());
    let label = the_claim(&cones, "out");
    assert!(!label.schedule_sensitive(), "{label:?}");
    assert!(label.harmless.iter().any(|s| s.op == "batch"), "{label:?}");
}

/// U2 under the order-insensitivity analysis: a choice at a sender counts only if it feeds
/// what the sender sends. Here the process batches its second input into a local `for_each`
/// only, so its sends to the members are in input order in every schedule; the conservative
/// walk, which takes everything at the sender, counts the batch.
#[cfg(stageleft_runtime)]
#[test]
fn a_choice_at_a_sender_that_feeds_no_send_does_not_reorder_it() {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

    use crate::provenance::sensitivity::label_cones_conservative;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send_a, a) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (_send_b, b) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = source.tick();
    let at_members = a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed */
        ));
    at_members.for_each(q!(|_| {}));
    b.batch(&tick, nondet!(/** test */))
        .all_ticks()
        .for_each(q!(|_| {}));

    let built = flow.finalize();
    let calm = label_cones(built.ir());
    assert!(!calm.is_empty());
    assert!(calm.iter().all(|c| !c.schedule_sensitive()), "{calm:?}");
    let old = label_cones_conservative(built.ir());
    assert!(
        old.iter()
            .any(|c| c.sites.iter().any(|s| s.op == "batch" && s.send_order)),
        "{old:?}"
    );
}

/// Faults that *create* a record (docs/design/hydrofrac-lite.md §4.9, "Established vs not").
/// A source sends `100, 1, 2` over an ordered lossy channel (`TCP.lossy`, which keeps
/// `TotalOrder`), and each member reports every `x < 100` that is not at position `x` of what it
/// received (none, without faults), next to a constant `5`. Omitting the filler `100` to one
/// member shifts `1` and `2` and makes that member report them: a divergent fact, created by an
/// omission, with no crash. The constant's derivation has no delivery, so lineage gives the
/// search nothing to kill and the strong rule would certify the label. Nothing else in the cone
/// is a site (the channel's `nondet!` is the omission itself), so the walk counts the channel.
///
/// The simulator rejects `TCP.lossy` today; the second half of the test pins that, so that
/// support for it is noticed and this program is cross-checked against the brute force.
#[cfg(stageleft_runtime)]
#[test]
fn an_ordered_lossy_channel_is_opaque() {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};

    use crate::provenance::sensitivity::label_cones_conservative;

    let program = || {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<()>();
        let members = flow.cluster::<()>();
        let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let shifted = input
            .broadcast_closed(
                &members,
                TCP.lossy(nondet!(/** omissions, which the check injects */))
                    .name("ordered")
                    .bincode(),
            )
            .weaken_consistency()
            .enumerate()
            .filter_map(q!(|(i, x)| (x < 100 && i != x as usize).then_some(x)))
            .weaken_ordering::<NoOrder>();
        let constant = members
            .source_iter(q!([5u32]))
            .weaken_consistency()
            .weaken_ordering::<NoOrder>();
        let _out = shifted
            .merge_unordered(constant.into())
            .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
                /** claimed (wrongly, under omissions): every member sees the same positions */
            ))
            .sim_cluster_output();
        (flow, members, send)
    };

    let (flow, _, _) = program();
    let built = flow.finalize();
    for cones in [label_cones(built.ir()), label_cones_conservative(built.ir())] {
        let label = the_claim(&cones, "out");
        assert!(
            label
                .sites
                .iter()
                .all(|s| s.op == "ordered_lossy_network" && s.opaque),
            "only the channel: {label:?}"
        );
        assert!(label.existence_sensitive(), "{label:?}");
    }

    let (flow, members, _send) = program();
    let rejected = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = flow.sim().with_cluster_size(&members, 2).compiled();
    }));
    let message = rejected
        .err()
        .and_then(|e| {
            e.downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
        })
        .unwrap_or_default();
    assert!(
        message.contains("only supports fail-stop and lossy-delayed-forever"),
        "the simulator now runs `TCP.lossy`: cross-check this program against the brute force \
         (`EcCheck::cross_check`) and revisit the walk's `ordered_lossy_network` ({message:?})"
    );
}
