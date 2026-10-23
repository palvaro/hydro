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
