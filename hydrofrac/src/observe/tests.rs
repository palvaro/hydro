//! Tests of label enumeration, planning and the tap pass.
//!
//! Every item is `cfg(stageleft_runtime)` because the module itself must exist in the staged copy
//! of the crate (its `q!` closures resolve through `crate::__staged::observe::tests`), while what
//! the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::cell::{Cell, RefCell};
#[cfg(stageleft_runtime)]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(stageleft_runtime)]
use std::panic::AssertUnwindSafe;
#[cfg(stageleft_runtime)]
use std::rc::Rc;

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::Cluster;
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::quiesce;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::network_hook::{IrPassStage, SimNetworkHook};

#[cfg(stageleft_runtime)]
use super::kind_name;
#[cfg(stageleft_runtime)]
use super::{LabelPlan, Labels, NotCheckedReason, ec_collections, plan, tap_pass};
#[cfg(stageleft_runtime)]
use crate::observe::{Fact, decode_output};

#[cfg(stageleft_runtime)]
/// `(file, name, occurrence)`: an identity that survives edits elsewhere in the file.
fn stable_key(c: &super::EcCollection) -> (String, String, usize) {
    (
        c.file.clone().unwrap_or_default(),
        c.display_name().to_owned(),
        c.occurrence,
    )
}

#[cfg(stageleft_runtime)]
fn ports_of(labels: &Labels, label: usize) -> impl Iterator<Item = usize> + '_ {
    labels
        .ports
        .iter()
        .filter(move |(_, l)| **l == label)
        .map(|(p, _)| *p)
}

#[cfg(stageleft_runtime)]
fn searched(labels: &Labels) -> impl Iterator<Item = (usize, &LabelPlan)> {
    labels.plans.iter().enumerate().filter(|(_, p)| p.is_searched())
}

#[cfg(stageleft_runtime)]
fn plans_of(flow: FlowBuilder<'_>) -> Vec<LabelPlan> {
    let built = flow.finalize();
    plan(ec_collections(built.ir()))
}

#[cfg(stageleft_runtime)]
fn find<'p>(plans: &'p [LabelPlan], name: &str) -> (usize, &'p LabelPlan) {
    plans
        .iter()
        .enumerate()
        .find(|(_, p)| p.collection.display_name() == name)
        .unwrap_or_else(|| {
            panic!(
                "no label `{name}` among {:?}",
                plans
                    .iter()
                    .map(|p| stable_key(&p.collection))
                    .collect::<Vec<_>>()
            )
        })
}

#[cfg(stageleft_runtime)]
/// Records every external output, by `(port, member)`, decoded as a tap.
#[derive(Default)]
struct Outputs {
    seen: RefCell<Vec<(usize, Option<u32>, Vec<u8>)>>,
}

#[cfg(stageleft_runtime)]
impl SimNetworkHook for Outputs {
    fn on_instance_begin(&self) {
        self.seen.borrow_mut().clear();
    }

    fn on_send(
        &self,
        _channel: usize,
        _from_member: Option<u32>,
        _to_member: Option<u32>,
        _payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        Some(0)
    }

    fn on_output(
        &self,
        port: usize,
        member: Option<u32>,
        payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> bool {
        self.seen
            .borrow_mut()
            .push((port, member, payload.to_vec()));
        true
    }
}

#[cfg(stageleft_runtime)]
impl Outputs {
    /// The tap facts of `label` per member.
    fn facts(&self, labels: &Labels, label: usize) -> BTreeMap<u32, BTreeSet<Fact>> {
        let mut out: BTreeMap<u32, BTreeSet<Fact>> = BTreeMap::new();
        for (port, member, payload) in self.seen.borrow().iter() {
            if labels.label_of_port(*port) == Some(label) {
                out.entry(member.expect("taps are on clusters"))
                    .or_default()
                    .insert(decode_output(payload));
            }
        }
        out
    }
}

#[cfg(stageleft_runtime)]
#[test]
fn a_closed_broadcast_is_one_searched_label_named_after_its_binding() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    delivered.for_each(q!(|_| {}));

    let plans = plans_of(flow);
    assert_eq!(plans.len(), 1);
    let plan = &plans[0];
    assert!(plan.is_searched(), "{plan:?}");
    assert_eq!(
        stable_key(&plan.collection),
        ("tests.rs".to_owned(), "delivered".to_owned(), 0)
    );
}

#[cfg(stageleft_runtime)]
#[test]
fn a_label_with_local_inputs_only_is_checked() {
    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let local = members
        .source_iter(q!(vec![1u32, 2]))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** the same constant at every member */
        ));
    local.for_each(q!(|_| {}));

    let plans = plans_of(flow);
    let (_, plan) = find(&plans, "local");
    // No fault can withhold its inputs, but members can still disagree on it without any fault
    // (`review_c_local_only_label_that_disagrees_passes_assert_holds`), so it is checked.
    assert!(!plan.collection.fed_by_network, "{plan:?}");
    assert!(plan.is_searched(), "{plan:?}");
}

/// Uniform reliable broadcast delivers through a closure that reads the quorum's state by
/// reference (`HydroNode::Reference`s in the closure's `singleton_refs`, which
/// `HydroNode::input` does not list). The delivered set is fed by the echo network through those
/// references, so it is checked, not "local inputs only".
#[cfg(stageleft_runtime)]
#[test]
fn a_label_fed_through_closure_references_is_checked() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered =
        hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed(
            input, &members, 2,
        );
    let _out = delivered.sim_cluster_output();

    let plans = plans_of(flow);
    let delivered: Vec<_> = plans
        .iter()
        .filter(|p| p.collection.file.as_deref() == Some("uniform_broadcast.rs"))
        .collect();
    assert_eq!(delivered.len(), 1, "{plans:?}");
    assert!(delivered[0].is_searched(), "{:?}", delivered[0]);
}

/// The gossip state (a fold of `all_elements`) is not checked: agreement on the stream's set of
/// facts does not fix a fold's value (a count over duplicates differs), so a scalar is never
/// covered by its stream.
#[cfg(stageleft_runtime)]
#[test]
fn gossip_state_is_not_covered_by_the_stream_it_folds() {
    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let (_u, updates) = members.sim_input::<u32, NoOrder, ExactlyOnce>();
    let (_t, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
    let state = hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip(&members, updates, ticks);
    let obs_tick = members.tick();
    let _out = state
        .snapshot(&obs_tick, nondet!(/** test observation */))
        .all_ticks()
        .sim_cluster_output();

    let plans = plans_of(flow);
    let (all, all_elements) = find(&plans, "all_elements");
    assert!(all_elements.is_searched());
    let (_, state) = find(&plans, "gset_state");
    assert_eq!(state.collection.covered_by, Some(all));
    assert!(matches!(state.not_checked, Some(NotCheckedReason::NotAStream(_))), "{state:?}");
    assert!(!state.is_searched());
    for hop in ["initial", "echo"] {
        assert!(find(&plans, hop).1.is_searched());
    }
}

#[cfg(stageleft_runtime)]
#[test]
fn broadcast_live_is_labelled_at_its_fan_out() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = hydro_std::ec_inference_demos::broadcast_live::broadcast_live_from_process(
        input,
        &members,
        TCP.fail_stop().bincode(),
    );
    let _out = delivered.sim_cluster_output();

    let plans = plans_of(flow);
    let searched: Vec<_> = plans.iter().filter(|p| p.is_searched()).collect();
    assert_eq!(searched.len(), 1, "{plans:?}");
    assert_eq!(searched[0].collection.file.as_deref(), Some("fan_out.rs"));
}

#[cfg(stageleft_runtime)]
#[test]
fn taps_deliver_every_element_to_the_host_per_member_with_its_debug() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    let out = delivered.map(q!(|x| x * 10)).sim_cluster_output();

    let (taps, pass) = tap_pass();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 3)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PreNetwork, pass)
        .compiled();
    let labels = taps.labels();
    let (label, _) = find(&labels.plans, "delivered");
    assert_eq!(ports_of(&labels, label).count(), 1);

    let outputs = Rc::new(Outputs::default());
    sim.set_network_hook(outputs.clone());
    let outputs = AssertUnwindSafe(outputs);
    let runs = AssertUnwindSafe(Cell::new(0));
    sim.exhaustive(async || {
        send.send(1);
        send.send(2);
        quiesce().await;
        // The program's own output is unchanged by the tap.
        for member in 0..3u32 {
            let mut got: Vec<u32> = out.collect(member).await;
            got.sort();
            assert_eq!(got, vec![10, 20]);
        }
        let facts = outputs.facts(&labels, label);
        assert_eq!(facts.len(), 3, "{facts:?}");
        for held in facts.values() {
            let rendered: Vec<String> = held.iter().map(ToString::to_string).collect();
            assert_eq!(rendered, vec!["1", "2"]);
            let decoded: Vec<u32> = held
                .iter()
                .map(|f| bincode::deserialize(&f.bytes).unwrap())
                .collect();
            assert_eq!(decoded, vec![1, 2]);
        }
        runs.set(runs.get() + 1);
    });
    assert!(runs.get() > 0);
}

#[cfg(stageleft_runtime)]
/// A labelled `Singleton` whose value is overwritten is not tapped.
///
/// Its versions are not facts: member A going `1 → 2` and member B going `2 → 1` have the same
/// union of versions but different final values, so a union view over versions would certify a
/// run that diverges (`crate::spec` tests the union view itself). This test fails if such a
/// label is ever tapped, or classified as checked.
#[test]
fn an_overwritten_singleton_is_not_tapped() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let latest = input
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .weaken_consistency()
        .fold(q!(|| 0u32), q!(|acc, x| *acc = x))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** deliberately unproven: the last write wins, and arrival order may differ */
        ));
    let tick = members.tick();
    let _out = latest
        .snapshot(&tick, nondet!(/** test observation */))
        .all_ticks()
        .sim_cluster_output();

    let (taps, pass) = tap_pass();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .skip_consistency_assertions()
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PreNetwork, pass)
        .compiled();
    let labels = taps.labels();
    let (label, plan) = labels
        .plans
        .iter()
        .enumerate()
        .find(|(_, p)| {
            p.collection.display_name() == "latest"
                && kind_name(&p.collection.collection_kind) == "Singleton"
        })
        .unwrap_or_else(|| panic!("the singleton is a label of its own: {:?}", labels.plans));
    assert_eq!(
        plan.not_checked,
        Some(NotCheckedReason::NotAStream("Singleton".to_owned())),
        "an overwritten singleton must not be checked by a union view: {plan:?}"
    );
    assert_eq!(ports_of(&labels, label).count(), 0);
    // The broadcast in the same statement is a label of its own (a stream, so it is checked),
    // and it is the only one tapped.
    let searched: Vec<_> = searched(&labels).collect();
    assert_eq!(searched.len(), 1, "{:?}", labels.plans);
    assert_eq!(
        kind_name(&searched[0].1.collection.collection_kind),
        "Stream"
    );
    let stream_ports: Vec<usize> = ports_of(&labels, searched[0].0).collect();

    let outputs = Rc::new(Outputs::default());
    sim.set_network_hook(outputs.clone());
    let outputs = AssertUnwindSafe(outputs);
    sim.first_schedule(async || {
        send.send(1);
        send.send(2);
        quiesce().await;
        // Every tap output is the broadcast's; none carries the singleton's versions.
        let seen = outputs.seen.borrow();
        assert!(!seen.is_empty());
        for (port, _, _) in seen.iter() {
            if labels.label_of_port(*port).is_some() {
                assert!(stream_ports.contains(port));
            }
        }
    });
}

#[cfg(stageleft_runtime)]
/// A cluster-to-cluster closed broadcast is a labelled `KeyedStream`: each fact is a
/// `(sender, value)` entry, and every member sees the same entries.
///
/// (Programs from dev-dependencies such as `hydro_std` cannot be simulated from this crate's
/// tests: the simulation dylib links only this crate and `hydro_lang`. Their labels are checked by
/// enumeration above; their taps run when the corpus is ported.)
#[test]
fn keyed_taps_carry_entries_and_agree_across_members() {
    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let (send, input) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    let _out = delivered.entries().sim_cluster_output();

    let (taps, pass) = tap_pass();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PreNetwork, pass)
        .compiled();
    let labels = taps.labels();
    let (label, plan) = find(&labels.plans, "delivered");
    assert_eq!(kind_name(&plan.collection.collection_kind), "KeyedStream");
    let outputs = Rc::new(Outputs::default());
    sim.set_network_hook(outputs.clone());
    let outputs = AssertUnwindSafe(outputs);
    sim.exhaustive(async || {
        send.send_many_unordered([(0, 1u32), (1, 2u32)]);
        quiesce().await;
        let facts = outputs.facts(&labels, label);
        assert_eq!(facts.len(), 2, "{facts:?}");
        assert_eq!(facts[&0], facts[&1]);
        assert_eq!(facts[&0].len(), 2);
        for fact in &facts[&0] {
            let debug = fact.debug.as_deref().unwrap();
            assert!(
                debug.ends_with(", 1)") || debug.ends_with(", 2)"),
                "{debug}"
            );
        }
    });
}

#[cfg(stageleft_runtime)]
#[test]
fn a_cluster_broadcasts_member_list_is_not_a_label() {
    // A cluster-to-cluster `broadcast_closed` labels the destination's member-id list and joins
    // the payload with it. That list is local (a `ClusterMembers` source), and it used to be
    // listed as a not-checked `echoed` Stream next to the real echo, under the broadcasting
    // statement's name.
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input.broadcast_closed(&members, TCP.fail_stop().name("initial").bincode());
    let echoed = initial
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values();
    let delivered = initial.weaken_ordering::<NoOrder>().merge_unordered(echoed);
    let _out = delivered.sim_cluster_output();
    let plans = plans_of(flow);

    let rows: Vec<(&str, &str)> = plans
        .iter()
        .map(|p| {
            (
                p.collection.display_name(),
                kind_name(&p.collection.collection_kind),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("initial", "Stream"),
            ("echoed", "KeyedStream"),
            ("delivered", "Stream")
        ],
        "{plans:#?}"
    );
    assert!(
        plans.iter().all(|p| p.not_checked.is_none()),
        "every label is fed by a network: {plans:#?}"
    );
}
