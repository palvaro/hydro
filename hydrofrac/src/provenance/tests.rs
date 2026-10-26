//! Tests for the record-provenance rewrite. Each builds a small program, installs the rewrite
//! and a recording network hook, runs every schedule, and checks the lineage the hook received as
//! message metadata against what each record was actually computed from.

// Module-level items here are also copied into the crate's staged copy (for the `q!` closures),
// which is compiled into simulation dylibs; everything that only the host needs lives in
// `support`, which is runtime-only.
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::nondet::nondet;
use hydro_lang::prelude::FlowBuilder;
use stageleft::q;

#[cfg(stageleft_runtime)]
use support::*;

#[cfg(stageleft_runtime)]
mod support {
    use std::cell::{Cell, RefCell};
    pub(super) use std::collections::{BTreeMap, BTreeSet};
    pub(super) use std::panic::AssertUnwindSafe;
    pub(super) use std::rc::Rc;

    pub(super) use hydro_lang::sim::compiled::CompiledSim;
    pub(super) use hydro_lang::sim::flow::SimFlow;
    pub(super) use hydro_lang::sim::network_hook::SimNetworkHook;

    pub(super) use super::super::{RecordProv, install};

    /// One delivered send, as the hook saw it.
    #[derive(Clone, Debug)]
    pub(super) struct Sent {
        pub(super) tag: u64,
        pub(super) channel: usize,
        pub(super) payload: Vec<u8>,
        pub(super) prov: Option<RecordProv>,
    }

    /// One external output, as the hook saw it.
    #[derive(Clone, Debug)]
    pub(super) struct Output {
        pub(super) payload: Vec<u8>,
        pub(super) prov: Option<RecordProv>,
    }

    /// Tags every send with a fresh id (from 1) and records sends, receives and outputs of the
    /// current instance.
    #[derive(Default)]
    pub(super) struct Recorder {
        pub(super) next: Cell<u64>,
        pub(super) sent: RefCell<Vec<Sent>>,
        pub(super) received: RefCell<Vec<u64>>,
        pub(super) outputs: RefCell<Vec<Output>>,
    }

    impl SimNetworkHook for Recorder {
        fn on_instance_begin(&self) {
            self.next.set(1);
            self.sent.borrow_mut().clear();
            self.received.borrow_mut().clear();
            self.outputs.borrow_mut().clear();
        }

        fn on_send(
            &self,
            channel: usize,
            _from_member: Option<u32>,
            _to_member: Option<u32>,
            payload: &[u8],
            meta: Option<Vec<u8>>,
        ) -> Option<u64> {
            let tag = self.next.get();
            self.next.set(tag + 1);
            self.sent.borrow_mut().push(Sent {
                tag,
                channel,
                payload: payload.to_vec(),
                prov: meta.map(|m| RecordProv::decode(&m)),
            });
            Some(tag)
        }

        fn on_recv(&self, _channel: usize, _to_member: Option<u32>, tag: u64) {
            self.received.borrow_mut().push(tag);
        }

        fn on_output(
            &self,
            _port: usize,
            _member: Option<u32>,
            payload: &[u8],
            meta: Option<Vec<u8>>,
        ) -> bool {
            self.outputs.borrow_mut().push(Output {
                payload: payload.to_vec(),
                prov: meta.map(|m| RecordProv::decode(&m)),
            });
            true
        }
    }

    impl Recorder {
        /// The send with this tag.
        pub(super) fn send(&self, tag: u64) -> Sent {
            self.sent
                .borrow()
                .iter()
                .find(|s| s.tag == tag)
                .unwrap_or_else(|| panic!("no send tagged {tag}"))
                .clone()
        }

        /// The ids of an output's lineage.
        pub(super) fn ids(prov: &Option<RecordProv>) -> BTreeSet<u64> {
            match prov {
                Some(RecordProv::Ids(ids)) => ids.clone(),
                other => panic!("expected precise lineage, got {other:?}"),
            }
        }
    }

    /// Installs the rewrite on `flow`, compiles it, and installs a fresh recorder.
    pub(super) fn compile<'a>(
        flow: SimFlow<'a>,
    ) -> (
        CompiledSim,
        AssertUnwindSafe<Rc<Recorder>>,
        Rc<RefCell<Vec<String>>>,
    ) {
        let islands = Rc::new(RefCell::new(Vec::new()));
        let compiled = install(flow, Some(islands.clone())).compiled();
        let hook = Rc::new(Recorder::default());
        compiled.set_network_hook(hook.clone());
        (compiled, AssertUnwindSafe(hook), islands)
    }
}

/// A received record depends on exactly its delivery, and a value from an external input on
/// nothing.
#[test]
fn received_record_names_exactly_its_delivery() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let (input, items) = a.sim_input::<i32, TotalOrder, ExactlyOnce>();
    let out = items
        .send(&b, TCP.fail_stop().bincode())
        .map(q!(|x| x * 2))
        .sim_output();

    let (compiled, hook, islands) = compile(flow.sim());
    let mut instances = 0;
    compiled.exhaustive(async || {
        input.send(5);
        out.assert_yields([10]).await;
        let sent = hook.sent.borrow().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].prov, Some(RecordProv::Ids(BTreeSet::new())));
        let outputs = hook.outputs.borrow();
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            Recorder::ids(&outputs[0].prov),
            BTreeSet::from([sent[0].tag])
        );
        instances += 1;
    });
    assert!(instances > 0);
    assert!(islands.borrow().is_empty(), "{:?}", islands.borrow());
}

/// Lineage closes backwards through sends: a record forwarded over two hops names the second
/// delivery, and that delivery's send carried the first.
#[test]
fn lineage_closes_through_two_hops() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let c = flow.process::<()>();
    let (input, items) = a.sim_input::<i32, TotalOrder, ExactlyOnce>();
    let out = items
        .send(&b, TCP.fail_stop().bincode())
        .map(q!(|x| x + 1))
        .send(&c, TCP.fail_stop().bincode())
        .filter(q!(|x| *x > 0))
        .sim_output();

    let (compiled, hook, _) = compile(flow.sim());
    compiled.exhaustive(async || {
        input.send(1);
        input.send(-5);
        out.assert_yields([2]).await;
        let outputs = hook.outputs.borrow().clone();
        assert_eq!(outputs.len(), 1);
        let second = Recorder::ids(&outputs[0].prov);
        assert_eq!(second.len(), 1);
        let hop2 = hook.send(*second.first().unwrap());
        let value: i32 = bincode::deserialize(&hop2.payload).unwrap();
        assert_eq!(value, 2);
        let first = Recorder::ids(&hop2.prov);
        assert_eq!(first.len(), 1);
        let hop1 = hook.send(*first.first().unwrap());
        assert_ne!(hop1.channel, hop2.channel);
        let value: i32 = bincode::deserialize(&hop1.payload).unwrap();
        assert_eq!(
            value, 1,
            "the second hop's lineage names the wrong first-hop delivery"
        );
        assert_eq!(hop1.prov, Some(RecordProv::Ids(BTreeSet::new())));
    });
}

/// A fold over a tick's batch depends on exactly the deliveries in that batch.
#[test]
fn fold_accumulates_the_deliveries_it_folded() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let (input, items) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = b.tick();
    let out = items
        .send(&b, TCP.fail_stop().bincode())
        .batch(&tick, nondet!(/** test */))
        .fold(q!(|| 0u32), q!(|acc, v| *acc += v))
        .all_ticks()
        .sim_output();

    let (compiled, hook, _) = compile(flow.sim());
    let mut shapes = BTreeSet::new();
    compiled.exhaustive(async || {
        input.send(1);
        input.send(2);
        input.send(4);
        // Batches partition the three deliveries; each sum names a unique subset.
        let mut total = 0;
        while total < 7 {
            let sum: u32 = out.next().await;
            total += sum;
        }
        let outputs = hook.outputs.borrow().clone();
        let mut covered = BTreeSet::new();
        for o in outputs.iter() {
            // Sim outputs use the default sim codec (MessagePack); network payloads are bincode.
            let sum: u32 =
                <hydro_lang::sim::codec::RmpCodec as hydro_lang::sim::codec::SimCodec<u32>>::decode(&o.payload);
            let ids = Recorder::ids(&o.prov);
            let from: u32 = ids
                .iter()
                .map(|t| bincode::deserialize::<u32>(&hook.send(*t).payload).unwrap())
                .sum();
            assert_eq!(
                from, sum,
                "fold output {sum} names deliveries summing to {from}"
            );
            covered.extend(ids);
        }
        assert_eq!(covered.len(), 3);
        shapes.insert(outputs.iter().filter(|o| o.payload != [0, 0, 0, 0]).count());
    });
    assert!(
        shapes.len() > 1,
        "expected schedules with different batchings"
    );
}

/// A join's output depends on both sides.
#[test]
fn join_unions_both_sides() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let c = flow.process::<()>();
    let (left_in, left) = a.sim_input::<(u32, char), TotalOrder, ExactlyOnce>();
    let (right_in, right) = b.sim_input::<(u32, u64), TotalOrder, ExactlyOnce>();
    let out = left
        .send(&c, TCP.fail_stop().bincode())
        .join(right.send(&c, TCP.fail_stop().bincode()))
        .sim_output();

    let (compiled, hook, _) = compile(flow.sim());
    compiled.exhaustive(async || {
        left_in.send((7, 'x'));
        right_in.send((7, 70));
        out.assert_yields_unordered([(7, ('x', 70))]).await;
        let sent = hook.sent.borrow().clone();
        assert_eq!(sent.len(), 2);
        let outputs = hook.outputs.borrow();
        assert_eq!(
            Recorder::ids(&outputs[0].prov),
            sent.iter().map(|s| s.tag).collect::<BTreeSet<_>>()
        );
    });
}

/// A closure reading Hydro state by reference absorbs everything that state absorbed.
#[test]
fn by_ref_state_absorbs_its_lineage() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let (state_in, state_items) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (probe_in, probes) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = b.tick();
    let total = state_items
        .send(&b, TCP.fail_stop().bincode().name("state"))
        .batch(&tick, nondet!(/** test */))
        .fold(q!(|| 0u32), q!(|acc, v| *acc += v));
    let total_ref = total.by_ref();
    let out = probes
        .send(&b, TCP.fail_stop().bincode().name("probe"))
        .batch(&tick, nondet!(/** test */))
        .map(q!(|x| x + *total_ref))
        .all_ticks()
        .sim_output();

    let (compiled, hook, _) = compile(flow.sim());
    let mut saw_state = false;
    compiled.exhaustive(async || {
        state_in.send(10);
        probe_in.send(1);
        let v: u32 = out.next().await;
        let outputs = hook.outputs.borrow().clone();
        let ids = Recorder::ids(&outputs[0].prov);
        let sent = hook.sent.borrow().clone();
        let probe = sent
            .iter()
            .find(|s| bincode::deserialize::<u32>(&s.payload).unwrap() == 1);
        let state = sent
            .iter()
            .find(|s| bincode::deserialize::<u32>(&s.payload).unwrap() == 10);
        assert!(
            ids.contains(&probe.unwrap().tag),
            "the probe's own delivery is missing"
        );
        if v == 11 {
            // The state had absorbed the state delivery when the probe read it.
            saw_state = true;
            assert!(
                ids.contains(&state.unwrap().tag),
                "the probe read state built from a delivery its lineage does not name"
            );
        }
    });
    assert!(saw_state);
}

/// `entries_partially_ordered` on a keyed stream (the leader merge of hydro_std's
/// `leader_merge_broadcast`, docs/design/hydrofrac-lite.md §4.2): the simulator's hook orders
/// the keyed entries, and lineage stays precise through it.
#[test]
fn leader_merge_entries_partially_ordered_is_precise() {
    let mut flow = FlowBuilder::new();
    let writers = flow.cluster::<()>();
    let leader = flow.process::<()>();
    let replicas = flow.cluster::<()>();
    let (write, writes) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = writes
        .send(&leader, TCP.fail_stop().name("to_leader").bincode())
        .entries_partially_ordered(nondet!(/** the leader's interleaving of the writers */))
        .broadcast_closed(&replicas, TCP.fail_stop().name("merged").bincode())
        .sim_cluster_output();

    let (compiled, hook, islands) = compile(
        flow.sim()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&replicas, 2),
    );
    let mut instances = 0;
    compiled.exhaustive(async || {
        write.send(0, 100);
        write.send(1, 200);
        for member in 0..2 {
            let _ = out.next(member).await;
            let _ = out.next(member).await;
        }
        let outputs = hook.outputs.borrow().clone();
        assert_eq!(outputs.len(), 4);
        for o in outputs {
            // Each replica's record names its leader→replica delivery, whose send names the
            // writer→leader delivery of the same value.
            let ids = Recorder::ids(&o.prov);
            assert_eq!(ids.len(), 1);
            let hop2 = hook.send(*ids.first().unwrap());
            let hop1_ids = Recorder::ids(&hop2.prov);
            assert_eq!(hop1_ids.len(), 1);
            let hop1 = hook.send(*hop1_ids.first().unwrap());
            let written: u32 = bincode::deserialize(&hop1.payload).unwrap();
            // The forwarded entry is `(writer, value)`; bincode puts the `u32` value last.
            let tail: [u8; 4] = hop2.payload[hop2.payload.len() - 4..].try_into().unwrap();
            let forwarded = u32::from_le_bytes(tail);
            assert_eq!(
                written, forwarded,
                "lineage names another writer's delivery"
            );
        }
        instances += 1;
    });
    assert!(instances > 1, "expected several interleavings");
    assert!(islands.borrow().is_empty(), "{:?}", islands.borrow());
}

/// Instrumenting a program changes neither its outputs nor its schedule count.
#[test]
fn the_rewrite_preserves_behaviour_and_schedules() {
    fn build(instrument: bool) -> (usize, BTreeMap<Vec<u32>, usize>) {
        let mut flow = FlowBuilder::new();
        let a = flow.process::<()>();
        let b = flow.process::<()>();
        let (input, items) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let tick = b.tick();
        let out = items
            .send(&b, TCP.fail_stop().bincode())
            .batch(&tick, nondet!(/** test */))
            .fold(q!(|| 0u32), q!(|acc, v| *acc += v))
            .all_ticks()
            .sim_output();
        let sim = flow.sim();
        let compiled = if instrument {
            compile(sim).0
        } else {
            sim.compiled()
        };
        let mut runs = BTreeMap::new();
        let count = compiled.exhaustive(async || {
            input.send(1);
            input.send(2);
            let mut got = Vec::new();
            while got.iter().sum::<u32>() < 3 {
                got.push(out.next().await);
            }
            *runs.entry(got).or_insert(0) += 1;
        });
        (count, runs)
    }
    assert_eq!(build(true), build(false));
}

/// A closure writing Hydro state through `by_mut` carries each item's lineage into the state, and
/// a later read of that state names it.
#[test]
fn by_mut_state_carries_the_writers_lineage() {
    use hydro_lang::prelude::sliced;

    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let (add_in, adds) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (read_in, reads) = a.sim_input::<(), TotalOrder, ExactlyOnce>();
    let adds = adds.send(&b, TCP.fail_stop().bincode().name("adds"));
    let reads = reads.send(&b, TCP.fail_stop().bincode().name("reads"));
    let out = sliced! {
        let mut count = use::state(|l| l.singleton(q!(0u32)));
        let add_batch = use::batch(adds, nondet!(/** test */));
        let read_batch = use::batch(reads, nondet!(/** test */));
        let count_mut = count.by_mut();
        add_batch.for_each(q!(|x| *count_mut += x));
        read_batch.map(q!(|_| *count_mut))
    }
    .sim_output();

    let (compiled, hook, _) = compile(flow.sim());
    let mut saw_add = false;
    compiled.exhaustive(async || {
        add_in.send(3);
        read_in.send(());
        let v: u32 = out.next().await;
        let outputs = hook.outputs.borrow().clone();
        let ids = Recorder::ids(&outputs[0].prov);
        let sent = hook.sent.borrow().clone();
        let add = sent.iter().find(|s| s.payload.len() == 4).unwrap();
        let read = sent.iter().find(|s| s.payload.is_empty()).unwrap();
        assert!(
            ids.contains(&read.tag),
            "the read's own delivery is missing"
        );
        if v == 3 {
            saw_add = true;
            assert!(
                ids.contains(&add.tag),
                "the read saw state written by a delivery its lineage does not name"
            );
        }
    });
    assert!(saw_add);
}

/// An operator the rewrite cannot see into is an opaque island: the program still runs, the
/// island is reported, and records computed through it are opaque.
#[test]
fn opaque_islands_run_and_report_opaque_lineage() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let (input, items) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = items
        .send(&b, TCP.fail_stop().bincode())
        .map(q!(|x| async move { x + 1 }))
        .resolve_futures_ordered()
        .sim_output();

    let (compiled, hook, islands) = compile(flow.sim());
    compiled.exhaustive(async || {
        input.send(1);
        out.assert_yields([2]).await;
        let outputs = hook.outputs.borrow();
        assert_eq!(outputs[0].prov, Some(RecordProv::Opaque));
    });
    assert_eq!(*islands.borrow(), ["ResolveFuturesOrdered"]);
}

/// The generated code encodes [`RecordProv`] without naming it (see `rewrite::meta_expr`); the
/// encodings must agree.
#[cfg(stageleft_runtime)]
#[test]
fn inline_metadata_encoding_matches_record_prov() {
    let ids = BTreeSet::from([1u64, 7, u64::MAX - 1]);
    assert_eq!(
        bincode::serialize(&(0u32, ids.clone())).unwrap(),
        RecordProv::Ids(ids.clone()).encode()
    );
    assert_eq!(
        bincode::serialize(&1u32).unwrap(),
        RecordProv::Opaque.encode()
    );
    assert_eq!(
        RecordProv::decode(&RecordProv::Ids(ids.clone()).encode()),
        RecordProv::Ids(ids)
    );
}
