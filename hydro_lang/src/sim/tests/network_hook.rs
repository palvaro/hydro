//! Tests for the simulator extension points in [`crate::sim::network_hook`]. None of them uses
//! any fault-injection logic: each installs a small purpose-built hook or IR pass.

// Module-level items here are also copied into the staged crate (for the `q!` closures), where
// `sim::network_hook` does not exist; everything that names it lives in `support`, which is
// runtime-only.
use stageleft::q;

use crate::location::Location;
use crate::networking::TCP;
use crate::prelude::FlowBuilder;

#[cfg(stageleft_runtime)]
use support::*;

#[cfg(stageleft_runtime)]
mod support {
    pub(super) use std::cell::RefCell;
    pub(super) use std::collections::BTreeMap;
    pub(super) use std::rc::Rc;

    pub(super) use crate::compile::ir::{
        DebugExpr, HydroNode, HydroRoot, NetworkRecv, NetworkSend,
    };
    pub(super) use crate::location::dynamic::LocationId;
    pub(super) use crate::networking::{NetworkingInfo, TcpFault};
    pub(super) use crate::sim::network_hook::{IrPassStage, SimNetworkHook};

    /// Applies `f` to every `Network` node in `ir`.
    pub(super) fn for_each_network(ir: &mut [HydroRoot], mut f: impl FnMut(&mut HydroNode)) {
        let mut seen = Default::default();
        for root in ir.iter_mut() {
            root.transform_bottom_up(
                &mut |_| {},
                &mut |n| {
                    if matches!(n, HydroNode::Network { .. }) {
                        f(n)
                    }
                },
                &mut seen,
                false,
            );
        }
    }

    /// A hook that drops every message on the channels in `drop`, tags the rest with a counter, and
    /// records what it saw.
    #[derive(Default)]
    pub(super) struct RecordingHook {
        pub(super) drop: Vec<usize>,
        pub(super) next_tag: RefCell<u64>,
        /// `(channel, from, to, tag)` per delivered send.
        pub(super) sent: RefCell<Vec<(usize, Option<u32>, Option<u32>, u64)>>,
        /// `(channel, to, tag)` per receive.
        pub(super) received: RefCell<Vec<(usize, Option<u32>, u64)>>,
        /// Meta handed to `on_send`, per delivered send, with the payload it came with.
        pub(super) metas: RefCell<Vec<(Option<u32>, Vec<u8>, Vec<u8>)>>,
        pub(super) outputs: RefCell<Vec<(usize, Option<u32>)>>,
        /// `(location, member, ended)` per execution.
        pub(super) execs: RefCell<Vec<(LocationId, Option<u32>, bool)>>,
        pub(super) instances: RefCell<usize>,
        /// After this many delivered sends, ask the scheduler to halt the sender.
        pub(super) halt_after: Option<(usize, LocationId)>,
        pub(super) halts: RefCell<Vec<(LocationId, Option<u32>)>>,
        /// The execution in which the halt was requested.
        pub(super) halted_in: RefCell<Option<usize>>,
    }

    impl SimNetworkHook for RecordingHook {
        fn on_instance_begin(&self) {
            *self.instances.borrow_mut() += 1;
            if self.halt_after.is_some() {
                // Halting tests look at one instance at a time.
                self.sent.borrow_mut().clear();
                self.execs.borrow_mut().clear();
                self.halts.borrow_mut().clear();
            }
        }

        fn on_send(
            &self,
            channel: usize,
            from_member: Option<u32>,
            to_member: Option<u32>,
            payload: &[u8],
            meta: Option<Vec<u8>>,
        ) -> Option<u64> {
            if self.drop.contains(&channel) {
                return None;
            }
            let tag = {
                let mut next = self.next_tag.borrow_mut();
                *next += 1;
                *next
            };
            self.sent
                .borrow_mut()
                .push((channel, from_member, to_member, tag));
            if let Some(meta) = meta {
                self.metas
                    .borrow_mut()
                    .push((to_member, meta, payload.to_vec()));
            }
            if let Some((n, loc)) = &self.halt_after
                && self.sent.borrow().len() == *n
            {
                self.halts.borrow_mut().push((loc.clone(), from_member));
                *self.halted_in.borrow_mut() = Some(self.execs.borrow().len() - 1);
            }
            Some(tag)
        }

        fn on_recv(&self, channel: usize, to_member: Option<u32>, tag: u64) {
            self.received.borrow_mut().push((channel, to_member, tag));
        }

        fn on_output(
            &self,
            port: usize,
            member: Option<u32>,
            _payload: &[u8],
            _meta: Option<Vec<u8>>,
        ) -> bool {
            self.outputs.borrow_mut().push((port, member));
            true
        }

        fn on_exec_begin(&self, location: &LocationId, member: Option<u32>) {
            self.execs
                .borrow_mut()
                .push((location.clone(), member, false));
        }

        fn on_exec_end(&self) {
            let mut execs = self.execs.borrow_mut();
            let last = execs.last_mut().expect("on_exec_end without on_exec_begin");
            assert!(!last.2, "on_exec_end twice for one execution");
            last.2 = true;
        }

        fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
            std::mem::take(&mut self.halts.borrow_mut())
        }
    }
}

/// E1 + E5: a hook that drops every message on one channel; delivered messages carry the hook's
/// tag to the receiver. E4: the channel and location tables describe the program.
#[cfg(stageleft_runtime)]
#[test]
fn hook_drops_one_channel_and_tags_round_trip() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();

    let (in_send, input) = p1.sim_input::<i32, _, _>();
    let input = input.weaken_ordering().weaken_retries();
    let a = input
        .clone()
        .map(q!(|x| x * 10))
        .send(&p2, TCP.fail_stop().bincode().name("a"));
    let b = input
        .map(q!(|x| x * 100))
        .send(&p2, TCP.fail_stop().bincode().name("b"));
    let out = a.merge_unordered(b).sim_output();

    let compiled = flow.sim().with_network_hook_points().compiled();
    let [a, b] = compiled.channels() else {
        panic!("expected two channels")
    };
    let fail_stop = NetworkingInfo::Tcp {
        fault: TcpFault::FailStop,
    };
    assert_eq!(
        (a.name.as_deref(), &a.from, &a.to, &a.networking, a.intercepted),
        (Some("a"), &p1.id(), &p2.id(), &fail_stop, true)
    );
    assert_eq!(b.name.as_deref(), Some("b"));
    assert_ne!(a.id, b.id);
    let locations: Vec<_> = compiled.locations().iter().map(|l| &l.id).collect();
    assert_eq!(locations, [&p1.id(), &p2.id()]);
    assert!(compiled.locations().iter().all(|l| l.members.is_none()));
    let a_id = a.id;
    let hook = Rc::new(RecordingHook {
        drop: vec![a_id],
        ..Default::default()
    });
    compiled.set_network_hook(hook.clone());

    let count = compiled.exhaustive(async || {
        in_send.send_many_unordered([1, 2]);
        let got: Vec<i32> = out.collect_sorted().await;
        assert_eq!(got, vec![100, 200]);
    });

    assert_eq!(*hook.instances.borrow(), count);
    // Every delivered message was received with the tag it was sent with.
    let sent: BTreeMap<u64, usize> = hook.sent.borrow().iter().map(|s| (s.3, s.0)).collect();
    let received: BTreeMap<u64, usize> =
        hook.received.borrow().iter().map(|r| (r.2, r.0)).collect();
    assert_eq!(sent, received);
    assert!(sent.values().all(|c| *c != a_id));
    assert!(!hook.outputs.borrow().is_empty());
    // Execution boundaries are reported in pairs.
    assert!(hook.execs.borrow().iter().all(|e| e.2));
    assert!(hook.execs.borrow().iter().any(|e| e.0 == p1.id()));
}

/// E5 meta slot: a PostNetwork pass wraps a broadcast's serialize to attach the item it is
/// serializing; one serialize per (member, item) feeds the demux, and the hook must see each
/// meta with its own message. A wrapped deserialize sees the receive's tag.
#[cfg(stageleft_runtime)]
#[test]
fn send_meta_pairs_with_its_own_message_across_a_broadcast() {
    let mut flow = FlowBuilder::new();
    let leader = flow.process::<()>();
    let workers = flow.cluster::<()>();

    let (in_send, input) = leader.sim_input::<i32, _, _>();
    let out = input
        .broadcast_closed(&workers, TCP.fail_stop().bincode().name("fanout"))
        .sim_cluster_output();

    let compiled = flow
        .sim()
        .with_cluster_size(&workers, 3)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            for_each_network(ir, |n| {
                let HydroNode::Network {
                    name,
                    serialize,
                    deserialize,
                    ..
                } = n
                else {
                    unreachable!()
                };
                if name.as_deref() != Some("fanout") {
                    return;
                }
                let NetworkSend::Custom {
                    serialize_fn: Some(ser),
                } = serialize
                else {
                    panic!("expected a bincode channel")
                };
                let root = crate::staging_util::get_this_crate();
                let orig = &ser.0;
                let wrapped: syn::Expr = syn::parse_quote! {
                    {
                        move |__item: (#root::__staged::location::MemberId<()>, i32)| {
                            // The meta is the destination member and the value, independently
                            // of the payload encoding.
                            let __to = #root::__staged::location::TaglessMemberId::get_raw_id(
                                &::core::clone::Clone::clone(&__item.0).into_tagless(),
                            );
                            #root::sim::network_hook::rt::set_send_meta(
                                #root::runtime_support::bincode::serialize(&(__to, __item.1))
                                    .unwrap(),
                            );
                            #root::sim::provenance::apply1(#orig, __item)
                        }
                    }
                };
                *ser = DebugExpr::from(wrapped);
                let NetworkRecv::Custom {
                    deserialize_fn: Some(deser),
                } = deserialize
                else {
                    panic!("expected a bincode channel")
                };
                let orig = &deser.0;
                let wrapped: syn::Expr = syn::parse_quote! {
                    {
                        move |__res| {
                            assert!(
                                #root::sim::network_hook::rt::recv_tag().is_some(),
                                "deserialize ran without its receive's tag"
                            );
                            #root::sim::provenance::apply1(#orig, __res)
                        }
                    }
                };
                *deser = DebugExpr::from(wrapped);
            });
        })
        .compiled();
    let w = compiled.locations().iter().find(|l| l.id == workers.id());
    assert_eq!(w.unwrap().members.as_deref(), Some(&[0u32, 1, 2][..]));

    let hook = Rc::new(RecordingHook::default());
    compiled.set_network_hook(hook.clone());
    compiled.exhaustive(async || {
        in_send.send(5);
        in_send.send(6);
        for member in 0..3 {
            let got: Vec<i32> = out.collect(member).await;
            assert_eq!(got, vec![5, 6]);
        }
    });

    let metas = hook.metas.borrow();
    assert!(!metas.is_empty());
    for (to_member, meta, payload) in metas.iter() {
        let (member, value): (u32, i32) = bincode::deserialize(meta).unwrap();
        let sent: i32 = bincode::deserialize(payload).unwrap();
        assert_eq!(
            Some(member),
            *to_member,
            "meta attached to another member's send"
        );
        assert_eq!(value, sent, "meta attached to another item's send");
    }
}

/// A second `set_send_meta` before its send fails loudly instead of mis-attributing.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "send-meta slot already occupied")]
fn send_meta_slot_rejects_two_serializations_before_a_send() {
    crate::sim::network_hook::rt::install(None);
    crate::sim::network_hook::rt::set_send_meta(vec![1]);
    crate::sim::network_hook::rt::set_send_meta(vec![2]);
}

/// E1 halts: a member the hook halts sends nothing more in the same instance.
#[cfg(stageleft_runtime)]
#[test]
fn halt_requested_by_hook_stops_further_sends() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();

    let (in_send, input) = p1.sim_input::<i32, _, _>();
    let out = input
        .send(&p2, TCP.fail_stop().bincode().name("ch"))
        .sim_output();

    let compiled = flow.sim().with_network_hook_points().compiled();
    let hook = Rc::new(RecordingHook {
        halt_after: Some((1, p1.id())),
        ..Default::default()
    });
    compiled.set_network_hook(hook.clone());

    compiled.exhaustive(async || {
        in_send.send(1);
        assert_eq!(out.next().await, 1);
        // `p1` is halted now: these inputs are never processed.
        in_send.send(2);
        in_send.send(3);
        out.assert_no_more().await;
    });

    assert_eq!(hook.sent.borrow().len(), 1);
    // The halted process ran no execution after its halt.
    let execs = hook.execs.borrow();
    let halted_in = hook.halted_in.borrow().unwrap();
    assert!(execs[halted_in + 1..].iter().all(|e| e.0 != p1.id()));
}

/// E2: passes run at their stage, in registration order, and can rewrite the program.
#[cfg(stageleft_runtime)]
#[test]
fn ir_passes_run_in_stage_and_registration_order() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();

    let (in_send, input) = p1.sim_input::<i32, _, _>();
    let out = input.send(&p2, TCP.fail_stop().bincode()).sim_output();

    let out_port = out.0;
    let order = Rc::new(RefCell::new(vec![]));
    let finalized = |ir: &mut [HydroRoot]| {
        let mut fin = vec![];
        for_each_network(ir, |n| {
            let HydroNode::Network { instantiate_fn, .. } = n else {
                unreachable!()
            };
            fin.push(matches!(
                instantiate_fn,
                crate::compile::ir::DebugInstantiate::Finalized(_)
            ));
        });
        fin
    };

    let (o1, o2, o3) = (order.clone(), order.clone(), order.clone());
    let compiled = flow
        .sim()
        .with_ir_pass(IrPassStage::PostNetwork, move |ir| {
            o1.borrow_mut().push(("post-1", finalized(ir)));
            // Rewrite: add 100 to everything the program outputs.
            let plus_100: syn::Expr = syn::parse_quote!(|x: i32| x + 100);
            for root in ir.iter_mut() {
                if let HydroRoot::SendExternal {
                    to_port_id, input, ..
                } = root
                    && *to_port_id == out_port
                {
                    let metadata = input.metadata().clone();
                    let inner = std::mem::replace(&mut **input, HydroNode::Placeholder);
                    **input = HydroNode::Map {
                        f: plus_100.clone().into(),
                        input: Box::new(inner),
                        metadata,
                    };
                }
            }
        })
        .with_ir_pass(IrPassStage::PreNetwork, move |ir| {
            o2.borrow_mut().push(("pre", finalized(ir)));
        })
        .with_ir_pass(IrPassStage::PostNetwork, move |ir| {
            o3.borrow_mut().push(("post-2", finalized(ir)));
        })
        .compiled();

    assert_eq!(
        *order.borrow(),
        vec![
            ("pre", vec![false]),
            ("post-1", vec![true]),
            ("post-2", vec![true]),
        ]
    );

    compiled.exhaustive(async || {
        in_send.send(1);
        assert_eq!(out.next().await, 101);
    });
}

/// Hook points are opt-in at codegen: a flow that does not call `with_network_hook_points()`
/// generates no call into the network-hook runtime (so, since `network_hooks` is the only codegen
/// switch, its simulation is generated exactly as with the `hydrofrac` feature off), and one that
/// does gets the send, receive and output interception plus the dylib's hook installer.
#[cfg(stageleft_runtime)]
#[test]
fn hook_points_are_opt_in_at_codegen() {
    fn source(hooks: bool) -> String {
        let mut flow = FlowBuilder::new();
        let p1 = flow.process::<()>();
        let p2 = flow.process::<()>();
        let (in_send, input) = p1.sim_input::<i32, _, _>();
        let out = input
            .send(&p2, TCP.fail_stop().bincode().name("ch"))
            .sim_output();
        let sim = flow.sim();
        let sim = if hooks {
            sim.with_network_hook_points()
        } else {
            sim
        };
        let compiled = sim.compiled();
        let source = crate::sim::graph::LAST_SIM_SOURCE
            .with(|last| last.borrow_mut().take())
            .expect("compiling a simulation records its source");
        compiled.exhaustive(async || {
            in_send.send(1);
            assert_eq!(out.next().await, 1);
        });
        source
    }

    let plain = source(false);
    assert!(
        !plain.contains("network_hook"),
        "a flow without hook points must not call into the network-hook runtime"
    );
    assert!(!plain.contains("__hydro_set_network_hook"));
    assert_eq!(plain, source(false), "codegen is deterministic");

    let hooked = source(true);
    for call in [
        "network_hook::rt::on_send",
        "network_hook::rt::on_recv",
        "network_hook::rt::on_output",
        "__hydro_set_network_hook",
    ] {
        assert!(hooked.contains(call), "hook points must emit `{call}`");
    }
}

/// A network hook that panics when it sees a message whose payload is `7` (as bincode).
#[cfg(stageleft_runtime)]
struct PanickingHook;

#[cfg(stageleft_runtime)]
impl SimNetworkHook for PanickingHook {
    fn on_send(
        &self,
        _channel: usize,
        _from_member: Option<u32>,
        _to_member: Option<u32>,
        payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        if payload == 7u32.to_le_bytes() {
            panic!("hook panicked on a send");
        }
        Some(1)
    }
}

/// A host hook's panic inside a callback the dylib makes mid-tick reaches the host with its
/// original payload, instead of unwinding into the dylib's guard.
#[cfg(stageleft_runtime)]
#[test]
fn a_hook_panic_inside_a_dylib_callback_is_caught_on_the_host() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (in_send, input) = p1.sim_input::<u32, _, _>();
    let out = input.send(&p2, TCP.fail_stop().bincode()).sim_output();
    let sim = flow.sim().with_network_hook_points().compiled();
    sim.set_network_hook(Rc::new(PanickingHook));

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sim.exhaustive(async || {
            in_send.send(7);
            out.assert_yields_only(Vec::<u32>::new()).await;
        });
    }));
    let payload = caught.expect_err("the hook's panic must propagate to the host");
    let msg = payload.downcast_ref::<&str>().copied().unwrap_or_default();
    assert!(msg.contains("hook panicked on a send"), "{msg}");

    // Messages the hook accepts still flow, in a fresh search on the same compiled simulation.
    sim.exhaustive(async || {
        in_send.send(1);
        out.assert_yields_only([1]).await;
    });
}
