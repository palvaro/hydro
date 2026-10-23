//! A panic raised by program code inside the simulation dylib must reach the host as an ordinary
//! Rust panic, with its message, so `catch_unwind` (and `#[should_panic]`) work around a search.

use stageleft::q;

use crate::location::Location;
use crate::prelude::FlowBuilder;

/// Compiles a process that echoes its input and panics on `7`.
#[cfg(stageleft_runtime)]
fn panicking_echo() -> (
    crate::sim::compiled::CompiledSim,
    crate::sim::SimSender<
        u32,
        crate::live_collections::stream::TotalOrder,
        crate::live_collections::stream::ExactlyOnce,
    >,
    crate::sim::SimReceiver<
        u32,
        crate::live_collections::stream::TotalOrder,
        crate::live_collections::stream::ExactlyOnce,
    >,
) {
    let mut flow = FlowBuilder::new();
    let node = flow.process::<()>();
    let (in_send, input) = node.sim_input::<u32, _, _>();
    let out_recv = input
        .map(q!(|x: u32| {
            if x == 7 {
                panic!("program panicked on {}", x);
            }
            x
        }))
        .sim_output();
    (flow.sim().compiled(), in_send, out_recv)
}

/// Extracts the message of a caught panic.
#[cfg(stageleft_runtime)]
fn message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else {
        "<non-string payload>".to_owned()
    }
}

#[test]
fn a_program_panic_is_caught_on_the_host() {
    let (sim, in_send, out_recv) = panicking_echo();

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sim.exhaustive(async || {
            in_send.send(7);
            out_recv.assert_yields_only(Vec::<u32>::new()).await;
        });
    }));
    let payload = caught.expect_err("the program's panic must propagate to the host");
    let msg = message(&*payload);
    assert!(msg.contains("program panicked on 7"), "{msg}");

    // The compiled simulation is still usable afterwards.
    let runs = sim.exhaustive(async || {
        in_send.send(1);
        out_recv.assert_yields_only([1]).await;
    });
    assert!(runs >= 1);
}

#[test]
fn a_program_panic_is_caught_under_fuzzing() {
    let (sim, in_send, out_recv) = panicking_echo();

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sim.fuzz_bounded(4, async || {
            in_send.send(7);
            out_recv.assert_yields_only(Vec::<u32>::new()).await;
        });
    }));
    let payload = caught.expect_err("the program's panic must propagate to the host");
    let msg = message(&*payload);
    assert!(msg.contains("program panicked on 7"), "{msg}");
}

#[test]
#[should_panic(expected = "program panicked on 7")]
fn a_program_panic_fails_the_test_with_its_message() {
    let (sim, in_send, out_recv) = panicking_echo();
    sim.exhaustive(async || {
        in_send.send(7);
        out_recv.assert_yields_only(Vec::<u32>::new()).await;
    });
}

/// A network hook that panics when it sees a message whose payload is `7` (as bincode).
#[cfg(all(stageleft_runtime, feature = "hydrofrac"))]
struct PanickingHook;

#[cfg(all(stageleft_runtime, feature = "hydrofrac"))]
impl crate::sim::network_hook::SimNetworkHook for PanickingHook {
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
#[cfg(all(stageleft_runtime, feature = "hydrofrac"))]
#[test]
fn a_hook_panic_inside_a_dylib_callback_is_caught_on_the_host() {
    use crate::networking::TCP;

    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (in_send, input) = p1.sim_input::<u32, _, _>();
    let out = input.send(&p2, TCP.fail_stop().bincode()).sim_output();
    let sim = flow.sim().with_network_hook_points().compiled();
    sim.set_network_hook(std::rc::Rc::new(PanickingHook));

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sim.exhaustive(async || {
            in_send.send(7);
            out.assert_yields_only(Vec::<u32>::new()).await;
        });
    }));
    let payload = caught.expect_err("the hook's panic must propagate to the host");
    let msg = message(&*payload);
    assert!(msg.contains("hook panicked on a send"), "{msg}");

    // Messages the hook accepts still flow, in a fresh search on the same compiled simulation.
    sim.exhaustive(async || {
        in_send.send(1);
        out.assert_yields_only([1]).await;
    });
}
