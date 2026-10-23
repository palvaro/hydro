//! The hydrofrac brute-force cross-check (docs/design/hydrofrac-lite.md §7, "Honesty check") on
//! corpus programs: every admissible fault set of at most two faults is run without lineage
//! pruning, over every schedule, and `EcCheck`'s verdicts must agree with what the enumeration
//! finds. These programs batch in ticks, so unlike the inline programs in
//! `hydrofrac::check::brute_tests` their fault sets run over many schedules.
//!
//! The tables print with `--nocapture`.
//!
//! `reliable_broadcast::broadcast` *with* the echo is not here: one exhaustive run of it over
//! three members exceeds 160k schedules, and the check holds every schedule's record until the
//! run ends, so even single faults do not finish (the process was killed after 31 minutes). The
//! echo logic is cross-checked on the inline echo broadcasts in `hydrofrac::check::brute_tests`
//! and on `hydro_std`'s `reliable_broadcast_closed` below.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;
use hydrofrac::check::CrossCheck;

use super::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};

/// The largest fault set the enumeration covers.
const MAX_FAULTS: usize = 2;

fn agree(name: &str, cross: &CrossCheck) {
    eprintln!("=== {name}\n{cross}");
    cross.assert_agrees();
}

/// `reliable_broadcast::broadcast` to three members, without the echo.
fn plain_reliable_broadcast() -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        false,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(Payload(42));
        },
        MAX_FAULTS,
    )
}

#[test]
fn brute_force_agrees_on_a_plain_broadcast() {
    let cross = plain_reliable_broadcast();
    agree("reliable_broadcast::broadcast (plain)", &cross);
    // Without the echo, a broadcaster crash mid fan-out breaks every searched label.
    assert!(!cross.brute.labels.is_empty(), "{cross}");
    for b in &cross.brute.labels {
        assert_eq!(b.min_crashes(), Some(1), "{}: {cross}", b.name);
    }
}

/// `hydro_std`'s `broadcast_live_from_process`: a process broadcasts to the cluster over its
/// live membership.
#[test]
fn brute_force_agrees_on_broadcast_live() {
    use hydro_std::ec_inference_demos::broadcast_live::broadcast_live_from_process;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = broadcast_live_from_process(input, &members, TCP.fail_stop().bincode())
        .sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    );
    agree("broadcast_live_from_process", &cross);
}

/// `hydro_std`'s `reliable_broadcast_closed`: echo-based reliable broadcast over `fail_stop`.
#[test]
fn brute_force_agrees_on_reliable_broadcast_closed() {
    use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = reliable_broadcast_closed(input, &members).sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    );
    agree("reliable_broadcast_closed", &cross);
}
