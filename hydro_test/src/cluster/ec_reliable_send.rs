//! Tests of [`reliable_send`]: delivery over a lossy network, in the simulator and under
//! hydrofrac.
//!
//! | test | program | faults | outcome |
//! |---|---|---|---|
//! | [`delivers_each_value_once_without_faults`] | `reliable_send` | none (exhaustive sim) | each value delivered once |
//! | [`reliable_send_delivers_despite_losses`] | `reliable_send` | omissions, crashes | `delivery` holds |
//! | [`plain_send_loses_a_message`] | `send` | omissions, crashes | `delivery` broken by one lost message |
//!
//! The rule `delivery` is validity, as in `ec_implies_checks.rs`: **every value a correct
//! sender sends, a correct receiver delivers** (`ASide::Correct`, `BSide::AllCorrect`). A
//! crashed sender owes nothing and a crashed receiver is owed nothing, so the rule asks only
//! what the protocol promises (fair loss, no crashes).
//!
//! The hydrofrac harness is a sender's life: in the fault phase it sends its values while its
//! timer fires between them, and the network may drop any message. After the network heals the
//! timer keeps firing, once per round, each round settling before the next
//! ([`EcCheck::run_then_recover`]).

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::SimSender;
use hydro_lang::sim::compiled::CompiledSim;
use hydro_std::reliable_send::reliable_send;
use hydrofrac::{ASide, BSide, EcCheck, Implies, Results};

/// The process that sends.
pub struct Sender;

/// The process that delivers.
pub struct Receiver;

/// The values the sender sends in the simulator test.
const VALUES: [u32; 2] = [1, 2];

/// The values the sender sends under hydrofrac. One: with two (`[1, 2]`, a tick after each),
/// the search for `delivery` ran out of its hypothesis budget (1024 hypotheses, 12,307
/// schedules, 211 s) without a verdict.
const CHECKED_VALUES: [u32; 1] = [1];

/// Timer ticks after the network heals, each in a round of its own.
const RECOVERY_TICKS: usize = 2;

/// **Without faults, each value is delivered exactly once.** The sender sends [`VALUES`], its
/// timer firing after each, over every schedule of batching and delivery: retransmissions of a
/// value that already arrived are not delivered again. (Three values and three ticks ran past
/// five minutes of exhaustive exploration without finishing.)
#[test]
fn delivers_each_value_once_without_faults() {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receiver = flow.process::<Receiver>();
    let (vals_send, vals) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (ticks_send, ticks) = sender.sim_input::<(), TotalOrder, ExactlyOnce>();
    let delivered = reliable_send(vals, &receiver, ticks).sim_output();

    let schedules = flow.sim().test_safety_only().exhaustive(async || {
        for v in VALUES {
            vals_send.send(v);
            ticks_send.send(());
        }
        delivered.assert_yields_only_unordered(VALUES).await;
    });
    eprintln!("{schedules} schedules");
}

/// The program, with the rule `delivery` from what the sender sends to what the receiver
/// receives: over [`reliable_send`], or over plain `send` (`reliable == false`).
fn program(
    check: &mut EcCheck,
    reliable: bool,
) -> (
    CompiledSim,
    SimSender<u32, TotalOrder, ExactlyOnce>,
    SimSender<(), TotalOrder, ExactlyOnce>,
) {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receiver = flow.process::<Receiver>();
    let (vals_send, vals) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (ticks_send, ticks) = sender.sim_input::<(), TotalOrder, ExactlyOnce>();
    if reliable {
        let delivered = reliable_send(vals.clone(), &receiver, ticks);
        vals.implies(
            "delivery",
            &delivered,
            q!(|v| v),
            q!(|v| v),
            ASide::Correct,
            BSide::AllCorrect,
        );
    } else {
        drop(ticks);
        let delivered = vals.clone().send(
            &receiver,
            TCP.lossy_delayed_forever().name("data").bincode(),
        );
        vals.implies(
            "delivery",
            &delivered,
            q!(|v| v),
            q!(|v| v),
            ASide::Correct,
            BSide::AllCorrect,
        );
    }
    drop(vals);
    let sim = check.instrument(flow.sim()).compiled();
    (sim, vals_send, ticks_send)
}

/// The harness (see the [module docs](self)) on the program over [`reliable_send`] or plain
/// `send`.
fn check(reliable: bool) -> Results {
    let mut check = EcCheck::new();
    let (sim, vals, ticks) = program(&mut check, reliable);
    let start = std::time::Instant::now();
    let results = check.run_then_recover(
        &sim,
        async || {
            for v in CHECKED_VALUES {
                vals.send(v);
                ticks.send(());
            }
        },
        async || {
            for _ in 0..RECOVERY_TICKS {
                ticks.send(());
                hydro_lang::sim::compiled::quiesce().await;
            }
        },
    );
    eprintln!(
        "reliable: {reliable}, {:.1}s\n{results}",
        start.elapsed().as_secs_f64()
    );
    results
}

/// **Resending until acknowledged delivers despite losses.** Whatever the network drops while
/// the sender sends, the resends after it heals get through.
#[test]
fn reliable_send_delivers_despite_losses() {
    check(true).label("delivery").assert_holds();
}

/// **Plain `send` over a lossy link loses a message.** The negative control: the same program
/// and harness without resends. One lost message breaks `delivery`, with no crash.
#[test]
fn plain_send_loses_a_message() {
    let results = check(false);
    let cex = results
        .label("delivery")
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample:\n{results}"));
    assert_eq!((cex.crashes(), cex.faults.len()), (0, 1), "{results}");
}
