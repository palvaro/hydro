//! Tests of [`reliable_send`]: delivery over a lossy network, in the simulator and under
//! hydrofrac.
//!
//! | test | program | faults | outcome |
//! |---|---|---|---|
//! | [`delivers_each_value_once_without_faults`] | `reliable_send` | none (exhaustive sim) | each value delivered once |
//! | [`reliable_send_delivers_despite_losses`] | `reliable_send`, one value, a resend before the heal | omissions, crashes | `delivery` holds |
//! | [`reliable_send_delivers_three_values_despite_losses`] | `reliable_send`, three values, resends after the heal | omissions, crashes | `delivery` holds |
//! | [`plain_send_loses_a_message`] | `send` | omissions, crashes | `delivery` broken by one lost message |
//!
//! The rule `delivery` is validity, as in `ec_implies_checks.rs`: **every value a correct
//! sender sends, a correct receiver delivers** (`ASide::Correct`, `BSide::AllCorrect`). A
//! crashed sender owes nothing and a crashed receiver is owed nothing, so the rule asks only
//! what the protocol promises (fair loss, no crashes).
//!
//! The hydrofrac harness is a sender's life: in the fault phase it sends its values, its timer
//! firing after each of the first few, and the network may drop any message. After the network
//! heals the timer keeps firing, once per round, each round settling before the next
//! ([`EcCheck::run_then_recover`]).
//!
//! # What the checks cost, and why two harnesses
//!
//! hydrofrac bounds crashes but not omissions, and here it can prune almost nothing, so a check
//! costs about `2^k` fault sets for the `k` lossy messages sent before the heal (times the crash
//! points):
//! - Every resend passes two fragile steps of the sender's tick, `filter_not_in(acked)` (a
//!   negation) and `filter_if` on the timer (trigger meets state). A value whose first copy is
//!   lost has only fragile derivations, which never prune, so every fault set that drops a first
//!   copy is explored.
//! - The program has fragile steps, so a certificate also runs the Claim B sweep: every fault set
//!   within the crash bound, in every schedule (docs/design/hydrofrac-soundness.md §13).
//!
//! A fault-phase tick resends every outstanding value, and each resend is acknowledged, so it
//! adds about two lossy messages per outstanding value. Measured (crashes checked): one value and
//! one tick, 112 sets (1 s); three values and no tick, 576 sets (2⁶ sets of omissions × 9
//! crash options), 74 s; two values and a tick after each passed the hypothesis budget (1,024
//! sets) without a verdict, as did two values and one tick. So one check loses a resend
//! ([`reliable_send_delivers_despite_losses`]), and one interleaves three values but resends only
//! after the heal ([`reliable_send_delivers_three_values_despite_losses`]). Losing resends of
//! several values before the heal is not checked.

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

/// The values the sender sends under hydrofrac, with a tick after each (see the
/// [module docs](self) for why one).
const CHECKED_VALUES: [u32; 1] = [1];

/// The values the sender sends under hydrofrac with no tick before the heal.
const CHECKED_VALUES_NO_TICK: [u32; 3] = [1, 2, 3];

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
/// `send`: [`CHECKED_VALUES`] with a tick after each.
fn check(reliable: bool) -> Results {
    check_values(reliable, &CHECKED_VALUES, true)
}

/// The harness on `values`, with a tick after each if `ticks_before_heal`, then
/// [`RECOVERY_TICKS`] rounds of one tick each.
fn check_values(reliable: bool, values: &[u32], ticks_before_heal: bool) -> Results {
    let mut check = EcCheck::new();
    let (sim, vals, ticks) = program(&mut check, reliable);
    let start = std::time::Instant::now();
    let results = check.run_then_recover(
        &sim,
        async || {
            for &v in values {
                vals.send(v);
                if ticks_before_heal {
                    ticks.send(());
                }
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
        "reliable: {reliable}, values: {values:?}, ticks before the heal: {ticks_before_heal}, \
         {:.1}s\n{results}",
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

/// **Three values get through despite losses.** The sender sends three values with no tick
/// before the heal, so any of their first copies and acknowledgements may be lost, in any
/// interleaving, alongside any crash; the resends after the heal deliver what was lost. (No
/// resend is lost: see the [module docs](self) for the cost of losing those.)
#[test]
fn reliable_send_delivers_three_values_despite_losses() {
    check_values(true, &CHECKED_VALUES_NO_TICK, false)
        .label("delivery")
        .assert_holds();
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
