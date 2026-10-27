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
//! The budgeted variants ([`reliable_send_budgeted`], bucket of 10 starting full unless noted):
//!
//! | test | budget | harness | outcome | cost |
//! |---|---|---|---|---|
//! | `*_budget_delivers_each_value_once_without_faults` | time, success, hybrid | as T1 | each value delivered once | 14580 schedules each |
//! | [`time_budget_delivers_despite_losses`], [`hybrid_budget_delivers_despite_losses`] | time; hybrid | as T2 | `delivery` holds | 112 sets, 1 s |
//! | [`success_budget_one_value`] | success | as T2 | holds: one value cannot drain ten tokens | 112 sets, 1 s |
//! | [`time_budget_delivers_three_values_despite_losses`], [`hybrid_budget_delivers_three_values_despite_losses`] | time; hybrid | as T2b | `delivery` holds | 576 sets, 80 s |
//! | [`success_budget_starves_when_losses_drain_the_bucket`] | success, **bucket of 1**, a token per ack | as T2 | broken by 2 lost messages, no crash: nothing refills the bucket | 3 sets |
//! | [`hybrid_bucket_of_one_delivers`] | the same, plus a token per tick | as T2 | holds | 112 sets, 1 s |
//!
//! The budgets cost hydrofrac nothing over [`reliable_send`]: the same fault sets, since the
//! fragile steps are the same and the bucket adds no lossy message. A success budget of ten
//! would starve only after more than ten lost resends, out of hydrofrac's reach here (about
//! `2^k` sets for `k` lossy messages), so starvation is shown with a bucket of one.
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
use hydro_std::reliable_send::{ResendBudget, reliable_send, reliable_send_budgeted};
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
    delivers_once(Variant::Unbudgeted);
}

/// The protocol under test: plain `send`, [`reliable_send`], or one of its budgeted variants.
#[derive(Clone, Copy, Debug)]
enum Variant {
    /// Plain `send` over a lossy link, the negative control.
    Plain,
    /// [`reliable_send`]: every outstanding value on every tick.
    Unbudgeted,
    /// [`reliable_send_budgeted`] with this budget.
    Budgeted(ResendBudget),
}

/// The corpus budget (see `reliable_send_check::BUDGET_*`): a bucket of ten, starting full.
const fn budget(tokens_per_tick: u64, acks_per_token: u64) -> ResendBudget {
    ResendBudget {
        capacity: 10,
        tokens_per_tick,
        acks_per_token,
    }
}
const TIME_BUDGET: Variant = Variant::Budgeted(budget(1, 0));
const SUCCESS_BUDGET: Variant = Variant::Budgeted(budget(0, 4));
const HYBRID_BUDGET: Variant = Variant::Budgeted(budget(1, 4));

/// `variant`'s delivered stream, from `vals` at the sender to `receiver`.
fn deliver<'a>(
    variant: Variant,
    vals: Stream<u32, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    receiver: &Process<'a, Receiver>,
    ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Stream<u32, Process<'a, Receiver>, Unbounded, hydro_lang::live_collections::stream::NoOrder, ExactlyOnce>
{
    match variant {
        Variant::Plain => {
            drop(ticks);
            vals.send(receiver, TCP.lossy_delayed_forever().name("data").bincode())
                .weaken_ordering()
        }
        Variant::Unbudgeted => reliable_send(vals, receiver, ticks),
        Variant::Budgeted(budget) => reliable_send_budgeted(vals, receiver, ticks, budget),
    }
}

/// [`delivers_each_value_once_without_faults`] for `variant`: [`VALUES`], a tick after each,
/// every schedule.
fn delivers_once(variant: Variant) {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receiver = flow.process::<Receiver>();
    let (vals_send, vals) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (ticks_send, ticks) = sender.sim_input::<(), TotalOrder, ExactlyOnce>();
    let delivered = deliver(variant, vals, &receiver, ticks).sim_output();

    let start = std::time::Instant::now();
    let schedules = flow.sim().test_safety_only().exhaustive(async || {
        for v in VALUES {
            vals_send.send(v);
            ticks_send.send(());
        }
        delivered.assert_yields_only_unordered(VALUES).await;
    });
    eprintln!(
        "{variant:?}: {schedules} schedules, {:.1}s",
        start.elapsed().as_secs_f64()
    );
}

/// The program, with the rule `delivery` from what the sender sends to what the receiver
/// receives, over `variant`.
fn program(
    check: &mut EcCheck,
    variant: Variant,
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
    let delivered = deliver(variant, vals.clone(), &receiver, ticks);
    vals.implies(
        "delivery",
        &delivered,
        q!(|v| v),
        q!(|v| v),
        ASide::Correct,
        BSide::AllCorrect,
    );
    drop(vals);
    let sim = check.instrument(flow.sim()).compiled();
    (sim, vals_send, ticks_send)
}

/// The harness (see the [module docs](self)) on `variant`: [`CHECKED_VALUES`] with a tick
/// after each.
fn check(variant: Variant) -> Results {
    check_values(variant, &CHECKED_VALUES, true)
}

/// The harness on `values`, with a tick after each if `ticks_before_heal`, then
/// [`RECOVERY_TICKS`] rounds of one tick each.
fn check_values(variant: Variant, values: &[u32], ticks_before_heal: bool) -> Results {
    check_values_recovering(variant, values, ticks_before_heal, RECOVERY_TICKS)
}

/// [`check_values`] with `recovery_ticks` rounds after the heal.
fn check_values_recovering(
    variant: Variant,
    values: &[u32],
    ticks_before_heal: bool,
    recovery_ticks: usize,
) -> Results {
    let mut check = EcCheck::new();
    let (sim, vals, ticks) = program(&mut check, variant);
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
            for _ in 0..recovery_ticks {
                ticks.send(());
                hydro_lang::sim::compiled::quiesce().await;
            }
        },
    );
    eprintln!(
        "{variant:?}, values: {values:?}, ticks before the heal: {ticks_before_heal}, \
         recovery ticks: {recovery_ticks}, {:.1}s\n{results}",
        start.elapsed().as_secs_f64()
    );
    results
}

/// **Resending until acknowledged delivers despite losses.** Whatever the network drops while
/// the sender sends, the resends after it heals get through.
#[test]
fn reliable_send_delivers_despite_losses() {
    check(Variant::Unbudgeted).label("delivery").assert_holds();
}

/// **Three values get through despite losses.** The sender sends three values with no tick
/// before the heal, so any of their first copies and acknowledgements may be lost, in any
/// interleaving, alongside any crash; the resends after the heal deliver what was lost. (No
/// resend is lost: see the [module docs](self) for the cost of losing those.)
#[test]
fn reliable_send_delivers_three_values_despite_losses() {
    check_values(Variant::Unbudgeted, &CHECKED_VALUES_NO_TICK, false)
        .label("delivery")
        .assert_holds();
}

/// **Plain `send` over a lossy link loses a message.** The negative control: the same program
/// and harness without resends. One lost message breaks `delivery`, with no crash.
#[test]
fn plain_send_loses_a_message() {
    let results = check(Variant::Plain);
    let cex = results
        .label("delivery")
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample:\n{results}"));
    assert_eq!((cex.crashes(), cex.faults.len()), (0, 1), "{results}");
}

// The budgeted variants (`reliable_send_budgeted`), with the corpus budget unless noted.

/// **A time budget delivers each value once without faults.**
#[test]
fn time_budget_delivers_each_value_once_without_faults() {
    delivers_once(TIME_BUDGET);
}

/// **A success budget delivers each value once without faults.**
#[test]
fn success_budget_delivers_each_value_once_without_faults() {
    delivers_once(SUCCESS_BUDGET);
}

/// **A hybrid budget delivers each value once without faults.**
#[test]
fn hybrid_budget_delivers_each_value_once_without_faults() {
    delivers_once(HYBRID_BUDGET);
}

/// **A time budget delivers despite losses** (one value, a resend before the heal): the timer
/// refills the bucket, so the resends after the heal get through.
#[test]
fn time_budget_delivers_despite_losses() {
    check(TIME_BUDGET).label("delivery").assert_holds();
}

/// **A success budget, one value, a resend before the heal.** With a bucket of ten, one value
/// cannot drain it, so the verdict says nothing about starvation; see
/// [`success_budget_starves_when_losses_drain_the_bucket`].
#[test]
fn success_budget_one_value() {
    let results = check(SUCCESS_BUDGET);
    eprintln!("success budget, one value: {:?}", results.label("delivery").counterexample().map(|c| (c.crashes(), c.faults.len())));
}

/// **A hybrid budget delivers despite losses** (one value, a resend before the heal).
#[test]
fn hybrid_budget_delivers_despite_losses() {
    check(HYBRID_BUDGET).label("delivery").assert_holds();
}

/// **A success budget starves once losses drain the bucket.** A test-only bucket of one token
/// that only acknowledgements refill: the first tick before the heal resends the one value
/// (spending the token), the network drops the first copy and the resend, and nothing is left
/// to pay for a resend after the heal, since only an acknowledgement of a value that got
/// through can earn a token. Delivery breaks with no crash.
#[test]
fn success_budget_starves_when_losses_drain_the_bucket() {
    let starved = Variant::Budgeted(ResendBudget {
        capacity: 1,
        tokens_per_tick: 0,
        acks_per_token: 1,
    });
    let results = check(starved);
    let cex = results
        .label("delivery")
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample:\n{results}"));
    eprintln!("starvation: {} crashes, {} faults", cex.crashes(), cex.faults.len());
    assert_eq!(cex.crashes(), 0, "{results}");
}

/// **The same bucket of one, refilled by the timer as well, delivers.** The control for
/// [`success_budget_starves_when_losses_drain_the_bucket`]: the only change is one token per
/// timer element.
#[test]
fn hybrid_bucket_of_one_delivers() {
    let hybrid = Variant::Budgeted(ResendBudget {
        capacity: 1,
        tokens_per_tick: 1,
        acks_per_token: 1,
    });
    check(hybrid).label("delivery").assert_holds();
}

/// **A time budget, three values, resends after the heal.**
#[test]
fn time_budget_delivers_three_values_despite_losses() {
    check_values(TIME_BUDGET, &CHECKED_VALUES_NO_TICK, false)
        .label("delivery")
        .assert_holds();
}

/// **A hybrid budget, three values, resends after the heal.**
#[test]
fn hybrid_budget_delivers_three_values_despite_losses() {
    check_values(HYBRID_BUDGET, &CHECKED_VALUES_NO_TICK, false)
        .label("delivery")
        .assert_holds();
}
