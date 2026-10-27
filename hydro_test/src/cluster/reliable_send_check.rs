//! Cress on [`hydro_std::reliable_send`]: does delay alone make reliable delivery do work its
//! input did not require, and where?
//!
//! [`reliable_send_pair`] is one sender and one receiver joined by `reliable_send`, with the
//! sender's resend timer as a parameter, as a deployment wires it (`source_interval`) and as
//! Cress feeds it (one element per round). The sender numbers its input, so each delivery names
//! the request it answers. Cress holds each of the protocol's three batches in
//! turn, the sender's new values, its acknowledgements, and its timer interrupts, and reads the
//! sender's and receiver's messages against the unheld run.
//!
//! In the simulator nothing is lost on a lossy channel: only fault injection (hydrofrac) drops
//! messages. So every resend Cress counts is caused by delay, not by loss.
//!
//! Run the attribute's configuration with `./scripts/cress hydro_test reliable_send_pair`, or
//! the hand-wired checks with
//! `cargo test -p hydro_test --features cress --lib reliable_send_check`.

#[cfg(feature = "cress")]
use cress::SimOutputs;
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydro_std::reliable_send::{
    ResendBudget, reliable_send, reliable_send_budgeted, reliable_send_hybrid_budget,
    reliable_send_success_budget, reliable_send_time_budget,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The process that sends.
pub struct Sender;

/// The process that delivers.
pub struct Receiver;

/// A value with the sender's sequence number for it: its position in the sender's input. Two
/// values with the same body are still two messages, and a load generator can match each
/// delivery to the request it offered.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Numbered<T> {
    pub id: u64,
    pub body: T,
}

/// What the receiver delivers: every value the sender was given, once each.
#[cfg_attr(feature = "cress", derive(SimOutputs))]
pub struct ReliableSendOutputs<'a, T> {
    pub delivered: Stream<Numbered<T>, Process<'a, Receiver>, Unbounded, NoOrder, ExactlyOnce>,
}

/// What the Sedge overload harness watches: deliveries by sequence number. The sender numbers its
/// input in arrival order, which is the order the harness's generator offers it, so a delivery's
/// `id` is the generator's sequence number for the value. `reliable_send` never gives up on a
/// value, so nothing is abandoned, and it exports no counters: what each node puts on the wire,
/// resends included, is measured from outside it (Sedge's `/proc` sampling of the containers'
/// kernel writes, `wchar_kb_*` in the traces).
#[cfg(feature = "sedge")]
impl<'a, T> sedge::Observed<'a> for ReliableSendOutputs<'a, T>
where
    T: Serialize + DeserializeOwned + 'a,
{
    fn observe(self, obs: &mut sedge::Observer<'a>) {
        obs.completions(self.delivered.map(q!(|n| n.id)));
    }
}

/// `values` sent from `sender` to `receiver` with [`reliable_send`], numbered in the order the
/// sender receives them. The sender resends everything unacknowledged on each element of
/// `resend_ticks`, its timer interrupt: a deployment wires `source_interval(period)` into it and a
/// simulation feeds it by hand.
///
/// Both of `reliable_send`'s channels are `lossy_delayed_forever`, which the simulator accepts only
/// with `test_safety_only`; it never drops on them, so every resend Cress counts is caused by
/// delay.
///
/// The configuration `two_per_round` offers two new values per round for the first 125 rounds of
/// a 1000-round run, with the timer firing every round throughout, so that holds longer than the
/// input show whether extra work stops when the input does or keeps rising with the delay. Run it
/// with `./scripts/cress hydro_test reliable_send_pair`.
///
/// The `overload_check` attribute deploys the same configuration (same name, so that Sedge reads
/// Cress's classification of `delivered`) under the Sedge overload harness:
/// `./scripts/sedge hydro_test reliable_send_pair --factors 2,4,8,16`, after the Cress run.
///
/// - **Capacity from outside.** Only the receiver, the resource under test, gets a CPU quota: 1 %
///   of a core (1 ms of CPU every 100 ms). The sender stands for the callers and gets none. The
///   program has no capacity code; the receiver's per-arrival work is real (decode, the `unique`
///   check, and the acknowledgement, which is the value itself).
/// - **Load on the sender.** The generator feeds `values` open loop with 4 KiB bodies
///   ([`super::rpc_retry::Payload`], distinct per sequence number, since the value is its own
///   identity).
/// - **The resend timer** fires every second, TCP's initial retransmission timeout (RFC 6298), and
///   well above the receiver's round trip at baseline, so that few values are resent before
///   their acknowledgement can return. Each tick resends everything outstanding.
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = two_per_round,
    T = super::rpc_retry::Payload<4096>,
    load = values,
    mu = 500,
    cpus(Sender = none, Receiver = 0.01),
    cpu_period_us = 100_000,
    timers(resend_ticks = std::time::Duration::from_secs(1)),
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = two_per_round,
    T = u64,
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
pub fn reliable_send_pair<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> ReliableSendOutputs<'a, T>
where
    T: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + 'a,
{
    let numbered = values.enumerate().map(q!(|(id, body)| Numbered {
        id: id as u64,
        body
    }));
    ReliableSendOutputs {
        delivered: reliable_send(numbered, receiver, resend_ticks),
    }
}

/// The resend budget of the budgeted variants below, the same for all three so that they differ
/// only in where tokens come from: a bucket of ten, starting full; the timer variant grants one
/// token per timer element, the success variant one per four acknowledgements that clear a
/// value, and the hybrid both. These are the retry-governance corpus's constants
/// (`witnesses/blind_budget/`: bucket 10, one per clock element, one per four useful replies;
/// its `success_budget` attribute uses five, its module docs and `hybrid_budget` four).
///
/// Against the cress workload (two new values and one timer element per round) the timer's
/// grant is half the input rate. On a deployment the timer fires once a second, so a time
/// budget allows one resend per second after the first ten.
pub const BUDGET_CAPACITY: u64 = 10;
/// Tokens per timer element (time and hybrid budgets).
pub const BUDGET_TOKENS_PER_TICK: u64 = 1;
/// Useful acknowledgements per token (success and hybrid budgets).
pub const BUDGET_ACKS_PER_TOKEN: u64 = 4;

/// The sender's input, numbered in the order the sender receives it (see [`Numbered`]).
fn numbered<'a, T>(
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Stream<Numbered<T>, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>
where
    T: 'a,
{
    values.enumerate().map(q!(|(id, body)| Numbered {
        id: id as u64,
        body
    }))
}

/// [`reliable_send_pair`] over [`reliable_send_time_budget`]: each resend spends a token, and
/// the timer grants [`BUDGET_TOKENS_PER_TICK`] per element, up to [`BUDGET_CAPACITY`]. The same
/// Cress and Sedge configurations as [`reliable_send_pair`].
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = two_per_round,
    T = super::rpc_retry::Payload<4096>,
    load = values,
    mu = 500,
    cpus(Sender = none, Receiver = 0.01),
    cpu_period_us = 100_000,
    timers(resend_ticks = std::time::Duration::from_secs(1)),
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = two_per_round,
    T = u64,
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
pub fn reliable_send_pair_time_budget<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> ReliableSendOutputs<'a, T>
where
    T: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + 'a,
{
    ReliableSendOutputs {
        delivered: reliable_send_time_budget(
            numbered(values),
            receiver,
            resend_ticks,
            BUDGET_CAPACITY,
            BUDGET_TOKENS_PER_TICK,
        ),
    }
}

/// [`reliable_send_pair`] over [`reliable_send_success_budget`]: each resend spends a token, and
/// every [`BUDGET_ACKS_PER_TOKEN`] acknowledgements that clear a value grant one, up to
/// [`BUDGET_CAPACITY`]. The same Cress and Sedge configurations as [`reliable_send_pair`].
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = two_per_round,
    T = super::rpc_retry::Payload<4096>,
    load = values,
    mu = 500,
    cpus(Sender = none, Receiver = 0.01),
    cpu_period_us = 100_000,
    timers(resend_ticks = std::time::Duration::from_secs(1)),
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = two_per_round,
    T = u64,
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
pub fn reliable_send_pair_success_budget<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> ReliableSendOutputs<'a, T>
where
    T: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + 'a,
{
    ReliableSendOutputs {
        delivered: reliable_send_success_budget(
            numbered(values),
            receiver,
            resend_ticks,
            BUDGET_CAPACITY,
            BUDGET_ACKS_PER_TOKEN,
        ),
    }
}

/// [`reliable_send_pair`] over [`reliable_send_hybrid_budget`]: each resend spends a token; the
/// timer grants [`BUDGET_TOKENS_PER_TICK`] per element and every [`BUDGET_ACKS_PER_TOKEN`]
/// acknowledgements that clear a value grant one, up to [`BUDGET_CAPACITY`]. The same Cress and
/// Sedge configurations as [`reliable_send_pair`].
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = two_per_round,
    T = super::rpc_retry::Payload<4096>,
    load = values,
    mu = 500,
    cpus(Sender = none, Receiver = 0.01),
    cpu_period_us = 100_000,
    timers(resend_ticks = std::time::Duration::from_secs(1)),
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = two_per_round,
    T = u64,
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
pub fn reliable_send_pair_hybrid_budget<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
) -> ReliableSendOutputs<'a, T>
where
    T: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + 'a,
{
    ReliableSendOutputs {
        delivered: reliable_send_hybrid_budget(
            numbered(values),
            receiver,
            resend_ticks,
            BUDGET_CAPACITY,
            BUDGET_TOKENS_PER_TICK,
            BUDGET_ACKS_PER_TOKEN,
        ),
    }
}

/// One sender and one receiver joined by [`reliable_send`] (`budget = None`) or by
/// [`reliable_send_budgeted`] with the given bucket, for sweeping Cress configurations over the
/// resend policy without a wrapper per setting. The attribute sets `budget` as a plain parameter.
/// The `*_rates` configurations vary the input rate at the default bucket (see
/// [`BUDGET_CAPACITY`]); `v1_cap*` and `v2_cap*` vary the bucket at two values per round; the
/// `*_ticks4` configurations fire the timer four times per round, so that resends happen while
/// acknowledgements still flow. Run with `./scripts/cress hydro_test reliable_send_sweep`. Its
/// name does not contain `reliable_send_pair`, so runs of the four wrappers above do not
/// include it.
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v0_rates,
    T = u64,
    budget = None,
    workload(values = [1, 2, 4]),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_rates,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 1, acks_per_token: 0 }),
    workload(values = [1, 2, 4]),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v2_rates,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 0, acks_per_token: 4 }),
    workload(values = [1, 2, 4]),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v3_rates,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 1, acks_per_token: 4 }),
    workload(values = [1, 2, 4]),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_cap5_tick1,
    T = u64,
    budget = Some(ResendBudget { capacity: 5, tokens_per_tick: 1, acks_per_token: 0 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_cap20_tick1,
    T = u64,
    budget = Some(ResendBudget { capacity: 20, tokens_per_tick: 1, acks_per_token: 0 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_cap5_tick2,
    T = u64,
    budget = Some(ResendBudget { capacity: 5, tokens_per_tick: 2, acks_per_token: 0 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_cap10_tick2,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 2, acks_per_token: 0 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_cap20_tick2,
    T = u64,
    budget = Some(ResendBudget { capacity: 20, tokens_per_tick: 2, acks_per_token: 0 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v2_cap5_ack4,
    T = u64,
    budget = Some(ResendBudget { capacity: 5, tokens_per_tick: 0, acks_per_token: 4 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v2_cap20_ack4,
    T = u64,
    budget = Some(ResendBudget { capacity: 20, tokens_per_tick: 0, acks_per_token: 4 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v2_cap10_ack1,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 0, acks_per_token: 1 }),
    workload(values = 2),
    workload_rounds = 125,
    test_safety_only,
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v1_ticks4,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 1, acks_per_token: 0 }),
    workload_rounds = 125,
    test_safety_only,
    round = |round, inputs| {
        if round < 125 {
            for i in 0..2u64 {
                inputs.values.send(round as u64 * 2 + i);
            }
        }
        for _ in 0..4 {
            inputs.resend_ticks.send(());
        }
    },
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v3_ticks4,
    T = u64,
    budget = Some(ResendBudget { capacity: 10, tokens_per_tick: 1, acks_per_token: 4 }),
    workload_rounds = 125,
    test_safety_only,
    round = |round, inputs| {
        if round < 125 {
            for i in 0..2u64 {
                inputs.values.send(round as u64 * 2 + i);
            }
        }
        for _ in 0..4 {
            inputs.resend_ticks.send(());
        }
    },
))]
#[cfg_attr(feature = "cress", cress::amplification_check(
    name = v0_ticks4,
    T = u64,
    budget = None,
    workload_rounds = 125,
    test_safety_only,
    round = |round, inputs| {
        if round < 125 {
            for i in 0..2u64 {
                inputs.values.send(round as u64 * 2 + i);
            }
        }
        for _ in 0..4 {
            inputs.resend_ticks.send(());
        }
    },
))]
pub fn reliable_send_sweep<'a, T>(
    _sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    values: Stream<T, Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    resend_ticks: Stream<(), Process<'a, Sender>, Unbounded, TotalOrder, ExactlyOnce>,
    budget: Option<ResendBudget>,
) -> ReliableSendOutputs<'a, T>
where
    T: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + 'a,
{
    let delivered = match budget {
        None => reliable_send(numbered(values), receiver, resend_ticks),
        Some(budget) => reliable_send_budgeted(numbered(values), receiver, resend_ticks, budget),
    };
    ReliableSendOutputs { delivered }
}

#[cfg(all(test, feature = "cress"))]
mod tests {
    use cress::{CheckConfig, HookCurve, Report, Shape, Verdict, check};
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

    use super::*;

    const ROUNDS: usize = 1000;
    const DATA_ROUNDS: usize = 125;
    const PER_ROUND: u64 = 2;

    /// `reliable_send.rs:<line>` of the `use::batch` whose first argument is `input`, found in
    /// the source so that editing the file does not break the test. Read at run time (an
    /// `include_str!` would also be expanded in the staged copy of this crate).
    fn batch_location(input: &str) -> String {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../hydro_std/src/reliable_send.rs"
        );
        let source = std::fs::read_to_string(path).expect("read reliable_send.rs");
        let needle = format!("use::batch({input},");
        let line = source
            .lines()
            .position(|l| l.contains(&needle))
            .unwrap_or_else(|| panic!("reliable_send.rs has a `{needle}`"))
            + 1;
        format!("reliable_send.rs:{line}")
    }

    fn curve<'r>(report: &'r Report, input: &str) -> &'r HookCurve {
        let location = batch_location(input);
        report
            .curves
            .iter()
            .find(|c| c.hook.contains(&location))
            .unwrap_or_else(|| {
                panic!(
                    "no curve for {location}; curves: {:?}",
                    report.curves.iter().map(|c| &c.hook).collect::<Vec<_>>()
                )
            })
    }

    /// The sender's and receiver's places in the counts: locations are numbered in creation
    /// order from 1, and [`run`] creates the sender first.
    const SENDER: &str = "Process(loc1v1)";
    const RECEIVER: &str = "Process(loc2v1)";

    /// Messages sent from `place` at hold length `k` on `curve`.
    fn sends_at(curve: &HookCurve, place: &str, k: usize) -> u64 {
        let (_, counts) = curve
            .counts
            .iter()
            .find(|(kk, _)| *kk == k)
            .unwrap_or_else(|| panic!("no hold of {k} on {}", curve.hook));
        counts
            .sends_by_member()
            .iter()
            .find(|(p, _)| p.ends_with(place))
            .map(|(_, n)| *n)
            .unwrap_or_else(|| panic!("no sends from {place}: {:?}", counts.sends_by_member()))
    }

    fn run() -> Report {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<Sender>();
        let receiver = flow.process::<Receiver>();
        let (values_send, values) = sender.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (ticks_send, ticks) = sender.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = reliable_send_pair(&sender, &receiver, values, ticks);
        let _delivered = outputs.delivered.sim_output();

        let config = CheckConfig::new(ROUNDS).with_workload_rounds(DATA_ROUNDS);
        let report = check(flow.sim().test_safety_only(), &config, async |round| {
            ticks_send.send(());
            if round < DATA_ROUNDS {
                for i in 0..PER_ROUND {
                    values_send.send(round as u64 * PER_ROUND + i);
                }
            }
        });
        println!("{report}");
        report
    }

    /// Holding the acknowledgements makes the sender resend everything outstanding on every
    /// timer interrupt, and the receiver acknowledge every copy, so both sides' messages rise
    /// with the length of the hold and keep rising after the input is over, by the size of the
    /// outstanding set per round of delay. Holding the timer or the new values adds nothing.
    ///
    /// The run: two new values per round for 125 rounds, the timer every round for 1000. The
    /// unheld run sends each of the 250 values once and acknowledges it once. Every hold begins
    /// at round 1, so the 248 values sent from then on stay outstanding while the acknowledgements
    /// are held, and each round of the hold beyond the input resends all 248 (and acknowledges
    /// them): 16494 sender messages at a hold of 128, 48238 at 256, 111726 at 512, a rate of
    /// 248 per round with no ceiling.
    #[test]
    fn holding_acks_amplifies_and_holding_ticks_does_not() {
        let report = run();
        assert_eq!(report.verdict, Verdict::Hazardous);

        let input = DATA_ROUNDS as u64 * PER_ROUND;
        let acks = curve(&report, "acks");
        assert_eq!(sends_at(acks, SENDER, 0), input);
        assert_eq!(sends_at(acks, RECEIVER, 0), input);
        assert_eq!(acks.first_reaction_at, Some(2));
        assert_eq!(sends_at(acks, SENDER, 128), 16494);
        assert_eq!(sends_at(acks, SENDER, 256), 48238);
        assert_eq!(sends_at(acks, SENDER, 512), 111726);
        // Every copy that arrives is acknowledged.
        for &k in &report.hold_lengths {
            assert_eq!(
                sends_at(acks, RECEIVER, k),
                sends_at(acks, SENDER, k),
                "hold {k}"
            );
        }

        // Cress names the acknowledgement batch, and reads the growth past the input as a steady
        // rate, not a ceiling.
        let location = report.location.as_ref().unwrap();
        assert!(
            location.source_location.ends_with(&batch_location("acks")),
            "expected the acknowledgement batch ({}), got {}",
            batch_location("acks"),
            location.source_location
        );
        let (_, worst) = report
            .growth
            .as_ref()
            .unwrap()
            .shape
            .as_ref()
            .expect("the run separates the input from the delay");
        let beyond = worst.beyond.as_ref().unwrap();
        match beyond.shape {
            Shape::Linear { b, .. } => assert_eq!(b, 248.0, "{beyond:?}"),
            ref other => panic!("expected a steady rate per round of delay, got {other:?}"),
        }

        // Holding the timer or the new values adds no work.
        for held in ["resend_ticks", "source"] {
            let c = curve(&report, held);
            assert_eq!(
                c.first_reaction_at, None,
                "holding {held} added work: {:?}",
                c.rose
            );
        }
        let ticks = curve(&report, "resend_ticks");
        for &k in &report.hold_lengths {
            assert_eq!(sends_at(ticks, SENDER, k), input, "hold {k} of the timer");
        }
    }
}
