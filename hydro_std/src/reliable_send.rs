//! Reliable delivery from one process to another over a lossy network.
//!
//! [`reliable_send`] is [`Stream::send`] for a network that may drop messages: what the sender
//! sends, the receiver delivers, exactly once, as long as both stay up.
//!
//! # The protocol
//!
//! Resend until acknowledged, with the value itself as its identity:
//!
//! - **Sender.** Each new value goes on the wire at once and joins the *outstanding* set. An
//!   acknowledgement of a value removes it from the set. On each element of `resend_ticks`, the
//!   sender resends every value still outstanding, byte for byte as it first sent it.
//! - **Receiver.** Every arrival is acknowledged at once, a retransmission as well as the first
//!   copy, since an earlier acknowledgement may have been lost. Delivery is the arrivals with
//!   duplicates removed ([`Stream::unique`]), so each value is delivered once.
//!
//! The value is its own message identity: there are no sequence numbers and no random ids. Two
//! sends of one value are one message, delivered once.
//!
//! # Fault model
//!
//! Both channels (data, and acknowledgements) are lossy
//! ([`lossy_delayed_forever`](hydro_lang::networking::NetworkingConfig::lossy_delayed_forever)).
//! Delivery needs *fair loss*: only finitely many messages are dropped, so after the last drop a
//! resend of each outstanding value, and its acknowledgement, get through. With a network that
//! drops forever, no protocol delivers. Neither side may crash: a crashed sender resends
//! nothing, and a crashed receiver delivers nothing.
//!
//! # Time
//!
//! The protocol has no clock. `resend_ticks` is its timer interrupt: a deployment wires
//! `source_interval(period)` into it, and a test fires it by hand. "Timing out" a message means
//! that it is still outstanding when a tick fires. Like Raft's heartbeat, which resends the
//! whole unacknowledged suffix of the log, a tick resends everything outstanding, not only the
//! oldest message, and there are no per-message timestamps.
//!
//! # Cost
//!
//! - The outstanding set is state of the sender's tick, rebuilt on every tick, so each tick
//!   costs time proportional to its size, as does each resend.
//! - Each tick that fires puts every outstanding value on the wire again, including values
//!   that have arrived but whose acknowledgement has not.
//! - The receiver's `unique` keeps every distinct value it has received, forever.
//!
//! # Budgeted variants
//!
//! [`reliable_send`] resends *everything* outstanding on every tick, so a backlog of
//! unacknowledged values (late acknowledgements, a slow receiver) multiplies the traffic it
//! offers. The budgeted variants cap resends with a token bucket on the sender, an experiment
//! in how a reliable sender should govern its retries:
//!
//! | function | tokens come from |
//! |---|---|
//! | [`reliable_send_time_budget`] | the timer: `tokens_per_tick` per element of `resend_ticks` |
//! | [`reliable_send_success_budget`] | useful acknowledgements: one per `acks_per_token` that clear an outstanding value |
//! | [`reliable_send_hybrid_budget`] | both |
//!
//! In all three, first sends are free and each resend spends one token. The bucket starts full
//! and holds at most `capacity` tokens; grants above it are discarded. The rules follow the
//! retry-governance corpus (`hydro_test/src/cluster/witnesses/blind_budget/`), and the bucket is
//! held as in its pure-Hydro token bucket (`witnesses/blind/token_bucket.rs`): a `Singleton`
//! of tick state, granted by `count()`, capped by `min`.
//!
//! **Which values a token buys.** The outstanding set is a FIFO (the variants need a
//! [`TotalOrder`] source for that). On a tick, its first `available` values are resent and move
//! to the back, behind the values not resent, and new values join behind them. So resends go
//! round robin, and a value waits at most `ceil(outstanding / tokens)` resend opportunities.
//!
//! **What a budget does to delivery.** A time budget refills while the timer runs, so after
//! the network heals every outstanding value is eventually resent, and delivery keeps
//! [`reliable_send`]'s guarantee. A success budget refills only from acknowledgements: if
//! losses drain it while values are outstanding, no acknowledgement can arrive to refill it,
//! and those values are never resent. The hybrid refills from the timer, so it delivers.
//!
//! One of these may replace [`reliable_send`] once the experiment has a winner.

use std::hash::Hash;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, Ordering, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Sends `source` to `to`, like [`Stream::send`], over a lossy network, resending every value
/// not yet acknowledged on each element of `resend_ticks`. Every value the sender sends, the
/// receiver delivers exactly once, provided the network drops only finitely many messages and
/// neither process crashes. See the [module docs](self).
///
/// Both channels are fixed: `TCP.lossy_delayed_forever()`, named `data` and `ack`.
pub fn reliable_send<'a, T, L, L2, O>(
    source: Stream<T, Process<'a, L>, Unbounded, O, ExactlyOnce>,
    to: &Process<'a, L2>,
    resend_ticks: Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Stream<T, Process<'a, L2>, Unbounded, NoOrder, ExactlyOnce>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
    O: Ordering,
{
    let sender = source.location().clone();
    let (acks_complete, acks) =
        sender.forward_ref::<Stream<T, Process<'a, L>, Unbounded, NoOrder, ExactlyOnce>>();

    let wire = sliced! {
        let new = use::batch(source, nondet!(
            /// Which tick first sends a value. It is outstanding from that tick on, so this
            /// only shifts when it is sent, not whether it is.
        ));
        let acked = use::batch(acks, nondet!(
            /// How long an acknowledgement waits before it clears its value. A late one costs
            /// resends of a value that has already arrived, which the receiver deduplicates.
        ));
        let ticks = use::batch(resend_ticks, nondet!(
            /// Which tick sees a timer interrupt. It only shifts when outstanding values are
            /// resent; every interrupt resends whatever is outstanding then.
        ));
        let mut outstanding = use::state_null::<Stream<T, Tick<_>, Bounded, NoOrder>>();

        let still = outstanding.filter_not_in(acked);
        let resent = still.clone().filter_if(ticks.first().is_some());
        outstanding = still.chain(new.clone()).unique();

        new.chain(resent)
    };

    let arrivals = wire.send(to, TCP.lossy_delayed_forever().name("data").bincode());
    acks_complete.complete(
        arrivals
            .clone()
            .send(&sender, TCP.lossy_delayed_forever().name("ack").bincode()),
    );
    arrivals.unique()
}

/// The resend budget of the [budgeted variants](self#budgeted-variants): a token bucket on the
/// sender, refilled by the timer, by useful acknowledgements, or both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResendBudget {
    /// The most tokens the bucket holds, and how many it starts with.
    pub capacity: u64,
    /// Tokens granted per element of `resend_ticks`; 0 for none.
    pub tokens_per_tick: u64,
    /// Acknowledgements, among those that clear an outstanding value, per token granted; 0 for
    /// none.
    pub acks_per_token: u64,
}

/// [`reliable_send`] with resends paid from a token bucket that the timer refills:
/// `tokens_per_tick` per element of `resend_ticks`, up to `capacity`. Resends are at most
/// `capacity + tokens_per_tick × ticks`, whatever the backlog. See the
/// [budgeted variants](self#budgeted-variants).
pub fn reliable_send_time_budget<'a, T, L, L2>(
    source: Stream<T, Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    to: &Process<'a, L2>,
    resend_ticks: Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    capacity: u64,
    tokens_per_tick: u64,
) -> Stream<T, Process<'a, L2>, Unbounded, NoOrder, ExactlyOnce>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
{
    reliable_send_budgeted(
        source,
        to,
        resend_ticks,
        ResendBudget {
            capacity,
            tokens_per_tick,
            acks_per_token: 0,
        },
    )
}

/// [`reliable_send`] with resends paid from a token bucket that useful acknowledgements refill:
/// one token per `acks_per_token` acknowledgements that clear an outstanding value, up to
/// `capacity`. Resends are at most `capacity + values / acks_per_token`, a bound on the input
/// alone. **It does not deliver under every fair-loss network**: losses that drain the bucket
/// while values are outstanding leave nothing to refill it. See the
/// [budgeted variants](self#budgeted-variants).
pub fn reliable_send_success_budget<'a, T, L, L2>(
    source: Stream<T, Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    to: &Process<'a, L2>,
    resend_ticks: Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    capacity: u64,
    acks_per_token: u64,
) -> Stream<T, Process<'a, L2>, Unbounded, NoOrder, ExactlyOnce>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
{
    reliable_send_budgeted(
        source,
        to,
        resend_ticks,
        ResendBudget {
            capacity,
            tokens_per_tick: 0,
            acks_per_token,
        },
    )
}

/// [`reliable_send`] with resends paid from a token bucket that both the timer and useful
/// acknowledgements refill. See [`reliable_send_time_budget`],
/// [`reliable_send_success_budget`] and the [budgeted variants](self#budgeted-variants).
pub fn reliable_send_hybrid_budget<'a, T, L, L2>(
    source: Stream<T, Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    to: &Process<'a, L2>,
    resend_ticks: Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    capacity: u64,
    tokens_per_tick: u64,
    acks_per_token: u64,
) -> Stream<T, Process<'a, L2>, Unbounded, NoOrder, ExactlyOnce>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
{
    reliable_send_budgeted(
        source,
        to,
        resend_ticks,
        ResendBudget {
            capacity,
            tokens_per_tick,
            acks_per_token,
        },
    )
}

/// [`reliable_send`] with each resend paid from the token bucket `budget`. The shared body of
/// the [budgeted variants](self#budgeted-variants).
pub fn reliable_send_budgeted<'a, T, L, L2>(
    source: Stream<T, Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    to: &Process<'a, L2>,
    resend_ticks: Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
    budget: ResendBudget,
) -> Stream<T, Process<'a, L2>, Unbounded, NoOrder, ExactlyOnce>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L: 'a,
    L2: 'a,
{
    let ResendBudget {
        capacity,
        tokens_per_tick,
        acks_per_token,
    } = budget;
    let sender = source.location().clone();
    let (acks_complete, acks) =
        sender.forward_ref::<Stream<T, Process<'a, L>, Unbounded, NoOrder, ExactlyOnce>>();

    let wire = sliced! {
        let new = use::batch(source, nondet!(
            /// Which tick first sends a value. It is outstanding from that tick on, so this
            /// only shifts when it is sent, not whether it is.
        ));
        let acked = use::batch(acks, nondet!(
            /// How long an acknowledgement waits before it clears its value. A late one costs
            /// resends of a value that has already arrived, and delays the token it may earn.
        ));
        let ticks = use::batch(resend_ticks, nondet!(
            /// Which tick sees a timer interrupt. It shifts when tokens are granted and when
            /// outstanding values are resent; every interrupt grants and resends.
        ));
        let mut outstanding = use::state_null::<Stream<T, Tick<_>, Bounded, TotalOrder>>();
        let mut tokens = use::state(|l| l.singleton(q!(capacity)));
        let mut ack_credit = use::state(|l| l.singleton(q!(0u64)));

        // Acknowledgements first: those that clear an outstanding value are useful, and every
        // `acks_per_token` of them earn a token. The remainder carries over.
        let still = outstanding.clone().filter_not_in(acked);
        let useful = outstanding
            .count()
            .zip(still.clone().count())
            .map(q!(|(before, after)| (before - after) as u64));
        let credit = ack_credit.zip(useful).map(q!(|(saved, useful)| saved + useful));
        let ack_grant = credit.clone().map(q!(move |credit| if acks_per_token == 0 {
            0
        } else {
            credit / acks_per_token
        }));
        ack_credit = credit.map(q!(move |credit| if acks_per_token == 0 {
            0
        } else {
            credit % acks_per_token
        }));

        // Then the timer's grant, and the cap.
        let fired = ticks.count();
        let tick_grant = fired.clone().map(q!(move |n| n as u64 * tokens_per_tick));
        let available = tokens
            .zip(tick_grant)
            .zip(ack_grant)
            .map(q!(move |((saved, by_tick), by_ack)| (saved + by_tick + by_ack).min(capacity)));

        // On a tick, the first `available` outstanding values are resent and go to the back.
        let placed = still
            .enumerate()
            .cross_singleton(available.clone())
            .cross_singleton(fired)
            .map(q!(|(((position, value), budget), fired)| {
                (fired > 0 && (position as u64) < budget, value)
            }));
        let resent = placed
            .clone()
            .filter_map(q!(|(resend, value)| resend.then_some(value)));
        let waiting = placed.filter_map(q!(|(resend, value)| (!resend).then_some(value)));
        let spent = resent.clone().count().map(q!(|n| n as u64));
        tokens = available.zip(spent).map(q!(|(available, spent)| available - spent));
        outstanding = waiting.chain(resent.clone()).chain(new.clone()).unique();

        new.chain(resent)
    };

    let arrivals = wire.send(to, TCP.lossy_delayed_forever().name("data").bincode());
    acks_complete.complete(
        arrivals
            .clone()
            .send(&sender, TCP.lossy_delayed_forever().name("ack").bincode()),
    );
    arrivals.unique()
}
