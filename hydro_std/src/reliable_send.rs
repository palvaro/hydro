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
