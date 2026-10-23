//! Relay broadcast: a source process broadcasts request ids to a cluster, and the members relay
//! what they receive so that a source crash cannot leave the members disagreeing.
//!
//! # The story
//!
//! 1. **One hop is not enough.** With [`RelayPolicy::None`] the program is a plain
//!    `broadcast_closed` from the source. If the source crashes part-way through sending an id,
//!    some members have it and others never will: the members' `delivered` sets diverge forever,
//!    so `delivered` is not eventually consistent under a source crash.
//! 2. **The repair.** With [`RelayPolicy::Echo`] every member that receives an id from the source
//!    relays (echoes) it to every peer, and with [`RelayPolicy::EchoResend`] it also keeps each
//!    echo in an outstanding table and re-sends it, with identical bytes, until the peer
//!    acknowledges it. Any member that got an id therefore pushes it to every peer, which is the
//!    argument behind the `manual_proof!` on `delivered`.
//! 3. **Cress flags the repair.** Resend-until-acked is a schedule amplifier: when a peer's inbox
//!    is deep its acknowledgements arrive after the timeout, so the relay re-sends, which deepens
//!    the inbox further. Cress labels `resend_on` hazardous and `echo_only` benign.
//! 4. **Sedge collapses it.** Deployed at a modest overprovisioning factor, a burst tips the
//!    `resend_on` configuration into a state where the re-sends alone keep the inboxes growing.
//!
//! # Mechanism
//!
//! Each member keeps a FIFO inbox of arrivals (initial broadcasts from the source and relays from
//! peers) and merges at most [`RelayConfig::max_merges_per_tick`] of them per timer element.
//! Merging a relay sends an acknowledgement back to the relaying peer; initial arrivals are not
//! acknowledged (the source never re-sends). A member echoes an initial arrival to every peer in
//! the tick that sees it, and records each echo in its outstanding table; an acknowledgement
//! clears the entry. With `EchoResend`, on each timer element and for each peer, the oldest
//! outstanding echo that has waited at least `ack_timeout_ticks` ticks is re-sent through the
//! same relay channel.
//!
//! # Deployed constants (Sedge)
//!
//! A round is one timer element at every member (50 ms deployed) and one request at the source.
//! Each member then receives one initial arrival and four relays per round, five merges against
//! a budget of `max_merges_per_tick = 7` per tick: 140 merges/s at 20 ticks/s, so `mu = 28`
//! rounds/s. Once inboxes are deeper than the 3-tick timeout, each member also receives one
//! re-send per peer per tick, 80/s, on top of `20ρ` first-time arrivals; the inboxes keep
//! growing while `5ρ + 80 > 140`, that is `ρ > 12` rounds/s, so the critical factor is about
//! `f = 28 / 12 ≈ 2.3`.

use std::marker::PhantomData;
#[cfg(any(feature = "cress", feature = "sedge"))]
use std::time::Duration;

#[cfg(feature = "cress")]
use cress::{SimOutputs, amplification_check};
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, ClusterIds, EventualConsistency};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Location, MemberId};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Source;
pub struct Member;

/// A relayed id. `from` is the relaying member's raw id.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Relay {
    pub from: u32,
    pub id: u64,
}

/// Acknowledges that `from` has merged the relay of `id` sent by the receiver.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ack {
    pub from: u32,
    pub id: u64,
}

/// One entry of a member's inbox: an id, and the relaying peer if it came from a relay
/// (`None` for the source's initial broadcast).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Arrival {
    pub relay_from: Option<u32>,
    pub id: u64,
}

/// How members repair a partial broadcast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayPolicy {
    /// No relaying: one-hop broadcast only.
    None,
    /// Echo every id received from the source to every peer, once.
    Echo,
    /// Echo, and re-send each echo until the peer acknowledges it.
    EchoResend {
        /// Ticks an echo may wait for its acknowledgement before it is re-sent.
        ack_timeout_ticks: u64,
    },
    /// Reliable broadcast with retransmission: relay every id on its *first receipt from any
    /// sender* (the source or a peer) to every peer, once, and re-send each relay until the
    /// peer acknowledges it. Unlike `EchoResend`, an id that reached a member only through a
    /// relay is relayed onward too, so a relayer crashing part-way through its fan-out is
    /// covered by whichever peer it did reach.
    ReliableResend {
        /// Ticks a relay may wait for its acknowledgement before it is re-sent.
        ack_timeout_ticks: u64,
    },
}

/// The failure model of the relay channel, chosen when the graph is built. The rest of the
/// program is identical for both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayChannel {
    /// `TCP.fail_stop()`: a lost connection means the peer has failed. Cress and Sedge use this.
    FailStop,
    /// `TCP.lossy_delayed_forever()`: individual relays may be dropped (in the simulator, delayed
    /// indefinitely), which is what makes re-sending necessary. Simulating it requires
    /// `.test_safety_only()`.
    LossyDelayedForever,
}

#[derive(Clone, Copy, Debug)]
pub struct RelayConfig {
    /// Arrivals merged per timer element at each member: the member's capacity.
    pub max_merges_per_tick: u32,
    pub policy: RelayPolicy,
    pub channel: RelayChannel,
}

#[cfg_attr(feature = "cress", derive(SimOutputs))]
#[cfg_attr(feature = "sedge", derive(sedge::SedgeOutputs))]
pub struct Outputs<'a> {
    /// Every relay a member puts on the wire, as `(destination raw id, relay)`, first echoes and
    /// re-sends alike.
    pub wire: Stream<(u32, Relay), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every arrival a member merges, including ids it has merged before.
    pub merged: Stream<Arrival, Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement a member sends, as `(destination raw id, ack)`.
    pub acks_sent: Stream<(u32, Ack), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Inbox depth at the end of each tick.
    pub inbox_depth: Stream<usize, Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Size of the outstanding-echo table at the end of each tick.
    pub outstanding_depth: Stream<usize, Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every id a member receives, from the source or from a relay, with duplicates.
    pub delivered:
        Stream<u64, Cluster<'a, Member, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>,
}

#[cfg_attr(feature = "cress", amplification_check(
    name = echo_only,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::Echo, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = resend_on,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::EchoResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = echo_only,
    members = 5,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::Echo, channel: RelayChannel::FailStop },
    mu = 28,
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = resend_on,
    members = 5,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::EchoResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 28,
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend,
    members = 5,
    config = RelayConfig { max_merges_per_tick: 7, policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 28,
    timers(timer = Duration::from_millis(50)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
pub fn relay_broadcast<'a>(
    source: &Process<'a, Source>,
    members: &Cluster<'a, Member>,
    requests: Stream<u64, Process<'a, Source>, Unbounded, TotalOrder, ExactlyOnce>,
    timer: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RelayConfig,
) -> Outputs<'a> {
    let _ = source;
    let RelayConfig {
        max_merges_per_tick,
        policy,
        channel,
    } = config;
    // The policy is fixed when the graph is built: whether members echo at all, and after how
    // many ticks an unacknowledged echo is re-sent (zero: never).
    // `relay_any`: relay ids first received from a peer as well, deduplicated per member.
    let (echo_enabled, relay_any, ack_timeout_ticks) = match policy {
        RelayPolicy::None => (false, false, 0u64),
        RelayPolicy::Echo => (true, false, 0u64),
        RelayPolicy::EchoResend { ack_timeout_ticks } => (true, false, ack_timeout_ticks),
        RelayPolicy::ReliableResend { ack_timeout_ticks } => (true, true, ack_timeout_ticks),
    };

    let LocationId::Cluster(cluster_key) = Location::id(members) else {
        unreachable!("relay broadcast runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    // 1. One-hop broadcast from the source.
    let initial = requests.broadcast_closed(members, TCP.fail_stop().bincode().name("bcast"));

    let (relays_complete, relays_in) =
        members
            .forward_ref::<Stream<Relay, Cluster<'a, Member>, Unbounded, NoOrder, ExactlyOnce>>();
    let (acks_complete, acks_in) =
        members.forward_ref::<Stream<Ack, Cluster<'a, Member>, Unbounded, NoOrder, ExactlyOnce>>();

    // 3. What a member has received, from the source or from any relay.
    let delivered = initial
        .clone()
        .weaken_consistency()
        .merge_unordered(relays_in.clone().map(q!(|r| r.id)))
        .assert_has_consistency_of::<Cluster<'a, Member, EventualConsistency>>(manual_proof!(
            /** reliable broadcast + resend-until-ack (`ReliableResend`): a member relays every
            id to all peers on its first receipt from any sender, source or peer, and re-sends
            each relay until the peer acknowledges. A live member that received id has
            therefore relayed it to every peer, and re-sends past any omission until acked, so
            every live member eventually receives every id that any live member received.
            `None`, `Echo` and `EchoResend` share this label but not the argument: they relay
            only ids received from the source, which HydroFrac refutes */
        ));

    let arrivals_in = initial
        .weaken_consistency()
        .map(q!(|id| Arrival {
            relay_from: None,
            id
        }))
        .merge_unordered(relays_in.map(q!(|r| Arrival {
            relay_from: Some(r.from),
            id: r.id
        })));

    let (wire, merged, acks_sent, inbox_depth, outstanding_depth) = sliced! {
        let clock = use::batch(timer.enumerate(), nondet!(/** batching only shifts which tick observes a timer element; each element still advances the clock and releases one budget */));
        let arrivals = use::batch(arrivals_in, nondet!(/** how long an arrival waits before the inbox sees it; this is the delay the mechanism depends on */));
        let acks = use::batch(acks_in, nondet!(/** how long an acknowledgement waits before it clears the outstanding entry; late acknowledgements cause re-sends */));
        let mut inbox = use::state_null::<Stream<Arrival, Tick<_>, Bounded, TotalOrder>>();
        let mut outstanding = use::state_null::<KeyedSingleton<(u32, u64), u64, Tick<_>, Bounded>>();
        // Ids this member has already relayed (`ReliableResend` only; empty otherwise).
        let mut relayed = use::state_null::<KeyedSingleton<u64, (), Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        // The other members' raw ids: deploy-time metadata, identical on every member and never
        // reassigned, so this state is a constant.
        let mut peers = use::state(|l| l.singleton(q!({
            let me = CLUSTER_SELF_ID.get_raw_id();
            cluster_members
                .iter()
                .map(|id| MemberId::<crate::cluster::relay_broadcast::Member>::from_tagless(id.clone()).get_raw_id())
                .filter(|id| *id != me)
                .collect::<Vec<u32>>()
        })));
        peers = peers.clone();

        let now_cur = clock.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        let pump = clock.count().map(q!(|c| c > 0));
        let take = pump.clone().map(q!(move |p| if p { max_merges_per_tick as usize } else { 0 }));

        let arrivals = arrivals.sort();

        // ---- Receiver: FIFO inbox, bounded merges per timer element, one ack per relay ------
        let queued = inbox.chain(arrivals.clone()).enumerate();
        let merged = queued
            .clone()
            .cross_singleton(take.clone())
            .filter_map(q!(|((i, a), take)| if i < take { Some(a) } else { None }));
        inbox = queued
            .cross_singleton(take)
            .filter_map(q!(|((i, a), take)| if i >= take { Some(a) } else { None }));
        let acks_sent = merged.clone().filter_map(q!(move |a| {
            let me = CLUSTER_SELF_ID.get_raw_id();
            a.relay_from.map(move |peer| (peer, Ack { from: me, id: a.id }))
        }));

        // ---- Relay: echo each initial arrival to every peer, outstanding table, re-send -----
        // `ReliableResend` relays on first receipt from any sender: every arrival's id, minus
        // those already relayed, once per tick.
        let to_relay = arrivals
            .filter_map(q!(move |a| if echo_enabled && (relay_any || a.relay_from.is_none()) { Some(a.id) } else { None }))
            .unique()
            .filter_not_in(relayed.clone().keys());
        relayed = relayed
            .into_keyed_stream()
            .chain(to_relay.clone().filter(q!(move |_| relay_any)).map(q!(|id| (id, ()))).into_keyed())
            .first();
        let echoes = to_relay
            .cross_singleton(peers.clone())
            .flat_map_ordered(q!(|(id, peers)| peers.into_iter().map(move |peer| (peer, id))));

        let acked_keys = acks.map(q!(|a| (a.from, a.id)));
        let still_waiting = outstanding.filter_key_not_in(acked_keys);
        // Per peer, the oldest outstanding echo that has waited past the timeout. This is the
        // operator where the mechanism lives; with `ack_timeout_ticks == 0` nothing passes.
        let due = still_waiting
            .clone()
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .cross_singleton(pump)
            .entries()
            .filter_map(q!(move |((peer, id), ((last_sent, now), pump))| {
                if pump && ack_timeout_ticks > 0 && now - last_sent >= ack_timeout_ticks {
                    Some((peer, id))
                } else {
                    None
                }
            }))
            .into_keyed()
            .sort()
            .first()
            .entries()
            .sort();

        let resent = due
            .clone()
            .cross_singleton(now_cur.clone())
            .map(q!(|((peer, id), now)| ((peer, id), now)))
            .into_keyed();
        let kept = still_waiting.filter_key_not_in(due.clone()).into_keyed_stream();
        // Only `EchoResend` waits for acknowledgements; the plain echo keeps no table.
        let new_entries = echoes
            .clone()
            .filter(q!(move |_| ack_timeout_ticks > 0))
            .cross_singleton(now_cur.clone())
            .map(q!(|((peer, id), now)| ((peer, id), now)))
            .into_keyed();
        outstanding = kept.chain(resent).chain(new_entries).first();

        let wire = echoes.chain(due).map(q!(move |(peer, id)| (
            peer,
            Relay { from: CLUSTER_SELF_ID.get_raw_id(), id }
        )));

        (
            wire,
            merged,
            acks_sent,
            inbox.clone().count().into_stream(),
            outstanding.clone().keys().count().into_stream(),
        )
    };

    // 2. The relay channel: echoes and re-sends, identical bytes.
    let relays_out = wire
        .clone()
        .map(q!(|(dest, r)| (MemberId::<Member>::from_raw_id(dest), r)))
        .into_keyed();
    relays_complete.complete(match channel {
        RelayChannel::FailStop => relays_out
            .demux(members, TCP.fail_stop().bincode().name("relay"))
            .values(),
        RelayChannel::LossyDelayedForever => relays_out
            .demux(members, TCP.lossy_delayed_forever().bincode().name("relay"))
            .values(),
    });
    acks_complete.complete(
        acks_sent
            .clone()
            .map(q!(|(dest, a)| (MemberId::<Member>::from_raw_id(dest), a)))
            .into_keyed()
            .demux(members, TCP.fail_stop().bincode().name("ack"))
            .values(),
    );

    Outputs {
        wire,
        merged,
        acks_sent,
        inbox_depth,
        outstanding_depth,
        delivered,
    }
}
