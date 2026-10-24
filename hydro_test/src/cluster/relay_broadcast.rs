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
//! 3. **Cress flags the repair.** Resend-until-acked is a schedule amplifier: when a peer is
//!    behind, its acknowledgements arrive after the timeout, so the relay re-sends, which puts
//!    the peer further behind. Cress labels `resend_on` and `reliable_resend` hazardous and
//!    `echo_only` benign.
//! 4. **Sedge collapses it.** Deployed with the members' CPU capped and a modest
//!    overprovisioning factor, a burst tips `reliable_resend` into a state where the re-sends
//!    alone keep the members behind.
//!
//! # Mechanism
//!
//! On every tick a member merges every arrival it has received since the last one (initial
//! broadcasts from the source and relays from peers), and acknowledges every relay it merges
//! back to the relaying peer; initial arrivals are not acknowledged (the source never re-sends).
//! A member echoes an initial arrival to every peer in the tick that sees it (`ReliableResend`:
//! any first arrival), and records each echo in its outstanding table; an acknowledgement clears
//! the entry. With a resend policy, on each element of the member's timer (its protocol clock,
//! 50 ms when deployed) and for each peer, the oldest outstanding echo that has waited at least
//! `ack_timeout_ticks` timer elements is re-sent through the same relay channel.
//!
//! # No capacity model
//!
//! An earlier version modelled each member's capacity inside the program: a FIFO inbox from
//! which at most `max_merges_per_tick` arrivals were merged per timer element. That knob only
//! made overload reachable, and it is gone; a member merges whatever has arrived, and its
//! capacity is whatever the code achieves on the CPU it is given. The timer is only the
//! protocol's resend clock. Sedge caps the members' CPU from outside (see the `overload_check`
//! attributes), and leaves the source, which stands for the callers, uncapped.

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

/// One arrival at a member: an id, and the relaying peer if it came from a relay
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
        /// Re-sends allowed per peer per resend opportunity: the `resend_batch` oldest timed-out
        /// relays to that peer go back on the wire. `1` re-sends only the oldest.
        resend_batch: usize,
        /// Whether a tick that sees several timer elements (missed timer periods, delivered
        /// together after the member was descheduled) gets one resend opportunity per element,
        /// so the resend schedule catches up with wall-clock time, rather than one per tick.
        resend_catch_up: bool,
    },
}

impl RelayPolicy {
    /// `ReliableResend` as originally specified: re-send the oldest timed-out relay per peer,
    /// once per tick that sees the timer.
    pub const fn reliable_resend(ack_timeout_ticks: u64) -> Self {
        RelayPolicy::ReliableResend {
            ack_timeout_ticks,
            resend_batch: 1,
            resend_catch_up: false,
        }
    }
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
    pub policy: RelayPolicy,
    pub channel: RelayChannel,
}

#[cfg_attr(feature = "cress", derive(SimOutputs))]
pub struct Outputs<'a> {
    /// Every relay a member puts on the wire, as `(destination raw id, relay)`, first echoes and
    /// re-sends alike.
    pub wire: Stream<(u32, Relay), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every arrival a member merges, including ids it has merged before.
    pub merged: Stream<Arrival, Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement a member sends, as `(destination raw id, ack)`.
    pub acks_sent: Stream<(u32, Ack), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Size of the outstanding-echo table at the end of each tick.
    pub outstanding_depth: Stream<usize, Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every id a member receives, from the source or from a relay, with duplicates.
    pub delivered:
        Stream<u64, Cluster<'a, Member, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>,
    /// `(window index, metrics)` once per `report_tick`, at every member.
    pub metrics:
        Stream<(usize, RelayMetrics), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// What Sedge watches: each member's once-per-second metrics, summed over members. Deliveries
/// (arrivals merged, duplicates included), wire sends (first relays and re-sends: the work the
/// resend loop amplifies), re-sends alone, and the unacknowledged relays at the report (the
/// backlog). Observing the per-message streams instead would cost each CPU-capped member one
/// extra message per message it handles, and make the observation part of the load.
#[cfg(feature = "sedge")]
impl<'a> sedge::Observed<'a> for Outputs<'a> {
    fn observe(self, obs: &mut sedge::Observer<'a>) {
        obs.cluster_counter(
            "delivered",
            self.metrics.clone().map(q!(|(_, m)| m.delivered as f64)),
        );
        obs.cluster_counter("wire", self.metrics.clone().map(q!(|(_, m)| m.wire as f64)));
        obs.cluster_counter(
            "resent",
            self.metrics.clone().map(q!(|(_, m)| m.resent as f64)),
        );
        obs.cluster_gauge("unacked", self.metrics.map(q!(|(_, m)| m.unacked as f64)));
    }
}

#[cfg_attr(feature = "cress", amplification_check(
    name = echo_only,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::Echo, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = resend_on,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::EchoResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::reliable_resend(3), channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = echo_only,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::Echo, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = resend_on,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::EchoResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::reliable_resend(3), channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
// Retry-volume variants of `ReliableResend`: `rr_k<k>` re-sends the k oldest timed-out relays
// per peer per tick; `rr_catchup*` re-sends once per timer element, so a member that was
// descheduled for several timer periods catches up. Same CPU cap as `reliable_resend`.
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_k2,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 2, resend_catch_up: false }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_k4,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 4, resend_catch_up: false }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_k6,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 6, resend_catch_up: false }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_k8,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 8, resend_catch_up: false }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_catchup,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 1, resend_catch_up: true }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = rr_catchup_t1,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 1, resend_batch: 1, resend_catch_up: true }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_k2,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 2, resend_catch_up: false }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_k4,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 4, resend_catch_up: false }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_k6,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 6, resend_catch_up: false }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_k8,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 8, resend_catch_up: false }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_catchup,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3, resend_batch: 1, resend_catch_up: true }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = rr_catchup_t1,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 1, resend_batch: 1, resend_catch_up: true }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(round as u64);
    },
))]
pub fn relay_broadcast<'a>(
    source: &Process<'a, Source>,
    members: &Cluster<'a, Member>,
    requests: Stream<u64, Process<'a, Source>, Unbounded, TotalOrder, ExactlyOnce>,
    timer: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    report_tick: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RelayConfig,
) -> Outputs<'a> {
    let _ = source;
    let RelayConfig { policy, channel } = config;
    // The policy is fixed when the graph is built: whether members echo at all, and after how
    // many ticks an unacknowledged echo is re-sent (zero: never).
    // `relay_any`: relay ids first received from a peer as well, deduplicated per member.
    // `resend_batch`/`resend_catch_up`: how many timed-out relays per peer go back on the wire
    // per tick (see `RelayPolicy::ReliableResend`).
    let (echo_enabled, relay_any, ack_timeout_ticks, resend_batch, resend_catch_up) = match policy {
        RelayPolicy::None => (false, false, 0u64, 1usize, false),
        RelayPolicy::Echo => (true, false, 0u64, 1usize, false),
        RelayPolicy::EchoResend { ack_timeout_ticks } => (true, false, ack_timeout_ticks, 1usize, false),
        RelayPolicy::ReliableResend {
            ack_timeout_ticks,
            resend_batch,
            resend_catch_up,
        } => (true, true, ack_timeout_ticks, resend_batch, resend_catch_up),
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

    let (wire, merged, acks_sent, resends, unacked, outstanding_depth) = sliced! {
        let clock = use::batch(timer.enumerate(), nondet!(/** batching only shifts which tick observes a timer element; each element still advances the protocol clock */));
        let arrivals = use::batch(arrivals_in, nondet!(/** how long an arrival waits before the member merges it; this is the delay the mechanism depends on */));
        let acks = use::batch(acks_in, nondet!(/** how long an acknowledgement waits before it clears the outstanding entry; late acknowledgements cause re-sends */));
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
        // How many re-sends per peer this tick may make: none without a timer element; otherwise
        // `resend_batch`, once per tick or (`resend_catch_up`) once per timer element seen.
        let allowance = clock.count().map(q!(move |c| {
            if c == 0 {
                0usize
            } else if resend_catch_up {
                c * resend_batch
            } else {
                resend_batch
            }
        }));

        let arrivals = arrivals.sort();

        // ---- Receiver: merge every arrival, one ack per relay ---------------------------------
        let merged = arrivals.clone();
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
        // Per peer, the `allowance` oldest outstanding echoes that have waited past the timeout.
        // This is the operator where the mechanism lives; with `ack_timeout_ticks == 0` nothing
        // passes.
        let due = still_waiting
            .clone()
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .entries()
            .filter_map(q!(move |((peer, id), (last_sent, now))| {
                if ack_timeout_ticks > 0 && now - last_sent >= ack_timeout_ticks {
                    Some((peer, id))
                } else {
                    None
                }
            }))
            .into_keyed()
            .sort()
            .enumerate()
            .cross_singleton(allowance)
            .filter_map(q!(|((index, id), allowance)| if index < allowance { Some(id) } else { None }))
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

        let wire = echoes.chain(due.clone()).map(q!(move |(peer, id)| (
            peer,
            Relay { from: CLUSTER_SELF_ID.get_raw_id(), id }
        )));

        let depth = outstanding.clone().keys().count();
        (
            wire,
            merged,
            acks_sent,
            due,
            depth.clone(),
            depth.into_stream(),
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

    // ---- Metrics: once per report tick, per member ----------------------------------------
    // Observability only: counts of what the member did since its last report, and its table
    // of unacknowledged relays at the report. Reporting these once a window costs a member
    // a few messages a second, where observing every record would cost one per message.
    let metrics = sliced! {
        let punctuation = use::batch(report_tick.enumerate(), nondet!(/** metrics window boundary */));
        let merged_now = use::batch(merged.clone(), nondet!(/** metrics window boundary */));
        let wire_now = use::batch(wire.clone(), nondet!(/** metrics window boundary */));
        let resent_now = use::batch(resends, nondet!(/** metrics window boundary */));
        let unacked = use::snapshot(unacked, nondet!(/** metrics window boundary */));
        let mut acc = use::state(|l| l.singleton(q!(RelayMetrics::default())));

        let window_end = punctuation.first();
        let summed = acc
            .clone()
            .zip(merged_now.count())
            .zip(wire_now.count())
            .zip(resent_now.count())
            .map(q!(|(((a, m), w), r)| RelayMetrics {
                delivered: a.delivered + m as u64,
                wire: a.wire + w as u64,
                resent: a.resent + r as u64,
                unacked: 0,
            }));
        let report = summed
            .clone()
            .filter_if(window_end.clone().is_some())
            .zip(window_end.clone())
            .zip(unacked)
            .map(q!(|((m, (i, _)), u)| (i, RelayMetrics { unacked: u as u64, ..m })));
        acc = summed
            .filter_if(window_end.is_none())
            .unwrap_or(acc.clone().map(q!(|_| RelayMetrics::default())));

        report.into_stream()
    };

    Outputs {
        wire,
        merged,
        acks_sent,
        outstanding_depth,
        metrics,
        delivered,
    }
}

/// One member's metrics for one report window.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelayMetrics {
    /// Arrivals merged in the window (initial broadcasts and relays, duplicates included).
    pub delivered: u64,
    /// Relays put on the wire in the window, first sends and re-sends.
    pub wire: u64,
    /// Of `wire`, the re-sends.
    pub resent: u64,
    /// Relays awaiting an acknowledgement at the end of the window.
    pub unacked: u64,
}
