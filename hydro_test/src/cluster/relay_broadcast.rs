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

/// A request body. The source assigns each id exactly one body, and relays and re-sends copy it
/// unchanged, so the id determines the body: bodies compare equal and hash to nothing, and a
/// `(id, body)` pair is compared, ordered and deduplicated by its id alone, without touching the
/// bytes. The bytes are shared (`Bytes`), so the copies a member keeps for re-sending, one per
/// peer, cost a reference count rather than a copy of the body each.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Body(pub bytes::Bytes);

impl PartialEq for Body {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for Body {}
impl PartialOrd for Body {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Body {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}
impl std::hash::Hash for Body {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

/// One request from the source: an id and its body. The body is part of the offered workload
/// (empty in most configurations); members relay, store and re-send it with the id, so a larger
/// body makes every relay and re-send cost real bytes and CPU.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub body: Body,
}

impl Request {
    /// A request with an empty body.
    pub fn new(id: u64) -> Self {
        Self { id, body: Body::default() }
    }

    /// A request whose body is `len` bytes derived from `id`.
    pub fn with_body(id: u64, len: usize) -> Self {
        Self { id, body: Body(bytes::Bytes::from(vec![(id % 251) as u8; len])) }
    }
}

/// A relayed request. `from` is the relaying member's raw id.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Relay {
    pub from: u32,
    pub id: u64,
    pub body: Body,
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
    pub body: Body,
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
    /// `ReliableResend` with a retry timer per relay: on every timer tick, *every* relay whose
    /// acknowledgement is `ack_timeout_ticks` overdue is re-sent (there is no per-peer or per-tick
    /// cap), and its deadline restarts. After `max_attempts` sends in all (`None`: no limit) the
    /// relay is given up and dropped from the table.
    ReliableResendPerEntry {
        /// Ticks a relay may wait for its acknowledgement before it is re-sent.
        ack_timeout_ticks: u64,
        /// Sends per relay, the first included, before it is given up; `None` never gives up.
        max_attempts: Option<u32>,
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
    /// Size of the outstanding-echo table at the end of each tick that saw a timer element.
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
        inputs.requests.send(Request::new(round as u64));
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
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend,
    members = 5,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n3,
    members = 3,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n7,
    members = 7,
    workload_rounds = 125,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n9,
    members = 9,
    workload_rounds = 40,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n13,
    members = 13,
    workload_rounds = 40,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n17,
    members = 17,
    workload_rounds = 40,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n9_4k,
    members = 9,
    workload_rounds = 40,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = reliable_resend_n13_4k,
    members = 13,
    workload_rounds = 40,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = per_entry_n9_4k,
    members = 9,
    workload_rounds = 10, horizon = 200,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: None }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = per_entry5_n9_4k,
    members = 9,
    workload_rounds = 10, horizon = 200,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(5) }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "cress", amplification_check(
    name = per_entry3_n9_4k,
    members = 9,
    workload_rounds = 10, horizon = 200,
    skip_consistency_assertions,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(3) }, channel: RelayChannel::FailStop },
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
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
        inputs.requests.send(Request::new(round as u64));
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
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend,
    members = 5,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n3,
    members = 3,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n7,
    members = 7,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n9,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n13,
    members = 13,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n17,
    members = 17,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::new(round as u64));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n9_4k,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = reliable_resend_n13_4k,
    members = 13,
    config = RelayConfig { policy: RelayPolicy::ReliableResend { ack_timeout_ticks: 3 }, channel: RelayChannel::FailStop },
    mu = 500,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = per_entry_n9_4k,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: None }, channel: RelayChannel::FailStop },
    mu = 150,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = per_entry5_n9_4k,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(5) }, channel: RelayChannel::FailStop },
    mu = 150,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
#[cfg_attr(feature = "sedge", sedge::overload_check(
    name = per_entry3_n9_4k,
    members = 9,
    config = RelayConfig { policy: RelayPolicy::ReliableResendPerEntry { ack_timeout_ticks: 3, max_attempts: Some(3) }, channel: RelayChannel::FailStop },
    mu = 150,
    cpus(Member = 0.01),
    cpu_period_us = 100_000,
    timers(timer = Duration::from_millis(50), report_tick = Duration::from_secs(1)),
    round_data = |round, inputs| {
        inputs.requests.send(Request::with_body(round as u64, 4096));
    },
))]
pub fn relay_broadcast<'a>(
    source: &Process<'a, Source>,
    members: &Cluster<'a, Member>,
    requests: Stream<Request, Process<'a, Source>, Unbounded, TotalOrder, ExactlyOnce>,
    timer: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    report_tick: Stream<(), Cluster<'a, Member>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RelayConfig,
) -> Outputs<'a> {
    let _ = source;
    let RelayConfig { policy, channel } = config;
    // The policy is fixed when the graph is built: whether members echo at all, and after how
    // many ticks an unacknowledged echo is re-sent (zero: never).
    // `relay_any`: relay ids first received from a peer as well, deduplicated per member.
    // `per_entry`: re-send every overdue relay on each timer tick, rather than each peer's oldest;
    // `max_attempts`: sends per relay before giving up (0: never give up).
    let (echo_enabled, relay_any, ack_timeout_ticks, per_entry, max_attempts) = match policy {
        RelayPolicy::None => (false, false, 0u64, false, 0u32),
        RelayPolicy::Echo => (true, false, 0u64, false, 0u32),
        RelayPolicy::EchoResend { ack_timeout_ticks } => {
            (true, false, ack_timeout_ticks, false, 0u32)
        }
        RelayPolicy::ReliableResend { ack_timeout_ticks } => {
            (true, true, ack_timeout_ticks, false, 0u32)
        }
        RelayPolicy::ReliableResendPerEntry {
            ack_timeout_ticks,
            max_attempts,
        } => (true, true, ack_timeout_ticks, true, max_attempts.unwrap_or(0)),
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
        .map(q!(|r| r.id))
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
        .map(q!(|r| Arrival {
            relay_from: None,
            id: r.id,
            body: r.body
        }))
        .merge_unordered(relays_in.map(q!(|r| Arrival {
            relay_from: Some(r.from),
            id: r.id,
            body: r.body
        })));

    // What a member may relay: under `ReliableResend` every receipt from any sender, otherwise
    // the source's broadcast. Only the first receipt of each id is relayed; the outstanding
    // table below deduplicates by id in a persistent set of ids (O(1) per receipt), which keeps
    // no bodies. (A top-level `unique` over `(id, body)` did the same, but its set held every
    // body ever received, so a member's memory grew with the run.)
    let first_receipts = arrivals_in
        .clone()
        .filter_map(q!(move |a| if echo_enabled && (relay_any || a.relay_from.is_none()) {
            Some((a.id, a.body))
        } else {
            None
        }));

    // The members' tick: merge and acknowledge arrivals, fan out first receipts, and turn the
    // tick's acknowledgements, fan-out and timer element into events for the outstanding table.
    let (merged, acks_sent, table_events) = sliced! {
        let clock = use::batch(timer.enumerate(), nondet!(/** batching only shifts which tick observes a timer element; each element still advances the protocol clock */));
        let arrivals = use::batch(arrivals_in, nondet!(/** how long an arrival waits before the member merges it; this is the delay the mechanism depends on */));
        let first_receipts = use::batch(first_receipts, nondet!(/** how long a first receipt waits before the member relays it; relaying later only delays the fan-out */));
        let acks = use::batch(acks_in, nondet!(/** how long an acknowledgement waits before it clears the outstanding entry; late acknowledgements cause re-sends */));
        let mut now = use::state(|l| l.singleton(q!(0u64)));

        let now_cur = clock.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        // Whether this tick saw a timer element: re-sends are judged once per protocol tick.
        let pump = clock.count().map(q!(|c| c > 0));

        let arrivals = arrivals.sort();

        // ---- Receiver: merge every arrival, one ack per relay ---------------------------------
        let merged = arrivals.clone();
        let acks_sent = merged.clone().filter_map(q!(move |a| {
            let me = CLUSTER_SELF_ID.get_raw_id();
            let id = a.id;
            a.relay_from.map(move |peer| (peer, Ack { from: me, id }))
        }));

        // The tick's events for the outstanding table, in the order the table applies them:
        // acknowledgements clear entries, then this tick's receipts are deduplicated and the
        // first receipts fanned out to every peer and recorded, then (on a timer tick) the
        // re-send pump runs.
        let ack_events = acks
            .sort()
            .map(q!(|a| TableEvent::Ack { peer: a.from, id: a.id }));
        let send_events = first_receipts
            .sort()
            .cross_singleton(now_cur.clone())
            .map(q!(|((id, body), now)| TableEvent::Receipt { id, body, now }));
        let pump_events = now_cur
            .filter_if(pump)
            .into_stream()
            .map(q!(|now| TableEvent::Pump { now }));
        (
            merged,
            acks_sent,
            ack_events.chain(send_events).chain(pump_events),
        )
    };

    // ---- The outstanding table: `(peer, id)` -> (timer tick last sent, body to re-send) ------
    // Persistent state, updated in place: an acknowledgement removes one entry, a first receipt
    // inserts one per peer, and the pump re-sends. With `ReliableResend` (and `EchoResend`) the
    // pump re-sends, per peer, the oldest outstanding relay (smallest id) that has waited at
    // least `ack_timeout_ticks` timer ticks; with `ReliableResendPerEntry`, every relay that has,
    // in deadline order. A tick therefore costs O(arrivals + acks + peers + re-sends), not
    // O(table): the table is never copied or rescanned as a whole. (An earlier version kept it as
    // a tick-scoped `KeyedSingleton` carried by `defer_tick`, which rebuilt and filtered the
    // whole table on every tick.) Only the resend policies keep a table; with
    // `ack_timeout_ticks == 0` a relay is sent once and forgotten.
    //
    // Deadlines are in timer elements (`now` is the timer's index), and the deployed timer is a
    // Tokio interval that delivers missed ticks late rather than dropping them, so a starved
    // member's clock catches up and its overdue relays are all re-sent on its next timer tick.
    let table_out = table_events
        .scan(
            q!(move || OutstandingTable::new({
                // The other members' raw ids: deploy-time metadata, identical on every member and
                // never reassigned.
                let me = CLUSTER_SELF_ID.get_raw_id();
                cluster_members
                    .iter()
                    .map(|id| {
                        MemberId::<crate::cluster::relay_broadcast::Member>::from_tagless(id.clone())
                            .get_raw_id()
                    })
                    .filter(|id| *id != me)
                    .collect::<Vec<u32>>()
            })),
            q!(move |table, event| Some(table.apply(
                event,
                ack_timeout_ticks,
                per_entry,
                max_attempts
            ))),
        )
        .flat_map_ordered(q!(|out| out));

    // Every relay on the wire, first sends then (on timer ticks) re-sends, in table order.
    let wire = table_out.clone().filter_map(q!(move |out| match out {
        TableOut::Wire { peer, id, body, .. } => Some((
            peer,
            Relay { from: CLUSTER_SELF_ID.get_raw_id(), id, body }
        )),
        TableOut::Depth(_) => None,
    }));
    let resends = table_out.clone().filter_map(q!(|out| match out {
        TableOut::Wire { peer, id, body, resend: true } => Some((peer, (id, body))),
        _ => None,
    }));
    // The table's size at the end of each timer tick.
    let outstanding_depth = table_out.filter_map(q!(|out| match out {
        TableOut::Depth(d) => Some(d),
        TableOut::Wire { .. } => None,
    }));
    let unacked = outstanding_depth
        .clone()
        .fold(q!(|| 0usize), q!(|last, d| *last = d));

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

/// One event for a member's [`OutstandingTable`], produced by the member's tick in the order the
/// table applies it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TableEvent {
    /// `peer` acknowledged the relay of `id`.
    Ack { peer: u32, id: u64 },
    /// The member received `id` (from the source, or from a peer under `ReliableResend`) at
    /// timer tick `now`: if it is the first receipt, relay it to every peer.
    Receipt { id: u64, body: Body, now: u64 },
    /// A timer tick: re-send what has waited past the timeout.
    Pump { now: u64 },
}

/// What the table emits for an event.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TableOut {
    /// A relay on the wire: a first send, or a re-send.
    Wire { peer: u32, id: u64, body: Body, resend: bool },
    /// The table's size at the end of a timer tick.
    Depth(usize),
}

/// A per-relay entry of the per-entry policy: when it is next due, how often it has been sent.
#[derive(Clone, Debug)]
struct PerEntry {
    deadline: u64,
    attempts: u32,
    body: Body,
}

/// A member's relay state, kept as persistent state and updated in place, so each event costs
/// O(log table) plus whatever it sends:
///
/// - `seen`: every id the member has relayed (ids only, no bodies), which deduplicates
///   receipts;
/// - for the per-peer policies (`EchoResend`, `ReliableResend`): `peer -> id -> (timer tick last
///   sent, body)`;
/// - for `ReliableResendPerEntry`: `(peer, id) -> (deadline, attempts, body)` with a deadline-
///   ordered index, so a timer tick touches only the relays that are due.
#[derive(Clone, Debug, Default)]
pub struct OutstandingTable {
    peers: Vec<u32>,
    seen: std::collections::HashSet<u64>,
    by_peer: std::collections::BTreeMap<u32, std::collections::BTreeMap<u64, (u64, Body)>>,
    entries: std::collections::HashMap<(u32, u64), PerEntry>,
    due: std::collections::BTreeSet<(u64, u32, u64)>,
    len: usize,
}

impl OutstandingTable {
    /// An empty table for a member whose peers are `peers`.
    pub fn new(peers: Vec<u32>) -> Self {
        Self {
            peers,
            ..Self::default()
        }
    }

    /// Relays awaiting an acknowledgement.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no relay awaits an acknowledgement.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Applies one event. With `ack_timeout_ticks == 0` (no resend policy) nothing is recorded,
    /// and first sends pass straight through. `per_entry` selects `ReliableResendPerEntry`'s
    /// pump, and `max_attempts` (0: unlimited) its retry limit.
    pub fn apply(
        &mut self,
        event: TableEvent,
        ack_timeout_ticks: u64,
        per_entry: bool,
        max_attempts: u32,
    ) -> Vec<TableOut> {
        match event {
            TableEvent::Ack { peer, id } => {
                if per_entry {
                    if let Some(e) = self.entries.remove(&(peer, id)) {
                        self.due.remove(&(e.deadline, peer, id));
                        self.len -= 1;
                    }
                } else if let Some(m) = self.by_peer.get_mut(&peer)
                    && m.remove(&id).is_some()
                {
                    self.len -= 1;
                }
                Vec::new()
            }
            TableEvent::Receipt { id, body, now } => {
                if !self.seen.insert(id) {
                    return Vec::new();
                }
                // A single attempt means nothing is ever re-sent, so nothing is kept.
                let keep = ack_timeout_ticks > 0 && !(per_entry && max_attempts == 1);
                let mut out = Vec::with_capacity(self.peers.len());
                for &peer in &self.peers {
                    if keep {
                        if per_entry {
                            let deadline = now + ack_timeout_ticks;
                            if self
                                .entries
                                .insert(
                                    (peer, id),
                                    PerEntry {
                                        deadline,
                                        attempts: 1,
                                        body: body.clone(),
                                    },
                                )
                                .is_none()
                            {
                                self.due.insert((deadline, peer, id));
                                self.len += 1;
                            }
                        } else if let std::collections::btree_map::Entry::Vacant(e) =
                            self.by_peer.entry(peer).or_default().entry(id)
                        {
                            e.insert((now, body.clone()));
                            self.len += 1;
                        }
                    }
                    out.push(TableOut::Wire {
                        peer,
                        id,
                        body: body.clone(),
                        resend: false,
                    });
                }
                out
            }
            TableEvent::Pump { now } => {
                let mut out = Vec::new();
                if ack_timeout_ticks > 0 && per_entry {
                    // Every relay whose deadline has passed, in deadline order.
                    while let Some(&(deadline, peer, id)) = self.due.first() {
                        if deadline > now {
                            break;
                        }
                        self.due.pop_first();
                        let e = self
                            .entries
                            .get_mut(&(peer, id))
                            .expect("every deadline has its entry");
                        e.attempts += 1;
                        out.push(TableOut::Wire {
                            peer,
                            id,
                            body: e.body.clone(),
                            resend: true,
                        });
                        if max_attempts > 0 && e.attempts >= max_attempts {
                            // The last attempt: give the relay up.
                            self.entries.remove(&(peer, id));
                            self.len -= 1;
                        } else {
                            e.deadline = now + ack_timeout_ticks;
                            self.due.insert((e.deadline, peer, id));
                        }
                    }
                } else if ack_timeout_ticks > 0 {
                    // Per peer, the smallest id that has waited past the timeout. The entries
                    // skipped on the way are the ones sent within the last `ack_timeout_ticks`
                    // timer ticks, so the walk is short.
                    for (peer, m) in self.by_peer.iter_mut() {
                        if let Some((id, (last_sent, body))) = m
                            .iter_mut()
                            .find(|(_, (last_sent, _))| now - *last_sent >= ack_timeout_ticks)
                        {
                            *last_sent = now;
                            out.push(TableOut::Wire {
                                peer: *peer,
                                id: *id,
                                body: body.clone(),
                                resend: true,
                            });
                        }
                    }
                }
                out.push(TableOut::Depth(self.len));
                out
            }
        }
    }
}

#[cfg(test)]
mod table_tests {
    use super::{Body, OutstandingTable, TableEvent, TableOut};

    fn receipt(id: u64, now: u64) -> TableEvent {
        TableEvent::Receipt {
            id,
            body: Body::default(),
            now,
        }
    }

    fn resent(out: &[TableOut]) -> Vec<(u32, u64)> {
        out.iter()
            .filter_map(|o| match o {
                TableOut::Wire {
                    peer,
                    id,
                    resend: true,
                    ..
                } => Some((*peer, *id)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn first_receipts_fan_out_once() {
        let mut t = OutstandingTable::new(vec![1, 2]);
        assert_eq!(t.apply(receipt(7, 0), 3, false, 0).len(), 2);
        assert!(t.apply(receipt(7, 0), 3, false, 0).is_empty());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn per_peer_pump_resends_each_peers_oldest_due() {
        let mut t = OutstandingTable::new(vec![1, 2]);
        t.apply(receipt(5, 0), 3, false, 0);
        t.apply(receipt(6, 0), 3, false, 0);
        assert!(resent(&t.apply(TableEvent::Pump { now: 2 }, 3, false, 0)).is_empty());
        assert_eq!(
            resent(&t.apply(TableEvent::Pump { now: 3 }, 3, false, 0)),
            vec![(1, 5), (2, 5)]
        );
        t.apply(TableEvent::Ack { peer: 1, id: 6 }, 3, false, 0);
        assert_eq!(
            resent(&t.apply(TableEvent::Pump { now: 4 }, 3, false, 0)),
            vec![(2, 6)]
        );
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn per_entry_pump_resends_every_due_entry_and_gives_up() {
        let mut t = OutstandingTable::new(vec![1, 2]);
        t.apply(receipt(5, 0), 3, true, 3);
        t.apply(receipt(6, 1), 3, true, 3);
        t.apply(TableEvent::Ack { peer: 2, id: 6 }, 3, true, 3);
        // A starved timer: the next tick is already at 10, so everything is overdue.
        assert_eq!(
            resent(&t.apply(TableEvent::Pump { now: 10 }, 3, true, 3)),
            vec![(1, 5), (2, 5), (1, 6)]
        );
        assert!(resent(&t.apply(TableEvent::Pump { now: 12 }, 3, true, 3)).is_empty());
        // The third and last attempt, then the entries are given up.
        assert_eq!(resent(&t.apply(TableEvent::Pump { now: 13 }, 3, true, 3)).len(), 3);
        assert_eq!(t.len(), 0);
        assert!(resent(&t.apply(TableEvent::Pump { now: 20 }, 3, true, 3)).is_empty());
    }

    #[test]
    fn per_entry_unbounded_never_gives_up() {
        let mut t = OutstandingTable::new(vec![1]);
        t.apply(receipt(5, 0), 3, true, 0);
        for k in 1..=20 {
            assert_eq!(
                resent(&t.apply(TableEvent::Pump { now: 3 * k }, 3, true, 0)),
                vec![(1, 5)]
            );
        }
        assert_eq!(t.len(), 1);
    }
}
