//! W3: delta gossip with re-send until acknowledged.
//!
//! Every member of a cluster maintains a G-Set. Each local update is numbered in arrival order
//! and sent, as a one-element delta, to every other member. A receiver keeps the deltas it has
//! not yet merged in a FIFO inbox and merges at most [`GossipConfig::max_merges_per_tick`] of
//! them per timer element, acknowledging each merged delta to its origin. A sender keeps every
//! delta it has sent to a peer in an outstanding table until the acknowledgement arrives. On
//! each timer element, for each peer, if the oldest outstanding delta to that peer has waited
//! at least [`GossipConfig::ack_timeout_ticks`] ticks, the sender re-sends that one delta. Merges
//! are idempotent, so a re-sent delta changes nothing at the receiver except that it costs one
//! unit of merge budget and one more acknowledgement.
//!
//! The mechanism is the re-send. When inboxes are deep, acknowledgements arrive after the
//! timeout, so every member re-sends one delta per peer per tick on top of its new deltas; the
//! re-sends fill the peers' inboxes further, which delays acknowledgements further. The knob is
//! `ack_timeout_ticks`: zero disables re-sending and gives a different program, in which a full
//! inbox simply drains at the merge rate.
//!
//! # Timer parameters
//!
//! - `timer`: one element per logical tick at each member. A deployment wires
//!   `cluster.source_interval(period)` into it; the simulation feeds it per member from
//!   `sim_input`. Each element advances that member's clock by one, releases one budget of
//!   merges, and judges the outstanding table against the timeout.
//!
//! The label is **hazardous** with re-sending on: a delayed acknowledgement makes the sender put
//! a delta on the wire, and the receiver merge it, that the input did not ask for. With
//! `ack_timeout_ticks = 0` the program is benign: every delta is sent once per peer and merged
//! once per member under any schedule.
//!
//! # Measured
//!
//! Five members, merge budget 5 per member per round, ack timeout 3, baseline 1 update per
//! member per round (20 deltas on the wire per round), 800 rounds, tail = rounds 600..800. The
//! trigger is 2 updates per member per round in rounds 100..160.
//!
//! | run | sends first / again | merges first / again | tail merges first / again | inbox total at 600 -> 800 (peak) | outstanding at end | label |
//! |---|---|---|---|---|---|---|
//! | re-send on, trigger | 17200 / 13858 | 11414 / 8075 | 2500 / 2500 | 8584 -> 11569 (11569) | 5786 | hazardous, collapses |
//! | re-send on, no trigger | 16000 / 0 | 15990 / 0 | 4000 / 0 | 10 -> 10 (10) | 10 | (control) |
//! | re-send off, trigger | 17200 / 0 | 17190 / 0 | 4000 / 0 | 10 -> 10 (911) | 10 | benign; inbox back to in-flight level by round 338 |
//!
//! The 10 deltas in flight at the end of a healthy round are the deltas sent by members whose
//! ticks ran after the receiver's in that round; they are merged in the next round. In the
//! collapsed run the inboxes grow by 15 per round, exactly the 3 per member the arithmetic in
//! `sim_tests` predicts, and half of every tail merge is of a delta already merged.

use std::marker::PhantomData;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, ClusterIds};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Location, MemberId};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Node;

/// A one-element delta of the origin's G-Set. `(from, seq)` identifies it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Delta {
    pub from: u32,
    pub seq: u64,
    pub value: u64,
}

/// Acknowledges that `from` has merged delta `seq` of the receiver.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ack {
    pub from: u32,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct GossipConfig {
    /// Deltas merged per timer element at each member: the member's capacity.
    pub max_merges_per_tick: u32,
    /// The mechanism knob. Ticks an outstanding delta may wait for its acknowledgement before
    /// it is re-sent; zero means never re-send.
    pub ack_timeout_ticks: u64,
}

pub struct Outputs<'a> {
    /// Every delta a member puts on the wire, as `(destination raw id, delta)`, first sends and
    /// re-sends alike.
    pub wire: Stream<(u32, Delta), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every delta a member merges, including deltas it has merged before.
    pub merged: Stream<Delta, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement a member sends.
    pub acks_sent: Stream<(u32, Ack), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Inbox depth at the end of each tick.
    pub inbox_depth: Stream<usize, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Size of the outstanding table at the end of each tick.
    pub outstanding_depth: Stream<usize, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
}

pub fn gossip_with_resend<'a>(
    cluster: &Cluster<'a, Node>,
    timer: Stream<(), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    local_updates: Stream<u64, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    config: GossipConfig,
) -> Outputs<'a> {
    let GossipConfig {
        max_merges_per_tick,
        ack_timeout_ticks,
    } = config;

    let LocationId::Cluster(cluster_key) = Location::id(cluster) else {
        unreachable!("gossip runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    let (deltas_complete, deltas_in) =
        cluster.forward_ref::<Stream<Delta, Cluster<'a, Node>, Unbounded, NoOrder, ExactlyOnce>>();
    let (acks_complete, acks_in) =
        cluster.forward_ref::<Stream<Ack, Cluster<'a, Node>, Unbounded, NoOrder, ExactlyOnce>>();

    let (wire, merged, acks_sent, inbox_depth, outstanding_depth) = sliced! {
        let clock = use::batch(timer.enumerate(), nondet!(/** batching only shifts which tick observes a timer element; each element still advances the clock and releases one budget */));
        let updates = use::batch(local_updates.enumerate(), nondet!(/** which tick first sends an update; only its timestamp changes */));
        let arrivals = use::batch(deltas_in, nondet!(/** how long a delta waits before the inbox sees it; this is the delay the mechanism depends on */));
        let acks = use::batch(acks_in, nondet!(/** how long an acknowledgement waits before it clears the outstanding entry; late acknowledgements cause re-sends */));
        let mut inbox = use::state_null::<Stream<Delta, Tick<_>, Bounded, TotalOrder>>();
        let mut outstanding = use::state_null::<KeyedSingleton<(u32, u64), (u64, u64), Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        // The other members' raw ids: deploy-time metadata, identical on every member and never
        // reassigned, so this state is a constant.
        let mut peers = use::state(|l| l.singleton(q!({
            let me = CLUSTER_SELF_ID.get_raw_id();
            cluster_members
                .iter()
                .map(|id| MemberId::<crate::cluster::witnesses::gossip_resend::Node>::from_tagless(id.clone()).get_raw_id())
                .filter(|id| *id != me)
                .collect::<Vec<u32>>()
        })));
        peers = peers.clone();

        let now_cur = clock.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        let pump = clock.count().map(q!(|c| c > 0));
        let take = pump.clone().map(q!(move |p| if p { max_merges_per_tick as usize } else { 0 }));

        // ---- Receiver: FIFO inbox, bounded merges per timer element, one ack per merge ------
        let queued = inbox.chain(arrivals.sort()).enumerate();
        let merged = queued
            .clone()
            .cross_singleton(take.clone())
            .filter_map(q!(|((i, d), take)| if i < take { Some(d) } else { None }));
        inbox = queued
            .cross_singleton(take)
            .filter_map(q!(|((i, d), take)| if i >= take { Some(d) } else { None }));
        let acks_sent = merged.clone().map(q!(move |d| (
            d.from,
            Ack { from: CLUSTER_SELF_ID.get_raw_id(), seq: d.seq }
        )));

        // ---- Sender: new deltas to every peer, outstanding table, re-send of the oldest -----
        let new_sends = updates
            .cross_singleton(peers.clone())
            .flat_map_ordered(q!(|((seq, value), peers)| peers
                .into_iter()
                .map(move |peer| (peer, seq as u64, value))));

        let acked_keys = acks.map(q!(|a| (a.from, a.seq)));
        let still_waiting = outstanding.filter_key_not_in(acked_keys);
        // Per peer, the oldest outstanding delta that has waited past the timeout. This is the
        // operator where the mechanism lives; with `ack_timeout_ticks == 0` nothing passes.
        let due = still_waiting
            .clone()
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .cross_singleton(pump)
            .entries()
            .filter_map(q!(move |((peer, seq), (((value, last_sent), now), pump))| {
                if pump && ack_timeout_ticks > 0 && now - last_sent >= ack_timeout_ticks {
                    Some((peer, (seq, value)))
                } else {
                    None
                }
            }))
            .into_keyed()
            .sort()
            .first()
            .entries()
            .map(q!(|(peer, (seq, value))| (peer, seq, value)))
            .sort();
        let due_keys = due.clone().map(q!(|(peer, seq, _)| (peer, seq)));

        let resent = due
            .clone()
            .cross_singleton(now_cur.clone())
            .map(q!(|((peer, seq, value), now)| ((peer, seq), (value, now))))
            .into_keyed();
        let kept = still_waiting.filter_key_not_in(due_keys).into_keyed_stream();
        let new_entries = new_sends
            .clone()
            .cross_singleton(now_cur.clone())
            .map(q!(|((peer, seq, value), now)| ((peer, seq), (value, now))))
            .into_keyed();
        outstanding = kept.chain(resent).chain(new_entries).first();

        let wire = new_sends.chain(due).map(q!(move |(peer, seq, value)| (
            peer,
            Delta { from: CLUSTER_SELF_ID.get_raw_id(), seq, value }
        )));

        (
            wire,
            merged,
            acks_sent,
            inbox.clone().count().into_stream(),
            outstanding.clone().keys().count().into_stream(),
        )
    };

    deltas_complete.complete(
        wire.clone()
            .map(q!(|(dest, d)| (MemberId::<Node>::from_raw_id(dest), d)))
            .into_keyed()
            .demux(cluster, TCP.fail_stop().bincode())
            .values(),
    );
    acks_complete.complete(
        acks_sent
            .clone()
            .map(q!(|(dest, a)| (MemberId::<Node>::from_raw_id(dest), a)))
            .into_keyed()
            .demux(cluster, TCP.fail_stop().bincode())
            .values(),
    );

    Outputs {
        wire,
        merged,
        acks_sent,
        inbox_depth,
        outstanding_depth,
    }
}

/// Local updates per member per round, higher inside the trigger window. This lives in the
/// harness, not in the program. (It sits outside the test module because the simulator stages
/// the crate and does not see `impl` blocks inside `#[cfg(test)]` modules.)
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_per_round: u32,
    pub trigger_per_round: u32,
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn at(&self, round: u64) -> u32 {
        if round >= self.trigger_start && round < self.trigger_end {
            self.trigger_per_round
        } else {
            self.baseline_per_round
        }
    }
}

/// One round is one timer element at every member plus that round's local updates at every
/// member, then `quiesce`. Per round each member merges one budget of its inbox, acknowledges
/// what it merged, sends its new deltas to every peer, and re-sends at most one overdue delta
/// per peer.
#[cfg(test)]
mod sim_tests {
    use std::collections::HashSet;

    use hydro_lang::sim::quiesce;

    use super::*;

    const MEMBERS: u32 = 5;

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Deltas on the wire whose `(from, seq, to)` had not been sent before.
        sent_first: u64,
        /// Deltas on the wire that had been sent to the same peer before: re-sends.
        sent_again: u64,
        /// Deltas merged by a member that had not merged them before.
        merged_first: u64,
        /// Deltas merged by a member that had merged them before: redundant merges.
        merged_again: u64,
        acks_sent: u64,
        /// Sum of inbox depths over members at the end of the round.
        inbox_total: usize,
        /// Sum of outstanding-table sizes over members at the end of the round.
        outstanding_total: usize,
    }

    fn run(workload: Workload, config: GossipConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (update_send, updates) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = gossip_with_resend(&cluster, timer, updates, config);
        let wire = outputs.wire.sim_cluster_output();
        let merged = outputs.merged.sim_cluster_output();
        let acks_sent = outputs.acks_sent.sim_cluster_output();
        let inbox_depth = outputs.inbox_depth.sim_cluster_output();
        let outstanding_depth = outputs.outstanding_depth.sim_cluster_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        let mut next_value = 0u64;

        flow.sim().with_cluster_size(&cluster, MEMBERS as usize).run_prompt(async move || {
            let mut inbox = vec![0usize; MEMBERS as usize];
            let mut outstanding = vec![0usize; MEMBERS as usize];
            let mut sent: HashSet<(u32, u64, u32)> = HashSet::new();
            let mut merged_ids: HashSet<(u32, u32, u64)> = HashSet::new();
            for round in 0..rounds as u64 {
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    for _ in 0..workload.at(round) {
                        update_send.send(m, next_value);
                        next_value += 1;
                    }
                }
                quiesce().await;

                let mut r = Round::default();
                for m in 0..MEMBERS {
                    while let Some((to, d)) = wire.try_next(m).await {
                        if sent.insert((d.from, d.seq, to)) {
                            r.sent_first += 1
                        } else {
                            r.sent_again += 1
                        }
                    }
                    while let Some(d) = merged.try_next(m).await {
                        if merged_ids.insert((m, d.from, d.seq)) {
                            r.merged_first += 1
                        } else {
                            r.merged_again += 1
                        }
                    }
                    while let Some(_) = acks_sent.try_next(m).await {
                        r.acks_sent += 1;
                    }
                    while let Some(depth) = inbox_depth.try_next(m).await {
                        inbox[m as usize] = depth;
                    }
                    while let Some(depth) = outstanding_depth.try_next(m).await {
                        outstanding[m as usize] = depth;
                    }
                }
                r.inbox_total = inbox.iter().sum();
                r.outstanding_total = outstanding.iter().sum();
                trace_ref.push(r);
            }
        });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    /// Hand-computed expectation, written before measurement.
    ///
    /// Five members, so each member has 4 peers. Baseline is 1 local update per member per
    /// round, so each member sends 4 deltas and receives 4 per round against a merge budget of
    /// 5; a delta is merged the round after it is sent and its acknowledgement clears the
    /// outstanding entry in that same round, a wait of 1 tick against a timeout of 3.
    ///
    /// The trigger raises updates to 2 per member per round for 60 rounds, so 8 deltas arrive
    /// per round against a budget of 5 and each inbox grows by 3 per round to about 180, a
    /// queueing delay of 36 rounds against the 3-round timeout. From then on every member finds,
    /// for every peer, an overdue delta on every tick and re-sends one per peer per tick, so
    /// after the trigger each member receives 4 new deltas plus 4 re-sends per round against a
    /// budget of 5: the inboxes grow by 3 per round forever, acknowledgements never catch up,
    /// and in the tail about half of every member's merges are of deltas it has already merged.
    /// With re-sending disabled, arrivals fall back to 4 per round against 5 and the 180-deep
    /// inbox drains at 1 per round, so the system recovers by about round 340 and every delta is
    /// merged exactly once per member.
    ///
    /// Measured notes. A healthy round ends with 10 deltas in flight rather than 0, because the
    /// members' ticks run in some order within a round and a delta sent by a later member waits
    /// for the next round's budget; the assertions allow one round of in-flight deltas. A run
    /// that varied the trigger length showed the same 13858 re-sends for 20- and 40-round
    /// triggers, because any trigger longer than about 5 rounds (enough to build an inbox of 15,
    /// a 3-round delay) tips the system into the collapsed state, where the program's cap of one
    /// re-send per peer per tick fixes the rate; the collapse run is therefore the confirmation
    /// of the label and there is no separate hold-growth test.
    const WORKLOAD: Workload = Workload {
        baseline_per_round: 1,
        trigger_per_round: 2,
        trigger_start: 100,
        trigger_end: 160,
    };
    const CONFIG: GossipConfig = GossipConfig {
        max_merges_per_tick: 5,
        ack_timeout_ticks: 3,
    };
    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 130, 159, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: sent_first={} sent_again={} merged_first={} merged_again={} acks={} inbox_total={} outstanding_total={}",
                    r.sent_first, r.sent_again, r.merged_first, r.merged_again, r.acks_sent, r.inbox_total, r.outstanding_total
                );
            }
        }
    }

    fn report(name: &str, trace: &[Round]) {
        let n = trace.len();
        println!(
            "{name}: sends first={} again={}; merges first={} again={}; tail merges first={} again={}; inbox total {} -> {} (peak {}); outstanding at end {}",
            sum(trace, 0, n, |r| r.sent_first),
            sum(trace, 0, n, |r| r.sent_again),
            sum(trace, 0, n, |r| r.merged_first),
            sum(trace, 0, n, |r| r.merged_again),
            sum(trace, TAIL_START, n, |r| r.merged_first),
            sum(trace, TAIL_START, n, |r| r.merged_again),
            trace[TAIL_START].inbox_total,
            trace[n - 1].inbox_total,
            trace.iter().map(|r| r.inbox_total).max().unwrap(),
            trace[n - 1].outstanding_total
        );
    }

    /// Deltas sent by peers whose tick ran later in the round wait for the next round's budget,
    /// so a healthy round ends with up to one round of deltas in flight. Measured: 10.
    const IN_FLIGHT: usize = (MEMBERS * (MEMBERS - 1)) as usize;

    fn assert_healthy_pre_trigger(trace: &[Round]) {
        let pre = &trace[10..100];
        let per_round = (MEMBERS * (MEMBERS - 1)) as u64;
        assert!(
            pre.iter().all(|r| r.sent_first == per_round && r.sent_again == 0 && r.merged_first == per_round && r.merged_again == 0 && r.inbox_total <= IN_FLIGHT),
            "the system should be healthy before the trigger"
        );
    }

    #[test]
    fn resends_keep_the_inboxes_from_draining() {
        let trace = run(WORKLOAD, CONFIG, ROUNDS);
        print_trajectory(&trace);
        report("re-send on", &trace);
        assert_healthy_pre_trigger(&trace);

        let tail = &trace[TAIL_START..];
        let tail_first = sum(&trace, TAIL_START, ROUNDS, |r| r.merged_first);
        let tail_again = sum(&trace, TAIL_START, ROUNDS, |r| r.merged_again);
        assert!(tail_again >= tail_first / 2, "a large share of tail merges should be redundant (first={tail_first}, again={tail_again})");
        assert!(sum(&trace, TAIL_START, ROUNDS, |r| r.sent_again) > 0);
        assert!(tail.last().unwrap().inbox_total > tail.first().unwrap().inbox_total, "the inboxes should still be growing");
        assert!(tail.windows(2).all(|w| w[1].inbox_total >= w[0].inbox_total), "inboxes never shrink in the tail");
    }

    /// Control: re-send on, no trigger.
    #[test]
    fn without_a_trigger_nothing_is_resent() {
        let trace = run(
            Workload {
                trigger_per_round: WORKLOAD.baseline_per_round,
                ..WORKLOAD
            },
            CONFIG,
            ROUNDS,
        );
        print_trajectory(&trace);
        report("no trigger", &trace);
        assert!(trace.iter().all(|r| r.sent_again == 0 && r.merged_again == 0 && r.inbox_total <= IN_FLIGHT));
    }

    /// The knob off: same trigger, no re-sends. Every delta is merged exactly once per member,
    /// which is the benign shape.
    #[test]
    fn without_resends_the_inboxes_drain() {
        let trace = run(WORKLOAD, GossipConfig { ack_timeout_ticks: 0, ..CONFIG }, ROUNDS);
        print_trajectory(&trace);
        report("re-send off", &trace);
        assert_healthy_pre_trigger(&trace);
        assert!(trace.iter().all(|r| r.sent_again == 0 && r.merged_again == 0));
        let peak = trace.iter().map(|r| r.inbox_total).max().unwrap();
        let recovered_at = trace.iter().rposition(|r| r.inbox_total > IN_FLIGHT).map(|i| i + 1).unwrap();
        println!("peak inbox total {peak}; inbox last above in-flight before round {recovered_at}");
        assert!(peak > 5 * 100, "the trigger should have built inboxes far past the timeout, got {peak}");
        assert!(trace[TAIL_START..].iter().all(|r| r.inbox_total <= IN_FLIGHT));
        let total_first = sum(&trace, 0, ROUNDS, |r| r.sent_first);
        let total_merged = sum(&trace, 0, ROUNDS, |r| r.merged_first);
        // Every delta is merged exactly once per peer, except the last round's in-flight deltas.
        assert!(total_first - total_merged <= IN_FLIGHT as u64, "sent {total_first}, merged {total_merged}");
        assert!(total_merged >= total_first - IN_FLIGHT as u64);
    }
}
