//! Lease renewal against a single lease server, with clients that re-send an unacknowledged
//! renewal every tick once it is older than a grace period.
//!
//! Each member of a client cluster holds one lease. Every [`ClientPolicy::renew_every_ticks`]
//! ticks of its logical clock (offset by its member id, so that renewals are spread evenly) it
//! starts a renewal period: it sends a `Renewal` whose `seq` is the tick the period began, and
//! keeps it in an outstanding table until the server's `Ack` for that `seq` arrives. A renewal
//! that is still outstanding when the next period begins is superseded, and an `Ack` for a
//! superseded `seq` is ignored. The client's lease is valid while the last matching `Ack` is
//! less than [`ClientPolicy::lease_ticks`] old. The server keeps every incoming job in a FIFO,
//! serves at most [`ServerConfig::max_per_tick`] jobs per element of its clock, and answers every
//! renewal it serves with an `Ack`. A second server input, `other_work`, carries unrelated jobs
//! that share the FIFO; the harness uses it as the trigger.
//!
//! # Mechanism and knob
//!
//! With [`ClientPolicy::resend_after_ticks`] set to `Some(g)`, a client whose outstanding
//! renewal is at least `g` ticks old re-sends it on every tick until it is acknowledged or
//! superseded. A server backlog deep enough to delay acknowledgements past `g` therefore turns
//! every client into a source of one job per tick instead of one job per period, and the
//! re-sends are served after the backlog that caused them, so their acknowledgements are stale
//! by the time they arrive and the clients keep re-sending. The offered load then exceeds the
//! server's capacity by itself, the acknowledgements never catch up, and every lease lapses. The
//! mechanism is present whenever `resend_after_ticks` is `Some`, so that configuration is
//! hazardous. With `None` a client sends exactly one message per renewal period whatever the
//! schedule does, so the program's work is bounded by its clocks and it is benign; superseding a
//! late renewal costs nothing.
//!
//! # Timer parameters
//!
//! - `client_clock` (per client member): one element per logical client tick; renewal periods,
//!   the grace period and lease validity are measured in these ticks.
//! - `server_clock`: one element per server tick; each element grants `max_per_tick` jobs.
//!
//! A deployment wires `clients.source_interval(period)` and `server.source_interval(period)` into
//! them; the simulation feeds them from `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Collapse runs: 20 clients with a 10-tick period start 2 renewals per round against a server
//! that serves 5 jobs per round; the trigger offers 12 other jobs per round during rounds 100 to
//! 160; grace 4 ticks, lease 30 ticks. Under the prompt schedule the server's tick runs before
//! the clients' in each round, so at baseline a renewal is acknowledged one tick after it is sent
//! and the FIFO holds that round's two renewals at the end of every round. Tail is rounds 600 to
//! 800, where baseline acknowledgements would be 400 and there are 4000 client ticks.
//!
//! | run | resend after | trigger | tail acked | tail sent first / re-send | tail served first / re-send | tail lapsed client ticks | backlog at 600 -> 800 | label |
//! |---|---|---|---|---|---|---|---|---|
//! | re-sending | Some(4) | yes | 0 | 400 / 2400 | 143 / 857 | 4000 of 4000 | 5117 -> 6908 | hazardous, collapses |
//! | one outstanding | None | yes | 400 | 400 / 0 | 400 / 0 | 0 | 2 -> 2 (peak 542 at round 159, steady state again from round 339; 4041 lapsed ticks and 436 superseded renewals over the run; 1600 sends for 1600 periods) | benign |
//! | no trigger | Some(4) | no | 400 | 400 / 0 | 400 / 0 | 0 | 2 -> 2 | healthy |
//!
//! Hold runs (`holding_acknowledgements_causes_extra_sends_only_with_resends`): no trigger, 300
//! rounds, with the server's clock withheld for `hold` rounds starting at round 100 so that
//! acknowledgements arrive `hold` ticks late. The column is renewals sent over the run beyond the
//! one per period that the client clocks define (600 for 20 clients over 300 rounds).
//!
//! | hold (rounds) | 0 | 2 | 4 | 8 | 16 | 32 |
//! |---|---|---|---|---|---|---|
//! | extra sends, resend after 4 | 0 | 0 | 2 | 30 | 2311 | 2334 |
//! | extra sends, one outstanding | 0 | 0 | 0 | 0 | 0 | 0 |
//!
//! With re-sends, a hold shorter than the grace period costs nothing, a hold of 8 costs 30 extra
//! sends and then recovers, and a hold of 16 rounds leaves a backlog deep enough that the storm
//! sustains itself for the rest of the run with no trigger at all. With one outstanding renewal
//! the wire carries exactly one message per period however long acknowledgements are held. The
//! collapse run matched the hand computation in `sim_tests` throughout: 1157 queued jobs at the
//! end of the trigger against about 1200 predicted, 6908 at round 800 against about 7000, 14
//! renewals per round of which 12 are re-sends, and every lease lapsed.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct LeaseClient;
pub struct LeaseServer;

/// What a client puts on the wire. `seq` is the client tick at which the renewal period began.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Renewal {
    pub seq: u64,
    /// `true` for every send of a renewal after its first.
    pub resend: bool,
}

/// The server's answer to a renewal it served.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ack {
    pub seq: u64,
}

/// Client-side bookkeeping for the renewal awaiting an acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Outstanding {
    pub first_sent: u64,
    pub last_sent: u64,
}

#[derive(Clone, Copy, Debug)]
enum Verdict {
    Keep((u64, Outstanding)),
    Resend((u64, Outstanding)),
}

/// A renewal whose acknowledgement arrived while it was still outstanding.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Acked {
    pub seq: u64,
    /// Client ticks from the first send to the acknowledgement.
    pub latency_ticks: u64,
}

/// A job in the server's FIFO.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Job {
    Renewal {
        client: MemberId<LeaseClient>,
        seq: u64,
        resend: bool,
    },
    /// Unrelated work that shares the server's FIFO; the harness's trigger.
    Other(u64),
}

#[derive(Clone, Copy, Debug)]
pub struct ClientPolicy {
    /// Length of a renewal period in client ticks.
    pub renew_every_ticks: u64,
    /// A lease is valid while the last acknowledged renewal is younger than this.
    pub lease_ticks: u64,
    /// `Some(g)`: re-send an outstanding renewal on every tick once it is `g` ticks old.
    /// `None`: never re-send; at most one message per renewal period.
    pub resend_after_ticks: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Jobs served per server clock element: the server's capacity.
    pub max_per_tick: u32,
}

pub struct LeaseOutputs<'a> {
    /// Every renewal a client puts on the wire, first sends and re-sends.
    pub sent: Stream<Renewal, Cluster<'a, LeaseClient>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Renewals acknowledged while still outstanding.
    pub acked: Stream<Acked, Cluster<'a, LeaseClient>, Unbounded, NoOrder, ExactlyOnce>,
    /// Renewals superseded by the next period before being acknowledged.
    pub superseded: Stream<u64, Cluster<'a, LeaseClient>, Unbounded, NoOrder, ExactlyOnce>,
    /// Once per client clock element: whether the lease is valid at that tick.
    pub lease_trace: Stream<bool, Cluster<'a, LeaseClient>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every job the server serves, in the order served.
    pub served: Stream<Job, Process<'a, LeaseServer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Depth of the server's FIFO at the end of each server tick.
    pub backlog_trace: Stream<usize, Process<'a, LeaseServer>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the client cluster and the lease server; see the module docs for the clock parameters.
pub fn lease_renewal<'a>(
    clients: &Cluster<'a, LeaseClient>,
    server: &Process<'a, LeaseServer>,
    client_clock: Stream<(), Cluster<'a, LeaseClient>, Unbounded, TotalOrder, ExactlyOnce>,
    server_clock: Stream<(), Process<'a, LeaseServer>, Unbounded, TotalOrder, ExactlyOnce>,
    other_work: Stream<u64, Process<'a, LeaseServer>, Unbounded, TotalOrder, ExactlyOnce>,
    policy: ClientPolicy,
    server_config: ServerConfig,
) -> LeaseOutputs<'a> {
    let ClientPolicy {
        renew_every_ticks,
        lease_ticks,
        resend_after_ticks,
    } = policy;
    // The knob is applied at build time: `None` becomes an age no renewal ever reaches.
    let resend_after = resend_after_ticks.unwrap_or(u64::MAX);
    let ServerConfig { max_per_tick } = server_config;

    // Acknowledgements come back from the server, which is downstream of `sent` (below).
    let (acks_complete, acks) = clients
        .forward_ref::<Stream<Ack, Cluster<'a, LeaseClient>, Unbounded, TotalOrder, ExactlyOnce>>();

    // ---- Client: logical clock, one outstanding renewal, grace-based re-sends -----------------
    let (sent, acked, superseded, lease_trace) = sliced! {
        let clock = use::batch(client_clock.enumerate(), nondet!(/** batching only shifts which client tick observes a clock element; every element still advances the clock */));
        let acks = use::batch(acks, nondet!(/** batching only affects when an acknowledgement is observed, and so whether a re-send fires first */));
        let mut outstanding = use::state_null::<KeyedSingleton<u64, Outstanding, Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut last_ack_at = use::state(|l| l.singleton(q!(0u64)));

        // The logical clock, and whether this tick of the dataflow advanced it. A tick that ran
        // only because acknowledgements arrived starts no period and re-sends nothing.
        let ticked = clock.clone().count().map(q!(|n| n > 0));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        // An acknowledgement for the outstanding seq completes it; any other is stale.
        let acked_seqs = acks.map(q!(|a| a.seq)).unique();
        let acked = acked_seqs
            .clone()
            .map(q!(|seq| (seq, ())))
            .into_keyed()
            .join_keyed_singleton(outstanding.clone())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((seq, (_, o)), now)| Acked {
                seq,
                latency_ticks: now - o.first_sent,
            }));
        let got_ack = acked.clone().count().map(q!(|n| n > 0));
        last_ack_at = now_cur.clone().filter_if(got_ack).unwrap_or(last_ack_at.clone());

        // A renewal period begins when the clock reaches a multiple of the period, offset by the
        // member id so that the cluster's renewals are spread over the period.
        let due = now_cur
            .clone()
            .filter(q!(move |now| (*now + CLUSTER_SELF_ID.get_raw_id() as u64) % renew_every_ticks == 0))
            .filter_if(ticked.clone());

        // Whatever is still waiting is superseded if a new period begins, and otherwise judged
        // against the grace period. `now > last_sent` keeps re-sends to one per tick.
        let waiting = outstanding.filter_key_not_in(acked_seqs).into_keyed_stream().entries();
        let superseded = waiting.clone().filter_if(due.clone().is_some()).map(q!(|(seq, _)| seq));
        let judged = waiting
            .filter_if(due.clone().is_none())
            .cross_singleton(now_cur.clone())
            .map(q!(move |((seq, o), now)| {
                if now - o.first_sent >= resend_after && now > o.last_sent {
                    Verdict::Resend((seq, Outstanding { first_sent: o.first_sent, last_sent: now }))
                } else {
                    Verdict::Keep((seq, o))
                }
            }));
        let kept = judged.clone().filter_map(q!(|v| match v {
            Verdict::Keep(x) => Some(x),
            _ => None,
        }));
        let resent = judged.filter_map(q!(|v| match v {
            Verdict::Resend(x) => Some(x),
            _ => None,
        }));
        let started = due
            .map(q!(|now| (now, Outstanding { first_sent: now, last_sent: now })))
            .into_stream();

        // `kept`, `resent` and `started` have pairwise distinct keys, so `first()` is exact; the
        // sort only restores the total order that `entries()` gave up.
        outstanding = kept.chain(resent.clone()).chain(started.clone()).sort().into_keyed().first();

        let sent = started
            .map(q!(|(seq, _)| Renewal { seq, resend: false }))
            .chain(resent.map(q!(|(seq, _)| Renewal { seq, resend: true })))
            .sort();

        let lease_valid = now_cur
            .zip(last_ack_at.clone())
            .map(q!(move |(now, last)| now - last < lease_ticks))
            .filter_if(ticked);

        (sent, acked, superseded, lease_valid.into_stream())
    };

    // ---- Server: one FIFO for renewals and other work, bounded jobs per tick -----------------
    let incoming = sent.clone().send(server, TCP.fail_stop().bincode());

    let (served, backlog_trace) = sliced! {
        let clock = use::batch(server_clock, nondet!(/** batching only decides how many ticks' capacity one dataflow tick spends; every element still grants one */));
        let renewals = use::batch(incoming.entries(), nondet!(/** arrivals are appended to the FIFO; batching decides how many share a tick, and so how long they wait */));
        let other = use::batch(other_work, nondet!(/** same as for renewals */));
        let mut backlog = use::state_null::<Stream<Job, Tick<_>, Bounded, TotalOrder>>();

        let budget = clock.count().map(q!(move |n| n * max_per_tick as usize));
        // Within a tick, other work is queued before renewals, and renewals in (client, seq)
        // order, so the FIFO is the same under every delivery order.
        let arrivals = other.map(q!(|w| Job::Other(w))).chain(
            renewals
                .map(q!(|(client, r)| Job::Renewal {
                    client,
                    seq: r.seq,
                    resend: r.resend,
                }))
                .sort(),
        );
        let queued = backlog.chain(arrivals).enumerate().cross_singleton(budget);
        let served = queued.clone().filter_map(q!(|((i, job), budget)| (i < budget).then_some(job)));
        backlog = queued.filter_map(q!(|((i, job), budget)| (i >= budget).then_some(job)));

        (served, backlog.clone().count().into_stream())
    };

    acks_complete.complete(
        served
            .clone()
            .filter_map(q!(|job| match job {
                Job::Renewal { client, seq, .. } => Some((client, Ack { seq })),
                Job::Other(_) => None,
            }))
            .demux(clients, TCP.fail_stop().bincode()),
    );

    LeaseOutputs {
        sent,
        acked,
        superseded,
        lease_trace,
        served,
        backlog_trace,
    }
}

/// The harness's trigger: unrelated jobs offered to the server per round, with a window in
/// which the rate is higher.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_other_per_round: u32,
    pub trigger_other_per_round: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn other_at(&self, round: u64) -> u32 {
        if round >= self.trigger_start && round < self.trigger_end {
            self.trigger_other_per_round
        } else {
            self.baseline_other_per_round
        }
    }
}

/// Simulation under a fixed, fair schedule. A *round* is one element on every client's clock,
/// one element on the server's clock, and that round's other work, followed by letting the
/// simulation quiesce. Per round each client runs a tick (start or re-send a renewal), the server
/// runs a tick (serve up to `max_per_tick`), and clients run again to observe acknowledgements.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Renewals acknowledged while outstanding, over all clients.
        acked: u64,
        latency_sum: u64,
        /// First sends and re-sends put on the wire this round, over all clients.
        sent_first: u64,
        sent_resend: u64,
        /// Renewals superseded before acknowledgement.
        superseded: u64,
        /// Client ticks this round at which the lease was lapsed.
        lapsed_ticks: u64,
        /// Jobs the server served this round, by kind.
        served_first: u64,
        served_resend: u64,
        served_other: u64,
        /// Server FIFO depth after this round's server tick (carried over if it did not run).
        backlog: usize,
    }

    fn run(n: u32, workload: Workload, policy: ClientPolicy, server_config: ServerConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let clients = flow.cluster::<LeaseClient>();
        let server = flow.process::<LeaseServer>();

        let (client_clock_send, client_clock) = clients.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_clock_send, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (other_send, other_work) = server.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outputs = lease_renewal(&clients, &server, client_clock, server_clock, other_work, policy, server_config);
        let sent = outputs.sent.sim_cluster_output();
        let acked = outputs.acked.sim_cluster_output();
        let superseded = outputs.superseded.sim_cluster_output();
        let lease_trace = outputs.lease_trace.sim_cluster_output();
        let served = outputs.served.sim_output();
        let backlog_trace = outputs.backlog_trace.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim()
            .with_cluster_size(&clients, n as usize)
            .run_prompt(async move || {
                let mut backlog = 0usize;
                let mut other_id = 0u64;
                for round in 0..rounds as u64 {
                    for member in 0..n {
                        client_clock_send.send(member, ());
                    }
                    server_clock_send.send(());
                    for _ in 0..workload.other_at(round) {
                        other_send.send(other_id);
                        other_id += 1;
                    }
                    quiesce().await;

                    let mut r = Round::default();
                    for member in 0..n {
                        for a in acked.collect_sorted::<Vec<Acked>>(member).await {
                            r.acked += 1;
                            r.latency_sum += a.latency_ticks;
                        }
                        for s in sent.collect::<Vec<Renewal>>(member).await {
                            if s.resend {
                                r.sent_resend += 1
                            } else {
                                r.sent_first += 1
                            }
                        }
                        r.superseded += superseded.collect_sorted::<Vec<u64>>(member).await.len() as u64;
                        for valid in lease_trace.collect::<Vec<bool>>(member).await {
                            if !valid {
                                r.lapsed_ticks += 1
                            }
                        }
                    }
                    while let Some(job) = served.try_next().await {
                        match job {
                            Job::Renewal { resend: false, .. } => r.served_first += 1,
                            Job::Renewal { resend: true, .. } => r.served_resend += 1,
                            Job::Other(_) => r.served_other += 1,
                        }
                    }
                    while let Some(depth) = backlog_trace.try_next().await {
                        backlog = depth;
                    }
                    r.backlog = backlog;
                    trace_ref.push(r);
                }
            });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Twenty clients with a period of 10 ticks, offset by member id, start exactly 2 renewals
    /// per round; the server serves 5 jobs per round (40% utilization), so at baseline every
    /// renewal is acknowledged in the round it was sent, the backlog is empty, nothing is re-sent
    /// or superseded, and no lease lapses.
    ///
    /// The trigger offers 12 other jobs per round for 60 rounds, so the FIFO grows by 9 per round
    /// at first. Once it is deeper than 20 jobs (round 103 or so) acknowledgements take longer
    /// than the 4-tick grace period, and with re-sends enabled every client with an outstanding
    /// renewal sends one more copy per tick until its period ends: 6 re-sends per period, so 7
    /// sends per client per period and 14 renewals per round into a server that serves 5. With
    /// the other work that is 26 arrivals against 5 served, so the backlog should be near 1200 by
    /// round 160 and grow by 9 per round from then on, reaching about 7000 by round 800. Every
    /// renewal the server serves is stale by the time it is served (the FIFO delay exceeds the
    /// 10-tick period from about round 106), so acknowledgements complete nothing, and every lease
    /// lapses once its last good acknowledgement is 30 ticks old, around round 135. In rounds 600
    /// to 800 the expectation is 0 acknowledged renewals against a baseline of 400, 400 first
    /// sends and about 2400 re-sends, about 1000 served jobs of which about six in seven are
    /// re-sends, all 4000 client ticks lapsed, and a backlog that never shrinks.
    ///
    /// With re-sends disabled the same trigger builds a backlog of about 540 by round 160, which
    /// drains at 3 per round and is empty by about round 340. Acknowledgements are stale while
    /// the backlog exceeds 50, so renewals are superseded and leases lapse from about round 136
    /// until about round 330, after which everything returns to baseline: in the tail 400
    /// acknowledged, 400 sent, no re-sends, no lapses, and 1600 sends over the whole run for the
    /// 1600 renewal periods the clocks define.
    const N: u32 = 20;
    const WORKLOAD: Workload = Workload {
        baseline_other_per_round: 0,
        trigger_other_per_round: 12,
        trigger_start: 100,
        trigger_end: 160,
    };
    const RESENDING: ClientPolicy = ClientPolicy {
        renew_every_ticks: 10,
        lease_ticks: 30,
        resend_after_ticks: Some(4),
    };
    const ONE_OUTSTANDING: ClientPolicy = ClientPolicy {
        resend_after_ticks: None,
        ..RESENDING
    };
    const SERVER: ServerConfig = ServerConfig { max_per_tick: 5 };

    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 103, 110, 130, 159, 160, 200, 250, 300, 350, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: acked={} sent_first={} sent_resend={} superseded={} lapsed_ticks={} served first={} resend={} other={} backlog={}",
                    r.acked, r.sent_first, r.sent_resend, r.superseded, r.lapsed_ticks, r.served_first, r.served_resend, r.served_other, r.backlog
                );
            }
        }
    }

    /// The healthy steady state. Under the prompt schedule the server's tick runs before the
    /// clients' in every round, so a renewal is served by the next round's server tick: the FIFO
    /// holds that round's two renewals at the end of every round and every acknowledgement takes
    /// one tick.
    fn assert_healthy(trace: &[Round], from: usize, to: usize) {
        for (i, r) in trace[from..to].iter().enumerate() {
            let i = i + from;
            assert!(r.backlog <= 2, "round {i}: only this round's renewals should be waiting, got {}", r.backlog);
            assert_eq!(r.sent_first, 2, "round {i}: two renewal periods begin per round");
            assert_eq!(r.sent_resend, 0, "round {i}: nothing to re-send");
            assert_eq!(r.acked, 2, "round {i}: the previous round's two renewals are acknowledged");
            assert_eq!(r.latency_sum, 2, "round {i}: each acknowledgement takes one tick");
            assert_eq!(r.superseded, 0, "round {i}: nothing superseded");
            assert_eq!(r.lapsed_ticks, 0, "round {i}: no lease lapses");
        }
    }

    #[test]
    fn resends_cause_a_renewal_storm_that_never_ends() {
        let trace = run(N, WORKLOAD, RESENDING, SERVER, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 2, 100);

        let tail = &trace[TAIL_START..];
        let tail_acked = sum(&trace, TAIL_START, ROUNDS, |r| r.acked);
        let tail_first = sum(&trace, TAIL_START, ROUNDS, |r| r.sent_first);
        let tail_resend = sum(&trace, TAIL_START, ROUNDS, |r| r.sent_resend);
        let tail_served_first = sum(&trace, TAIL_START, ROUNDS, |r| r.served_first);
        let tail_served_resend = sum(&trace, TAIL_START, ROUNDS, |r| r.served_resend);
        let tail_lapsed = sum(&trace, TAIL_START, ROUNDS, |r| r.lapsed_ticks);
        let baseline_acked = 2 * (ROUNDS - TAIL_START) as u64;
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): acked {tail_acked} (baseline would be {baseline_acked}), sent first={tail_first} resend={tail_resend}, served first={tail_served_first} resend={tail_served_resend}, lapsed client ticks {tail_lapsed} of {}, backlog {} -> {}",
            N as u64 * (ROUNDS - TAIL_START) as u64,
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        assert!(tail_acked < baseline_acked / 4, "acknowledgements should have collapsed: {tail_acked} vs {baseline_acked}");
        assert!(tail_resend > tail_first, "the wire should carry mostly re-sends (first={tail_first}, resend={tail_resend})");
        assert!(tail_served_resend > tail_served_first, "the server should spend most of its capacity on re-sends");
        assert!(tail_lapsed > N as u64 * (ROUNDS - TAIL_START) as u64 / 2, "most leases should be lapsed in the tail");
        assert!(tail.last().unwrap().backlog > tail.first().unwrap().backlog, "the backlog should still be growing");
        assert!(tail.windows(2).all(|w| w[1].backlog >= w[0].backlog), "backlog never shrinks in the tail");
    }

    /// Control: re-sends armed, no trigger. Nothing is ever late.
    #[test]
    fn without_a_trigger_the_system_stays_healthy() {
        let trace = run(
            N,
            Workload {
                trigger_other_per_round: WORKLOAD.baseline_other_per_round,
                ..WORKLOAD
            },
            RESENDING,
            SERVER,
            ROUNDS,
        );
        print_trajectory(&trace);
        assert_healthy(&trace, 2, ROUNDS);
    }

    /// The benign twin: same trigger, at most one message per renewal period. Leases lapse while
    /// the backlog drains and everything recovers; total sends equal the periods the clocks define.
    #[test]
    fn with_one_outstanding_renewal_the_system_recovers() {
        let trace = run(N, WORKLOAD, ONE_OUTSTANDING, SERVER, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 2, 100);
        assert!(trace.iter().all(|r| r.sent_resend == 0 && r.served_resend == 0));
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        let empty_from = trace[160..].iter().position(|r| r.backlog <= 2).map(|i| i + 160);
        let lapsed_total = sum(&trace, 0, ROUNDS, |r| r.lapsed_ticks);
        let superseded_total = sum(&trace, 0, ROUNDS, |r| r.superseded);
        let sent_total = sum(&trace, 0, ROUNDS, |r| r.sent_first);
        println!("peak backlog {peak}; back to steady state from round {empty_from:?}; lapsed client ticks {lapsed_total}; superseded {superseded_total}; sent {sent_total}");
        assert!(peak > 400, "the trigger should have built a backlog, got {peak}");
        assert!(lapsed_total > 0, "leases should lapse while the backlog drains");
        assert_eq!(sent_total, N as u64 * ROUNDS as u64 / RESENDING.renew_every_ticks, "exactly one send per renewal period");
        assert_healthy(&trace, TAIL_START, ROUNDS);
    }

    /// Runs the trigger-free workload but withholds the server's clock for `hold` rounds starting
    /// at round 100, so that acknowledgements are delayed by `hold` ticks while clients keep
    /// ticking. Returns the renewals put on the wire over the run beyond the one per period that
    /// the client clocks define.
    fn extra_sends_with_hold(policy: ClientPolicy, hold: usize, rounds: usize) -> u64 {
        const HOLD_AT: usize = 100;
        let mut flow = FlowBuilder::new();
        let clients = flow.cluster::<LeaseClient>();
        let server = flow.process::<LeaseServer>();
        let (client_clock_send, client_clock) = clients.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_clock_send, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_other_send, other_work) = server.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = lease_renewal(&clients, &server, client_clock, server_clock, other_work, policy, SERVER);
        let sent = outputs.sent.sim_cluster_output();

        let mut total = 0u64;
        let total_ref = &mut total;
        flow.sim().with_cluster_size(&clients, N as usize).run_prompt(async move || {
            for round in 0..rounds {
                for member in 0..N {
                    client_clock_send.send(member, ());
                }
                let held = round >= HOLD_AT && round < HOLD_AT + hold;
                let grants = if held { 0 } else if round == HOLD_AT + hold { hold + 1 } else { 1 };
                for _ in 0..grants {
                    server_clock_send.send(());
                }
                quiesce().await;
                for member in 0..N {
                    *total_ref += sent.collect::<Vec<Renewal>>(member).await.len() as u64;
                }
            }
        });
        let required = N as u64 * rounds as u64 / policy.renew_every_ticks;
        assert!(total >= required, "every period sends at least once");
        total - required
    }

    /// Hazard confirmation by holding the server's clock, with no trigger. Expected before
    /// measuring: with re-sends, zero extra sends for holds below the 4-tick grace period and an
    /// extra that grows with the hold above it (about 20 clients times the ticks by which the hold
    /// exceeds the grace, capped by superseding at 6 per client per period, and continuing after
    /// the release if the resulting backlog keeps acknowledgements later than the grace); with one
    /// outstanding renewal, exactly zero extra sends at every hold.
    #[test]
    fn holding_acknowledgements_causes_extra_sends_only_with_resends() {
        const RUN: usize = 300;
        let holds = [0usize, 2, 4, 8, 16, 32];
        let resending: Vec<u64> = holds.iter().map(|&h| extra_sends_with_hold(RESENDING, h, RUN)).collect();
        let one_outstanding: Vec<u64> = holds.iter().map(|&h| extra_sends_with_hold(ONE_OUTSTANDING, h, RUN)).collect();
        for ((h, r), o) in holds.iter().zip(&resending).zip(&one_outstanding) {
            println!("hold {h} rounds -> extra sends: resend after 4 = {r}, one outstanding = {o}");
        }
        assert!(one_outstanding.iter().all(|&e| e == 0), "one outstanding renewal never sends extra: {one_outstanding:?}");
        assert_eq!(resending[0], 0);
        assert!(resending.windows(2).all(|w| w[1] >= w[0]), "extra sends should not shrink as the hold grows: {resending:?}");
        assert!(resending.last().unwrap() > &0, "a long hold should cause re-sends: {resending:?}");
    }
}
