//! W5: heartbeat-timeout leader election with a follower stampede.
//!
//! A cluster of members runs a small leader-election protocol with no log. The member with raw
//! id 0 starts as leader in term 1 and, on every one of its timer elements, sends a heartbeat to
//! every other member. Every member keeps a FIFO inbox of the messages it has received
//! (heartbeats, vote requests, vote grants, and client requests) and processes at most
//! [`ElectionConfig::budget_per_tick`] of them per timer element; that budget is the member's
//! capacity. A follower or candidate that has processed no valid heartbeat for
//! `election_timeout` ticks starts an election: it increments its term, votes for itself, and
//! sends a vote request to every other member. A member grants a vote to the first request it
//! processes for a term higher than any it has seen, and a candidate that collects a majority
//! becomes leader and starts heartbeating. Client requests carry no protocol meaning; they cost
//! one unit of budget each and are the harness's way of loading the members.
//!
//! The mechanism is the election timeout, and the label is **hazardous** in both configurations:
//! whenever the schedule holds heartbeats long enough, every follower starts an election the
//! input never asked for, and each election sends a vote request to every peer and costs every
//! inbox budget to process. When client requests fill the inboxes, heartbeats wait behind them,
//! every follower's timeout fires in the same tick, and all of them become candidates for the
//! same term, so no one gets a majority; each further timeout repeats the stampede at a higher
//! term. The knob is [`ElectionConfig::timeout_spread_ticks`]: when it is zero all members use
//! the same timeout; when it is positive member `i` waits `election_timeout + i * spread`, which
//! shortens the storm once the inboxes drain but does not prevent it.
//!
//! The per-message state transitions live in [`step`], a plain function applied once per tick to
//! the messages the budget admits; the inbox, the budget, and the timeout are in the dataflow.
//!
//! # Timer parameters
//!
//! - `timer`: one element per logical tick at each member. A deployment wires
//!   `cluster.source_interval(period)` into it; the simulation feeds it per member from
//!   `sim_input`. Each element advances that member's clock by one and releases one budget of
//!   inbox processing.
//!
//! # Measured
//!
//! Five members, budget 3 inbox messages per member per round, election timeout 6, baseline
//! load 1 client request per member per round plus heartbeats, 800 rounds, tail = rounds
//! 600..800. The trigger is 30 client requests per member per round in rounds 100..120. The
//! required work is the heartbeats and client requests; elections and vote requests are work the
//! schedule caused.
//!
//! | run | elections started | vote requests sent | peak inbox total | leaderless rounds | last unhealthy round | final term | tail heartbeats processed (baseline 800) | label |
//! |---|---|---|---|---|---|---|---|---|
//! | uniform timeouts, trigger | 353 | 1412 | 2828 | > 100 | 579 | 80 | 800 | hazardous |
//! | uniform timeouts, no trigger | 0 | 0 | 0 | 0 | none | 1 | 800 | (control) |
//! | spread 3, trigger | 128 | 512 | 2788 | > 100 | 497 | 51 | 800 | hazardous |
//!
//! Hold run (uniform timeouts; the hold is the length of the trigger window, since the inbox
//! backlog it builds is what holds heartbeats past the timeout):
//!
//! | trigger length (rounds) | elections started | vote requests sent | last unhealthy round |
//! |---|---|---|---|
//! | 10 | 173 | 692 | 362 |
//! | 20 | 353 | 1412 | 579 |
//! | 30 | 537 | 2148 | 796 |
//!
//! The expectation that uniform timeouts would keep the followers split forever was overturned:
//! the members are not perfectly symmetric (the old leader's inbox differs, and vote grants
//! break ties), so once the inboxes drain a candidate eventually wins. The stampede costs
//! hundreds of elections either way, and the cost grows with the hold.

use std::marker::PhantomData;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, ClusterIds};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Location, MemberId};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Node;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Msg {
    Heartbeat { term: u64, from: u32 },
    VoteRequest { term: u64, from: u32 },
    VoteGranted { term: u64, from: u32 },
    /// Harness load. Costs one unit of budget and has no other effect.
    Client { id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    pub id: u32,
    pub term: u64,
    pub role: Role,
    /// The tick at which this member last processed a valid heartbeat or started an election.
    pub last_heartbeat: u64,
    /// Votes collected in the current term, counting the vote for itself.
    pub votes: u32,
    /// The highest term in which this member has voted (for itself or another).
    pub voted_in_term: u64,
}

impl NodeState {
    pub fn initial(id: u32) -> Self {
        NodeState {
            id,
            term: 1,
            role: if id == 0 { Role::Leader } else { Role::Follower },
            last_heartbeat: 0,
            votes: 0,
            voted_in_term: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ElectionConfig {
    /// Ticks without a valid heartbeat before a non-leader starts an election.
    pub election_timeout_ticks: u64,
    /// The mechanism knob. Member `i` uses `election_timeout_ticks + i * timeout_spread_ticks`;
    /// zero means every member uses the same timeout.
    pub timeout_spread_ticks: u64,
    /// Inbox messages processed per timer element: the member's capacity.
    pub budget_per_tick: u32,
}

/// One tick's worth of state transitions. `msgs` are the inbox messages the budget admitted, in
/// FIFO order; `pump` says whether this tick carried a timer element, which is when a leader
/// heartbeats and timeouts are judged. Returns the new state, the messages to send as
/// `(destination raw id, message)`, and the term of an election started in this tick, if any.
pub fn step(
    mut st: NodeState,
    msgs: Vec<Msg>,
    now: u64,
    pump: bool,
    peers: &[u32],
    election_timeout: u64,
    timeout_spread: u64,
) -> (NodeState, Vec<(u32, Msg)>, Option<u64>) {
    let mut out = Vec::new();
    let majority = (peers.len() as u32 + 1) / 2 + 1;
    for m in msgs {
        match m {
            Msg::Heartbeat { term, from: _ } if term >= st.term => {
                st.term = term;
                st.role = Role::Follower;
                st.last_heartbeat = now;
                st.votes = 0;
            }
            Msg::VoteRequest { term, from } if term > st.term && term > st.voted_in_term => {
                st.term = term;
                st.role = Role::Follower;
                st.voted_in_term = term;
                st.last_heartbeat = now;
                st.votes = 0;
                out.push((from, Msg::VoteGranted { term, from: st.id }));
            }
            Msg::VoteGranted { term, from: _ } if term == st.term && st.role == Role::Candidate => {
                st.votes += 1;
                if st.votes >= majority {
                    st.role = Role::Leader;
                }
            }
            _ => {}
        }
    }
    let mut started = None;
    if pump {
        let timeout = election_timeout + st.id as u64 * timeout_spread;
        if st.role != Role::Leader && now - st.last_heartbeat >= timeout {
            st.term += 1;
            st.role = Role::Candidate;
            st.voted_in_term = st.term;
            st.votes = 1;
            st.last_heartbeat = now;
            started = Some(st.term);
            for p in peers {
                out.push((*p, Msg::VoteRequest { term: st.term, from: st.id }));
            }
        }
        if st.role == Role::Leader {
            for p in peers {
                out.push((*p, Msg::Heartbeat { term: st.term, from: st.id }));
            }
        }
    }
    (st, out, started)
}

pub struct Outputs<'a> {
    /// Every message a member puts on the wire, as `(destination raw id, message)`.
    pub wire: Stream<(u32, Msg), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every inbox message a member processes.
    pub processed: Stream<Msg, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Inbox depth at the end of each tick.
    pub inbox_depth: Stream<usize, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// `(term, role)` at the end of each tick.
    pub state_trace: Stream<(u64, Role), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    /// The term of each election a member starts.
    pub elections: Stream<u64, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
}

pub fn election<'a>(
    cluster: &Cluster<'a, Node>,
    timer: Stream<(), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    client_requests: Stream<u64, Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
    config: ElectionConfig,
) -> Outputs<'a> {
    let ElectionConfig {
        election_timeout_ticks,
        timeout_spread_ticks,
        budget_per_tick,
    } = config;

    let LocationId::Cluster(cluster_key) = Location::id(cluster) else {
        unreachable!("election runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    let (incoming_complete, incoming) =
        cluster.forward_ref::<Stream<Msg, Cluster<'a, Node>, Unbounded, NoOrder, ExactlyOnce>>();

    let (wire, processed, inbox_depth, state_trace, elections) = sliced! {
        let clock = use::batch(timer.enumerate(), nondet!(/** batching only shifts which tick observes a timer element; each element still advances the clock and releases one budget */));
        let arrivals = use::batch(incoming, nondet!(/** how long a message waits before the inbox sees it; this is the delay the mechanism depends on */));
        let clients = use::batch(client_requests, nondet!(/** which tick appends a client request to the inbox */));
        let mut inbox = use::state_null::<Stream<Msg, Tick<_>, Bounded, TotalOrder>>();
        let mut state = use::state(|l| l.singleton(q!(crate::cluster::witnesses::election_stampede::NodeState::initial(CLUSTER_SELF_ID.get_raw_id()))));
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        // The other members' raw ids: deploy-time metadata, identical on every member and never
        // reassigned, so this state is a constant.
        let mut peers = use::state(|l| l.singleton(q!({
            let me = CLUSTER_SELF_ID.get_raw_id();
            cluster_members
                .iter()
                .map(|id| MemberId::<crate::cluster::witnesses::election_stampede::Node>::from_tagless(id.clone()).get_raw_id())
                .filter(|id| *id != me)
                .collect::<Vec<u32>>()
        })));

        let now_cur = clock.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        // One budget per timer element; a tick that only received messages processes nothing.
        let pump = clock.count().map(q!(|c| c > 0));
        let take = pump.clone().map(q!(move |p| if p { budget_per_tick as usize } else { 0 }));

        // The inbox is a FIFO shared by every kind of message. Arrivals within a tick are sorted
        // so the order does not depend on how the network interleaved them.
        let queued = inbox
            .chain(arrivals.sort())
            .chain(clients.map(q!(|id| Msg::Client { id })))
            .enumerate();
        let served = queued
            .clone()
            .cross_singleton(take.clone())
            .filter_map(q!(|((i, m), take)| if i < take { Some(m) } else { None }));
        inbox = queued
            .cross_singleton(take)
            .filter_map(q!(|((i, m), take)| if i >= take { Some(m) } else { None }));

        let served_vec = served.clone().fold(q!(Vec::new), q!(|v, m| v.push(m)));
        let stepped = state
            .clone()
            .zip(served_vec)
            .zip(now_cur.clone())
            .zip(pump)
            .zip(peers.clone())
            .map(q!(move |((((st, msgs), now), pump), peers)| {
                crate::cluster::witnesses::election_stampede::step(
                    st,
                    msgs,
                    now,
                    pump,
                    &peers,
                    election_timeout_ticks,
                    timeout_spread_ticks,
                )
            }));
        state = stepped.clone().map(q!(|(st, _, _)| st));
        peers = peers.clone();

        let wire = stepped.clone().flat_map_ordered(q!(|(_, out, _)| out));
        let elections = stepped.clone().filter_map(q!(|(_, _, started)| started)).into_stream();
        let state_trace = state.clone().map(q!(|st| (st.term, st.role))).into_stream();

        (wire, served, inbox.clone().count().into_stream(), state_trace, elections)
    };

    incoming_complete.complete(
        wire.clone()
            .map(q!(|(dest, m)| (MemberId::<Node>::from_raw_id(dest), m)))
            .into_keyed()
            .demux(cluster, TCP.fail_stop().bincode())
            .values(),
    );

    Outputs {
        wire,
        processed,
        inbox_depth,
        state_trace,
        elections,
    }
}

/// Client requests per member per round, higher inside the trigger window. This lives in the
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

/// One round is one timer element at every member plus that round's client requests to every
/// member, then `quiesce`. Per round each member processes one budget of its inbox, the leader
/// sends one heartbeat to each peer, and timeouts are judged once.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    const MEMBERS: u32 = 5;

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Heartbeats processed by followers this round; the liveness signal. Baseline is
        /// `MEMBERS - 1` per round.
        heartbeats_processed: u64,
        /// Vote requests put on the wire this round.
        vote_requests_sent: u64,
        /// Heartbeats put on the wire this round.
        heartbeats_sent: u64,
        /// Elections started this round, across members.
        elections_started: u64,
        /// Client requests processed this round, across members.
        clients_processed: u64,
        /// Highest term any member is in at the end of the round.
        max_term: u64,
        /// Members whose role is `Leader` at the end of the round.
        leaders: u64,
        /// Sum of inbox depths over members at the end of the round.
        inbox_total: usize,
    }

    fn run(workload: Workload, config: ElectionConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (client_send, clients) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = election(&cluster, timer, clients, config);
        let wire = outputs.wire.sim_cluster_output();
        let processed = outputs.processed.sim_cluster_output();
        let inbox_depth = outputs.inbox_depth.sim_cluster_output();
        let state_trace = outputs.state_trace.sim_cluster_output();
        let elections = outputs.elections.sim_cluster_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        let mut next_client_id = 0u64;

        flow.sim().with_cluster_size(&cluster, MEMBERS as usize).run_prompt(async move || {
            let mut inbox = vec![0usize; MEMBERS as usize];
            let mut role = vec![(1u64, Role::Follower); MEMBERS as usize];
            role[0].1 = Role::Leader;
            for round in 0..rounds as u64 {
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    for _ in 0..workload.at(round) {
                        client_send.send(m, next_client_id);
                        next_client_id += 1;
                    }
                }
                quiesce().await;

                let mut r = Round::default();
                for m in 0..MEMBERS {
                    while let Some((_, msg)) = wire.try_next(m).await {
                        match msg {
                            Msg::VoteRequest { .. } => r.vote_requests_sent += 1,
                            Msg::Heartbeat { .. } => r.heartbeats_sent += 1,
                            _ => {}
                        }
                    }
                    while let Some(msg) = processed.try_next(m).await {
                        match msg {
                            Msg::Heartbeat { .. } => r.heartbeats_processed += 1,
                            Msg::Client { .. } => r.clients_processed += 1,
                            _ => {}
                        }
                    }
                    while let Some(_) = elections.try_next(m).await {
                        r.elections_started += 1;
                    }
                    while let Some(depth) = inbox_depth.try_next(m).await {
                        inbox[m as usize] = depth;
                    }
                    while let Some(s) = state_trace.try_next(m).await {
                        role[m as usize] = s;
                    }
                }
                r.max_term = role.iter().map(|(t, _)| *t).max().unwrap();
                r.leaders = role.iter().filter(|(_, r)| *r == Role::Leader).count() as u64;
                r.inbox_total = inbox.iter().sum();
                trace_ref.push(r);
            }
        });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    /// Hand-computed expectation, written before measurement, followed by the correction the
    /// first measurement forced.
    ///
    /// Five members. Each member's budget is 3 inbox messages per round. Baseline load is one
    /// client request per member per round plus, at each follower, one heartbeat, so a follower
    /// processes 2 of its budget of 3 and heartbeats are seen the round after they are sent.
    ///
    /// First attempt (8 client requests per member per round for 60 rounds): I expected the
    /// heartbeats to queue behind client requests and every follower to time out around round
    /// 109. Measured: no election ever fired. Heartbeats carry no timestamp, so a heartbeat
    /// processed late still resets the follower's timer, and with heartbeats being 1 of every 9
    /// inbox messages a follower still processed one every 3 rounds, inside the 6-round timeout.
    /// The timeout fires only when the heartbeat processing rate `budget / (c + 1)` drops below
    /// `1 / timeout`, i.e. when `c > budget x timeout - 1 = 17` client requests per round.
    ///
    /// Corrected trigger: 30 client requests per member per round for 20 rounds (rounds 100..120).
    /// Arrivals are 31 per round against a budget of 3, so each inbox grows by 28 per round to
    /// about 560, and a follower processes a heartbeat only every 31 / 3, about 10, rounds,
    /// which exceeds the 6-round timeout. All four followers therefore start an election for
    /// the same term in the same round. Every follower votes for itself and refuses the others'
    /// requests for the same term, so no candidate reaches a majority of 3, and the followers
    /// repeat the stampede every 6 rounds at a new term for as long as they stay synchronized,
    /// which under a deterministic schedule is forever. Each stampede puts 4 vote requests into
    /// every inbox. After the trigger ends, arrivals at each member are 1 client request plus
    /// about 4/6 vote requests per round against a budget of 3, so the inboxes drain at about
    /// 1.3 per round and are empty by about round 540, but with uniform timeouts no leader is
    /// ever elected: the tail (rounds 600..800) should show zero heartbeats processed against a
    /// baseline of 4 per round, elections continuing at about 4 per 6 rounds, and the term
    /// still rising at the end.
    ///
    /// With a spread of 3 ticks member `i` times out after `6 + 3i` rounds, so once the inboxes
    /// have drained (by about round 470) the member with the shortest timeout, which also holds
    /// the highest term because it has timed out most often, sends a request that every other
    /// member grants, and it becomes leader. The tail should then be healthy: 4 heartbeats
    /// processed per round, one leader, no elections, empty inboxes.
    const WORKLOAD: Workload = Workload {
        baseline_per_round: 1,
        trigger_per_round: 30,
        trigger_start: 100,
        trigger_end: 120,
    };
    const CONFIG: ElectionConfig = ElectionConfig {
        election_timeout_ticks: 6,
        timeout_spread_ticks: 0,
        budget_per_tick: 3,
    };
    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 105, 110, 119, 130, 160, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: hb_processed={} hb_sent={} vote_req_sent={} elections={} clients_processed={} max_term={} leaders={} inbox_total={}",
                    r.heartbeats_processed, r.heartbeats_sent, r.vote_requests_sent, r.elections_started, r.clients_processed, r.max_term, r.leaders, r.inbox_total
                );
            }
        }
    }

    fn assert_healthy_pre_trigger(trace: &[Round]) {
        let pre = &trace[10..100];
        assert!(
            pre.iter().all(|r| r.heartbeats_processed == (MEMBERS - 1) as u64 && r.elections_started == 0 && r.leaders == 1 && r.inbox_total == 0),
            "the system should be healthy before the trigger"
        );
    }

    /// Everything a run does that the input did not ask for: elections started and vote
    /// requests sent. Heartbeats and client requests are the required work.
    fn wasted_work(trace: &[Round]) -> (u64, u64) {
        (
            sum(trace, 0, trace.len(), |r| r.elections_started),
            sum(trace, 0, trace.len(), |r| r.vote_requests_sent),
        )
    }

    fn report(name: &str, trace: &[Round]) {
        let (elections, votes) = wasted_work(trace);
        let peak_inbox = trace.iter().map(|r| r.inbox_total).max().unwrap();
        let last_unhealthy = trace.iter().rposition(|r| r.leaders != 1 || r.elections_started > 0).map(|i| i + 1).unwrap_or(0);
        let tail_hb = sum(trace, TAIL_START, trace.len(), |r| r.heartbeats_processed);
        println!(
            "{name}: elections started {elections}; vote requests sent {votes}; peak inbox total {peak_inbox}; last unhealthy round before {last_unhealthy}; final term {}; tail heartbeats processed {tail_hb} (baseline {}); tail inbox {} -> {}",
            trace.last().unwrap().max_term,
            (MEMBERS - 1) as u64 * (trace.len() - TAIL_START) as u64,
            trace[TAIL_START].inbox_total,
            trace.last().unwrap().inbox_total
        );
    }

    /// The main run with uniform timeouts. The input asks for heartbeats and client requests
    /// only; every election and vote request is work the schedule caused.
    #[test]
    fn uniform_timeouts_stampede_under_load() {
        let trace = run(WORKLOAD, CONFIG, ROUNDS);
        print_trajectory(&trace);
        report("uniform", &trace);
        assert_healthy_pre_trigger(&trace);

        let (elections, votes) = wasted_work(&trace);
        assert!(elections >= 4 * 10, "the trigger should have caused repeated stampedes, got {elections} elections");
        assert!(votes >= 4 * elections, "each election should have sent a vote request to every peer");
        // A stampede is every follower starting an election in the same round.
        let stampedes = trace.iter().filter(|r| r.elections_started >= (MEMBERS - 1) as u64).count();
        assert!(stampedes >= 10, "followers should have timed out together, got {stampedes} stampede rounds");
        // The leader was lost for a long stretch.
        let leaderless = trace.iter().filter(|r| r.leaders == 0).count();
        assert!(leaderless > 100, "the cluster should have been leaderless for a long stretch, got {leaderless} rounds");
    }

    /// Wasted work grows with how long heartbeats are held behind the trigger's load.
    #[test]
    fn wasted_work_grows_with_the_hold() {
        let mut previous = (0u64, 0u64);
        for hold in [10u64, 20, 30] {
            let trace = run(
                Workload {
                    trigger_end: WORKLOAD.trigger_start + hold,
                    ..WORKLOAD
                },
                CONFIG,
                ROUNDS,
            );
            let wasted = wasted_work(&trace);
            let last_unhealthy = trace.iter().rposition(|r| r.leaders != 1 || r.elections_started > 0).map(|i| i + 1).unwrap_or(0);
            println!("hold {hold} rounds: elections {} vote requests {} last unhealthy round before {last_unhealthy}", wasted.0, wasted.1);
            assert!(wasted > previous, "wasted work should grow with the hold: {wasted:?} after {previous:?}");
            previous = wasted;
        }
    }

    /// Control: uniform timeouts, no trigger.
    #[test]
    fn without_a_trigger_the_leader_keeps_its_term() {
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
        assert!(trace[1..].iter().all(|r| r.heartbeats_processed == (MEMBERS - 1) as u64 && r.leaders == 1 && r.inbox_total == 0));
        assert!(trace.iter().all(|r| r.elections_started == 0 && r.vote_requests_sent == 0 && r.max_term == 1));
    }

    /// The knob variant: same trigger, staggered timeouts. Also hazardous; the spread changes
    /// how the storm ends, not whether the timeout causes it.
    #[test]
    fn spread_timeouts_still_stampede_but_settle_sooner() {
        let uniform = run(WORKLOAD, CONFIG, ROUNDS);
        let spread = run(WORKLOAD, ElectionConfig { timeout_spread_ticks: 3, ..CONFIG }, ROUNDS);
        print_trajectory(&spread);
        report("uniform", &uniform);
        report("spread 3", &spread);
        assert_healthy_pre_trigger(&spread);
        let (elections, _) = wasted_work(&spread);
        assert!(elections > 0, "the trigger should still cause elections with staggered timeouts");
        let settle = |t: &[Round]| t.iter().rposition(|r| r.leaders != 1 || r.elections_started > 0).map(|i| i + 1).unwrap_or(0);
        assert!(settle(&spread) <= settle(&uniform));
        assert!(spread[TAIL_START..].iter().all(|r| r.heartbeats_processed == (MEMBERS - 1) as u64 && r.leaders == 1 && r.elections_started == 0));
    }
}
