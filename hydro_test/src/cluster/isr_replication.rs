//! Kafka-style in-sync-replica (ISR) replication, the LDFI paper's real-world example and the
//! first target whose bug needs **omission and crash together**.
//!
//! A cluster of replicas; one of them is the leader by configuration (member 0 initially), not
//! by type. A client's write goes to every replica and the current leader takes it: it appends,
//! forwards the write to every other ISR member, and acknowledges the client once each of them
//! has acked. A ZK-like process (`Zk`) holds the view `(leader, isr)` and republishes
//! it every round: a follower whose ack has not arrived by the leader's timer is shrunk out of
//! the ISR at the leader's request; a replica whose heartbeat is missing is dead, and if it was
//! the leader a live replica is promoted.
//!
//! **Goal**: every write the client was acknowledged for is in the final leader's log.
//!
//! The known bug is *unclean leader election* (`unclean = true`): promoting a live replica that
//! was outside the ISR when the write was acknowledged. It takes a lost replication message
//! (the follower falls out of the ISR), the leader acknowledging on the shrunken ISR, and then
//! the leader's crash, so that ZK promotes the stale follower. With `unclean = false` ZK only
//! promotes ISR members, and the acknowledged write survives (or no leader is available).
//!
//! Fault model: the leader→follower replication channel is lossy (omission), replicas are
//! crashable, client and ZK are not. Timers are driven by the test in rounds.
//!
//! The protocol exists in two encodings (see [`Encoding`]) that differ only in what the
//! simulator's static lineage can see: a per-tick step closure over a state struct, and
//! per-message-type dataflow. The tests run both and the comparison table reports them side
//! by side.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use hydro_lang::sim_hooks::{BatchHook, OnCluster, OnProcess};
use serde::{Deserialize, Serialize};

pub struct Client {}

pub struct Zk {}

pub struct Replica {}

/// A client write.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Write(pub u32);

/// ZK's view of the replica group; member ids are raw.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct View {
    pub leader: Option<u32>,
    pub isr: Vec<u32>,
}

/// What replicas tell ZK.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ZkMsg {
    /// "I am alive" (sent on every timer round).
    Heartbeat,
    /// "These ISR members have not acked my pending writes; drop them from the ISR."
    Shrink(Vec<u32>),
}

/// A replica's persistent state.
#[derive(Clone, Debug, Default)]
pub struct ReplicaState {
    pub view: View,
    pub log: Vec<Write>,
    /// Acks received: `(follower, write)`.
    pub acks: Vec<(u32, Write)>,
    /// Writes already acknowledged to the client.
    pub acked: Vec<Write>,
}

/// One tick of a replica. Returns `(replications, acks, client_acks, zk_messages, appended)`.
pub fn replica_step(
    me: u32,
    state: &mut ReplicaState,
    views: &[View],
    writes: &[Write],
    replications: &[(u32, Write)],
    acks: &[(u32, Write)],
    timer_fired: bool,
) -> (
    Vec<(u32, Write)>,
    Vec<(u32, Write)>,
    Vec<Write>,
    Vec<ZkMsg>,
    Vec<Write>,
) {
    if let Some(view) = views.last() {
        state.view = view.clone();
    }
    let is_leader = state.view.leader == Some(me);
    let mut replicate = vec![];
    let mut ack = vec![];
    let mut client_acks = vec![];
    let mut zk = vec![];
    let mut appended = vec![];

    // Leader: append new writes and forward them to the other ISR members.
    if is_leader {
        for write in writes {
            if !state.log.contains(write) {
                state.log.push(write.clone());
                appended.push(write.clone());
            }
            for follower in state.view.isr.iter().filter(|m| **m != me) {
                replicate.push((*follower, write.clone()));
            }
        }
    }
    // Follower: append replicated writes and ack them to the sender.
    for (leader, write) in replications {
        if !state.log.contains(write) {
            state.log.push(write.clone());
            appended.push(write.clone());
        }
        ack.push((*leader, write.clone()));
    }
    state.acks.extend(acks.iter().cloned());

    if is_leader {
        // Acknowledge every logged write that every other ISR member has acked.
        for write in &state.log {
            if state.acked.contains(write) {
                continue;
            }
            let all_acked = state
                .view
                .isr
                .iter()
                .filter(|m| **m != me)
                .all(|m| state.acks.contains(&(*m, write.clone())));
            if all_acked {
                state.acked.push(write.clone());
                client_acks.push(write.clone());
            }
        }
        // On a timer: ISR members still owing an ack for a pending write are shrunk out.
        if timer_fired {
            let mut missing: Vec<u32> = vec![];
            for write in &state.log {
                if state.acked.contains(write) {
                    continue;
                }
                for m in state.view.isr.iter().filter(|m| **m != me) {
                    if !state.acks.contains(&(*m, write.clone())) && !missing.contains(m) {
                        missing.push(*m);
                    }
                }
            }
            if !missing.is_empty() {
                zk.push(ZkMsg::Shrink(missing));
            }
        }
    }
    if timer_fired {
        zk.push(ZkMsg::Heartbeat);
    }
    (replicate, ack, client_acks, zk, appended)
}

/// ZK's state: the view and the heartbeats seen since its last timer.
#[derive(Clone, Debug, Default)]
pub struct ZkState {
    pub view: View,
    pub alive: Vec<u32>,
}

/// One tick of ZK. On a timer it publishes the view, after dropping replicas whose
/// heartbeat is missing and, if the leader is among them, promoting a live replica: the
/// lowest-numbered live ISR member, or with `unclean` the lowest-numbered live replica.
pub fn zk_step(
    state: &mut ZkState,
    members: u32,
    unclean: bool,
    messages: &[(u32, ZkMsg)],
    timer_fired: bool,
) -> Vec<View> {
    for (from, message) in messages {
        match message {
            ZkMsg::Heartbeat => {
                if !state.alive.contains(from) {
                    state.alive.push(*from);
                }
            }
            ZkMsg::Shrink(gone) => state.view.isr.retain(|m| !gone.contains(m)),
        }
    }
    if !timer_fired {
        return vec![];
    }
    let mut live: Vec<u32> = (0..members).filter(|m| state.alive.contains(m)).collect();
    live.sort_unstable();
    state.view.isr.retain(|m| live.contains(m));
    if let Some(leader) = state.view.leader
        && !live.contains(&leader)
    {
        state.view.leader = if unclean {
            live.first().copied()
        } else {
            live.iter().find(|m| state.view.isr.contains(m)).copied()
        };
    }
    state.alive.clear();
    vec![state.view.clone()]
}

/// Hook on each replica's batch of client writes.
pub type WriteHook = BatchHook<Write, TotalOrder, ExactlyOnce, OnCluster<Replica>>;
/// Hook on each replica's batch of replicated writes, keyed by the sending leader.
pub type ReplicationHook =
    BatchHook<(MemberId<Replica>, Write), NoOrder, ExactlyOnce, OnCluster<Replica>>;
/// Hook on each replica's batch of follower acks, keyed by the follower.
pub type AckHook = BatchHook<(MemberId<Replica>, Write), NoOrder, ExactlyOnce, OnCluster<Replica>>;
/// Hook on each replica's batch of ZK views.
pub type ViewHook = BatchHook<View, TotalOrder, ExactlyOnce, OnCluster<Replica>>;
/// Hook on each replica's timer.
pub type ReplicaTimerHook = BatchHook<(), TotalOrder, ExactlyOnce, OnCluster<Replica>>;
/// Hook on ZK's batch of replica messages, keyed by the replica (closure encoding: one channel).
pub type ZkInHook = BatchHook<(MemberId<Replica>, ZkMsg), NoOrder, ExactlyOnce, OnProcess<Zk>>;
/// Hook on ZK's batch of heartbeats, keyed by the replica (dataflow encoding).
pub type ZkHeartbeatHook = BatchHook<(MemberId<Replica>, ()), NoOrder, ExactlyOnce, OnProcess<Zk>>;
/// Hook on ZK's batch of shrink requests, keyed by the replica (dataflow encoding).
pub type ZkShrinkHook =
    BatchHook<(MemberId<Replica>, Vec<u32>), NoOrder, ExactlyOnce, OnProcess<Zk>>;
/// Hook on ZK's timer.
pub type ZkTimerHook = BatchHook<(), TotalOrder, ExactlyOnce, OnProcess<Zk>>;

/// Every hook of the protocol, one `flow.sim_hook()` away.
#[derive(Clone, Copy)]
pub struct Hooks {
    pub writes: WriteHook,
    pub replication: ReplicationHook,
    pub acks: AckHook,
    pub views: ViewHook,
    pub replica_timer: ReplicaTimerHook,
    pub zk_in: ZkInHook,
    pub zk_hb: ZkHeartbeatHook,
    pub zk_shrink: ZkShrinkHook,
    pub zk_timer: ZkTimerHook,
}

impl hydro_lang::sim_hooks::SimHook for Hooks {
    fn create(next_id: &mut dyn FnMut() -> usize) -> Self {
        Hooks {
            writes: hydro_lang::sim_hooks::SimHook::create(next_id),
            replication: hydro_lang::sim_hooks::SimHook::create(next_id),
            acks: hydro_lang::sim_hooks::SimHook::create(next_id),
            views: hydro_lang::sim_hooks::SimHook::create(next_id),
            replica_timer: hydro_lang::sim_hooks::SimHook::create(next_id),
            zk_in: hydro_lang::sim_hooks::SimHook::create(next_id),
            zk_hb: hydro_lang::sim_hooks::SimHook::create(next_id),
            zk_shrink: hydro_lang::sim_hooks::SimHook::create(next_id),
            zk_timer: hydro_lang::sim_hooks::SimHook::create(next_id),
        }
    }
}

/// What the protocol exposes to a test.
pub struct Outputs<'a> {
    /// Writes acknowledged to the client.
    pub client_acks: Stream<Write, Process<'a, Client>, Unbounded, NoOrder>,
    /// Every view ZK published.
    pub views: Stream<View, Process<'a, Zk>>,
    /// Every write each replica appended to its log.
    pub appended: Stream<Write, Cluster<'a, Replica>, Unbounded, NoOrder>,
}

/// How the replica and ZK logic is written. Same protocol, same channels (except that the
/// dataflow encoding sends heartbeats and shrink requests to ZK on separate channels), same
/// fault model; the encodings differ only in what the simulator's static lineage can see.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// One step function per tick, mutating a state struct behind a `by_mut` reference
    /// (`replica_step`, `zk_step`). Opaque to dataflow slicing: every output depends on
    /// every input.
    Closure,
    /// Per-message-type streams, state as `use::state` cycles, outputs as separate paths.
    Dataflow,
}

/// Builds the protocol over `members` replicas with member 0 the initial leader and every
/// member in the initial ISR.
#[expect(
    clippy::too_many_arguments,
    reason = "one input stream per external stimulus"
)]
pub fn isr_replication<'a>(
    client: &Process<'a, Client>,
    zk: &Process<'a, Zk>,
    replicas: &Cluster<'a, Replica>,
    members: u32,
    unclean: bool,
    encoding: Encoding,
    writes: Stream<Write, Process<'a, Client>>,
    replica_timer: Stream<(), Cluster<'a, Replica>>,
    zk_timer: Stream<(), Process<'a, Zk>>,
    hooks: Hooks,
) -> Outputs<'a> {
    match encoding {
        Encoding::Closure => isr_replication_closure(
            client,
            zk,
            replicas,
            members,
            unclean,
            writes,
            replica_timer,
            zk_timer,
            hooks,
        ),
        Encoding::Dataflow => isr_replication_dataflow(
            client,
            zk,
            replicas,
            members,
            unclean,
            writes,
            replica_timer,
            zk_timer,
            hooks,
        ),
    }
}

/// The dataflow encoding: one stream per message type, state as `use::state` cycles, and a
/// separate output path per kind of message a replica sends.
#[expect(
    clippy::too_many_arguments,
    reason = "one input stream per external stimulus"
)]
pub fn isr_replication_dataflow<'a>(
    client: &Process<'a, Client>,
    zk: &Process<'a, Zk>,
    replicas: &Cluster<'a, Replica>,
    members: u32,
    unclean: bool,
    writes: Stream<Write, Process<'a, Client>>,
    replica_timer: Stream<(), Cluster<'a, Replica>>,
    zk_timer: Stream<(), Process<'a, Zk>>,
    hooks: Hooks,
) -> Outputs<'a> {
    let writes_at_replicas =
        writes.broadcast_closed(replicas, TCP.fail_stop().name("write").bincode());

    let (replication_handle, replication_in) = replicas.forward_ref::<Stream<
        (MemberId<Replica>, Write),
        Cluster<'a, Replica>,
        Unbounded,
        NoOrder,
    >>();
    let (acks_handle, acks_in) = replicas.forward_ref::<Stream<
        (MemberId<Replica>, Write),
        Cluster<'a, Replica>,
        Unbounded,
        NoOrder,
    >>();
    let (views_handle, views_in) =
        writes_at_replicas
            .location()
            .forward_ref::<Stream<View, _, Unbounded, TotalOrder>>();

    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (replicate, acks_out, client_acks, heartbeats, shrinks, appended): (
        Stream<(MemberId<Replica>, Write), Cluster<'a, Replica>, Unbounded, NoOrder>,
        Stream<(MemberId<Replica>, Write), Cluster<'a, Replica>, Unbounded, NoOrder>,
        Stream<Write, Cluster<'a, Replica>, Unbounded, NoOrder>,
        Stream<(), Cluster<'a, Replica>, Unbounded, NoOrder>,
        Stream<Vec<u32>, Cluster<'a, Replica>, Unbounded, NoOrder>,
        Stream<Write, Cluster<'a, Replica>, Unbounded, NoOrder>,
    ) = sliced! {
        let write_batch = use::batch(writes_at_replicas, nondet!(/** LDFI delivery point */ hook = hooks.writes));
        let replication_batch = use::batch(replication_in, nondet!(/** LDFI delivery point */ hook = hooks.replication));
        let ack_batch = use::batch(acks_in, nondet!(/** LDFI delivery point */ hook = hooks.acks));
        let view_batch = use::batch(views_in, nondet!(/** LDFI delivery point */ hook = hooks.views));
        let timer_batch = use::batch(replica_timer, nondet!(/** external input */ hook = hooks.replica_timer));
        let mut view = use::state(|l| l.singleton(q!(View {
            leader: Some(0),
            isr: (0..members).collect(),
        })));
        let mut log = use::state(|l| {
            l.source_iter(q!(Vec::<Write>::new())).weaken_ordering::<NoOrder>()
        });
        let mut acks = use::state(|l| {
            l.source_iter(q!(Vec::<(u32, Write)>::new())).weaken_ordering::<NoOrder>()
        });
        let mut acked = use::state(|l| {
            l.source_iter(q!(Vec::<Write>::new())).weaken_ordering::<NoOrder>()
        });

        // The view: the latest one ZK published this tick, else the one we had.
        let latest_view = view_batch.fold(
            q!(|| None::<View>),
            q!(|acc, v| *acc = Some(v)),
        );
        let view_now = view
            .zip(latest_view)
            .map(q!(|(old, new)| new.unwrap_or(old)));
        view = view_now.clone();
        let is_leader = view_now
            .clone()
            .map(q!(move |v| v.leader == Some(CLUSTER_SELF_ID.get_raw_id())));
        let isr_others = view_now
            .map(q!(move |v| {
                let me = CLUSTER_SELF_ID.get_raw_id();
                v.isr.into_iter().filter(move |m| *m != me).collect::<Vec<u32>>()
            }))
            .flatten_unordered();

        // Leader: take client writes and forward them to the other ISR members.
        let leader_writes = write_batch
            .weaken_ordering::<NoOrder>()
            .filter_if(is_leader.clone());
        let replicate = leader_writes
            .clone()
            .cross_product(isr_others.clone())
            .map(q!(|(w, m)| (MemberId::<Replica>::from_raw_id(m), w)));

        // Follower: append replicated writes and ack them to the sender.
        let replicated = replication_batch.clone().map(q!(|(_, w)| w));
        let acks_out = replication_batch;

        // The log grows by whatever arrived that it did not have.
        let appended = leader_writes
            .chain(replicated)
            .unique()
            .filter_not_in(log.clone());
        let log_now = log.chain(appended.clone());
        log = log_now.clone();

        // Acks received so far, as `(follower, write)`.
        let acks_now = acks.chain(ack_batch.map(q!(|(from, w)| (from.get_raw_id(), w))));
        acks = acks_now.clone();

        // Leader: acknowledge every logged write that every other ISR member has acked.
        let owed = log_now
            .clone()
            .cross_product(isr_others)
            .map(q!(|(w, m)| (m, w)))
            .filter_not_in(acks_now);
        let fully_acked = log_now.filter_not_in(owed.clone().map(q!(|(_, w)| w)));
        let client_acks = fully_acked
            .filter_not_in(acked.clone())
            .filter_if(is_leader.clone());
        acked = acked.chain(client_acks.clone());

        // Timer: every replica heartbeats; the leader shrinks owing members out of the ISR.
        let timer_fired = timer_batch.clone().count().map(q!(|n| n > 0));
        let heartbeats = timer_batch.first().into_stream().weaken_ordering::<NoOrder>();
        let owing = owed
            .map(q!(|(m, _)| m))
            .unique()
            .fold(
                q!(Vec::new),
                q!(|v, m| v.push(m), commutative = manual_proof!(/** sorted below */)),
            )
            .map(q!(|mut v: Vec<u32>| {
                v.sort_unstable();
                v
            }));
        let shrinks = owing
            .filter(q!(|v| !v.is_empty()))
            .into_stream()
            .weaken_ordering::<NoOrder>()
            .filter_if(timer_fired)
            .filter_if(is_leader);

        (replicate, acks_out, client_acks, heartbeats, shrinks, appended)
    };

    replication_handle.complete(
        replicate
            .demux(replicas, TCP.lossy_delayed_forever().name("repl").bincode())
            .entries(),
    );
    acks_handle.complete(
        acks_out
            .demux(replicas, TCP.fail_stop().name("ack").bincode())
            .entries(),
    );

    // ZK: heartbeats and shrink requests arrive on their own channels.
    let heartbeats_in = heartbeats
        .send(zk, TCP.fail_stop().name("hb").bincode())
        .entries();
    let shrinks_in = shrinks
        .send(zk, TCP.fail_stop().name("shrink").bincode())
        .entries();
    let views: Stream<View, Process<'a, Zk>> = sliced! {
        let hb_batch = use::batch(heartbeats_in, nondet!(/** LDFI delivery point */ hook = hooks.zk_hb));
        let shrink_batch = use::batch(shrinks_in, nondet!(/** LDFI delivery point */ hook = hooks.zk_shrink));
        let timer_batch = use::batch(zk_timer, nondet!(/** external input */ hook = hooks.zk_timer));
        let mut view = use::state(|l| l.singleton(q!(View {
            leader: Some(0),
            isr: (0..members).collect(),
        })));
        let mut alive = use::state(|l| {
            l.source_iter(q!(Vec::<u32>::new())).weaken_ordering::<NoOrder>()
        });

        // Heartbeats seen since the last timer.
        let alive_now = alive
            .chain(hb_batch.map(q!(|(from, _)| from.get_raw_id())))
            .unique();
        // Shrink requests apply immediately.
        let gone = shrink_batch
            .flat_map_unordered(q!(|(_, gone)| gone))
            .fold(
                q!(Vec::new),
                q!(|v, m| v.push(m), commutative = manual_proof!(/** used as a set */)),
            );
        let shrunk = view.zip(gone).map(q!(|(mut v, gone): (View, Vec<u32>)| {
            v.isr.retain(|m| !gone.contains(m));
            v
        }));

        // On a timer: drop replicas whose heartbeat is missing; if the leader is among them,
        // promote the lowest-numbered live ISR member (or, unclean, the lowest live replica).
        let timer_fired = timer_batch.count().map(q!(|n| n > 0));
        let live = alive_now
            .clone()
            .fold(
                q!(Vec::new),
                q!(|v, m| v.push(m), commutative = manual_proof!(/** sorted below */)),
            )
            .map(q!(|mut v: Vec<u32>| {
                v.sort_unstable();
                v
            }));
        let elected = shrunk
            .clone()
            .zip(live)
            .map(q!(move |(mut v, live): (View, Vec<u32>)| {
                v.isr.retain(|m| live.contains(m));
                if let Some(leader) = v.leader
                    && !live.contains(&leader)
                {
                    v.leader = if unclean {
                        live.first().copied()
                    } else {
                        live.iter().find(|m| v.isr.contains(m)).copied()
                    };
                }
                v
            }));
        let published = elected
            .clone()
            .into_stream()
            .filter_if(timer_fired.clone());
        view = timer_fired
            .clone()
            .zip(shrunk)
            .zip(elected)
            .map(q!(|((fired, shrunk), elected)| if fired { elected } else { shrunk }));
        alive = alive_now.filter_if(timer_fired.map(q!(|fired| !fired)));

        published
    };
    views_handle.complete(
        views
            .clone()
            .broadcast_closed(replicas, TCP.fail_stop().name("view").bincode()),
    );

    Outputs {
        client_acks: client_acks
            .send(client, TCP.fail_stop().name("client-ack").bincode())
            .values(),
        views,
        appended,
    }
}

/// The closure encoding: `replica_step` / `zk_step` once per tick over a state struct.
#[expect(
    clippy::too_many_arguments,
    reason = "one input stream per external stimulus"
)]
pub fn isr_replication_closure<'a>(
    client: &Process<'a, Client>,
    zk: &Process<'a, Zk>,
    replicas: &Cluster<'a, Replica>,
    members: u32,
    unclean: bool,
    writes: Stream<Write, Process<'a, Client>>,
    replica_timer: Stream<(), Cluster<'a, Replica>>,
    zk_timer: Stream<(), Process<'a, Zk>>,
    hooks: Hooks,
) -> Outputs<'a> {
    let writes_at_replicas =
        writes.broadcast_closed(replicas, TCP.fail_stop().name("write").bincode());

    // Cycles through the network: replication (leader → followers, lossy), acks (followers →
    // leader), and views (ZK → replicas).
    let (replication_handle, replication_in) = replicas.forward_ref::<Stream<
        (MemberId<Replica>, Write),
        Cluster<'a, Replica>,
        Unbounded,
        NoOrder,
    >>();
    let (acks_handle, acks_in) = replicas.forward_ref::<Stream<
        (MemberId<Replica>, Write),
        Cluster<'a, Replica>,
        Unbounded,
        NoOrder,
    >>();
    let (views_handle, views_in) =
        writes_at_replicas
            .location()
            .forward_ref::<Stream<View, _, Unbounded, TotalOrder>>();

    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (replicate, acks_out, client_acks, to_zk, appended): (
        Stream<(MemberId<Replica>, Write), Cluster<'a, Replica>>,
        Stream<(MemberId<Replica>, Write), Cluster<'a, Replica>>,
        Stream<Write, Cluster<'a, Replica>>,
        Stream<ZkMsg, Cluster<'a, Replica>>,
        Stream<Write, Cluster<'a, Replica>>,
    ) = sliced! {
        let write_batch = use::batch(writes_at_replicas, nondet!(/** LDFI delivery point */ hook = hooks.writes));
        let replication_batch = use::batch(replication_in, nondet!(/** LDFI delivery point */ hook = hooks.replication));
        let ack_batch = use::batch(acks_in, nondet!(/** LDFI delivery point */ hook = hooks.acks));
        let view_batch = use::batch(views_in, nondet!(/** LDFI delivery point */ hook = hooks.views));
        let timer_batch = use::batch(replica_timer, nondet!(/** external input */ hook = hooks.replica_timer));
        let mut state = use::state(|l| l.singleton(q!(ReplicaState {
            view: View { leader: Some(0), isr: (0..members).collect() },
            log: Vec::new(),
            acks: Vec::new(),
            acked: Vec::new(),
        })));

        let tick = write_batch.location().clone();
        let views_vec = view_batch.fold(q!(Vec::new), q!(|v, x| v.push(x)));
        let writes_vec = write_batch.fold(q!(Vec::new), q!(|v, x| v.push(x)));
        let replications_vec = replication_batch.fold(
            q!(Vec::new),
            q!(|v, (from, w): (MemberId<Replica>, Write)| v.push((from.get_raw_id(), w)),
                commutative = manual_proof!(/** consumed as a set by replica_step */)),
        );
        let acks_vec = ack_batch.fold(
            q!(Vec::new),
            q!(|v, (from, w): (MemberId<Replica>, Write)| v.push((from.get_raw_id(), w)),
                commutative = manual_proof!(/** consumed as a set by replica_step */)),
        );
        let timer_fired = timer_batch.count().map(q!(|n| n > 0));

        let state_ref = state.by_mut();
        let views_ref = views_vec.by_ref();
        let writes_ref = writes_vec.by_ref();
        let replications_ref = replications_vec.by_ref();
        let acks_ref = acks_vec.by_ref();
        let timer_ref = timer_fired.by_ref();

        let acks_out: Stream<(MemberId<Replica>, Write), _, Bounded> = tick.source_iter(q!(Vec::new()));
        let acks_out_ref = acks_out.by_mut();
        let client_acks: Stream<Write, _, Bounded> = tick.source_iter(q!(Vec::new()));
        let client_acks_ref = client_acks.by_mut();
        let to_zk: Stream<ZkMsg, _, Bounded> = tick.source_iter(q!(Vec::new()));
        let to_zk_ref = to_zk.by_mut();
        let appended: Stream<Write, _, Bounded> = tick.source_iter(q!(Vec::new()));
        let appended_ref = appended.by_mut();

        let replicate = tick
            .singleton(q!(()))
            .into_stream()
            .flat_map_ordered(q!(move |_| {
                let (replicate, acks, client, zk, appended) =
                    crate::cluster::isr_replication::replica_step(
                        CLUSTER_SELF_ID.get_raw_id(),
                        &mut *state_ref,
                        &views_ref,
                        &writes_ref,
                        &replications_ref,
                        &acks_ref,
                        *timer_ref,
                    );
                for (to, w) in acks {
                    acks_out_ref.push((MemberId::<Replica>::from_raw_id(to), w));
                }
                client_acks_ref.extend(client);
                to_zk_ref.extend(zk);
                appended_ref.extend(appended);
                replicate
                    .into_iter()
                    .map(|(to, w)| (MemberId::<Replica>::from_raw_id(to), w))
            }));

        (replicate, acks_out, client_acks, to_zk, appended)
    };

    replication_handle.complete(
        replicate
            .demux(replicas, TCP.lossy_delayed_forever().name("repl").bincode())
            .entries(),
    );
    acks_handle.complete(
        acks_out
            .demux(replicas, TCP.fail_stop().name("ack").bincode())
            .entries(),
    );

    // ZK.
    let zk_in = to_zk
        .send(zk, TCP.fail_stop().name("zk").bincode())
        .entries();
    let views: Stream<View, Process<'a, Zk>> = sliced! {
        let message_batch = use::batch(zk_in, nondet!(/** LDFI delivery point */ hook = hooks.zk_in));
        let timer_batch = use::batch(zk_timer, nondet!(/** external input */ hook = hooks.zk_timer));
        let mut state = use::state(|l| l.singleton(q!(ZkState {
            view: View { leader: Some(0), isr: (0..members).collect() },
            alive: Vec::new(),
        })));

        let tick = message_batch.location().clone();
        let messages_vec = message_batch.fold(
            q!(Vec::new),
            q!(|v, (from, m): (MemberId<Replica>, ZkMsg)| v.push((from.get_raw_id(), m)),
                commutative = manual_proof!(/** consumed as a set by zk_step */)),
        );
        let timer_fired = timer_batch.count().map(q!(|n| n > 0));
        let state_ref = state.by_mut();
        let messages_ref = messages_vec.by_ref();
        let timer_ref = timer_fired.by_ref();

        tick.singleton(q!(()))
            .into_stream()
            .flat_map_ordered(q!(move |_| crate::cluster::isr_replication::zk_step(
                &mut *state_ref,
                members,
                unclean,
                &messages_ref,
                *timer_ref,
            )))
    };
    views_handle.complete(
        views
            .clone()
            .broadcast_closed(replicas, TCP.fail_stop().name("view").bincode()),
    );

    Outputs {
        client_acks: client_acks
            .send(client, TCP.fail_stop().name("client-ack").bincode())
            .values(),
        views,
        appended: appended.weaken_ordering::<NoOrder>(),
    }
}

/// The test harness: compiles a configuration and drives one run. Shared with the LDFI
/// measurement grid (`ldfi_grid.rs`).
#[cfg(test)]
pub(crate) mod harness {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Ldfi, Level, Run};
    use hydro_lang::sim::spec::Spec;
    use hydro_lang::sim::{SimClusterSender, SimSender};

    use super::{Client, Encoding, Hooks, Replica, Write, Zk, isr_replication};

    /// The protocol's size and horizon.
    #[derive(Clone, Copy, Debug)]
    pub(crate) struct Shape {
        pub(crate) members: u32,
        pub(crate) rounds: u32,
        /// Writes per run; the specification is about the first, the rest are load.
        pub(crate) writes: u32,
    }

    pub(crate) const DEFAULT_SHAPE: Shape = Shape {
        members: 3,
        rounds: 4,
        writes: 1,
    };

    pub(crate) struct Harness {
        shape: Shape,
        write_send: SimSender<Write, TotalOrder, ExactlyOnce>,
        replica_timer: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        zk_timer: SimSender<(), TotalOrder, ExactlyOnce>,
    }

    pub(crate) fn compile(
        unclean: bool,
        encoding: Encoding,
        shape: Shape,
        fuzz_iterations: usize,
    ) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        compile_with(unclean, encoding, shape, true, fuzz_iterations)
    }

    /// [`compile`] with or without record-level provenance.
    ///
    /// The specification is durability of the first acknowledged write, as views over the
    /// client's acknowledgements, ZK's views and the replicas' logs (see
    /// `hydro_lang::sim::spec`):
    ///
    /// - `PRE`: the client was acknowledged for `Write(1)`, and ZK's **final** view names a
    ///   leader (no leader is unavailability, not a safety violation).
    /// - `POST`: the final leader's log holds `Write(1)`.
    ///
    /// "Final" is `observe_sequenced(views).max()`: the last view ZK published. A view about
    /// a last value is not monotone in the facts (a later view changes it), which is what the
    /// first version's `witness_final` expressed by hand; the certificate is relative to that
    /// (see `history-hydrofrac-negation.md`).
    pub(crate) fn compile_with(
        unclean: bool,
        encoding: Encoding,
        shape: Shape,
        provenance: bool,
        fuzz_iterations: usize,
    ) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        let Shape { members, .. } = shape;
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let zk = flow.process::<Zk>();
        let replicas = flow.cluster::<Replica>();
        let hooks: Hooks = flow.sim_hook();

        let (write_send, writes) = client.sim_input::<Write, TotalOrder, ExactlyOnce>();
        let (replica_timer, replica_timers) = replicas.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (zk_timer, zk_timers) = zk.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = isr_replication(
            &client,
            &zk,
            &replicas,
            members,
            unclean,
            encoding,
            writes,
            replica_timers,
            zk_timers,
            hooks,
        );

        // The specification.
        let mut spec = Spec::new(&mut flow);
        let acked = spec.observe(outputs.client_acks);
        let views = spec.observe_sequenced(outputs.views);
        let appended = spec.observe_cluster(outputs.appended);
        let acked_first = acked.filter(q!(|w| w.0 == 1)).first();
        let final_leader = views.max().filter_map(q!(|(_, view)| view
            .leader
            .map(MemberId::<Replica>::from_raw_id)));
        spec.pre(acked_first.zip(final_leader.clone()));
        let leader_has_first = appended
            .filter(q!(|(_, w)| w.0 == 1))
            .map(q!(|(replica, _)| replica))
            .cross_singleton(final_leader)
            .filter(q!(|(replica, leader)| replica == leader))
            .first();
        spec.post(leader_has_first);

        let replicas_id = replicas.clone();
        let mut sim = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, members as usize)
            .unit_test_fuzz_iterations(fuzz_iterations);
        if provenance && std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        let harness = Harness {
            shape,
            write_send,
            replica_timer,
            zk_timer,
        };
        // Fault model: replicas may crash; the replication channel is lossy (declared by the
        // protocol's transport); everything else is reliable and the client and ZK do not
        // crash.
        let configure = move |crashes: usize| {
            Ldfi::new()
                .level(if provenance {
                    Level::Record
                } else {
                    Level::Slice
                })
                .level_from_env()
                .crashable_cluster("replica", &replicas_id, 0..members)
                .crash_budget(crashes)
                .spec(&spec)
        };
        (compiled, harness, configure)
    }

    /// One run: the client writes; then `rounds` rounds of replica timers followed by a ZK
    /// timer. Omissions are allowed only in the first round (the replication of the write);
    /// crashes anywhere. The verdict is the specification's.
    pub(crate) async fn body(h: &Harness, run: &Run<'_>) {
        let Shape {
            members,
            rounds,
            writes,
        } = h.shape;
        for w in 1..=writes {
            h.write_send.send(Write(w));
        }
        hydro_lang::sim::quiesce().await;
        run.end_of_finite_failures();
        for _ in 0..rounds {
            h.replica_timer.send_many((0..members).map(|m| (m, ())));
            hydro_lang::sim::quiesce().await;
            h.zk_timer.send(());
            hydro_lang::sim::quiesce().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::sim::ldfi::{Exploration, Fault, Outcome, Strategy};

    use super::Encoding;
    use super::harness::{DEFAULT_SHAPE, body, compile};

    /// Schedules sampled per hypothesis. Four rounds of three replicas plus ZK give
    /// tens of thousands of interleavings per hypothesis under `exhaustive`; the protocol's
    /// outcome does not depend on them (every hook is under a policy, so only tick order
    /// varies), but a certificate here is relative to the sample.
    const SAMPLED: usize = 8;

    fn search(unclean: bool, encoding: Encoding, crashes: usize) -> Outcome {
        let (compiled, h, configure) = compile(unclean, encoding, DEFAULT_SHAPE, 8);
        configure(crashes)
            .exploration(Exploration::Fuzz { instances: SAMPLED })
            .search(&compiled, async |run| body(&h, run).await)
    }

    /// Unclean election loses an acknowledged write: a replication message is lost, the
    /// follower is shrunk out of the ISR, the leader acknowledges, the leader dies, and ZK
    /// promotes the stale follower. One omission and one crash.
    fn finds_the_unclean_election_bug(encoding: Encoding) {
        let outcome = search(true, encoding, 1);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert!(
            faults.iter().any(|f| matches!(f, Fault::Omit(_))),
            "the counterexample needs a lost replication: {faults:?}"
        );
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "replica@0")),
            "the counterexample needs the leader's crash: {faults:?}"
        );
    }

    #[test]
    fn ldfi_finds_the_unclean_election_bug() {
        finds_the_unclean_election_bug(Encoding::Closure);
    }

    #[test]
    fn ldfi_finds_the_unclean_election_bug_dataflow() {
        finds_the_unclean_election_bug(Encoding::Dataflow);
    }

    /// Clean election (ISR members only) keeps every acknowledged write: no fault set within
    /// one crash and omissions in the first round violates the goal (in the sampled schedules).
    fn certifies_clean_election_under_one_crash(encoding: Encoding) {
        let outcome = search(false, encoding, 1);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    #[test]
    fn ldfi_certifies_clean_election_under_one_crash() {
        certifies_clean_election_under_one_crash(Encoding::Closure);
    }

    #[test]
    fn ldfi_certifies_clean_election_under_one_crash_dataflow() {
        certifies_clean_election_under_one_crash(Encoding::Dataflow);
    }

    /// Hunts a false certificate on the closure encoding at load 2 under the opaque-step
    /// replay (one schedule per hypothesis; the verdict varies with the schedule). Prints
    /// the search that certified, for diagnosis.
    #[test]
    #[ignore = "diagnostic; loops until a certificate appears"]
    fn ldfi_hunt_false_certificate() {
        use hydro_lang::sim::ldfi::Level;

        use super::harness::{Shape, compile_with};
        let shape = Shape {
            members: 3,
            rounds: 4,
            writes: 2,
        };
        let level = match std::env::var("LDFI_LEVEL").as_deref() {
            Ok("replay") => Level::Counterfactual,
            _ => Level::Record,
        };
        let (compiled, h, configure) = compile_with(true, Encoding::Closure, shape, true, 1);
        for attempt in 0..40 {
            let outcome = configure(1)
                .level(level)
                .exploration(Exploration::Fuzz { instances: 1 })
                .search(&compiled, async |run| body(&h, run).await);
            eprintln!("attempt {attempt}: {outcome}");
            if matches!(outcome, Outcome::Certificate { .. }) {
                panic!("false certificate on attempt {attempt}");
            }
        }
    }

    /// The bug needs a specific omission *and* a specific crash: the regime where lineage is
    /// supposed to beat blind search. Exhaustive exploration is out of reach here (tens of
    /// thousands of schedules per hypothesis), so every searcher samples `SAMPLED` schedules
    /// per hypothesis, and the random fuzzer gets the same number of instances as the blind
    /// enumeration used.
    #[test]
    fn compare_search_strategies() {
        fn verdict(o: &Outcome) -> &'static str {
            match o {
                Outcome::Counterexample { .. } => "violated",
                Outcome::Certificate { .. } => "certificate",
                Outcome::Exhausted { .. } => "exhausted",
                Outcome::Budget { .. } => "budget",
                Outcome::Vacuous { .. } => "vacuous",
            }
        }
        fn cell(o: &Outcome) -> String {
            format!(
                "{:>12} {:>4} {:>7}",
                verdict(o),
                o.runs().len(),
                o.schedules()
            )
        }

        eprintln!(
            "\n{:<12} {:<8} {:>3} | {:<25} | {:<25} | {:<13}",
            "protocol",
            "encoding",
            "cr",
            "ldfi/fuzz 8      runs sched",
            "blind/fuzz 8     runs sched",
            "fuzz    inst"
        );
        for (unclean, encoding, crashes) in [
            (true, Encoding::Closure, 1),
            (true, Encoding::Dataflow, 1),
            (false, Encoding::Closure, 1),
            (false, Encoding::Dataflow, 1),
        ] {
            let (compiled, h, configure) = compile(unclean, encoding, DEFAULT_SHAPE, 8);
            let ldfi = configure(crashes)
                .exploration(Exploration::Fuzz { instances: SAMPLED })
                .quiet()
                .search(&compiled, async |run| body(&h, run).await);
            let blind = configure(crashes)
                .strategy(Strategy::Blind)
                .exploration(Exploration::Fuzz { instances: SAMPLED })
                .quiet()
                .search(&compiled, async |run| body(&h, run).await);
            // The fuzzer draws from the same vocabulary: crashes, and omission of every message
            // observed on a lossy channel in the failure-free run. It gets as many instances as
            // the blind search spent.
            let omittable = ldfi.runs()[0].omittable();
            let crash_points = ldfi.runs()[0].crash_points();
            let (compiled_fuzz, h_fuzz, configure_fuzz) =
                compile(unclean, encoding, DEFAULT_SHAPE, blind.schedules().max(1));
            let fuzz = configure_fuzz(crashes).quiet().fuzz_faults(
                &compiled_fuzz,
                &omittable,
                &crash_points,
                async |run| {
                    body(&h_fuzz, run).await;
                    true
                },
            );
            eprintln!(
                "{:<12} {:<8} {:>3} | {} | {} | {:>8} {:>4}",
                if unclean { "isr unclean" } else { "isr clean" },
                match encoding {
                    Encoding::Closure => "closure",
                    Encoding::Dataflow => "dataflow",
                },
                crashes,
                cell(&ldfi),
                cell(&blind),
                if fuzz.violation.is_some() {
                    "violated"
                } else {
                    "none"
                },
                fuzz.runs,
            );
            assert_eq!(ldfi.is_counterexample(), blind.is_counterexample());
            assert!(ldfi.runs().len() <= blind.runs().len());
        }
    }
}
