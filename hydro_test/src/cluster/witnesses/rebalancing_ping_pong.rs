//! Workers that rebalance their queues on the strength of each other's queue-length reports.
//!
//! Each member of a worker cluster receives tasks from the outside into a FIFO and processes
//! them with a per-tick budget of [`RebalanceConfig::work_per_tick`] units, one unit per task. On
//! every element of `report_tick` a worker broadcasts its queue length to its peers, and keeps
//! the latest length it has heard from each peer. On every element of `clock` a worker compares
//! its own length with the reported length of each peer and, if it is longer than some peer by
//! more than [`RebalanceConfig::threshold`], migrates half the difference from the tail of its
//! queue to the shortest such peer. Admitting a migrated task costs the receiver one unit of the
//! same budget before it processes anything, so migration competes with useful work.
//!
//! # Mechanism and knob
//!
//! A worker decides on reports that are as old as the report period, and a report says nothing
//! about the tasks already in flight toward its sender. With fresh reports (one report per work
//! tick) the half-the-difference rule converges. With reports a few ticks stale the loop
//! overshoots: a worker keeps sending to a peer whose report still says it is short, the peer
//! then finds itself the longer one against the sender's own stale report and sends tasks back,
//! and the same tasks bounce between the two while every bounce costs the receiver budget it
//! would have spent processing. The knob is [`RebalanceConfig::cooldown_ticks`]: after migrating
//! to a peer, a worker does not migrate to that peer again for this many ticks, so its next
//! decision rests on a report that already reflects the previous transfer. With a cooldown of
//! zero the program is hazardous; with a cooldown of at least the report period the return trip
//! is blocked under the trigger used here, though acting on stale reports remains in the program
//! and a cooldown can only move the threshold at which the bounce runs away. The measured runs
//! show that the exchange stays bounded and the cluster recovers, so this witness is hazardous
//! without being metastable under the trigger used here.
//!
//! The corpus sketch proposed per-item hysteresis. In a two-worker system that does not stop the
//! bounce, because the peer that wants to send back can always send its own original tasks
//! instead of the ones it just received; what stops it is not acting twice on the same report.
//!
//! # Timer parameters
//!
//! - `clock` (per worker): one element per work tick; grants the budget, advances the logical
//!   clock, and triggers a rebalancing decision.
//! - `report_tick` (per worker): one element per queue-length report sent to peers. The harness
//!   pumps it every `report_every` rounds; the ratio to `clock` is the staleness of the reports.
//!
//! A deployment wires `workers.source_interval(period)` into each; the simulation feeds them from
//! `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Two workers, 3 tasks per round each against 5 units per round each, threshold 10. The trigger
//! gives worker 0 twelve tasks per round during rounds 100 to 160. Tail is rounds 600 to 800,
//! where baseline completions are 1200. The expected label was amplifying with a collapse; the
//! measured runs show amplification (537 migrations against 195 required) without a collapse, for
//! the reason given in `sim_tests`. The corpus table records the cooldown configuration as having
//! no ground truth, since no test collapsed it and no assurance argument exists for it.
//!
//! | run | report period | cooldown | trigger | migrations over the run | tasks completed after bouncing | peak total queue, empty from round | tail | label |
//! |---|---|---|---|---|---|---|---|---|
//! | stale reports | 4 | 0 | yes | 537 | 126 | 458, round 276 | baseline (1200 completed, 0 migrated) | hazardous, recovers |
//! | stale reports, cooldown | 4 | 8 | yes | 246 | 0 | 346, round 248 | baseline | no ground truth; the mechanism is present and the tool is expected to find it |
//! | fresh reports | 1 | 0 | yes | 195 | 0 | 452, round 273 | baseline | schedule control |
//! | no trigger | 4 | 0 | no | 0 | 0 | 0 | baseline | healthy |
//!
//! Staleness table (`migrations_grow_with_report_staleness_unless_cooled_down`): same trigger,
//! 400 rounds, reports every `k` rounds. Migrations under fresh reports move work toward idle
//! capacity; a bounced task is back where it started and has cost two admissions.
//!
//! | report period `k` | 1 | 2 | 3 | 4 | 6 | 8 |
//! |---|---|---|---|---|---|---|
//! | migrations, no cooldown | 195 | 196 | 391 | 537 | 366 | 620 |
//! | bounced, no cooldown | 0 | 0 | 68 | 126 | 61 | 179 |
//! | migrations, cooldown 8 | 246 | 246 | 246 | 246 | 246 | 242 |
//! | bounced, cooldown 8 | 0 | 0 | 0 | 0 | 0 | 0 |
//!
//! Staleness of three ticks or more makes tasks bounce and roughly doubles or triples the
//! migrations, not monotonically in `k` because the report phase relative to the trigger matters;
//! the cooldown removes every bounce at every staleness. The cluster recovers in every
//! configuration because a worker's own length is fresh even when its peer's is stale, so the
//! exchange is biased but stable rather than divergent; the extra work is real and chosen by the
//! schedule, but bounded by the skew rather than by the queue.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Worker;

/// A task in a worker's queue. `hops` counts the migrations it has undergone.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Task {
    pub id: u64,
    pub hops: u32,
}

/// What a worker did in one tick of its dataflow.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerTick {
    /// Clock elements observed; the budget was this many times `work_per_tick`.
    pub clock_elements: u64,
    /// Migrated tasks admitted this tick, each costing one unit.
    pub admitted: usize,
    /// Tasks processed this tick.
    pub processed: usize,
    /// Tasks migrated away this tick.
    pub migrated: usize,
    /// Queue length at the end of the tick.
    pub queue_len: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RebalanceConfig {
    /// Units per clock element, shared between admitting migrated tasks and processing.
    pub work_per_tick: usize,
    /// Migrate only to a peer whose reported length is shorter by more than this.
    pub threshold: usize,
    /// After migrating to a peer, do not migrate to it again for this many ticks.
    pub cooldown_ticks: u64,
}

pub struct RebalanceOutputs<'a> {
    /// Tasks processed, at the worker that processed them.
    pub completed: Stream<Task, Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Tasks migrated away, with their destination, at the worker that sent them.
    pub migrated:
        Stream<(MemberId<Worker>, Task), Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
    /// One record per tick of each worker's dataflow.
    pub ticks: Stream<WorkerTick, Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the worker cluster; see the module docs for the timer parameters. `tasks` is each
/// worker's own arrival stream, with ids unique across the cluster.
pub fn rebalancing_workers<'a>(
    workers: &Cluster<'a, Worker>,
    tasks: Stream<u64, Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
    clock: Stream<(), Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
    report_tick: Stream<(), Cluster<'a, Worker>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RebalanceConfig,
) -> RebalanceOutputs<'a> {
    let RebalanceConfig {
        work_per_tick,
        threshold,
        cooldown_ticks,
    } = config;

    // Migrated tasks and reports come from peers, downstream of this block's own outputs.
    let (migrations_complete, migrations_in) = workers.forward_ref::<Stream<
        (MemberId<Worker>, Task),
        Cluster<'a, Worker>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >>();
    let (reports_complete, reports_in) = workers.forward_ref::<Stream<
        (MemberId<Worker>, usize),
        Cluster<'a, Worker>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >>();

    let (completed, migrated, reports_out, ticks) = sliced! {
        let clock = use::batch(clock.enumerate(), nondet!(/** batching only decides how many ticks' budgets one dataflow tick spends; every element still grants one and advances the clock */));
        let report_pump = use::batch(report_tick, nondet!(/** batching only shifts which tick sends a report */));
        let tasks = use::batch(tasks, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let migrated_in = use::batch(migrations_in, nondet!(/** how long migrated tasks are in flight, and so how stale the reports they invalidate are */));
        let reports = use::batch(reports_in, nondet!(/** how stale a peer's reported length is when a decision is made */));
        let mut queue = use::state_null::<Stream<Task, Tick<_>, Bounded, TotalOrder>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut peer_len = use::state_null::<KeyedSingleton<MemberId<Worker>, usize, Tick<_>, Bounded>>();
        let mut last_sent_to = use::state_null::<KeyedSingleton<MemberId<Worker>, u64, Tick<_>, Bounded>>();

        let clock_elements = clock.clone().count().map(q!(|n| n as u64));
        let ticked = clock_elements.clone().map(q!(|n| n > 0));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        let budget = clock_elements.clone().map(q!(move |n| n as usize * work_per_tick));

        // Admission first: every migrated task costs one unit before any processing.
        let admitted_tasks = migrated_in.map(q!(|(_, task)| task)).sort();
        let admitted = admitted_tasks.clone().count();
        let process_budget = budget.zip(admitted.clone()).map(q!(|(b, a)| b.saturating_sub(a)));
        let all = queue.chain(admitted_tasks).chain(tasks.map(q!(|id| Task { id, hops: 0 })));
        let indexed = all.enumerate().cross_singleton(process_budget);
        let completed = indexed.clone().filter_map(q!(|((i, task), b)| (i < b).then_some(task)));
        let remaining = indexed.filter_map(q!(|((i, task), b)| (i >= b).then_some(task)));
        let my_len = remaining.clone().count();

        // Latest reported length per peer, and this worker's own report if a report tick fired.
        let fresh = reports.sort().into_keyed().first();
        peer_len = fresh.into_keyed_stream().chain(peer_len.into_keyed_stream()).first();
        let report_now = my_len.clone().filter_if(report_pump.count().map(q!(|n| n > 0)));

        // Rebalancing decision, on clock ticks only: among peers reported shorter by more than
        // the threshold and not sent to within the cooldown, pick the shortest and send it half
        // the difference from the tail of the queue.
        let recently_sent_to = last_sent_to
            .clone()
            .entries()
            .cross_singleton(now_cur.clone())
            .filter(q!(move |((_, sent_at), now)| *now - *sent_at < cooldown_ticks))
            .map(q!(|((peer, _), _)| peer));
        let candidates = peer_len
            .clone()
            .entries()
            .filter(q!(move |(peer, _)| *peer != CLUSTER_SELF_ID))
            .cross_singleton(my_len.clone())
            .filter(q!(move |((_, peer_len), mine)| *mine > *peer_len + threshold))
            .map(q!(|((peer, peer_len), _)| (peer, peer_len)))
            .into_keyed()
            .filter_key_not_in(recently_sent_to)
            .entries()
            .map(q!(|(peer, peer_len)| (peer_len, peer)))
            .filter_if(ticked);
        let target = candidates.min();
        let plan = target
            .zip(my_len.clone())
            .map(q!(|((peer_len, peer), mine)| (peer, mine - (mine - peer_len) / 2)));
        let planned = remaining.clone().enumerate().cross_singleton(plan.clone());
        let migrated = planned.clone().filter_map(q!(|((i, task), (peer, keep))| {
            (i >= keep).then_some((peer, Task { id: task.id, hops: task.hops + 1 }))
        }));
        let staying = planned.filter_map(q!(|((i, task), (_, keep))| (i < keep).then_some(task)));
        queue = staying.chain(remaining.filter_if(plan.clone().is_none()));

        let sent_now = plan
            .zip(now_cur)
            .map(q!(|((peer, _), now)| (peer, now)))
            .into_stream()
            .into_keyed()
            .first();
        last_sent_to = sent_now.into_keyed_stream().chain(last_sent_to.into_keyed_stream()).first();

        let stats = clock_elements
            .zip(admitted)
            .zip(completed.clone().count())
            .zip(migrated.clone().count())
            .zip(queue.clone().count())
            .map(q!(|((((clock_elements, admitted), processed), migrated), queue_len)| WorkerTick {
                clock_elements,
                admitted,
                processed,
                migrated,
                queue_len,
            }));

        (completed, migrated, report_now.into_stream(), stats.into_stream())
    };

    migrations_complete.complete(
        migrated
            .clone()
            .demux(workers, TCP.fail_stop().bincode())
            .entries(),
    );
    reports_complete.complete(
        reports_out
            .broadcast_closed(workers, TCP.fail_stop().bincode())
            .entries()
            .weaken_consistency(),
    );

    RebalanceOutputs {
        completed,
        migrated,
        ticks,
    }
}

/// The harness's workload: tasks per round per worker, with a window in which one worker gets
/// more, and the report period in rounds.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_per_worker: u32,
    /// Tasks per round for worker 0 inside the trigger window.
    pub trigger_for_worker_0: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
    /// `report_tick` is pumped once every this many rounds.
    pub report_every: u64,
}

impl Workload {
    pub fn tasks_at(&self, round: u64, worker: u32) -> u32 {
        if worker == 0 && round >= self.trigger_start && round < self.trigger_end {
            self.trigger_for_worker_0
        } else {
            self.baseline_per_worker
        }
    }
}

/// Simulation under a fixed, fair schedule. A *round* is one element on every worker's clock,
/// one element on every worker's report tick if the round is a multiple of `report_every`, and
/// that round's tasks, followed by letting the simulation quiesce. Tasks migrated in a round are
/// admitted by a further tick of the receiver in the same round, whose budget is zero, so they
/// are charged against the receiver's next clock tick.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Tasks processed this round, over all workers.
        completed: u64,
        /// Of those, tasks that had migrated at least twice: the ones that bounced.
        completed_bounced: u64,
        /// Migrations this round, over all workers.
        migrated: u64,
        /// Units spent admitting migrated tasks this round.
        admitted: u64,
        /// Queue lengths at the end of the round, per worker.
        queues: Vec<usize>,
    }

    /// Total queue length over all workers. (A free function rather than a method because `impl`
    /// blocks inside this module are not carried into the staged crate.)
    fn total_queue(r: &Round) -> usize {
        r.queues.iter().sum()
    }

    fn run(n: u32, workload: Workload, config: RebalanceConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let workers = flow.cluster::<Worker>();
        let (task_send, tasks) = workers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (report_send, report_tick) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = rebalancing_workers(&workers, tasks, clock, report_tick, config);
        let completed = outputs.completed.sim_cluster_output();
        let migrated = outputs.migrated.sim_cluster_output();
        let ticks = outputs.ticks.sim_cluster_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim()
            .with_cluster_size(&workers, n as usize)
            .run_prompt(async move || {
                let mut queues = vec![0usize; n as usize];
                let mut next_id = 0u64;
                for round in 0..rounds as u64 {
                    for worker in 0..n {
                        clock_send.send(worker, ());
                        if round % workload.report_every == 0 {
                            report_send.send(worker, ());
                        }
                        for _ in 0..workload.tasks_at(round, worker) {
                            task_send.send(worker, next_id);
                            next_id += 1;
                        }
                    }
                    quiesce().await;

                    let mut r = Round::default();
                    for worker in 0..n {
                        for task in completed.collect::<Vec<Task>>(worker).await {
                            r.completed += 1;
                            if task.hops >= 2 {
                                r.completed_bounced += 1;
                            }
                        }
                        r.migrated += migrated.collect::<Vec<(MemberId<Worker>, Task)>>(worker).await.len() as u64;
                        for t in ticks.collect::<Vec<WorkerTick>>(worker).await {
                            r.admitted += t.admitted as u64;
                            queues[worker as usize] = t.queue_len;
                        }
                    }
                    r.queues = queues.clone();
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
    /// Two workers, 3 tasks per round each against 5 units per round each (60% utilization),
    /// threshold 10, reports every 4 rounds. At baseline every task is processed in the round it
    /// arrives, queues are empty, no report differs from another by more than the threshold, and
    /// nothing migrates.
    ///
    /// The trigger gives worker 0 twelve tasks per round for 60 rounds. Its queue grows by 7 per
    /// round until, a report period or so in, it exceeds worker 1's reported length by more than
    /// 10 and it sends half the difference. Worker 1 has 2 spare units per round, so the cluster
    /// as a whole receives 15 tasks per round against a capacity of 10 and the total queue grows
    /// by at least 5 per round, to about 300 by round 160, before admission costs. Because reports
    /// are four ticks stale, the expectation was that a half-the-difference rule is unstable:
    /// worker 0 keeps sending while worker 1's report still says it is short, worker 1 then sees
    /// itself longer than worker 0's stale report and sends back, and the amplitude of the
    /// exchange grows until admission consumes the budget, nothing is processed, and the total
    /// queue grows without bound after the trigger.
    ///
    /// Measured: the first half of that happens and the second does not. Tasks do bounce (a
    /// task that has migrated twice completes in rounds 104 and 160, and migrations over a
    /// 400-round run are 537 with reports every 4 rounds against 195 with reports every round),
    /// but the exchange does not grow. The reason is that a worker's own length is always fresh
    /// and only the peer's is stale, so the decision is made on the true difference plus a bias
    /// bounded by what the peer's queue changed over one report period; that is a biased but
    /// stable loop, not the doubly delayed one the expectation assumed. The queues drain at 4 per
    /// round after the trigger and are empty by round 300 in every configuration. The label is
    /// therefore hazardous (stale reports make the schedule choose how many times a task is
    /// moved, and each move costs budget the input did not require) but not collapsing under this
    /// trigger; the confirmation is the staleness table rather than a collapse.
    ///
    /// With a cooldown of 8 ticks (two report periods) a worker's next decision toward a peer
    /// rests on a report that reflects its previous transfer, so no task should bounce at any
    /// report period, and with reports every round and no cooldown likewise.
    const N: u32 = 2;
    const WORKLOAD: Workload = Workload {
        baseline_per_worker: 3,
        trigger_for_worker_0: 12,
        trigger_start: 100,
        trigger_end: 160,
        report_every: 4,
    };
    const STALE_NO_COOLDOWN: RebalanceConfig = RebalanceConfig {
        work_per_tick: 5,
        threshold: 10,
        cooldown_ticks: 0,
    };
    const STALE_WITH_COOLDOWN: RebalanceConfig = RebalanceConfig {
        cooldown_ticks: 8,
        ..STALE_NO_COOLDOWN
    };

    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 104, 110, 130, 159, 160, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: completed={} bounced={} migrated={} admitted={} queues={:?}",
                    r.completed, r.completed_bounced, r.migrated, r.admitted, r.queues
                );
            }
        }
    }

    fn assert_healthy(trace: &[Round], from: usize, to: usize) {
        for (i, r) in trace[from..to].iter().enumerate() {
            let i = i + from;
            assert_eq!(r.completed, 6, "round {i}: every task processed in its round");
            assert_eq!(r.migrated, 0, "round {i}: nothing to rebalance");
            assert_eq!(r.admitted, 0, "round {i}: nothing admitted");
            assert!(r.queues.iter().all(|&q| q == 0), "round {i}: queues empty, got {:?}", r.queues);
        }
    }

    fn summarize(trace: &[Round]) -> (u64, u64, u64, u64) {
        let completed = sum(trace, TAIL_START, ROUNDS, |r| r.completed);
        let bounced = sum(trace, TAIL_START, ROUNDS, |r| r.completed_bounced);
        let migrated = sum(trace, TAIL_START, ROUNDS, |r| r.migrated);
        let admitted = sum(trace, TAIL_START, ROUNDS, |r| r.admitted);
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): completed {completed} (baseline would be {}), of which bounced {bounced}; migrated {migrated}; admission units {admitted}; total queue {} -> {}",
            6 * (ROUNDS - TAIL_START),
            total_queue(&trace[TAIL_START]),
            total_queue(&trace[ROUNDS - 1])
        );
        (completed, bounced, migrated, admitted)
    }

    #[test]
    fn stale_reports_make_tasks_bounce_but_the_cluster_recovers() {
        let trace = run(N, WORKLOAD, STALE_NO_COOLDOWN, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 0, 100);

        let (completed, bounced, migrated, admitted) = summarize(&trace);
        let migrated_total = sum(&trace, 0, ROUNDS, |r| r.migrated);
        let bounced_total = sum(&trace, 0, ROUNDS, |r| r.completed_bounced);
        let peak = trace.iter().map(total_queue).max().unwrap();
        let empty_from = trace[160..].iter().position(|r| total_queue(r) == 0).map(|i| i + 160);
        println!("migrated {migrated_total} tasks over the run, {bounced_total} of them more than once; peak total queue {peak}; empty from round {empty_from:?}");
        assert!(peak > 200, "the trigger should have built a queue, got {peak}");
        assert!(bounced_total > 0, "stale reports should make some tasks bounce");
        // The expected collapse did not happen: the tail is baseline.
        assert_eq!((completed, bounced, migrated, admitted), (6 * (ROUNDS - TAIL_START) as u64, 0, 0, 0));
        assert_healthy(&trace, TAIL_START, ROUNDS);
    }

    /// Control: stale reports, no trigger. Nothing ever exceeds the threshold.
    #[test]
    fn without_a_trigger_the_cluster_stays_healthy() {
        let trace = run(
            N,
            Workload {
                trigger_for_worker_0: WORKLOAD.baseline_per_worker,
                ..WORKLOAD
            },
            STALE_NO_COOLDOWN,
            ROUNDS,
        );
        print_trajectory(&trace);
        assert_healthy(&trace, 0, ROUNDS);
    }

    /// The cooldown configuration: same stale reports and trigger, cooldown of two report periods. A few
    /// productive migrations, no bounces, and a drain.
    #[test]
    fn with_a_cooldown_the_cluster_recovers() {
        let trace = run(N, WORKLOAD, STALE_WITH_COOLDOWN, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 0, 100);
        let migrated_total = sum(&trace, 0, ROUNDS, |r| r.migrated);
        let bounced_total = sum(&trace, 0, ROUNDS, |r| r.completed_bounced);
        let peak = trace.iter().map(total_queue).max().unwrap();
        let empty_from = trace[160..].iter().position(|r| total_queue(r) == 0).map(|i| i + 160);
        println!("migrated {migrated_total} tasks over the run, {bounced_total} of them more than once; peak total queue {peak}; empty from round {empty_from:?}");
        assert!(peak > 200, "the trigger should have built a queue, got {peak}");
        assert!(migrated_total > 0, "the skew should have been rebalanced at least once");
        assert_healthy(&trace, TAIL_START, ROUNDS);
    }

    /// The same program under fresh reports: same trigger, no cooldown, one report per tick.
    #[test]
    fn with_fresh_reports_the_cluster_recovers() {
        let trace = run(N, Workload { report_every: 1, ..WORKLOAD }, STALE_NO_COOLDOWN, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 0, 100);
        let migrated_total = sum(&trace, 0, ROUNDS, |r| r.migrated);
        let bounced_total = sum(&trace, 0, ROUNDS, |r| r.completed_bounced);
        let peak = trace.iter().map(total_queue).max().unwrap();
        let empty_from = trace[160..].iter().position(|r| total_queue(r) == 0).map(|i| i + 160);
        println!("migrated {migrated_total} tasks over the run, {bounced_total} of them more than once; peak total queue {peak}; empty from round {empty_from:?}");
        assert!(peak > 200, "the trigger should have built a queue, got {peak}");
        assert_healthy(&trace, TAIL_START, ROUNDS);
    }

    /// Hazard confirmation by staleness. Same trigger, 400 rounds, reports every `k` rounds.
    /// The columns are migrations over the run and tasks completed after migrating more than
    /// once. Migrations under fresh reports move work toward idle capacity; bounces are pure
    /// waste, since a task that has come back is where it started and has cost two admissions.
    /// Expected before measuring: no bounces at `k` of 1 and with the cooldown at every `k`,
    /// bounces and a multiple of the migrations at `k` of 3 or more without it.
    #[test]
    fn migrations_grow_with_report_staleness_unless_cooled_down() {
        const RUN: usize = 400;
        let periods = [1u64, 2, 3, 4, 6, 8];
        let measure = |k: u64, config: RebalanceConfig| {
            let trace = run(N, Workload { report_every: k, ..WORKLOAD }, config, RUN);
            (sum(&trace, 0, RUN, |r| r.migrated), sum(&trace, 0, RUN, |r| r.completed_bounced))
        };
        let hot: Vec<(u64, u64)> = periods.iter().map(|&k| measure(k, STALE_NO_COOLDOWN)).collect();
        let cooled: Vec<(u64, u64)> = periods.iter().map(|&k| measure(k, STALE_WITH_COOLDOWN)).collect();
        for ((k, h), c) in periods.iter().zip(&hot).zip(&cooled) {
            println!(
                "reports every {k} rounds -> over {RUN} rounds: no cooldown migrated {} bounced {}, cooldown 8 migrated {} bounced {}",
                h.0, h.1, c.0, c.1
            );
        }
        assert_eq!(hot[0].1, 0, "fresh reports should bounce nothing: {hot:?}");
        assert!(hot[2..].iter().all(|h| h.1 > 0), "stale reports should bounce tasks: {hot:?}");
        assert!(hot[3].0 >= 2 * hot[0].0 && hot[5].0 >= 2 * hot[0].0, "stale reports should multiply migrations: {hot:?}");
        assert!(cooled.iter().all(|c| c.1 == 0), "the cooldown should bounce nothing: {cooled:?}");
        assert!(cooled.iter().all(|c| c.0 <= 2 * hot[0].0), "the cooldown should keep migrations near the fresh-report count: {cooled:?} vs {}", hot[0].0);
    }
}
