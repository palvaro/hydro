//! A work queue with visibility-timeout redelivery.
//!
//! The broker assigns every submitted payload an id and immediately delivers it to the worker. It
//! retains the task in an outstanding table until a completion arrives. Every broker clock element
//! advances logical time. If an outstanding delivery has been invisible for
//! [`QueueConfig::visibility_ticks`], the broker puts the same task on the wire again. The worker
//! appends deliveries to one FIFO and processes at most [`WorkerConfig::max_per_tick`] entries per
//! worker clock element. Processing a repeated delivery is real work even when its eventual
//! completion is stale at the broker.
//!
//! A deployment wires `broker.source_interval(period)` and `worker.source_interval(period)` into
//! `broker_clock` and `worker_clock`. The simulation supplies both clocks with `sim_input`; the
//! dataflow never reads wall-clock time.
//!
//! # Label
//!
//! | run | baseline submissions / round | worker capacity | trigger | visibility | tail first / repeated processing | pending at 600 -> 800 | worker backlog at 600 -> 800 | label and basis |
//! |---|---:|---:|---|---:|---:|---:|---:|---|
//! | redelivery | 2 | 5 | 12 / round in `[100, 160)` | 6 | 63 / 937 | 927 -> 1264 | 50216 -> 85465 | hazardous, by exhibited collapse |
//! | no trigger | 2 | 5 | none | 6 | 1600 / 0 over all 800 rounds | at most 2 | at most 2 | healthy control |
//!
//! The hand calculation appears on the workload constants in `sim_tests`. The bounded trigger
//! creates enough worker delay for the broker to make copies. Those copies share the worker FIFO
//! with first deliveries, delay completions further, and cause still more visibility expirations.
//! The collapse test establishes the hazardous label constructively by asserting that repeated
//! processing dominates the tail and that redundant work remains backlogged hundreds of rounds
//! after the trigger ends.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::amplification::{SimOutputs, amplification_check};
use serde::{Deserialize, Serialize};

pub struct QueueBroker;
pub struct QueueWorker;

#[derive(Clone, Copy, Debug)]
pub struct QueueConfig {
    /// Number of broker clock ticks before an uncompleted delivery becomes visible again.
    pub visibility_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct WorkerConfig {
    /// FIFO entries processed per worker clock element.
    pub max_per_tick: u32,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Delivery {
    pub id: u64,
    pub payload: u64,
    pub first_sent: u64,
    pub redelivery: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    pub latency_ticks: u64,
    pub repeated_delivery: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Outstanding {
    pub payload: u64,
    pub first_sent: u64,
    pub last_sent: u64,
}

#[derive(Clone, Copy, Debug)]
enum VisibilityVerdict {
    Keep((u64, Outstanding)),
    Redeliver((u64, Outstanding)),
}

#[derive(SimOutputs)]
pub struct VisibilityQueueOutputs<'a> {
    /// Every initial delivery and redelivery put on the wire.
    pub delivered: Stream<Delivery, Process<'a, QueueBroker>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every FIFO entry processed, including duplicates.
    pub processed: Stream<Completion, Process<'a, QueueWorker>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Outstanding tasks after each broker tick.
    pub pending_trace: Stream<usize, Process<'a, QueueBroker>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Waiting FIFO entries after each worker tick.
    pub worker_backlog_trace:
        Stream<usize, Process<'a, QueueWorker>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds a visibility-timeout broker and one bounded-capacity worker.
#[amplification_check(
    workload(submissions = 2),
    queue_config = QueueConfig { visibility_ticks: 6 },
    worker_config = WorkerConfig { max_per_tick: 5 },
)]
pub fn visibility_queue<'a>(
    broker: &Process<'a, QueueBroker>,
    worker: &Process<'a, QueueWorker>,
    submissions: Stream<u64, Process<'a, QueueBroker>, Unbounded, TotalOrder, ExactlyOnce>,
    broker_clock: Stream<(), Process<'a, QueueBroker>, Unbounded, TotalOrder, ExactlyOnce>,
    worker_clock: Stream<(), Process<'a, QueueWorker>, Unbounded, TotalOrder, ExactlyOnce>,
    queue_config: QueueConfig,
    worker_config: WorkerConfig,
) -> VisibilityQueueOutputs<'a> {
    let QueueConfig { visibility_ticks } = queue_config;
    let WorkerConfig { max_per_tick } = worker_config;
    assert!(visibility_ticks > 0, "visibility timeout must be positive");

    let (acks_complete, acks) = broker.forward_ref::<Stream<
        u64,
        Process<'a, QueueBroker>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >>();

    let (delivered, pending_trace) = sliced! {
        let clock = use::batch(
            broker_clock.enumerate(),
            nondet!(/** batching changes which broker tick observes an event, but every clock element advances logical time once */),
        );
        let arrivals = use::batch(
            submissions.enumerate(),
            nondet!(/** batching changes when submitted work enters the outstanding table, but neither its id nor payload */),
        );
        let completions = use::batch(
            acks,
            nondet!(/** delaying a completion can make its task visible and therefore create another delivery */),
        );
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut outstanding =
            use::state_null::<KeyedSingleton<u64, Outstanding, Tick<_>, Bounded>>();

        let now_cur = clock
            .map(q!(|(tick, _)| tick as u64))
            .max()
            .unwrap_or(now.clone());
        now = now_cur.clone();

        let completed_ids = completions.unique();
        let waiting = outstanding
            .filter_key_not_in(completed_ids)
            .into_keyed_stream()
            .entries();
        let judged = waiting
            .cross_singleton(now_cur.clone())
            .map(q!(move |((id, task), now)| {
                if now - task.last_sent >= visibility_ticks {
                    VisibilityVerdict::Redeliver((
                        id,
                        Outstanding {
                            last_sent: now,
                            ..task
                        },
                    ))
                } else {
                    VisibilityVerdict::Keep((id, task))
                }
            }));
        let kept = judged.clone().filter_map(q!(|verdict| match verdict {
            VisibilityVerdict::Keep(task) => Some(task),
            VisibilityVerdict::Redeliver(_) => None,
        }));
        let expired = judged.filter_map(q!(|verdict| match verdict {
            VisibilityVerdict::Redeliver(task) => Some(task),
            VisibilityVerdict::Keep(_) => None,
        }));

        let started = arrivals
            .cross_singleton(now_cur)
            .map(q!(|((id, payload), now)| {
                (
                    id as u64,
                    Outstanding {
                        payload,
                        first_sent: now,
                        last_sent: now,
                    },
                )
            }));

        let next = kept.chain(expired.clone()).chain(started.clone()).sort();
        let pending_trace = next.clone().count().into_stream();
        outstanding = next.into_keyed().first();

        let first_deliveries = started.map(q!(|(id, task)| Delivery {
            id,
            payload: task.payload,
            first_sent: task.first_sent,
            redelivery: false,
        }));
        let redeliveries = expired.map(q!(|(id, task)| Delivery {
            id,
            payload: task.payload,
            first_sent: task.first_sent,
            redelivery: true,
        }));
        (first_deliveries.chain(redeliveries).sort(), pending_trace)
    };

    let incoming = delivered.clone().send(worker, TCP.fail_stop().bincode());
    let (processed, worker_backlog_trace) = sliced! {
        let clock = use::batch(
            worker_clock.enumerate(),
            nondet!(/** batching only combines clock elements' fixed processing capacity */),
        );
        let deliveries = use::batch(
            incoming,
            nondet!(/** batching controls how long a delivery waits before entering the worker FIFO */),
        );
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut backlog = use::state_null::<Stream<Delivery, Tick<_>, Bounded, TotalOrder>>();

        let now_cur = clock
            .clone()
            .map(q!(|(tick, _)| tick as u64))
            .max()
            .unwrap_or(now.clone());
        now = now_cur.clone();
        let budget = clock.count().map(q!(move |n| n * max_per_tick as usize));
        let queued = backlog
            .chain(deliveries)
            .enumerate()
            .cross_singleton(budget);
        let served = queued
            .clone()
            .filter_map(q!(|((position, delivery), budget)| {
                (position < budget).then_some(delivery)
            }));
        backlog = queued.filter_map(q!(|((position, delivery), budget)| {
            (position >= budget).then_some(delivery)
        }));
        let worker_backlog_trace = backlog.clone().count().into_stream();
        let processed = served.cross_singleton(now_cur).map(q!(|(delivery, now)| Completion {
            id: delivery.id,
            latency_ticks: now - delivery.first_sent,
            repeated_delivery: delivery.redelivery,
        }));
        (processed, worker_backlog_trace)
    };

    acks_complete.complete(
        processed
            .clone()
            .map(q!(|completion| completion.id))
            .send(broker, TCP.fail_stop().bincode()),
    );

    VisibilityQueueOutputs {
        delivered,
        processed,
        pending_trace,
        worker_backlog_trace,
    }
}

#[cfg(test)]
mod sim_tests {
    use std::collections::HashSet;

    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct Round {
        delivered_first: usize,
        delivered_repeat: usize,
        processed_first: usize,
        processed_repeat: usize,
        useful_completions: usize,
        pending: usize,
        worker_backlog: usize,
    }

    #[derive(Clone, Copy)]
    struct Workload {
        baseline: u32,
        trigger: u32,
        trigger_start: usize,
        trigger_end: usize,
    }

    fn run(workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let broker = flow.process::<QueueBroker>();
        let worker = flow.process::<QueueWorker>();
        let (submission_send, submissions) = broker.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (broker_clock_send, broker_clock) = broker.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (worker_clock_send, worker_clock) = worker.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = visibility_queue(
            &broker,
            &worker,
            submissions,
            broker_clock,
            worker_clock,
            QueueConfig {
                visibility_ticks: VISIBILITY_TICKS,
            },
            WorkerConfig {
                max_per_tick: CAPACITY,
            },
        );
        let delivered = outputs.delivered.sim_output();
        let processed = outputs.processed.sim_output();
        let pending_trace = outputs.pending_trace.sim_output();
        let worker_backlog_trace = outputs.worker_backlog_trace.sim_output();

        let mut trace = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            let mut next_payload = 0u64;
            let mut completed = HashSet::new();
            let mut pending = 0;
            let mut worker_backlog = 0;
            for round_index in 0..rounds {
                broker_clock_send.send(());
                worker_clock_send.send(());
                let offered =
                    if (workload.trigger_start..workload.trigger_end).contains(&round_index) {
                        workload.trigger
                    } else {
                        workload.baseline
                    };
                for _ in 0..offered {
                    submission_send.send(next_payload);
                    next_payload += 1;
                }
                quiesce().await;

                let mut round = Round::default();
                for item in delivered.collect::<Vec<_>>().await {
                    if item.redelivery {
                        round.delivered_repeat += 1;
                    } else {
                        round.delivered_first += 1;
                    }
                }
                for item in processed.collect::<Vec<_>>().await {
                    if item.repeated_delivery {
                        round.processed_repeat += 1;
                    } else {
                        round.processed_first += 1;
                    }
                    if completed.insert(item.id) {
                        round.useful_completions += 1;
                    }
                }
                for depth in pending_trace.collect::<Vec<_>>().await {
                    pending = depth;
                }
                for depth in worker_backlog_trace.collect::<Vec<_>>().await {
                    worker_backlog = depth;
                }
                round.pending = pending;
                round.worker_backlog = worker_backlog;
                trace_ref.push(round);
            }
        });
        trace
    }

    /// At baseline, two submissions consume two of five worker slots each round, so neither the
    /// broker nor worker accumulates work. During rounds 100 through 159, twelve submissions exceed
    /// capacity by seven per round and build about 420 FIFO entries before redelivery. With a
    /// six-tick visibility timeout, that delay makes roughly every outstanding task offer another
    /// copy every six rounds. Once pending exceeds 30, redelivery alone can exceed capacity five,
    /// so the queue should keep growing after the bounded trigger ends. The tail is rounds 600 to
    /// 799, 440 rounds after the trigger ended.
    const BASELINE: u32 = 2;
    const TRIGGER: u32 = 12;
    const TRIGGER_START: usize = 100;
    const TRIGGER_END: usize = 160;
    const CAPACITY: u32 = 5;
    const VISIBILITY_TICKS: u64 = 6;
    const ROUNDS: usize = 800;

    #[test]
    fn visibility_redelivery_causes_collapse() {
        let trace = run(
            Workload {
                baseline: BASELINE,
                trigger: TRIGGER,
                trigger_start: TRIGGER_START,
                trigger_end: TRIGGER_END,
            },
            ROUNDS,
        );
        let pre = &trace[20..100];
        assert_eq!(pre.iter().map(|r| r.useful_completions).sum::<usize>(), 160);
        assert_eq!(pre.iter().map(|r| r.delivered_repeat).sum::<usize>(), 0);
        assert!(pre.iter().all(|r| r.worker_backlog <= BASELINE as usize));

        let tail = &trace[600..800];
        let tail_first = tail.iter().map(|r| r.processed_first).sum::<usize>();
        let tail_repeat = tail.iter().map(|r| r.processed_repeat).sum::<usize>();
        let tail_useful = tail.iter().map(|r| r.useful_completions).sum::<usize>();
        eprintln!(
            "tail first={tail_first} repeated={tail_repeat} useful={tail_useful} pending {} -> {}, worker backlog {} -> {}",
            trace[599].pending,
            trace[799].pending,
            trace[599].worker_backlog,
            trace[799].worker_backlog,
        );
        assert!(
            tail_repeat >= 900,
            "almost all tail capacity should be redundant"
        );
        assert!(
            tail_first <= 100,
            "first processing should remain near zero"
        );
        assert!(
            tail_useful <= 100,
            "useful completions should remain near zero"
        );
        assert!(trace[799].worker_backlog > trace[599].worker_backlog + 1_000);
    }

    #[test]
    fn without_a_trigger_the_queue_stays_healthy() {
        let trace = run(
            Workload {
                baseline: BASELINE,
                trigger: BASELINE,
                trigger_start: TRIGGER_START,
                trigger_end: TRIGGER_END,
            },
            ROUNDS,
        );
        assert_eq!(
            trace.iter().map(|r| r.delivered_first).sum::<usize>(),
            ROUNDS * BASELINE as usize
        );
        assert_eq!(trace.iter().map(|r| r.delivered_repeat).sum::<usize>(), 0);
        assert_eq!(trace.iter().map(|r| r.processed_repeat).sum::<usize>(), 0);
        assert!(trace.iter().all(|r| r.pending <= BASELINE as usize));
        assert!(trace.iter().all(|r| r.worker_backlog <= BASELINE as usize));
    }
}
