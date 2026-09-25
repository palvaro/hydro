//! Size-or-time batching whose open batch is retained until item acknowledgements arrive.
//!
//! The batcher assigns each application item an id and retains it in an open batch. A clock
//! element flushes every retained item when either `batch_size` items are waiting or the oldest
//! item has waited `flush_after_ticks`. A flush does not remove items: only acknowledgements do.
//! Consequently, a delayed acknowledgement lets later clock elements put another copy of an item
//! on the wire. The sink queues copies, processes at most `max_per_tick` per sink clock element,
//! and acknowledges every processed copy. Duplicate copies consume the same capacity as first
//! copies. This makes the batching policy hazardous.
//!
//! In deployment, `batcher.source_interval(period)` supplies `batcher_clock`, and
//! `sink.source_interval(period)` supplies `sink_clock`. Simulation supplies both as explicit
//! streams, because simulated locations have no wall clock.
//!
//! # Measured label
//!
//! | baseline / round | sink capacity | trigger | size / time threshold | asserted tail | label and basis |
//! |---|---|---|---|---|---|
//! | 2 items | 8 copies | 12 items/round, rounds 100..120 | 8 items / 4 ticks | rounds 600..800: 18 completions, 225,506 sent copies; pending 938 -> 1,318; sink FIFO 240,682 -> 463,658 | hazardous, by exhibited collapse |
//!
//! Under the prompt schedule, the trigger run's rounds 600..800 complete 18 of the healthy 400
//! items while sending 225,506 copies. Pending application items grow from 938 to 1,318 and the
//! sink FIFO grows from 240,682 to 463,658. The trigger ended at round 120, so this continuing
//! storm 480 rounds later is an exhibited collapse. The no-trigger control completes at least all
//! but one bounded batch and keeps each queue at or below eight.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::amplification::{SimOutputs, amplification_check};
use serde::{Deserialize, Serialize};

pub struct Batcher;
pub struct Sink;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pending {
    pub arrived_tick: u64,
}

/// One item copy in a logical flush. Copies with the same `flush_tick` form one batch.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FlushCopy {
    pub item_id: u64,
    pub flush_tick: u64,
}

/// The first acknowledgement observed for an application item.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Completion {
    pub item_id: u64,
    pub latency_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    /// Flush immediately once this many unacknowledged items are retained.
    pub batch_size: usize,
    /// Flush when the oldest retained item has waited this many batcher ticks.
    pub flush_after_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct SinkConfig {
    /// Item copies processed per sink clock element.
    pub max_per_tick: u32,
}

#[derive(SimOutputs)]
pub struct BatchOutputs<'a> {
    /// Every item copy put on the wire. Equal `flush_tick` values identify a logical batch.
    pub sent: Stream<FlushCopy, Process<'a, Batcher>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every copy processed by the sink, including duplicates.
    pub processed: Stream<FlushCopy, Process<'a, Sink>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement put on the wire by the sink, one per processed copy.
    pub acknowledgements: Stream<u64, Process<'a, Sink>, Unbounded, TotalOrder, ExactlyOnce>,
    /// First acknowledgements, with latency from application arrival in batcher ticks.
    pub completed: Stream<Completion, Process<'a, Batcher>, Unbounded, NoOrder, ExactlyOnce>,
    /// Unacknowledged application items after each batcher tick.
    pub pending_trace: Stream<usize, Process<'a, Batcher>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Queued item copies after each sink tick or arrival admission.
    pub sink_backlog_trace: Stream<usize, Process<'a, Sink>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the size-or-time batcher and bounded-capacity sink.
#[amplification_check(
    ignore = "this check takes about twenty minutes at the default horizon; run it with --include-ignored",
    workload(items = 2),
    batch_config = BatchConfig { batch_size: 8, flush_after_ticks: 4 },
    sink_config = SinkConfig { max_per_tick: 8 },
)]
pub fn batch_flush<'a>(
    batcher: &Process<'a, Batcher>,
    sink: &Process<'a, Sink>,
    items: Stream<(), Process<'a, Batcher>, Unbounded, TotalOrder, ExactlyOnce>,
    batcher_clock: Stream<(), Process<'a, Batcher>, Unbounded, TotalOrder, ExactlyOnce>,
    sink_clock: Stream<(), Process<'a, Sink>, Unbounded, TotalOrder, ExactlyOnce>,
    batch_config: BatchConfig,
    sink_config: SinkConfig,
) -> BatchOutputs<'a> {
    let BatchConfig {
        batch_size,
        flush_after_ticks,
    } = batch_config;
    let SinkConfig { max_per_tick } = sink_config;

    let (acks_complete, acks) =
        batcher
            .forward_ref::<Stream<u64, Process<'a, Batcher>, Unbounded, TotalOrder, ExactlyOnce>>();

    let (sent, completed, pending_trace) = sliced! {
        let clock = use::batch(batcher_clock.enumerate(), nondet!(/** batching changes when a logical batching deadline is observed, but every clock element still advances logical time */));
        let arrivals = use::batch(items.enumerate(), nondet!(/** batching changes which open batch receives an item, while its globally assigned id and eventual acknowledgement remain unchanged */));
        let acks = use::batch(acks, nondet!(/** batching changes whether an acknowledgement removes an item before the next size-or-time flush */));
        let mut pending = use::state_null::<KeyedSingleton<u64, Pending, Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));

        let ticked = clock.clone().count().map(q!(|n| n > 0));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let acked_ids = acks.unique();
        let completed = acked_ids
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(pending.clone())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((item_id, (_, p)), now)| Completion {
                item_id,
                latency_ticks: now - p.arrived_tick,
            }));

        let retained = pending.filter_key_not_in(acked_ids);
        let added = arrivals
            .cross_singleton(now_cur.clone())
            .map(q!(|((id, ()), now)| (id as u64, Pending { arrived_tick: now })))
            .into_keyed();
        // Arrival ids are globally fresh, so the two sides have disjoint keys.
        let all = retained.into_keyed_stream().chain(added).first();
        let entries = all.clone().into_keyed_stream().entries();
        let count = entries.clone().count();
        let oldest = entries
            .clone()
            .map(q!(|(_, p)| p.arrived_tick))
            .min()
            .unwrap_or(now_cur.clone());
        let flush = count
            .clone()
            .zip(oldest)
            .zip(now_cur.clone())
            .filter(q!(move |((count, oldest), now)| {
                *count >= batch_size || (*count > 0 && now - oldest >= flush_after_ticks)
            }))
            .filter_if(ticked.clone());

        let sent = entries
            .filter_if(flush.is_some())
            .cross_singleton(now_cur)
            .map(q!(|((item_id, _), flush_tick)| FlushCopy { item_id, flush_tick }))
            .sort();
        pending = all;
        let pending_trace = count.filter_if(ticked).into_stream();

        (sent, completed, pending_trace)
    };

    let incoming = sent.clone().send(sink, TCP.fail_stop().bincode());
    let (processed, sink_backlog_trace) = sliced! {
        let clock = use::batch(sink_clock, nondet!(/** batching only combines sink capacity grants; every clock element grants exactly `max_per_tick` copy-processing slots */));
        let arrivals = use::batch(incoming, nondet!(/** batching changes when flushed copies enter the FIFO and therefore how late their acknowledgements become */));
        let mut backlog = use::state_null::<Stream<FlushCopy, Tick<_>, Bounded, TotalOrder>>();

        let budget = clock.count().map(q!(move |n| n * max_per_tick as usize));
        let queued = backlog.chain(arrivals).enumerate().cross_singleton(budget);
        let processed = queued
            .clone()
            .filter_map(q!(|((i, copy), budget)| (i < budget).then_some(copy)));
        backlog = queued.filter_map(q!(|((i, copy), budget)| (i >= budget).then_some(copy)));
        (processed, backlog.clone().count().into_stream())
    };

    let acknowledgements = processed.clone().map(q!(|copy| copy.item_id));
    acks_complete.complete(
        acknowledgements
            .clone()
            .send(batcher, TCP.fail_stop().bincode()),
    );

    BatchOutputs {
        sent,
        processed,
        acknowledgements,
        completed,
        pending_trace,
        sink_backlog_trace,
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct Round {
        sent: u64,
        processed: u64,
        acknowledgements: u64,
        completed: u64,
        latency_sum: u64,
        pending: usize,
        sink_backlog: usize,
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
        let batcher = flow.process::<Batcher>();
        let sink = flow.process::<Sink>();
        let (items_send, items) = batcher.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (batcher_clock_send, batcher_clock) =
            batcher.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (sink_clock_send, sink_clock) = sink.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = batch_flush(
            &batcher,
            &sink,
            items,
            batcher_clock,
            sink_clock,
            BATCH,
            SINK,
        );
        let sent = outputs.sent.sim_output();
        let processed = outputs.processed.sim_output();
        let acknowledgements = outputs.acknowledgements.sim_output();
        let completed = outputs.completed.sim_output();
        let pending_trace = outputs.pending_trace.sim_output();
        let sink_backlog_trace = outputs.sink_backlog_trace.sim_output();

        let mut trace = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            let mut pending = 0;
            let mut sink_backlog = 0;
            for round in 0..rounds {
                let offered = if round >= workload.trigger_start && round < workload.trigger_end {
                    workload.trigger
                } else {
                    workload.baseline
                };
                items_send.send_many((0..offered).map(|_| ()));
                batcher_clock_send.send(());
                sink_clock_send.send(());
                quiesce().await;

                let sent_count = sent.collect::<Vec<_>>().await.len() as u64;
                let processed_count = processed.collect::<Vec<_>>().await.len() as u64;
                let acknowledgement_count = acknowledgements.collect::<Vec<_>>().await.len() as u64;
                let completions = completed.collect_sorted::<Vec<_>>().await;
                while let Some(value) = pending_trace.try_next().await {
                    pending = value;
                }
                while let Some(value) = sink_backlog_trace.try_next().await {
                    sink_backlog = value;
                }
                trace_ref.push(Round {
                    sent: sent_count,
                    processed: processed_count,
                    acknowledgements: acknowledgement_count,
                    completed: completions.len() as u64,
                    latency_sum: completions.iter().map(|c| c.latency_ticks).sum(),
                    pending,
                    sink_backlog,
                });
            }
        });
        trace
    }

    fn total(trace: &[Round], from: usize, field: impl Fn(&Round) -> u64) -> u64 {
        trace[from..].iter().map(field).sum()
    }

    /// Hand-computed expectation, written before measurement.
    ///
    /// Baseline is two application items per round and eight copy-processing slots. With a batch
    /// size of eight, the batcher flushes roughly every four rounds. The sink can process all eight
    /// copies on its next tick, so acknowledgements should empty the batch before another flush.
    ///
    /// The bounded trigger raises arrivals to 12 per round for rounds 100..120, adding 200 items.
    /// Once the pending set exceeds eight, every batcher tick flushes it. During the trigger at
    /// least four more distinct items arrive than the sink can acknowledge per round, while all
    /// retained items are copied again. The copy FIFO should therefore exceed 2,000 by round 120.
    /// Old duplicate copies then consume the eight-slot capacity, acknowledgements for already
    /// completed ids cease helping, and two fresh baseline items keep the open batch and copy FIFO
    /// growing. In rounds 600..800, fewer than one quarter of the healthy 400 completions should
    /// occur, sent copies should exceed 10,000, and both pending and sink backlog should grow.
    const BATCH: BatchConfig = BatchConfig {
        batch_size: 8,
        flush_after_ticks: 4,
    };
    const SINK: SinkConfig = SinkConfig { max_per_tick: 8 };
    const WORKLOAD: Workload = Workload {
        baseline: 2,
        trigger: 12,
        trigger_start: 100,
        trigger_end: 120,
    };
    const ROUNDS: usize = 800;
    const TAIL: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [3, 20, 99, 100, 110, 119, 120, 200, 400, 600, 700, 799] {
            let r = &trace[i];
            println!(
                "round {i}: sent={} processed={} acknowledgements={} completed={} latency_sum={} pending={} sink_backlog={}",
                r.sent,
                r.processed,
                r.acknowledgements,
                r.completed,
                r.latency_sum,
                r.pending,
                r.sink_backlog
            );
        }
    }

    #[test]
    fn delayed_batch_acknowledgements_exhibit_persistent_collapse() {
        let trace = run(WORKLOAD, ROUNDS);
        print_trajectory(&trace);
        let pre_completed = total(&trace[..100], 20, |r| r.completed);
        let tail_completed = total(&trace, TAIL, |r| r.completed);
        let tail_sent = total(&trace, TAIL, |r| r.sent);
        println!(
            "tail rounds {TAIL}..{ROUNDS}: completed={tail_completed}, sent={tail_sent}, pending {} -> {}, sink backlog {} -> {}",
            trace[TAIL].pending,
            trace[ROUNDS - 1].pending,
            trace[TAIL].sink_backlog,
            trace[ROUNDS - 1].sink_backlog
        );
        assert!(
            pre_completed >= 150,
            "baseline should complete nearly all offered work: {pre_completed}"
        );
        assert!(
            tail_completed < 100,
            "tail completions should be below one quarter of the healthy 400: {tail_completed}"
        );
        assert!(
            tail_sent > 10_000,
            "the tail should still carry a flush storm: {tail_sent}"
        );
        assert!(trace[ROUNDS - 1].pending > trace[TAIL].pending);
        assert!(trace[ROUNDS - 1].sink_backlog > trace[TAIL].sink_backlog);
    }

    /// Control: with the trigger removed, every bounded batch is acknowledged before it is flushed
    /// again, so all input completes and both queues remain bounded.
    #[test]
    fn without_the_burst_batching_stays_healthy() {
        let trace = run(
            Workload {
                trigger: WORKLOAD.baseline,
                ..WORKLOAD
            },
            ROUNDS,
        );
        print_trajectory(&trace);
        let offered = WORKLOAD.baseline as u64 * ROUNDS as u64;
        let completed = total(&trace, 0, |r| r.completed);
        assert!(trace.iter().all(|r| r.processed == r.acknowledgements));
        assert!(completed >= offered - BATCH.batch_size as u64);
        assert!(trace.iter().all(|r| r.pending <= BATCH.batch_size));
        assert!(trace.iter().all(|r| r.sink_backlog <= BATCH.batch_size));
        assert!(trace[TAIL..].iter().all(|r| r.completed <= 8));
    }
}
