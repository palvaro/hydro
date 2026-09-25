//! The frozen amplification checker run on the blind witness programs.
//!
//! The seven programs under `blind/` were written by an author who knew nothing about the
//! checker. This module was written by an operator who read, for each program, only its public
//! function signature and the steady-state (no-trigger) input its own harness feeds per round.
//! The verdicts in `design_docs/2026-09_blind_results.md` were recorded before any label was
//! read, under the fixed hold grid the checker had at the time (holds of up to 100 rounds from
//! round 20, 240 rounds). The checker has since replaced the grid with a geometric sequence of
//! holds up to the horizon, and these tests now run at the default horizon
//! (`CheckConfig::default()`); every verdict is unchanged. The named hook moved in the four
//! hazardous programs, in each case to another edge of the same resend loop, because the last
//! hold lasts to the end of the run and a permanent hold on any edge of the loop makes the
//! sender exhaust its resends, so several hooks reach the same ceiling on sender messages and
//! the tie goes to identity order.
//!
//! Each test prints the full `Report` and asserts nothing about the verdict.

#[cfg(test)]
mod blind_check {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::amplification::{CheckConfig, Report, check};

    /// The default horizon, so these tests validate the configuration a caller gets without
    /// choosing one.
    fn config() -> CheckConfig {
        CheckConfig::default()
    }

    fn show(label: &str, r: &Report) {
        println!("[blind {label}]\n{r}");
        if let Some(l) = &r.location {
            println!(
                "[blind {label}] location: hook {} at {} rose {} extra work {} first reacted at {}",
                l.hook, l.source_location, l.rose, l.extra_work, l.first_reaction_at
            );
        }
        for c in &r.curves {
            let extra: Vec<i64> = c.extra_admitted.iter().map(|(_, e)| *e).collect();
            println!("[blind {label}] curve {} extra_admitted {:?}", c.hook, extra);
            for (k, m) in &c.extra_sends_by_member {
                let nonzero: Vec<String> = m
                    .iter()
                    .filter(|(_, d)| **d != 0)
                    .map(|(p, d)| format!("{p}:{d}"))
                    .collect();
                if !nonzero.is_empty() {
                    println!("[blind {label}]   k={k} extra sends {}", nonzero.join(" "));
                }
            }
        }
    }

    // batch_flush: batch_size 8, flush_after_ticks 4, sink max_per_tick 8; 2 items per round,
    // one batcher clock and one sink clock element per round.
    #[test]
    #[ignore = "this check takes about twenty minutes at the default horizon; run it with --include-ignored"]
    fn blind_batch_flush() {
        use crate::cluster::witnesses::blind::batch_flush::{
            BatchConfig, Batcher, Sink, SinkConfig, batch_flush,
        };
        let mut flow = FlowBuilder::new();
        let batcher = flow.process::<Batcher>();
        let sink = flow.process::<Sink>();
        let (items_send, items) = batcher.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (batcher_clock_send, batcher_clock) = batcher.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (sink_clock_send, sink_clock) = sink.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = batch_flush(
            &batcher,
            &sink,
            items,
            batcher_clock,
            sink_clock,
            BatchConfig {
                batch_size: 8,
                flush_after_ticks: 4,
            },
            SinkConfig { max_per_tick: 8 },
        );
        let _sent = outputs.sent.sim_output();
        let _processed = outputs.processed.sim_output();
        let _acks = outputs.acknowledgements.sim_output();
        let _completed = outputs.completed.sim_output();
        let _pending = outputs.pending_trace.sim_output();
        let _backlog = outputs.sink_backlog_trace.sim_output();
        let r = check(flow.sim(), &config(), async |_round| {
            items_send.send_many((0..2).map(|_| ()));
            batcher_clock_send.send(());
            sink_clock_send.send(());
        });
        show("batch_flush", &r);
    }

    // bounded_fanout: 3 subscribers, subscribers_per_publication 3; 2 publications per round.
    #[test]
    fn blind_bounded_fanout() {
        use crate::cluster::witnesses::blind::bounded_fanout::{
            FanoutConfig, Publisher, Subscriber, bounded_fanout,
        };
        const FANOUT: u32 = 3;
        let mut flow = FlowBuilder::new();
        let publisher = flow.process::<Publisher>();
        let subscribers = flow.cluster::<Subscriber>();
        let (publication_send, publications) = publisher.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = bounded_fanout(
            &publisher,
            &subscribers,
            publications,
            FanoutConfig {
                subscribers_per_publication: FANOUT,
            },
        );
        let _wire = outputs.wire.sim_output();
        let _processed = outputs.processed.sim_cluster_output();
        let _pending = outputs.pending.sim_cluster_output();
        let r = check(
            flow.sim().with_cluster_size(&subscribers, FANOUT as usize),
            &config(),
            async |round| {
                for i in 0..2u64 {
                    publication_send.send(round as u64 * 2 + i);
                }
            },
        );
        show("bounded_fanout", &r);
    }

    // credit_flow: producer_capacity 4, consumer_capacity 4, credit_window 16; 2 jobs per round,
    // one producer clock and one consumer clock element per round.
    #[test]
    fn blind_credit_flow() {
        use crate::cluster::witnesses::blind::credit_flow::{
            Consumer, CreditConfig, Producer, credit_flow,
        };
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<Producer>();
        let consumer = flow.process::<Consumer>();
        let (jobs_send, jobs) = producer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (producer_clock_send, producer_clock) =
            producer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (consumer_clock_send, consumer_clock) =
            consumer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = credit_flow(
            &producer,
            &consumer,
            jobs,
            producer_clock,
            consumer_clock,
            CreditConfig {
                producer_capacity: 4,
                consumer_capacity: 4,
                credit_window: 16,
            },
        );
        let _outgoing = outputs.outgoing.sim_output();
        let _processed = outputs.processed.sim_output();
        let _acks = outputs.acknowledgements.sim_output();
        let _completed = outputs.completed.sim_output();
        let _pt = outputs.producer_ticks.sim_output();
        let _ct = outputs.consumer_ticks.sim_output();
        let r = check(flow.sim(), &config(), async |_round| {
            jobs_send.send_many(std::iter::repeat_n((), 2));
            producer_clock_send.send(());
            consumer_clock_send.send(());
        });
        show("credit_flow", &r);
    }

    // log_catchup: poll_every_ticks 6, retry_after_ticks 4, chunk_size 12, leader_capacity 10;
    // 2 appends per round, no unrelated work, one follower clock and one leader clock element.
    #[test]
    fn blind_log_catchup() {
        use crate::cluster::witnesses::blind::log_catchup::{
            CatchupConfig, CatchupFollower, LogLeader, replicated_log_catchup,
        };
        let mut flow = FlowBuilder::new();
        let follower = flow.process::<CatchupFollower>();
        let leader = flow.process::<LogLeader>();
        let (append_send, appends) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_other_send, other) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (follower_clock_send, follower_clock) =
            follower.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (leader_clock_send, leader_clock) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = replicated_log_catchup(
            &follower,
            &leader,
            appends,
            other,
            follower_clock,
            leader_clock,
            CatchupConfig {
                poll_every_ticks: 6,
                retry_after_ticks: 4,
                chunk_size: 12,
                leader_capacity: 10,
            },
        );
        let _requests = outputs.requests.sim_output();
        let _served = outputs.served.sim_output();
        let _received = outputs.received.sim_output();
        let _applied = outputs.applied.sim_output();
        let _completions = outputs.completions.sim_output();
        let _backlog = outputs.backlog.sim_output();
        let r = check(flow.sim(), &config(), async |_round| {
            append_send.send_many([(), ()]);
            follower_clock_send.send(());
            leader_clock_send.send(());
        });
        show("log_catchup", &r);
    }

    // token_bucket: refill_tokens 5, bucket_capacity 10; 3 applications per round, one refill
    // clock element per round.
    #[test]
    fn blind_token_bucket() {
        use crate::cluster::witnesses::blind::token_bucket::{
            Producer, Regulator, TokenBucketConfig, token_bucket,
        };
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<Producer>();
        let regulator = flow.process::<Regulator>();
        let (app_send, applications) = producer.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = regulator.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = token_bucket(
            &producer,
            &regulator,
            applications,
            clock,
            TokenBucketConfig {
                refill_tokens: 5,
                bucket_capacity: 10,
            },
        );
        let _outgoing = outputs.outgoing.sim_output();
        let _completed = outputs.completed.sim_output();
        let _backlog = outputs.backlog.sim_output();
        let r = check(flow.sim(), &config(), async |round| {
            app_send.send_many((0..3).map(|_| round as u64));
            clock_send.send(());
        });
        show("token_bucket", &r);
    }

    // two_phase_commit: prepare_timeout_ticks 4, participant_capacity 5; one transaction per
    // round, one coordinator timer and one participant timer element per round.
    #[test]
    fn blind_two_phase_commit() {
        use crate::cluster::witnesses::blind::two_phase_commit::{
            Config, Coordinator, Participant, two_phase_commit,
        };
        let mut flow = FlowBuilder::new();
        let coordinator = flow.process::<Coordinator>();
        let participant = flow.process::<Participant>();
        let (tx_send, transactions) = coordinator.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (coord_tick_send, coord_ticks) = coordinator.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (part_tick_send, part_ticks) = participant.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = two_phase_commit(
            &coordinator,
            &participant,
            transactions,
            coord_ticks,
            part_ticks,
            Config {
                prepare_timeout_ticks: 4,
                participant_capacity: 5,
            },
        );
        let _wire = outputs.coordinator_wire.sim_output();
        let _votes = outputs.vote_wire.sim_output();
        let _processed = outputs.processed.sim_output();
        let _completed = outputs.completed.sim_output();
        let _retries = outputs.retries.sim_output();
        let _backlog = outputs.backlog.sim_output();
        let _pending = outputs.pending.sim_output();
        let r = check(flow.sim(), &config(), async |_round| {
            coord_tick_send.send(());
            part_tick_send.send(());
            tx_send.send_many(std::iter::repeat_n((), 1));
        });
        show("two_phase_commit", &r);
    }

    // visibility_queue: visibility_ticks 6, worker max_per_tick 5; 2 submissions per round, one
    // broker clock and one worker clock element per round.
    #[test]
    fn blind_visibility_queue() {
        use crate::cluster::witnesses::blind::visibility_queue::{
            QueueBroker, QueueConfig, QueueWorker, WorkerConfig, visibility_queue,
        };
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
                visibility_ticks: 6,
            },
            WorkerConfig { max_per_tick: 5 },
        );
        let _delivered = outputs.delivered.sim_output();
        let _processed = outputs.processed.sim_output();
        let _pending = outputs.pending_trace.sim_output();
        let _backlog = outputs.worker_backlog_trace.sim_output();
        let r = check(flow.sim(), &config(), async |round| {
            broker_clock_send.send(());
            worker_clock_send.send(());
            for i in 0..2u64 {
                submission_send.send(round as u64 * 2 + i);
            }
        });
        show("visibility_queue", &r);
    }
}
