//! A credit-window work channel, labeled benign by an assurance argument.
//!
//! The producer starts with [`CreditConfig::credit_window`] local credits. It assigns each input a
//! dataflow id, queues it, and sends it once when both a credit and producer capacity are available.
//! The consumer queues delivered jobs, processes at most its configured capacity per timer element,
//! and sends one acknowledgement back for each processed job. An acknowledgement restores one
//! producer credit; there are no timeout, retry, or expiry operators.
//!
//! # Assurance argument
//!
//! For `J` job-input elements, every id leaves the producer queue at most once because sent jobs are
//! removed from the queue and no path inserts an acknowledgement back into that queue. Consequently
//! job messages are exactly `J`, consumer processing records are exactly `J`, acknowledgements are
//! exactly `J`, and completions are exactly `J`, once enough timer input is supplied to drain the
//! finite queues. Before a drain, each of these counts is at most `J`. Batching can only change when
//! a queued item crosses either capacity boundary. It cannot create an item. Thus total countable
//! work after draining is the closed form `4J` under every schedule, independent of delay.
//!
//! # Timer parameters
//!
//! A deployment wires `producer.source_interval(period)` to `producer_clock` and
//! `consumer.source_interval(period)` to `consumer_clock`. Each timer element grants one tick of
//! the corresponding configured capacity. Simulation harnesses supply both through `sim_input`.
//!
//! # Measured configurations
//!
//! Baseline is two jobs per round. The trigger raises this to eight jobs in rounds 50 through 70.
//! Producer and consumer capacity are four per timer element and the credit window is 16. The
//! hand-computed trigger backlog is 80 jobs: each trigger round offers four above capacity for 20
//! rounds. It therefore drains in 20 later rounds. No timeout exists, so delayed work produces no
//! additional offered load.
//!
//! | configuration | baseline | capacity / window | trigger | timeout | asserted tail | label and basis |
//! |---|---:|---|---|---|---|---|
//! | burst | 2 jobs/round | producer 4, consumer 4, credits 16 | 8 jobs/round, rounds 50..70 | none | rounds 150..200: 100 completions, final backlogs 0; 520 inputs produce exactly 2,080 work records | benign, by assurance argument |
//! | no trigger control | 2 jobs/round | producer 4, consumer 4, credits 16 | none | none | 400 inputs produce exactly 1,600 work records, final backlogs 0 | healthy control |
//! | 256 fuzzed schedules | 1 job/round | producer 3, consumer 2, credits 4 | 5 jobs in rounds 2..5 | none | 20 inputs produce exactly 80 work records after drain | benign, by assurance argument |

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Producer;
pub struct Consumer;

#[derive(Clone, Copy, Debug)]
pub struct CreditConfig {
    /// Maximum jobs put on the wire per producer timer element.
    pub producer_capacity: usize,
    /// Maximum jobs processed per consumer timer element.
    pub consumer_capacity: usize,
    /// Maximum jobs in flight between acknowledgement admissions.
    pub credit_window: usize,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Job {
    pub id: u64,
    pub admitted_tick: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ack {
    pub job: Job,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    pub latency_ticks: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProducerTick {
    pub sent: usize,
    pub backlog: usize,
    pub available_credits: usize,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerTick {
    pub processed: usize,
    pub backlog: usize,
}

pub struct CreditOutputs<'a> {
    /// Every job message put on the producer-to-consumer wire.
    pub outgoing: Stream<Job, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every job processed by the consumer.
    pub processed: Stream<Job, Process<'a, Consumer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement put on the consumer-to-producer wire.
    pub acknowledgements: Stream<Ack, Process<'a, Consumer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every acknowledgement admitted as a completion, with logical producer-tick latency.
    pub completed: Stream<Completion, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Producer queue and credit observations.
    pub producer_ticks:
        Stream<ProducerTick, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Consumer queue observations.
    pub consumer_ticks:
        Stream<ConsumerTick, Process<'a, Consumer>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the credit-controlled channel. Input values carry no ids; ids are assigned by the
/// producer's arrival-ordered dataflow.
pub fn credit_flow<'a>(
    producer: &Process<'a, Producer>,
    consumer: &Process<'a, Consumer>,
    jobs: Stream<(), Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    producer_clock: Stream<(), Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    consumer_clock: Stream<(), Process<'a, Consumer>, Unbounded, TotalOrder, ExactlyOnce>,
    config: CreditConfig,
) -> CreditOutputs<'a> {
    let CreditConfig {
        producer_capacity,
        consumer_capacity,
        credit_window,
    } = config;

    let (acks_complete, acks_in) =
        producer
            .forward_ref::<Stream<Ack, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>>(
            );

    let (outgoing, completed, producer_ticks) = sliced! {
        let clock = use::batch(producer_clock.enumerate(), nondet!(/** batching changes how many producer capacity grants are spent together, but not the total grants */));
        let arrivals = use::batch(jobs.enumerate(), nondet!(/** batching changes when an input gets its id and enters the FIFO, but cannot duplicate it */));
        let acks = use::batch(acks_in, nondet!(/** delaying an acknowledgement delays credit reuse but cannot cause another send of its completed id */));
        let mut backlog = use::state_null::<Stream<Job, Tick<_>, Bounded, TotalOrder>>();
        let mut credits = use::state(|l| l.singleton(q!(credit_window)));
        let mut now = use::state(|l| l.singleton(q!(0u64)));

        let clock_elements = clock.clone().count();
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let completed = acks
            .clone()
            .cross_singleton(now_cur.clone())
            .map(q!(|(ack, now)| Completion {
                id: ack.job.id,
                latency_ticks: now.saturating_sub(ack.job.admitted_tick),
            }));
        let available = credits
            .clone()
            .zip(acks.count())
            .map(q!(|(credits, returned)| credits + returned));
        let budget = clock_elements.map(q!(move |ticks| ticks * producer_capacity));
        let limit = budget
            .zip(available.clone())
            .map(q!(|(budget, credits)| budget.min(credits)));
        let new_jobs = arrivals
            .cross_singleton(now_cur)
            .map(q!(|((id, ()), now)| Job { id: id as u64, admitted_tick: now }));
        let indexed = backlog.chain(new_jobs).enumerate().cross_singleton(limit);
        let outgoing = indexed
            .clone()
            .filter_map(q!(|((position, job), limit)| (position < limit).then_some(job)));
        backlog = indexed
            .filter_map(q!(|((position, job), limit)| (position >= limit).then_some(job)));
        let sent = outgoing.clone().count();
        credits = available
            .zip(sent.clone())
            .map(q!(|(available, sent)| available - sent));
        let producer_ticks = sent
            .zip(backlog.clone().count())
            .zip(credits.clone())
            .map(q!(|((sent, backlog), available_credits)| ProducerTick {
                sent,
                backlog,
                available_credits,
            }))
            .into_stream();

        (outgoing, completed, producer_ticks)
    };

    let incoming = outgoing.clone().send(consumer, TCP.fail_stop().bincode());
    let (processed, acknowledgements, consumer_ticks) = sliced! {
        let clock = use::batch(consumer_clock, nondet!(/** batching changes how many consumer capacity grants are spent together, but not the total grants */));
        let arrivals = use::batch(incoming, nondet!(/** delivery delay changes queue residence only; every wire job is admitted once */));
        let mut backlog = use::state_null::<Stream<Job, Tick<_>, Bounded, TotalOrder>>();

        let budget = clock.count().map(q!(move |ticks| ticks * consumer_capacity));
        let indexed = backlog.chain(arrivals).enumerate().cross_singleton(budget);
        let processed = indexed
            .clone()
            .filter_map(q!(|((position, job), budget)| (position < budget).then_some(job)));
        backlog = indexed
            .filter_map(q!(|((position, job), budget)| (position >= budget).then_some(job)));
        let acknowledgements = processed.clone().map(q!(|job| Ack { job }));
        let consumer_ticks = processed
            .clone()
            .count()
            .zip(backlog.clone().count())
            .map(q!(|(processed, backlog)| ConsumerTick { processed, backlog }))
            .into_stream();

        (processed, acknowledgements, consumer_ticks)
    };

    acks_complete.complete(
        acknowledgements
            .clone()
            .send(producer, TCP.fail_stop().bincode()),
    );

    CreditOutputs {
        outgoing,
        processed,
        acknowledgements,
        completed,
        producer_ticks,
        consumer_ticks,
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use super::{ConsumerTick, CreditConfig, CreditOutputs, ProducerTick, credit_flow};

    #[derive(Clone, Debug, Default)]
    struct Round {
        inputs: usize,
        outgoing: usize,
        processed: usize,
        acknowledgements: usize,
        completions: usize,
        producer_backlog: usize,
        consumer_backlog: usize,
    }

    #[derive(Clone, Copy)]
    struct Workload {
        baseline: usize,
        trigger: usize,
        trigger_start: usize,
        trigger_end: usize,
    }

    fn run(workload: Workload, rounds: usize, config: CreditConfig) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<super::Producer>();
        let consumer = flow.process::<super::Consumer>();
        let (jobs_send, jobs) = producer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (producer_clock_send, producer_clock) =
            producer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (consumer_clock_send, consumer_clock) =
            consumer.sim_input::<(), TotalOrder, ExactlyOnce>();
        let CreditOutputs {
            outgoing,
            processed,
            acknowledgements,
            completed,
            producer_ticks,
            consumer_ticks,
        } = credit_flow(
            &producer,
            &consumer,
            jobs,
            producer_clock,
            consumer_clock,
            config,
        );
        let outgoing = outgoing.sim_output();
        let processed = processed.sim_output();
        let acknowledgements = acknowledgements.sim_output();
        let completed = completed.sim_output();
        let producer_ticks = producer_ticks.sim_output();
        let consumer_ticks = consumer_ticks.sim_output();

        let mut trace = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            for round in 0..rounds {
                let inputs = if (workload.trigger_start..workload.trigger_end).contains(&round) {
                    workload.trigger
                } else {
                    workload.baseline
                };
                jobs_send.send_many(std::iter::repeat_n((), inputs));
                producer_clock_send.send(());
                consumer_clock_send.send(());
                quiesce().await;
                let producer_stats = producer_ticks.collect::<Vec<ProducerTick>>().await;
                let consumer_stats = consumer_ticks.collect::<Vec<ConsumerTick>>().await;
                trace_ref.push(Round {
                    inputs,
                    outgoing: outgoing.collect::<Vec<_>>().await.len(),
                    processed: processed.collect::<Vec<_>>().await.len(),
                    acknowledgements: acknowledgements.collect::<Vec<_>>().await.len(),
                    completions: completed.collect::<Vec<_>>().await.len(),
                    producer_backlog: producer_stats.last().map_or(0, |tick| tick.backlog),
                    consumer_backlog: consumer_stats.last().map_or(0, |tick| tick.backlog),
                });
            }
        });
        trace
    }

    fn totals(trace: &[Round]) -> (usize, usize, usize, usize, usize) {
        trace.iter().fold((0, 0, 0, 0, 0), |acc, round| {
            (
                acc.0 + round.inputs,
                acc.1 + round.outgoing,
                acc.2 + round.processed,
                acc.3 + round.acknowledgements,
                acc.4 + round.completions,
            )
        })
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// The baseline offers two jobs against producer and consumer capacities of four. The 20-round
    /// trigger offers eight, building `20 * (8 - 4) = 80` queued jobs. Four spare slots per later
    /// round drain that queue in 20 rounds. The 16-credit window can shift work between the two
    /// queues but cannot change the total. Across 200 rounds, input is `180 * 2 + 20 * 8 = 520`, so
    /// each of the four externally counted stages must contain 520 records, or 2,080 work records
    /// total. The tail from round 150 has 100 inputs and therefore 100 records at each stage.
    const CONFIG: CreditConfig = CreditConfig {
        producer_capacity: 4,
        consumer_capacity: 4,
        credit_window: 16,
    };
    const BURST: Workload = Workload {
        baseline: 2,
        trigger: 8,
        trigger_start: 50,
        trigger_end: 70,
    };
    const CONTROL: Workload = Workload {
        baseline: 2,
        trigger: 2,
        trigger_start: 50,
        trigger_end: 70,
    };
    const ROUNDS: usize = 200;

    #[test]
    fn burst_obeys_the_closed_form_and_drains() {
        let trace = run(BURST, ROUNDS, CONFIG);
        let total = totals(&trace);
        println!(
            "burst totals: input={} outgoing={} processed={} acks={} completions={}; final backlogs producer={} consumer={}",
            total.0,
            total.1,
            total.2,
            total.3,
            total.4,
            trace.last().unwrap().producer_backlog,
            trace.last().unwrap().consumer_backlog,
        );
        assert_eq!(total, (520, 520, 520, 520, 520));
        assert_eq!(
            trace[150..]
                .iter()
                .map(|round| round.completions)
                .sum::<usize>(),
            100
        );
        assert_eq!(trace.last().unwrap().producer_backlog, 0);
        assert_eq!(trace.last().unwrap().consumer_backlog, 0);
    }

    #[test]
    fn no_trigger_control_stays_healthy() {
        let trace = run(CONTROL, ROUNDS, CONFIG);
        assert_eq!(totals(&trace), (400, 400, 400, 400, 400));
        assert!(trace.iter().all(|round| round.producer_backlog == 0));
        assert!(trace.iter().all(|round| round.consumer_backlog == 0));
    }

    /// Hand-computed fuzz expectation: eight baseline inputs plus twelve trigger inputs make 20.
    /// Supplying 16 drain timer elements exceeds the ten consumer ticks and seven producer ticks
    /// needed even if all inputs are admitted after the workload clocks. Every schedule must
    /// therefore expose exactly 20 records at each of the four work stages.
    #[test]
    fn closed_form_holds_across_256_schedules() {
        const FUZZ_CONFIG: CreditConfig = CreditConfig {
            producer_capacity: 3,
            consumer_capacity: 2,
            credit_window: 4,
        };
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<super::Producer>();
        let consumer = flow.process::<super::Consumer>();
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
            FUZZ_CONFIG,
        );
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();
        let acknowledgements = outputs.acknowledgements.sim_output();
        let completed = outputs.completed.sim_output();
        let producer_ticks = outputs.producer_ticks.sim_output();
        let consumer_ticks = outputs.consumer_ticks.sim_output();

        flow.sim().unit_test_fuzz_iterations(256).fuzz(async || {
            for round in 0..8 {
                let count = if (2..5).contains(&round) { 5 } else { 1 };
                jobs_send.send_many(std::iter::repeat_n((), count));
                producer_clock_send.send(());
                consumer_clock_send.send(());
                quiesce().await;
            }
            for _ in 0..16 {
                producer_clock_send.send(());
                consumer_clock_send.send(());
                quiesce().await;
            }
            assert_eq!(outgoing.collect::<Vec<_>>().await.len(), 20);
            assert_eq!(processed.collect::<Vec<_>>().await.len(), 20);
            assert_eq!(acknowledgements.collect::<Vec<_>>().await.len(), 20);
            assert_eq!(completed.collect::<Vec<_>>().await.len(), 20);
            let _ = producer_ticks.collect::<Vec<_>>().await;
            let _ = consumer_ticks.collect::<Vec<_>>().await;
        });
    }
}
