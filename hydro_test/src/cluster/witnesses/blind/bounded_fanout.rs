//! A bounded publish/subscribe fan-out with no acknowledgements or retries.
//!
//! Each publication receives an id at the publisher and is copied to exactly
//! [`FanoutConfig::subscribers_per_publication`] subscriber members. A subscriber processes every
//! delivery once when the simulator admits it. The fan-out bound is a capacity bound on work per
//! publication, rather than a per-tick service budget. A deployment must create at least that many
//! subscriber members; member raw ids `0..subscribers_per_publication` are the subscription set.
//!
//! # Assurance argument
//!
//! Let `P` be the number of publication input elements and `F` be
//! `subscribers_per_publication`. The publisher's only emission path is the `flat_map_ordered`
//! below, which emits exactly `F` distinct destination/delivery pairs for each input. Thus wire
//! work is exactly `P * F`. The subscriber has no emission path and processes every received pair
//! exactly once, so processing work is also exactly `P * F`. Total countable work is therefore
//! exactly `2 * P * F` under every schedule. `use::batch` may change when either stage sees an
//! element and how elements are grouped, but it cannot create an element. There is no clock,
//! timeout, acknowledgement, retry, or absence-sensitive operator. The end-of-tick pending count
//! is always zero because each admitted batch is processed in full.
//!
//! The deterministic burst and control tests, and 256 random schedules, assert this closed form.
//!
//! # Timer parameters
//!
//! This program has no timer. A deployment therefore has no `source_interval` to wire. Delivery
//! is driven only by publications and by network admission.
//!
//! # Measured (see `sim_tests`)
//!
//! The burst run has baseline 2 publications per round, a trigger of 11 per round in rounds
//! 100..160, and fan-out capacity 3. Tail is rounds 600..800.
//!
//! | run | baseline | fan-out capacity | trigger | timeout | tail wire / processed / pending | whole-run input / wire / processed | label and basis |
//! |---|---:|---:|---|---|---|---|---|
//! | burst | 2 | 3 | 11 for 60 rounds | none | 1200 / 1200 / 0 | 2140 / 6420 / 6420 | benign, by assurance argument |
//! | control | 2 | 3 | none | none | 1200 / 1200 / 0 | 1600 / 4800 / 4800 | benign, by assurance argument |
//! | 256 fuzzed schedules | 2 | 3 | 5 in rounds 2..5 of 8 | none | n/a | 25 / 75 / 75 each | benign, by assurance argument |

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Publisher;
pub struct Subscriber;

/// A publication after the publisher assigns its arrival-ordered id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Publication {
    pub id: u64,
    pub value: u64,
}

/// A publication copy addressed to one subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Delivery {
    pub publication_id: u64,
    pub value: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct FanoutConfig {
    /// Number of subscriber members that receive each publication.
    pub subscribers_per_publication: u32,
}

pub struct FanoutOutputs<'a> {
    /// Every message placed on the wire, paired with its destination raw id.
    pub wire: Stream<(u32, Delivery), Process<'a, Publisher>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every delivery processed at its destination.
    pub processed: Stream<Delivery, Cluster<'a, Subscriber>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Pending deliveries after each admitted subscriber batch. This implementation drains the
    /// admitted batch completely, so every observation is zero.
    pub pending: Stream<usize, Cluster<'a, Subscriber>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the bounded fan-out program.
///
/// `subscribers` must have at least `config.subscribers_per_publication` members.
pub fn bounded_fanout<'a>(
    _publisher: &Process<'a, Publisher>,
    subscribers: &Cluster<'a, Subscriber>,
    publications: Stream<u64, Process<'a, Publisher>, Unbounded, TotalOrder, ExactlyOnce>,
    config: FanoutConfig,
) -> FanoutOutputs<'a> {
    let fanout = config.subscribers_per_publication;

    let wire = sliced! {
        let publications = use::batch(publications.enumerate(), nondet!(/** batching may delay a publication, but every input is assigned one stable id and admitted exactly once */));
        publications
            .map(q!(|(id, value)| Publication { id: id as u64, value }))
            .flat_map_ordered(q!(move |publication| (0..fanout).map(move |destination| (
                destination,
                Delivery {
                    publication_id: publication.id,
                    value: publication.value,
                },
            ))))
    };

    let incoming = wire
        .clone()
        .map(q!(|(destination, delivery)| (
            MemberId::<Subscriber>::from_raw_id(destination),
            delivery,
        )))
        .into_keyed()
        .demux(subscribers, TCP.fail_stop().bincode());

    let (processed, pending) = sliced! {
        let admitted = use::batch(incoming, nondet!(/** batching may delay and regroup deliveries, but cannot duplicate or discard them */));
        let processed = admitted.sort();
        let pending = processed.clone().count().map(q!(|_| 0usize)).into_stream();
        (processed, pending)
    };

    FanoutOutputs {
        wire,
        processed,
        pending,
    }
}

/// Harness workload. This item is outside the test module because staged closures refer to it.
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

#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct Round {
        input: u64,
        wire: u64,
        processed: u64,
        pending: Vec<usize>,
    }

    fn run(workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let publisher = flow.process::<Publisher>();
        let subscribers = flow.cluster::<Subscriber>();
        let (publication_send, publications) =
            publisher.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = bounded_fanout(
            &publisher,
            &subscribers,
            publications,
            FanoutConfig {
                subscribers_per_publication: FANOUT,
            },
        );
        let wire = outputs.wire.sim_output();
        let processed = outputs.processed.sim_cluster_output();
        let pending = outputs.pending.sim_cluster_output();

        let mut trace = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim()
            .with_cluster_size(&subscribers, FANOUT as usize)
            .run_prompt(async move || {
                let mut next_value = 0u64;
                for round in 0..rounds as u64 {
                    let count = workload.at(round);
                    for _ in 0..count {
                        publication_send.send(next_value);
                        next_value += 1;
                    }
                    quiesce().await;

                    let mut record = Round {
                        input: count as u64,
                        wire: wire.collect::<Vec<(u32, Delivery)>>().await.len() as u64,
                        ..Round::default()
                    };
                    for member in 0..FANOUT {
                        record.processed += processed
                            .collect::<Vec<Delivery>>(member)
                            .await
                            .len() as u64;
                        record
                            .pending
                            .extend(pending.collect::<Vec<usize>>(member).await);
                    }
                    trace_ref.push(record);
                }
            });
        trace
    }

    fn assert_closed_form(trace: &[Round]) {
        for (round, record) in trace.iter().enumerate() {
            assert_eq!(
                record.wire,
                record.input * FANOUT as u64,
                "wire round {round}"
            );
            assert_eq!(
                record.processed,
                record.input * FANOUT as u64,
                "processed round {round}"
            );
            assert!(
                record.pending.iter().all(|pending| *pending == 0),
                "pending round {round}: {:?}",
                record.pending
            );
        }
    }

    fn totals(trace: &[Round]) -> (u64, u64, u64) {
        trace.iter().fold((0, 0, 0), |totals, round| {
            (
                totals.0 + round.input,
                totals.1 + round.wire,
                totals.2 + round.processed,
            )
        })
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Baseline is 2 publications per round and fan-out capacity is 3, so baseline work is 6
    /// wire messages plus 6 processing events per round. The bounded trigger offers 11 per round
    /// for 60 rounds, producing 33 messages and 33 processing events per trigger round. No
    /// timeout or expiry exists, no backlog can build, and post-trigger offered load immediately
    /// returns to 6 messages per round. Across 800 rounds there are `2*800 + 9*60 = 2140`
    /// publications and exactly 6420 wire plus 6420 processing events. Tail rounds 600..800 have
    /// 400 publications, 1200 wire messages, 1200 processing events, and zero pending work.
    const FANOUT: u32 = 3;
    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;
    const BURST: Workload = Workload {
        baseline_per_round: 2,
        trigger_per_round: 11,
        trigger_start: 100,
        trigger_end: 160,
    };
    const CONTROL: Workload = Workload {
        trigger_per_round: 2,
        ..BURST
    };

    #[test]
    fn burst_work_is_exactly_bounded_by_input() {
        let trace = run(BURST, ROUNDS);
        assert_closed_form(&trace);
        assert!(trace[..100].iter().all(|round| round.wire == 6));
        assert!(trace[100..160].iter().all(|round| round.wire == 33));
        assert!(trace[160..].iter().all(|round| round.wire == 6));
        assert_eq!(totals(&trace), (2140, 6420, 6420));
        assert_eq!(totals(&trace[TAIL_START..]), (400, 1200, 1200));
        println!(
            "tail {TAIL_START}..{ROUNDS}: input / wire / processed = {:?}, pending 0",
            totals(&trace[TAIL_START..])
        );
    }

    #[test]
    fn trigger_free_control_stays_at_the_closed_form() {
        let trace = run(CONTROL, ROUNDS);
        assert_closed_form(&trace);
        assert!(trace.iter().all(|round| round.wire == 6));
        assert_eq!(totals(&trace), (1600, 4800, 4800));
        assert_eq!(totals(&trace[TAIL_START..]), (400, 1200, 1200));
    }

    /// The same 25 publications produce exactly 75 wire messages and 75 processing events in
    /// each of 256 random schedules.
    #[test]
    fn closed_form_holds_across_schedules() {
        const FUZZ_ROUNDS: usize = 8;
        const FUZZ_WORKLOAD: Workload = Workload {
            baseline_per_round: 2,
            trigger_per_round: 5,
            trigger_start: 2,
            trigger_end: 5,
        };

        let mut flow = FlowBuilder::new();
        let publisher = flow.process::<Publisher>();
        let subscribers = flow.cluster::<Subscriber>();
        let (publication_send, publications) =
            publisher.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = bounded_fanout(
            &publisher,
            &subscribers,
            publications,
            FanoutConfig {
                subscribers_per_publication: FANOUT,
            },
        );
        let wire = outputs.wire.sim_output();
        let processed = outputs.processed.sim_cluster_output();
        let pending = outputs.pending.sim_cluster_output();

        flow.sim()
            .with_cluster_size(&subscribers, FANOUT as usize)
            .unit_test_fuzz_iterations(256)
            .fuzz(async || {
                let mut value = 0u64;
                let mut total_input = 0u64;
                let mut total_wire = 0u64;
                let mut total_processed = 0u64;
                for round in 0..FUZZ_ROUNDS as u64 {
                    let count = FUZZ_WORKLOAD.at(round);
                    total_input += count as u64;
                    for _ in 0..count {
                        publication_send.send(value);
                        value += 1;
                    }
                    quiesce().await;
                    total_wire += wire.collect::<Vec<(u32, Delivery)>>().await.len() as u64;
                    for member in 0..FANOUT {
                        total_processed += processed
                            .collect::<Vec<Delivery>>(member)
                            .await
                            .len() as u64;
                        assert!(
                            pending
                                .collect::<Vec<usize>>(member)
                                .await
                                .into_iter()
                                .all(|depth| depth == 0)
                        );
                    }
                }
                assert_eq!(total_input, 25);
                assert_eq!(total_wire, total_input * FANOUT as u64);
                assert_eq!(total_processed, total_input * FANOUT as u64);
            });
    }
}
