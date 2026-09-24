//! A token-bucket admission queue, labeled benign by assurance argument.
//!
//! | configuration | baseline per round | capacity | trigger | timeout / expiry | asserted tail | label and basis |
//! |---|---:|---:|---:|---|---|---|
//! | refill 5, bucket 10 | 3 requests | 5 tokens | 18 requests in rounds 50..70 | none | rounds 220..300: 240 served, backlog 0 throughout | benign, by assurance argument |
//!
//! Each application input is assigned one id and emits exactly one `Request` on the wire. At the
//! regulator, that request occupies exactly one of two disjoint states: pending in the FIFO, or
//! served. Serving removes it permanently. Tokens come only from timer elements, and neither an
//! empty bucket nor a delayed request emits anything. Thus, for `N` application inputs, wire work
//! is exactly `N`, service work is at most `N`, and after enough refill inputs to drain the FIFO it
//! is exactly `N`. Total completed-run work is the closed form `2N`, under every schedule. Batch
//! choices can change latency and how many refill tokens are discarded at the bucket cap, but
//! cannot duplicate a request or create work. The deterministic burst test and 256 random
//! schedules assert this closed form.
//!
//! `refill_clock` is the regulator's only timer. A deployment wires
//! `regulator.source_interval(period)` into it. The simulation supplies every timer element through
//! `sim_input`; no wall clock is read.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Producer;
pub struct Regulator;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Request {
    pub id: u64,
    pub issued_tick: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    pub latency_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct TokenBucketConfig {
    /// Tokens added by each refill timer element.
    pub refill_tokens: u64,
    /// Maximum tokens retained between ticks.
    pub bucket_capacity: u64,
}

pub struct TokenBucketOutputs<'a> {
    /// Every request put on the producer-to-regulator wire.
    pub outgoing: Stream<Request, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every request served, once, with latency in logical regulator ticks.
    pub completed: Stream<Completion, Process<'a, Regulator>, Unbounded, TotalOrder, ExactlyOnce>,
    /// FIFO depth after each runnable regulator tick.
    pub backlog: Stream<usize, Process<'a, Regulator>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds a token-bucket regulator. `applications` carries the logical tick at which each request
/// was offered; the dataflow assigns ids in arrival order.
pub fn token_bucket<'a>(
    producer: &Process<'a, Producer>,
    regulator: &Process<'a, Regulator>,
    applications: Stream<u64, Process<'a, Producer>, Unbounded, TotalOrder, ExactlyOnce>,
    refill_clock: Stream<(), Process<'a, Regulator>, Unbounded, TotalOrder, ExactlyOnce>,
    config: TokenBucketConfig,
) -> TokenBucketOutputs<'a> {
    let TokenBucketConfig {
        refill_tokens,
        bucket_capacity,
    } = config;

    let outgoing = applications
        .enumerate()
        .map(q!(|(id, issued_tick)| Request {
            id: id as u64,
            issued_tick,
        }));
    let incoming = outgoing.clone().send(regulator, TCP.fail_stop().bincode());
    let _ = producer;

    let (completed, backlog_trace) = sliced! {
        let refills = use::batch(refill_clock.enumerate(), nondet!(/** batching changes when credits become visible and whether the cap discards them; each timer element contributes at most one refill */));
        let arrivals = use::batch(incoming, nondet!(/** batching changes request latency and token availability, but each request enters the FIFO exactly once */));
        let mut backlog = use::state_null::<Stream<Request, Tick<_>, Bounded, TotalOrder>>();
        let mut tokens = use::state(|l| l.singleton(q!(0u64)));
        let mut now = use::state(|l| l.singleton(q!(0u64)));

        let now_cur = refills
            .clone()
            .map(q!(|(tick, _)| tick as u64))
            .max()
            .unwrap_or(now.clone());
        now = now_cur.clone();

        let grants = refills
            .count()
            .map(q!(move |count| count as u64 * refill_tokens));
        let available = tokens
            .zip(grants)
            .map(q!(move |(saved, granted)| (saved + granted).min(bucket_capacity)));

        let queued = backlog.chain(arrivals).enumerate();
        let placed = queued
            .cross_singleton(available.clone())
            .map(q!(|((position, request), budget)| (position < budget as usize, request)));
        let served = placed
            .clone()
            .filter_map(q!(|(serve, request)| serve.then_some(request)));
        backlog = placed.filter_map(q!(|(serve, request)| (!serve).then_some(request)));

        let spent = served.clone().count().map(q!(|count| count as u64));
        tokens = available.zip(spent).map(q!(|(available, spent)| available - spent));

        let completed = served
            .cross_singleton(now_cur)
            .map(q!(|(request, now)| Completion {
                id: request.id,
                latency_ticks: now.saturating_sub(request.issued_tick),
            }));
        (completed, backlog.clone().count().into_stream())
    };

    TokenBucketOutputs {
        outgoing,
        completed,
        backlog: backlog_trace,
    }
}

#[cfg(test)]
mod sim_tests {
    use std::collections::HashSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use super::{Completion, Producer, Regulator, Request, TokenBucketConfig, token_bucket};

    #[derive(Clone, Debug, Default)]
    struct Round {
        offered: u64,
        sent: u64,
        served: u64,
        backlog: usize,
    }

    #[derive(Clone, Copy)]
    struct Workload {
        baseline: u32,
        trigger: u32,
        trigger_start: usize,
        trigger_end: usize,
    }

    impl Workload {
        fn at(self, round: usize, enabled: bool) -> u32 {
            if enabled && (self.trigger_start..self.trigger_end).contains(&round) {
                self.trigger
            } else {
                self.baseline
            }
        }

        fn total(self, rounds: usize, enabled: bool) -> u64 {
            (0..rounds)
                .map(|round| self.at(round, enabled) as u64)
                .sum()
        }
    }

    const CONFIG: TokenBucketConfig = TokenBucketConfig {
        refill_tokens: 5,
        bucket_capacity: 10,
    };

    /// Hand-computed expectation, written before measuring.
    ///
    /// Baseline offers 3 requests against 5 refill tokens, so the queue stays empty. Rounds 50
    /// through 69 offer 18. Seven saved tokens allow 10 services in the first trigger round, then
    /// 5 per round, so the trigger leaves 255 pending. Baseline has two spare tokens per round and
    /// drains that queue in 128 rounds, by round 198. The 300-round burst run offers 1,200 requests;
    /// after extra drain ticks it must report exactly 1,200 sends and 1,200 services. Rounds 220
    /// through 299 therefore serve 240 requests with zero backlog throughout. Without the trigger,
    /// 900 requests are sent and served and backlog is always zero.
    const WORKLOAD: Workload = Workload {
        baseline: 3,
        trigger: 18,
        trigger_start: 50,
        trigger_end: 70,
    };
    const ROUNDS: usize = 300;
    const TAIL_START: usize = 220;

    fn run(trigger: bool) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<Producer>();
        let regulator = flow.process::<Regulator>();
        let (app_send, applications) = producer.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = regulator.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = token_bucket(&producer, &regulator, applications, clock, CONFIG);
        let outgoing = outputs.outgoing.sim_output();
        let completed = outputs.completed.sim_output();
        let backlog = outputs.backlog.sim_output();

        let mut trace = Vec::with_capacity(ROUNDS);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            for round in 0..ROUNDS {
                let offered =
                    if trigger && (WORKLOAD.trigger_start..WORKLOAD.trigger_end).contains(&round) {
                        WORKLOAD.trigger
                    } else {
                        WORKLOAD.baseline
                    };
                app_send.send_many((0..offered).map(|_| round as u64));
                clock_send.send(());
                quiesce().await;
                let sent = outgoing.collect::<Vec<Request>>().await.len() as u64;
                let served = completed.collect::<Vec<Completion>>().await.len() as u64;
                let depths = backlog.collect::<Vec<usize>>().await;
                trace_ref.push(Round {
                    offered: offered as u64,
                    sent,
                    served,
                    backlog: depths.last().copied().unwrap_or_default(),
                });
            }
            // More input timer elements, rather than an out-of-band drain, finish any finite FIFO.
            for _ in 0..100 {
                clock_send.send(());
                quiesce().await;
                let sent = outgoing.collect::<Vec<Request>>().await.len() as u64;
                let served = completed.collect::<Vec<Completion>>().await.len() as u64;
                let depths = backlog.collect::<Vec<usize>>().await;
                trace_ref.push(Round {
                    offered: 0,
                    sent,
                    served,
                    backlog: depths.last().copied().unwrap_or_default(),
                });
            }
        });
        trace
    }

    #[test]
    fn burst_obeys_the_closed_form_and_recovers() {
        let trace = run(true);
        let inputs = WORKLOAD.total(ROUNDS, true);
        assert_eq!(inputs, 1_200);
        assert_eq!(trace.iter().map(|round| round.offered).sum::<u64>(), inputs);
        assert_eq!(trace.iter().map(|round| round.sent).sum::<u64>(), inputs);
        assert_eq!(trace.iter().map(|round| round.served).sum::<u64>(), inputs);
        assert!(
            trace[..WORKLOAD.trigger_start]
                .iter()
                .all(|round| round.backlog == 0)
        );
        assert_eq!(
            trace[TAIL_START..ROUNDS]
                .iter()
                .map(|r| r.served)
                .sum::<u64>(),
            240
        );
        assert!(trace[TAIL_START..].iter().all(|round| round.backlog == 0));
    }

    #[test]
    fn trigger_free_control_stays_healthy() {
        let trace = run(false);
        let inputs = WORKLOAD.total(ROUNDS, false);
        assert_eq!(inputs, 900);
        assert_eq!(trace.iter().map(|round| round.sent).sum::<u64>(), inputs);
        assert_eq!(trace.iter().map(|round| round.served).sum::<u64>(), inputs);
        assert!(trace.iter().all(|round| round.backlog == 0));
    }

    /// Eight rounds offer 26 requests. Twenty more refill elements provide more than enough
    /// capacity, so every one of 256 schedules must send and serve ids 0 through 25 exactly once.
    #[test]
    fn closed_form_holds_across_schedules() {
        let mut flow = FlowBuilder::new();
        let producer = flow.process::<Producer>();
        let regulator = flow.process::<Regulator>();
        let (app_send, applications) = producer.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = regulator.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = token_bucket(&producer, &regulator, applications, clock, CONFIG);
        let outgoing = outputs.outgoing.sim_output();
        let completed = outputs.completed.sim_output();
        let backlog = outputs.backlog.sim_output();

        flow.sim().unit_test_fuzz_iterations(256).fuzz(async || {
            let offered = [2u32, 2, 7, 7, 2, 2, 2, 2];
            for (round, count) in offered.into_iter().enumerate() {
                app_send.send_many((0..count).map(|_| round as u64));
                clock_send.send(());
                quiesce().await;
            }
            for _ in 0..20 {
                clock_send.send(());
                quiesce().await;
            }

            let sent = outgoing.collect::<Vec<Request>>().await;
            let served = completed.collect::<Vec<Completion>>().await;
            let sent_ids = sent
                .iter()
                .map(|request| request.id)
                .collect::<HashSet<_>>();
            let served_ids = served.iter().map(|done| done.id).collect::<HashSet<_>>();
            assert_eq!(sent.len(), 26);
            assert_eq!(served.len(), 26);
            assert_eq!(sent_ids.len(), 26);
            assert_eq!(served_ids, sent_ids);
            assert_eq!(backlog.collect::<Vec<usize>>().await.last(), Some(&0));
        });
    }
}
