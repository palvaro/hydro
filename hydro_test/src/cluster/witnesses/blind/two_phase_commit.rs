//! Two-phase commit with prepare timeouts and duplicate participant work.
//!
//! A coordinator assigns each transaction an id, sends `Prepare` to two logical participants,
//! and sends `Commit` after both votes arrive. A participant service has one FIFO and processes at
//! most [`Config::participant_capacity`] protocol messages per timer element. Until both votes are
//! observed, the coordinator resends each missing prepare every
//! [`Config::prepare_timeout_ticks`]. Participants deliberately have no duplicate suppression, so
//! every repeated prepare consumes capacity and produces another vote.
//!
//! The label is **hazardous, by exhibited collapse**. A bounded transaction burst builds a FIFO
//! delay longer than the prepare timeout. Resends then add work that the transactions did not
//! require, consume the same participant capacity as the delayed prepares, and keep later votes
//! late enough to cause further resends. The deterministic test asserts that this redundant work
//! and backlog persist hundreds of rounds after the burst ends.
//!
//! # Timer parameters
//!
//! A deployment wires `coordinator.source_interval(period)` to `coordinator_timer` and
//! `participant.source_interval(period)` to `participant_timer`. In simulation both are explicit
//! inputs. Coordinator elements advance timeout time. Participant elements release one bounded
//! service quantum.
//!
//! # Measured configuration
//!
//! The hand calculation uses baseline 1 transaction per round, participant capacity 5 messages
//! per round, timeout 4 ticks, and a trigger of 15 transactions per round for rounds 100..120.
//! A transaction requires four participant operations (two prepares and two commits), so baseline
//! demand is 4 against capacity 5. The trigger adds 14 * 20 * 2 = 560 initial prepare operations
//! before commits or retries, implying at least 112 rounds of delay. That exceeds the timeout, so
//! each pending participant vote can offer another prepare every four coordinator ticks. The exact
//! deterministic totals differ because votes and commits join the FIFO while the trigger runs.
//!
//! | run | baseline | capacity | trigger | timeout | asserted tail (rounds 600..800) | label and basis |
//! |---|---:|---:|---|---:|---|---|
//! | burst | 1 transaction/round | 5 messages/round | 15/round, rounds 100..120 | 4 | 8 completions, 56,742 retries, backlog 73,335 -> 129,260 | hazardous, by exhibited collapse |
//! | control | 1 transaction/round | 5 messages/round | none | 4 | 200 completions, 0 retries, maximum backlog 2 | healthy control |

use std::collections::BTreeMap;

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Coordinator;
pub struct Participant;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Coordinator ticks between sends of a prepare whose vote is still missing.
    pub prepare_timeout_ticks: u64,
    /// Protocol messages the participant service processes per participant timer element.
    pub participant_capacity: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ParticipantMessage {
    Prepare { tx_id: u64, participant: u8 },
    Commit { tx_id: u64, participant: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Vote {
    pub tx_id: u64,
    pub participant: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Completion {
    pub tx_id: u64,
    pub latency_ticks: u64,
}

#[derive(Clone, Debug)]
pub struct PendingTx {
    pub first_sent: u64,
    pub last_sent: [u64; 2],
    pub votes: u8,
}

#[derive(Clone, Debug, Default)]
pub struct CoordinatorState {
    pub pending: BTreeMap<u64, PendingTx>,
}

/// Applies one coordinator tick. New transactions are inserted before votes are applied; timeout
/// retries are generated only when a coordinator timer element was admitted.
pub fn coordinator_step(
    mut state: CoordinatorState,
    new_ids: Vec<u64>,
    votes: Vec<Vote>,
    now: u64,
    pump: bool,
    timeout: u64,
) -> (
    CoordinatorState,
    Vec<ParticipantMessage>,
    Vec<Completion>,
    u64,
) {
    let mut wire = Vec::new();
    let mut completed = Vec::new();
    let mut retries = 0;

    for tx_id in new_ids {
        state.pending.insert(
            tx_id,
            PendingTx {
                first_sent: now,
                last_sent: [now, now],
                votes: 0,
            },
        );
        for participant in 0..2 {
            wire.push(ParticipantMessage::Prepare { tx_id, participant });
        }
    }

    for vote in votes {
        if let Some(tx) = state.pending.get_mut(&vote.tx_id) {
            tx.votes |= 1 << vote.participant;
        }
    }

    let done = state
        .pending
        .iter()
        .filter_map(|(&id, tx)| (tx.votes == 0b11).then_some(id))
        .collect::<Vec<_>>();
    for tx_id in done {
        let tx = state
            .pending
            .remove(&tx_id)
            .expect("completed transaction exists");
        completed.push(Completion {
            tx_id,
            latency_ticks: now - tx.first_sent,
        });
        for participant in 0..2 {
            wire.push(ParticipantMessage::Commit { tx_id, participant });
        }
    }

    if pump {
        for (&tx_id, tx) in &mut state.pending {
            for participant in 0..2u8 {
                let mask = 1 << participant;
                if tx.votes & mask == 0
                    && now.saturating_sub(tx.last_sent[participant as usize]) >= timeout
                {
                    tx.last_sent[participant as usize] = now;
                    wire.push(ParticipantMessage::Prepare { tx_id, participant });
                    retries += 1;
                }
            }
        }
    }

    (state, wire, completed, retries)
}

pub struct Outputs<'a> {
    /// Every prepare and commit put on the coordinator-to-participant wire.
    pub coordinator_wire:
        Stream<ParticipantMessage, Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every vote put on the participant-to-coordinator wire.
    pub vote_wire: Stream<Vote, Process<'a, Participant>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every protocol message for which participant capacity was consumed.
    pub processed:
        Stream<ParticipantMessage, Process<'a, Participant>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Transactions committed for the first time, with coordinator-clock latency.
    pub completed: Stream<Completion, Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Participant FIFO depth after each slice execution.
    pub backlog: Stream<usize, Process<'a, Participant>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Coordinator pending transaction count after each slice execution.
    pub pending: Stream<usize, Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Number of timeout-generated prepare sends in each coordinator slice execution.
    pub retries: Stream<u64, Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
}

pub fn two_phase_commit<'a>(
    coordinator: &Process<'a, Coordinator>,
    participant: &Process<'a, Participant>,
    transactions: Stream<(), Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
    coordinator_timer: Stream<(), Process<'a, Coordinator>, Unbounded, TotalOrder, ExactlyOnce>,
    participant_timer: Stream<(), Process<'a, Participant>, Unbounded, TotalOrder, ExactlyOnce>,
    config: Config,
) -> Outputs<'a> {
    let Config {
        prepare_timeout_ticks,
        participant_capacity,
    } = config;

    let (votes_complete, votes) = coordinator.forward_ref::<Stream<
        Vote,
        Process<'a, Coordinator>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >>();

    let (coordinator_wire, completed, pending, retries) = sliced! {
        let clocks = use::batch(coordinator_timer.enumerate(), nondet!(/** delaying clock admission changes when timeouts are judged, but never invents a clock element */));
        let new_transactions = use::batch(transactions.enumerate(), nondet!(/** transaction admission changes its logical start tick, while its id remains arrival ordered */));
        let admitted_votes = use::batch(votes, nondet!(/** delaying a vote can cause timeout-generated duplicate prepares */));
        let mut state = use::state(|l| l.singleton(q!(crate::cluster::witnesses::blind::two_phase_commit::CoordinatorState::default())));
        let mut now = use::state(|l| l.singleton(q!(0u64)));

        let now_cur = clocks.clone().map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        let pump = clocks.count().map(q!(|count| count > 0));
        let ids = new_transactions.map(q!(|(id, ())| id as u64)).fold(q!(Vec::new), q!(|v, id| v.push(id)));
        let vote_vec = admitted_votes.fold(q!(Vec::new), q!(|v, vote| v.push(vote)));
        let stepped = state
            .zip(ids)
            .zip(vote_vec)
            .zip(now_cur)
            .zip(pump)
            .map(q!(move |((((state, ids), votes), now), pump)| {
                crate::cluster::witnesses::blind::two_phase_commit::coordinator_step(
                    state,
                    ids,
                    votes,
                    now,
                    pump,
                    prepare_timeout_ticks,
                )
            }));
        state = stepped.clone().map(q!(|(state, _, _, _)| state));
        let wire = stepped.clone().flat_map_ordered(q!(|(_, wire, _, _)| wire));
        let completed = stepped.clone().flat_map_ordered(q!(|(_, _, completed, _)| completed));
        let pending = state.clone().map(q!(|state| state.pending.len())).into_stream();
        let retries = stepped.map(q!(|(_, _, _, retries)| retries)).into_stream();
        (wire, completed, pending, retries)
    };

    let participant_incoming = coordinator_wire
        .clone()
        .send(participant, TCP.fail_stop().bincode());

    let (processed, backlog, vote_wire) = sliced! {
        let clocks = use::batch(participant_timer, nondet!(/** delaying a timer withholds participant capacity; each admitted timer releases one service quantum */));
        let arrivals = use::batch(participant_incoming, nondet!(/** delaying messages changes FIFO wait and therefore whether coordinator timeouts fire */));
        let mut queue = use::state_null::<Stream<ParticipantMessage, Tick<_>, Bounded, TotalOrder>>();
        let take = clocks.count().map(q!(move |count| if count > 0 { participant_capacity as usize } else { 0 }));
        let queued = queue.chain(arrivals).enumerate();
        let served = queued
            .clone()
            .cross_singleton(take.clone())
            .filter_map(q!(|((index, message), take)| (index < take).then_some(message)));
        queue = queued
            .cross_singleton(take)
            .filter_map(q!(|((index, message), take)| (index >= take).then_some(message)));
        let votes = served.clone().filter_map(q!(|message| match message {
            ParticipantMessage::Prepare { tx_id, participant } => Some(Vote { tx_id, participant }),
            ParticipantMessage::Commit { .. } => None,
        }));
        (served, queue.clone().count().into_stream(), votes)
    };

    votes_complete.complete(
        vote_wire
            .clone()
            .send(coordinator, TCP.fail_stop().bincode()),
    );

    Outputs {
        coordinator_wire,
        vote_wire,
        processed,
        completed,
        backlog,
        pending,
        retries,
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    const ROUNDS: usize = 800;
    const TRIGGER_START: usize = 100;
    const TRIGGER_END: usize = 120;
    const BASELINE: usize = 1;
    const TRIGGER: usize = 15;
    const CONFIG: Config = Config {
        prepare_timeout_ticks: 4,
        participant_capacity: 5,
    };

    #[derive(Clone, Debug, Default)]
    struct Round {
        prepares: usize,
        commits: usize,
        votes: usize,
        processed: usize,
        completed: usize,
        retries: u64,
        backlog: usize,
        pending: usize,
    }

    fn run(with_trigger: bool) -> Vec<Round> {
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
            CONFIG,
        );
        let wire = outputs.coordinator_wire.sim_output();
        let votes = outputs.vote_wire.sim_output();
        let processed = outputs.processed.sim_output();
        let completed = outputs.completed.sim_output();
        let retries = outputs.retries.sim_output();
        let backlog = outputs.backlog.sim_output();
        let pending = outputs.pending.sim_output();
        let mut trace = Vec::with_capacity(ROUNDS);
        let trace_ref = &mut trace;

        flow.sim().run_prompt(async move || {
            let mut last_backlog = 0;
            let mut last_pending = 0;
            for round in 0..ROUNDS {
                coord_tick_send.send(());
                part_tick_send.send(());
                let offered = if with_trigger && (TRIGGER_START..TRIGGER_END).contains(&round) {
                    TRIGGER
                } else {
                    BASELINE
                };
                tx_send.send_many(std::iter::repeat_n((), offered));
                quiesce().await;

                let mut record = Round::default();
                for message in wire.collect::<Vec<_>>().await {
                    match message {
                        ParticipantMessage::Prepare { .. } => record.prepares += 1,
                        ParticipantMessage::Commit { .. } => record.commits += 1,
                    }
                }
                record.votes = votes.collect::<Vec<_>>().await.len();
                record.processed = processed.collect::<Vec<_>>().await.len();
                record.completed = completed.collect::<Vec<_>>().await.len();
                record.retries = retries.collect::<Vec<_>>().await.into_iter().sum();
                if let Some(value) = backlog.collect::<Vec<_>>().await.into_iter().last() {
                    last_backlog = value;
                }
                if let Some(value) = pending.collect::<Vec<_>>().await.into_iter().last() {
                    last_pending = value;
                }
                record.backlog = last_backlog;
                record.pending = last_pending;
                trace_ref.push(record);
            }
        });
        trace
    }

    fn sum(trace: &[Round], field: impl Fn(&Round) -> usize) -> usize {
        trace.iter().map(field).sum()
    }

    #[test]
    fn prepare_retries_exhibit_persistent_collapse() {
        let trace = run(true);
        let pre = &trace[20..80];
        let tail = &trace[600..800];
        let pre_completed = sum(pre, |r| r.completed);
        let tail_completed = sum(tail, |r| r.completed);
        let tail_retries: u64 = tail.iter().map(|r| r.retries).sum();
        let tail_processed = sum(tail, |r| r.processed);
        let backlog_start = tail.first().unwrap().backlog;
        let backlog_end = tail.last().unwrap().backlog;
        let pending_end = tail.last().unwrap().pending;
        eprintln!(
            "round collapse: pre_completed={pre_completed}, tail_completed={tail_completed}, tail_retries={tail_retries}, tail_processed={tail_processed}, backlog={backlog_start}->{backlog_end}, pending_end={pending_end}"
        );
        assert!(
            pre_completed >= 55,
            "the light-load baseline must be healthy"
        );
        assert!(
            tail_retries >= 1_000,
            "timeout-generated work must persist in the far tail"
        );
        assert!(
            backlog_end > backlog_start,
            "the participant backlog must still grow in the tail"
        );
        assert!(
            tail_completed <= 10,
            "completion throughput must remain collapsed"
        );
    }

    #[test]
    fn no_trigger_control_stays_healthy() {
        let trace = run(false);
        let tail = &trace[600..800];
        let total_retries: u64 = trace.iter().map(|r| r.retries).sum();
        let tail_completed = sum(tail, |r| r.completed);
        let max_backlog = trace.iter().map(|r| r.backlog).max().unwrap();
        let pending_end = trace.last().unwrap().pending;
        eprintln!(
            "round control: tail_completed={tail_completed}, total_retries={total_retries}, max_backlog={max_backlog}, pending_end={pending_end}"
        );
        assert_eq!(tail_completed, 200);
        assert_eq!(total_retries, 0);
        assert!(max_backlog <= 4);
        assert!(pending_end <= 1);
    }
}
