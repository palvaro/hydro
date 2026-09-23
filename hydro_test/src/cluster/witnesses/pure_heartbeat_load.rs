//! Load harness for the pure heartbeat of [`crate::cluster::pure_heartbeat`], a benign negative
//! control.
//!
//! The program is unchanged: on every element of its timer, each member of a cluster broadcasts
//! one fixed-size heartbeat to every member (itself included) over `broadcast_closed`. There is
//! no acknowledgement, no retry, no state on the work path, and nothing that reads what arrived.
//! Work is therefore a pure function of the input: every timer element produces exactly `n`
//! messages, whatever the scheduler does with them.
//!
//! # Mechanism and knob
//!
//! There is no mechanism to switch off. The harness confirms the negative: a burst of timer
//! elements at one member produces exactly `n` messages per element and nothing after the burst
//! ends; withholding a member's timer for any number of rounds and releasing it produces the
//! same total; and under schedule exploration the total received always equals `n` times the
//! elements sent.
//!
//! # Timer parameters
//!
//! - `timer` (per member): one element per heartbeat sent. A deployment wires
//!   `cluster.source_interval(period)` into it; the simulation feeds it from `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Three members, one timer element each per round; the trigger pulses member 0 twelve times per
//! round during rounds 100 to 160. Tail is rounds 600 to 800.
//!
//! | run | trigger | messages per round before / during / after | whole run: elements sent, messages received | tail: elements, messages | label |
//! |---|---|---|---|---|---|
//! | burst at one member | yes | 9 / 42 / 9 | 3060, 9180 (exactly 3 per element) | 600, 1800 | benign |
//! | timer of member 0 held 0, 4, 16 or 64 rounds, no trigger, 300 rounds | no | 9 except during the hold and its release | 900, 2700 at every hold | | benign |
//! | 256 fuzzed schedules, 8 rounds with a 3-round burst | yes | `n` times the elements sent in every round | | | benign |
//!
//! Every member received exactly the elements sent in every one of the 800 rounds, the round
//! after the burst was already back to 9, and neither holding the timer nor varying the schedule
//! changed the total by a single message.

/// The harness's workload: timer elements per member per round, with a window in which member 0
/// is pulsed faster.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_pulses: u32,
    /// Pulses per round for member 0 inside the trigger window.
    pub trigger_pulses_for_member_0: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn pulses_at(&self, round: u64, member: u32) -> u32 {
        if member == 0 && round >= self.trigger_start && round < self.trigger_end {
            self.trigger_pulses_for_member_0
        } else {
            self.baseline_pulses
        }
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use super::Workload;
    use crate::cluster::pure_heartbeat::{Heartbeat, pure_heartbeat};

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Timer elements sent this round, over all members.
        pulses: u64,
        /// Heartbeats received this round, per member.
        received: Vec<u64>,
    }

    fn total_received(r: &Round) -> u64 {
        r.received.iter().sum()
    }

    fn run(n: u32, workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer).entries().sim_cluster_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim()
            .with_cluster_size(&cluster, n as usize)
            .run_prompt(async move || {
                for round in 0..rounds as u64 {
                    let mut r = Round::default();
                    for member in 0..n {
                        for _ in 0..workload.pulses_at(round, member) {
                            timer_send.send(member, ());
                            r.pulses += 1;
                        }
                    }
                    quiesce().await;
                    for member in 0..n {
                        let got = received
                            .collect_sorted::<Vec<(MemberId<()>, Heartbeat)>>(member)
                            .await;
                        r.received.push(got.len() as u64);
                    }
                    trace_ref.push(r);
                }
            });
        trace
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Three members, one timer element each per round: every member receives exactly 3
    /// heartbeats per round (one from each member, itself included), 9 messages per round in
    /// all. During the trigger member 0 is pulsed 12 times per round, so it sends 12 heartbeats
    /// to each member: every member receives 14 per round and the total is 42, which is exactly
    /// `n` times the 14 elements sent. The round after the trigger ends the total is 9 again;
    /// nothing is queued, retried or repeated, and over the whole run the total received is
    /// exactly `n` times the total elements sent. Withholding member 0's timer for `hold` rounds
    /// and releasing it changes when the heartbeats are sent and nothing else.
    const N: u32 = 3;
    const WORKLOAD: Workload = Workload {
        baseline_pulses: 1,
        trigger_pulses_for_member_0: 12,
        trigger_start: 100,
        trigger_end: 160,
    };
    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 100, 130, 159, 160, 161, 200, 400, 600, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!("round {i}: pulses={} received={:?} total={}", r.pulses, r.received, total_received(r));
            }
        }
    }

    #[test]
    fn every_timer_element_costs_exactly_n_messages() {
        let trace = run(N, WORKLOAD, ROUNDS);
        print_trajectory(&trace);
        for (i, r) in trace.iter().enumerate() {
            assert_eq!(total_received(r), N as u64 * r.pulses, "round {i}: work is exactly n per element");
            assert!(r.received.iter().all(|&x| x == r.pulses), "round {i}: every member receives every heartbeat");
        }
        assert!(trace[..100].iter().all(|r| total_received(r) == 9));
        assert!(trace[100..160].iter().all(|r| total_received(r) == 42));
        assert!(trace[160..].iter().all(|r| total_received(r) == 9));
        let total: u64 = trace.iter().map(total_received).sum();
        let pulses: u64 = trace.iter().map(|r| r.pulses).sum();
        println!(
            "whole run: {pulses} elements, {total} messages ({}x); tail rounds {TAIL_START}..{ROUNDS}: {} messages for {} elements",
            total / pulses,
            trace[TAIL_START..].iter().map(total_received).sum::<u64>(),
            trace[TAIL_START..].iter().map(|r| r.pulses).sum::<u64>()
        );
        assert_eq!(total, N as u64 * pulses);
    }

    /// Withholding member 0's timer for `hold` rounds from round 100 and releasing it together
    /// leaves the total unchanged: the trigger-free run sends 2400 elements and receives 7200
    /// messages at every hold.
    #[test]
    fn holding_the_timer_changes_nothing_but_timing() {
        const RUN: usize = 300;
        for hold in [0usize, 4, 16, 64] {
            let mut flow = FlowBuilder::new();
            let cluster = flow.cluster::<()>();
            let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
            let received = pure_heartbeat(&cluster, timer).entries().sim_cluster_output();
            let mut total = 0u64;
            let total_ref = &mut total;
            flow.sim()
                .with_cluster_size(&cluster, N as usize)
                .run_prompt(async move || {
                    for round in 0..RUN {
                        for member in 1..N {
                            timer_send.send(member, ());
                        }
                        let held = (100..100 + hold).contains(&round);
                        let pulses = if held { 0 } else if round == 100 + hold { hold + 1 } else { 1 };
                        for _ in 0..pulses {
                            timer_send.send(0, ());
                        }
                        quiesce().await;
                        for member in 0..N {
                            *total_ref += received
                                .collect_sorted::<Vec<(MemberId<()>, Heartbeat)>>(member)
                                .await
                                .len() as u64;
                        }
                    }
                });
            println!("hold {hold} rounds -> {total} messages for {} elements", N as usize * RUN);
            assert_eq!(total, (N as u64) * (N as u64) * RUN as u64);
        }
    }

    /// Under schedule exploration the total received after quiescence is `n` times the elements
    /// sent, in every round, whatever the batching.
    #[test]
    fn the_bound_holds_across_schedules() {
        const ROUNDS: usize = 8;
        let workload = Workload {
            baseline_pulses: 1,
            trigger_pulses_for_member_0: 5,
            trigger_start: 2,
            trigger_end: 5,
        };
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer).entries().sim_cluster_output();
        flow.sim()
            .with_cluster_size(&cluster, N as usize)
            .unit_test_fuzz_iterations(256)
            .fuzz(async || {
                for round in 0..ROUNDS as u64 {
                    let mut pulses = 0u64;
                    for member in 0..N {
                        for _ in 0..workload.pulses_at(round, member) {
                            timer_send.send(member, ());
                            pulses += 1;
                        }
                    }
                    quiesce().await;
                    let mut total = 0u64;
                    for member in 0..N {
                        total += received
                            .collect_sorted::<Vec<(MemberId<()>, Heartbeat)>>(member)
                            .await
                            .len() as u64;
                    }
                    assert_eq!(total, N as u64 * pulses, "round {round}");
                }
            });
    }
}
