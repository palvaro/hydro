//! A load harness for the state-based G-Set gossip in
//! [`hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip`].
//!
//! The program is unchanged and imported. Each member folds its own updates and everything it
//! receives into a `BTreeSet`; a new local update is broadcast to every peer at once, and on every
//! `gossip_ticks` element a member re-broadcasts its whole set to every peer. The program exports
//! only the set, so the harness observes every version of each member's set (one per merged
//! element) and derives the wire work from its size: a pump element makes a member send its set
//! to each of the `n - 1` peers, so the elements a member puts on the wire in a round are at most
//! `(n - 1) * (updates + |set|)`.
//!
//! # Mechanism and knob
//!
//! There is no retry, timeout, or acknowledgement anywhere in the program. Re-broadcasts are
//! driven by the pump alone, and a tick that observes a pump element sends exactly one copy of
//! the set to each peer whatever it received or failed to receive. Holding gossip messages in a
//! batch therefore delays convergence but cannot cause an extra send, and per-round work is a
//! function of the set's size, which the input alone determines. There is no knob because there
//! is no mechanism to switch off; the trigger here is a burst of updates, which permanently
//! enlarges the set and so the cost of every later round, but does not change how many sends a
//! round makes.
//!
//! # Timer parameters
//!
//! - `gossip_ticks`: one element per member per gossip period. A deployment wires
//!   `cluster.source_interval(period)` into it; the simulation feeds it from `sim_input`, one
//!   element per member per round.
//!
//! # Measured (see `sim_tests`)
//!
//! Three members, one update per round dealt round-robin, pump at every member every round. The
//! trigger raises updates to 10 per round during rounds 30 to 50. Tail is rounds 90 to 120. The
//! `merges` column is the number of set versions the observer saw per member per round, which is
//! the number of elements the member's fold re-merged in that round (`3 |set| - 2`).
//!
//! | run | trigger | missing elements, any member, any round | union after 29 / 49 / 119 | wire bound per round at 30 / 49 / 119 | merges per member per round at 29 / 49 / 119 | tail wire growth per round | label |
//! |---|---|---|---|---|---|---|---|
//! | burst | yes | 0 | 30 / 230 / 300 | 260 / 1400 / 1802 | 88 / 670 / 898 | 6, constant | benign |
//! | no trigger | no | 0 | 30 / 50 / 120 | 188 / 302 / 722 | 88 / 148 / 358 | 6, constant | healthy |
//!
//! Under 256 fuzzed schedules of a 6-round run (3 members, 1 update per round rising to 4 in
//! rounds 1 and 2), every member's set is a subset of the union at every observation and equals
//! the union after every quiescence.
//!
//! The per-round cost of full-state gossip is `(n - 1) * (n * |set| + updates)` elements on the
//! wire and `(n - 1) |set|` merges per member, which the input alone determines; the burst leaves
//! the set, and so every later round, larger, but no schedule and no trigger changes the number of
//! sends a round makes. Work is bounded by a function of the input under every schedule, which is
//! the benign label, although total work over a run grows with the square of the number of updates
//! rather than linearly. The run is 120
//! rounds rather than 800 because the simulator's cost is quadratic in the set (see `sim_tests`).

/// An open-loop workload for the tests: updates per round, dealt round-robin to members, with a
/// window in which the rate is higher. Update values are unique across rounds and members. This
/// lives in the harness.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline_per_round: u32,
    pub trigger_per_round: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn updates_at(&self, round: u64) -> u32 {
        if round >= self.trigger_start && round < self.trigger_end {
            self.trigger_per_round
        } else {
            self.baseline_per_round
        }
    }

    /// The `k`-th update of `round` goes to this member.
    pub fn member_of(round: u64, k: u32, n: u32) -> u32 {
        ((round as u32) + k) % n
    }

    pub fn value(round: u64, k: u32) -> u32 {
        (round as u32) * 1000 + k
    }
}

#[cfg(test)]
mod sim_tests {
    use std::collections::BTreeSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;
    use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

    use super::Workload;

    #[derive(Debug, Clone, Default)]
    struct Round {
        /// Number of distinct values any member has issued so far.
        union_size: usize,
        /// Per member: size of its set at the end of the round.
        set_sizes: Vec<usize>,
        /// Per member: `union_size - set size`, the elements it has not merged yet.
        missing: Vec<usize>,
        /// Per member: how many set snapshots the observer saw this round, i.e. an upper bound on
        /// how many times the set changed.
        snapshots: Vec<usize>,
        /// Upper bound on elements put on the wire this round, derived as
        /// `sum over members of (n - 1) * (updates issued + set size)`.
        wire_elements_bound: usize,
    }

    fn run(n: u32, workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (pump_send, pumps) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();

        let state = g_set_gossip(&cluster, updates, pumps);

        let obs_tick = cluster.tick();
        let observed = state
            .snapshot(&obs_tick, nondet!(/** harness observation */))
            .all_ticks()
            .sim_cluster_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, n as usize)
            .run_prompt(async move || {
                let mut union: BTreeSet<u32> = BTreeSet::new();
                let mut latest: Vec<BTreeSet<u32>> = vec![BTreeSet::new(); n as usize];
                for round in 0..rounds as u64 {
                    let u = workload.updates_at(round);
                    for member in 0..n {
                        pump_send.send(member, ());
                    }
                    for k in 0..u {
                        let v = Workload::value(round, k);
                        union.insert(v);
                        update_send.send_many_unordered([(Workload::member_of(round, k, n), v)]);
                    }
                    quiesce().await;

                    let mut r = Round {
                        union_size: union.len(),
                        ..Round::default()
                    };
                    for member in 0..n {
                        let snapshots: Vec<BTreeSet<u32>> = observed.collect(member).await;
                        r.snapshots.push(snapshots.len());
                        if let Some(last) = snapshots.into_iter().last() {
                            latest[member as usize] = last;
                        }
                        let size = latest[member as usize].len();
                        r.set_sizes.push(size);
                        r.missing.push(union.len() - size);
                        let issued = (0..u).filter(|&k| Workload::member_of(round, k, n) == member).count();
                        r.wire_elements_bound += (n as usize - 1) * (issued + size);
                    }
                    trace_ref.push(r);
                }
            });
        trace
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Three members, one update per round dealt round-robin, pump at every member every round.
    /// Every update is broadcast to both peers as soon as it arrives, so at the end of every
    /// round every member's set equals the union: `missing` is 0 for every member in every
    /// round, before, during and after the trigger. The union grows by 1 per round at baseline
    /// and by 10 per round during the 20 trigger rounds, so it is 30 after round 29, 230 after
    /// round 49 and 300 after round 119. The wire bound per round is
    /// `sum over members of 2 * (issued + |set|) = 2 * u + 6 * |set|`, i.e. 260 in round 30,
    /// 1400 in round 49 and 1802 in round 119; it grows with the set and with nothing else, and
    /// in the tail it grows by exactly 6 per round.
    ///
    /// The run is 120 rounds rather than the corpus's usual 800 because the simulator's cost of
    /// this program grows with the square of the set: every element of every re-broadcast set is
    /// re-merged by the receiver's fold, so each member does `(n - 1) * |set|` merges per round,
    /// and each merge yields a new version of the set for the observer. A first attempt at 800
    /// rounds with three updates per round (union 4020) had not finished after an hour; a probe
    /// measured about 1.1 s per round at a union of 350. The trigger sits in rounds 30 to 50 and
    /// the tail is rounds 90 to 120, which keeps the same shape.
    const WORKLOAD: Workload = Workload {
        baseline_per_round: 1,
        trigger_per_round: 10,
        trigger_start: 30,
        trigger_end: 50,
    };
    const N: u32 = 3;
    const ROUNDS: usize = 120;
    const TAIL_START: usize = 90;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 1, 15, 29, 30, 40, 49, 50, 70, 90, 119] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: union={} sets={:?} missing={:?} snapshots={:?} wire_bound={}",
                    r.union_size, r.set_sizes, r.missing, r.snapshots, r.wire_elements_bound
                );
            }
        }
    }

    #[test]
    fn gossip_converges_every_round_and_work_tracks_state_size() {
        let trace = run(N, WORKLOAD, ROUNDS);
        print_trajectory(&trace);

        // Every member holds the union at the end of every round, trigger or not.
        for (i, r) in trace.iter().enumerate() {
            assert!(r.missing.iter().all(|&m| m == 0), "round {i}: members missing elements: {:?}", r.missing);
        }
        // The union is what the workload issued.
        assert_eq!(trace[29].union_size, 30);
        assert_eq!(trace[49].union_size, 230);
        assert_eq!(trace[ROUNDS - 1].union_size, 300);
        // Per-round wire work is exactly the derived function of set size: `(n - 1) (u + n |S|)`.
        for (i, r) in trace.iter().enumerate() {
            let u = WORKLOAD.updates_at(i as u64) as usize;
            assert_eq!(r.wire_elements_bound, (N as usize - 1) * (u + (N as usize) * r.union_size));
        }
        assert_eq!(trace[30].wire_elements_bound, 260);
        assert_eq!(trace[49].wire_elements_bound, 1400);
        assert_eq!(trace[ROUNDS - 1].wire_elements_bound, 1802);
        // The tail's per-round work is the post-trigger set size times a constant, not a runaway:
        // it grows by exactly `n (n - 1) * baseline` elements per round.
        let tail = &trace[TAIL_START..];
        for w in tail.windows(2) {
            assert_eq!(
                w[1].wire_elements_bound - w[0].wire_elements_bound,
                (N as usize) * (N as usize - 1) * WORKLOAD.baseline_per_round as usize
            );
        }
        let total_bound: usize = trace.iter().map(|r| r.wire_elements_bound).sum();
        println!(
            "wire bound: round 29 {}, round 49 {}, round 119 {}, total over the run {total_bound} for {} updates",
            trace[29].wire_elements_bound,
            trace[49].wire_elements_bound,
            trace[ROUNDS - 1].wire_elements_bound,
            trace[ROUNDS - 1].union_size
        );
    }

    /// Control: no trigger. The only difference is the set size, and so the per-round bound.
    #[test]
    fn without_a_trigger_the_only_difference_is_set_size() {
        let trace = run(
            N,
            Workload {
                trigger_per_round: WORKLOAD.baseline_per_round,
                ..WORKLOAD
            },
            ROUNDS,
        );
        print_trajectory(&trace);
        assert!(trace.iter().all(|r| r.missing.iter().all(|&m| m == 0)));
        assert_eq!(trace[ROUNDS - 1].union_size, ROUNDS);
        assert_eq!(trace[ROUNDS - 1].wire_elements_bound, 2 * (1 + 3 * ROUNDS));
    }

    /// Under schedule exploration a member's set is always a subset of the union, and once the
    /// simulation has quiesced every member holds the union: holding gossip in a batch delays
    /// nothing past quiescence and causes no extra broadcast, since the pump alone drives them.
    #[test]
    fn convergence_holds_across_schedules() {
        const ROUNDS: usize = 6;
        let workload = Workload {
            baseline_per_round: 1,
            trigger_per_round: 4,
            trigger_start: 1,
            trigger_end: 3,
        };
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (pump_send, pumps) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let state = g_set_gossip(&cluster, updates, pumps);
        let obs_tick = cluster.tick();
        let observed = state
            .snapshot(&obs_tick, nondet!(/** harness observation */))
            .all_ticks()
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .unit_test_fuzz_iterations(256)
            .fuzz(async || {
                let mut union: BTreeSet<u32> = BTreeSet::new();
                for round in 0..ROUNDS as u64 {
                    for member in 0..3u32 {
                        pump_send.send(member, ());
                    }
                    for k in 0..workload.updates_at(round) {
                        let v = Workload::value(round, k);
                        union.insert(v);
                        update_send.send_many_unordered([(Workload::member_of(round, k, 3), v)]);
                    }
                    quiesce().await;
                    for member in 0..3u32 {
                        let snapshots: Vec<BTreeSet<u32>> = observed.collect(member).await;
                        for s in &snapshots {
                            assert!(s.is_subset(&union), "member {member} holds a value nobody issued");
                        }
                        assert_eq!(
                            snapshots.last(),
                            Some(&union),
                            "member {member} did not hold the union after quiescence in round {round}"
                        );
                    }
                }
            });
    }
}
