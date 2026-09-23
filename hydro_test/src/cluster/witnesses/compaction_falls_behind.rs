//! A single-process log-structured store whose compaction is background work.
//!
//! The store receives a totally ordered stream of operations, each either a `Put` (append one
//! record to the uncompacted segment) or a `Get` (a lookup that must scan the uncompacted segment
//! before it can consult the compacted index). Operations wait in a FIFO. Each element of `clock`
//! grants the store [`StoreConfig::budget_per_tick`] units of work. Serving a `Put` costs one
//! unit; serving a `Get` costs one unit plus one unit per record in the uncompacted segment at the
//! start of the tick. The store serves operations in FIFO order while the units already spent
//! this tick are below the serving budget, so an expensive `Get` may overrun the budget, and a
//! tick always serves at least one operation when it has any. Compaction moves records from the
//! segment into the index at one unit per record, and it runs with whatever budget is left after
//! serving.
//!
//! # Mechanism and knob
//!
//! Compaction and serving share one budget, and the cost of a `Get` grows with the segment that
//! compaction has not yet processed. When a burst of reads saturates the budget, compaction gets
//! nothing, every served `Put` lengthens the segment, every `Get` gets more expensive, and the
//! number of operations a tick can serve falls. Once a `Get` costs more than a whole tick, the
//! store serves about one read per tick no matter how light the offered load is, so the backlog
//! grows forever under a load it handled before the burst. The knob is
//! [`StoreConfig::compaction_reserve`]: the number of units taken off the top of every tick for
//! compaction before any operation is served. With a reserve of zero, compaction is pure
//! background work and the program amplifies. With a reserve at least as large as the puts that
//! arrive per tick, the segment is bounded, every `Get` costs about one unit, and the program
//! recovers from the same burst. The mechanism is present in both configurations, so both are
//! labelled hazardous; the reserve changes the outcome under this trigger without removing the
//! coupling, and the corpus table records the reserved configuration as having no ground truth.
//!
//! This differs from the corpus sketch, in which a non-incremental compaction blocks appends. In
//! that version every record is appended once and scanned once, so total work is a fixed two
//! units per record under every schedule and nothing feeds back. The read-cost coupling used
//! here is the one that makes compaction debt self-reinforcing.
//!
//! # Timer parameters
//!
//! - `clock`: one element per logical store tick. Each element grants one tick's budget; a tick
//!   of the dataflow that runs without a clock element (for instance because only operations
//!   arrived) has no budget and only queues. A deployment wires `store.source_interval(period)`
//!   into it; the simulation feeds it from `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Collapse runs: baseline 1 `Put` and 4 `Get`s per round against a budget of 30 units per round;
//! the trigger offers 1 `Put` and 40 `Get`s per round during rounds 100 to 160. Tail is rounds
//! 600 to 800, where baseline completions would be 1000 (800 of them reads).
//!
//! | run | reserve | trigger | tail served (puts / gets) | tail units (serving / compaction) | backlog at 600 -> 800 | segment at 600 -> 800 | label |
//! |---|---|---|---|---|---|---|---|
//! | background compaction | 0 | yes | 5 / 200 | 6805 / 0 | 3432 -> 4223 | 31 -> 36 | hazardous, collapses |
//! | reserved compaction | 8 | yes | 200 / 800 | 1000 / 200 | 0 -> 0 (peak 703 at round 159, empty from round 189) | 0 -> 0 (never above 6) | no ground truth; the coupling is present and the tool is expected to find it |
//! | no trigger | 0 | no | 200 / 800 | 1000 / 200 | 0 -> 0 | 0 -> 0 | healthy |
//!
//! Hold runs (`held_arrivals_cost_extra_units_growing_with_the_hold`): no trigger, 3 `Put`s and
//! 2 `Get`s per round for 300 rounds, with the operations of `hold` rounds starting at round 100
//! withheld and delivered together. The column is the units spent over the run beyond the one
//! unit per operation and one compaction unit per put that the inputs require.
//!
//! | hold (rounds) | 0 | 4 | 8 | 16 | 32 |
//! |---|---|---|---|---|---|
//! | extra units, reserve 8 | 0 | 4 | 52 | 102 | 195 |
//! | extra units, reserve 0 | 0 | 40 | 30625 | 28181 | 23581 |
//!
//! With the reserve the extra work grows with the hold and stays small, because each get pays
//! at most for the puts the previous tick served beyond the reserve. Without the reserve a hold
//! of eight rounds (45 operations in one batch, 27 of them puts) is enough on its own, with no
//! burst, to tip the store into the permanent collapse of the trigger run; the extra over a fixed
//! window then grows with the rounds remaining after the release rather than with the hold. The
//! corpus mix of one put per four gets never puts more than six puts in a tick, so under it the
//! reserved store's surcharge is exactly zero at every hold, which is why the hold runs use a
//! put-heavier mix.
//!
//! In the amplifying run the backlog stands at 2111 and the segment at 9 when the trigger ends;
//! from then on the store serves one or two reads per round, each costing the whole budget and
//! more, reaches a put only about once every twenty rounds, and never has a unit left over for
//! compaction, so the backlog grows by about 4 per round and the segment ratchets upward. The
//! reserved store serves 29 operations per round during the same burst because its reads always
//! cost one unit, and it drains the resulting backlog within 30 rounds. The hand computation in
//! `sim_tests` predicted the shape and the tail throughput (about 200 operations against a
//! baseline of 1000) and overestimated the segment, which grew to 9 rather than 15 during the
//! trigger because puts reached the head of the FIFO less often than assumed.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Store;

/// An operation against the store. Keys are not modelled; only the cost structure matters.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Op {
    /// Append one record to the uncompacted segment. Costs one unit.
    Put,
    /// Look a key up. Costs one unit plus one unit per uncompacted record.
    Get,
}

/// One operation the store has served: its arrival index, what it was, and what it cost.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Served {
    pub id: u64,
    pub op: Op,
    pub cost: u64,
}

/// What the store did in one tick of its dataflow.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickStats {
    /// Clock elements observed by this tick; the budget was this many times `budget_per_tick`.
    pub clock_elements: u64,
    /// Units spent serving operations, including any overrun by the last one served.
    pub serve_units: u64,
    /// Records compacted, which is also the units spent compacting.
    pub compact_units: u64,
    /// Operations still queued at the end of the tick.
    pub backlog: usize,
    /// Uncompacted records at the end of the tick.
    pub segment: u64,
    /// Records in the compacted index at the end of the tick.
    pub indexed: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    /// Units of work granted per clock element.
    pub budget_per_tick: u64,
    /// Units taken off the top of every tick for compaction before serving. Zero makes
    /// compaction pure background work.
    pub compaction_reserve: u64,
}

pub struct StoreOutputs<'a> {
    /// Every operation served, in the order served.
    pub served: Stream<Served, Process<'a, Store>, Unbounded, TotalOrder, ExactlyOnce>,
    /// One record per tick of the store's dataflow.
    pub ticks: Stream<TickStats, Process<'a, Store>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the store. `ops` is the application's operation stream; see the module docs for the
/// clock parameter.
pub fn log_with_compaction<'a>(
    store: &Process<'a, Store>,
    ops: Stream<Op, Process<'a, Store>, Unbounded, TotalOrder, ExactlyOnce>,
    clock: Stream<(), Process<'a, Store>, Unbounded, TotalOrder, ExactlyOnce>,
    config: StoreConfig,
) -> StoreOutputs<'a> {
    let StoreConfig {
        budget_per_tick,
        compaction_reserve,
    } = config;
    let _ = store;

    let (served, ticks) = sliced! {
        let clock = use::batch(clock, nondet!(/** batching only decides how many ticks' budgets one dataflow tick spends; every element still grants one budget */));
        let arrivals = use::batch(ops.enumerate(), nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<(u64, Op), Tick<_>, Bounded, TotalOrder>>();
        let mut segment = use::state(|l| l.singleton(q!(0u64)));
        let mut indexed = use::state(|l| l.singleton(q!(0u64)));

        // Budget for this tick: one grant per clock element, so a tick without a clock element
        // does no work.
        let clock_elements = clock.count().map(q!(|n| n as u64));
        let budget = clock_elements.clone().map(q!(move |n| n * budget_per_tick));
        let reserve = clock_elements.clone().map(q!(move |n| n * compaction_reserve));

        // Reserved compaction runs first, on the segment as it stands at the start of the tick.
        let reserved = segment.clone().zip(reserve).map(q!(|(seg, res)| seg.min(res)));
        let segment_after_reserve = segment.clone().zip(reserved.clone()).map(q!(|(seg, r)| seg - r));
        let serve_budget = budget.zip(reserved.clone()).map(q!(|(b, r)| b - r));

        // Cost every queued operation against the segment the tick starts with, then serve in
        // FIFO order while the units spent so far are below the serving budget.
        let queued = backlog.chain(arrivals.map(q!(|(i, op)| (i as u64, op))));
        let costed = queued
            .cross_singleton(segment_after_reserve.clone())
            .map(q!(|((id, op), seg)| (id, op, match op {
                Op::Put => 1,
                Op::Get => 1 + seg,
            })));
        let with_prefix = costed
            .scan(
                q!(|| 0u64),
                q!(|spent_before, (id, op, cost)| {
                    let before = *spent_before;
                    *spent_before += cost;
                    Some((before, id, op, cost))
                }),
            )
            .cross_singleton(serve_budget.clone());
        let served = with_prefix.clone().filter_map(q!(|((before, id, op, cost), budget)| {
            (before < budget).then_some(Served { id, op, cost })
        }));
        backlog = with_prefix.filter_map(q!(|((before, id, op, _), budget)| {
            (before >= budget).then_some((id, op))
        }));

        let serve_units = served
            .clone()
            .map(q!(|s| s.cost))
            .fold(q!(|| 0u64), q!(|acc, c| *acc += c));
        let puts_served = served
            .clone()
            .filter(q!(|s| matches!(s.op, Op::Put)))
            .count()
            .map(q!(|n| n as u64));

        // Served puts extend the segment; background compaction then spends whatever serving
        // left over.
        let segment_after_puts = segment_after_reserve.zip(puts_served).map(q!(|(seg, p)| seg + p));
        let leftover = serve_budget.zip(serve_units.clone()).map(q!(|(b, s)| b.saturating_sub(s)));
        let background = segment_after_puts.clone().zip(leftover).map(q!(|(seg, l)| seg.min(l)));
        let compact_units = reserved.zip(background.clone()).map(q!(|(r, b)| r + b));

        segment = segment_after_puts.zip(background).map(q!(|(seg, b)| seg - b));
        indexed = indexed.zip(compact_units.clone()).map(q!(|(i, c)| i + c));

        let stats = clock_elements
            .zip(serve_units)
            .zip(compact_units)
            .zip(backlog.clone().count())
            .zip(segment.clone())
            .zip(indexed.clone())
            .map(q!(|(((((clock_elements, serve_units), compact_units), backlog), segment), indexed)| TickStats {
                clock_elements,
                serve_units,
                compact_units,
                backlog,
                segment,
                indexed,
            }));

        (served, stats.into_stream())
    };

    StoreOutputs { served, ticks }
}

/// An open-loop workload for the tests: a fixed mix of operations per round, with a window in
/// which the read rate is higher. This lives in the harness, not in the program.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub puts_per_round: u32,
    pub baseline_gets_per_round: u32,
    pub trigger_gets_per_round: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn gets_at(&self, round: u64) -> u32 {
        if round >= self.trigger_start && round < self.trigger_end {
            self.trigger_gets_per_round
        } else {
            self.baseline_gets_per_round
        }
    }
}

/// Simulation under a fixed, fair schedule. A *round* is one element on `clock` plus that
/// round's operations (the put first, then the gets), followed by letting the simulation
/// quiesce, so per round the store runs one tick with one budget.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        served_puts: u64,
        served_gets: u64,
        serve_units: u64,
        compact_units: u64,
        /// Operations queued at the end of the round.
        backlog: usize,
        /// Uncompacted records at the end of the round.
        segment: u64,
        /// Dataflow ticks the store ran this round.
        ticks: u64,
    }

    fn run(workload: Workload, config: StoreConfig, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let store = flow.process::<Store>();
        let (op_send, ops) = store.sim_input::<Op, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = store.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = log_with_compaction(&store, ops, clock, config);
        let served = outputs.served.sim_output();
        let ticks = outputs.ticks.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim().run_prompt(async move || {
            let mut backlog = 0usize;
            let mut segment = 0u64;
            for round in 0..rounds as u64 {
                clock_send.send(());
                for _ in 0..workload.puts_per_round {
                    op_send.send(Op::Put);
                }
                for _ in 0..workload.gets_at(round) {
                    op_send.send(Op::Get);
                }
                quiesce().await;

                let mut r = Round::default();
                while let Some(s) = served.try_next().await {
                    match s.op {
                        Op::Put => r.served_puts += 1,
                        Op::Get => r.served_gets += 1,
                    }
                }
                while let Some(t) = ticks.try_next().await {
                    r.ticks += 1;
                    r.serve_units += t.serve_units;
                    r.compact_units += t.compact_units;
                    backlog = t.backlog;
                    segment = t.segment;
                }
                r.backlog = backlog;
                r.segment = segment;
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
    /// Baseline is 1 put and 4 gets per round against a budget of 30 units. With the segment
    /// empty every operation costs one unit, serving costs 5, the put leaves one record in the
    /// segment, and the 25 leftover units compact it in the same tick, so the segment is empty at
    /// the start of every round and the store spends 6 of its 30 units (20% utilization).
    ///
    /// The trigger offers 1 put and 40 gets per round for 60 rounds. In the first trigger round
    /// the store serves 30 of the 41 operations and has no leftover, so the put's record stays in
    /// the segment; from then on every get costs at least 2 units and the store serves about 10
    /// to 16 operations per round against 41 arriving, so the backlog grows by about 25 to 30 per
    /// round and reaches roughly 1800 by round 160. Only a put that reaches the head of the FIFO
    /// lengthens the segment, and there is one put per 41 queued operations, so the segment grows
    /// by about one record every four rounds during the trigger and stands near 15 at its end.
    ///
    /// After the trigger the offered load is back to 5 per round, but a get now costs about 16
    /// units, so a round serves 2 operations against 5 arriving and the backlog keeps growing.
    /// Every put served adds a record, so the cost keeps rising: near round 600 the segment should
    /// be around 47, a get should cost about 48 units, and a round should serve one operation.
    /// Compaction never runs again because serving never leaves a unit over. In rounds 600 to
    /// 800 the expectation is therefore about 200 served operations against a baseline of 1000,
    /// a backlog growing by about 4 per round, and a non-decreasing segment.
    ///
    /// With `compaction_reserve = 8`, the reserve compacts the previous round's put before
    /// serving, so a get always costs one unit. During the trigger the store serves 29 of the 41
    /// operations per round and the backlog grows by 12 per round to about 720; after the trigger
    /// it drains at 24 per round and is empty by round 190. The tail is baseline.
    const WORKLOAD: Workload = Workload {
        puts_per_round: 1,
        baseline_gets_per_round: 4,
        trigger_gets_per_round: 40,
        trigger_start: 100,
        trigger_end: 160,
    };
    const BACKGROUND: StoreConfig = StoreConfig {
        budget_per_tick: 30,
        compaction_reserve: 0,
    };
    const RESERVED: StoreConfig = StoreConfig {
        budget_per_tick: 30,
        compaction_reserve: 8,
    };

    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 50, 99, 100, 101, 130, 159, 160, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: ticks={} served_puts={} served_gets={} serve_units={} compact_units={} backlog={} segment={}",
                    r.ticks, r.served_puts, r.served_gets, r.serve_units, r.compact_units, r.backlog, r.segment
                );
            }
        }
    }

    fn assert_healthy(trace: &[Round], from: usize, to: usize) {
        for (i, r) in trace[from..to].iter().enumerate() {
            let i = i + from;
            assert_eq!(r.ticks, 1, "round {i}: the store should run one tick per round");
            assert_eq!(r.backlog, 0, "round {i}: backlog should be empty");
            assert_eq!(r.segment, 0, "round {i}: segment should be compacted");
            assert_eq!((r.served_puts, r.served_gets), (1, 4), "round {i}: every operation served");
            assert_eq!(r.serve_units, 5, "round {i}: every operation costs one unit");
            assert_eq!(r.compact_units, 1, "round {i}: the put is compacted in the same round");
        }
    }

    #[test]
    fn background_compaction_starves_and_reads_collapse() {
        let trace = run(WORKLOAD, BACKGROUND, ROUNDS);
        print_trajectory(&trace);

        assert_healthy(&trace, 0, 100);

        let tail = &trace[TAIL_START..];
        let tail_puts = sum(&trace, TAIL_START, ROUNDS, |r| r.served_puts);
        let tail_gets = sum(&trace, TAIL_START, ROUNDS, |r| r.served_gets);
        let tail_serve = sum(&trace, TAIL_START, ROUNDS, |r| r.serve_units);
        let tail_compact = sum(&trace, TAIL_START, ROUNDS, |r| r.compact_units);
        let baseline_served = 5 * (ROUNDS - TAIL_START) as u64;
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): served puts={tail_puts} gets={tail_gets} (baseline would be {baseline_served}), units serving={tail_serve} compacting={tail_compact}, backlog {} -> {}, segment {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog,
            tail.first().unwrap().segment,
            tail.last().unwrap().segment,
        );
        assert!(
            tail_puts + tail_gets < baseline_served / 4,
            "throughput should have collapsed: served {} vs baseline {baseline_served}",
            tail_puts + tail_gets
        );
        assert!(
            tail_serve > 5 * (ROUNDS - TAIL_START) as u64 * 2,
            "the store should spend far more than baseline units serving far fewer operations (spent {tail_serve})"
        );
        assert_eq!(tail_compact, 0, "compaction should never get a unit in the tail");
        assert!(
            tail.last().unwrap().backlog > tail.first().unwrap().backlog,
            "the backlog should still be growing at the end of the run"
        );
        assert!(tail.windows(2).all(|w| w[1].backlog >= w[0].backlog), "backlog never shrinks in the tail");
        assert!(tail.windows(2).all(|w| w[1].segment >= w[0].segment), "segment never shrinks in the tail");
    }

    /// Control: background compaction, no trigger. The put is compacted every round.
    #[test]
    fn without_a_trigger_the_store_stays_healthy() {
        let trace = run(
            Workload {
                trigger_gets_per_round: WORKLOAD.baseline_gets_per_round,
                ..WORKLOAD
            },
            BACKGROUND,
            ROUNDS,
        );
        print_trajectory(&trace);
        assert_healthy(&trace, 0, ROUNDS);
    }

    /// Control: same trigger, compaction reserved. The backlog builds and drains; the segment
    /// never exceeds the reserve at the end of a round. This shows the reserve bounds the damage;
    /// it does not make the mechanism go away, which `held_arrivals_cost_extra_units_growing_with_the_hold`
    /// measures.
    #[test]
    fn with_a_compaction_reserve_the_store_recovers() {
        let trace = run(WORKLOAD, RESERVED, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 0, 100);
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        let peak_round = trace.iter().position(|r| r.backlog == peak).unwrap();
        let empty_from = trace[160..].iter().position(|r| r.backlog == 0).map(|i| i + 160);
        let max_segment = trace.iter().map(|r| r.segment).max().unwrap();
        println!("peak backlog {peak} at round {peak_round}; empty again from round {empty_from:?}; largest end-of-round segment {max_segment}");
        assert!(peak > 500, "the trigger should have built a backlog, got {peak}");
        assert!(
            max_segment <= RESERVED.compaction_reserve,
            "the reserve compacts everything the previous round appended, so the segment never exceeds the reserve (got {max_segment})"
        );
        assert_eq!(sum(&trace, 0, ROUNDS, |r| r.served_gets), sum(&trace, 0, ROUNDS, |r| r.serve_units) - sum(&trace, 0, ROUNDS, |r| r.served_puts), "every get should cost exactly one unit");
        assert_healthy(&trace, TAIL_START, ROUNDS);
    }

    /// Runs a trigger-free workload of `HOLD_WORKLOAD`, but withholds the operations of rounds
    /// `[HOLD_AT, HOLD_AT + hold)` and delivers them all in round `HOLD_AT + hold`, which is what
    /// the operations' `use::batch` does when it holds arrivals for `hold` ticks. The clock is
    /// never held. Returns the units the store spent over the whole run beyond what the distinct
    /// operations require, which is one unit per operation plus one compaction unit per put.
    fn extra_units_with_hold(config: StoreConfig, hold: usize, rounds: usize) -> u64 {
        const HOLD_AT: usize = 100;
        let workload = HOLD_WORKLOAD;
        let mut flow = FlowBuilder::new();
        let store = flow.process::<Store>();
        let (op_send, ops) = store.sim_input::<Op, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = store.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = log_with_compaction(&store, ops, clock, config);
        let served = outputs.served.sim_output();
        let ticks = outputs.ticks.sim_output();

        let mut spent = 0u64;
        let mut required = 0u64;
        let (spent_ref, required_ref) = (&mut spent, &mut required);
        flow.sim().run_prompt(async move || {
            for round in 0..rounds {
                clock_send.send(());
                let held = round >= HOLD_AT && round < HOLD_AT + hold;
                let rounds_to_send = if held {
                    0
                } else if round == HOLD_AT + hold {
                    hold + 1
                } else {
                    1
                };
                for _ in 0..rounds_to_send {
                    for _ in 0..workload.puts_per_round {
                        op_send.send(Op::Put);
                        *required_ref += 2;
                    }
                    for _ in 0..workload.baseline_gets_per_round {
                        op_send.send(Op::Get);
                        *required_ref += 1;
                    }
                }
                quiesce().await;
                while served.try_next().await.is_some() {}
                while let Some(t) = ticks.try_next().await {
                    *spent_ref += t.serve_units + t.compact_units;
                }
            }
        });
        assert!(spent >= required, "the store cannot do less than the required work");
        spent - required
    }

    /// Hazard evidence for the reserved configuration. Holding the operations edge makes
    /// puts and gets that would have been served in separate ticks share a burst, so a tick can
    /// serve more puts than the reserve compacts at the start of the next tick, and the gets
    /// served in that next tick pay for the remainder. The reserve bounds each get's surcharge
    /// and removes it entirely when a tick never serves more than `compaction_reserve` puts; the
    /// corpus mix of one put per four gets never does, so this measurement uses three puts per
    /// two gets. The number of gets that pay grows with the hold, so the extra work grows with
    /// the hold and is zero without it. Expected before measuring: zero extra units at hold 0 for
    /// both configurations, a strictly increasing extra for holds of 4, 8, 16 and 32 rounds, and a
    /// smaller extra with the reserve than without it at every hold.
    const HOLD_WORKLOAD: Workload = Workload {
        puts_per_round: 3,
        baseline_gets_per_round: 2,
        trigger_gets_per_round: 2,
        trigger_start: 0,
        trigger_end: 0,
    };

    #[test]
    fn held_arrivals_cost_extra_units_growing_with_the_hold() {
        const RUN: usize = 300;
        let holds = [0usize, 4, 8, 16, 32];
        let reserved: Vec<u64> = holds.iter().map(|&h| extra_units_with_hold(RESERVED, h, RUN)).collect();
        let background: Vec<u64> = holds.iter().map(|&h| extra_units_with_hold(BACKGROUND, h, RUN)).collect();
        for ((h, r), b) in holds.iter().zip(&reserved).zip(&background) {
            println!("hold {h} rounds -> extra units: reserve 8 = {r}, reserve 0 = {b}");
        }
        assert_eq!(reserved[0], 0, "with nothing held the reserved store never scans an uncompacted record");
        assert_eq!(background[0], 0, "with nothing held the background store never scans an uncompacted record");
        assert!(reserved.windows(2).all(|w| w[1] > w[0]), "extra work should grow with the hold: {reserved:?}");
        assert!(reserved[1..].iter().zip(&background[1..]).all(|(r, b)| r < b), "the reserve should bound the extra work: {reserved:?} vs {background:?}");
        // Without the reserve, a hold of eight rounds (45 operations in one batch, 27 of them puts)
        // is enough on its own to tip the store into the permanent collapse of the trigger run, so
        // the extra work over a fixed 300-round window then grows with the rounds remaining after
        // the release rather than with the hold. The reserved store's extra work stays within a
        // few hundred units.
        assert!(background[2] > 100 * reserved[2], "a hold of eight rounds should collapse the background store: {background:?}");
    }
}
