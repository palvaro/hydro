//! Load harness for the semi-naive transitive closure of [`crate::local::productive_tc`], a benign
//! negative control with recursion and duplicates.
//!
//! The program is unchanged: each element of `graphs` admits one finite edge set, each element
//! of `steps` performs one semi-naive iteration (join the novelty frontier with all edges, drop
//! candidates that are duplicates or already known, and make the survivors the next frontier),
//! and the trace stream reports per iteration how many candidates were generated, how many were
//! rejected, and how many facts were novel. The frontier empties when there is nothing new to
//! derive, after which a step costs nothing.
//!
//! # Mechanism and knob
//!
//! There is no mechanism to switch off. The recursion generates intermediate tuples, including
//! duplicates when a graph has several paths between the same pair, but every candidate is a
//! path in the admitted graph, so the work to reach the closure is fixed by the graph. The
//! harness confirms the negative: a burst of graphs with many redundant paths raises the work per
//! round while their closures are being computed and by nothing afterwards; total candidates
//! over the run equal the sum over graphs of the paths of length two or more in each graph; and
//! the same totals hold under schedule exploration, since batching several step tokens into one
//! tick performs one iteration and leaves the rest for later ticks.
//!
//! Graphs admitted in different rounds use disjoint node ranges. The program joins the frontier
//! with all edges, so a late edge extends only paths that start from it; disjoint ranges keep the
//! expected closure the sum of the per-graph closures.
//!
//! # Timer parameters
//!
//! - `steps`: one element per semi-naive iteration. A deployment wires
//!   `process.source_interval(period)` into it; the simulation feeds it from `sim_input`.
//!
//! # Measured (see `sim_tests`)
//!
//! Baseline is a chain of 4 edges (closure 10, 6 join candidates) every 5 rounds; the trigger
//! admits a layered graph of 4 layers of 3 nodes (27 edges, closure 54, 81 join candidates of
//! which 54 are duplicates) in every round from 100 to 160. One step per round, 800 rounds.
//!
//! | run | trigger | candidates per round before / during / after | whole run: edges, candidates (rejected), facts | tail rounds 600 to 800 | label |
//! |---|---|---|---|---|---|
//! | layered burst | yes | 0,3,2,1,0 per chain / 81 / 30, 2, 1, 0 then 0,3,2,1,0 per chain | 2212, 5748 (3240), 4720 | 240 candidates, the baseline pattern | benign |
//! | no trigger | no | 0,3,2,1,0 per chain throughout, 0 rejected | | | benign |
//! | 256 fuzzed schedules, 12 rounds plus 6 to drain | yes | at most the 33 candidates the graphs' paths allow | | | benign |
//!
//! Every number in the main run matched the hand computation: 81 candidates in each round from
//! 102 to 160, 30 in round 161, and 5748 over the run, which is exactly the sum over admitted
//! graphs of the paths a semi-naive evaluation joins, with 4720 facts, which is exactly the sum
//! of the closures. The work rises with the trigger and stops three rounds after it, because a
//! graph with no more novel facts costs nothing on later steps.
//!
//! One finding about the unmodified program: under schedule exploration 212 of 256 schedules
//! produced fewer candidates than the closure requires, and none more. The program replaces its
//! frontier with each tick's novel facts, so a tick that admits a graph without a step token
//! discards the frontier the previous step left, and the closure comes out incomplete. This is a
//! shortfall of work, not an excess, so it does not affect the label, but it means the program's
//! completeness depends on the schedule.

/// The shapes of graph the harness admits.
#[derive(Clone, Copy, Debug)]
pub enum Shape {
    /// `edges` edges in a line: closure `edges (edges + 1) / 2`, every pair reachable by one path.
    Chain { edges: u32 },
    /// `layers` layers of `width` nodes with every edge between consecutive layers: many pairs
    /// reachable by several paths, so many duplicate candidates.
    Layered { layers: u32, width: u32 },
}

impl Shape {
    /// The edges of the graph, with node ids offset by `base`.
    pub fn edges(&self, base: u32) -> Vec<(u32, u32)> {
        match *self {
            Shape::Chain { edges } => (0..edges).map(|i| (base + i, base + i + 1)).collect(),
            Shape::Layered { layers, width } => {
                let mut out = Vec::new();
                for layer in 0..layers - 1 {
                    for a in 0..width {
                        for b in 0..width {
                            out.push((base + layer * width + a, base + (layer + 1) * width + b));
                        }
                    }
                }
                out
            }
        }
    }

    /// Size of the transitive closure.
    pub fn closure_size(&self) -> u64 {
        match *self {
            Shape::Chain { edges } => edges as u64 * (edges as u64 + 1) / 2,
            Shape::Layered { layers, width } => {
                let l = layers as u64;
                (width as u64 * width as u64) * l * (l - 1) / 2
            }
        }
    }

    /// Number of join candidates a semi-naive evaluation generates before duplicate suppression.
    /// Each fact of the closure is joined with the edges exactly once, in the iteration after it
    /// became novel, so this is the sum over closure facts of the out-degree of their endpoint.
    pub fn join_candidates(&self) -> u64 {
        match *self {
            Shape::Chain { edges } => self.closure_size() - edges as u64,
            Shape::Layered { layers, width } => {
                // Facts ending in layer `j` number `j * width^2` and each extends `width` ways;
                // facts ending in the last layer extend nowhere.
                let (l, w) = (layers as u64, width as u64);
                w * w * w * (l - 2) * (l - 1) / 2
            }
        }
    }

    /// Iterations needed to reach the closure from admission.
    pub fn depth(&self) -> u64 {
        match *self {
            Shape::Chain { edges } => edges as u64,
            Shape::Layered { layers, .. } => layers as u64 - 1,
        }
    }
}

/// The harness's workload: which graph, if any, to admit in each round.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub baseline: Shape,
    /// A baseline graph is admitted every this many rounds outside the trigger.
    pub baseline_every: u64,
    pub trigger: Shape,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`; one trigger graph per round.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn graph_at(&self, round: u64) -> Option<Shape> {
        if round >= self.trigger_start && round < self.trigger_end {
            Some(self.trigger)
        } else if round % self.baseline_every == 0 {
            Some(self.baseline)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use super::{Shape, Workload};
    use crate::local::productive_tc::{TcTrace, productive_transitive_closure};

    #[derive(Debug, Clone, Default)]
    struct Round {
        edges_admitted: u64,
        candidates: u64,
        rejected: u64,
        novel: u64,
        /// Known facts after the round.
        known: u64,
    }

    fn run(workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let facts = facts
            .assume_ordering::<TotalOrder>(nondet!(/** the harness only counts facts */))
            .sim_output();
        let traces = traces.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            let mut known = 0u64;
            let mut graphs_admitted = 0u32;
            for round in 0..rounds as u64 {
                let mut r = Round::default();
                if let Some(shape) = workload.graph_at(round) {
                    let edges = shape.edges(graphs_admitted * 1000);
                    r.edges_admitted = edges.len() as u64;
                    graph_send.send(edges);
                    graphs_admitted += 1;
                }
                step_send.send(());
                quiesce().await;
                while let Some(t) = traces.try_next().await {
                    let t: TcTrace = t;
                    r.candidates += t.candidates_generated as u64;
                    r.rejected += t.candidates_rejected as u64;
                    r.novel += t.novel_facts as u64;
                    known = t.known_facts as u64;
                }
                while facts.try_next().await.is_some() {}
                r.known = known;
                trace_ref.push(r);
            }
        });
        trace
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// A step joins the frontier left by the previous tick, so a graph admitted in round `r` is
    /// first expanded in round `r + 1`.
    ///
    /// Baseline: a chain of 4 edges every 5 rounds. Its closure has 10 facts; semi-naive
    /// evaluation generates 3, 2 and 1 candidates in the three iterations after admission (the
    /// paths of length 2, 3 and 4), all novel, so 6 candidates and 6 derived facts per graph. The
    /// five rounds from an admission therefore generate 0, 3, 2, 1, 0 candidates.
    ///
    /// Trigger: rounds 100 to 160 each admit a layered graph of 4 layers of 3 nodes, 27 edges.
    /// Its closure has 54 facts. The first iteration joins the 27 edges with the edges: the 9
    /// edges into layer 1 extend 3 ways each and the rest extend nowhere, 54 candidates, of which
    /// 18 are novel (the 9 pairs from layer 0 to 2 and the 9 from layer 1 to 3) and 36 are
    /// duplicates. The second iteration joins those 18: the 9 pairs into layer 2 extend 3 ways,
    /// 27 candidates, 9 novel and 18 duplicates. The third joins 9 facts into layer 3 and finds
    /// nothing. So 81 candidates per graph, 27 novel and 54 rejected. With one graph admitted per
    /// round, each round from 102 to 160 performs the first iteration of one graph and the second
    /// of another: 81 candidates per round. Round 161 performs the last graph's second iteration
    /// and the first of the chain admitted in round 160, 30 candidates; rounds 162 to 164 finish
    /// that chain with 2, 1 and 0; and the baseline pattern resumes from round 165. Over the run,
    /// total candidates should be exactly the sum of `join_candidates` over the graphs admitted,
    /// total novel facts exactly the sum of closures, and the same totals should hold under
    /// schedule exploration.
    const WORKLOAD: Workload = Workload {
        baseline: Shape::Chain { edges: 4 },
        baseline_every: 5,
        trigger: Shape::Layered { layers: 4, width: 3 },
        trigger_start: 100,
        trigger_end: 160,
    };
    const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn expected_totals(workload: &Workload, rounds: usize) -> (u64, u64, u64) {
        let mut edges = 0;
        let mut candidates = 0;
        let mut closure = 0;
        for round in 0..rounds as u64 {
            if let Some(shape) = workload.graph_at(round) {
                edges += shape.edges(0).len() as u64;
                candidates += shape.join_candidates();
                closure += shape.closure_size();
            }
        }
        (edges, candidates, closure)
    }

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 1, 2, 3, 4, 5, 99, 100, 101, 102, 103, 130, 159, 160, 161, 162, 163, 200, 600, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: edges={} candidates={} rejected={} novel={} known={}",
                    r.edges_admitted, r.candidates, r.rejected, r.novel, r.known
                );
            }
        }
    }

    #[test]
    fn work_is_the_closure_and_stops_when_novelty_runs_out() {
        let trace = run(WORKLOAD, ROUNDS);
        print_trajectory(&trace);
        let (edges, expected_candidates, expected_closure) = expected_totals(&WORKLOAD, ROUNDS);
        let candidates: u64 = trace.iter().map(|r| r.candidates).sum();
        let novel: u64 = trace.iter().map(|r| r.novel).sum();
        let rejected: u64 = trace.iter().map(|r| r.rejected).sum();
        println!(
            "whole run: {edges} edges admitted, {candidates} candidates ({rejected} rejected), {novel} novel facts, known {}; trigger rounds 100..160 generated {} candidates, tail rounds {TAIL_START}..{ROUNDS} generated {}",
            trace.last().unwrap().known,
            trace[100..160].iter().map(|r| r.candidates).sum::<u64>(),
            trace[TAIL_START..].iter().map(|r| r.candidates).sum::<u64>()
        );
        assert_eq!(candidates, expected_candidates, "every candidate is a path of length two or more in an admitted graph");
        assert_eq!(novel, expected_closure, "the novel facts are exactly the closure");
        assert_eq!(trace.last().unwrap().known, expected_closure);
        // The baseline pattern: 0, 3, 2, 1, 0 candidates in the five rounds from a chain's admission.
        for start in (0..100).step_by(5) {
            let pattern: Vec<u64> = trace[start..start + 5].iter().map(|r| r.candidates).collect();
            assert_eq!(pattern, vec![0, 3, 2, 1, 0], "rounds {start}..{}", start + 5);
        }
        // Two layered graphs expanded per round during the trigger, 81 candidates per round.
        assert!(trace[102..=160].iter().all(|r| r.candidates == 81), "{:?}", trace[102..=160].iter().map(|r| r.candidates).collect::<Vec<_>>());
        // Nothing lingers: the last trigger graph finishes in round 161 alongside the next chain.
        assert_eq!(trace[161].candidates, 27 + 3);
        assert_eq!(trace[162].candidates, 2);
        assert_eq!(trace[163].candidates, 1);
        assert_eq!(trace[164].candidates, 0);
        for start in (165..ROUNDS - 5).step_by(5) {
            let pattern: Vec<u64> = trace[start..start + 5].iter().map(|r| r.candidates).collect();
            assert_eq!(pattern, vec![0, 3, 2, 1, 0], "rounds {start}..{}", start + 5);
        }
    }

    /// Control: no trigger. The baseline pattern throughout.
    #[test]
    fn without_a_trigger_the_pattern_never_changes() {
        let trace = run(Workload { trigger: WORKLOAD.baseline, trigger_start: 0, trigger_end: 0, ..WORKLOAD }, ROUNDS);
        print_trajectory(&trace);
        for start in (0..ROUNDS - 5).step_by(5) {
            let pattern: Vec<u64> = trace[start..start + 5].iter().map(|r| r.candidates).collect();
            assert_eq!(pattern, vec![0, 3, 2, 1, 0], "rounds {start}..{}", start + 5);
        }
        assert_eq!(trace.iter().map(|r| r.rejected).sum::<u64>(), 0, "a chain has no duplicate paths");
    }

    /// Under schedule exploration the work never exceeds the closure's join candidates: however
    /// graphs and steps are batched, every candidate is still a path. The run ends with step-only
    /// rounds so that a graph whose first iteration slipped a round can drain.
    ///
    /// The totals are not always equal to the prompt run's, and the difference is a shortfall,
    /// never an excess. The program replaces its frontier with each tick's novel facts, so when a
    /// schedule delivers a graph into a tick that holds no step token the frontier left by the
    /// previous step is discarded and its facts are never joined; the closure then comes out
    /// incomplete by the paths through them. Under the prompt schedule a round's graph and step
    /// always share a tick and this never happens. The test asserts the upper bound, which is
    /// what the label is about, and reports how many of the schedules fell short.
    #[test]
    fn the_totals_hold_across_schedules() {
        const ROUNDS: usize = 12;
        const DRAIN: usize = 6;
        let workload = Workload {
            baseline: Shape::Chain { edges: 3 },
            baseline_every: 4,
            trigger: Shape::Layered { layers: 3, width: 2 },
            trigger_start: 1,
            trigger_end: 4,
        };
        let (_, expected_candidates, expected_closure) = expected_totals(&workload, ROUNDS);
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let facts = facts
            .assume_ordering::<TotalOrder>(nondet!(/** the harness only counts facts */))
            .sim_output();
        let traces = traces.sim_output();
        let short = std::sync::atomic::AtomicU32::new(0);
        let runs = std::sync::atomic::AtomicU32::new(0);
        let (short_ref, runs_ref) = (&short, &runs);
        flow.sim().unit_test_fuzz_iterations(256).fuzz(async || {
            let mut candidates = 0u64;
            let mut novel = 0u64;
            let mut graphs_admitted = 0u32;
            for round in 0..(ROUNDS + DRAIN) as u64 {
                if round < ROUNDS as u64 {
                    if let Some(shape) = workload.graph_at(round) {
                        graph_send.send(shape.edges(graphs_admitted * 1000));
                        graphs_admitted += 1;
                    }
                }
                step_send.send(());
                quiesce().await;
                while let Some(t) = traces.try_next().await {
                    let t: TcTrace = t;
                    candidates += t.candidates_generated as u64;
                    novel += t.novel_facts as u64;
                }
                while facts.try_next().await.is_some() {}
            }
            assert!(candidates <= expected_candidates, "more candidates than paths: {candidates} > {expected_candidates}");
            assert!(novel <= expected_closure, "more novel facts than the closure: {novel} > {expected_closure}");
            if candidates < expected_candidates {
                short_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            runs_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        println!(
            "{} of {} fuzzed schedules fell short of the closure's {expected_candidates} candidates; none exceeded it",
            short.load(std::sync::atomic::Ordering::Relaxed),
            runs.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
}
