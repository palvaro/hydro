//! Productive recursive work: semi-naive transitive closure with an explicit
//! novelty frontier.
//!
//! This is deliberately application-aware ground truth for metastability
//! experiments. It demonstrates that a feedback cycle may generate many
//! intermediate tuples (including duplicates) yet still drain after logical
//! novelty is exhausted.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// One logical iteration of the transitive-closure computation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TcTrace {
    /// Zero for graph admission; incremented by each requested recursive step.
    pub iteration: usize,
    /// Number of input edges admitted in this iteration.
    pub input_edges: usize,
    /// Join results produced before duplicate and known-fact suppression.
    pub candidates_generated: usize,
    /// Candidate tuples rejected because they were duplicates or already known.
    pub candidates_rejected: usize,
    /// Facts crossing the feedback boundary for the next iteration.
    pub novel_facts: usize,
    /// Number of reachability facts retained after this iteration.
    pub known_facts: usize,
    /// Whether another recursive step can perform useful join work.
    pub feedback_enabled: bool,
}

/// Computes transitive closure one semi-naive iteration per `steps` element.
///
/// Each element of `graphs` atomically admits one finite edge set. Keeping the
/// graph in one message makes the iteration boundary reproducible; after
/// admission, edges are ordinary Hydro tuples. `steps` is an experimental
/// logical clock, not a special TC evaluator. The recursive body is expressed
/// with Hydro state, a join, and an anti-join against established facts.
///
/// The returned fact stream emits each reachable pair once. The trace stream is
/// the semantic oracle described by the metastability ground-truth design doc.
pub fn productive_transitive_closure<'a>(
    graphs: Stream<Vec<(u32, u32)>, Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
    steps: Stream<(), Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
) -> (
    Stream<(u32, u32), Process<'a>, Unbounded, NoOrder>,
    Stream<TcTrace, Process<'a>, Unbounded, TotalOrder>,
) {
    transitive_closure_impl(graphs, steps, true)
}

/// [`productive_transitive_closure`] with the known-fact gate removed: candidates are no longer
/// anti-joined against established facts, so the frontier is simply "everything derived this
/// step". On an acyclic graph every path still has a unique length and the computation drains;
/// on a graph with a cycle the frontier never empties. This exists as a paired mutation for the
/// feedback evidence matrix; it is not a useful TC implementation.
pub fn ungated_transitive_closure<'a>(
    graphs: Stream<Vec<(u32, u32)>, Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
    steps: Stream<(), Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
) -> (
    Stream<(u32, u32), Process<'a>, Unbounded, NoOrder>,
    Stream<TcTrace, Process<'a>, Unbounded, TotalOrder>,
) {
    transitive_closure_impl(graphs, steps, false)
}

fn transitive_closure_impl<'a>(
    graphs: Stream<Vec<(u32, u32)>, Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
    steps: Stream<(), Process<'a>, Unbounded, TotalOrder, ExactlyOnce>,
    gate_known: bool,
) -> (
    Stream<(u32, u32), Process<'a>, Unbounded, NoOrder>,
    Stream<TcTrace, Process<'a>, Unbounded, TotalOrder>,
) {
    sliced! {
        let admitted_graphs = use::batch(
            graphs,
            nondet!(/** the harness chooses when a complete finite graph is admitted */),
        );
        let step_batch = use::batch(
            steps,
            nondet!(/** each element requests one logical TC iteration */),
        );
        let mut edges = use::state_null::<Stream<(u32, u32), Tick<_>, Bounded, NoOrder>>();
        let mut known = use::state_null::<Stream<((u32, u32), ()), Tick<_>, Bounded, NoOrder>>();
        let mut frontier = use::state_null::<Stream<(u32, u32), Tick<_>, Bounded, NoOrder>>();
        let mut iteration = use::state(|l| l.singleton(q!(0usize)));

        let admitted = admitted_graphs
            .flatten_ordered()
            .weaken_ordering::<NoOrder>()
            .unique();
        let all_edges = edges.chain(admitted.clone()).unique();

        // A step token gates the join. When frontier is empty, the join has no
        // input and therefore no recursive work remains enabled.
        let step_count = step_batch.count();
        let gated_frontier = frontier
            .clone()
            .cross_singleton(step_count.clone())
            .filter(q!(|(_, n)| *n > 0))
            .map(q!(|(fact, _)| fact));
        let candidates = gated_frontier
            .map(q!(|(src, via)| (via, src)))
            .join(all_edges.clone().map(q!(|(via, dst)| (via, dst))))
            .map(q!(|(_via, (src, dst))| (src, dst)));

        let candidate_count = candidates.clone().count();
        let candidate_unique = candidates
            .map(q!(|fact| (fact, ())))
            .unique();
        let candidate_novel = if gate_known {
            candidate_unique.anti_join(known.clone().map(q!(|(fact, ())| fact)))
        } else {
            candidate_unique
        };
        let candidate_novel_count = candidate_novel.clone().count();

        // Newly admitted edges seed the first frontier. The same anti-join also
        // permits later graph admission without re-emitting established facts.
        let seed_novel = if gate_known {
            admitted
                .clone()
                .map(q!(|fact| (fact, ())))
                .anti_join(known.clone().map(q!(|(fact, ())| fact)))
        } else {
            admitted.clone().map(q!(|fact| (fact, ())))
        };
        let novel = seed_novel.chain(candidate_novel).unique();
        let novel_count = novel.clone().count();
        let next_known = known.chain(novel.clone()).unique();
        let known_count = next_known.clone().count();

        let next_iteration = iteration
            .clone()
            .zip(step_count.clone())
            .map(q!(|(old, steps)| old + steps));
        let input_count = admitted.count();
        let should_trace = input_count
            .clone()
            .zip(step_count)
            .map(q!(|(input, steps)| input > 0 || steps > 0));
        let trace = next_iteration
            .clone()
            .zip(input_count)
            .zip(candidate_count)
            .zip(candidate_novel_count)
            .zip(novel_count.clone())
            .zip(known_count)
            .zip(should_trace)
            .filter_map(q!(|((((((iteration, input_edges), candidates_generated), candidate_novel), novel_facts), known_facts), emit)| {
                emit.then_some(TcTrace {
                    iteration,
                    input_edges,
                    candidates_generated,
                    candidates_rejected: candidates_generated - candidate_novel,
                    novel_facts,
                    known_facts,
                    feedback_enabled: novel_facts > 0,
                })
            }));

        edges = all_edges;
        known = next_known;
        frontier = novel.clone().map(q!(|(fact, ())| fact));
        iteration = next_iteration;

        (novel.map(q!(|(fact, ())| fact)), trace.into_stream())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn reference_closure(edges: &[(u32, u32)]) -> BTreeSet<(u32, u32)> {
        let mut known: BTreeSet<_> = edges.iter().copied().collect();
        loop {
            let next: Vec<_> = known
                .iter()
                .flat_map(|(src, via)| {
                    edges
                        .iter()
                        .filter(move |(from, _)| from == via)
                        .map(move |(_, dst)| (*src, *dst))
                })
                .collect();
            let old_len = known.len();
            known.extend(next);
            if known.len() == old_len {
                return known;
            }
        }
    }

    fn check_graph(edges: Vec<(u32, u32)>, expect_redundancy: bool) {
        let expected = reference_closure(&edges);
        let max_steps = expected.len() + 1;
        let expected_for_run = expected.clone();

        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let fact_recv = facts
            .assume_ordering::<TotalOrder>(nondet!(/** tests compare facts as a set */))
            .sim_output();
        let trace_recv = traces.sim_output();

        flow.sim().exhaustive(async move || {
            graph_send.send(edges.clone());
            let admitted = trace_recv.next().await;
            assert_eq!(admitted.input_edges, edges.len());
            assert_eq!(admitted.novel_facts, BTreeSet::from_iter(edges.iter().copied()).len());

            let mut saw_redundancy = false;
            let mut drained = false;
            for _ in 0..max_steps {
                step_send.send(());
                let trace = trace_recv.next().await;
                saw_redundancy |= trace.candidates_rejected > 0;
                if !trace.feedback_enabled {
                    assert_eq!(trace.novel_facts, 0);
                    drained = true;
                    break;
                }
            }
            assert!(drained, "finite TC must reach an empty novelty frontier");
            assert!(!expect_redundancy || saw_redundancy);

            let mut actual = BTreeSet::new();
            for _ in 0..expected_for_run.len() {
                actual.insert(fact_recv.next().await);
            }
            assert_eq!(actual, expected_for_run);
            fact_recv.assert_no_more().await;
        });
    }

    #[test]
    fn chain_is_productive_and_drains() {
        check_graph(vec![(0, 1), (1, 2), (2, 3), (3, 4)], false);
    }

    #[test]
    fn layered_diamond_has_finite_redundant_work() {
        check_graph(
            vec![(0, 1), (0, 2), (1, 3), (2, 3), (3, 4), (3, 5)],
            true,
        );
    }

    #[test]
    fn cyclic_graph_still_drains() {
        check_graph(vec![(0, 1), (1, 2), (2, 0)], true);
    }
}
