//! Molly-style why-provenance graphs of LDFI derivations (`hydro_lang::sim::viz::provenance`)
//! on two-phase commit: the graph recorded beside each derivation agrees with it at every
//! lineage level, recording does not change the search, and the renderings are stable.

use std::collections::BTreeSet;

use hydro_lang::sim::ldfi::{Level, Outcome};
use hydro_lang::sim::viz::provenance::{self, ProvMessage, ProvNodeKind, ProvOptions};

use super::two_pc_ldfi::harness::{body, compile};

/// The blocking 2PC search (two participants, one coordinator crash: a counterexample on the
/// second hypothesis) at `level`, with and without provenance graphs.
fn search_both_ways(level: Level) -> (Outcome, Outcome) {
    let (compiled, h, configure) = compile(2, false, 8);
    let with = configure(1)
        .level(level)
        .with_provenance_graphs()
        .search(&compiled, async |run| body(&h, run).await);
    let without = configure(1)
        .level(level)
        .search(&compiled, async |run| body(&h, run).await);
    (with, without)
}

/// Every schedule of `outcome` has one graph per derivation whose fault points are exactly
/// the derivation; returns how many graphs were checked.
fn graphs_agree_with_derivations(outcome: &Outcome) -> usize {
    let mut checked = 0;
    for hypothesis in outcome.runs() {
        for schedule in &hypothesis.schedules {
            assert_eq!(
                schedule.provenance.len(),
                schedule.derivations.len(),
                "one graph per derivation"
            );
            for (derivation, graph) in schedule.derivations.iter().zip(&schedule.provenance) {
                assert!(!graph.is_empty());
                assert_eq!(graph.roots.len(), 1, "one goal");
                assert!(matches!(
                    graph.nodes[graph.roots[0]].kind,
                    ProvNodeKind::Goal { .. }
                ));
                let expected: BTreeSet<ProvMessage> =
                    derivation.iter().map(ProvMessage::from).collect();
                assert_eq!(
                    graph.dependencies(),
                    expected,
                    "the graph's fault points are the derivation"
                );
                // Every node but the goal is reachable from it.
                let mut reached = vec![false; graph.node_count()];
                let mut stack = graph.roots.clone();
                while let Some(i) = stack.pop() {
                    if std::mem::replace(&mut reached[i], true) {
                        continue;
                    }
                    stack.extend(graph.out_edges(i).map(|e| e.to));
                }
                assert!(reached.iter().all(|r| *r), "every node hangs from the goal");
                checked += 1;
            }
        }
    }
    checked
}

#[test]
fn provenance_graphs_agree_with_derivations_at_every_level() {
    for level in [Level::Slice, Level::Record, Level::Counterfactual] {
        let (with, without) = search_both_ways(level);
        assert!(
            with.is_counterexample(),
            "{level:?}: expected a counterexample, got {with}"
        );
        assert_eq!(
            with.to_string(),
            without.to_string(),
            "{level:?}: recording graphs must not change the search"
        );
        for hypothesis in without.runs() {
            for schedule in &hypothesis.schedules {
                assert!(schedule.provenance.is_empty(), "no graphs unless asked");
            }
        }
        let checked = graphs_agree_with_derivations(&with);
        assert!(
            checked > 0,
            "{level:?}: the failure-free run has a derivation"
        );
        eprintln!("{level:?}: {checked} graph(s) agree with their derivations");
    }
}

/// The failure-free run's graph under record-level lineage: `POST` → the observer's output →
/// the commits it was computed from → the votes → the prepares → the transaction (a local
/// input), with every message a fault point of the coordinator's crash.
#[test]
fn provenance_graph_of_two_pc_renders() {
    let (with, _) = search_both_ways(Level::Record);
    let failure_free = &with.runs()[0].schedules[0];
    assert!(failure_free.goal_held);
    let graph = &failure_free.provenance[0];
    let kinds = |graph: &provenance::ProvGraph| {
        let mut counts = std::collections::BTreeMap::new();
        for node in &graph.nodes {
            let kind = match node.kind {
                ProvNodeKind::Goal { .. } => "goal",
                ProvNodeKind::Output { .. } => "output",
                ProvNodeKind::Exec { .. } => "exec",
                ProvNodeKind::Message { .. } => "message",
                ProvNodeKind::Alive { .. } => "alive",
                ProvNodeKind::Base { .. } => "base",
                ProvNodeKind::Opaque { .. } => "opaque",
            };
            *counts.entry(kind).or_insert(0usize) += 1;
        }
        counts
    };
    eprintln!("record-level graph: {:?}", kinds(graph));
    assert!(
        kinds(graph)["message"] >= 4,
        "prepares, votes, commits: {:?}",
        kinds(graph)
    );

    let dot = provenance::dot(graph, &ProvOptions::default());
    hydro_build_utils::assert_snapshot!("two_pc_record_dot", dot);
    let mermaid = provenance::mermaid(
        graph,
        &ProvOptions {
            collapse_execs: true,
            show_levels: false,
            ..ProvOptions::default()
        },
    );
    hydro_build_utils::assert_snapshot!("two_pc_record_mermaid_collapsed", mermaid);
    let json: serde_json::Value = serde_json::from_str(&graph.to_json()).unwrap();
    assert_eq!(json["nodes"].as_array().unwrap().len(), graph.node_count());

    // Slice-level lineage walks the execution graph: the same goal, drawn through the tick
    // executions and async runs that carried it, grouped by node.
    let (with, _) = search_both_ways(Level::Slice);
    let graph = &with.runs()[0].schedules[0].provenance[0];
    eprintln!("slice-level graph: {:?}", kinds(graph));
    assert!(kinds(graph)["exec"] >= 3, "{:?}", kinds(graph));
    let dot = provenance::dot(
        graph,
        &ProvOptions {
            title: Some("2PC, failure-free run, slice-level lineage".to_owned()),
            ..ProvOptions::default()
        },
    );
    hydro_build_utils::assert_snapshot!("two_pc_slice_dot", dot);
}
