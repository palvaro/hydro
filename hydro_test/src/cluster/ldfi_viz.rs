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
    for level in [Level::Execution, Level::Record] {
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

    // Execution-level lineage walks the execution graph: the same goal, drawn through the
    // tick executions and async runs that carried it, grouped by node.
    let (with, _) = search_both_ways(Level::Execution);
    let graph = &with.runs()[0].schedules[0].provenance[0];
    eprintln!("execution-level graph: {:?}", kinds(graph));
    assert!(kinds(graph)["exec"] >= 3, "{:?}", kinds(graph));
    let dot = provenance::dot(
        graph,
        &ProvOptions {
            title: Some("2PC, failure-free run, execution-level lineage".to_owned()),
            ..ProvOptions::default()
        },
    );
    hydro_build_utils::assert_snapshot!("two_pc_execution_dot", dot);
}

/// Writes the whole Molly-style report of the blocking 2PC counterexample (both Lamport
/// diagrams and the witness's provenance graph, in Mermaid and DOT) into the directory named
/// by `MOLLY_VIZ_OUT`, for rendering with `dot -Tpng` / `mmdc`. A no-op unless that variable is
/// set, so it never writes into the tree during `cargo test`.
#[test]
fn render_molly_report_to_disk() {
    let Ok(out) = std::env::var("MOLLY_VIZ_OUT") else {
        return;
    };
    let out = std::path::PathBuf::from(out);
    std::fs::create_dir_all(&out).unwrap();
    let write = |name: &str, text: &str| std::fs::write(out.join(name), text).unwrap();

    let (compiled, h, configure) = compile(2, false, 8);
    let outcome = configure(1)
        .level(Level::Record)
        .with_traces()
        .with_provenance_graphs()
        .search(&compiled, async |run| body(&h, run).await);
    assert!(outcome.is_counterexample(), "{outcome}");
    let viz = outcome.viz().unwrap();

    // Zoom level 0: the failing run.
    write("failing.mmd", &viz.lamport_mermaid().unwrap());
    write("failing.dot", &viz.lamport_dot().unwrap());
    // Zoom level 1: a run in which the goal held, its support highlighted.
    write("witness.mmd", &viz.support_lamport_mermaid().unwrap());
    write("witness.dot", &viz.support_lamport_dot().unwrap());
    // Zoom level 2: the why-provenance graph behind that support.
    let witness = viz.witness().unwrap();
    let index = witness
        .schedule
        .derivations
        .iter()
        .position(|d| std::ptr::eq(d, witness.derivation))
        .unwrap();
    let graph = &witness.schedule.provenance[index];
    write(
        "provenance.dot",
        &provenance::dot(
            graph,
            &ProvOptions {
                title: Some("2PC: why the goal held (record-level lineage)".to_owned()),
                ..ProvOptions::default()
            },
        ),
    );
    write(
        "provenance_collapsed.mmd",
        &provenance::mermaid(
            graph,
            &ProvOptions {
                collapse_execs: true,
                show_levels: false,
                ..ProvOptions::default()
            },
        ),
    );
    write("provenance.json", &graph.to_json());
    write("report.md", &viz.markdown());
    eprintln!("wrote the Molly report to {}", out.display());
}

/// The directory a counterexample gallery entry goes to: `$MOLLY_VIZ_OUT/<name>/`, or `None`
/// when `MOLLY_VIZ_OUT` is unset (the gallery tests are then no-ops, so `cargo test` never
/// writes into the tree).
pub(crate) fn gallery_dir(name: &str) -> Option<std::path::PathBuf> {
    let out = std::env::var("MOLLY_VIZ_OUT").ok()?;
    let dir = std::path::PathBuf::from(out).join(name);
    std::fs::create_dir_all(&dir).unwrap();
    Some(dir)
}

/// Writes one gallery entry's pictures for a search run at `level` under
/// [`Ldfi::with_traces`](hydro_lang::sim::ldfi::Ldfi::with_traces) and
/// [`Ldfi::with_provenance_graphs`](hydro_lang::sim::ldfi::Ldfi::with_provenance_graphs) into
/// `dir`: `failing.dot` (zoom 0; the same whatever the level, written once), `witness_<level>.dot`
/// (zoom 1: the support at that level), `provenance_<level>.dot` (zoom 2: the why-provenance
/// graph behind that support, executions collapsed) and `report.md`. `protocol` names the
/// entry in the provenance graph's title.
pub(crate) fn write_gallery(
    dir: &std::path::Path,
    protocol: &str,
    level: Level,
    outcome: &Outcome,
) {
    let suffix = match level {
        Level::Execution => "execution",
        Level::Record => "record",
    };
    let write = |name: &str, text: &str| std::fs::write(dir.join(name), text).unwrap();
    let Some(viz) = outcome.viz() else {
        eprintln!("{protocol} at {level:?}: not a counterexample, nothing to draw: {outcome}");
        return;
    };
    if !dir.join("failing.dot").exists() {
        write("failing.dot", &viz.lamport_dot().unwrap());
        write("report.md", &viz.markdown());
    }
    let Some(witness) = viz.witness() else {
        eprintln!(
            "{protocol} at {level:?}: no witness (no run in which the goal held with a derivation)"
        );
        return;
    };
    write(
        &format!("witness_{suffix}.dot"),
        &viz.support_lamport_dot().unwrap(),
    );
    let index = witness
        .schedule
        .derivations
        .iter()
        .position(|d| std::ptr::eq(d, witness.derivation))
        .unwrap();
    if let Some(graph) = witness.schedule.provenance.get(index) {
        write(
            &format!("provenance_{suffix}.dot"),
            &provenance::dot(
                graph,
                &ProvOptions {
                    collapse_execs: true,
                    title: Some(format!(
                        "{protocol}: why the goal held ({suffix}-level lineage)"
                    )),
                    ..ProvOptions::default()
                },
            ),
        );
    }
    eprintln!(
        "wrote the {protocol} gallery entry at {level:?} to {}",
        dir.display()
    );
}

/// The 2PC gallery entry: blocking 2PC, two participants, one coordinator crash.
#[test]
fn gallery_two_pc() {
    let Some(dir) = gallery_dir("two_pc") else {
        return;
    };
    let (compiled, h, configure) = compile(2, false, 8);
    for level in [Level::Execution, Level::Record] {
        let outcome = configure(1)
            .level(level)
            .with_traces()
            .with_provenance_graphs()
            .search(&compiled, async |run| body(&h, run).await);
        write_gallery(&dir, "2PC", level, &outcome);
    }
}

/// The ISR gallery entry: unclean leader election loses an acknowledged write (one lost
/// replication, the leader's crash after its acknowledgement).
#[test]
fn gallery_isr_unclean_election() {
    use hydro_lang::sim::ldfi::Exploration;

    use super::isr_replication::Encoding;
    use super::isr_replication::harness::{
        DEFAULT_SHAPE, body as isr_body, compile as isr_compile,
    };

    let Some(dir) = gallery_dir("isr_unclean_election") else {
        return;
    };
    let (compiled, h, configure) = isr_compile(true, Encoding::Dataflow, DEFAULT_SHAPE, 8);
    for level in [Level::Execution, Level::Record] {
        let outcome = configure(1)
            .level(level)
            .exploration(Exploration::Fuzz { instances: 8 })
            .with_traces()
            .with_provenance_graphs()
            .search(&compiled, async |run| isr_body(&h, run).await);
        write_gallery(&dir, "ISR replication", level, &outcome);
    }
}

/// The Paxos gallery entry: a commit quorum one acknowledgement short lets a new leader
/// choose `y` at the slot where `x` was chosen (two proposers, three acceptors, one crash
/// allowed, four sampled schedules per hypothesis as in `paxos_ldfi`'s own test: with one,
/// whether the sampled schedule lets `PRE` hold is luck, and the search can run past a
/// few hundred hypotheses without a counterexample).
#[cfg(feature = "tokio")]
#[test]
fn gallery_paxos_commit_quorum() {
    use hydro_lang::sim::ldfi::Exploration;

    use super::paxos_ldfi::tests::{body as paxos_body, compile as paxos_compile};

    let Some(dir) = gallery_dir("paxos_commit_quorum") else {
        return;
    };
    let (compiled, h, configure) = paxos_compile(true);
    for level in [Level::Execution, Level::Record] {
        let outcome = configure(1)
            .level(level)
            .exploration(Exploration::Fuzz { instances: 4 })
            .max_hypotheses(256)
            .with_traces()
            .with_provenance_graphs()
            .search(&compiled, async |run| paxos_body(&h, run).await);
        write_gallery(&dir, "Paxos", level, &outcome);
    }
}
