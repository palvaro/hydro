//! End-to-end tests of the check on small inline programs.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{EcCheck, Results, VacuousReason, Verdict};
#[cfg(stageleft_runtime)]
use crate::search::{CrashAt, Fault, Tolerance};

#[cfg(stageleft_runtime)]
const MEMBERS: usize = 3;

/// A process broadcasts its inputs to a cluster over `fail_stop` (label `delivered`), and
/// runs `check` on it with two inputs.
#[cfg(stageleft_runtime)]
fn plain_broadcast(check: EcCheck, lossy: bool) -> Results {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    if lossy {
        let delivered = input.broadcast_closed(&members, TCP.lossy_delayed_forever().bincode());
        let _out = delivered.sim_cluster_output();
    } else {
        let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
        delivered.for_each(q!(|_| {}));
    }

    let mut check = check;
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    check.run(&sim, async || {
        send.send(1);
    })
}

/// A process broadcasts to a cluster, and every member echoes what the process sent it to
/// every member; the label `delivered` is everything a member received, from either.
/// Agreement survives any single crash: one surviving recipient echoes to everyone.
#[cfg(stageleft_runtime)]
fn echo_broadcast(check: EcCheck) -> Results {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .weaken_consistency();
    let echoed = initial
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let delivered = initial
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: one correct recipient of the broadcast echoes it to everyone */
        ));
    let _out = delivered.sim_cluster_output();

    let mut check = check;
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    check.run(&sim, async || {
        send.send(7);
    })
}

#[cfg(stageleft_runtime)]
#[test]
fn a_plain_broadcast_tolerates_no_crash_and_breaks_at_a_mid_fan_out_crash() {
    let results = plain_broadcast(EcCheck::new(), false);
    let label = results.label("delivered");
    let Verdict::Counterexample(cex) = &label.verdict else {
        panic!("expected a counterexample:\n{results}");
    };
    // No omission is admitted on `fail_stop`, so bound 0 is certified; one crash breaks it.
    assert_eq!(cex.tolerates, Some(0), "{results}");
    assert_eq!(cex.crashes(), 1, "{results}");
    // The source dies after reaching some, not all, members.
    let [Fault::Crash { node, at }] = cex.faults.iter().collect::<Vec<_>>()[..] else {
        panic!("one crash: {results}");
    };
    assert!(node.starts_with("process"), "{results}");
    assert!(matches!(at, CrashAt::After(_)), "{results}");
    assert!(!cex.missing.is_empty(), "{results}");
    assert!(!results.holds());
    assert!(label.stats.hypotheses >= 2, "{results}");
    assert!(label.stats.schedules >= 2, "{results}");
}

#[cfg(stageleft_runtime)]
#[test]
fn a_lossy_broadcast_breaks_by_omission_alone() {
    let results = plain_broadcast(EcCheck::new(), true);
    let label = results.label("delivered");
    let Verdict::Counterexample(cex) = &label.verdict else {
        panic!("expected a counterexample:\n{results}");
    };
    assert_eq!(cex.tolerates, None, "{results}");
    assert_eq!(cex.crashes(), 0, "{results}");
    assert_eq!(cex.faults.len(), 1, "one omission: {results}");
    assert!(matches!(cex.faults.iter().next(), Some(Fault::Omit(_))));
    // The first hypothesis after the probe already breaks it.
    assert_eq!(cex.hypothesis, 1, "{results}");
}

#[cfg(stageleft_runtime)]
#[test]
fn an_echo_broadcast_tolerates_one_crash_and_breaks_at_two() {
    let results = echo_broadcast(EcCheck::new());
    let label = results.label("delivered");
    let Verdict::Counterexample(cex) = &label.verdict else {
        panic!("expected a counterexample:\n{results}");
    };
    assert_eq!(cex.tolerates, Some(1), "{results}");
    assert_eq!(cex.crashes(), 2, "{results}");
}

#[cfg(stageleft_runtime)]
#[test]
fn the_crash_cap_ends_the_sweep_with_a_bounded_certificate() {
    let results = echo_broadcast(EcCheck::new().max_crashes(1));
    let label = results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(1)
            }
        ),
        "{results}"
    );
    // The plain hops (`initial`, and the echo's keyed hop) are labels of their own, and a
    // single crash breaks them.
    let Verdict::Counterexample(hop) = &results.label("initial").verdict else {
        panic!("the plain hop breaks: {results}");
    };
    assert_eq!(hop.tolerates, Some(0), "{results}");
}

#[cfg(stageleft_runtime)]
#[test]
fn a_label_that_sees_nothing_without_faults_is_vacuous() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    delivered.for_each(q!(|_| {}));

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    // The body sends nothing.
    let results = check.run(&sim, async || {});
    assert!(
        matches!(
            results.label("delivered").verdict,
            Verdict::Vacuous(VacuousReason::EmptyDomain)
        ),
        "{results}"
    );
    results.assert_holds();
}

#[cfg(stageleft_runtime)]
#[test]
fn sampled_schedules_find_the_same_counterexample() {
    let results = plain_broadcast(EcCheck::new().sample_schedules(4), false);
    let label = results.label("delivered");
    let Verdict::Counterexample(cex) = &label.verdict else {
        panic!("expected a counterexample:\n{results}");
    };
    assert_eq!(cex.tolerates, Some(0), "{results}");
    assert_eq!(cex.crashes(), 1, "{results}");
}

#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "hydrofrac: a label does not hold")]
fn assert_holds_panics_with_the_report() {
    plain_broadcast(EcCheck::new(), false).assert_holds();
}

#[cfg(stageleft_runtime)]
#[test]
fn members_that_disagree_without_faults_break_at_the_probe() {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    // Every member tags what it received with its own id: members never agree.
    let tagged = input
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .weaken_consistency()
        .map(q!(move |x| (x, CLUSTER_SELF_ID.get_raw_id())))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** deliberately false */
        ));
    tagged.for_each(q!(|_| {}));

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });
    // `tagged` names both the broadcast hop (which breaks at one crash, like any plain
    // broadcast) and the labelled `map`, which breaks at the failure-free probe.
    let at_probe: Vec<_> = results
        .labels_named("tagged")
        .filter_map(|l| Some((l, l.verdict.counterexample()?)))
        .filter(|(_, cex)| cex.hypothesis == 0)
        .collect();
    let [(label, cex)] = at_probe[..] else {
        panic!("one label breaks at the probe:\n{results}");
    };
    assert!(cex.faults.is_empty(), "{results}");
    assert_eq!(cex.tolerates, None, "{results}");
    assert_eq!(label.stats.hypotheses, 1, "{results}");
    // Each member lacks the other members' tags.
    assert_eq!(cex.missing.len(), MEMBERS * (MEMBERS - 1), "{results}");
}

/// A body that panics (a harness assertion failing under injected faults) is a
/// counterexample. (So is a panic inside the program's own dataflow: the simulation dylib
/// catches it and raises it again on the host, `hydro_lang::sim::panic_guard`.)
#[cfg(stageleft_runtime)]
#[test]
fn a_body_that_panics_is_a_counterexample() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    delivered.for_each(q!(|_| {}));

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(13);
        panic!("unlucky");
    });
    let Verdict::Counterexample(cex) = &results.label("delivered").verdict else {
        panic!("expected a counterexample:\n{results}");
    };
    assert!(
        cex.panic.as_deref().is_some_and(|p| p.contains("unlucky")),
        "{results}"
    );
    assert!(cex.faults.is_empty(), "{results}");
}

/// The check attaches record provenance: on these programs the rewrite leaves no opaque island.
#[cfg(stageleft_runtime)]
#[test]
fn the_check_tracks_record_provenance_precisely() {
    for results in [
        plain_broadcast(EcCheck::new(), false),
        plain_broadcast(EcCheck::new(), true),
        echo_broadcast(EcCheck::new()),
    ] {
        assert!(results.islands.is_empty(), "{results}");
    }
}

/// Each counterexample's Lamport diagram is written under
/// `target/hydrofrac/<test>/<label>/`, and the report says where.
#[cfg(stageleft_runtime)]
#[test]
fn counterexamples_write_their_lamport_diagram() {
    let results = plain_broadcast(EcCheck::new(), false);
    let label = results.label("delivered");
    let cex = label.verdict.counterexample().expect("a counterexample");
    let diagram = cex.diagram.as_ref().expect("the diagram was written");
    let dot = diagram.with_extension("dot");
    assert!(dot.is_file(), "{}", dot.display());
    let parent = dot.parent().unwrap();
    assert!(
        parent
            .to_string_lossy()
            .contains("hydrofrac/counterexamples_write_their_lamport_diagram/delivered"),
        "{}",
        parent.display()
    );
    let source = std::fs::read_to_string(&dot).unwrap();
    assert!(source.starts_with("digraph"), "{source}");
    assert!(results.to_string().contains("diagram: "), "{results}");
}

/// The report names faults, payloads and facts in source terms: no raw bincode, channels by
/// name, and the sweep's verdict form.
#[cfg(stageleft_runtime)]
#[test]
fn the_report_is_in_source_names() {
    let results = echo_broadcast(EcCheck::new());
    let report = results.to_string();
    println!("{report}");
    let delivered = results.label("delivered");
    let line = report
        .lines()
        .find(|l| l.contains(&format!("[{}] delivered", delivered.index)))
        .unwrap_or_else(|| panic!("a row for `delivered`:\n{report}"));
    assert!(
        line.contains("tolerates 1 crash, breaks at 2: {crash("),
        "{report}"
    );
    // The broadcast value 7 is decoded through the tapped fact with the same bytes.
    assert!(line.contains("]: 7"), "{report}");
    assert!(!report.contains("07000000"), "raw bincode in the report:\n{report}");
    assert!(report.contains("lacks 7"), "{report}");
    // Channels are named by the program's names.
    assert!(line.contains("initial[") || line.contains("echo["), "{report}");
}

#[cfg(stageleft_runtime)]
#[test]
fn the_report_states_omission_only_counterexamples() {
    let results = plain_broadcast(EcCheck::new(), true);
    let report = results.to_string();
    assert!(
        report.contains("breaks under omissions alone: {omit("),
        "{report}"
    );
    assert!(report.contains("[process→cluster@"), "{report}");
}

/// Two members on fail-stop channels, each sending its own fact to the other and keeping it
/// (a crash of either leaves one correct member), and batching what it collected before it
/// reports it: the label holds under every admissible fault set (a batch only delays), but its
/// search cannot certify it, since its fault sets have several executions
/// (docs/design/hydrofrac-lite.md §4.9).
#[cfg(stageleft_runtime)]
fn batched_exchange(check: EcCheck) -> Results {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .weaken_consistency();
    let echoed = initial
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let tick = members.tick();
    let delivered = initial
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed)
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: one correct recipient echoes to the other */
        ));
    let _out = delivered.sim_cluster_output();

    let mut check = check;
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    check.run(&sim, async || {
        send.send(5);
    })
}

#[cfg(stageleft_runtime)]
#[test]
fn a_schedule_sensitive_label_is_exhausted_not_certified() {
    let results = batched_exchange(EcCheck::new());
    let label = results.label("delivered");
    assert!(
        label.plan.sites.iter().any(|s| s.op == "batch"),
        "{results}"
    );
    let Verdict::Exhausted { tolerates, sites } = &label.verdict else {
        panic!("expected an exhausted search:\n{results}");
    };
    assert_eq!(*tolerates, Tolerance::All, "{results}");
    assert_eq!(sites, &label.plan.sites, "{results}");
    assert!(
        results.to_string().contains("not a certificate (schedule-sensitive: batch at"),
        "{results}"
    );
    assert!(!results.holds(), "{results}");
    // The plain `initial` hop, which nothing batches, still breaks at one crash: its cone and
    // its sender have no site, so its verdict is a certificate-backed counterexample.
    let initial = results.label("initial");
    assert!(initial.plan.sites.is_empty(), "{results}");
    let cex = initial.verdict.counterexample().expect("the hop breaks");
    assert!(cex.tolerance_certified, "{results}");
    assert_eq!(cex.tolerates, Some(0), "{results}");
}

/// `assert_holds` rejects an exhausted search, and says why; `assert_no_counterexample` accepts
/// it. Capped at zero crashes, the exchange has no counterexample: the plain `initial` hop
/// (site-free) is certified for omissions alone, which `fail_stop` admits none of, and the
/// batched label's search ends exhausted.
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "is schedule-sensitive, so its search is not a certificate")]
fn assert_holds_rejects_an_exhausted_search() {
    let results = batched_exchange(EcCheck::new().max_crashes(0));
    assert!(
        matches!(
            results.label("initial").verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(0)
            }
        ),
        "{results}"
    );
    assert!(
        matches!(
            results.label("delivered").verdict,
            Verdict::Exhausted {
                tolerates: Tolerance::UpTo(0),
                ..
            }
        ),
        "{results}"
    );
    results.assert_no_counterexample();
    results.assert_holds();
}

/// A message type with `Debug`: the report and the diagram render its payloads by it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Order {
    /// The order's id.
    pub id: u32,
}

/// A message's payload renders by its type's `Debug`: the rewrite renders it at the send, where
/// the type is known, and carries it in the send's metadata.
#[cfg(stageleft_runtime)]
#[test]
fn message_payloads_render_by_their_debug() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<Order, TotalOrder, ExactlyOnce>();
    let orders = input.broadcast_closed(&members, TCP.lossy_delayed_forever().bincode());
    let _out = orders.sim_cluster_output();
    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(Order { id: 1 });
    });

    let label = results.label("orders");
    let cex = label.verdict.counterexample().expect("an omission breaks it");
    // The rendering comes from the send's metadata, not from a fact with the same bytes.
    let carried = cex.record.log.events().iter().any(|e| match e {
        crate::fault::Event::Sent { meta: Some(m), .. } => {
            crate::provenance::split_meta(m).and_then(|(_, d)| d) == Some("Order { id: 1 }".into())
        }
        _ => false,
    });
    assert!(carried, "{:?}", cex.record.log);
    let report = results.to_string();
    assert!(report.contains("]: Order { id: 1 })}"), "{report}");
    let diagram = cex.diagram.as_ref().expect("the diagram was written");
    let dot = std::fs::read_to_string(diagram.with_extension("dot")).unwrap();
    assert!(dot.contains("Order { id: 1 }"), "{dot}");
}
