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

/// Two members on fail-stop channels, each sending its own fact to the other and keeping it:
/// a crash of either leaves one correct member, which agrees with itself, so every admissible
/// fault set holds.
#[cfg(stageleft_runtime)]
#[test]
fn a_two_member_exchange_holds_under_every_admissible_fault_set() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    // The source's crash can only withhold the input from everyone, or (mid fan-out) from one
    // member: then the recipient echoes it.
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
            /** claimed: one correct recipient echoes to the other */
        ));
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(5);
    });
    assert!(
        matches!(
            results.label("delivered").verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "{results}"
    );
}

/// The check attaches record provenance: on these programs no record falls back to the
/// execution rule, and the rewrite leaves no opaque island.
#[cfg(stageleft_runtime)]
#[test]
fn the_check_tracks_record_provenance_precisely() {
    for results in [
        plain_broadcast(EcCheck::new(), false),
        plain_broadcast(EcCheck::new(), true),
        echo_broadcast(EcCheck::new()),
    ] {
        assert!(results.islands.is_empty(), "{results}");
        for label in results.results() {
            assert_eq!(
                label.stats.opaque_fallbacks,
                0,
                "{}: {results}",
                label.name()
            );
        }
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
    // The missing fact's support is highlighted, and the tap's fact is decoded.
    assert!(results.to_string().contains("diagram: "), "{results}");
}

/// The report names faults, payloads and facts in source terms: no raw bincode, channels by
/// name, the sweep's verdict form, and the plain hops listed under the label they merge into.
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
    assert!(
        !report.contains("07000000"),
        "raw bincode in the report:\n{report}"
    );
    assert!(report.contains("lacks 7"), "{report}");
    // Channels are named by the program's names.
    assert!(
        line.contains("initial[") || line.contains("echo["),
        "{report}"
    );
}

/// The echo broadcast without `weaken_consistency`: the plain hops stay labelled and reach the
/// outside only merged into `delivered`, so the report lists them under it.
#[cfg(stageleft_runtime)]
#[test]
fn the_report_lists_merged_hops_under_the_label_they_merge_into() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input.broadcast_closed(&members, TCP.fail_stop().name("initial").bincode());
    let echoed = initial
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values();
    let delivered = initial.weaken_ordering::<NoOrder>().merge_unordered(echoed);
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(7);
    });
    let report = results.to_string();
    println!("{report}");
    let hops: Vec<_> = results
        .results()
        .iter()
        .filter(|l| l.plan.merged_at.is_some())
        .collect();
    assert!(
        !hops.is_empty(),
        "the plain hops are merge-dominated:\n{report}"
    );
    let lines: Vec<&str> = report.lines().collect();
    for hop in hops {
        let at = hop.plan.merged_at.unwrap();
        let row = lines
            .iter()
            .position(|l| l.contains(&format!("[{}] ", hop.index)))
            .unwrap();
        let merged = lines
            .iter()
            .position(|l| l.trim_start().starts_with(&format!("[{at}] ")))
            .unwrap();
        assert!(
            lines[row].contains("↳") && lines[row].contains(&format!("a hop merged at [{at}]")),
            "{report}"
        );
        // Under the label it merges into: after its row, before the next unindented label.
        assert!(merged < row, "{report}");
        assert!(
            lines[merged + 1..row].iter().all(|l| !l.starts_with("  [")),
            "{report}"
        );
    }
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

#[cfg(stageleft_runtime)]
#[test]
fn rows_carry_the_normalized_counterexample() {
    let results = echo_broadcast(EcCheck::new());
    let rows = results.rows();
    assert_eq!(rows.len(), results.results().len());
    let delivered = results.label("delivered");
    let row = &rows[delivered.index];
    assert_eq!(row.collection, "delivered");
    assert_eq!(row.verdict, "counterexample");
    assert_eq!(row.tolerates, "1");
    assert!(
        row.cex.starts_with("{crash(") && !row.cex.contains("after"),
        "{row:?}"
    );
    assert_eq!(row.hypotheses, delivered.stats.hypotheses);
    assert_eq!(row.test, "rows_carry_the_normalized_counterexample");
    let capped = echo_broadcast(EcCheck::new().max_crashes(1)).rows();
    assert_eq!(capped[delivered.index].verdict, "certificate");
    assert_eq!(capped[delivered.index].tolerates, "1");
}

/// The two-member exchange, but each member batches what it collected before it reports it,
/// and reports a tick's batch only if the tick's count of it is positive (a `filter_if` that
/// never filters anything out, but reads the tick): the label still holds under every
/// admissible fault set (a batch only delays), but its search can no longer certify it, since
/// its fault sets now have several executions and the order-insensitivity analysis cannot see
/// that the snapshot is harmless (docs/design/hydrofrac-lite.md §4.9). Explored exhaustively,
/// its few fault sets are enumerated instead ([`Verdict::CertifiedByEnumeration`]); sampled,
/// the search ends [`Verdict::Exhausted`]. Without the snapshot (`batched_exchange_with(..,
/// false)`) the batch is discharged and the label is certified by its search.
#[cfg(stageleft_runtime)]
fn batched_exchange(check: EcCheck) -> Results {
    batched_exchange_with(check, true)
}

#[cfg(stageleft_runtime)]
fn batched_exchange_with(check: EcCheck, snapshot: bool) -> Results {
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
    let batch = initial
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed)
        .batch(&tick, nondet!(/** which tick a delivery lands in */));
    let batch = if snapshot {
        let arrived = batch.clone().count().map(q!(|n| n > 0));
        batch.filter_if(arrived)
    } else {
        batch
    };
    let delivered = batch
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
    // Sampled schedules: no bounded enumeration either.
    let results = batched_exchange(EcCheck::new().sample_schedules(16));
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
        results
            .to_string()
            .contains("not a certificate (schedule-sensitive: batch at"),
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
    let row = results
        .rows()
        .into_iter()
        .find(|r| r.collection == "delivered")
        .unwrap();
    assert_eq!(row.verdict, "exhausted");
}

/// The batched exchange without the snapshot: its batch reaches the label through `all_ticks`
/// only, so the order-insensitivity analysis discharges it (§4.9), the label has no site, and
/// its search certifies it, on sampled schedules too (the brute force agrees:
/// `brute_force_agrees_on_a_monotone_batched_echo`).
#[cfg(stageleft_runtime)]
#[test]
fn a_batch_with_a_monotone_downstream_is_certified_by_the_search() {
    let results = batched_exchange_with(EcCheck::new().sample_schedules(16), false);
    let label = results.label("delivered");
    assert!(label.plan.sites.is_empty(), "{results}");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "{results}"
    );
}

/// `assert_holds` rejects an exhausted search, and says why; `assert_no_counterexample` accepts
/// it. Capped at zero crashes, the exchange has no counterexample: the plain `initial` hop
/// (site-free) is certified for omissions alone, which `fail_stop` admits none of, and the
/// batched label's (sampled) search ends exhausted.
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "is schedule-sensitive, so its search is not a certificate")]
fn assert_holds_rejects_an_exhausted_search() {
    let results = batched_exchange(EcCheck::new().max_crashes(0).sample_schedules(16));
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

/// Explored exhaustively, the batched exchange has a handful of admissible fault sets (neither
/// member may crash, since one correct member would be left: |C| − 2 = 0; the source may crash
/// before any send or right after one), so the sensitive label's search ends in a bounded
/// certificate by enumeration: every admissible set, over every schedule, holds.
#[cfg(stageleft_runtime)]
#[test]
fn a_small_schedule_sensitive_label_is_certified_by_enumeration() {
    let results = batched_exchange(EcCheck::new());
    let label = results.label("delivered");
    let Verdict::CertifiedByEnumeration {
        tolerates,
        max_faults,
        sets,
        sites,
    } = &label.verdict
    else {
        panic!("expected a certificate by enumeration:\n{results}");
    };
    assert_eq!(*tolerates, Tolerance::All, "{results}");
    assert_eq!(
        *max_faults, None,
        "every admissible set, whatever its size:\n{results}"
    );
    // The failure-free set, and the source's crash before any send and right after each of its
    // two sends.
    assert_eq!(*sets, 4, "{results}");
    assert!(sites.iter().any(|s| s.op == "batch"), "{results}");
    assert!(
        results
            .to_string()
            .contains("holds under any crashes by enumeration (4 fault sets over every schedule; schedule-sensitive: batch at"),
        "{results}"
    );
    // A certificate, so `assert_holds` accepts it.
    assert!(!label.verdict.fails(), "{results}");
    let row = results
        .rows()
        .into_iter()
        .find(|r| r.collection == "delivered")
        .unwrap();
    assert_eq!(row.verdict, "enumerated");
    assert_eq!(row.tolerates, "all");
    // Capped at zero crashes, `fail_stop` admits no fault: the failure-free run is all there is.
    let capped = batched_exchange(EcCheck::new().max_crashes(0));
    assert!(
        matches!(
            capped.label("delivered").verdict,
            Verdict::CertifiedByEnumeration {
                tolerates: Tolerance::UpTo(0),
                max_faults: None,
                sets: 1,
                ..
            }
        ),
        "{capped}"
    );
    capped.assert_holds();
}

/// A message type with `Debug`: the report and the diagram render its payloads by it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Order {
    /// The order's id.
    pub id: u32,
}

/// A message type without `Debug`: its payloads stay short hex.
#[derive(Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Sealed(
    /// The sealed value.
    pub u32,
);

/// A process broadcasts one `Order { id: 1 }` to a cluster over a lossy channel (label
/// `orders`); one omission breaks it.
#[cfg(stageleft_runtime)]
fn lossy_orders() -> Results {
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
    check.run(&sim, async || {
        send.send(Order { id: 1 });
    })
}

/// [`lossy_orders`] with a payload type that has no `Debug`.
#[cfg(stageleft_runtime)]
fn lossy_sealed() -> Results {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<Sealed, TotalOrder, ExactlyOnce>();
    let sealed = input.broadcast_closed(&members, TCP.lossy_delayed_forever().bincode());
    let _out = sealed.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    check.run(&sim, async || {
        send.send(Sealed(1));
    })
}

/// A message's payload renders by its type's `Debug`: the rewrite renders it at the send, where
/// the type is known, and carries it in the send's metadata.
#[cfg(stageleft_runtime)]
#[test]
fn message_payloads_render_by_their_debug() {
    let results = lossy_orders();
    let label = results.label("orders");
    let cex = label
        .verdict
        .counterexample()
        .expect("an omission breaks it");
    // The rendering comes from the send's metadata, not from a fact with the same bytes.
    let renderings = crate::report::payload_renderings(&cex.record.log);
    assert!(
        renderings.values().any(|d| d == "Order { id: 1 }"),
        "{renderings:?}"
    );
    let report = results.to_string();
    assert!(
        report.contains("breaks under omissions alone: {omit(")
            && report.contains("]: Order { id: 1 })}"),
        "{report}"
    );
    // `Order { id: 1 }` as bincode: a little-endian u32.
    assert!(
        !report.contains("01000000"),
        "raw bincode in the report:\n{report}"
    );
    // The diagram labels the message by it too.
    let dot = std::fs::read_to_string(
        cex.diagram
            .as_ref()
            .expect("the diagram was written")
            .with_extension("dot"),
    )
    .unwrap();
    assert!(dot.contains("Order { id: 1 }"), "{dot}");
    // Provenance stays precise with the rendering appended to the metadata.
    assert_eq!(label.stats.opaque_fallbacks, 0, "{results}");
}

/// A payload type without `Debug` falls back to short hex.
#[cfg(stageleft_runtime)]
#[test]
fn payloads_without_debug_stay_hex() {
    let results = lossy_sealed();
    let label = results.label("sealed");
    let cex = label
        .verdict
        .counterexample()
        .expect("an omission breaks it");
    assert!(
        crate::report::payload_renderings(&cex.record.log).is_empty(),
        "no rendering without `Debug`"
    );
    let report = results.to_string();
    assert!(report.contains("]: 01000000)}"), "{report}");
    assert_eq!(label.stats.opaque_fallbacks, 0, "{results}");
}
