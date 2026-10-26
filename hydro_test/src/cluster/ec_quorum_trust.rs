//! Trusted `quorum` (docs/design/hydrofrac-races.md §8.2): the check trusts
//! `hydro_std::ec_inference_demos::quorum::quorum`'s mint (and only it, by its exact compiled
//! form), so a certificate's lineage is its fact's own attestations and no step of the mint is
//! fragile.
//!
//! Here, and not in `hydrofrac`, because the programs use `hydro_std`, which `hydrofrac`'s
//! simulations cannot link.
//!
//! - [`quorum_lineage_is_the_facts_attestations`]: in fragile mode, the mint's certificates carry
//!   no fragile site, and each one's lineage is the deliveries of its own fact only (two facts in
//!   flight); [`hand_written_quorum_stays_fragile`]: a copy of the same logic, quoted in this file,
//!   is not trusted.
//! - [`races_urb3_threshold2`]: uniform reliable broadcast over three members, which brute force
//!   cannot exhaust, checked by the search alone.
//! - [`races_planted_local_attestation`]: a planted bug through the real `quorum` (members attest
//!   only their own receipt), found by the search and by brute force.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::compiled::{CompiledSim, quiesce};
use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::{SimClusterReceiver, SimSender};
use hydro_std::ec_inference_demos::quorum::quorum;
use hydrofrac::EcCheck;
use hydrofrac::check::{CrossCheck, Verdict};
use hydrofrac::fault::{FaultHook, FaultPlan};
use hydrofrac::lineage::{Tables, derive};
use hydrofrac::provenance::{FragileSite, install_fragile};

pub struct Source;
pub struct Member;

/// How many random schedules the lineage tests run.
const SAMPLED: usize = 300;

/// One certificate observed in a run: the member, the fact, the fragile sites of its
/// derivation, and the payload bytes of every delivery in its lineage.
#[derive(Debug)]
struct Seen {
    member: Option<u32>,
    fact: u32,
    sites: BTreeSet<usize>,
    deps: Vec<Vec<u8>>,
}

/// Compiles `flow` with the rewrite in fragile mode; returns it with the program's fragile steps.
fn compile(flow: SimFlow<'_>) -> (CompiledSim, Vec<FragileSite>) {
    let sites = Rc::new(RefCell::new(vec![]));
    let sim = install_fragile(flow.skip_consistency_assertions(), None, sites.clone()).compiled();
    let sites = sites.borrow().clone();
    (sim, sites)
}

/// Runs [`SAMPLED`] random failure-free schedules of `sim` (`body` drives the inputs, then the
/// run quiesces) and returns each run's observations, every output a `u32`. Sampled: two facts
/// over two members have well over 140,000 schedules.
fn observe(sim: &CompiledSim, mut body: impl AsyncFnMut()) -> Vec<Vec<Seen>> {
    let tables = Tables::new(sim.channels(), sim.locations());
    let hook = Rc::new(FaultHook::new(&FaultPlan::default(), sim));
    sim.set_network_hook(hook.clone());
    let mut body = std::panic::AssertUnwindSafe(&mut body);
    sim.fuzz_bounded(SAMPLED, async || {
        (*body)().await;
        quiesce().await;
    });
    sim.clear_network_hook();
    let runs: Vec<Vec<Seen>> = hook
        .drain_instances()
        .iter()
        .map(|record| {
            let lineage = derive(&record.log, &tables);
            lineage
                .observations()
                .map(|o| Seen {
                    member: o.node.member,
                    // Sim outputs cross the dylib boundary in the default sim codec (MessagePack).
                    fact: <hydro_lang::sim::codec::RmpCodec as hydro_lang::sim::codec::SimCodec<
                        u32,
                    >>::decode(o.bytes),
                    sites: (*lineage.fragile_sites(o.event)).clone(),
                    deps: lineage
                        .derivation(o.event)
                        .iter()
                        .map(|d| d.message.bytes.to_vec())
                        .collect(),
                })
                .collect()
        })
        .collect();
    assert!(runs.iter().any(|r| !r.is_empty()), "nothing observed");
    runs
}

/// `items`, attested by every member to every member as it receives them, certified at
/// `threshold` attestations by the real `quorum`, or by a hand-written copy of its logic
/// (hydro_std's `quorum.rs:121-185`, certifying the bare fact): the same steps, but quoted here,
/// so not the trusted mint.
fn attest_all<'a>(
    flow: &mut FlowBuilder<'a>,
    threshold: usize,
    copy: bool,
) -> (
    Cluster<'a, Member>,
    SimSender<u32, TotalOrder, ExactlyOnce>,
    SimClusterReceiver<u32, NoOrder, ExactlyOnce>,
) {
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let acks = items
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .entries()
        .map(q!(|(attestor, m)| (m, attestor)));
    let certified = if copy {
        let distinct_acks = acks.unique();
        let certified = sliced! {
            let new_acks = use::batch(distinct_acks, nondet!(/** test copy */));
            let mut pending = use::state(|l| l.singleton(q!(HashMap::new())));
            let mut fired = use::state(|l| l.singleton(q!(HashSet::new())));
            let ack_vec = new_acks.fold(
                q!(|| Vec::new()),
                q!(|items, item| items.push(item), commutative = manual_proof!(/** test copy */)),
            );
            let pending_ref = pending.by_mut();
            let fired_ref = fired.by_mut();
            let ack_vec_ref = ack_vec.by_ref();
            let tick = ack_vec.location().clone();

            tick.singleton(q!(()))
                .into_stream()
                .flat_map_unordered(q!(move |_| {
                    let mut newly = Vec::new();
                    for (fact, attestor) in ack_vec_ref.iter().cloned() {
                        if fired_ref.contains(&fact) {
                            continue;
                        }
                        let attestors = pending_ref.entry(fact).or_insert_with(HashSet::new);
                        attestors.insert(attestor);
                        if attestors.len() >= threshold {
                            pending_ref.remove(&fact);
                            fired_ref.insert(fact);
                            newly.push(fact);
                        }
                    }
                    newly
                }))
        };
        certified.assert_has_consistency_of(manual_proof!(/** test copy */))
    } else {
        quorum(threshold, acks).map(q!(|cert| cert.into_fact()))
    };
    let out = certified.sim_cluster_output();
    (members, send, out)
}

/// The bytes of fact `v` on the wire (bincode of a `u32`).
fn bytes(v: u32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

/// Two members attest two facts to each other; the real `quorum` certifies each at two
/// attestations. No step of the program is fragile, every certificate is robust, and its
/// lineage is the deliveries of its own fact (the source's and the attestations), not the
/// other fact's.
#[test]
fn quorum_lineage_is_the_facts_attestations() {
    let mut flow = FlowBuilder::new();
    let (members, send, _out) = attest_all(&mut flow, 2, false);
    let (sim, sites) = compile(flow.sim().with_cluster_size(&members, 2));
    assert!(sites.is_empty(), "{sites:?}");
    let runs = observe(&sim, async || {
        send.send(1);
        send.send(2);
    });
    for seen in runs.iter().flatten() {
        assert!(seen.sites.is_empty(), "{seen:?} is not robust");
        assert!(!seen.deps.is_empty(), "{seen:?} has no lineage");
        assert!(
            seen.deps.iter().all(|d| *d == bytes(seen.fact)),
            "{seen:?}: lineage beyond its fact's own deliveries"
        );
        // Both attestations (one from each member) and the source's delivery to each attestor.
        assert_eq!(seen.deps.len(), 4, "{seen:?}");
    }
    for run in &runs {
        for m in 0..2 {
            let mut facts: Vec<u32> = run
                .iter()
                .filter(|s| s.member == Some(m))
                .map(|s| s.fact)
                .collect();
            facts.sort_unstable();
            assert_eq!(facts, vec![1, 2], "member {m}: {run:?}");
        }
    }
}

/// The same program with the hand-written copy of `quorum`: its batch, fold and state are
/// fragile steps, and every certificate passes them.
#[test]
fn hand_written_quorum_stays_fragile() {
    let mut flow = FlowBuilder::new();
    let (members, send, _out) = attest_all(&mut flow, 2, true);
    let (sim, sites) = compile(flow.sim().with_cluster_size(&members, 2));
    assert!(!sites.is_empty(), "the copy has no fragile step");
    eprintln!("fragile sites of the copy: {sites:?}");
    let runs = observe(&sim, async || {
        send.send(1);
        send.send(2);
    });
    for seen in runs.iter().flatten() {
        assert!(!seen.sites.is_empty(), "{seen:?} is robust");
    }
}

/// A verdict on one line.
fn short(results: &hydrofrac::Results, v: &Verdict) -> String {
    match v {
        Verdict::Counterexample(c) => format!(
            "COUNTEREXAMPLE {} (tolerated {:?}, certified {})",
            results.report_names().fault_set(&c.faults),
            c.tolerates,
            c.tolerance_certified
        ),
        Verdict::Certified { tolerates } => format!("CERTIFIED (tolerates {tolerates:?})"),
        Verdict::Exhausted { tolerates, sites } => {
            format!("EXHAUSTED up to {tolerates:?} ({} sites)", sites.len())
        }
        other => format!("{other:?}"),
    }
}

/// Prints `results`, then one `COST` line per searched label.
fn costs(name: &str, results: &hydrofrac::Results, wall: std::time::Duration) {
    eprintln!("=== {name} ({:.1}s)\n{results}", wall.as_secs_f64());
    for l in results.results() {
        eprintln!(
            "COST {name} / {}: {} | search {} sets, {} schedules, {:.1}s, races {:?}",
            l.name(),
            short(results, &l.verdict),
            l.stats.hypotheses,
            l.stats.schedules,
            l.stats.wall.as_secs_f64(),
            l.stats.races,
        );
    }
}

/// `uniform_reliable_broadcast_closed` over `n` members, with echo threshold `threshold`, one
/// item, checked by the search alone (up to two crashes).
fn urb_races(n: usize, threshold: usize) -> hydrofrac::Results {
    use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = uniform_reliable_broadcast_closed(items, &members, threshold).sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(2);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, n))
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        send.send(1);
    });
    costs(
        &format!("urb ({n} members, threshold {threshold})"),
        &results,
        start.elapsed(),
    );
    results
}

/// The searched label `name` (by display name).
fn label<'r>(results: &'r hydrofrac::Results, name: &str) -> &'r hydrofrac::check::LabelResult {
    results
        .results()
        .iter()
        .find(|l| l.plan.collection.display_name() == name && l.stats.hypotheses > 0)
        .unwrap_or_else(|| panic!("no searched label `{name}`:\n{results}"))
}

/// Three members, threshold 2: beyond brute force (its failure-free schedules alone pass
/// 215,452 without finishing, `races_targets_probe_urb3`). The delivery label is certified for
/// every admissible set of at most two crashes, one schedule per set (measured: 18 sets, 18
/// schedules). Two crashed members leave one live member, which agrees with itself; with at
/// most one crashed member, the threshold of 2 means a delivered message was held by a live
/// member, which echoes it.
#[test]
fn races_urb3_threshold2() {
    let results = urb_races(3, 2);
    for name in ["uniform_reliable_broadcast_closed", "certified"] {
        let l = label(&results, name);
        assert!(
            matches!(
                l.verdict,
                Verdict::Certified {
                    tolerates: hydrofrac::search::Tolerance::UpTo(2)
                        | hydrofrac::search::Tolerance::All
                }
            ),
            "{name}: {}\n{results}",
            short(&results, &l.verdict)
        );
        assert!(l.stats.races.is_some_and(|r| r.claim_b), "{name}: Claim B");
        assert_eq!(
            l.stats.schedules, l.stats.hypotheses,
            "{name}: one schedule per set"
        );
    }
}

/// The planted bug: each member certifies a fact on its own receipt alone (it attests to itself
/// only; threshold 1), through the real `quorum`. If the source crashes mid fan-out, one member
/// delivers and the other never does.
fn local_attestation(check: EcCheck, max_faults: usize) -> CrossCheck {
    let mut check = check;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let attestations = items
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        // Its own receipt is its only attestation (under one nominal attestor id: at threshold
        // 1, who attests does not matter).
        .map(q!(|m| (m, hydro_lang::location::MemberId::<()>::from_raw_id(0))))
        .weaken_ordering::<NoOrder>();
    let _out = quorum(1, attestations)
        .map(q!(|cert| cert.into_fact()))
        .sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        max_faults,
    )
}

/// The planted bug's delivery label is a counterexample, crashing the source mid fan-out, and
/// the mode agrees with brute force.
fn planted(check: EcCheck) {
    let start = std::time::Instant::now();
    let cross = local_attestation(check.max_crashes(2), 2);
    costs("planted local attestation", &cross.results, start.elapsed());
    eprintln!(
        "brute force: {} sets, {} schedules, {:.1}s\n{cross}",
        cross.brute.sets,
        cross.brute.schedules,
        cross.brute.wall.as_secs_f64()
    );
    cross.assert_agrees();
    let names = cross.results.report_names();
    // The delivered output, the last label searched.
    let l = cross
        .results
        .results()
        .iter()
        .rfind(|l| l.stats.hypotheses > 0)
        .expect("a searched label");
    let Verdict::Counterexample(c) = &l.verdict else {
        panic!("{}: {:?}\n{}", l.name(), l.verdict, cross.results);
    };
    let faults = names.fault_set(&c.faults);
    eprintln!("COUNTEREXAMPLE {}: {faults}", l.name());
    assert!(faults.contains("crash(Source"), "{faults}");
}

#[test]
fn races_planted_local_attestation() {
    planted(EcCheck::new());
}
