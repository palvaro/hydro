//! Fragile-step tags end to end (docs/design/hydrofrac-races.md §3.1, §4.1): small programs run
//! in the simulator with the rewrite in fragile mode, checking which fragile sites
//! [`Lineage::fragile_sites`] derives for each observation.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::cell::RefCell;
#[cfg(stageleft_runtime)]
use std::collections::BTreeSet;
#[cfg(stageleft_runtime)]
use std::rc::Rc;

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::MemberId;
#[cfg(stageleft_runtime)]
use hydro_lang::networking::TCP;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::{CompiledSim, quiesce};
#[cfg(stageleft_runtime)]
use hydro_lang::sim::flow::SimFlow;
#[cfg(stageleft_runtime)]
use stageleft::q;

#[cfg(stageleft_runtime)]
use super::*;
#[cfg(stageleft_runtime)]
use crate::fault::{FaultHook, FaultPlan};
#[cfg(stageleft_runtime)]
use crate::provenance::{FragileSite, install_fragile};

#[cfg(stageleft_runtime)]
/// One observation of a run: who emitted which value, and the fragile sites of its derivation.
#[derive(Debug)]
struct Seen {
    member: Option<u32>,
    value: u32,
    sites: BTreeSet<usize>,
}

#[cfg(stageleft_runtime)]
/// Compiles `flow` with the rewrite in fragile mode; returns it with the program's fragile steps.
fn compile(flow: SimFlow<'_>) -> (CompiledSim, Vec<FragileSite>) {
    let sites = Rc::new(RefCell::new(vec![]));
    let sim = install_fragile(flow.test_safety_only(), None, sites.clone()).compiled();
    let sites = sites.borrow().clone();
    (sim, sites)
}

#[cfg(stageleft_runtime)]
/// Runs every failure-free schedule of `sim` (`body` drives the inputs, then the run quiesces)
/// and returns each run's observations, every output a `u32`.
fn observe(sim: &CompiledSim, mut body: impl AsyncFnMut()) -> Vec<Vec<Seen>> {
    let tables = Tables::new(sim.channels(), sim.locations());
    let hook = Rc::new(FaultHook::new(&FaultPlan::default(), sim));
    sim.set_network_hook(hook.clone());
    let mut body = std::panic::AssertUnwindSafe(&mut body);
    sim.exhaustive(async || {
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
                    value: <hydro_lang::sim::codec::RmpCodec as hydro_lang::sim::codec::SimCodec<
                        u32,
                    >>::decode(o.bytes),
                    sites: (*lineage.fragile_sites(o.event)).clone(),
                })
                .collect()
        })
        .collect();
    assert!(!runs.is_empty());
    runs
}

#[cfg(stageleft_runtime)]
/// The site of the fragile step `op` (one of `|`-separated names) built at line `line` of this
/// file.
fn site(sites: &[FragileSite], op: &str, line: u32) -> usize {
    let at = format!("fragile_tests.rs:{line}");
    let ops: Vec<&str> = op.split('|').collect();
    let found: Vec<_> = sites
        .iter()
        .filter(|s| ops.contains(&s.op.as_str()) && s.site == at)
        .collect();
    assert_eq!(found.len(), 1, "one `{op}` at {at} in {sites:?}");
    found[0].index
}

#[cfg(stageleft_runtime)]
/// Asserts every observation of every run is robust, and that there is one.
fn assert_robust(runs: &[Vec<Seen>]) {
    assert!(runs.iter().any(|r| !r.is_empty()), "nothing observed");
    for seen in runs.iter().flatten() {
        assert!(seen.sites.is_empty(), "{seen:?} is not robust");
    }
}

#[cfg(stageleft_runtime)]
/// Asserts every observation of every run passed site `site`, and that there is one.
fn assert_all_pass(runs: &[Vec<Seen>], site: usize) {
    assert!(runs.iter().any(|r| !r.is_empty()), "nothing observed");
    for seen in runs.iter().flatten() {
        assert!(
            seen.sites.contains(&site),
            "{seen:?} does not pass site {site}"
        );
    }
}

#[cfg(stageleft_runtime)]
/// `batch → map → send`: the batch only decides when a record enters a tick, so the record is
/// robust, and the program has no fragile step at all.
#[test]
fn batch_map_send_is_robust() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (send, input) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = p1.tick();
    let _out = input
        .batch(&tick, nondet!(/** test */))
        .map(q!(|x| x * 10))
        .all_ticks()
        .send(&p2, TCP.fail_stop().bincode())
        .sim_output();
    let (sim, sites) = compile(flow.sim());
    assert!(sites.is_empty(), "{sites:?}");
    let runs = observe(&sim, async || {
        send.send(1);
        send.send(2);
    });
    assert_robust(&runs);
}

#[cfg(stageleft_runtime)]
/// A gossip pump: a snapshot of accumulated state, released when this tick's batch has a
/// trigger. What the snapshot holds when the trigger is released is the schedule's.
#[test]
fn snapshot_meets_trigger_is_fragile() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (value_send, values) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (pump_send, pumps) = p1.sim_input::<(), TotalOrder, ExactlyOnce>();
    let state = values.fold(q!(|| 0u32), q!(|acc, x| *acc += x));
    let tick = p1.tick();
    let pump = pumps.batch(&tick, nondet!(/** test */)).first().is_some();
    // On one line, so the steps' sites are the line `line!()` reports.
    #[rustfmt::skip]
    let (pumped, line) = (state.snapshot(&tick, nondet!(/** test */)).filter_if(pump), line!());
    let _out = pumped
        .all_ticks()
        .send(&p2, TCP.fail_stop().bincode())
        .sim_output();
    let (sim, sites) = compile(flow.sim());
    let snapshot = site(&sites, "snapshot", line);
    let filter_if = site(&sites, "cross_singleton", line);
    let runs = observe(&sim, async || {
        value_send.send(3);
        pump_send.send(());
    });
    assert_all_pass(&runs, snapshot);
    assert_all_pass(&runs, filter_if);
    // The race is real: the pump saw the state before and after the value.
    let values: BTreeSet<u32> = runs.iter().flatten().map(|s| s.value).collect();
    assert_eq!(values, BTreeSet::from([0, 3]));
}

#[cfg(stageleft_runtime)]
/// The `ask_once` miniature (`hydro_test`'s `ec_bruteforce.rs`), with the line of member 1's
/// answer (`known_now.filter_if(queried)`) returned. Member 0 announces its input to members 1
/// and 2; member 2, if its timer fires while it knows nothing, asks member 1, which answers with
/// what it knows in that tick. The output is what each member newly learns.
fn ask_once<'a>(
    peers: &Cluster<'a, ()>,
    values: Stream<u32, Cluster<'a, ()>, Unbounded, TotalOrder>,
    timeouts: Stream<(), Cluster<'a, ()>, Unbounded, TotalOrder>,
) -> (Stream<u32, Cluster<'a, ()>, Unbounded, NoOrder>, u32) {
    let (net_handle, net_in) =
        peers.forward_ref::<Stream<(MemberId<()>, Option<u32>), _, Unbounded, NoOrder>>();
    let answer_line = std::cell::Cell::new(0);
    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (out, newly): (
        Stream<(MemberId<()>, Option<u32>), Cluster<'a, ()>, Unbounded, NoOrder>,
        Stream<u32, Cluster<'a, ()>, Unbounded, NoOrder>,
    ) = sliced! {
        let value_batch = use::batch(values, nondet!(/** test */));
        let net_batch = use::batch(net_in, nondet!(/** test */));
        let timeout_batch = use::batch(timeouts, nondet!(/** test */));
        let mut known = use::state(|l| {
            l.source_iter(q!(Vec::<u32>::new())).weaken_ordering::<NoOrder>()
        });

        let own = value_batch.weaken_ordering::<NoOrder>();
        let heard = net_batch.clone().filter_map(q!(|(_, message)| message));
        let newly = own.clone().chain(heard).unique().filter_not_in(known.clone());
        let known_now = known.chain(newly.clone());
        known = known_now.clone();

        let announce = own.flat_map_ordered(q!(|value| [
            (hydro_lang::location::MemberId::from_raw_id(1), Some(value)),
            (hydro_lang::location::MemberId::from_raw_id(2), Some(value)),
        ]));
        let knows_nothing = known_now.clone().count().map(q!(|n| n == 0));
        let ask = timeout_batch
            .weaken_ordering::<NoOrder>()
            .filter_if(knows_nothing)
            .map(q!(|()| (hydro_lang::location::MemberId::from_raw_id(1), None::<u32>)));
        let queried = net_batch
            .filter(q!(|(_, message)| message.is_none()))
            .count()
            .map(q!(|n| n > 0));
        // On one line, so the answer's site is the line `line!()` reports.
        answer_line.set(line!()); let answers = known_now.filter_if(queried);
        let answers = answers
            .map(q!(|value| (hydro_lang::location::MemberId::from_raw_id(2), Some(value))));

        (announce.chain(ask).chain(answers), newly)
    };
    net_handle.complete(
        out.demux(peers, TCP.fail_stop().name("net").bincode())
            .entries(),
    );
    (newly, answer_line.get())
}

#[cfg(stageleft_runtime)]
/// `ask_once` (docs/design/hydrofrac-races.md §3.3): member 2's value is fragile when it came
/// through member 1's answer (trigger meets state) and robust when it came through member 0's
/// announcement; members 0 and 1 only ever learn it robustly. The dedup step every member's
/// output passes is not fragile.
#[test]
fn ask_once_answer_is_fragile_and_announcement_robust() {
    let mut flow = FlowBuilder::new();
    let peers = flow.cluster::<()>();
    let (value_send, values) = peers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = peers.sim_input::<(), TotalOrder, ExactlyOnce>();
    let (newly, answer_line) = ask_once(&peers, values, timeouts);
    let _out = newly.sim_cluster_output();
    let (sim, sites) = compile(flow.sim().with_cluster_size(&peers, 3));
    let answer = site(&sites, "cross_singleton", answer_line);
    assert!(!sites.iter().any(|s| s.op == "difference"), "{sites:?}");

    let runs = observe(&sim, async || {
        value_send.send(0, 7);
        timeout_send.send(2, ());
    });
    let (mut robust_at_2, mut answered_at_2) = (0, 0);
    for seen in runs.iter().flatten() {
        assert_eq!(seen.value, 7);
        match seen.member {
            Some(2) if seen.sites.is_empty() => robust_at_2 += 1,
            Some(2) => {
                assert!(seen.sites.contains(&answer), "{seen:?} via the answer");
                answered_at_2 += 1;
            }
            _ => assert!(seen.sites.is_empty(), "{seen:?} is not robust"),
        }
    }
    assert!(
        robust_at_2 > 0,
        "some schedule delivers the announcement first"
    );
    assert!(answered_at_2 > 0, "some schedule delivers the answer first");
}

#[cfg(stageleft_runtime)]
/// The dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)`: over the
/// run it emits every distinct arrival, whatever the batching, so it is not fragile.
#[test]
fn dedup_against_own_history_is_robust() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (send, input) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let arrivals = input.send(&p2, TCP.fail_stop().bincode());
    let fresh = sliced! {
        let arrived = use::batch(arrivals, nondet!(/** test */));
        let mut seen = use::state(|l| {
            l.source_iter(q!(Vec::<u32>::new())).weaken_ordering::<NoOrder>()
        });
        let fresh = arrived.weaken_ordering::<NoOrder>().unique().filter_not_in(seen.clone());
        seen = seen.chain(fresh.clone());
        fresh
    };
    let _out = fresh.sim_output();
    let (sim, sites) = compile(flow.sim());
    assert!(
        sites
            .iter()
            .all(|s| s.op != "difference" && s.op != "cross_singleton"),
        "{sites:?}"
    );
    let runs = observe(&sim, async || {
        send.send(1);
        send.send(1);
        send.send(2);
    });
    assert_robust(&runs);
}

#[cfg(stageleft_runtime)]
/// `anti_join` against a negative side unrelated to the output: whether the negative side holds
/// a key at that tick is the schedule's, so the step is fragile even though the output's own
/// lineage never names the negative side.
#[test]
fn anti_join_is_fragile() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (x_send, xs) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (y_send, ys) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let xs = xs.send(&p2, TCP.fail_stop().bincode().name("xs"));
    let ys = ys.send(&p2, TCP.fail_stop().bincode().name("ys"));
    let tick = p2.tick();
    let negative = ys.batch(&tick, nondet!(/** test */));
    // On one line, so the step's site is the line `line!()` reports.
    #[rustfmt::skip]
    let (kept, line) = (xs.batch(&tick, nondet!(/** test */)).map(q!(|x| (x, ()))).anti_join(negative), line!());
    let _out = kept.map(q!(|(x, ())| x)).all_ticks().sim_output();
    let (sim, sites) = compile(flow.sim());
    let anti_join = site(&sites, "anti_join", line);
    let runs = observe(&sim, async || {
        x_send.send(1);
        y_send.send(2);
    });
    assert_all_pass(&runs, anti_join);
}

#[cfg(stageleft_runtime)]
/// A `flat_map` closure reading state through `by_mut`: what it emits depends on the state's
/// contents at that moment, so it is fragile.
#[test]
fn closure_over_state_is_fragile() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (send, input) = p1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let arrivals = input.send(&p2, TCP.fail_stop().bincode());
    let line = std::cell::Cell::new(0);
    let out = sliced! {
        let batch = use::batch(arrivals, nondet!(/** test */));
        let mut count = use::state(|l| l.singleton(q!(0u32)));
        let count_mut = count.by_mut();
        // On one line, so the closure's site is the line `line!()` reports.
        line.set(line!()); let out = batch.flat_map_ordered(q!(|x| { *count_mut += 1; vec![x + *count_mut] }));
        out
    };
    let _out = out.sim_output();
    let (sim, sites) = compile(flow.sim());
    let closure = site(&sites, "closure_over_state", line.get());
    let runs = observe(&sim, async || {
        send.send(10);
        send.send(20);
    });
    assert_all_pass(&runs, closure);
}

#[cfg(stageleft_runtime)]
/// A join outside any tick, of two network streams: every pair is produced once both records
/// have arrived, in any order, so it is robust.
#[test]
fn top_level_join_is_robust() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (a_send, a) = p1.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let (b_send, b) = p1.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let a = a.send(&p2, TCP.fail_stop().bincode().name("a"));
    let b = b.send(&p2, TCP.fail_stop().bincode().name("b"));
    let _out = a.join(b).map(q!(|(k, (x, y))| k + x + y)).sim_output();
    let (sim, sites) = compile(flow.sim());
    assert!(sites.is_empty(), "{sites:?}");
    let runs = observe(&sim, async || {
        a_send.send((1, 10));
        b_send.send((1, 100));
    });
    assert_robust(&runs);
}

#[cfg(stageleft_runtime)]
/// A join of two batches inside a tick: the two records may be released in different ticks
/// and never meet, so it is fragile.
#[test]
fn tick_join_is_fragile() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (a_send, a) = p1.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let (b_send, b) = p1.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let a = a.send(&p2, TCP.fail_stop().bincode().name("a"));
    let b = b.send(&p2, TCP.fail_stop().bincode().name("b"));
    let tick = p2.tick();
    let right = b.batch(&tick, nondet!(/** test */));
    // On one line, so the step's site is the line `line!()` reports.
    #[rustfmt::skip]
    let (joined, line) = (a.batch(&tick, nondet!(/** test */)).join(right), line!());
    let _out = joined
        .map(q!(|(k, (x, y))| k + x + y))
        .all_ticks()
        .sim_output();
    let (sim, sites) = compile(flow.sim());
    // A join of two bounded inputs lowers to a `JoinHalf`.
    let join = site(&sites, "join|join_half", line);
    let runs = observe(&sim, async || {
        a_send.send((1, 10));
        b_send.send((1, 100));
    });
    assert_all_pass(&runs, join);
    // The race is real: some schedule releases the two in different ticks.
    assert!(runs.iter().any(|r| r.is_empty()));
}
