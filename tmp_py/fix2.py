p='hydrofrac/src/provenance/fragile.rs'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:80]
    s=s.replace(a,b)
# module docs
rep('''//! - plumbing that passes every record through unchanged (casts, tees, references, atomic
//!   brackets, consistency assertions, counters, `observe_nondet`, cycle sources, `defer_tick`,
//!   `all_ticks`);''','''//! - plumbing that passes every record through unchanged (casts, tees, references, atomic
//!   brackets, consistency assertions, counters, `observe_nondet`, cycle sources, `all_ticks`);''')
rep('''//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::is_dedup`);''','''//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::dedup_edge`), and the `defer_tick` that carries its `seen`
//!   to the next tick: that edge only feeds the negation's negative side, whose tags the rewrite
//!   drops, and the idiom's output over the run does not depend on how many ticks run;''')
rep('''//! inside a tick; snapshots (a `batch` of a singleton, optional or keyed singleton); closures that
//! read Hydro state by reference; and other sources (timers, intervals, streams).''','''//! inside a tick; snapshots (a `batch` of a singleton, optional or keyed singleton); closures that
//! read Hydro state by reference; other sources (timers, intervals, streams); and `defer_tick`
//! (but the dedup idiom's): a deferred record reaches the next tick only if a next tick runs at
//! that location, and the simulator runs a tick only when some hook releases something
//! (`hydro_lang/src/sim/compiled.rs`, `SimTick::can_run`), so whether it is ever emitted depends
//! on how the deliveries were batched.''')
# View fields
rep('''struct View<'a> {
    ir: &'a [HydroRoot],
    /// Each tick cycle's sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
}''','''struct View<'a> {
    ir: &'a [HydroRoot],
    /// Each tick cycle's sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
    /// The `DeferTick`s that carry a dedup idiom's state to the next tick ([`Self::dedup_edge`]).
    dedup_edges: HashSet<*const HydroNode>,
}''')
rep('''        View { ir, cycle_sinks }
    }''','''        let mut view = View {
            ir,
            cycle_sinks,
            dedup_edges: HashSet::new(),
        };
        let edges = view
            .nodes()
            .into_iter()
            // SAFETY: as in `classify`.
            .filter_map(|ptr| view.dedup_edge(unsafe { &*ptr }))
            .collect();
        view.dedup_edges = edges;
        view
    }''')
rep('''            | UnboundSingleton { .. }
            | CycleSource { .. }
            | DeferTick { .. }
            | YieldConcat { .. } => true,''','''            | UnboundSingleton { .. }
            | CycleSource { .. }
            | YieldConcat { .. } => true,
            // A deferred record exists only if a later tick runs (see the module docs), except on
            // the dedup idiom's state edge, which only feeds a negation's negative side.
            DeferTick { .. } => self.dedup_edges.contains(&std::ptr::from_ref(node)),''')
rep('''            // The dedup idiom: every record of `pos` not in the constant initial state.
            Difference { .. } => self.is_dedup(node),''','''            // The dedup idiom: every record of `pos` not in the constant initial state.
            Difference { .. } => self.dedup_edge(node).is_some(),''')
rep('''            Batch { .. } => "snapshot",
            _ => "unlisted",''','''            Batch { .. } => "snapshot",
            DeferTick { .. } => "defer_tick",
            _ => "unlisted",''')
# is_dedup -> dedup_edge returning the carried DeferTick
rep('''    /// Whether `node` is `filter_not_in` against its own history: a `Difference` whose `neg` is
    /// a tick state that starts as a constant and then accumulates the `Difference`'s own
    /// output (`seen = seen.chain(fresh)` with `fresh = arrived.filter_not_in(seen)`, the idiom
    /// of `use::state`). Its output over the run is then every record of `pos` not in the
    /// initial state, whatever ticks they arrive in. Ported from the research branch's
    /// `Cones::self_difference` (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    fn is_dedup(&self, node: &HydroNode) -> bool {
        let HydroNode::Difference { neg, .. } = node else {
            return false;
        };''','''    /// If `node` is `filter_not_in` against its own history, the `DeferTick` that carries that
    /// history to the next tick. That is a `Difference` whose `neg` is a tick state that starts
    /// as a constant and then accumulates the `Difference`'s own output (`seen = seen.chain(fresh)`
    /// with `fresh = arrived.filter_not_in(seen)`, the idiom of `use::state`). Its output over the
    /// run is then every record of `pos` not in the initial state, whatever ticks they arrive in.
    /// Ported from the research branch's `Cones::self_difference`
    /// (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    fn dedup_edge(&self, node: &HydroNode) -> Option<*const HydroNode> {
        let HydroNode::Difference { neg, .. } = node else {
            return None;
        };''')
rep('''        let HydroNode::Chain { first, second, .. } = (unsafe { &*state }) else {
            return false;
        };
        // The cycle a side carries over from the previous tick, if it does.
        let carried = |side: &HydroNode| -> Option<CycleId> {
            // SAFETY: as in `strip`.
            let HydroNode::DeferTick { input, .. } = (unsafe { &*strip(std::ptr::from_ref(side)) })
            else {
                return None;
            };
            // SAFETY: as in `strip`.
            match unsafe { &*strip(std::ptr::from_ref(&**input)) } {
                HydroNode::CycleSource { cycle_id, .. } => Some(*cycle_id),
                _ => None,
            }
        };
        let (cycle, init) = match (carried(first), carried(second)) {
            (Some(c), _) => (c, std::ptr::from_ref(&**second)),
            (None, Some(c)) => (c, std::ptr::from_ref(&**first)),
            (None, None) => return false,
        };
        let Some(&sink) = self.cycle_sinks.get(&cycle) else {
            return false;
        };
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*strip(sink) }) else {
            return false;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        (parts == (state, me) || parts == (me, state)) && self.constant(init)
    }''','''        let HydroNode::Chain { first, second, .. } = (unsafe { &*state }) else {
            return None;
        };
        // The cycle a side carries over from the previous tick, and its `DeferTick`, if it does.
        let carried = |side: &HydroNode| -> Option<(CycleId, *const HydroNode)> {
            let defer = strip(std::ptr::from_ref(side));
            // SAFETY: as in `strip`.
            let HydroNode::DeferTick { input, .. } = (unsafe { &*defer }) else {
                return None;
            };
            // SAFETY: as in `strip`.
            match unsafe { &*strip(std::ptr::from_ref(&**input)) } {
                HydroNode::CycleSource { cycle_id, .. } => Some((*cycle_id, defer)),
                _ => None,
            }
        };
        let ((cycle, defer), init) = match (carried(first), carried(second)) {
            (Some(c), _) => (c, std::ptr::from_ref(&**second)),
            (None, Some(c)) => (c, std::ptr::from_ref(&**first)),
            (None, None) => return None,
        };
        let &sink = self.cycle_sinks.get(&cycle)?;
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*strip(sink) }) else {
            return None;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        ((parts == (state, me) || parts == (me, state)) && self.constant(init)).then_some(defer)
    }''')
open(p,'w').write(s)

p='hydrofrac/src/check/brute_tests.rs'
s=open(p).read()
a=s.index('/// PROBE (soundness review): a record that reaches the output only through `defer_tick` is')
s=s[:a]+'''/// A record that reaches the output only through `defer_tick` is emitted only if a later tick
/// runs at that member, and the simulator runs a tick only when a hook releases something. Two
/// values to two members, each batched, deferred a tick and emitted: a member that batched them
/// apart emits the first (the second tick runs), one that batched them together emits nothing.
/// Before `defer_tick` was fragile, this program counted as fragile-free and was certified;
/// brute force broke it with the empty set.
#[cfg(stageleft_runtime)]
#[test]
fn deferred_records_are_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let delayed = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .defer_tick()
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member emits what it received, a tick later */
        ));
    let _out = delayed.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "defer_tick"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    agree("deferred records", &cross);
    let broken = cross
        .results
        .results()
        .iter()
        .filter_map(|l| l.verdict.counterexample())
        .any(|c| c.faults.is_empty());
    assert!(broken, "the members disagree with no fault: {cross}");
}
'''
open(p,'w').write(s)
