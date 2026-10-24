import re
p = 'hydrofrac/src/provenance/fragile.rs'
s = open(p).read()

def rep(old, new, count=1):
    global s
    assert s.count(old) == count, (old[:80], s.count(old))
    s = s.replace(old, new)

# ---- module docs
rep("""//! - plumbing that passes every record through unchanged (casts, tees, references, atomic
//!   brackets, consistency assertions, counters, `observe_nondet`, cycle sources, `all_ticks`);""",
"""//! - plumbing that passes every record through unchanged (casts, tees, references, the end of an
//!   atomic region, consistency assertions, counters, `observe_nondet`, cycle sources,
//!   `all_ticks`);""")
rep("""//! - networks, a `batch` of a stream, and sources that are the harness's inputs or fixed
//!   (iterators, singleton sources, cluster members, external inputs);""",
"""//! - networks; a `batch` (or `atomic`) of a stream into a tick that snapshots no singleton (see
//!   below); sources that are the harness's inputs or fixed (top-level iterators and singleton
//!   sources, cluster members, external inputs);""")
rep("""//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::dedup_edge`), and the `defer_tick` that carries its `seen`
//!   to the next tick: that edge only feeds the negation's negative side, whose tags the rewrite
//!   drops, and the idiom's output over the run does not depend on how many ticks run;""",
"""//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::dedup_idiom`), the `defer_tick` that carries its `seen` to
//!   the next tick, and the initial state's own nodes, provided nothing else reads `seen`, the
//!   carried edge or the initial state: then their records only reach the negation's negative
//!   side, whose tags the rewrite drops, and the idiom's output over the run does not depend on
//!   how many ticks run;""")
rep("""//! Everything else is fragile. That includes: negation, `cross_singleton` (so `filter_if`),""",
"""//! Everything else is fragile. That includes: any operator whose closure (or a source's
//! expression) names shared or outside state (`static`, `Cell`, `Mutex`, atomics, `std::time`,
//! `std::fs`, randomness, ..., [`names_impure`]): such a closure may give different results for
//! the same input, so lineage's assumption that each output is a function of its inputs (A3) is
//! not checked for it; a constant inside a tick (`tick.singleton`, `optional_first_tick`, a
//! tick's `source_iter`): it is emitted in every tick that runs, and only if one runs; a `batch`
//! (or `atomic`) into a tick that also snapshots a singleton: the simulator runs that tick only
//! once the singleton has a value (`hydro_lang/src/sim/runtime.rs`, `SingletonHook::is_ready`),
//! so the batched records exist only if whatever feeds the singleton arrived;
//! negation, `cross_singleton` (so `filter_if`),""")

# ---- imports
rep("""use hydro_lang::compile::ir::{CollectionKind, HydroNode, HydroRoot, HydroSource};

use super::choices::{cell, closures};
use super::rewrite::node_key;
use crate::observe::ec_label;""",
"""use hydro_lang::compile::ir::{CollectionKind, HydroNode, HydroRoot, HydroSource};
use hydro_lang::location::dynamic::LocationId;
use proc_macro2::{TokenStream, TokenTree};

use super::choices::{cell, closures};
use super::rewrite::node_key;
use crate::observe::{ec_label, tick_of};""")

# ---- View struct
rep("""/// A read-only view of the IR, by node address.
struct View<'a> {
    ir: &'a [HydroRoot],
    /// Each tick cycle's sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
    /// The `DeferTick`s that carry a dedup idiom's state to the next tick ([`Self::dedup_edge`]).
    dedup_edges: HashSet<*const HydroNode>,
}
""",
"""/// Who reads a node's records: another node, or a root (by index in the IR).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reader {
    Node(*const HydroNode),
    Root(usize),
}

/// A read-only view of the IR, by node address.
struct View<'a> {
    ir: &'a [HydroRoot],
    /// Each tick cycle's sink input, and the index of its `CycleSink` root.
    cycle_sinks: HashMap<CycleId, (*const HydroNode, usize)>,
    /// The readers of each node, as [`inputs`] and the closures' state references list them.
    readers: HashMap<*const HydroNode, Vec<Reader>>,
    /// The `DeferTick`s that carry a dedup idiom's state to the next tick ([`Self::dedup_idiom`]).
    dedup_edges: HashSet<*const HydroNode>,
    /// The nodes of the dedup idioms' initial states ([`Self::dedup_idiom`]).
    dedup_inits: HashSet<*const HydroNode>,
    /// The ticks that snapshot a singleton (a `batch` or `atomic` of a `Singleton` into them).
    snapshot_ticks: HashSet<LocationId>,
}
""")

# ---- impure helpers after reads_state
rep("""fn reads_state(node: &HydroNode) -> bool {
    closures(node)
        .iter()
        .any(|f| !f.singleton_refs().is_empty())
}
""",
"""fn reads_state(node: &HydroNode) -> bool {
    closures(node)
        .iter()
        .any(|f| !f.singleton_refs().is_empty())
}

/// Names whose use in a closure can make its result depend on more than its input: shared or
/// interior-mutable state, `unsafe`, randomness and clocks.
const IMPURE_NAMES: &[&str] = &[
    "static", "thread_local", "unsafe", "Cell", "RefCell", "UnsafeCell", "OnceCell", "OnceLock",
    "LazyCell", "LazyLock", "Mutex", "RwLock", "rand", "random", "thread_rng", "Instant",
    "SystemTime", "tokio",
];

/// Modules of `std` (or `core`) whose functions read or change state outside the closure.
const IMPURE_MODULES: &[&str] = &["env", "fs", "io", "net", "process", "ptr", "thread", "time"];

/// Whether `tokens` (a closure or a source expression) names any of [`IMPURE_NAMES`], an
/// `Atomic*` type, or one of [`IMPURE_MODULES`] (as `std::fs`, `core::ptr`, ...). A textual,
/// conservative guard for A3: it cannot see into functions the closure calls, and it flags some
/// pure closures (a variable named `random`, say), which only costs schedules.
pub(crate) fn names_impure(tokens: TokenStream) -> bool {
    fn flatten(tokens: TokenStream, out: &mut Vec<TokenTree>) {
        for t in tokens {
            match t {
                TokenTree::Group(g) => flatten(g.stream(), out),
                t => out.push(t),
            }
        }
    }
    let mut flat = vec![];
    flatten(tokens, &mut flat);
    let ident = |i: usize| match flat.get(i) {
        Some(TokenTree::Ident(id)) => Some(id.to_string()),
        _ => None,
    };
    let punct = |i: usize, c: char| matches!(flat.get(i), Some(TokenTree::Punct(p)) if p.as_char() == c);
    (0..flat.len()).any(|i| {
        let Some(name) = ident(i) else {
            return false;
        };
        // `'static` is a lifetime, not a static item.
        if i > 0 && punct(i - 1, '\\'') {
            return false;
        }
        let module = || {
            punct(i + 1, ':')
                && punct(i + 2, ':')
                && ident(i + 3).is_some_and(|m| IMPURE_MODULES.contains(&m.as_str()))
        };
        IMPURE_NAMES.contains(&name.as_str())
            || name.starts_with("Atomic")
            || (name == "std" || name == "core") && module()
    })
}

/// Whether one of `node`'s closures, or its source expression, [`names_impure`]. An expression
/// that does not print as tokens counts as impure.
fn impure(node: &HydroNode) -> bool {
    let mut texts: Vec<String> = closures(node).iter().map(|f| format!("{f:?}")).collect();
    match node {
        HydroNode::SingletonSource { value, .. } => texts.push(format!("{value:?}")),
        HydroNode::Source {
            source: HydroSource::Iter(expr),
            ..
        } => texts.push(format!("{expr:?}")),
        _ => {}
    }
    texts
        .iter()
        .any(|t| t.parse::<TokenStream>().map_or(true, names_impure))
}

/// Whether `node`'s records are a stream (or keyed stream), not a snapshot of a scalar.
fn is_stream(node: &HydroNode) -> bool {
    !matches!(
        node.metadata().collection_kind,
        CollectionKind::Singleton { .. }
            | CollectionKind::Optional { .. }
            | CollectionKind::KeyedSingleton { .. }
    )
}
""")

# ---- View::new
rep("""    fn new(ir: &'a [HydroRoot]) -> Self {
        let cycle_sinks = ir
            .iter()
            .filter_map(|root| match root {
                HydroRoot::CycleSink {
                    cycle_id, input, ..
                } => Some((*cycle_id, std::ptr::from_ref(&**input))),
                _ => None,
            })
            .collect();
        let mut view = View {
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
    }
""",
"""    fn new(ir: &'a [HydroRoot]) -> Self {
        let cycle_sinks = ir
            .iter()
            .enumerate()
            .filter_map(|(index, root)| match root {
                HydroRoot::CycleSink {
                    cycle_id, input, ..
                } => Some((*cycle_id, (std::ptr::from_ref(&**input), index))),
                _ => None,
            })
            .collect();
        let mut view = View {
            ir,
            cycle_sinks,
            readers: HashMap::new(),
            dedup_edges: HashSet::new(),
            dedup_inits: HashSet::new(),
            snapshot_ticks: HashSet::new(),
        };
        let nodes = view.nodes();
        let mut readers: HashMap<*const HydroNode, Vec<Reader>> = HashMap::new();
        for (index, root) in ir.iter().enumerate() {
            let mut read = |ptr| readers.entry(ptr).or_default().push(Reader::Root(index));
            read(std::ptr::from_ref(root.input()));
            if let HydroRoot::ForEach { f, .. } = root {
                f.singleton_refs()
                    .iter()
                    .filter_map(|(r, _)| cell(r))
                    .for_each(&mut read);
            }
        }
        for &ptr in &nodes {
            // SAFETY: as in `classify`.
            let node = unsafe { &*ptr };
            let state = closures(node)
                .into_iter()
                .flat_map(|f| f.singleton_refs().iter().filter_map(|(r, _)| cell(r)));
            for input in inputs(node).into_iter().chain(state) {
                readers.entry(input).or_default().push(Reader::Node(ptr));
            }
            if let HydroNode::Batch { inner, metadata } | HydroNode::BeginAtomic { inner, metadata } =
                node
                && matches!(
                    inner.metadata().collection_kind,
                    CollectionKind::Singleton { .. }
                )
                && let Some(tick) = tick_of(&metadata.location_id)
            {
                view.snapshot_ticks.insert(tick.clone());
            }
        }
        view.readers = readers;
        for &ptr in &nodes {
            // SAFETY: as in `classify`.
            if let Some((edge, init)) = view.dedup_idiom(unsafe { &*ptr }) {
                view.dedup_edges.insert(edge);
                view.dedup_inits.extend(init);
            }
        }
        view
    }

    /// The nodes and roots that read `ptr`'s records, looking through `Tee`s.
    fn consumers(&self, ptr: *const HydroNode) -> Vec<Reader> {
        let (mut seen, mut stack, mut out) = (HashSet::new(), vec![ptr], vec![]);
        while let Some(ptr) = stack.pop() {
            if !seen.insert(ptr) {
                continue;
            }
            for &reader in self.readers.get(&ptr).into_iter().flatten() {
                match reader {
                    // SAFETY: as in `classify`.
                    Reader::Node(r) if matches!(unsafe { &*r }, HydroNode::Tee { .. }) => {
                        stack.push(r);
                    }
                    reader => out.push(reader),
                }
            }
        }
        out
    }

    /// Whether only `allowed` read `ptr`'s records.
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr).iter().all(|r| allowed.contains(r))
    }

    /// `ptr` and every node it is computed from, through shared nodes.
    fn upstream(&self, ptr: *const HydroNode) -> Vec<*const HydroNode> {
        let (mut seen, mut stack, mut out) = (HashSet::new(), vec![ptr], vec![]);
        while let Some(ptr) = stack.pop() {
            if seen.insert(ptr) {
                out.push(ptr);
                // SAFETY: as in `classify`.
                stack.extend(inputs(unsafe { &*ptr }));
            }
        }
        out
    }
""")

# ---- fragile(): impurity, tick constants, batch beside snapshot
rep("""    fn fragile(&self, node: &HydroNode) -> Option<&'static str> {
        use HydroNode::*;
        if reads_state(node) {
            // A closure that reads Hydro state sees whatever the state holds at that moment.
            return Some("closure_over_state");
        }
        let top_level = node.metadata().location_id.is_top_level();
""",
"""    fn fragile(&self, node: &HydroNode) -> Option<&'static str> {
        use HydroNode::*;
        if let Placeholder = node {
            return None;
        }
        if impure(node) {
            // The closure may compute different records from the same inputs (A3).
            return Some("impure_closure");
        }
        if reads_state(node) {
            // A closure that reads Hydro state sees whatever the state holds at that moment.
            return Some("closure_over_state");
        }
        let top_level = node.metadata().location_id.is_top_level();
        let tick = tick_of(&node.metadata().location_id);
        // A constant at top level is emitted once. Inside a tick it is emitted in every tick that
        // runs, so its records over the run depend on whether (and, downstream, how often) the
        // tick ran, unless it is a dedup idiom's initial state, which only that idiom reads.
        let fixed = tick.is_none() || self.dedup_inits.contains(&std::ptr::from_ref(node));
        // The simulator runs a tick that snapshots a singleton only once that singleton has a
        // value, so a record batched into it exists only if what feeds the singleton arrived.
        let beside_snapshot = tick.is_some_and(|t| self.snapshot_ticks.contains(t));
""")
rep("""            | Reference { .. }
            | BeginAtomic { .. }
            | EndAtomic { .. }""",
"""            | Reference { .. }
            | EndAtomic { .. }""")
rep("""            // A batch of a stream decides when a record enters a tick, not whether.
            Batch { inner, .. } => !matches!(
                inner.metadata().collection_kind,
                CollectionKind::Singleton { .. }
                    | CollectionKind::Optional { .. }
                    | CollectionKind::KeyedSingleton { .. }
            ),""",
"""            // A batch of a stream decides when a record enters a tick, not whether, if nothing
            // else can keep the tick from running. The simulator lowers `atomic` to a batch
            // (`hydro_lang/src/sim/builder.rs`, `begin_atomic`).
            Batch { inner, .. } | BeginAtomic { inner, .. } => {
                if is_stream(inner) && !beside_snapshot {
                    return None;
                }
                return Some(if is_stream(inner) {
                    "batch_beside_snapshot"
                } else {
                    "snapshot"
                });
            }""")
rep("""            Source { source, .. } => {
                return (!matches!(
                    source,
                    HydroSource::Iter(_)
                        | HydroSource::ClusterMembers(..)
                        | HydroSource::ExternalNetwork()
                ))
                .then_some("source");
            }
            SingletonSource { .. } | ExternalInput { .. } => true,""",
"""            Source { source, .. } => {
                return match source {
                    HydroSource::ClusterMembers(..) | HydroSource::ExternalNetwork() => None,
                    HydroSource::Iter(_) if fixed => None,
                    HydroSource::Iter(_) => Some("tick_constant"),
                    _ => Some("source"),
                };
            }
            SingletonSource { .. } => return (!fixed).then_some("tick_constant"),
            ExternalInput { .. } => true,""")
rep("""            Difference { .. } => self.dedup_edge(node).is_some(),""",
"""            Difference { .. } => self.dedup_idiom(node).is_some(),""")
rep("""        // Same records in every schedule if every input is (every node here has an input).
        if inputs(node).into_iter().all(|i| self.constant(i)) {
            return None;
        }""",
"""        // Same records in every schedule if every input is (every node here has an input). The
        // constants inside a tick are tagged at their source, so their tags still reach the
        // output. Not for `defer_tick`, whose records exist only if a later tick runs.
        if !matches!(node, DeferTick { .. }) && inputs(node).into_iter().all(|i| self.constant(i))
        {
            return None;
        }""")
rep("""            ChainFirst { .. } => "chain_first",
            Batch { .. } => "snapshot",
            DeferTick { .. } => "defer_tick",""",
"""            ChainFirst { .. } => "chain_first",
            DeferTick { .. } => "defer_tick",""")

# ---- dedup_edge -> dedup_idiom with exclusivity
rep("""    /// Ported from the research branch's `Cones::self_difference`
    /// (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    fn dedup_edge(&self, node: &HydroNode) -> Option<*const HydroNode> {""",
"""    /// Ported from the research branch's `Cones::self_difference`
    /// (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    ///
    /// Only if nothing else reads the idiom's state: `seen` is read only by the negation and the
    /// cycle's sink, the sink only by its `CycleSink`, the cycle source only by the carrying
    /// `DeferTick`, which only `seen` reads, and the initial state's nodes only by each other and
    /// `seen`. Another reader of any of them (`seen.clone().all_ticks()`, say) would see records
    /// that exist only if a later tick runs. Returns the carrying `DeferTick` and the nodes of the
    /// initial state.
    fn dedup_idiom(&self, node: &HydroNode) -> Option<(*const HydroNode, Vec<*const HydroNode>)> {""")
rep("""        let &sink = self.cycle_sinks.get(&cycle)?;
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*strip(sink) }) else {
            return None;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        ((parts == (state, me) || parts == (me, state)) && self.constant(init)).then_some(defer)
    }""",
"""        let &(sink, root) = self.cycle_sinks.get(&cycle)?;
        let sink = strip(sink);
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*sink }) else {
            return None;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        if !((parts == (state, me) || parts == (me, state)) && self.constant(init)) {
            return None;
        }
        // SAFETY: as in `strip`.
        let HydroNode::DeferTick { input, .. } = (unsafe { &*defer }) else {
            return None;
        };
        let source = strip(std::ptr::from_ref(&**input));
        let init_nodes = self.upstream(init);
        let mut in_init: Vec<Reader> = init_nodes.iter().map(|&n| Reader::Node(n)).collect();
        in_init.push(Reader::Node(state));
        let exclusive = self.read_only_by(state, &[Reader::Node(me), Reader::Node(sink)])
            && self.read_only_by(sink, &[Reader::Root(root)])
            && self.read_only_by(source, &[Reader::Node(defer)])
            && self.read_only_by(defer, &[Reader::Node(state)])
            && init_nodes.iter().all(|&n| self.read_only_by(n, &in_init));
        exclusive.then_some((defer, init_nodes))
    }""")
open(p, 'w').write(s)
print("ok")
