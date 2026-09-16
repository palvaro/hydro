//! Passive per-edge record counting at the simulator's tick-boundary hooks.
//!
//! Every `batch` (and keyed `batch`) in a simulated program becomes a hook that buffers records
//! and, once per tick of its slice, decides how many of them to release. The hook already holds
//! the concrete `Vec<T>` it is about to release and the source location of the operator, so the
//! scheduler can count records crossing that edge without touching the program or its IR. This
//! module is that counter, plus the small piece of context a scheduling policy needs to know
//! *which* hook is currently asking it for a decision.
//!
//! Both live in thread-locals on the host side of the simulation (the scheduler in
//! [`super::compiled`] runs on the test thread; the compiled program is a separate dylib whose
//! statics are not shared, so nothing here may be written from generated code).
//!
//! Nothing is recorded unless a run is wrapped in [`count_edges`].

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

/// Source location of a hook's operator, the text of that source line, and the
/// `std::any::type_name` of the records it buffers, as the hooks in [`super::runtime`] carry
/// them. Several `batch`es inside one `sliced!` block report the block's location, so the
/// element type is part of an edge's identity.
pub type EdgeLocation = (&'static str, &'static str, &'static str);

/// The key under which an edge is counted and matched by scheduling policies:
/// `"<file>:<line>:<col>#<index> <element type>"`, where `index` is the hook's position among
/// the hooks of its tick (two batches of the same type in one `sliced!` block, e.g. two `()`
/// timers, differ only there).
pub fn edge_key(location: EdgeLocation, index: usize) -> String {
    format!("{}#{} <{}>", location.0, index, location.2)
}

/// What one tick-boundary hook released over a run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeCount {
    /// The source line of the operator (for reports).
    pub source_line: String,
    /// Total records released across the edge.
    pub records: u64,
    /// Decisions (ticks of the slice) in which the hook was asked, including ones that released
    /// nothing.
    pub decisions: u64,
    /// Decisions that released at least one record.
    pub nonempty_decisions: u64,
}

/// Per-edge counts for one simulated run, keyed by [`edge_key`] (the operator's source location
/// `file:line:col`, relative to the crate under test, and its element type).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeCounts(pub BTreeMap<String, EdgeCount>);

impl EdgeCounts {
    /// The count for the unique edge whose location matches `pred`. Panics if the match is not
    /// unique, since an experiment that names the wrong edge should fail loudly.
    pub fn edge(&self, pred: impl Fn(&str) -> bool) -> &EdgeCount {
        let mut found = self.0.iter().filter(|(loc, _)| pred(loc));
        let (_, count) = found
            .next()
            .expect("no counted edge matches the predicate");
        assert!(
            found.next().is_none(),
            "more than one counted edge matches the predicate"
        );
        count
    }

    /// Records released across the unique edge matching `pred`.
    pub fn records(&self, pred: impl Fn(&str) -> bool) -> u64 {
        self.edge(pred).records
    }
}

/// What a scheduling policy is told about the hook currently asking for a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookContext {
    /// The edge the hook stands for.
    pub location: EdgeLocation,
    /// The hook's position among the hooks of its tick (see [`edge_key`]).
    pub index: usize,
    /// The cluster member the hook belongs to, if the location is a cluster.
    pub member: Option<u32>,
    /// Records buffered in the hook before this decision.
    pub buffered: usize,
    /// A serial number distinguishing this decision from every other decision in the run, so
    /// a policy can tell whether a question continues the current decision or starts a new one.
    pub decision: u64,
}

thread_local! {
    static RECORDER: RefCell<Option<EdgeCounts>> = const { RefCell::new(None) };
    static CURRENT_HOOK: Cell<Option<HookContext>> = const { Cell::new(None) };
    static DECISIONS: Cell<u64> = const { Cell::new(0) };
    static EMPTY_TICKS: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with the scheduler's forcing rule switched off on this thread: normally, when no hook
/// of a tick has released anything, the last undecided hook is forced to release at least one
/// item, so a tick never runs empty and a delivery can only be held while another hook of the
/// same tick (a metered clock) releases. Under this option a tick whose hooks all decide to hold
/// runs with no new input (state carry only) and stays runnable, so every `batch` edge can be
/// held or paced by the policy, including one that is alone in its tick. The scheduler does not
/// quiesce while anything is held, so every policy in force must release eventually.
pub fn with_empty_ticks_allowed<R>(f: impl FnOnce() -> R) -> R {
    let previous = EMPTY_TICKS.with(|e| e.replace(true));
    let result = f();
    EMPTY_TICKS.with(|e| e.set(previous));
    result
}

/// Whether [`with_empty_ticks_allowed`] is in force on this thread.
pub(crate) fn empty_ticks_allowed() -> bool {
    EMPTY_TICKS.with(|e| e.get())
}

/// Runs `f` with edge counting enabled on this thread and returns what was counted. Nested
/// calls are not supported (the inner run replaces the outer recorder).
pub fn count_edges<R>(f: impl FnOnce() -> R) -> (R, EdgeCounts) {
    RECORDER.with(|r| *r.borrow_mut() = Some(EdgeCounts::default()));
    let result = f();
    let counts = RECORDER.with(|r| r.borrow_mut().take()).unwrap_or_default();
    (result, counts)
}

/// Called by the scheduler just before a hook releases `records` items across its edge.
pub(crate) fn record_release(location: EdgeLocation, index: usize, records: usize) {
    RECORDER.with(|r| {
        if let Some(counts) = r.borrow_mut().as_mut() {
            let entry = counts.0.entry(edge_key(location, index)).or_default();
            if entry.source_line.is_empty() {
                entry.source_line = location.1.trim().to_owned();
            }
            entry.records += records as u64;
            entry.decisions += 1;
            if records > 0 {
                entry.nonempty_decisions += 1;
            }
        }
    });
}

/// The hook currently being asked for a decision, if the scheduler is inside a hook's
/// `autonomous_decision` and the hook stands for an edge. A scheduling policy (a bolero
/// `Driver`) reads this to apply a per-edge rule; see [`super::hold_schedule`].
pub fn current_hook() -> Option<HookContext> {
    CURRENT_HOOK.with(|c| c.get())
}

/// Runs `f` (one hook's decision) with [`current_hook`] describing that hook, or unset if the
/// hook does not stand for an edge.
pub(crate) fn with_current_hook<R>(
    hook: Option<(EdgeLocation, usize)>,
    index: usize,
    member: Option<u32>,
    f: impl FnOnce() -> R,
) -> R {
    let context = hook.map(|(location, buffered)| HookContext {
        location,
        index,
        member,
        buffered,
        decision: DECISIONS.with(|d| {
            let next = d.get() + 1;
            d.set(next);
            next
        }),
    });
    let previous = CURRENT_HOOK.with(|c| c.replace(context));
    let result = f();
    CURRENT_HOOK.with(|c| c.set(previous));
    result
}
