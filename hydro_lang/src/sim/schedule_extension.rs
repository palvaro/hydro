//! Generic observation and control points for simulator scheduling.
//!
//! The simulator exposes hook identity and release events through this interface without knowing
//! which tool consumes them. With no extension installed every operation is a cheap no-op and
//! scheduling is unchanged. An extension is scoped to the current thread because the simulator's
//! driver and scheduler execute on the same current-thread runtime.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use dfir_rs::scheduled::metrics::DfirMetrics;

use super::runtime::HookKind;

/// Identity metadata for one tick-input hook.
#[derive(Debug, Clone, Copy)]
pub struct HookMetadata {
    /// Source location of the unsafe operator represented by the hook.
    pub location: &'static str,
    /// Position of the hook within its tick.
    pub index: usize,
    /// Rust item type carried by the hook, for diagnostics and stable identification.
    pub item_type: &'static str,
    /// Whether the hook batches records or observes a snapshot.
    pub kind: HookKind,
    /// Cluster member that owns the hook, or `None` for a process.
    pub member: Option<u32>,
}

/// Metadata for the hook decision currently being made.
#[derive(Debug, Clone, Copy)]
pub struct HookDecision {
    /// Hook whose scheduling decision is being made.
    pub hook: HookMetadata,
    /// Whether the simulator requires this hook to trigger its tick.
    pub forced: bool,
    /// Distinguishes successive decisions by the same hook. All questions asked while resolving
    /// one decision carry the same sequence number.
    pub sequence: u64,
}

/// Optional simulator scheduling instrumentation.
///
/// Implementations may suppress a hook as a tick trigger, observe released records, and collect
/// final DFIR metrics. The default methods preserve ordinary simulator behavior.
pub trait ScheduleExtension {
    /// Decides whether this hook may make its tick runnable. `default` is the hook's own result.
    fn allows_trigger(&self, _hook: HookMetadata, default: bool) -> bool {
        default
    }

    /// Observes records about to be released into a tick by a hook.
    fn hook_released(&self, _hook: HookMetadata, _records: usize) {}

    /// Observes final cumulative metrics for one DFIR instance.
    fn dfir_finished(&self, _location: &str, _member: Option<u32>, _metrics: &DfirMetrics) {}

    /// Reports scripted hooks that bypass ordinary driver decisions.
    fn scripted_hooks_present(&self, _count: usize) {}
}

/// Combines two independent scheduling extensions.
pub struct ChainedExtension {
    first: Rc<dyn ScheduleExtension>,
    second: Rc<dyn ScheduleExtension>,
}

impl ChainedExtension {
    /// Returns a shared extension that forwards each event to `first` and `second`.
    pub fn new(first: Rc<dyn ScheduleExtension>, second: Rc<dyn ScheduleExtension>) -> Rc<Self> {
        Rc::new(Self { first, second })
    }
}

impl ScheduleExtension for ChainedExtension {
    fn allows_trigger(&self, hook: HookMetadata, default: bool) -> bool {
        self.first.allows_trigger(hook, default) && self.second.allows_trigger(hook, default)
    }

    fn hook_released(&self, hook: HookMetadata, records: usize) {
        self.first.hook_released(hook, records);
        self.second.hook_released(hook, records);
    }

    fn dfir_finished(&self, location: &str, member: Option<u32>, metrics: &DfirMetrics) {
        self.first.dfir_finished(location, member, metrics);
        self.second.dfir_finished(location, member, metrics);
    }

    fn scripted_hooks_present(&self, count: usize) {
        self.first.scripted_hooks_present(count);
        self.second.scripted_hooks_present(count);
    }
}

thread_local! {
    static EXTENSION: RefCell<Option<Rc<dyn ScheduleExtension>>> = const { RefCell::new(None) };
    static CURRENT_DECISION: Cell<Option<HookDecision>> = const { Cell::new(None) };
    static DECISION_SEQUENCE: Cell<u64> = const { Cell::new(0) };
}

struct RestoreExtension(Option<Rc<dyn ScheduleExtension>>);

impl Drop for RestoreExtension {
    fn drop(&mut self) {
        EXTENSION.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

/// Runs `f` with `extension` installed on the current simulator thread. The previous extension is
/// restored even if `f` panics.
pub fn with_extension<R>(extension: Rc<dyn ScheduleExtension>, f: impl FnOnce() -> R) -> R {
    let previous = EXTENSION.with(|slot| slot.borrow_mut().replace(extension));
    let _restore = RestoreExtension(previous);
    f()
}

/// Whether any scheduling extension is installed.
#[inline]
pub(crate) fn is_active() -> bool {
    EXTENSION.with(|slot| slot.borrow().is_some())
}

#[inline]
fn active_extension() -> Option<Rc<dyn ScheduleExtension>> {
    EXTENSION.with(|slot| slot.borrow().clone())
}

#[inline]
pub(crate) fn allows_trigger(hook: HookMetadata, default: bool) -> bool {
    active_extension().map_or(default, |extension| extension.allows_trigger(hook, default))
}

struct RestoreDecision(Option<HookDecision>);

impl Drop for RestoreDecision {
    fn drop(&mut self) {
        CURRENT_DECISION.with(|slot| slot.set(self.0));
    }
}

/// Publishes typed hook context while the hook asks its driver one or more questions. The prior
/// context is restored even if the hook or driver panics.
pub(crate) fn with_hook_decision<R>(hook: HookMetadata, forced: bool, f: impl FnOnce() -> R) -> R {
    if !is_active() {
        return f();
    }
    let sequence = DECISION_SEQUENCE.with(|slot| {
        let next = slot.get().wrapping_add(1);
        slot.set(next);
        next
    });
    let decision = HookDecision {
        hook,
        forced,
        sequence,
    };
    let previous = CURRENT_DECISION.with(|slot| slot.replace(Some(decision)));
    let _restore = RestoreDecision(previous);
    f()
}

/// Returns the hook decision currently asking the schedule driver a question.
pub fn current_hook_decision() -> Option<HookDecision> {
    CURRENT_DECISION.with(Cell::get)
}

#[inline]
pub(crate) fn hook_released(hook: HookMetadata, records: usize) {
    if let Some(extension) = active_extension() {
        extension.hook_released(hook, records);
    }
}

#[inline]
pub(crate) fn dfir_finished(location: &str, member: Option<u32>, metrics: &DfirMetrics) {
    if let Some(extension) = active_extension() {
        extension.dfir_finished(location, member, metrics);
    }
}

#[inline]
pub(crate) fn scripted_hooks_present(count: usize) {
    if count > 0
        && let Some(extension) = active_extension()
    {
        extension.scripted_hooks_present(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_extension_is_a_no_op() {
        let hook = HookMetadata {
            location: "test.rs:1:1",
            index: 0,
            item_type: "u64",
            kind: HookKind::Batch,
            member: None,
        };
        assert!(!is_active());
        assert!(allows_trigger(hook, true));
        assert!(!allows_trigger(hook, false));
        assert_eq!(with_hook_decision(hook, false, || 42), 42);
        assert!(current_hook_decision().is_none());
    }

    #[test]
    fn nested_extensions_restore_the_outer_extension() {
        struct Allow(bool);
        impl ScheduleExtension for Allow {
            fn allows_trigger(&self, _hook: HookMetadata, _default: bool) -> bool {
                self.0
            }
        }
        let hook = HookMetadata {
            location: "test.rs:1:1",
            index: 0,
            item_type: "u64",
            kind: HookKind::Batch,
            member: None,
        };
        with_extension(Rc::new(Allow(false)), || {
            assert!(!allows_trigger(hook, true));
            with_extension(Rc::new(Allow(true)), || assert!(allows_trigger(hook, true)));
            assert!(!allows_trigger(hook, true));
        });
        assert!(!is_active());
    }

    #[test]
    fn extension_and_decision_context_are_restored_after_panic() {
        struct Noop;
        impl ScheduleExtension for Noop {}

        let hook = HookMetadata {
            location: "test.rs:1:1",
            index: 0,
            item_type: "u64",
            kind: HookKind::Batch,
            member: None,
        };
        let _ = std::panic::catch_unwind(|| {
            with_extension(Rc::new(Noop), || {
                with_hook_decision(hook, false, || panic!("expected"));
            });
        });
        assert!(current_hook_decision().is_none());
        assert!(!is_active());
    }
}
