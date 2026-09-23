//! A schedule that holds one chosen `use::batch` hook and follows the prompt schedule
//! everywhere else.
//!
//! Random schedule exploration makes every release decision independently, so the chance that
//! a buffered item survives `k` consecutive decisions falls geometrically in `k`. A program
//! whose hazard needs a delivery delay of forty ticks is out of reach of such a search. This
//! driver turns "delay everything arriving at one edge for a while" into a single decision: a
//! harness names the hook by its `use::batch` source location, switches the hold on for as many
//! rounds as it likes, and switches it off. While the hold is on, every question the named hook
//! asks is answered "release nothing"; every other question is answered as
//! [`super::prompt_schedule::PromptScheduleDriver`] would.
//!
//! # How the driver knows who is asking
//!
//! A bolero [`Driver`] sees only typed range questions. The scheduler's `run_hooks` therefore
//! publishes, in a thread-local, the identity of the hook whose `autonomous_decision` it is
//! about to call (see [`with_current_hook`]), and clears it afterwards. The identity is the
//! hook's `use::batch` source location followed by `#` and the hook's index within its tick,
//! because every batch inside one `sliced!` block reports the block's location, so the location
//! alone does not tell the batches of one tick apart. Hooks that carry no batch location (crash
//! hooks, membership hooks) publish nothing, as does the scheduler's own "which ready tick runs
//! next" question. This is read-only metadata; it changes no scheduling behaviour.
//!
//! # Holds the simulator refuses
//!
//! `run_hooks` forces the last undecided hook of a tick to release at least one item when no
//! other hook in that tick released anything, so that a runnable tick never runs empty. When
//! the held hook is that last hook, its question arrives with a lower bound of one (for ordered
//! streams) or skips the "stop releasing?" question altogether (for unordered streams). The
//! driver answers the minimum, records a *forced release*, and the harness can read the count
//! back through [`HoldHandle::forced_releases`]. A hold whose target sits on a tick with no
//! other input (a server with no timer, for example) is therefore reported as impossible rather
//! than silently ignored.
//!
//! # Discovery
//!
//! The driver also records every hook location it is asked about, so a harness can run once
//! under the prompt schedule, read [`HoldHandle::hooks_seen`], and then hold each in turn.

use core::ops::Bound;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bolero::bolero_engine::driver::Driver;

thread_local! {
    static CURRENT_HOOK: Cell<Option<(&'static str, usize, &'static str)>> = const { Cell::new(None) };
    /// The hook identity being held, mirrored here so the scheduler can consult it without
    /// reaching into the driver. The harness and the scheduler share one thread under
    /// `run_with_driver`.
    static HOLD_TARGET: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Whether the hook at `location` and `index` within its tick is currently held. The scheduler
/// treats a held hook as unable to release, so a tick whose only loaded hook is held is not
/// runnable and parks until other input arrives; without this, `run_hooks` would force the held
/// hook to release the moment it was the tick's only input, and no hold could outlive one round.
/// With no hold set this is always false and the scheduler behaves exactly as before.
#[doc(hidden)]
pub fn is_held(location: Option<&'static str>, index: usize, item_type: &'static str) -> bool {
    let Some(location) = location else {
        return false;
    };
    HOLD_TARGET.with(|t| {
        t.borrow()
            .as_deref()
            .is_some_and(|target| target == hook_id(location, index, item_type))
    })
}

/// Runs `f` with `location` and `index` (the hook's position within its tick) published as the
/// hook currently asking the driver for a decision. Called by the scheduler around every
/// `autonomous_decision`; not for use by harnesses.
#[doc(hidden)]
pub fn with_current_hook<R>(
    location: Option<&'static str>,
    index: usize,
    item_type: &'static str,
    f: impl FnOnce() -> R,
) -> R {
    let previous = CURRENT_HOOK.with(|c| c.replace(location.map(|l| (l, index, item_type))));
    let result = f();
    CURRENT_HOOK.with(|c| c.set(previous));
    result
}

/// The identity string for a hook: `location#index [item type]`. The item type is there for
/// the reader, with module paths stripped; the location and index identify the hook.
pub fn hook_id(location: &str, index: usize, item_type: &str) -> String {
    format!("{location}#{index} [{}]", strip_paths(item_type))
}

/// Removes `path::` prefixes from a type name, so `(u64, a::b::Op)` reads `(u64, Op)`.
fn strip_paths(type_name: &str) -> String {
    let mut out = String::with_capacity(type_name.len());
    let mut ident = String::new();
    let mut rest = type_name;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix("::") {
            // The identifier just collected was a path segment; drop it.
            ident.clear();
            rest = after;
            continue;
        }
        let c = rest.chars().next().unwrap();
        rest = &rest[c.len_utf8()..];
        if c.is_alphanumeric() || c == '_' {
            ident.push(c);
        } else {
            out.push_str(&ident);
            ident.clear();
            out.push(c);
        }
    }
    out.push_str(&ident);
    out
}

fn current_hook() -> Option<String> {
    CURRENT_HOOK.with(|c| c.get()).map(|(l, i, t)| hook_id(l, i, t))
}

#[derive(Debug, Default)]
struct HoldState {
    /// The `use::batch` location being held, if a hold is on.
    target: Option<String>,
    /// Every hook location that asked a question, in first-seen order.
    hooks_seen: BTreeSet<String>,
    /// Questions from the target answered "release nothing" while a hold was on.
    holds_applied: u64,
    /// Questions from the target that the scheduler would not let us answer with zero.
    forced_releases: u64,
}

/// The harness's side of a [`HoldOneHookDriver`]: switches the hold on and off and reads back
/// what happened. Cloning gives another handle to the same driver.
#[derive(Debug, Clone)]
pub struct HoldHandle {
    state: Arc<Mutex<HoldState>>,
}

impl HoldHandle {
    /// Starts holding the hook identified by `id` (`location#index`, as listed by
    /// [`HoldHandle::hooks_seen`]). On a cluster the same identity exists on every member, so
    /// the hold applies to all of them.
    pub fn begin_hold(&self, id: &str) {
        self.state.lock().unwrap().target = Some(id.to_owned());
        HOLD_TARGET.with(|t| *t.borrow_mut() = Some(id.to_owned()));
    }

    /// Stops holding; the next question from the held hook is answered as the prompt schedule
    /// would, which releases everything it buffered.
    pub fn end_hold(&self) {
        self.state.lock().unwrap().target = None;
        HOLD_TARGET.with(|t| *t.borrow_mut() = None);
    }

    /// The location currently held, if any.
    pub fn held(&self) -> Option<String> {
        self.state.lock().unwrap().target.clone()
    }

    /// Every hook location that has asked the driver a question so far, sorted.
    pub fn hooks_seen(&self) -> Vec<String> {
        self.state.lock().unwrap().hooks_seen.iter().cloned().collect()
    }

    /// How many times the held hook was answered "release nothing".
    pub fn holds_applied(&self) -> u64 {
        self.state.lock().unwrap().holds_applied
    }

    /// How many times the held hook was forced to release because it was the only hook on its
    /// tick with anything buffered. Nonzero means the hold could not be kept.
    pub fn forced_releases(&self) -> u64 {
        self.state.lock().unwrap().forced_releases
    }

    /// Resets the counters (not the target or the discovered hooks).
    pub fn reset_counts(&self) {
        let mut s = self.state.lock().unwrap();
        s.holds_applied = 0;
        s.forced_releases = 0;
    }
}

/// See the [module documentation](self).
#[derive(Debug)]
pub struct HoldOneHookDriver {
    state: Arc<Mutex<HoldState>>,
    depth: usize,
}

impl HoldOneHookDriver {
    /// Creates a driver with no hold on, and the handle a harness uses to control it.
    pub fn new() -> (Self, HoldHandle) {
        let state = Arc::new(Mutex::new(HoldState::default()));
        (
            Self {
                state: state.clone(),
                depth: 0,
            },
            HoldHandle { state },
        )
    }

    /// Whether the hook currently asking is the held one. When `record` is set, also remembers
    /// the asker as a hook worth holding (a question with more than one answer means the hook
    /// had something buffered).
    fn asking_hook_is_held(&self, record: bool) -> bool {
        let Some(id) = current_hook() else {
            return false;
        };
        let mut s = self.state.lock().unwrap();
        if record && !s.hooks_seen.contains(&id) {
            s.hooks_seen.insert(id.clone());
        }
        s.target.as_deref() == Some(id.as_str())
    }
}

macro_rules! hold_int {
    ($name:ident, $ty:ty) => {
        #[inline]
        fn $name(&mut self, min: Bound<&$ty>, max: Bound<&$ty>) -> Option<$ty> {
            let least = match min {
                Bound::Included(m) => *m,
                Bound::Excluded(m) => m.checked_add(1 as $ty)?,
                Bound::Unbounded => 0 as $ty,
            };
            let nontrivial = match max {
                Bound::Included(m) => *m > least,
                Bound::Excluded(_) | Bound::Unbounded => true,
            };
            if self.asking_hook_is_held(nontrivial) {
                let mut s = self.state.lock().unwrap();
                match max {
                    // "How many to release": zero if allowed, else a forced release.
                    Bound::Included(_) => {
                        if least == 0 as $ty {
                            s.holds_applied += 1;
                        } else {
                            s.forced_releases += 1;
                        }
                    }
                    // "Which item": for an unordered hook this question is only reached when
                    // the scheduler skipped the "stop releasing?" question, i.e. it forced us.
                    Bound::Excluded(_) | Bound::Unbounded => {
                        s.forced_releases += 1;
                    }
                }
                return Some(least);
            }
            Some(match max {
                Bound::Included(max) => *max,
                Bound::Excluded(_) | Bound::Unbounded => least,
            })
        }
    };
}

impl Driver for HoldOneHookDriver {
    fn depth(&self) -> usize {
        self.depth
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn max_depth(&self) -> usize {
        usize::MAX
    }

    fn gen_variant(&mut self, _variants: usize, base_case: usize) -> Option<usize> {
        Some(base_case)
    }

    hold_int!(gen_u8, u8);
    hold_int!(gen_i8, i8);
    hold_int!(gen_u16, u16);
    hold_int!(gen_i16, i16);
    hold_int!(gen_u32, u32);
    hold_int!(gen_i32, i32);
    hold_int!(gen_u64, u64);
    hold_int!(gen_i64, i64);
    hold_int!(gen_u128, u128);
    hold_int!(gen_i128, i128);
    hold_int!(gen_usize, usize);
    hold_int!(gen_isize, isize);

    fn gen_f32(&mut self, min: Bound<&f32>, _max: Bound<&f32>) -> Option<f32> {
        Some(match min {
            Bound::Included(m) | Bound::Excluded(m) => *m,
            Bound::Unbounded => 0.0,
        })
    }

    fn gen_f64(&mut self, min: Bound<&f64>, _max: Bound<&f64>) -> Option<f64> {
        Some(match min {
            Bound::Included(m) | Bound::Excluded(m) => *m,
            Bound::Unbounded => 0.0,
        })
    }

    fn gen_char(&mut self, min: Bound<&char>, _max: Bound<&char>) -> Option<char> {
        Some(match min {
            Bound::Included(m) | Bound::Excluded(m) => *m,
            Bound::Unbounded => '\0',
        })
    }

    /// A boolean from a hook is "do something unusual": stop releasing early (unordered
    /// streams), re-release a stale snapshot, crash. Held hooks stop releasing; everyone else
    /// does the ordinary thing.
    fn gen_bool(&mut self, _probability: Option<f32>) -> Option<bool> {
        if self.asking_hook_is_held(true) {
            self.state.lock().unwrap().holds_applied += 1;
            return Some(true);
        }
        Some(false)
    }

    fn gen_from_bytes<Hint, Gen, T>(&mut self, hint: Hint, mut produce: Gen) -> Option<T>
    where
        Hint: FnOnce() -> (usize, Option<usize>),
        Gen: FnMut(&[u8]) -> Option<(usize, T)>,
    {
        let (min_len, max_len) = hint();
        let zeros = vec![0u8; max_len.unwrap_or(min_len).max(min_len)];
        produce(&zeros).map(|(_, value)| value)
    }
}
