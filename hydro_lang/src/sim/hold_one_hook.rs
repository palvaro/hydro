//! A schedule that holds one chosen `use::batch` or `use::snapshot` hook and follows the prompt
//! schedule everywhere else.
//!
//! Random schedule exploration makes every release decision independently, so the chance that
//! a buffered item survives `k` consecutive decisions falls geometrically in `k`. A program
//! whose hazard needs a delivery delay of forty ticks is out of reach of such a search. This
//! driver turns "delay everything arriving at one edge for a while" into a single decision: a
//! harness names the hook by its source location, switches the hold on for as many rounds as it
//! likes, and switches it off. While the hold is on, every question the named hook asks is
//! answered "do not move"; every other question is answered as
//! [`super::prompt_schedule::PromptScheduleDriver`] would.
//!
//! # What a hold means for each kind of hook
//!
//! A held **batch** hook releases nothing: records that arrive at it stay buffered, and when the
//! hold ends the next decision releases them all (the prompt policy). The tick sees a gap and
//! then a dump, which is what a delayed network edge looks like.
//!
//! A held **snapshot** hook keeps observing the version of its singleton that it observed last
//! before the hold began: every "observe the last version again?" question is answered yes, and
//! new versions queue up unobserved. The tick keeps acting on stale state, which is what a
//! delayed state update looks like; a program that re-derives work from the stale value (a retry
//! judged against an old clock, a fetch issued because an old cache still shows a miss) pays for
//! the delay the same way it would for a delayed message. When the hold ends, the first decision
//! skips straight to the newest queued version, the analogue of a batch hook releasing its whole
//! buffer, so that the hold's effect is a delay of `k` rounds and not a lag that persists for the
//! rest of the run. (Under the prompt policy alone a snapshot hook observes queued versions one
//! per tick, oldest first; a hook with `k` versions queued would otherwise stay `k` versions
//! behind until the run ended.) A snapshot hook that has never observed anything when the hold
//! begins has nothing to pin; its first observation goes ahead as the prompt policy would, and
//! the pin takes effect from there. A tick whose only new input is a queued version of a held
//! snapshot parks, exactly as a tick whose only buffered records sit behind a held batch does.
//!
//! # How the driver knows who is asking
//!
//! A bolero [`Driver`] sees only typed range questions. The scheduler's `run_hooks` therefore
//! publishes, in a thread-local, the identity of the hook whose `autonomous_decision` it is
//! about to call, together with whether the scheduler is forcing that hook to make a nontrivial
//! decision (see [`with_current_hook`]), and clears it afterwards. The identity is the hook's
//! source location followed by `#` and the hook's index within its tick, because every batch and
//! snapshot inside one `sliced!` block reports the block's location, so the location alone does
//! not tell the hooks of one tick apart; snapshot hooks carry the word `snapshot` before their
//! item type. Hooks that carry no location (crash hooks, membership hooks, top-level ordering
//! hooks) publish nothing, as does the scheduler's own "which ready tick runs next" question.
//! This is read-only metadata; it changes no scheduling behaviour.
//!
//! # Holds the simulator refuses
//!
//! `run_hooks` forces the last undecided hook of a tick to make a nontrivial decision when no
//! other hook in that tick did, so that a runnable tick never runs empty. When the held hook is
//! that last hook, its question arrives with a lower bound of one (ordered batch), or skips the
//! "stop releasing?" or "observe again?" question altogether (unordered batch, snapshot). The
//! driver answers the minimum, records a *forced release*, and the harness can read the count
//! back through [`HoldHandle::forced_releases`]. Because the scheduler does not count a held hook
//! toward a tick's runnability, this happens only when the tick was made runnable by another
//! hook that then decided trivially, which the prompt policy never does; the count is kept so a
//! harness can tell rather than assume.
//!
//! # Discovery
//!
//! The driver also records every hook that asks it a question with more than one answer, so a
//! harness can run once under the prompt schedule, read [`HoldHandle::hooks_seen`], and then
//! hold each in turn.

use core::ops::Bound;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bolero::bolero_engine::driver::Driver;

use super::runtime::HookKind;

/// What the scheduler publishes about the hook it is about to ask.
#[derive(Debug, Clone, Copy)]
struct CurrentHook {
    location: &'static str,
    index: usize,
    item_type: &'static str,
    kind: HookKind,
    /// Whether the scheduler is forcing this hook to make a nontrivial decision.
    forced: bool,
    /// Counts `with_current_hook` calls, so the driver can tell the questions of one
    /// `autonomous_decision` from those of the next call to the same hook.
    decision_seq: u64,
}

thread_local! {
    static CURRENT_HOOK: Cell<Option<CurrentHook>> = const { Cell::new(None) };
    static DECISION_SEQ: Cell<u64> = const { Cell::new(0) };
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
pub fn is_held(
    location: Option<&'static str>,
    index: usize,
    item_type: &'static str,
    kind: HookKind,
) -> bool {
    let Some(location) = location else {
        return false;
    };
    HOLD_TARGET.with(|t| {
        t.borrow()
            .as_deref()
            .is_some_and(|target| target == hook_id(location, index, item_type, kind))
    })
}

/// Runs `f` with the hook's identity, and whether the scheduler is forcing it to decide
/// nontrivially, published as the hook currently asking the driver for a decision. Called by
/// the scheduler around every `autonomous_decision`; not for use by harnesses.
#[doc(hidden)]
pub fn with_current_hook<R>(
    location: Option<&'static str>,
    index: usize,
    item_type: &'static str,
    kind: HookKind,
    forced: bool,
    f: impl FnOnce() -> R,
) -> R {
    let seq = DECISION_SEQ.with(|s| {
        let n = s.get().wrapping_add(1);
        s.set(n);
        n
    });
    let previous = CURRENT_HOOK.with(|c| {
        c.replace(location.map(|location| CurrentHook {
            location,
            index,
            item_type,
            kind,
            forced,
            decision_seq: seq,
        }))
    });
    let result = f();
    CURRENT_HOOK.with(|c| c.set(previous));
    result
}

/// The identity string for a hook: `location#index [item type]` for a batch and
/// `location#index [snapshot item type]` for a snapshot. The item type is there for the reader,
/// with module paths stripped; the location, index and kind identify the hook.
pub fn hook_id(location: &str, index: usize, item_type: &str, kind: HookKind) -> String {
    match kind {
        HookKind::Batch => format!("{location}#{index} [{}]", strip_paths(item_type)),
        HookKind::Snapshot => {
            format!("{location}#{index} [snapshot {}]", strip_paths(item_type))
        }
    }
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

fn current_hook() -> Option<CurrentHook> {
    CURRENT_HOOK.with(|c| c.get())
}

fn id_of(h: &CurrentHook) -> String {
    hook_id(h.location, h.index, h.item_type, h.kind)
}

#[derive(Debug, Default)]
struct HoldState {
    /// The hook identity being held, if a hold is on.
    target: Option<String>,
    /// A snapshot hook whose hold just ended and which has not yet made its first decision
    /// since: that decision skips to the newest queued version. The sequence number is set when
    /// the first question of that decision arrives, so later questions of the same decision
    /// (one per key, for a keyed singleton) are answered the same way and the next decision is
    /// not.
    catching_up: Option<(String, Option<u64>)>,
    /// Every hook identity that asked a question with more than one answer, sorted.
    hooks_seen: BTreeSet<String>,
    /// Questions from the target answered "do not move" while a hold was on.
    holds_applied: u64,
    /// Questions from the target that the scheduler would not let us answer with "do not move".
    forced_releases: u64,
}

/// The harness's side of a [`HoldOneHookDriver`]: switches the hold on and off and reads back
/// what happened. Cloning gives another handle to the same driver.
#[derive(Debug, Clone)]
pub struct HoldHandle {
    state: Arc<Mutex<HoldState>>,
}

impl HoldHandle {
    /// Starts holding the hook identified by `id` (as listed by [`HoldHandle::hooks_seen`]). On
    /// a cluster the same identity exists on every member, so the hold applies to all of them.
    pub fn begin_hold(&self, id: &str) {
        let mut s = self.state.lock().unwrap();
        s.target = Some(id.to_owned());
        s.catching_up = None;
        HOLD_TARGET.with(|t| *t.borrow_mut() = Some(id.to_owned()));
    }

    /// Stops holding; the next question from the held hook is answered as the prompt schedule
    /// would, which releases everything a batch hook buffered. A released snapshot hook first
    /// skips to the newest version it has queued (see the [module docs](self)).
    pub fn end_hold(&self) {
        let mut s = self.state.lock().unwrap();
        if let Some(target) = s.target.take()
            && target.contains("[snapshot ")
        {
            s.catching_up = Some((target, None));
        }
        HOLD_TARGET.with(|t| *t.borrow_mut() = None);
    }

    /// The hook identity currently held, if any.
    pub fn held(&self) -> Option<String> {
        self.state.lock().unwrap().target.clone()
    }

    /// Every hook identity that has asked the driver a question with more than one answer so
    /// far, sorted.
    pub fn hooks_seen(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .hooks_seen
            .iter()
            .cloned()
            .collect()
    }

    /// How many times the held hook was answered "do not move".
    pub fn holds_applied(&self) -> u64 {
        self.state.lock().unwrap().holds_applied
    }

    /// How many times the held hook was forced to make a nontrivial decision because the
    /// scheduler required one from it. Nonzero means the hold could not be kept.
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

/// How the driver should answer the question a hook is asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    /// Not the held hook and not catching up: the prompt policy.
    Prompt,
    /// The held hook: do not move.
    Hold { forced: bool, kind: HookKind },
    /// A snapshot hook whose hold just ended: observe the newest queued version.
    CatchUp,
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

    /// Decides how to answer the current question. When `record` is set, also remembers the
    /// asker as a hook worth holding (a question with more than one answer means the hook had
    /// something buffered).
    fn policy(&self, record: bool) -> Policy {
        let Some(h) = current_hook() else {
            return Policy::Prompt;
        };
        let id = id_of(&h);
        let mut s = self.state.lock().unwrap();
        if record && !s.hooks_seen.contains(&id) {
            s.hooks_seen.insert(id.clone());
        }
        if s.target.as_deref() == Some(id.as_str()) {
            return Policy::Hold {
                forced: h.forced,
                kind: h.kind,
            };
        }
        if let Some((hook, seq)) = &mut s.catching_up
            && *hook == id
        {
            match seq {
                None => {
                    *seq = Some(h.decision_seq);
                    return Policy::CatchUp;
                }
                Some(seq) if *seq == h.decision_seq => return Policy::CatchUp,
                Some(_) => {
                    s.catching_up = None;
                }
            }
        }
        Policy::Prompt
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
            match self.policy(nontrivial) {
                Policy::Hold { forced, kind } => {
                    let mut s = self.state.lock().unwrap();
                    match (max, kind) {
                        // A batch's "how many to release": zero if allowed, else a forced
                        // release.
                        (Bound::Included(_), _) => {
                            if least == 0 as $ty {
                                s.holds_applied += 1;
                            } else {
                                s.forced_releases += 1;
                            }
                        }
                        // A batch's "which item": for an unordered hook this question is only
                        // reached when the scheduler skipped the "stop releasing?" question,
                        // i.e. it forced us.
                        (Bound::Excluded(_) | Bound::Unbounded, HookKind::Batch) => {
                            s.forced_releases += 1;
                        }
                        // A snapshot's "which version": reached either because the scheduler
                        // forced a fresh observation, or because the hook has never observed
                        // anything and so has nothing to pin. The second is not a refusal; the
                        // observation goes ahead (oldest version, as the prompt policy would)
                        // and the pin holds from there.
                        (Bound::Excluded(_) | Bound::Unbounded, HookKind::Snapshot) => {
                            if forced {
                                s.forced_releases += 1;
                            }
                        }
                    }
                    Some(least)
                }
                Policy::CatchUp => Some(match max {
                    Bound::Included(max) => *max,
                    // "Which version": the newest.
                    Bound::Excluded(max) => {
                        (*max).checked_sub(1 as $ty).unwrap_or(least).max(least)
                    }
                    Bound::Unbounded => least,
                }),
                Policy::Prompt => Some(match max {
                    Bound::Included(max) => *max,
                    Bound::Excluded(_) | Bound::Unbounded => least,
                }),
            }
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
    /// batch), observe the last snapshot again or leave a key out of a keyed snapshot
    /// (snapshot), crash. A held hook does the unusual thing, which for a batch means it stops
    /// releasing and for a snapshot means it stays pinned; everyone else does the ordinary
    /// thing.
    fn gen_bool(&mut self, _probability: Option<f32>) -> Option<bool> {
        match self.policy(true) {
            Policy::Hold { .. } => {
                self.state.lock().unwrap().holds_applied += 1;
                Some(true)
            }
            Policy::CatchUp | Policy::Prompt => Some(false),
        }
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

#[cfg(test)]
mod tests {
    use core::ops::Bound;

    use bolero::bolero_engine::driver::Driver;

    use super::*;

    const LOC: &str = "a.rs:1:1";

    /// Asks the driver as a snapshot hook would: first "observe the last version again?" (a
    /// bool), then, if not, "which queued version?" (`0..len`).
    fn ask_snapshot(
        d: &mut HoldOneHookDriver,
        forced: bool,
        len: usize,
    ) -> (Option<bool>, Option<usize>) {
        with_current_hook(Some(LOC), 0, "T", HookKind::Snapshot, forced, || {
            let again = if forced { None } else { d.gen_bool(None) };
            if again == Some(true) {
                (again, None)
            } else {
                (
                    again,
                    d.gen_usize(Bound::Included(&0), Bound::Excluded(&len)),
                )
            }
        })
    }

    #[test]
    fn snapshot_hold_pins_then_catches_up() {
        let (mut d, h) = HoldOneHookDriver::new();
        let id = hook_id(LOC, 0, "T", HookKind::Snapshot);
        assert_eq!(id, "a.rs:1:1#0 [snapshot T]");

        // Prompt policy: observe a new version, the oldest queued.
        assert_eq!(ask_snapshot(&mut d, false, 3), (Some(false), Some(0)));
        assert_eq!(h.hooks_seen(), vec![id.clone()]);

        // Held: observe the last version again; nothing forced.
        h.begin_hold(&id);
        assert_eq!(ask_snapshot(&mut d, false, 3), (Some(true), None));
        assert_eq!(ask_snapshot(&mut d, false, 5), (Some(true), None));
        assert_eq!(h.holds_applied(), 2);
        assert_eq!(h.forced_releases(), 0);

        // Held and forced: the observation goes ahead (oldest) and is recorded as a refusal.
        assert_eq!(ask_snapshot(&mut d, true, 5), (None, Some(0)));
        assert_eq!(h.forced_releases(), 1);

        // Hold ends: the first decision skips to the newest queued version, and every question
        // of that same decision does too (a keyed singleton asks once per key).
        h.end_hold();
        let (again, idx) = with_current_hook(Some(LOC), 0, "T", HookKind::Snapshot, false, || {
            let again = d.gen_bool(None);
            let first = d.gen_usize(Bound::Included(&0), Bound::Excluded(&7));
            let second = d.gen_usize(Bound::Included(&0), Bound::Excluded(&4));
            (again, (first, second))
        });
        assert_eq!(again, Some(false));
        assert_eq!(idx, (Some(6), Some(3)));

        // The next decision is back on the prompt policy.
        assert_eq!(ask_snapshot(&mut d, false, 3), (Some(false), Some(0)));
    }

    #[test]
    fn batch_hold_is_unchanged() {
        let (mut d, h) = HoldOneHookDriver::new();
        let id = hook_id(LOC, 1, "u64", HookKind::Batch);
        assert_eq!(id, "a.rs:1:1#1 [u64]");
        let ask = |d: &mut HoldOneHookDriver, forced: bool| {
            with_current_hook(Some(LOC), 1, "u64", HookKind::Batch, forced, || {
                let least = if forced { 1 } else { 0 };
                d.gen_usize(Bound::Included(&least), Bound::Included(&4))
            })
        };
        assert_eq!(ask(&mut d, false), Some(4));
        h.begin_hold(&id);
        assert_eq!(ask(&mut d, false), Some(0));
        assert_eq!(ask(&mut d, true), Some(1));
        assert_eq!((h.holds_applied(), h.forced_releases()), (1, 1));
        h.end_hold();
        // A released batch hook does not catch up specially; the prompt policy already releases
        // everything.
        assert_eq!(ask(&mut d, false), Some(4));
        assert!(h.held().is_none());
    }

    #[test]
    fn a_held_hook_is_reported_to_the_scheduler() {
        let (_d, h) = HoldOneHookDriver::new();
        let id = hook_id(LOC, 0, "T", HookKind::Snapshot);
        assert!(!is_held(Some(LOC), 0, "T", HookKind::Snapshot));
        h.begin_hold(&id);
        assert!(is_held(Some(LOC), 0, "T", HookKind::Snapshot));
        // Same location and index but a batch is a different hook.
        assert!(!is_held(Some(LOC), 0, "T", HookKind::Batch));
        assert!(!is_held(None, 0, "T", HookKind::Snapshot));
        h.end_hold();
        assert!(!is_held(Some(LOC), 0, "T", HookKind::Snapshot));
    }
}
