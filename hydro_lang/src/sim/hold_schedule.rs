//! A deterministic schedule that *holds* deliveries: the prompt schedule of
//! [`super::prompt_schedule`] with a per-edge policy for the tick-boundary buffers (`batch`) an
//! experiment names.
//!
//! The scheduler tells the driver which hook is asking through
//! [`super::edge_counts::current_hook`], so a policy can be attached to an edge by its source
//! location. Three policies exist:
//!
//! - [`EdgePolicy::Prompt`]: release everything buffered (the default for every edge).
//! - [`EdgePolicy::Metered(n)`]: release exactly `n` buffered items per decision. With `n = 1`
//!   this makes the edge a clock: a stream of timer elements sent to the simulation up front is
//!   consumed one per tick of the slice that reads it, so the slice's tick count *is* logical
//!   time and the driver, not the test harness, decides when time advances. With `n > 1` it
//!   paces an input that arrives at a fixed rate per tick (a workload sent up front).
//! - [`EdgePolicy::Periodic { period, count }`]: release `count` items (or everything) at every
//!   `period`-th decision and nothing in between. With `count = Some(1)` this is a slower clock
//!   (a timer that fires every `period` ticks of its slice); with `count = None` it is bursty
//!   delivery: the same maximum delay as `Hold(period)`, but as a gap followed by a burst rather
//!   than a constant shift.
//! - [`EdgePolicy::Hold(d)`]: items that first appear in the buffer at the hook's `k`-th
//!   decision are released at its `(k + d)`-th decision. Since the hook is asked exactly once
//!   per tick of its slice, and a metered clock in the same slice advances once per tick, a
//!   record is held for `d` ticks of logical time. `Hold(0)` is `Prompt`.
//!
//! A **fork** ([`Fork`], [`HoldScheduleDriver::with_fork`]) is a counterfactual edit to a `Hold`
//! policy: at one decision of one hook, the records that arrived at a given decision are released
//! although the policy would hold them, and everything else proceeds under the same policy. On an
//! unordered hook exactly those records are released (the driver picks their positions); on a
//! totally ordered hook the hook only releases a prefix, so everything older is released with
//! them. Replaying a run with one fork and diffing it against the run without is the per-decision
//! counterfactual of `design_docs/2026-09_perffuzz_spike.md`.
//!
//! When the scheduler *forces* a hook to release (no other hook in the tick released anything,
//! so the hook must release at least one item) and nothing held is due, the driver releases
//! everything: the only way that happens is when the clock has run dry, so this is the
//! end-of-run flush.
//!
//! The hooks phrase their decisions in two ways, both handled by `Hold`. A totally ordered
//! `batch` asks one question, "how many items from the front (in `min..=buffered`)". An
//! unordered `batch` asks, per item, "stop releasing?" (a boolean; skipped for the first item
//! when forced) and then "which of the remaining items?" (an exclusive range); the policy stops
//! after the due count and always picks the oldest. A keyed `batch` asks one count per key; only
//! `Periodic` (all keys or none) is supported there, the other policies panic at the first
//! decision.

use core::ops::Bound;
use std::collections::{HashMap, VecDeque};

use bolero::bolero_engine::driver::Driver;

use super::edge_counts::{HookContext, current_hook, edge_key};

/// How the driver answers the "how many to release" question of one edge's hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgePolicy {
    /// Release everything buffered.
    Prompt,
    /// Release exactly this many items per decision (fewer only when fewer are buffered).
    Metered(usize),
    /// Release `count` items (everything if `None`) at every `period`-th decision, nothing
    /// otherwise.
    Periodic {
        /// Decisions between releases; a release happens when the decision index is a multiple.
        period: u64,
        /// Items per release; `None` releases everything buffered.
        count: Option<usize>,
    },
    /// Release an item `d` decisions after it first appeared in the buffer.
    Hold(u64),
}

/// A counterfactual edit to a `Hold` policy: at the hook's `decision`, release the records that
/// arrived at `arrived` (see the [module documentation](self)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fork {
    /// The edge key (see [`edge_key`]).
    pub edge: String,
    /// The cluster member the hook belongs to, if any.
    pub member: Option<u32>,
    /// The hook's decision index (0-based) at which to release.
    pub decision: u64,
    /// The decision index at which the records to release arrived.
    pub arrived: u64,
}

/// Per-hook bookkeeping for the stateful policies; one per (edge, cluster member).
#[derive(Debug, Default)]
struct HoldState {
    /// Decisions this hook has been asked for so far (its slice's tick count).
    decisions: u64,
    /// Groups of items still held, as (decision at which they appeared, count), oldest first.
    groups: VecDeque<(u64, usize)>,
    /// Serial of the decision currently being answered (see [`HookContext::decision`]).
    current: Option<u64>,
    /// Items buffered at the start of the current decision.
    buffered: usize,
    /// Items due for release in the current decision.
    due: usize,
    /// Items released so far in the current decision.
    released: usize,
    /// Questions answered so far in the current decision.
    questions: usize,
    /// A fork applies to this decision: the arrival tick of the records to release, until the
    /// first question resolves it.
    fork: Option<u64>,
    /// The forked group's position in the buffer once the due items are gone, and its size
    /// (unordered hooks).
    fork_position: usize,
    fork_count: usize,
}

impl HoldState {
    /// Starts a new decision: accounts for items that arrived since the last one and computes
    /// how many are due under `policy`.
    fn begin(&mut self, context: &HookContext, policy: EdgePolicy, fork: Option<&Fork>) {
        // Items held across from the previous decision.
        let held = self.buffered - self.released;
        let tick = self.decisions;
        self.decisions += 1;
        self.current = Some(context.decision);
        self.buffered = context.buffered;
        self.released = 0;
        self.questions = 0;
        self.fork = fork.filter(|f| f.decision == tick).map(|f| f.arrived);
        self.fork_position = 0;
        self.fork_count = 0;

        self.due = match policy {
            EdgePolicy::Prompt => context.buffered,
            EdgePolicy::Metered(n) => context.buffered.min(n),
            EdgePolicy::Periodic { period, count } => {
                if tick.is_multiple_of(period) {
                    count.map_or(context.buffered, |n| context.buffered.min(n))
                } else {
                    0
                }
            }
            EdgePolicy::Hold(d) => {
                let newly_arrived = context
                    .buffered
                    .checked_sub(held)
                    .expect("buffer shrank without the driver releasing from it");
                if newly_arrived > 0 {
                    self.groups.push_back((tick, newly_arrived));
                }
                let mut due = 0;
                while let Some(&(arrived, count)) = self.groups.front() {
                    if arrived + d <= tick {
                        due += count;
                        self.groups.pop_front();
                    } else {
                        break;
                    }
                }
                due
            }
        };
    }

    /// Forced to release with nothing due: the clock has run dry, flush everything.
    fn flush(&mut self) {
        self.groups.clear();
        self.due = self.buffered;
        self.fork = None;
        self.fork_count = 0;
    }

    /// Resolves a pending fork for a hook that releases a prefix: everything that arrived at or
    /// before the forked tick becomes due.
    fn fork_prefix(&mut self) {
        if let Some(arrived) = self.fork.take() {
            while let Some(&(a, count)) = self.groups.front() {
                if a <= arrived {
                    self.due += count;
                    self.groups.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// Resolves a pending fork for a hook that releases by position: the forked group leaves
    /// the held groups and its position (after the due items, which sit in front of it, are
    /// gone) is remembered.
    fn fork_exact(&mut self) {
        if let Some(arrived) = self.fork.take() {
            let mut position = 0;
            for (i, &(a, count)) in self.groups.iter().enumerate() {
                if a == arrived {
                    self.fork_position = position;
                    self.fork_count = count;
                    self.groups.remove(i);
                    return;
                }
                position += count;
            }
        }
    }
}

/// See the [module documentation](self).
pub struct HoldScheduleDriver {
    depth: usize,
    rules: Vec<(Box<dyn Fn(&str) -> bool>, EdgePolicy)>,
    states: HashMap<(String, Option<u32>), HoldState>,
    forks: Vec<Fork>,
}

impl Default for HoldScheduleDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HoldScheduleDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoldScheduleDriver")
            .field("rules", &self.rules.iter().map(|(_, p)| p).collect::<Vec<_>>())
            .field("states", &self.states)
            .field("forks", &self.forks)
            .finish()
    }
}

/// The question a hook is asking, as far as the policies care.
enum Question {
    /// "How many items to release", `min..=buffered`.
    Count { min: usize },
    /// "Stop releasing?" (unordered hooks, once per item).
    Stop,
    /// "Which item next?" (unordered hooks, once per item).
    Which,
}

impl HoldScheduleDriver {
    /// A driver with no rules, which behaves exactly like the prompt schedule.
    pub fn new() -> Self {
        Self {
            depth: 0,
            rules: vec![],
            states: HashMap::new(),
            forks: vec![],
        }
    }

    /// Adds a counterfactual edit (see [`Fork`]); only meaningful on an edge under `Hold`.
    pub fn with_fork(mut self, fork: Fork) -> Self {
        self.forks.push(fork);
        self
    }

    /// Attaches `policy` to every edge whose key (see [`edge_key`]: `file:line:col <element
    /// type>`) satisfies `pred`. The first matching rule wins.
    pub fn with_policy(mut self, pred: impl Fn(&str) -> bool + 'static, policy: EdgePolicy) -> Self {
        self.rules.push((Box::new(pred), policy));
        self
    }

    /// Shorthand for [`Self::with_policy`] with [`EdgePolicy::Metered`]`(1)`: the edge is a
    /// clock.
    pub fn meter(self, pred: impl Fn(&str) -> bool + 'static) -> Self {
        self.with_policy(pred, EdgePolicy::Metered(1))
    }

    /// Shorthand for [`Self::with_policy`] with [`EdgePolicy::Metered`]`(per_decision)`.
    pub fn meter_n(self, pred: impl Fn(&str) -> bool + 'static, per_decision: usize) -> Self {
        self.with_policy(pred, EdgePolicy::Metered(per_decision))
    }

    /// Shorthand for [`Self::with_policy`] with [`EdgePolicy::Hold`].
    pub fn hold(self, pred: impl Fn(&str) -> bool + 'static, ticks: u64) -> Self {
        self.with_policy(pred, EdgePolicy::Hold(ticks))
    }

    fn policy_for(&self, key: &str) -> EdgePolicy {
        self.rules
            .iter()
            .find(|(pred, _)| pred(key))
            .map(|(_, policy)| *policy)
            .unwrap_or(EdgePolicy::Prompt)
    }

    /// Answers `question` for the hook described by `context`, or `None` to fall back to the
    /// prompt answer. `Count` answers are counts; `Stop` answers are 0 (continue) or 1 (stop);
    /// `Which` answers are indexes.
    fn answer(&mut self, context: HookContext, question: Question) -> Option<usize> {
        let key = edge_key(context.location, context.index);
        let policy = self.policy_for(&key);
        if let EdgePolicy::Prompt = policy {
            return None;
        }
        let fork = self
            .forks
            .iter()
            .find(|f| f.edge == key && f.member == context.member);
        let state = self.states.entry((key, context.member)).or_default();
        if state.current != Some(context.decision) {
            state.begin(&context, policy, fork);
        }
        state.questions += 1;
        Some(match question {
            Count { min } => {
                // A keyed `batch` asks one count per key. `Periodic` releases all keys or none,
                // so the same answer (clamped to each key's queue) serves every question; the
                // other policies would need per-key arrival bookkeeping the hook does not offer.
                assert!(
                    state.questions == 1 || matches!(policy, EdgePolicy::Periodic { .. }),
                    "policy {policy:?} attached to a hook that asks several release counts per decision (keyed batch?): {}",
                    context.location.0
                );
                state.fork_prefix();
                if min > state.due {
                    state.flush();
                }
                state.released = state.due;
                state.due
            }
            Stop => {
                state.fork_exact();
                if state.released >= state.due + state.fork_count {
                    1
                } else {
                    0
                }
            }
            Which => {
                state.fork_exact();
                if state.questions == 1 && state.due == 0 && state.fork_count == 0 {
                    // The first item is being taken without asking: a forced release.
                    state.flush();
                }
                let index = if state.released < state.due { 0 } else { state.fork_position };
                state.released += 1;
                index
            }
        })
    }
}

use Question::*;

macro_rules! prompt_int {
    ($name:ident, $ty:ty) => {
        #[inline]
        fn $name(&mut self, min: Bound<&$ty>, max: Bound<&$ty>) -> Option<$ty> {
            Some(match max {
                Bound::Included(max) => *max,
                Bound::Excluded(_) | Bound::Unbounded => match min {
                    Bound::Included(min) => *min,
                    Bound::Excluded(min) => min.checked_add(1 as $ty)?,
                    Bound::Unbounded => 0 as $ty,
                },
            })
        }
    };
}

impl Driver for HoldScheduleDriver {
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

    prompt_int!(gen_u8, u8);
    prompt_int!(gen_i8, i8);
    prompt_int!(gen_u16, u16);
    prompt_int!(gen_i16, i16);
    prompt_int!(gen_u32, u32);
    prompt_int!(gen_i32, i32);
    prompt_int!(gen_u64, u64);
    prompt_int!(gen_i64, i64);
    prompt_int!(gen_u128, u128);
    prompt_int!(gen_i128, i128);
    prompt_int!(gen_isize, isize);

    /// The hooks' questions are `usize` ranges: inclusive upper bound for "how many to
    /// release", exclusive for "which one".
    fn gen_usize(&mut self, min: Bound<&usize>, max: Bound<&usize>) -> Option<usize> {
        let min_value = match min {
            Bound::Included(m) => *m,
            Bound::Excluded(m) => m.checked_add(1)?,
            Bound::Unbounded => 0,
        };
        let prompt = match max {
            Bound::Included(max) => *max,
            Bound::Excluded(_) | Bound::Unbounded => min_value,
        };
        let Some(context) = current_hook() else {
            return Some(prompt);
        };
        let question = match max {
            Bound::Included(_) => Count { min: min_value },
            Bound::Excluded(_) | Bound::Unbounded => Which,
        };
        Some(match self.answer(context, question) {
            Some(answer) => match question_bounds(answer, min_value, max) {
                Some(clamped) => clamped,
                None => prompt,
            },
            None => prompt,
        })
    }

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

    /// The only boolean a stream hook asks is "stop releasing?"; everything else that asks a
    /// boolean (crashes, stale snapshots) gets `false`.
    fn gen_bool(&mut self, _probability: Option<f32>) -> Option<bool> {
        Some(match current_hook().and_then(|context| self.answer(context, Stop)) {
            Some(answer) => answer == 1,
            None => false,
        })
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

/// Clamps a policy's answer into the range the hook offered; `None` if the range is empty.
fn question_bounds(answer: usize, min: usize, max: Bound<&usize>) -> Option<usize> {
    let max = match max {
        Bound::Included(m) => *m,
        Bound::Excluded(m) => m.checked_sub(1)?,
        Bound::Unbounded => usize::MAX,
    };
    if min > max {
        return None;
    }
    Some(answer.clamp(min, max))
}
