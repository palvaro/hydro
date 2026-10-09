//! Scripted hooks: manual control of unsafe operators from simulation tests.
//!
//! A hook that is bound to a test-side handle (`nondet!(... hook = handle)`) is emitted as
//! [`Scripted<H>`] wrapping the ordinary hook type, which implements [`ScriptableHook`]
//! (the per-kind decision semantics). The scripted hook is shared, via `Rc<RefCell<dyn
//! ScriptedRuntimeHook>>`, between the scheduler's tick lists and a per-instance registry
//! keyed by handle ID, so test-side handles can install decisions and read status
//! on demand. Kind-specific payloads (decisions in, status out) cross the registry's
//! type-erased surface bincode-serialized: the registry must be `dyn` (it holds every
//! hook kind), associated types are unusable through a `dyn` trait object, and
//! downcasting is not an option because the generated dylib links its own copy of this
//! crate — `TypeId`s differ across the boundary, so `Any`-based casts would fail (or
//! worse). Serialization sidesteps all of that: the handle and the hook statically know
//! the same types (the `NonDet` typing guarantees a handle can only bind to a matching
//! operator), so each side simply encodes/decodes and no runtime dispatch is needed.
//!
//! A scripted hook never makes a decision on its own — no fuzzer entropy is ever spent on
//! it; everything it releases was scripted, with one narrow exception: when there is only
//! one thing the hook could possibly do (empty buffer → empty batch; no newer versions →
//! re-reveal), it does that implicitly.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::Colorize;

use super::{HookLocationMeta, InlineHook, RuntimeHook, SimLocation, abort};

/// The decisions a test-side handle can script for one kind of hook. Serialized when
/// crossing the type-erased registry surface (see [`ScriptedHookControl::install_decision`]).
pub trait ScriptDecision: serde::Serialize + serde::de::DeserializeOwned {
    /// A short human-readable rendering for error messages.
    fn describe(&self) -> String;
}

/// The standing-policy type of hook kinds that support no standing policy (every kind
/// except unordered batch hooks). Uninhabited: no value of this type can be installed.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum NoPolicy {}

impl ScriptDecision for NoPolicy {
    fn describe(&self) -> String {
        match *self {}
    }
}

/// What a hook under a standing policy has done so far: every element it delivered and
/// every element it discarded, each bincode-serialized individually (the shell does not
/// know the element type; the test-side handle does and decodes them). This is the
/// Level 0 lineage of a fault-injection run: the set of messages that actually flowed.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PolicyTrace {
    /// Elements released into the operator, in release order.
    pub delivered: Vec<Vec<u8>>,
    /// Elements discarded by the policy, in discard order.
    pub dropped: Vec<Vec<u8>>,
}

/// The per-kind decision semantics of a hook that can be driven by a test-side handle,
/// implemented directly on the ordinary hook types ([`StreamHook`](super::StreamHook),
/// [`SingletonHook`](super::SingletonHook)).
/// The generic [`Scripted<H>`] shell turns any implementor into a scripted hook.
pub trait ScriptableHook: RuntimeHook {
    /// The decisions a handle can script for this kind of hook.
    type Decision: ScriptDecision;
    /// This kind's view of its pending, undecided input, read on demand by the test-side
    /// handle (for `pause_until_*` predicates).
    type Status: serde::Serialize;
    /// A **standing policy** this kind can be placed under instead of a schedule of
    /// decisions: a filter that is applied autonomously (without entropy and without a
    /// per-execution decision) whenever the hook's tick runs, for as long as it is
    /// installed. Used by fault injection, where a channel's receiving hook is told
    /// "deliver everything except these" once. Kinds without such a policy use
    /// [`NoPolicy`].
    type Policy: ScriptDecision;

    /// Sweeps the buffered input under the standing policy: discards (and records in
    /// `trace`) every element the policy omits, then reports whether anything deliverable
    /// remains, which makes the hook's tick runnable on this hook's account. Called at
    /// every scheduling query, so omission takes effect on arrival, as on a lossy
    /// channel, whether or not the tick ever runs again.
    fn policy_sweep(&self, _policy: &Self::Policy, _trace: &mut PolicyTrace) -> bool {
        false
    }

    /// Applies the standing policy to the (already swept) buffered input, staging the
    /// release of everything deliverable (possibly nothing) and recording it in `trace`.
    /// Called instead of [`Self::apply`] / [`Self::implicit`] whenever the tick runs while
    /// a policy is installed.
    fn apply_policy(&mut self, _policy: &mut Self::Policy, _trace: &mut PolicyTrace) {
        abort!("this hook kind does not support a standing policy");
    }

    /// Whether the decision could be honored right now, against the current buffer.
    /// Returns an error when the decision is already known to be invalid rather than
    /// merely waiting for more input. The host scheduler reports that error without
    /// unwinding through the generated dylib boundary.
    fn is_honorable(&self, decision: &Self::Decision) -> Result<bool, String>;
    /// Applies the decision, staging the release. Whether the decision triggers the
    /// tick is a separate, kind-specific question
    /// ([`ScriptableTickInputHook::decision_triggers_tick`]); observations always fire.
    fn apply(&mut self, decision: Self::Decision);
    /// Stages the only-possibility implicit behavior (empty batch / re-reveal), which by
    /// definition never triggers the tick. Called only when the hook's tick was forced
    /// to run by *other* hooks while this hook had no queued decision (nothing to
    /// decide, or held). Top-level observation hooks abort here: an observation consists
    /// of exactly one hook, so nothing can force it to run without a scripted decision.
    fn implicit(&mut self);
    /// The current pending-input view.
    fn status(&self) -> Self::Status;
    /// A human-readable rendering of the pending, undecided input (e.g. `2 buffered
    /// item(s): [1, 2]`), or `None` when there is nothing pending. Used by the
    /// forgotten-hook and stuck-decision error messages.
    fn describe_pending(&self) -> Option<String>;
}

/// The scheduler action selected by a scripted decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScriptTarget {
    /// A real tick. Multiple decisions for different hooks may join one tick group.
    Tick {
        /// The tick's location.
        location: SimLocation,
    },
    /// One top-level observation hook, treated as its own virtual tick.
    Observation {
        /// The top-level location the hook lives at.
        location: SimLocation,
        /// The bound handle's ID, distinguishing this hook from co-located ones.
        hook_id: usize,
    },
}

impl ScriptTarget {
    /// The location of the targeted action.
    pub fn location(&self) -> &SimLocation {
        match self {
            ScriptTarget::Tick { location } | ScriptTarget::Observation { location, .. } => {
                location
            }
        }
    }
}

/// Handle-facing state shared by pre-tick, top-level observation, and inline scripted hooks.
pub trait ScriptedHookControl {
    fn target(&self) -> ScriptTarget;
    fn location_meta(&self) -> HookLocationMeta;
    fn has_decision(&self) -> bool;
    fn describe_decision(&self) -> Option<String>;
    fn describe_pending(&self) -> Option<String>;
    fn install_decision(&mut self, decision_blob: &[u8]);
    fn status_blob(&self) -> Vec<u8>;
    fn set_hold(&mut self, hold: bool);
    fn release_hold(&mut self);
    fn set_auto_pause(&mut self, auto_pause: bool);
    /// Installs (or replaces) a standing policy (bincode of the kind's
    /// [`ScriptableHook::Policy`]). A hook under a policy takes no scripted decisions
    /// and is never *forgotten*; the policy stays in force until replaced.
    fn install_policy(&mut self, policy_blob: &[u8]);
    /// The bincode-serialized [`PolicyTrace`] of everything this hook has delivered and
    /// discarded while under a standing policy.
    fn policy_trace_blob(&self) -> Vec<u8>;
}

/// The scheduler-facing interface shared by all scripted top-level hooks; refined by
/// [`ScriptedTickInputHook`] and [`ScriptedObservationHook`], mirroring the
/// unscripted [`TickInputHook`](super::TickInputHook) /
/// [`ObservationHook`](super::ObservationHook) split.
pub trait ScriptedRuntimeHook: RuntimeHook + ScriptedHookControl {
    /// Executes and releases this hook's queued (or implicit only-possibility) scripted
    /// decision, without consulting the entropy driver. Whether it triggers is known
    /// *before* running, via [`ScriptedTickInputHook::can_trigger_tick`] /
    /// [`ScriptedObservationHook::can_fire`].
    fn run_decision(&mut self, log_writer: Option<&mut dyn std::fmt::Write>);

    /// Whether this hook currently *blocks* its tick from running: a queued decision that
    /// is not yet honorable prevents the tick from executing at all, so the decision can
    /// never be skipped over or paired with a different execution.
    fn blocks_tick(&self) -> bool;

    /// Called at every scheduling boundary (async dataflows exhausted, ticks about to be
    /// considered). Reports an error if the hook is *forgotten*: it holds pending input
    /// with no queued decision and no hold.
    fn boundary_check(&self) -> Result<(), String>;
}

/// The per-kind question of a scripted tick-input hook, implemented directly on the
/// hook types: which decisions trigger the tick. This does not require the
/// autonomous surface ([`TickInputHook`](super::TickInputHook)), so a hook kind can be
/// scriptable without any autonomous decision capability.
pub trait ScriptableTickInputHook: ScriptableHook {
    /// Whether applying this decision (when honorable) would trigger the tick, making
    /// it runnable on this hook's account. Empty/keep decisions do not trigger; they
    /// are consumed when another hook makes the same tick run.
    fn decision_triggers_tick(&self, decision: &Self::Decision) -> bool;
}

/// Marks a [`ScriptableHook`] kind as a top-level observation. There is no per-kind
/// question to answer: observation decisions always release. Like
/// [`ScriptableTickInputHook`], this does not require the autonomous surface
/// ([`ObservationHook`](super::ObservationHook)).
pub trait ScriptableObservationHook: ScriptableHook {}

/// The scheduler-facing (object-safe) surface of a scripted tick-input hook: the
/// scripted counterpart of [`TickInputHook`](super::TickInputHook). A scripted hook
/// triggers its tick only through an honorable queued decision that triggers — pending
/// input alone never does (the boundary scan reacts to that). Blanket
/// implemented on [`Scripted<H>`] from the per-kind [`ScriptableTickInputHook`].
pub trait ScriptedTickInputHook: ScriptedRuntimeHook {
    /// Whether the queued decision is honorable and triggers the tick.
    fn can_trigger_tick(&self) -> bool;
}

impl<H: ScriptableTickInputHook> ScriptedTickInputHook for Scripted<H> {
    fn can_trigger_tick(&self) -> bool {
        if let Some(policy) = &self.policy {
            return self
                .core
                .policy_sweep(policy, &mut self.policy_trace.borrow_mut());
        }
        self.honorable_queued_decision()
            .is_some_and(|decision| self.core.decision_triggers_tick(decision))
    }
}

/// The scheduler-facing (object-safe) surface of a scripted top-level observation hook:
/// the scripted counterpart of [`ObservationHook`](super::ObservationHook). The
/// observation runs exactly when its queued decision is honorable (observation
/// decisions always release). Blanket implemented on [`Scripted<H>`] from the
/// [`ScriptableObservationHook`] marker.
pub trait ScriptedObservationHook: ScriptedRuntimeHook {
    /// Whether the queued decision is honorable, making the observation runnable.
    fn can_fire(&self) -> bool;
}

impl<H: ScriptableObservationHook> ScriptedObservationHook for Scripted<H> {
    fn can_fire(&self) -> bool {
        if let Some(policy) = &self.policy {
            return self
                .core
                .policy_sweep(policy, &mut self.policy_trace.borrow_mut());
        }
        self.honorable_queued_decision().is_some()
    }
}

/// A scripted observation reached while a tick DFIR is executing.
pub trait ScriptedInlineHook: ScriptedHookControl {
    /// Whether the hook holds staged in-tick input awaiting a decision.
    fn has_pending_input(&self) -> bool;
    /// Consumes the queued decision (or the only-possibility implicit behavior) and
    /// releases it.
    ///
    /// Unlike the top-level scripted hooks, this can *fail*: it runs while the tick DFIR
    /// is blocked mid-execution, and decisions can only be installed while the test body
    /// is polled — which never happens during a tick — so a missing decision cannot be
    /// waited out and is reported immediately. It is an error *value* rather than a
    /// panic because this code is monomorphized into the generated dylib, and a panic
    /// must not unwind across that boundary — the host scheduler reports the error.
    fn run_decision(&mut self, log_writer: Option<&mut dyn std::fmt::Write>) -> Result<(), String>;
    fn location_meta(&self) -> HookLocationMeta;
}

/// The per-kind decision semantics of an in-tick hook that can be driven by a test-side
/// handle — the inline analog of [`ScriptableHook`], implemented directly on the ordinary
/// inline hook types ([`StreamOrderHook`](super::StreamOrderHook), and the keyed/partial-order/merge kinds as
/// they become scriptable). The generic [`ScriptedInline<H>`] shell turns any implementor
/// into a scripted inline hook.
pub trait ScriptableInlineHook: InlineHook {
    /// The decisions a handle can script for this kind of hook.
    type Decision: ScriptDecision;
    /// This kind's view of its pending, undecided input, read on demand by the test-side
    /// handle (for `pause_until_*` predicates).
    type Status: serde::Serialize;

    /// Consumes the pending in-tick input together with the queued decision, staging and
    /// releasing it. `None` is passed only when there is exactly one possible outcome
    /// ([`RuntimeHook::only_one_possible_decision`](super::RuntimeHook::only_one_possible_decision)); the missing-decision error for a
    /// genuine choice is raised by the shell before this is called. Returns an error value
    /// instead of panicking: this code is monomorphized into the generated dylib, and a
    /// panic must not unwind across that boundary.
    fn apply_scripted(
        &mut self,
        decision: Option<Self::Decision>,
        log_writer: Option<&mut dyn std::fmt::Write>,
    ) -> Result<(), String>;

    /// The current pending-input view.
    fn status(&self) -> Self::Status;

    /// A human-readable rendering of the pending, undecided input, or `None` when there
    /// is nothing pending.
    fn describe_pending(&self) -> Option<String>;
}

/// Scripted hooks keyed by the scheduler action they feed. Top-level locations become
/// observations; tick locations become ordinary tick inputs.
pub type ScriptedTickHooks = HashMap<SimLocation, Vec<Rc<RefCell<dyn ScriptedTickInputHook>>>>;

pub type ScriptedObservationHooks =
    HashMap<SimLocation, Vec<Rc<RefCell<dyn ScriptedObservationHook>>>>;

pub type ScriptedInlineHooks = HashMap<SimLocation, Vec<Rc<RefCell<dyn ScriptedInlineHook>>>>;

/// The per-instance registry of every scripted hook, keyed by handle ID plus the cluster
/// member ID (`None` for hooks on processes). A handle bound to an operator running on a
/// cluster has one entry — one independent hook instance — per member.
pub type ScriptedHookRegistry =
    std::collections::BTreeMap<(usize, Option<u32>), Rc<RefCell<dyn ScriptedHookControl>>>;

/// The scripted variation of a hook: wraps the ordinary hook type (which owns the buffers
/// and implements the decision semantics via [`ScriptableHook`]) together with the script
/// state: at most one queued decision (decisions are installed group by group, and a
/// hook's earlier decision is always consumed before its next one arrives), plus a `hold`
/// flag for the `pause()` family. The fields are consulted in order — standing policy
/// installed → the policy decides autonomously at every execution; otherwise decision
/// present → normal decision semantics; otherwise hold set → held; otherwise pending
/// input → the *forgotten* state, which the scheduler's boundary scan reports as an error.
pub struct Scripted<H: ScriptableHook> {
    pub(crate) core: H,
    target: ScriptTarget,
    next_decision: Option<H::Decision>,
    hold: bool,
    auto_pause: bool,
    /// A standing policy (see [`ScriptableHook::Policy`]). While installed, the hook is
    /// not scripted: it never takes a queued decision, never blocks or is forgotten, and
    /// triggers its tick exactly when the policy would release something.
    policy: Option<H::Policy>,
    /// Everything delivered and discarded under the policy so far. Discards are recorded
    /// from the `&self` scheduling queries (see [`ScriptableHook::policy_sweep`]).
    policy_trace: RefCell<PolicyTrace>,
}

impl<H: ScriptableHook> Scripted<H> {
    /// Wraps `core` for scripting. `tick` is the execution unit the hook feeds. Called
    /// from generated code.
    pub fn new(core: H, target: ScriptTarget) -> Self {
        Scripted {
            core,
            target,
            next_decision: None,
            hold: false,
            auto_pause: false,
            policy: None,
            policy_trace: RefCell::new(PolicyTrace::default()),
        }
    }

    /// Consumes the queued decision if honorable (or the implicit only-possibility
    /// behavior if none is queued), staging the release on the core hook. Under a
    /// standing policy, the policy is applied instead.
    fn scripted_step(&mut self) {
        if let Some(policy) = &mut self.policy {
            self.core
                .apply_policy(policy, &mut self.policy_trace.borrow_mut());
            return;
        }
        match self.next_decision.take() {
            Some(decision) if matches!(self.core.is_honorable(&decision), Ok(true)) => {
                self.core.apply(decision);
            }
            Some(decision) => {
                // `blocks_tick()` prevents the tick from running while a queued decision
                // is not honorable, so this is unreachable.
                abort!(
                    "tick ran while scripted decision {} was not honorable",
                    decision.describe()
                );
            }
            None => {
                // The boundary scan guarantees this hook either has nothing pending or is
                // held, so the implicit "nothing new" behavior is the only possibility —
                // which never triggers the tick.
                self.core.implicit();
            }
        }
    }

    /// The queued decision, if it is honorable right now. The scheduling questions
    /// ([`ScriptedTickInputHook::can_trigger_tick`] /
    /// [`ScriptedObservationHook::can_fire`]) are answered against this.
    pub(crate) fn honorable_queued_decision(&self) -> Option<&H::Decision> {
        self.next_decision
            .as_ref()
            .filter(|decision| matches!(self.core.is_honorable(decision), Ok(true)))
    }
}

impl<H: ScriptableHook> RuntimeHook for Scripted<H> {
    fn has_pending_input(&self) -> bool {
        self.core.has_pending_input()
    }

    fn only_one_possible_decision(&self) -> bool {
        // The choice question belongs to the wrapped hook: scripting changes who answers
        // a choice, not whether one exists.
        self.core.only_one_possible_decision()
    }

    fn release_decision(&mut self, mut log_writer: Option<&mut dyn std::fmt::Write>) {
        if let Some(w) = log_writer.as_mut() {
            let label = if self.policy.is_some() {
                "(standing policy)"
            } else {
                "(scripted)"
            };
            let _ = writeln!(w, "{}", label.color(colored::Color::Cyan));
        }
        self.core.release_decision(log_writer);
    }

    fn location_meta(&self) -> HookLocationMeta {
        self.core.location_meta()
    }
}

impl<H: ScriptableHook> ScriptedHookControl for Scripted<H> {
    fn target(&self) -> ScriptTarget {
        self.target.clone()
    }

    fn location_meta(&self) -> HookLocationMeta {
        self.core.location_meta()
    }

    fn has_decision(&self) -> bool {
        self.next_decision.is_some()
    }

    fn describe_decision(&self) -> Option<String> {
        self.next_decision.as_ref().map(|d| d.describe())
    }

    fn describe_pending(&self) -> Option<String> {
        self.core.describe_pending()
    }

    fn install_decision(&mut self, decision_blob: &[u8]) {
        assert!(
            self.policy.is_none(),
            "cannot script a decision on a hook that is under a standing policy (`omit` / `deliver_all`); the policy decides every execution"
        );
        assert!(
            self.next_decision.is_none(),
            "internal error: installed a scripted decision while one was still queued"
        );
        self.next_decision = Some(bincode::deserialize(decision_blob).expect(
            "internal error: scripted decision blob did not match the hook's decision type",
        ));
        self.hold = self.auto_pause;
    }

    fn status_blob(&self) -> Vec<u8> {
        bincode::serialize(&self.core.status()).unwrap()
    }

    fn set_hold(&mut self, hold: bool) {
        self.hold = hold;
    }

    fn release_hold(&mut self) {
        self.hold = self.auto_pause;
    }

    fn set_auto_pause(&mut self, auto_pause: bool) {
        self.auto_pause = auto_pause;
    }

    fn install_policy(&mut self, policy_blob: &[u8]) {
        assert!(
            self.next_decision.is_none(),
            "cannot install a standing policy on a hook with a queued scripted decision; a hook is either scripted or under a policy"
        );
        self.policy =
            Some(bincode::deserialize(policy_blob).expect(
                "internal error: standing policy blob did not match the hook's policy type",
            ));
    }

    fn policy_trace_blob(&self) -> Vec<u8> {
        bincode::serialize(&*self.policy_trace.borrow()).unwrap()
    }
}

impl<H: ScriptableHook> ScriptedRuntimeHook for Scripted<H> {
    fn run_decision(&mut self, log_writer: Option<&mut dyn std::fmt::Write>) {
        self.scripted_step();
        self.release_decision(log_writer);
    }

    fn blocks_tick(&self) -> bool {
        self.policy.is_none()
            && self
                .next_decision
                .as_ref()
                .is_some_and(|d| matches!(self.core.is_honorable(d), Ok(false)))
    }

    fn boundary_check(&self) -> Result<(), String> {
        // A standing policy decides every execution, so nothing is ever forgotten.
        if self.policy.is_some() {
            return Ok(());
        }

        if let Some(Err(message)) = self
            .next_decision
            .as_ref()
            .map(|decision| self.core.is_honorable(decision))
        {
            return Err(message);
        }

        if self.next_decision.is_none() && !self.hold && self.core.has_pending_input() {
            Err(render_forgotten_error(
                self.core.location_meta(),
                self.target.location().cluster_id,
                self.core.describe_pending().as_deref(),
                "script a decision (e.g. `.release(..)` / `.reveal(..)`) or call `.pause()` if buffering is intended",
            ))
        } else {
            Ok(())
        }
    }
}

/// The scripted variation of an in-tick hook: wraps the ordinary inline hook type (which
/// implements the per-kind decision semantics via [`ScriptableInlineHook`]) together with
/// the queued decision. There is no hold state: an in-tick hook's input exists only
/// *during* one tick execution, so there is no buffering across scheduling boundaries to
/// declare with the pause family.
pub struct ScriptedInline<H: ScriptableInlineHook> {
    core: H,
    target: ScriptTarget,
    next_decision: Option<H::Decision>,
}

impl<H: ScriptableInlineHook> ScriptedInline<H> {
    pub fn new(core: H, target: ScriptTarget) -> Self {
        Self {
            core,
            target,
            next_decision: None,
        }
    }
}

impl<H: ScriptableInlineHook> ScriptedHookControl for ScriptedInline<H> {
    fn target(&self) -> ScriptTarget {
        self.target.clone()
    }

    fn location_meta(&self) -> HookLocationMeta {
        self.core.location_meta()
    }

    fn has_decision(&self) -> bool {
        self.next_decision.is_some()
    }

    fn describe_decision(&self) -> Option<String> {
        self.next_decision.as_ref().map(ScriptDecision::describe)
    }

    fn describe_pending(&self) -> Option<String> {
        self.core.describe_pending()
    }

    fn install_decision(&mut self, decision_blob: &[u8]) {
        assert!(self.next_decision.is_none());
        self.next_decision = Some(
            bincode::deserialize(decision_blob)
                .expect("internal error: scripted inline decision had the wrong type"),
        );
    }

    fn status_blob(&self) -> Vec<u8> {
        bincode::serialize(&self.core.status()).unwrap()
    }

    // The pause family is meaningless for an in-tick hook (see the struct docs), so holds
    // are accepted and ignored.
    fn set_hold(&mut self, _hold: bool) {}
    fn release_hold(&mut self) {}
    fn set_auto_pause(&mut self, _auto_pause: bool) {}

    // In-tick hooks have no cross-execution buffer to place under a standing policy; the
    // handle types that offer `omit` never bind to them.
    fn install_policy(&mut self, _policy_blob: &[u8]) {
        abort!("in-tick hooks do not support a standing policy");
    }
    fn policy_trace_blob(&self) -> Vec<u8> {
        bincode::serialize(&PolicyTrace::default()).unwrap()
    }
}

impl<H: ScriptableInlineHook> ScriptedInlineHook for ScriptedInline<H> {
    fn has_pending_input(&self) -> bool {
        self.core.has_pending_input()
    }

    fn run_decision(&mut self, log_writer: Option<&mut dyn std::fmt::Write>) -> Result<(), String> {
        let decision = self.next_decision.take();
        if decision.is_none() && !self.core.only_one_possible_decision() {
            return Err(render_forgotten_error(
                self.core.location_meta(),
                self.target.location().cluster_id,
                ScriptableInlineHook::describe_pending(&self.core).as_deref(),
                "script an explicit decision (e.g. `.order(..)`); in-tick hooks cannot be paused",
            ));
        }
        self.core.apply_scripted(decision, log_writer)
    }

    fn location_meta(&self) -> HookLocationMeta {
        self.core.location_meta()
    }
}

/// Renders the error for a scripted hook that holds buffered input with neither a
/// decision nor a declared pause. `member` names the cluster member whose instance is
/// forgotten (`None` for hooks on processes), `pending` describes the buffered input
/// (`None` renders a generic placeholder), and `help` suggests the appropriate scripting
/// calls for the hook's kind.
fn render_forgotten_error(
    location: HookLocationMeta,
    member: Option<u32>,
    pending: Option<&str>,
    help: &str,
) -> String {
    let HookLocationMeta {
        location: loc,
        line,
        caret_indent: caret,
    } = location;
    let pending = pending.unwrap_or("pending input");
    let member = member
        .map(|m| format!(" (cluster member {m})"))
        .unwrap_or_default();
    format!(
        "scripted hook has buffered input but no decision:\n--> {loc}{member}\n |{line}\n |{caret}^ {pending}\nhelp: {help}"
    )
}
