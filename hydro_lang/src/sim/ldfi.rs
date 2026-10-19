//! Lineage-driven fault injection (LDFI) for Hydro simulations.
//!
//! LDFI (Alvaro, Rosen, Hellerstein, SIGMOD 2015) searches for the faults that make a
//! distributed program violate a correctness goal, using the *lineage* of successful runs
//! to decide which faults are worth trying: if a run succeeded because messages
//! `D = {m1, m2, ...}` were delivered, then any fault set that leaves all of `D` intact
//! cannot break that run, so the next hypothesis must kill at least one of them. Each
//! hypothesis run that still succeeds reveals a new support (the retry, the redundant
//! path, the echo) which becomes a new clause, until either a fault set breaks the goal (a
//! bug within the fault model) or no fault set can hit every support (a certificate within
//! bounds).
//!
//! # The fault point
//!
//! The simulator puts a **filter** in front of every network send (see
//! [`sim::runtime::lineage`](crate::sim::runtime)). Every `Network` node of the program is a
//! fault point by construction; nothing in the program or the harness registers channels.
//! The transport the program declared is its fault model: a `lossy_delayed_forever` channel
//! admits omission, a `fail_stop` channel admits only its sender's crash.
//!
//! # Fault vocabulary
//!
//! - **Omission** `omit(message)`: the filter drops the send, identified by its channel,
//!   sender, destination and wire bytes. Every byte-identical copy on that channel is
//!   dropped while finite failures last ([`Run::end_of_finite_failures`]), so a
//!   retransmission with the same payload is the same message.
//! - **Crash** `crash(node after m)`, available for nodes registered with
//!   [`Ldfi::crashable`] / [`Ldfi::crashable_cluster`]: the node performs every send up to
//!   and including `m` and every later send is dropped at the filter (and its external
//!   outputs are suppressed once it is dead). This is Molly's definition of a crash, named
//!   by a message rather than a send count so the fault means the same thing in every run,
//!   and it can split the fan-out of one broadcast.
//!
//! **Every failure happens at a time `≤ EFF`** (Molly's fault model): omissions apply only
//! while finite failures last, and a crash happens only if its message is sent before the
//! end of finite failures in the run at hand. Crash points are *named* by a pre-`EFF` send of
//! the run that minted them (or are `before any send`); in the run of another hypothesis the
//! same message may be sent after `EFF`, or never, and the crash then does not happen there
//! (`FaultState::filter` in `sim::runtime::lineage`). The run after `EFF` is failure-free
//! recovery, the window in which the protocol is expected to converge. The search's notion
//! of what a crash kills follows the simulator's exactly ([`Dependency::killed_by`]).
//!
//! # Lineage
//!
//! Lineage is **execution-granular**. The simulator records every execution (an async run
//! or a tick execution), every send with its fate and a delivery id, every receive by the
//! delivery id it took off the channel, which hook releases fed each tick execution, and
//! which execution produced each `sim_output` value. The support of a goal output is the
//! backward closure from the execution that produced it: a tick execution depends on the
//! channel messages its hooked batches released into it, on the arrivals at its node that
//! an unhooked batch may have released into it, and, as a sound over-approximation of
//! persisted state, on the earlier executions at its node; an async run depends on what it
//! received and on the tick executions at its location since its previous run. This is
//! [`Level::Execution`]. The precise level ([`Level::Precise`], the default) replaces these
//! rules with the delivery ids each element was computed from where the program carries
//! record provenance (`SimFlow::with_provenance`), and derives every other record (one the
//! provenance rewrite could not see into, or any record of a program compiled without
//! provenance) by counterfactual replay of the node, with the execution closure as the
//! fallback where replay is inconclusive.
//!
//! Scripted batch hooks are optional precision, not a soundness requirement: a hooked
//! batch's releases say exactly which tick execution consumed a message, and are what
//! counterfactual replay feeds. The search places every scripted batch hook of the program
//! under a traced pass-through policy; the harness never scripts them.
//!
//! A support is a set of [`Dependency`]s (message, sender, ordinal). A fault *kills* a
//! dependency if it is `omit` of that message on a lossy channel, or `crash(sender, m)`
//! with `m` sent before the dependency. A hypothesis is a hitting set under `kills`; crash
//! literals are capped by [`Ldfi::crash_budget`].
//!
//! # Goals
//!
//! - [`Ldfi::goal`]: existential ("some value was output"): every output is one derivation.
//! - [`Ldfi::goal_all_members`]: a conjunction over the *correct* (non-crashed) members of
//!   a cluster, as in reliable broadcast's agreement: a derivation of the conjunction is one
//!   derivation per correct member, unioned. If no correct member produced output the goal
//!   held vacuously and contributes no clause.
//! - [`Ldfi::goal_witnessed`]: the body names the facts ([`Run::witness`], [`Run::require`]).
//!
//! The body itself decides whether the goal held (it receives the [`Run`] and can ask which
//! nodes crashed), so a conditional invariant like "if any correct member delivered, all
//! did" is expressed directly.
//!
//! # Composition with the schedule explorer, and bounds
//!
//! Each hypothesis is run under [`CompiledSim::exhaustive`] or a sampled exploration: the
//! fault set is fixed and the simulator explores the batching/ordering schedules under it.
//! The body drives time (timers are `sim_input`s), so the horizon `EOT` is what the body
//! fires; the end of finite failures `EFF` is declared by [`Run::end_of_finite_failures`],
//! after which omissions stop and only messages sent before it are omission candidates.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Debug, Display};
use std::panic::RefUnwindSafe;
use std::rc::Rc;

use serde::Serialize;

use crate::compile::builder::ExternalPortId;
use crate::live_collections::stream::{Ordering, Retries};
use crate::location::dynamic::LocationId;
use crate::location::{Cluster, Location, Process, TaglessMemberId};
use crate::sim::compiled::{
    CompiledSim, TracedHook, lineage_events, lineage_handles, lineage_port,
    lineage_provenance_enabled, lineage_slices, replay_all_hooks, trace_all_hooks,
};
pub use crate::sim::observe::{LabelPlan, NotCheckedReason};
use crate::sim::runtime::{
    CrashPoint, DeliveryId, ExecId, LineageEvent, NodeKey, SendFate, SendKey,
};
use crate::sim::slice::{ChannelInfo, EcCollection, Slices};
use crate::sim::spec::{Spec, SpecParts};
use crate::sim::viz::Trace;
use crate::sim::viz::provenance::{
    ProvEdgeKind, ProvFate, ProvGraph, ProvKey, ProvLevel, ProvMessage, ProvNodeKind, ProvRecorder,
};
use crate::sim::viz::trace::{Crash, Fate, TraceEvent, TraceEventKind, TraceMessage, TraceNode};
use crate::sim::{SimClusterReceiver, SimReceiver};

/// A message as the omission vocabulary names it: the channel it travelled on, its sender
/// and destination (members, for cluster endpoints), and its payload. Identity is by those
/// four: omitting a message omits every byte-identical copy from that sender to that
/// destination on that channel while finite failures last.
#[derive(Clone)]
pub struct Message {
    /// The channel's label (see [`ChannelInfo::label`]).
    pub channel: String,
    /// The channel's sink id.
    channel_id: usize,
    /// The sending member, for a channel out of a cluster.
    pub from_member: Option<u32>,
    /// The receiving member, for a channel into a cluster.
    pub to_member: Option<u32>,
    /// The wire bytes.
    payload: Vec<u8>,
    /// The payload's `Debug` rendering, for reports.
    pub display: String,
}

impl Message {
    fn key(&self) -> (usize, Option<u32>, Option<u32>, &[u8]) {
        (
            self.channel_id,
            self.from_member,
            self.to_member,
            &self.payload,
        )
    }

    /// The channel's sink id.
    pub(crate) fn channel_id(&self) -> usize {
        self.channel_id
    }

    /// The wire bytes.
    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// `label[from→to]`: the channel with its endpoints, omitting the brackets for a
    /// process-to-process channel.
    fn endpoint(&self) -> String {
        render_endpoint(&self.channel, self.from_member, self.to_member)
    }
}

fn render_endpoint(label: &str, from_member: Option<u32>, to_member: Option<u32>) -> String {
    match (from_member, to_member) {
        (None, None) => label.to_owned(),
        (from, to) => format!(
            "{label}[{}→{}]",
            from.map(|m| m.to_string()).unwrap_or_default(),
            to.map(|m| m.to_string()).unwrap_or_default()
        ),
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Message {}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.endpoint(), self.display)
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

/// One fault.
///
/// The derived order is only for sets; the search orders same-size hypotheses weakest first
/// (a crash after a later message before a crash after an earlier one) using the send
/// positions it has observed, so a hypothesis leaves as much of the protocol running as a
/// minimal hitting set allows.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fault {
    /// Discard every copy of this message on its channel.
    Omit(Message),
    /// The node dies right after sending `after` (`None`: before its first send). Named by a
    /// message rather than a send count so the fault means the same thing in every run.
    Crash {
        /// The node label (`label` for a process, `label@member` for a cluster member).
        node: String,
        /// The last message the node sends.
        after: Option<SendId>,
    },
}

/// A message a node sent, as the identity of a crash point: the channel (sink id), the
/// destination member, and the wire bytes. `display` is derived and not part of identity.
#[derive(Clone)]
pub struct SendId {
    channel: usize,
    to_member: Option<u32>,
    bytes: Vec<u8>,
    display: String,
}

impl PartialEq for SendId {
    fn eq(&self, other: &Self) -> bool {
        (self.channel, self.to_member, &self.bytes)
            == (other.channel, other.to_member, &other.bytes)
    }
}

impl Eq for SendId {}

impl PartialOrd for SendId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SendId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.channel, self.to_member, &self.bytes).cmp(&(
            other.channel,
            other.to_member,
            &other.bytes,
        ))
    }
}

impl Debug for SendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl Display for SendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl Debug for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Omit(m) => write!(f, "omit({m})"),
            Fault::Crash {
                node,
                after: Some(m),
            } => write!(f, "crash({node} after {m})"),
            Fault::Crash { node, after: None } => write!(f, "crash({node} before any send)"),
        }
    }
}

impl Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

/// A hypothesis: the faults to inject in a run.
pub type FaultSet = BTreeSet<Fault>;

/// One delivered message in a goal's support, with what is known about its origin.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependency {
    /// The message, by channel and payload.
    pub message: Message,
    /// The sending node's label and the message's position among that node's sends in the
    /// run it was observed in, when the sender is crashable. The position is exact (not
    /// clamped to the end of finite failures): together with the run's send sequence and its
    /// pre-`EFF` send counts it decides whether a crash point kills the delivery (see
    /// [`Self::killed_by`]).
    pub sender: Option<(String, u64)>,
    /// Whether the channel admits omission.
    pub lossy: bool,
}

/// Every crashable node's send sequence in one run, which gives crash points their meaning
/// there: `crash(node after m)` kills the sends after `m`'s position.
pub type SendSequences = BTreeMap<String, Vec<SendId>>;

/// Per crashable node, how many of its sends in one run happened before the end of finite
/// failures (see [`Schedule::pre_eff_sends`]).
pub type PreEffSends = BTreeMap<String, usize>;

impl Dependency {
    /// Whether `fault` prevents this delivery, in a run whose crashable nodes sent `sends`,
    /// `pre_eff` of them before the end of finite failures.
    ///
    /// A crash point is named by a message `m`, and the simulator kills the node right after
    /// it sends `m` **if that happens before `EFF`** in this run (every failure happens at a
    /// time `≤ EFF`; see the module docs). So the relation is exact position arithmetic in
    /// this run: the crash kills the delivery iff `m` is among the sender's pre-`EFF` sends
    /// and was sent before the delivery. A message sent before `EFF` in the run that minted
    /// the crash point may be sent after it in the run of another hypothesis; the crash then
    /// does not happen there and kills nothing. Clamping the delivery's own position to `EFF`
    /// would make the clause claim a crash kills nothing of what it does kill (a post-`EFF`
    /// delivery is killed by every earlier pre-`EFF` crash), and a hypothesis that breaks the
    /// goal would never be a hitting set (a false certificate).
    pub fn killed_by(&self, fault: &Fault, sends: &SendSequences, pre_eff: &PreEffSends) -> bool {
        match fault {
            Fault::Omit(message) => self.lossy && *message == self.message,
            Fault::Crash { node, after } => {
                let Some((sender, ordinal)) = &self.sender else {
                    return false;
                };
                if sender != node {
                    return false;
                }
                match after {
                    None => true,
                    Some(m) => {
                        let fires_before = pre_eff.get(node).copied().unwrap_or(0);
                        sends
                            .get(node)
                            .and_then(|seq| seq.iter().take(fires_before).position(|s| s == m))
                            .is_some_and(|at| (at as u64) < *ordinal)
                    }
                }
            }
        }
    }

    /// The weakest faults that kill this delivery: its omission (if lossy) and the crash of
    /// its sender right after the send preceding this one (if crashable).
    ///
    /// Crash points are named by pre-`EFF` sends only (`pre_eff`): every failure happens at
    /// a time `≤ EFF`, and the run after `EFF` is the window in which the protocol is expected
    /// to converge. A delivery sent after `EFF` is therefore killed by every crash point of
    /// its sender that fires in this run, and the weakest of those is the crash right after
    /// the sender's last pre-`EFF` send. [`Self::killed_by`] uses the exact position.
    fn candidate_faults(&self, sends: &SendSequences, pre_eff: &PreEffSends) -> Vec<Fault> {
        let mut faults = vec![];
        if self.lossy {
            faults.push(Fault::Omit(self.message.clone()));
        }
        if let Some((sender, ordinal)) = &self.sender {
            let cap = pre_eff.get(sender).copied().unwrap_or(0) as u64;
            let ordinal = (*ordinal).min(cap);
            let after = (ordinal > 0)
                .then(|| {
                    sends
                        .get(sender)
                        .and_then(|seq| seq.get(ordinal as usize - 1))
                        .cloned()
                })
                .flatten();
            faults.push(Fault::Crash {
                node: sender.clone(),
                after,
            });
        }
        faults
    }
}

impl Debug for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some((sender, ordinal)) = &self.sender {
            write!(f, "←{sender}#{ordinal}")?;
        }
        Ok(())
    }
}

impl Display for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl From<&Dependency> for ProvMessage {
    fn from(dependency: &Dependency) -> Self {
        ProvMessage {
            channel: dependency.message.channel.clone(),
            from_member: dependency.message.from_member,
            to_member: dependency.message.to_member,
            display: dependency.message.display.clone(),
            sender: dependency.sender.clone(),
            lossy: dependency.lossy,
        }
    }
}

/// A derivation of the goal: the deliveries it depended on. Killing any one of them kills
/// the derivation.
pub type Derivation = BTreeSet<Dependency>;

/// One explored schedule of one hypothesis run.
#[derive(Clone, Debug)]
pub struct Schedule {
    /// Whether the goal held.
    pub goal_held: bool,
    /// The goal's derivations in this schedule (see the module docs on goals).
    pub derivations: Vec<Derivation>,
    /// The why-provenance graph behind each derivation, parallel to `derivations`: the DAG
    /// the backward closure walked, from the goal down to the deliveries
    /// (see [`crate::sim::viz::provenance`]). Empty unless the search was configured with
    /// [`Ldfi::with_provenance_graphs`].
    pub provenance: Vec<ProvGraph>,
    /// Every delivery through a registered channel that some fault could have prevented: the
    /// fault universe a lineage-blind search sees.
    pub observed: Derivation,
    /// Every message delivered through a registered channel, in delivery order.
    pub delivered: Vec<Message>,
    /// Every message an installed omission discarded.
    pub dropped: Vec<Message>,
    /// Sends dropped by crashes: `(node, ordinal)`.
    pub crashed_sends: Vec<(String, u64)>,
    /// Every crashable node's send sequence (the meaning of crash points in this run).
    pub sends: Rc<SendSequences>,
    /// Per crashable node, how many of its sends happened before the end of finite failures:
    /// the crash points that can fire in this run are `crash(node before any send)` and
    /// `crash(node after m)` for those `m` only, and they are the only ones the search
    /// *proposes*. Every failure happens at a time `≤ EFF` (Molly's fault model, enforced by
    /// the simulator's filter): a crash point named by a message that this run sends only
    /// after `EFF`, or never, does not fire here. Whether a crash point kills a delivery is
    /// exact position arithmetic within the pre-`EFF` prefix ([`Dependency::killed_by`]).
    pub pre_eff_sends: PreEffSends,
    /// The lineage of the outputs the body required (the goal's pre-condition), whose sends
    /// the search wants to preserve.
    pub requirements: Derivation,
    /// Deliveries at registered hooks that could not be traced to a send (a lineage gap).
    pub unmatched_releases: usize,
    /// Whether the body declared the run vacuous ([`Run::mark_vacuous`]: the goal's
    /// pre-condition failed, so the run says nothing about the protocol).
    pub vacuous: bool,
    /// Counterfactual replays run to derive this schedule's lineage (at records without usable
    /// provenance under [`Level::Precise`], and at the observer for a hand-written
    /// specification view).
    pub replays: usize,
    /// Derivation steps where the node did not reproduce its output under replay and the
    /// closure fell back to the recorded lineage.
    pub replay_fallbacks: usize,
    /// The run's event trace, for visualization (see [`crate::sim::viz`]). Retained for every
    /// failing schedule, and for every schedule under [`Ldfi::with_traces`].
    pub trace: Option<Rc<Trace>>,
}

/// A derivation together with the send sequences of the run it was observed in.
#[derive(Clone, Debug)]
pub struct Clause {
    /// The deliveries the goal was derived from.
    pub deps: Derivation,
    /// The run's send sequences.
    pub sends: Rc<SendSequences>,
    /// The run's pre-`EFF` send counts (see [`Schedule::pre_eff_sends`]): the crash points
    /// that fire in this run, and so the ones this clause proposes and is hit by.
    pub pre_eff_sends: PreEffSends,
    /// The crashes of the hypothesis the derivation was observed under. A derivation found in
    /// a world where a node was absent is sufficient only there: a goal such as agreement is
    /// not monotone in a node's presence (the absent node, alive, may act against it), so a
    /// hypothesis that does not include these crashes is treated as hitting the clause.
    pub premises: BTreeSet<Fault>,
}

impl Clause {
    /// Whether `fault` kills some delivery of this clause.
    pub fn hit_by(&self, fault: &Fault) -> bool {
        self.deps
            .iter()
            .any(|dep| dep.killed_by(fault, &self.sends, &self.pre_eff_sends))
    }

    /// Every fault that kills some delivery of this clause.
    pub fn candidate_faults(&self) -> impl Iterator<Item = Fault> + '_ {
        self.deps
            .iter()
            .flat_map(|dep| dep.candidate_faults(&self.sends, &self.pre_eff_sends))
    }
}

/// One hypothesis and the schedules explored under it.
#[derive(Clone, Debug)]
pub struct Hypothesis {
    /// The injected faults.
    pub faults: FaultSet,
    /// Every schedule explored under the hypothesis.
    pub schedules: Vec<Schedule>,
}

/// What a certificate is worth: whether the hitting-set inference behind it ("a fault set that
/// leaves a derivation intact cannot break the view") is known to be valid for the view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grade {
    /// The specification is the generated one ([`Spec::eventual_consistency`]): a universal
    /// over a fact domain and a member domain the hypothesis fixes, whose derivation implies
    /// it by construction. The certificate holds within the fault model and the explored
    /// schedules.
    Generated,
    /// A hand-written `PRE`/`POST`. No soundness claim is made for the inference: the view may
    /// be falsified by an *extra* fact (agreement, uniqueness, a final value), which no
    /// derivation names. The certificate is relative to the recorded derivations persisting
    /// under the fault sets it excluded.
    HandWritten,
    /// A goal that is not a specification (the first version's `goal` / `witness` API).
    Goal,
}

impl Display for Grade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Grade::Generated => write!(
                f,
                "within the fault model and the explored schedules (generated specification)"
            ),
            Grade::HandWritten => write!(
                f,
                "RELATIVE to the recorded derivations persisting under the excluded fault sets (hand-written specification: no soundness claim for the inference)"
            ),
            Grade::Goal => write!(f, "relative to the recorded derivations (goal API)"),
        }
    }
}

/// The result of an LDFI search.
#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a search result, produced once; the counterexample's schedule is what the caller reads"
)]
pub enum Outcome {
    /// A fault set under which the goal fails in some schedule.
    Counterexample {
        /// The faults.
        faults: FaultSet,
        /// The failing schedule.
        schedule: Schedule,
        /// Every hypothesis run, in order (the last one is the counterexample).
        runs: Vec<Hypothesis>,
    },
    /// No fault set within the bounds (horizon, finite-failure window, crash budget) kills
    /// every observed derivation.
    Certificate {
        /// The derivations that could not all be killed.
        derivations: Vec<Derivation>,
        /// Every hypothesis run, in order.
        runs: Vec<Hypothesis>,
        /// How each hypothesis's schedules were explored; a certificate is a proof only under
        /// [`Exploration::Exhaustive`].
        exploration: Exploration,
        /// What the certificate is worth (see [`Grade`]).
        grade: Grade,
    },
    /// The hypothesis budget ran out before the search finished.
    Budget {
        /// Every hypothesis run, in order.
        runs: Vec<Hypothesis>,
    },
    /// The failure-free run was vacuous in every explored schedule: the goal's pre-condition
    /// (a [`Spec`]'s `PRE`, or [`Run::mark_vacuous`]) never held, so there is nothing to
    /// protect and nothing was searched. A verdict about the harness, not the protocol.
    Vacuous {
        /// The one hypothesis run (the failure-free run).
        runs: Vec<Hypothesis>,
    },
    /// Every admissible fault set over the observed fault universe was tried without a
    /// violation ([`Strategy::Blind`] only; lineage-driven search ends with a
    /// [`Outcome::Certificate`] instead).
    Exhausted {
        /// Every hypothesis run, in order.
        runs: Vec<Hypothesis>,
    },
}

impl Hypothesis {
    /// Every crash point observed in this hypothesis's schedules (a crashable node before its
    /// first send, or after any message it sent): the crash vocabulary a lineage-blind
    /// searcher can be given.
    pub fn crash_points(&self) -> Vec<Fault> {
        let mut points: BTreeSet<Fault> = BTreeSet::new();
        for schedule in &self.schedules {
            for (node, sends) in schedule.sends.iter() {
                points.insert(Fault::Crash {
                    node: node.clone(),
                    after: None,
                });
                let pre_eff = schedule.pre_eff_sends.get(node).copied().unwrap_or(0);
                for send in sends.iter().take(pre_eff) {
                    points.insert(Fault::Crash {
                        node: node.clone(),
                        after: Some(send.clone()),
                    });
                }
            }
        }
        points.into_iter().collect()
    }

    /// The messages observed on lossy channels in this hypothesis's schedules: the omission
    /// vocabulary a lineage-blind searcher can be given.
    pub fn omittable(&self) -> Vec<Message> {
        self.schedules
            .iter()
            .flat_map(|s| s.observed.iter())
            .filter(|d| d.lossy)
            .map(|d| d.message.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

impl Outcome {
    /// The hypotheses run, in order.
    pub fn runs(&self) -> &[Hypothesis] {
        match self {
            Outcome::Counterexample { runs, .. }
            | Outcome::Certificate { runs, .. }
            | Outcome::Budget { runs }
            | Outcome::Vacuous { runs }
            | Outcome::Exhausted { runs } => runs,
        }
    }

    /// The total number of schedules explored across all hypothesis runs.
    pub fn schedules(&self) -> usize {
        self.runs().iter().map(|h| h.schedules.len()).sum()
    }

    /// Whether the search found a violation.
    pub fn is_counterexample(&self) -> bool {
        matches!(self, Outcome::Counterexample { .. })
    }

    /// The counterexample's fault set, if one was found.
    pub fn counterexample(&self) -> Option<&FaultSet> {
        match self {
            Outcome::Counterexample { faults, .. } => Some(faults),
            _ => None,
        }
    }

    /// A certificate's grade (see [`Grade`]).
    pub fn grade(&self) -> Option<Grade> {
        match self {
            Outcome::Certificate { grade, .. } => Some(*grade),
            _ => None,
        }
    }
}

fn render_set<T: Display>(items: impl IntoIterator<Item = T>) -> String {
    let items: Vec<String> = items.into_iter().map(|i| i.to_string()).collect();
    format!("{{{}}}", items.join(", "))
}

impl Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Counterexample {
                faults,
                schedule,
                runs,
            } => {
                writeln!(
                    f,
                    "LDFI: goal violated after {} hypothesis run(s) by {}",
                    runs.len(),
                    render_set(faults)
                )?;
                writeln!(
                    f,
                    "  delivered in the failing schedule: {}",
                    render_set(&schedule.delivered)
                )?;
                if !schedule.dropped.is_empty() {
                    writeln!(f, "  omitted: {}", render_set(&schedule.dropped))?;
                }
                if !schedule.crashed_sends.is_empty() {
                    writeln!(
                        f,
                        "  sends lost to crashes: {}",
                        render_set(
                            schedule
                                .crashed_sends
                                .iter()
                                .map(|(node, ordinal)| format!("{node}#{ordinal}"))
                        )
                    )?;
                }
                Ok(())
            }
            Outcome::Certificate {
                derivations,
                runs,
                exploration,
                grade,
            } => {
                match exploration {
                    Exploration::Exhaustive => writeln!(
                        f,
                        "LDFI: no fault set within bounds violates the goal under any schedule ({} hypothesis run(s))",
                        runs.len()
                    )?,
                    Exploration::Fuzz { instances } => writeln!(
                        f,
                        "LDFI: no fault set within bounds violated the goal in {instances} sampled schedule(s) per hypothesis ({} hypothesis run(s)); not a proof",
                        runs.len()
                    )?,
                }
                writeln!(f, "  certificate {grade}")?;
                for derivation in derivations {
                    writeln!(f, "  derivation: {}", render_set(derivation))?;
                }
                Ok(())
            }
            Outcome::Budget { runs } => writeln!(
                f,
                "LDFI: hypothesis budget exhausted after {} run(s) without a verdict",
                runs.len()
            ),
            Outcome::Vacuous { runs } => writeln!(
                f,
                "LDFI: VACUOUS: the pre-condition never held in the failure-free run ({} schedule(s)); nothing to protect, nothing searched",
                runs.iter().map(|h| h.schedules.len()).sum::<usize>()
            ),
            Outcome::Exhausted { runs } => writeln!(
                f,
                "blind search: every admissible fault set over the observed universe was tried ({} run(s)), no violation",
                runs.len()
            ),
        }
    }
}

/// How the schedules of one hypothesis are explored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exploration {
    /// Every schedule ([`CompiledSim::exhaustive`]). A certificate is then a proof within
    /// the bounds: no admissible fault set breaks the goal under any schedule.
    Exhaustive,
    /// A bounded random sample of schedules per hypothesis ([`CompiledSim::fuzz_bounded`]).
    /// Tractable where exhaustive enumeration is not, at a price: a violation hiding in an
    /// unsampled schedule is missed, so a certificate is relative to the sampled schedules
    /// (the paper's Molly explored exactly one schedule per hypothesis).
    Fuzz {
        /// Schedules sampled per hypothesis.
        instances: usize,
    },
}

/// How the next hypothesis is chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Lineage-driven: a minimal fault set that kills every observed derivation of the goal.
    Lineage,
    /// The ablation: the same loop, the same fault universe (every candidate fault of every
    /// observed delivery), the same order, but no lineage, so hypotheses are simply the
    /// untried admissible fault sets by increasing weight. Measures what lineage buys.
    Blind,
}

/// How a goal's derivations are read off the outputs.
enum Goal {
    /// Every output on the port is one derivation.
    Any { port: ExternalPortId },
    /// A conjunction over the correct members of the cluster registered as `cluster`.
    AllMembers {
        port: ExternalPortId,
        cluster: String,
        members: Vec<u32>,
    },
    /// The body names the outputs that witness the goal ([`Run::witness`] /
    /// [`Run::witness_member`]); the derivation is the union of their lineages.
    Witnessed,
    /// A [`Spec`]: `PRE` and `POST` views evaluated at the end of the run. The goal is
    /// violated when `PRE` holds and `POST` does not; `POST`'s derivation is the clause and
    /// `PRE`'s the requirement.
    Spec(SpecParts),
}

/// The per-run handle the harness body receives: the installed faults and the one bound the
/// body controls, the end of finite failures.
pub struct Run<'a> {
    faults: &'a FaultSet,
    search: &'a Search,
    goal_port: Option<usize>,
    /// The lineage log length when `end_of_finite_failures` was called: releases recorded
    /// before this index are omission candidates.
    eff_cut: RefCell<Option<usize>>,
    /// Goal witnesses named by the body: `(simulator port, member, serialized value)`.
    witnesses: RefCell<Vec<(usize, Option<u32>, Vec<u8>)>>,
    /// Final-state goal witnesses ([`Run::witness_final`]).
    final_witnesses: RefCell<Vec<(usize, Option<u32>, Vec<u8>)>>,
    /// Pre-condition witnesses named by the body (outputs the goal is conditioned on).
    requirements: RefCell<Vec<(usize, Option<u32>, Vec<u8>)>>,
    /// Set by the body when the goal's pre-condition failed (nothing to check).
    vacuous: Cell<bool>,
    /// Which lineage to derive with.
    level: Level,
    /// The scripted batch hooks placed under a traced pass-through policy at install.
    hooks: RefCell<Vec<TracedHook>>,
}

impl Run<'_> {
    /// Marks this run as vacuous: the goal's pre-condition did not hold, so the goal holds
    /// trivially and the run says nothing about the protocol. Diagnostic only (counted by
    /// the random baselines as the masking rate).
    pub fn mark_vacuous(&self) {
        self.vacuous.set(true);
    }

    /// The faults installed for this run.
    pub fn faults(&self) -> &FaultSet {
        self.faults
    }

    /// Whether the node with this label crashes in this run (`label` for a process,
    /// `label@member` for a cluster member; see [`Self::crashed_member`]).
    pub fn crashed(&self, node: &str) -> bool {
        self.faults
            .iter()
            .any(|fault| matches!(fault, Fault::Crash { node: n, .. } if n == node))
    }

    /// Whether member `member` of the cluster registered as `cluster` crashes in this run.
    pub fn crashed_member(&self, cluster: &str, member: u32) -> bool {
        self.crashed(&member_label(cluster, member))
    }

    /// Names an output value on a process port as a witness of the goal (for a goal registered
    /// with [`Ldfi::goal_witnessed`]): the goal's derivation is the union of the lineages of
    /// every witness, read from the latest output equal to `value`. Call it for each fact the
    /// goal's post-condition rests on, e.g. the final leader view and the leader's log entry.
    pub fn witness<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimReceiver<T, O, R>,
        value: &T,
    ) {
        self.witnesses.borrow_mut().push((
            lineage_port(output.0),
            None,
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Like [`Self::witness`], for a value output by member `member` of a cluster.
    pub fn witness_member<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimClusterReceiver<T, O, R>,
        member: u32,
        value: &T,
    ) {
        self.witnesses.borrow_mut().push((
            lineage_port(output.0),
            Some(member),
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Names the **final** value on a process port as a witness of the goal: the fact is not
    /// that `value` was output at some point but that it was the *last* output, which also
    /// rests on nothing superseding it (a "final view", a "last committed index"). Under
    /// counterfactual replay a delivery is in its derivation if removing it changes the
    /// node's last output, not merely whether `value` ever appears; the recorded lineage
    /// levels derive the latest output equal to `value` as [`Self::witness`] does.
    pub fn witness_final<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimReceiver<T, O, R>,
        value: &T,
    ) {
        self.final_witnesses.borrow_mut().push((
            lineage_port(output.0),
            None,
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Like [`Self::witness_final`], for the last value output by member `member` of a
    /// cluster.
    pub fn witness_member_final<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimClusterReceiver<T, O, R>,
        member: u32,
        value: &T,
    ) {
        self.final_witnesses.borrow_mut().push((
            lineage_port(output.0),
            Some(member),
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Names an output the goal is *conditioned on* (its pre-condition: "if this write was
    /// acknowledged, then ..."). A fault set that prevents it makes the goal vacuous rather
    /// than violated, so the search wants to *preserve* it while killing the goal's own
    /// derivation: the crash right after each send in the requirement's lineage becomes a
    /// candidate fault (the weakest crash that keeps the requirement and kills what follows).
    /// Without this, a crash between two sends that are not in the goal's lineage is never
    /// proposed, and a bug that needs "the leader acknowledged, then died" can be missed.
    pub fn require<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimReceiver<T, O, R>,
        value: &T,
    ) {
        self.requirements.borrow_mut().push((
            lineage_port(output.0),
            None,
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Like [`Self::require`], for a value output by member `member` of a cluster.
    pub fn require_member<T: Serialize, O: Ordering, R: Retries>(
        &self,
        output: &SimClusterReceiver<T, O, R>,
        member: u32,
        value: &T,
    ) {
        self.requirements.borrow_mut().push((
            lineage_port(output.0),
            Some(member),
            bincode::serialize(value).unwrap(),
        ));
    }

    /// Declares the end of finite failures (Molly's `EFF`): the filter delivers everything
    /// from here on (crashes stand), and only messages sent so far are omission candidates.
    /// Optional: a bound a body may impose on where omissions can fall (a protocol that gives
    /// up after `n` rounds fails only if the losses fall in those rounds); without it, any
    /// send of the run may be omitted. Call it at most once.
    pub fn end_of_finite_failures(&self) {
        let mut cut = self.eff_cut.borrow_mut();
        assert!(
            cut.is_none(),
            "end_of_finite_failures() was called twice in one run"
        );
        *cut = Some(lineage_events().len());
        lineage_handles().faults.borrow_mut().end_finite_failures();
    }

    /// Installs the hypothesis into the instance's filter and places every scripted batch
    /// hook under a traced pass-through policy.
    fn install(&self) {
        let slices = lineage_slices();
        let handles = lineage_handles();
        let mut faults = handles.faults.borrow_mut();
        for fault in self.faults {
            match fault {
                Fault::Omit(message) => {
                    let channel = slices.channel(message.channel_id).unwrap_or_else(|| {
                        panic!("omission names unknown channel #{}", message.channel_id)
                    });
                    faults.omit(SendKey {
                        channel: message.channel_id,
                        from: (channel.from.clone(), message.from_member),
                        to_member: message.to_member,
                        bytes: message.payload.clone(),
                    });
                }
                Fault::Crash { node, after } => {
                    let key =
                        self.search.nodes.get(node).unwrap_or_else(|| {
                            panic!("crash fault names unregistered node {node:?}")
                        });
                    let point = match after {
                        None => CrashPoint::BeforeAnySend,
                        Some(send) => CrashPoint::AfterSend {
                            channel: send.channel,
                            to_member: send.to_member,
                            bytes: send.bytes.clone(),
                        },
                    };
                    faults.install(key.clone(), point);
                }
            }
        }
        drop(faults);
        *self.hooks.borrow_mut() = trace_all_hooks();
        // The specification's view of the fault model: which members stay up.
        if let Some(Goal::Spec(spec)) = &self.search.goal {
            let crashed: std::collections::HashSet<NodeKey> = self
                .faults
                .iter()
                .filter_map(|fault| match fault {
                    Fault::Crash { node, .. } => self.search.nodes.get(node).cloned(),
                    Fault::Omit(_) => None,
                })
                .collect();
            for feed in &spec.correct {
                feed.feed(&crashed);
            }
            // The generated specification's fact domain (the facts of the failure-free run).
            if let Some(facts) = &spec.facts {
                facts.feed();
            }
            // An automatic specification: which labelled collection this search is about.
            if let Some(all_labels) = &spec.all_labels {
                all_labels.feed();
            }
        }
    }

    fn finish(self, mut goal_held: bool) -> RunRecord {
        let events = lineage_events();
        let eff_cut = (*self.eff_cut.borrow()).unwrap_or(events.len());
        // A specification decides the verdict from its views' final values.
        let mut spec_pre: Option<(usize, Vec<u8>)> = None;
        let mut spec_post: Option<(usize, Vec<u8>)> = None;
        let mut spec_facts: BTreeSet<Vec<u8>> = BTreeSet::new();
        if let Some(Goal::Spec(spec)) = &self.search.goal {
            // The facts correct members held, for the generated specification's domain.
            if let Some(facts) = &spec.facts {
                let port = lineage_port(facts.port);
                spec_facts = events
                    .iter()
                    .filter_map(|event| match event {
                        LineageEvent::Output {
                            port: p,
                            bytes,
                            dropped: false,
                            ..
                        } if *p == port => Some(bytes.clone()),
                        _ => None,
                    })
                    .collect();
            }
            let last_output = |port: usize| -> Option<Vec<u8>> {
                events.iter().rev().find_map(|event| match event {
                    LineageEvent::Output {
                        port: p,
                        bytes,
                        dropped: false,
                        ..
                    } if *p == port => Some(bytes.clone()),
                    _ => None,
                })
            };
            let pre = match spec.pre {
                Some(port) => {
                    let port = lineage_port(port);
                    let last = last_output(port);
                    let holds = Spec::holds(last.as_deref());
                    spec_pre = last.filter(|_| holds).map(|bytes| (port, bytes));
                    holds
                }
                None => true,
            };
            let post_port = lineage_port(spec.post);
            let last = last_output(post_port);
            let post = Spec::holds(last.as_deref());
            spec_post = last.filter(|_| post).map(|bytes| (post_port, bytes));
            if !pre {
                self.vacuous.set(true);
            }
            goal_held = !pre || post;
        }
        RunRecord {
            goal_held,
            vacuous: self.vacuous.get(),
            spec_pre,
            spec_post,
            spec_facts,
            faults: self.faults.clone(),
            goal_port: self.goal_port,
            level: self.level,
            events,
            eff_cut,
            witnesses: self.witnesses.into_inner(),
            final_witnesses: self.final_witnesses.into_inner(),
            requirements: self.requirements.into_inner(),
            context: InstanceContext {
                slices: lineage_slices(),
                provenance: lineage_provenance_enabled(),
                hooks: self.hooks.into_inner(),
            },
        }
    }
}

/// What a [`Lineage`] needs from the simulation instance, captured while it is live: the
/// derivations are computed after the instance returns, because counterfactual replays are
/// instances themselves.
struct InstanceContext {
    slices: Rc<Slices>,
    /// Whether the program was compiled with record-level provenance.
    provenance: bool,
    /// The scripted batch hooks under a traced pass-through policy.
    hooks: Vec<TracedHook>,
}

/// Everything one run of the body produced, taken out of the instance; turned into a
/// [`Schedule`] by [`Self::schedule`] once the instance has returned.
struct RunRecord {
    goal_held: bool,
    vacuous: bool,
    /// Under a [`Spec`]: the port and final value of `PRE` / `POST`, when each held.
    spec_pre: Option<(usize, Vec<u8>)>,
    spec_post: Option<(usize, Vec<u8>)>,
    /// Under a generated [`Spec`]: the facts correct members held (bincode), the search's fact
    /// domain when this is the failure-free probe.
    spec_facts: BTreeSet<Vec<u8>>,
    faults: FaultSet,
    goal_port: Option<usize>,
    level: Level,
    events: Vec<LineageEvent>,
    eff_cut: usize,
    witnesses: Vec<(usize, Option<u32>, Vec<u8>)>,
    final_witnesses: Vec<(usize, Option<u32>, Vec<u8>)>,
    requirements: Vec<(usize, Option<u32>, Vec<u8>)>,
    context: InstanceContext,
}

/// The facts a generated specification published across `records`: the fact domain of the
/// search (see `Spec::eventual_consistency`).
fn learn_fact_domain(records: &[RunRecord]) -> BTreeSet<Vec<u8>> {
    records
        .iter()
        .flat_map(|record| record.spec_facts.iter().cloned())
        .collect()
}

impl RunRecord {
    fn crashed_member(&self, cluster: &str, member: u32) -> bool {
        let label = member_label(cluster, member);
        self.faults
            .iter()
            .any(|fault| matches!(fault, Fault::Crash { node: n, .. } if *n == label))
    }

    fn schedule(
        &self,
        search: &Search,
        replayer: Option<&Replayer<'_>>,
        retain_trace: bool,
        prov_graphs: bool,
    ) -> Schedule {
        let lineage = Lineage::new(
            &self.events,
            search,
            self.eff_cut,
            self.level,
            &self.context,
            replayer,
            prov_graphs,
        );
        // A failing schedule is rare and is what a counterexample shows: always keep its
        // trace. Successful schedules keep theirs only on request (the witness picture).
        let trace = (retain_trace || !self.goal_held).then(|| {
            Rc::new(build_trace(
                &lineage,
                &self.events,
                self.eff_cut,
                &self.faults,
            ))
        });
        let protocol = lineage.sent.iter().filter(|s| !s.observation);
        let crashed_sends = protocol
            .clone()
            .filter(|s| s.fate == SendFate::Crashed)
            .filter_map(|s| s.sender.clone())
            .collect();
        let delivered = protocol
            .clone()
            .filter(|s| s.fate == SendFate::Delivered)
            .map(|s| lineage.message(s))
            .collect();
        let dropped = protocol
            .filter(|s| s.fate == SendFate::Omitted)
            .map(|s| lineage.message(s))
            .collect();
        let mut vacuous = self.vacuous;
        let derivations = match (&search.goal, self.goal_port) {
            (Some(Goal::Any { .. }), Some(port)) => lineage.any_output(port),
            (
                Some(Goal::AllMembers {
                    cluster, members, ..
                }),
                Some(port),
            ) => {
                let correct: Vec<u32> = members
                    .iter()
                    .copied()
                    .filter(|m| !self.crashed_member(cluster, *m))
                    .collect();
                lineage.all_members(port, &correct)
            }
            (Some(Goal::Witnessed), _) => {
                if self.witnesses.is_empty() && self.final_witnesses.is_empty() {
                    vec![]
                } else {
                    lineage.prov_begin("witnessed outputs");
                    let mut derivation = lineage.witnessed(&self.witnesses);
                    derivation.extend(lineage.witnessed_final(&self.final_witnesses));
                    lineage.prov_keep(lineage.prov_end());
                    vec![derivation]
                }
            }
            // PRE held and POST held: POST's final value is the fact to derive.
            (Some(Goal::Spec(spec)), _) => match (&self.spec_pre, &self.spec_post) {
                (_, Some((port, bytes))) if !self.vacuous => {
                    lineage.prov_begin("POST");
                    let derivation = lineage.spec_view(*port, bytes, spec.facts.is_some());
                    let graph = lineage.prov_end();
                    if spec.facts.is_some() && derivation.is_empty() {
                        // The generated specification held because no correct member holds
                        // any fact of the domain (every producer was crashed before it
                        // produced), so nothing was required of anyone. Molly's vacuous
                        // case: the run yields no clause. (An empty clause would end the
                        // search with a certificate.)
                        vacuous = true;
                        vec![]
                    } else {
                        lineage.prov_keep(graph);
                        vec![derivation]
                    }
                }
                _ => vec![],
            },
            // No goal registered: everything delivered before EFF is the one derivation.
            _ => {
                lineage.prov_begin("every delivery before EFF");
                let derivation = lineage.all_pre_eff_deliveries();
                lineage.prov_keep(lineage.prov_end());
                vec![derivation]
            }
        };
        let provenance = lineage.kept.take();
        debug_assert!(
            !prov_graphs || provenance.len() == derivations.len(),
            "one provenance graph per derivation"
        );
        let mut requirements = if self.requirements.is_empty() {
            Derivation::new()
        } else {
            lineage.witnessed(&self.requirements)
        };
        if let (Some((port, bytes)), Some(Goal::Spec(spec))) = (&self.spec_pre, &search.goal) {
            requirements.extend(lineage.spec_view(*port, bytes, spec.facts.is_some()));
        }
        let (sends, pre_eff_sends) = lineage.send_sequences();
        Schedule {
            goal_held: self.goal_held,
            derivations,
            provenance,
            observed: lineage.all_deliveries(),
            delivered,
            dropped,
            crashed_sends,
            sends: Rc::new(sends),
            pre_eff_sends,
            requirements,
            unmatched_releases: lineage.unmatched_releases,
            vacuous,
            replays: lineage.replays.get(),
            replay_fallbacks: lineage.replay_fallbacks.get(),
            trace,
        }
    }
}

/// The nodes of a [`Trace`] as they are discovered, keyed by `(root location, member)`.
struct TraceNodes<'a> {
    nodes: Vec<TraceNode>,
    index: HashMap<NodeKey, usize>,
    labels: HashMap<&'a NodeKey, &'a String>,
    observer: Option<&'a LocationId>,
}

impl TraceNodes<'_> {
    fn node_of(&mut self, location: &LocationId, member: Option<u32>) -> usize {
        let key: NodeKey = (location.root().clone(), member);
        if let Some(i) = self.index.get(&key) {
            return *i;
        }
        let is_observer = self.observer == Some(&key.0);
        let crashable = self.labels.contains_key(&key);
        let label = match self.labels.get(&key) {
            Some(label) => (*label).clone(),
            None if is_observer => "observer".to_owned(),
            None => match member {
                Some(m) => format!("{:?}@{m}", key.0),
                None => format!("{:?}", key.0),
            },
        };
        self.nodes.push(TraceNode {
            label,
            location: format!("{:?}", key.0),
            member,
            is_observer,
            crashable,
            crash: None,
        });
        self.index.insert(key, self.nodes.len() - 1);
        self.nodes.len() - 1
    }
}

/// The run's [`Trace`] (see [`crate::sim::viz`]): the event log reduced to nodes, ordered
/// events and messages, with receives (top-level, or a scripted hook's releases into a
/// tick) paired to their sends the way the lineage pairs them.
fn build_trace(
    lineage: &Lineage<'_>,
    events: &[LineageEvent],
    eff_cut: usize,
    faults: &FaultSet,
) -> Trace {
    let search = lineage.search;
    let mut nodes = TraceNodes {
        nodes: vec![],
        index: HashMap::new(),
        labels: search
            .nodes
            .iter()
            .map(|(label, key)| (key, label))
            .collect(),
        observer: search.observer(),
    };

    // Messages, in send order, indexed like `lineage.sent`; their events are filled in below.
    let mut messages: Vec<TraceMessage> = lineage
        .sent
        .iter()
        .map(|record| TraceMessage {
            channel: lineage.channel_label(record.channel),
            channel_id: record.channel,
            display: render_payload(record),
            from: nodes.node_of(&record.from.0, record.from.1),
            to: nodes.node_of(&record.to.0, record.to.1),
            send_event: 0,
            recv_event: None,
            fate: match record.fate {
                SendFate::Delivered => Fate::Delivered,
                SendFate::Omitted => Fate::Omitted,
                SendFate::Crashed => Fate::Crashed,
            },
            observation: record.observation,
            lossy: lineage.channel(record).is_some_and(|c| c.lossy),
            delivery_id: 0,
            ordinal: record.sender.as_ref().map(|(_, ordinal)| *ordinal),
            payload_hash: crate::sim::viz::trace::payload_hash(&record.bytes),
        })
        .collect();

    // Events, in log order. Releases are paired to sends through the lineage's matching;
    // the matched records of an execution are consumed in order.
    let mut trace_events: Vec<TraceEvent> = vec![];
    let mut trace_eff: Option<usize> = None;
    let mut release_cursor: HashMap<ExecId, usize> = HashMap::new();
    let mut sent_cursor = 0usize;
    for (index, event) in events.iter().enumerate() {
        if index == eff_cut {
            trace_eff = Some(trace_events.len());
        }
        let exec_of = |exec: &ExecId| lineage.execs.get(exec);
        match event {
            LineageEvent::Exec {
                id,
                location,
                member,
                tick,
            } => {
                trace_events.push(TraceEvent {
                    node: nodes.node_of(location, *member),
                    exec: *id,
                    tick: *tick,
                    kind: TraceEventKind::Exec,
                });
            }
            LineageEvent::Sent {
                exec,
                id,
                from,
                from_member,
                ..
            } => {
                let message = sent_cursor;
                sent_cursor += 1;
                messages[message].send_event = trace_events.len();
                messages[message].delivery_id = *id;
                trace_events.push(TraceEvent {
                    node: nodes.node_of(from, *from_member),
                    exec: *exec,
                    tick: exec_of(exec).is_some_and(|(_, _, tick)| *tick),
                    kind: TraceEventKind::Send(message),
                });
            }
            LineageEvent::Recv {
                exec,
                to,
                to_member,
                send,
                ..
            } => {
                if let Some(message) = lineage.by_id.get(send).copied() {
                    messages[message].recv_event = Some(trace_events.len());
                    trace_events.push(TraceEvent {
                        node: nodes.node_of(to, *to_member),
                        exec: *exec,
                        tick: exec_of(exec).is_some_and(|(_, _, tick)| *tick),
                        kind: TraceEventKind::Recv(message),
                    });
                }
            }
            LineageEvent::Released {
                exec,
                hook_id,
                member,
                items,
            } => {
                let Some((location, _, tick)) = exec_of(exec) else {
                    continue;
                };
                let node = nodes.node_of(location, *member);
                let cursor = release_cursor.entry(*exec).or_insert(0);
                let releases = lineage.released.get(exec);
                for _ in items {
                    let release = releases.and_then(|r| r.get(*cursor));
                    *cursor += 1;
                    if let Some(message) = release
                        .filter(|r| r.hook_id == *hook_id)
                        .and_then(|r| r.send)
                    {
                        messages[message].recv_event = Some(trace_events.len());
                        trace_events.push(TraceEvent {
                            node,
                            exec: *exec,
                            tick: *tick,
                            kind: TraceEventKind::Recv(message),
                        });
                    }
                }
            }
            LineageEvent::Output {
                exec,
                member,
                bytes,
                dropped,
                ..
            } => {
                let Some((location, _, tick)) = exec_of(exec) else {
                    continue;
                };
                trace_events.push(TraceEvent {
                    node: nodes.node_of(location, *member),
                    exec: *exec,
                    tick: *tick,
                    kind: TraceEventKind::Output {
                        display: render_bytes(bytes),
                        dropped: *dropped,
                    },
                });
            }
        }
    }
    if eff_cut >= events.len() {
        // `finish` sets the cut to the log's length when the body never declared it.
        trace_eff = None;
    }
    // Where each installed crash took effect.
    for fault in faults {
        let Fault::Crash { node, after } = fault else {
            continue;
        };
        let Some(key) = search.nodes.get(node) else {
            continue;
        };
        let index = nodes.node_of(&key.0, key.1);
        nodes.nodes[index].crash = match after {
            None => Some(Crash::BeforeAnySend),
            Some(send) => lineage
                .sent
                .iter()
                .position(|record| {
                    record.from == *key
                        && record.channel == send.channel
                        && record.to.1 == send.to_member
                        && record.bytes == send.bytes
                })
                .map(|message| Crash::AfterEvent(messages[message].send_event)),
        };
    }
    // The observer last.
    let TraceNodes { nodes, .. } = nodes;
    let mut order: Vec<usize> = (0..nodes.len()).collect();
    order.sort_by_key(|i| nodes[*i].is_observer);
    let mut renumber = vec![0usize; nodes.len()];
    for (new, old) in order.iter().enumerate() {
        renumber[*old] = new;
    }
    for event in &mut trace_events {
        event.node = renumber[event.node];
    }
    for message in &mut messages {
        message.from = renumber[message.from];
        message.to = renumber[message.to];
    }
    Trace {
        nodes: order.into_iter().map(|i| nodes[i].clone()).collect(),
        events: trace_events,
        messages,
        eff_cut: trace_eff,
    }
}

/// A hex prefix of some wire bytes, for outputs whose `Debug` rendering is not recorded.
fn render_bytes(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().take(8).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    });
    format!("{hex}{}", if bytes.len() > 8 { "…" } else { "" })
}

/// A recorded send, resolved against the registered nodes.
struct SentRecord {
    exec: ExecId,
    channel: usize,
    from: NodeKey,
    to: NodeKey,
    bytes: Vec<u8>,
    /// The payload's `Debug` rendering, when its type has one.
    display: Option<String>,
    /// `(node label, ordinal)` when the sender is crashable. For an observation the ordinal
    /// is the number of protocol sends the node had performed by then (it is not a send of
    /// the protocol itself).
    sender: Option<(String, u64)>,
    fate: SendFate,
    /// Whether the send happened before the end of finite failures (an omission candidate,
    /// if its channel is lossy).
    pre_eff: bool,
    /// A send to the specification's observer: not a message of the protocol, not a fault
    /// point, transparent to lineage except for the sender having been alive to perform it.
    observation: bool,
    /// Delivery ids the record was computed from (record-level provenance; empty without).
    prov: Vec<u64>,
}

/// A hook release, resolved against the sends.
struct ReleaseRecord {
    hook_id: usize,
    payload: Vec<u8>,
    /// Index into `sent` of the matched send.
    send: Option<usize>,
}

/// A delivered goal-candidate output.
struct OutputRecord {
    exec: ExecId,
    sink: usize,
    port: usize,
    member: Option<u32>,
    bytes: Vec<u8>,
    /// Delivery ids the record was computed from (record-level provenance; empty without).
    prov: Vec<u64>,
}

/// A top-level receive, resolved against the sends by delivery id.
struct RecvRecord {
    /// Index into `sent` of the send.
    producer: Option<usize>,
}

/// The lineage graph of one run, indexed for backward closure.
struct Lineage<'a> {
    search: &'a Search,
    slices: Rc<Slices>,
    /// `exec` → `(location, member, is_tick)`.
    execs: BTreeMap<ExecId, (LocationId, Option<u32>, bool)>,
    /// Tick executions per `(tick location, member)`, ascending.
    ticks_at: HashMap<NodeKey, Vec<ExecId>>,
    /// Async runs per `(location, member)`, ascending.
    asyncs_at: HashMap<NodeKey, Vec<ExecId>>,
    /// Releases into each tick execution, each matched to the send that produced it.
    released: HashMap<ExecId, Vec<ReleaseRecord>>,
    /// Top-level receives by each async run.
    recvs: HashMap<ExecId, Vec<RecvRecord>>,
    /// Receives per node, in event order: `(exec, index into sent)`.
    recvs_at: HashMap<NodeKey, Vec<(ExecId, usize)>>,
    sent: Vec<SentRecord>,
    /// Index into `sent` by delivery id.
    by_id: HashMap<DeliveryId, usize>,
    /// Delivered goal-candidate outputs.
    outputs: Vec<OutputRecord>,
    /// Which lineage the closure uses.
    level: Level,
    /// Whether the program was compiled with record-level provenance.
    provenance: bool,
    /// The scripted batch hooks under a traced pass-through policy, by `(hook id, member)`.
    hooks: HashMap<(usize, Option<u32>), TracedHook>,
    /// Releases that matched no send although their hook batches a channel (diagnostic).
    unmatched_releases: usize,
    /// The replay oracle (see [`Level::Precise`]).
    replayer: Option<&'a Replayer<'a>>,
    /// Counterfactual derivations of sends, by index into `sent`.
    send_memo: RefCell<HashMap<usize, Derivation>>,
    /// Replays run so far.
    replays: Cell<usize>,
    /// Derivation steps that fell back to the recorded lineage.
    replay_fallbacks: Cell<usize>,
    /// The provenance-graph recorder, when the search asked for graphs
    /// ([`Ldfi::with_provenance_graphs`]); see [`crate::sim::viz::provenance`].
    prov: Option<RefCell<ProvRecorder>>,
    /// The recorded sub-graph of each memoized send derivation, by index into `sent`, spliced
    /// into every graph that reaches the send again.
    send_prov_memo: RefCell<HashMap<usize, ProvGraph>>,
    /// The finished graphs, one per derivation, in the order the derivations were produced.
    kept: RefCell<Vec<ProvGraph>>,
}

impl<'a> Lineage<'a> {
    fn new(
        events: &[LineageEvent],
        search: &'a Search,
        eff_cut: usize,
        level: Level,
        context: &InstanceContext,
        replayer: Option<&'a Replayer<'a>>,
        prov_graphs: bool,
    ) -> Self {
        let labels_by_node: HashMap<&NodeKey, &String> = search
            .nodes
            .iter()
            .map(|(label, key)| (key, label))
            .collect();
        let mut lineage = Lineage {
            search,
            slices: context.slices.clone(),
            execs: BTreeMap::new(),
            ticks_at: HashMap::new(),
            asyncs_at: HashMap::new(),
            released: HashMap::new(),
            recvs: HashMap::new(),
            recvs_at: HashMap::new(),
            sent: vec![],
            by_id: HashMap::new(),
            outputs: vec![],
            level,
            provenance: context.provenance,
            hooks: context
                .hooks
                .iter()
                .map(|hook| ((hook.hook_id, hook.member), hook.clone()))
                .collect(),
            unmatched_releases: 0,
            replayer,
            send_memo: RefCell::new(HashMap::new()),
            replays: Cell::new(0),
            replay_fallbacks: Cell::new(0),
            prov: prov_graphs.then(|| RefCell::new(ProvRecorder::new())),
            send_prov_memo: RefCell::new(HashMap::new()),
            kept: RefCell::new(vec![]),
        };
        let mut send_counts: HashMap<NodeKey, u64> = HashMap::new();
        // `(exec, hook, member, payload)` in event order, matched to sends below.
        let mut raw_releases: Vec<(ExecId, usize, Option<u32>, Vec<u8>)> = vec![];
        // `(exec, channel, node, delivery id)` in event order.
        let mut raw_recvs: Vec<(ExecId, usize, NodeKey, DeliveryId)> = vec![];
        for (index, event) in events.iter().enumerate() {
            match event {
                LineageEvent::Exec {
                    id,
                    location,
                    member,
                    tick,
                } => {
                    lineage
                        .execs
                        .insert(*id, (location.clone(), *member, *tick));
                    let at = if *tick {
                        &mut lineage.ticks_at
                    } else {
                        &mut lineage.asyncs_at
                    };
                    at.entry((location.clone(), *member)).or_default().push(*id);
                }
                LineageEvent::Released {
                    exec,
                    hook_id,
                    member,
                    items,
                } => {
                    raw_releases.extend(
                        items
                            .iter()
                            .map(|item| (*exec, *hook_id, *member, item.clone())),
                    );
                }
                LineageEvent::Sent {
                    exec,
                    id,
                    channel,
                    from,
                    from_member,
                    to,
                    to_member,
                    bytes,
                    display,
                    fate,
                    prov,
                } => {
                    let node = (from.clone(), *from_member);
                    let observation = search.observer() == Some(to);
                    let ordinal = send_counts.entry(node.clone()).or_insert(0);
                    let sender = labels_by_node
                        .get(&node)
                        .map(|label| ((*label).clone(), *ordinal));
                    if !observation {
                        *ordinal += 1;
                    }
                    lineage.by_id.insert(*id, lineage.sent.len());
                    lineage.sent.push(SentRecord {
                        exec: *exec,
                        observation,
                        channel: *channel,
                        from: node,
                        to: (to.clone(), *to_member),
                        bytes: bytes.clone(),
                        display: display.clone(),
                        sender,
                        fate: *fate,
                        pre_eff: index < eff_cut,
                        prov: prov.clone(),
                    });
                }
                LineageEvent::Recv {
                    exec,
                    channel,
                    to,
                    to_member,
                    send,
                } => {
                    raw_recvs.push((*exec, *channel, (to.clone(), *to_member), *send));
                }
                LineageEvent::Output {
                    exec,
                    sink,
                    port,
                    member,
                    bytes,
                    dropped,
                    prov,
                } => {
                    if !dropped {
                        lineage.outputs.push(OutputRecord {
                            exec: *exec,
                            sink: *sink,
                            port: *port,
                            member: *member,
                            bytes: bytes.clone(),
                            prov: prov.clone(),
                        });
                    }
                }
            }
        }

        // A receive names its send by delivery id.
        for (exec, _channel, node, id) in raw_recvs {
            let producer = lineage.by_id.get(&id).copied();
            if let Some(i) = producer {
                lineage
                    .recvs_at
                    .entry(node.clone())
                    .or_default()
                    .push((exec, i));
            }
            lineage
                .recvs
                .entry(exec)
                .or_default()
                .push(RecvRecord { producer });
        }

        // Match each hook release to the send that produced it. A hook's releases are the
        // deserialized items of what it batches, so the static slice names the channel(s)
        // whose sends it may release, and a release is matched against sends on those
        // channels to the hook's node (any send to that node when the slice could not
        // resolve the hook's feeds). A send is delivered at most once, so releases (in event
        // order) take the earliest unmatched delivered send with the same bytes that happened
        // before the release.
        let mut matched = vec![false; lineage.sent.len()];
        for (exec, hook_id, member, payload) in raw_releases {
            let Some(hook) = lineage.hooks.get(&(hook_id, member)) else {
                continue;
            };
            let to = hook.node.clone();
            let channels = lineage
                .slices
                .hook_channels
                .get(&hook_id)
                .filter(|c| !c.is_empty());
            let hit = lineage.sent.iter().enumerate().position(|(i, record)| {
                !matched[i]
                    && record.fate == SendFate::Delivered
                    && record.exec < exec
                    && record.to == to
                    && channels.is_none_or(|c| c.contains(&record.channel))
                    && payload_matches(&payload, &record.bytes, record.from.1)
            });
            if let Some(i) = hit {
                matched[i] = true;
            } else if channels.is_some() {
                lineage.unmatched_releases += 1;
            }
            lineage
                .released
                .entry(exec)
                .or_default()
                .push(ReleaseRecord {
                    hook_id,
                    payload,
                    send: hit,
                });
        }
        if std::env::var("LDFI_LINEAGE_DEBUG").is_ok() {
            #[expect(clippy::disallowed_methods, reason = "counts; order is irrelevant")]
            let releases: usize = lineage.released.values().map(Vec::len).sum();
            #[expect(clippy::disallowed_methods, reason = "counts; order is irrelevant")]
            let matched: usize = lineage
                .released
                .values()
                .flatten()
                .filter(|r| r.send.is_some())
                .count();
            #[expect(clippy::disallowed_methods, reason = "counts; order is irrelevant")]
            let recvs: usize = lineage.recvs.values().map(Vec::len).sum();
            eprintln!(
                "lineage: {} execs, {} sends ({} omitted, {} lost to crashes), {} releases ({matched} matched to sends), {} recvs, {} outputs",
                lineage.execs.len(),
                lineage.sent.len(),
                lineage
                    .sent
                    .iter()
                    .filter(|s| s.fate == SendFate::Omitted)
                    .count(),
                lineage
                    .sent
                    .iter()
                    .filter(|s| s.fate == SendFate::Crashed)
                    .count(),
                releases,
                recvs,
                lineage.outputs.len(),
            );
            for output in &lineage.outputs {
                eprintln!(
                    "lineage:   output exec {} sink {} port {} member {:?} {} bytes prov {:?}",
                    output.exec,
                    output.sink,
                    output.port,
                    output.member,
                    output.bytes.len(),
                    output.prov
                );
            }
            for (exec, (location, member, tick)) in &lineage.execs {
                let releases = lineage.released.get(exec).map_or(0, Vec::len);
                let recvs = lineage.recvs.get(exec).map_or(0, Vec::len);
                let sends = lineage.sent.iter().filter(|s| s.exec == *exec).count();
                let outputs = lineage.outputs.iter().filter(|o| o.exec == *exec).count();
                if releases + recvs + sends + outputs > 0 {
                    eprintln!(
                        "lineage:   exec {exec} {}{:?} member {:?}: {releases} releases, {recvs} recvs, {sends} sends, {outputs} outputs",
                        if *tick { "tick " } else { "async " },
                        location,
                        member
                    );
                }
            }
        }
        lineage
    }

    /// The channel a send travelled on.
    fn channel(&self, record: &SentRecord) -> Option<&ChannelInfo> {
        self.slices.channel(record.channel)
    }

    /// The channel's label, `ch#id` if the channel is unknown.
    fn channel_label(&self, channel: usize) -> String {
        self.slices
            .channel(channel)
            .map(|c| c.label.clone())
            .unwrap_or_else(|| format!("ch{channel}"))
    }

    /// A send as the omission vocabulary names it.
    fn message(&self, record: &SentRecord) -> Message {
        Message {
            channel: self.channel_label(record.channel),
            channel_id: record.channel,
            from_member: record.from.1,
            to_member: record.to.1,
            payload: record.bytes.clone(),
            display: render_payload(record),
        }
    }

    /// The identity of a recorded send as a crash point, with a readable rendering.
    fn send_id(&self, record: &SentRecord) -> SendId {
        SendId {
            channel: record.channel,
            to_member: record.to.1,
            bytes: record.bytes.clone(),
            display: format!(
                "{}:{}",
                render_endpoint(
                    &self.channel_label(record.channel),
                    record.from.1,
                    record.to.1
                ),
                render_payload(record)
            ),
        }
    }

    /// The dependency on a send: omittable if its channel is lossy and it happened before the
    /// end of finite failures, killed by its sender's crash if the sender is crashable. An
    /// observation is never omittable; its dependency is on the observed node having been
    /// alive to produce the fact, which the crash of that node before its `k`-th protocol
    /// send kills.
    fn send_dependency(&self, i: usize) -> Dependency {
        let record = &self.sent[i];
        if record.observation {
            let node = record
                .sender
                .as_ref()
                .map(|(label, _)| label.clone())
                .unwrap_or_else(|| format!("{:?}", record.from));
            let through = record.sender.as_ref().map_or(0, |(_, k)| *k);
            return Dependency {
                message: Message {
                    channel: "observed".to_owned(),
                    channel_id: record.channel,
                    from_member: record.from.1,
                    to_member: None,
                    payload: record.bytes.clone(),
                    display: format!(
                        "{} from {node} through send #{through}",
                        render_payload(record)
                    ),
                },
                sender: self.crash_ordinal(record),
                lossy: false,
            };
        }
        Dependency {
            message: self.message(record),
            sender: self.crash_ordinal(record),
            lossy: record.pre_eff && self.channel(record).is_some_and(|c| c.lossy),
        }
    }

    /// The sender and ordinal a dependency on `record` is killed through: the sender's
    /// `ordinal`-th send, exactly. The ordinal is **not** clamped to the sender's count of
    /// pre-`EFF` sends: a crash point is named by a message and fires right after that message
    /// when it is sent before `EFF` (`FaultState::filter`), killing every later send, before
    /// or after `EFF`; so a clause has to know the real order of the sender's sends to say
    /// which of them a given crash point kills. (An earlier version clamped here, which
    /// declared every pre-`EFF` crash harmless to the sender's post-`EFF` sends; the hypothesis
    /// that broke the goal was never a hitting set, and the search certified a buggy
    /// protocol.) Which crash points fire in a run is decided from the run's pre-`EFF` send
    /// counts, in [`Dependency::killed_by`] and [`Dependency::candidate_faults`].
    fn crash_ordinal(&self, record: &SentRecord) -> Option<(String, u64)> {
        record.sender.clone()
    }

    /// Every crashable node's performed sends, in order.
    fn send_sequences(&self) -> (SendSequences, PreEffSends) {
        let mut sequences: SendSequences = BTreeMap::new();
        let mut pre_eff: PreEffSends = BTreeMap::new();
        for record in &self.sent {
            if record.fate == SendFate::Crashed || record.observation {
                continue;
            }
            if let Some((node, _)) = &record.sender {
                sequences
                    .entry(node.clone())
                    .or_default()
                    .push(self.send_id(record));
                if record.pre_eff {
                    *pre_eff.entry(node.clone()).or_default() += 1;
                }
            }
        }
        (sequences, pre_eff)
    }

    // ---- Provenance-graph recording (see `crate::sim::viz::provenance`) ----
    //
    // Every derivation function below takes or computes an *anchor*: the graph node of the
    // thing being derived (an output or a message), under which the step's edges hang. The
    // anchors are `None` when no graph is being recorded, and every helper is a no-op then.

    /// Whether a provenance graph is being recorded.
    fn recording(&self) -> bool {
        self.prov.is_some()
    }

    fn with_prov<R>(&self, f: impl FnOnce(&mut ProvRecorder) -> R) -> Option<R> {
        self.prov.as_ref().map(|prov| f(&mut prov.borrow_mut()))
    }

    /// Starts recording a fresh graph for a goal.
    fn prov_begin(&self, goal: &str) {
        self.with_prov(|prov| prov.begin(Some(goal)));
    }

    /// Takes the graph begun by [`Self::prov_begin`].
    fn prov_end(&self) -> Option<ProvGraph> {
        self.with_prov(ProvRecorder::end)
    }

    /// Files a finished graph beside its derivation.
    fn prov_keep(&self, graph: Option<ProvGraph>) {
        if let Some(graph) = graph {
            self.kept.borrow_mut().push(graph);
        }
    }

    /// The label of a node: its registered label when crashable, `observer` for the
    /// specification's observer, else its location.
    fn node_label(&self, node: &NodeKey) -> String {
        if let Some((label, _)) = self.search.nodes.iter().find(|(_, key)| **key == *node) {
            return label.clone();
        }
        if self.search.observer() == Some(&node.0) {
            return "observer".to_owned();
        }
        match node.1 {
            Some(member) => format!("{:?}@{member}", node.0),
            None => format!("{:?}", node.0),
        }
    }

    fn prov_edge(
        &self,
        from: Option<usize>,
        to: Option<usize>,
        kind: ProvEdgeKind,
        level: Option<ProvLevel>,
    ) {
        if let (Some(from), Some(to)) = (from, to) {
            self.with_prov(|prov| prov.edge(from, to, kind, level));
        }
    }

    /// The graph node of an execution.
    fn prov_exec_node(&self, exec: ExecId) -> Option<usize> {
        if !self.recording() {
            return None;
        }
        let (location, member, tick) = self.execs.get(&exec)?;
        let node = self.node_label(&(location.root().clone(), *member));
        let location = format!("{location:?}");
        let tick = *tick;
        self.with_prov(|prov| {
            prov.node(ProvKey::Exec(exec), || ProvNodeKind::Exec {
                node,
                exec,
                tick,
                location,
            })
        })
    }

    /// The graph node of a send (by index into `sent`).
    fn prov_message_node(&self, i: usize) -> Option<usize> {
        if !self.recording() {
            return None;
        }
        let record = &self.sent[i];
        let dependency = self.send_dependency(i);
        let kind = ProvNodeKind::Message {
            message: ProvMessage::from(&dependency),
            from: self.node_label(&record.from),
            to: self.node_label(&record.to),
            fate: match record.fate {
                SendFate::Delivered => ProvFate::Delivered,
                SendFate::Omitted => ProvFate::Omitted,
                SendFate::Crashed => ProvFate::Crashed,
            },
            observation: record.observation,
            fault_point: dependency.lossy || dependency.sender.is_some(),
        };
        self.with_prov(|prov| prov.node(ProvKey::Message(i), || kind))
    }

    /// The graph node of an output, checked by the current goal (if one is begun).
    fn prov_output_node(&self, output: &OutputRecord) -> Option<usize> {
        if !self.recording() {
            return None;
        }
        let node = self
            .exec_node(output.exec)
            .map(|node| self.node_label(&node))
            .unwrap_or_else(|| "?".to_owned());
        let key = ProvKey::Output {
            exec: output.exec,
            port: output.port,
            member: output.member,
            bytes: output.bytes.clone(),
        };
        let kind = ProvNodeKind::Output {
            node,
            exec: output.exec,
            port: output.port,
            display: render_bytes(&output.bytes),
        };
        self.with_prov(|prov| {
            let i = prov.node(key, || kind);
            if let Some(goal) = prov.goal() {
                prov.edge(goal, i, ProvEdgeKind::Checked, None);
            }
            i
        })
    }

    /// Records that the step at `anchor` fell back to a coarser lineage, returning the node
    /// the fallback's edges hang from.
    fn prov_opaque_node(&self, anchor: Option<usize>, reason: &str) -> Option<usize> {
        let anchor = anchor?;
        self.with_prov(|prov| {
            let under = prov
                .key(anchor)
                .unwrap_or_else(|| ProvKey::Base("?".to_owned()));
            let label = reason.to_owned();
            let i = prov.node(ProvKey::Opaque(Box::new(under), reason.to_owned()), || {
                ProvNodeKind::Opaque { reason: label }
            });
            prov.edge(anchor, i, ProvEdgeKind::Fallback, None);
            i
        })
    }

    /// Records that `anchor` was computed from nothing that came off a channel.
    fn prov_base_node(&self, anchor: Option<usize>, label: &str, level: ProvLevel) {
        let Some(anchor) = anchor else {
            return;
        };
        self.with_prov(|prov| {
            let kind = label.to_owned();
            let i = prov.node(ProvKey::Base(label.to_owned()), || ProvNodeKind::Base {
                label: kind,
            });
            prov.edge(anchor, i, ProvEdgeKind::Derived, Some(level));
        });
    }

    /// The derivation of one output, at the configured lineage level.
    fn derive(&self, output: &OutputRecord) -> Derivation {
        let anchor = self.prov_output_node(output);
        match self.level {
            Level::Execution => self.closure(output.exec, anchor),
            Level::Precise => {
                if self.by_replay(&output.prov) {
                    self.counterfactual_output(output, anchor)
                } else {
                    self.record_closure(&output.prov, output.exec, anchor)
                }
            }
        }
    }

    /// Whether, under [`Level::Precise`], a record with provenance `prov` is derived by
    /// counterfactual replay: it is opaque (it came out of a closure the rewrite cannot see
    /// into), or the program carries no provenance at all, in which case every record is.
    fn by_replay(&self, prov: &[u64]) -> bool {
        !self.provenance || is_opaque(prov)
    }

    /// The derivation of a send, at the configured level (memoized by index into `sent`):
    /// record-level where the record carries provenance, by replay where it does not under
    /// [`Level::Precise`], execution-level otherwise.
    fn send_derivation(&self, i: usize) -> Derivation {
        if let Some(derivation) = self.send_memo.borrow().get(&i) {
            if self.recording()
                && let Some(sub) = self.send_prov_memo.borrow().get(&i)
            {
                self.with_prov(|prov| prov.splice(sub));
            }
            return derivation.clone();
        }
        // Recorded into a sub-graph of its own, kept with the memoized derivation.
        self.with_prov(ProvRecorder::push);
        let anchor = self.prov_message_node(i);
        let record = &self.sent[i];
        let derivation = match self.level {
            Level::Precise if self.by_replay(&record.prov) => self.counterfactual_send(i, anchor),
            Level::Precise | Level::Execution => {
                self.record_closure(&record.prov, record.exec, anchor)
            }
        };
        if let Some(sub) = self.with_prov(ProvRecorder::pop) {
            self.send_prov_memo.borrow_mut().insert(i, sub);
        }
        self.send_memo.borrow_mut().insert(i, derivation.clone());
        derivation
    }

    /// The recorded (non-counterfactual) derivation of a send, as the fallback when replay
    /// cannot reproduce it: record-level where the record carries real provenance,
    /// execution-level otherwise (opaque records, or no provenance in the program).
    fn recorded_send_closure(&self, i: usize, anchor: Option<usize>) -> Derivation {
        let record = &self.sent[i];
        if self.by_replay(&record.prov) {
            self.closure(record.exec, anchor)
        } else {
            self.record_closure(&record.prov, record.exec, anchor)
        }
    }

    /// The node an execution ran at.
    fn exec_node(&self, exec: ExecId) -> Option<NodeKey> {
        self.execs
            .get(&exec)
            .map(|(location, member, _)| (location.root().clone(), *member))
    }

    /// Counterfactual replay for an output (a record without usable provenance under
    /// [`Level::Precise`]): the 1-minimal set of deliveries at the producing node without
    /// which it no longer produces the output, each followed to the send that produced it.
    fn counterfactual_output(&self, output: &OutputRecord, anchor: Option<usize>) -> Derivation {
        let fallback = |anchor: Option<usize>| {
            if self.by_replay(&output.prov) {
                self.closure(output.exec, anchor)
            } else {
                self.record_closure(&output.prov, output.exec, anchor)
            }
        };
        let Some(node) = self.exec_node(output.exec) else {
            return fallback(anchor);
        };
        let (port, member, bytes) = (output.port, output.member, output.bytes.clone());
        let produced = move |events: &[LineageEvent]| {
            events.iter().any(|event| {
                matches!(
                    event,
                    LineageEvent::Output { port: p, member: m, bytes: b, .. }
                        if *p == port && *m == member && *b == bytes
                )
            })
        };
        self.counterfactual(&node, output.exec, &produced, fallback, anchor)
    }

    /// The counterfactual derivation of a send (see [`Self::send_derivation`]).
    fn counterfactual_send(&self, i: usize, anchor: Option<usize>) -> Derivation {
        let record = &self.sent[i];
        match self.exec_node(record.exec) {
            None => self.recorded_send_closure(i, anchor),
            Some(node) => {
                let (channel, to, bytes) =
                    (record.channel, record.to.clone(), record.bytes.clone());
                let from = node.clone();
                let produced = move |events: &[LineageEvent]| {
                    events.iter().any(|event| {
                        matches!(
                            event,
                            LineageEvent::Sent { channel: c, from: f, from_member: fm, to: t, to_member: tm, bytes: b, .. }
                                if *c == channel && (f.clone(), *fm) == from && (t.clone(), *tm) == to && *b == bytes
                        )
                    })
                };
                self.counterfactual(
                    &node,
                    record.exec,
                    &produced,
                    |anchor| self.recorded_send_closure(i, anchor),
                    anchor,
                )
            }
        }
    }

    /// The tick executions at `node` (every tick location rooted there), ascending.
    fn tick_execs_at(&self, node: &NodeKey) -> Vec<ExecId> {
        #[expect(
            clippy::disallowed_methods,
            reason = "collected and sorted; iteration order is irrelevant"
        )]
        let mut execs: Vec<ExecId> = self
            .ticks_at
            .iter()
            .filter(|((tick, member), _)| tick.root() == &node.0 && *member == node.1)
            .flat_map(|(_, execs)| execs.iter().copied())
            .collect();
        execs.sort_unstable();
        execs
    }

    /// Whether removing this delivery from a replay could change what the solver sees: it
    /// came off a channel, so it is a fault candidate or leads to a send with lineage of its
    /// own. A release that matched no send is a local or external item.
    fn matters(&self, release: &ReleaseRecord) -> bool {
        release.send.is_some()
    }

    /// One counterfactual derivation step at `node`: greedy deletion over the deliveries
    /// into the node's ticks before `exec`, latest first; `produced` recognizes the target in
    /// a replay's events. Falls back to `fallback` when the node does not reproduce the
    /// target with every delivery kept, or when there is no replay oracle. `anchor` is the
    /// provenance-graph node of the target (see [`Self::prov_output_node`]); a fallback hangs
    /// from an opaque node under it.
    fn counterfactual(
        &self,
        node: &NodeKey,
        exec: ExecId,
        produced: &dyn Fn(&[LineageEvent]) -> bool,
        fallback: impl FnOnce(Option<usize>) -> Derivation,
        anchor: Option<usize>,
    ) -> Derivation {
        let Some(replayer) = self.replayer else {
            return fallback(self.prov_opaque_node(anchor, "no replay oracle"));
        };
        let tick_execs = self.tick_execs_at(node);
        // Every delivery into the node's ticks, as `(exec, index)`, in event order.
        let all: Vec<(ExecId, usize)> = tick_execs
            .iter()
            .flat_map(|e| (0..self.released.get(e).map_or(0, Vec::len)).map(move |i| (*e, i)))
            .collect();
        // Every delivery into the node is a candidate, including those after the target's
        // execution: a later delivery can substitute for an earlier one (an echo for the
        // original), and the minimal sufficient set must be found among all of them.
        let mut candidates: Vec<(ExecId, usize)> = all
            .iter()
            .copied()
            .filter(|(e, i)| self.matters(&self.released[e][*i]))
            .collect();
        if all.is_empty() {
            // A node with no scripted inputs (fed only through hook-free paths): nothing to
            // replay against.
            return fallback(self.prov_opaque_node(anchor, "no scripted inputs to replay"));
        }
        if candidates.is_empty() {
            self.prov_base_node(
                anchor,
                "local input only (no delivery to remove)",
                ProvLevel::Replay,
            );
            return Derivation::new();
        }
        let mut removed: BTreeSet<(ExecId, usize)> = BTreeSet::new();
        let debug = std::env::var("LDFI_REPLAY_DEBUG").is_ok();
        let candidate_count = candidates.len();
        let run = |removed: &BTreeSet<(ExecId, usize)>| -> bool {
            self.replays.set(self.replays.get() + 1);
            let scripts = self.replay_scripts(node, &tick_execs, removed);
            // A replay the program did not survive is inconclusive: the delivery whose
            // removal provoked it is kept (treated as needed), which can only weaken the
            // derivation, never drop a real dependency.
            let events = replayer.replay(&scripts).unwrap_or_default();
            let hit = !events.is_empty() && produced(&events);
            if debug {
                let sent = events
                    .iter()
                    .filter(|e| matches!(e, LineageEvent::Sent { .. }))
                    .count();
                let outputs = events
                    .iter()
                    .filter(|e| matches!(e, LineageEvent::Output { .. }))
                    .count();
                let execs = events
                    .iter()
                    .filter(|e| matches!(e, LineageEvent::Exec { .. }))
                    .count();
                eprintln!(
                    "  replay {:?} exec<= {exec} removed {} of {}: {execs} execs, {sent} sends, {outputs} outputs, produced={hit}; scripts {:?}",
                    node,
                    removed.len(),
                    candidate_count,
                    {
                        #[expect(
                            clippy::disallowed_methods,
                            reason = "collected into an ordered map; iteration order is irrelevant"
                        )]
                        let sizes: BTreeMap<_, _> = scripts
                            .iter()
                            .map(|(hook, s)| (hook, s.iter().map(Vec::len).collect::<Vec<_>>()))
                            .collect();
                        sizes
                    }
                );
            }
            hit
        };
        if !run(&removed) {
            self.replay_fallbacks.set(self.replay_fallbacks.get() + 1);
            if replayer.log {
                eprintln!(
                    "{}: replay of {:?} did not reproduce the target; falling back to recorded lineage",
                    replayer.tag, node
                );
            }
            return fallback(self.prov_opaque_node(anchor, "replay did not reproduce the target"));
        }
        // Latest first: the derivation keeps the earliest sufficient deliveries.
        candidates.reverse();
        for candidate in &candidates {
            removed.insert(*candidate);
            if !run(&removed) {
                removed.remove(candidate);
            }
        }
        let mut derivation = Derivation::new();
        for (e, i) in candidates.iter().rev() {
            if removed.contains(&(*e, *i)) {
                continue;
            }
            let release = &self.released[e][*i];
            if let Some(send) = release.send {
                let dependency = self.send_dependency(send);
                if dependency.lossy || dependency.sender.is_some() {
                    derivation.insert(dependency);
                }
                let kept = self.prov_message_node(send);
                self.prov_edge(
                    anchor,
                    kept,
                    ProvEdgeKind::Counterfactual,
                    Some(ProvLevel::Replay),
                );
                derivation.extend(self.send_derivation(send));
            }
        }
        derivation
    }

    /// The replay scripts for the hooks at `node`: each hook's recorded releases per
    /// execution of its tick minus `removed`. Hooks elsewhere get no script (their tick never
    /// runs).
    fn replay_scripts(
        &self,
        node: &NodeKey,
        tick_execs: &[ExecId],
        removed: &BTreeSet<(ExecId, usize)>,
    ) -> HashMap<(usize, Option<u32>), Vec<Vec<Vec<u8>>>> {
        #[expect(
            clippy::disallowed_methods,
            reason = "re-keyed into a HashMap; iteration order is irrelevant"
        )]
        self.hooks
            .values()
            .filter(|hook| hook.node == *node)
            .map(|hook| {
                let script = tick_execs
                    .iter()
                    .filter(|e| {
                        self.execs
                            .get(e)
                            .is_some_and(|(l, m, _)| (l, *m) == (&hook.tick.0, hook.tick.1))
                    })
                    .map(|e| {
                        self.released
                            .get(e)
                            .into_iter()
                            .flatten()
                            .enumerate()
                            .filter(|(i, release)| {
                                release.hook_id == hook.hook_id && !removed.contains(&(*e, *i))
                            })
                            .map(|(_, release)| release.payload.clone())
                            .collect()
                    })
                    .collect();
                ((hook.hook_id, hook.member), script)
            })
            .collect()
    }

    /// The record-level backward closure: delivery ids → the sends that produced them →
    /// their delivery ids. Without record-level provenance in the program this is the
    /// execution-level closure. With it, a record's provenance is exact: every network
    /// arrival carries the id of its send, and an empty set means the record was computed
    /// from nothing that came off a channel (a constant, a timer, an external input).
    fn record_closure(&self, prov: &[u64], exec: ExecId, anchor: Option<usize>) -> Derivation {
        if !self.provenance {
            return self.closure(exec, anchor);
        }
        let mut derivation = Derivation::new();
        let mut visited: BTreeSet<u64> = BTreeSet::new();
        if prov.is_empty() {
            self.prov_base_node(anchor, LOCAL_INPUT, ProvLevel::Record);
        }
        // `(delivery id, the graph node that depends on it)`.
        let mut stack: Vec<(u64, Option<usize>)> = prov.iter().map(|id| (*id, anchor)).collect();
        while let Some((id, parent)) = stack.pop() {
            let Some(&i) = self.by_id.get(&id) else {
                visited.insert(id);
                self.prov_base_node(
                    parent,
                    &format!("delivery #{id} (not a recorded send)"),
                    ProvLevel::Record,
                );
                continue;
            };
            let message = self.prov_message_node(i);
            self.prov_edge(
                parent,
                message,
                ProvEdgeKind::Derived,
                Some(ProvLevel::Record),
            );
            if !visited.insert(id) {
                continue;
            }
            let dependency = self.send_dependency(i);
            if dependency.lossy || dependency.sender.is_some() {
                derivation.insert(dependency);
            }
            let record = &self.sent[i];
            if is_opaque(&record.prov) {
                derivation.extend(self.send_derivation(i));
            } else {
                if record.prov.is_empty() {
                    self.prov_base_node(message, LOCAL_INPUT, ProvLevel::Record);
                }
                stack.extend(record.prov.iter().map(|id| (*id, message)));
            }
        }
        derivation
    }

    /// One derivation per output on `port`.
    fn any_output(&self, port: usize) -> Vec<Derivation> {
        let mut derivations: Vec<Derivation> = vec![];
        for output in &self.outputs {
            if output.port == port {
                self.prov_begin(&format!("some output on port {port}"));
                let derivation = self.derive(output);
                let graph = self.prov_end();
                if !derivations.contains(&derivation) {
                    derivations.push(derivation);
                    self.prov_keep(graph);
                }
            }
        }
        derivations
    }

    /// The derivations of "every member in `members` produced output on `port`": one per
    /// combination of one derivation per member, unioned. Empty if some member produced
    /// nothing.
    fn all_members(&self, port: usize, members: &[u32]) -> Vec<Derivation> {
        let mut combined: Vec<(Derivation, Option<ProvGraph>)> = vec![(Derivation::new(), None)];
        for member in members {
            let mut per_member: Vec<(Derivation, Option<ProvGraph>)> = vec![];
            for output in &self.outputs {
                if output.port == port && output.member == Some(*member) {
                    self.prov_begin(&format!("every correct member outputs on port {port}"));
                    let derivation = self.derive(output);
                    let graph = self.prov_end();
                    if !per_member.iter().any(|(d, _)| *d == derivation) {
                        per_member.push((derivation, graph));
                    }
                }
            }
            if per_member.is_empty() {
                return vec![];
            }
            combined = combined
                .iter()
                .flat_map(|(prefix, prefix_graph)| {
                    per_member.iter().map(move |(d, graph)| {
                        let merged = match (prefix_graph, graph) {
                            (Some(a), Some(b)) => {
                                let mut merged = a.clone();
                                merged.merge(b);
                                Some(merged)
                            }
                            (Some(a), None) | (None, Some(a)) => Some(a.clone()),
                            (None, None) => None,
                        };
                        (prefix.union(d).cloned().collect(), merged)
                    })
                })
                .collect();
        }
        combined
            .into_iter()
            .map(|(derivation, graph)| {
                self.prov_keep(graph);
                derivation
            })
            .collect()
    }

    /// The union of the lineages of the named witnesses, each resolved to the latest output
    /// with that port, member, and value. A witness with no matching output contributes
    /// nothing (its fact does not hold; the body should have reported a violation).
    fn witnessed(&self, witnesses: &[(usize, Option<u32>, Vec<u8>)]) -> Derivation {
        let mut derivation = Derivation::new();
        for (port, member, bytes) in witnesses {
            let latest = self
                .outputs
                .iter()
                .filter(|o| o.port == *port && o.member == *member && o.bytes == *bytes)
                .max_by_key(|o| o.exec);
            if let Some(output) = latest {
                derivation.extend(self.derive(output));
                derivation.extend(self.alive_dependency(output));
            }
        }
        derivation
    }

    /// A witnessed output also depends on its producer being alive to emit it (a crashed
    /// node's outputs are suppressed): a dependency "node `N` survives through its `k`-th
    /// send", `k` the sends `N` had performed by the output, which every earlier crash point
    /// of `N` kills and whose preserve point (the crash right after send `k`) is the weakest
    /// crash that keeps the output. Only for crashable producers.
    fn alive_dependency(&self, output: &OutputRecord) -> Option<Dependency> {
        let node = self.exec_node(output.exec)?;
        let label = self
            .search
            .nodes
            .iter()
            .find(|(_, key)| **key == node)
            .map(|(label, _)| label.clone())?;
        let sends = self
            .sent
            .iter()
            .filter(|s| {
                s.fate != SendFate::Crashed
                    && !s.observation
                    && s.exec <= output.exec
                    && s.sender.as_ref().is_some_and(|(l, _)| *l == label)
            })
            .count() as u64;
        let dependency = Dependency {
            message: Message {
                channel: "alive".to_owned(),
                channel_id: usize::MAX,
                from_member: None,
                to_member: None,
                payload: bincode::serialize(&(label.clone(), sends)).unwrap(),
                display: format!("{label} through send #{sends}"),
            },
            sender: Some((label.clone(), sends)),
            lossy: false,
        };
        if let Some(anchor) = self.prov_output_node(output) {
            let message = ProvMessage::from(&dependency);
            self.with_prov(|prov| {
                let i = prov.node(ProvKey::Alive(label.clone(), sends), || {
                    ProvNodeKind::Alive {
                        node: label,
                        through: sends,
                        message,
                    }
                });
                prov.edge(anchor, i, ProvEdgeKind::Alive, None);
            });
        }
        Some(dependency)
    }

    /// The union of the lineages of the named final-state witnesses: each must be the last
    /// output on its port and member (otherwise the fact does not hold and it contributes
    /// nothing), and is derived as a final-state fact (see [`Run::witness_final`]).
    fn witnessed_final(&self, witnesses: &[(usize, Option<u32>, Vec<u8>)]) -> Derivation {
        let mut derivation = Derivation::new();
        for (port, member, bytes) in witnesses {
            let last = self
                .outputs
                .iter()
                .filter(|o| o.port == *port && o.member == *member)
                .max_by_key(|o| o.exec);
            if let Some(output) = last
                && output.bytes == *bytes
            {
                derivation.extend(self.derive_final(output));
                derivation.extend(self.alive_dependency(output));
            }
        }
        derivation
    }

    /// The derivation of a specification view's final value, as the clause (for `POST`) or the
    /// requirement (for `PRE`).
    ///
    /// For the **generated** specification (`generated`), the derivation is the view's record
    /// provenance at the configured level: every delivery behind every `(member, fact)` pair
    /// the value was computed from. The view is a universal over domains the hypothesis
    /// fixes, so keeping every one of those deliveries keeps the view, which is all the
    /// hitting-set inference needs (see [`Grade::Generated`]).
    ///
    /// For a **hand-written** view, two derivations are taken together: a counterfactual one
    /// at the observer (replay the observer alone with observed facts removed one at a time,
    /// keeping a fact iff the value changes without it, which answers a view written through
    /// negation) and the recorded one (which names what a final value rests on where a
    /// 1-minimal sufficient set is too small). The union is sound where either is; it costs
    /// minimality, and the certificate says it is relative ([`Grade::HandWritten`]).
    fn spec_view(&self, port: usize, bytes: &[u8], generated: bool) -> Derivation {
        let Some(output) = self
            .outputs
            .iter()
            .filter(|o| o.port == port && o.member.is_none())
            .max_by_key(|o| o.exec)
        else {
            return Derivation::new();
        };
        if output.bytes != bytes {
            return Derivation::new();
        }
        let recorded = self.derive(output);
        if generated {
            return recorded;
        }
        let fallback = |_anchor: Option<usize>| recorded.clone();
        let Some(node) = self.exec_node(output.exec) else {
            return fallback(None);
        };
        let anchor = self.prov_output_node(output);
        let (member, bytes) = (output.member, output.bytes.clone());
        let produced = move |events: &[LineageEvent]| {
            events
                .iter()
                .rev()
                .find_map(|event| match event {
                    LineageEvent::Output {
                        port: p,
                        member: m,
                        bytes: b,
                        ..
                    } if *p == port && *m == member => Some(b == &bytes),
                    _ => None,
                })
                .unwrap_or(false)
        };
        let mut derivation = self.counterfactual(&node, output.exec, &produced, fallback, anchor);
        derivation.extend(recorded);
        derivation
    }

    /// The derivation of an output as a final-state fact: under replay, the deliveries
    /// without which the node's *last* output on the port differs; otherwise as
    /// [`Self::derive`].
    fn derive_final(&self, output: &OutputRecord) -> Derivation {
        let by_replay = match self.level {
            Level::Precise => self.by_replay(&output.prov),
            Level::Execution => false,
        };
        if !by_replay {
            return self.derive(output);
        }
        let anchor = self.prov_output_node(output);
        let fallback = |anchor: Option<usize>| self.closure(output.exec, anchor);
        let Some(node) = self.exec_node(output.exec) else {
            return fallback(anchor);
        };
        let (port, member, bytes) = (output.port, output.member, output.bytes.clone());
        let produced = move |events: &[LineageEvent]| {
            events
                .iter()
                .rev()
                .find_map(|event| match event {
                    LineageEvent::Output {
                        port: p,
                        member: m,
                        bytes: b,
                        ..
                    } if *p == port && *m == member => Some(b == &bytes),
                    _ => None,
                })
                .unwrap_or(false)
        };
        self.counterfactual(&node, output.exec, &produced, fallback, anchor)
    }

    /// Every pre-EFF delivered send, as one derivation.
    fn all_pre_eff_deliveries(&self) -> Derivation {
        let goal = self.with_prov(ProvRecorder::goal).flatten();
        (0..self.sent.len())
            .filter(|i| {
                let record = &self.sent[*i];
                record.fate == SendFate::Delivered && record.pre_eff && !record.observation
            })
            .map(|i| {
                self.prov_edge(goal, self.prov_message_node(i), ProvEdgeKind::Checked, None);
                self.send_dependency(i)
            })
            .collect()
    }

    /// Every delivered send some fault could have prevented (omission only before EFF),
    /// regardless of whether the goal depended on it.
    fn all_deliveries(&self) -> Derivation {
        (0..self.sent.len())
            .filter(|i| self.sent[*i].fate == SendFate::Delivered && !self.sent[*i].observation)
            .map(|i| self.send_dependency(i))
            .filter(|dependency| dependency.lossy || dependency.sender.is_some())
            .collect()
    }

    /// The execution-level backward closure of what execution `root` produced. Each visited
    /// execution contributes every channel delivery that reached it (a hook release, a
    /// top-level receive, an arrival at its node that an unscripted batch may have released)
    /// and, as a sound over-approximation of persisted state, the earlier executions at its
    /// node.
    fn closure(&self, root: ExecId, anchor: Option<usize>) -> Derivation {
        let mut derivation = Derivation::new();
        let mut visited = BTreeSet::new();
        let mut stack = vec![root];
        let level = Some(ProvLevel::Execution);
        self.prov_edge(
            anchor,
            self.prov_exec_node(root),
            ProvEdgeKind::Produced,
            level,
        );
        while let Some(exec) = stack.pop() {
            if !visited.insert(exec) {
                continue;
            }
            let Some((location, member, is_tick)) = self.execs.get(&exec) else {
                continue;
            };
            let node = (location.clone(), *member);
            let root_node = (location.root().clone(), *member);
            let exec_node = self.prov_exec_node(exec);

            // A send is a dependency (a fault could prevent its delivery), and leads to the
            // execution that performed it.
            let take_send = |i: usize, derivation: &mut Derivation, stack: &mut Vec<ExecId>| {
                let dependency = self.send_dependency(i);
                if dependency.lossy || dependency.sender.is_some() {
                    derivation.insert(dependency);
                }
                let record = &self.sent[i];
                let message = self.prov_message_node(i);
                self.prov_edge(exec_node, message, ProvEdgeKind::Consumed, level);
                self.prov_edge(
                    message,
                    self.prov_exec_node(record.exec),
                    ProvEdgeKind::Produced,
                    level,
                );
                stack.push(record.exec);
            };

            if *is_tick {
                // A hook release that came off a channel is a dependency on its send; one
                // that did not is the output of a local tick, batched.
                for release in self.released.get(&exec).into_iter().flatten() {
                    match release.send {
                        Some(i) => take_send(i, &mut derivation, &mut stack),
                        None => {
                            for tick in self
                                .slices
                                .hook_yields
                                .get(&release.hook_id)
                                .into_iter()
                                .flatten()
                            {
                                self.push_ticks_before(tick, *member, exec, None, &mut stack);
                            }
                        }
                    }
                }
                // Every arrival at the node so far (an unscripted batch may have released
                // it), and persisted state: earlier executions of the same tick and of every
                // other tick at the node (their yields may have been batched).
                for (recv_exec, i) in self.recvs_at.get(&root_node).into_iter().flatten() {
                    if *recv_exec < exec {
                        take_send(*i, &mut derivation, &mut stack);
                    }
                }
                #[expect(
                    clippy::disallowed_methods,
                    reason = "pushes onto a stack whose order does not affect the closure"
                )]
                for (tick_location, tick_member) in self.ticks_at.keys() {
                    if tick_location.root() == location.root() && tick_member == member {
                        self.push_ticks_before(tick_location, *member, exec, None, &mut stack);
                    }
                }
            } else {
                // The previous async run at this node: it consumed everything before it.
                let previous = self
                    .asyncs_at
                    .get(&node)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|a| *a < exec)
                    .max();
                for recv in self.recvs.get(&exec).into_iter().flatten() {
                    if let Some(i) = recv.producer {
                        take_send(i, &mut derivation, &mut stack);
                    }
                }
                // An async run consumes what the location's ticks yielded since its previous
                // run.
                #[expect(
                    clippy::disallowed_methods,
                    reason = "pushes onto a stack whose order does not affect the closure"
                )]
                for (tick_location, tick_member) in self.ticks_at.keys() {
                    if tick_location.root() == location && tick_member == member {
                        self.push_ticks_before(tick_location, *member, exec, previous, &mut stack);
                    }
                }
            }
        }
        derivation
    }

    /// Pushes the executions of tick `tick` at `member` before `exec` (and after `since`, if
    /// given). A yield can take more than one async run to reach a sink (an `atomic` boundary
    /// adds a hop), so an empty window widens to the latest execution before `exec`: an
    /// over-approximation, like the rule itself. Records a `Carried` edge from `exec` to each
    /// pushed execution.
    fn push_ticks_before(
        &self,
        tick: &LocationId,
        member: Option<u32>,
        exec: ExecId,
        since: Option<ExecId>,
        stack: &mut Vec<ExecId>,
    ) {
        let level = Some(ProvLevel::Execution);
        let mut pushed = false;
        let mut latest: Option<ExecId> = None;
        let from = self.prov_exec_node(exec);
        for t in self
            .ticks_at
            .get(&(tick.clone(), member))
            .into_iter()
            .flatten()
        {
            if *t < exec {
                latest = Some(*t);
                if since.is_none_or(|s| *t > s) {
                    stack.push(*t);
                    self.prov_edge(from, self.prov_exec_node(*t), ProvEdgeKind::Carried, level);
                    pushed = true;
                }
            }
        }
        if !pushed && let Some(t) = latest {
            stack.push(t);
            self.prov_edge(from, self.prov_exec_node(t), ProvEdgeKind::Carried, level);
        }
    }
}

/// The label of the base node a record computed from nothing that came off a channel hangs
/// from.
const LOCAL_INPUT: &str = "local input\n(constant, timer, or external input)";

/// Whether a record's provenance is opaque (it came out of a closure the rewrite cannot see
/// into; see `Prov::OPAQUE`).
fn is_opaque(prov: &[u64]) -> bool {
    prov.contains(&crate::sim::runtime::Prov::OPAQUE)
}

/// The payload rendering of a send for reports: its `Debug` form when the type has one, else
/// a hex prefix of the wire bytes.
fn render_payload(record: &SentRecord) -> String {
    record
        .display
        .clone()
        .unwrap_or_else(|| render_bytes(&record.bytes))
}

/// Whether a payload released at a receiving hook is the serialized message `sent`. A
/// cluster-sourced channel delivers `(MemberId, T)`, so the received bytes carry the
/// serialized sender member id as a prefix. A sequenced observation
/// ([`Spec::observe_sequenced`]) batches `(n, T)` with `n` the arrival position
/// (`enumerate` before the observer's hook), so the received bytes carry a serialized
/// `usize` as a prefix; left unmatched, the observation would be a lineage gap at the
/// observer, and counterfactual replay would hold the views fixed and never follow them to
/// the sends they rest on.
fn payload_matches(received: &[u8], sent: &[u8], from_member: Option<u32>) -> bool {
    received == sent
        || from_member.is_some_and(|m| {
            let prefix = bincode::serialize(&TaglessMemberId::from_raw_id(m)).unwrap();
            received.len() == sent.len() + prefix.len()
                && received[..prefix.len()] == prefix[..]
                && received[prefix.len()..] == *sent
        })
        || {
            let n = bincode::serialized_size(&0usize).unwrap() as usize;
            received.len() == sent.len() + n && received[n..] == *sent
        }
}

/// A human-readable summary of the program's channels.
fn describe_channels(slices: &Slices) -> String {
    slices
        .channels
        .values()
        .map(|channel| {
            format!(
                "  channel#{} {:?}: {:?} → {:?}, {}",
                channel.id,
                channel.label,
                channel.from,
                channel.to,
                if channel.lossy { "lossy" } else { "reliable" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn member_label(cluster: &str, member: u32) -> String {
    format!("{cluster}@{member}")
}

/// The counterfactual replay oracle (see [`Level::Precise`]): runs one node alone, every
/// scripted batch hook under a replay policy, in a quiet deterministic instance, and returns
/// what the instance recorded.
struct Replayer<'a> {
    sim: &'a CompiledSim,
    log: bool,
    tag: &'static str,
}

impl Replayer<'_> {
    /// Replays with `scripts[hook]` (serialized items per tick execution) installed on each
    /// hook; a hook with no script gets an empty one, which keeps its tick from ever running.
    /// `None` when the program's own code panicked under the counterfactual inputs (a
    /// diverged state may reach conditions no real execution does): the replay is
    /// inconclusive and the caller falls back to the recorded lineage.
    fn replay(
        &self,
        scripts: &HashMap<(usize, Option<u32>), Vec<Vec<Vec<u8>>>>,
    ) -> Option<Vec<LineageEvent>> {
        let mut events: Vec<LineageEvent> = vec![];
        let out = &mut events;
        match self.sim.replay(async || {
            replay_all_hooks(scripts);
            crate::sim::compiled::quiesce().await;
            *out = lineage_events();
        }) {
            Ok(()) => Some(events),
            Err(message) => {
                if self.log {
                    eprintln!(
                        "{}: replay inconclusive, the program panicked under the counterfactual inputs: {message}",
                        self.tag
                    );
                }
                None
            }
        }
    }
}

/// The crashable nodes and the goal.
struct Search {
    /// Node label → node.
    nodes: BTreeMap<String, NodeKey>,
    goal: Option<Goal>,
}

impl Search {
    /// The specification's observer location, if the goal is a [`Spec`]: sends to it are
    /// observations, not messages of the protocol.
    fn observer(&self) -> Option<&LocationId> {
        match &self.goal {
            Some(Goal::Spec(spec)) => Some(&spec.observer),
            _ => None,
        }
    }
}

/// Which lineage oracle the backward closure follows.
///
/// Both levels run the same search on the same program; only the derivation of a goal
/// differs. `Precise` is the default and the recommended level for any program; `Execution`
/// is the coarse, replay-free closure, for a program whose records are all opaque and whose
/// nodes are expensive to replay. An earlier version had two more levels, a static-slice
/// narrowing of the execution closure and counterfactual replay as a level of its own;
/// measured on 2PC, ISR, reliable broadcast, Raft and Paxos
/// (`docs/design/ldfi-level-ablation.md`), the slice never separated a record from the inputs
/// of its execution, and standalone replay produced the derivations of record provenance
/// wherever record provenance applied, at two to ten times the cost. Replay survives as
/// `Precise`'s oracle wherever the program carries no provenance for a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// Every output of an execution depends on every input of it and of every earlier
    /// execution at its node (persisted state): the execution-granular lineage log read
    /// directly. Sound for any program, including one compiled without provenance.
    Execution,
    /// Record provenance where the program carries it, counterfactual replay where it does
    /// not; falls back to execution lineage only when replay is inconclusive.
    ///
    /// With `SimFlow::with_provenance`, each element carries the delivery ids it was computed
    /// from, and the derivation follows them. Where an element came out of a closure the
    /// rewrite cannot see into (`Prov::OPAQUE`), or the program was compiled without
    /// provenance at all (every record is then opaque), the step is derived by
    /// **counterfactual replay** of the node alone: its recorded deliveries are removed one at
    /// a time, and a delivery is in the derivation iff the node stops producing the record
    /// without it (a 1-minimal sufficient set, latest deliveries removed first). Where replay
    /// does not reproduce the record with every delivery kept (counted in
    /// [`Schedule::replay_fallbacks`]), there is no replay oracle, or the node has no scripted
    /// inputs to replay against, the step falls back to the execution closure.
    Precise,
}

/// An LDFI search configuration: the crashable nodes, the goal, and the budgets. The
/// program's channels are fault points by construction (see the module docs).
pub struct Ldfi {
    search: Search,
    strategy: Strategy,
    exploration: Exploration,
    level: Level,
    crash_budget: usize,
    max_hypotheses: usize,
    log: bool,
    /// Whether every schedule keeps its [`Trace`] (failing schedules always do).
    traces: bool,
    /// Record a why-provenance graph beside each derivation (`Schedule::provenance`).
    prov_graphs: bool,
}

impl Default for Ldfi {
    fn default() -> Self {
        Self::new()
    }
}

impl Ldfi {
    /// A search with no crashable nodes, a crash budget of 0, and a budget of 256
    /// hypotheses. Omission is available on every lossy channel of the program.
    pub fn new() -> Self {
        Ldfi {
            search: Search {
                nodes: BTreeMap::new(),
                goal: None,
            },
            strategy: Strategy::Lineage,
            exploration: Exploration::Exhaustive,
            level: Level::Precise,
            crash_budget: 0,
            max_hypotheses: 256,
            log: true,
            traces: false,
            prov_graphs: false,
        }
    }

    /// Keeps the event [`Trace`] of every explored schedule (see [`Schedule::trace`]), not
    /// only of failing ones, so a counterexample can be shown next to a run in which the goal
    /// held with its support highlighted (see [`crate::sim::viz`]). Costs memory in
    /// proportion to the schedules explored.
    #[must_use]
    pub fn with_traces(mut self) -> Self {
        self.traces = true;
        self
    }

    /// Records the why-provenance graph behind every derivation ([`Schedule::provenance`],
    /// see [`crate::sim::viz::provenance`]): the DAG the backward closure walked from the
    /// goal down to the deliveries, for Molly-style lineage diagrams. Off by default; the
    /// derivations are the same either way.
    #[must_use]
    pub fn with_provenance_graphs(mut self) -> Self {
        self.prov_graphs = true;
        self
    }

    /// Selects how hypotheses are chosen (default [`Strategy::Lineage`]).
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Selects which lineage the backward closure follows (default [`Level::Precise`]).
    #[must_use]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Reads the lineage level from `LDFI_LEVEL` (`exec` or `precise`; `record` is accepted
    /// as the former name of `precise`) if set; for experiments that compare levels without
    /// recompiling the test.
    #[must_use]
    pub fn level_from_env(self) -> Self {
        match std::env::var("LDFI_LEVEL").as_deref() {
            Ok("exec" | "execution") => self.level(Level::Execution),
            Ok("precise" | "record") => self.level(Level::Precise),
            _ => self,
        }
    }

    /// Selects how each hypothesis's schedules are explored (default
    /// [`Exploration::Exhaustive`]).
    #[must_use]
    pub fn exploration(mut self, exploration: Exploration) -> Self {
        self.exploration = exploration;
        self
    }

    /// Registers a process as crashable under `label`.
    #[must_use]
    pub fn crashable<P>(mut self, label: &str, process: &Process<'_, P>) -> Self {
        self.add_node(label.to_owned(), (Location::id(process), None));
        self
    }

    /// Registers the given members of a cluster as crashable, under `label@member`.
    #[must_use]
    pub fn crashable_cluster<C>(
        mut self,
        label: &str,
        cluster: &Cluster<'_, C>,
        members: impl IntoIterator<Item = u32>,
    ) -> Self {
        for member in members {
            self.add_node(
                member_label(label, member),
                (Location::id(cluster), Some(member)),
            );
        }
        self
    }

    fn add_node(&mut self, label: String, key: NodeKey) {
        assert!(
            !self.search.nodes.contains_key(&label),
            "duplicate LDFI node label {label:?}"
        );
        self.search.nodes.insert(label, key);
    }

    /// The maximum number of crash faults in one hypothesis.
    #[must_use]
    pub fn crash_budget(mut self, crashes: usize) -> Self {
        self.crash_budget = crashes;
        self
    }

    /// Names the external output whose values witness the goal: every value the program
    /// produces on it is one derivation of the goal.
    #[must_use]
    pub fn goal<T, O: Ordering, R: Retries>(mut self, output: &SimReceiver<T, O, R>) -> Self {
        self.search.goal = Some(Goal::Any { port: output.0 });
        self
    }

    /// Names a cluster output whose values witness a goal that is a conjunction over the
    /// *correct* members of the cluster registered as `cluster` (see
    /// [`Self::crashable_cluster`]): the goal holds when every member of `members` that did
    /// not crash produced output. A derivation of the conjunction is one derivation per
    /// correct member, unioned.
    #[must_use]
    pub fn goal_all_members<T, O: Ordering, R: Retries>(
        mut self,
        output: &SimClusterReceiver<T, O, R>,
        cluster: &str,
        members: impl IntoIterator<Item = u32>,
    ) -> Self {
        self.search.goal = Some(Goal::AllMembers {
            port: output.0,
            cluster: cluster.to_owned(),
            members: members.into_iter().collect(),
        });
        self
    }

    /// The goal's witnesses are named by the body at the end of each run ([`Run::witness`],
    /// [`Run::witness_member`]), for post-conditions that are not "some output exists": e.g.
    /// "the final leader's log holds every acknowledged write", whose lineage must include
    /// how the final leader came to be the leader.
    #[must_use]
    pub fn goal_witnessed(mut self) -> Self {
        self.search.goal = Some(Goal::Witnessed);
        self
    }

    /// The goal is a [`Spec`]: `PRE` and `POST` views over the program's outputs, written in
    /// Hydro. A run violates the goal when `PRE` holds and `POST` does not at its end; the
    /// derivation of `POST` is the clause, the derivation of `PRE` the requirement, and a run
    /// in which `PRE` does not hold is vacuous. Run such a search with [`Self::search`].
    #[must_use]
    pub fn spec(mut self, spec: &Spec<'_>) -> Self {
        self.search.goal = Some(Goal::Spec(spec.parts()));
        self
    }

    /// Caps the number of hypotheses (including the failure-free run).
    #[must_use]
    pub fn max_hypotheses(mut self, n: usize) -> Self {
        self.max_hypotheses = n;
        self
    }

    /// Silences the per-hypothesis progress lines on stderr.
    #[must_use]
    pub fn quiet(mut self) -> Self {
        self.log = false;
        self
    }

    /// An automatic specification ([`Spec::all_labels`]) observes nothing until the search
    /// names the collection; only [`Self::search_all_labels`] does.
    fn assert_label_chosen(&self) {
        if let Some(Goal::Spec(spec)) = &self.search.goal
            && let Some(all_labels) = &spec.all_labels
        {
            assert!(
                all_labels.current.get().is_some(),
                "a `Spec::all_labels` specification is driven by `Ldfi::search_all_labels`"
            );
        }
    }

    /// Runs the search against a [`Spec`] (see [`Self::spec`]). `body` drives one run: it
    /// sends inputs, fires timers and awaits `quiesce()`; the verdict comes from the
    /// specification's views at the end of the run, not from the body.
    pub fn search(
        self,
        sim: &CompiledSim,
        body: impl AsyncFn(&Run<'_>) + RefUnwindSafe,
    ) -> Outcome {
        assert!(
            matches!(self.search.goal, Some(Goal::Spec(_))),
            "`Ldfi::search` needs a specification (`Ldfi::spec`); use `Ldfi::run` with a goal and a body that returns the verdict"
        );
        self.run(sim, async |run| {
            body(run).await;
            true
        })
    }

    /// Runs one search per collection the program labels `EventualConsistency`, against an
    /// automatic specification ([`Spec::all_labels`]) and the shared `body`, and returns a
    /// verdict for each: the [`Outcome`] of its search, or why it was not checked. Every
    /// setting of this `Ldfi` (crashable nodes, budget, exploration, level) applies to every
    /// collection. Unless [`Self::quiet`], prints one summary line per collection as it
    /// finishes, and the full outcome of a counterexample.
    pub fn search_all_labels(
        &self,
        sim: &CompiledSim,
        body: impl AsyncFn(&Run<'_>) + RefUnwindSafe,
    ) -> LabelResults {
        let Some(Goal::Spec(spec)) = &self.search.goal else {
            panic!("`Ldfi::search_all_labels` needs a specification (`Ldfi::spec`)");
        };
        let (Some(all_labels), Some(facts)) = (&spec.all_labels, &spec.facts) else {
            panic!("`Ldfi::search_all_labels` needs a specification built with `Spec::all_labels`");
        };
        let labels = sim.labels().unwrap_or_else(|| {
            panic!(
                "`Ldfi::search_all_labels`: the compiled program carries no observation slot; was the `Spec::all_labels` created on the flow that was compiled?"
            )
        });
        let mut results = LabelResults { verdicts: vec![] };
        for (index, plan) in labels.iter().enumerate() {
            let verdict = if let Some(reason) = &plan.not_checked {
                LabelVerdict::NotChecked(reason.clone())
            } else {
                let members = sim
                        .cluster_members(&plan.collection.location)
                        .unwrap_or_else(|| {
                            panic!(
                                "no cluster size for {:?}; call `with_cluster_size` before `compiled()`",
                                plan.collection.location
                            )
                        })
                        .to_vec();
                all_labels.current.set(Some(index));
                all_labels
                    .correct
                    .set_target(plan.collection.location.clone(), members);
                facts.domain.borrow_mut().clear();
                if self.log {
                    eprintln!(
                        "hydrofrac: searching EventualConsistency at {} ({})",
                        plan.collection.label,
                        plan.collection.location_label()
                    );
                }
                let outcome = self.run(sim, async |run| {
                    body(run).await;
                    true
                });
                if self.log && outcome.is_counterexample() {
                    eprintln!("{outcome}");
                }
                LabelVerdict::Searched(outcome)
            };
            let result = LabelResult {
                collection: plan.collection.clone(),
                verdict,
            };
            if self.log {
                eprintln!("{result}");
            }
            results.verdicts.push(result);
        }
        results
    }

    /// Runs the search. `body` is the test body for one run: it drives inputs and time and
    /// returns whether the goal held (ignored under a [`Spec`], whose views decide). It is
    /// run under the configured [`Exploration`] for every hypothesis, starting with no
    /// faults.
    pub fn run(
        &self,
        sim: &CompiledSim,
        body: impl AsyncFn(&Run<'_>) -> bool + RefUnwindSafe,
    ) -> Outcome {
        let search = &self.search;
        self.assert_label_chosen();
        let goal_port = match &search.goal {
            Some(Goal::Any { port } | Goal::AllMembers { port, .. }) => Some(*port),
            Some(Goal::Witnessed | Goal::Spec(_)) | None => None,
        };
        let mut runs: Vec<Hypothesis> = vec![];
        let mut clauses: Vec<Clause> = vec![];
        // Every candidate fault of every observed delivery: the blind search's universe.
        let mut observed: BTreeSet<Fault> = BTreeSet::new();
        // The latest position each crash point's message was seen at, per node: the
        // weakest-first tie-break for same-size hypotheses.
        let mut positions: BTreeMap<Fault, u64> = BTreeMap::new();
        // Crash points right after a send in a requirement's lineage: the weakest crashes
        // that preserve the pre-condition.
        let mut preserve_points: BTreeSet<Fault> = BTreeSet::new();
        // Every observed derivation of the pre-condition, as a clause: a hypothesis should
        // leave at least one of them intact, or the run it proposes says nothing.
        let mut required: Vec<Clause> = vec![];
        // Hypotheses whose every schedule was vacuous: their supersets are not proposed
        // (a heuristic; vacuity is not strictly monotone in the fault set).
        let mut vacuous_sets: Vec<FaultSet> = vec![];
        let mut tried: BTreeSet<FaultSet> = BTreeSet::new();
        let mut faults = FaultSet::new();
        let level = self.level;
        let tag = match (self.strategy, level) {
            (Strategy::Blind, _) => "blind",
            (Strategy::Lineage, Level::Execution) => "ldfi/exec",
            (Strategy::Lineage, Level::Precise) => "ldfi",
        };
        if self.log
            && self.strategy == Strategy::Lineage
            && level == Level::Precise
            && !sim.provenance
        {
            eprintln!(
                "{tag}: the program was compiled without record-level provenance (`SimFlow::with_provenance`); replaying every record (execution closure where replay is inconclusive)"
            );
        }
        let mut slices: Option<Rc<Slices>> = None;

        loop {
            if runs.len() >= self.max_hypotheses {
                return Outcome::Budget { runs };
            }

            let mut records: Vec<RunRecord> = vec![];
            {
                let faults_ref = &faults;
                let records_ref = &mut records;
                let slices_ref = &mut slices;
                let body_ref = &body;
                let instance = async || {
                    let run = Run {
                        faults: faults_ref,
                        search,
                        goal_port: goal_port.map(lineage_port),
                        eff_cut: RefCell::new(None),
                        witnesses: RefCell::new(vec![]),
                        final_witnesses: RefCell::new(vec![]),
                        requirements: RefCell::new(vec![]),
                        vacuous: Cell::new(false),
                        level,
                        hooks: RefCell::new(vec![]),
                    };
                    run.install();
                    let goal_held = body_ref(&run).await;
                    records_ref.push(run.finish(goal_held));
                    *slices_ref = Some(lineage_slices());
                };
                match self.exploration {
                    Exploration::Exhaustive => {
                        sim.exhaustive(instance);
                    }
                    Exploration::Fuzz { instances } => sim.fuzz_bounded(instances, instance),
                }
            }
            // A generated specification quantifies over the facts of the failure-free run,
            // which the observer has to be told: the first failure-free run is a probe that
            // learns them, and the search then starts over with the domain fed (section 1.1
            // of the design document: Molly's "the good outcome persists").
            if let Some(Goal::Spec(spec)) = &search.goal
                && let Some(facts) = &spec.facts
                && faults.is_empty()
                && facts.domain.borrow().is_empty()
            {
                let learned = learn_fact_domain(&records);
                if learned.is_empty() {
                    // Nothing was held anywhere: the body protects nothing.
                    return Outcome::Vacuous { runs };
                }
                if self.log {
                    eprintln!(
                        "{tag}: fact domain learned from the failure-free run: {} fact(s)",
                        learned.len()
                    );
                }
                *facts.domain.borrow_mut() = learned;
                continue;
            }
            tried.insert(faults.clone());
            if !records.is_empty() && records.iter().all(|r| r.vacuous) {
                vacuous_sets.push(faults.clone());
            }
            // Derivations are computed outside the instance: counterfactual replays are
            // instances of their own.
            let replayer = Replayer {
                sim,
                log: self.log,
                tag,
            };
            let schedules: Vec<Schedule> = records
                .iter()
                .map(|record| {
                    record.schedule(search, Some(&replayer), self.traces, self.prov_graphs)
                })
                .collect();

            let failing = schedules.iter().find(|s| !s.goal_held).cloned();
            let new_derivations: BTreeSet<Derivation> = schedules
                .iter()
                .filter(|s| s.goal_held)
                .flat_map(|s| s.derivations.iter().cloned())
                .collect();
            if self.log
                && matches!(search.goal, Some(Goal::Spec(_)))
                && schedules.iter().any(|s| {
                    s.goal_held && !s.vacuous && s.derivations.iter().any(BTreeSet::is_empty)
                })
            {
                eprintln!(
                    "{tag}: warning: POST held with an empty derivation: no observed fact's removal changes its final value (is it independent of the protocol's outputs?); the search will certify immediately"
                );
            }
            // The crashes of this hypothesis: premises of every clause it yields.
            let premises: BTreeSet<Fault> = faults
                .iter()
                .filter(|f| matches!(f, Fault::Crash { .. }))
                .cloned()
                .collect();
            for schedule in &schedules {
                observed.extend(schedule.observed.iter().flat_map(|dep| {
                    dep.candidate_faults(&schedule.sends, &schedule.pre_eff_sends)
                }));
                if !schedule.requirements.is_empty()
                    && !required
                        .iter()
                        .any(|c| c.deps == schedule.requirements && c.premises == premises)
                {
                    required.push(Clause {
                        deps: schedule.requirements.clone(),
                        sends: schedule.sends.clone(),
                        pre_eff_sends: schedule.pre_eff_sends.clone(),
                        premises: premises.clone(),
                    });
                }
                for dep in &schedule.requirements {
                    // The weakest crash that preserves a requirement's send is the crash right
                    // after it. A requirement sent after `EFF` has no preserve point: every
                    // crash that fires in this run (a pre-`EFF` one) kills it.
                    if let Some((node, ordinal)) = &dep.sender
                        && let Some(send) = schedule.sends.get(node).and_then(|seq| {
                            let cap = schedule.pre_eff_sends.get(node).copied().unwrap_or(0);
                            seq[..cap.min(seq.len())].get(*ordinal as usize)
                        })
                    {
                        preserve_points.insert(Fault::Crash {
                            node: node.clone(),
                            after: Some(send.clone()),
                        });
                    }
                }
                for (node, sends) in schedule.sends.iter() {
                    observed.insert(Fault::Crash {
                        node: node.clone(),
                        after: None,
                    });
                    let pre_eff = schedule.pre_eff_sends.get(node).copied().unwrap_or(0);
                    for (at, send) in sends.iter().enumerate().take(pre_eff) {
                        let fault = Fault::Crash {
                            node: node.clone(),
                            after: Some(send.clone()),
                        };
                        let at = at as u64 + 1;
                        positions
                            .entry(fault)
                            .and_modify(|p| *p = (*p).max(at))
                            .or_insert(at);
                    }
                }
            }
            if self.log
                && runs.is_empty()
                && let Some(slices) = &slices
            {
                eprintln!("{tag}: channels:\n{}", describe_channels(slices));
                if let Some(Goal::Spec(spec)) = &search.goal {
                    eprintln!(
                        "{tag}: specification is {}",
                        if spec.facts.is_some() {
                            "generated (EventualConsistency)"
                        } else {
                            "hand-written (no soundness claim for the inference)"
                        }
                    );
                }
            }
            if self.log {
                let unmatched: usize = schedules.iter().map(|s| s.unmatched_releases).sum();
                let replays: usize = schedules.iter().map(|s| s.replays).sum();
                let fallbacks: usize = schedules.iter().map(|s| s.replay_fallbacks).sum();
                eprintln!(
                    "{tag}: run #{} {}: {} schedule(s), goal {}; derivations {}{}{}",
                    runs.len() + 1,
                    render_set(&faults),
                    schedules.len(),
                    if failing.is_some() {
                        "VIOLATED"
                    } else {
                        "held"
                    },
                    render_set(new_derivations.iter().map(render_set)),
                    if unmatched > 0 {
                        format!("; {unmatched} untraced deliveries")
                    } else {
                        String::new()
                    },
                    if replays > 0 {
                        format!("; {replays} replays, {fallbacks} fallbacks")
                    } else {
                        String::new()
                    },
                );
            }
            runs.push(Hypothesis {
                faults: faults.clone(),
                schedules,
            });

            if let Some(schedule) = failing {
                return Outcome::Counterexample {
                    faults,
                    schedule,
                    runs,
                };
            }
            // A failure-free run in which the pre-condition never held protects nothing: say
            // so rather than certify.
            if runs.len() == 1
                && faults.is_empty()
                && runs[0].schedules.iter().all(|s| s.vacuous)
                && !runs[0].schedules.is_empty()
            {
                return Outcome::Vacuous { runs };
            }

            for schedule in runs.last().into_iter().flat_map(|h| &h.schedules) {
                if !schedule.goal_held {
                    continue;
                }
                for derivation in &schedule.derivations {
                    if !clauses
                        .iter()
                        .any(|c| c.deps == *derivation && c.premises == premises)
                    {
                        clauses.push(Clause {
                            deps: derivation.clone(),
                            sends: schedule.sends.clone(),
                            pre_eff_sends: schedule.pre_eff_sends.clone(),
                            premises: premises.clone(),
                        });
                    }
                }
            }

            // Weakest first: omissions before crashes, and within a node a crash after a
            // later message before one after an earlier message (before-any-send last).
            let order = |fault: &Fault| -> (u8, String, std::cmp::Reverse<u64>, String) {
                match fault {
                    Fault::Omit(m) => (0, String::new(), std::cmp::Reverse(0), format!("{m:?}")),
                    Fault::Crash { node, after } => (
                        1,
                        node.clone(),
                        std::cmp::Reverse(match after {
                            Some(_) => positions.get(fault).copied().unwrap_or(0),
                            None => 0,
                        }),
                        format!("{fault}"),
                    ),
                }
            };
            let next = match self.strategy {
                Strategy::Lineage => {
                    // The weakest faults that kill some dependency of a clause (the omission
                    // of a lossy delivery; the crash right before a dependency's send), plus
                    // the weakest crashes that preserve a requirement (right after one of
                    // its sends). The latter kill the same dependencies as the crash before
                    // the next dependency send but keep the pre-condition, which the lineage
                    // of the goal alone does not see.
                    let mut universe: BTreeSet<Fault> =
                        clauses.iter().flat_map(Clause::candidate_faults).collect();
                    universe.extend(preserve_points.iter().cloned());
                    let mut universe: Vec<Fault> = universe.into_iter().collect();
                    universe.sort_by_key(order);
                    if std::env::var("LDFI_SOLVER_DEBUG").is_ok() {
                        for (i, clause) in clauses.iter().enumerate() {
                            eprintln!("{tag}: clause #{i}: {}", render_set(&clause.deps));
                            for (node, sends) in clause.sends.iter() {
                                eprintln!(
                                    "{tag}:   sends of {node}: {}",
                                    sends
                                        .iter()
                                        .enumerate()
                                        .map(|(k, s)| format!("#{k} {s}"))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                            }
                        }
                    }
                    if self.log {
                        eprintln!(
                            "{tag}: universe {}; preserve points {}; requirements {}",
                            render_set(&universe),
                            render_set(&preserve_points),
                            render_set(
                                runs.last()
                                    .into_iter()
                                    .flat_map(|h| &h.schedules)
                                    .flat_map(|s| s.requirements.iter())
                            ),
                        );
                    }
                    next_hypothesis(
                        &universe,
                        &clauses,
                        &required,
                        &tried,
                        &vacuous_sets,
                        self.crash_budget,
                    )
                }
                Strategy::Blind => {
                    let mut universe: Vec<Fault> = observed.iter().cloned().collect();
                    universe.sort_by_key(order);
                    next_hypothesis(
                        &universe,
                        &[],
                        &[],
                        &tried,
                        &vacuous_sets,
                        self.crash_budget,
                    )
                }
            };
            match next {
                Some(next) => faults = next,
                None => {
                    return match self.strategy {
                        Strategy::Lineage => Outcome::Certificate {
                            derivations: clauses.into_iter().map(|c| c.deps).collect(),
                            runs,
                            exploration: self.exploration,
                            grade: match &search.goal {
                                Some(Goal::Spec(spec)) if spec.facts.is_some() => Grade::Generated,
                                Some(Goal::Spec(_)) => Grade::HandWritten,
                                _ => Grade::Goal,
                            },
                        },
                        Strategy::Blind => Outcome::Exhausted { runs },
                    };
                }
            }
        }
    }
}

/// The verdict of [`Ldfi::search_all_labels`] on one labelled collection.
#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "an outcome carries every hypothesis; a few verdicts per program"
)]
pub enum LabelVerdict {
    /// The collection was searched: a certificate, a counterexample, an exhausted budget, or
    /// a vacuous failure-free run.
    Searched(Outcome),
    /// The automatic specification does not apply to the collection.
    NotChecked(NotCheckedReason),
}

/// One row of [`LabelResults`]: a labelled collection and its verdict.
#[derive(Clone, Debug)]
pub struct LabelResult {
    /// The collection: its source site, operator, cluster and role.
    pub collection: EcCollection,
    /// The verdict.
    pub verdict: LabelVerdict,
}

impl LabelResult {
    /// The source site of the labelling operator (or its tag).
    pub fn label(&self) -> &str {
        &self.collection.label
    }
}

impl Display for LabelResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = &self.collection;
        let role = match (c.minted, c.exit) {
            (true, true) => "minted and delivered",
            (true, false) => "minted",
            (false, true) => "delivered",
            (false, false) => "",
        };
        write!(
            f,
            "EventualConsistency at {} ({:?}, {}, {role}): ",
            c.label, c.location, c.op
        )?;
        match &self.verdict {
            LabelVerdict::NotChecked(reason) => write!(f, "not checked: {reason}"),
            LabelVerdict::Searched(outcome) => match outcome {
                Outcome::Certificate {
                    runs, exploration, ..
                } => {
                    write!(f, "CERTIFICATE, {} hypotheses", runs.len())?;
                    match exploration {
                        Exploration::Exhaustive => write!(f, ", exhaustive"),
                        Exploration::Fuzz { instances } => {
                            write!(f, ", {instances} sampled schedules")
                        }
                    }
                }
                Outcome::Counterexample { faults, runs, .. } => write!(
                    f,
                    "VIOLATED by {}, {} hypotheses",
                    render_set(faults),
                    runs.len()
                ),
                Outcome::Budget { runs } => {
                    write!(f, "BUDGET exhausted after {} hypotheses", runs.len())
                }
                Outcome::Vacuous { .. } => {
                    write!(f, "VACUOUS (nothing held in the failure-free run)")
                }
                Outcome::Exhausted { runs } => {
                    write!(
                        f,
                        "exhausted the fault universe, {} runs, no violation",
                        runs.len()
                    )
                }
            },
        }
    }
}

/// The result of [`Ldfi::search_all_labels`]: one verdict per collection the program labels
/// `EventualConsistency`, in the program's enumeration order. Displays as a table, one line
/// per collection.
#[derive(Clone, Debug, Default)]
pub struct LabelResults {
    /// The verdicts.
    pub verdicts: Vec<LabelResult>,
}

impl LabelResults {
    /// Whether every collection either certified or was not checked (no counterexample, no
    /// exhausted budget, no vacuous search).
    pub fn all_certified_or_not_checked(&self) -> bool {
        self.verdicts.iter().all(|r| {
            matches!(
                r.verdict,
                LabelVerdict::NotChecked(_) | LabelVerdict::Searched(Outcome::Certificate { .. })
            )
        })
    }

    /// The verdicts whose label contains `site` (a source-site substring).
    pub fn at(&self, site: &str) -> impl Iterator<Item = &LabelResult> {
        self.verdicts
            .iter()
            .filter(move |r| r.collection.label.contains(site))
    }

    /// The verdicts that were searched, with their outcomes.
    pub fn searched(&self) -> impl Iterator<Item = (&LabelResult, &Outcome)> {
        self.verdicts.iter().filter_map(|r| match &r.verdict {
            LabelVerdict::Searched(outcome) => Some((r, outcome)),
            LabelVerdict::NotChecked(_) => None,
        })
    }

    /// The counterexamples found, by label.
    pub fn counterexamples(&self) -> impl Iterator<Item = (&LabelResult, &FaultSet)> {
        self.verdicts.iter().filter_map(|r| match &r.verdict {
            LabelVerdict::Searched(Outcome::Counterexample { faults, .. }) => Some((r, faults)),
            _ => None,
        })
    }
}

impl Display for LabelResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for result in &self.verdicts {
            writeln!(f, "{result}")?;
        }
        Ok(())
    }
}

/// The result of [`Ldfi::fuzz_faults`].
#[derive(Clone, Debug)]
pub struct FuzzOutcome {
    /// Instances run before the first violation (all of them when none was found).
    pub runs: usize,
    /// The first violating fault set, if any.
    pub violation: Option<FaultSet>,
    /// Instances (among `runs`) the body marked vacuous: the pre-condition failed under the
    /// drawn faults, so the instance tested nothing.
    pub vacuous: usize,
}

/// How a random baseline draws each instance's fault set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuzzDraw {
    /// Every omittable message independently with probability 1/2; every crashable node,
    /// visited in random order, crashes with probability 1/2 (while the budget allows) at a
    /// uniformly chosen one of its crash points.
    Bernoulli,
    /// Up to `k` faults, each drawn uniformly from the vocabulary (omittable messages and
    /// crash points); draws that would exceed the crash budget or crash a node twice are
    /// skipped.
    UpTo(usize),
}

/// A random baseline's configuration.
#[derive(Clone, Debug)]
pub struct FuzzConfig<'a> {
    /// Messages the baseline may omit (observed on lossy channels).
    pub omittable: &'a [Message],
    /// Crash faults the baseline may inject (observed crash points; see
    /// [`Hypothesis::crash_points`]).
    pub crash_points: &'a [Fault],
    /// How each instance's fault set is drawn.
    pub draw: FuzzDraw,
}

impl Display for FuzzOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.violation {
            Some(faults) => writeln!(
                f,
                "fuzz: goal violated at instance {} by {}",
                self.runs,
                render_set(faults)
            ),
            None => writeln!(f, "fuzz: no violation in {} instance(s)", self.runs),
        }
    }
}

impl Ldfi {
    /// The random baseline: one [`CompiledSim::fuzz`] call whose instances each draw a fault
    /// set (every omittable message with probability 1/2; every crashable node, in random
    /// order, crashes with probability 1/2 at a uniform crash point while the budget allows)
    /// before running `body`. Under `cargo test` the draws are uniform random with the
    /// configured number of instances; under `cargo sim` they are libFuzzer-guided.
    /// `omittable` and `crash_points` are the vocabulary, usually taken from a failure-free
    /// LDFI run ([`Hypothesis::omittable`], [`Hypothesis::crash_points`]).
    pub fn fuzz_faults(
        self,
        sim: &CompiledSim,
        omittable: &[Message],
        crash_points: &[Fault],
        body: impl AsyncFn(&Run<'_>) -> bool + RefUnwindSafe,
    ) -> FuzzOutcome {
        self.fuzz_faults_with(
            sim,
            FuzzConfig {
                omittable,
                crash_points,
                draw: FuzzDraw::Bernoulli,
            },
            body,
        )
    }

    /// [`Self::fuzz_faults`] with an explicit draw.
    pub fn fuzz_faults_with(
        self,
        sim: &CompiledSim,
        config: FuzzConfig<'_>,
        body: impl AsyncFn(&Run<'_>) -> bool + RefUnwindSafe,
    ) -> FuzzOutcome {
        use bolero::generator::bolero_generator::any::Any;

        let search = &self.search;
        self.assert_label_chosen();
        let level = self.level;
        let goal_port = match &search.goal {
            Some(Goal::Any { port } | Goal::AllMembers { port, .. }) => Some(*port),
            Some(Goal::Witnessed | Goal::Spec(_)) | None => None,
        };
        let crash_budget = self.crash_budget;
        let FuzzConfig {
            omittable,
            crash_points,
            draw,
        } = config;
        // Crash points per node, so a node is crashed at most once.
        let mut by_node: BTreeMap<&String, Vec<&Fault>> = BTreeMap::new();
        for fault in crash_points {
            if let Fault::Crash { node, .. } = fault {
                by_node.entry(node).or_default().push(fault);
            }
        }
        let nodes: Vec<&String> = by_node.keys().copied().collect();
        // A generated specification needs its fact domain (see `Ldfi::run`): a failure-free
        // probe learns it when the lineage search has not already.
        if let Some(Goal::Spec(spec)) = &search.goal
            && let Some(facts) = &spec.facts
            && facts.domain.borrow().is_empty()
        {
            let mut records: Vec<RunRecord> = vec![];
            {
                let records_ref = &mut records;
                let body_ref = &body;
                let empty = FaultSet::new();
                let empty_ref = &empty;
                sim.fuzz_bounded(1, async || {
                    let run = Run {
                        faults: empty_ref,
                        search,
                        goal_port: goal_port.map(lineage_port),
                        eff_cut: RefCell::new(None),
                        witnesses: RefCell::new(vec![]),
                        final_witnesses: RefCell::new(vec![]),
                        requirements: RefCell::new(vec![]),
                        vacuous: Cell::new(false),
                        level,
                        hooks: RefCell::new(vec![]),
                    };
                    run.install();
                    let goal_held = body_ref(&run).await;
                    records_ref.push(run.finish(goal_held));
                });
            }
            *facts.domain.borrow_mut() = learn_fact_domain(&records);
        }
        let mut runs = 0usize;
        let mut vacuous = 0usize;
        let mut violation: Option<FaultSet> = None;
        {
            let runs_ref = &mut runs;
            let vacuous_ref = &mut vacuous;
            let violation_ref = &mut violation;
            let body_ref = &body;
            sim.fuzz(async || {
                let mut faults = FaultSet::new();
                let crashes_in = |faults: &FaultSet| {
                    faults
                        .iter()
                        .filter(|f| matches!(f, Fault::Crash { .. }))
                        .count()
                };
                let crashed = |faults: &FaultSet, node: &str| {
                    faults
                        .iter()
                        .any(|f| matches!(f, Fault::Crash { node: n, .. } if n == node))
                };
                match draw {
                    FuzzDraw::Bernoulli => {
                        for message in omittable {
                            if (0..2u8).any() == 1 {
                                faults.insert(Fault::Omit(message.clone()));
                            }
                        }
                        let mut order: Vec<&String> = nodes.clone();
                        for i in 0..order.len() {
                            let j = (i..order.len()).any();
                            order.swap(i, j);
                        }
                        for node in &order {
                            if crashes_in(&faults) < crash_budget && (0..2u8).any() == 1 {
                                let points = &by_node[*node];
                                let pick = (0..points.len()).any();
                                faults.insert(points[pick].clone());
                            }
                        }
                    }
                    FuzzDraw::UpTo(k) => {
                        let vocabulary = omittable.len() + crash_points.len();
                        for _ in 0..k {
                            if vocabulary == 0 {
                                break;
                            }
                            let pick = (0..vocabulary).any();
                            if pick < omittable.len() {
                                faults.insert(Fault::Omit(omittable[pick].clone()));
                            } else {
                                let fault = &crash_points[pick - omittable.len()];
                                let Fault::Crash { node, .. } = fault else {
                                    unreachable!("crash_points holds crash faults");
                                };
                                if crashes_in(&faults) < crash_budget && !crashed(&faults, node) {
                                    faults.insert(fault.clone());
                                }
                            }
                        }
                    }
                }
                if violation_ref.is_some() {
                    // Already found; the remaining instances are not counted.
                    return;
                }
                *runs_ref += 1;
                let run = Run {
                    faults: &faults,
                    search,
                    goal_port: goal_port.map(lineage_port),
                    eff_cut: RefCell::new(None),
                    witnesses: RefCell::new(vec![]),
                    final_witnesses: RefCell::new(vec![]),
                    requirements: RefCell::new(vec![]),
                    vacuous: Cell::new(false),
                    level,
                    hooks: RefCell::new(vec![]),
                };
                run.install();
                let goal_held = body_ref(&run).await;
                // Under a specification the views decide; `finish` reads them.
                let record = run.finish(goal_held);
                if record.vacuous {
                    *vacuous_ref += 1;
                }
                if !record.goal_held {
                    *violation_ref = Some(faults);
                }
            });
        }
        FuzzOutcome {
            runs,
            violation,
            vacuous,
        }
    }
}

/// The next hypothesis: a minimum-weight subset of `universe` (sorted) that kills some
/// dependency of every clause, holds at most `crash_budget` crashes and at most one crash per
/// node, and has not been tried. `None` when no such set exists: some clause is empty, or the
/// hitting-set problem is infeasible over the candidate faults, which is the certificate.
/// With no clauses this enumerates every admissible subset by weight (the blind search).
///
/// This is a minimal hitting set, solved as an integer program (`HiGHS` via `good_lp`):
///
/// - a binary `x_f` per candidate fault `f` (the omission of a lossy delivery, or the crash
///   of a crashable sender right after the send preceding one of its observed sends);
/// - minimize `Σ w_f x_f`, where `w_f` is `1` plus a tie-break that prefers weaker faults (a
///   crash after a later message), so a hypothesis leaves as much of the protocol running as
///   a minimum hitting set allows;
/// - cover: for every clause `D`, `Σ { x_f : f kills some d ∈ D } ≥ 1`;
/// - `Σ_{crash} x_f ≤ crash_budget`, and `Σ_{f = crash(n, ·)} x_f ≤ 1` per node (an earlier
///   crash of the same node dominates a later one);
/// - a no-good per tried set `T`: `Σ_{f∈T} x_f − Σ_{f∉T} x_f ≤ |T| − 1`, excluding exactly `T`.
fn next_hypothesis(
    universe: &[Fault],
    clauses: &[Clause],
    required: &[Clause],
    tried: &BTreeSet<FaultSet>,
    vacuous: &[FaultSet],
    crash_budget: usize,
) -> Option<FaultSet> {
    // Prefer a hypothesis that preserves some observed derivation of the pre-condition (a
    // run whose pre-condition fails contributes nothing); if none exists, drop the
    // preference rather than the search.
    if !required.is_empty()
        && let Some(next) =
            solve_hypothesis(universe, clauses, required, tried, vacuous, crash_budget)
    {
        return Some(next);
    }
    solve_hypothesis(universe, clauses, &[], tried, vacuous, crash_budget)
}

/// [`next_hypothesis`] with the pre-condition preference as a hard constraint when
/// `required` is non-empty: an indicator per required derivation that is 1 only if the
/// hypothesis kills none of its dependencies, at least one of which must be 1.
fn solve_hypothesis(
    universe: &[Fault],
    clauses: &[Clause],
    required: &[Clause],
    tried: &BTreeSet<FaultSet>,
    vacuous: &[FaultSet],
    crash_budget: usize,
) -> Option<FaultSet> {
    use good_lp::{Expression, ProblemVariables, ResolutionError, Solution, SolverModel, variable};

    if clauses.iter().any(|c| c.deps.is_empty()) || universe.is_empty() {
        return None;
    }
    // `universe` is sorted weakest first, so position doubles as the tie-break.
    let index_of = |fault: &Fault| universe.iter().position(|f| f == fault);

    let mut vars = ProblemVariables::new();
    let x: Vec<_> = universe
        .iter()
        .map(|_| vars.add(variable().binary()))
        .collect();
    // One indicator per required derivation: 1 only if the hypothesis kills none of it.
    let preserved: Vec<_> = required
        .iter()
        .map(|_| vars.add(variable().binary()))
        .collect();
    let tie_break = 0.5 / universe.len() as f64;
    let objective: Expression = x
        .iter()
        .enumerate()
        .map(|(i, &v)| (1.0 + tie_break * i as f64) * v)
        .sum();
    let mut model = vars.minimise(objective).using(good_lp::highs);

    // A clause is covered by killing one of its deliveries, or by leaving out one of its
    // premises (a crash the derivation was conditional on): Σ hits + Σ (1 - x_p) ≥ 1.
    for clause in clauses {
        let mut covering: Expression = x
            .iter()
            .enumerate()
            .filter(|(i, _)| clause.hit_by(&universe[*i]))
            .map(|(_, &v)| Expression::from(v))
            .sum();
        let mut bound = 1.0;
        for premise in &clause.premises {
            match index_of(premise) {
                Some(i) => {
                    covering -= x[i];
                    bound -= 1.0;
                }
                // A premise outside the universe can never be selected: the clause does
                // not apply to any hypothesis the solver can produce.
                None => bound = f64::NEG_INFINITY,
            }
        }
        if bound > f64::NEG_INFINITY {
            model.add_constraint(covering.geq(bound));
        }
    }

    if !required.is_empty() {
        for (clause, &indicator) in required.iter().zip(&preserved) {
            for (i, &v) in x.iter().enumerate() {
                if clause.hit_by(&universe[i]) {
                    model.add_constraint((Expression::from(v) + indicator).leq(1.0));
                }
            }
            // A required derivation from a crash world is preserved only in a world with
            // the same crashes.
            for premise in &clause.premises {
                match index_of(premise) {
                    Some(i) => {
                        model.add_constraint((Expression::from(indicator) - x[i]).leq(0.0));
                    }
                    None => {
                        model.add_constraint(Expression::from(indicator).leq(0.0));
                    }
                }
            }
        }
        let any: Expression = preserved.iter().map(|&v| Expression::from(v)).sum();
        model.add_constraint(any.geq(1.0));
    }

    let crashes: Vec<usize> = (0..universe.len())
        .filter(|&i| matches!(universe[i], Fault::Crash { .. }))
        .collect();
    let crash_total: Expression = crashes.iter().map(|&i| Expression::from(x[i])).sum();
    model.add_constraint(crash_total.leq(crash_budget as f64));
    let nodes: BTreeSet<&String> = crashes
        .iter()
        .filter_map(|&i| match &universe[i] {
            Fault::Crash { node, .. } => Some(node),
            Fault::Omit(_) => None,
        })
        .collect();
    for node in nodes {
        let per_node: Expression = crashes
            .iter()
            .filter(|&&i| matches!(&universe[i], Fault::Crash { node: n, .. } if n == node))
            .map(|&i| Expression::from(x[i]))
            .sum();
        model.add_constraint(per_node.leq(1.0));
    }

    // No superset of a vacuous hypothesis.
    for vacuous_set in vacuous {
        let inside: Vec<usize> = vacuous_set.iter().filter_map(index_of).collect();
        if inside.len() != vacuous_set.len() {
            continue;
        }
        let selected: Expression = inside.iter().map(|&i| Expression::from(x[i])).sum();
        model.add_constraint(selected.leq(inside.len() as f64 - 1.0));
    }

    for tried_set in tried {
        // Every tried set was drawn from an earlier (smaller) universe, so its faults index.
        let inside: Vec<usize> = tried_set.iter().filter_map(index_of).collect();
        let mut no_good = Expression::from(0.0);
        for (i, &v) in x.iter().enumerate() {
            if inside.contains(&i) {
                no_good += v;
            } else {
                no_good -= v;
            }
        }
        model.add_constraint(no_good.leq(inside.len() as f64 - 1.0));
    }

    match model.solve() {
        Ok(solution) => Some(
            x.iter()
                .enumerate()
                .filter(|(_, v)| solution.value(**v) > 0.5)
                .map(|(i, _)| universe[i].clone())
                .collect(),
        ),
        Err(ResolutionError::Infeasible) => None,
        Err(error) => panic!("hitting-set ILP failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(channel: &str, payload: u32) -> Message {
        Message {
            channel: channel.to_owned(),
            // Identity is by channel id; derive a distinct one per label.
            channel_id: channel.bytes().fold(0usize, |h, b| h * 31 + b as usize),
            from_member: None,
            to_member: None,
            payload: bincode::serialize(&payload).unwrap(),
            display: payload.to_string(),
        }
    }

    fn lossy(channel: &str, payload: u32) -> Dependency {
        Dependency {
            message: message(channel, payload),
            sender: None,
            lossy: true,
        }
    }

    fn reliable(channel: &str, payload: u32, sender: &str, ordinal: u64) -> Dependency {
        Dependency {
            message: message(channel, payload),
            sender: Some((sender.to_owned(), ordinal)),
            lossy: false,
        }
    }

    fn send(channel: usize, payload: u32) -> SendId {
        SendId {
            channel,
            to_member: None,
            bytes: bincode::serialize(&payload).unwrap(),
            display: format!("ch{channel}:{payload}"),
        }
    }

    /// A clause whose run ended finite failures after its last send (every send is a crash
    /// point).
    fn clause(deps: impl IntoIterator<Item = Dependency>, sends: &Rc<SendSequences>) -> Clause {
        Clause {
            deps: deps.into_iter().collect(),
            sends: sends.clone(),
            pre_eff_sends: sends
                .iter()
                .map(|(node, seq)| (node.clone(), seq.len()))
                .collect(),
            premises: BTreeSet::new(),
        }
    }

    /// The universe in weakest-first order, as `run` builds it: omissions, then crashes with
    /// the later crash point first.
    fn universe_of(clauses: &[Clause]) -> Vec<Fault> {
        let mut universe: Vec<Fault> = clauses
            .iter()
            .flat_map(Clause::candidate_faults)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let position = |fault: &Fault| -> u64 {
            let Fault::Crash {
                node,
                after: Some(m),
            } = fault
            else {
                return 0;
            };
            clauses
                .iter()
                .filter_map(|c| c.sends.get(node))
                .filter_map(|seq| seq.iter().position(|s| s == m))
                .map(|p| p as u64 + 1)
                .max()
                .unwrap_or(0)
        };
        universe.sort_by_key(|f| match f {
            Fault::Omit(m) => (0, String::new(), std::cmp::Reverse(0), format!("{m:?}")),
            Fault::Crash { node, .. } => (
                1,
                node.clone(),
                std::cmp::Reverse(position(f)),
                format!("{f}"),
            ),
        });
        universe
    }

    #[test]
    fn hitting_sets_grow_by_size_and_skip_tried() {
        let sends = Rc::new(SendSequences::new());
        let clauses = vec![
            clause([lossy("req", 1), lossy("ack", 1)], &sends),
            clause([lossy("req", 2), lossy("ack", 2)], &sends),
        ];
        let universe = universe_of(&clauses);
        let mut tried = BTreeSet::new();
        // Every minimal hitting set (one literal per clause) comes first, each exactly once...
        for _ in 0..4 {
            let h = next_hypothesis(&universe, &clauses, &[], &tried, &[], 0).unwrap();
            assert_eq!(h.len(), 2);
            assert!(tried.insert(h));
        }
        // ...and only then the non-minimal ones.
        let h = next_hypothesis(&universe, &clauses, &[], &tried, &[], 0).unwrap();
        assert_eq!(h.len(), 3);
    }

    #[test]
    fn empty_clause_has_no_hitting_set() {
        let sends = Rc::new(SendSequences::new());
        let clauses = vec![clause([lossy("req", 1)], &sends), clause([], &sends)];
        let universe = universe_of(&clauses);
        assert!(next_hypothesis(&universe, &clauses, &[], &BTreeSet::new(), &[], 0).is_none());
    }

    /// A crash after message `m` kills every send after `m`'s position in the run, so one
    /// crash can hit a clause of several messages from the same sender, and the weakest such
    /// crash (after the latest message) is tried first.
    #[test]
    fn crash_kills_later_sends_and_respects_budget() {
        // The sender's fan-out to two members: sends `m0` then `m1`.
        let (m0, m1) = (send(0, 42), send(1, 42));
        let sends = Rc::new(SendSequences::from([(
            "s".to_owned(),
            vec![m0.clone(), m1.clone()],
        )]));
        let clauses = vec![clause(
            [reliable("to@0", 42, "s", 0), reliable("to@1", 42, "s", 1)],
            &sends,
        )];
        let universe = universe_of(&clauses);
        let tried = BTreeSet::new();
        assert_eq!(
            next_hypothesis(&universe, &clauses, &[], &tried, &[], 0),
            None,
            "a reliable channel admits no omission; with no crash budget nothing can be done"
        );
        // Weakest first: the crash after `m0` (killing only `m1`) is tried before the crash
        // before any send (killing both).
        let first = next_hypothesis(&universe, &clauses, &[], &tried, &[], 1).unwrap();
        let later = Fault::Crash {
            node: "s".to_owned(),
            after: Some(m0),
        };
        assert_eq!(first, [later.clone()].into_iter().collect());
        let killed: Vec<bool> = clauses[0]
            .deps
            .iter()
            .map(|d| d.killed_by(&later, &sends, &clauses[0].pre_eff_sends))
            .collect();
        assert_eq!(killed, [false, true]);
        let earlier = Fault::Crash {
            node: "s".to_owned(),
            after: None,
        };
        assert!(clauses[0].deps.iter().all(|d| d.killed_by(
            &earlier,
            &sends,
            &clauses[0].pre_eff_sends
        )));
        // In a run where `m0` was never sent, a crash after it never fires.
        let other_run = Rc::new(SendSequences::from([("s".to_owned(), vec![m1])]));
        let other_pre_eff: PreEffSends = [("s".to_owned(), 1)].into_iter().collect();
        assert!(!reliable("to@1", 42, "s", 0).killed_by(&later, &other_run, &other_pre_eff));
    }

    /// Every failure happens at a time `≤ EFF`: a crash point named by a message that this run
    /// sends only after the end of finite failures does not fire here and kills nothing, while
    /// a pre-`EFF` crash point kills every later send, including the post-`EFF` ones; the
    /// clause must say both. Only pre-`EFF` crash points are proposed. (The ISR
    /// unclean-election bug needs `crash(leader after client-ack)` to kill the leader's later
    /// heartbeats; with the kill relation clamped at `EFF` the counterexample was never a
    /// hitting set and the search certified the bug.)
    #[test]
    fn crash_points_fire_only_before_eff_and_kill_every_later_send() {
        // The sender's run: `a`, `b` before EFF; `c`, `d` after it.
        let (a, b, c, d) = (send(0, 1), send(0, 2), send(0, 3), send(0, 4));
        let sends = Rc::new(SendSequences::from([(
            "s".to_owned(),
            vec![a.clone(), b.clone(), c.clone(), d.clone()],
        )]));
        let pre_eff: PreEffSends = [("s".to_owned(), 2)].into_iter().collect();
        let dep_b = reliable("to", 2, "s", 1);
        let dep_d = reliable("to", 4, "s", 3);
        let after = |m: &SendId| Fault::Crash {
            node: "s".to_owned(),
            after: Some(m.clone()),
        };
        // A pre-EFF crash point kills every later send, post-EFF ones included.
        assert!(dep_d.killed_by(&after(&a), &sends, &pre_eff));
        assert!(dep_d.killed_by(&after(&b), &sends, &pre_eff));
        assert!(dep_b.killed_by(&after(&a), &sends, &pre_eff));
        assert!(!dep_b.killed_by(&after(&b), &sends, &pre_eff));
        // A crash point named by a post-EFF send of this run (minted in some other run where
        // `c` was pre-EFF) does not fire here.
        assert!(!dep_d.killed_by(&after(&c), &sends, &pre_eff));
        assert!(!dep_d.killed_by(&after(&d), &sends, &pre_eff));
        // Had `c` been pre-EFF, the crash after it would kill `d`.
        let later_eff: PreEffSends = [("s".to_owned(), 3)].into_iter().collect();
        assert!(dep_d.killed_by(&after(&c), &sends, &later_eff));
        // Proposal: the weakest crash point that kills `d` is the one right after the last
        // pre-EFF send, `b`; neither `c` nor `d` is minted as a crash point.
        let proposed = dep_d.candidate_faults(&sends, &pre_eff);
        assert_eq!(proposed, vec![after(&b)]);
        let clause = Clause {
            deps: [dep_d].into_iter().collect(),
            sends,
            pre_eff_sends: pre_eff,
            premises: BTreeSet::new(),
        };
        assert!(clause.hit_by(&after(&a)));
        assert!(clause.hit_by(&after(&b)));
        assert!(!clause.hit_by(&after(&c)));
        assert!(!clause.hit_by(&after(&d)));
    }
}
