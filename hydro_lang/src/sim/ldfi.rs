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
//! # Fault vocabulary
//!
//! - **Omission** `omit(channel, payload)`, available on channels registered with
//!   [`Ldfi::lossy_channel`]: the receiving hook is placed under a standing
//!   [omission policy](crate::sim::hooks::BatchHook::omit). Only unordered (`NoOrder`)
//!   hooks offer it, which is the guarantee a lossy transport yields.
//! - **Crash** `crash(node, after k sends)`, available for nodes registered with
//!   [`Ldfi::crashable`] / [`Ldfi::crashable_cluster`]: the node performs its first `k`
//!   sends and every later send is dropped at the send half (and its external outputs are
//!   suppressed once it is dead); see `sim::runtime::lineage`. This is Molly's definition
//!   of a crash, and it can split the fan-out of one broadcast. Channels registered with
//!   [`Ldfi::reliable_channel`] admit no omission; crashes of their senders are their only
//!   faults, which is what `fail_stop` declares.
//!
//! # Lineage
//!
//! Lineage is **execution-granular**. The simulator records every execution (an async run
//! or a tick execution), which hook releases fed each tick execution, which execution sent
//! each network message (and which node, as its `j`-th send), and which execution produced
//! each `sim_output` value. The support of a goal output is the backward closure from the
//! execution that produced it: a tick execution depends on the channel messages its hooks
//! released into it (each traced to the execution that sent it) and, as a sound
//! over-approximation of persisted state, on the earlier executions of the same tick; an
//! async run depends on the tick executions at its location since its previous run.
//! Messages that arrive through an unregistered path are invisible, so every channel a
//! goal can depend on must be registered for the closure to be sound.
//!
//! A support is a set of [`Dependency`]s (message, sender, ordinal). A fault *kills* a
//! dependency if it is `omit` of that message on a lossy channel, or `crash(sender, k)`
//! with `k ≤` the message's ordinal among the sender's sends. A hypothesis is a hitting
//! set under `kills`; crash literals are capped by [`Ldfi::crash_budget`].
//!
//! # Goals
//!
//! - [`Ldfi::goal`]: existential ("some value was output"): every output is one derivation.
//! - [`Ldfi::goal_all_members`]: a conjunction over the *correct* (non-crashed) members of
//!   a cluster, as in reliable broadcast's agreement: a derivation of the conjunction is one
//!   derivation per correct member, unioned. If no correct member produced output the goal
//!   held vacuously and contributes no clause.
//!
//! The body itself decides whether the goal held (it receives the [`Run`] and can ask which
//! nodes crashed), so a conditional invariant like "if any correct member delivered, all
//! did" is expressed directly.
//!
//! # Composition with the schedule explorer, and bounds
//!
//! Each hypothesis is run under [`CompiledSim::exhaustive`]: the fault set is fixed and the
//! simulator explores every batching/ordering schedule under it. The body drives time
//! (timers are `sim_input`s), so the horizon `EOT` is what the body fires; the end of finite
//! failures `EFF` is declared by [`Run::end_of_finite_failures`], after which omissions
//! stop and only messages delivered before it are omission candidates.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Debug, Display};
use std::panic::RefUnwindSafe;
use std::rc::Rc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::compile::builder::ExternalPortId;
use crate::live_collections::stream::{NoOrder, Ordering, Retries, TotalOrder};
use crate::location::dynamic::LocationId;
use crate::location::{Cluster, Location, Process, TaglessMemberId};
use crate::sim::compiled::{
    CompiledSim, hook_root_location, hook_tick_location, lineage_events, lineage_handles,
    lineage_port, lineage_provenance_enabled, lineage_slices,
};
use crate::sim::hooks::{BatchHook, ScriptableHookScope};
use crate::sim::runtime::{CrashPoint, ExecId, LineageEvent, NodeKey};
use crate::sim::slice::{SinkSlice, SliceInput, Slices};
use crate::sim::{SimClusterReceiver, SimReceiver};

/// A message as the omission vocabulary names it: the channel it travelled on (the label of
/// its fault point) and its payload. Identity is by channel and payload value: omitting a
/// message omits every byte-identical copy on that channel while the policy stands.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Message {
    /// The fault point's label.
    pub channel: String,
    /// The bincode-serialized payload.
    payload: Vec<u8>,
    /// The payload's `Debug` rendering, for reports.
    pub display: String,
}

impl Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.channel, self.display)
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
    /// run it was observed in, when the sender is crashable.
    pub sender: Option<(String, u64)>,
    /// Whether the channel admits omission.
    pub lossy: bool,
}

/// Every crashable node's send sequence in one run, which gives crash points their meaning
/// there: `crash(node after m)` kills the sends after `m`'s position.
pub type SendSequences = BTreeMap<String, Vec<SendId>>;

impl Dependency {
    /// Whether `fault` prevents this delivery, in a run whose crashable nodes sent `sends`.
    pub fn killed_by(&self, fault: &Fault, sends: &SendSequences) -> bool {
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
                    Some(m) => sends
                        .get(node)
                        .and_then(|seq| seq.iter().position(|s| s == m))
                        .is_some_and(|at| (at as u64) < *ordinal),
                }
            }
        }
    }

    /// The weakest faults that kill this delivery: its omission (if lossy) and the crash of
    /// its sender right after the send preceding this one (if crashable).
    fn candidate_faults(&self, sends: &SendSequences) -> Vec<Fault> {
        let mut faults = vec![];
        if self.lossy {
            faults.push(Fault::Omit(self.message.clone()));
        }
        if let Some((sender, ordinal)) = &self.sender {
            let after = (*ordinal > 0)
                .then(|| {
                    sends
                        .get(sender)
                        .and_then(|seq| seq.get(*ordinal as usize - 1))
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

/// A derivation of the goal: the deliveries it depended on. Killing any one of them kills
/// the derivation.
pub type Derivation = BTreeSet<Dependency>;

/// The batch hooks that can be fault points: ordered (reliable channel) and unordered
/// (lossy channel) batch hooks, under a standing policy.
#[sealed::sealed]
pub trait PolicyHook<T>: RefUnwindSafe {
    #[doc(hidden)]
    fn key(&self) -> (usize, Option<u32>);
    /// Installs the policy omitting `omit` (which is empty for ordered hooks).
    #[doc(hidden)]
    fn install(&self, omit: Vec<T>);
    /// Installs a replay policy (see [`BatchHook::replay`]).
    #[doc(hidden)]
    fn replay(&self, execs: Vec<Vec<T>>);
    #[doc(hidden)]
    fn delivered(&self) -> Vec<T>;
    #[doc(hidden)]
    fn dropped(&self) -> Vec<T>;
}

#[sealed::sealed]
impl<T, R, S> PolicyHook<T> for BatchHook<T, NoOrder, R, S>
where
    T: Serialize + DeserializeOwned + PartialEq,
    R: Retries,
    S: ScriptableHookScope,
{
    fn key(&self) -> (usize, Option<u32>) {
        (self.id, self.member)
    }
    fn install(&self, omit: Vec<T>) {
        self.omit(omit);
    }
    fn replay(&self, execs: Vec<Vec<T>>) {
        BatchHook::<T, NoOrder, R, S>::replay(self, execs);
    }
    fn delivered(&self) -> Vec<T> {
        BatchHook::delivered(self)
    }
    fn dropped(&self) -> Vec<T> {
        BatchHook::dropped(self)
    }
}

#[sealed::sealed]
impl<T, R, S> PolicyHook<T> for BatchHook<T, TotalOrder, R, S>
where
    T: Serialize + DeserializeOwned + PartialEq,
    R: Retries,
    S: ScriptableHookScope,
{
    fn key(&self) -> (usize, Option<u32>) {
        (self.id, self.member)
    }
    fn install(&self, omit: Vec<T>) {
        assert!(
            omit.is_empty(),
            "internal error: omission requested on a reliable channel"
        );
        self.deliver_all();
    }
    fn replay(&self, execs: Vec<Vec<T>>) {
        BatchHook::<T, TotalOrder, R, S>::replay(self, execs);
    }
    fn delivered(&self) -> Vec<T> {
        BatchHook::delivered(self)
    }
    fn dropped(&self) -> Vec<T> {
        BatchHook::dropped(self)
    }
}

/// A type-erased fault point: one channel's receiving hook.
trait FaultPoint: RefUnwindSafe {
    fn label(&self) -> &str;
    fn hook_key(&self) -> (usize, Option<u32>);
    /// Whether omission is in this channel's vocabulary.
    fn lossy(&self) -> bool;
    fn decode(&self, payload: &[u8]) -> Message;
    /// Installs this channel's share of `faults`.
    fn install(&self, faults: &FaultSet);
    fn deliver_all(&self);
    /// Places the hook under a replay policy: `execs[k]` (serialized items) is released into
    /// the `k`-th execution of its tick.
    fn replay(&self, execs: Vec<Vec<Vec<u8>>>);
    fn delivered(&self) -> Vec<Message>;
    fn dropped(&self) -> Vec<Message>;
}

struct ChannelPoint<T, H> {
    label: String,
    hook: H,
    /// `false` for a channel registered as reliable even though its hook is unordered.
    lossy: bool,
    _phantom: std::marker::PhantomData<fn(T)>,
}

impl<T, H> ChannelPoint<T, H>
where
    T: Serialize + DeserializeOwned + Debug,
{
    fn message(&self, payload: &T) -> Message {
        Message {
            channel: self.label.clone(),
            payload: bincode::serialize(payload).unwrap(),
            display: format!("{payload:?}"),
        }
    }
}

impl<T, H> FaultPoint for ChannelPoint<T, H>
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
    H: PolicyHook<T>,
{
    fn label(&self) -> &str {
        &self.label
    }

    fn hook_key(&self) -> (usize, Option<u32>) {
        self.hook.key()
    }

    fn lossy(&self) -> bool {
        self.lossy
    }

    fn decode(&self, payload: &[u8]) -> Message {
        let value: T = bincode::deserialize(payload)
            .expect("a released payload did not decode as the hook's element type");
        self.message(&value)
    }

    fn install(&self, faults: &FaultSet) {
        let omit: Vec<T> = faults
            .iter()
            .filter_map(|fault| match fault {
                Fault::Omit(m) if m.channel == self.label => {
                    Some(bincode::deserialize(&m.payload).unwrap())
                }
                _ => None,
            })
            .collect();
        self.hook.install(omit);
    }

    fn deliver_all(&self) {
        self.hook.install(vec![]);
    }

    fn replay(&self, execs: Vec<Vec<Vec<u8>>>) {
        let execs: Vec<Vec<T>> = execs
            .iter()
            .map(|items| {
                items
                    .iter()
                    .map(|payload| {
                        bincode::deserialize(payload)
                            .expect("a recorded release did not decode as the hook's element type")
                    })
                    .collect()
            })
            .collect();
        self.hook.replay(execs);
    }

    fn delivered(&self) -> Vec<Message> {
        self.hook
            .delivered()
            .iter()
            .map(|payload| self.message(payload))
            .collect()
    }

    fn dropped(&self) -> Vec<Message> {
        self.hook
            .dropped()
            .iter()
            .map(|payload| self.message(payload))
            .collect()
    }
}

/// One explored schedule of one hypothesis run.
#[derive(Clone, Debug)]
pub struct Schedule {
    /// Whether the goal held.
    pub goal_held: bool,
    /// The goal's derivations in this schedule (see the module docs on goals).
    pub derivations: Vec<Derivation>,
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
    /// The lineage of the outputs the body required (the goal's pre-condition), whose sends
    /// the search wants to preserve.
    pub requirements: Derivation,
    /// Deliveries at registered hooks that could not be traced to a send (a lineage gap).
    pub unmatched_releases: usize,
    /// Whether the body declared the run vacuous ([`Run::mark_vacuous`]: the goal's
    /// pre-condition failed, so the run says nothing about the protocol).
    pub vacuous: bool,
    /// Counterfactual replays run to derive this schedule's lineage ([`Level::Counterfactual`]).
    pub replays: usize,
    /// Derivation steps where the node did not reproduce its output under replay and the
    /// closure fell back to the recorded lineage.
    pub replay_fallbacks: usize,
}

/// A derivation together with the send sequences of the run it was observed in.
#[derive(Clone, Debug)]
pub struct Clause {
    /// The deliveries the goal was derived from.
    pub deps: Derivation,
    /// The run's send sequences.
    pub sends: Rc<SendSequences>,
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
            .any(|dep| dep.killed_by(fault, &self.sends))
    }

    /// Every fault that kills some delivery of this clause.
    pub fn candidate_faults(&self) -> impl Iterator<Item = Fault> + '_ {
        self.deps
            .iter()
            .flat_map(|dep| dep.candidate_faults(&self.sends))
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

/// The result of an LDFI search.
#[derive(Clone, Debug)]
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
    },
    /// The hypothesis budget ran out before the search finished.
    Budget {
        /// Every hypothesis run, in order.
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
                for send in sends {
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
#[derive(Clone, Debug)]
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

    /// Declares the end of finite failures (`EFF`): every lossy channel delivers everything
    /// from here on, and only messages delivered so far are omission candidates. Call it at
    /// most once; without it, `EFF = EOT`.
    pub fn end_of_finite_failures(&self) {
        let mut cut = self.eff_cut.borrow_mut();
        assert!(
            cut.is_none(),
            "end_of_finite_failures() was called twice in one run"
        );
        *cut = Some(lineage_events().len());
        for point in &self.search.points {
            point.deliver_all();
        }
    }

    fn install(&self) {
        for point in &self.search.points {
            point.install(self.faults);
        }
        let handles = lineage_handles();
        let mut crashes = handles.crashes.borrow_mut();
        for fault in self.faults {
            if let Fault::Crash { node, after } = fault {
                let key = self
                    .search
                    .nodes
                    .get(node)
                    .unwrap_or_else(|| panic!("crash fault names unregistered node {node:?}"));
                let point = match after {
                    None => CrashPoint::BeforeAnySend,
                    Some(send) => CrashPoint::AfterSend {
                        channel: send.channel,
                        to_member: send.to_member,
                        bytes: send.bytes.clone(),
                    },
                };
                crashes.install(key.clone(), point);
            }
        }
    }

    fn finish(self, goal_held: bool) -> RunRecord {
        let points = &self.search.points;
        let delivered: Vec<Message> = points.iter().flat_map(|p| p.delivered()).collect();
        let dropped: Vec<Message> = points.iter().flat_map(|p| p.dropped()).collect();
        let events = lineage_events();
        let eff_cut = (*self.eff_cut.borrow()).unwrap_or(events.len());
        RunRecord {
            goal_held,
            vacuous: self.vacuous.get(),
            faults: self.faults.clone(),
            goal_port: self.goal_port,
            level: self.level,
            events,
            eff_cut,
            witnesses: self.witnesses.into_inner(),
            final_witnesses: self.final_witnesses.into_inner(),
            requirements: self.requirements.into_inner(),
            delivered,
            dropped,
            context: InstanceContext::capture(self.search),
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
    /// Receiving node of each point's hook.
    point_nodes: Vec<NodeKey>,
    /// The tick (execution unit) and member each point's hook feeds.
    point_ticks: Vec<NodeKey>,
}

impl InstanceContext {
    fn capture(search: &Search) -> Self {
        InstanceContext {
            slices: lineage_slices(),
            provenance: lineage_provenance_enabled(),
            point_nodes: search
                .points
                .iter()
                .map(|p| {
                    let (hook_id, member) = p.hook_key();
                    hook_root_location(hook_id, member)
                })
                .collect(),
            point_ticks: search
                .points
                .iter()
                .map(|p| {
                    let (hook_id, member) = p.hook_key();
                    hook_tick_location(hook_id, member)
                })
                .collect(),
        }
    }
}

/// Everything one run of the body produced, taken out of the instance; turned into a
/// [`Schedule`] by [`Self::schedule`] once the instance has returned.
struct RunRecord {
    goal_held: bool,
    vacuous: bool,
    faults: FaultSet,
    goal_port: Option<usize>,
    level: Level,
    events: Vec<LineageEvent>,
    eff_cut: usize,
    witnesses: Vec<(usize, Option<u32>, Vec<u8>)>,
    final_witnesses: Vec<(usize, Option<u32>, Vec<u8>)>,
    requirements: Vec<(usize, Option<u32>, Vec<u8>)>,
    delivered: Vec<Message>,
    dropped: Vec<Message>,
    context: InstanceContext,
}

impl RunRecord {
    fn crashed_member(&self, cluster: &str, member: u32) -> bool {
        let label = member_label(cluster, member);
        self.faults
            .iter()
            .any(|fault| matches!(fault, Fault::Crash { node: n, .. } if *n == label))
    }

    fn schedule(&self, search: &Search, replayer: Option<&Replayer<'_>>) -> Schedule {
        let lineage = Lineage::new(
            &self.events,
            search,
            self.eff_cut,
            self.level,
            &self.context,
            replayer,
        );
        let crashed_sends = lineage
            .sent
            .iter()
            .filter(|s| s.dropped)
            .filter_map(|s| s.sender.clone())
            .collect();
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
                    let mut derivation = lineage.witnessed(&self.witnesses);
                    derivation.extend(lineage.witnessed_final(&self.final_witnesses));
                    vec![derivation]
                }
            }
            // No goal registered: everything delivered before EFF is the one derivation.
            _ => vec![lineage.all_pre_eff_deliveries()],
        };
        let requirements = if self.requirements.is_empty() {
            Derivation::new()
        } else {
            lineage.witnessed(&self.requirements)
        };
        Schedule {
            goal_held: self.goal_held,
            derivations,
            observed: lineage.all_deliveries(),
            delivered: self.delivered.clone(),
            dropped: self.dropped.clone(),
            crashed_sends,
            sends: Rc::new(lineage.send_sequences()),
            requirements,
            unmatched_releases: lineage.unmatched_releases,
            vacuous: self.vacuous,
            replays: lineage.replays.get(),
            replay_fallbacks: lineage.replay_fallbacks.get(),
        }
    }
}

/// A recorded send, resolved against the registered nodes.
struct SentRecord {
    exec: ExecId,
    channel: usize,
    to: NodeKey,
    from_member: Option<u32>,
    bytes: Vec<u8>,
    /// `(node label, ordinal)` when the sender is crashable.
    sender: Option<(String, u64)>,
    dropped: bool,
    /// Delivery ids the record was computed from (record-level provenance; empty without).
    prov: Vec<u64>,
}

/// A hook release, resolved against the sends.
struct ReleaseRecord {
    point: usize,
    hook_id: usize,
    payload: Vec<u8>,
    pre_eff: bool,
    /// `(node label, ordinal)` of the matched send when its sender is crashable.
    sender: Option<(String, u64)>,
    /// The execution that performed the matched send, and the channel it used.
    producer: Option<(ExecId, usize)>,
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

/// A delivery id assigned by the provenance rewrite to one item a batch released, resolved
/// against the sends like a release.
struct TagRecord {
    /// The registered fault point of the batch's hook, if any.
    point: Option<usize>,
    bytes: Vec<u8>,
    pre_eff: bool,
    /// `(node label, ordinal)` of the matched send when its sender is crashable.
    sender: Option<(String, u64)>,
    /// Index into `sent` of the matched send.
    producer: Option<usize>,
}

/// A top-level receive, resolved against the sends (by arrival order on its channel).
struct RecvRecord {
    channel: usize,
    /// Index into `sent` of the matching send.
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
    sent: Vec<SentRecord>,
    /// Delivered goal-candidate outputs.
    outputs: Vec<OutputRecord>,
    /// Delivery ids (record-level provenance), by id.
    tags: HashMap<u64, TagRecord>,
    /// Which lineage the closure uses.
    level: Level,
    /// Whether the program was compiled with record-level provenance.
    provenance: bool,
    /// Receiving node of each point's hook.
    point_nodes: Vec<NodeKey>,
    /// The tick and member each point's hook feeds.
    point_ticks: Vec<NodeKey>,
    /// Releases that matched no send (diagnostic).
    unmatched_releases: usize,
    /// The replay oracle ([`Level::Counterfactual`]).
    replayer: Option<&'a Replayer<'a>>,
    /// Counterfactual derivations of sends, by index into `sent`.
    send_memo: RefCell<HashMap<usize, Derivation>>,
    /// Replays run so far.
    replays: Cell<usize>,
    /// Derivation steps that fell back to the recorded lineage.
    replay_fallbacks: Cell<usize>,
}

impl<'a> Lineage<'a> {
    fn new(
        events: &[LineageEvent],
        search: &'a Search,
        eff_cut: usize,
        level: Level,
        context: &InstanceContext,
        replayer: Option<&'a Replayer<'a>>,
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
            sent: vec![],
            outputs: vec![],
            tags: HashMap::new(),
            level,
            provenance: context.provenance,
            point_nodes: context.point_nodes.clone(),
            point_ticks: context.point_ticks.clone(),
            unmatched_releases: 0,
            replayer,
            send_memo: RefCell::new(HashMap::new()),
            replays: Cell::new(0),
            replay_fallbacks: Cell::new(0),
        };
        let mut send_counts: HashMap<NodeKey, u64> = HashMap::new();
        // `(exec, point, payload, pre_eff)` in event order, matched to sends below.
        let mut raw_releases: Vec<(ExecId, usize, Vec<u8>, bool)> = vec![];
        // `(exec, channel, node)` in event order, matched to sends below.
        let mut raw_recvs: Vec<(ExecId, usize, NodeKey)> = vec![];
        // `(exec, id, hook, member, bytes, pre_eff)` in event order, matched to sends below.
        let mut raw_tags: Vec<(ExecId, u64, Option<usize>, Option<u32>, Vec<u8>, bool)> = vec![];
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
                    if let Some(point) = search
                        .points
                        .iter()
                        .position(|p| p.hook_key() == (*hook_id, *member))
                    {
                        raw_releases.extend(
                            items
                                .iter()
                                .map(|item| (*exec, point, item.clone(), index < eff_cut)),
                        );
                    }
                }
                LineageEvent::Sent {
                    exec,
                    channel,
                    from,
                    from_member,
                    to,
                    to_member,
                    bytes,
                    dropped,
                    prov,
                } => {
                    let node = (from.clone(), *from_member);
                    let ordinal = send_counts.entry(node.clone()).or_insert(0);
                    let sender = labels_by_node
                        .get(&node)
                        .map(|label| ((*label).clone(), *ordinal));
                    *ordinal += 1;
                    lineage.sent.push(SentRecord {
                        exec: *exec,
                        channel: *channel,
                        to: (to.clone(), *to_member),
                        from_member: *from_member,
                        bytes: bytes.clone(),
                        sender,
                        dropped: *dropped,
                        prov: prov.clone(),
                    });
                }
                LineageEvent::Recv {
                    exec,
                    channel,
                    to,
                    to_member,
                    ..
                } => {
                    raw_recvs.push((*exec, *channel, (to.clone(), *to_member)));
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
                LineageEvent::Tag {
                    exec,
                    id,
                    hook_id,
                    member,
                    bytes,
                } => {
                    raw_tags.push((
                        *exec,
                        *id,
                        *hook_id,
                        *member,
                        bytes.clone(),
                        index < eff_cut,
                    ));
                }
            }
        }

        // Match each release to the send that produced it. The static slice names the
        // channel(s) a hook batches, so a release is matched only against sends on those
        // channels to the receiving node, and otherwise against any send to that node (a
        // hook the slice could not resolve). A send is delivered at most once, so releases
        // (in event order) take the earliest unmatched send with the same bytes that
        // happened before the release.
        let mut matched = vec![false; lineage.sent.len()];
        for (exec, point, payload, pre_eff) in raw_releases {
            let to = &lineage.point_nodes[point];
            let hook_id = search.points[point].hook_key().0;
            let channels = lineage
                .slices
                .hook_channels
                .get(&hook_id)
                .filter(|c| !c.is_empty());
            let hit = lineage.sent.iter().enumerate().position(|(i, record)| {
                !matched[i]
                    && !record.dropped
                    && record.exec < exec
                    && record.to == *to
                    && channels.is_none_or(|c| c.contains(&record.channel))
                    && payload_matches(&payload, &record.bytes, record.from_member)
            });
            let (sender, producer, send) = if let Some(i) = hit {
                matched[i] = true;
                (
                    lineage.sent[i].sender.clone(),
                    Some((lineage.sent[i].exec, lineage.sent[i].channel)),
                    Some(i),
                )
            } else {
                if channels.is_some() {
                    lineage.unmatched_releases += 1;
                }
                (None, None, None)
            };
            lineage
                .released
                .entry(exec)
                .or_default()
                .push(ReleaseRecord {
                    point,
                    hook_id,
                    payload,
                    pre_eff,
                    sender,
                    producer,
                    send,
                });
        }

        // Arrivals on one channel at one node are in send order: the k-th receive is the
        // k-th non-dropped send.
        let mut recv_counts: HashMap<(usize, NodeKey), usize> = HashMap::new();
        for (exec, channel, node) in raw_recvs {
            let k = recv_counts.entry((channel, node.clone())).or_insert(0);
            let producer = lineage
                .sent
                .iter()
                .enumerate()
                .filter(|(_, record)| {
                    !record.dropped && record.channel == channel && record.to == node
                })
                .nth(*k)
                .map(|(i, _)| i);
            *k += 1;
            lineage
                .recvs
                .entry(exec)
                .or_default()
                .push(RecvRecord { channel, producer });
        }

        // Match each delivery id to the send that produced it, with the same one-to-one
        // discipline as releases (a tag and its release are the same delivery, so they are
        // matched independently).
        let mut matched = vec![false; lineage.sent.len()];
        for (exec, id, hook_id, member, bytes, pre_eff) in raw_tags {
            let Some((location, _, _)) = lineage.execs.get(&exec) else {
                continue;
            };
            let to = (location.root().clone(), member);
            let point = hook_id.and_then(|hook| {
                search
                    .points
                    .iter()
                    .position(|p| p.hook_key() == (hook, member))
            });
            let channels = hook_id
                .and_then(|hook| lineage.slices.hook_channels.get(&hook))
                .filter(|c| !c.is_empty());
            let hit = lineage.sent.iter().enumerate().position(|(i, record)| {
                !matched[i]
                    && !record.dropped
                    && record.exec < exec
                    && record.to == to
                    && channels.is_none_or(|c| c.contains(&record.channel))
                    && payload_matches(&bytes, &record.bytes, record.from_member)
            });
            let sender = hit.and_then(|i| {
                matched[i] = true;
                lineage.sent[i].sender.clone()
            });
            lineage.tags.insert(
                id,
                TagRecord {
                    point,
                    bytes,
                    pre_eff,
                    sender,
                    producer: hit,
                },
            );
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
                "lineage: {} execs, {} sends ({} dropped), {} releases ({matched} matched to sends), {} recvs, {} outputs, {} tags",
                lineage.execs.len(),
                lineage.sent.len(),
                lineage.sent.iter().filter(|s| s.dropped).count(),
                releases,
                recvs,
                lineage.outputs.len(),
                lineage.tags.len(),
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

    /// The identity of a recorded send as a crash point, with a readable rendering: the
    /// receiving fault point's label and decoded payload when the channel is registered.
    fn send_id(&self, record: &SentRecord) -> SendId {
        let display = self
            .search
            .points
            .iter()
            .enumerate()
            .find(|(i, point)| {
                self.point_nodes[*i] == record.to
                    && self
                        .slices
                        .hook_channels
                        .get(&point.hook_key().0)
                        .is_some_and(|channels| channels.contains(&record.channel))
            })
            .map(|(_, point)| {
                // The point decodes what the receiver sees: a cluster-sourced channel
                // carries the sender's member id before the payload.
                let received = match record.from_member {
                    Some(m) => {
                        let mut bytes =
                            bincode::serialize(&TaglessMemberId::from_raw_id(m)).unwrap();
                        bytes.extend_from_slice(&record.bytes);
                        bytes
                    }
                    None => record.bytes.clone(),
                };
                format!("{:?}", point.decode(&received))
            })
            .unwrap_or_else(|| {
                let bytes: String =
                    record
                        .bytes
                        .iter()
                        .take(8)
                        .fold(String::new(), |mut acc, b| {
                            use std::fmt::Write as _;
                            let _ = write!(acc, "{b:02x}");
                            acc
                        });
                format!(
                    "ch{}→{:?}{}:{bytes}{}",
                    record.channel,
                    record.to.0,
                    record.to.1.map(|m| format!("@{m}")).unwrap_or_default(),
                    if record.bytes.len() > 8 { "…" } else { "" }
                )
            });
        SendId {
            channel: record.channel,
            to_member: record.to.1,
            bytes: record.bytes.clone(),
            display,
        }
    }

    /// A dependency on a send itself (a receive on a channel with no fault point): not
    /// omittable, killed by its sender's crash.
    fn send_dependency(&self, record: &SentRecord) -> Dependency {
        let id = self.send_id(record);
        Dependency {
            message: Message {
                channel: format!("ch{}", record.channel),
                payload: record.bytes.clone(),
                display: id.display,
            },
            sender: record.sender.clone(),
            lossy: false,
        }
    }

    /// Every crashable node's performed sends, in order.
    fn send_sequences(&self) -> SendSequences {
        let mut sequences: SendSequences = BTreeMap::new();
        for record in &self.sent {
            if record.dropped {
                continue;
            }
            if let Some((node, _)) = &record.sender {
                sequences
                    .entry(node.clone())
                    .or_default()
                    .push(self.send_id(record));
            }
        }
        sequences
    }

    /// The derivation of one output, at the configured lineage level.
    fn derive(&self, output: &OutputRecord) -> Derivation {
        match self.level {
            Level::Execution => self.closure(output.exec, None),
            Level::Slice => self.closure(output.exec, Some(output.sink)),
            Level::Record => {
                if is_opaque(&output.prov) {
                    self.counterfactual_output(output)
                } else {
                    self.record_closure(&output.prov, output.exec, Some(output.sink))
                }
            }
            Level::Counterfactual => self.counterfactual_output(output),
        }
    }

    /// The derivation of a send, at the configured level (memoized by index into `sent`):
    /// record-level where the record carries provenance, by replay where it is opaque or
    /// under [`Level::Counterfactual`], slice-level otherwise.
    fn send_derivation(&self, i: usize) -> Derivation {
        if let Some(derivation) = self.send_memo.borrow().get(&i) {
            return derivation.clone();
        }
        let record = &self.sent[i];
        let derivation = match self.level {
            Level::Counterfactual => self.counterfactual_send(i),
            Level::Record if is_opaque(&record.prov) => self.counterfactual_send(i),
            _ => self.record_closure(&record.prov, record.exec, Some(record.channel)),
        };
        self.send_memo.borrow_mut().insert(i, derivation.clone());
        derivation
    }

    /// The recorded (non-counterfactual) derivation of a send, as the fallback when replay
    /// cannot reproduce it: record-level where the record carries real provenance,
    /// slice-level otherwise (including opaque records).
    fn recorded_send_closure(&self, i: usize) -> Derivation {
        let record = &self.sent[i];
        if is_opaque(&record.prov) {
            self.closure(record.exec, Some(record.channel))
        } else {
            self.record_closure(&record.prov, record.exec, Some(record.channel))
        }
    }

    /// The node an execution ran at.
    fn exec_node(&self, exec: ExecId) -> Option<NodeKey> {
        self.execs
            .get(&exec)
            .map(|(location, member, _)| (location.root().clone(), *member))
    }

    /// [`Level::Counterfactual`] for an output: the 1-minimal set of deliveries at the
    /// producing node without which it no longer produces the output, each followed to the
    /// send that produced it.
    fn counterfactual_output(&self, output: &OutputRecord) -> Derivation {
        let fallback = || {
            if is_opaque(&output.prov) {
                self.closure(output.exec, Some(output.sink))
            } else {
                self.record_closure(&output.prov, output.exec, Some(output.sink))
            }
        };
        let Some(node) = self.exec_node(output.exec) else {
            return fallback();
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
        self.counterfactual(&node, output.exec, &produced, fallback)
    }

    /// The counterfactual derivation of a send (see [`Self::send_derivation`]).
    fn counterfactual_send(&self, i: usize) -> Derivation {
        let record = &self.sent[i];
        match self.exec_node(record.exec) {
            None => self.recorded_send_closure(i),
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
                self.counterfactual(&node, record.exec, &produced, || {
                    self.recorded_send_closure(i)
                })
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

    /// Whether removing this delivery from a replay could change what the solver sees:
    /// it is a fault candidate itself, or it leads to a send with lineage of its own.
    fn matters(&self, release: &ReleaseRecord) -> bool {
        let dependency = self.dependency(release);
        (dependency.lossy && release.pre_eff)
            || dependency.sender.is_some()
            || release.send.is_some()
    }

    /// One counterfactual derivation step at `node`: greedy deletion over the deliveries
    /// into the node's ticks before `exec`, latest first; `produced` recognizes the target in
    /// a replay's events. Falls back to `fallback` when the node does not reproduce the
    /// target with every delivery kept, or when there is no replay oracle.
    fn counterfactual(
        &self,
        node: &NodeKey,
        exec: ExecId,
        produced: &dyn Fn(&[LineageEvent]) -> bool,
        fallback: impl FnOnce() -> Derivation,
    ) -> Derivation {
        let Some(replayer) = self.replayer else {
            return fallback();
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
            return fallback();
        }
        if candidates.is_empty() {
            return Derivation::new();
        }
        let mut removed: BTreeSet<(ExecId, usize)> = BTreeSet::new();
        let debug = std::env::var("LDFI_REPLAY_DEBUG").is_ok();
        let candidate_count = candidates.len();
        let run = |removed: &BTreeSet<(ExecId, usize)>| -> bool {
            self.replays.set(self.replays.get() + 1);
            let scripts = self.replay_scripts(node, &tick_execs, removed);
            let events = replayer.replay(&scripts);
            let hit = produced(&events);
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
                    scripts
                        .iter()
                        .map(|s| s.iter().map(Vec::len).collect::<Vec<_>>())
                        .collect::<Vec<_>>()
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
            return fallback();
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
            let mut dependency = self.dependency(release);
            if !release.pre_eff {
                dependency.lossy = false;
            }
            if dependency.lossy || dependency.sender.is_some() {
                derivation.insert(dependency);
            }
            if let Some(send) = release.send {
                derivation.extend(self.send_derivation(send));
            }
        }
        derivation
    }

    /// The replay scripts for every fault point: at `node`, each point's recorded releases
    /// per execution of its tick minus `removed`; elsewhere nothing (the tick never runs).
    fn replay_scripts(
        &self,
        node: &NodeKey,
        tick_execs: &[ExecId],
        removed: &BTreeSet<(ExecId, usize)>,
    ) -> Vec<Vec<Vec<Vec<u8>>>> {
        self.search
            .points
            .iter()
            .enumerate()
            .map(|(point, _)| {
                if self.point_nodes[point] != *node {
                    return vec![];
                }
                let tick = &self.point_ticks[point];
                tick_execs
                    .iter()
                    .filter(|e| {
                        self.execs
                            .get(e)
                            .is_some_and(|(l, m, _)| (l, *m) == (&tick.0, tick.1))
                    })
                    .map(|e| {
                        self.released
                            .get(e)
                            .into_iter()
                            .flatten()
                            .enumerate()
                            .filter(|(i, release)| {
                                release.point == point && !removed.contains(&(*e, *i))
                            })
                            .map(|(_, release)| release.payload.clone())
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    /// The record-level backward closure: delivery ids → the sends that produced them →
    /// their delivery ids. A send recorded without provenance (a path the rewrite does not
    /// tag, or a simulation compiled without it) falls back to the slice-level closure of
    /// its execution.
    fn record_closure(&self, prov: &[u64], exec: ExecId, sink: Option<usize>) -> Derivation {
        // No provenance on the record itself: a simulation compiled without it, or an output
        // on a path with no hook (a top-level `sim_output` of what a channel delivered).
        if !self.provenance || prov.is_empty() {
            return self.closure(exec, sink);
        }
        let mut derivation = Derivation::new();
        let mut visited: BTreeSet<u64> = BTreeSet::new();
        let mut stack: Vec<u64> = prov.to_vec();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            let Some(tag) = self.tags.get(&id) else {
                continue;
            };
            if let Some(point) = tag.point {
                let fault_point = &self.search.points[point];
                let dependency = Dependency {
                    message: fault_point.decode(&tag.bytes),
                    sender: tag.sender.clone(),
                    lossy: fault_point.lossy() && tag.pre_eff,
                };
                if dependency.lossy || dependency.sender.is_some() {
                    derivation.insert(dependency);
                }
            }
            if let Some(i) = tag.producer {
                let record = &self.sent[i];
                if record.prov.is_empty() {
                    derivation.extend(self.closure(record.exec, Some(record.channel)));
                } else if is_opaque(&record.prov) {
                    derivation.extend(self.send_derivation(i));
                } else {
                    stack.extend(record.prov.iter().copied());
                }
            }
        }
        derivation
    }

    /// One derivation per output on `port`.
    fn any_output(&self, port: usize) -> Vec<Derivation> {
        let mut derivations: Vec<Derivation> = vec![];
        for output in &self.outputs {
            if output.port == port {
                let derivation = self.derive(output);
                if !derivations.contains(&derivation) {
                    derivations.push(derivation);
                }
            }
        }
        derivations
    }

    /// The derivations of "every member in `members` produced output on `port`": one per
    /// combination of one derivation per member, unioned. Empty if some member produced
    /// nothing.
    fn all_members(&self, port: usize, members: &[u32]) -> Vec<Derivation> {
        let mut combined: Vec<Derivation> = vec![Derivation::new()];
        for member in members {
            let mut per_member: Vec<Derivation> = vec![];
            for output in &self.outputs {
                if output.port == port && output.member == Some(*member) {
                    let derivation = self.derive(output);
                    if !per_member.contains(&derivation) {
                        per_member.push(derivation);
                    }
                }
            }
            if per_member.is_empty() {
                return vec![];
            }
            combined = combined
                .iter()
                .flat_map(|prefix| {
                    per_member
                        .iter()
                        .map(move |d| prefix.union(d).cloned().collect())
                })
                .collect();
        }
        combined
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
                !s.dropped
                    && s.exec <= output.exec
                    && s.sender.as_ref().is_some_and(|(l, _)| *l == label)
            })
            .count() as u64;
        Some(Dependency {
            message: Message {
                channel: "alive".to_owned(),
                payload: bincode::serialize(&(label.clone(), sends)).unwrap(),
                display: format!("{label} through send #{sends}"),
            },
            sender: Some((label, sends)),
            lossy: false,
        })
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

    /// The derivation of an output as a final-state fact: under replay, the deliveries
    /// without which the node's *last* output on the port differs; otherwise as
    /// [`Self::derive`].
    fn derive_final(&self, output: &OutputRecord) -> Derivation {
        let by_replay = match self.level {
            Level::Counterfactual => true,
            Level::Record => is_opaque(&output.prov),
            Level::Execution | Level::Slice => false,
        };
        if !by_replay {
            return self.derive(output);
        }
        let fallback = || {
            if is_opaque(&output.prov) {
                self.closure(output.exec, Some(output.sink))
            } else {
                self.record_closure(&output.prov, output.exec, Some(output.sink))
            }
        };
        let Some(node) = self.exec_node(output.exec) else {
            return fallback();
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
        self.counterfactual(&node, output.exec, &produced, fallback)
    }

    /// Every pre-EFF delivery through a registered channel, as one derivation.
    fn all_pre_eff_deliveries(&self) -> Derivation {
        #[expect(
            clippy::disallowed_methods,
            reason = "collected into an ordered set; iteration order is irrelevant"
        )]
        self.released
            .values()
            .flatten()
            .filter(|release| release.pre_eff)
            .map(|release| self.dependency(release))
            .collect()
    }

    /// Every delivery some fault could prevent (omission only before EFF), regardless of
    /// whether the goal depended on it.
    fn all_deliveries(&self) -> Derivation {
        #[expect(
            clippy::disallowed_methods,
            reason = "collected into an ordered set; iteration order is irrelevant"
        )]
        self.released
            .values()
            .flatten()
            .filter_map(|release| {
                let mut dependency = self.dependency(release);
                if !release.pre_eff {
                    dependency.lossy = false;
                }
                (dependency.lossy || dependency.sender.is_some()).then_some(dependency)
            })
            .collect()
    }

    fn dependency(&self, release: &ReleaseRecord) -> Dependency {
        let fault_point = &self.search.points[release.point];
        Dependency {
            message: fault_point.decode(&release.payload),
            sender: release.sender.clone(),
            lossy: fault_point.lossy(),
        }
    }

    /// The backward closure of what execution `root` produced through sink `sink` (`None`:
    /// everything it produced). Each visited `(execution, sink)` contributes the channel
    /// deliveries that sink's static slice names (see [`crate::sim::slice`]), or every
    /// delivery into the execution and its predecessors when the slice is unknown.
    fn closure(&self, root: ExecId, sink: Option<usize>) -> Derivation {
        let mut derivation = Derivation::new();
        let mut visited = BTreeSet::new();
        let mut stack = vec![(root, sink)];
        while let Some((exec, sink)) = stack.pop() {
            if !visited.insert((exec, sink)) {
                continue;
            }
            let Some((location, member, is_tick)) = self.execs.get(&exec) else {
                continue;
            };
            let slice = sink.and_then(|s| self.slices.precise(s));
            let node = (location.clone(), *member);

            // A release is a dependency (a fault could prevent it), and leads to the
            // execution that sent it.
            let take_release =
                |release: &ReleaseRecord,
                 derivation: &mut Derivation,
                 stack: &mut Vec<(ExecId, Option<usize>)>| {
                    let mut dependency = self.dependency(release);
                    // A message delivered after EFF cannot be omitted; its sender's crash is
                    // still a candidate.
                    if !release.pre_eff {
                        dependency.lossy = false;
                    }
                    if dependency.lossy || dependency.sender.is_some() {
                        derivation.insert(dependency);
                    }
                    match release.producer {
                        Some((producer, channel)) => stack.push((producer, Some(channel))),
                        None => {
                            // Not a channel delivery: output of a local tick, batched.
                            for tick in self
                                .slices
                                .hook_yields
                                .get(&release.hook_id)
                                .into_iter()
                                .flatten()
                            {
                                self.push_ticks_before(tick, *member, exec, None, None, stack);
                            }
                        }
                    }
                };

            // A top-level receive on an unregistered channel is not a fault point, but the
            // send that produced it can be killed by its sender's crash, so it is a
            // dependency on that send (and leads to the sending execution).
            let take_recv = |recv: &RecvRecord,
                             derivation: &mut Derivation,
                             stack: &mut Vec<(ExecId, Option<usize>)>| {
                let Some(i) = recv.producer else {
                    return;
                };
                let record = &self.sent[i];
                if record.sender.is_some() {
                    derivation.insert(self.send_dependency(record));
                }
                stack.push((record.exec, Some(record.channel)));
            };

            if *is_tick {
                if let Some(slice) = slice {
                    let direct = |hook: usize| {
                        slice.direct.contains(&SliceInput::Hook(hook))
                            || slice.carried.contains(&SliceInput::Hook(hook))
                    };
                    let carried = |hook: usize| slice.carried.contains(&SliceInput::Hook(hook));
                    for release in self.released.get(&exec).into_iter().flatten() {
                        if direct(release.hook_id) {
                            take_release(release, &mut derivation, &mut stack);
                        }
                    }
                    // State carried from earlier executions of this tick.
                    if !slice.carried.is_empty() {
                        for earlier in self.ticks_at.get(&node).into_iter().flatten() {
                            if *earlier >= exec {
                                break;
                            }
                            for release in self.released.get(earlier).into_iter().flatten() {
                                if carried(release.hook_id) {
                                    take_release(release, &mut derivation, &mut stack);
                                }
                            }
                        }
                    }
                } else {
                    for release in self.released.get(&exec).into_iter().flatten() {
                        take_release(release, &mut derivation, &mut stack);
                    }
                    // Persisted state: earlier executions of the same tick.
                    if let Some(ticks) = self.ticks_at.get(&node) {
                        stack.extend(
                            ticks
                                .iter()
                                .copied()
                                .filter(|t| *t < exec)
                                .map(|t| (t, None)),
                        );
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
                if let Some(slice) = slice {
                    let wants = |channel: usize, inputs: &BTreeSet<SliceInput>| {
                        inputs.contains(&SliceInput::Channel(channel))
                    };
                    for recv in self.recvs.get(&exec).into_iter().flatten() {
                        if wants(recv.channel, &slice.direct) || wants(recv.channel, &slice.carried)
                        {
                            take_recv(recv, &mut derivation, &mut stack);
                        }
                    }
                    if !slice.carried.is_empty() {
                        for earlier in self.asyncs_at.get(&node).into_iter().flatten() {
                            if *earlier >= exec {
                                break;
                            }
                            for recv in self.recvs.get(earlier).into_iter().flatten() {
                                if wants(recv.channel, &slice.carried) {
                                    take_recv(recv, &mut derivation, &mut stack);
                                }
                            }
                        }
                    }
                    // The ticks whose yields this run consumed; the same slice applies
                    // inside them.
                    for input in slice.direct.iter().chain(&slice.carried) {
                        if let SliceInput::Yield(tick) = input {
                            self.push_ticks_before(tick, *member, exec, previous, sink, &mut stack);
                        }
                    }
                } else {
                    for recv in self.recvs.get(&exec).into_iter().flatten() {
                        take_recv(recv, &mut derivation, &mut stack);
                    }
                    // An async run consumes what the location's ticks yielded since its
                    // previous run.
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "pushes onto a stack whose order does not affect the closure"
                    )]
                    for (tick_location, tick_member) in self.ticks_at.keys() {
                        if tick_location.root() == location && tick_member == member {
                            self.push_ticks_before(
                                tick_location,
                                *member,
                                exec,
                                previous,
                                None,
                                &mut stack,
                            );
                        }
                    }
                }
            }
        }
        derivation
    }

    /// Pushes the executions of tick `tick` at `member` before `exec` (and after `since`, if
    /// given), to be explored through `sink`'s slice (`None`: their whole input is assumed
    /// relevant). A yield can take more than one async run to reach a sink (an `atomic`
    /// boundary adds a hop), so an empty window widens to the latest execution before
    /// `exec`: an over-approximation, like the rule itself.
    fn push_ticks_before(
        &self,
        tick: &LocationId,
        member: Option<u32>,
        exec: ExecId,
        since: Option<ExecId>,
        sink: Option<usize>,
        stack: &mut Vec<(ExecId, Option<usize>)>,
    ) {
        let mut pushed = false;
        let mut latest: Option<ExecId> = None;
        for t in self
            .ticks_at
            .get(&(tick.clone(), member))
            .into_iter()
            .flatten()
        {
            if *t < exec {
                latest = Some(*t);
                if since.is_none_or(|s| *t > s) {
                    stack.push((*t, sink));
                    pushed = true;
                }
            }
        }
        if !pushed && let Some(t) = latest {
            stack.push((t, sink));
        }
    }
}

/// Whether a record's provenance is opaque (it came out of a closure the rewrite cannot see
/// into; see `Prov::OPAQUE`).
fn is_opaque(prov: &[u64]) -> bool {
    prov.contains(&crate::sim::runtime::Prov::OPAQUE)
}

/// Whether a payload released at a receiving hook is the serialized message `sent`. A
/// cluster-sourced channel delivers `(MemberId, T)`, so the received bytes carry the
/// serialized sender member id as a prefix.
fn payload_matches(received: &[u8], sent: &[u8], from_member: Option<u32>) -> bool {
    received == sent
        || from_member.is_some_and(|m| {
            let prefix = bincode::serialize(&TaglessMemberId::from_raw_id(m)).unwrap();
            received.len() == sent.len() + prefix.len()
                && received[..prefix.len()] == prefix[..]
                && received[prefix.len()..] == *sent
        })
}

/// A human-readable summary of every sink's slice, with hooks named by their registered
/// labels where possible.
fn describe_slices(slices: &Slices, search: &Search) -> String {
    let hook_label = |hook: usize| -> String {
        search
            .points
            .iter()
            .find(|p| p.hook_key().0 == hook)
            .map(|p| p.label().to_owned())
            .unwrap_or_else(|| format!("hook#{hook}"))
    };
    let render = |inputs: &BTreeSet<SliceInput>| -> String {
        inputs
            .iter()
            .map(|input| match input {
                SliceInput::Hook(h) => hook_label(*h),
                SliceInput::Channel(c) => format!("channel#{c}"),
                SliceInput::Yield(t) => format!("yield({t:?})"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut lines: Vec<String> = vec![];
    #[expect(
        clippy::disallowed_methods,
        reason = "collected into an ordered map; iteration order is irrelevant"
    )]
    let sinks: BTreeMap<&usize, &SinkSlice> = slices.sinks.iter().collect();
    for (id, slice) in sinks {
        let kind = if slices.sink_in_tick.get(id).copied().unwrap_or(false) {
            "tick"
        } else {
            "async"
        };
        lines.push(format!(
            "  sink#{id} ({kind}){}: direct {{{}}} carried {{{}}}",
            if slice.opaque { " OPAQUE" } else { "" },
            render(&slice.direct),
            render(&slice.carried),
        ));
    }
    lines.join("\n")
}

fn member_label(cluster: &str, member: u32) -> String {
    format!("{cluster}@{member}")
}

/// The counterfactual replay oracle ([`Level::Counterfactual`]): runs one node alone, every
/// fault point under a replay policy, in a quiet deterministic instance, and returns what
/// the instance recorded.
struct Replayer<'a> {
    sim: &'a CompiledSim,
    search: &'a Search,
    log: bool,
    tag: &'static str,
}

impl Replayer<'_> {
    /// Replays with `scripts[p]` (serialized items per tick execution) installed on fault
    /// point `p`; an empty script keeps that point's tick from ever running.
    fn replay(&self, scripts: &[Vec<Vec<Vec<u8>>>]) -> Vec<LineageEvent> {
        let mut events: Vec<LineageEvent> = vec![];
        let out = &mut events;
        let points = &self.search.points;
        self.sim.replay(async || {
            for (point, script) in points.iter().zip(scripts) {
                point.replay(script.clone());
            }
            crate::sim::compiled::quiesce().await;
            *out = lineage_events();
        });
        events
    }
}

/// The registered fault points, crashable nodes, and goal.
struct Search {
    points: Vec<Box<dyn FaultPoint>>,
    /// Node label → node.
    nodes: BTreeMap<String, NodeKey>,
    goal: Option<Goal>,
}

/// Which lineage the backward closure follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// Level 0: every output of an execution depends on every input of it and of every
    /// earlier execution of the same tick.
    Execution,
    /// Level 1 (default): as above, narrowed by the static slice of the producing sink.
    Slice,
    /// Level 2: record-level provenance (requires `SimFlow::with_provenance`); falls back
    /// to Level 1 where a record carries none.
    Record,
    /// Level 3: counterfactual lineage by replay. Each node is re-executed alone, with its
    /// recorded deliveries held fixed, once per candidate delivery removed; a delivery is in
    /// the derivation iff the node stops producing the output without it (a 1-minimal
    /// sufficient set, latest deliveries removed first). Sees through closures and state the
    /// static slice and the provenance rewrite cannot. Falls back to Level 2 (or 1) where a
    /// node does not reproduce its output under replay with everything kept.
    Counterfactual,
}

/// An LDFI search configuration: the fault points, the crashable nodes, the goal, and the
/// budgets.
pub struct Ldfi {
    search: Search,
    strategy: Strategy,
    exploration: Exploration,
    level: Level,
    crash_budget: usize,
    max_hypotheses: usize,
    log: bool,
}

impl Default for Ldfi {
    fn default() -> Self {
        Self::new()
    }
}

impl Ldfi {
    /// A search with no fault points, no crashable nodes, a crash budget of 0, and a budget
    /// of 256 hypotheses.
    pub fn new() -> Self {
        Ldfi {
            search: Search {
                points: vec![],
                nodes: BTreeMap::new(),
                goal: None,
            },
            strategy: Strategy::Lineage,
            exploration: Exploration::Exhaustive,
            level: Level::Slice,
            crash_budget: 0,
            max_hypotheses: 256,
            log: true,
        }
    }

    /// Selects how hypotheses are chosen (default [`Strategy::Lineage`]).
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Selects which lineage the backward closure follows (default [`Level::Slice`]).
    #[must_use]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Reads the lineage level from `LDFI_LEVEL` (`exec`, `slice`, `record`, `replay`) if
    /// set; for experiments that compare levels without recompiling the test.
    #[must_use]
    pub fn level_from_env(self) -> Self {
        match std::env::var("LDFI_LEVEL").as_deref() {
            Ok("exec") => self.level(Level::Execution),
            Ok("slice") => self.level(Level::Slice),
            Ok("record") => self.level(Level::Record),
            Ok("replay" | "counterfactual") => self.level(Level::Counterfactual),
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

    fn add_point<T, H>(&mut self, label: &str, hook: H, lossy: bool)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug + 'static,
        H: PolicyHook<T> + 'static,
    {
        assert!(
            self.search.points.iter().all(|p| p.label() != label),
            "duplicate LDFI channel label {label:?}"
        );
        self.search.points.push(Box::new(ChannelPoint {
            label: label.to_owned(),
            hook,
            lossy,
            _phantom: std::marker::PhantomData,
        }));
    }

    /// Registers the receiving hook of a lossy channel as a fault point under `label`:
    /// its messages can be omitted. The hook must be bound to the `batch` that consumes the
    /// channel's output; for a hook on a cluster, register each member's instance
    /// separately with `hook.on(member)`.
    #[must_use]
    pub fn lossy_channel<T, R, S>(mut self, label: &str, hook: BatchHook<T, NoOrder, R, S>) -> Self
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug + 'static,
        R: Retries + 'static,
        S: ScriptableHookScope + 'static,
    {
        self.add_point(label, hook, true);
        self
    }

    /// Registers the receiving hook of a reliable channel under `label`: its deliveries
    /// are traced for lineage but never omitted; the only faults that can kill them are
    /// crashes of their senders. Accepts ordered hooks (a `fail_stop` channel's output) and
    /// unordered ones (e.g. the `entries()` of a keyed stream from a reliable channel).
    #[must_use]
    pub fn reliable_channel<T, O, R, S>(mut self, label: &str, hook: BatchHook<T, O, R, S>) -> Self
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug + 'static,
        O: Ordering + 'static,
        R: Retries + 'static,
        S: ScriptableHookScope + 'static,
        BatchHook<T, O, R, S>: PolicyHook<T>,
    {
        self.add_point(label, hook, false);
        self
    }

    /// Registers the hook on a batch of external input (a timer, a client request stream) so
    /// it is consumed deterministically as it arrives, keeping the schedule explorer focused
    /// on the program's own non-determinism. Such input is not a fault point: it has no
    /// sender in the system, so it never enters a derivation.
    #[must_use]
    pub fn input<T, O, R, S>(self, label: &str, hook: BatchHook<T, O, R, S>) -> Self
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug + 'static,
        O: Ordering + 'static,
        R: Retries + 'static,
        S: ScriptableHookScope + 'static,
        BatchHook<T, O, R, S>: PolicyHook<T>,
    {
        self.reliable_channel(label, hook)
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

    /// Runs the search. `body` is the test body for one run: it drives inputs and time,
    /// may call [`Run::end_of_finite_failures`], and returns whether the goal held. It is
    /// run under [`CompiledSim::exhaustive`] for every hypothesis, starting with no faults.
    pub fn run(
        self,
        sim: &CompiledSim,
        body: impl AsyncFn(&Run<'_>) -> bool + RefUnwindSafe,
    ) -> Outcome {
        let search = &self.search;
        let goal_port = match &search.goal {
            Some(Goal::Any { port } | Goal::AllMembers { port, .. }) => Some(*port),
            Some(Goal::Witnessed) | None => None,
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
        let tag = match (self.strategy, self.level) {
            (Strategy::Blind, _) => "blind",
            (Strategy::Lineage, Level::Execution) => "ldfi/L0",
            (Strategy::Lineage, Level::Slice) => "ldfi",
            (Strategy::Lineage, Level::Record) => "ldfi/L2",
            (Strategy::Lineage, Level::Counterfactual) => "ldfi/L3",
        };
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
                        level: self.level,
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
            tried.insert(faults.clone());
            if !records.is_empty() && records.iter().all(|r| r.vacuous) {
                vacuous_sets.push(faults.clone());
            }
            // Derivations are computed outside the instance: counterfactual replays are
            // instances of their own.
            let replayer = Replayer {
                sim,
                search,
                log: self.log,
                tag,
            };
            let schedules: Vec<Schedule> = records
                .iter()
                .map(|record| record.schedule(search, Some(&replayer)))
                .collect();

            let failing = schedules.iter().find(|s| !s.goal_held).cloned();
            let new_derivations: BTreeSet<Derivation> = schedules
                .iter()
                .filter(|s| s.goal_held)
                .flat_map(|s| s.derivations.iter().cloned())
                .collect();
            // The crashes of this hypothesis: premises of every clause it yields.
            let premises: BTreeSet<Fault> = faults
                .iter()
                .filter(|f| matches!(f, Fault::Crash { .. }))
                .cloned()
                .collect();
            for schedule in &schedules {
                observed.extend(
                    schedule
                        .observed
                        .iter()
                        .flat_map(|dep| dep.candidate_faults(&schedule.sends)),
                );
                if !schedule.requirements.is_empty()
                    && !required
                        .iter()
                        .any(|c| c.deps == schedule.requirements && c.premises == premises)
                {
                    required.push(Clause {
                        deps: schedule.requirements.clone(),
                        sends: schedule.sends.clone(),
                        premises: premises.clone(),
                    });
                }
                for dep in &schedule.requirements {
                    if let Some((node, ordinal)) = &dep.sender
                        && let Some(send) = schedule
                            .sends
                            .get(node)
                            .and_then(|seq| seq.get(*ordinal as usize))
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
                    for (at, send) in sends.iter().enumerate() {
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
                eprintln!("{tag}: static slices:\n{}", describe_slices(slices, search));
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
                        },
                        Strategy::Blind => Outcome::Exhausted { runs },
                    };
                }
            }
        }
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
        let goal_port = match &search.goal {
            Some(Goal::Any { port } | Goal::AllMembers { port, .. }) => Some(*port),
            Some(Goal::Witnessed) | None => None,
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
                    level: self.level,
                };
                run.install();
                let goal_held = body_ref(&run).await;
                if run.vacuous.get() {
                    *vacuous_ref += 1;
                }
                if !goal_held {
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

    fn lossy(channel: &str, payload: u32) -> Dependency {
        Dependency {
            message: Message {
                channel: channel.to_owned(),
                payload: bincode::serialize(&payload).unwrap(),
                display: payload.to_string(),
            },
            sender: None,
            lossy: true,
        }
    }

    fn reliable(channel: &str, payload: u32, sender: &str, ordinal: u64) -> Dependency {
        Dependency {
            message: Message {
                channel: channel.to_owned(),
                payload: bincode::serialize(&payload).unwrap(),
                display: payload.to_string(),
            },
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

    fn clause(deps: impl IntoIterator<Item = Dependency>, sends: &Rc<SendSequences>) -> Clause {
        Clause {
            deps: deps.into_iter().collect(),
            sends: sends.clone(),
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
            .map(|d| d.killed_by(&later, &sends))
            .collect();
        assert_eq!(killed, [false, true]);
        let earlier = Fault::Crash {
            node: "s".to_owned(),
            after: None,
        };
        assert!(
            clauses[0]
                .deps
                .iter()
                .all(|d| d.killed_by(&earlier, &sends))
        );
        // In a run where `m0` was never sent, a crash after it never fires.
        let other_run = Rc::new(SendSequences::from([("s".to_owned(), vec![m1])]));
        assert!(!reliable("to@1", 42, "s", 0).killed_by(&later, &other_run));
    }
}
