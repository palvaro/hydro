//! Lamport (spacetime) diagrams of a run, as Graphviz DOT (docs/design/hydrofrac-lite.md §4.6).
//!
//! [`trace`] reduces one instance's [`EventLog`] to a [`Trace`]: the nodes, the events in program
//! order, and every message with its send, its [`Fate`] and, if it was received, its receive.
//! [`dot`] draws the trace with one lane per node and time flowing down. A message is an edge
//! from the instant it was sent to the instant it was received, so a message sent before its
//! sender crashed and received afterwards starts above the crash mark. That is the reason a
//! sequence diagram (Mermaid) was rejected: it draws a message as one horizontal arrow and so
//! shows such a message as if the dead node sent it. [`write()`] writes `<name>.dot`, plus
//! `<name>.svg` when Graphviz is installed; [`default_dir`] is the conventional
//! `target/hydrofrac/<test>/<label>/`.
//!
//! What the diagram shows:
//! - omitted messages as red dashed edges ending in `✗` on the receiver's lane, and sends lost
//!   because the sender had crashed as gray ones;
//! - messages still in flight when the run ended as dotted edges to the bottom of the lane;
//! - crashes as `☠ crashed` marks that end the lane, and the heal (§4.6) as a dashed rule across
//!   all lanes;
//! - external outputs (observations) as notes beside their lane;
//! - the faults of the hypothesis, and any crash that never fired, in the title.
//!
//! Three fixes for legibility (§4.6):
//! - payloads and outputs are rendered by caller-supplied decoders ([`Labels`]), with a short
//!   hex prefix as the fallback;
//! - a label longer than [`Options::max_label`] is cut, and its full text goes to a numbered
//!   legend under the diagram;
//! - consecutive identical rounds (the same messages, with the same fates, again and again) are
//!   drawn once, with a `×k` bracket beside them ([`Options::collapse_rounds`]).

mod dot;
mod out;
#[cfg(all(test, stageleft_runtime))]
mod tests;

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;

use hydro_lang::sim::network_hook::ChannelInfo;

pub use self::dot::{Options, dot};
pub use self::out::{Written, claim, default_dir, sanitize, target_dir, test_name, write};
use crate::fault::{DeliveryId, Event, EventLog, Fate, MessageId, NodeId, NodeNames};
use crate::search::{CrashAt, Fault, FaultSet, Node};

/// One lane: a process, or one member of a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceNode {
    /// The node's name (`label` or `label@member`, as [`NodeNames`] assigns it).
    pub label: Node,
    /// The node.
    pub id: NodeId,
    /// Whether the node belongs to an eager location (an observer), which is never faulty and
    /// is hidden by default.
    pub eager: bool,
    /// The index into [`Trace::events`] of the node's crash, if a crash of the plan fired.
    pub crash: Option<usize>,
}

/// What happened at one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEventKind {
    /// An execution started at the node (not drawn; kept for reference).
    Exec,
    /// The node sent message `messages[i]`.
    Send(usize),
    /// The node received message `messages[i]`.
    Recv(usize),
    /// The node emitted a value on an external output.
    Output {
        /// The external port.
        port: usize,
        /// The value, rendered by [`Labels`].
        display: String,
    },
    /// A crash of the plan fired at the node: it sends and emits nothing more.
    Crash,
    /// The network healed (no node): no omission applies and no crash fires from here on.
    Heal,
}

/// One event, in program order: `events[i]` happened before `events[j]` when `i < j`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEvent {
    /// The index into [`Trace::nodes`] of where it happened (`None` for [`TraceEventKind::Heal`]).
    pub node: Option<usize>,
    /// What happened.
    pub kind: TraceEventKind,
}

/// One message: its send and, if it was received before the run ended, its receive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceMessage {
    /// Its identity (the search's message type).
    pub msg: MessageId,
    /// Its delivery id in this run.
    pub id: DeliveryId,
    /// The channel's name, or `c<id>` when the program did not name it.
    pub channel: String,
    /// The payload, rendered by [`Labels`].
    pub display: String,
    /// The index into [`Trace::nodes`] of the sender.
    pub from: usize,
    /// The index into [`Trace::nodes`] of the destination.
    pub to: usize,
    /// The index into [`Trace::events`] of the send.
    pub send_event: usize,
    /// The index into [`Trace::events`] of the receive, if the destination received it.
    pub recv_event: Option<usize>,
    /// What happened to the send.
    pub fate: Fate,
}

/// One run, ready to draw.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    /// The lanes: every node that appears in the run or that the plan crashes, in node order
    /// (location, then member).
    pub nodes: Vec<TraceNode>,
    /// Every event, in program order.
    pub events: Vec<TraceEvent>,
    /// Every message, in send order.
    pub messages: Vec<TraceMessage>,
    /// The faults of the plan, rendered.
    pub faults: Vec<String>,
    /// The crashes of the plan that never fired (their crash point was never reached).
    pub unfired: Vec<String>,
}

impl Trace {
    /// The index of the node called `label`, if it has a lane.
    pub fn node(&self, label: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.label == label)
    }

    /// The index into [`Self::events`] of the heal, if the run healed.
    pub fn heal_event(&self) -> Option<usize> {
        self.events
            .iter()
            .position(|e| e.kind == TraceEventKind::Heal)
    }

    /// Whether message `m` was delivered but not yet received when the run ended.
    pub fn in_flight(&self, m: usize) -> bool {
        let m = &self.messages[m];
        m.fate == Fate::Delivered && m.recv_event.is_none()
    }

    /// The index into [`Self::messages`] of the message with delivery id `id`.
    pub fn message(&self, id: DeliveryId) -> Option<usize> {
        self.messages.iter().position(|m| m.id == id)
    }
}

type ChannelName<'a> = dyn Fn(usize) -> Option<String> + 'a;
type Payload<'a> = dyn Fn(&MessageId) -> String + 'a;
type OutputValue<'a> = dyn Fn(usize, &[u8]) -> String + 'a;

/// How a [`Trace`] renders channels, payloads and outputs. By default channels are `c<id>` and
/// payloads and outputs are a short hex prefix of their bytes ([`short_hex`]).
pub struct Labels<'a> {
    channel: Box<ChannelName<'a>>,
    payload: Box<Payload<'a>>,
    output: Box<OutputValue<'a>>,
}

impl Default for Labels<'_> {
    fn default() -> Self {
        Labels {
            channel: Box::new(|_| None),
            payload: Box::new(|m| short_hex(&m.bytes)),
            output: Box::new(|_, bytes| short_hex(bytes)),
        }
    }
}

impl<'a> Labels<'a> {
    /// Names channels as the program named them (`ChannelInfo::name`).
    #[must_use]
    pub fn with_channels(mut self, channels: &[ChannelInfo]) -> Self {
        let names: HashMap<usize, String> = channels
            .iter()
            .filter_map(|c| Some((c.id, c.name.clone()?)))
            .collect();
        self.channel = Box::new(move |id| names.get(&id).cloned());
        self
    }

    /// Names channels with `f` (`None`: `c<id>`).
    #[must_use]
    pub fn channel_names(mut self, f: impl Fn(usize) -> Option<String> + 'a) -> Self {
        self.channel = Box::new(f);
        self
    }

    /// Renders message payloads with `f` (for instance, `Debug` of the decoded value).
    #[must_use]
    pub fn payloads(mut self, f: impl Fn(&MessageId) -> String + 'a) -> Self {
        self.payload = Box::new(f);
        self
    }

    /// Renders external outputs (port, bytes) with `f`.
    #[must_use]
    pub fn outputs(mut self, f: impl Fn(usize, &[u8]) -> String + 'a) -> Self {
        self.output = Box::new(f);
        self
    }

    fn channel(&self, id: usize) -> String {
        (self.channel)(id).unwrap_or_else(|| format!("c{id}"))
    }
}

/// The first 8 bytes of `bytes` in hex, with `…` when there are more.
pub fn short_hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for b in bytes.iter().take(8) {
        let _ = write!(out, "{b:02x}");
    }
    if bytes.len() > 8 {
        out.push('…');
    }
    if out.is_empty() {
        out.push_str("()");
    }
    out
}

/// The trace of one run with default [`Labels`]; see [`trace_with`].
pub fn trace(log: &EventLog, names: &NodeNames, faults: &FaultSet<MessageId>) -> Trace {
    trace_with(log, names, faults, &Labels::default())
}

/// The trace of the run `log` recorded, under the plan `faults`, rendered with `labels`.
pub fn trace_with(
    log: &EventLog,
    names: &NodeNames,
    faults: &FaultSet<MessageId>,
    labels: &Labels<'_>,
) -> Trace {
    let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
    let mut ids: BTreeSet<NodeId> = BTreeSet::new();
    for event in log.events() {
        match event {
            Event::Exec { node, .. } | Event::Output { node, .. } | Event::Halted { node } => {
                ids.insert(node.clone());
            }
            Event::Sent { msg, .. } => {
                ids.insert(msg.from.clone());
                ids.insert(msg.to.clone());
            }
            Event::Recv { to, .. } => {
                ids.insert(to.clone());
            }
            Event::ExecEnd { .. } | Event::Heal => {}
        }
    }
    for fault in faults {
        if let Some(id) = fault.crashed_node().and_then(|n| names.id(n)) {
            ids.insert(id.clone());
        }
    }
    let mut nodes: Vec<TraceNode> = ids
        .into_iter()
        .map(|id| TraceNode {
            label: names.name(&id),
            eager: !crashable.contains(&id),
            crash: None,
            id,
        })
        .collect();
    let lane: HashMap<NodeId, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.clone(), i))
        .collect();

    let mut events: Vec<TraceEvent> = vec![];
    let mut messages: Vec<TraceMessage> = vec![];
    let mut by_id: HashMap<DeliveryId, usize> = HashMap::new();
    for event in log.events() {
        let at = events.len();
        let (node, kind) = match event {
            Event::Exec { node, .. } => (Some(lane[node]), TraceEventKind::Exec),
            Event::ExecEnd { .. } => continue,
            Event::Sent { id, msg, fate, .. } => {
                let m = messages.len();
                messages.push(TraceMessage {
                    msg: msg.clone(),
                    id: *id,
                    channel: labels.channel(msg.channel),
                    display: (labels.payload)(msg),
                    from: lane[&msg.from],
                    to: lane[&msg.to],
                    send_event: at,
                    recv_event: None,
                    fate: *fate,
                });
                by_id.insert(*id, m);
                (Some(lane[&msg.from]), TraceEventKind::Send(m))
            }
            Event::Recv { id, to, .. } => {
                let Some(&m) = by_id.get(id) else {
                    continue;
                };
                messages[m].recv_event = Some(at);
                (Some(lane[to]), TraceEventKind::Recv(m))
            }
            Event::Output {
                port, node, bytes, ..
            } => (
                Some(lane[node]),
                TraceEventKind::Output {
                    port: *port,
                    display: (labels.output)(*port, bytes),
                },
            ),
            Event::Halted { node } => {
                let n = lane[node];
                nodes[n].crash.get_or_insert(at);
                (Some(n), TraceEventKind::Crash)
            }
            Event::Heal => (None, TraceEventKind::Heal),
        };
        events.push(TraceEvent { node, kind });
    }

    let describe = |fault: &Fault<MessageId>| match fault {
        Fault::Omit(m) => format!(
            "omit({}: {}→{} {})",
            labels.channel(m.channel),
            names.name(&m.from),
            names.name(&m.to),
            (labels.payload)(m)
        ),
        Fault::Crash {
            node,
            at: CrashAt::BeforeAnySend,
        } => format!("crash({node})"),
        Fault::Crash {
            node,
            at: CrashAt::After(m),
        } => format!(
            "crash({node} after {}: →{} {})",
            labels.channel(m.channel),
            names.name(&m.to),
            (labels.payload)(m)
        ),
    };
    let unfired = faults
        .iter()
        .filter(|f| {
            f.crashed_node()
                .and_then(|n| names.id(n))
                .is_some_and(|id| nodes[lane[id]].crash.is_none())
        })
        .map(describe)
        .collect();
    Trace {
        faults: faults.iter().map(describe).collect(),
        unfired,
        nodes,
        events,
        messages,
    }
}
