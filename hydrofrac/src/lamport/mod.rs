//! Lamport (spacetime) diagrams of a counterexample, as Graphviz DOT
//! (docs/design/hydrofrac-lite.md §4.7).
//!
//! [`trace_with`] reduces one instance's [`EventLog`] to a [`Trace`]; [`dot`] draws it with one
//! lane per node and time flowing down, a message as an edge from its send to its receive. So a
//! message sent before its sender crashed and received afterwards starts above the crash mark,
//! which a sequence diagram (Mermaid) cannot show. Omitted messages end in a red `✗`, sends lost
//! to a crash in a gray one, crashes are `☠ crashed` marks and the heal a dashed rule across the
//! lanes. Labels longer than 40 characters are cut, with their full text in a legend.
//! [`write()`] writes `<name>.dot`, plus `<name>.svg` when Graphviz is installed.
//!
//! A counterexample's diagram shows only what the broken fact depended on ([`focus`]): its
//! sends and their lineage, the faults and the heal, with a caption counting what it left
//! out. The whole run is drawn beside it, as `counterexample-full`.

mod dot;
mod focus;
mod out;
// Two attributes, not `cfg(all(..))`: stageleft keeps the module out of the staged copy only on an
// exact `#[cfg(stageleft_runtime)]`, and the copy cannot call `Log`'s methods.
#[cfg(test)]
#[cfg(stageleft_runtime)]
mod tests;

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;

use hydro_lang::sim::network_hook::ChannelInfo;

pub use self::dot::dot;
pub use self::focus::focus;
pub use self::out::{Written, claim, default_dir, sanitize, test_name, write};
use crate::fault::{DeliveryId, Event, EventLog, Fate, MessageId, NodeId, NodeNames};
use crate::search::{CrashAt, Fault, FaultSet, Node};

/// One lane: a process, or one member of a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceNode {
    /// The node's name, as [`NodeNames`] assigns it.
    pub label: Node,
    pub id: NodeId,
    /// The index into [`Trace::events`] of the node's crash, if one fired.
    pub crash: Option<usize>,
}

/// What happened at one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEventKind {
    /// The node sent `messages[i]`.
    Send(usize),
    /// The node received `messages[i]`.
    Recv(usize),
    /// The node emitted `display` on external output `port`.
    Output { port: usize, display: String },
    /// A crash of the plan fired at the node.
    Crash,
    /// The network healed (no node).
    Heal,
}

/// One event; `events[i]` happened before `events[j]` when `i < j`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEvent {
    /// The index into [`Trace::nodes`] of where it happened (`None` for the heal).
    pub node: Option<usize>,
    pub kind: TraceEventKind,
}

/// One message: its send and, if it was received, its receive (indices into [`Trace::events`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceMessage {
    pub msg: MessageId,
    pub id: DeliveryId,
    /// The channel's name, or `c<id>`.
    pub channel: String,
    /// The payload, rendered by [`Labels`].
    pub display: String,
    /// Sender and destination, as indices into [`Trace::nodes`].
    pub from: usize,
    pub to: usize,
    pub send_event: usize,
    pub recv_event: Option<usize>,
    pub fate: Fate,
}

/// One run, ready to draw.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    /// Every node that appears in the run or that the plan crashes, in node order.
    pub nodes: Vec<TraceNode>,
    pub events: Vec<TraceEvent>,
    /// Every message, in send order.
    pub messages: Vec<TraceMessage>,
    /// The faults of the plan, rendered.
    pub faults: Vec<String>,
    /// The crashes of the plan that never fired.
    pub unfired: Vec<String>,
    /// Notes at the bottom of lanes (index into [`Self::nodes`], text): what a member that the
    /// broken fact owed a record ends without.
    pub notes: Vec<(usize, String)>,
}

impl Trace {
    /// The index of the node called `label`, if it has a lane.
    pub fn node(&self, label: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.label == label)
    }
}

type ChannelName<'a> = dyn Fn(usize) -> Option<String> + 'a;
type Payload<'a> = dyn Fn(&MessageId) -> String + 'a;
type OutputValue<'a> = dyn Fn(usize, &[u8]) -> String + 'a;

/// How a [`Trace`] renders channels, payloads and outputs: by default `c<id>` and
/// [`short_hex`].
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
    /// Names channels as the report does ([`crate::report::channel_name`]): the name the
    /// program gave them, else the variable the sending statement binds, else its `file:line`.
    #[must_use]
    pub fn with_channels(mut self, channels: &[ChannelInfo]) -> Self {
        let names: HashMap<usize, String> = channels
            .iter()
            .map(|c| (c.id, crate::report::channel_name(c)))
            .collect();
        self.channel = Box::new(move |id| names.get(&id).cloned());
        self
    }

    /// Renders message payloads with `f`.
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

/// The first 8 bytes of `bytes` in hex, with `…` when there are more; `()` for none.
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

/// [`trace_with`] default [`Labels`].
pub fn trace(log: &EventLog, names: &NodeNames, faults: &FaultSet<MessageId>) -> Trace {
    trace_with(log, names, faults, &Labels::default())
}

/// The trace of the run `log` recorded under the plan `faults`, rendered with `labels`.
pub fn trace_with(
    log: &EventLog,
    names: &NodeNames,
    faults: &FaultSet<MessageId>,
    labels: &Labels<'_>,
) -> Trace {
    trace_with_notes(log, names, faults, labels, &[])
}

/// [`trace_with`], with `notes` (node, text) at the bottom of their nodes' lanes. A noted node
/// gets a lane even if nothing happened at it.
pub fn trace_with_notes(
    log: &EventLog,
    names: &NodeNames,
    faults: &FaultSet<MessageId>,
    labels: &Labels<'_>,
    notes: &[(NodeId, String)],
) -> Trace {
    let mut ids: BTreeSet<NodeId> = faults
        .iter()
        .filter_map(|f| f.crashed_node().and_then(|n| names.id(n)).cloned())
        .chain(notes.iter().map(|(id, _)| id.clone()))
        .collect();
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
            Event::ExecEnd | Event::Heal => {}
        }
    }
    let mut nodes: Vec<TraceNode> = ids
        .into_iter()
        .map(|id| TraceNode {
            label: names.name(&id),
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
            Event::Exec { .. } | Event::ExecEnd => continue,
            Event::Sent { id, msg, fate, .. } => {
                by_id.insert(*id, messages.len());
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
                (
                    Some(lane[&msg.from]),
                    TraceEventKind::Send(messages.len() - 1),
                )
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
                nodes[lane[node]].crash.get_or_insert(at);
                (Some(lane[node]), TraceEventKind::Crash)
            }
            Event::Heal => (None, TraceEventKind::Heal),
        };
        events.push(TraceEvent { node, kind });
    }

    let message = |m: &MessageId, from: bool| {
        let from = if from {
            names.name(&m.from)
        } else {
            String::new()
        };
        let to = names.name(&m.to);
        format!(
            "{}: {from}→{to} {}",
            labels.channel(m.channel),
            (labels.payload)(m)
        )
    };
    let describe = |fault: &Fault<MessageId>| match fault {
        Fault::Omit(m) => format!("omit({})", message(m, true)),
        Fault::Crash {
            node,
            at: CrashAt::BeforeAnySend,
        } => format!("crash({node})"),
        Fault::Crash {
            node,
            at: CrashAt::After(m),
        } => format!("crash({node} after {})", message(m, false)),
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
        notes: notes
            .iter()
            .map(|(id, text)| (lane[id], text.clone()))
            .collect(),
        nodes,
        events,
        messages,
    }
}
