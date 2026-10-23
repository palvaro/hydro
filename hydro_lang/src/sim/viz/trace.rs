//! A compact, serializable record of one simulated run: its nodes, the ordered events at
//! each, and every message with its send and (if any) receive. This is what the Lamport
//! diagram renderers draw (see [`super::lamport`]), and the one structural artifact of a run
//! that [`Schedule`](crate::sim::ldfi::Schedule) retains beyond the flat sets the search
//! itself needs.
//!
//! A `Trace` is built from the runtime's lineage log once the run has ended, by the search
//! (`hydro_lang::sim::ldfi`). It keeps no payload bytes: a message is identified for
//! matching against a [`Derivation`] by its channel, endpoints, and a hash of its wire bytes
//! ([`payload_hash`]), and rendered by its `display` string.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::sim::ldfi::Derivation;
use crate::sim::runtime::ExecId;

/// One node of the simulated system: a process, or one member of a cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceNode {
    /// A human label: the label the search registered the node under when it is crashable
    /// (`label`, or `label@member`), `observer` for a specification's observer, else the
    /// location's debug rendering with `@member` for a cluster member.
    pub label: String,
    /// The root location's debug rendering (`Process(..)` / `Cluster(..)`).
    pub location: String,
    /// The member id, for a cluster node.
    pub member: Option<u32>,
    /// Whether this is a [`Spec`](crate::sim::spec::Spec)'s observer: its inbound traffic is
    /// the specification's plumbing, not the protocol's messages.
    pub is_observer: bool,
    /// Whether the search registered this node as crashable.
    pub crashable: bool,
    /// Where the node died in this run, if an installed crash took effect.
    pub crash: Option<Crash>,
}

/// Where a node died.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Crash {
    /// Before its first send: dead from the start of the run.
    BeforeAnySend,
    /// Right after the send recorded as event `events[i]` (a [`TraceEventKind::Send`]).
    AfterEvent(usize),
}

/// What the filter did with a send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fate {
    /// Put on the wire. It was received iff [`TraceMessage::recv_event`] is set; otherwise it
    /// was still in flight when the run ended.
    Delivered,
    /// Dropped by an installed omission.
    Omitted,
    /// Dropped because the sender had crashed.
    Crashed,
}

/// One message: a send and, when it was delivered and consumed before the run ended, the
/// receive that took it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceMessage {
    /// The channel's label (see `ChannelInfo::label`).
    pub channel: String,
    /// The channel's sink id.
    pub channel_id: usize,
    /// The payload's rendering for reports.
    pub display: String,
    /// Index into [`Trace::nodes`] of the sender.
    pub from: usize,
    /// Index into [`Trace::nodes`] of the destination.
    pub to: usize,
    /// Index into [`Trace::events`] of the send.
    pub send_event: usize,
    /// Index into [`Trace::events`] of the receive, when the destination consumed the message
    /// before the run ended.
    pub recv_event: Option<usize>,
    /// What the filter did with the send.
    pub fate: Fate,
    /// Whether this is a send to the specification's observer rather than a protocol message.
    pub observation: bool,
    /// Whether the channel admits omission (whether the message was an omission candidate also
    /// depends on it having been sent before [`Trace::eff_cut`]).
    pub lossy: bool,
    /// The delivery id the filter minted for the send.
    pub delivery_id: u64,
    /// The sender's send ordinal (its position among the sender's protocol sends), when the
    /// sender is crashable: the `ordinal` of a [`Dependency`](crate::sim::ldfi::Dependency)
    /// on this message.
    pub ordinal: Option<u64>,
    /// [`payload_hash`] of the wire bytes, for matching against a derivation.
    pub payload_hash: u64,
}

/// What happened at one event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceEventKind {
    /// An execution (an async run or a tick execution) started at the node.
    Exec,
    /// The node sent message `messages[i]`.
    Send(usize),
    /// The node consumed message `messages[i]` (a top-level receive, or a scripted hook's
    /// release into a tick execution).
    Recv(usize),
    /// The node produced a value on an external output.
    Output {
        /// The output's rendering (its `Debug` form when known, else a hex prefix).
        display: String,
        /// Whether the value was suppressed because the node had crashed.
        dropped: bool,
    },
}

/// One event at one node. Events are in global scheduling order: `events[i]` happened
/// before `events[j]` when `i < j`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Index into [`Trace::nodes`].
    pub node: usize,
    /// The execution the event belongs to.
    pub exec: ExecId,
    /// Whether that execution is a tick execution (`false`: an async run).
    pub tick: bool,
    /// What happened.
    pub kind: TraceEventKind,
}

/// The record of one run.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Trace {
    /// The nodes, in order of first appearance (the observer last).
    pub nodes: Vec<TraceNode>,
    /// Every event, in global order.
    pub events: Vec<TraceEvent>,
    /// Every message, in send order.
    pub messages: Vec<TraceMessage>,
    /// The end of finite failures, as an index into `events`: `events[..eff_cut]` happened
    /// before it. `None` when the run never declared it.
    pub eff_cut: Option<usize>,
}

/// A stable 64-bit hash of a payload's wire bytes (FNV-1a), so a trace can match messages
/// against a derivation without keeping the bytes.
pub fn payload_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

impl Trace {
    /// The index of the node with the given label, if any.
    pub fn node(&self, label: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.label == label)
    }

    /// The messages in `derivation`, as indices into [`Self::messages`]: every message whose
    /// channel, endpoints and payload match a dependency's. A dependency names a message by
    /// identity, so a byte-identical retransmission matches too (as the omission of that
    /// message would drop it too).
    pub fn support(&self, derivation: &Derivation) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        for dep in derivation {
            let message = &dep.message;
            let hash = payload_hash(message.payload());
            for (index, m) in self.messages.iter().enumerate() {
                if m.channel_id == message.channel_id()
                    && m.payload_hash == hash
                    && self.nodes[m.from].member == message.from_member
                    && (m.observation || self.nodes[m.to].member == message.to_member)
                {
                    support.insert(index);
                }
            }
        }
        support
    }

    /// The events at `node`, in order, as indices into [`Self::events`].
    pub fn events_at(&self, node: usize) -> impl Iterator<Item = usize> + '_ {
        self.events
            .iter()
            .enumerate()
            .filter(move |(_, e)| e.node == node)
            .map(|(i, _)| i)
    }

    /// Whether `event` happened before the end of finite failures.
    pub fn pre_eff(&self, event: usize) -> bool {
        self.eff_cut.is_none_or(|cut| event < cut)
    }

    /// Whether message `message` was still on the wire when the run ended.
    pub fn in_flight(&self, message: usize) -> bool {
        let m = &self.messages[message];
        m.fate == Fate::Delivered && m.recv_event.is_none()
    }

    /// The trace as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a trace serializes")
    }

    /// Parses a trace written by [`Self::to_json`].
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A sender that sends three attempts (the first consumed, the second omitted, the third
    /// in flight), is acknowledged, dies after its third send, and attempts a fourth.
    pub(crate) fn two_node_trace() -> Trace {
        let nodes = vec![
            TraceNode {
                label: "sender".to_owned(),
                location: "Process(loc0)".to_owned(),
                member: None,
                is_observer: false,
                crashable: true,
                crash: Some(Crash::AfterEvent(3)),
            },
            TraceNode {
                label: "receiver".to_owned(),
                location: "Process(loc1)".to_owned(),
                member: None,
                is_observer: false,
                crashable: false,
                crash: None,
            },
        ];
        let event = |node, exec, kind| TraceEvent {
            node,
            exec,
            tick: false,
            kind,
        };
        let events = vec![
            event(0, 0, TraceEventKind::Exec),
            event(0, 0, TraceEventKind::Send(0)),
            event(0, 0, TraceEventKind::Send(1)),
            event(0, 0, TraceEventKind::Send(2)),
            event(1, 1, TraceEventKind::Exec),
            event(1, 1, TraceEventKind::Recv(0)),
            event(
                1,
                1,
                TraceEventKind::Output {
                    display: "Msg { attempt: 1 }".to_owned(),
                    dropped: false,
                },
            ),
            event(1, 1, TraceEventKind::Send(3)),
            event(0, 2, TraceEventKind::Exec),
            event(0, 2, TraceEventKind::Send(4)),
        ];
        let message = |display: &str, from, to, send_event, recv_event, fate| TraceMessage {
            channel: if from == 0 { "msg" } else { "ack" }.to_owned(),
            channel_id: if from == 0 { 7 } else { 9 },
            display: display.to_owned(),
            from,
            to,
            send_event,
            recv_event,
            fate,
            observation: false,
            lossy: true,
            delivery_id: send_event as u64,
            ordinal: (from == 0).then_some(send_event as u64),
            payload_hash: payload_hash(display.as_bytes()),
        };
        let messages = vec![
            message("Msg { attempt: 1 }", 0, 1, 1, Some(5), Fate::Delivered),
            message("Msg { attempt: 2 }", 0, 1, 2, None, Fate::Omitted),
            message("Msg { attempt: 3 }", 0, 1, 3, None, Fate::Delivered),
            message("Ack { attempt: 1 }", 1, 0, 7, None, Fate::Delivered),
            message("Msg { attempt: 4 }", 0, 1, 9, None, Fate::Crashed),
        ];
        Trace {
            nodes,
            events,
            messages,
            eff_cut: Some(4),
        }
    }

    #[test]
    fn queries() {
        let trace = two_node_trace();
        assert_eq!(trace.node("receiver"), Some(1));
        assert_eq!(trace.events_at(1).collect::<Vec<_>>(), vec![4, 5, 6, 7]);
        assert!(trace.pre_eff(3));
        assert!(!trace.pre_eff(4));
        assert!(!trace.in_flight(0));
        assert!(!trace.in_flight(1));
        assert!(trace.in_flight(2));
        assert!(trace.in_flight(3));
        assert!(!trace.in_flight(4));
    }

    #[test]
    fn json_round_trip() {
        let trace = two_node_trace();
        let json = trace.to_json();
        assert_eq!(Trace::from_json(&json).unwrap(), trace);
    }

    #[test]
    fn payload_hash_is_stable() {
        assert_eq!(payload_hash(b""), 0xCBF2_9CE4_8422_2325);
        assert_eq!(payload_hash(b"a"), 0xAF63_DC4C_8601_EC8C);
        assert_ne!(payload_hash(b"ab"), payload_hash(b"ba"));
    }
}
