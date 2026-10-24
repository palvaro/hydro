//! Fault state of one simulated run: the per-send filter, positional crash points, and the event
//! log the lineage is read from (docs/design/hydrofrac-lite.md §3, §4.1, §4.6).
//!
//! [`FaultHook`] is the simulator network hook (`hydro_lang::sim::network_hook`) that applies a
//! [`FaultPlan`] to every intercepted send:
//! - an **omission** discards every copy of one [`MessageId`] on a lossy channel;
//! - a **positional crash** `crash(node after m)` lets the node perform its sends up to and
//!   including `m`, then halts it, so nothing after `m` leaves it (`crash(node)` halts it
//!   before its first send).
//!
//! Faults only happen before [`FaultHook::heal`]: after it, no omission applies and no crash
//! fires, but crashes that already fired persist (§4.6).
//!
//! Every callback is appended to the instance's [`EventLog`] in program order.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Display};
use std::rc::Rc;

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::networking::{NetworkingInfo, TcpFault};
use hydro_lang::sim::compiled::CompiledSim;
use hydro_lang::sim::network_hook::{ChannelInfo, LocationInfo, SimNetworkHook};

use crate::search::{CrashAt, Fault, FaultSet, Node};

/// One node of the program: a process, or one member of a cluster.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId {
    /// The process or cluster (always a root location).
    pub location: LocationId,
    /// The cluster member, `None` for a process.
    pub member: Option<u32>,
}

impl NodeId {
    /// The node `member` (`None` for a process) of `location`, normalised to its root location.
    pub fn new(location: &LocationId, member: Option<u32>) -> Self {
        NodeId {
            location: location.root().clone(),
            member,
        }
    }

    /// The node's display name, `label` for a process and `label@member` for a cluster member,
    /// as [`NodeNames`] assigns it over `locations`.
    pub fn node_name(&self, locations: &[LocationInfo]) -> Node {
        NodeNames::new(locations).name(self)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.member {
            None => write!(f, "{:?}", self.location),
            Some(m) => write!(f, "{:?}@{m}", self.location),
        }
    }
}

/// The two-way map between [`NodeId`]s and the search's node names ([`Node`]).
///
/// A location's label is the last path segment of its type name, without generics (`()` becomes
/// `process` or `cluster`). Locations whose labels collide get `#i` suffixes in location order.
/// Cluster members are `label@member`.
#[derive(Clone, Debug, Default)]
pub struct NodeNames {
    by_id: BTreeMap<NodeId, Node>,
    by_name: BTreeMap<Node, NodeId>,
    labels: BTreeMap<LocationId, String>,
    /// Nodes that can fail, in location then member order: every node.
    crashable: Vec<NodeId>,
}

fn short_label(info: &LocationInfo) -> String {
    let name = info.name.split('<').next().unwrap_or(&info.name);
    let name = name.rsplit("::").next().unwrap_or(name).trim();
    if name.is_empty() || name == "()" {
        if info.members.is_some() {
            "cluster".to_owned()
        } else {
            "process".to_owned()
        }
    } else {
        name.to_owned()
    }
}

impl NodeNames {
    /// Names every node of `locations` (as [`CompiledSim::locations`] returns them).
    pub fn new(locations: &[LocationInfo]) -> Self {
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for l in locations {
            *counts.entry(short_label(l)).or_default() += 1;
        }
        let mut seen: BTreeMap<String, usize> = BTreeMap::new();
        let mut names = NodeNames::default();
        for l in locations {
            let base = short_label(l);
            let label = if counts[&base] > 1 {
                let i = seen.entry(base.clone()).or_default();
                let label = format!("{base}#{i}");
                *i += 1;
                label
            } else {
                base
            };
            names.labels.insert(l.id.root().clone(), label.clone());
            let nodes: Vec<(NodeId, Node)> = match &l.members {
                None => vec![(NodeId::new(&l.id, None), label.clone())],
                Some(members) => members
                    .iter()
                    .map(|m| (NodeId::new(&l.id, Some(*m)), format!("{label}@{m}")))
                    .collect(),
            };
            for (id, name) in nodes {
                names.crashable.push(id.clone());
                names.by_name.insert(name.clone(), id.clone());
                names.by_id.insert(id, name);
            }
        }
        names
    }

    /// The name of `node`. Nodes outside the table (which a well-formed simulation never
    /// reports) are named by their [`Display`] form.
    pub fn name(&self, node: &NodeId) -> Node {
        self.by_id
            .get(node)
            .cloned()
            .unwrap_or_else(|| node.to_string())
    }

    /// The node called `name`, if any.
    pub fn id(&self, name: &str) -> Option<&NodeId> {
        self.by_name.get(name)
    }

    /// The label of a process or cluster.
    pub fn label(&self, location: &LocationId) -> Option<&str> {
        self.labels.get(location.root()).map(String::as_str)
    }

    /// Every node that can crash (every node of the program), in location then member order.
    pub fn crashable(&self) -> &[NodeId] {
        &self.crashable
    }
}

/// The identity of a message: `(channel, sender, destination, wire bytes)` (§3). Omitting it
/// discards every copy; it is the search's message type `M`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MessageId {
    /// The channel ([`ChannelInfo::id`]).
    pub channel: usize,
    /// The sender.
    pub from: NodeId,
    /// The destination.
    pub to: NodeId,
    /// The serialized payload.
    pub bytes: Rc<[u8]>,
}

impl Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c{}:{}→{}:", self.channel, self.from, self.to)?;
        for b in self.bytes.iter().take(8) {
            write!(f, "{b:02x}")?;
        }
        if self.bytes.len() > 8 {
            write!(f, "…")?;
        }
        Ok(())
    }
}

/// The wire tag of one send in one instance (E5), fresh per send, starting at 1. Dropped sends
/// get one too, so the log can name them.
pub type DeliveryId = u64;

/// What happened to a send.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Fate {
    /// Delivered (eventually) to its destination.
    Delivered,
    /// Discarded by an omission of the plan.
    Omitted,
    /// Discarded because its sender had crashed.
    SenderDead,
}

/// One entry of the event log, in program order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Event {
    /// The scheduler starts an execution at `node`.
    Exec {
        /// Where it runs.
        node: NodeId,
    },
    /// The execution announced by the last [`Event::Exec`] finished.
    ExecEnd,
    /// A send was attempted.
    Sent {
        /// Its tag.
        id: DeliveryId,
        /// The message.
        msg: MessageId,
        /// Its position among the sender's sends in this instance, from 0.
        ordinal: u64,
        /// The metadata a post-network IR pass attached to it, if any.
        meta: Option<Vec<u8>>,
        /// What happened to it.
        fate: Fate,
    },
    /// A delivered message was received.
    Recv {
        /// Its tag (the [`Event::Sent`] it came from).
        id: DeliveryId,
        /// The channel.
        channel: usize,
        /// The receiver.
        to: NodeId,
    },
    /// A value was emitted on an external output.
    Output {
        /// The external port.
        port: usize,
        /// The emitting node.
        node: NodeId,
        /// The serialized value.
        bytes: Vec<u8>,
        /// The metadata a post-network IR pass attached to it, if any.
        meta: Option<Vec<u8>>,
    },
    /// A crash of the plan fired: `node` sends nothing from here on, and the scheduler halts it.
    Halted {
        /// The crashed node.
        node: NodeId,
    },
    /// The network healed ([`FaultHook::heal`]).
    Heal,
}

/// The events of one simulation instance, in program order.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct EventLog(pub Vec<Event>);

impl EventLog {
    /// The events, in program order.
    pub fn events(&self) -> &[Event] {
        &self.0
    }

    /// The sends, in program order, as `(id, message, ordinal, fate)`.
    pub fn sends(&self) -> impl Iterator<Item = (DeliveryId, &MessageId, u64, Fate)> + '_ {
        self.0.iter().filter_map(|e| match e {
            Event::Sent {
                id,
                msg,
                ordinal,
                fate,
                ..
            } => Some((*id, msg, *ordinal, *fate)),
            _ => None,
        })
    }
}

/// The faults to inject into a run: one hypothesis.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct FaultPlan {
    /// The faults; crash points name nodes by their [`NodeNames`] name.
    pub faults: FaultSet<MessageId>,
}

/// What one instance left behind.
#[derive(Clone, Debug, Default)]
pub struct InstanceRecord {
    /// Its events.
    pub log: EventLog,
    /// The nodes whose crash fired. Every other node is correct.
    pub crashed: BTreeSet<NodeId>,
}

#[derive(Default)]
struct Instance {
    started: bool,
    log: Vec<Event>,
    next_id: DeliveryId,
    ordinals: HashMap<NodeId, u64>,
    dead: BTreeSet<NodeId>,
    halts: Vec<(LocationId, Option<u32>)>,
    healed: bool,
    /// The node of the execution in progress (outputs are attributed to it).
    exec: Option<NodeId>,
}

impl Instance {
    fn crash(&mut self, node: &NodeId) {
        if self.dead.insert(node.clone()) {
            self.log.push(Event::Halted { node: node.clone() });
            self.halts.push((node.location.clone(), node.member));
        }
    }

    fn record(self) -> InstanceRecord {
        InstanceRecord {
            log: EventLog(self.log),
            crashed: self.dead,
        }
    }
}

/// The simulator network hook that injects a [`FaultPlan`] and records the [`EventLog`].
///
/// Install it with [`CompiledSim::set_network_hook`] on a flow compiled with
/// `with_network_hook_points()`. One hook serves many instances (schedules): each starts with
/// fresh fault state. Collect each instance's record with [`Self::crashed`] from inside the
/// instance, or all of them afterwards with [`Self::drain_instances`].
pub struct FaultHook {
    channels: HashMap<usize, ChannelInfo>,
    names: NodeNames,
    omit: BTreeSet<MessageId>,
    crashes: BTreeMap<NodeId, CrashAt<MessageId>>,
    current: RefCell<Instance>,
    finished: RefCell<Vec<InstanceRecord>>,
}

impl FaultHook {
    /// A hook injecting `plan` into `sim`.
    ///
    /// # Panics
    /// If a crash of the plan names a node `sim` does not have, or a crash point `After(m)` names
    /// a message `m` its node does not send.
    pub fn new(plan: &FaultPlan, sim: &CompiledSim) -> Self {
        Self::from_tables(plan, sim.channels(), sim.locations())
    }

    /// [`Self::new`], from the simulation's tables.
    ///
    /// # Panics
    /// Also if a lossy channel is not intercepted (its omissions could not be injected).
    pub fn from_tables(
        plan: &FaultPlan,
        channels: &[ChannelInfo],
        locations: &[LocationInfo],
    ) -> Self {
        for c in channels {
            assert!(
                c.intercepted || !is_lossy(&c.networking),
                "lossy channel {c:?} is not intercepted"
            );
        }
        let names = NodeNames::new(locations);
        let mut omit = BTreeSet::new();
        let mut crashes = BTreeMap::new();
        for fault in &plan.faults {
            match fault {
                Fault::Omit(m) => {
                    omit.insert(m.clone());
                }
                Fault::Crash { node, at } => {
                    let id = names
                        .id(node)
                        .unwrap_or_else(|| panic!("crash of unknown node `{node}`"))
                        .clone();
                    if let CrashAt::After(m) = at {
                        assert_eq!(
                            m.from, id,
                            "crash point of `{node}` names a message `{m}` it does not send"
                        );
                    }
                    let previous = crashes.insert(id, at.clone());
                    assert!(previous.is_none(), "two crashes of `{node}` in one plan");
                }
            }
        }
        FaultHook {
            channels: channels.iter().map(|c| (c.id, c.clone())).collect(),
            names,
            omit,
            crashes,
            current: RefCell::new(Instance::default()),
            finished: RefCell::new(Vec::new()),
        }
    }

    /// The node names of the simulation.
    pub fn names(&self) -> &NodeNames {
        &self.names
    }

    /// The channel with id `id`.
    pub fn channel(&self, id: usize) -> Option<&ChannelInfo> {
        self.channels.get(&id)
    }

    /// Heals the network for the rest of the current instance: from now on no omission applies
    /// and no crash fires. Crashes that already fired persist.
    pub fn heal(&self) {
        let mut cur = self.current.borrow_mut();
        if !cur.healed {
            cur.healed = true;
            cur.log.push(Event::Heal);
        }
    }

    /// The nodes whose crash has fired in the current instance.
    pub fn crashed(&self) -> BTreeSet<NodeId> {
        self.current.borrow().dead.clone()
    }

    /// Takes the records of every instance run so far, including the current one, which ends.
    pub fn drain_instances(&self) -> Vec<InstanceRecord> {
        let mut finished = std::mem::take(&mut *self.finished.borrow_mut());
        let cur = std::mem::take(&mut *self.current.borrow_mut());
        if cur.started {
            finished.push(cur.record());
        }
        finished
    }

}

/// Whether a channel with this networking may lose messages (so omissions apply to it):
/// everything but `fail_stop` TCP.
pub fn is_lossy(networking: &NetworkingInfo) -> bool {
    !matches!(
        networking,
        NetworkingInfo::Tcp {
            fault: TcpFault::FailStop
        }
    )
}

impl SimNetworkHook for FaultHook {
    fn on_instance_begin(&self) {
        let previous = std::mem::take(&mut *self.current.borrow_mut());
        if previous.started {
            self.finished.borrow_mut().push(previous.record());
        }
        let mut cur = self.current.borrow_mut();
        cur.started = true;
        cur.next_id = 1;
        for (node, at) in &self.crashes {
            if *at == CrashAt::BeforeAnySend {
                cur.crash(node);
            }
        }
    }

    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        let info = self
            .channels
            .get(&channel)
            .unwrap_or_else(|| panic!("send on unknown channel {channel}"));
        let msg = MessageId {
            channel,
            from: NodeId::new(&info.from, from_member),
            to: NodeId::new(&info.to, to_member),
            bytes: payload.into(),
        };
        let mut cur = self.current.borrow_mut();
        let id = cur.next_id;
        cur.next_id += 1;
        let ordinal = {
            let o = cur.ordinals.entry(msg.from.clone()).or_default();
            let ordinal = *o;
            *o += 1;
            ordinal
        };
        let fate = if cur.dead.contains(&msg.from) {
            Fate::SenderDead
        } else if !cur.healed && is_lossy(&info.networking) && self.omit.contains(&msg) {
            Fate::Omitted
        } else {
            Fate::Delivered
        };
        let fires = fate != Fate::SenderDead
            && !cur.healed
            && self.crashes.get(&msg.from) == Some(&CrashAt::After(msg.clone()));
        let from = msg.from.clone();
        cur.log.push(Event::Sent {
            id,
            msg,
            ordinal,
            meta,
            fate,
        });
        if fires {
            cur.crash(&from);
        }
        (fate == Fate::Delivered).then_some(id)
    }

    fn on_recv(&self, channel: usize, to_member: Option<u32>, tag: u64) {
        let to = match self.channels.get(&channel) {
            Some(info) => NodeId::new(&info.to, to_member),
            None => panic!("receive on unknown channel {channel}"),
        };
        self.current.borrow_mut().log.push(Event::Recv {
            id: tag,
            channel,
            to,
        });
    }

    fn on_output(
        &self,
        port: usize,
        member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> bool {
        let mut cur = self.current.borrow_mut();
        // Only the scheduler's executions run dataflow, so every output belongs to one.
        let node = cur
            .exec
            .clone()
            .unwrap_or_else(|| panic!("output on port {port} outside an execution"));
        assert_eq!(node.member, member, "output on port {port} from another member");
        // A crashed node emits nothing, like it sends nothing (between its crash firing and the
        // scheduler halting it, its current execution may still run).
        if cur.dead.contains(&node) {
            return false;
        }
        cur.log.push(Event::Output {
            port,
            node,
            bytes: payload.to_vec(),
            meta,
        });
        true
    }

    fn on_exec_begin(&self, location: &LocationId, member: Option<u32>) {
        let node = NodeId::new(location, member);
        let mut cur = self.current.borrow_mut();
        cur.exec = Some(node.clone());
        cur.log.push(Event::Exec { node });
    }

    fn on_exec_end(&self) {
        let mut cur = self.current.borrow_mut();
        cur.exec = None;
        cur.log.push(Event::ExecEnd);
    }

    fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
        std::mem::take(&mut self.current.borrow_mut().halts)
    }
}

// In the staged copy too: the tests' `q!` closures are resolved there.
#[cfg(test)]
mod tests;
