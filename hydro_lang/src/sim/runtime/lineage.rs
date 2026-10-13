//! Execution-granular lineage tracing and send-side crash faults for the simulator.
//!
//! The scheduler runs the program as a sequence of **executions**: one run of an async
//! (top-level) DFIR at a location, or one execution of a tick DFIR. Each gets a fresh
//! [`ExecId`]. Generated code tags every network send and every external output with the
//! execution that produced it (read from the shared [`LineageHandles::current_exec`]
//! cell, which the host sets before each execution), and the host records which hook
//! releases fed each tick execution. Together these events form a graph from which a
//! test harness can compute the backward closure of any output down to the channel
//! deliveries it depended on; see `hydro_lang::sim::ldfi`.
//!
//! The same send-side instrumentation is the fault point for **crashes**: a node may be
//! told to die right after sending a given message, or before its first send
//! ([`CrashState`], [`CrashPoint`]). Every later send is dropped on the floor (recorded,
//! never put on the wire) and, once dead, the node's external outputs are suppressed too.
//! This is the LDFI definition of a crash (omit everything the node sends after time `t`),
//! with the crash time named by a message rather than a send count so that the same fault
//! means the same thing in every run (a send count shifts whenever an upstream fault changes
//! what the node sends). It works on reliable (`fail_stop`) channels, whose receivers see a
//! prefix, and it can split the sends of a single execution, which is what "crashed between
//! two broadcast fan-out sends" needs.
//!
//! Events are plain data shared by `Rc` between the generated dylib (which creates the
//! handles and appends `Sent` / `Output`) and the host scheduler (which appends `Exec` /
//! `Released`), following the same pattern as the hook objects.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dfir_rs::bytes::Bytes;
use dfir_rs::util::unsync::mpsc::Sender;

use crate::location::dynamic::LocationId;

/// Identifies one execution (async run or tick execution) within a simulation instance.
/// Assigned in scheduling order, so `a < b` means `a` ran before `b`.
pub type ExecId = u64;

/// A node of the simulated system: a process, or one member of a cluster.
pub type NodeKey = (LocationId, Option<u32>);

/// One lineage event.
#[derive(Clone, Debug)]
pub enum LineageEvent {
    /// An execution ran (recorded by the host).
    Exec {
        id: ExecId,
        /// The top-level location for an async run; the tick location for a tick execution.
        location: LocationId,
        member: Option<u32>,
        /// `true` for a tick execution, `false` for an async run.
        tick: bool,
    },
    /// A scripted hook under a standing policy released `items` (each bincode-serialized)
    /// into tick execution `exec` (recorded by the host from the hook's policy trace).
    Released {
        exec: ExecId,
        hook_id: usize,
        member: Option<u32>,
        items: Vec<Vec<u8>>,
    },
    /// Execution `exec` at node `(from, from_member)` sent a serialized payload over a
    /// network channel to `(to, to_member)` (recorded by generated code at the send half).
    /// `dropped` is set when the sender had crashed, in which case nothing was put on the
    /// wire.
    Sent {
        exec: ExecId,
        /// The sending `Network` node's sink id (see `sim::slice`).
        channel: usize,
        from: LocationId,
        from_member: Option<u32>,
        to: LocationId,
        to_member: Option<u32>,
        bytes: Vec<u8>,
        dropped: bool,
        /// The delivery ids the sent record was computed from (record-level provenance;
        /// empty when the simulation was compiled without it).
        prov: Vec<u64>,
    },
    /// A scripted batch released an item into execution `exec` and the provenance rewrite
    /// gave it delivery id `id` (recorded by generated code right after the batch).
    Tag {
        exec: ExecId,
        id: u64,
        /// The batch's hook handle, if scripted.
        hook_id: Option<usize>,
        member: Option<u32>,
        bytes: Vec<u8>,
    },
    /// Execution `exec` at node `(to, to_member)` took a serialized payload off network
    /// channel `channel` (recorded by generated code at the receive half, before
    /// deserialization). Arrivals on one channel at one node are in send order.
    Recv {
        exec: ExecId,
        channel: usize,
        to: LocationId,
        to_member: Option<u32>,
        bytes: Vec<u8>,
    },
    /// Execution `exec` produced a serialized value on external output `port` (recorded by
    /// generated code at the `sim_output` sink). `dropped` is set when the node had
    /// crashed, in which case the value was not delivered to the test.
    Output {
        exec: ExecId,
        /// The `SendExternal` root's sink id (see `sim::slice`).
        sink: usize,
        port: usize,
        member: Option<u32>,
        bytes: Vec<u8>,
        dropped: bool,
        /// The delivery ids the output record was computed from (see `Sent::prov`).
        prov: Vec<u64>,
    },
}

/// Where a node dies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    /// Before its first send.
    BeforeAnySend,
    /// Right after it sends this message (by channel, destination member, and wire bytes);
    /// if the message is never sent, the node never dies.
    AfterSend {
        channel: usize,
        to_member: Option<u32>,
        bytes: Vec<u8>,
    },
}

/// Installed crash faults and which nodes have died so far.
#[derive(Clone, Debug, Default)]
pub struct CrashState {
    /// Nodes that crash, and where.
    pub crash_points: HashMap<NodeKey, CrashPoint>,
    /// Nodes that have died (every later send is dropped, outputs suppressed).
    pub dead: HashSet<NodeKey>,
    /// Sends attempted so far per node (successful or dropped), for reports.
    pub sends: HashMap<NodeKey, u64>,
}

impl CrashState {
    /// Installs a crash point (a `BeforeAnySend` kills the node immediately).
    pub fn install(&mut self, node: NodeKey, point: CrashPoint) {
        if point == CrashPoint::BeforeAnySend {
            self.dead.insert(node.clone());
        }
        self.crash_points.insert(node, point);
    }

    /// Whether the node has died.
    pub fn is_dead(&self, node: &NodeKey) -> bool {
        self.dead.contains(node)
    }

    /// Notes a send by `node`; returns whether it was dropped (the node was already dead),
    /// and kills the node afterwards if this send was its crash point.
    fn note_send(
        &mut self,
        node: &NodeKey,
        channel: usize,
        to_member: Option<u32>,
        bytes: &[u8],
    ) -> bool {
        *self.sends.entry(node.clone()).or_insert(0) += 1;
        if self.dead.contains(node) {
            return true;
        }
        if let Some(CrashPoint::AfterSend {
            channel: c,
            to_member: m,
            bytes: b,
        }) = self.crash_points.get(node)
            && *c == channel
            && *m == to_member
            && b.as_slice() == bytes
        {
            self.dead.insert(node.clone());
        }
        false
    }
}

/// The shared lineage and crash state of one simulation instance, created by generated
/// code and returned to the host alongside the hook maps.
#[derive(Clone, Default)]
pub struct LineageHandles {
    /// The execution currently running; set by the host before each execution.
    pub current_exec: Rc<Cell<ExecId>>,
    /// The event log.
    pub events: Rc<RefCell<Vec<LineageEvent>>>,
    /// Crash faults.
    pub crashes: Rc<RefCell<CrashState>>,
    /// The next delivery id for record-level provenance.
    pub next_delivery: Rc<Cell<u64>>,
}

impl LineageHandles {
    /// Records a network send from the current execution and reports whether it should
    /// actually be performed (`false` once the sending node has crashed). Called by
    /// generated code.
    #[doc(hidden)]
    pub fn record_sent(
        &self,
        channel: usize,
        from: &LocationId,
        from_member: Option<u32>,
        to: &LocationId,
        to_member: Option<u32>,
        bytes: &Bytes,
    ) -> bool {
        self.record_sent_with(channel, from, from_member, to, to_member, bytes, Vec::new())
    }

    /// [`Self::record_sent`] with the sent record's provenance.
    #[doc(hidden)]
    #[expect(clippy::too_many_arguments, reason = "mirrors the generated send site")]
    pub fn record_sent_with(
        &self,
        channel: usize,
        from: &LocationId,
        from_member: Option<u32>,
        to: &LocationId,
        to_member: Option<u32>,
        bytes: &Bytes,
        prov: Vec<u64>,
    ) -> bool {
        let node = (from.clone(), from_member);
        let dropped = self
            .crashes
            .borrow_mut()
            .note_send(&node, channel, to_member, bytes);
        self.events.borrow_mut().push(LineageEvent::Sent {
            exec: self.current_exec.get(),
            channel,
            from: from.clone(),
            from_member,
            to: to.clone(),
            to_member,
            bytes: bytes.to_vec(),
            dropped,
            prov,
        });
        !dropped
    }

    /// Assigns a fresh delivery id to an item a batch released into the current execution
    /// and records it. Called by generated code (provenance rewrite).
    #[doc(hidden)]
    pub fn record_tag(&self, hook_id: Option<usize>, member: Option<u32>, bytes: Vec<u8>) -> u64 {
        let id = self.next_delivery.get();
        self.next_delivery.set(id + 1);
        self.events.borrow_mut().push(LineageEvent::Tag {
            exec: self.current_exec.get(),
            id,
            hook_id,
            member,
            bytes,
        });
        id
    }

    /// Records a network receive by the current execution. Called by generated code.
    #[doc(hidden)]
    pub fn record_recv(
        &self,
        channel: usize,
        to: &LocationId,
        to_member: Option<u32>,
        bytes: &Bytes,
    ) {
        self.events.borrow_mut().push(LineageEvent::Recv {
            exec: self.current_exec.get(),
            channel,
            to: to.clone(),
            to_member,
            bytes: bytes.to_vec(),
        });
    }
}

/// An external-output sender that records every value it forwards as a lineage
/// [`LineageEvent::Output`] of the current execution, and suppresses the outputs of a
/// crashed node. Generated `sim_output` sinks call `try_send` on it exactly as they would
/// on the underlying channel sender.
#[doc(hidden)]
pub struct LineageOutputSender {
    pub inner: Sender<Bytes>,
    pub sink: usize,
    pub port: usize,
    /// The producing node's root location, filled in by `at` once the sink is placed.
    pub location: Option<LocationId>,
    pub member: Option<u32>,
    pub lineage: LineageHandles,
}

impl LineageOutputSender {
    /// Names the node this sink belongs to (needed to honor its crash) and its sink id.
    /// Called by generated code.
    pub fn at(mut self, location: LocationId, sink: usize) -> Self {
        self.location = Some(location);
        self.sink = sink;
        self
    }

    pub fn try_send(
        &self,
        value: Bytes,
    ) -> Result<(), dfir_rs::util::unsync::mpsc::TrySendError<Bytes>> {
        self.try_send_with(value, Vec::new())
    }

    /// [`Self::try_send`] with the output record's provenance.
    pub fn try_send_with(
        &self,
        value: Bytes,
        prov: Vec<u64>,
    ) -> Result<(), dfir_rs::util::unsync::mpsc::TrySendError<Bytes>> {
        let dropped = self.location.as_ref().is_some_and(|location| {
            self.lineage
                .crashes
                .borrow()
                .is_dead(&(location.clone(), self.member))
        });
        self.lineage.events.borrow_mut().push(LineageEvent::Output {
            exec: self.lineage.current_exec.get(),
            sink: self.sink,
            port: self.port,
            member: self.member,
            bytes: value.to_vec(),
            dropped,
            prov,
        });
        if dropped {
            Ok(())
        } else {
            self.inner.try_send(value)
        }
    }
}
