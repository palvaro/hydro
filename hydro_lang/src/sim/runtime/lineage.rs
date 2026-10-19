//! The simulator's fault point and lineage log: a **filter** on every network send.
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
//! # The filter
//!
//! Every network send passes through [`LineageHandles::record_sent`], which consults the
//! instance's [`FaultState`] and decides the send's [`SendFate`]:
//!
//! - **Omission.** A send whose [`SendKey`] (send site, sender, destination, wire bytes) is
//!   in the installed omission set is dropped, provided its channel is *lossy* (the
//!   transport the program declared admits loss) and the end of finite failures has not been
//!   declared. Omission is keyed by the message's identity, not by a send ordinal, so a
//!   byte-identical retransmission is omitted too for as long as the fault stands, and the
//!   same fault means the same thing in every run.
//! - **Crash.** A node may be told to die right after sending a given message, or before
//!   its first send ([`CrashPoint`]). Every later send on every channel is dropped and, once
//!   dead, the node's external outputs are suppressed too. This is Molly's crash (omit
//!   everything the node sends after time `t`), with the crash time named by a message.
//!   It works on reliable (`fail_stop`) channels, whose receivers then see a prefix, and it
//!   can split the sends of one execution, which "crashed mid fan-out" needs.
//! - **End of finite failures.** [`FaultState::end_finite_failures`] is a switch: from then
//!   on no new failure happens. Omissions are ignored, and a crash point whose message has
//!   not been sent by then never fires (a node that is already dead stays dead: a crash is
//!   permanent). This is Molly's fault model: every failure, omission or crash, happens at a
//!   time `≤ EFF`, and the run after `EFF` is failure-free recovery. A crash point is *named*
//!   by a message sent before `EFF` in the run that proposed it; in another run the same
//!   message may be sent after `EFF` (or never), and the crash then does not happen there.
//!
//! A send that reaches the wire carries a fresh **delivery id**, which the receive half
//! records ([`LineageEvent::Recv`]) and, under record-level provenance, attaches to the
//! deserialized record, so a delivery is attributed to its send exactly.
//!
//! Events are plain data shared by `Rc` between the generated dylib (which creates the
//! handles and appends `Sent` / `Recv` / `Output`) and the host scheduler (which appends
//! `Exec` / `Released` and installs faults), following the same pattern as the hook objects.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dfir_rs::bytes::Bytes;
use dfir_rs::util::unsync::mpsc::Sender;

use super::provenance::{DEFAULT_MAX_WITNESSES, Prov, RecordProv};
use crate::location::dynamic::LocationId;

/// Identifies one execution (async run or tick execution) within a simulation instance.
/// Assigned in scheduling order, so `a < b` means `a` ran before `b`.
pub type ExecId = u64;

/// A node of the simulated system: a process, or one member of a cluster.
pub type NodeKey = (LocationId, Option<u32>);

/// A delivery id: the identity of one send that reached the wire, minted by the filter and
/// carried to the receiver.
pub type DeliveryId = u64;

/// What the filter did with a send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SendFate {
    /// Put on the wire.
    Delivered,
    /// Dropped by an installed omission.
    Omitted,
    /// Dropped because the sender had crashed.
    Crashed,
}

/// The identity of a send as the fault vocabulary names it: the send site (the sending
/// `Network` node's sink id, see `sim::slice`), the sending node, the destination member, and
/// the wire bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SendKey {
    pub channel: usize,
    pub from: NodeKey,
    pub to_member: Option<u32>,
    pub bytes: Vec<u8>,
}

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
    /// network channel to `(to, to_member)`, and the filter decided its `fate` (recorded by
    /// generated code at the send half). `id` is the delivery id the receiver sees when the
    /// send was delivered.
    Sent {
        exec: ExecId,
        id: DeliveryId,
        /// The sending `Network` node's sink id (see `sim::slice`).
        channel: usize,
        from: LocationId,
        from_member: Option<u32>,
        to: LocationId,
        to_member: Option<u32>,
        bytes: Vec<u8>,
        /// The payload's `Debug` rendering, when its type has one.
        display: Option<String>,
        fate: SendFate,
        /// The deliveries the sent record was computed from (record-level provenance; empty
        /// when the simulation was compiled without it), resolved when the log was read
        /// (see [`LineageHandles::snapshot`]).
        prov: RecordProv,
    },
    /// Execution `exec` at node `(to, to_member)` took delivery `send` off network channel
    /// `channel` (recorded by generated code at the receive half, before deserialization).
    Recv {
        exec: ExecId,
        channel: usize,
        to: LocationId,
        to_member: Option<u32>,
        send: DeliveryId,
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
        /// The deliveries the output record was computed from (see `Sent::prov`).
        prov: RecordProv,
    },
}

/// Where a node dies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    /// Before its first send.
    BeforeAnySend,
    /// Right after it sends this message (by channel, destination member, and wire bytes),
    /// provided the message is sent before the end of finite failures; if it is never sent,
    /// or first sent after `EFF`, the node never dies (see the module docs).
    AfterSend {
        channel: usize,
        to_member: Option<u32>,
        bytes: Vec<u8>,
    },
}

/// The faults installed in one simulation instance, and what the filter has done so far.
#[derive(Clone, Debug, Default)]
pub struct FaultState {
    /// Channels (by sink id) whose transport admits omission. Installed by the host from the
    /// program's declared networking before the instance runs.
    pub lossy: HashSet<usize>,
    /// Sends to omit, while finite failures last.
    pub omissions: HashSet<SendKey>,
    /// Whether the end of finite failures has been declared (omissions no longer apply).
    pub finite_failures_ended: bool,
    /// Nodes that crash, and where.
    pub crash_points: HashMap<NodeKey, CrashPoint>,
    /// Nodes that have died (every later send is dropped, outputs suppressed).
    pub dead: HashSet<NodeKey>,
    /// Sends attempted so far per node (whatever their fate), for reports.
    pub sends: HashMap<NodeKey, u64>,
}

impl FaultState {
    /// Whether `channel` admits omission.
    pub fn is_lossy(&self, channel: usize) -> bool {
        self.lossy.contains(&channel)
    }

    /// Installs an omission. Panics if the channel does not admit it: the transport type
    /// the program declared is its own statement of its fault model, and the filter enforces
    /// it.
    pub fn omit(&mut self, key: SendKey) {
        assert!(
            self.is_lossy(key.channel),
            "omission installed on channel #{} whose transport admits no loss",
            key.channel
        );
        self.omissions.insert(key);
    }

    /// Installs a crash point (a `BeforeAnySend` kills the node immediately).
    pub fn install(&mut self, node: NodeKey, point: CrashPoint) {
        if point == CrashPoint::BeforeAnySend {
            self.dead.insert(node.clone());
        }
        self.crash_points.insert(node, point);
    }

    /// Declares the end of finite failures: omissions stop applying, and no crash point fires
    /// from now on (a node already dead stays dead).
    pub fn end_finite_failures(&mut self) {
        self.finite_failures_ended = true;
    }

    /// Whether the node has died.
    pub fn is_dead(&self, node: &NodeKey) -> bool {
        self.dead.contains(node)
    }

    /// The filter: decides the fate of a send by `node` and updates the state (kills the node
    /// afterwards if this send was its crash point and finite failures have not ended). A
    /// crash point fires whether or not the send itself was omitted: the node performed the
    /// send; the network lost it.
    fn filter(&mut self, key: &SendKey) -> SendFate {
        *self.sends.entry(key.from.clone()).or_insert(0) += 1;
        if self.dead.contains(&key.from) {
            return SendFate::Crashed;
        }
        if !self.finite_failures_ended
            && let Some(CrashPoint::AfterSend {
                channel,
                to_member,
                bytes,
            }) = self.crash_points.get(&key.from)
            && *channel == key.channel
            && *to_member == key.to_member
            && *bytes == key.bytes
        {
            self.dead.insert(key.from.clone());
        }
        if !self.finite_failures_ended && self.is_lossy(key.channel) && self.omissions.contains(key)
        {
            return SendFate::Omitted;
        }
        SendFate::Delivered
    }
}

/// The shared lineage and fault state of one simulation instance, created by generated
/// code and returned to the host alongside the hook maps.
#[derive(Clone, Default)]
pub struct LineageHandles {
    /// The execution currently running; set by the host before each execution.
    pub current_exec: Rc<Cell<ExecId>>,
    /// The event log.
    pub events: Rc<RefCell<Vec<LineageEvent>>>,
    /// The installed faults and the filter's state.
    pub faults: Rc<RefCell<FaultState>>,
    /// The next delivery id.
    pub next_delivery: Rc<Cell<DeliveryId>>,
    /// The provenance formula of each `Sent` / `Output` event (by index into `events`) that
    /// carried one, resolved when the log is read: a formula is lazy, since a record may
    /// still acquire alternative derivations after it was sent (see `super::provenance`).
    pub pending_prov: Rc<RefCell<Vec<(usize, Prov)>>>,
    /// The cap on witnesses per record when resolving (`0` for the default,
    /// [`DEFAULT_MAX_WITNESSES`]).
    pub witness_cap: Rc<Cell<usize>>,
    /// The message of a panic raised by the program's own code (an operator closure) and
    /// caught on the generated code's side of the dynamic-library boundary by
    /// [`erase_guarded`]; the host reports it. Unwinding across that boundary aborts the
    /// process, which a test must not do and a counterfactual replay (whose diverged state
    /// may drive the program into conditions no real execution reaches) must survive.
    pub panicked: Rc<RefCell<Option<String>>>,
}

/// Type-erases a location's dataflow for the host, with its tick closure guarded against
/// panics: a panic in the program's code is caught here, inside the generated crate, and
/// recorded in `handles.panicked` for the host to report (see
/// [`LineageHandles::panicked`]). Called by generated code.
#[doc(hidden)]
pub fn erase_guarded<Tick>(
    dfir: dfir_rs::scheduled::context::Dfir<Tick>,
    handles: LineageHandles,
) -> dfir_rs::scheduled::context::DfirErased
where
    Tick: 'static + for<'a> AsyncFnMut(&'a mut dfir_rs::scheduled::context::Context) -> bool,
{
    use futures::FutureExt;
    dfir.map_tick_closure(move |mut tick| {
        async move |ctx: &mut dfir_rs::scheduled::context::Context| -> bool {
            match std::panic::AssertUnwindSafe(tick(ctx)).catch_unwind().await {
                Ok(progressed) => progressed,
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                        .unwrap_or_else(|| "(non-string panic payload)".to_owned());
                    let mut panicked = handles.panicked.borrow_mut();
                    if panicked.is_none() {
                        *panicked = Some(message);
                    }
                    false
                }
            }
        }
    })
    .into_erased()
}

impl LineageHandles {
    /// The filter at a network send: records the send from the current execution and returns
    /// the delivery id to put on the wire with it, or `None` when the send is dropped
    /// (omitted, or the sender has crashed). Called by generated code.
    #[doc(hidden)]
    #[expect(clippy::too_many_arguments, reason = "mirrors the generated send site")]
    pub fn record_sent(
        &self,
        channel: usize,
        from: &LocationId,
        from_member: Option<u32>,
        to: &LocationId,
        to_member: Option<u32>,
        bytes: &Bytes,
        display: Option<String>,
    ) -> Option<DeliveryId> {
        self.record_sent_with(
            channel,
            from,
            from_member,
            to,
            to_member,
            bytes,
            display,
            Prov::empty(),
        )
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
        display: Option<String>,
        prov: Prov,
    ) -> Option<DeliveryId> {
        let key = SendKey {
            channel,
            from: (from.clone(), from_member),
            to_member,
            bytes: bytes.to_vec(),
        };
        let fate = self.faults.borrow_mut().filter(&key);
        let id = self.next_delivery.get();
        self.next_delivery.set(id + 1);
        self.note_prov(prov);
        self.events.borrow_mut().push(LineageEvent::Sent {
            exec: self.current_exec.get(),
            id,
            channel,
            from: from.clone(),
            from_member,
            to: to.clone(),
            to_member,
            bytes: key.bytes,
            display,
            fate,
            prov: RecordProv::default(),
        });
        (fate == SendFate::Delivered).then_some(id)
    }

    /// Files the provenance of the event about to be pushed, for resolution at
    /// [`Self::snapshot`]. An empty formula needs no filing: its resolution is the default.
    fn note_prov(&self, prov: Prov) {
        if !prov.is_empty() {
            let index = self.events.borrow().len();
            self.pending_prov.borrow_mut().push((index, prov));
        }
    }

    /// Drops every event from `len` on (and the provenance filed for them).
    pub fn truncate(&self, len: usize) {
        self.events.borrow_mut().truncate(len);
        self.pending_prov
            .borrow_mut()
            .retain(|(index, _)| *index < len);
    }

    /// The cap on witnesses per record (see `super::provenance`).
    pub fn max_witnesses(&self) -> usize {
        match self.witness_cap.get() {
            0 => DEFAULT_MAX_WITNESSES,
            cap => cap,
        }
    }

    /// A copy of the event log with every record's provenance resolved to its witnesses under
    /// the current cap (see [`Self::witness_cap`]).
    pub fn snapshot(&self) -> Vec<LineageEvent> {
        let mut events = self.events.borrow().clone();
        let cap = self.max_witnesses();
        for (index, prov) in self.pending_prov.borrow().iter() {
            if let Some(
                LineageEvent::Sent { prov: slot, .. } | LineageEvent::Output { prov: slot, .. },
            ) = events.get_mut(*index)
            {
                *slot = RecordProv::resolve(prov, cap);
            }
        }
        events
    }

    /// Records a network receive by the current execution. Called by generated code.
    #[doc(hidden)]
    pub fn record_recv(
        &self,
        channel: usize,
        to: &LocationId,
        to_member: Option<u32>,
        send: DeliveryId,
    ) {
        self.events.borrow_mut().push(LineageEvent::Recv {
            exec: self.current_exec.get(),
            channel,
            to: to.clone(),
            to_member,
            send,
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
        self.try_send_with(value, Prov::empty())
    }

    /// [`Self::try_send`] with the output record's provenance.
    pub fn try_send_with(
        &self,
        value: Bytes,
        prov: Prov,
    ) -> Result<(), dfir_rs::util::unsync::mpsc::TrySendError<Bytes>> {
        let dropped = self.location.as_ref().is_some_and(|location| {
            self.lineage
                .faults
                .borrow()
                .is_dead(&(location.clone(), self.member))
        });
        self.lineage.note_prov(prov);
        self.lineage.events.borrow_mut().push(LineageEvent::Output {
            exec: self.lineage.current_exec.get(),
            sink: self.sink,
            port: self.port,
            member: self.member,
            bytes: value.to_vec(),
            dropped,
            prov: RecordProv::default(),
        });
        if dropped {
            Ok(())
        } else {
            self.inner.try_send(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeKey {
        (
            LocationId::Process(crate::location::LocationKey::FIRST),
            None,
        )
    }

    fn key(bytes: u8) -> SendKey {
        SendKey {
            channel: 7,
            from: node(),
            to_member: None,
            bytes: vec![bytes],
        }
    }

    fn after(bytes: u8) -> CrashPoint {
        CrashPoint::AfterSend {
            channel: 7,
            to_member: None,
            bytes: vec![bytes],
        }
    }

    /// A crash point fires right after its message, and every later send is dropped.
    #[test]
    fn crash_point_fires_before_eff_and_is_permanent() {
        let mut faults = FaultState::default();
        faults.install(node(), after(1));
        assert_eq!(faults.filter(&key(0)), SendFate::Delivered);
        assert_eq!(faults.filter(&key(1)), SendFate::Delivered);
        assert!(faults.is_dead(&node()));
        assert_eq!(faults.filter(&key(2)), SendFate::Crashed);
        // Permanent across the end of finite failures.
        faults.end_finite_failures();
        assert_eq!(faults.filter(&key(3)), SendFate::Crashed);
    }

    /// Molly's fault model: every failure happens at a time `≤ EFF`. A crash point whose
    /// message is first sent after the end of finite failures does not fire in that run.
    #[test]
    fn crash_point_named_by_a_post_eff_send_does_not_fire() {
        let mut faults = FaultState::default();
        faults.install(node(), after(1));
        assert_eq!(faults.filter(&key(0)), SendFate::Delivered);
        faults.end_finite_failures();
        assert_eq!(faults.filter(&key(1)), SendFate::Delivered);
        assert!(!faults.is_dead(&node()));
        assert_eq!(faults.filter(&key(2)), SendFate::Delivered);
        assert_eq!(faults.filter(&key(1)), SendFate::Delivered);
    }

    /// `BeforeAnySend` kills the node at installation, which is before `EFF` by construction.
    #[test]
    fn crash_before_any_send_kills_immediately() {
        let mut faults = FaultState::default();
        faults.install(node(), CrashPoint::BeforeAnySend);
        assert!(faults.is_dead(&node()));
        assert_eq!(faults.filter(&key(0)), SendFate::Crashed);
    }
}
