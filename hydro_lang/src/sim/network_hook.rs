//! Extension points for external simulator tooling: network interception, IR passes, eager
//! locations and the channel table.
//!
//! These are generic seams. Core Hydro implements no policy on top of them: a fault injector, a
//! network tracer or a test oracle lives in its own crate and plugs in here. The host-side
//! entry points ([`SimFlow::with_network_hook_points`](super::flow::SimFlow::with_network_hook_points),
//! [`SimFlow::with_ir_pass`](super::flow::SimFlow::with_ir_pass),
//! [`SimFlow::with_eager_location`](super::flow::SimFlow::with_eager_location),
//! [`CompiledSim::set_network_hook`](super::compiled::CompiledSim::set_network_hook) and the
//! channel/location tables) require the `hydrofrac` feature. Without it, simulator codegen and the
//! scheduler are unchanged.
//!
//! # Network interception
//!
//! When a flow opts in with `with_network_hook_points`, every `Network` send in the simulator
//! (other than embedded, non-bincode channels and multi-version splices) calls
//! [`SimNetworkHook::on_send`] with the serialized payload. The hook either drops the message or
//! returns an opaque `u64` tag, which travels with the message on the simulator's in-memory wire
//! and is handed back to [`SimNetworkHook::on_recv`] when the receiver pulls the message. Every
//! external output (`sim_output`) likewise calls [`SimNetworkHook::on_output`], which may drop it.
//! The scheduler reports execution boundaries ([`SimNetworkHook::on_exec_begin`] /
//! [`SimNetworkHook::on_exec_end`]), and a hook can permanently halt a location member through
//! [`SimNetworkHook::take_halts`] (the same mechanism crash-fault injection uses).
//!
//! All callbacks arrive in program order on the simulation thread, so a hook can attribute each
//! receive and send to the execution it happens in.
//!
//! # Per-message metadata (for IR passes)
//!
//! An IR pass registered at [`IrPassStage::PostNetwork`] may wrap a channel's serialize closure
//! and call [`rt::set_send_meta`] with opaque bytes just before returning; the send that
//! immediately follows hands those bytes to [`SimNetworkHook::on_send`]. On the receiving side, a
//! wrapped deserialize closure can read the message's tag with [`rt::recv_tag`]. Serialize and
//! send (and receive and deserialize) are fused per item in the generated dataflow, so the slots
//! always pair a value with its own message; a second `set_send_meta` before the send panics in
//! debug builds rather than mis-attributing metadata.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::compile::ir::HydroIrOpMetadata;
use crate::location::dynamic::LocationId;
use crate::networking::NetworkingInfo;

/// What kind of execution the scheduler is about to run (see [`SimNetworkHook::on_exec_begin`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecKind {
    /// One run of a location's top-level (asynchronous) dataflow.
    Async,
    /// One tick of a tick-scoped dataflow at the location.
    Tick,
}

/// Callbacks a simulator network hook receives. See the [module docs](self).
///
/// All methods take `&self` (use interior mutability) because callbacks are delivered from
/// generated dataflow code while the scheduler is running; they are never re-entrant.
pub trait SimNetworkHook {
    /// Called at the start of every simulation instance (each execution of an exhaustive or
    /// fuzzing search), before any dataflow runs.
    fn on_instance_begin(&self) {}

    /// A message is being sent on network channel `channel` (the [`ChannelInfo::id`]) from
    /// `from_member` (the sending cluster member, `None` for a process) to `to_member` (the
    /// receiving cluster member, `None` for a process). `meta` is whatever the serialize closure
    /// passed to [`rt::set_send_meta`] for this message, if anything.
    ///
    /// Return `Some(tag)` to deliver the message carrying `tag`, or `None` to drop it.
    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> Option<u64>;

    /// The receiver (`to_member`, `None` for a process) is pulling a message carrying `tag` off
    /// network channel `channel`.
    fn on_recv(&self, _channel: usize, _to_member: Option<u32>, _tag: u64) {}

    /// A value is being emitted on external output `port` by `member` (`None` for a process).
    /// Return `false` to drop it.
    fn on_output(
        &self,
        _port: usize,
        _member: Option<u32>,
        _payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> bool {
        true
    }

    /// The scheduler is about to run an execution of `kind` at `location` (`member` for a
    /// cluster member).
    fn on_exec_begin(&self, _location: &LocationId, _member: Option<u32>, _kind: ExecKind) {}

    /// The execution announced by the last [`Self::on_exec_begin`] finished. `progressed` is
    /// `false` for an asynchronous run that found no work (no sends or receives happened in it).
    fn on_exec_end(&self, _progressed: bool) {}

    /// Locations (and cluster members) the hook wants permanently halted, polled by the scheduler
    /// after every step. A halted member runs no further dataflow, so it sends nothing more; its
    /// inbound channels stay open.
    fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
        vec![]
    }
}

/// Transport fault model of a network channel, as declared by its networking config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    /// `fail_stop` TCP: messages are lost only if their sender crashes.
    FailStop,
    /// A lossy channel (`lossy_delayed_forever` TCP or UDP): messages may also be omitted.
    Lossy,
}

impl Transport {
    pub(crate) fn of(info: &NetworkingInfo) -> Self {
        use crate::networking::TcpFault;
        match info {
            NetworkingInfo::Tcp {
                fault: TcpFault::FailStop,
            } => Transport::FailStop,
            NetworkingInfo::Tcp { .. } | NetworkingInfo::Udp { .. } => Transport::Lossy,
        }
    }
}

/// One network channel of a compiled simulation.
#[derive(Clone)]
pub struct ChannelInfo {
    /// The channel id passed to [`SimNetworkHook::on_send`] / [`SimNetworkHook::on_recv`].
    pub id: usize,
    /// The channel's name, if the program gave it one.
    pub name: Option<String>,
    /// The sending location (a process or cluster).
    pub from: LocationId,
    /// The receiving location (a process or cluster).
    pub to: LocationId,
    /// The transport's fault model.
    pub transport: Transport,
    /// Whether [`SimNetworkHook::on_send`] is called for this channel. False for embedded
    /// (non-bincode) channels, and for every channel when hook points are off.
    pub intercepted: bool,
    /// Operator metadata of the network's receiving node (its source location, for reports).
    pub op: HydroIrOpMetadata,
}

impl std::fmt::Debug for ChannelInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelInfo")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("transport", &self.transport)
            .field("intercepted", &self.intercepted)
            .finish_non_exhaustive()
    }
}

/// One external output of a compiled simulation, as seen by [`SimNetworkHook::on_output`].
#[derive(Debug, Clone)]
pub struct OutputInfo {
    /// The external port id passed to [`SimNetworkHook::on_output`].
    pub port: usize,
    /// The location emitting on the port.
    pub location: LocationId,
}

/// One process or cluster of a compiled simulation.
#[derive(Debug, Clone)]
pub struct LocationInfo {
    /// The location.
    pub id: LocationId,
    /// The location's name (its Rust type name).
    pub name: String,
    /// Member ids, for a cluster; `None` for a process.
    pub members: Option<Vec<u32>>,
    /// Whether the location is eager (see `SimFlow::with_eager_location`).
    pub eager: bool,
}

/// When an IR pass registered with `SimFlow::with_ir_pass` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IrPassStage {
    /// Before networks are compiled: a pass here may add `Network` nodes (e.g. observation taps)
    /// and they are compiled like the program's own.
    PreNetwork,
    /// After networks are compiled (and multi-version networks spliced), just before codegen: a
    /// pass here sees the final IR, including every network's serialize/deserialize closures.
    PostNetwork,
}

/// Runtime support called from generated simulation code. Not intended for direct use, except
/// [`rt::set_send_meta`] and [`rt::recv_tag`] from IR-pass-generated code.
///
/// This lives in the simulation dylib's own copy of `hydro_lang` (not gated by the `hydrofrac`
/// feature, so dylibs always link); the host installs its hook through the dylib's exported
/// `__hydro_set_network_hook`.
#[doc(hidden)]
pub mod rt {
    use super::*;

    thread_local! {
        static HOOK: RefCell<Option<Rc<dyn SimNetworkHook>>> = const { RefCell::new(None) };
        static SEND_META: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
        static RECV_TAG: Cell<Option<u64>> = const { Cell::new(None) };
    }

    fn hook() -> Option<Rc<dyn SimNetworkHook>> {
        HOOK.with(|h| h.borrow().clone())
    }

    /// Installs (or clears) the hook for this thread, and clears both per-message slots.
    pub fn install(hook: Option<Rc<dyn SimNetworkHook>>) {
        HOOK.with(|h| *h.borrow_mut() = hook);
        SEND_META.with(|m| *m.borrow_mut() = None);
        RECV_TAG.with(|t| t.set(None));
    }

    /// Attaches opaque metadata to the message whose serialization is in progress. Call from a
    /// wrapped serialize closure; the send that immediately follows hands it to
    /// [`SimNetworkHook::on_send`] (or [`SimNetworkHook::on_output`]).
    pub fn set_send_meta(meta: Vec<u8>) {
        SEND_META.with(|m| {
            let mut m = m.borrow_mut();
            debug_assert!(
                m.is_none(),
                "send-meta slot already occupied: two serializations ran before their sends, so \
                 metadata would be attached to the wrong message"
            );
            *m = Some(meta);
        });
    }

    fn take_send_meta() -> Option<Vec<u8>> {
        SEND_META.with(|m| m.borrow_mut().take())
    }

    /// The tag of the message whose deserialization is in progress (set by the receive that
    /// immediately precedes it). Taken: a second call for the same message returns `None`.
    pub fn recv_tag() -> Option<u64> {
        RECV_TAG.with(|t| t.take())
    }

    /// Generated at every intercepted send. `None` drops the message.
    pub fn on_send(
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
    ) -> Option<u64> {
        let meta = take_send_meta();
        match hook() {
            Some(h) => h.on_send(channel, from_member, to_member, payload, meta),
            None => Some(0),
        }
    }

    /// Generated at every intercepted receive.
    pub fn on_recv(channel: usize, to_member: Option<u32>, tag: u64) {
        RECV_TAG.with(|t| t.set(Some(tag)));
        if let Some(h) = hook() {
            h.on_recv(channel, to_member, tag);
        }
    }

    /// Generated at every intercepted external output. `false` drops the value.
    pub fn on_output(port: usize, member: Option<u32>, payload: &[u8]) -> bool {
        let meta = take_send_meta();
        match hook() {
            Some(h) => h.on_output(port, member, payload, meta),
            None => true,
        }
    }
}
