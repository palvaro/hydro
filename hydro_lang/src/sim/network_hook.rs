//! Extension points for external simulator tooling (fault injection, network tracing, test
//! oracles). Core implements no policy on top of them. The host-side entry points require the
//! `hydrofrac` feature; without them, simulator codegen and the scheduler are unchanged.
//!
//! With [`SimFlow::with_network_hook_points`](super::flow::SimFlow::with_network_hook_points),
//! every bincode `Network` send, receive and external output calls the installed
//! [`SimNetworkHook`], which may drop sends and outputs and tag deliveries. All callbacks arrive
//! in program order on the simulation thread.
//!
//! A [`IrPassStage::PostNetwork`] pass may wrap a serialize closure to call
//! [`rt::set_send_meta`]; the send that immediately follows hands those bytes to the hook. A
//! wrapped deserialize reads the delivery's tag with [`rt::recv_tag`].

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::compile::ir::HydroIrOpMetadata;
use crate::location::dynamic::LocationId;
use crate::networking::NetworkingInfo;

/// Callbacks a simulator network hook receives. They take `&self` because they are delivered
/// from generated dataflow code; they are never re-entrant.
pub trait SimNetworkHook {
    /// Called at the start of every simulation instance, before any dataflow runs.
    fn on_instance_begin(&self) {}

    /// A message is being sent on channel `channel` ([`ChannelInfo::id`]) between cluster
    /// members (`None` for a process). `meta` is what the serialize closure passed to
    /// [`rt::set_send_meta`], if anything. Return `Some(tag)` to deliver it carrying `tag`, or
    /// `None` to drop it.
    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> Option<u64>;

    /// The receiver is pulling a message carrying `tag` off channel `channel`.
    fn on_recv(&self, _channel: usize, _to_member: Option<u32>, _tag: u64) {}

    /// A value is being emitted on external output `port`. Return `false` to drop it.
    fn on_output(
        &self,
        _port: usize,
        _member: Option<u32>,
        _payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> bool {
        true
    }

    /// The scheduler is about to run dataflow at `location` (`member` of a cluster).
    fn on_exec_begin(&self, _location: &LocationId, _member: Option<u32>) {}

    /// The execution announced by the last [`Self::on_exec_begin`] finished.
    fn on_exec_end(&self) {}

    /// Members to halt permanently, polled after every scheduler step. A halted member runs no
    /// further dataflow; its inbound channels stay open.
    fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
        vec![]
    }
}

/// One network channel of a compiled simulation.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// The id passed to [`SimNetworkHook::on_send`] and [`SimNetworkHook::on_recv`].
    pub id: usize,
    /// The name the program gave the channel, if any.
    pub name: Option<String>,
    /// The sending process or cluster.
    pub from: LocationId,
    /// The receiving process or cluster.
    pub to: LocationId,
    /// The channel's declared networking (transport and fault model).
    pub networking: NetworkingInfo,
    /// Whether the hook sees this channel's messages: false for embedded (non-bincode)
    /// channels, and for every channel without hook points.
    pub intercepted: bool,
    /// The receiving node's operator metadata (its source location).
    pub op: HydroIrOpMetadata,
}

/// One process or cluster of a compiled simulation.
#[derive(Debug, Clone)]
pub struct LocationInfo {
    /// The location.
    pub id: LocationId,
    /// The location's Rust type name.
    pub name: String,
    /// The member ids of a cluster; `None` for a process.
    pub members: Option<Vec<u32>>,
}

/// When an IR pass registered with `SimFlow::with_ir_pass` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IrPassStage {
    /// Before networks are compiled, so `Network` nodes it adds are compiled too.
    PreNetwork,
    /// On the final IR, just before codegen.
    PostNetwork,
}

/// Runtime support for generated simulation code, in the dylib's copy of `hydro_lang` (not
/// feature-gated, so dylibs always link). IR-pass code may call [`set_send_meta`] and
/// [`recv_tag`].
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

    /// Attaches opaque metadata to the message being serialized.
    pub fn set_send_meta(meta: Vec<u8>) {
        SEND_META.with(|m| {
            let mut m = m.borrow_mut();
            debug_assert!(
                m.is_none(),
                "send-meta slot already occupied: two serializations ran before their sends"
            );
            *m = Some(meta);
        });
    }

    fn take_send_meta() -> Option<Vec<u8>> {
        SEND_META.with(|m| m.borrow_mut().take())
    }

    /// The tag of the message being deserialized. Taken: a second call returns `None`.
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

    /// Generated at every external output. `false` drops the value.
    pub fn on_output(port: usize, member: Option<u32>, payload: &[u8]) -> bool {
        let meta = take_send_meta();
        match hook() {
            Some(h) => h.on_output(port, member, payload, meta),
            None => true,
        }
    }
}
