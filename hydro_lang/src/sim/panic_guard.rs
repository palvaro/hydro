//! Carrying panics across the boundary of the simulation dylib.
//!
//! A compiled simulation is a `cdylib` with its own copy of the standard library. A panic raised
//! by program code inside it (a `map` closure that panics, an `unwrap` on a fault-injected
//! `None`) is therefore a *foreign* exception to the host: unwinding it into a host
//! `catch_unwind` (bolero's, a test's, a search's) aborts the process with "Rust cannot catch
//! foreign exceptions". The same holds the other way round for a host panic unwinding into a
//! `catch_unwind` compiled into the dylib.
//!
//! The fix keeps every unwind inside the standard library that raised it:
//! - **Program panics.** Generated code wraps every tick of every DFIR in [`guard_tick`] (and the
//!   instantiation in [`guard_runtime`]). These run in the dylib, so their `catch_unwind` is the
//!   dylib's own and catches the program's panic. They hand its message to the host's
//!   [`PanicHandler`], which raises it again as an ordinary host panic carrying a `String`
//!   payload. That host panic unwinds out of the handler, through the guard's frame (which is no
//!   longer catching), and on into the host, where `catch_unwind` and `#[should_panic]` see it
//!   like any other.
//! - **Host panics inside dylib callbacks.** The only host code the dylib calls back into while a
//!   tick runs is the installed [`SimNetworkHook`](super::network_hook::SimNetworkHook)
//!   (`on_send`, `on_recv`, `on_output`). The host wraps it in [`GuardedNetworkHook`], which
//!   catches a panic on the host side, stashes its payload and returns a harmless default
//!   (drop the message). The guard around the tick asks the handler after every poll, and the
//!   handler re-raises the stashed payload, unchanged, as soon as the tick returns control.
//!
//! Nothing changes when nothing panics: each poll costs one call to the handler, which checks an
//! empty thread-local.
//!
//! Not covered: the decision hooks ([`SimHook`](super::runtime::SimHook)) that the scheduler
//! calls between ticks. They are simulator code, not program code, and they call back into the
//! host's fuzzing driver, whose own panics (bolero's control flow) must keep unwinding through
//! them untouched.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use dfir_rs::scheduled::context::Context;

use super::network_hook::SimNetworkHook;
use crate::location::dynamic::LocationId;
use crate::sim::network_hook::ExecKind;

/// The host function the dylib reports to, passed to the dylib's `__hydro_runtime`.
///
/// `Some(message)`: program code in the dylib panicked with `message`; the handler must not return
/// (it raises the panic on the host). `None`: control is back in the dylib after a poll; the
/// handler raises any panic the host stashed meanwhile (see [`GuardedNetworkHook`]) and otherwise
/// returns.
pub type PanicHandler = fn(Option<String>);

/// The message of a caught panic payload, as `panic!` formats it.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "simulated program panicked with a non-string payload".to_owned()
    }
}

/// Reports the outcome of one guarded call to the host. Diverges if the call panicked.
fn report(handler: PanicHandler, outcome: Result<(), Box<dyn Any + Send>>) {
    match outcome {
        Ok(()) => handler(None),
        Err(payload) => {
            let message = panic_message(&*payload);
            // Free the payload with the allocator and drop glue of the code that created it,
            // before control leaves this frame.
            drop(payload);
            handler(Some(message));
            // A handler that returns after `Some` would let a half-run tick continue.
            eprintln!("hydro sim: the panic handler returned after a program panic; aborting");
            std::process::abort();
        }
    }
}

/// Polls `fut`, catching a panic raised while polling it (see the [module docs](self)).
async fn guarded<Fut: Future<Output = bool>>(fut: Fut, handler: PanicHandler) -> bool {
    let mut fut = std::pin::pin!(fut);
    std::future::poll_fn(move |cx| {
        let mut polled = None;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            polled = Some(fut.as_mut().poll(cx));
        }));
        report(handler, outcome);
        polled.expect("a guarded poll that did not panic produced a result")
    })
    .await
}

/// Wraps a DFIR tick closure so a panic raised by the tick is reported to `handler` instead of
/// unwinding out of the dylib. Called from generated code, through
/// [`Dfir::map_tick_closure`](dfir_rs::scheduled::context::Dfir::map_tick_closure).
pub fn guard_tick<F>(
    mut tick: F,
    handler: PanicHandler,
) -> impl 'static + for<'a> AsyncFnMut(&'a mut Context) -> bool
where
    F: 'static + for<'a> AsyncFnMut(&'a mut Context) -> bool,
{
    async move |ctx: &mut Context| -> bool { guarded(tick(ctx), handler).await }
}

/// Runs `f` (the dylib's instantiation of the program), reporting a panic to `handler` instead
/// of unwinding out of the dylib.
pub fn guard_runtime<R>(handler: PanicHandler, f: impl FnOnce() -> R) -> R {
    let mut result = None;
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        result = Some(f());
    }));
    report(handler, outcome);
    result.expect("a guarded call that did not panic produced a result")
}

thread_local! {
    /// A host panic caught inside a dylib callback, waiting to be raised again once the dylib
    /// returns control (host side only).
    static PENDING: RefCell<Option<Box<dyn Any + Send>>> = const { RefCell::new(None) };
}

/// The host's [`PanicHandler`]: raises a program panic, or a stashed host panic, as an ordinary
/// host panic.
pub(crate) fn host_handler(message: Option<String>) {
    if let Some(message) = message {
        // A stashed host panic is superseded: the program panic is what ended the tick.
        PENDING.with(|p| p.borrow_mut().take());
        std::panic::resume_unwind(Box::new(message));
    }
    if let Some(payload) = PENDING.with(|p| p.borrow_mut().take()) {
        std::panic::resume_unwind(payload);
    }
}

/// Forgets any stashed host panic (at the start of an instance).
pub(crate) fn clear_pending() {
    PENDING.with(|p| p.borrow_mut().take());
}

/// Runs host code called back from the dylib. A panic is caught here, on the host side, and
/// stashed for [`host_handler`]; the dylib gets `default` instead. Only the first panic is kept.
fn catch_host<R>(default: R, f: impl FnOnce() -> R) -> R {
    match std::panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            PENDING.with(|p| {
                let mut p = p.borrow_mut();
                if p.is_none() {
                    *p = Some(payload);
                }
            });
            default
        }
    }
}

/// The installed network hook, as the dylib sees it: the callbacks the dylib makes are guarded
/// with [`catch_host`]. The host calls the others directly on the inner hook.
pub(crate) struct GuardedNetworkHook(pub(crate) Rc<dyn SimNetworkHook>);

impl SimNetworkHook for GuardedNetworkHook {
    fn on_instance_begin(&self) {
        self.0.on_instance_begin()
    }

    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        catch_host(None, || {
            self.0
                .on_send(channel, from_member, to_member, payload, meta)
        })
    }

    fn on_recv(&self, channel: usize, to_member: Option<u32>, tag: u64) {
        catch_host((), || self.0.on_recv(channel, to_member, tag))
    }

    fn on_output(
        &self,
        port: usize,
        member: Option<u32>,
        payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> bool {
        catch_host(false, || self.0.on_output(port, member, payload, meta))
    }

    fn on_exec_begin(&self, location: &LocationId, member: Option<u32>, kind: ExecKind) {
        self.0.on_exec_begin(location, member, kind)
    }

    fn on_exec_end(&self, progressed: bool) {
        self.0.on_exec_end(progressed)
    }

    fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
        self.0.take_halts()
    }
}
