//! A lineage-checking network hook, for tests of the rewrite on whole programs (here and in
//! crates whose programs it instruments). Not part of the API.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::network_hook::SimNetworkHook;

use super::{RecordProv, install};

/// What one program's runs handed the hook.
#[derive(Default, Debug)]
pub struct Stats {
    pub instances: usize,
    pub sends: usize,
    pub opaque_sends: usize,
    pub outputs: usize,
    pub opaque_outputs: usize,
    pub islands: Vec<String>,
}

/// Counts lineage and checks it is well-formed: every metadata decodes, and every delivery a
/// lineage names was sent earlier in the same run.
#[derive(Default)]
pub struct Checker {
    next: std::cell::Cell<u64>,
    sent: RefCell<BTreeSet<u64>>,
    pub stats: RefCell<Stats>,
}

impl Checker {
    fn check(&self, meta: Option<Vec<u8>>, what: &str) -> bool {
        let meta = meta.unwrap_or_else(|| panic!("{what} carried no lineage"));
        match RecordProv::decode(&meta) {
            RecordProv::Opaque => true,
            RecordProv::Ids(ids) => {
                let sent = self.sent.borrow();
                for id in ids {
                    assert!(
                        sent.contains(&id),
                        "{what} names delivery {id}, which was never sent in this run"
                    );
                }
                false
            }
        }
    }
}

impl SimNetworkHook for Checker {
    fn on_instance_begin(&self) {
        self.next.set(1);
        self.sent.borrow_mut().clear();
        self.stats.borrow_mut().instances += 1;
    }

    fn on_send(
        &self,
        _channel: usize,
        _from_member: Option<u32>,
        _to_member: Option<u32>,
        _payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        let opaque = self.check(meta, "a send");
        let mut stats = self.stats.borrow_mut();
        stats.sends += 1;
        stats.opaque_sends += usize::from(opaque);
        let tag = self.next.get();
        self.next.set(tag + 1);
        self.sent.borrow_mut().insert(tag);
        Some(tag)
    }

    fn on_recv(&self, _channel: usize, _to_member: Option<u32>, tag: u64) {
        assert!(
            self.sent.borrow().contains(&tag),
            "received unsent delivery {tag}"
        );
    }

    fn on_output(
        &self,
        _port: usize,
        _member: Option<u32>,
        _payload: &[u8],
        meta: Option<Vec<u8>>,
    ) -> bool {
        let opaque = self.check(meta, "an output");
        let mut stats = self.stats.borrow_mut();
        stats.outputs += 1;
        stats.opaque_outputs += usize::from(opaque);
        true
    }
}

/// Compiles `flow` with the rewrite and a [`Checker`], and runs `iterations` sampled
/// schedules of `thunk`.
pub fn run_checked(
    name: &str,
    flow: SimFlow<'_>,
    iterations: usize,
    thunk: impl AsyncFnMut() + std::panic::RefUnwindSafe,
) -> Stats {
    let islands = Rc::new(RefCell::new(Vec::new()));
    let compiled = install(flow, Some(islands.clone())).compiled();
    let hook = Rc::new(Checker::default());
    compiled.set_network_hook(hook.clone());
    compiled.fuzz_bounded(iterations, thunk);
    let mut stats = std::mem::take(&mut *hook.stats.borrow_mut());
    stats.islands = islands.borrow().clone();
    eprintln!("provenance: {name}: {stats:?}");
    assert!(stats.instances >= iterations, "{name}: {stats:?}");
    assert!(stats.sends > 0 && stats.outputs > 0, "{name}: {stats:?}");
    stats
}
