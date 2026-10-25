//! REVIEWER-C: adversarial tests of what hydrofrac observes and models. Each test runs
//! hydrofrac (and its brute-force oracle) and then a hand-written enumeration of a fault the
//! model cannot express, and shows they disagree.
//!
//! Every item is `cfg(stageleft_runtime)`, like the other test modules of `check`.

#[cfg(stageleft_runtime)]
use std::cell::RefCell;
#[cfg(stageleft_runtime)]
use std::collections::BTreeSet;
#[cfg(stageleft_runtime)]
use std::panic::AssertUnwindSafe;
#[cfg(stageleft_runtime)]
use std::rc::Rc;

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::quiesce;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::network_hook::SimNetworkHook;

#[cfg(stageleft_runtime)]
use super::{EcCheck, Verdict};

/// A hand-written fault: drops only the `nth` (0-based) send whose destination member is `to`
/// and whose payload is `payload`, on any channel. Every other send is delivered.
#[cfg(stageleft_runtime)]
struct DropNth {
    to: u32,
    payload: Vec<u8>,
    nth: usize,
    seen: RefCell<usize>,
    next: RefCell<u64>,
    dropped: RefCell<usize>,
}

#[cfg(stageleft_runtime)]
impl DropNth {
    fn new(to: u32, payload: Vec<u8>, nth: usize) -> Self {
        DropNth {
            to,
            payload,
            nth,
            seen: RefCell::new(0),
            next: RefCell::new(1),
            dropped: RefCell::new(0),
        }
    }
}

#[cfg(stageleft_runtime)]
impl SimNetworkHook for DropNth {
    fn on_instance_begin(&self) {
        *self.seen.borrow_mut() = 0;
        *self.next.borrow_mut() = 1;
    }

    fn on_send(
        &self,
        _channel: usize,
        _from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        let id = {
            let mut n = self.next.borrow_mut();
            *n += 1;
            *n - 1
        };
        if to_member == Some(self.to) && payload == self.payload.as_slice() {
            let k = {
                let mut s = self.seen.borrow_mut();
                *s += 1;
                *s - 1
            };
            if k == self.nth {
                *self.dropped.borrow_mut() += 1;
                return None;
            }
        }
        Some(id)
    }
}

/// **Omission of one of two identical messages.** A source process sends the payload `7`
/// twice, then (in the fault-free recovery phase) the trigger `0`, all over one lossy
/// `broadcast_closed` to three members. Each member counts the `7`s it received and, on the
/// trigger, emits the parity of that count (label `parity`).
///
/// The fault model names an omission by its `MessageId` (channel, sender, destination,
/// payload), so `omit(7 → member 1)` drops *both* copies (parity 0 again): no fault of the
/// model loses exactly one copy. Losing exactly one copy is an ordinary lossy-channel fault,
/// and it makes member 1 emit parity 1 while members 0 and 2 emit parity 0.
#[cfg(stageleft_runtime)]
#[test]
fn review_c_omitting_one_of_two_identical_messages_is_not_modelled() {
    let mut check = EcCheck::new();
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let parity = input
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("votes").bincode())
        .weaken_consistency()
        .assume_ordering::<TotalOrder>(nondet!(/** the 7s are identical and the trigger comes after a quiesce */))
        .scan(
            q!(|| 0u32),
            q!(|count, x| {
                if x == 0 {
                    Some(Some(*count % 2))
                } else {
                    *count += 1;
                    Some(None)
                }
            }),
        )
        .flatten_ordered()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: every member counts the same votes */
        ));
    let out = parity.sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check_then_recover(
        &sim,
        async || {
            send.send(7);
            send.send(7);
        },
        async || {
            send.send(0);
        },
        2,
    );
    eprintln!("=== hydrofrac\n{}\n=== cross-check\n{cross}", cross.results);
    // The label minted by the assertion is named after this function (the name `parity` goes
    // to the raw broadcast on the `let parity = ...` line, a separate label).
    let label = cross
        .results
        .label("review_c_omitting_one_of_two_identical_messages_is_not_modelled");
    let certified = matches!(label.verdict, Verdict::Certified { .. });

    // Hand enumeration: drop only the first copy of `7 → member 1`, in every schedule.
    let seven = bincode::serialize(&7u32).unwrap();
    let hook = Rc::new(DropNth::new(1, seven, 0));
    sim.set_network_hook(hook.clone());
    let broken = AssertUnwindSafe(RefCell::new(BTreeSet::new()));
    let runs = AssertUnwindSafe(RefCell::new(0usize));
    sim.exhaustive(async || {
        send.send(7);
        send.send(7);
        quiesce().await;
        send.send(0);
        quiesce().await;
        let mut held = vec![];
        for m in 0..3u32 {
            let got: Vec<u32> = out.collect(m).await;
            held.push(got);
        }
        *runs.borrow_mut() += 1;
        if held.iter().any(|h| h != &held[0]) {
            broken.borrow_mut().insert(format!("{held:?}"));
        }
    });
    sim.clear_network_hook();
    eprintln!(
        "hand enumeration: dropped {} sends over {} schedules; member views that disagree: {:?}",
        hook.dropped.borrow(),
        runs.borrow(),
        broken.borrow()
    );
    assert!(
        certified,
        "hydrofrac did not certify (so this is not a false certificate): {}",
        cross.results
    );
    assert!(
        !broken.borrow().is_empty(),
        "dropping one copy did not break the label"
    );
}

/// **A label computed from local inputs passes `assert_holds` while members disagree with no
/// fault at all.** Each member labels `[its own id]` `EventualConsistency`. The plan classifies
/// the label `NotChecked(LocalInputsOnly)` ("no fault can withhold its inputs"), and
/// [`super::Results::holds`] counts `NotChecked` as holding, so `assert_holds` passes. Yet the
/// three members end with `{0}`, `{1}` and `{2}` in the failure-free run.
#[cfg(stageleft_runtime)]
#[test]
fn review_c_local_only_label_that_disagrees_passes_assert_holds() {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut check = EcCheck::new();
    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let mine = members
        .source_iter(q!(vec![CLUSTER_SELF_ID.get_raw_id()]))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member has the same ids */
        ));
    let out = mine.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let results = check.run(&sim, async || {});
    eprintln!("=== hydrofrac\n{results}");

    let views = AssertUnwindSafe(RefCell::new(BTreeSet::new()));
    sim.exhaustive(async || {
        quiesce().await;
        let mut held = vec![];
        for m in 0..3u32 {
            let got: Vec<u32> = out.collect(m).await;
            held.push(got);
        }
        views.borrow_mut().insert(format!("{held:?}"));
    });
    eprintln!("hand run, no faults: member views {:?}", views.borrow());
    assert!(results.holds(), "hydrofrac flagged it: {results}");
    results.assert_holds();
    assert!(
        views.borrow().iter().all(|v| v == "[[0], [1], [2]]"),
        "{:?}",
        views.borrow()
    );
}
