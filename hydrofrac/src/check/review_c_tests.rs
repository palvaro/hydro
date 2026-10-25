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
        !broken.borrow().is_empty(),
        "dropping one copy did not break the label"
    );
    // Fixed: omissions name copies, so the search and both oracles lose one copy.
    assert!(!certified, "{cross}");
    super::review2a_tests::assert_found(
        &cross,
        "review_c_omitting_one_of_two_identical_messages_is_not_modelled",
        1,
        0,
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

/// A hand-written crash: halts the sender of channel `channel` right after its `nth` (1-based)
/// send of `payload` to member `to`; from then on every send on that channel is dropped.
#[cfg(stageleft_runtime)]
struct CrashAfterNth {
    channel: usize,
    sender: hydro_lang::location::dynamic::LocationId,
    to: u32,
    payload: Vec<u8>,
    nth: usize,
    seen: RefCell<usize>,
    dead: RefCell<bool>,
    halt: RefCell<bool>,
    next: RefCell<u64>,
    fired: RefCell<usize>,
}

#[cfg(stageleft_runtime)]
impl SimNetworkHook for CrashAfterNth {
    fn on_instance_begin(&self) {
        *self.seen.borrow_mut() = 0;
        *self.dead.borrow_mut() = false;
        *self.halt.borrow_mut() = false;
        *self.next.borrow_mut() = 1;
    }

    fn on_send(
        &self,
        channel: usize,
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
        if channel != self.channel {
            return Some(id);
        }
        if *self.dead.borrow() {
            return None;
        }
        if to_member == Some(self.to) && payload == self.payload.as_slice() {
            let k = {
                let mut s = self.seen.borrow_mut();
                *s += 1;
                *s
            };
            if k == self.nth {
                *self.dead.borrow_mut() = true;
                *self.halt.borrow_mut() = true;
                *self.fired.borrow_mut() += 1;
            }
        }
        Some(id)
    }

    fn take_halts(&self) -> Vec<(hydro_lang::location::dynamic::LocationId, Option<u32>)> {
        if std::mem::take(&mut *self.halt.borrow_mut()) {
            vec![(self.sender.clone(), None)]
        } else {
            vec![]
        }
    }
}

/// **A crash between two identical messages is not modelled.** Process `S` sends member 1 the
/// update `7` twice, then member 2 the update `5` (one `fail_stop` demux channel `updates`).
/// After the heal, process `R` broadcasts a trigger; on it every member reports how many `7`s
/// it counted to every member (`reports`). The label `agreed` holds `1` at member 0 and 1 if
/// some member reported 2, and at member 2 if some member reported 2 *and* it got the `5`.
///
/// `crash(S after m)` names `m` by its `MessageId`, and the hook fires it at the first send of
/// `m`: the crash points of `S` cut its sends to `[]`, `[7→1]` or `[7→1, 7→1, 5→2]`. No fault
/// of the model cuts them to `[7→1, 7→1]`, which is a real single crash of `S` (right after its
/// second send): member 1 counts 2, so members 0 and 1 hold `1`, but member 2 never got the `5`.
#[cfg(stageleft_runtime)]
#[test]
fn review_c_crash_between_two_identical_messages_is_not_modelled() {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut check = EcCheck::new().max_crashes(2);
    let mut flow = FlowBuilder::new();
    let s = flow.process::<()>();
    let r = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_s, in_s) = s.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let (send_r, in_r) = r.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let from_s = in_s
        .map(q!(|(d, v)| (hydro_lang::location::MemberId::<()>::from_raw_id(d), v)))
        .demux(&members, TCP.fail_stop().name("updates").bincode());
    let trigger = in_r
        .broadcast_closed(&members, TCP.fail_stop().name("trigger").bincode())
        .weaken_consistency();
    let counted = from_s
        .clone()
        .filter(q!(|v| *v == 7))
        .map(q!(|_| 0u8))
        .weaken_ordering::<NoOrder>()
        .merge_unordered(trigger.map(q!(|_| 1u8)).weaken_ordering::<NoOrder>())
        .assume_ordering::<TotalOrder>(nondet!(/** the trigger comes after a quiesce */))
        .scan(
            q!(|| 0u32),
            q!(|c, x| {
                if x == 0 {
                    *c += 1;
                    Some(None)
                } else {
                    Some(Some(*c))
                }
            }),
        )
        .flatten_ordered();
    let two = counted
        .broadcast_closed(&members, TCP.fail_stop().name("reports").bincode())
        .values()
        .weaken_consistency()
        .filter(q!(|c| *c == 2));
    let five = from_s.filter(q!(|v| *v == 5)).weaken_ordering::<NoOrder>();
    let at_two = two
        .clone()
        .cross_product(five)
        .map(q!(|_| 1u32))
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 2));
    let elsewhere = two
        .map(q!(|_| 1u32))
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() != 2));
    let agreed = elsewhere
        .merge_unordered(at_two)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: member 2 got the 5 whenever member 1 counted two 7s */
        ));
    let out = agreed.sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check_then_recover(
        &sim,
        async || {
            send_s.send((1, 7));
            send_s.send((1, 7));
            send_s.send((2, 5));
        },
        async || {
            send_r.send(0);
        },
        2,
    );
    eprintln!("=== hydrofrac\n{}\n=== cross-check\n{cross}", cross.results);
    let label = cross.results.label("agreed");
    let verdict = format!("{:?}", label.verdict);

    // Hand enumeration: `S` crashes right after its second send of `7` to member 1.
    let updates = sim
        .channels()
        .iter()
        .find(|c| c.name.as_deref() == Some("updates"))
        .expect("the updates channel")
        .clone();
    let hook = Rc::new(CrashAfterNth {
        channel: updates.id,
        sender: updates.from.clone(),
        to: 1,
        payload: bincode::serialize(&7u32).unwrap(),
        nth: 2,
        seen: RefCell::new(0),
        dead: RefCell::new(false),
        halt: RefCell::new(false),
        next: RefCell::new(1),
        fired: RefCell::new(0),
    });
    sim.set_network_hook(hook.clone());
    let views = AssertUnwindSafe(RefCell::new(BTreeSet::new()));
    let runs = AssertUnwindSafe(RefCell::new(0usize));
    sim.exhaustive(async || {
        send_s.send((1, 7));
        send_s.send((1, 7));
        send_s.send((2, 5));
        quiesce().await;
        send_r.send(0);
        quiesce().await;
        let mut held = vec![];
        for m in 0..3u32 {
            let mut got: Vec<u32> = out.collect_sorted(m).await;
            got.sort();
            got.dedup();
            held.push(got);
        }
        *runs.borrow_mut() += 1;
        views.borrow_mut().insert(format!("{held:?}"));
    });
    sim.clear_network_hook();
    eprintln!(
        "hand enumeration: crash fired in {} of {} schedules; member views {:?}",
        hook.fired.borrow(),
        runs.borrow(),
        views.borrow()
    );
    assert!(
        views.borrow().contains("[[1], [1], []]"),
        "the crash did not break the label: {:?}",
        views.borrow()
    );
    // Fixed: crash points name copies, so `crash(S after 7#1 → member 1)` is a fault of the
    // search and of the brute force, as oracle 2's ordinal crash points always were.
    eprintln!("agreed: {verdict}");
    super::review2a_tests::assert_found(&cross, "agreed", 1, 1);
}

/// **The choice-free shortcut treats a count-sensitive `scan` as robust, and the crash rule
/// treats a message sent twice as sent once.** Two members (so no member crash is admissible).
/// Process `S` sends member 1 the update `7` twice (`fail_stop` demux). Member 1 scans the
/// updates and emits the running count (`1`, then `2`), and gossips each count to both members
/// (`fail_stop`). Process `T` sends member 0 the value `2` directly, which member 0 keeps once
/// it has heard a count of `1`. The label `known` is the gossip plus member 0's direct value.
///
/// The program is choice-free, so every derivation counts as robust. Member 1's derivation of
/// `2` uses only one message of `S`, `7 → member 1` (its two copies share a `MessageId`), so by
/// the crash rule `crash(S after 7 → member 1)` spares it, and the search prunes that set. But
/// the hook fires the crash at the *first* copy: member 1 counts to 1 only, member 0 still
/// holds `2` (direct), and member 1 never does. The brute-force enumeration runs the set and
/// finds it.
#[cfg(stageleft_runtime)]
#[test]
fn review_c_choice_free_scan_over_a_repeated_message_is_pruned_wrongly() {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut check = EcCheck::new();
    let mut flow = FlowBuilder::new();
    let s = flow.process::<()>();
    let t = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_s, in_s) = s.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let (send_t, in_t) = t.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let counts = in_s
        .map(q!(|(d, v)| (hydro_lang::location::MemberId::<()>::from_raw_id(d), v)))
        .demux(&members, TCP.fail_stop().name("updates").bincode())
        .scan(
            q!(|| 0u32),
            q!(|c, _x| {
                *c += 1;
                Some(*c)
            }),
        );
    let heard = counts
        .broadcast_closed(&members, TCP.fail_stop().name("gossip").bincode())
        .values()
        .weaken_consistency();
    let direct = in_t
        .map(q!(|(d, v)| (hydro_lang::location::MemberId::<()>::from_raw_id(d), v)))
        .demux(&members, TCP.fail_stop().name("direct").bincode())
        .weaken_ordering::<NoOrder>()
        .cross_product(heard.clone().filter(q!(|c| *c == 1)))
        .map(q!(|(v, _)| v))
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0));
    let known = heard
        .merge_unordered(direct)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: member 1 counts every update it is sent */
        ));
    let _out = known.sim_cluster_output();

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_s.send((1, 7));
            send_s.send((1, 7));
            send_t.send((0, 2));
        },
        2,
    );
    eprintln!("=== hydrofrac\n{}\n=== cross-check\n{cross}", cross.results);
    // Fixed: member 1's derivation of `2` uses `7#1`, so it keeps the `scan`'s premise and
    // protects nothing; the search runs `crash(S after 7 → member 1)` and finds it.
    super::review2a_tests::assert_found(&cross, "known", 1, 1);
}
