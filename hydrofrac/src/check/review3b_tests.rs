//! REVIEWER-3B (review 3): adversarial cross-checks of the races search after the per-copy
//! messages, the Claim B sweep and the solver-row cache. Each test pins a wrong verdict
//! (a false certificate) next to the oracles that break it, or pins a verdict that held up.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{EcCheck, Verdict};

#[cfg(stageleft_runtime)]
const MAX_FAULTS: usize = 2;

/// Finding 1: a send that is "after the heal" in one run and "before the heal" in another.
///
/// Process `q` gets input 1 in the fault phase and passes it to process `p` over a lossy
/// channel (`to_p`). Process `p` gets input 2 in the recovery phase. `p` maps both to the
/// same value 7, keeps only the first (`unique`), and broadcasts it to two members over a lossy
/// channel (`data`). The label is what the members receive.
///
/// - Without faults, `p` hears 1 before the heal and sends 7 to both members then; the
///   recovery input 2 is a duplicate, and `unique` drops it.
/// - If `to_p` loses 1, `p` first makes 7 from input 2, after the heal, and the broadcast then
///   cannot be lost. Lineage marks that delivery "not lossy" (`lossy = before_heal && ...`),
///   so the clause it gives is robust and only a crash of `p` kills it.
/// - That clause then protects the pair against `omit(7 → m1)`: but in the failure-free order
///   that omission drops the only copy `m1` ever gets (the recovery copy is a duplicate that
///   `unique` drops), so `m1` misses 7 while `m0` holds it.
///
/// The program is fragile-free (`unique`, maps, merges, networks), so Claim B is not swept.
#[cfg(stageleft_runtime)]
fn heal_flips_lossiness(check: &mut EcCheck) -> (
    hydro_lang::sim::compiled::CompiledSim,
    impl AsyncFnMut() + use<>,
    impl AsyncFnMut() + use<>,
) {
    let mut flow = FlowBuilder::new();
    let q = flow.process::<()>();
    let p = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = q.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = p.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_p = in_a
        .send(&p, TCP.lossy_delayed_forever().name("to_p").bincode())
        .weaken_ordering::<NoOrder>()
        .merge_unordered(in_b.weaken_ordering::<NoOrder>())
        .map(q!(|_| 7u32))
        .unique();
    let delivered = at_p
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("data").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): the recovery input re-sends 7 */
        ));
    let _out = delivered.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    (
        sim,
        async move || send_a.send(1),
        async move || send_b.send(2),
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review3b_a_post_heal_copy_protects_a_pre_heal_loss() {
    let mut check = EcCheck::new().max_crashes(0);
    let (sim, mut inputs, mut recover) = heal_flips_lossiness(&mut check);
    let cross = check.cross_check_then_recover(&sim, &mut inputs, &mut recover, MAX_FAULTS);
    eprintln!("=== heal flips lossiness\n{}\n{cross}", cross.results);
    // The false certificate: `assert_holds` accepts it.
    assert!(matches!(
        cross.results.results()[0].verdict,
        Verdict::Certified { .. }
    ));
    cross.results.assert_holds();
    // Both oracles break it with the one omission of 7 to member 0.
    let brute = &cross.brute.labels[0];
    assert_eq!(brute.min_crashes(), Some(0), "{cross}");
    let breaking = brute.breaking_within(0).expect("brute force breaks it");
    assert_eq!(breaking.len(), 1, "{cross}");
    assert_eq!(cross.disagreements.len(), 2, "{cross}");
    assert!(cross.disagreements[1].starts_with("oracle 2:"), "{cross}");
}

/// A hand-written fault set of three faults, beyond the oracles' two: drop every send on
/// channel `data` to members `drop_to`, and halt member `crash` right after its send on
/// channel `echo` to member `crash_after_to` (its later sends on `echo` are dropped).
#[cfg(stageleft_runtime)]
struct ThreeFaults {
    data: usize,
    drop_to: Vec<u32>,
    echo: usize,
    cluster: hydro_lang::location::dynamic::LocationId,
    crash: u32,
    crash_after_to: u32,
    dead: std::cell::RefCell<bool>,
    halt: std::cell::RefCell<bool>,
    next: std::cell::RefCell<u64>,
    dropped: std::cell::RefCell<usize>,
    fired: std::cell::RefCell<usize>,
}

#[cfg(stageleft_runtime)]
impl hydro_lang::sim::network_hook::SimNetworkHook for ThreeFaults {
    fn on_instance_begin(&self) {
        *self.dead.borrow_mut() = false;
        *self.halt.borrow_mut() = false;
        *self.next.borrow_mut() = 1;
    }

    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        _payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        let id = {
            let mut n = self.next.borrow_mut();
            *n += 1;
            *n - 1
        };
        if channel == self.data && to_member.is_some_and(|m| self.drop_to.contains(&m)) {
            *self.dropped.borrow_mut() += 1;
            return None;
        }
        if channel == self.echo && from_member == Some(self.crash) {
            if *self.dead.borrow() {
                return None;
            }
            if to_member == Some(self.crash_after_to) {
                *self.dead.borrow_mut() = true;
                *self.halt.borrow_mut() = true;
                *self.fired.borrow_mut() += 1;
            }
        }
        Some(id)
    }

    fn take_halts(&self) -> Vec<(hydro_lang::location::dynamic::LocationId, Option<u32>)> {
        if std::mem::take(&mut *self.halt.borrow_mut()) {
            vec![(self.cluster.clone(), Some(self.crash))]
        } else {
            vec![]
        }
    }
}

/// Finding 2 (the oracles' blind spot): the mechanism of finding 1, in a program whose every
/// breaking fault set has three faults, so neither oracle (two faults at most) can see it.
///
/// As [`heal_flips_lossiness`], plus: three members, and each member echoes what it got from
/// `p` to every member over `fail_stop` (`echo`). The label is what a member got from `p` or
/// from an echo. With at most one crash, a member misses 7 while another correct member holds
/// it only if: `p`'s 7 to member 1 and to member 2 are both lost, and member 0 crashes right
/// after echoing 7 to member 1 (before its echo to member 2). Then member 1 holds 7 and member
/// 2 never gets it. Every fault set of two faults or fewer leaves member 2 a copy from `p` or
/// an echo from a member that holds 7.
#[cfg(stageleft_runtime)]
fn three_fault_break(recovery_resends: bool) -> (Verdict, std::collections::BTreeSet<String>) {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::panic::AssertUnwindSafe;
    use std::rc::Rc;

    use hydro_lang::sim::compiled::quiesce;

    let mut check = EcCheck::new().max_crashes(1);
    let mut flow = FlowBuilder::new();
    let q = flow.process::<()>();
    let p = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = q.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = p.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_p = in_a
        .send(&p, TCP.lossy_delayed_forever().name("to_p").bincode())
        .weaken_ordering::<NoOrder>()
        .merge_unordered(in_b.weaken_ordering::<NoOrder>())
        .map(q!(|_| 7u32))
        .unique();
    let direct = at_p
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("data").bincode())
        .weaken_consistency();
    let echoed = direct
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let delivered = direct
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed.weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): the recovery input re-sends 7, and the echo covers the rest */
        ));
    let out = delivered.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check_then_recover(
        &sim,
        async || send_a.send(1),
        async || {
            if recovery_resends {
                send_b.send(2);
            }
        },
        MAX_FAULTS,
    );
    eprintln!("=== three-fault break\n{}\n{cross}", cross.results);
    let label = cross
        .results
        .label("delivered");
    eprintln!("verdict: {:?}", label.verdict);
    let verdict = label.verdict.clone();
    // Neither oracle breaks `delivered` with two faults or fewer.
    let name = "delivered (review3b_tests.rs)";
    assert!(
        cross.disagreements.iter().all(|d| !d.contains(name)),
        "{cross}"
    );
    let brute = cross
        .brute
        .labels
        .iter()
        .find(|l| l.name.starts_with("delivered"))
        .expect("delivered was enumerated");
    assert!(brute.breaking.is_empty(), "{cross}");

    // Hand enumeration of the three-fault set, in every schedule.
    let channel = |name: &str| {
        sim.channels()
            .iter()
            .find(|c| c.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("channel {name}"))
            .clone()
    };
    let data = channel("data");
    let echo = channel("echo");
    let hook = Rc::new(ThreeFaults {
        data: data.id,
        drop_to: vec![1, 2],
        echo: echo.id,
        cluster: echo.from.clone(),
        crash: 0,
        crash_after_to: 1,
        dead: RefCell::new(false),
        halt: RefCell::new(false),
        next: RefCell::new(1),
        dropped: RefCell::new(0),
        fired: RefCell::new(0),
    });
    sim.set_network_hook(hook.clone());
    let views = AssertUnwindSafe(RefCell::new(BTreeSet::new()));
    let runs = AssertUnwindSafe(RefCell::new(0usize));
    sim.exhaustive(async || {
        send_a.send(1);
        quiesce().await;
        if recovery_resends {
            send_b.send(2);
        }
        quiesce().await;
        let mut held = vec![];
        // Member 0 crashed; members 1 and 2 are correct.
        for m in 1..3u32 {
            let mut got: Vec<u32> = out.collect_sorted(m).await;
            got.dedup();
            held.push(got);
        }
        *runs.borrow_mut() += 1;
        views.borrow_mut().insert(format!("{held:?}"));
    });
    sim.clear_network_hook();
    eprintln!(
        "hand enumeration: {} drops, crash fired {} times over {} schedules; views of members \
         1 and 2: {:?}",
        hook.dropped.borrow(),
        hook.fired.borrow(),
        runs.borrow(),
        views.borrow()
    );
    let views = views.borrow().clone();
    (verdict, views)
}

/// Finding 2: certified for one crash, though a three-fault set breaks it (by hand).
#[cfg(stageleft_runtime)]
#[test]
fn review3b_a_three_fault_break_beyond_both_oracles() {
    let (verdict, views) = three_fault_break(true);
    assert!(
        matches!(verdict, Verdict::Certified { .. }),
        "{verdict:?}"
    );
    // Member 1 holds 7, member 2 never gets it, in every schedule of the three faults.
    assert_eq!(
        views,
        std::collections::BTreeSet::from(["[[7], []]".to_string()])
    );
}

/// The control for finding 2: without the recovery input (so no post-heal copy, and no clause
/// marked "cannot be lost"), the search finds the three-fault break itself.
#[cfg(stageleft_runtime)]
#[test]
fn review3b_a_three_fault_break_without_the_recovery_copy_is_found() {
    let (verdict, views) = three_fault_break(false);
    assert!(
        !matches!(verdict, Verdict::Certified { .. }),
        "{verdict:?}"
    );
    assert_eq!(
        views,
        std::collections::BTreeSet::from(["[[7], []]".to_string()])
    );
}

/// Finding 3: an input the harness sends only after an output arrives.
///
/// Process `q` gets input 7 and broadcasts it to two members over a lossy channel (`first`).
/// Process `p` gets input 7 too and broadcasts it over `fail_stop` (`second`). The label is
/// what the members got from either. The harness sends 7 to `q`, **waits until member 1 shows
/// 7**, and only then sends 7 to `p` (a wait, not a branch: the sequence it sends is a prefix of
/// one fixed sequence, as A8 allows: "a wait that never returns ends the sequence early").
/// Written with `out.next(1).await` instead of `try_next`, the search still certifies and both
/// oracles break the label by the harness's panic ("Stream ended"); `try_next` shows the
/// members' disagreement itself.
///
/// Every derivation through `p` is robust, and only a crash of `p` kills it, so with no crash
/// every set is pruned. But losing `7 → member 1` on `first` makes the wait never return, so
/// `p` never gets its input: member 0 holds 7 and member 1 never does.
#[cfg(stageleft_runtime)]
#[test]
fn review3b_an_input_sent_after_a_wait_is_not_in_every_run() {
    let mut check = EcCheck::new().max_crashes(0);
    let mut flow = FlowBuilder::new();
    let q = flow.process::<()>();
    let p = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_q, in_q) = q.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_p, in_p) = p.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let first = in_q
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("first").bincode());
    let second = in_p.broadcast_closed(&members, TCP.fail_stop().name("second").bincode());
    let delivered = first
        .weaken_ordering::<NoOrder>()
        .merge_unordered(second.weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): `p`'s reliable copy covers every loss on `first` */
        ));
    let out = delivered
        .clone()
        .assume_ordering::<TotalOrder>(nondet!(/** the harness waits for one value */))
        .sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_q.send(7);
            // A wait: if 7 never reaches member 1, the harness sends nothing more.
            if out.try_next(1).await.is_some() {
                send_p.send(7);
            }
        },
        MAX_FAULTS,
    );
    eprintln!("=== input after a wait\n{}\n{cross}", cross.results);
    let label = cross.results.label("delivered");
    assert!(
        matches!(label.verdict, Verdict::Certified { .. }),
        "{:?}",
        label.verdict
    );
    let brute = cross
        .brute
        .labels
        .iter()
        .find(|l| l.name.starts_with("delivered"))
        .expect("delivered was enumerated");
    // `{omit(7 → member 1 on first)}` breaks it: member 0 holds 7, member 1 never does.
    assert_eq!(brute.min_crashes(), Some(0), "{cross}");
    assert_eq!(brute.breaking_within(0).map(|s| s.len()), Some(1), "{cross}");
    let about: Vec<&String> = cross
        .disagreements
        .iter()
        .filter(|d| d.contains("delivered (review3b_tests.rs)"))
        .collect();
    assert_eq!(about.len(), 2, "{cross}");
    assert!(about[1].starts_with("oracle 2:"), "{cross}");
}
