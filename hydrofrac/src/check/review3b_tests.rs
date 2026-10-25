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
