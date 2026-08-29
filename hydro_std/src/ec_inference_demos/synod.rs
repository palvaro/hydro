//! Rung 3 of the quorum→consensus ladder: **single-decree synod** — one slot
//! of Paxos, built from the extracted quorum mints.
//!
//! # The construction: ABD's skeleton + one new rule
//!
//! Proposers (a cluster, so they can duel) race to get a single value chosen
//! by acceptors. Ballot = [`Ts`] unchanged. Phase 1 = the covering pattern
//! unchanged ([`covering_quorum`]: promises carry the acceptor's highest
//! accepted proposal; certificate at majority). Phase 2 = the [`quorum`]
//! mint unchanged (accepted-acks at majority ⇒ chosen). The one new thing is
//! **adopt-highest**: where ABD's client may write its own value over a
//! covering (overwriting is legal register semantics), the synod proposer
//! MUST propose the covering's max-ballot value if one exists. That
//! conditional is the entire difference between a register and consensus —
//! and it is the splice invariant in miniature (ballot = epoch, adopt =
//! splice). The deliberately broken variant
//! [`synod_without_adoption_for_refutation`] exists so the simulator can
//! prove the rule is load-bearing.
//!
//! # The acceptor inverts ABD's typing story
//!
//! ABD's replica was a lattice (order-insensitive max-merge), hence a
//! top-level fold, hence EC inferred. The synod acceptor cannot be:
//! `promise(b)` exists to *refuse* future lower-ballot accepts, so
//! `(max_promised, accepted)` is order-sensitive — the same message set in
//! different arrival orders ends in different states. Refusal is the
//! mechanism, and refusal does not commute. The acceptor is therefore a
//! tick-serialized slice with no EC label, **correctly**: this is the
//! determination/commitment boundary surfaced as a type boundary
//! (`2026-08_quorum_certificates.md` §3a).
//!
//! # Batch semantics at the acceptor (safety argument)
//!
//! Within a tick, responses are computed against the tick-start
//! `max_promised`, with the batch's accepts folded into `accepted` before
//! promises are issued — equivalent to the serialization "all accepts, then
//! all prepares". Two relaxations are deliberate and safe:
//! - Two prepares in one batch may both receive promises even though the
//!   lower one is doomed: a promise is a one-directional constraint, and the
//!   doomed proposer's accept is refused later against the (monotone)
//!   updated `max_promised`. Liveness noise only.
//! - Promises report the post-batch `accepted`: reporting MORE accepted
//!   history than a strict serialization would is always safe (adopt-highest
//!   only becomes more conservative).
//!
//! # Ledger
//!
//! Zero new mints, zero consistency assertions, and the same two combiner
//! obligations as ABD (inside the mints). Proposer and acceptor traffic is
//! indexical, correctly un-EC. Progress is a driver discipline, not a
//! protocol feature: ballot escalation is owned by the caller (like Raft's
//! timer inputs), because dueling proposers can livelock — that is FLP, not
//! a bug.

use std::hash::Hash;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::quorum::{Ts, covering_quorum, quorum};

/// Responses from acceptors to proposers (one slice produces both kinds; the
/// acceptor's identity rides on the channel keying).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum ToProposer<V> {
    Promise { b: Ts, accepted: Option<(Ts, V)> },
    Accepted { b: Ts },
}

/// Single-decree synod. Each element of `proposals` is one attempt
/// `(round, value)`; the ballot is `(round, proposer member id)`, so rounds
/// must be distinct per proposer member (the caller owns ballot escalation).
/// Returns the chosen certificates this member learns for its own ballots:
/// `(ballot, value)`. Across all members and ballots, every chosen value is
/// the same — that is the agreement the tests attack.
pub fn synod<'a, V, P, A>(
    acceptors: &Cluster<'a, A>,
    majority: usize,
    proposals: Stream<(u64, V), Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Stream<(Ts, V), Cluster<'a, P>, Unbounded, NoOrder, ExactlyOnce>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
{
    synod_inner(acceptors, majority, proposals, true)
}

/// **Deliberately broken — exists only to be refuted.** Identical to
/// [`synod`] except the proposer ignores the covering and always proposes its
/// own value. The simulator must find executions with two different chosen
/// values (see `synod_no_adoption_violates_agreement`), proving adopt-highest
/// is load-bearing.
#[doc(hidden)]
pub fn synod_without_adoption_for_refutation<'a, V, P, A>(
    acceptors: &Cluster<'a, A>,
    majority: usize,
    proposals: Stream<(u64, V), Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
) -> Stream<(Ts, V), Cluster<'a, P>, Unbounded, NoOrder, ExactlyOnce>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
{
    synod_inner(acceptors, majority, proposals, false)
}

fn synod_inner<'a, V, P, A>(
    acceptors: &Cluster<'a, A>,
    majority: usize,
    proposals: Stream<(u64, V), Cluster<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    adopt_highest: bool,
) -> Stream<(Ts, V), Cluster<'a, P>, Unbounded, NoOrder, ExactlyOnce>
where
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    P: 'a,
    A: 'a,
{
    let proposer_cluster = proposals.location().clone();

    // Each attempt gets its ballot: (round, own member id).
    let attempts = proposals.map(q!(move |(round, v)| {
        (
            Ts {
                round,
                writer: CLUSTER_SELF_ID.clone().into_tagless(),
            },
            v,
        )
    }));

    // ---- Phase 1 out: prepare(b) to every acceptor -------------------------
    let prepares = attempts
        .clone()
        .map(q!(|(b, _v)| b))
        .broadcast_closed(acceptors, TCP.fail_stop().bincode())
        .entries(); // (proposer, b) at each acceptor

    // Phase-2 accepts close a cycle (they depend on promises, which depend on
    // the acceptor): forward_ref at the acceptor location, ABD-style.
    let (accepts_handle, accepts_fwd) =
        prepares
            .location()
            .forward_ref::<Stream<(MemberId<P>, (Ts, V)), _, Unbounded, NoOrder>>();

    // ---- The acceptor: the determination kernel ----------------------------
    // Order-sensitive by design (refusal does not commute): one slice,
    // tick-serialized, no EC label available or wanted.
    let acceptor_out = sliced! {
        let mut max_promised = use::state(|l| l.singleton(q!(None)));
        let mut accepted = use::state(|l| l.singleton(q!(None)));

        let prepare_batch = use::batch(prepares, nondet!(
            /// Arrival timing of prepares — which tick considers them. All
            /// checks are against the monotone max_promised, so batching
            /// only permutes legal serializations (module docs).
        ));
        let accept_batch = use::batch(accepts_fwd, nondet!(
            /// Arrival timing of accepts, same argument.
        ));

        // Accepts, checked against tick-start max_promised.
        let acc_checked = accept_batch.cross_singleton(max_promised.clone());

        let acks = acc_checked.clone().filter_map(q!(
            |((proposer, (b, _v)), mp): ((_, (Ts, _)), Option<Ts>)| {
                if mp.as_ref().map(|m| b >= *m).unwrap_or(true) {
                    Some((proposer, ToProposer::Accepted { b }))
                } else {
                    None
                }
            }
        ));

        // Fold passing accepts into the accepted register (max by ballot; the
        // register never regresses).
        let batch_accepted = acc_checked
            .filter_map(q!(|((_proposer, (b, v)), mp): ((_, (Ts, _)), Option<Ts>)| {
                if mp.as_ref().map(|m| b >= *m).unwrap_or(true) {
                    Some((b, v))
                } else {
                    None
                }
            }))
            .fold(
                q!(|| None),
                q!(|acc: &mut Option<(Ts, _)>, (b, v)| {
                    if acc.as_ref().map(|(a, _)| *a < b).unwrap_or(true) {
                        *acc = Some((b, v));
                    }
                }, commutative = manual_proof!(
                    /** max by the total ballot order is commutative: writer
                    ids make ties across proposers impossible, and rounds are
                    distinct per proposer (caller contract). */
                )),
            );

        let new_accepted = accepted.zip(batch_accepted).map(q!(|(old, batch)| {
            match (old, batch) {
                (None, b) => b,
                (a, None) => a,
                (Some(a), Some(b)) => Some(if a.0 >= b.0 { a } else { b }),
            }
        }));
        accepted = new_accepted.clone();

        // Promises: checked against tick-start max_promised, reporting the
        // post-batch accepted register (safe; module docs).
        let proms = prepare_batch
            .clone()
            .cross_singleton(max_promised.clone())
            .cross_singleton(new_accepted)
            .filter_map(q!(|(((proposer, b), mp), acc): (((_, Ts), Option<Ts>), _)| {
                if mp.as_ref().map(|m| b > *m).unwrap_or(true) {
                    Some((proposer, ToProposer::Promise { b, accepted: acc }))
                } else {
                    None
                }
            }));

        // The commitment: max_promised advances by this batch's prepares and
        // refuses lower ballots forever after.
        let batch_max_prepare = prepare_batch.map(q!(|(_p, b)| b)).fold(
            q!(|| None),
            q!(|acc: &mut Option<Ts>, b| {
                if acc.as_ref().map(|a| *a < b).unwrap_or(true) {
                    *acc = Some(b);
                }
            }, commutative = manual_proof!(/** max is commutative (total order) */)),
        );
        max_promised = max_promised.zip(batch_max_prepare).map(q!(|(old, batch)| {
            match (old, batch) {
                (None, b) => b,
                (a, None) => a,
                (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
            }
        }));

        acks.chain(proms)
    };

    // Route responses back to their proposers.
    let from_acceptors = acceptor_out
        .into_keyed()
        .demux(&proposer_cluster, TCP.fail_stop().bincode())
        .entries(); // (acceptor, ToProposer) at each proposer

    let promises = from_acceptors
        .clone()
        .filter_map(q!(|(acceptor, msg)| match msg {
            ToProposer::Promise { b, accepted } => Some((b, (acceptor, accepted))),
            _ => None,
        }));

    let accepted_acks = from_acceptors.filter_map(q!(|(acceptor, msg)| match msg {
        ToProposer::Accepted { b } => Some((b, acceptor)),
        _ => None,
    }));

    // ---- Phase 1 in: the covering certificate ------------------------------
    let covered = covering_quorum(majority, promises).map(q!(|(b, cov)| (b, cov.into_aggregate())));

    // ---- The splice rule: adopt-highest, then phase 2 -----------------------
    // (rid-keyed join = the phase transition; the ballot is the continuation.)
    let joined = covered.join(attempts);
    let proposed = if adopt_highest {
        joined.map(q!(|(b, (covered_max, my_v))| {
            (b, covered_max.map(|(_, adopted)| adopted).unwrap_or(my_v))
        }))
    } else {
        // The refutation variant: ignore the covering. UNSAFE by design.
        joined.map(q!(|(b, (_covered_max, my_v))| (b, my_v)))
    };

    let accepts = proposed
        .clone()
        .map(q!(|(b, v)| (b, v)))
        .broadcast_closed(acceptors, TCP.fail_stop().bincode())
        .entries();
    accepts_handle.complete(accepts);

    // ---- Chosen: a Durable certificate on the ballot ------------------------
    let certified = quorum(majority, accepted_acks).map(q!(|cert| cert.into_fact()));

    certified
        .map(q!(|b| (b, ())))
        .join(proposed)
        .map(q!(|(b, ((), v))| (b, v)))
        .weaken_ordering::<NoOrder>()
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::super::quorum::Ts;
    use super::{synod, synod_without_adoption_for_refutation};

    const N: usize = 3;
    const MAJORITY: usize = 2; // N/2 + 1
    const F: usize = 1;

    /// Smoke: one proposer, one attempt, its value is chosen.
    #[test]
    fn synod_chooses_proposed_value() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, MAJORITY, proposals).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 1)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                p_send.send(0, (1, 42u32));
                let got: Vec<(Ts, u32)> = chosen_recv.collect_n_sorted(0, 1).await;
                assert_eq!(got[0].1, 42, "the sole proposer's value must be chosen");
            });
    }

    /// **Agreement under dueling proposers (the money test).** Two proposers
    /// race concurrently — no barriers, every interleaving of prepares,
    /// promises, accepts, and coverings is fair game. In EVERY explored
    /// execution, all chosen values (across members and ballots) are equal.
    /// The higher ballot always completes, so something is chosen in every
    /// execution too.
    #[test]
    fn synod_agreement_under_dueling_proposers() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, MAJORITY, proposals).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .fuzz(async || {
                p_send.send(0, (1, 10u32));
                p_send.send(1, (2, 20u32));

                let mut chosen_values = std::collections::BTreeSet::new();
                for member in 0..2u32 {
                    let got: Vec<(Ts, u32)> = chosen_recv.collect_sorted(member).await;
                    for (_b, v) in got {
                        chosen_values.insert(v);
                    }
                }
                assert!(
                    chosen_values.len() <= 1,
                    "AGREEMENT VIOLATED: two different values chosen: {chosen_values:?}"
                );
                assert!(
                    !chosen_values.is_empty(),
                    "the higher ballot always completes; something must be chosen"
                );
            });
    }

    /// Dueling proposers AND one untargeted acceptor crash: agreement still
    /// holds in every explored execution.
    #[test]
    fn synod_agreement_under_dueling_and_acceptor_crash() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, MAJORITY, proposals).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .with_crashable_cluster(&acceptors, F)
            .fuzz(async || {
                p_send.send(0, (1, 10u32));
                p_send.send(1, (2, 20u32));

                let mut chosen_values = std::collections::BTreeSet::new();
                for member in 0..2u32 {
                    let got: Vec<(Ts, u32)> = chosen_recv.collect_sorted(member).await;
                    for (_b, v) in got {
                        chosen_values.insert(v);
                    }
                }
                assert!(
                    chosen_values.len() <= 1,
                    "AGREEMENT VIOLATED under acceptor crash: {chosen_values:?}"
                );
            });
    }

    /// **RED: adopt-highest is load-bearing.** The refutation variant ignores
    /// the covering and proposes its own value. Sequenced attempts (barrier
    /// between waves) make the violation deterministic: wave 1 chooses 10,
    /// wave 2's naive proposer overwrites with 20 — two chosen values. The
    /// search must witness it.
    #[test]
    fn synod_no_adoption_violates_agreement() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod_without_adoption_for_refutation(&acceptors, MAJORITY, proposals)
            .sim_cluster_output();

        let mut saw_divergence = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                p_send.send(0, (1, 10u32));
                hydro_lang::sim::quiesce().await;
                p_send.send(1, (2, 20u32));

                let mut chosen_values = std::collections::BTreeSet::new();
                for member in 0..2u32 {
                    let got: Vec<(Ts, u32)> = chosen_recv.collect_sorted(member).await;
                    for (_b, v) in got {
                        chosen_values.insert(v);
                    }
                }
                if chosen_values.len() > 1 {
                    saw_divergence = true;
                }
            });

        assert!(
            saw_divergence,
            "without adopt-highest, the search must find two different chosen values"
        );
    }

    /// **RED: the intersection premise is load-bearing.** Quorum size 1 of 3:
    /// two \"quorums\" need not intersect, so two proposers can each assemble
    /// a covering that missed the other's accept, and both values get chosen.
    /// This directly attacks the fault-model `manual_proof!` behind the
    /// rung-0 mint — the first mechanical audit of a trusted mint.
    #[test]
    fn synod_sub_majority_quorum_violates_agreement() {
        const BROKEN_QUORUM: usize = 1;

        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, BROKEN_QUORUM, proposals).sim_cluster_output();

        let mut saw_divergence = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 2)
            .fuzz(async || {
                p_send.send(0, (1, 10u32));
                p_send.send(1, (2, 20u32));

                let mut chosen_values = std::collections::BTreeSet::new();
                for member in 0..2u32 {
                    let got: Vec<(Ts, u32)> = chosen_recv.collect_sorted(member).await;
                    for (_b, v) in got {
                        chosen_values.insert(v);
                    }
                }
                if chosen_values.len() > 1 {
                    saw_divergence = true;
                }
            });

        assert!(
            saw_divergence,
            "with non-intersecting quorums, the search must find two different chosen values"
        );
    }

    /// **RED: the distinct-rounds contract is load-bearing.** A proposer
    /// that reuses a round mints the same ballot for two values; the ballot-
    /// keyed covering fires once, joins against BOTH attempts, and both
    /// values ride the same certificate to "chosen" — two chosen values for
    /// one ballot, agreement destroyed. The search must witness it.
    #[test]
    fn synod_violating_distinct_rounds_violates_agreement() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, MAJORITY, proposals).sim_cluster_output();

        let mut saw_divergence = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 1)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                // CONTRACT VIOLATION: the same round twice, different values.
                p_send.send(0, (1, 10u32));
                p_send.send(0, (1, 20u32));

                let got: Vec<(Ts, u32)> = chosen_recv.collect_sorted(0).await;
                let values: std::collections::BTreeSet<u32> =
                    got.iter().map(|(_b, v)| *v).collect();
                if values.len() > 1 {
                    saw_divergence = true;
                }
            });

        assert!(
            saw_divergence,
            "violating distinct-rounds must let the search choose two different values \
             under one ballot"
        );
    }

    /// **Progress under the Ω discipline.** A single designated proposer
    /// (no duel — that is the oracle's job) with one untargeted acceptor
    /// crash: the value is chosen in EVERY explored execution, asserted by
    /// `collect_n_sorted` itself (a blocked synod quiesces without output).
    #[test]
    fn synod_progress_under_acceptor_crash() {
        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();

        let (p_send, proposals) = proposers.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let chosen_recv = synod(&acceptors, MAJORITY, proposals).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, N)
            .with_cluster_size(&proposers, 1)
            .with_crashable_cluster(&acceptors, F)
            .fuzz(async || {
                p_send.send(0, (1, 42u32));
                let got: Vec<(Ts, u32)> = chosen_recv.collect_n_sorted(0, 1).await;
                assert_eq!(got[0].1, 42, "chosen despite the crash");
            });
    }
}
