//! Rung 1 of the quorum→consensus ladder: **uniform reliable broadcast** (URB).
//!
//! # Uniformity: the property regular RB does not have
//!
//! [`reliable_broadcast_closed`](crate::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed)
//! guarantees agreement among *correct* members: if any correct member
//! delivers, all correct members deliver. It says nothing about a member that
//! delivers and then crashes — and that hole is real: a member can deliver a
//! message to its application (an escaped side effect) and die before its echo
//! flushes, after which no other member ever sees the message. The application
//! acted on a fact the system then lost.
//!
//! **Uniform** reliable broadcast closes the hole: if *any* member — correct
//! or faulty — delivers m, then every correct member eventually delivers m.
//!
//! # The construction: RB skeleton + certificate-gated delivery
//!
//! The dissemination is exactly the RB echo cycle (every member re-broadcasts
//! each first-seen message). The only change is the delivery rule: instead of
//! delivering on first receipt, a member delivers m when it holds a
//! [`Durable`](super::quorum::Durable) certificate for m — `threshold`
//! distinct members have echoed it. With `threshold = F + 1`, delivery implies
//! some *correct* member holds m, and correct members echo what they hold, so
//! every correct member eventually certifies and delivers m. Deliver-on-
//! certificate is precisely "deliver only facts that are already crash-proof."
//!
//! # The witness ledger
//!
//! Zero consistency assertions and zero `manual_proof!`s in this protocol
//! body. EC is inferred around the echo cycle exactly as in RB, and the
//! quorum gate's obligations live in the one audited mint
//! ([`quorum`](super::quorum::quorum)) — the sequestration story, one rung up.

use std::hash::Hash;

use hydro_lang::live_collections::boundedness::Boundedness;
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::quorum::quorum;

/// Uniform reliable broadcast from a Process to a static Cluster.
///
/// `threshold` is the certificate size required for delivery; choose
/// `F + 1` where `F` is the crash budget. See the module docs for the
/// property and the construction.
pub fn uniform_reliable_broadcast_closed<
    'a,
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    L,
    L2: 'a,
    B: Boundedness,
    O: hydro_lang::live_collections::stream::Ordering,
    R: hydro_lang::live_collections::stream::Retries,
>(
    source: Stream<T, Process<'a, L>, B, O, R>,
    cluster: &Cluster<'a, L2>,
    threshold: usize,
) -> Stream<T, Cluster<'a, L2, EventualConsistency>, Unbounded, NoOrder, ExactlyOnce>
where
    O: hydro_lang::live_collections::stream::MinOrder<
            hydro_lang::live_collections::stream::TotalOrder,
        >,
{
    // Dissemination: RB's echo cycle, CONSUMED rather than restated — the
    // wide interface exports the echoes, and an echo is an attestation
    // "member m holds this message".
    let (_deliveries, echoes) =
        crate::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed_with_echoes(
            source, cluster,
        );

    // Delivery rule: deliver m only once `threshold` distinct members have
    // echoed it — i.e., only facts that are already crash-durable.
    let attestations = echoes.entries().map(q!(|(echoer, m)| (m, echoer)));
    quorum(threshold, attestations).map(q!(|cert| cert.into_fact()))
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::uniform_reliable_broadcast_closed;
    use crate::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

    const N: usize = 3;
    /// Crash budget of the cluster fault domain in the tests below.
    const F: usize = 1;
    /// Delivery certificate size: F + 1.
    const THRESHOLD: usize = F + 1;

    /// Baseline, crash-free, exhaustive: URB delivers to every member (the
    /// certificate gate does not spuriously withhold delivery).
    #[test]
    fn urb_delivers_to_all() {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv =
            uniform_reliable_broadcast_closed(data, &cluster, THRESHOLD).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, N)
            .exhaustive(async || {
                in_send.send(42);
                for member in 0..N as u32 {
                    let got: Vec<u32> = out_recv.collect_n_sorted(member, 1).await;
                    assert_eq!(got, vec![42], "member {member} did not deliver");
                }
            });
    }

    /// **The separation, red half: regular RB is not uniform.** Sender and one
    /// cluster member may crash. The search must find an execution where SOME
    /// member delivered 42 but fewer than N − F members ever do — i.e. a
    /// (faulty) member delivered and the fact then died with it. This is the
    /// deliver-then-crash hole described in the module docs, witnessed.
    #[test]
    fn rb_violates_uniformity_under_deliverer_crash() {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv = reliable_broadcast_closed(data, &cluster).sim_cluster_output();

        let mut saw_uniformity_violation = false;
        let mut saw_full_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, N)
            .with_crashable_process(&sender)
            .with_crashable_cluster(&cluster, F)
            .fuzz(async || {
                in_send.send(42);

                let mut deliverers = 0usize;
                for member in 0..N as u32 {
                    let got: Vec<u32> = out_recv.collect_sorted(member).await;
                    if got.contains(&42) {
                        deliverers += 1;
                    }
                }

                // Uniformity, phrased survivor-agnostically: if ANYONE
                // delivered (even a member that then crashed), at least
                // N − F members must deliver.
                if deliverers > 0 && deliverers < N - F {
                    saw_uniformity_violation = true;
                }
                if deliverers == N {
                    saw_full_delivery = true;
                }
            });

        assert!(
            saw_uniformity_violation,
            "regular RB must exhibit the deliver-then-crash hole: some execution where a \
             member delivered 42 and fewer than {} members ever do",
            N - F
        );
        assert!(
            saw_full_delivery,
            "sanity: some (e.g. crash-free) execution delivers everywhere"
        );
    }

    /// **The separation, green half: URB is uniform.** The identical fault
    /// model — crashable sender AND one crashable cluster member — but
    /// delivery is certificate-gated at threshold F + 1. In every explored
    /// execution: if any member delivers 42, at least N − F members do. A
    /// delivered fact was crash-durable by construction, so it cannot die
    /// with its deliverer.
    #[test]
    fn urb_uniform_agreement_under_deliverer_crash() {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let out_recv =
            uniform_reliable_broadcast_closed(data, &cluster, THRESHOLD).sim_cluster_output();

        let mut saw_full_delivery = false;
        let mut saw_no_delivery = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, N)
            .with_crashable_process(&sender)
            .with_crashable_cluster(&cluster, F)
            .fuzz(async || {
                in_send.send(42);

                let mut deliverers = 0usize;
                for member in 0..N as u32 {
                    let got: Vec<u32> = out_recv.collect_sorted(member).await;
                    if got.contains(&42) {
                        deliverers += 1;
                    }
                }

                // UNIFORM agreement, in every explored execution.
                assert!(
                    deliverers == 0 || deliverers >= N - F,
                    "URB let a delivered fact die with its deliverer: only {deliverers} of {N} \
                     members delivered"
                );

                if deliverers == N {
                    saw_full_delivery = true;
                }
                if deliverers == 0 {
                    saw_no_delivery = true;
                }
            });

        assert!(
            saw_full_delivery,
            "sanity: some (e.g. crash-free) execution delivers everywhere"
        );
        assert!(
            saw_no_delivery,
            "sanity: some execution (sender dies early) delivers nowhere — which uniformity \
             permits"
        );
    }
}
