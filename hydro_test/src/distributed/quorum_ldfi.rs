//! `hydro_std::quorum::collect_quorum` under lineage-driven fault injection: an LDFI unit test
//! of a library building block rather than of a whole protocol.
//!
//! A coordinator broadcasts a request to `n` responders over a lossy channel; each responder
//! answers every request it has seen whenever its timer fires (so a reply may be sent more
//! than once, as a retransmitting responder would), over a lossy channel back; the coordinator
//! collects a quorum of `min` replies with `collect_quorum`. The library counts *replies*, not
//! *responders*: it is the caller's job to deduplicate. `dedup = false` is the injected bug —
//! the caller forgot — and two copies of one responder's reply then pass for a quorum.
//!
//! Two specifications (see `hydro_lang::sim::spec`):
//!
//! - **Safety of the quorum claim.** `PRE`: the coordinator declared a quorum for `r`; `POST`:
//!   the coordinator received replies for `r` from at least `min` distinct responders. Holds
//!   for the deduplicating caller under any faults (a certificate); the duplicate-counting
//!   caller breaks it when every responder but one is silenced.
//! - **Availability under the horizon.** `PRE`: a request was issued; `POST`: a quorum was
//!   declared by the end of the run. A one-shot request is the "try once" shape of the paper's
//!   first example: losing it on the way to `n - min + 1` responders is enough.

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_std::quorum::collect_quorum;
use serde::{Deserialize, Serialize};

pub struct Coordinator {}

pub struct Responder {}

/// A request the coordinator wants a quorum of acknowledgements for.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request(pub u32);

/// What the coordinator produces, for the specification to observe.
pub struct QuorumOutputs<'a> {
    /// Requests for which `collect_quorum` declared a quorum.
    pub decided: Stream<Request, Process<'a, Coordinator>, Unbounded, NoOrder>,
    /// Every reply the coordinator received, with its responder.
    pub received:
        Stream<(MemberId<Responder>, Request), Process<'a, Coordinator>, Unbounded, NoOrder>,
    /// The requests the coordinator issued.
    pub requests: Stream<Request, Process<'a, Coordinator>, Unbounded, NoOrder>,
}

/// Builds the protocol. `rounds` fires each responder's reply timer (one `()` per member,
/// driven by the test); a responder replies to every request it has seen on every round, so
/// replies are retransmitted. With `dedup` the coordinator counts each responder once per
/// request before `collect_quorum`; without, it feeds the library every reply it receives.
///
/// # Non-Determinism
/// The batching of requests and timer rounds at the responders and of replies at the
/// coordinator is explored by the simulator.
pub fn quorum_rpc<'a>(
    coordinator: &Process<'a, Coordinator>,
    responders: &Cluster<'a, Responder>,
    n: usize,
    min: usize,
    requests: Stream<Request, Process<'a, Coordinator>>,
    rounds: Stream<(), Cluster<'a, Responder>>,
    dedup: bool,
) -> QuorumOutputs<'a> {
    let requests = requests.weaken_ordering::<NoOrder>();
    let at_responders = requests.clone().broadcast_closed(
        responders,
        TCP.lossy_delayed_forever().name("req").bincode(),
    );

    // A responder answers every request it has seen whenever its timer fires.
    let replies: Stream<Request, Cluster<'a, Responder>, Unbounded, NoOrder> = sliced! {
        let new_requests = use::batch(at_responders, nondet!(
            /// When a request is processed relative to a timer round only decides how many
            /// times it is answered.
        ));
        let fired = use::batch(rounds, nondet!(/** likewise */));
        let mut seen = use::state(|l| {
            l.source_iter(q!(Vec::<Request>::new()))
                .weaken_ordering::<NoOrder>()
        });
        let all = seen.chain(new_requests);
        seen = all.clone();
        all.cross_singleton(fired.first()).map(q!(|(request, _)| request))
    };

    let received = replies
        .send(
            coordinator,
            TCP.lossy_delayed_forever().name("resp").bincode(),
        )
        .entries();

    // The library counts replies; deduplicating by responder is the caller's job.
    let counted = if dedup {
        received.clone().unique()
    } else {
        received.clone()
    };
    let (decided, _failed) = collect_quorum(
        counted.map(q!(|(_, request)| (request, Ok::<(), ()>(())))),
        min,
        n,
    );

    QuorumOutputs {
        decided,
        received,
        requests,
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{Exploration, Fault, Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::Spec;

    use super::{Coordinator, Request, Responder, quorum_rpc};

    const N: u32 = 3;
    const MIN: usize = 2;
    const ROUNDS: u32 = 2;
    /// Schedules sampled per hypothesis. Three responders answering twice each, with the
    /// coordinator's own batch inside `collect_quorum`, give ~5,600 interleavings per
    /// hypothesis under `exhaustive`; the outcome does not depend on them (only tick order
    /// varies), but a certificate here is relative to the sample.
    const SAMPLED: usize = 8;

    /// Which specification the search runs.
    #[derive(Clone, Copy, Debug)]
    enum Property {
        /// `PRE`: a quorum was declared for the request; `POST`: at least `MIN` distinct
        /// responders' replies for it were received.
        QuorumClaim,
        /// `PRE`: the request was issued; `POST`: a quorum was declared by the end.
        Availability,
    }

    fn search(dedup: bool, crashes: usize, property: Property) -> Outcome {
        let mut flow = FlowBuilder::new();
        let coordinator = flow.process::<Coordinator>();
        let responders = flow.cluster::<Responder>();

        let (request_send, requests) = coordinator.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (round_send, rounds) = responders.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = quorum_rpc(
            &coordinator,
            &responders,
            N as usize,
            MIN,
            requests,
            rounds,
            dedup,
        );

        // The specification.
        let mut spec = Spec::new(&mut flow);
        let decided = spec.observe(outputs.decided);
        let received = spec.observe(outputs.received);
        let issued = spec.observe(outputs.requests);
        let decided_first = decided.filter(q!(|r| r.0 == 1)).first();
        match property {
            Property::QuorumClaim => {
                spec.pre(decided_first);
                let distinct_responders = received
                    .filter(q!(|(_, r)| r.0 == 1))
                    .map(q!(|(responder, _)| responder))
                    .unique()
                    .count();
                let min = MIN;
                spec.post(distinct_responders.filter(q!(move |replied| *replied >= min)));
            }
            Property::Availability => {
                spec.pre(issued.filter(q!(|r| r.0 == 1)).first());
                spec.post(decided_first);
            }
        }

        let mut sim = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&responders, N as usize)
            .unit_test_fuzz_iterations(SAMPLED);
        if std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        Ldfi::new()
            .level(Level::Record)
            .level_from_env()
            .crashable_cluster("responder", &responders, 0..N)
            .crash_budget(crashes)
            .exploration(Exploration::Fuzz { instances: SAMPLED })
            .spec(&spec)
            .search(&compiled, async |_run| {
                request_send.send(Request(1));
                hydro_lang::sim::quiesce().await;
                for _ in 0..ROUNDS {
                    round_send.send_many((0..N).map(|m| (m, ())));
                    hydro_lang::sim::quiesce().await;
                }
            })
    }

    /// The deduplicating caller: a declared quorum rests on `MIN` distinct responders whatever
    /// is lost or who crashes, so the search ends with a certificate.
    #[test]
    fn ldfi_certifies_quorum_claim_with_deduplicated_replies() {
        let outcome = search(true, 1, Property::QuorumClaim);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// The caller that feeds `collect_quorum` every reply it receives: silence every responder
    /// but one, and that one's retransmitted reply is counted twice, which the library takes
    /// for a quorum. The counterexample needs an omission (a crashed responder sends nothing,
    /// but one crash is not enough to silence two).
    #[test]
    fn ldfi_breaks_quorum_claim_that_counts_duplicate_replies() {
        let outcome = search(false, 1, Property::QuorumClaim);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert!(
            faults.iter().any(|f| matches!(f, Fault::Omit(_))),
            "the counterexample needs a lost reply: {faults:?}"
        );
    }

    /// A one-shot request is the "try once" protocol of the paper's first example: losing it
    /// (or its replies) on the way to two of three responders leaves no quorum by the end.
    #[test]
    fn ldfi_breaks_quorum_availability_under_two_omissions() {
        let outcome = search(true, 0, Property::Availability);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert_eq!(faults.len(), 2, "two omissions: {faults:?}");
        assert!(faults.iter().all(|f| matches!(f, Fault::Omit(_))));
    }
}
