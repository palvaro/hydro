//! Reliable broadcast (ported from the echo-based `reliable_broadcast_closed` on the
//! `eventual_consistency` branch), as the second target for lineage-driven fault injection.
//!
//! # The property
//!
//! **Agreement**: if any correct member delivers `m`, every correct member eventually
//! delivers `m`. The protocol assumes reliable links (`fail_stop`), so the only fault in its
//! model is a crash, and the fault that matters is the broadcaster dying *between two of its
//! fan-out sends*: with plain broadcast the members it reached deliver and the rest never
//! will; with reliable broadcast every member that receives `m` re-broadcasts it before
//! delivering, so one surviving recipient is enough.
//!
//! # Shape
//!
//! Each member runs one tick that consumes both channels (the broadcaster's and the echoes')
//! through hooked batches, so every delivery is a traced lineage event, deduplicates against
//! persisted state, echoes fresh messages to every member (when `echo` is set), and delivers
//! them. The echo channel closes a dataflow cycle through the network, which is an
//! asynchronous boundary.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_lang::sim_hooks::{BatchHook, OnCluster};
use serde::{Deserialize, Serialize};

pub struct Broadcaster {}

pub struct Member {}

/// The broadcast payload.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Payload(pub u32);

/// The hook on each member's batch of messages from the broadcaster.
pub type InitialHook = BatchHook<Payload, TotalOrder, ExactlyOnce, OnCluster<Member>>;

/// The hook on each member's batch of echoes from other members (keyed by the echoer).
pub type EchoHook = BatchHook<(MemberId<Member>, Payload), NoOrder, ExactlyOnce, OnCluster<Member>>;

/// Broadcasts `source` from the broadcaster to every member. With `echo`, every member
/// re-broadcasts each message it sees for the first time before delivering it (reliable
/// broadcast); without, members deliver only what the broadcaster sent them (plain
/// broadcast). Returns each member's deliveries.
///
/// # Non-Determinism
/// - `nondet_initial`: the batching of the broadcaster's messages at each member.
/// - `nondet_echo`: the batching of echoes at each member.
pub fn broadcast<'a>(
    members: &Cluster<'a, Member>,
    source: Stream<Payload, Process<'a, Broadcaster>>,
    echo: bool,
    nondet_initial: NonDet<Option<InitialHook>>,
    nondet_echo: NonDet<Option<EchoHook>>,
) -> Stream<Payload, Cluster<'a, Member>, Unbounded, NoOrder> {
    let initial = source.broadcast_closed(members, TCP.fail_stop().name("initial").bincode());

    // The echo cycle: declared on the location the initial broadcast yields, so the
    // completing stream (also from `broadcast_closed` over `fail_stop`) matches its type.
    let (echo_handle, echoes) =
        initial
            .location()
            .forward_ref::<Stream<(MemberId<Member>, Payload), _, Unbounded, NoOrder>>();

    // Batching drops the consistency label, so the tick and its outputs live on the plain
    // cluster location.
    let (deliveries, to_echo): (
        Stream<Payload, Cluster<'a, Member>, Unbounded, NoOrder>,
        Stream<Payload, Cluster<'a, Member>, Unbounded, NoOrder>,
    ) = sliced! {
        let from_broadcaster = use::batch(initial, nondet_initial);
        let from_members = use::batch(echoes, nondet_echo);
        let mut seen = use::state(|l| {
            l.source_iter(q!(Vec::<Payload>::new()))
                .weaken_ordering::<NoOrder>()
        });

        let arrived = from_broadcaster.chain(from_members.map(q!(|(_, payload)| payload)));
        let fresh = arrived.unique().filter_not_in(seen.clone());
        seen = seen.chain(fresh.clone());

        let echoed = fresh.clone().filter(q!(move |_| echo));
        (fresh, echoed)
    };

    echo_handle.complete(
        to_echo
            .broadcast_closed(members, TCP.fail_stop().name("echo").bincode())
            .entries(),
    );

    deliveries
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{CrashAt, Fault, Grade, Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::Spec;

    use super::{Broadcaster, EchoHook, InitialHook, Member, Payload, broadcast};

    const MEMBERS: u32 = 3;

    /// How `POST` ("every correct member delivered `m`") is written.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Post {
        /// Positively: the correct members that delivered `m` are as many as the correct
        /// members. Every delivery is a positive contribution to the view, so the recorded
        /// lineage levels see it.
        Positive,
        /// Through negation: no correct member is missing from those that delivered `m`
        /// (`filter_not_in`). The view holds by the *absence* of facts, so its record
        /// provenance is empty; the search derives specification views counterfactually at
        /// the observer, which sees the same dependencies as for [`Post::Positive`].
        Negated,
        /// A different property, as a control for the certificate grading: integrity, "no member
        /// delivered `m` twice". It is non-monotone in the deliveries (a further delivery
        /// can falsify it), so a certificate for it is only relative to the recorded
        /// derivations and the search must say so.
        NoDuplicates,
    }

    /// Runs LDFI against the broadcast with or without echo, under one specification written
    /// as Hydro views over the members' deliveries (see `hydro_lang::sim::spec`):
    ///
    /// - `PRE`: some member delivered `m`.
    /// - `POST`: every correct member delivered `m`, written as `post` says.
    ///
    /// Fault model: the broadcaster and every member may crash (budget `crashes`); links are
    /// reliable. The same specification separates the two protocols: plain broadcast violates
    /// it, reliable broadcast satisfies it.
    fn search(echo: bool, crashes: usize, post: Post) -> Outcome {
        search_with(echo, crashes, post, |ldfi| ldfi)
    }

    /// [`search`], with `configure` applied to the search before it runs (traces, provenance
    /// graphs, the lineage level).
    fn search_with(
        echo: bool,
        crashes: usize,
        post: Post,
        configure: impl FnOnce(Ldfi) -> Ldfi,
    ) -> Outcome {
        let mut flow = FlowBuilder::new();
        let broadcaster = flow.process::<Broadcaster>();
        let members = flow.cluster::<Member>();
        let initial_hook: InitialHook = flow.sim_hook();
        let echo_hook: EchoHook = flow.sim_hook();

        let (source_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
        let deliveries = broadcast(
            &members,
            source,
            echo,
            nondet!(/** traced by the search */ hook = initial_hook),
            nondet!(/** traced by the search */ hook = echo_hook),
        );

        // The specification, as views over the facts the run produces.
        let mut spec = Spec::new(&mut flow);
        let delivered = spec.observe_cluster(deliveries);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let delivered_m = delivered.filter(q!(|(_, p)| p.0 == 42));
        // PRE: a member delivered m.
        spec.pre(delivered_m.clone().first());
        // POST: every correct member delivered m.
        match post {
            Post::Positive => {
                // "The correct members that delivered m are as many as the correct members":
                // each one's delivery is a positive contribution to the view.
                let delivering = delivered_m.map(q!(|(m, _)| (m, ()))).unique();
                let correct_delivering = correct
                    .clone()
                    .map(q!(|m| (m, ())))
                    .join(delivering)
                    .count();
                spec.post(correct_delivering.zip(correct.count()).filter(q!(|(
                    delivering,
                    correct,
                )| {
                    delivering == correct
                })));
            }
            Post::Negated => {
                // "No correct member is missing from those that delivered m": the view holds
                // by the absence of facts.
                let delivering = delivered_m.map(q!(|(m, _)| m)).unique();
                let missing = correct.clone().filter_not_in(delivering);
                spec.post(missing.count().filter(q!(|n| *n == 0)));
            }
            Post::NoDuplicates => {
                // "No member delivered m more than once": a delivery count per member, and
                // no count above one.
                let per_member = delivered_m
                    .map(q!(|(m, _)| (m, ())))
                    .into_keyed()
                    .fold(q!(|| 0usize), q!(|n, _| *n += 1));
                let duplicated = per_member.entries().filter(q!(|(_, n)| *n > 1));
                spec.post(duplicated.count().filter(q!(|n| *n == 0)));
            }
        }

        // Record-level provenance is the natural lineage for a specification: the
        // derivation of a view is exactly the deliveries its value was computed from. Without
        // it (`LDFI_NO_PROVENANCE`) `Precise` replays every record, which is much slower over
        // the hundreds of schedules of the echo variant; `Execution` is coarser still.
        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize);
        if std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        configure(
            Ldfi::new()
                .level(Level::Precise)
                .level_from_env()
                .crashable("broadcaster", &broadcaster)
                .crashable_cluster("member", &members, 0..MEMBERS)
                .crash_budget(crashes)
                .spec(&spec),
        )
        .search(&compiled, async |_run| {
            source_send.send(Payload(42));
            hydro_lang::sim::quiesce().await;
        })
    }

    /// The gallery entry (see `ldfi_viz`): plain broadcast, three members, one crash.
    #[test]
    fn gallery_plain_broadcast() {
        use crate::cluster::ldfi_viz::{gallery_dir, write_gallery};

        let Some(dir) = gallery_dir("plain_broadcast") else {
            return;
        };
        for level in [Level::Execution, Level::Precise] {
            let outcome = search_with(false, 1, Post::Positive, |ldfi| {
                ldfi.level(level).with_traces().with_provenance_graphs()
            });
            write_gallery(&dir, "plain broadcast", level, &outcome);
        }
    }

    /// Plain broadcast violates agreement under one crash: the broadcaster dies after its
    /// second send, members 0 and 1 deliver, member 2 never does. Lineage makes the search
    /// two runs: the failure-free derivation of `POST` is the three deliveries, each resting
    /// on one fan-out send `{b→0, b→1, b→2}`, so the candidate crashes are the broadcaster's
    /// at each send, weakest first.
    #[test]
    fn ldfi_breaks_plain_broadcast_with_a_mid_fanout_crash() {
        let outcome = search(false, 1, Post::Positive);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, runs, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        // The broadcaster dies after its second fan-out send.
        assert_eq!(faults.len(), 1);
        assert!(
            matches!(faults.first(), Some(Fault::Crash { node, at: CrashAt::After(m) })
                if node == "broadcaster" && m.to_string().starts_with("initial[→1]:")),
            "expected the broadcaster to die after its second send: {faults:?}"
        );
        assert_eq!(runs.len(), 2);
    }

    /// Reliable broadcast survives one crash: under the same fault, the echoes of members 0
    /// and 1 deliver to member 2, which appears as a new derivation of `POST` through the
    /// echo channel. Because a member's delivery may rest on an echo rather than on the
    /// broadcaster's send, every member's crash points are candidates too, and each is
    /// refuted by a run of its own; no single crash kills every derivation, so the search
    /// ends with a certificate.
    #[test]
    fn ldfi_certifies_reliable_broadcast_under_one_crash() {
        let outcome = search(true, 1, Post::Positive);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// Two crashes are not enough either: whoever the broadcaster reaches echoes to
    /// everyone, and whoever that echo reaches echoes again, so agreement among the correct
    /// members holds as long as one of them received the message at all.
    #[test]
    fn ldfi_certifies_reliable_broadcast_under_two_crashes() {
        let outcome = search(true, 2, Post::Positive);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// Disjunctive provenance (`Ldfi::disjunctive_provenance`) on the echo protocol: a member
    /// that gets the payload from the broadcaster and from an echo in the same tick records
    /// both as derivations of its delivery, so one good run yields every alternative it
    /// exhibited. The verdict is the same certificate; the two hypothesis sequences are
    /// printed for comparison (`LDFI_BROADCAST_DIAG`).
    #[test]
    fn ldfi_certifies_reliable_broadcast_with_disjunctive_provenance() {
        let diag = std::env::var("LDFI_BROADCAST_DIAG").is_ok();
        let crashes: usize = std::env::var("LDFI_BROADCAST_CRASHES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let single = search_with(true, crashes, Post::Positive, |ldfi| {
            if diag { ldfi } else { ldfi.quiet() }
        });
        let disjunctive = search_with(true, crashes, Post::Positive, |ldfi| {
            let ldfi = ldfi.disjunctive_provenance(true);
            if diag { ldfi } else { ldfi.quiet() }
        });
        for (name, outcome) in [("single", &single), ("disjunctive", &disjunctive)] {
            eprintln!("{name}: {} hypotheses", outcome.runs().len());
            for (i, hyp) in outcome.runs().iter().enumerate() {
                let clauses: usize = hyp
                    .schedules
                    .iter()
                    .filter(|s| s.goal_held)
                    .map(|s| s.derivations.len())
                    .sum();
                eprintln!(
                    "  #{i} {:?}: {} schedules, {clauses} clauses",
                    hyp.faults,
                    hyp.schedules.len()
                );
            }
        }
        assert!(matches!(single, Outcome::Certificate { .. }), "{single}");
        assert!(
            matches!(disjunctive, Outcome::Certificate { .. }),
            "{disjunctive}"
        );
    }

    /// The same search with `POST` written through negation ([`Post::Negated`]). The record
    /// provenance of a view that holds by the absence of facts is empty, but the search
    /// derives specification views counterfactually at the observer (replaying it with
    /// observed facts removed), which asks the same question of both formulations: removing
    /// any correct member's delivery falsifies `POST`, so the derivation is the same three
    /// deliveries and the counterexample the same mid-fan-out crash in two runs.
    #[test]
    fn ldfi_breaks_plain_broadcast_with_a_negated_post() {
        let outcome = search(false, 1, Post::Negated);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, runs, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert!(
            matches!(faults.first(), Some(Fault::Crash { node, at: CrashAt::After(m) })
                if node == "broadcaster" && m.to_string().starts_with("initial[→1]:")),
            "expected the broadcaster to die after its second send: {faults:?}"
        );
        assert_eq!(runs.len(), 2);
    }

    /// Reliable broadcast under one crash with the negated `POST`: a certificate, as with the
    /// positive formulation. Both are hand-written views, so the certificate is graded
    /// relative; the generated specification of `ec_ldfi.rs` is the one graded within the
    /// fault model.
    #[test]
    fn ldfi_certifies_reliable_broadcast_with_a_negated_post() {
        let outcome = search(true, 1, Post::Negated);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
        assert_eq!(outcome.grade(), Some(Grade::HandWritten));
    }

    /// The control for the grading. Integrity ("no member delivered `m` twice") holds of the
    /// broadcast under any crash: members deduplicate before delivering. But the search
    /// cannot earn that certificate. The view holds by the absence of a duplicate, so no
    /// delivery's removal changes it (the counterfactual derivation is empty) and its record
    /// provenance is empty too; the search certifies after the failure-free run with nothing
    /// checked. That is the hitting-set inference applied outside its hypothesis: a further
    /// fact (a duplicate) can falsify the view, which no derivation names. A hand-written view
    /// gets the RELATIVE grade for exactly this reason, and the outcome says so.
    #[test]
    fn ldfi_grades_a_hand_written_post_as_relative() {
        let outcome = search(true, 1, Post::NoDuplicates);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
        assert_eq!(outcome.grade(), Some(Grade::HandWritten));
        assert!(
            outcome.to_string().contains("RELATIVE"),
            "expected the certificate to be graded relative: {outcome}"
        );
    }
}
