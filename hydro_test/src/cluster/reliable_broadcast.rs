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
    let initial = source.broadcast_closed(members, TCP.fail_stop().bincode());

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
            .broadcast_closed(members, TCP.fail_stop().bincode())
            .entries(),
    );

    deliveries
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{Fault, Ldfi, Outcome};

    use super::{Broadcaster, EchoHook, InitialHook, Member, Payload, broadcast};

    const MEMBERS: u32 = 3;

    /// Runs LDFI against the broadcast with or without echo. Fault model: the broadcaster and
    /// every member may crash (budget `crashes`); links are reliable. Goal: agreement over
    /// the correct members.
    fn search(echo: bool, crashes: usize) -> Outcome {
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
            nondet!(/** LDFI delivery point: the broadcaster's channel */ hook = initial_hook),
            nondet!(/** LDFI delivery point: the echo channel */ hook = echo_hook),
        );
        let out = deliveries.sim_cluster_output();

        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize);
        if std::env::var("LDFI_PROVENANCE").is_ok() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        let mut ldfi = Ldfi::new()
            .level_from_env()
            .crashable("broadcaster", &broadcaster)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .crash_budget(crashes)
            .goal_all_members(&out, "member", 0..MEMBERS);
        for m in 0..MEMBERS {
            ldfi = ldfi
                .reliable_channel(&format!("initial@{m}"), initial_hook.on(m))
                .reliable_channel(&format!("echo@{m}"), echo_hook.on(m));
        }

        ldfi.run(&compiled, async |run| {
            source_send.send(Payload(42));
            hydro_lang::sim::quiesce().await;

            let mut delivered = vec![];
            for m in 0..MEMBERS {
                if run.crashed_member("member", m) {
                    continue;
                }
                let got: Vec<Payload> = out.collect_sorted(m).await;
                delivered.push(got.contains(&Payload(42)));
            }
            // Agreement: the correct members all delivered, or none did.
            delivered.iter().all(|d| *d) || delivered.iter().all(|d| !*d)
        })
    }

    /// Plain broadcast violates agreement under one crash: the broadcaster dies after its
    /// second send, members 0 and 1 deliver, member 2 never does. Lineage makes the search
    /// two runs: the failure-free derivation is the fan-out `{b→0, b→1, b→2}`, so the
    /// candidate crashes are the broadcaster's at each send, weakest first.
    #[test]
    fn ldfi_breaks_plain_broadcast_with_a_mid_fanout_crash() {
        let outcome = search(false, 1);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, runs, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        // The broadcaster dies after its second fan-out send.
        assert_eq!(faults.len(), 1);
        assert!(
            matches!(faults.first(), Some(Fault::Crash { node, after: Some(m) })
                if node == "broadcaster" && m.to_string().starts_with("initial@1:")),
            "expected the broadcaster to die after its second send: {faults:?}"
        );
        assert_eq!(runs.len(), 2);
    }

    /// Reliable broadcast survives one crash: under the same fault, the echoes of members 0
    /// and 1 deliver to member 2, which appears as a new derivation through the echo channel.
    /// No single crash kills every derivation, so the search ends with a certificate.
    #[test]
    fn ldfi_certifies_reliable_broadcast_under_one_crash() {
        let outcome = search(true, 1);
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
        let outcome = search(true, 2);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }
}
