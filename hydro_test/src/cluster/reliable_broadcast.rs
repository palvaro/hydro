//! Reliable broadcast (ported from the echo-based `reliable_broadcast_closed` on the
//! `eventual_consistency` branch), a target for lineage-driven fault injection.
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
//! The deliveries carry the `EventualConsistency` label as a *claim*: it holds with the echo
//! and is refuted without it, which is what a fault-injection checker should find.
//!
//! # Shape
//!
//! Each member runs one tick that consumes both channels (the broadcaster's and the echoes')
//! through batches, deduplicates against persisted state, echoes fresh messages to every
//! member (when `echo` is set), and delivers them. The echo channel closes a dataflow cycle
//! through the network, which is an asynchronous boundary.

use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Broadcaster {}

pub struct Member {}

/// The broadcast payload.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Payload(pub u32);

/// Broadcasts `source` from the broadcaster to every member. With `echo`, every member
/// re-broadcasts each message it sees for the first time before delivering it (reliable
/// broadcast); without, members deliver only what the broadcaster sent them (plain
/// broadcast). Returns each member's deliveries, labelled `EventualConsistency` (a claim that
/// only the echoing variant keeps under a broadcaster crash).
///
/// # Non-Determinism
/// - `nondet_initial`: the batching of the broadcaster's messages at each member.
/// - `nondet_echo`: the batching of echoes at each member.
pub fn broadcast<'a>(
    members: &Cluster<'a, Member>,
    source: Stream<Payload, Process<'a, Broadcaster>, Unbounded, TotalOrder>,
    echo: bool,
    nondet_initial: NonDet,
    nondet_echo: NonDet,
) -> Stream<Payload, Cluster<'a, Member, EventualConsistency>, Unbounded, NoOrder> {
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
        let from_broadcaster = use::batch(initial, nondet!(/** The caller's `nondet_initial`. */ nondet_initial));
        let from_members = use::batch(echoes, nondet!(/** The caller's `nondet_echo`. */ nondet_echo));
        let mut seen = use::state(|l| {
            l.source_iter(q!(Vec::<Payload>::new()))
                .weaken_ordering::<NoOrder>()
        });

        let arrived = from_broadcaster.chain(from_members.map(q!(|(_, payload)| payload)));
        let fresh = arrived.unique().filter_not_in(seen.clone());
        seen = seen.chain(fresh.clone());

        // `echo` is fixed when the program is built, so it selects the dataflow here rather
        // than being captured by a staged closure.
        let echoed = if echo {
            fresh.clone()
        } else {
            fresh.clone().filter(q!(|_| false))
        };
        (fresh, echoed)
    };

    echo_handle.complete(
        to_echo
            .broadcast_closed(members, TCP.fail_stop().name("echo").bincode())
            .entries(),
    );

    deliveries.assert_has_consistency_of::<Cluster<'a, Member, EventualConsistency>>(manual_proof!(
        /** claimed, not proven: with `echo`, every member re-broadcasts what it first sees
        before delivering, so one correct recipient reaches every correct member; without
        `echo`, a broadcaster crash mid-fan-out refutes it. This label is the target of
        fault injection. */
    ))
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::{Broadcaster, Member, Payload, broadcast};

    const MEMBERS: u32 = 3;

    /// Without faults, both variants deliver the message to every member.
    fn delivers_to_all(echo: bool) {
        let mut flow = FlowBuilder::new();
        let broadcaster = flow.process::<Broadcaster>();
        let members = flow.cluster::<Member>();
        let (source_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
        let out = broadcast(
            &members,
            source,
            echo,
            nondet!(/** test */),
            nondet!(/** test */),
        )
        .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .exhaustive(async || {
                source_send.send(Payload(42));
                for member in 0..MEMBERS {
                    let got: Vec<Payload> = out.collect_n_sorted_only(member, 1).await;
                    assert_eq!(got, vec![Payload(42)], "member {member}");
                }
            });
    }

    #[test]
    fn plain_broadcast_delivers_to_all_without_faults() {
        delivers_to_all(false);
    }

    #[test]
    fn reliable_broadcast_delivers_to_all_without_faults() {
        delivers_to_all(true);
    }
}
