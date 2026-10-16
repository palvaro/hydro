//! Lineage-driven fault injection on every operator in `hydro_lang` that *produces* a
//! collection labelled `EventualConsistency`, under the specification the label stands for
//! (`Spec::eventual_consistency`; `docs/design/hydrofrac.md`): every correct member holds
//! every fact of the collection that some correct member holds, by the end of the run, over
//! the facts of the failure-free run.
//!
//! The label promises that replicas compute the same contents given the same inputs. These
//! tests ask whether the fault model the transport declares can withhold inputs from some
//! replicas: the premise the label leaves to the program. The producers:
//!
//! - [`Stream::broadcast_closed`] from a process to a cluster, over `fail_stop` (the
//!   transport's `ConsistencyGuarantee` is `EventualConsistency`, justified in
//!   `hydro_lang::networking` by "a failed connection models the *recipient* as having
//!   failed"). The sender is not considered; a crash of the sender between two sends of one
//!   fan-out leaves two members with different sets. One crash.
//! - The same over `lossy_delayed_forever` (also `EventualConsistency`, justified by "dropped
//!   messages are modeled as indefinitely delayed, so the output is a lower bound that is
//!   eventually delivered in full"). The closed broadcast sends once and nothing retransmits,
//!   so a message delayed forever is a message a member never gets. One omission, no crash.
//! - [`Stream::broadcast_closed`] from a cluster to a cluster (every source member to every
//!   destination member), over `fail_stop`. A source member crashing mid-fan-out.
//! - The echoing reliable broadcast of [`super::reliable_broadcast`], as the control: the
//!   same specification, generated rather than written, certifies it.
//!
//! The other producers of the label, for the record: `source_interval*` on a cluster asserts
//! it on a stream of `()` (trivially the same contents at every member); keyed `fold` /
//! `reduce` with algebraic proofs preserve it (same inputs, same result; not a fault
//! question); `hydro_std::quorum::collect_quorum*` assert it with a proof that reads `TODO`;
//! Raft asserts it on its `committed` output, which the durability specification of
//! [`super::raft_ldfi`] is the generated shape of.

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{Fault, Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::Spec;
    use serde::{Deserialize, Serialize};

    use crate::cluster::reliable_broadcast::{
        Broadcaster, EchoHook, InitialHook, Member, Payload, broadcast,
    };

    pub struct Source;

    const MEMBERS: u32 = 3;
    const SOURCES: u32 = 2;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    struct Item(u32);

    /// Which transport the closed broadcast declares.
    #[derive(Clone, Copy, Debug)]
    enum Transport {
        FailStop,
        LossyDelayedForever,
    }

    /// `broadcast_closed` from one process to a cluster of three, under the generated
    /// specification. The body sends two items and quiesces; there is nothing to converge
    /// beyond delivery, so quiescence is the convergence witness.
    fn process_to_cluster(transport: Transport, crashes: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<Item, TotalOrder, ExactlyOnce>();

        // The collection the label is on. Batching drops the label, so it is observed through
        // a hook-free path straight off the channel; the observer's accumulation is the view.
        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        match transport {
            Transport::FailStop => {
                let at_members =
                    items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
                let observed = spec.observe_cluster(at_members.weaken_consistency());
                spec.eventual_consistency(observed, correct);
            }
            Transport::LossyDelayedForever => {
                let at_members = items.broadcast_closed(
                    &members,
                    TCP.lossy_delayed_forever().name("bcast").bincode(),
                );
                let observed = spec.observe_cluster(at_members.weaken_consistency());
                spec.eventual_consistency(observed, correct);
            }
        }

        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .with_provenance();
        if matches!(transport, Transport::LossyDelayedForever) {
            // The simulator's own model of this transport is "a dropped message arrives at the
            // end"; the search's omission is the drop itself.
            sim = sim.test_safety_only();
        }
        let compiled = sim.compiled();

        Ldfi::new()
            .level(Level::Record)
            .crashable("source", &source)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .crash_budget(crashes)
            .spec(&spec)
            .search(&compiled, async |_run| {
                send.send(Item(1));
                send.send(Item(2));
                hydro_lang::sim::quiesce().await;
            })
    }

    /// `broadcast_closed` from a cluster of two sources to a cluster of three members over
    /// `fail_stop`, each source broadcasting its own item.
    fn cluster_to_cluster(crashes: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let sources = flow.cluster::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = sources.sim_input::<Item, TotalOrder, ExactlyOnce>();

        let at_members = items
            .broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode())
            .values();

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let observed = spec.observe_cluster(at_members.weaken_consistency());
        spec.eventual_consistency(observed, correct);

        let compiled = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&sources, SOURCES as usize)
            .with_cluster_size(&members, MEMBERS as usize)
            .with_provenance()
            .compiled();

        Ldfi::new()
            .level(Level::Record)
            .crashable_cluster("source", &sources, 0..SOURCES)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .crash_budget(crashes)
            .spec(&spec)
            .search(&compiled, async |_run| {
                send.send(0, Item(10));
                send.send(1, Item(20));
                hydro_lang::sim::quiesce().await;
            })
    }

    /// The reliable broadcast of [`super::super::reliable_broadcast`] (every member re-broadcasts
    /// what it first sees) under the generated specification, with or without the echo.
    fn echo_broadcast(echo: bool, crashes: usize) -> Outcome {
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

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let observed = spec.observe_cluster(deliveries);
        spec.eventual_consistency(observed, correct);

        let compiled = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .with_provenance()
            .compiled();

        Ldfi::new()
            .level(Level::Record)
            .crashable("broadcaster", &broadcaster)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .crash_budget(crashes)
            .spec(&spec)
            .search(&compiled, async |_run| {
                source_send.send(Payload(42));
                hydro_lang::sim::quiesce().await;
            })
    }

    fn expect_counterexample(outcome: Outcome, what: &str) -> Vec<Fault> {
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = outcome else {
            panic!("{what}: expected a counterexample, got {outcome}");
        };
        faults.iter().cloned().collect()
    }

    /// Process → cluster over `fail_stop`: the label is `EventualConsistency`; one crash of the
    /// sender between two fan-out sends refutes its premise.
    #[test]
    fn ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash() {
        let faults = expect_counterexample(
            process_to_cluster(Transport::FailStop, 1),
            "broadcast_closed over fail_stop",
        );
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "source")),
            "the counterexample should be the sender's crash: {faults:?}"
        );
    }

    /// Process → cluster over `lossy_delayed_forever`: the label is `EventualConsistency` too,
    /// on the reading that a dropped message is only delayed. Nothing retransmits a closed
    /// broadcast, so one omission and no crash refute the premise.
    #[test]
    fn ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission() {
        let faults = expect_counterexample(
            process_to_cluster(Transport::LossyDelayedForever, 0),
            "broadcast_closed over lossy_delayed_forever",
        );
        assert_eq!(faults.len(), 1, "one omission suffices: {faults:?}");
        assert!(matches!(faults[0], Fault::Omit(_)), "{faults:?}");
    }

    /// Cluster → cluster over `fail_stop`: a source member crashing mid-fan-out.
    #[test]
    fn ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash() {
        let faults = expect_counterexample(cluster_to_cluster(1), "cluster broadcast_closed");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node.starts_with("source"))),
            "the counterexample should be a source member's crash: {faults:?}"
        );
    }

    /// Control: the generated specification separates the protocols the way the hand-written
    /// one did. Plain broadcast breaks; the echoing broadcast certifies under one crash.
    #[test]
    fn ec_generated_spec_separates_plain_from_reliable_broadcast() {
        expect_counterexample(echo_broadcast(false, 1), "plain broadcast");
        let outcome = echo_broadcast(true, 1);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "reliable broadcast: expected a certificate, got {outcome}"
        );
    }
}
