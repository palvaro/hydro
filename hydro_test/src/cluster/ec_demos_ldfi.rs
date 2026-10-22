//! Lineage-driven fault injection on the EC-inference demonstration protocols of
//! `hydro_std::ec_inference_demos` (brought over from the `eventual_consistency` branch),
//! under the specification their `EventualConsistency` label stands for
//! (`Spec::eventual_consistency`; `docs/design/hydrofrac.md`).
//!
//! Those protocols were written to show EC *inferred* by the type system from a few trusted
//! premises: `fan_out`'s single `manual_proof!` rests on membership completeness, holder
//! persistence and channel eventual delivery. None of the premises names the *data holder
//! crashing*, and the branch's own docs record the gap in prose (`leader_merge`: "this EC
//! label silently assumes the leader's survival, `F = {leader}`"). The generated
//! specification turns each such sentence into a search:
//!
//! - [`broadcast_live_from_process`]: one hop over the live membership relation, EC minted
//!   by `fan_out`. The sender crashing mid-fan-out should break it.
//! - [`reliable_broadcast_closed`] and [`reliable_broadcast_live`]: the echo cycle is the
//!   retransmission the premises assume; both should certify.
//! - [`g_set_gossip`] and [`g_set_gossip_live`]: state-based gossip; should certify.
//! - [`uniform_reliable_broadcast_closed`]: delivery after a threshold of echoes; the
//!   threshold is a quorum argument and the generated spec will say whether its delivered
//!   set converges under the crashes that leave two members correct.
//! - [`leader_merge_broadcast`]: EC rests on the leader; the leader crashing mid-broadcast
//!   should break it, which is the branch's own claim.
//!
//! Membership in the simulator is static ("everyone joined"), so the `_live` variants are
//! exercised for faults, not for join timing. All channels in these demos are `fail_stop`,
//! so the fault vocabulary is crashes, under the default crash bounds: every combination of
//! crashes of the registered nodes except the vacuous ones (all of them; all or all but one
//! of the checked cluster's members).

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Fault, LabelVerdict, Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::Spec;
    use hydro_std::ec_inference_demos::broadcast_live::broadcast_live_from_process;
    use hydro_std::ec_inference_demos::crdt_gossip::{g_set_gossip, g_set_gossip_live};
    use hydro_std::ec_inference_demos::leader_merge::leader_merge_broadcast;
    use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;
    use hydro_std::ec_inference_demos::reliable_broadcast::{
        reliable_broadcast_closed, reliable_broadcast_live,
    };
    use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

    pub struct Source;
    pub struct Member;
    pub struct Writer;

    const MEMBERS: u32 = 3;

    /// Which broadcast demo a process → cluster search runs.
    #[derive(Clone, Copy, Debug)]
    enum Broadcast {
        Live,
        ReliableClosed,
        ReliableLive,
        /// Uniform reliable broadcast with this echo threshold.
        Uniform(usize),
    }

    /// A process source broadcasting two items to a cluster of three, under the generated
    /// specification on the demo's EC output. Source and members crashable.
    fn broadcast_search(which: Broadcast) -> Outcome {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let delivered: Stream<u32, Cluster<'_, Member>, Unbounded, NoOrder, ExactlyOnce> =
            match which {
                Broadcast::Live => broadcast_live_from_process(
                    items,
                    &members,
                    TCP.fail_stop().name("bcast").bincode(),
                )
                .weaken_consistency(),
                Broadcast::ReliableClosed => {
                    reliable_broadcast_closed(items, &members).weaken_consistency()
                }
                Broadcast::ReliableLive => {
                    reliable_broadcast_live(items, &members).weaken_consistency()
                }
                Broadcast::Uniform(threshold) => {
                    uniform_reliable_broadcast_closed(items, &members, threshold)
                        .weaken_consistency()
                }
            };

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let observed = spec.observe_cluster(delivered);
        spec.eventual_consistency(observed, correct);

        // The echo cycles of the reliable variants are small enough to explore exhaustively;
        // uniform broadcast's threshold logic over the echoes is not (its schedule space ran
        // past 240,000 interleavings), so it is sampled.
        let (exploration, fuzz) = match which {
            Broadcast::Uniform(_) => (Exploration::Fuzz { instances: 8 }, 8),
            _ => (Exploration::Exhaustive, 1),
        };
        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .unit_test_fuzz_iterations(fuzz)
            .with_provenance()
            .compiled();

        Ldfi::new()
            .level(Level::Precise)
            .exploration(exploration)
            .crashable("source", &source)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .spec(&spec)
            .search(&compiled, async |_run| {
                send.send(1);
                send.send(2);
                hydro_lang::sim::quiesce().await;
            })
    }

    /// G-Set gossip (closed or live fan-out) among three members, each contributing one
    /// element, with `ROUNDS` gossip ticks fired at every member after the updates. The EC
    /// output is an unbounded set-valued singleton per member; it is observed through
    /// `sample_eager` and flattened, so the facts are the elements each member's set has
    /// held.
    fn gossip_search(live: bool) -> Outcome {
        const ROUNDS: u32 = 3;
        let mut flow = FlowBuilder::new();
        let members = flow.cluster::<Member>();
        let (update, updates) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
        let updates = updates.weaken_ordering::<NoOrder>();
        let set = if live {
            g_set_gossip_live(&members, updates, ticks)
        } else {
            g_set_gossip(&members, updates, ticks)
        };
        // Observed through a member-side tick: each execution snapshots the set and emits its
        // elements; the observer accumulates them as facts. (`sample_eager` yields an
        // at-least-once stream, which the simulator cannot yet send across the network.)
        let member_tick = members.tick();
        let elements = set
            .weaken_consistency()
            .snapshot(
                &member_tick,
                nondet!(/** every change of the set is observed */),
            )
            .into_stream()
            .flat_map_unordered(q!(|set: BTreeSet<u32>| set))
            .all_ticks();

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let observed = spec.observe_cluster(elements);
        spec.eventual_consistency(observed, correct);

        // Gossip rounds among three members multiply interleavings; sampled schedules.
        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .unit_test_fuzz_iterations(8);
        if std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled: CompiledSim = sim.compiled();

        Ldfi::new()
            .level(Level::Precise)
            .level_from_env()
            .exploration(Exploration::Fuzz { instances: 8 })
            .crashable_cluster("member", &members, 0..MEMBERS)
            .spec(&spec)
            .search(&compiled, async |_run| {
                for m in 0..MEMBERS {
                    update.send(m, 10 + m);
                }
                hydro_lang::sim::quiesce().await;
                for _ in 0..ROUNDS {
                    tick.send_many((0..MEMBERS).map(|m| (m, ())));
                    hydro_lang::sim::quiesce().await;
                }
            })
    }

    /// Two writers send keyed values to one leader process, which merges them into one
    /// order and broadcasts it to three replicas (`leader_merge_broadcast`). The EC label on
    /// the replicas' stream rests on the leader; leader and replicas are crashable.
    fn leader_merge_search() -> Outcome {
        let mut flow = FlowBuilder::new();
        let writers = flow.cluster::<Writer>();
        let leader = flow.process::<Source>();
        let replicas = flow.cluster::<Member>();
        let (write, writes) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let at_leader = writes.send(&leader, TCP.fail_stop().name("to_leader").bincode());
        let merged = leader_merge_broadcast(
            at_leader,
            &replicas,
            TCP.fail_stop().name("merged").bincode(),
            nondet!(/** the leader's interleaving of the writers */),
        )
        .weaken_consistency();

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&replicas, 0..MEMBERS);
        let observed = spec.observe_cluster(merged);
        spec.eventual_consistency(observed, correct);

        // The provenance rewrite does not yet handle `entries_partially_ordered` on a keyed
        // stream (it destructures the element as `Tagged<_>` where the layout is
        // `(key, Tagged<_>)`), so the program is compiled without provenance; at `Precise` the
        // search then derives every record by counterfactual replay.
        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&replicas, MEMBERS as usize)
            .compiled();

        Ldfi::new()
            .level(Level::Precise)
            .level_from_env()
            .crashable("leader", &leader)
            .crashable_cluster("replica", &replicas, 0..MEMBERS)
            .spec(&spec)
            .search(&compiled, async |_run| {
                write.send(0, 100);
                write.send(1, 200);
                hydro_lang::sim::quiesce().await;
            })
    }

    pub struct Proposer;
    pub struct Acceptor;
    pub struct Learner;

    /// Multi-decree Paxos (`multi_paxos`): one proposer told to lead round 1, two commands,
    /// three acceptors (majority two), three learners. The EC output is `learned`, the chosen
    /// decrees at each learner, disseminated by the leader's broadcast plus a learner-side
    /// echo cycle ("every live learner converges to the same set"). Proposers, acceptors and
    /// learners are crashable, one crash per hypothesis; all channels are `fail_stop`.
    fn multi_paxos_search() -> Outcome {
        let mut flow = FlowBuilder::new();
        let proposers = flow.cluster::<Proposer>();
        let acceptors = flow.cluster::<Acceptor>();
        let learners = flow.cluster::<Learner>();
        let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
        let learned = outputs.learned.weaken_consistency();

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&learners, 0..MEMBERS);
        let observed = spec.observe_cluster(learned);
        spec.eventual_consistency(observed, correct);

        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&acceptors, 3)
            .with_cluster_size(&learners, MEMBERS as usize)
            .unit_test_fuzz_iterations(8)
            .with_provenance()
            .compiled();

        Ldfi::new()
            .level(Level::Precise)
            .exploration(Exploration::Fuzz { instances: 8 })
            .crashable_cluster("proposer", &proposers, 0..1)
            .crashable_cluster("acceptor", &acceptors, 0..3)
            .crashable_cluster("learner", &learners, 0..MEMBERS)
            // The protocol's own fault model: one crash (`f = 1` of three acceptors). Under the
            // default bounds the search does not finish within 2,000 hypotheses (see
            // `docs/design/ldfi-level-ablation.md`, "Unbounded crash budget").
            .crash_budget(1)
            .spec(&spec)
            .search(&compiled, async |_run| {
                lead.send(0, 1);
                hydro_lang::sim::quiesce().await;
                command.send(0, 7);
                command.send(0, 8);
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

    fn expect_certificate(outcome: Outcome, what: &str) {
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "{what}: expected a certificate, got {outcome}"
        );
    }

    /// `fan_out`'s premises do not include the data holder surviving: one hop over live
    /// membership breaks under the sender's crash between two fan-out sends.
    #[test]
    fn ec_demo_broadcast_live_breaks_under_a_sender_crash() {
        let faults = expect_counterexample(broadcast_search(Broadcast::Live), "broadcast_live");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "source")),
            "{faults:?}"
        );
    }

    /// The echo cycle is the retransmission the premises assume: reliable broadcast, closed
    /// and live, certifies under every combination of crashes that leaves two members correct.
    #[test]
    fn ec_demo_reliable_broadcast_certifies() {
        expect_certificate(
            broadcast_search(Broadcast::ReliableClosed),
            "reliable_broadcast_closed",
        );
        expect_certificate(
            broadcast_search(Broadcast::ReliableLive),
            "reliable_broadcast_live",
        );
    }

    /// Uniform reliable broadcast delivers after `threshold` echoes. With a threshold of
    /// two among three members, the admissible crashes leave two correct members; the test
    /// reports what the search finds for thresholds 1 and 2.
    #[test]
    fn ec_demo_uniform_broadcast() {
        let one = broadcast_search(Broadcast::Uniform(1));
        eprintln!("uniform(1): {one}");
        let two = broadcast_search(Broadcast::Uniform(2));
        eprintln!("uniform(2): {two}");
        assert!(
            !matches!(one, Outcome::Budget { .. } | Outcome::Vacuous { .. }),
            "uniform(1): {one}"
        );
        assert!(
            !matches!(two, Outcome::Budget { .. } | Outcome::Vacuous { .. }),
            "uniform(2): {two}"
        );
    }

    /// State-based G-Set gossip converges under the admissible crashes, closed and live.
    #[test]
    fn ec_demo_g_set_gossip_certifies() {
        expect_certificate(gossip_search(false), "g_set_gossip");
        expect_certificate(gossip_search(true), "g_set_gossip_live");
    }

    /// Every label of G-Set gossip (closed or live fan-out), automatically
    /// (`Spec::all_labels`), on the setup of [`gossip_search`]. The G-Set is a labelled
    /// `Singleton` (the `fold` at `crdt_gossip.rs:57`, `:141` live), covered by the stream it
    /// folds: the merge of the fresh elements with the peers' gossiped state. Failures end
    /// after the updates (`end_of_finite_failures`); the gossip rounds that follow are the
    /// retransmission that makes the merged stream converge among the survivors.
    fn gossip_all_labels(live: bool) -> hydro_lang::sim::ldfi::LabelResults {
        const ROUNDS: u32 = 3;
        let mut flow = FlowBuilder::new();
        let members = flow.cluster::<Member>();
        let (update, updates) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
        let updates = updates.weaken_ordering::<NoOrder>();
        let _set = if live {
            g_set_gossip_live(&members, updates, ticks)
        } else {
            g_set_gossip(&members, updates, ticks)
        };

        let spec = Spec::all_labels(&mut flow);

        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .unit_test_fuzz_iterations(8)
            .with_provenance()
            .compiled();

        let start = std::time::Instant::now();
        let results = Ldfi::new()
            .level(Level::Precise)
            .exploration(Exploration::Fuzz { instances: 8 })
            .crashable_cluster("member", &members, 0..MEMBERS)
            .spec(&spec)
            .search_all_labels(&compiled, async |run| {
                for m in 0..MEMBERS {
                    update.send(m, 10 + m);
                }
                hydro_lang::sim::quiesce().await;
                run.end_of_finite_failures();
                for _ in 0..ROUNDS {
                    tick.send_many((0..MEMBERS).map(|m| (m, ())));
                    hydro_lang::sim::quiesce().await;
                }
            });
        eprintln!(
            "g_set_gossip (live: {live}): {:.1}s",
            start.elapsed().as_secs_f64()
        );
        results
    }

    /// The G-Set, covered by its input, certifies under every admissible crash, closed and
    /// live; so does every label that is a verdict (the single hops into the merge are
    /// informational).
    #[test]
    fn ec_demo_g_set_gossip_all_labels() {
        for (live, site) in [(false, "crdt_gossip.rs:57"), (true, "crdt_gossip.rs:141")] {
            let results = gossip_all_labels(live);
            let set: Vec<_> = results.at(site).collect();
            assert_eq!(set.len(), 1, "{results}");
            assert!(
                matches!(
                    set[0].verdict,
                    LabelVerdict::CoveredBy {
                        outcome: Outcome::Certificate { .. },
                        ..
                    }
                ),
                "{results}"
            );
            assert!(results.all_certified_or_not_checked(), "{results}");
        }
    }

    /// `leader_merge`'s EC label "silently assumes the leader's survival" (its docs): the
    /// leader crashing between two of its broadcast sends is the counterexample.
    #[test]
    fn ec_demo_leader_merge_breaks_under_a_leader_crash() {
        let faults = expect_counterexample(leader_merge_search(), "leader_merge_broadcast");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "leader")),
            "{faults:?}"
        );
    }

    /// `multi_paxos.learned` is labelled EC with the argument that the learner-side echo makes
    /// dissemination all-or-nothing among live learners even if the proposer crashes
    /// mid-broadcast. Under one crash anywhere (proposer, acceptor or learner) that is what
    /// the search reports.
    #[test]
    fn ec_demo_multi_paxos_learned_under_one_crash() {
        let outcome = multi_paxos_search();
        eprintln!("{outcome}");
        assert!(
            !matches!(outcome, Outcome::Budget { .. } | Outcome::Vacuous { .. }),
            "{outcome}"
        );
    }

    /// Every label of `reliable_broadcast_closed`, automatically (`Spec::all_labels`): the
    /// delivered set certifies in the same 49 hypotheses as the manual closed search above (every
    /// admissible combination of a source crash point and one member's crash point); the initial
    /// single-hop broadcast the label is inferred from is violated on its own by the source's
    /// crash between two of its fan-out sends, and reported as an informational hop, since it
    /// reaches the delivered set only merged with the echo.
    #[test]
    fn ec_demo_reliable_broadcast_all_labels() {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let delivered = reliable_broadcast_closed(items, &members);
        let _out = delivered.weaken_consistency().sim_cluster_output();

        let spec = Spec::all_labels(&mut flow);

        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .with_provenance()
            .compiled();

        let results = Ldfi::new()
            .level(Level::Precise)
            .crashable("source", &source)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .spec(&spec)
            .search_all_labels(&compiled, async |_run| {
                send.send(1);
                send.send(2);
                hydro_lang::sim::quiesce().await;
            });
        // The delivered set (`new_messages`, the `unique`): the manual search's certificate.
        let delivered: Vec<_> = results.at("reliable_broadcast.rs:149").collect();
        assert_eq!(delivered.len(), 1, "{results}");
        let LabelVerdict::Searched(Outcome::Certificate { runs, .. }) = &delivered[0].verdict
        else {
            panic!("{results}");
        };
        assert_eq!(runs.len(), 49, "{results}");
        // The initial single-hop broadcast the label is inferred from: on its own, violated
        // by the source's crash; but it reaches the delivered set only merged with the echo,
        // so it is a merge-dominated hop, informational, and the `unique` is its verdict.
        let initial: Vec<_> = results.at("reliable_broadcast.rs:130").collect();
        assert_eq!(initial.len(), 1, "{results}");
        let LabelVerdict::Hop {
            merged_at,
            outcome: Outcome::Counterexample { faults, .. },
        } = &initial[0].verdict
        else {
            panic!("{results}");
        };
        assert!(merged_at.contains("reliable_broadcast.rs:149"), "{results}");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "source")),
            "{results}"
        );
        // The echo hop, likewise merge-dominated.
        let echo: Vec<_> = results.at("reliable_broadcast.rs:156").collect();
        assert_eq!(echo.len(), 1, "{results}");
        assert!(
            matches!(&echo[0].verdict, LabelVerdict::Hop { merged_at, .. } if merged_at.contains("reliable_broadcast.rs:149")),
            "{results}"
        );
        // Nothing is skipped silently: every label was searched; and with the hops
        // informational, the protocol's labels hold.
        assert!(
            results.verdicts.iter().all(|r| matches!(
                r.verdict,
                LabelVerdict::Searched(_) | LabelVerdict::Hop { .. }
            )),
            "{results}"
        );
        assert!(results.all_certified_or_not_checked(), "{results}");
        assert_eq!(results.counterexamples().count(), 0, "{results}");
    }

    /// Every label of `multi_paxos`, automatically. `learned` (the learners' delivered set)
    /// certifies as in the manual search above; the rest get verdicts, recorded in section 6
    /// of `docs/design/hydrofrac.md`. The spliced log (`epoch_splice.rs:190`, a
    /// `Singleton<SpliceState>`) is covered by the `SpliceFact` stream it folds
    /// (`multi_paxos.rs:608`), and the learners' two single hops into `learned`, the
    /// proposer's broadcast (`:582`) and the echo (`:591`), are informational: they reach
    /// the outside only merged, at `:588`.
    #[test]
    fn ec_demo_multi_paxos_all_labels() {
        let mut flow = FlowBuilder::new();
        let proposers = flow.cluster::<Proposer>();
        let acceptors = flow.cluster::<Acceptor>();
        let learners = flow.cluster::<Learner>();
        let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
        let _learned = outputs.learned.weaken_consistency().sim_cluster_output();

        let spec = Spec::all_labels(&mut flow);

        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&acceptors, 3)
            .with_cluster_size(&learners, MEMBERS as usize)
            .unit_test_fuzz_iterations(8)
            .with_provenance()
            .compiled();

        let results = Ldfi::new()
            .level(Level::Precise)
            .exploration(Exploration::Fuzz { instances: 8 })
            .crashable_cluster("proposer", &proposers, 0..1)
            .crashable_cluster("acceptor", &acceptors, 0..3)
            .crashable_cluster("learner", &learners, 0..MEMBERS)
            // One crash: the protocol's fault model, as in `multi_paxos_search`.
            .crash_budget(1)
            .spec(&spec)
            .search_all_labels(&compiled, async |_run| {
                lead.send(0, 1);
                hydro_lang::sim::quiesce().await;
                command.send(0, 7);
                command.send(0, 8);
                hydro_lang::sim::quiesce().await;
            });
        let learned: Vec<_> = results
            .at("multi_paxos.rs:588")
            .filter(|r| r.collection.exit)
            .collect();
        assert_eq!(learned.len(), 1, "{results}");
        assert!(
            matches!(
                learned[0].verdict,
                LabelVerdict::Searched(Outcome::Certificate { .. })
            ),
            "{results}"
        );
        let log: Vec<_> = results.at("epoch_splice.rs:190").collect();
        assert_eq!(log.len(), 1, "{results}");
        assert!(
            matches!(
                &log[0].verdict,
                LabelVerdict::CoveredBy {
                    input_site,
                    outcome: Outcome::Certificate { .. },
                } if input_site.contains("multi_paxos.rs:608")
            ),
            "{results}"
        );
        for hop in ["multi_paxos.rs:582", "multi_paxos.rs:591"] {
            assert!(
                results.at(hop).all(|r| matches!(
                    &r.verdict,
                    LabelVerdict::Hop { merged_at, .. } if merged_at.contains("multi_paxos.rs:588")
                )),
                "{hop}: {results}"
            );
        }
        assert!(
            !results
                .verdicts
                .iter()
                .any(|r| matches!(r.verdict.outcome(), Some(Outcome::Budget { .. }))),
            "{results}"
        );
    }
}
