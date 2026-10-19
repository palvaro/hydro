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
//!   set converges under one crash.
//! - [`leader_merge_broadcast`]: EC rests on the leader; the leader crashing mid-broadcast
//!   should break it, which is the branch's own claim.
//!
//! Membership in the simulator is static ("everyone joined"), so the `_live` variants are
//! exercised for faults, not for join timing. All channels in these demos are `fail_stop`,
//! so the fault vocabulary is crashes.

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
    /// specification on the demo's EC output. Source and members crashable, budget
    /// `crashes`.
    fn broadcast_search(which: Broadcast, crashes: usize) -> Outcome {
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
            .crash_budget(crashes)
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
    fn gossip_search(live: bool, crashes: usize) -> Outcome {
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
            .crash_budget(crashes)
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
    fn leader_merge_search(crashes: usize) -> Outcome {
        leader_merge_search_with(crashes, Level::Precise, false, false)
    }

    /// [`leader_merge_search`] with the lineage level, disjunctive provenance and the
    /// provenance compile as parameters (for the level ablation; see
    /// `docs/design/ldfi-level-ablation.md`).
    fn leader_merge_search_with(
        crashes: usize,
        level: Level,
        disjunctive: bool,
        provenance: bool,
    ) -> Outcome {
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
        // search then derives every record by counterfactual replay. `provenance` opts in to
        // the rewrite anyway (to measure whether it still rejects the program).
        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&replicas, MEMBERS as usize);
        if provenance {
            sim = sim.with_provenance();
        }
        let compiled: CompiledSim = sim.compiled();

        Ldfi::new()
            .level(level)
            .level_from_env()
            .disjunctive_provenance(disjunctive)
            .crashable("leader", &leader)
            .crashable_cluster("replica", &replicas, 0..MEMBERS)
            .crash_budget(crashes)
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
    /// learners are crashable; all channels are `fail_stop`.
    fn multi_paxos_search(crashes: usize) -> Outcome {
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
            .crash_budget(crashes)
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
        let faults = expect_counterexample(broadcast_search(Broadcast::Live, 1), "broadcast_live");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "source")),
            "{faults:?}"
        );
    }

    /// The echo cycle is the retransmission the premises assume: reliable broadcast, closed
    /// and live, certifies under one crash.
    #[test]
    fn ec_demo_reliable_broadcast_certifies_under_one_crash() {
        expect_certificate(
            broadcast_search(Broadcast::ReliableClosed, 1),
            "reliable_broadcast_closed",
        );
        expect_certificate(
            broadcast_search(Broadcast::ReliableLive, 1),
            "reliable_broadcast_live",
        );
    }

    /// Uniform reliable broadcast delivers after `threshold` echoes. With a threshold of
    /// two among three members, one crash leaves two correct members; the test reports what
    /// the search finds for thresholds 1 and 2.
    #[test]
    fn ec_demo_uniform_broadcast_under_one_crash() {
        let one = broadcast_search(Broadcast::Uniform(1), 1);
        eprintln!("uniform(1): {one}");
        let two = broadcast_search(Broadcast::Uniform(2), 1);
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

    /// State-based G-Set gossip converges under one crash, closed and live.
    #[test]
    fn ec_demo_g_set_gossip_certifies_under_one_crash() {
        expect_certificate(gossip_search(false, 1), "g_set_gossip");
        expect_certificate(gossip_search(true, 1), "g_set_gossip_live");
    }

    /// `leader_merge`'s EC label "silently assumes the leader's survival" (its docs): the
    /// leader crashing between two of its broadcast sends is the counterexample.
    #[test]
    fn ec_demo_leader_merge_breaks_under_a_leader_crash() {
        let faults = expect_counterexample(leader_merge_search(1), "leader_merge_broadcast");
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "leader")),
            "{faults:?}"
        );
    }

    /// Level ablation of `leader_merge_broadcast` (measurement, not a test: does nothing
    /// unless `LDFI_ABLATION_OUT` is set; `docs/design/ldfi-level-ablation.md`). Runs the
    /// `exec`, `precise` and `disjunctive` columns under exhaustive exploration at crash
    /// budgets 1 and 2, compiled without provenance (the default) and, if
    /// `LDFI_ABLATION_PROVENANCE` is set, with it.
    #[test]
    fn ec_demo_leader_merge_level_ablation() {
        let Ok(out) = std::env::var("LDFI_ABLATION_OUT") else {
            return;
        };
        let provenance = std::env::var("LDFI_ABLATION_PROVENANCE").is_ok();
        let mut table = String::from(
            "| crashes | provenance | column | verdict | hypotheses | schedules | mean deriv | clauses/run | replays | fallbacks | overflows | secs |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n",
        );
        for crashes in [1usize, 2] {
            for (name, level, disjunctive) in [
                ("exec", Level::Execution, false),
                ("precise", Level::Precise, false),
                ("disjunctive", Level::Precise, true),
            ] {
                let start = std::time::Instant::now();
                let outcome = std::panic::catch_unwind(|| {
                    leader_merge_search_with(crashes, level, disjunctive, provenance)
                });
                let secs = start.elapsed().as_secs_f64();
                let row = match outcome {
                    Err(_) => format!(
                        "| {crashes} | {provenance} | {name} | n/a: panicked | | | | | | | | {secs:.1} |\n"
                    ),
                    Ok(outcome) => {
                        eprintln!("{outcome}");
                        let verdict = match &outcome {
                            Outcome::Counterexample { .. } => "violated",
                            Outcome::Certificate { .. } => "certificate",
                            Outcome::Exhausted { .. } => "exhausted",
                            Outcome::Budget { .. } => "budget",
                            Outcome::Vacuous { .. } => "vacuous",
                        };
                        let (mut derivations, mut deps, mut replays, mut fallbacks, mut overflows) =
                            (0usize, 0usize, 0usize, 0usize, 0usize);
                        for schedule in outcome.runs().iter().flat_map(|h| &h.schedules) {
                            replays += schedule.replays;
                            fallbacks += schedule.replay_fallbacks;
                            overflows += schedule.witness_overflows;
                            if schedule.goal_held {
                                for d in &schedule.derivations {
                                    derivations += 1;
                                    deps += d.len();
                                }
                            }
                        }
                        let hyp = outcome.runs().len();
                        format!(
                            "| {crashes} | {provenance} | {name} | {verdict} | {hyp} | {} | {:.2} | {:.2} | {replays} | {fallbacks} | {overflows} | {secs:.1} |\n",
                            outcome.schedules(),
                            if derivations == 0 {
                                0.0
                            } else {
                                deps as f64 / derivations as f64
                            },
                            if hyp == 0 {
                                0.0
                            } else {
                                derivations as f64 / hyp as f64
                            },
                        )
                    }
                };
                eprint!("{row}");
                table.push_str(&row);
                std::fs::write(&out, &table).expect("LDFI_ABLATION_OUT not writable");
            }
        }
    }

    /// `multi_paxos.learned` is labelled EC with the argument that the learner-side echo makes
    /// dissemination all-or-nothing among live learners even if the proposer crashes
    /// mid-broadcast. Under one crash anywhere (proposer, acceptor or learner) that is what
    /// the search reports.
    #[test]
    fn ec_demo_multi_paxos_learned_under_one_crash() {
        let outcome = multi_paxos_search(1);
        eprintln!("{outcome}");
        assert!(
            !matches!(outcome, Outcome::Budget { .. } | Outcome::Vacuous { .. }),
            "{outcome}"
        );
    }

    /// Every label of `reliable_broadcast_closed`, automatically (`Spec::all_labels`): the
    /// delivered set certifies in the same 10 hypotheses as the manual search above; the
    /// initial single-hop broadcast the label is inferred from is violated by the source's
    /// crash between two of its fan-out sends.
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
            .crash_budget(1)
            .spec(&spec)
            .search_all_labels(&compiled, async |_run| {
                send.send(1);
                send.send(2);
                hydro_lang::sim::quiesce().await;
            });
        eprintln!("{results}");
        // The delivered set (`new_messages`, the `unique`): the manual search's certificate.
        let delivered: Vec<_> = results.at("reliable_broadcast.rs:149").collect();
        assert_eq!(delivered.len(), 1, "{results}");
        let LabelVerdict::Searched(Outcome::Certificate { runs, .. }) = &delivered[0].verdict
        else {
            panic!("{results}");
        };
        assert_eq!(runs.len(), 10, "{results}");
        // The initial single-hop broadcast the label is inferred from: the source's crash.
        let initial: Vec<_> = results.at("reliable_broadcast.rs:130").collect();
        assert_eq!(initial.len(), 1, "{results}");
        let LabelVerdict::Searched(Outcome::Counterexample { faults, .. }) = &initial[0].verdict
        else {
            panic!("{results}");
        };
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "source")),
            "{results}"
        );
        // Nothing is skipped silently: every label has a verdict.
        assert!(
            results
                .verdicts
                .iter()
                .all(|r| matches!(r.verdict, LabelVerdict::Searched(_))),
            "{results}"
        );
    }

    /// Every label of `multi_paxos`, automatically. `learned` (the learners' delivered set)
    /// certifies as in the manual search above; the rest get verdicts, recorded in section 6
    /// of `docs/design/hydrofrac.md`.
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
            .crash_budget(1)
            .spec(&spec)
            .search_all_labels(&compiled, async |_run| {
                lead.send(0, 1);
                hydro_lang::sim::quiesce().await;
                command.send(0, 7);
                command.send(0, 8);
                hydro_lang::sim::quiesce().await;
            });
        eprintln!("{results}");
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
        assert!(
            !results
                .verdicts
                .iter()
                .any(|r| matches!(r.verdict, LabelVerdict::Searched(Outcome::Budget { .. }))),
            "{results}"
        );
    }
}
