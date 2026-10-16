//! Lineage-driven fault injection on RAFT ([`super::raft`]), the closure-encoded protocol
//! the static lineage levels cannot see into: one `raft_step` closure per tick over a
//! `RaftServerState`, reading its inputs and writing its outputs through `by_ref` /
//! `by_mut` handles. Under the provenance rewrite the step's outputs are *opaque*
//! (`Prov::OPAQUE`), and their lineage is derived by counterfactual replay of the member.
//!
//! The specification is **agreement** (state machine safety), as views over the members'
//! committed entries (`hydro_lang::sim::spec`): `PRE`, member 0 committed its request `a`;
//! `POST`, no two committed entries at the same index disagree, over the correct members and
//! member 0's own commits while it lived. Three members, members crashable, intra-cluster
//! traffic on a lossy channel (omissions until the end of finite failures, which the body
//! declares once that entry has been replicated and committed).
//!
//! The scenario lets the simulator's interleavings and the search's faults compose. Member 1
//! wins term 1 and receives request `b`; member 0's election timer fires in the same phase,
//! so the schedule decides whether member 2 sees `RequestVote(term 2)` before or after
//! `AppendEntries(b)`, i.e. whether 0 wins term 2 while 1 is left holding an unreplicated
//! `b@1`. If it does, 0 commits `a@1` (with 2's acknowledgement); then 1 campaigns, and if
//! elected commits `y`. RAFT's §5.4.1 election restriction keeps 1 from winning: its last
//! entry is of an older term than 0's and 2's. With the injected bug
//! ([`RaftConfig::election_restriction_ignores_term`], which compares indexes only and which
//! every unit test of `raft.rs` passes) the search must find the loss of `AppendEntries(a)` to
//! member 1, under a schedule in which 0 was elected: then 1's stale `b@1` is as long as the
//! others' logs, it is granted the votes, and `b` overwrites the committed `a`.
//!
//! Each hypothesis is run under several simulator schedules (`Exploration::Fuzz`).

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::SimClusterSender;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Fault, Ldfi, Level, Outcome, Run, Strategy};
    use hydro_lang::sim::spec::Spec;

    use crate::cluster::raft::{RaftConfig, RaftHooks, Replica, raft_server};

    const MEMBERS: u32 = 3;
    /// Schedules sampled per hypothesis.
    const SCHEDULES: usize = 4;

    struct Harness {
        election: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        heartbeat: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        requests: SimClusterSender<String, TotalOrder, ExactlyOnce>,
    }

    /// Compiles a three-member RAFT cluster with every batch hooked, with or without the
    /// injected bug. Fault model: members crash; the intra-cluster channel is lossy.
    fn compile(bug: bool) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        compile_with(bug, SCHEDULES)
    }

    /// [`compile`] with the number of instances a `fuzz` / `fuzz_faults` run gets.
    ///
    /// The specification, over the committed entries as `(index, message)` facts per member:
    ///
    /// - `PRE`: member 0 committed `a`. Member 0 is the leader of term 2 in the scenario; its
    ///   commit is observed while it lives, so a crash after the commit keeps `PRE` (and the
    ///   crash right after each send in `PRE`'s lineage is a preserve point).
    /// - `POST`: agreement at every index among the entries that count, the correct members'
    ///   and member 0's own: the distinct `(index, message)` pairs are as many as the distinct
    ///   indices. Stated as a count comparison so every committed entry contributes to the
    ///   view's lineage (the first version named each as a witness). Agreement is a property
    ///   about the *absence* of a conflict, so it is not monotone in the facts: a hypothesis
    ///   breaks it by making a member commit something *else*, which in this protocol also
    ///   removes that member's recorded commit of `a` (the lost `AppendEntries(a)`), which is
    ///   why the search still proposes it; see `hydrofrac-negation.md` §4.
    fn compile_with(
        bug: bool,
        fuzz_iterations: usize,
    ) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Replica>();
        let hooks: RaftHooks<String, Replica> = flow.sim_hook();

        let (election, election_timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (heartbeat, heartbeat_timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (requests, request_stream) = cluster.sim_input::<String, TotalOrder, ExactlyOnce>();
        let outputs = raft_server(
            &cluster,
            request_stream,
            election_timer,
            heartbeat_timer,
            RaftConfig {
                cluster_size: MEMBERS as usize,
                election_restriction_ignores_term: bug,
            },
            TCP.lossy_delayed_forever().name("rpc").bincode(),
            nondet!(
                /// Every batch is bound to a hook under a fault-injection policy; the
                /// search owns the non-determinism.
                hook = hooks
            ),
        );
        // The specification.
        let mut spec = Spec::new(&mut flow);
        let committed = spec.observe_cluster(
            outputs
                .committed
                .end_atomic()
                .weaken_consistency()
                .map(q!(|entry| (entry.index, entry.message))),
        );
        let correct = spec.correct_members(&cluster, 0..MEMBERS);
        let leader_committed_a = committed
            .clone()
            .filter(q!(|(member, (_, message))| {
                *member == MemberId::<Replica>::from_raw_id(0) && message == "a"
            }))
            .first();
        spec.pre(leader_committed_a);
        // The entries that count: the correct members' (while the run lasts) and member 0's
        // own while it lived.
        let by_correct = committed
            .clone()
            .join(correct.map(q!(|member| (member, ()))))
            .map(q!(|(_, (entry, ()))| entry));
        let by_leader = committed
            .filter(q!(
                |(member, _)| *member == MemberId::<Replica>::from_raw_id(0)
            ))
            .map(q!(|(_, entry)| entry));
        let counted = by_leader.chain(by_correct);
        let pairs = counted.clone().unique().count();
        let indices = counted.map(q!(|(index, _)| index)).unique().count();
        spec.post(
            pairs
                .zip(indices)
                .filter(q!(|(pairs, indices)| pairs == indices)),
        );

        let cluster_id = cluster.clone();
        let mut sim = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, MEMBERS as usize)
            .unit_test_fuzz_iterations(fuzz_iterations);
        if std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();
        let harness = Harness {
            election,
            heartbeat,
            requests,
        };
        let configure = move |crashes: usize| {
            // The step closure is opaque to the recorded lineage levels; record provenance
            // with replay at the opaque step is this harness's default (`LDFI_LEVEL`
            // overrides).
            Ldfi::new()
                .level(Level::Record)
                .level_from_env()
                .crashable_cluster("raft", &cluster_id, 0..MEMBERS)
                .crash_budget(crashes)
                .spec(&spec)
        };
        (compiled, harness, configure)
    }

    /// One run (see the module docs). Member 1 wins term 1; request `b` at 1 and member 0's
    /// election interrupt land in the same phase, so the schedule decides whether 0 wins
    /// term 2. Then request `a` at 0, two heartbeat rounds, the end of finite failures;
    /// member 1's interrupt twice (the first fire is suppressed if it saw a heartbeat),
    /// request `y` at 1, two of its heartbeat rounds. The verdict is the specification's.
    async fn body(h: &Harness, run: &Run<'_>) {
        h.election.send(1, ());
        hydro_lang::sim::quiesce().await;
        h.requests.send(1, "b".to_owned());
        h.election.send(0, ());
        hydro_lang::sim::quiesce().await;
        h.requests.send(0, "a".to_owned());
        hydro_lang::sim::quiesce().await;
        for _ in 0..2 {
            h.heartbeat.send(0, ());
            hydro_lang::sim::quiesce().await;
        }
        run.end_of_finite_failures();
        for _ in 0..2 {
            h.election.send(1, ());
            hydro_lang::sim::quiesce().await;
        }
        h.requests.send(1, "y".to_owned());
        hydro_lang::sim::quiesce().await;
        for _ in 0..2 {
            h.heartbeat.send(1, ());
            hydro_lang::sim::quiesce().await;
        }
    }

    fn search(bug: bool, crashes: usize, strategy: Strategy) -> Outcome {
        let (compiled, h, configure) = compile(bug);
        configure(crashes)
            .strategy(strategy)
            .exploration(Exploration::Fuzz {
                instances: SCHEDULES,
            })
            .search(&compiled, async |run| body(&h, run).await)
    }

    /// RAFT keeps agreement across a leader change under omissions and one crash: a
    /// certificate (relative to the sampled schedules).
    #[test]
    fn ldfi_certifies_raft_agreement_under_one_crash() {
        let outcome = search(false, 1, Strategy::Lineage);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// With the election restriction comparing indexes only, a lost replication (under a
    /// schedule in which member 0 took over term 2) lets the stale member 1 win and overwrite
    /// a committed entry.
    #[test]
    fn ldfi_finds_the_election_restriction_bug() {
        let outcome = search(true, 1, Strategy::Lineage);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert!(
            faults.iter().any(|f| matches!(f, Fault::Omit(_))),
            "the counterexample needs a lost message: {faults:?}"
        );
    }

    /// Lineage, blind enumeration and random fault injection on the injected bug (and
    /// lineage vs blind on correct RAFT), for the record. Random draws from the vocabulary
    /// lineage observed in its failure-free run (every message on the lossy channel, every
    /// crash point) and gets as many instances as lineage spent; `bern` is the Bernoulli
    /// draw, `k2` up to two uniform faults.
    #[test]
    #[ignore = "measurement; run with --ignored --nocapture"]
    fn ldfi_raft_compare() {
        use hydro_lang::sim::ldfi::{FuzzConfig, FuzzDraw, FuzzOutcome};

        const REPEATS: usize = 5;
        for bug in [true, false] {
            let ldfi = search(bug, 1, Strategy::Lineage);
            let blind = search(bug, 1, Strategy::Blind);
            eprintln!(
                "raft n=3 cr=1 bug={bug} | lineage {} in {} hypotheses ({} instances) | blind {} in {} hypotheses ({} instances)",
                verdict(&ldfi),
                ldfi.runs().len(),
                ldfi.schedules(),
                verdict(&blind),
                blind.runs().len(),
                blind.schedules(),
            );
            if !bug {
                continue;
            }
            let omittable = ldfi.runs()[0].omittable();
            let crash_points = ldfi.runs()[0].crash_points();
            let budget = ldfi.schedules().max(blind.schedules()).max(1);
            for (name, draw) in [("bern", FuzzDraw::Bernoulli), ("k2", FuzzDraw::UpTo(2))] {
                let mut found: Vec<usize> = vec![];
                let mut misses = 0;
                let mut vacuous_total = 0;
                for _ in 0..REPEATS {
                    let (compiled, h, configure) = compile_with(true, budget);
                    let FuzzOutcome {
                        runs,
                        violation,
                        vacuous,
                    } = configure(1).quiet().fuzz_faults_with(
                        &compiled,
                        FuzzConfig {
                            omittable: &omittable,
                            crash_points: &crash_points,
                            draw,
                        },
                        async |run| {
                            body(&h, run).await;
                            true
                        },
                    );
                    vacuous_total += vacuous;
                    match violation {
                        Some(_) => found.push(runs),
                        None => misses += 1,
                    }
                }
                found.sort_unstable();
                eprintln!(
                    "raft n=3 cr=1 bug=true | random {name}: {} of {REPEATS} found within {budget} instances; instances to bug {:?}; vacuous {vacuous_total}",
                    found.len(),
                    found
                );
                let _ = misses;
            }
        }
    }

    fn verdict(o: &Outcome) -> &'static str {
        match o {
            Outcome::Counterexample { .. } => "violated",
            Outcome::Certificate { .. } => "certificate",
            Outcome::Exhausted { .. } => "exhausted",
            Outcome::Budget { .. } => "budget",
        }
    }
}
