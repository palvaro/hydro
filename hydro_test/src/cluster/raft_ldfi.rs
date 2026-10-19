//! Lineage-driven fault injection on RAFT ([`super::raft`]), the closure-encoded protocol
//! the static lineage levels cannot see into: one `raft_step` closure per tick over a
//! `RaftServerState`, reading its inputs and writing its outputs through `by_ref` /
//! `by_mut` handles. Under the provenance rewrite the step's outputs are *opaque*
//! (`Prov::OPAQUE`), and their lineage is derived by counterfactual replay of the member.
//!
//! The specification is **durability of a committed entry** (RAFT's Leader Completeness, as
//! an end-of-run fact), as views over the members' committed entries
//! (`hydro_lang::sim::spec`): `PRE`, some member committed some entry (the first commit the
//! observer sees, as `(index, request)`); `POST`, every correct member committed that same
//! `(index, request)` by the end of the run. This is a universal over `correct_members`, a domain the hypothesis fixes, so the
//! view is monotone in the observed facts and a certificate is one within the fault model
//! (the search prints its grade). It is the property lineage-driven search is built for: the
//! derivation of "member *m* has the entry" names the `AppendEntries` whose loss is the
//! threat. Agreement proper (no index holds two entries) is the *absence* of a conflict and
//! is left to the simulator's own tests and to Maelstrom.
//!
//! The run is a **generic schedule that encodes nothing about any bug**: `ROUNDS` rounds in
//! which an election timer fires at one member (rotating), a client request lands at that
//! same member (a request at a non-leader is redirected, i.e. dropped by this harness, so a
//! client that follows the latest campaigner is the simplest one whose requests can commit
//! at all), and every member's heartbeat timer fires; then the end of finite failures and a
//! **heal phase**, `HEAL` cycles of, at every member in turn, its election timer, a client
//! request at every member, and everyone's heartbeat timer. An election interrupt is
//! suppressed when an `AppendEntries` was seen since the previous one, so a live leader is
//! undisturbed and a leaderless cluster elects one within two fires per member; the request
//! is what lets a new leader advance its commit index at all, since this RAFT appends no
//! no-op entry on election and §5.4.2 forbids a leader to commit a prior-term entry except
//! under one of its own term. `POST` then judges the state the protocol converges to with no
//! faults, not a message in flight or a cluster never given a timer or a request. Faults of
//! every kind, crashes included, lie before the end of finite failures (see
//! `hydro_lang::sim::ldfi`): omissions on any intra-cluster message until then, crashes at
//! any send until then; the heal phase is the convergence window.
//!
//! Four earlier versions of this generic body were shorter and "violated" durability on
//! *correct* RAFT: a leaderless cluster (no election timer after the failures); a leader
//! holding the entry but unable to commit it (no request of its own term); a member whose
//! single post-EFF timer fire was spent on the suppression a late pre-EFF heartbeat had
//! re-armed; and a follower still catching up (this RAFT backed `next_index` off one entry
//! per rejected heartbeat; it now carries §5.3's conflict hint in the rejection, a protocol
//! improvement made for this harness, since a heal phase long enough for the linear
//! back-off made every counterfactual replay a second long and a hypothesis an hour). All
//! four are liveness artefacts of the harness, not of the protocol, and the heal phase is
//! what it is because of them: a harness too short for the fault-free protocol to converge
//! cannot judge it. The body was shaped by the *correct* protocol's needs only; whether the
//! injected bug ([`RaftConfig::election_restriction_ignores_term`], which compares last-log
//! indexes only, ignoring terms) is reachable under it is for the search to find out. The
//! earlier version of this harness staged the race and the stale node's re-election by hand,
//! and its results (25 then 17 hypotheses) measured a search *within* a scenario built by
//! someone who knew the bug, not a search for it.
//!
//! A note on what this harness is and is not (`docs/design/hydrofrac.md`, "Hand-written specifications"). The injected
//! bug is a *safety* bug, a violation of agreement; that is outside the class of properties
//! lineage-driven search is sound for (fault-tolerant convergence of derived facts). The search
//! reaches it because the bug's symptom includes a convergence failure, the stale leader never
//! receiving the committed entry. The result is a curiosity about fault exploration, not an
//! instance of the guarantee. Two further things the fifth round learned here: `PRE` should be
//! a convergence witness (a settled leader every correct member acknowledges), so that a run
//! too short to converge is vacuous rather than a violation, which would have made the four
//! body revisions above unnecessary; this harness still has the weaker `PRE` and the tuned
//! body, and the rewrite is in the roadmap.
//!
//! Each hypothesis is run under several simulator schedules (`Exploration::Fuzz`).

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::SimClusterSender;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Fault, Ldfi, Level, Outcome, Run, Strategy};
    use hydro_lang::sim::spec::Spec;

    use crate::cluster::raft::{RaftConfig, RaftHooks, Replica, raft_server};

    const MEMBERS: u32 = 3;
    /// Rounds of the generic schedule (see [`body`]).
    const ROUNDS: u32 = 3;
    /// Cycles of the heal phase after the end of finite failures (see [`body`]): two, so
    /// that every member's election timer fires twice with heartbeat rounds in between. A
    /// fire is suppressed when an `AppendEntries` was seen since the previous fire, and the
    /// suppression is consumed by it; a member whose last pre-EFF heartbeat arrived after
    /// its last fire spends its first heal fire on that stale suppression and campaigns on
    /// the second, unless a live leader's heartbeats in between re-arm it, in which case
    /// there already is a leader. (One cycle "violated" durability on correct RAFT this way.)
    const HEAL: u32 = 2;
    /// Heartbeat rounds after each heal-phase request: one for the leader to learn where a
    /// follower's log diverges (the reply's `conflict_hint`), one to ship the missing suffix
    /// and collect the acknowledgements that advance the commit index, one to carry the new
    /// commit index to the followers.
    const HEAL_HEARTBEATS: u32 = 3;
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
    /// - `PRE`: some member committed some entry `(i, r)` (the first commit observed; a member
    ///   that later crashes still counts, its commit was observed while it lived, and the
    ///   crash right after each send in `PRE`'s lineage is a preserve point).
    /// - `POST`: every correct member committed `(i, r)`: the correct members holding that
    ///   entry are as many as the correct members. Monotone in the observed facts.
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
        // PRE: something was committed somewhere (the first commit observed).
        let first_committed = committed.clone().map(q!(|(_, entry)| entry)).first();
        spec.pre(first_committed.clone());
        // POST: every correct member holds that very entry.
        let holders = committed
            .cross_singleton(first_committed)
            .filter(q!(|((_, entry), chosen)| entry == chosen))
            .map(q!(|((member, _), _)| (member, ())))
            .unique();
        let correct_holders = correct
            .clone()
            .map(q!(|member| (member, ())))
            .join(holders)
            .count();
        spec.post(
            correct_holders
                .zip(correct.count())
                .filter(q!(|(holders, correct)| holders == correct)),
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
            // The step closure is opaque to the execution-level lineage; record provenance
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

    /// One run (see the module docs): a generic schedule with no knowledge of any bug.
    /// Round `k`: the election timer fires at member `k mod 3`; a request `r{k}` arrives at
    /// that member; every member's heartbeat timer fires (only a leader acts on it).
    /// Then the end of finite failures and the heal phase: `HEAL` cycles of, for each
    /// member, its election timer, a request `h{cycle}.{member}` at every member (whoever
    /// is leader appends it; a suppressed timer leaves the leader where it was), then
    /// everyone's heartbeat timer [`HEAL_HEARTBEATS`] times (one `next_index` back-off per
    /// round for a follower missing a prefix). The verdict is the specification's.
    async fn body(h: &Harness, run: &Run<'_>) {
        for round in 0..ROUNDS {
            h.election.send(round % MEMBERS, ());
            hydro_lang::sim::quiesce().await;
            h.requests.send(round % MEMBERS, format!("r{round}"));
            hydro_lang::sim::quiesce().await;
            h.heartbeat.send_many((0..MEMBERS).map(|m| (m, ())));
            hydro_lang::sim::quiesce().await;
        }
        run.end_of_finite_failures();
        for cycle in 0..HEAL {
            for member in 0..MEMBERS {
                h.election.send(member, ());
                hydro_lang::sim::quiesce().await;
                h.requests
                    .send_many((0..MEMBERS).map(|m| (m, format!("h{cycle}.{member}"))));
                hydro_lang::sim::quiesce().await;
                for _ in 0..HEAL_HEARTBEATS {
                    h.heartbeat.send_many((0..MEMBERS).map(|m| (m, ())));
                    hydro_lang::sim::quiesce().await;
                }
            }
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

    /// Correct RAFT keeps a committed entry at every correct member under omissions and one
    /// crash: a certificate (relative to the sampled schedules, and graded RELATIVE as every
    /// hand-written specification is; the generated specification is the one with a soundness
    /// claim).
    ///
    /// Measured: 235 hypotheses, 940 instances, about 68 minutes alongside the bug search
    /// (365k counterfactual replays between the two). Too long for the default suite.
    #[test]
    #[ignore = "about an hour; run with --ignored --nocapture"]
    fn ldfi_certifies_raft_durability_under_one_crash() {
        let outcome = search(false, 1, Strategy::Lineage);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// With the election restriction comparing indexes only, a stale member can win an
    /// election and overwrite a committed entry. Whether the generic schedule reaches that
    /// world, and with which lost messages, is what this test reports; it asserts only that a
    /// counterexample exists and involves an omission.
    ///
    /// Measured: a counterexample after 90 hypotheses (360 instances, about 30 minutes), four
    /// omissions and no crash: `AppendEntries(r0)` from 0 lost to both followers, so 0 keeps
    /// an unreplicated `r0` at index 1 of term 1; 1 wins term 2 with 2's vote, appends `r1` at
    /// index 1, replicates and commits it with 2, while its `AppendEntries(r1)` to 0 is lost
    /// twice (the entry and its heartbeat retransmission); in the heal phase 0 campaigns for
    /// term 3 with last entry `(1, term 1)`, and under the index-only check 1 and 2 grant,
    /// after which 0's heartbeats overwrite the committed `r1` at both. Correct RAFT denies
    /// the vote (term 1 < term 2). Not a scenario anyone wrote down.
    #[test]
    #[ignore = "about half an hour; run with --ignored --nocapture"]
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
            Outcome::Vacuous { .. } => "vacuous",
        }
    }
}
