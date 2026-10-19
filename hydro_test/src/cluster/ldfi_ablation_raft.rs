//! Lineage-level ablation of hydrofrac on RAFT ([`super::raft_ldfi`]) and, for reference, on
//! core Paxos ([`super::paxos_ldfi`]): the same search at each lineage level
//! (`Level::Execution`, `Level::Record`), on the same provenance-compiled program, recording
//! verdict, hypotheses, schedules, mean derivation size, replays and wall time.
//!
//! A measurement, not a test of the protocols: it does nothing unless `LDFI_ABLATION_OUT`
//! names the markdown file to write, and the configuration is a *shrunken* copy of the
//! `raft_ldfi` / `paxos_ldfi` harnesses, so that a sixteen-cell grid fits in minutes rather
//! than the hours the full searches take (see the knobs below). The harness code is copied
//! rather than shared because the originals live in private `mod tests` blocks.
//!
//! ```text
//! LDFI_ABLATION_OUT=target/ldfi-ablation-raft.md \
//!   cargo test -p hydro_test level_ablation_raft -- --test-threads=1 --nocapture
//! ```
//!
//! Knobs (all optional): `LDFI_ABLATION_ROUNDS` (RAFT rounds before the end of finite
//! failures; default 2, the original has 3), `LDFI_ABLATION_SCHEDULES` (schedules sampled per
//! hypothesis; default 1, the originals use 4), `LDFI_ABLATION_MAX_HYP` (hypothesis cap;
//! default 64 for RAFT and 256 for Paxos), `LDFI_ABLATION_LEVELS` (comma-separated subset of `exec,record`),
//! `LDFI_ABLATION_CELLS` (comma-separated subset of `raft-bug,raft-ok,paxos-bug,paxos-ok`),
//! `LDFI_ABLATION_VERBOSE` (keep the per-hypothesis log).

#[cfg(test)]
#[cfg(feature = "tokio")]
mod tests {
    use std::fmt::Write as _;
    use std::panic::AssertUnwindSafe;
    use std::time::Instant;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::SimClusterSender;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Ldfi, Level, Outcome, Run};
    use hydro_lang::sim::spec::Spec;

    use crate::cluster::paxos::{
        Acceptor, LeaderTiming, PaxosConfig, PaxosTestVariant, Proposer, paxos_core_with,
    };
    use crate::cluster::raft::{RaftConfig, RaftHooks, Replica, raft_server};

    // ------------------------------------------------------------------------------------
    // Knobs.
    // ------------------------------------------------------------------------------------

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn env_list(name: &str) -> Option<Vec<String>> {
        std::env::var(name)
            .ok()
            .map(|v| v.split(',').map(|s| s.trim().to_owned()).collect())
    }

    fn wants(list: Option<&Vec<String>>, name: &str) -> bool {
        list.is_none_or(|l| l.iter().any(|x| x == name))
    }

    const LEVELS: [(&str, Level); 2] = [("exec", Level::Execution), ("record", Level::Record)];

    // ------------------------------------------------------------------------------------
    // One measurement.
    // ------------------------------------------------------------------------------------

    struct Cell {
        protocol: &'static str,
        goal: String,
        config: String,
        level: &'static str,
        /// `None` when the search panicked (the message is in `error`).
        verdict: Option<&'static str>,
        error: Option<String>,
        hypotheses: usize,
        schedules: usize,
        /// Derivations of goal-held schedules, and the sum of their sizes.
        derivations: usize,
        dependencies: usize,
        replays: usize,
        fallbacks: usize,
        /// Failure-free runs redrawn because `PRE` did not hold in the sampled schedule.
        vacuous_redraws: usize,
        secs: f64,
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

    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_owned()
        } else {
            "panic (non-string payload)".to_owned()
        }
    }

    /// Runs `configure()` at `level` against `sim` and `body`, catching a panic so that one
    /// failing cell does not kill the grid.
    #[expect(clippy::too_many_arguments, reason = "a measurement row's coordinates")]
    fn measure(
        protocol: &'static str,
        goal: &str,
        config: &str,
        level: (&'static str, Level),
        max_hyp: usize,
        sim: &CompiledSim,
        configure: &dyn Fn() -> Ldfi,
        body: impl AsyncFn(&Run<'_>) + std::panic::RefUnwindSafe,
    ) -> Cell {
        let schedules = env_usize("LDFI_ABLATION_SCHEDULES", 1);
        let verbose = std::env::var("LDFI_ABLATION_VERBOSE").is_ok();
        let mut cell = Cell {
            protocol,
            goal: goal.to_owned(),
            config: config.to_owned(),
            level: level.0,
            verdict: None,
            error: None,
            hypotheses: 0,
            schedules: 0,
            derivations: 0,
            dependencies: 0,
            replays: 0,
            fallbacks: 0,
            vacuous_redraws: 0,
            secs: 0.0,
        };
        eprintln!(
            "ablation: {protocol} {goal} [{config}] level={} ...",
            level.0
        );
        let start = Instant::now();
        // The failure-free run samples one schedule; when `PRE` does not hold in it (Paxos:
        // the first leader did not get `x` chosen before the body moved on) the search is
        // vacuous, which says nothing about the level. Redraw a few times.
        const VACUOUS_RETRIES: usize = 4;
        let mut result = None;
        for attempt in 0..=VACUOUS_RETRIES {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let mut ldfi = configure()
                    .level(level.1)
                    .exploration(Exploration::Fuzz {
                        instances: schedules,
                    })
                    .max_hypotheses(max_hyp);
                if !verbose {
                    ldfi = ldfi.quiet();
                }
                ldfi.search(sim, &body)
            }));
            if matches!(outcome, Ok(Outcome::Vacuous { .. })) && attempt < VACUOUS_RETRIES {
                cell.vacuous_redraws += 1;
                continue;
            }
            result = Some(outcome);
            break;
        }
        let result = result.expect("at least one attempt");
        cell.secs = start.elapsed().as_secs_f64();
        match result {
            Ok(outcome) => {
                cell.verdict = Some(verdict(&outcome));
                cell.hypotheses = outcome.runs().len();
                cell.schedules = outcome.schedules();
                for schedule in outcome.runs().iter().flat_map(|h| &h.schedules) {
                    cell.replays += schedule.replays;
                    cell.fallbacks += schedule.replay_fallbacks;
                    if schedule.goal_held {
                        for derivation in &schedule.derivations {
                            cell.derivations += 1;
                            cell.dependencies += derivation.len();
                        }
                    }
                }
                if let Outcome::Counterexample { faults, .. } = &outcome {
                    eprintln!("ablation:   counterexample {faults:?}");
                }
            }
            Err(payload) => {
                let message = panic_message(payload);
                eprintln!("ablation:   PANIC {message}");
                cell.error = Some(message.lines().next().unwrap_or("").to_owned());
            }
        }
        eprintln!(
            "ablation:   {} hyp={} sched={} mean-deriv={} replays={} fallbacks={} redraws={} {:.1}s",
            cell.verdict.unwrap_or("n/a"),
            cell.hypotheses,
            cell.schedules,
            mean_deriv(&cell),
            cell.replays,
            cell.fallbacks,
            cell.vacuous_redraws,
            cell.secs
        );
        cell
    }

    fn mean_deriv(cell: &Cell) -> String {
        if cell.derivations == 0 {
            "-".to_owned()
        } else {
            format!("{:.1}", cell.dependencies as f64 / cell.derivations as f64)
        }
    }

    fn render(cells: &[Cell], header: &str) -> String {
        let mut out = String::new();
        writeln!(out, "{header}").unwrap();
        writeln!(out).unwrap();
        writeln!(
            out,
            "Cells: `verdict / hypotheses / schedules / mean derivation size / seconds`; \
             `n/a: ...` when the search panicked. Replays and fallbacks below."
        )
        .unwrap();
        writeln!(out).unwrap();
        writeln!(
            out,
            "| protocol | goal | config | {} |",
            LEVELS
                .iter()
                .map(|(n, _)| (*n).to_owned())
                .collect::<Vec<_>>()
                .join(" | ")
        )
        .unwrap();
        writeln!(out, "|---|---|---|{}", "---|".repeat(LEVELS.len())).unwrap();
        let mut keys: Vec<(&str, &str, &str)> = vec![];
        for cell in cells {
            let key = (cell.protocol, cell.goal.as_str(), cell.config.as_str());
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        for (protocol, goal, config) in &keys {
            write!(out, "| {protocol} | {goal} | {config} |").unwrap();
            for (name, _) in LEVELS {
                let cell = cells.iter().find(|c| {
                    c.protocol == *protocol
                        && c.goal == *goal
                        && c.config == *config
                        && c.level == name
                });
                match cell {
                    None => write!(out, " (not run) |").unwrap(),
                    Some(c) => match (&c.verdict, &c.error) {
                        (Some(v), _) => write!(
                            out,
                            " {v} / {} / {} / {} / {:.0}s |",
                            c.hypotheses,
                            c.schedules,
                            mean_deriv(c),
                            c.secs
                        )
                        .unwrap(),
                        (None, Some(e)) => write!(out, " n/a: {} ({:.0}s) |", e, c.secs).unwrap(),
                        (None, None) => write!(out, " n/a |").unwrap(),
                    },
                }
            }
            writeln!(out).unwrap();
        }
        writeln!(out).unwrap();
        writeln!(
            out,
            "Replays / replay fallbacks / vacuous redraws of the failure-free run per cell:"
        )
        .unwrap();
        writeln!(out).unwrap();
        writeln!(
            out,
            "| protocol | goal | config | {} |",
            LEVELS
                .iter()
                .map(|(n, _)| (*n).to_owned())
                .collect::<Vec<_>>()
                .join(" | ")
        )
        .unwrap();
        writeln!(out, "|---|---|---|{}", "---|".repeat(LEVELS.len())).unwrap();
        for (protocol, goal, config) in &keys {
            write!(out, "| {protocol} | {goal} | {config} |").unwrap();
            for (name, _) in LEVELS {
                let cell = cells.iter().find(|c| {
                    c.protocol == *protocol
                        && c.goal == *goal
                        && c.config == *config
                        && c.level == name
                });
                match cell {
                    Some(c) if c.verdict.is_some() => write!(
                        out,
                        " {} / {} / {} |",
                        c.replays, c.fallbacks, c.vacuous_redraws
                    )
                    .unwrap(),
                    _ => write!(out, " - |").unwrap(),
                }
            }
            writeln!(out).unwrap();
        }
        out
    }

    fn write_out(path: &str, cells: &[Cell], header: &str) {
        std::fs::write(path, render(cells, header)).expect("LDFI_ABLATION_OUT not writable");
    }

    // ------------------------------------------------------------------------------------
    // RAFT (a copy of `raft_ldfi`'s harness, with the round count a knob).
    // ------------------------------------------------------------------------------------

    const MEMBERS: u32 = 3;
    const HEAL: u32 = 2;
    const HEAL_HEARTBEATS: u32 = 3;

    struct RaftHarness {
        election: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        heartbeat: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        requests: SimClusterSender<String, TotalOrder, ExactlyOnce>,
    }

    fn compile_raft(bug: bool) -> (CompiledSim, RaftHarness, impl Fn(usize) -> Ldfi) {
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
            nondet!(/** the search owns the non-determinism */ hook = hooks),
        );
        let mut spec = Spec::new(&mut flow);
        let committed = spec.observe_cluster(
            outputs
                .committed
                .end_atomic()
                .weaken_consistency()
                .map(q!(|entry| (entry.index, entry.message))),
        );
        let correct = spec.correct_members(&cluster, 0..MEMBERS);
        let first_committed = committed.clone().map(q!(|(_, entry)| entry)).first();
        spec.pre(first_committed.clone());
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
        let compiled = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, MEMBERS as usize)
            .unit_test_fuzz_iterations(1)
            .with_provenance()
            .compiled();
        let harness = RaftHarness {
            election,
            heartbeat,
            requests,
        };
        let configure = move |crashes: usize| {
            Ldfi::new()
                .crashable_cluster("raft", &cluster_id, 0..MEMBERS)
                .crash_budget(crashes)
                .spec(&spec)
        };
        (compiled, harness, configure)
    }

    async fn raft_body(h: &RaftHarness, rounds: u32, run: &Run<'_>) {
        for round in 0..rounds {
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

    // ------------------------------------------------------------------------------------
    // Core Paxos (a copy of `paxos_ldfi`'s core harness).
    // ------------------------------------------------------------------------------------

    const F: usize = 1;
    const PROPOSERS: u32 = 2;
    const ACCEPTORS: u32 = 2 * F as u32 + 1;

    struct PaxosHarness {
        heartbeat: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        check: SimClusterSender<(), TotalOrder, ExactlyOnce>,
        payloads: SimClusterSender<String, TotalOrder, ExactlyOnce>,
    }

    fn compile_paxos(bug: bool) -> (CompiledSim, PaxosHarness, impl Fn(usize) -> Ldfi) {
        let mut flow = FlowBuilder::new();
        let proposers = flow.cluster::<Proposer>();
        let acceptors = flow.cluster::<Acceptor>();

        let (heartbeat, heartbeat_stream) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (check, check_stream) = proposers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (payloads, payload_stream) = proposers.sim_input::<String, TotalOrder, ExactlyOnce>();
        let a_checkpoint = acceptors
            .source_iter(q!(Vec::<usize>::new()))
            .weaken_boundedness::<Unbounded>()
            .first()
            .into();

        let (_ballots, chosen) = paxos_core_with(
            &proposers,
            &acceptors,
            a_checkpoint,
            |_new_leader| payload_stream,
            PaxosConfig {
                f: F,
                i_am_leader_send_timeout: 1,
                i_am_leader_check_timeout: 1,
                i_am_leader_check_timeout_delay_multiplier: 1,
            },
            LeaderTiming::Driven {
                heartbeat: heartbeat_stream,
                check: check_stream,
            },
            PaxosTestVariant {
                commit_quorum_off_by_one: bug,
                lossy_acceptor_links: true,
            },
            nondet!(/** the simulator's schedule explorer owns the non-determinism */),
            nondet!(/** as above */),
        );

        let mut spec = Spec::new(&mut flow);
        let chosen = spec.observe_cluster(chosen);
        let leader_chose_x = chosen
            .clone()
            .filter(q!(|(member, (_, value))| {
                *member == MemberId::<Proposer>::from_raw_id(0) && value.as_deref() == Some("x")
            }))
            .first();
        spec.pre(leader_chose_x);
        let pairs_by_slot = chosen.map(q!(|(_, pair)| pair));
        let pairs = pairs_by_slot.clone().unique().count();
        let slots = pairs_by_slot.map(q!(|(slot, _)| slot)).unique().count();
        spec.post(pairs.zip(slots).filter(q!(|(pairs, slots)| pairs == slots)));

        let proposers_id = proposers.clone();
        let acceptors_id = acceptors.clone();
        let compiled = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&proposers, PROPOSERS as usize)
            .with_cluster_size(&acceptors, ACCEPTORS as usize)
            .unit_test_fuzz_iterations(1)
            .with_provenance()
            .compiled();
        let harness = PaxosHarness {
            heartbeat,
            check,
            payloads,
        };
        let configure = move |crashes: usize| {
            Ldfi::new()
                .crashable_cluster("proposer", &proposers_id, 0..PROPOSERS)
                .crashable_cluster("acceptor", &acceptors_id, 0..ACCEPTORS)
                .crash_budget(crashes)
                .spec(&spec)
        };
        (compiled, harness, configure)
    }

    async fn paxos_body(h: &PaxosHarness, run: &Run<'_>) {
        h.check.send(0, ());
        hydro_lang::sim::quiesce().await;
        h.payloads.send(0, "x".to_owned());
        hydro_lang::sim::quiesce().await;
        h.check.send(1, ());
        hydro_lang::sim::quiesce().await;
        run.end_of_finite_failures();
        h.check.send(1, ());
        hydro_lang::sim::quiesce().await;
        h.payloads.send(1, "y".to_owned());
        hydro_lang::sim::quiesce().await;
        h.heartbeat.send(1, ());
        hydro_lang::sim::quiesce().await;
    }

    // ------------------------------------------------------------------------------------
    // The grid.
    // ------------------------------------------------------------------------------------

    /// RAFT durability (bug and correct) at both lineage levels; then core Paxos
    /// agreement (bug and correct) at both levels. Writes `LDFI_ABLATION_OUT`.
    #[test]
    fn level_ablation_raft() {
        let Ok(out) = std::env::var("LDFI_ABLATION_OUT") else {
            return;
        };
        let rounds = env_usize("LDFI_ABLATION_ROUNDS", 2) as u32;
        let schedules = env_usize("LDFI_ABLATION_SCHEDULES", 1);
        // RAFT costs about five seconds per hypothesis under replay; Paxos a fraction of one.
        let raft_max_hyp = env_usize("LDFI_ABLATION_MAX_HYP", 64);
        let paxos_max_hyp = env_usize("LDFI_ABLATION_MAX_HYP", 256);
        let levels = env_list("LDFI_ABLATION_LEVELS");
        let only = env_list("LDFI_ABLATION_CELLS");
        let header = format!(
            "<!-- generated by `level_ablation_raft` (hydro_test/src/cluster/ldfi_ablation_raft.rs): \
             rounds={rounds} schedules/hypothesis={schedules} max_hypotheses raft={raft_max_hyp} paxos={paxos_max_hyp} -->"
        );
        let mut cells: Vec<Cell> = vec![];

        for (name, bug) in [("raft-bug", true), ("raft-ok", false)] {
            if !wants(only.as_ref(), name) {
                continue;
            }
            let (sim, h, configure) = compile_raft(bug);
            let config = format!(
                "n={MEMBERS} cr=1 rounds={rounds} heal={HEAL} sched={schedules} max={raft_max_hyp} {}",
                if bug {
                    "bug=election-restriction-ignores-term"
                } else {
                    "correct"
                }
            );
            for level in LEVELS {
                if !wants(levels.as_ref(), level.0) {
                    continue;
                }
                cells.push(measure(
                    "raft",
                    "durability of a committed entry",
                    &config,
                    level,
                    raft_max_hyp,
                    &sim,
                    &|| configure(1),
                    async |run: &Run<'_>| raft_body(&h, rounds, run).await,
                ));
                write_out(&out, &cells, &header);
            }
        }

        for (name, bug) in [("paxos-bug", true), ("paxos-ok", false)] {
            if !wants(only.as_ref(), name) {
                continue;
            }
            let (sim, h, configure) = compile_paxos(bug);
            let config = format!(
                "p={PROPOSERS} a={ACCEPTORS} cr=1 sched={schedules} max={paxos_max_hyp} {}",
                if bug {
                    "bug=commit-quorum-off-by-one"
                } else {
                    "correct"
                }
            );
            for level in LEVELS {
                if !wants(levels.as_ref(), level.0) {
                    continue;
                }
                cells.push(measure(
                    "paxos",
                    "agreement across a leader change",
                    &config,
                    level,
                    paxos_max_hyp,
                    &sim,
                    &|| configure(1),
                    async |run: &Run<'_>| paxos_body(&h, run).await,
                ));
                write_out(&out, &cells, &header);
            }
        }
        eprintln!("{}", render(&cells, &header));
    }
}
