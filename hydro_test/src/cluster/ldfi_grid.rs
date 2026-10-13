//! The LDFI measurement grid: lineage-guided search against blind enumeration and random
//! fault injection, across protocol sizes, with repeats.
//!
//! Run explicitly (it is `#[ignore]`d; it takes tens of minutes):
//!
//! ```text
//! LDFI_GRID_CSV=target/ldfi-grid.csv cargo test -p hydro_test --lib -- ldfi_grid --ignored --nocapture
//! ```
//!
//! Every searcher is charged in simulator instances. LDFI and blind run one schedule per
//! hypothesis (`ldfi/1`, `blind/1`); `ldfi/8` samples eight, to check that verdicts do not
//! depend on the schedule. Random draws a fault set per instance (`Bernoulli`, or up to `k`
//! faults uniformly from the vocabulary) and is repeated several times (the engine draws its
//! entropy from the OS; repeats are independent, not seeded), so its column reports the
//! median, the range, and how many repeats missed within the budget.

#[cfg(test)]
mod grid {
    use std::fmt::Write as _;
    use std::io::Write as _;

    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{
        Exploration, FuzzConfig, FuzzDraw, FuzzOutcome, Ldfi, Level, Outcome, Run, Strategy,
    };

    use crate::cluster::isr_replication::Encoding;
    use crate::cluster::isr_replication::harness as isr;
    use crate::cluster::two_pc_ldfi::harness as two_pc;

    /// One measurement.
    struct Row {
        protocol: String,
        config: String,
        searcher: String,
        repeat: Option<u64>,
        verdict: &'static str,
        hypotheses: usize,
        instances: usize,
        /// Random rows: instances whose pre-condition failed. Lineage-level rows:
        /// counterfactual replays run (the extra cost of `L3/replay`, and of `L2/record` on
        /// a closure-encoded program).
        vacuous: usize,
    }

    struct Sink {
        csv: Option<std::fs::File>,
        rows: Vec<Row>,
    }

    // Free functions rather than an `impl`: the staging copy of this crate drops impl blocks
    // in test modules.
    fn new_sink() -> Sink {
        let csv = std::env::var("LDFI_GRID_CSV").ok().map(|path| {
            let mut file = std::fs::File::create(&path).expect("LDFI_GRID_CSV not writable");
            writeln!(
                file,
                "protocol,config,searcher,repeat,verdict,hypotheses,instances,vacuous_or_replays"
            )
            .unwrap();
            file
        });
        Sink { csv, rows: vec![] }
    }

    fn push_row(sink: &mut Sink, row: Row) {
        let line = format!(
            "{},{},{},{},{},{},{},{}",
            row.protocol,
            row.config,
            row.searcher,
            row.repeat.map(|s| s.to_string()).unwrap_or_default(),
            row.verdict,
            row.hypotheses,
            row.instances,
            row.vacuous
        );
        eprintln!("row: {line}");
        if let Some(csv) = &mut sink.csv {
            writeln!(csv, "{line}").unwrap();
        }
        sink.rows.push(row);
    }

    fn verdict(o: &Outcome) -> &'static str {
        match o {
            Outcome::Counterexample { .. } => "violated",
            Outcome::Certificate { .. } => "certificate",
            Outcome::Exhausted { .. } => "exhausted",
            Outcome::Budget { .. } => "budget",
        }
    }

    const SEEDS_2PC: u64 = 10;
    const BUDGET_2PC: usize = 2000;
    const SEEDS_ISR: u64 = 5;
    const BUDGET_ISR: usize = 1000;
    const MAX_HYPOTHESES: usize = 400;
    const LDFI_REPEATS: u64 = 5;

    /// Runs every searcher on one compiled configuration.
    #[expect(clippy::too_many_arguments, reason = "a measurement cell")]
    fn measure(
        sink: &mut Sink,
        protocol: &str,
        config: &str,
        sim: &CompiledSim,
        configure: &dyn Fn() -> Ldfi,
        body: impl AsyncFn(&Run<'_>) -> bool + std::panic::RefUnwindSafe,
        repeats: u64,
        draws: &[FuzzDraw],
    ) {
        let run_body = &body;

        // LDFI, one schedule per hypothesis. The schedule drawn decides which derivations
        // are observed and so the search path; repeated for its spread.
        let mut ldfi1 = None;
        for repeat in 0..LDFI_REPEATS {
            let outcome = configure()
                .exploration(Exploration::Fuzz { instances: 1 })
                .max_hypotheses(MAX_HYPOTHESES)
                .quiet()
                .run(sim, run_body);
            push_row(
                sink,
                Row {
                    protocol: protocol.into(),
                    config: config.into(),
                    searcher: "ldfi/1".into(),
                    repeat: Some(repeat),
                    verdict: verdict(&outcome),
                    hypotheses: outcome.runs().len(),
                    instances: outcome.schedules(),
                    vacuous: 0,
                },
            );
            ldfi1.get_or_insert(outcome);
        }
        let ldfi1 = ldfi1.unwrap();
        // LDFI, eight schedules per hypothesis: do the verdicts depend on the schedule?
        let ldfi8 = configure()
            .exploration(Exploration::Fuzz { instances: 8 })
            .max_hypotheses(MAX_HYPOTHESES)
            .quiet()
            .run(sim, run_body);
        push_row(
            sink,
            Row {
                protocol: protocol.into(),
                config: config.into(),
                searcher: "ldfi/8".into(),
                repeat: None,
                verdict: verdict(&ldfi8),
                hypotheses: ldfi8.runs().len(),
                instances: ldfi8.schedules(),
                vacuous: 0,
            },
        );
        if ldfi1.is_counterexample() != ldfi8.is_counterexample() {
            eprintln!(
                "NOTE: {protocol} {config}: ldfi/1 and ldfi/8 disagree ({} vs {})",
                verdict(&ldfi1),
                verdict(&ldfi8)
            );
        }
        // Blind enumeration, one schedule per hypothesis.
        let blind = configure()
            .strategy(Strategy::Blind)
            .exploration(Exploration::Fuzz { instances: 1 })
            .max_hypotheses(MAX_HYPOTHESES)
            .quiet()
            .run(sim, run_body);
        push_row(
            sink,
            Row {
                protocol: protocol.into(),
                config: config.into(),
                searcher: "blind/1".into(),
                repeat: None,
                verdict: verdict(&blind),
                hypotheses: blind.runs().len(),
                instances: blind.schedules(),
                vacuous: 0,
            },
        );

        // Random, from the vocabulary LDFI observed in the failure-free run.
        let omittable = ldfi1.runs()[0].omittable();
        let crash_points = ldfi1.runs()[0].crash_points();
        let draws: &[FuzzDraw] = if std::env::var("LDFI_GRID_NO_RANDOM").is_ok() {
            &[]
        } else {
            draws
        };
        for draw in draws {
            let name = match draw {
                FuzzDraw::Bernoulli => "random/bern".to_owned(),
                FuzzDraw::UpTo(k) => format!("random/k{k}"),
            };
            for seed in 0..repeats {
                let FuzzOutcome {
                    runs,
                    violation,
                    vacuous,
                } = configure().quiet().fuzz_faults_with(
                    sim,
                    FuzzConfig {
                        omittable: &omittable,
                        crash_points: &crash_points,
                        draw: *draw,
                    },
                    run_body,
                );
                push_row(
                    sink,
                    Row {
                        protocol: protocol.into(),
                        config: config.into(),
                        searcher: name.clone(),
                        repeat: Some(seed),
                        verdict: if violation.is_some() {
                            "violated"
                        } else {
                            "none"
                        },
                        hypotheses: runs,
                        instances: runs,
                        vacuous,
                    },
                );
            }
        }
    }

    fn summarize(sink: &Sink) -> String {
        let mut out = String::new();
        let mut cells: Vec<(String, String)> = vec![];
        for row in &sink.rows {
            let cell = (row.protocol.clone(), row.config.clone());
            if !cells.contains(&cell) {
                cells.push(cell);
            }
        }
        writeln!(
            out,
            "\n{:<14} {:<26} | {:<24} | {:<24} | {:<24} | random: median [min..max] misses/repeats vacuous%",
            "protocol", "config", "ldfi/1", "ldfi/8", "blind/1"
        )
        .unwrap();
        for (protocol, config) in cells {
            let rows: Vec<&Row> = sink
                .rows
                .iter()
                .filter(|r| r.protocol == protocol && r.config == config)
                .collect();
            // verdict, then median [min..max] of instances over repeats.
            let single = |name: &str| -> String {
                let runs: Vec<&&Row> = rows.iter().filter(|r| r.searcher == name).collect();
                if runs.is_empty() {
                    return format!("{:>24}", "-");
                }
                let mut instances: Vec<usize> = runs.iter().map(|r| r.instances).collect();
                instances.sort_unstable();
                let verdicts: std::collections::BTreeSet<&str> =
                    runs.iter().map(|r| r.verdict).collect();
                let verdict = verdicts.into_iter().collect::<Vec<_>>().join("/");
                if instances.len() == 1 {
                    format!("{:>11} {:>12}", verdict, instances[0])
                } else {
                    format!(
                        "{:>11} {:>4} [{}..{}]",
                        verdict,
                        instances[instances.len() / 2],
                        instances[0],
                        instances[instances.len() - 1]
                    )
                }
            };
            let mut random = String::new();
            let mut names: Vec<&String> = rows
                .iter()
                .map(|r| &r.searcher)
                .filter(|s| s.starts_with("random/"))
                .collect();
            names.dedup();
            for name in names {
                let runs: Vec<&&Row> = rows.iter().filter(|r| r.searcher == *name).collect();
                let mut hits: Vec<usize> = runs
                    .iter()
                    .filter(|r| r.verdict == "violated")
                    .map(|r| r.instances)
                    .collect();
                hits.sort_unstable();
                let misses = runs.len() - hits.len();
                let total: usize = runs.iter().map(|r| r.instances).sum();
                let vacuous: usize = runs.iter().map(|r| r.vacuous).sum();
                let pct = (100 * vacuous).checked_div(total).unwrap_or(0);
                let dist = if hits.is_empty() {
                    "-".to_owned()
                } else {
                    format!(
                        "{} [{}..{}]",
                        hits[hits.len() / 2],
                        hits[0],
                        hits[hits.len() - 1]
                    )
                };
                write!(
                    random,
                    " {}: {} {}/{} {}%",
                    name.trim_start_matches("random/"),
                    dist,
                    misses,
                    runs.len(),
                    pct
                )
                .unwrap();
            }
            writeln!(
                out,
                "{:<14} {:<26} | {} | {} | {} |{}",
                protocol,
                config,
                single("ldfi/1"),
                single("ldfi/8"),
                single("blind/1"),
                random
            )
            .unwrap();
        }
        out
    }

    #[test]
    #[ignore = "measurement grid; minutes to hours. Run with --ignored --nocapture"]
    fn ldfi_grid() {
        let mut sink = new_sink();
        let only = std::env::var("LDFI_GRID_ONLY").ok();
        let wants = |name: &str| only.as_deref().is_none_or(|o| name.starts_with(o));

        // 2PC with cooperative termination: the two-crash bug, and the one-crash certificate.
        if wants("2pc") {
            let sizes: Vec<u32> = std::env::var("LDFI_GRID_N")
                .ok()
                .map(|v| v.split(',').map(|x| x.parse().unwrap()).collect())
                .unwrap_or_else(|| vec![2, 3, 4, 5, 6]);
            for n in sizes {
                let (sim, h, configure) = two_pc::compile(n, true, BUDGET_2PC);
                for crashes in [1usize, 2] {
                    measure(
                        &mut sink,
                        "2pc+ctp",
                        &format!("n={n} cr={crashes}"),
                        &sim,
                        &|| configure(crashes),
                        async |run: &Run<'_>| two_pc::body(&h, run).await,
                        SEEDS_2PC,
                        &[
                            FuzzDraw::Bernoulli,
                            FuzzDraw::UpTo(1),
                            FuzzDraw::UpTo(2),
                            FuzzDraw::UpTo(3),
                        ],
                    );
                }
            }
        }

        // ISR: the unclean-election bug (omission + crash) and the clean certificate.
        if wants("isr") {
            let shapes = [(3u32, 4u32), (5, 4), (3, 8), (5, 8), (7, 4)];
            for (encoding, unclean, shapes) in [
                (Encoding::Closure, true, &shapes[..1]),
                (Encoding::Dataflow, true, &shapes[..]),
                (Encoding::Dataflow, false, &shapes[..2]),
            ] {
                for &(members, rounds) in shapes {
                    let shape = isr::Shape {
                        members,
                        rounds,
                        writes: 1,
                    };
                    let (sim, h, configure) = isr::compile(unclean, encoding, shape, BUDGET_ISR);
                    measure(
                        &mut sink,
                        if unclean { "isr unclean" } else { "isr clean" },
                        &format!(
                            "{} m={members} r={rounds}",
                            match encoding {
                                Encoding::Closure => "closure",
                                Encoding::Dataflow => "dataflow",
                            }
                        ),
                        &sim,
                        &|| configure(1),
                        async |run: &Run<'_>| isr::body(&h, run).await,
                        SEEDS_ISR,
                        &[FuzzDraw::Bernoulli, FuzzDraw::UpTo(2)],
                    );
                }
            }
        }

        eprintln!("{}", summarize(&sink));
    }

    /// Lineage ablation: Level 0 (execution), Level 1 (static slice), Level 2 (record
    /// provenance) on the same provenance-compiled program, across a workload axis (extra
    /// transactions / writes as background load; the goal is checked on the first). Blind and
    /// random are run once per configuration for reference.
    fn ablate(
        sink: &mut Sink,
        protocol: &str,
        config: &str,
        sim: &CompiledSim,
        configure: &dyn Fn() -> Ldfi,
        body: impl AsyncFn(&Run<'_>) -> bool + std::panic::RefUnwindSafe,
        repeats: u64,
    ) {
        let run_body = &body;
        for (name, level) in [
            ("L0/exec", Level::Execution),
            ("L1/slice", Level::Slice),
            ("L2/record", Level::Record),
            ("L3/replay", Level::Counterfactual),
        ] {
            if name == "L3/replay" && std::env::var("LDFI_GRID_REPLAY").is_err() {
                continue;
            }
            let repeats = if std::env::var("LDFI_GRID_VERBOSE").is_ok() {
                1
            } else {
                LDFI_REPEATS
            };
            for repeat in 0..repeats {
                let mut ldfi = configure()
                    .level(level)
                    .exploration(Exploration::Fuzz { instances: 1 })
                    .max_hypotheses(MAX_HYPOTHESES);
                if std::env::var("LDFI_GRID_VERBOSE").is_err() {
                    ldfi = ldfi.quiet();
                }
                let outcome = ldfi.run(sim, run_body);
                if std::env::var("LDFI_GRID_VERBOSE").is_ok() {
                    eprintln!("{outcome}");
                }
                let replays: usize = outcome
                    .runs()
                    .iter()
                    .flat_map(|h| &h.schedules)
                    .map(|s| s.replays)
                    .sum();
                push_row(
                    sink,
                    Row {
                        protocol: protocol.into(),
                        config: config.into(),
                        searcher: name.into(),
                        repeat: Some(repeat),
                        verdict: verdict(&outcome),
                        hypotheses: outcome.runs().len(),
                        instances: outcome.schedules(),
                        vacuous: replays,
                    },
                );
            }
        }
        let blind = configure()
            .strategy(Strategy::Blind)
            .exploration(Exploration::Fuzz { instances: 1 })
            .max_hypotheses(MAX_HYPOTHESES)
            .quiet()
            .run(sim, run_body);
        push_row(
            sink,
            Row {
                protocol: protocol.into(),
                config: config.into(),
                searcher: "blind/1".into(),
                repeat: None,
                verdict: verdict(&blind),
                hypotheses: blind.runs().len(),
                instances: blind.schedules(),
                vacuous: 0,
            },
        );
        if std::env::var("LDFI_GRID_NO_RANDOM").is_err() {
            let omittable = blind.runs()[0].omittable();
            let crash_points = blind.runs()[0].crash_points();
            for repeat in 0..repeats {
                let FuzzOutcome {
                    runs,
                    violation,
                    vacuous,
                } = configure().quiet().fuzz_faults_with(
                    sim,
                    FuzzConfig {
                        omittable: &omittable,
                        crash_points: &crash_points,
                        draw: FuzzDraw::UpTo(2),
                    },
                    run_body,
                );
                push_row(
                    sink,
                    Row {
                        protocol: protocol.into(),
                        config: config.into(),
                        searcher: "random/k2".into(),
                        repeat: Some(repeat),
                        verdict: if violation.is_some() {
                            "violated"
                        } else {
                            "none"
                        },
                        hypotheses: runs,
                        instances: runs,
                        vacuous,
                    },
                );
            }
        }
    }

    fn summarize_levels(sink: &Sink) -> String {
        let mut out = String::new();
        let mut cells: Vec<(String, String)> = vec![];
        for row in &sink.rows {
            let cell = (row.protocol.clone(), row.config.clone());
            if !cells.contains(&cell) {
                cells.push(cell);
            }
        }
        writeln!(
            out,
            "\n{:<12} {:<22} | {:<26} | {:<26} | {:<26} | {:<26} | {:<14} | random k2",
            "protocol", "config", "L0/exec", "L1/slice", "L2/record", "L3/replay", "blind/1"
        )
        .unwrap();
        for (protocol, config) in cells {
            let rows: Vec<&Row> = sink
                .rows
                .iter()
                .filter(|r| r.protocol == protocol && r.config == config)
                .collect();
            let dist = |name: &str| -> String {
                let runs: Vec<&&Row> = rows.iter().filter(|r| r.searcher == name).collect();
                if runs.is_empty() {
                    return format!("{:>26}", "-");
                }
                let mut instances: Vec<usize> = runs.iter().map(|r| r.instances).collect();
                instances.sort_unstable();
                let verdicts: std::collections::BTreeSet<&str> =
                    runs.iter().map(|r| r.verdict).collect();
                let verdict = verdicts.into_iter().collect::<Vec<_>>().join("/");
                let misses = runs.iter().filter(|r| r.verdict == "none").count();
                let hits: Vec<usize> = runs
                    .iter()
                    .filter(|r| r.verdict != "none")
                    .map(|r| r.instances)
                    .collect();
                let shown = if hits.is_empty() { instances } else { hits };
                let mut shown = shown;
                shown.sort_unstable();
                if misses > 0 {
                    format!(
                        "{:>11} {:>4} [{}..{}] {}miss",
                        verdict,
                        shown[shown.len() / 2],
                        shown[0],
                        shown[shown.len() - 1],
                        misses
                    )
                } else {
                    format!(
                        "{:>11} {:>4} [{}..{}]",
                        verdict,
                        shown[shown.len() / 2],
                        shown[0],
                        shown[shown.len() - 1]
                    )
                }
            };
            writeln!(
                out,
                "{:<12} {:<22} | {} | {} | {} | {} | {} | {}",
                protocol,
                config,
                dist("L0/exec"),
                dist("L1/slice"),
                dist("L2/record"),
                dist("L3/replay"),
                dist("blind/1"),
                dist("random/k2"),
            )
            .unwrap();
        }
        out
    }

    #[test]
    #[ignore = "lineage ablation; minutes. Run with --ignored --nocapture"]
    fn ldfi_ablation() {
        let mut sink = new_sink();
        let only = std::env::var("LDFI_GRID_ONLY").ok();
        let wants = |name: &str| only.as_deref().is_none_or(|o| name.starts_with(o));
        let loads: Vec<u32> = std::env::var("LDFI_GRID_LOAD")
            .ok()
            .map(|v| v.split(',').map(|x| x.parse().unwrap()).collect())
            .unwrap_or_else(|| vec![1, 2, 4, 8]);

        if wants("2pc") {
            for &txns in &loads {
                let (sim, h, configure) = two_pc::compile_with(3, true, txns, true, BUDGET_2PC);
                for crashes in [1usize, 2] {
                    ablate(
                        &mut sink,
                        "2pc+ctp",
                        &format!("n=3 cr={crashes} load={txns}"),
                        &sim,
                        &|| configure(crashes),
                        async |run: &Run<'_>| two_pc::body(&h, run).await,
                        SEEDS_2PC,
                    );
                }
            }
        }
        if wants("isr") {
            for &writes in &loads {
                if writes > 4 {
                    continue;
                }
                let shape = isr::Shape {
                    members: 3,
                    rounds: 4,
                    writes,
                };
                for unclean in [true, false] {
                    // Both encodings of the same protocol. On the closure encoding the
                    // step is one opaque node, so `L2/record` is record provenance up to the
                    // step and counterfactual replay through it (`Prov::OPAQUE`).
                    for encoding in [Encoding::Dataflow, Encoding::Closure] {
                        let (sim, h, configure) =
                            isr::compile_with(unclean, encoding, shape, true, BUDGET_ISR);
                        ablate(
                            &mut sink,
                            if unclean { "isr unclean" } else { "isr clean" },
                            &format!("{encoding:?} m=3 r=4 load={writes}").to_lowercase(),
                            &sim,
                            &|| configure(1),
                            async |run: &Run<'_>| isr::body(&h, run).await,
                            SEEDS_ISR,
                        );
                    }
                }
            }
        }
        eprintln!("{}", summarize_levels(&sink));
    }
}
