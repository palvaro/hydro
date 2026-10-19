//! Lineage-level ablation on 2PC: the same search at every [`Level`] on the same
//! provenance-compiled program, over participants × workload × crash budget, with and without
//! cooperative termination. Writes a Markdown table; the results are in
//! `docs/design/ldfi-level-ablation.md`.
//!
//! Gated on `LDFI_ABLATION_OUT` (the path of the table to write); without it the test returns
//! at once. Knobs:
//!
//! - `LDFI_ABLATION_EXPLORATION`: `fuzz<k>` (k schedules per hypothesis; default `fuzz1`,
//!   one schedule per hypothesis as in the earlier ablation in `ldfi_grid.rs`) or
//!   `exhaustive` (every schedule; 864 per hypothesis at n=3 load=8, so minutes per level).
//! - `LDFI_ABLATION_MAX_HYPOTHESES`: the hypothesis budget (default 400).
//! - `LDFI_ABLATION_REPEATS`: searches per cell (default 1; under `fuzz<k>` the schedule drawn
//!   decides which derivations are observed, so repeats show the spread). Rows are then
//!   suffixed `#<repeat>`.
//! - `LDFI_ABLATION_N`, `LDFI_ABLATION_LOAD`, `LDFI_ABLATION_CRASHES`: comma-separated
//!   subsets of the grid axes.
//! - `LDFI_ABLATION_PROVENANCE=0`: compile **without** `with_provenance()`, so `Precise`
//!   derives every record by counterfactual replay.
//!
//! ```text
//! LDFI_ABLATION_OUT=target/ablation-2pc.md cargo test -p hydro_test level_ablation_two_pc -- --test-threads=1 --nocapture
//! ```
//!
//! The per-`n` shards (`level_ablation_two_pc_n2`, …) run the same grid for one `n` each and
//! write to `${LDFI_ABLATION_OUT}.n<N>`, for `--test-threads=3`.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use hydro_lang::sim::compiled::CompiledSim;
use hydro_lang::sim::ldfi::{Derivation, Exploration, Grade, Ldfi, Level, Outcome, Run};

use crate::cluster::two_pc_ldfi::harness as two_pc;

const LEVELS: [(&str, Level); 2] = [("exec", Level::Execution), ("precise", Level::Precise)];

/// One measurement: one configuration at one level.
struct Cell {
    ctp: bool,
    repeat: usize,
    n: u32,
    txns: u32,
    crashes: usize,
    level: &'static str,
    /// `CEX`, `CERT(..)`, `BUDGET`, `VACUOUS`, `EXHAUSTED` or `PANIC: ..`.
    verdict: String,
    /// Whether the verdict is a counterexample (`None` on a panic).
    counterexample: Option<bool>,
    hypotheses: usize,
    schedules: usize,
    /// Mean number of dependencies per derivation, over every derivation of every schedule in
    /// which the goal held.
    mean_derivation: f64,
    derivations: usize,
    distinct_derivations: usize,
    replays: usize,
    replay_fallbacks: usize,
    secs: f64,
}

fn env_list<T: std::str::FromStr>(name: &str, default: &[T]) -> Vec<T>
where
    T: Clone,
    T::Err: std::fmt::Debug,
{
    std::env::var(name)
        .ok()
        .map(|v| v.split(',').map(|x| x.trim().parse().unwrap()).collect())
        .unwrap_or_else(|| default.to_vec())
}

fn exploration() -> Exploration {
    match std::env::var("LDFI_ABLATION_EXPLORATION").as_deref() {
        Ok(s) if s.starts_with("fuzz") => Exploration::Fuzz {
            instances: s[4..].parse().expect("LDFI_ABLATION_EXPLORATION=fuzz<k>"),
        },
        Ok("exhaustive") => Exploration::Exhaustive,
        _ => Exploration::Fuzz { instances: 1 },
    }
}

fn repeats() -> usize {
    std::env::var("LDFI_ABLATION_REPEATS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(1)
}

fn max_hypotheses() -> usize {
    std::env::var("LDFI_ABLATION_MAX_HYPOTHESES")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(400)
}

fn verdict(o: &Outcome) -> String {
    match o {
        Outcome::Counterexample { .. } => "CEX".into(),
        Outcome::Certificate { grade, .. } => format!(
            "CERT({})",
            match grade {
                Grade::Generated => "gen",
                Grade::HandWritten => "hand",
                Grade::Goal => "goal",
            }
        ),
        Outcome::Budget { .. } => "BUDGET".into(),
        Outcome::Vacuous { .. } => "VACUOUS".into(),
        Outcome::Exhausted { .. } => "EXHAUSTED".into(),
    }
}

/// Every level on one compiled program at one load and crash budget.
#[expect(clippy::too_many_arguments, reason = "a measurement cell")]
fn measure(
    ctp: bool,
    n: u32,
    txns: u32,
    crashes: usize,
    sim: &CompiledSim,
    h: &two_pc::Harness,
    configure: &dyn Fn(usize) -> Ldfi,
    out: &mut Vec<Cell>,
) {
    let body = async |run: &Run<'_>| {
        two_pc::body(h, run).await;
        true
    };
    for repeat in 0..repeats() {
        for (name, level) in LEVELS {
            let start = Instant::now();
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                configure(crashes)
                    .level(level)
                    .exploration(exploration())
                    .max_hypotheses(max_hypotheses())
                    .quiet()
                    .run(sim, &body)
            }));
            let secs = start.elapsed().as_secs_f64();
            let cell = match result {
                Ok(outcome) => {
                    let held: Vec<_> = outcome
                        .runs()
                        .iter()
                        .flat_map(|h| &h.schedules)
                        .filter(|s| s.goal_held)
                        .collect();
                    let derivations: Vec<&Derivation> =
                        held.iter().flat_map(|s| &s.derivations).collect();
                    let distinct: BTreeSet<&Derivation> = derivations.iter().copied().collect();
                    let total_deps: usize = derivations.iter().map(|d| d.len()).sum();
                    let all = outcome.runs().iter().flat_map(|h| &h.schedules);
                    let (replays, fallbacks) =
                        all.fold((0, 0), |(r, f), s| (r + s.replays, f + s.replay_fallbacks));
                    Cell {
                        ctp,
                        repeat,
                        n,
                        txns,
                        crashes,
                        level: name,
                        verdict: verdict(&outcome),
                        counterexample: Some(outcome.is_counterexample()),
                        hypotheses: outcome.runs().len(),
                        schedules: outcome.schedules(),
                        mean_derivation: if derivations.is_empty() {
                            0.0
                        } else {
                            total_deps as f64 / derivations.len() as f64
                        },
                        derivations: derivations.len(),
                        distinct_derivations: distinct.len(),
                        replays,
                        replay_fallbacks: fallbacks,
                        secs,
                    }
                }
                Err(payload) => {
                    let msg = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                        .unwrap_or_else(|| "non-string panic".into());
                    let msg: String = msg.lines().next().unwrap_or("").chars().take(80).collect();
                    Cell {
                        ctp,
                        repeat,
                        n,
                        txns,
                        crashes,
                        level: name,
                        verdict: format!("PANIC: {msg}"),
                        counterexample: None,
                        hypotheses: 0,
                        schedules: 0,
                        mean_derivation: 0.0,
                        derivations: 0,
                        distinct_derivations: 0,
                        replays: 0,
                        replay_fallbacks: 0,
                        secs,
                    }
                }
            };
            eprintln!(
                "cell: ctp={ctp} n={n} load={txns} cr={crashes} #{repeat} {name}: {} hyp={} sched={} deriv={} ({} distinct, mean {:.2}) replays={} fallbacks={} {:.1}s",
                cell.verdict,
                cell.hypotheses,
                cell.schedules,
                cell.derivations,
                cell.distinct_derivations,
                cell.mean_derivation,
                cell.replays,
                cell.replay_fallbacks,
                cell.secs
            );
            out.push(cell);
        }
    }
}

fn config_label(c: &Cell) -> String {
    if repeats() > 1 {
        format!("n={} load={} cr={} #{}", c.n, c.txns, c.crashes, c.repeat)
    } else {
        format!("n={} load={} cr={}", c.n, c.txns, c.crashes)
    }
}

/// The Markdown report: one table per protocol variant (rows = configuration, columns =
/// level, cells = `verdict / hypotheses / schedules / mean-derivation-size / secs`), a
/// detail table with the remaining counters, and the verdict-consistency check.
fn render(cells: &[Cell]) -> String {
    let mut out = String::new();
    writeln!(
        out,
        "Exploration: `{:?}`; hypothesis budget {}; `fuzz_iterations` 8 (unused under exhaustive). Cell = `verdict / hypotheses / schedules / mean derivation size / seconds`.\n",
        exploration(),
        max_hypotheses()
    )
    .unwrap();
    for ctp in [false, true] {
        let rows: Vec<&Cell> = cells.iter().filter(|c| c.ctp == ctp).collect();
        if rows.is_empty() {
            continue;
        }
        writeln!(
            out,
            "### 2PC{} (`ctp={ctp}`)\n",
            if ctp {
                " with cooperative termination"
            } else {
                " without cooperative termination"
            }
        )
        .unwrap();
        write!(out, "| config |").unwrap();
        for (name, _) in LEVELS {
            write!(out, " {name} |").unwrap();
        }
        writeln!(out).unwrap();
        write!(out, "|---|").unwrap();
        for _ in LEVELS {
            write!(out, "---|").unwrap();
        }
        writeln!(out).unwrap();
        let mut configs: Vec<String> = vec![];
        for c in &rows {
            let label = config_label(c);
            if !configs.contains(&label) {
                configs.push(label);
            }
        }
        for label in &configs {
            write!(out, "| {label} |").unwrap();
            for (name, _) in LEVELS {
                match rows
                    .iter()
                    .find(|c| &config_label(c) == label && c.level == name)
                {
                    Some(c) if c.counterexample.is_some() => write!(
                        out,
                        " {} / {} / {} / {:.2} / {:.1}s |",
                        c.verdict, c.hypotheses, c.schedules, c.mean_derivation, c.secs
                    )
                    .unwrap(),
                    Some(c) => write!(out, " {} / {:.1}s |", c.verdict, c.secs).unwrap(),
                    None => write!(out, " - |").unwrap(),
                }
            }
            writeln!(out).unwrap();
        }
        writeln!(out).unwrap();

        writeln!(
            out,
            "Detail (`derivations observed / distinct derivations / replays / replay fallbacks`):\n"
        )
        .unwrap();
        write!(out, "| config |").unwrap();
        for (name, _) in LEVELS {
            write!(out, " {name} |").unwrap();
        }
        writeln!(out).unwrap();
        write!(out, "|---|").unwrap();
        for _ in LEVELS {
            write!(out, "---|").unwrap();
        }
        writeln!(out).unwrap();
        for label in &configs {
            write!(out, "| {label} |").unwrap();
            for (name, _) in LEVELS {
                match rows
                    .iter()
                    .find(|c| &config_label(c) == label && c.level == name)
                {
                    Some(c) => write!(
                        out,
                        " {} / {} / {} / {} |",
                        c.derivations, c.distinct_derivations, c.replays, c.replay_fallbacks
                    )
                    .unwrap(),
                    None => write!(out, " - |").unwrap(),
                }
            }
            writeln!(out).unwrap();
        }
        writeln!(out).unwrap();

        // Verdict consistency across levels.
        let mut disagreements = vec![];
        for label in &configs {
            let verdicts: BTreeSet<Option<bool>> = rows
                .iter()
                .filter(|c| &config_label(c) == label)
                .map(|c| c.counterexample)
                .collect();
            if verdicts.len() > 1 {
                disagreements.push(label.clone());
            }
        }
        if disagreements.is_empty() {
            writeln!(
                out,
                "Verdicts (counterexample vs. certificate) agree across every level for every configuration.\n"
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "**VERDICT DISAGREEMENT ACROSS LEVELS** (a soundness bug or a panic) at: {}.\n",
                disagreements.join(", ")
            )
            .unwrap();
        }
    }
    out
}

fn grid(ns: &[u32]) -> Vec<Cell> {
    let loads = env_list("LDFI_ABLATION_LOAD", &[1u32, 4, 8]);
    let crashes = env_list("LDFI_ABLATION_CRASHES", &[0usize, 1]);
    let mut cells = vec![];
    for &n in ns {
        for ctp in [false, true] {
            // One compilation (a trybuild, about a minute) per protocol size and variant; the
            // workload and the crash budget are runtime knobs.
            let provenance = std::env::var("LDFI_ABLATION_PROVENANCE").as_deref() != Ok("0");
            let (sim, mut h, configure) = two_pc::compile_with(n, ctp, 1, provenance, 8);
            for &txns in &loads {
                h = two_pc::with_txns(h, txns);
                for &cr in &crashes {
                    measure(ctp, n, txns, cr, &sim, &h, &configure, &mut cells);
                }
            }
        }
    }
    cells
}

fn run_and_write(ns: &[u32], suffix: &str) {
    let Ok(path) = std::env::var("LDFI_ABLATION_OUT") else {
        return;
    };
    let cells = grid(ns);
    let report = render(&cells);
    eprintln!("{report}");
    std::fs::write(format!("{path}{suffix}"), report).expect("LDFI_ABLATION_OUT not writable");
}

/// The whole grid: n ∈ {2, 3, 4} (or `LDFI_ABLATION_N`) × load ∈ {1, 4, 8} × ctp × crash
/// budget ∈ {0, 1} × four levels.
#[test]
fn level_ablation_two_pc() {
    if std::env::var("LDFI_ABLATION_OUT").is_err() || std::env::var("LDFI_ABLATION_SHARDS").is_ok()
    {
        return;
    }
    let ns = env_list("LDFI_ABLATION_N", &[2u32, 3, 4]);
    run_and_write(&ns, "");
}

/// One shard per `n`, for parallel runs: `LDFI_ABLATION_SHARDS=1`.
#[test]
fn level_ablation_two_pc_n2() {
    if std::env::var("LDFI_ABLATION_SHARDS").is_err() {
        return;
    }
    run_and_write(&[2], ".n2");
}

#[test]
fn level_ablation_two_pc_n3() {
    if std::env::var("LDFI_ABLATION_SHARDS").is_err() {
        return;
    }
    run_and_write(&[3], ".n3");
}

#[test]
fn level_ablation_two_pc_n4() {
    if std::env::var("LDFI_ABLATION_SHARDS").is_err() {
        return;
    }
    run_and_write(&[4], ".n4");
}
