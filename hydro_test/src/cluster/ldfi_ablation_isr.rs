//! Lineage-level ablation of hydrofrac on ISR replication: the same provenance-compiled
//! program searched under every [`Level`], for both encodings of the protocol (closure and
//! dataflow), both variants (unclean election, the bug; clean election, the certificate) and
//! the crash budgets the unit tests use.
//!
//! Gated on `LDFI_ABLATION_OUT` (the path of a Markdown file to write); without it the test
//! returns immediately. Run with
//!
//! ```text
//! LDFI_ABLATION_OUT=target/isr-ablation.md cargo test -p hydro_test level_ablation_isr -- --test-threads=1 --nocapture
//! ```
//!
//! `LDFI_ABLATION_ONLY=closure|dataflow` restricts to one encoding.

use std::fmt::Write as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use hydro_lang::sim::compiled::CompiledSim;
use hydro_lang::sim::ldfi::{Exploration, Ldfi, Level, Outcome, Run};

use crate::cluster::isr_replication::Encoding;
use crate::cluster::isr_replication::harness as isr;

/// Schedules sampled per hypothesis by the ISR unit tests (`isr_replication::tests::SAMPLED`).
const SAMPLED: usize = 8;

/// The columns: `(name, level, disjunctive provenance)`.
const LEVELS: [(&str, Level, bool); 3] = [
    ("exec", Level::Execution, false),
    ("record", Level::Record, false),
    ("disjunctive", Level::Record, true),
];

/// One (encoding, config) × level measurement.
struct Cell {
    verdict: &'static str,
    hypotheses: usize,
    schedules: usize,
    /// Mean number of dependencies per derivation, over the derivations of goal-held
    /// schedules (`None` when there were none).
    mean_derivation: Option<f64>,
    /// Derivations (clauses) per hypothesis run.
    clauses_per_run: f64,
    replays: usize,
    fallbacks: usize,
    /// Steps where the witness cap truncated (disjunctive provenance only).
    overflows: usize,
    secs: f64,
}

impl Cell {
    fn render(&self) -> String {
        let deriv = match self.mean_derivation {
            Some(d) => format!("{d:.1}"),
            None => "-".into(),
        };
        let mut s = format!(
            "{} / {} / {} / {} / {:.2} / {:.1}s",
            self.verdict, self.hypotheses, self.schedules, deriv, self.clauses_per_run, self.secs
        );
        if self.replays > 0 || self.fallbacks > 0 || self.overflows > 0 {
            write!(
                s,
                " (replays {}, fallbacks {}, overflows {})",
                self.replays, self.fallbacks, self.overflows
            )
            .unwrap();
        }
        s
    }
}

fn verdict(o: &Outcome) -> &'static str {
    match o {
        Outcome::Counterexample { .. } => "bug",
        Outcome::Certificate { .. } => "cert",
        Outcome::Exhausted { .. } => "exhausted",
        Outcome::Budget { .. } => "budget",
        Outcome::Vacuous { .. } => "vacuous",
    }
}

fn measure(sim: &CompiledSim, ldfi: Ldfi, body: impl AsyncFn(&Run<'_>)) -> Result<Cell, String> {
    let start = Instant::now();
    let body = AssertUnwindSafe(&body);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        ldfi.search(sim, async |run| body(run).await)
    }))
    .map_err(|e| {
        e.downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "panic".into())
    })?;
    let secs = start.elapsed().as_secs_f64();
    let schedules = outcome.runs().iter().flat_map(|h| &h.schedules);
    let (mut deriv_sum, mut deriv_n) = (0usize, 0usize);
    let (mut replays, mut fallbacks, mut overflows) = (0usize, 0usize, 0usize);
    for s in schedules {
        replays += s.replays;
        fallbacks += s.replay_fallbacks;
        overflows += s.witness_overflows;
        if s.goal_held {
            for d in &s.derivations {
                deriv_sum += d.len();
                deriv_n += 1;
            }
        }
    }
    Ok(Cell {
        verdict: verdict(&outcome),
        hypotheses: outcome.runs().len(),
        schedules: outcome.schedules(),
        mean_derivation: (deriv_n > 0).then(|| deriv_sum as f64 / deriv_n as f64),
        clauses_per_run: if outcome.runs().is_empty() {
            0.0
        } else {
            deriv_n as f64 / outcome.runs().len() as f64
        },
        replays,
        fallbacks,
        overflows,
        secs,
    })
}

/// The lineage ablation on ISR replication; see the module docs.
#[test]
fn level_ablation_isr() {
    let Ok(out) = std::env::var("LDFI_ABLATION_OUT") else {
        return;
    };
    let only = std::env::var("LDFI_ABLATION_ONLY").ok();

    let mut md = String::new();
    writeln!(md, "## ISR replication").unwrap();
    writeln!(md).unwrap();
    writeln!(
        md,
        "Harness: `isr_replication::harness` (3 replicas, 4 rounds, 1 write), hand-written \
         durability specification, replicas crashable, omissions on the replication channel in \
         the first round. Every cell searches the **same provenance-compiled program** \
         (`with_provenance()`), so the levels differ only in which lineage the backward closure \
         follows. `fuzz k` = `Exploration::Fuzz {{ instances: k }}` (k = 8 is what the unit \
         tests use; k = 1 is the measurement grid's one schedule per hypothesis). Hypothesis \
         budget 256 (the default)."
    )
    .unwrap();
    writeln!(md).unwrap();
    writeln!(
        md,
        "Cell = `verdict / hypotheses / schedules / mean derivation size / clauses per run / wall secs`; \
         mean derivation size is the mean number of dependencies per derivation over goal-held \
         schedules; `(replays, fallbacks)` are counterfactual replays and replay fallbacks."
    )
    .unwrap();
    writeln!(md).unwrap();
    writeln!(
        md,
        "| encoding | config | {} |",
        LEVELS.map(|(n, _, _)| n).join(" | ")
    )
    .unwrap();
    writeln!(md, "|---|---|{}", "---|".repeat(LEVELS.len())).unwrap();

    let mut rows: Vec<(String, String, Vec<Result<Cell, String>>)> = vec![];
    for encoding in [Encoding::Closure, Encoding::Dataflow] {
        let name = format!("{encoding:?}").to_lowercase();
        if only.as_deref().is_some_and(|o| o != name) {
            continue;
        }
        for unclean in [true, false] {
            let (sim, h, configure) =
                isr::compile_with(unclean, encoding, isr::DEFAULT_SHAPE, true, 8);
            let variant = if unclean { "unclean" } else { "clean" };
            for (crashes, instances) in [(1usize, SAMPLED), (1, 1), (0, SAMPLED)] {
                let config = format!("{variant} cr={crashes} fuzz {instances}");
                let mut cells = vec![];
                for (level_name, level, disjunctive) in LEVELS {
                    eprintln!("ablation: {name} {config} {level_name}");
                    let cell = measure(
                        &sim,
                        configure(crashes)
                            .level(level)
                            .disjunctive_provenance(disjunctive)
                            .exploration(Exploration::Fuzz { instances })
                            .quiet(),
                        async |run| isr::body(&h, run).await,
                    );
                    match &cell {
                        Ok(c) => eprintln!("  -> {}", c.render()),
                        Err(e) => eprintln!("  -> panic: {e}"),
                    }
                    cells.push(cell);
                }
                rows.push((name.clone(), config, cells));
            }
        }
    }

    for (encoding, config, cells) in &rows {
        write!(md, "| {encoding} | {config} |").unwrap();
        for cell in cells {
            match cell {
                Ok(c) => write!(md, " {} |", c.render()).unwrap(),
                Err(e) => {
                    let e = e.lines().next().unwrap_or("").replace('|', "\\|");
                    write!(md, " n/a: {e} |").unwrap()
                }
            }
        }
        writeln!(md).unwrap();
    }

    eprintln!("\n{md}");
    std::fs::write(&out, md).expect("LDFI_ABLATION_OUT not writable");
}

/// Diagnostic for a verdict disagreement between levels: prints the full outcome of the
/// unclean-election search at every level (`LDFI_ABLATION_DIAG=closure|dataflow`), with the
/// search log on.
#[test]
fn level_ablation_isr_diagnose() {
    let Ok(which) = std::env::var("LDFI_ABLATION_DIAG") else {
        return;
    };
    let encoding = if which == "closure" {
        Encoding::Closure
    } else {
        Encoding::Dataflow
    };
    let (sim, h, configure) = isr::compile_with(true, encoding, isr::DEFAULT_SHAPE, true, 8);
    for (level_name, level, disjunctive) in LEVELS {
        eprintln!("\n===== {encoding:?} unclean cr=1 fuzz 1 {level_name} =====");
        let outcome = configure(1)
            .level(level)
            .disjunctive_provenance(disjunctive)
            .exploration(Exploration::Fuzz { instances: 1 })
            .search(&sim, async |run| isr::body(&h, run).await);
        eprintln!("{outcome}");
        for (i, hyp) in outcome.runs().iter().enumerate() {
            eprintln!("hypothesis {i}: {:?}", hyp.faults);
            for s in &hyp.schedules {
                eprintln!(
                    "  goal_held={} vacuous={} derivations={:?}",
                    s.goal_held, s.vacuous, s.derivations
                );
            }
        }
    }
}
