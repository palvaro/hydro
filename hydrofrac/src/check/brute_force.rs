//! The brute-force cross-check of the search (docs/design/hydrofrac-lite.md §7, "Honesty
//! check"): every admissible fault set up to a size, run without any lineage pruning, against
//! what [`EcCheck::run`] concluded.
//!
//! The enumerator learns its universe from the runs themselves. The failure-free probe names
//! the first messages; every set it runs may reveal more (a crash can change what the others
//! send), and those join the universe, until a pass over every set up to the size reveals
//! nothing new. The candidates are those of the fault model:
//! - `omit(m)` for every message `m` sent before the heal on a lossy channel;
//! - `crash(n)` for every crashable node, and `crash(n after m)` for every message `m` it sent
//!   before the heal.
//!
//! A set is admissible for a label under the same bounds the search uses: at most one crash per
//! node, never every node, never all or all but one of the checked cluster, and the
//! [`EcCheck::max_crashes`] cap. Each set runs over the same schedules, with the same driving,
//! heal and quiesce, as the search's; the runs are read by the same evaluation. Only the choice
//! of what to run differs, so agreement tests the lineage and the hitting-set pruning.
//!
//! What agreement means, for the fault sets the enumeration covers:
//! - a certificate for `k` crashes ([`Tolerance::UpTo`]) means no set with at most `k` crashes
//!   breaks the label, and [`Tolerance::All`] that no admissible set does;
//! - a counterexample after tolerating `t` crashes means no set with at most `t` crashes breaks
//!   the label, the counterexample has exactly `t + 1` crashes (none when `t` is `None`), and
//!   rerunning its fault set breaks the label again (under exhaustive exploration).
//!
//! So whenever the enumeration finds a breaking set with `c` crashes, the search must report a
//! counterexample with at most `c` crashes (it tolerates fewer than `c`); and when the
//! enumeration finds none, the search must not report one within the enumerated sizes.
//!
//! A schedule-sensitive label is never certified ([`Verdict::Exhausted`]), and its tolerances
//! are only what its heuristic search found. The same comparison still applies to it, as a
//! check of the heuristic rather than of a proof: `Exhausted` agrees only where the enumeration
//! finds no breaking set within the bound the search swept, and a counterexample that
//! "tolerated" `t` crashes must not have missed one with fewer. A disagreement there is a miss
//! of the heuristic, not a false certificate, and the report says so.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::sim::compiled::CompiledSim;

use super::enumerate::{admissible, harvest};
use super::{EcCheck, Engine, Run};
use crate::check::results::{LabelResult, Results, Verdict, label_name};
use crate::fault::MessageId;
use crate::report::ReportNames;
use crate::search::{CrashBounds, Fault, FaultSet, Tolerance, crash_count};

/// The outcome of [`EcCheck::cross_check`]: the search's results, the enumeration's, and every
/// way they disagree.
#[doc(hidden)]
pub struct CrossCheck {
    /// What [`EcCheck::run`] concluded.
    pub results: Results,
    /// What the enumeration found.
    pub brute: BruteForce,
    /// Every disagreement, in words. Empty when they agree.
    pub disagreements: Vec<String>,
    /// For each counterexample whose fault set was rerun, whether the rerun broke its label
    /// again (by label index).
    pub confirmed: BTreeMap<usize, bool>,
}

impl CrossCheck {
    /// Recomputes [`Self::disagreements`] from [`Self::results`] and [`Self::brute`] (after a
    /// test edits a verdict, to check that the comparison catches it).
    pub fn recheck(&mut self) {
        self.disagreements = disagreements(&self.results, &self.brute, &self.confirmed);
    }

    /// Panics with both reports if the search and the enumeration disagree.
    pub fn assert_agrees(&self) {
        assert!(
            self.disagreements.is_empty(),
            "hydrofrac: the search and the brute-force enumeration disagree:\n- {}\n\nsearch:\n{}\nenumeration:\n{}",
            self.disagreements.join("\n- "),
            self.results,
            self.brute.rendered(Some(&self.results.report_names())),
        );
    }
}

impl Display for CrossCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            self.brute.rendered(Some(&self.results.report_names()))
        )?;
        if self.disagreements.is_empty() {
            writeln!(f, "the search agrees with the enumeration on every label")
        } else {
            for d in &self.disagreements {
                writeln!(f, "DISAGREEMENT: {d}")?;
            }
            Ok(())
        }
    }
}

/// What the enumeration found, per searched label.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct BruteForce {
    /// The largest set size enumerated.
    pub max_faults: usize,
    /// One entry per label the search searched (labels decided without a search, such as
    /// vacuous or single-member ones, have none).
    pub labels: Vec<BruteLabel>,
    /// The size of the final universe of candidate faults.
    pub universe: usize,
    /// The fault sets run, counting the failure-free probe.
    pub sets: usize,
    /// The schedules explored over those sets.
    pub schedules: usize,
    /// How many passes the universe took to reach its fixpoint.
    pub passes: usize,
    /// Wall time of the enumeration.
    pub wall: Duration,
    /// Every set run, counting the failure-free probe.
    pub enumerated: BTreeSet<FaultSet<MessageId>>,
}

impl BruteForce {
    /// The entry for the label with this index.
    pub fn label(&self, index: usize) -> Option<&BruteLabel> {
        self.labels.iter().find(|l| l.index == index)
    }
}

/// One label's enumeration.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct BruteLabel {
    /// The label's index ([`crate::observe::Labels::plans`]).
    pub index: usize,
    /// Its name, as the report prints it.
    pub name: String,
    /// The admissible sets run for it, counting the failure-free probe.
    pub admissible: usize,
    /// Every admissible set that breaks it, fewest crashes first, then fewest faults. The
    /// empty set if the failure-free run already breaks it.
    pub breaking: Vec<FaultSet<MessageId>>,
}

impl BruteLabel {
    /// The fewest crashes of any breaking set (`Some(0)` if omissions alone break the label).
    pub fn min_crashes(&self) -> Option<usize> {
        self.breaking.first().map(crash_count)
    }

    /// A breaking set with at most `crashes` crashes, if any.
    pub fn breaking_within(&self, crashes: usize) -> Option<&FaultSet<MessageId>> {
        self.breaking.iter().find(|s| crash_count(s) <= crashes)
    }
}

/// `{f1, f2}` in the report's names (source names for channels and nodes, decoded payloads),
/// or in the faults' own rendering when no names are at hand.
fn show(names: Option<&ReportNames<'_>>, set: &FaultSet<MessageId>) -> String {
    if let Some(names) = names {
        names.fault_set(set)
    } else {
        let faults: Vec<String> = set.iter().map(|f| f.to_string()).collect();
        format!("{{{}}}", faults.join(", "))
    }
}

impl Display for BruteForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.render(f, None)
    }
}

impl BruteForce {
    /// The enumeration's report, naming faults the way `names` does (raw without it).
    pub fn rendered(&self, names: Option<&ReportNames<'_>>) -> String {
        struct Rendered<'a, 'n>(&'a BruteForce, Option<&'a ReportNames<'n>>);
        impl Display for Rendered<'_, '_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.render(f, self.1)
            }
        }
        Rendered(self, names).to_string()
    }

    fn render(&self, f: &mut fmt::Formatter<'_>, names: Option<&ReportNames<'_>>) -> fmt::Result {
        writeln!(
            f,
            "brute force up to {} fault(s): {} candidate faults, {} sets, {} schedules, {} pass(es), {:.2?}",
            self.max_faults, self.universe, self.sets, self.schedules, self.passes, self.wall
        )?;
        for l in &self.labels {
            write!(f, "  {}: ", l.name)?;
            match l.breaking.first() {
                None => write!(f, "no admissible set breaks it")?,
                Some(set) if set.is_empty() => write!(f, "the failure-free run breaks it")?,
                Some(set) => match crash_count(set) {
                    0 => write!(f, "omissions alone break it: {}", show(names, set))?,
                    c => write!(f, "breaks with {c} crash(es): {}", show(names, set))?,
                },
            }
            writeln!(
                f,
                " ({} breaking of {} admissible sets)",
                l.breaking.len(),
                l.admissible
            )?;
        }
        Ok(())
    }
}

impl EcCheck {
    /// Runs [`Self::run`], then the brute-force enumeration of every admissible fault set of at
    /// most `max_faults` faults (see the [module docs](self)), and compares them. Test-only:
    /// the enumeration is exponential in `max_faults`.
    ///
    /// # Panics
    /// If `sim` was not instrumented by this check.
    #[doc(hidden)]
    pub fn cross_check(
        &self,
        sim: &CompiledSim,
        mut body: impl AsyncFnMut(),
        max_faults: usize,
    ) -> CrossCheck {
        let results = self.run(sim, &mut body);
        let labels = self
            .taps
            .as_ref()
            .expect("`EcCheck::cross_check` needs a simulation compiled from `EcCheck::instrument`")
            .labels();
        let mut engine = Engine::new(self, sim, &labels, &mut body);
        let (brute, confirmed) = engine.brute_force(max_faults, &results);
        let disagreements = disagreements(&results, &brute, &confirmed);
        CrossCheck {
            results,
            brute,
            disagreements,
            confirmed,
        }
    }
}

/// Every subset of `universe` of exactly `size` faults, in order.
fn subsets(universe: &[Fault<MessageId>], size: usize) -> Vec<FaultSet<MessageId>> {
    fn go(
        universe: &[Fault<MessageId>],
        start: usize,
        size: usize,
        current: &mut Vec<Fault<MessageId>>,
        out: &mut Vec<FaultSet<MessageId>>,
    ) {
        if current.len() == size {
            out.push(current.iter().cloned().collect());
            return;
        }
        for i in start..universe.len() {
            current.push(universe[i].clone());
            go(universe, i + 1, size, current, out);
            current.pop();
        }
    }
    let mut out = vec![];
    go(universe, 0, size, &mut vec![], &mut out);
    out
}

impl<B: AsyncFnMut()> Engine<'_, B> {
    /// The enumeration (see the [module docs](self)), and for every counterexample in
    /// `results` whose set was rerun, whether the rerun broke its label again (by label index).
    fn brute_force(
        &mut self,
        max_faults: usize,
        results: &Results,
    ) -> (BruteForce, BTreeMap<usize, bool>) {
        let start = Instant::now();
        let mut universe: BTreeSet<Fault<MessageId>> = BTreeSet::new();
        let (_, targets, probe) =
            self.prepare(|tables, record| harvest(record, tables, &mut universe));
        let bounds: Vec<CrashBounds> = targets.iter().map(|t| self.bounds(t)).collect();
        let mut done: BTreeSet<FaultSet<MessageId>> = BTreeSet::from([FaultSet::new()]);
        let mut runs: Vec<(FaultSet<MessageId>, Rc<Run>)> = vec![(FaultSet::new(), probe.clone())];
        let mut schedules = probe.schedules;
        let mut passes = 0;
        loop {
            passes += 1;
            let candidates: Vec<Fault<MessageId>> = universe.iter().cloned().collect();
            let fresh: Vec<FaultSet<MessageId>> = (1..=max_faults)
                .flat_map(|size| subsets(&candidates, size))
                .filter(|set| !done.contains(set))
                .filter(|set| bounds.iter().any(|b| admissible(set, b)))
                .collect();
            if fresh.is_empty() {
                break;
            }
            for set in fresh {
                done.insert(set.clone());
                let run = Rc::new(self.execute(&set, &targets, false, |tables, record| {
                    harvest(record, tables, &mut universe)
                }));
                schedules += run.schedules;
                self.runs.insert(set.clone(), run.clone());
                runs.push((set, run));
            }
        }

        let labels = targets
            .iter()
            .zip(&bounds)
            .map(|(t, b)| {
                let mut admissible_sets = 0;
                let mut breaking = vec![];
                for (set, run) in &runs {
                    if !admissible(set, b) {
                        continue;
                    }
                    admissible_sets += 1;
                    if run.labels[&t.label].failure.is_some() {
                        breaking.push(set.clone());
                    }
                }
                breaking.sort_by_key(|s| (crash_count(s), s.len()));
                BruteLabel {
                    index: t.label,
                    name: label_name(&self.labels.plans[t.label]),
                    admissible: admissible_sets,
                    breaking,
                }
            })
            .collect();

        // Rerun every counterexample's set (outside the enumeration if it is larger), unless
        // schedules are sampled: a sample may miss the failing schedule.
        let mut confirmed = BTreeMap::new();
        if self.sample_schedules.is_none() {
            for label in results.results() {
                let Some(cex) = label.verdict.counterexample() else {
                    continue;
                };
                if !targets.iter().any(|t| t.label == label.index) {
                    continue;
                }
                let run = self.run(&cex.faults, &targets);
                confirmed.insert(label.index, run.labels[&label.index].failure.is_some());
            }
        }

        let brute = BruteForce {
            max_faults,
            labels,
            universe: universe.len(),
            sets: runs.len(),
            schedules,
            passes,
            wall: start.elapsed(),
            enumerated: done,
        };
        (brute, confirmed)
    }
}

/// Every way `results` and `brute` disagree (see the [module docs](self)).
fn disagreements(
    results: &Results,
    brute: &BruteForce,
    confirmed: &BTreeMap<usize, bool>,
) -> Vec<String> {
    let names = results.report_names();
    let mut out = vec![];
    for label in results.results() {
        let Some(b) = brute.label(label.index) else {
            continue;
        };
        if let Some(problem) = disagreement(
            label,
            b,
            brute,
            confirmed.get(&label.index).copied(),
            &names,
        ) {
            out.push(format!("{}: {problem}", b.name));
        }
    }
    out
}

/// How `label`'s verdict disagrees with the enumeration `b`, if it does.
fn disagreement(
    label: &LabelResult,
    b: &BruteLabel,
    brute: &BruteForce,
    confirmed: Option<bool>,
    names: &ReportNames<'_>,
) -> Option<String> {
    let show = |set: &FaultSet<MessageId>| show(Some(names), set);
    let refuted = |k: usize, claim: &str| {
        b.breaking_within(k).map(|set| {
            format!(
                "the search says it {claim}, but {} ({} crash(es)) breaks it",
                show(set),
                crash_count(set)
            )
        })
    };
    // The same, for a schedule-sensitive label's search, which proves nothing.
    let missed = |k: usize, claim: &str| {
        b.breaking_within(k).map(|set| {
            format!(
                "the search {claim}, but {} ({} crash(es)) breaks it (the label is \
                 schedule-sensitive: a miss of the search heuristic, not a false certificate)",
                show(set),
                crash_count(set)
            )
        })
    };
    match &label.verdict {
        Verdict::Certified {
            tolerates: Tolerance::All,
        } => refuted(usize::MAX, "holds under every admissible fault set"),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(k),
        } => refuted(*k, &format!("holds with up to {k} crash(es)")),
        Verdict::Budget { tolerates: Some(t) } => {
            refuted(*t, &format!("holds with up to {t} crash(es) (budget)"))
        }
        Verdict::Exhausted {
            tolerates: Tolerance::All,
            ..
        } => missed(usize::MAX, "found no counterexample"),
        Verdict::Exhausted {
            tolerates: Tolerance::UpTo(k),
            ..
        } => missed(
            *k,
            &format!("found no counterexample with up to {k} crash(es)"),
        ),
        // A bounded certificate covers the sets of at most `max_faults` faults (all, if
        // `None`) with at most `tolerates` crashes.
        Verdict::CertifiedByEnumeration {
            tolerates,
            max_faults,
            ..
        } => {
            let k = match tolerates {
                Tolerance::All => usize::MAX,
                Tolerance::UpTo(k) => *k,
            };
            b.breaking
                .iter()
                .find(|s| crash_count(s) <= k && max_faults.is_none_or(|f| s.len() <= f))
                .map(|set| {
                    format!(
                        "the search's enumeration says it holds for every admissible set of up to \
                         {} fault(s) with up to {} crash(es), but {} ({} crash(es)) breaks it",
                        max_faults.map_or_else(|| "any number of".to_owned(), |f| f.to_string()),
                        if k == usize::MAX {
                            "any".to_owned()
                        } else {
                            k.to_string()
                        },
                        show(set),
                        crash_count(set)
                    )
                })
        }
        Verdict::Counterexample(cex) => {
            if let Some(t) = cex.tolerates {
                let problem = if cex.tolerance_certified {
                    refuted(t, &format!("tolerates {t} crash(es)"))
                } else {
                    missed(
                        t,
                        &format!("found no counterexample with up to {t} crash(es)"),
                    )
                };
                if let Some(problem) = problem {
                    return Some(problem);
                }
            }
            let expected = cex.tolerates.map_or(0, |t| t + 1);
            if cex.crashes() != expected {
                return Some(format!(
                    "the counterexample {} has {} crash(es), but the sweep tolerated {:?} and so \
                     should break at exactly {expected}",
                    show(&cex.faults),
                    cex.crashes(),
                    cex.tolerates,
                ));
            }
            if confirmed == Some(false) {
                return Some(format!(
                    "rerunning the counterexample {} does not break the label",
                    show(&cex.faults)
                ));
            }
            // A counterexample within the enumerated sizes must be one the enumeration ran
            // and saw break.
            if cex.faults.len() <= brute.max_faults {
                if !brute.enumerated.contains(&cex.faults) {
                    return Some(format!(
                        "the search's counterexample {} is within the enumerated sizes, but the \
                         enumeration never ran it (its universe missed a fault, or the set is \
                         inadmissible)",
                        show(&cex.faults)
                    ));
                }
                if !b.breaking.contains(&cex.faults) {
                    return Some(format!(
                        "the enumeration ran the search's counterexample {} and it held",
                        show(&cex.faults)
                    ));
                }
            }
            None
        }
        _ => None,
    }
}
