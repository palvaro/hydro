//! What [`super::EcCheck::run`] found: one verdict per label, and a readable report.

use std::fmt::{self, Display};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::network_hook::ChannelInfo;

use crate::fault::{Event, InstanceRecord, MessageId, NodeNames};
use crate::observe::{Fact, LabelPlan, Labels, NotCheckedReason, decode_output, kind_name};
pub use crate::provenance::choices::ChoiceSite;
use crate::report::ReportNames;
use crate::search::{FaultSet, Tolerance, crash_count};

/// Why a label has nothing to check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VacuousReason {
    /// The failure-free probe held no facts at any correct member: nothing to agree on.
    EmptyDomain,
    /// The label lives on a single node (a process, or a cluster of one member), which always
    /// agrees with itself.
    SingleMember,
}

impl Display for VacuousReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VacuousReason::EmptyDomain => write!(f, "the failure-free run holds no facts"),
            VacuousReason::SingleMember => write!(f, "a single member agrees with itself"),
        }
    }
}

/// A run that breaks a label.
#[derive(Clone, Debug)]
pub struct Counterexample {
    /// The fault set (the hypothesis) the run was under. Empty for a failure-free run that
    /// already breaks the label.
    pub faults: FaultSet<MessageId>,
    /// The largest crash bound the sweep cleared before this counterexample: no fault set with
    /// at most this many crashes broke the label. `None` when nothing was cleared, not even
    /// omissions alone. A certificate when [`Self::tolerance_certified`], else only what the
    /// search found.
    pub tolerates: Option<usize>,
    /// Whether [`Self::tolerates`] is a certificate. It is unless the search sampled schedules
    /// (a hypothesis past the check's schedule budget, see [`crate::EcCheck`]): then it says
    /// only that the sample showed no counterexample with fewer crashes.
    pub tolerance_certified: bool,
    /// The index of the hypothesis in the label's search (0 is the failure-free probe).
    pub hypothesis: usize,
    /// The `(correct member, fact)` pairs the run did not deliver.
    pub missing: Vec<(u32, Fact)>,
    /// Facts outside the fact domain that some correct members hold and others do not: a fault
    /// made members derive something the failure-free run never did, and they disagree on it.
    pub divergent: Vec<Fact>,
    /// The panic message, if the run panicked (a program that panics under injected faults is
    /// a counterexample).
    pub panic: Option<String>,
    /// The failing schedule: its event log and crashed nodes (for the Lamport diagram).
    pub record: Rc<InstanceRecord>,
    /// Where the failing schedule's Lamport diagram was written (the `.svg` if Graphviz
    /// rendered it, else the `.dot`), under `target/hydrofrac/<test>/<label>/`. `None` if
    /// writing it failed.
    pub diagram: Option<PathBuf>,
}

impl Counterexample {
    /// The number of crashes in [`Self::faults`].
    pub fn crashes(&self) -> usize {
        crash_count(&self.faults)
    }
}

/// The verdict on one label.
#[derive(Clone, Debug)]
pub enum Verdict {
    /// No admissible fault set breaks the label.
    Certified {
        /// The crashes it tolerates.
        tolerates: Tolerance,
    },
    /// A fault set that breaks the label.
    Counterexample(Box<Counterexample>),
    /// Nothing to check.
    Vacuous(VacuousReason),
    /// The hypothesis budget ran out: not a certificate.
    Budget {
        /// The largest crash bound cleared before the budget ran out, if any (a certificate
        /// unless schedules were sampled).
        tolerates: Option<usize>,
    },
    /// A label whose search sampled schedules (a hypothesis past the check's schedule budget,
    /// see [`crate::EcCheck`]) and found no counterexample: **not a certificate**, as a
    /// schedule outside the sample may break it.
    Exhausted {
        /// How far the search went: every crash bound up to the vacuity bounds
        /// ([`Tolerance::All`]), or up to the `max_crashes` cap.
        tolerates: Tolerance,
        /// The program's schedule choices ([`Results::choices`]).
        sites: Vec<ChoiceSite>,
    },
    /// The generated specification does not apply to the collection: **not a certificate**
    /// (members may disagree on it; [`Results::holds`] rejects it).
    NotChecked(NotCheckedReason),
}

impl Verdict {
    /// Whether the verdict is not a certificate of a label that needs checking: a
    /// counterexample, an exhausted budget, a sampled search, or a label the specification
    /// does not check (what [`Results::assert_holds`] rejects).
    pub fn fails(&self) -> bool {
        matches!(
            self,
            Verdict::Counterexample(_)
                | Verdict::Budget { .. }
                | Verdict::Exhausted { .. }
                | Verdict::NotChecked(_)
        )
    }

    /// Whether the verdict is a counterexample (what [`Results::assert_no_counterexample`]
    /// rejects).
    pub fn is_counterexample(&self) -> bool {
        matches!(self, Verdict::Counterexample(_))
    }

    /// The counterexample, if this is one.
    pub fn counterexample(&self) -> Option<&Counterexample> {
        match self {
            Verdict::Counterexample(c) => Some(c),
            _ => None,
        }
    }
}

/// What one label's search cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Hypotheses tried, counting the failure-free probe.
    pub hypotheses: usize,
    /// Schedules explored over those hypotheses (a run shared with another label counts for
    /// both).
    pub schedules: usize,
    /// Wall time of the search, counting the runs it caused (not those it reused).
    pub wall: Duration,
    /// How the search spent its hypotheses; `None` for a label decided without a run.
    pub races: Option<RaceNotes>,
    /// The facts in the label's domain: what its correct members held in the failure-free
    /// probe (0 for a label decided without a run).
    pub domain: usize,
}

/// How a label's search (docs/design/hydrofrac-races.md §4.4) spent its hypotheses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RaceNotes {
    /// Fault sets whose exploration stopped early: their derivations so far covered every pair.
    pub early_stops: usize,
    /// Fault sets run over one schedule only (every run of them has the same facts).
    pub single_schedule_sets: usize,
    /// Whether the verdict certifies Claim B (no new disagreement outside the domain): true of
    /// every certificate, since the search checks Claim B before it certifies (by §13 of
    /// docs/design/hydrofrac-soundness.md in a fragile-free or choice-free program, else by
    /// running every fault set within the bounds in every schedule). Always `false` for other
    /// verdicts.
    pub claim_b: bool,
}

/// The result for one labelled collection.
#[derive(Clone, Debug)]
pub struct LabelResult {
    /// The label's index ([`crate::observe::Labels::plans`]).
    pub index: usize,
    /// The labelled collection and how it is checked.
    pub plan: LabelPlan,
    /// The verdict.
    pub verdict: Verdict,
    /// What the search cost (zero for labels that were not searched).
    pub stats: Stats,
}

impl LabelResult {
    /// The label's display name: its binding (or operator), file and occurrence.
    pub fn name(&self) -> String {
        label_name(&self.plan)
    }
}

/// The display name of the label `plan` checks: its binding (or operator), file and
/// occurrence.
pub(crate) fn label_name(plan: &LabelPlan) -> String {
    let c = &plan.collection;
    let mut name = c.display_name().to_owned();
    if let Some(file) = &c.file {
        name.push_str(&format!(" ({file}"));
        if c.occurrence > 0 {
            name.push_str(&format!(" #{}", c.occurrence));
        }
        name.push(')');
    }
    name
}

/// The verdicts on every label of the program.
#[derive(Clone, Debug)]
pub struct Results {
    /// One result per labelled collection, in label order.
    pub labels: Vec<LabelResult>,
    /// The program's node names (counterexamples name nodes by them).
    pub names: NodeNames,
    /// The operators the provenance rewrite could not see into (opaque islands), by IR node
    /// name. Records computed through them use the execution rule: sound, but coarser.
    /// Empty when the whole program is tracked precisely.
    pub islands: Vec<String>,
    /// The program's schedule choices (docs/design/hydrofrac-lite.md §4.9): what the scan of its
    /// IR found, then its opaque [`Self::islands`]. A sampled verdict ([`Verdict::Exhausted`])
    /// lists them.
    pub choices: Vec<ChoiceSite>,
    /// The program's network channels (the report names channels by them).
    pub channels: Vec<ChannelInfo>,
    /// The labels and their tap output ports (the report decodes observed facts by them).
    pub taps: Labels,
}

impl Results {
    /// The results.
    pub fn results(&self) -> &[LabelResult] {
        &self.labels
    }

    /// The results of every label whose display name (binding or operator) is `name`.
    pub fn labels_named<'r>(&'r self, name: &'r str) -> impl Iterator<Item = &'r LabelResult> {
        self.labels
            .iter()
            .filter(move |l| l.plan.collection.display_name() == name)
    }

    /// The result of the label whose display name (binding or operator) is `name`.
    ///
    /// # Panics
    /// If no label, or more than one, has that name.
    pub fn label(&self, name: &str) -> &LabelResult {
        let mut found = self
            .labels
            .iter()
            .filter(|l| l.plan.collection.display_name() == name);
        let first = found
            .next()
            .unwrap_or_else(|| panic!("no label `{name}` in\n{self}"));
        assert!(found.next().is_none(), "several labels named `{name}`");
        first
    }

    /// Whether every label is certified (or vacuous): no counterexample, no exhausted budget,
    /// no label whose sampled search merely found nothing ([`Verdict::Exhausted`]), and no label
    /// the specification does not check ([`Verdict::NotChecked`]: a scalar, or a collection
    /// inside a tick, on which members may disagree).
    pub fn holds(&self) -> bool {
        !self.labels.iter().any(|l| l.verdict.fails())
    }

    /// Panics with the report unless every label holds (see [`Self::holds`]). A sampled search
    /// certifies nothing, so for a program past the check's schedule budget use
    /// [`Self::assert_no_counterexample`]. A label that is not checked is not certified: assert
    /// the labels that are, one by one ([`Self::label`]).
    #[track_caller]
    pub fn assert_holds(&self) {
        let unchecked = self.labels.iter().find_map(|l| match &l.verdict {
            Verdict::NotChecked(reason) => Some((l, reason)),
            _ => None,
        });
        if let Some((l, reason)) = unchecked
            && self.labels.iter().all(|l| {
                !l.verdict.fails() || matches!(l.verdict, Verdict::NotChecked(_))
            })
        {
            panic!(
                "hydrofrac: label `{}` is not checked ({reason}), so it is not certified\n{self}",
                l.name()
            );
        }
        if let Some(l) = self
            .labels
            .iter()
            .find(|l| matches!(l.verdict, Verdict::Exhausted { .. }))
            && !self.labels.iter().any(|l| l.verdict.is_counterexample())
        {
            panic!(
                "hydrofrac: schedules were sampled, so the search of label `{}` is not a \
                 certificate (`assert_no_counterexample` accepts it)\n{self}",
                l.name()
            );
        }
        assert!(self.holds(), "hydrofrac: a label does not hold\n{self}");
    }

    /// Whether no label has a counterexample (searches that found nothing count, certificates
    /// or not).
    pub fn no_counterexample(&self) -> bool {
        !self.labels.iter().any(|l| l.verdict.is_counterexample())
    }

    /// Panics with the report if some label has a counterexample. Weaker than
    /// [`Self::assert_holds`]: it also accepts a sampled search that found nothing
    /// ([`Verdict::Exhausted`]) and an exhausted budget, neither of which is a certificate.
    #[track_caller]
    pub fn assert_no_counterexample(&self) {
        assert!(
            self.no_counterexample(),
            "hydrofrac: a label has a counterexample\n{self}"
        );
    }

    /// How this report names nodes, channels and payloads: by the `Debug` renderings the
    /// counterexamples' sends carried, else by the facts their runs observed.
    pub fn report_names(&self) -> ReportNames<'_> {
        let cexes: Vec<&Counterexample> =
            self.labels.iter().filter_map(|l| l.verdict.counterexample()).collect();
        let facts: Vec<Fact> = cexes
            .iter()
            .flat_map(|c| counterexample_facts(c, &self.taps))
            .collect();
        let mut names = ReportNames::new(&self.names, &self.channels).with_facts(facts.iter());
        for c in cexes {
            names = names.with_log(&c.record.log);
        }
        names
    }
}

/// The first few of a program's schedule choices, for an exhausted label's verdict line.
pub(crate) fn site_list(sites: &[ChoiceSite]) -> String {
    const SHOWN: usize = 3;
    let mut shown: Vec<String> = sites.iter().take(SHOWN).map(ToString::to_string).collect();
    let more = sites.len().saturating_sub(SHOWN);
    if more > 0 {
        shown.push(format!("and {more} more"));
    }
    shown.join(", ")
}

/// The facts a counterexample's report and diagram can decode payloads with: the missing and
/// divergent facts, and every fact the failing schedule's taps observed.
pub(crate) fn counterexample_facts(cex: &Counterexample, taps: &Labels) -> Vec<Fact> {
    let mut facts: Vec<Fact> = cex.missing.iter().map(|(_, f)| f.clone()).collect();
    facts.extend(cex.divergent.iter().cloned());
    for e in cex.record.log.events() {
        if let Event::Output { port, bytes, .. } = e
            && taps.label_of_port(*port).is_some()
        {
            facts.push(decode_output(bytes));
        }
    }
    facts
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {many}")
    }
}

fn crashes(n: usize) -> String {
    plural(n, "crash", "crashes")
}

/// The verdict line of a label, naming faults with `names`.
pub(crate) fn verdict_line(verdict: &Verdict, names: &ReportNames<'_>) -> String {
    match verdict {
        Verdict::Certified {
            tolerates: Tolerance::All,
        } => "holds under any crashes".to_owned(),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(k),
        } => format!("holds with up to {} (the `max_crashes` cap)", crashes(*k)),
        Verdict::Counterexample(c) => {
            let n = c.crashes();
            let mut line = match c.tolerates {
                _ if c.faults.is_empty() => "breaks without faults".to_owned(),
                Some(k) if c.tolerance_certified => {
                    format!("tolerates {}, breaks at {n}", crashes(k))
                }
                // Schedule choices: fewer crashes were searched, not ruled out.
                Some(k) => format!(
                    "no counterexample with up to {} found, breaks at {n}",
                    crashes(k)
                ),
                None if n == 0 => "breaks under omissions alone".to_owned(),
                None => format!("breaks at {}", crashes(n)),
            };
            if !c.faults.is_empty() {
                line.push_str(": ");
                line.push_str(&names.fault_set(&c.faults));
            }
            if let Some(panic) = &c.panic {
                line.push_str(&format!(" (the run panicked: {panic})"));
            }
            line
        }
        Verdict::Vacuous(reason) => format!("vacuous: {reason}"),
        Verdict::Budget { tolerates } => {
            let mut line = "hypothesis budget spent, not a certificate".to_owned();
            if let Some(k) = tolerates {
                line.push_str(&format!(" (holds with up to {})", crashes(*k)));
            }
            line
        }
        Verdict::Exhausted { tolerates, sites } => {
            let searched = match tolerates {
                Tolerance::All => "no counterexample found".to_owned(),
                Tolerance::UpTo(k) => {
                    format!("no counterexample with up to {} found", crashes(*k))
                }
            };
            format!(
                "{searched} on sampled schedules; not a certificate (schedule choices: {})",
                site_list(sites)
            )
        }
        Verdict::NotChecked(reason) => format!("not checked: {reason}; not a certificate"),
    }
}

impl Results {
    fn write_label(
        &self,
        f: &mut fmt::Formatter<'_>,
        l: &LabelResult,
        names: &ReportNames<'_>,
    ) -> fmt::Result {
        let (indent, detail) = ("  ", "      ");
        let c = &l.plan.collection;
        write!(
            f,
            "{indent}[{}] {} ({}, {})",
            l.index,
            c.display_name(),
            kind_name(&c.collection_kind),
            c.label
        )?;
        let line = verdict_line(&l.verdict, names);
        writeln!(f, ": {line}")?;
        if let Verdict::Counterexample(cx) = &l.verdict {
            let cluster = self.names.label(l.plan.cluster()).unwrap_or("member");
            for (member, fact) in &cx.missing {
                writeln!(
                    f,
                    "{detail}{cluster}@{member} lacks {}",
                    names.abbreviate(fact.to_string())
                )?;
            }
            for fact in &cx.divergent {
                writeln!(
                    f,
                    "{detail}correct members disagree on {} (outside the fact domain)",
                    names.abbreviate(fact.to_string())
                )?;
            }
            if let Some(diagram) = &cx.diagram {
                writeln!(f, "{detail}diagram: {}", diagram.display())?;
            }
        }
        if l.stats.hypotheses > 0 {
            writeln!(
                f,
                "{detail}{}, {}, {:.2?}",
                plural(l.stats.hypotheses, "hypothesis", "hypotheses"),
                plural(l.stats.schedules, "schedule", "schedules"),
                l.stats.wall
            )?;
            if let Some(r) = &l.stats.races {
                writeln!(
                    f,
                    "{detail}races: {} stopped early, {} on one schedule; claim B {}",
                    plural(r.early_stops, "set", "sets"),
                    plural(r.single_schedule_sets, "set", "sets"),
                    if r.claim_b {
                        "certified"
                    } else {
                        "not certified"
                    }
                )?;
            }
        }
        Ok(())
    }
}

/// The report: one block per cluster, one row per label in source names.
impl Display for Results {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self.report_names();
        let mut clusters: Vec<&LocationId> = vec![];
        for l in &self.labels {
            let cluster = l.plan.cluster().root();
            if !clusters.contains(&cluster) {
                clusters.push(cluster);
            }
        }
        for cluster in clusters {
            let in_cluster: Vec<&LabelResult> = self
                .labels
                .iter()
                .filter(|l| l.plan.cluster().root() == cluster)
                .collect();
            match self.names.label(cluster) {
                Some(label) => writeln!(f, "{label}:")?,
                None => writeln!(f, "{cluster:?}:")?,
            }
            for l in &in_cluster {
                self.write_label(f, l, &names)?;
            }
        }
        let legend = names.legend();
        if !legend.is_empty() {
            writeln!(f, "legend:")?;
            for (k, text) in legend {
                writeln!(f, "  ‹{k}› = {text}")?;
            }
        }
        if !self.islands.is_empty() {
            writeln!(
                f,
                "opaque to provenance (execution rule): {}",
                self.islands.join(", ")
            )?;
        }
        Ok(())
    }
}
