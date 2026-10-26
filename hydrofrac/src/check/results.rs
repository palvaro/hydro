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
    /// What the label's report names nodes, channels and payloads by (shared with the other
    /// labels of the same [`Results`]).
    pub(crate) context: Rc<ReportContext>,
}

/// What a label's report needs from the program it was checked in: [`Results::names`],
/// [`Results::channels`] and [`Results::taps`].
pub(crate) struct ReportContext {
    pub(crate) names: NodeNames,
    pub(crate) channels: Vec<ChannelInfo>,
    pub(crate) taps: Labels,
}

impl fmt::Debug for ReportContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReportContext").finish_non_exhaustive()
    }
}

impl LabelResult {
    /// The label's display name: its binding (or operator), file and occurrence.
    pub fn name(&self) -> String {
        label_name(&self.plan)
    }

    /// The counterexample that breaks the label, if one does.
    pub fn counterexample(&self) -> Option<&Counterexample> {
        self.verdict.counterexample()
    }

    /// Whether the label is certified (or vacuous), by the rule of [`Results::holds`]: not a
    /// counterexample, an exhausted budget, a sampled search ([`Verdict::Exhausted`]) or a
    /// label the specification does not check ([`Verdict::NotChecked`]).
    pub fn holds(&self) -> bool {
        !self.verdict.fails()
    }

    /// Panics with the label's report unless it holds (see [`Self::holds`]), saying why: the
    /// counterexample in words and the path of its diagram, a sampled search, a spent budget,
    /// or a label the specification does not check. The label-by-label form of
    /// [`Results::assert_holds`].
    #[track_caller]
    pub fn assert_holds(&self) {
        if let Some(failure) = Failure::first([self]) {
            failure.panic(self);
        }
    }
}

/// Why a set of labels does not hold: what [`Results::assert_holds`] and
/// [`LabelResult::assert_holds`] panic with.
enum Failure<'l> {
    /// Every label that fails is one the specification does not check.
    NotChecked(&'l LabelResult, &'l NotCheckedReason),
    /// No counterexample, but a search sampled schedules.
    Sampled(&'l LabelResult),
    /// A counterexample (the first in `labels`), or else a spent budget.
    Fails(&'l LabelResult),
}

impl<'l> Failure<'l> {
    /// The reason the first of `labels` that fails (in their order) does, preferring the most
    /// actionable: a counterexample over a sampled search, over a spent budget, over a label
    /// that is not checked. `None` if every label holds.
    fn first<I>(labels: I) -> Option<Self>
    where
        I: IntoIterator<Item = &'l LabelResult>,
    {
        let failing: Vec<&LabelResult> = labels.into_iter().filter(|l| !l.holds()).collect();
        if let Some(l) = failing.iter().find(|l| l.verdict.is_counterexample()) {
            return Some(Failure::Fails(l));
        }
        if let Some(l) = failing
            .iter()
            .find(|l| matches!(l.verdict, Verdict::Exhausted { .. }))
        {
            return Some(Failure::Sampled(l));
        }
        if let Some(l) = failing
            .iter()
            .find(|l| matches!(l.verdict, Verdict::Budget { .. }))
        {
            return Some(Failure::Fails(l));
        }
        failing.first().map(|l| match &l.verdict {
            Verdict::NotChecked(reason) => Failure::NotChecked(l, reason),
            _ => Failure::Fails(l),
        })
    }

    /// Panics, naming the failing label and why, followed by `report`.
    #[track_caller]
    fn panic(&self, report: &dyn Display) -> ! {
        match self {
            Failure::NotChecked(l, reason) => panic!(
                "hydrofrac: label `{}` is not checked ({reason}), so it is not certified\n{report}",
                l.name()
            ),
            Failure::Sampled(l) => panic!(
                "hydrofrac: schedules were sampled, so the search of label `{}` is not a \
                 certificate (`assert_no_counterexample` accepts it)\n{report}",
                l.name()
            ),
            Failure::Fails(l) => panic!("hydrofrac: label `{}` does not hold\n{report}", l.name()),
        }
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
        self.labels.iter().all(LabelResult::holds)
    }

    /// Panics with the report unless every label holds (see [`Self::holds`]), naming the first
    /// label that does not (in report order) and why. A sampled search certifies nothing, so
    /// for a program past the check's schedule budget use [`Self::assert_no_counterexample`].
    /// A label that is not checked is not certified: assert the labels that are, one by one
    /// ([`LabelResult::assert_holds`]).
    #[track_caller]
    pub fn assert_holds(&self) {
        if let Some(failure) = Failure::first(self.in_report_order()) {
            failure.panic(self);
        }
    }

    /// The first counterexample in report order (clusters in the order their first label
    /// appears, then labels in order), if any label has one.
    pub fn counterexample(&self) -> Option<&Counterexample> {
        self.in_report_order()
            .into_iter()
            .find_map(LabelResult::counterexample)
    }

    /// The labels in the order the report lists them: by cluster, in the order of each
    /// cluster's first label, then in label order.
    pub fn in_report_order(&self) -> Vec<&LabelResult> {
        let mut clusters: Vec<&LocationId> = vec![];
        for l in &self.labels {
            let cluster = l.plan.cluster().root();
            if !clusters.contains(&cluster) {
                clusters.push(cluster);
            }
        }
        clusters
            .into_iter()
            .flat_map(|c| {
                self.labels
                    .iter()
                    .filter(move |l| l.plan.cluster().root() == c)
            })
            .collect()
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
        let cexes: Vec<&Counterexample> = self
            .labels
            .iter()
            .filter_map(|l| l.verdict.counterexample())
            .collect();
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

/// `items` joined as an English list: `a`, `a and b`, `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [init @ .., last] => format!("{} and {last}", init.join(", ")),
    }
}

/// At most this many facts are listed in a sentence; the rest are counted.
const FACTS_SHOWN: usize = 4;

/// `facts` in words, abbreviated with `names`: `1`, `1 and 2`, `1, 2, 3, 4 and 5 more`.
fn fact_list<'f>(facts: impl IntoIterator<Item = &'f Fact>, names: &ReportNames<'_>) -> String {
    let facts: Vec<&Fact> = facts.into_iter().collect();
    let mut shown: Vec<String> = facts
        .iter()
        .take(FACTS_SHOWN)
        .map(|f| names.abbreviate(f.to_string()))
        .collect();
    let more = facts.len().saturating_sub(FACTS_SHOWN);
    if more > 0 {
        shown.push(format!("{more} more"));
    }
    and_list(&shown)
}

/// What the correct members lack, in words: `Receiver@1 without 1`, `Receiver@0 and
/// Receiver@1 without 1 and 2`, `Receiver@0 without 1, and Receiver@1 without 2`. Members
/// that lack the same facts are named together.
fn missing_in_words(missing: &[(u32, Fact)], cluster: &str, names: &ReportNames<'_>) -> String {
    let mut by_member: Vec<(u32, Vec<&Fact>)> = vec![];
    for (member, fact) in missing {
        match by_member.iter_mut().find(|(m, _)| m == member) {
            Some((_, facts)) => facts.push(fact),
            None => by_member.push((*member, vec![fact])),
        }
    }
    let mut groups: Vec<(Vec<u32>, Vec<&Fact>)> = vec![];
    for (member, facts) in by_member {
        match groups.iter_mut().find(|(_, f)| *f == facts) {
            Some((members, _)) => members.push(member),
            None => groups.push((vec![member], facts)),
        }
    }
    let groups: Vec<String> = groups
        .into_iter()
        .map(|(members, facts)| {
            let members: Vec<String> = members.iter().map(|m| format!("{cluster}@{m}")).collect();
            format!("{} without {}", and_list(&members), fact_list(facts, names))
        })
        .collect();
    match groups.as_slice() {
        [init @ .., last] if !init.is_empty() => format!("{}, and {last}", init.join(", ")),
        _ => groups.join(""),
    }
}

/// What each member `missing` names ends without, as its lane's note in a diagram: `(1, "✗
/// never holds 1 and 2")`, in member order. The facts are in full (the diagram cuts long notes
/// into its own legend), at most [`FACTS_SHOWN`] of them, then a count.
pub(crate) fn missing_notes(missing: &[(u32, Fact)]) -> Vec<(u32, String)> {
    let mut by_member: std::collections::BTreeMap<u32, Vec<String>> = Default::default();
    for (member, fact) in missing {
        by_member.entry(*member).or_default().push(fact.to_string());
    }
    by_member
        .into_iter()
        .map(|(member, facts)| {
            let mut shown: Vec<String> = facts.iter().take(FACTS_SHOWN).cloned().collect();
            let more = facts.len().saturating_sub(FACTS_SHOWN);
            if more > 0 {
                shown.push(format!("{more} more"));
            }
            (member, format!("✗ never holds {}", and_list(&shown)))
        })
        .collect()
}

/// What a certified crash bound rules out, in words: `no single crash`, `no combination of up
/// to 3 crashes`.
fn no_crashes_up_to(k: usize) -> String {
    if k == 1 {
        "no single crash".to_owned()
    } else {
        format!("no combination of up to {k} crashes")
    }
}

/// A counterexample in words: what breaks the label, what it leaves the correct members
/// without, and how many crashes the search ruled out first.
fn counterexample_in_words(c: &Counterexample, cluster: &str, names: &ReportNames<'_>) -> String {
    let (subject, plural) = names
        .fault_set_in_words(&c.faults)
        .unwrap_or_else(|| ("even the failure-free run".to_owned(), false));
    let verb = |one: &str, many: &str| {
        if plural {
            many.to_owned()
        } else {
            one.to_owned()
        }
    };
    let mut effects = vec![];
    if !c.missing.is_empty() {
        effects.push(format!(
            "{} {}",
            verb("leaves", "leave"),
            missing_in_words(&c.missing, cluster, names)
        ));
    }
    if !c.divergent.is_empty() {
        effects.push(format!(
            "{} correct members disagree on {}, which the failure-free run never derived",
            verb("makes", "make"),
            fact_list(&c.divergent, names)
        ));
    }
    let mut sentence = match (&c.panic, effects.is_empty()) {
        (Some(panic), true) => {
            format!("{subject} {} the run panic: {panic}", verb("makes", "make"))
        }
        (_, true) => format!("{subject} {} it", verb("breaks", "break")),
        (panic, false) => {
            let mut s = format!("{subject} {}", effects.join(" and "));
            if let Some(panic) = panic {
                s.push_str(&format!(" (the run panicked: {panic})"));
            }
            s
        }
    };
    sentence.push('.');
    if !c.faults.is_empty() {
        match c.tolerates {
            Some(k) if c.tolerance_certified && k > 0 => {
                let mut cleared = no_crashes_up_to(k);
                cleared[..1].make_ascii_uppercase();
                sentence.push_str(&format!(" {cleared} breaks it."));
            }
            Some(_) if !c.tolerance_certified => sentence.push_str(
                " No counterexample with fewer crashes was found, but schedules were sampled, so \
                 that is not a certificate.",
            ),
            _ => {}
        }
    }
    sentence
}

/// The verdict on a label in words, naming the members of `cluster` (the label's cluster) by
/// it, and faults, payloads and facts with `names`.
pub(crate) fn verdict_line(verdict: &Verdict, cluster: &str, names: &ReportNames<'_>) -> String {
    match verdict {
        Verdict::Certified {
            tolerates: Tolerance::All,
        } => "holds: no number of crashes or lost messages breaks it.".to_owned(),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(0),
        } => "holds when nothing crashes (`max_crashes` is 0, so crashes were not checked)."
            .to_owned(),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(k),
        } => format!(
            "holds: {} breaks it (`max_crashes` is {k}, so more were not checked).",
            no_crashes_up_to(*k)
        ),
        Verdict::Counterexample(c) => {
            format!("broken: {}", counterexample_in_words(c, cluster, names))
        }
        Verdict::Vacuous(reason) => format!("nothing to check ({reason})."),
        Verdict::Budget { tolerates } => {
            let mut line = "not a certificate: the hypothesis budget ran out".to_owned();
            match tolerates {
                Some(0) => line.push_str(" (before then, nothing broke it without crashes)"),
                Some(k) => line.push_str(&format!(
                    " (before then, {} broke it)",
                    no_crashes_up_to(*k)
                )),
                None => {}
            }
            line.push('.');
            line
        }
        Verdict::Exhausted { tolerates, sites } => {
            let searched = match tolerates {
                Tolerance::All => String::new(),
                Tolerance::UpTo(k) => format!("searched up to {}; ", crashes(*k)),
            };
            format!(
                "no counterexample found, but schedules were sampled, so this is not a \
                 certificate ({searched}schedule choices: {}).",
                site_list(sites)
            )
        }
        Verdict::NotChecked(reason) => {
            format!("not checked ({reason}), so this is not a certificate.")
        }
    }
}

/// Writes one label's rows of the report: its verdict in words, the diagram of its
/// counterexample, and what its search cost.
fn write_label(
    f: &mut fmt::Formatter<'_>,
    l: &LabelResult,
    nodes: &NodeNames,
    names: &ReportNames<'_>,
) -> fmt::Result {
    let (indent, detail) = ("  ", "      ");
    let c = &l.plan.collection;
    // Missing facts are at members of the rule's B-side (the label's own cluster, for agreement).
    let b = &l.context.taps.plans[l.context.taps.rules[l.index].b];
    let cluster = nodes.label(b.cluster()).unwrap_or("member");
    writeln!(
        f,
        "{indent}[{}] {} ({}, {}): {}",
        l.index,
        c.display_name(),
        kind_name(&c.collection_kind),
        c.label,
        verdict_line(&l.verdict, cluster, names)
    )?;
    if let Verdict::Counterexample(cx) = &l.verdict
        && let Some(diagram) = &cx.diagram
    {
        writeln!(f, "{detail}diagram: {}", diagram.display())?;
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

/// Writes the header of `cluster`'s block of the report.
fn write_cluster(
    f: &mut fmt::Formatter<'_>,
    cluster: &LocationId,
    nodes: &NodeNames,
) -> fmt::Result {
    match nodes.label(cluster) {
        Some(label) => writeln!(f, "{label}:"),
        None => writeln!(f, "{cluster:?}:"),
    }
}

/// Writes the legend of the payloads `names` abbreviated.
fn write_legend(f: &mut fmt::Formatter<'_>, names: &ReportNames<'_>) -> fmt::Result {
    let legend = names.legend();
    if !legend.is_empty() {
        writeln!(f, "legend:")?;
        for (k, text) in legend {
            writeln!(f, "  ‹{k}› = {text}")?;
        }
    }
    Ok(())
}

/// The report: one block per cluster, one row per label in source names.
impl Display for Results {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self.report_names();
        let mut cluster = None;
        for l in self.in_report_order() {
            let root = l.plan.cluster().root();
            if cluster != Some(root) {
                write_cluster(f, root, &self.names)?;
                cluster = Some(root);
            }
            write_label(f, l, &self.names, &names)?;
        }
        write_legend(f, &names)?;
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

/// The label's part of the report: its cluster, its row, and the legend of the payloads the
/// row abbreviated.
impl Display for LabelResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ctx = &self.context;
        let mut names = ReportNames::new(&ctx.names, &ctx.channels);
        if let Some(c) = self.verdict.counterexample() {
            let facts = counterexample_facts(c, &ctx.taps);
            names = names.with_facts(facts.iter()).with_log(&c.record.log);
        }
        write_cluster(f, self.plan.cluster().root(), &ctx.names)?;
        write_label(f, self, &ctx.names, &names)?;
        write_legend(f, &names)
    }
}
