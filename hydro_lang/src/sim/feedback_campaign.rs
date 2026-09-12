//! Deterministic campaigns for probing feedback-driven work in simulated Hydro programs.
//!
//! The campaign runner deliberately knows nothing about protocols. A typed adapter supplies two
//! synchronous actions: inject the next `n` valid data values, and fire one operational input.
//! The runner owns the experiment: geometric state growth, operational repetitions, quiescence
//! barriers, provenance collection, and witness extraction.
//!
//! Adapters may be derived from existing tests, which are often the best executable specification
//! of valid values and reachable inputs. They must not supply expected labels, protocol phases, or
//! conclusions. This keeps the campaign deterministic and makes its conclusions a function of the
//! emitted provenance records rather than of protocol-specific test assertions.

use std::collections::{BTreeMap, BTreeSet};

use super::provenance::{
    Channel, Classified, EmissionPointKind, EmissionRecord, Label, Tag, classify, take_emissions,
};
use super::quiesce;

/// Direction of a simulator boundary port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BoundaryDirection {
    /// Values injected into the Hydro graph.
    Input,
    /// Values emitted from the Hydro graph.
    Output,
}

/// Whether an input supplies application data or controls execution timing/operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InputRole {
    /// Ordinary application input.
    Data,
    /// Timer, election, service pulse, or other operational input.
    Operational,
}

/// One externally visible simulator port discovered from Hydro IR.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundaryPort {
    /// Stable numeric external port ID.
    pub port: usize,
    /// Input or output.
    pub direction: BoundaryDirection,
    /// Data or operational for inputs; `None` for outputs.
    pub role: Option<InputRole>,
    /// Debug representation of the process/cluster location.
    pub location: String,
    /// Debug representation of the boundary codec or collection element type.
    pub type_name: String,
    /// Whether this is a cluster-many boundary rather than a process boundary.
    pub many: bool,
}

/// A feedback-cycle sink discovered from Hydro IR.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoundaryCycle {
    /// Stable numeric cycle ID.
    pub cycle: usize,
    /// Location containing the cycle.
    pub location: String,
    /// Debug representation of the carried collection shape.
    pub collection: String,
}

/// Read-only inventory of graph boundaries relevant to a feedback campaign.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundaryManifest {
    /// External simulator inputs and outputs, sorted deterministically.
    pub ports: Vec<BoundaryPort>,
    /// Cycle sinks, sorted deterministically.
    pub cycles: Vec<BoundaryCycle>,
}

impl BoundaryManifest {
    /// Input port IDs which an adapter must cover with a typed injector or explicitly waive.
    pub fn input_ports(&self) -> impl Iterator<Item = &BoundaryPort> {
        self.ports
            .iter()
            .filter(|port| port.direction == BoundaryDirection::Input)
    }

    /// Panics if a deterministic adapter did not account for every discovered input port.
    pub fn assert_inputs_covered(&self, covered_ports: impl IntoIterator<Item = usize>) {
        let covered: BTreeSet<_> = covered_ports.into_iter().collect();
        let missing: Vec<_> = self
            .input_ports()
            .filter(|port| !covered.contains(&port.port))
            .map(|port| (port.port, port.role, port.type_name.clone()))
            .collect();
        assert!(missing.is_empty(), "campaign adapter omitted input ports: {missing:?}");
    }
}

/// Fixed workload parameters for a retained-state feedback campaign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignConfig {
    /// Cumulative numbers of data values admitted before each probe. Values must be strictly
    /// increasing. Use `[0]` for a flow with no data input, such as a pure heartbeat.
    pub scales: Vec<usize>,
    /// Maximum number of times to fire the selected operational input at every scale.
    pub operational_repetitions: usize,
    /// Stop a scale after this many consecutive operational epochs have the same physical
    /// emission signature. Two detects both persistent fixed traffic and completed drainage
    /// (two empty epochs) without requiring protocol-specific stopping logic.
    pub stop_after_stable_repetitions: usize,
}

impl CampaignConfig {
    /// Validates campaign parameters before any input is sent.
    pub fn validate(&self) {
        assert!(!self.scales.is_empty(), "a feedback campaign needs at least one scale");
        assert!(
            self.scales.windows(2).all(|w| w[0] < w[1]),
            "campaign scales must be strictly increasing: {:?}",
            self.scales
        );
        assert!(
            self.operational_repetitions > 0,
            "a feedback campaign needs at least one operational repetition"
        );
        assert!(
            self.stop_after_stable_repetitions > 0,
            "a feedback campaign needs a positive stability threshold"
        );
    }
}

/// The action whose emissions form an observation epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CampaignAction {
    /// New data was admitted to grow the retained state from the previous scale.
    GrowData,
    /// The selected operational input was fired once.
    FireOperational,
}

/// Generic measurements for one quiescence-delimited epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochObservation {
    /// Cumulative data scale at the start of the operational probe.
    pub scale: usize,
    /// What the runner did before waiting for quiescence.
    pub action: CampaignAction,
    /// Zero-based repetition for an operational action; `None` for data growth.
    pub repetition: Option<usize>,
    /// Every record emitted during the epoch, including receives and cycle carries.
    pub records: Vec<EmissionRecord>,
    /// Classified physical emissions (receives and cycle carries excluded).
    pub classified: Vec<Classified>,
    /// Number of network sends and simulator outputs in the epoch.
    pub physical_emissions: usize,
    /// Serialized bytes in those physical emissions.
    pub bytes: usize,
    /// Distinct `(channel, payload hash)` pairs in those physical emissions.
    pub distinct_payloads: usize,
    /// Distinct data source events represented by those physical emissions.
    pub data_sources: BTreeSet<Tag>,
    /// Label counts over physical emissions.
    pub labels: BTreeMap<Label, usize>,
}

impl EpochObservation {
    fn from_epoch(
        scale: usize,
        action: CampaignAction,
        repetition: Option<usize>,
        records: Vec<EmissionRecord>,
        classified: Vec<Classified>,
    ) -> Self {
        let physical: Vec<_> = records
            .iter()
            .filter(|r| matches!(r.kind, EmissionPointKind::Network | EmissionPointKind::Output))
            .collect();
        let bytes = physical.iter().map(|r| r.bytes).sum();
        let distinct_payloads = physical
            .iter()
            .map(|r| (r.channel(), r.payload_hash))
            .collect::<BTreeSet<_>>()
            .len();
        let data_sources = physical
            .iter()
            .flat_map(|r| r.data_tags().copied())
            .collect();
        let mut labels = BTreeMap::new();
        for c in &classified {
            *labels.entry(c.label).or_default() += 1;
        }
        Self {
            scale,
            action,
            repetition,
            physical_emissions: physical.len(),
            bytes,
            distinct_payloads,
            data_sources,
            labels,
            records,
            classified,
        }
    }

    /// A deterministic signature of physical work in the epoch. Source tags are deliberately not
    /// included: recurrence asks whether the same payload crosses the same channel again even
    /// though a later operational event has a different tag.
    fn physical_signature(&self) -> BTreeMap<(EmissionPointKind, Channel, u64), usize> {
        let mut signature = BTreeMap::new();
        for record in self.records.iter().filter(|r| {
            matches!(r.kind, EmissionPointKind::Network | EmissionPointKind::Output)
        }) {
            *signature
                .entry((record.kind, record.channel(), record.payload_hash))
                .or_default() += 1;
        }
        signature
    }

    /// Count of one classification label in this epoch.
    pub fn label_count(&self, label: Label) -> usize {
        self.labels.get(&label).copied().unwrap_or(0)
    }
}

/// A positive, replayable observation made by the campaign. These are bounded witnesses, not
/// safety or metastability verdicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CampaignWitness {
    /// An operational firing emitted work with no application-data ancestry.
    OperationalOnlyWork {
        /// Cumulative data population when the event fired.
        scale: usize,
        /// Zero-based operational repetition at this scale.
        repetition: usize,
        /// Operational-only physical emissions observed.
        emissions: usize,
    },
    /// An operational firing re-emitted application ancestry previously sent on the channel.
    ReplayedAncestry {
        /// Cumulative data population when the event fired.
        scale: usize,
        /// Zero-based operational repetition at this scale.
        repetition: usize,
        /// Physical emissions classified as replay.
        emissions: usize,
    },
    /// The same channel and payload appeared in two operational epochs at one scale.
    RecurringPayload {
        /// Cumulative data population for both occurrences.
        scale: usize,
        /// Sender-recipient channel carrying the repeated payload.
        channel: Channel,
        /// Hash of the uninstrumented serialized payload.
        payload_hash: u64,
        /// First operational repetition carrying it.
        first_repetition: usize,
        /// Later operational repetition carrying it again.
        later_repetition: usize,
    },
    /// Data-admission work increased between adjacent geometric input scales.
    InputWorkScaled {
        /// Smaller cumulative data population.
        lower_scale: usize,
        /// Physical emissions while growing to the smaller scale.
        lower_emissions: usize,
        /// Serialized bytes while growing to the smaller scale.
        lower_bytes: usize,
        /// Larger cumulative data population.
        higher_scale: usize,
        /// Physical emissions while growing to the larger scale.
        higher_emissions: usize,
        /// Serialized bytes while growing to the larger scale.
        higher_bytes: usize,
    },
    /// Work caused by the same operational repetition increased as the runner grew data state.
    WorkScaledWithData {
        /// Operational repetition compared across scales.
        repetition: usize,
        /// Smaller cumulative data population.
        lower_scale: usize,
        /// Physical emissions at the smaller scale.
        lower_emissions: usize,
        /// Serialized bytes at the smaller scale.
        lower_bytes: usize,
        /// Larger cumulative data population.
        higher_scale: usize,
        /// Physical emissions at the larger scale.
        higher_emissions: usize,
        /// Serialized bytes at the larger scale.
        higher_bytes: usize,
    },
}

/// Complete deterministic output of one feedback campaign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignReport {
    /// Parameters that generated the observations.
    pub config: CampaignConfig,
    /// Quiescence-delimited observations, in execution order.
    pub epochs: Vec<EpochObservation>,
    /// Positive witnesses derived mechanically from the observations.
    pub witnesses: Vec<CampaignWitness>,
}

impl CampaignReport {
    /// Renders a deterministic, protocol-independent summary suitable for logs and regression
    /// artifacts. It reports the search envelope and positive witnesses; absence is always scoped
    /// to that envelope.
    pub fn render_summary(&self) -> String {
        let operational_epochs = self
            .epochs
            .iter()
            .filter(|e| e.action == CampaignAction::FireOperational)
            .count();
        format!(
            "feedback campaign: scales={:?}, max_operational_repetitions={}, stability_threshold={}, epochs={}, operational_epochs={}\nobserved_replay={}\nobserved_input_scaling={}\nobserved_operational_scaling={}\nwitnesses={:#?}\n",
            self.config.scales,
            self.config.operational_repetitions,
            self.config.stop_after_stable_repetitions,
            self.epochs.len(),
            operational_epochs,
            self.observed_replay(),
            self.observed_input_scaling(),
            self.observed_operational_scaling(),
            self.witnesses,
        )
    }

    /// Whether any operational epoch replayed already-sent application ancestry.
    pub fn observed_replay(&self) -> bool {
        self.witnesses
            .iter()
            .any(|w| matches!(w, CampaignWitness::ReplayedAncestry { .. }))
    }

    /// Whether data-admission work increased across two tested scales.
    pub fn observed_input_scaling(&self) -> bool {
        self.witnesses
            .iter()
            .any(|w| matches!(w, CampaignWitness::InputWorkScaled { .. }))
    }

    /// Whether physical work from the selected operational event increased across two tested data
    /// scales.
    pub fn observed_operational_scaling(&self) -> bool {
        self.witnesses
            .iter()
            .any(|w| matches!(w, CampaignWitness::WorkScaledWithData { .. }))
    }
}

/// Runs a deterministic retained-state campaign inside a simulation compiled with provenance.
///
/// `inject_data(start, count)` must inject `count` valid values beginning at deterministic index
/// `start`. It is the only type-specific value-generation hook. `fire_operational(repetition)`
/// must fire one selected operational input. Neither callback controls phase ordering, scale,
/// quiescence, analysis, or expected results.
///
/// The campaign grows one simulation instance cumulatively. At every configured scale it waits
/// for data-driven work to quiesce, then fires the selected operational input the configured
/// number of times, waiting for quiescence after every firing. This intentionally tests both
/// large retained states and recurrence. Independent control/trigger instances are a later
/// campaign dimension.
pub async fn run_feedback_campaign(
    config: CampaignConfig,
    mut inject_data: impl FnMut(usize, usize),
    mut fire_operational: impl FnMut(usize),
) -> CampaignReport {
    config.validate();
    let _ = take_emissions();
    let mut history = Vec::new();
    let mut epochs = Vec::new();
    let mut admitted = 0;

    for &scale in &config.scales {
        assert!(
            scale >= admitted,
            "campaign scale moved backwards from {admitted} to {scale}"
        );
        let delta = scale - admitted;
        if delta > 0 {
            inject_data(admitted, delta);
            quiesce().await;
            let records = take_emissions();
            let before = classify(&history, false).len();
            history.extend(records.iter().cloned());
            let classified = classify(&history, false).into_iter().skip(before).collect();
            epochs.push(EpochObservation::from_epoch(
                scale,
                CampaignAction::GrowData,
                None,
                records,
                classified,
            ));
            admitted = scale;
        }

        let mut stable_repetitions = 0;
        let mut previous_signature = None;
        for repetition in 0..config.operational_repetitions {
            fire_operational(repetition);
            quiesce().await;
            let records = take_emissions();
            let before = classify(&history, false).len();
            history.extend(records.iter().cloned());
            let classified = classify(&history, false).into_iter().skip(before).collect();
            let observation = EpochObservation::from_epoch(
                scale,
                CampaignAction::FireOperational,
                Some(repetition),
                records,
                classified,
            );
            let signature = observation.physical_signature();
            if previous_signature.as_ref() == Some(&signature) {
                stable_repetitions += 1;
            } else {
                stable_repetitions = 1;
            }
            previous_signature = Some(signature);
            epochs.push(observation);
            if stable_repetitions >= config.stop_after_stable_repetitions {
                break;
            }
        }
    }

    let witnesses = derive_witnesses(&epochs);
    CampaignReport {
        config,
        epochs,
        witnesses,
    }
}

fn derive_witnesses(epochs: &[EpochObservation]) -> Vec<CampaignWitness> {
    let mut out = Vec::new();
    let operational: Vec<_> = epochs
        .iter()
        .filter(|e| e.action == CampaignAction::FireOperational)
        .collect();

    for epoch in &operational {
        let repetition = epoch.repetition.expect("operational epochs have repetitions");
        let fixed = epoch.label_count(Label::FixedOperational);
        if fixed > 0 {
            out.push(CampaignWitness::OperationalOnlyWork {
                scale: epoch.scale,
                repetition,
                emissions: fixed,
            });
        }
        let replayed = epoch.label_count(Label::Reactivated);
        if replayed > 0 {
            out.push(CampaignWitness::ReplayedAncestry {
                scale: epoch.scale,
                repetition,
                emissions: replayed,
            });
        }
    }

    // Recurrence is content identity across repeated operational firings at one data scale.
    let mut first_payload: BTreeMap<(usize, Channel, u64), usize> = BTreeMap::new();
    for epoch in &operational {
        let repetition = epoch.repetition.unwrap();
        for record in epoch.records.iter().filter(|r| {
            matches!(r.kind, EmissionPointKind::Network | EmissionPointKind::Output)
        }) {
            let key = (epoch.scale, record.channel(), record.payload_hash);
            if let Some(&first_repetition) = first_payload.get(&key) {
                if first_repetition != repetition {
                    let witness = CampaignWitness::RecurringPayload {
                        scale: epoch.scale,
                        channel: record.channel(),
                        payload_hash: record.payload_hash,
                        first_repetition,
                        later_repetition: repetition,
                    };
                    if !out.contains(&witness) {
                        out.push(witness);
                    }
                }
            } else {
                first_payload.insert(key, repetition);
            }
        }
    }

    // Compare adjacent data-growth epochs. This records ordinary input scaling separately from
    // operational scaling: productive recursion can grow here without replaying old ancestry.
    let growth: Vec<_> = epochs
        .iter()
        .filter(|e| e.action == CampaignAction::GrowData)
        .collect();
    for pair in growth.windows(2) {
        let [lower, higher] = pair else { unreachable!() };
        if higher.physical_emissions > lower.physical_emissions || higher.bytes > lower.bytes {
            out.push(CampaignWitness::InputWorkScaled {
                lower_scale: lower.scale,
                lower_emissions: lower.physical_emissions,
                lower_bytes: lower.bytes,
                higher_scale: higher.scale,
                higher_emissions: higher.physical_emissions,
                higher_bytes: higher.bytes,
            });
        }
    }

    // Compare the same operational repetition across adjacent data scales. An increase in either
    // messages or bytes is reported; the runner does not decide whether the scaling is harmful.
    let mut by_repetition: BTreeMap<usize, Vec<&EpochObservation>> = BTreeMap::new();
    for epoch in operational {
        by_repetition
            .entry(epoch.repetition.unwrap())
            .or_default()
            .push(epoch);
    }
    for (repetition, mut observations) in by_repetition {
        observations.sort_by_key(|e| e.scale);
        for pair in observations.windows(2) {
            let [lower, higher] = pair else { unreachable!() };
            if higher.physical_emissions > lower.physical_emissions || higher.bytes > lower.bytes {
                out.push(CampaignWitness::WorkScaledWithData {
                    repetition,
                    lower_scale: lower.scale,
                    lower_emissions: lower.physical_emissions,
                    lower_bytes: lower.bytes,
                    higher_scale: higher.scale,
                    higher_emissions: higher.physical_emissions,
                    higher_bytes: higher.bytes,
                });
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_fixed_campaign_algebra() {
        CampaignConfig {
            scales: vec![1, 4, 16],
            operational_repetitions: 3,
            stop_after_stable_repetitions: 2,
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn rejects_scenario_specific_or_ambiguous_scale_order() {
        CampaignConfig {
            scales: vec![1, 4, 4],
            operational_repetitions: 3,
            stop_after_stable_repetitions: 2,
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "omitted input ports")]
    fn manifest_rejects_silently_omitted_inputs() {
        let manifest = BoundaryManifest {
            ports: vec![
                BoundaryPort {
                    port: 1,
                    direction: BoundaryDirection::Input,
                    role: Some(InputRole::Data),
                    location: "Process(1)".into(),
                    type_name: "Request".into(),
                    many: false,
                },
                BoundaryPort {
                    port: 2,
                    direction: BoundaryDirection::Input,
                    role: Some(InputRole::Operational),
                    location: "Process(1)".into(),
                    type_name: "()".into(),
                    many: false,
                },
            ],
            cycles: vec![],
        };
        manifest.assert_inputs_covered([1]);
    }
}
