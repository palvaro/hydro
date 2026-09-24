//! Deterministic campaigns for probing feedback-driven work in simulated Hydro programs.
//!
//! The campaign runner deliberately knows nothing about protocols. A typed adapter supplies two
//! synchronous actions: inject the next `n` valid data values, and fire one operational input.
//! The runner owns the experiment: geometric state growth, operational repetitions, quiescence
//! barriers, provenance collection, and witness extraction.
//!
//! Adapters may be derived from existing tests, which are often the best executable specification
//! of valid values and reachable inputs. They must not supply expected labels, protocol phases, or
//! interpretations. This keeps the campaign deterministic and makes its evidence a function of the
//! emitted provenance records rather than of protocol-specific test assertions.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::RefUnwindSafe;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::compiled::CompiledSim;
use super::provenance::{
    Channel, Classified, EmissionPointKind, EmissionRecord, Label, Tag, TagSet, classify,
    take_emissions,
};
use super::{SimClusterSender, SimSender, quiesce};
use crate::live_collections::stream::{ExactlyOnce, Ordering};

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
        assert!(
            missing.is_empty(),
            "campaign adapter omitted input ports: {missing:?}"
        );
    }
}

/// Deterministic typed-input registry consumed by [`run_evidence_matrix`]. Every discovered input
/// must be registered. Registration supplies serialization-safe values and legal member targets;
/// the matrix runner alone chooses port combinations, scales, action order, repetitions, and analysis.
pub struct InputRegistry<'a> {
    inputs: BTreeMap<usize, RegisteredInput<'a>>,
}

struct RegisteredInput<'a> {
    role: InputRole,
    targets: Vec<Option<u32>>,
    send_index: Box<dyn Fn(Option<u32>, usize) + RefUnwindSafe + 'a>,
}

impl<'a> Default for InputRegistry<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> InputRegistry<'a> {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            inputs: BTreeMap::new(),
        }
    }

    /// Registers a process data input and deterministic value generator.
    pub fn process_data<T, O>(
        &mut self,
        sender: &'a SimSender<T, O, ExactlyOnce>,
        generator: impl Fn(usize) -> T + RefUnwindSafe + 'a,
    ) where
        T: Serialize + DeserializeOwned + RefUnwindSafe,
        O: Ordering + RefUnwindSafe,
    {
        let port = sender.port_id();
        self.insert(
            port,
            RegisteredInput {
                role: InputRole::Data,
                targets: vec![None],
                send_index: Box::new(move |target, index| {
                    assert_eq!(target, None);
                    sender.send_indexed(generator(index));
                }),
            },
        );
    }

    /// Registers a process operational input. The value is fixed because operational events carry
    /// no application information; use a data input if values are semantically meaningful.
    pub fn process_operational<T, O>(&mut self, sender: &'a SimSender<T, O, ExactlyOnce>, value: T)
    where
        T: Serialize + DeserializeOwned + Clone + RefUnwindSafe + 'a,
        O: Ordering + RefUnwindSafe,
    {
        let port = sender.port_id();
        self.insert(
            port,
            RegisteredInput {
                role: InputRole::Operational,
                targets: vec![None],
                send_index: Box::new(move |target, _| {
                    assert_eq!(target, None);
                    sender.send_indexed(value.clone());
                }),
            },
        );
    }

    /// Registers a cluster data input, deterministic generator, and every legal target member.
    pub fn cluster_data<T, O>(
        &mut self,
        sender: &'a SimClusterSender<T, O, ExactlyOnce>,
        members: impl IntoIterator<Item = u32>,
        generator: impl Fn(usize) -> T + RefUnwindSafe + 'a,
    ) where
        T: Serialize + DeserializeOwned + RefUnwindSafe,
        O: Ordering + RefUnwindSafe,
    {
        let port = sender.port_id();
        let targets = members.into_iter().map(Some).collect();
        self.insert(
            port,
            RegisteredInput {
                role: InputRole::Data,
                targets,
                send_index: Box::new(move |target, index| {
                    sender.send_indexed(target.expect("cluster input target"), generator(index));
                }),
            },
        );
    }

    /// Registers a cluster operational input and every legal target member.
    pub fn cluster_operational<T, O>(
        &mut self,
        sender: &'a SimClusterSender<T, O, ExactlyOnce>,
        members: impl IntoIterator<Item = u32>,
        value: T,
    ) where
        T: Serialize + DeserializeOwned + Clone + RefUnwindSafe + 'a,
        O: Ordering + RefUnwindSafe,
    {
        let port = sender.port_id();
        let targets = members.into_iter().map(Some).collect();
        self.insert(
            port,
            RegisteredInput {
                role: InputRole::Operational,
                targets,
                send_index: Box::new(move |target, _| {
                    sender.send_indexed(target.expect("cluster operational target"), value.clone());
                }),
            },
        );
    }

    fn insert(&mut self, port: usize, input: RegisteredInput<'a>) {
        assert!(
            !input.targets.is_empty(),
            "input port {port} has no legal targets"
        );
        assert!(
            self.inputs.insert(port, input).is_none(),
            "input port {port} registered twice"
        );
    }

    fn validate(&self, manifest: &BoundaryManifest) {
        manifest.assert_inputs_covered(self.inputs.keys().copied());
        let extra: Vec<_> = self
            .inputs
            .keys()
            .filter(|port| !manifest.input_ports().any(|p| p.port == **port))
            .copied()
            .collect();
        assert!(
            extra.is_empty(),
            "registered ports absent from manifest: {extra:?}"
        );
        for port in manifest.input_ports() {
            let registered = &self.inputs[&port.port];
            assert_eq!(
                port.role,
                Some(registered.role),
                "input role mismatch for port {}",
                port.port
            );
        }
    }
}

/// Fixed workload parameters for a retained-state feedback campaign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignConfig {
    /// Cumulative numbers of data values admitted before each probe. Values must be strictly
    /// increasing. Use `[0]` for a flow with no data input, such as a pure heartbeat.
    pub scales: Vec<usize>,
    /// Deterministic scheduler seeds. The full port/target/scale matrix is repeated for each seed.
    pub schedule_seeds: Vec<u64>,
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
        assert!(
            !self.scales.is_empty(),
            "a feedback campaign needs at least one scale"
        );
        assert!(
            self.scales.windows(2).all(|w| w[0] < w[1]),
            "campaign scales must be strictly increasing: {:?}",
            self.scales
        );
        assert!(
            !self.schedule_seeds.is_empty(),
            "a feedback campaign needs at least one deterministic schedule seed"
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
    /// The selected operational input(s) were fired once.
    FireOperational,
}

/// A node identity: root location rendering plus cluster member (`None` for a process). Matches
/// `(source, member)` / `(destination, recipient)` on an [`EmissionRecord`] and the manifest's
/// port location plus the fired member target.
pub type Node = (String, Option<u32>);

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
    /// Operational source tags that appear in this epoch and in no earlier record of the
    /// campaign: the events created by this epoch's firing. Coarse state can carry *old*
    /// operational tags forward; those are excluded here.
    pub fresh_operational: BTreeSet<Tag>,
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

/// Raw per-emission shape used by the stopping rule and the terminal-tail columns. Contains no
/// dominance judgement: serialized size, the literal data-ancestry set, whether any operational
/// tag is present, whether lineage is coarse, and — only when the emission carries application
/// ancestry — the payload hash. Operational tag identity necessarily changes on every firing and
/// is not part of the shape. Payload identity is dropped for data-free emissions because their
/// content can only vary by operational bookkeeping (sequence numbers); it is kept for
/// data-bearing emissions because it distinguishes "the same retained item again" from "the next
/// item out of a queue", which repeated firings must not conflate.
pub type EmissionShape = (usize, TagSet, bool, bool, u64);

fn shape_of(record: &EmissionRecord) -> EmissionShape {
    let data: TagSet = record.data_tags().copied().collect();
    let payload = if data.is_empty() {
        0
    } else {
        record.payload_hash
    };
    (
        record.bytes,
        data,
        record.operational_tags().next().is_some(),
        record.coarse,
        payload,
    )
}

/// Number of consecutive identical epochs required before a scale stops. A stock of `scale`
/// admitted obligations released one per epoch would look identical for `scale` epochs, so the
/// window grows with the scale; the configured minimum applies at scale 0.
fn stable_window(config: &CampaignConfig, scale: usize) -> usize {
    config.stop_after_stable_repetitions.max(scale + 1)
}

fn is_physical(record: &EmissionRecord) -> bool {
    matches!(
        record.kind,
        EmissionPointKind::Network | EmissionPointKind::Output
    )
}

impl EpochObservation {
    fn from_epoch(
        scale: usize,
        action: CampaignAction,
        repetition: Option<usize>,
        records: Vec<EmissionRecord>,
        classified: Vec<Classified>,
        fresh_operational: BTreeSet<Tag>,
    ) -> Self {
        let physical: Vec<_> = records.iter().filter(|r| is_physical(r)).collect();
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
            fresh_operational,
        }
    }

    /// Multiset of physical emission shapes per edge/channel. Two epochs with equal signatures
    /// emitted the same sizes carrying the same literal data ancestry on the same channels.
    fn physical_signature(&self) -> BTreeMap<(EmissionPointKind, Channel, EmissionShape), usize> {
        let mut signature = BTreeMap::new();
        for record in self.records.iter().filter(|r| is_physical(r)) {
            *signature
                .entry((record.kind, record.channel(), shape_of(record)))
                .or_default() += 1;
        }
        signature
    }

    /// Count of one internal ancestry-comparison category in this epoch.
    pub fn label_count(&self, label: Label) -> usize {
        self.labels.get(&label).copied().unwrap_or(0)
    }
}

/// Complete deterministic output of one feedback campaign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignReport {
    /// Parameters that generated the observations.
    pub config: CampaignConfig,
    /// Quiescence-delimited observations, in execution order.
    pub epochs: Vec<EpochObservation>,
    /// Input port → (root location, is cluster port), from the boundary manifest. Used to map
    /// an operational tag back to the node whose input was fired. Empty when unknown.
    pub port_nodes: BTreeMap<u32, (String, bool)>,
}

/// Identity of one physical emission edge. Every experiment run is reduced to rows with this key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvidenceEdge {
    /// Network or simulator-output boundary.
    pub kind: EmissionPointKind,
    /// Stable compiler-assigned emission-point ID.
    pub point: u32,
    /// Human-readable emission-point name, used only for reporting.
    pub name: String,
    /// Sender, destination, and recipient identifying the physical channel.
    pub channel: Channel,
}

impl EvidenceEdge {
    /// Key without the compiler-assigned point ID, for comparing two compilations of related
    /// programs (a mutation or refactor may renumber points but keeps names and channels).
    pub fn stable_key(&self) -> (EmissionPointKind, String, Channel) {
        (self.kind, self.name.clone(), self.channel.clone())
    }
}

/// Mechanically observed terminal signature of an edge after repeated operational firings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TerminalBehavior {
    /// Every tested scale ended with the configured number of empty epochs on this edge.
    EmptyTail,
    /// At least one tested scale ended with the same non-empty shape multiset repeatedly.
    RepeatedNonEmptyTail,
    /// The event budget ended without one common tail pattern, or scales had different patterns.
    Unresolved,
    /// The edge exists in this program (it emitted in some other case) but emitted nothing at all
    /// in this case. Recorded explicitly so a zero-work case is not confused with a missing case.
    NoEmission,
}

/// Identical evidence columns computed for every physical edge of every run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEvidence {
    /// Edge this row describes.
    pub edge: EvidenceEdge,
    /// Physical emissions caused while adding data across all tested scales.
    pub data_phase_emissions: usize,
    /// Bytes emitted while adding data across all tested scales.
    pub data_phase_bytes: usize,
    /// First-operational-firing `(scale, messages, bytes)` curve used by the scaling rule.
    pub operational_curve: Vec<(usize, usize, usize)>,
    /// Physical emissions caused by operational firings across all tested scales.
    pub operational_emissions: usize,
    /// Bytes emitted by operational firings across all tested scales.
    pub operational_bytes: usize,
    /// Emissions carrying no lineage at all (constants, membership).
    pub no_lineage: usize,
    /// Emissions carrying no application-data ancestry and at least one operational tag.
    pub no_data_ancestry: usize,
    /// Emissions whose data ancestry was not contained in one earlier emission on the channel.
    pub undominated_data_ancestry: usize,
    /// Emissions whose data ancestry was contained in an earlier emission and also carried an
    /// operational tag.
    pub operational_dominated_ancestry: usize,
    /// Emissions whose data ancestry was contained in an earlier emission without an operational
    /// tag.
    pub data_dominated_ancestry: usize,
    /// Emissions whose lineage was propagated exactly through supported structured operators.
    pub exact_lineage: usize,
    /// Emissions whose lineage includes conservative ancestry from opaque referenced state.
    pub coarse_lineage: usize,
    /// Distinct payload/channel pairs repeated in different operational epochs at one scale.
    pub recurring_payloads: usize,
    /// Whether first-firing work increased across adjacent tested data scales.
    pub operational_work_scales: bool,
    /// Operational-epoch emissions whose lineage contains an operational event created by that
    /// epoch's firing.
    pub fired_tag_emissions: usize,
    /// Of those, emissions sent by a node other than the one whose input was fired: work another
    /// node performed as a consequence of the firing.
    pub other_node_emissions: usize,
    /// `other_node_emissions` restricted to exact lineage. Coarse lineage can attach a fired tag to
    /// an emission merely because opaque state at the emitter had absorbed it.
    pub other_node_emissions_exact: usize,
    /// Of those, emissions addressed back to the node whose input was fired.
    pub returned_emissions: usize,
    /// Terminal behavior at each tested scale, before the cross-scale summary.
    pub terminal_by_scale: Vec<(usize, TerminalBehavior)>,
    /// Common terminal behavior across the tested operational sequences.
    pub terminal: TerminalBehavior,
}

impl EdgeEvidence {
    /// A row for an edge that emitted nothing in this case.
    pub fn absent(edge: EvidenceEdge) -> Self {
        Self {
            edge,
            data_phase_emissions: 0,
            data_phase_bytes: 0,
            operational_curve: Vec::new(),
            operational_emissions: 0,
            operational_bytes: 0,
            no_lineage: 0,
            no_data_ancestry: 0,
            undominated_data_ancestry: 0,
            operational_dominated_ancestry: 0,
            data_dominated_ancestry: 0,
            exact_lineage: 0,
            coarse_lineage: 0,
            recurring_payloads: 0,
            operational_work_scales: false,
            fired_tag_emissions: 0,
            other_node_emissions: 0,
            other_node_emissions_exact: 0,
            returned_emissions: 0,
            terminal_by_scale: Vec::new(),
            terminal: TerminalBehavior::NoEmission,
        }
    }

    /// The evidence schema as `(column, rendered value)`. Every renderer and comparison uses this
    /// single list, so the schema cannot drift between outputs.
    pub fn fields(&self) -> Vec<(&'static str, String)> {
        vec![
            ("data_msgs", self.data_phase_emissions.to_string()),
            ("data_bytes", self.data_phase_bytes.to_string()),
            (
                "op_curve(scale:msgs:bytes)",
                format!("{:?}", self.operational_curve),
            ),
            ("op_msgs", self.operational_emissions.to_string()),
            ("op_bytes", self.operational_bytes.to_string()),
            ("no_lineage", self.no_lineage.to_string()),
            ("no_data_ancestry", self.no_data_ancestry.to_string()),
            (
                "undominated_data_ancestry",
                self.undominated_data_ancestry.to_string(),
            ),
            (
                "operational_dominated_ancestry",
                self.operational_dominated_ancestry.to_string(),
            ),
            (
                "data_dominated_ancestry",
                self.data_dominated_ancestry.to_string(),
            ),
            ("exact_lineage", self.exact_lineage.to_string()),
            ("coarse_lineage", self.coarse_lineage.to_string()),
            ("recurring_payloads", self.recurring_payloads.to_string()),
            ("op_scales", self.operational_work_scales.to_string()),
            ("fired_tag_msgs", self.fired_tag_emissions.to_string()),
            ("other_node_msgs", self.other_node_emissions.to_string()),
            (
                "other_node_msgs_exact",
                self.other_node_emissions_exact.to_string(),
            ),
            ("returned_msgs", self.returned_emissions.to_string()),
            ("terminal_by_scale", format!("{:?}", self.terminal_by_scale)),
            ("terminal", format!("{:?}", self.terminal)),
        ]
    }

    /// Column names in schema order.
    pub fn column_names() -> Vec<&'static str> {
        Self::absent(EvidenceEdge {
            kind: EmissionPointKind::Output,
            point: 0,
            name: String::new(),
            channel: (None, String::new(), None),
        })
        .fields()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
    }
}

impl CampaignReport {
    fn tag_node(&self, tag: &Tag) -> Option<Node> {
        self.port_nodes.get(&tag.port).map(|(location, many)| {
            (
                location.clone(),
                if *many { Some(tag.member) } else { None },
            )
        })
    }

    /// Reduces the run to one uniform evidence row per physical edge and applies the same
    /// mechanical reductions to every row.
    pub fn edge_evidence(&self) -> Vec<EdgeEvidence> {
        #[derive(Default)]
        struct Acc {
            data_phase_emissions: usize,
            data_phase_bytes: usize,
            operational_emissions: usize,
            operational_bytes: usize,
            no_lineage: usize,
            no_data_ancestry: usize,
            undominated_data_ancestry: usize,
            operational_dominated_ancestry: usize,
            data_dominated_ancestry: usize,
            exact_lineage: usize,
            coarse_lineage: usize,
            fired_tag_emissions: usize,
            other_node_emissions: usize,
            other_node_emissions_exact: usize,
            returned_emissions: usize,
            payload_epochs: BTreeMap<(usize, u64), BTreeSet<usize>>,
            by_scale_first: BTreeMap<usize, (usize, usize)>,
            final_signatures: BTreeMap<usize, Vec<BTreeMap<EmissionShape, usize>>>,
        }

        fn edge_of(record: &EmissionRecord) -> EvidenceEdge {
            EvidenceEdge {
                kind: record.kind,
                point: record.point,
                name: record.name.clone(),
                channel: record.channel(),
            }
        }

        let mut acc: BTreeMap<EvidenceEdge, Acc> = BTreeMap::new();
        for epoch in &self.epochs {
            let mut signatures: BTreeMap<EvidenceEdge, BTreeMap<EmissionShape, usize>> =
                BTreeMap::new();
            for record in epoch.records.iter().filter(|r| is_physical(r)) {
                let edge = edge_of(record);
                let a = acc.entry(edge.clone()).or_default();
                match epoch.action {
                    CampaignAction::GrowData => {
                        a.data_phase_emissions += 1;
                        a.data_phase_bytes += record.bytes;
                    }
                    CampaignAction::FireOperational => {
                        a.operational_emissions += 1;
                        a.operational_bytes += record.bytes;
                        let repetition = epoch.repetition.unwrap();
                        a.payload_epochs
                            .entry((epoch.scale, record.payload_hash))
                            .or_default()
                            .insert(repetition);
                        if repetition == 0 {
                            let first = a.by_scale_first.entry(epoch.scale).or_default();
                            first.0 += 1;
                            first.1 += record.bytes;
                        }
                        *signatures
                            .entry(edge)
                            .or_default()
                            .entry(shape_of(record))
                            .or_default() += 1;

                        let fired_nodes: Vec<Node> = record
                            .tags
                            .iter()
                            .filter(|t| epoch.fresh_operational.contains(t))
                            .filter_map(|t| self.tag_node(t))
                            .collect();
                        let carries_fresh = record
                            .tags
                            .iter()
                            .any(|t| epoch.fresh_operational.contains(t));
                        if carries_fresh {
                            a.fired_tag_emissions += 1;
                            let emitter: Node = (record.source.clone(), record.member);
                            let destination: Node = (record.destination.clone(), record.recipient);
                            let others: Vec<&Node> =
                                fired_nodes.iter().filter(|n| **n != emitter).collect();
                            if !others.is_empty() {
                                a.other_node_emissions += 1;
                                if !record.coarse {
                                    a.other_node_emissions_exact += 1;
                                }
                                if others.iter().any(|n| **n == destination) {
                                    a.returned_emissions += 1;
                                }
                            }
                        }
                    }
                }
            }
            for classified in &epoch.classified {
                let edge = edge_of(&classified.record);
                let a = acc.entry(edge).or_default();
                match classified.label {
                    Label::Constant => a.no_lineage += 1,
                    Label::FixedOperational => a.no_data_ancestry += 1,
                    Label::Productive => a.undominated_data_ancestry += 1,
                    Label::Reactivated => a.operational_dominated_ancestry += 1,
                    Label::Redundant => a.data_dominated_ancestry += 1,
                }
                if classified.record.coarse {
                    a.coarse_lineage += 1;
                } else {
                    a.exact_lineage += 1;
                }
            }
            if epoch.action == CampaignAction::FireOperational {
                // Record an empty signature for edges known from another epoch so drainage is
                // represented identically to non-empty recurrence.
                let known: Vec<_> = acc.keys().cloned().collect();
                for edge in known {
                    let signature = signatures.remove(&edge).unwrap_or_default();
                    acc.get_mut(&edge)
                        .unwrap()
                        .final_signatures
                        .entry(epoch.scale)
                        .or_default()
                        .push(signature);
                }
            }
        }

        acc.into_iter()
            .map(|(edge, a)| {
                let recurring_payloads = a
                    .payload_epochs
                    .values()
                    .filter(|epochs| epochs.len() > 1)
                    .count();
                let operational_curve: Vec<_> = a
                    .by_scale_first
                    .iter()
                    .map(|(&scale, &(messages, bytes))| (scale, messages, bytes))
                    .collect();
                let operational_work_scales = operational_curve.windows(2).any(|pair| {
                    let [
                        (_, lower_messages, lower_bytes),
                        (_, higher_messages, higher_bytes),
                    ] = pair
                    else {
                        unreachable!()
                    };
                    higher_messages > lower_messages || higher_bytes > lower_bytes
                });
                let mut terminal_by_scale = Vec::new();
                for (&scale, signatures) in &a.final_signatures {
                    let window = stable_window(&self.config, scale);
                    let per_scale = if signatures.len() >= window {
                        let tail = &signatures[signatures.len() - window..];
                        if tail.iter().all(BTreeMap::is_empty) {
                            TerminalBehavior::EmptyTail
                        } else if tail.windows(2).all(|w| w[0] == w[1]) {
                            TerminalBehavior::RepeatedNonEmptyTail
                        } else {
                            TerminalBehavior::Unresolved
                        }
                    } else {
                        TerminalBehavior::Unresolved
                    };
                    terminal_by_scale.push((scale, per_scale));
                }
                let saw = |t: TerminalBehavior| terminal_by_scale.iter().any(|(_, b)| *b == t);
                let terminal = if saw(TerminalBehavior::Unresolved)
                    || (saw(TerminalBehavior::EmptyTail)
                        && saw(TerminalBehavior::RepeatedNonEmptyTail))
                {
                    TerminalBehavior::Unresolved
                } else if saw(TerminalBehavior::RepeatedNonEmptyTail) {
                    TerminalBehavior::RepeatedNonEmptyTail
                } else if saw(TerminalBehavior::EmptyTail) {
                    TerminalBehavior::EmptyTail
                } else {
                    TerminalBehavior::Unresolved
                };
                EdgeEvidence {
                    edge,
                    data_phase_emissions: a.data_phase_emissions,
                    data_phase_bytes: a.data_phase_bytes,
                    operational_curve,
                    operational_emissions: a.operational_emissions,
                    operational_bytes: a.operational_bytes,
                    no_lineage: a.no_lineage,
                    no_data_ancestry: a.no_data_ancestry,
                    undominated_data_ancestry: a.undominated_data_ancestry,
                    operational_dominated_ancestry: a.operational_dominated_ancestry,
                    data_dominated_ancestry: a.data_dominated_ancestry,
                    exact_lineage: a.exact_lineage,
                    coarse_lineage: a.coarse_lineage,
                    recurring_payloads,
                    operational_work_scales,
                    fired_tag_emissions: a.fired_tag_emissions,
                    other_node_emissions: a.other_node_emissions,
                    other_node_emissions_exact: a.other_node_emissions_exact,
                    returned_emissions: a.returned_emissions,
                    terminal_by_scale,
                    terminal,
                }
            })
            .collect()
    }
}

/// Which data input(s) a case grows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataSelection {
    /// No data at all: the empty-state operational probe.
    None,
    /// One registered data port and one legal target.
    Single {
        /// Data input port.
        port: usize,
        /// Member target for cluster ports.
        target: Option<u32>,
    },
    /// Every registered data port/target pair; value `i` goes to pair `i % pairs`.
    AllRoundRobin,
}

/// Which operational input(s) a case fires in each epoch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperationalSelection {
    /// One registered operational port and one legal target.
    Single {
        /// Operational input port.
        port: usize,
        /// Member target for cluster ports.
        target: Option<u32>,
    },
    /// Every registered operational port/target pair fired once, in registry order, per epoch.
    All,
}

fn render_target(target: &Option<u32>) -> String {
    match target {
        Some(member) => member.to_string(),
        None => "-".into(),
    }
}

impl DataSelection {
    fn columns(&self) -> (String, String) {
        match self {
            DataSelection::None => ("-".into(), "-".into()),
            DataSelection::Single { port, target } => (port.to_string(), render_target(target)),
            DataSelection::AllRoundRobin => ("all".into(), "all".into()),
        }
    }
}

impl OperationalSelection {
    fn columns(&self) -> (String, String) {
        match self {
            OperationalSelection::Single { port, target } => {
                (port.to_string(), render_target(target))
            }
            OperationalSelection::All => ("all".into(), "all".into()),
        }
    }
}

/// One protocol-blind evidence case.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvidenceCase {
    /// Deterministic simulator schedule seed.
    pub schedule_seed: u64,
    /// Data input(s) grown before each probe.
    pub data: DataSelection,
    /// Operational input(s) fired in each probe epoch.
    pub operational: OperationalSelection,
}

impl EvidenceCase {
    fn columns(&self) -> [String; 5] {
        let (dp, dt) = self.data.columns();
        let (op, ot) = self.operational.columns();
        [self.schedule_seed.to_string(), dp, dt, op, ot]
    }

    /// Case identity without the seed, for grouping across schedules.
    pub fn without_seed(&self) -> (DataSelection, OperationalSelection) {
        (self.data.clone(), self.operational.clone())
    }
}

/// Uniform evidence row with the input/target case that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceMatrixRow {
    /// Input combination exercised in a fresh simulator instance.
    pub case: EvidenceCase,
    /// Evidence for one physical edge under that combination.
    pub evidence: EdgeEvidence,
}

/// Ledger entry: one case that was executed, whether or not it produced any emission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseLedger {
    /// The executed case.
    pub case: EvidenceCase,
    /// Number of distinct physical edges that emitted at least once in this case.
    pub edges_observed: usize,
    /// Number of quiescence-delimited epochs executed across all scales.
    pub epochs: usize,
    /// Total physical emissions in the case.
    pub physical_emissions: usize,
    /// Raw per-epoch curve `(scale, action, repetition, physical emissions, bytes)` in execution
    /// order, before any per-edge reduction.
    pub epoch_curve: Vec<(usize, CampaignAction, Option<usize>, usize, usize)>,
}

/// Complete output of the generic evidence matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceMatrix {
    /// Graph boundary used to enforce exhaustive adapter registration.
    pub manifest: BoundaryManifest,
    /// Every executed case, including cases with zero emissions.
    pub cases: Vec<CaseLedger>,
    /// One row per (case, edge) for every edge observed anywhere in the program. Edges that
    /// emitted nothing in a case carry [`TerminalBehavior::NoEmission`].
    pub rows: Vec<EvidenceMatrixRow>,
}

/// One field that differs between a baseline matrix and a variant matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceDifference {
    /// Case in which the difference was observed.
    pub case: EvidenceCase,
    /// Stable edge key (kind, name, channel).
    pub edge: (EmissionPointKind, String, Channel),
    /// Column name, or `row` when the edge exists on only one side.
    pub field: &'static str,
    /// Baseline value (`-` if the row is missing on the baseline side).
    pub baseline: String,
    /// Variant value (`-` if the row is missing on the variant side).
    pub variant: String,
}

const CASE_COLUMNS: &str = "schedule_seed\tdata_port\tdata_target\top_port\top_target";
const EDGE_COLUMNS: &str = "point\tkind\tchannel";

impl EvidenceMatrix {
    /// Renders the discovered boundary so every input the adapter had to cover is visible.
    pub fn render_manifest_tsv(&self) -> String {
        let mut out = String::from("port\tdirection\tdeclared_role\tlocation\tmany\ttype\n");
        for port in &self.manifest.ports {
            out.push_str(&format!(
                "{}\t{:?}\t{}\t{}\t{}\t{}\n",
                port.port,
                port.direction,
                port.role.map_or("-".to_owned(), |r| format!("{r:?}")),
                port.location,
                port.many,
                port.type_name,
            ));
        }
        for cycle in &self.manifest.cycles {
            out.push_str(&format!(
                "cycle {}\tCycle\t-\t{}\t-\t{}\n",
                cycle.cycle, cycle.location, cycle.collection
            ));
        }
        out
    }

    /// Renders the case ledger. A case with `edges_observed = 0` produced no physical work.
    pub fn render_cases_tsv(&self) -> String {
        let mut out = format!("{CASE_COLUMNS}\tedges_observed\tepochs\tphysical_msgs\n");
        for ledger in &self.cases {
            out.push_str(&ledger.case.columns().join("\t"));
            out.push_str(&format!(
                "\t{}\t{}\t{}\n",
                ledger.edges_observed, ledger.epochs, ledger.physical_emissions
            ));
        }
        out
    }

    /// Renders every epoch of every case: the raw message/byte counts in execution order.
    pub fn render_epochs_tsv(&self) -> String {
        let mut out = format!("{CASE_COLUMNS}\tscale\taction\trepetition\tphysical_msgs\tbytes\n");
        for ledger in &self.cases {
            for (scale, action, repetition, msgs, bytes) in &ledger.epoch_curve {
                out.push_str(&ledger.case.columns().join("\t"));
                out.push_str(&format!(
                    "\t{scale}\t{action:?}\t{}\t{msgs}\t{bytes}\n",
                    repetition.map_or("-".to_owned(), |r| r.to_string())
                ));
            }
        }
        out
    }

    /// Renders every observation with one fixed schema. No protocol-specific columns, labels, or
    /// conclusions are added.
    pub fn render_tsv(&self) -> String {
        let mut out = format!(
            "{CASE_COLUMNS}\t{EDGE_COLUMNS}\t{}\n",
            EdgeEvidence::column_names().join("\t")
        );
        for row in &self.rows {
            let e = &row.evidence;
            out.push_str(&row.case.columns().join("\t"));
            out.push_str(&format!(
                "\t{}\t{:?}\t{:?}",
                e.edge.point, e.edge.kind, e.edge.channel
            ));
            for (_, value) in e.fields() {
                out.push('\t');
                out.push_str(&value);
            }
            out.push('\n');
        }
        out
    }

    /// Reports literal evidence fields that differ across schedule seeds for the same input and
    /// edge case. An empty list means all measured columns were identical across tested seeds.
    pub fn schedule_variations(
        &self,
    ) -> BTreeMap<((DataSelection, OperationalSelection), EvidenceEdge), Vec<&'static str>> {
        let mut groups: BTreeMap<_, Vec<&EdgeEvidence>> = BTreeMap::new();
        for row in &self.rows {
            groups
                .entry((row.case.without_seed(), row.evidence.edge.clone()))
                .or_default()
                .push(&row.evidence);
        }
        groups
            .into_iter()
            .map(|(key, rows)| {
                let first = rows[0].fields();
                let changed = rows
                    .iter()
                    .skip(1)
                    .flat_map(|row| {
                        row.fields()
                            .into_iter()
                            .zip(first.iter())
                            .filter(|((_, v), (_, f))| v != f)
                            .map(|((name, _), _)| name)
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                (key, changed)
            })
            .collect()
    }

    /// Renders schedule variation using literal field names rather than a stability verdict.
    pub fn render_schedule_variations_tsv(&self) -> String {
        let mut out = format!(
            "data_port\tdata_target\top_port\top_target\t{EDGE_COLUMNS}\tchanged_fields_across_schedules\n"
        );
        for (((data, operational), edge), changed) in self.schedule_variations() {
            let (dp, dt) = data.columns();
            let (op, ot) = operational.columns();
            out.push_str(&format!(
                "{dp}\t{dt}\t{op}\t{ot}\t{}\t{:?}\t{:?}\t{changed:?}\n",
                edge.point, edge.kind, edge.channel,
            ));
        }
        out
    }

    /// Field-by-field comparison with a matrix produced from a related program (a mechanism
    /// change or a refactor) under the same registry shape. Rows are matched on case and stable
    /// edge key; the compiler-assigned point ID is ignored.
    pub fn compare(&self, variant: &EvidenceMatrix) -> Vec<EvidenceDifference> {
        fn index(
            matrix: &EvidenceMatrix,
        ) -> BTreeMap<(EvidenceCase, (EmissionPointKind, String, Channel)), &EdgeEvidence> {
            matrix
                .rows
                .iter()
                .map(|row| {
                    (
                        (row.case.clone(), row.evidence.edge.stable_key()),
                        &row.evidence,
                    )
                })
                .collect()
        }
        let base = index(self);
        let var = index(variant);
        let keys: BTreeSet<_> = base.keys().chain(var.keys()).cloned().collect();
        let mut out = Vec::new();
        for (case, edge) in keys {
            match (
                base.get(&(case.clone(), edge.clone())),
                var.get(&(case.clone(), edge.clone())),
            ) {
                (Some(b), Some(v)) => {
                    for ((name, bv), (_, vv)) in b.fields().into_iter().zip(v.fields()) {
                        if bv != vv {
                            out.push(EvidenceDifference {
                                case: case.clone(),
                                edge: edge.clone(),
                                field: name,
                                baseline: bv,
                                variant: vv,
                            });
                        }
                    }
                }
                (Some(_), None) => out.push(EvidenceDifference {
                    case,
                    edge,
                    field: "row",
                    baseline: "present".into(),
                    variant: "-".into(),
                }),
                (None, Some(_)) => out.push(EvidenceDifference {
                    case,
                    edge,
                    field: "row",
                    baseline: "-".into(),
                    variant: "present".into(),
                }),
                (None, None) => unreachable!(),
            }
        }
        out
    }

    /// Renders a comparison; an empty body means the two matrices are identical field for field.
    pub fn render_comparison_tsv(differences: &[EvidenceDifference]) -> String {
        let mut out = format!("{CASE_COLUMNS}\tkind\tname\tchannel\tfield\tbaseline\tvariant\n");
        for d in differences {
            out.push_str(&d.case.columns().join("\t"));
            out.push_str(&format!(
                "\t{:?}\t{}\t{:?}\t{}\t{}\t{}\n",
                d.edge.0, d.edge.1, d.edge.2, d.field, d.baseline, d.variant
            ));
        }
        out
    }
}

/// Runs the same evidence matrix over all registered data/operational ports and all legal targets.
///
/// Every scale is executed in a fresh simulator instance using the same scheduler-decision byte
/// stream. For each seed the matrix contains, for every operational selection (each single
/// port/target, plus all of them fired together when there is more than one): an empty-state
/// probe, one case per single data port/target, and one round-robin case over all data
/// port/targets when there is more than one. Every edge observed anywhere in the program gets a
/// row in every case; a case in which it emitted nothing gets an explicit zero row. The function
/// contains no protocol names, selected ports, phases, expected classes, or interpretation
/// callbacks.
pub fn run_evidence_matrix(
    compiled: &CompiledSim,
    manifest: BoundaryManifest,
    registry: &InputRegistry<'_>,
    config: CampaignConfig,
) -> EvidenceMatrix {
    registry.validate(&manifest);
    config.validate();
    let pairs = |role: InputRole| -> Vec<(usize, Option<u32>)> {
        registry
            .inputs
            .iter()
            .filter(|(_, input)| input.role == role)
            .flat_map(|(&port, input)| input.targets.iter().copied().map(move |t| (port, t)))
            .collect()
    };
    let operational_pairs = pairs(InputRole::Operational);
    let data_pairs = pairs(InputRole::Data);
    assert!(
        !operational_pairs.is_empty(),
        "evidence matrix requires an operational input"
    );

    let mut operational_selections: Vec<OperationalSelection> = operational_pairs
        .iter()
        .map(|&(port, target)| OperationalSelection::Single { port, target })
        .collect();
    if operational_pairs.len() > 1 {
        operational_selections.push(OperationalSelection::All);
    }
    let mut data_selections = vec![DataSelection::None];
    data_selections.extend(
        data_pairs
            .iter()
            .map(|&(port, target)| DataSelection::Single { port, target }),
    );
    if data_pairs.len() > 1 {
        data_selections.push(DataSelection::AllRoundRobin);
    }

    let port_nodes: BTreeMap<u32, (String, bool)> = manifest
        .input_ports()
        .map(|p| (p.port as u32, (p.location.clone(), p.many)))
        .collect();

    let mut cases = Vec::new();
    let mut rows = Vec::new();
    for &schedule_seed in &config.schedule_seeds {
        for operational in &operational_selections {
            for data in &data_selections {
                let case = EvidenceCase {
                    schedule_seed,
                    data: data.clone(),
                    operational: operational.clone(),
                };
                let scales: &[usize] = if *data == DataSelection::None {
                    &[0]
                } else {
                    &config.scales
                };
                let report = run_evidence_case(
                    compiled,
                    registry,
                    &config,
                    &case,
                    scales,
                    &data_pairs,
                    &operational_pairs,
                    port_nodes.clone(),
                );
                let evidence = report.edge_evidence();
                cases.push(CaseLedger {
                    case: case.clone(),
                    edges_observed: evidence.len(),
                    epochs: report.epochs.len(),
                    physical_emissions: report.epochs.iter().map(|e| e.physical_emissions).sum(),
                    epoch_curve: report
                        .epochs
                        .iter()
                        .map(|e| {
                            (
                                e.scale,
                                e.action,
                                e.repetition,
                                e.physical_emissions,
                                e.bytes,
                            )
                        })
                        .collect(),
                });
                rows.extend(evidence.into_iter().map(|evidence| EvidenceMatrixRow {
                    case: case.clone(),
                    evidence,
                }));
            }
        }
    }

    // Explicit zero rows: every edge seen in any case appears in every case.
    let all_edges: BTreeSet<EvidenceEdge> =
        rows.iter().map(|row| row.evidence.edge.clone()).collect();
    for ledger in &cases {
        let present: BTreeSet<_> = rows
            .iter()
            .filter(|row| row.case == ledger.case)
            .map(|row| row.evidence.edge.clone())
            .collect();
        for edge in all_edges.difference(&present) {
            rows.push(EvidenceMatrixRow {
                case: ledger.case.clone(),
                evidence: EdgeEvidence::absent(edge.clone()),
            });
        }
    }
    rows.sort_by(|a, b| {
        a.case
            .cmp(&b.case)
            .then_with(|| a.evidence.edge.cmp(&b.evidence.edge))
    });
    EvidenceMatrix {
        manifest,
        cases,
        rows,
    }
}

#[expect(clippy::too_many_arguments, reason = "internal plumbing for one case")]
fn run_evidence_case(
    compiled: &CompiledSim,
    registry: &InputRegistry<'_>,
    config: &CampaignConfig,
    case: &EvidenceCase,
    scales: &[usize],
    data_pairs: &[(usize, Option<u32>)],
    operational_pairs: &[(usize, Option<u32>)],
    port_nodes: BTreeMap<u32, (String, bool)>,
) -> CampaignReport {
    let mut epochs = Vec::new();
    for &scale in scales {
        let output = std::sync::Mutex::new(None);
        let decisions = schedule_decisions(case.schedule_seed);
        compiled.fuzz_repro(decisions, async |instance| {
            instance
                .run_with_scheduler_and_logger(std::io::sink(), async {
                    let send = |(port, target): (usize, Option<u32>), index: usize| {
                        (registry.inputs[&port].send_index)(target, index);
                    };
                    let report = run_feedback_campaign(
                        CampaignConfig {
                            scales: vec![scale],
                            schedule_seeds: vec![case.schedule_seed],
                            operational_repetitions: config.operational_repetitions,
                            stop_after_stable_repetitions: config.stop_after_stable_repetitions,
                        },
                        |start, count| match &case.data {
                            DataSelection::None => assert_eq!(count, 0),
                            DataSelection::Single { port, target } => {
                                for index in start..start + count {
                                    send((*port, *target), index);
                                }
                            }
                            DataSelection::AllRoundRobin => {
                                for index in start..start + count {
                                    send(data_pairs[index % data_pairs.len()], index);
                                }
                            }
                        },
                        |repetition| match &case.operational {
                            OperationalSelection::Single { port, target } => {
                                send((*port, *target), repetition);
                            }
                            OperationalSelection::All => {
                                for &pair in operational_pairs {
                                    send(pair, repetition);
                                }
                            }
                        },
                    )
                    .await;
                    *output.lock().unwrap() = Some(report);
                })
                .await;
        });
        epochs.extend(
            output
                .into_inner()
                .unwrap()
                .expect("evidence-matrix instance returned no report")
                .epochs,
        );
    }
    CampaignReport {
        config: CampaignConfig {
            scales: scales.to_vec(),
            schedule_seeds: vec![case.schedule_seed],
            operational_repetitions: config.operational_repetitions,
            stop_after_stable_repetitions: config.stop_after_stable_repetitions,
        },
        epochs,
        port_nodes,
    }
}

fn schedule_decisions(seed: u64) -> Vec<u8> {
    let mut state = seed.max(1);
    let mut bytes = vec![0; 1 << 20];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    bytes
}

/// Runs one deterministic retained-state experiment inside a simulation compiled with provenance.
///
/// `inject_data(start, count)` must inject `count` valid values beginning at deterministic index
/// `start`. It is the only type-specific value-generation hook. `fire_operational(repetition)`
/// must fire the selected operational input(s) once. Neither callback controls phase ordering,
/// scale, quiescence, analysis, or expected results.
///
/// At every configured scale the run waits for data-driven work to quiesce, then fires the
/// operational input(s) repeatedly, waiting for quiescence after every firing, until the physical
/// emission shape multiset repeats `stop_after_stable_repetitions` times or the budget is spent.
pub async fn run_feedback_campaign(
    config: CampaignConfig,
    mut inject_data: impl FnMut(usize, usize),
    mut fire_operational: impl FnMut(usize),
) -> CampaignReport {
    config.validate();
    let _ = take_emissions();
    let mut history: Vec<EmissionRecord> = Vec::new();
    let mut seen_tags: BTreeSet<Tag> = BTreeSet::new();
    let mut epochs = Vec::new();
    let mut admitted = 0;

    let mut observe = |history: &mut Vec<EmissionRecord>,
                       seen_tags: &mut BTreeSet<Tag>,
                       scale: usize,
                       action: CampaignAction,
                       repetition: Option<usize>| {
        let records = take_emissions();
        let before = classify(history, false).len();
        history.extend(records.iter().cloned());
        let classified = classify(history, false).into_iter().skip(before).collect();
        let fresh_operational: BTreeSet<Tag> = records
            .iter()
            .flat_map(|r| r.operational_tags().copied())
            .filter(|t| !seen_tags.contains(t))
            .collect();
        seen_tags.extend(records.iter().flat_map(|r| r.tags.iter().copied()));
        EpochObservation::from_epoch(
            scale,
            action,
            repetition,
            records,
            classified,
            fresh_operational,
        )
    };

    for &scale in &config.scales {
        assert!(
            scale >= admitted,
            "scale moved backwards from {admitted} to {scale}"
        );
        let delta = scale - admitted;
        if delta > 0 {
            inject_data(admitted, delta);
            quiesce().await;
            epochs.push(observe(
                &mut history,
                &mut seen_tags,
                scale,
                CampaignAction::GrowData,
                None,
            ));
            admitted = scale;
        }

        let mut stable_repetitions = 0;
        let mut previous_signature = None;
        for repetition in 0..config.operational_repetitions {
            fire_operational(repetition);
            quiesce().await;
            let observation = observe(
                &mut history,
                &mut seen_tags,
                scale,
                CampaignAction::FireOperational,
                Some(repetition),
            );
            let signature = observation.physical_signature();
            if previous_signature.as_ref() == Some(&signature) {
                stable_repetitions += 1;
            } else {
                stable_repetitions = 1;
            }
            previous_signature = Some(signature);
            epochs.push(observation);
            if stable_repetitions >= stable_window(&config, scale) {
                break;
            }
        }
    }

    CampaignReport {
        config,
        epochs,
        port_nodes: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::provenance::TagKind;
    use super::*;

    #[test]
    fn validates_fixed_campaign_algebra() {
        CampaignConfig {
            scales: vec![1, 4, 16],
            schedule_seeds: vec![0x5a17],
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
            schedule_seeds: vec![0x5a17],
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

    fn record(
        source: &str,
        member: Option<u32>,
        destination: &str,
        recipient: Option<u32>,
        tags: &[Tag],
    ) -> EmissionRecord {
        EmissionRecord {
            kind: EmissionPointKind::Network,
            point: 1,
            name: "edge".into(),
            source: source.into(),
            member,
            destination: destination.into(),
            recipient,
            tags: tags.iter().copied().collect(),
            coarse: false,
            bytes: 4,
            payload_hash: 7,
        }
    }

    fn op_tag(port: u32, seq: u64) -> Tag {
        Tag {
            kind: TagKind::Operational,
            port,
            member: u32::MAX,
            seq,
        }
    }

    /// A message from node B carrying the operational event fired at node A, addressed to A, is
    /// counted as other-node work that returned; A's own message carrying the same event is not.
    #[test]
    fn other_node_and_returned_counts_follow_the_fired_tag() {
        let fired = op_tag(9, 0);
        let a_sends = record("Process(a)", None, "Process(b)", None, &[fired]);
        let b_returns = record("Process(b)", None, "Process(a)", None, &[fired]);
        let epoch = EpochObservation::from_epoch(
            1,
            CampaignAction::FireOperational,
            Some(0),
            vec![a_sends, b_returns],
            vec![],
            [fired].into_iter().collect(),
        );
        let report = CampaignReport {
            config: CampaignConfig {
                scales: vec![1],
                schedule_seeds: vec![1],
                operational_repetitions: 1,
                stop_after_stable_repetitions: 1,
            },
            epochs: vec![epoch],
            port_nodes: [(9, ("Process(a)".to_owned(), false))]
                .into_iter()
                .collect(),
        };
        let rows = report.edge_evidence();
        let to_b = rows
            .iter()
            .find(|r| r.edge.channel.1 == "Process(b)")
            .unwrap();
        let to_a = rows
            .iter()
            .find(|r| r.edge.channel.1 == "Process(a)")
            .unwrap();
        assert_eq!(
            (
                to_b.fired_tag_emissions,
                to_b.other_node_emissions,
                to_b.returned_emissions
            ),
            (1, 0, 0)
        );
        assert_eq!(
            (
                to_a.fired_tag_emissions,
                to_a.other_node_emissions,
                to_a.returned_emissions
            ),
            (1, 1, 1)
        );
    }

    /// The stopping shape ignores payload identity and operational tag identity but keeps the
    /// literal data-ancestry set, so fixed traffic with a changing sequence number repeats while a
    /// stream of new facts does not.
    #[test]
    fn signature_is_label_free_and_keeps_data_ancestry() {
        let data = |seq| Tag {
            kind: TagKind::Data,
            port: 1,
            member: u32::MAX,
            seq,
        };
        let mk = |tags: &[Tag], payload: u64| {
            let mut r = record("Process(a)", None, "Process(b)", None, tags);
            r.payload_hash = payload;
            r
        };
        let epoch = |records: Vec<EmissionRecord>| {
            EpochObservation::from_epoch(
                1,
                CampaignAction::FireOperational,
                Some(0),
                records,
                vec![],
                BTreeSet::new(),
            )
        };
        let heartbeat_1 = epoch(vec![mk(&[op_tag(2, 0)], 100)]);
        let heartbeat_2 = epoch(vec![mk(&[op_tag(2, 1)], 101)]);
        assert_eq!(
            heartbeat_1.physical_signature(),
            heartbeat_2.physical_signature()
        );
        let fact_1 = epoch(vec![mk(&[data(0), op_tag(2, 0)], 5)]);
        let fact_2 = epoch(vec![mk(&[data(1), op_tag(2, 1)], 5)]);
        assert_ne!(fact_1.physical_signature(), fact_2.physical_signature());
        // Same retained item re-sent: identical shape. Next item out of a queue: different.
        let replay_1 = epoch(vec![mk(&[data(0), op_tag(2, 0)], 5)]);
        let replay_2 = epoch(vec![mk(&[data(0), op_tag(2, 1)], 5)]);
        assert_eq!(replay_1.physical_signature(), replay_2.physical_signature());
        let next_item = epoch(vec![mk(&[data(0), op_tag(2, 1)], 6)]);
        assert_ne!(
            replay_1.physical_signature(),
            next_item.physical_signature()
        );
    }

    #[test]
    fn stability_window_grows_with_scale() {
        let config = CampaignConfig {
            scales: vec![1],
            schedule_seeds: vec![1],
            operational_repetitions: 64,
            stop_after_stable_repetitions: 2,
        };
        assert_eq!(stable_window(&config, 0), 2);
        assert_eq!(stable_window(&config, 1), 2);
        assert_eq!(stable_window(&config, 8), 9);
        assert_eq!(stable_window(&config, 32), 33);
    }
}
