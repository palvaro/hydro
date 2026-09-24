//! Runtime metrics for DFIR.

use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use dfir_lang::graph_ids::{GraphNodeId, GraphSubgraphId};
use pin_project_lite::pin_project;
use slotmap::SecondaryMap;
use web_time::{Duration, Instant};

#[cfg(feature = "meta")]
use dfir_lang::graph::DfirGraph;
#[cfg(feature = "meta")]
use dfir_lang::graph::ops::DelayType;

/// Metrics for a [`Dfir`](super::context::Dfir) graph instance.
///
/// Call [`Dfir::metrics`](super::context::Dfir::metrics) for reference-counted continually-updated metrics,
/// or call [`Dfir::metrics_intervals`](super::context::Dfir::metrics_intervals) to obtain a [`DfirMetricsIntervals`] handle, and use
/// [`DfirMetricsIntervals::take_interval`] to retrieve metrics for successive intervals.
#[derive(Default, Clone)]
#[non_exhaustive]
pub struct DfirMetrics {
    /// Per-subgraph metrics.
    pub subgraphs: SecondaryMap<GraphSubgraphId, SubgraphMetrics>,
    /// Per-handoff metrics.
    pub handoffs: SecondaryMap<GraphNodeId, HandoffMetrics>,
}

impl DfirMetrics {
    /// Subtracts `other` from self.
    pub(super) fn diff(&mut self, other: &Self) {
        for (sg_id, prev_sg_metrics) in other.subgraphs.iter() {
            if let Some(curr_sg_metrics) = self.subgraphs.get_mut(sg_id) {
                curr_sg_metrics.diff(prev_sg_metrics);
            }
        }
        for (handoff_id, prev_handoff_metrics) in other.handoffs.iter() {
            if let Some(curr_handoff_metrics) = self.handoffs.get_mut(handoff_id) {
                curr_handoff_metrics.diff(prev_handoff_metrics);
            }
        }
    }
}

/// A handle into a DFIR instance's metrics, where each call to [`Self::take_interval`] ends the current interval and
/// returns its metrics. Obtained via [`Dfir::metrics_intervals`](super::context::Dfir::metrics_intervals).
///
/// The first call to `take_interval` returns metrics since this DFIR instance was created. Each subsequent call to
/// `take_interval` returns metrics since the previous call.
///
/// Cloning the handle "forks" it from the original, as afterwards each interval may return different metrics
/// depending on when exactly `take_interval` is called.
#[derive(Clone)]
pub struct DfirMetricsIntervals {
    /// `curr` is continually updating (via shared ownership).
    pub(super) curr: Rc<DfirMetrics>,
    /// `prev` is an unchanging snapshot in time. `None` for "since creation".
    pub(super) prev: Option<DfirMetrics>,
}

impl DfirMetricsIntervals {
    /// Ends the current interval and returns the accumulated metrics across the interval.
    ///
    /// The first call to `take_interval` returns metrics since this DFIR instance was created. Each subsequent call to
    /// `take_interval` returns metrics since the previous call.
    pub fn take_interval(&mut self) -> DfirMetrics {
        let mut curr = self.curr.as_ref().clone();
        if let Some(prev) = self.prev.replace(curr.clone()) {
            curr.diff(&prev);
        }
        curr
    }

    /// Returns a reference-counted handle to the original continually-updated runtime metrics for this DFIR instance.
    ///
    /// See [`Dfir::metrics`](super::context::Dfir::metrics).
    pub fn all_metrics(&self) -> Rc<DfirMetrics> {
        Rc::clone(&self.curr)
    }
}

/// Declarative macro to generate metrics structs with Cell-based fields and getter methods.
macro_rules! define_metrics {
    (
        $(#[$struct_attr:meta])*
        pub struct $struct_name:ident {
            $(
                $( #[doc = $doc:literal] )*
                #[diff($diff:ident)]
                $( #[$field_attr:meta] )*
                $field_name:ident: Cell<$field_type:ty>,
            )*
        }
    ) => {
        $(#[$struct_attr])*
        #[derive(Default, Debug, Clone)]
        #[non_exhaustive] // May add more metrics later.
        pub struct $struct_name {
            $(
                #[doc(hidden)] // Public for codegen access; use the getter method instead.
                $(#[$field_attr])*
                pub $field_name: Cell<$field_type>,
            )*
        }

        impl $struct_name {
            $(
                $( #[doc = $doc] )*
                pub fn $field_name(&self) -> $field_type {
                    self.$field_name.get()
                }
            )*

            fn diff(&mut self, other: &Self) {
                $(
                    define_metrics_diff_field!($diff, $field_name, self, other);
                )*
            }
        }
    };
}

macro_rules! define_metrics_diff_field {
    (total, $field:ident, $slf:ident, $other:ident) => {
        debug_assert!($other.$field.get() <= $slf.$field.get());
        $slf.$field.update(|x| x - $other.$field.get());
    };
    (curr, $field:ident, $slf:ident, $other:ident) => {};
}

define_metrics! {
    /// Per-handoff metrics.
    pub struct HandoffMetrics {
        /// Number of items currently in the handoff.
        #[diff(curr)]
        curr_items_count: Cell<usize>,

        /// Total number of items read out of the handoff.
        #[diff(total)]
        total_items_count: Cell<usize>,
    }
}

define_metrics! {
    /// Per-subgraph metrics.
    pub struct SubgraphMetrics {
        /// Number of times the subgraph has run.
        #[diff(total)]
        total_run_count: Cell<usize>,

        /// Time elapsed during polling (when the subgraph is actively doing work).
        #[diff(total)]
        total_poll_duration: Cell<Duration>,

        /// Number of times the subgraph has been polled.
        #[diff(total)]
        total_poll_count: Cell<usize>,

        /// Time elapsed during idle (when the subgraph has yielded and is waiting for async events).
        #[diff(total)]
        total_idle_duration: Cell<Duration>,

        /// Number of serialized network messages emitted by this subgraph.
        #[diff(total)]
        network_message_count: Cell<usize>,

        /// Number of serialized payload bytes emitted by this subgraph.
        #[diff(total)]
        network_byte_count: Cell<usize>,

        /// Number of times the subgraph has been idle.
        #[diff(total)]
        total_idle_count: Cell<usize>,
    }
}

/// Extracts the serialized byte payload from Hydro's internal network-send shapes.
#[doc(hidden)]
pub trait SerializedPayload {
    /// Exact serialized payload length, excluding transport framing.
    fn serialized_payload_len(&self) -> usize;
}

impl SerializedPayload for bytes::Bytes {
    fn serialized_payload_len(&self) -> usize {
        self.len()
    }
}

impl SerializedPayload for bytes::BytesMut {
    fn serialized_payload_len(&self) -> usize {
        self.len()
    }
}

impl<T, B: SerializedPayload> SerializedPayload for (T, B) {
    fn serialized_payload_len(&self) -> usize {
        self.1.serialized_payload_len()
    }
}

/// One handoff's item count as seen in a metrics interval.
#[cfg(feature = "meta")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageHandoffMetrics {
    /// Runtime graph ID of the handoff.
    pub handoff_id: GraphNodeId,
    /// Items drained by the receiving stage during this interval.
    pub items: usize,
    /// Items present when the handoff was last inspected in this interval.
    pub buffered_items: usize,
    /// Tick/loop delay when this handoff is a feedback boundary.
    pub delay: Option<DelayType>,
}

/// Static topology for one fused DFIR stage.
#[cfg(feature = "meta")]
#[derive(Clone, Debug)]
pub struct StageTopologyEntry {
    /// Runtime graph ID of the fused stage.
    pub subgraph_id: GraphSubgraphId,
    /// Compiler/debug tags of operators fused into the stage.
    pub operator_tags: Vec<String>,
    /// DFIR operator names fused into the stage.
    pub operator_names: Vec<String>,
    /// Number of retained handoff/state references read by the stage.
    pub retained_state_reads: usize,
    /// Number of retained handoff/state references mutated by the stage.
    pub retained_state_writes: usize,
    /// Input handoff IDs and their optional feedback delay.
    pub inputs: Vec<(GraphNodeId, Option<DelayType>)>,
    /// Output handoff IDs and their optional feedback delay.
    pub outputs: Vec<(GraphNodeId, Option<DelayType>)>,
}

/// Owned stage topology captured once from a runtime meta-graph.
#[cfg(feature = "meta")]
#[derive(Clone, Debug)]
pub struct StageTopology(pub Vec<StageTopologyEntry>);

#[cfg(feature = "meta")]
impl StageTopology {
    /// Captures the stage/handoff structure needed to interpret metric windows.
    pub fn from_graph(graph: &DfirGraph) -> Self {
        let handoffs = graph.subgraph_handoffs();
        Self(
            graph
                .subgraphs()
                .map(|(subgraph_id, nodes)| {
                    let (inputs, outputs) = handoffs
                        .get(subgraph_id)
                        .map(|(inputs, outputs)| {
                            (
                                inputs
                                    .iter()
                                    .map(|id| (*id, graph.handoff_delay_type(*id)))
                                    .collect(),
                                outputs
                                    .iter()
                                    .map(|id| (*id, graph.handoff_delay_type(*id)))
                                    .collect(),
                            )
                        })
                        .unwrap_or_default();
                    StageTopologyEntry {
                        subgraph_id,
                        operator_tags: nodes
                            .iter()
                            .filter_map(|id| graph.operator_tag(*id).map(str::to_owned))
                            .collect(),
                        operator_names: nodes
                            .iter()
                            .map(|id| graph.node(*id).to_name_string().into_owned())
                            .collect(),
                        retained_state_reads: nodes
                            .iter()
                            .flat_map(|id| graph.node_handoff_references(*id))
                            .filter(|reference| !reference.is_mut)
                            .count(),
                        retained_state_writes: nodes
                            .iter()
                            .flat_map(|id| graph.node_handoff_references(*id))
                            .filter(|reference| reference.is_mut)
                            .count(),
                        inputs,
                        outputs,
                    }
                })
                .collect(),
        )
    }
}

/// Descriptive runtime metrics for one fused DFIR subgraph.
#[cfg(feature = "meta")]
#[derive(Clone, Debug)]
pub struct StageMetrics {
    /// Runtime graph ID of the fused stage.
    pub subgraph_id: GraphSubgraphId,
    /// Compiler/debug tags of operators fused into the stage.
    pub operator_tags: Vec<String>,
    /// DFIR operator names fused into the stage.
    pub operator_names: Vec<String>,
    /// Number of retained handoff/state references read by the stage.
    pub retained_state_reads: usize,
    /// Number of retained handoff/state references mutated by the stage.
    pub retained_state_writes: usize,
    /// Stage executions during this interval.
    pub run_count: usize,
    /// Polls during this interval.
    pub poll_count: usize,
    /// Time spent polling during this interval.
    pub poll_duration: Duration,
    /// Serialized network messages emitted during this interval.
    pub network_message_count: usize,
    /// Serialized network payload bytes emitted during this interval.
    pub network_byte_count: usize,
    /// Handoffs consumed by this stage.
    pub inputs: Vec<StageHandoffMetrics>,
    /// Handoffs produced by this stage.
    pub outputs: Vec<StageHandoffMetrics>,
}

#[cfg(feature = "meta")]
impl StageMetrics {
    /// Whether this stage contains a compiler-identified interval source.
    pub fn has_interval_source(&self) -> bool {
        self.operator_tags
            .iter()
            .any(|tag| tag.starts_with("interval__"))
    }

    /// Total items consumed across input handoffs in the interval.
    pub fn input_items(&self) -> usize {
        self.inputs.iter().map(|input| input.items).sum()
    }

    /// Total items made visible at output handoffs in the interval.
    pub fn output_items(&self) -> usize {
        self.outputs.iter().map(|output| output.items).sum()
    }

    /// Items consumed from tick/loop feedback handoffs.
    pub fn feedback_input_items(&self) -> usize {
        self.inputs
            .iter()
            .filter(|input| input.delay.is_some())
            .map(|input| input.items)
            .sum()
    }
}

/// Compares output per operational activation in paired windows.
///
/// Returns `None` when either window has no operational activation. This is a
/// descriptive ratio difference, not a hazard classification.
#[cfg(feature = "meta")]
pub fn paired_gain_delta(
    control_operational_inputs: usize,
    control_outputs: usize,
    triggered_operational_inputs: usize,
    triggered_outputs: usize,
) -> Option<f64> {
    if control_operational_inputs == 0 || triggered_operational_inputs == 0 {
        return None;
    }
    Some(
        triggered_outputs as f64 / triggered_operational_inputs as f64
            - control_outputs as f64 / control_operational_inputs as f64,
    )
}

#[cfg(feature = "meta")]
impl DfirMetrics {
    /// Correlates an interval's counters with previously captured stage topology.
    pub fn by_stage_topology(&self, topology: &StageTopology) -> Vec<StageMetrics> {
        topology
            .0
            .iter()
            .map(|stage| {
                let counters = self.subgraphs.get(stage.subgraph_id);
                let map_handoff = |(handoff_id, delay): &(GraphNodeId, Option<DelayType>)| {
                    let metrics = self.handoffs.get(*handoff_id);
                    StageHandoffMetrics {
                        handoff_id: *handoff_id,
                        items: metrics.map_or(0, HandoffMetrics::total_items_count),
                        buffered_items: metrics.map_or(0, HandoffMetrics::curr_items_count),
                        delay: *delay,
                    }
                };
                StageMetrics {
                    subgraph_id: stage.subgraph_id,
                    operator_tags: stage.operator_tags.clone(),
                    operator_names: stage.operator_names.clone(),
                    retained_state_reads: stage.retained_state_reads,
                    retained_state_writes: stage.retained_state_writes,
                    run_count: counters.map_or(0, SubgraphMetrics::total_run_count),
                    poll_count: counters.map_or(0, SubgraphMetrics::total_poll_count),
                    poll_duration: counters
                        .map_or(Duration::ZERO, SubgraphMetrics::total_poll_duration),
                    network_message_count: counters
                        .map_or(0, SubgraphMetrics::network_message_count),
                    network_byte_count: counters.map_or(0, SubgraphMetrics::network_byte_count),
                    inputs: stage.inputs.iter().map(map_handoff).collect(),
                    outputs: stage.outputs.iter().map(map_handoff).collect(),
                }
            })
            .collect()
    }

    /// Correlates an interval's counters with the runtime DFIR graph.
    ///
    /// This performs no hazard classification. It exposes enough structure for
    /// research code to compare positive, control, and feedback activity across
    /// paired executions.
    pub fn by_stage(&self, graph: &DfirGraph) -> Vec<StageMetrics> {
        self.by_stage_topology(&StageTopology::from_graph(graph))
    }
}

pin_project! {
    /// Helper struct which instruments a future to track polling times.
    #[doc(hidden)]
    pub struct InstrumentSubgraph<'a, Fut> {
        #[pin]
        future: Fut,
        idle_start: Option<Instant>,
        metrics: &'a SubgraphMetrics,
    }
}

impl<'a, Fut> InstrumentSubgraph<'a, Fut> {
    /// Wrap a future to track per-subgraph poll and idle durations.
    pub fn new(future: Fut, metrics: &'a SubgraphMetrics) -> Self {
        Self {
            future,
            idle_start: None,
            metrics,
        }
    }
}

impl<'a, Fut> Future for InstrumentSubgraph<'a, Fut>
where
    Fut: Future,
{
    type Output = Fut::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // End idle duration.
        if let Some(idle_start) = this.idle_start {
            this.metrics
                .total_idle_duration
                .update(|x| x + idle_start.elapsed());
            this.metrics.total_idle_count.update(|x| x + 1);
        }

        // Begin poll duration.
        let poll_start = Instant::now();
        let out = this.future.poll(cx);

        // End poll duration.
        this.metrics
            .total_poll_duration
            .update(|x| x + poll_start.elapsed());
        this.metrics.total_poll_count.update(|x| x + 1);

        // Begin idle duration.
        this.idle_start.replace(Instant::now());

        out
    }
}

#[cfg(test)]
mod test {
    use dfir_lang::graph_ids::{GraphNodeId, GraphSubgraphId};
    use slotmap::SlotMap;

    use super::*;

    #[test]
    fn serialized_payload_lengths_cover_keyed_and_unkeyed_sends() {
        let bytes = bytes::Bytes::from_static(b"hello");
        assert_eq!(bytes.serialized_payload_len(), 5);
        assert_eq!((7u32, bytes).serialized_payload_len(), 5);
    }

    #[test]
    fn paired_gain_distinguishes_fixed_from_history_dependent_work() {
        assert_eq!(super::paired_gain_delta(4, 12, 4, 12), Some(0.0));
        assert_eq!(super::paired_gain_delta(4, 12, 4, 28), Some(4.0));
        assert_eq!(super::paired_gain_delta(0, 0, 4, 28), None);
    }

    #[test]
    fn test_dfir_metrics_intervals() {
        // Create slotmaps to generate valid keys.
        let mut sg_map: SlotMap<GraphSubgraphId, ()> = SlotMap::with_key();
        let mut node_map: SlotMap<GraphNodeId, ()> = SlotMap::with_key();
        let sg_id = sg_map.insert(());
        let handoff_id = node_map.insert(());

        let mut metrics = DfirMetrics::default();
        metrics.subgraphs.insert(
            sg_id,
            SubgraphMetrics {
                total_run_count: Cell::new(5),
                total_poll_count: Cell::new(10),
                total_idle_count: Cell::new(2),
                total_poll_duration: Cell::new(Duration::from_millis(500)),
                total_idle_duration: Cell::new(Duration::from_millis(200)),
                network_message_count: Cell::new(0),
                network_byte_count: Cell::new(0),
            },
        );
        metrics.handoffs.insert(
            handoff_id,
            HandoffMetrics {
                curr_items_count: Cell::new(3),
                total_items_count: Cell::new(100),
            },
        );
        let metrics = Rc::new(metrics);

        let mut intervals = DfirMetricsIntervals {
            curr: Rc::clone(&metrics),
            prev: None,
        };

        // First iteration - captures initial state
        let first = intervals.take_interval();
        let sg_metrics = &first.subgraphs[sg_id];
        assert_eq!(sg_metrics.total_run_count(), 5);
        let hoff_metrics = &first.handoffs[handoff_id];
        assert_eq!(hoff_metrics.total_items_count(), 100);
        assert_eq!(hoff_metrics.curr_items_count(), 3);

        // Simulate more work being done.
        let sg_metrics = &metrics.subgraphs[sg_id];
        sg_metrics.total_run_count.set(12);
        sg_metrics.total_poll_count.set(25);
        sg_metrics.total_idle_count.set(7);
        sg_metrics
            .total_poll_duration
            .set(Duration::from_millis(1200));
        sg_metrics
            .total_idle_duration
            .set(Duration::from_millis(600));
        let hoff_metrics = &metrics.handoffs[handoff_id];
        hoff_metrics.total_items_count.set(250);
        hoff_metrics.curr_items_count.set(10);

        // Second iteration - should return the diff
        let second = intervals.take_interval();
        let sg_metrics = &second.subgraphs[sg_id];
        assert_eq!(sg_metrics.total_run_count(), 7); // 12 - 5
        assert_eq!(sg_metrics.total_poll_count(), 15); // 25 - 10
        assert_eq!(sg_metrics.total_idle_count(), 5); // 7 - 2
        //
        let hoff_metrics = &second.handoffs[handoff_id];
        // total_items_count should be diffed
        assert_eq!(hoff_metrics.total_items_count(), 150); // 250 - 100
        // curr_items_count should NOT be diffed (it's a current value, not cumulative)
        assert_eq!(hoff_metrics.curr_items_count(), 10);
    }
}
