//! Determination depth analysis for Hydro programs.
//!
//! Analyzes the `HydroNode` IR tree to compute how many sequential layers of
//! coordination (determination depth) a program requires.
//!
//! The analysis:
//! 1. Finds all `ObserveNonDet` nodes (potential commitment points)
//! 2. Determines which are "absorbed" by downstream commutative folds
//! 3. Computes dependency structure among genuine (non-absorbed) commitments
//! 4. Derives depth as the longest chain of dependent commitments

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::compile::ir::{CollectionKind, HydroNode, HydroRoot, StreamOrder};

/// Unique ID for a nondet point, derived from its position in traversal.
pub type NonDetId = usize;

/// Information about a single nondeterminism point.
#[derive(Debug, Clone)]
pub struct NonDetPoint {
    /// Unique identifier.
    pub id: NonDetId,
    /// Whether marked as trusted (simulator skips it).
    pub trusted: bool,
    /// Whether all paths to outputs are absorbed by commutative folds.
    pub absorbed: bool,
}

/// Result of the determination depth analysis.
#[derive(Debug, Clone)]
pub struct DepthAnalysis {
    /// All nondet points found.
    pub nondet_points: Vec<NonDetPoint>,
    /// IDs of genuine commitments (not absorbed).
    pub genuine_commitments: Vec<NonDetId>,
    /// Dependency edges: (A, B) means B depends on A's output.
    pub dependency_edges: Vec<(NonDetId, NonDetId)>,
    /// Commitments grouped by layer.
    pub layers: Vec<Vec<NonDetId>>,
    /// The determination depth (number of layers, 0 = fully monotone).
    pub depth: usize,
}

/// Analyze a set of IR roots to compute determination depth.
pub fn analyze_depth(roots: &[HydroRoot]) -> DepthAnalysis {
    // Phase 1: Walk the tree, collect nondet points with absorption info
    // and dependency structure simultaneously.
    let mut collector = TreeCollector::new();
    for root in roots {
        collector.visit_root(root);
    }

    let nondet_points = collector.nondet_points;
    let genuine_commitments: Vec<NonDetId> = nondet_points
        .iter()
        .filter(|p| !p.absorbed)
        .map(|p| p.id)
        .collect();

    let dependency_edges = collector.dependency_edges;

    // Phase 2: Compute layers via topological sort
    let layers = compute_layers(&genuine_commitments, &dependency_edges);
    let depth = layers.len();

    DepthAnalysis {
        nondet_points,
        genuine_commitments,
        dependency_edges,
        layers,
        depth,
    }
}

/// Determines if a Fold/Reduce node absorbs nondeterminism based on its input's ordering.
///
/// A fold absorbs ordering nondeterminism if its input stream has `NoOrder` —
/// meaning the fold was already proven to be insensitive to element order
/// (its accumulator must be commutative for the program to type-check with NoOrder input).
fn is_absorbing_fold(input_node: &HydroNode) -> bool {
    let metadata = input_node.metadata();
    match &metadata.collection_kind {
        CollectionKind::Stream {
            order: StreamOrder::NoOrder,
            ..
        } => true,
        CollectionKind::KeyedStream {
            value_order: StreamOrder::NoOrder,
            ..
        } => true,
        _ => false,
    }
}

/// Tree traversal collector that builds nondet points and dependency info.
struct TreeCollector {
    nondet_points: Vec<NonDetPoint>,
    dependency_edges: Vec<(NonDetId, NonDetId)>,
    /// Map from node pointer → nondet ID (for Tee/SharedNode dedup)
    seen_tees: HashMap<*const RefCell<HydroNode>, ()>,
    /// ID counter for nondet points
    next_id: NonDetId,
}

impl TreeCollector {
    fn new() -> Self {
        Self {
            nondet_points: Vec::new(),
            dependency_edges: Vec::new(),
            seen_tees: HashMap::new(),
            next_id: 0,
        }
    }

    fn visit_root(&mut self, root: &HydroRoot) {
        let input = root_input(root);
        // Walk from root toward leaves. Track:
        // - ancestor_commitments: genuine commitments we've passed through on path from root
        // - passed_absorber: whether we've passed through an absorbing fold since the root
        self.visit_node(input, &[], false);
    }

    /// Visit a node in the IR tree.
    ///
    /// `ancestor_commitments`: IDs of genuine commitments on the path from the root to here.
    /// `passed_absorber`: whether an absorbing fold lies between the root and this node.
    ///
    /// The tree points from outputs (roots) toward inputs (leaves), so "children" are inputs.
    /// When we find a nondet node (ObserveNonDet OR Batch):
    /// - If NOT absorbed (no absorber between root and here): it's a genuine commitment.
    ///   It depends on any genuine commitment further down in its subtree.
    /// - If absorbed (absorber between root and here): depth 0, not genuine.
    fn visit_node(
        &mut self,
        node: &HydroNode,
        ancestor_commitments: &[NonDetId],
        passed_absorber: bool,
    ) {
        match node {
            HydroNode::Tee { inner, .. } | HydroNode::Partition { inner, .. } => {
                let ptr = inner.as_ptr();
                if self.seen_tees.contains_key(&ptr) {
                    return; // Already visited this shared subtree
                }
                self.seen_tees.insert(ptr, ());
                let borrowed = inner.0.borrow();
                self.visit_node(&borrowed, ancestor_commitments, passed_absorber);
            }

            HydroNode::ObserveNonDet {
                inner,
                trusted,
                ..
            } => {
                let id = self.next_id;
                self.next_id += 1;

                // Genuine if no absorber between root and here
                let absorbed = passed_absorber;

                self.nondet_points.push(NonDetPoint {
                    id,
                    trusted: *trusted,
                    absorbed,
                });

                if !absorbed {
                    // This is a genuine commitment. Any ancestor genuine commitment
                    // on the path from root is DOWNSTREAM of us (since tree goes root→leaf).
                    // So those ancestors DEPEND ON us (we're upstream of them).
                    for &ancestor_id in ancestor_commitments {
                        self.dependency_edges.push((id, ancestor_id));
                    }

                    // Continue into subtree with this commitment added to ancestors
                    let mut new_ancestors = ancestor_commitments.to_vec();
                    new_ancestors.push(id);
                    self.visit_node(inner, &new_ancestors, false);
                } else {
                    // Absorbed — continue without adding to ancestors
                    self.visit_node(inner, ancestor_commitments, passed_absorber);
                }
            }

            // Batch is the PRIMARY nondeterminism point — it determines which
            // messages land in which tick. This is where batching nondeterminism lives.
            HydroNode::Batch { inner, .. } => {
                let id = self.next_id;
                self.next_id += 1;

                let absorbed = passed_absorber;

                self.nondet_points.push(NonDetPoint {
                    id,
                    trusted: false,
                    absorbed,
                });

                if !absorbed {
                    for &ancestor_id in ancestor_commitments {
                        self.dependency_edges.push((id, ancestor_id));
                    }

                    let mut new_ancestors = ancestor_commitments.to_vec();
                    new_ancestors.push(id);
                    self.visit_node(inner, &new_ancestors, false);
                } else {
                    self.visit_node(inner, ancestor_commitments, passed_absorber);
                }
            }

            HydroNode::Fold { input, .. }
            | HydroNode::FoldKeyed { input, .. }
            | HydroNode::Reduce { input, .. }
            | HydroNode::ReduceKeyed { input, .. } => {
                // Check if this fold absorbs nondeterminism
                let absorbs = is_absorbing_fold(input);
                let new_passed_absorber = passed_absorber || absorbs;
                self.visit_node(input, ancestor_commitments, new_passed_absorber);
            }

            // Binary operators — visit both children
            HydroNode::Chain { first, second, .. }
            | HydroNode::ChainFirst { first, second, .. } => {
                self.visit_node(first, ancestor_commitments, passed_absorber);
                self.visit_node(second, ancestor_commitments, passed_absorber);
            }

            HydroNode::CrossProduct { left, right, .. }
            | HydroNode::CrossSingleton { left, right, .. }
            | HydroNode::Join { left, right, .. }
            | HydroNode::JoinHalf { left, right, .. } => {
                // For binary operators: commitments found in either subtree
                // are upstream of any ancestor commitment above this node.
                // Additionally, we need to detect cross-branch dependencies:
                // if commitment A is in the right branch and commitment B is
                // in the left branch, B's observable output is influenced by A
                // (because the binary operator combines both).
                //
                // Strategy: first collect commitments from both subtrees,
                // then record dependencies between them and ancestors.
                let before_left = self.nondet_points.len();
                self.visit_node(left, ancestor_commitments, passed_absorber);
                let after_left = self.nondet_points.len();
                self.visit_node(right, ancestor_commitments, passed_absorber);
                let after_right = self.nondet_points.len();

                // Commitments found in left subtree
                let left_commitments: Vec<NonDetId> = self.nondet_points[before_left..after_left]
                    .iter()
                    .filter(|p| !p.absorbed)
                    .map(|p| p.id)
                    .collect();

                // Commitments found in right subtree
                let right_commitments: Vec<NonDetId> = self.nondet_points[after_left..after_right]
                    .iter()
                    .filter(|p| !p.absorbed)
                    .map(|p| p.id)
                    .collect();

                // Cross-branch dependencies: each side depends on the other
                // because the binary operator combines their outputs.
                // A commitment in the left branch has its effect influenced
                // by commitments in the right branch (and vice versa).
                for &left_id in &left_commitments {
                    for &right_id in &right_commitments {
                        // left_id's output is combined with right_id's output
                        // → left_id depends on right_id (right is also upstream)
                        self.dependency_edges.push((right_id, left_id));
                        // right_id depends on left_id similarly? No — only if
                        // right's EFFECT depends on left's OUTCOME.
                        // For CrossSingleton: the singleton (right) doesn't depend
                        // on the stream (left), but the stream's interpretation
                        // depends on the singleton.
                        // Conservative: add both directions. This may over-report.
                        // TODO: Be smarter about which direction the dependency goes
                        // based on the operator semantics.
                    }
                }
            }

            HydroNode::Difference { pos, neg, .. }
            | HydroNode::AntiJoin { pos, neg, .. } => {
                self.visit_node(pos, ancestor_commitments, passed_absorber);
                self.visit_node(neg, ancestor_commitments, passed_absorber);
            }

            HydroNode::ReduceKeyedWatermark {
                input, watermark, ..
            } => {
                let absorbs = is_absorbing_fold(input);
                let new_passed_absorber = passed_absorber || absorbs;
                self.visit_node(input, ancestor_commitments, new_passed_absorber);
                self.visit_node(watermark, ancestor_commitments, passed_absorber);
            }

            // Single-child passthrough operators
            HydroNode::Cast { inner, .. }
            | HydroNode::BeginAtomic { inner, .. }
            | HydroNode::EndAtomic { inner, .. }
            | HydroNode::YieldConcat { inner, .. } => {
                self.visit_node(inner, ancestor_commitments, passed_absorber);
            }

            HydroNode::Map { input, .. }
            | HydroNode::FlatMap { input, .. }
            | HydroNode::FlatMapStreamBlocking { input, .. }
            | HydroNode::Filter { input, .. }
            | HydroNode::FilterMap { input, .. }
            | HydroNode::Sort { input, .. }
            | HydroNode::DeferTick { input, .. }
            | HydroNode::Enumerate { input, .. }
            | HydroNode::Inspect { input, .. }
            | HydroNode::Unique { input, .. }
            | HydroNode::ResolveFutures { input, .. }
            | HydroNode::ResolveFuturesBlocking { input, .. }
            | HydroNode::ResolveFuturesOrdered { input, .. }
            | HydroNode::Scan { input, .. }
            | HydroNode::ScanAsyncBlocking { input, .. }
            | HydroNode::Network { input, .. }
            | HydroNode::Counter { input, .. } => {
                self.visit_node(input, ancestor_commitments, passed_absorber);
            }

            // Leaf nodes — no children
            HydroNode::Source { .. }
            | HydroNode::SingletonSource { .. }
            | HydroNode::CycleSource { .. }
            | HydroNode::ExternalInput { .. }
            | HydroNode::Placeholder => {}
        }
    }
}

/// Extract the input node from a HydroRoot.
fn root_input(root: &HydroRoot) -> &HydroNode {
    match root {
        HydroRoot::ForEach { input, .. }
        | HydroRoot::SendExternal { input, .. }
        | HydroRoot::DestSink { input, .. }
        | HydroRoot::CycleSink { input, .. }
        | HydroRoot::EmbeddedOutput { input, .. }
        | HydroRoot::Null { input, .. } => input,
    }
}

/// Compute layers via Kahn's topological sort.
///
/// Layer 0: commitments with no dependencies.
/// Layer k: commitments whose latest dependency is in layer k-1.
fn compute_layers(
    genuine_commitments: &[NonDetId],
    dependency_edges: &[(NonDetId, NonDetId)],
) -> Vec<Vec<NonDetId>> {
    if genuine_commitments.is_empty() {
        return Vec::new();
    }

    let commitment_set: HashSet<NonDetId> = genuine_commitments.iter().copied().collect();

    // Build in-degree map and successor lists
    let mut in_degree: HashMap<NonDetId, usize> = HashMap::new();
    let mut successors: HashMap<NonDetId, Vec<NonDetId>> = HashMap::new();

    for &id in genuine_commitments {
        in_degree.insert(id, 0);
        successors.insert(id, Vec::new());
    }

    for &(a, b) in dependency_edges {
        if commitment_set.contains(&a) && commitment_set.contains(&b) {
            *in_degree.entry(b).or_default() += 1;
            successors.entry(a).or_default().push(b);
        }
    }

    // Kahn's algorithm with layer tracking
    let mut layers: Vec<Vec<NonDetId>> = Vec::new();
    let mut queue: VecDeque<NonDetId> = VecDeque::new();

    for &id in genuine_commitments {
        if in_degree[&id] == 0 {
            queue.push_back(id);
        }
    }

    while !queue.is_empty() {
        let layer: Vec<NonDetId> = queue.drain(..).collect();

        for &node in &layer {
            for &succ in &successors[&node] {
                let deg = in_degree.get_mut(&succ).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(succ);
                }
            }
        }

        layers.push(layer);
    }

    layers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::ir::*;
    use crate::compile::ir::backtrace::Backtrace;
    use crate::location::dynamic::LocationId;
    use crate::location::LocationKey;
    use slotmap::SlotMap;

    /// Helper to create minimal metadata with a given collection kind.
    fn make_metadata(kind: CollectionKind) -> HydroIrMetadata {
        let mut map: SlotMap<LocationKey, ()> = SlotMap::with_key();
        let key = map.insert(());
        HydroIrMetadata {
            location_id: LocationId::Process(key),
            collection_kind: kind,
            cardinality: None,
            tag: None,
            op: dummy_op_metadata(),
        }
    }

    fn dummy_op_metadata() -> HydroIrOpMetadata {
        HydroIrOpMetadata {
            backtrace: Backtrace::get_backtrace(0),
            cpu_usage: None,
            network_recv_cpu_usage: None,
            id: None,
        }
    }

    fn stream_no_order() -> CollectionKind {
        CollectionKind::Stream {
            bound: BoundKind::Unbounded,
            order: StreamOrder::NoOrder,
            retry: StreamRetry::ExactlyOnce,
            element_type: DebugType(Box::new(syn::parse_quote!(()))),
        }
    }

    fn stream_total_order() -> CollectionKind {
        CollectionKind::Stream {
            bound: BoundKind::Unbounded,
            order: StreamOrder::TotalOrder,
            retry: StreamRetry::ExactlyOnce,
            element_type: DebugType(Box::new(syn::parse_quote!(()))),
        }
    }

    fn dummy_expr() -> DebugExpr {
        DebugExpr(Box::new(syn::parse_quote!(|| ())))
    }

    /// Example 1: Source → ObserveNonDet → Fold(input NoOrder) → Output
    /// The fold absorbs the nondet → depth 0
    #[test]
    fn example1_monotone_accumulation_depth_0() {
        let source = HydroNode::Source {
            source: HydroSource::Embedded(syn::parse_quote!(input)),
            metadata: make_metadata(stream_no_order()),
        };

        let nondet = HydroNode::ObserveNonDet {
            inner: Box::new(source),
            trusted: false,
            metadata: make_metadata(stream_no_order()),
        };

        // The fold's INPUT (nondet) has NoOrder → fold is commutative → absorbs
        let fold = HydroNode::Fold {
            init: dummy_expr(),
            acc: dummy_expr(),
            input: Box::new(nondet),
            metadata: make_metadata(stream_no_order()),
        };

        let root = HydroRoot::ForEach {
            f: dummy_expr(),
            input: Box::new(fold),
            op_metadata: dummy_op_metadata(),
        };

        let result = analyze_depth(&[root]);
        assert_eq!(result.depth, 0, "Monotone accumulation should be depth 0");
        assert_eq!(result.nondet_points.len(), 1);
        assert!(result.nondet_points[0].absorbed);
        assert!(result.genuine_commitments.is_empty());
    }

    /// Example 2: Source → ObserveNonDet → Fold(input TotalOrder) → Output
    /// The fold does NOT absorb (order matters) → depth 1
    #[test]
    fn example2_single_quorum_depth_1() {
        let source = HydroNode::Source {
            source: HydroSource::Embedded(syn::parse_quote!(input)),
            metadata: make_metadata(stream_total_order()),
        };

        let nondet = HydroNode::ObserveNonDet {
            inner: Box::new(source),
            trusted: false,
            metadata: make_metadata(stream_total_order()),
        };

        // The fold's INPUT (nondet) has TotalOrder → fold is NOT commutative → does not absorb
        let fold = HydroNode::Fold {
            init: dummy_expr(),
            acc: dummy_expr(),
            input: Box::new(nondet),
            metadata: make_metadata(stream_total_order()),
        };

        let root = HydroRoot::ForEach {
            f: dummy_expr(),
            input: Box::new(fold),
            op_metadata: dummy_op_metadata(),
        };

        let result = analyze_depth(&[root]);
        assert_eq!(result.depth, 1, "Leader election should be depth 1");
        assert_eq!(result.nondet_points.len(), 1);
        assert!(!result.nondet_points[0].absorbed);
        assert_eq!(result.genuine_commitments.len(), 1);
    }

    /// Example 3: Two nondet points where one depends on the other.
    /// Source1 → ObserveNonDet(A) → Fold(TotalOrder, not absorbing)
    ///   ↘ CrossSingleton with...
    /// Source2 → ObserveNonDet(B) → Fold(TotalOrder, not absorbing) → Output
    ///
    /// Both A and B are genuine. B is in A's input subtree? No — let's think:
    /// The tree is: Output ← Fold ← CrossSingleton(left: ← NonDet(B) ← Source2,
    ///                                              right: ← Fold ← NonDet(A) ← Source1)
    /// Walking root→leaf: we hit NonDet(B) first (left branch), then NonDet(A) (right branch via Fold).
    /// NonDet(A) is deeper in the tree = more upstream.
    /// The output depends on both. B's subtree doesn't contain A.
    /// But A's output feeds through the right side of CrossSingleton into the context where B operates.
    ///
    /// Actually in the tree representation (root→leaf):
    /// Root → Fold_outer → CrossSingleton(left=NonDet(B)→Source2, right=Fold_inner→NonDet(A)→Source1)
    ///
    /// When we visit CrossSingleton, we visit both children with the same ancestor state.
    /// NonDet(B) is found on the left. NonDet(A) is found on the right.
    /// Neither is in the other's subtree — they're siblings under CrossSingleton.
    ///
    /// For B to depend on A, we need A's OUTPUT to be in B's INPUT subtree.
    /// In a tree, that means A must be a descendant of B.
    /// Here they're siblings — so NO dependency is detected. That gives depth 1 (both in same layer).
    ///
    /// To get depth 2, A must be in B's subtree. Let's restructure:
    /// Root → Fold_outer → NonDet(B) → Fold_inner → NonDet(A) → Source
    /// Now A is a descendant of B. When we visit B, we add B to ancestors.
    /// When we then visit A, A records that all ancestors (B) depend on A.
    /// Edge: (A, B) — B depends on A.
    /// Both non-absorbed (both folds have TotalOrder input).
    /// Depth = 2.
    #[test]
    fn example3_sequential_slots_depth_2() {
        let source = HydroNode::Source {
            source: HydroSource::Embedded(syn::parse_quote!(input)),
            metadata: make_metadata(stream_total_order()),
        };

        // NonDet A (upstream / deeper in tree)
        let nondet_a = HydroNode::ObserveNonDet {
            inner: Box::new(source),
            trusted: false,
            metadata: make_metadata(stream_total_order()),
        };

        // Fold between A and B (not absorbing: TotalOrder input)
        let fold_inner = HydroNode::Fold {
            init: dummy_expr(),
            acc: dummy_expr(),
            input: Box::new(nondet_a),
            metadata: make_metadata(stream_total_order()),
        };

        // NonDet B (downstream of A / closer to root in tree)
        let nondet_b = HydroNode::ObserveNonDet {
            inner: Box::new(fold_inner),
            trusted: false,
            metadata: make_metadata(stream_total_order()),
        };

        // Outer fold (not absorbing)
        let fold_outer = HydroNode::Fold {
            init: dummy_expr(),
            acc: dummy_expr(),
            input: Box::new(nondet_b),
            metadata: make_metadata(stream_total_order()),
        };

        let root = HydroRoot::ForEach {
            f: dummy_expr(),
            input: Box::new(fold_outer),
            op_metadata: dummy_op_metadata(),
        };

        let result = analyze_depth(&[root]);
        assert_eq!(result.depth, 2, "Sequential slots should be depth 2");
        assert_eq!(result.nondet_points.len(), 2);
        assert_eq!(result.genuine_commitments.len(), 2);
        assert_eq!(result.dependency_edges.len(), 1);
        // Edge should be (A_id, B_id) meaning B depends on A
        assert_eq!(result.layers.len(), 2);
    }

    // =========================================================================
    // End-to-end tests: Hydro DSL → IR → analyze_depth
    //
    // These use source_iter (no sim feature needed) and finalize() to get IR.
    // =========================================================================

    use crate::prelude::*;
    use stageleft::q;

    /// End-to-end Example 1: Pure monotone accumulation.
    /// A simple source_iter → map → for_each pipeline with no nondet.
    /// Expected: depth 0 (no ObserveNonDet nodes at all).
    #[test]
    fn e2e_monotone_accumulation_depth_0() {
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();

        // Simple pipeline: no batching, no nondet
        process
            .source_iter(q!(0..10))
            .map(q!(|x| x * 2))
            .for_each(q!(|v| println!("{}", v)));

        let built = flow.finalize();
        let result = analyze_depth(built.ir());

        // No ObserveNonDet nodes in this simple pipeline → depth 0
        assert_eq!(
            result.depth, 0,
            "Simple source_iter → map → for_each should be depth 0. Got: {:?}",
            result
        );
    }

    /// End-to-end Example 2: Batching with non-commutative downstream.
    /// source_iter → batch (introduces nondet) → non-commutative fold (first wins).
    /// The batch nondet should NOT be absorbed.
    /// Expected: depth >= 1.
    #[test]
    fn e2e_batched_first_wins_depth_1() {
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let tick = process.tick();

        process
            .source_iter(q!(vec![1i32, 2, 3]))
            .batch(&tick, nondet!(/** test: batch boundary */))
            .fold(
                q!(|| None::<i32>),
                q!(|first, v| { if first.is_none() { *first = Some(v); } }),
            )
            .all_ticks()
            .for_each(q!(|v| println!("{:?}", v)));

        let built = flow.finalize();
        let result = analyze_depth(built.ir());

        // The batch introduces ObserveNonDet; fold is over TotalOrder input
        // (batch preserves order) → not absorbed → depth >= 1
        assert!(
            result.depth >= 1,
            "Batched first-wins should be depth >= 1. Got: {:?}",
            result
        );
    }

    /// End-to-end Example 3: Two batches where second depends on first.
    /// First batch decides a value, which feeds (via cross_singleton) into
    /// the context of the second batch's fold.
    /// Expected: depth >= 2.
    #[test]
    fn e2e_sequential_batches_depth_2() {
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let tick = process.tick();

        // Stream 1 → batch → fold (non-commutative) → produces threshold
        let threshold = process
            .source_iter(q!(vec![2usize, 3, 2]))
            .batch(&tick, nondet!(/** slot 1: which values are batched */))
            .fold(
                q!(|| 0usize),
                q!(|first, v| { if *first == 0 { *first = v; } }),
            );

        // Stream 2 → batch → cross_singleton(threshold) → fold (non-commutative)
        process
            .source_iter(q!(vec![10i32, 20, 30]))
            .batch(&tick, nondet!(/** slot 2: which ops are batched */))
            .cross_singleton(threshold)
            .fold(
                q!(|| None::<(i32, usize)>),
                q!(|first, v| { if first.is_none() { *first = Some(v); } }),
            )
            .all_ticks()
            .for_each(q!(|v| println!("{:?}", v)));

        let built = flow.finalize();
        let result = analyze_depth(built.ir());

        // Two nondet points (two batch calls). The second batch's fold
        // receives data crossed with the first batch's fold output.
        // Dependency: second depends on first → depth >= 2.
        assert!(
            result.depth >= 2,
            "Sequential batches with cross-dependency should be depth >= 2. Got: {:?}",
            result
        );
    }
}
