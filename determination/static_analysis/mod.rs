//! Static analysis for determination depth tracking.
//!
//! Analyzes the HydroNode IR tree to compute how many sequential layers of
//! coordination (determination depth) a program requires.
//!
//! The analysis identifies `ObserveNonDet` nodes as potential commitment points,
//! determines which are genuine (their nondeterminism propagates to the output),
//! and computes the dependency structure among genuine commitments.

pub mod absorption;
pub mod depth;
pub mod graph;

/// A unique identifier for a node in the analysis graph.
/// In practice this would be derived from the node's pointer or metadata ID.
pub type NodeId = usize;

/// Information about a single nondet point found in the IR.
#[derive(Debug, Clone)]
pub struct NonDetPoint {
    /// Unique identifier for this nondet point.
    pub id: NodeId,
    /// Whether the nondet is marked as `trusted` (meaning simulation skips it).
    pub trusted: bool,
    /// Human-readable description (derived from source location / backtrace).
    pub description: String,
    /// Whether this nondet point's nondeterminism is absorbed by downstream
    /// composition (making it depth-0) or propagates to an output (genuine commitment).
    pub absorbed: bool,
}

/// The result of the full determination depth analysis.
#[derive(Debug, Clone)]
pub struct DepthAnalysis {
    /// All nondet points found in the IR tree.
    pub nondet_points: Vec<NonDetPoint>,
    /// IDs of nondet points that are genuine commitments (not absorbed).
    pub genuine_commitments: Vec<NodeId>,
    /// Dependency edges: (A, B) means commitment B depends on commitment A's output.
    pub dependency_edges: Vec<(NodeId, NodeId)>,
    /// Commitments grouped by layer. Layer 0 = no dependencies on other commitments.
    /// Layer k = depends on at least one commitment in layer k-1.
    pub layers: Vec<Vec<NodeId>>,
    /// The determination depth: number of layers (0 = fully monotone, no commitments).
    pub depth: usize,
}

/// Entry point: analyze a program's IR to compute determination depth.
///
/// Takes a list of IR roots (the output interface of the program) and returns
/// the full depth analysis.
///
/// # Algorithm
///
/// 1. Walk the IR tree from each root, collecting all `ObserveNonDet` nodes.
/// 2. For each nondet node, trace paths to outputs and check for absorption.
/// 3. Build a dependency graph among genuine (non-absorbed) commitments.
/// 4. Compute layers via topological sort of the dependency graph.
pub fn analyze_depth(roots: &[graph::AnalysisNode]) -> DepthAnalysis {
    // Phase 1: Collect all nondet points
    let nondet_points = graph::collect_nondet_points(roots);

    // Phase 2: Determine which are genuine commitments (not absorbed)
    let genuine_commitments: Vec<NodeId> = nondet_points
        .iter()
        .filter(|p| !p.absorbed)
        .map(|p| p.id)
        .collect();

    // Phase 3: Compute dependency edges among genuine commitments
    let dependency_edges = graph::compute_dependencies(roots, &genuine_commitments);

    // Phase 4: Compute layers from dependency graph
    let layers = depth::compute_layers(&genuine_commitments, &dependency_edges);
    let depth = layers.len();

    DepthAnalysis {
        nondet_points,
        genuine_commitments,
        dependency_edges,
        layers,
        depth,
    }
}
