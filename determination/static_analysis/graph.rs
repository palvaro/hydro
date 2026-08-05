//! Graph extraction and traversal for the determination depth analysis.
//!
//! This module extracts a simplified dependency graph from the HydroNode IR tree.
//! The simplified graph contains two kinds of nodes:
//! - Nondet points (potential commitment sites)
//! - Output points (roots of the dataflow)
//!
//! Edges between them are annotated with path properties (notably: whether an
//! absorbing operator lies on the path).

use super::{NodeId, NonDetPoint};

/// A simplified representation of an IR node for analysis purposes.
/// In a full integration, this would reference the actual HydroNode;
/// here we use a simplified structure that captures the relevant properties.
#[derive(Debug, Clone)]
pub enum AnalysisNode {
    /// A source of nondeterminism (ObserveNonDet in the IR).
    NonDet {
        id: NodeId,
        trusted: bool,
        description: String,
        children: Vec<AnalysisNode>,
    },
    /// A fold/reduce operator that may absorb nondeterminism.
    Fold {
        id: NodeId,
        /// Whether this fold is known to be commutative
        /// (input stream is NoOrder, or manual_proof! declares commutativity).
        commutative: bool,
        children: Vec<AnalysisNode>,
    },
    /// A generic passthrough operator (map, filter, etc.) that neither
    /// introduces nor absorbs nondeterminism.
    Passthrough {
        id: NodeId,
        children: Vec<AnalysisNode>,
    },
    /// An operator that introduces non-monotonicity (Difference, AntiJoin).
    /// These do NOT absorb nondeterminism — they may amplify it.
    NonMonotone {
        id: NodeId,
        children: Vec<AnalysisNode>,
    },
    /// An output root (ForEach, DestSink, etc.).
    Output {
        id: NodeId,
    },
    /// A source node (no children in the backwards direction).
    Source {
        id: NodeId,
    },
}

impl AnalysisNode {
    pub fn id(&self) -> NodeId {
        match self {
            AnalysisNode::NonDet { id, .. }
            | AnalysisNode::Fold { id, .. }
            | AnalysisNode::Passthrough { id, .. }
            | AnalysisNode::NonMonotone { id, .. }
            | AnalysisNode::Output { id }
            | AnalysisNode::Source { id } => *id,
        }
    }

    pub fn children(&self) -> &[AnalysisNode] {
        match self {
            AnalysisNode::NonDet { children, .. }
            | AnalysisNode::Fold { children, .. }
            | AnalysisNode::Passthrough { children, .. }
            | AnalysisNode::NonMonotone { children, .. } => children,
            AnalysisNode::Output { .. } | AnalysisNode::Source { .. } => &[],
        }
    }
}

/// Collect all nondet points in the analysis graph, with absorption info.
///
/// Walks the graph from roots, finds all NonDet nodes, and for each one
/// checks whether all paths to outputs go through an absorber.
pub fn collect_nondet_points(roots: &[AnalysisNode]) -> Vec<NonDetPoint> {
    let mut result = Vec::new();
    let mut visited = std::collections::HashSet::new();
    for root in roots {
        collect_nondet_recursive(root, &mut result, &mut visited, roots);
    }
    result
}

fn collect_nondet_recursive(
    node: &AnalysisNode,
    result: &mut Vec<NonDetPoint>,
    visited: &mut std::collections::HashSet<NodeId>,
    roots: &[AnalysisNode],
) {
    if !visited.insert(node.id()) {
        return;
    }

    if let AnalysisNode::NonDet { id, trusted, description, .. } = node {
        // Check absorption: trace from this nondet point forward to all outputs.
        // If every path to an output goes through an absorber, it's absorbed.
        let absorbed = all_paths_absorbed(node, roots);
        result.push(NonDetPoint {
            id: *id,
            trusted: *trusted,
            description: description.clone(),
            absorbed,
        });
    }

    for child in node.children() {
        collect_nondet_recursive(child, result, visited, roots);
    }
}

/// Check whether ALL paths from a nondet node to any output are absorbed.
///
/// Note: In the actual IR, data flows from inputs (leaves) to outputs (roots).
/// The AnalysisNode tree here is structured with children being the INPUTS
/// (matching HydroNode's structure where `inner`/`input` fields point upstream).
///
/// For the absorption check, we need to trace DOWNSTREAM from a nondet point
/// to the outputs. This requires inverting the graph or walking from roots.
///
/// Simplified approach: walk from each root toward leaves. Track whether we've
/// passed through an absorber since the last nondet point. If we reach a nondet
/// point and we're on a non-absorbed path, that nondet propagates.
fn all_paths_absorbed(nondet_node: &AnalysisNode, roots: &[AnalysisNode]) -> bool {
    // For each root, check if there's a path from nondet_node to that root
    // that does NOT pass through an absorber.
    //
    // We trace backwards from the root: if we encounter the nondet node
    // and haven't passed through an absorber, the path is non-absorbed.
    let nondet_id = nondet_node.id();

    for root in roots {
        if has_non_absorbed_path_to(root, nondet_id, false) {
            return false; // Found a non-absorbed path → NOT fully absorbed
        }
    }
    true // All paths absorbed
}

/// Recursively check if there's a path from `node` backward to `target_id`
/// that does NOT pass through an absorber.
///
/// `passed_absorber` tracks whether we've already passed through an absorber
/// on the current path (from root toward leaves).
///
/// Returns true if a non-absorbed path to the target exists.
fn has_non_absorbed_path_to(node: &AnalysisNode, target_id: NodeId, passed_absorber: bool) -> bool {
    // If this is the target nondet node...
    if node.id() == target_id {
        // Path is non-absorbed if we haven't passed through an absorber
        return !passed_absorber;
    }

    // Update absorption state based on current node
    let absorber_here = matches!(node, AnalysisNode::Fold { commutative: true, .. });
    let new_passed_absorber = passed_absorber || absorber_here;

    // Recurse into children (inputs — going toward leaves)
    for child in node.children() {
        if has_non_absorbed_path_to(child, target_id, new_passed_absorber) {
            return true;
        }
    }

    false
}

/// Compute dependency edges among genuine commitments.
///
/// Commitment B depends on commitment A if there is a dataflow path from
/// A's output to B's input. In the IR tree (which points upstream), this
/// means: B is an ancestor of A in the tree (A appears in B's input subtree).
///
/// Conservative: we say B depends on A if A appears anywhere in the subtree
/// rooted at B. This over-approximates (ignores key partitioning).
pub fn compute_dependencies(
    roots: &[AnalysisNode],
    genuine_commitments: &[NodeId],
) -> Vec<(NodeId, NodeId)> {
    let commitment_set: std::collections::HashSet<NodeId> =
        genuine_commitments.iter().copied().collect();
    let mut edges = Vec::new();

    // For each genuine commitment B, check if any other genuine commitment A
    // appears in B's input subtree. If so, B depends on A.
    for root in roots {
        find_dependencies_recursive(root, &commitment_set, &[], &mut edges);
    }

    edges.sort();
    edges.dedup();
    edges
}

/// Walk the tree. Maintain a stack of genuine commitments we've passed through
/// on the path from root to current node. When we encounter a genuine commitment,
/// it depends on all commitments further down in its subtree.
fn find_dependencies_recursive(
    node: &AnalysisNode,
    commitments: &std::collections::HashSet<NodeId>,
    ancestors: &[NodeId], // commitment ancestors on the path from root
    edges: &mut Vec<(NodeId, NodeId)>,
) {
    let node_id = node.id();
    let is_commitment = commitments.contains(&node_id);

    if is_commitment {
        // This commitment depends on any commitment that appears in its subtree.
        // Conversely, it is depended upon by any commitment ancestor above it.
        // But we're looking for "B depends on A" where A is in B's input subtree.
        //
        // Since we're walking root→leaves and the tree points inputs as children,
        // if we're at commitment B and we find commitment A below us, then
        // A is in B's input subtree → B depends on A's output.
        //
        // We record: for each commitment ancestor above us, THEY depend on US.
        // Wait — no. The tree goes root→leaves = output→input.
        // Children are INPUTS. If commitment B has commitment A as a descendant,
        // that means A feeds into B. So B depends on A.
        //
        // Actually the flow is: leaves(sources) → roots(outputs).
        // Children in the tree = upstream inputs.
        // If commitment A is a descendant of commitment B in the tree,
        // then A is upstream of B — A's output feeds into B's input.
        // Therefore B depends on A.

        // Add edges: for each ancestor commitment C above us on the path from root,
        // C appears downstream of us. We (this node) are upstream of C.
        // So C depends on us: edge (us → C) meaning "C depends on us".
        for &ancestor_id in ancestors {
            edges.push((node_id, ancestor_id)); // ancestor depends on us
        }
    }

    // Build new ancestors list for recursion
    let new_ancestors: Vec<NodeId> = if is_commitment {
        let mut a = ancestors.to_vec();
        a.push(node_id);
        a
    } else {
        ancestors.to_vec()
    };

    for child in node.children() {
        find_dependencies_recursive(child, commitments, &new_ancestors, edges);
    }
}

// =============================================================================
// Conversion from actual HydroNode IR (sketch — requires integration)
// =============================================================================

/// TODO: Convert an actual HydroNode tree into our AnalysisNode representation.
///
/// This would walk the HydroNode enum and produce AnalysisNode variants:
/// - HydroNode::ObserveNonDet → AnalysisNode::NonDet
/// - HydroNode::Fold with NoOrder input → AnalysisNode::Fold { commutative: true }
/// - HydroNode::Fold with TotalOrder input → AnalysisNode::Fold { commutative: false }
/// - HydroNode::Difference / AntiJoin → AnalysisNode::NonMonotone
/// - HydroNode::Map / Filter / FlatMap / etc. → AnalysisNode::Passthrough
/// - HydroRoot::* → AnalysisNode::Output
/// - HydroNode::Source / CycleSource / ExternalInput → AnalysisNode::Source
///
/// The `commutative` flag on Fold can also be set by inspecting the accumulator
/// expression for `manual_proof!` annotations containing "commutative".
///
/// ```rust,ignore
/// fn convert_hydro_node(node: &HydroNode, id_counter: &mut usize) -> AnalysisNode {
///     let id = *id_counter;
///     *id_counter += 1;
///     match node {
///         HydroNode::ObserveNonDet { inner, trusted, metadata } => {
///             AnalysisNode::NonDet {
///                 id,
///                 trusted: *trusted,
///                 description: format!("{:?}", metadata.op.backtrace),
///                 children: vec![convert_hydro_node(inner, id_counter)],
///             }
///         }
///         HydroNode::Fold { input, metadata, .. } => {
///             let commutative = match &metadata.collection_kind {
///                 CollectionKind::Stream { order: StreamOrder::NoOrder, .. } => true,
///                 _ => false,
///             };
///             // Note: check input's metadata, not the fold's output metadata
///             AnalysisNode::Fold {
///                 id,
///                 commutative,
///                 children: vec![convert_hydro_node(input, id_counter)],
///             }
///         }
///         HydroNode::Difference { pos, neg, .. }
///         | HydroNode::AntiJoin { pos, neg, .. } => {
///             AnalysisNode::NonMonotone {
///                 id,
///                 children: vec![
///                     convert_hydro_node(pos, id_counter),
///                     convert_hydro_node(neg, id_counter),
///                 ],
///             }
///         }
///         HydroNode::Source { .. }
///         | HydroNode::SingletonSource { .. }
///         | HydroNode::CycleSource { .. }
///         | HydroNode::ExternalInput { .. } => {
///             AnalysisNode::Source { id }
///         }
///         // All other nodes are passthrough
///         _ => {
///             let children = collect_children(node, id_counter);
///             AnalysisNode::Passthrough { id, children }
///         }
///     }
/// }
/// ```
pub fn convert_from_ir() {
    todo!("Integration with actual HydroNode IR — see doc comment above for sketch")
}
