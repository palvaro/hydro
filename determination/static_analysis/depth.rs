//! Depth computation from the commitment dependency graph.
//!
//! Given a set of genuine commitments and the dependency edges between them,
//! compute the layer assignment (topological ordering) and the determination depth.

use super::NodeId;
use std::collections::{HashMap, HashSet, VecDeque};

/// Compute layers of commitments via topological sort.
///
/// Layer 0: commitments with no dependencies on other commitments (sources).
/// Layer k: commitments whose latest dependency is in layer k-1.
///
/// The depth is the number of layers (0 means no genuine commitments).
///
/// Panics if the dependency graph contains a cycle (which would indicate
/// a bug in the analysis — circular commitment dependencies shouldn't exist
/// in a well-formed dataflow).
pub fn compute_layers(
    genuine_commitments: &[NodeId],
    dependency_edges: &[(NodeId, NodeId)],
) -> Vec<Vec<NodeId>> {
    if genuine_commitments.is_empty() {
        return Vec::new();
    }

    let commitment_set: HashSet<NodeId> = genuine_commitments.iter().copied().collect();

    // Build adjacency lists: predecessors[B] = set of A where (A, B) is an edge
    // (meaning B depends on A's output)
    let mut predecessors: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
    let mut successors: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

    for &id in genuine_commitments {
        predecessors.entry(id).or_default();
        successors.entry(id).or_default();
    }

    for &(a, b) in dependency_edges {
        // Edge (A, B) means B depends on A
        if commitment_set.contains(&a) && commitment_set.contains(&b) {
            predecessors.entry(b).or_default().insert(a);
            successors.entry(a).or_default().insert(b);
        }
    }

    // Kahn's algorithm for topological sort with layer tracking
    let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
    for &id in genuine_commitments {
        in_degree.insert(id, predecessors.get(&id).map_or(0, |s| s.len()));
    }

    let mut layers: Vec<Vec<NodeId>> = Vec::new();
    let mut queue: VecDeque<NodeId> = VecDeque::new();

    // Start with all nodes that have in-degree 0
    for &id in genuine_commitments {
        if in_degree[&id] == 0 {
            queue.push_back(id);
        }
    }

    let mut processed = 0;

    while !queue.is_empty() {
        // All nodes currently in the queue form one layer
        let layer: Vec<NodeId> = queue.drain(..).collect();
        processed += layer.len();

        // Find next layer: reduce in-degrees for successors
        for &node in &layer {
            if let Some(succs) = successors.get(&node) {
                for &succ in succs {
                    let deg = in_degree.get_mut(&succ).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(succ);
                    }
                }
            }
        }

        layers.push(layer);
    }

    assert_eq!(
        processed,
        genuine_commitments.len(),
        "Cycle detected in commitment dependency graph — this indicates a bug"
    );

    layers
}

/// Compute the determination depth from layers.
/// This is simply the number of layers.
pub fn depth_from_layers(layers: &[Vec<NodeId>]) -> usize {
    layers.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_commitments_depth_zero() {
        let layers = compute_layers(&[], &[]);
        assert_eq!(layers.len(), 0);
    }

    #[test]
    fn single_commitment_depth_one() {
        let layers = compute_layers(&[1], &[]);
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0], vec![1]);
    }

    #[test]
    fn two_independent_commitments_depth_one() {
        // No edges between them — they commute (same layer)
        let layers = compute_layers(&[1, 2], &[]);
        assert_eq!(layers.len(), 1);
        assert!(layers[0].contains(&1));
        assert!(layers[0].contains(&2));
    }

    #[test]
    fn two_dependent_commitments_depth_two() {
        // Edge (1, 2): commitment 2 depends on commitment 1
        let layers = compute_layers(&[1, 2], &[(1, 2)]);
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0], vec![1]);
        assert_eq!(layers[1], vec![2]);
    }

    #[test]
    fn diamond_dependency_depth_three() {
        // 1 → 2, 1 → 3, 2 → 4, 3 → 4
        let layers = compute_layers(&[1, 2, 3, 4], &[(1, 2), (1, 3), (2, 4), (3, 4)]);
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec![1]);
        assert!(layers[1].contains(&2));
        assert!(layers[1].contains(&3));
        assert_eq!(layers[2], vec![4]);
    }

    #[test]
    fn chain_of_three_depth_three() {
        // 1 → 2 → 3
        let layers = compute_layers(&[1, 2, 3], &[(1, 2), (2, 3)]);
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec![1]);
        assert_eq!(layers[1], vec![2]);
        assert_eq!(layers[2], vec![3]);
    }

    #[test]
    #[should_panic(expected = "Cycle detected")]
    fn cycle_panics() {
        // 1 → 2, 2 → 1 (cycle)
        compute_layers(&[1, 2], &[(1, 2), (2, 1)]);
    }

    /// Matches our Example 1: monotone_accumulation — no commitments
    #[test]
    fn example1_monotone_accumulation() {
        let layers = compute_layers(&[], &[]);
        assert_eq!(depth_from_layers(&layers), 0);
    }

    /// Matches our Example 2: single_quorum — one commitment, no dependencies
    #[test]
    fn example2_single_quorum() {
        // One genuine commitment (the batch boundary that determines leader)
        let layers = compute_layers(&[1], &[]);
        assert_eq!(depth_from_layers(&layers), 1);
    }

    /// Matches our Example 3: sequential_slots — two dependent commitments
    #[test]
    fn example3_sequential_slots() {
        // Commitment 1 (slot 1 batch) → Commitment 2 (slot 2 batch)
        let layers = compute_layers(&[1, 2], &[(1, 2)]);
        assert_eq!(depth_from_layers(&layers), 2);
    }
}
