//! Example 1: Monotone Accumulation (Depth 0)
//!
//! A cluster of nodes receives values and accumulates them via set union.
//! Each node broadcasts its local values to all others and folds everything
//! into a growing set. This is purely monotone — no commitments, no
//! coordination needed.
//!
//! Determination depth: 0 (all outputs are robust)

use std::collections::HashSet;
use std::hash::Hash;

use hydro_lang::prelude::*;

/// Each node in the cluster accumulates values into a set via broadcast + union.
/// The output (the set) only grows — it is monotone under set inclusion.
pub fn monotone_set_accumulation<'a, T: Clone + Eq + Hash + 'static>(
    cluster: &Cluster<'a, ()>,
    local_values: Stream<T, Cluster<'a, ()>, Unbounded>,
) -> Singleton<HashSet<T>, Cluster<'a, ()>, Unbounded> {
    // Broadcast local values to all peers
    let all_values = local_values
        .broadcast_bincode(cluster)
        .chain(local_values);

    // Fold into a growing set — this is a lattice join (set union)
    all_values.fold(
        q!(|| HashSet::new()),
        q!(|set, value| { set.insert(value); }),
    )
}
