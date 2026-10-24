p='hydrofrac/src/provenance/fragile.rs'
s=open(p).read()
a=s.index('//! Fragile:')
b=s.index('use std::collections::{HashMap, HashSet};')
s=s[:a]+'''//! **An allowlist.** An operator is robust only if [`View::fragile`] names it, with the reason it
//! is insensitive to arrival order and batching. Every other operator is fragile, including node
//! kinds this module does not know (a new IR kind is fragile until someone proves otherwise).
//! The robust entries, each justified in [`View::fragile`]:
//! - plumbing that passes every record through unchanged (casts, tees, references, atomic
//!   brackets, consistency assertions, counters, `observe_nondet`, cycle sources, `defer_tick`,
//!   `all_ticks`);
//! - element-wise operators with pure closures (`map`, `flat_map`, `filter`, `filter_map`,
//!   `inspect`, partitions);
//! - set-preserving merges and reorderings (`chain`, `merge_ordered`, `sort`, `unique`);
//! - networks, a `batch` of a stream, and sources that are the harness's inputs or fixed
//!   (iterators, singleton sources, cluster members, external inputs);
//! - joins and cross products over a whole run (top level), and a half join at top level, whose
//!   bounded build side is complete before it is probed;
//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::is_dedup`);
//! - any structural step whose every input is *constant* (`View::constant`): it computes the same
//!   records in every schedule. This is what keeps `use::state`'s initial value
//!   (`initial.filter_if(first_tick)`) robust;
//! - the steps inside a trusted `quorum` (`super::quorum`), which [`classify`] skips.
//!
//! Everything else is fragile. That includes: negation, `cross_singleton` (so `filter_if`),
//! `enumerate`, `chain_first`; folds, reduces and scans anywhere, since even at top level their
//! input order can be a schedule choice (`assume_ordering` at top level is a simulator hook); joins
//! inside a tick; snapshots (a `batch` of a singleton, optional or keyed singleton); closures that
//! read Hydro state by reference; and other sources (timers, intervals, streams).
//!
//! Opaque islands (async and versioned operators, which the rewrite cannot see into) get no tag:
//! lineage treats every record through an opaque island as fragile, and an island makes the
//! program not fragile-free.

'''+s[b:]
a=s.index('    /// What fragile step `node` is, or `None` if it is not one.')
b=s.index('    /// Whether `ptr`\'s records are the same in every schedule')
s=s[:a]+'''    /// What fragile step `node` is, or `None` if it is robust (or opaque). An allowlist: every
    /// robust arm says why it is insensitive to arrival order and batching, and anything not
    /// listed is fragile.
    fn fragile(&self, node: &HydroNode) -> Option<&'static str> {
        use HydroNode::*;
        if reads_state(node) {
            // A closure that reads Hydro state sees whatever the state holds at that moment.
            return Some("closure_over_state");
        }
        let top_level = node.metadata().location_id.is_top_level();
        let robust = match node {
            // Plumbing: every record passes through unchanged (or, for `observe_nondet`, the
            // records are reordered, never added or dropped).
            Cast { .. }
            | ObserveNonDet { .. }
            | Tee { .. }
            | Reference { .. }
            | BeginAtomic { .. }
            | EndAtomic { .. }
            | AssertIsConsistent { .. }
            | Counter { .. }
            | UnboundSingleton { .. }
            | CycleSource { .. }
            | DeferTick { .. }
            | YieldConcat { .. } => true,
            // Element-wise with a pure closure (`reads_state` is false here): each output is a
            // function of one input record.
            Map { .. } | FlatMap { .. } | Filter { .. } | FilterMap { .. } | Inspect { .. } => true,
            // The partition closure runs at the shared node; its outputs leave through the sides.
            PartitionSide { inner, .. } => {
                return reads_state(&inner.0.borrow()).then_some("closure_over_state");
            }
            PartitionShared { .. } => true,
            // Set-preserving: the records over the run are the union (or the same set reordered,
            // or the distinct ones) of the inputs', however they were split into ticks.
            Chain { .. } | MergeOrdered { .. } | Sort { .. } | Unique { .. } => true,
            // A network delivers each send's payload; faults, not schedules, remove deliveries.
            Network { .. } => true,
            // A batch of a stream decides when a record enters a tick, not whether.
            Batch { inner, .. } => !matches!(
                inner.metadata().collection_kind,
                CollectionKind::Singleton { .. }
                    | CollectionKind::Optional { .. }
                    | CollectionKind::KeyedSingleton { .. }
            ),
            // The harness's inputs, fixed values, and the (static) cluster membership. Decided
            // here: a source has no inputs, so the constant-inputs exemption below would hold
            // vacuously.
            Source { source, .. } => {
                return (!matches!(
                    source,
                    HydroSource::Iter(_)
                        | HydroSource::ClusterMembers(..)
                        | HydroSource::ExternalNetwork()
                ))
                .then_some("source");
            }
            SingletonSource { .. } | ExternalInput { .. } => true,
            // Over a whole run, a symmetric join emits every matching pair once both sides have
            // arrived; a half join's bounded build side is complete before it is probed.
            Join { .. } | CrossProduct { .. } | JoinHalf { .. } => top_level,
            // The dedup idiom: every record of `pos` not in the constant initial state.
            Difference { .. } => self.is_dedup(node),
            // Opaque islands: no tag (lineage treats their records as fragile, see the module
            // docs).
            ResolveFutures { .. }
            | ResolveFuturesBlocking { .. }
            | ResolveFuturesOrdered { .. }
            | FlatMapStreamBlocking { .. }
            | ScanAsyncBlocking { .. }
            | ReduceKeyedWatermark { .. }
            | VersionedNetwork { .. }
            | VersionedNetworkFork { .. } => return None,
            Placeholder => return None,
            _ => false,
        };
        if robust {
            return None;
        }
        // Same records in every schedule if every input is (every node here has an input).
        if inputs(node).into_iter().all(|i| self.constant(i)) {
            return None;
        }
        Some(match node {
            AntiJoin { .. } => "anti_join",
            Difference { .. } => "difference",
            CrossSingleton { .. } => "cross_singleton",
            CrossProduct { .. } => "cross_product",
            Join { .. } => "join",
            JoinHalf { .. } => "join_half",
            Fold { .. } => "fold",
            FoldKeyed { .. } => "fold_keyed",
            Reduce { .. } => "reduce",
            ReduceKeyed { .. } => "reduce_keyed",
            Scan { .. } => "scan",
            Enumerate { .. } => "enumerate",
            ChainFirst { .. } => "chain_first",
            Batch { .. } => "snapshot",
            _ => "unlisted",
        })
    }

'''+s[b:]
open(p,'w').write(s)
