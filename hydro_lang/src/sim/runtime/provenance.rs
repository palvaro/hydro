//! Record-level provenance for the simulator (LDFI "Level 2" lineage).
//!
//! When a simulation is compiled with provenance (`SimFlow::with_provenance`), a sim-only IR
//! rewrite (`sim::provenance`) wraps every collection element in [`Tagged`]: the value plus
//! the set of **delivery ids** it was computed from. Each item a scripted batch releases into
//! a tick gets a fresh id (recorded as a [`LineageEvent::Tag`](super::LineageEvent::Tag));
//! operators propagate ids by what they actually did (a join unions both sides, a fold
//! accumulates every contribution, a map passes through); and a network send or external
//! output strips the tag from the wire payload and writes it into the `Sent` / `Output`
//! lineage event. The LDFI harness then follows record-to-record edges instead of
//! execution-to-execution ones.
//!
//! `Tagged<T>` compares, hashes, orders, prints, and serializes **as `T`**, so the program's
//! semantics (deduplication, joins, set difference, hook policies that match payload bytes)
//! are unchanged by the rewrite. Deduplicating operators keep the first provenance seen; the
//! alternatives surface in later LDFI runs, as they would with any single-derivation lineage.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A set of delivery ids, sorted and deduplicated.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Prov(Vec<u64>);

impl Prov {
    /// The delivery id standing for "unknown": the record came out of a closure whose reads
    /// the rewrite cannot see (a `Reference` capture), so its lineage must be derived by
    /// counterfactual replay of the node (see `sim::ldfi`, [`Level::Counterfactual`]).
    pub const OPAQUE: u64 = u64::MAX;

    /// No provenance (a constant, an external input, a network arrival at the top level).
    pub const fn empty() -> Self {
        Prov(Vec::new())
    }

    /// Exactly one delivery.
    pub fn single(id: u64) -> Self {
        Prov(vec![id])
    }

    /// The opaque provenance (see [`Self::OPAQUE`]).
    pub fn opaque() -> Self {
        Prov(vec![Self::OPAQUE])
    }

    /// Whether this provenance is opaque.
    pub fn is_opaque(&self) -> bool {
        self.0.last() == Some(&Self::OPAQUE)
    }

    /// Makes this provenance opaque.
    pub fn set_opaque(&mut self) {
        self.0 = vec![Self::OPAQUE];
    }

    /// The ids, ascending.
    pub fn ids(&self) -> &[u64] {
        &self.0
    }

    /// Adds every id of `other`.
    pub fn extend(&mut self, other: &Prov) {
        if other.0.is_empty() {
            return;
        }
        if self.0.is_empty() {
            self.0 = other.0.clone();
            return;
        }
        self.0.extend_from_slice(&other.0);
        self.0.sort_unstable();
        self.0.dedup();
    }

    /// Adds one id.
    pub fn insert(&mut self, id: u64) {
        if let Err(at) = self.0.binary_search(&id) {
            self.0.insert(at, id);
        }
    }

    /// The union of two sets.
    pub fn union(mut self, other: &Prov) -> Self {
        self.extend(other);
        self
    }
}

impl fmt::Debug for Prov {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(&self.0).finish()
    }
}

/// A value and the delivery ids it was computed from. Behaves as the value for every trait
/// the simulator or a DFIR operator may rely on (see the module docs).
#[derive(Clone, Default)]
pub struct Tagged<T>(pub T, pub Prov);

impl<T> Tagged<T> {
    /// A value with no provenance.
    pub fn empty(value: T) -> Self {
        Tagged(value, Prov::empty())
    }

    /// Splits into the value and its provenance.
    pub fn into_parts(self) -> (T, Prov) {
        (self.0, self.1)
    }

    /// The value.
    pub fn value(&self) -> &T {
        &self.0
    }

    /// The provenance.
    pub fn prov(&self) -> &Prov {
        &self.1
    }
}

impl<T: fmt::Debug> fmt::Debug for Tagged<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Tagged<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Tagged<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq> Eq for Tagged<T> {}

impl<T: Hash> Hash for Tagged<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<T: PartialOrd> PartialOrd for Tagged<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: Ord> Ord for Tagged<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T: serde::Serialize> serde::Serialize for Tagged<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Tagged<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Tagged::empty)
    }
}

/// Generated-code helpers for the provenance rewrite.
#[doc(hidden)]
pub mod codegen {
    use super::{Prov, Tagged};
    use crate::sim::runtime::LineageHandles;

    /// Tags a value released by a scripted batch with a fresh delivery id, recording it.
    pub fn tag_delivery<T: serde::Serialize>(
        lineage: &LineageHandles,
        hook_id: Option<usize>,
        member: Option<u32>,
        mut item: Tagged<T>,
    ) -> Tagged<T> {
        let bytes = bincode::serialize(&item.0).expect("tagged item should serialize");
        let id = lineage.record_tag(hook_id, member, bytes);
        item.1.insert(id);
        item
    }

    /// `(Tagged(a), Tagged(b))` → `Tagged((a, b))` with the union of both provenances.
    pub fn merge_pair<A, B>(pair: (Tagged<A>, Tagged<B>)) -> Tagged<(A, B)> {
        let (Tagged(a, pa), Tagged(b, pb)) = pair;
        Tagged((a, b), pa.union(&pb))
    }

    /// `Tagged((k, v))` → `(k, Tagged(v))`, for operators that key on the first component.
    pub fn key_out<K, V>(item: Tagged<(K, V)>) -> (K, Tagged<V>) {
        let Tagged((k, v), p) = item;
        (k, Tagged(v, p))
    }

    /// `(k, Tagged(v))` → `Tagged((k, v))`.
    pub fn key_in<K, V>(item: (K, Tagged<V>)) -> Tagged<(K, V)> {
        let (k, Tagged(v, p)) = item;
        Tagged((k, v), p)
    }

    /// `(k, (Tagged(a), Tagged(b)))` → `Tagged((k, (a, b)))` with the union, for joins.
    pub fn key_in_pair<K, A, B>(item: (K, (Tagged<A>, Tagged<B>))) -> Tagged<(K, (A, B))> {
        let (k, (Tagged(a, pa), Tagged(b, pb))) = item;
        Tagged((k, (a, b)), pa.union(&pb))
    }

    /// An empty provenance (for `Tagged::empty` in generated code).
    pub fn none() -> Prov {
        Prov::empty()
    }
}
