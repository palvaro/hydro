//! Record-level provenance for the simulator (LDFI "Level 2" lineage).
//!
//! When a simulation is compiled with provenance (`SimFlow::with_provenance`), a sim-only IR
//! rewrite (`sim::provenance`) wraps every collection element in [`Tagged`]: the value plus
//! the **delivery ids** it was computed from. Each send that reaches the wire has a delivery
//! id (minted by the filter, see [`LineageEvent::Sent`](super::LineageEvent::Sent)), which
//! the receive half attaches to the deserialized record; operators propagate ids by what they
//! actually did (a join unions both sides, a fold accumulates every contribution, a map
//! passes through); and a network send or external output strips the tag from the wire
//! payload and hands it to the lineage log, which records it with the `Sent` / `Output`
//! event. The LDFI harness then follows record-to-record edges instead of
//! execution-to-execution ones.
//!
//! `Tagged<T>` compares, hashes, orders, prints, and serializes **as `T`**, so the program's
//! semantics (deduplication, joins, set difference, hook policies that match payload bytes)
//! are unchanged by the rewrite.
//!
//! # Disjunction
//!
//! A [`Prov`] is a **formula** over delivery ids, not a set: a conjunction (`All`) of what a
//! record was computed from, and a disjunction (`Alts`) of the alternative derivations of
//! one value where a deduplicating operator saw several equal records. It is a shared,
//! lazily-resolved handle (`Rc<RefCell<..>>`): DFIR's `unique` is streaming, so the first
//! copy of a value has already been emitted (and perhaps sent) when its duplicate arrives in
//! the same tick, and the alternative can only reach the emitted record through a cell both
//! share. The rewrite inserts [`codegen::rebox`] before every `unique` inside a tick, giving
//! each arrival a fresh `Alts` cell shared by the stored copy and the emitted record alone;
//! [`PartialEq`] on `Tagged` — the only hook `unique`'s hash set gives us — pushes the
//! duplicate's alternatives into that cell when the values are equal. Absorption is opt-in
//! (only an `Alts` cell absorbs), so no other comparison site (a join key, an anti-join, a
//! `multiset_delta`) can create an alternative; a top-level (`'static`) `unique` is not
//! reboxed, since an alternative that arrives in a later execution is not a derivation of
//! what the earlier one produced.
//!
//! The formula is resolved to a minimal DNF of **witnesses** when the lineage log is read
//! ([`RecordProv::resolve`]), under a cap on the number of witnesses: a conjunction whose
//! cross product exceeds the cap falls back to the single union witness (every id of every
//! alternative, a superset of a real witness: sound, less pruning), a disjunction keeps its
//! first `cap` alternatives (dropping alternatives is sound). With a cap of 1 the result is
//! exactly the single derivation the first version of this module recorded (the first copy
//! wins at a dedup, everything unions at a combination), which is what
//! [`RecordProv::ids`] holds.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

/// One sufficient set of deliveries, sorted and deduplicated.
pub type Witness = Vec<u64>;

/// The default cap on witnesses per record (see the module docs).
pub const DEFAULT_MAX_WITNESSES: usize = 8;

enum Node {
    /// One delivery (or [`Prov::OPAQUE`]).
    Leaf(u64),
    /// Computed from all of these.
    All(Vec<Prov>),
    /// Equal values deduplicated: any of these suffices. The only kind of cell that absorbs
    /// (see [`Prov::absorb`]).
    Alts(Vec<Prov>),
}

/// The provenance of a record: a shared, lazily-resolved formula over delivery ids (see the
/// module docs). Cloning shares the handle, so records derived from one record see the
/// alternatives it later acquires.
#[derive(Clone, Default)]
pub struct Prov(Option<Rc<RefCell<Node>>>);

impl Prov {
    /// The delivery id standing for "unknown": the record came out of a closure whose reads
    /// the rewrite cannot see (a `Reference` capture), so its lineage must be derived by
    /// counterfactual replay of the node (see `sim::ldfi`, `Level::Record`).
    pub const OPAQUE: u64 = u64::MAX;

    /// No provenance (a constant, an external input, a network arrival at the top level).
    pub const fn empty() -> Self {
        Prov(None)
    }

    fn node(node: Node) -> Self {
        Prov(Some(Rc::new(RefCell::new(node))))
    }

    /// Exactly one delivery.
    pub fn single(id: u64) -> Self {
        Self::node(Node::Leaf(id))
    }

    /// The opaque provenance (see [`Self::OPAQUE`]).
    pub fn opaque() -> Self {
        Self::single(Self::OPAQUE)
    }

    /// Whether this provenance is empty (computed from nothing that came off a channel).
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Whether the single-derivation reading of this provenance is opaque.
    pub fn is_opaque(&self) -> bool {
        self.ids().last() == Some(&Self::OPAQUE)
    }

    /// Makes this provenance opaque.
    pub fn set_opaque(&mut self) {
        *self = Self::opaque();
    }

    /// The single-derivation reading: the ids of the first alternative at every dedup and
    /// the union at every combination, ascending (what the first version of this module
    /// recorded).
    pub fn ids(&self) -> Vec<u64> {
        RecordProv::resolve(self, 1).ids
    }

    /// Adds everything of `other`: this record was computed from both.
    pub fn extend(&mut self, other: &Prov) {
        let Some(theirs) = &other.0 else {
            return;
        };
        let Some(mine) = &mut self.0 else {
            self.0 = Some(theirs.clone());
            return;
        };
        if Rc::ptr_eq(mine, theirs) {
            return;
        }
        // In place when nobody else can see this cell (an accumulator the fold owns);
        // otherwise a fresh conjunction, so records that share the old cell are unchanged.
        if Rc::strong_count(mine) == 1
            && let Node::All(children) = &mut *mine.borrow_mut()
        {
            children.push(other.clone());
            return;
        }
        *self = Self::node(Node::All(vec![self.clone(), other.clone()]));
    }

    /// Adds one id.
    pub fn insert(&mut self, id: u64) {
        self.extend(&Self::single(id));
    }

    /// The conjunction of two provenances.
    pub fn union(mut self, other: &Prov) -> Self {
        self.extend(other);
        self
    }

    /// A fresh disjunction cell holding this provenance as its only alternative, for the
    /// records a deduplicating operator is about to see (see the module docs).
    pub fn rebox(&self) -> Self {
        Self::node(Node::Alts(vec![self.clone()]))
    }

    /// If this is a disjunction cell, adds the alternatives of `other` (an equal record a
    /// dedup is dropping) to it. A no-op on any other cell. Alternatives are copied by
    /// content, so cells never reference each other and the formula stays acyclic.
    pub fn absorb(&self, other: &Prov) {
        let Some(mine) = &self.0 else {
            return;
        };
        if let Some(theirs) = &other.0
            && Rc::ptr_eq(mine, theirs)
        {
            return;
        }
        let mut node = mine.borrow_mut();
        let Node::Alts(alternatives) = &mut *node else {
            return;
        };
        match other.0.as_deref().map(RefCell::borrow) {
            Some(theirs) => match &*theirs {
                Node::Alts(more) => alternatives.extend(more.iter().cloned()),
                Node::Leaf(_) | Node::All(_) => alternatives.push(other.clone()),
            },
            None => alternatives.push(other.clone()),
        }
    }
}

impl fmt::Debug for Prov {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let resolved = RecordProv::resolve(self, DEFAULT_MAX_WITNESSES);
        if resolved.witnesses.len() <= 1 {
            f.debug_set().entries(&resolved.ids).finish()
        } else {
            f.debug_list().entries(&resolved.witnesses).finish()
        }
    }
}

/// The resolved provenance of a sent or output record, as the lineage log stores it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordProv {
    /// The single-derivation reading (see [`Prov::ids`]): empty when the simulation was
    /// compiled without provenance.
    pub ids: Vec<u64>,
    /// Every witness (sufficient set of deliveries) of the record, minimal (no witness is a
    /// superset of another), at most the cap the log was resolved with. Equals `[ids]` where
    /// the record has one derivation.
    pub witnesses: Vec<Witness>,
    /// Whether the cap truncated the formula somewhere below this record (a conjunction fell
    /// back to its union witness, or a disjunction dropped alternatives).
    pub overflow: bool,
}

impl RecordProv {
    /// Resolves `prov` to at most `cap` witnesses (see the module docs).
    pub fn resolve(prov: &Prov, cap: usize) -> Self {
        let cap = cap.max(1);
        let mut resolver = Resolver {
            cap,
            overflow: false,
            memo: HashMap::new(),
            visiting: HashSet::new(),
        };
        let witnesses = resolver.resolve(prov);
        let ids = if cap == 1 {
            witnesses.first().cloned().unwrap_or_default()
        } else {
            Resolver {
                cap: 1,
                overflow: false,
                memo: HashMap::new(),
                visiting: HashSet::new(),
            }
            .resolve(prov)
            .first()
            .cloned()
            .unwrap_or_default()
        };
        RecordProv {
            ids,
            witnesses,
            overflow: resolver.overflow,
        }
    }

    /// Whether the record is opaque (some alternative needs counterfactual replay; see
    /// [`Prov::OPAQUE`]).
    pub fn is_opaque(&self) -> bool {
        self.ids.last() == Some(&Prov::OPAQUE)
            || self
                .witnesses
                .iter()
                .any(|w| w.last() == Some(&Prov::OPAQUE))
    }
}

struct Resolver {
    cap: usize,
    overflow: bool,
    memo: HashMap<*const RefCell<Node>, Vec<Witness>>,
    visiting: HashSet<*const RefCell<Node>>,
}

impl Resolver {
    fn resolve(&mut self, prov: &Prov) -> Vec<Witness> {
        let Some(cell) = &prov.0 else {
            return vec![vec![]];
        };
        let key = Rc::as_ptr(cell);
        if let Some(known) = self.memo.get(&key) {
            return known.clone();
        }
        if !self.visiting.insert(key) {
            // Unreachable by construction (cells only reference older cells); be
            // conservative if it ever happens.
            self.overflow = true;
            return vec![vec![Prov::OPAQUE]];
        }
        let out = match &*cell.borrow() {
            Node::Leaf(id) => vec![vec![*id]],
            Node::All(children) => {
                let mut acc: Vec<Witness> = vec![vec![]];
                let mut everything: Witness = vec![];
                let mut fell_back = false;
                for child in children {
                    let theirs = self.resolve(child);
                    for w in &theirs {
                        everything.extend_from_slice(w);
                    }
                    if fell_back {
                        continue;
                    }
                    let product = cross(&acc, &theirs);
                    if product.len() > self.cap {
                        fell_back = true;
                        self.overflow = true;
                    } else {
                        acc = product;
                    }
                }
                if fell_back {
                    everything.sort_unstable();
                    everything.dedup();
                    vec![everything]
                } else {
                    acc
                }
            }
            Node::Alts(alternatives) => {
                let mut all: Vec<Witness> = vec![];
                for alternative in alternatives {
                    all.extend(self.resolve(alternative));
                }
                let mut all = minimize(all);
                if all.len() > self.cap {
                    all.truncate(self.cap);
                    self.overflow = true;
                }
                all
            }
        };
        self.visiting.remove(&key);
        self.memo.insert(key, out.clone());
        out
    }
}

/// `{a ∪ b | a ∈ left, b ∈ right}`, minimal.
fn cross(left: &[Witness], right: &[Witness]) -> Vec<Witness> {
    let mut out = Vec::with_capacity(left.len() * right.len());
    for a in left {
        for b in right {
            let mut w = a.clone();
            w.extend_from_slice(b);
            w.sort_unstable();
            w.dedup();
            out.push(w);
        }
    }
    minimize(out)
}

/// Drops duplicates and every witness that is a strict superset of another (the smaller one
/// suffices, and any fault set hitting the smaller hits the larger). Keeps first-seen order.
pub fn minimize(mut witnesses: Vec<Witness>) -> Vec<Witness> {
    let mut seen: Vec<Witness> = vec![];
    for w in witnesses.drain(..) {
        if seen.iter().any(|s| is_subset(s, &w)) {
            continue;
        }
        seen.retain(|s| !is_subset(&w, s));
        seen.push(w);
    }
    seen
}

/// Whether sorted `a ⊆` sorted `b`.
fn is_subset(a: &[u64], b: &[u64]) -> bool {
    let mut j = 0;
    for x in a {
        while j < b.len() && b[j] < *x {
            j += 1;
        }
        if j == b.len() || b[j] != *x {
            return false;
        }
        j += 1;
    }
    true
}

/// A value and the provenance it was computed with. Behaves as the value for every trait the
/// simulator or a DFIR operator may rely on (see the module docs).
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
    /// Equality of the values. When they are equal, each side's disjunction cell (if it is
    /// one) absorbs the other's alternatives: a deduplicating operator comparing a new
    /// arrival with the copy it stored is how an alternative derivation reaches the record
    /// that was emitted (see the module docs). Cells that are not disjunction cells are
    /// unaffected.
    fn eq(&self, other: &Self) -> bool {
        let equal = self.0 == other.0;
        if equal {
            other.1.absorb(&self.1);
            self.1.absorb(&other.1);
        }
        equal
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

    /// Tags a record deserialized at a network receive with the delivery id of the send that
    /// produced it (the id travelled with the message; see `sim::runtime::lineage`).
    pub fn tag_recv<T>(id: u64, value: T) -> Tagged<T> {
        Tagged(value, Prov::single(id))
    }

    /// Renders the payload of a cluster-targeted send `(member, payload)` with `display`.
    /// Exists so generated code need not project the tuple itself, whose type may not be
    /// inferred yet where the closure is type-checked.
    pub fn display_payload<K, V>(
        display: fn(&V) -> Option<String>,
        value: &(K, V),
    ) -> Option<String> {
        display(&value.1)
    }

    /// Applies a (de)serialization closure to one value. Exists so generated code can call a
    /// closure whose parameter type is only known from its argument: the argument is
    /// type-checked first here, where an immediately-invoked closure would not infer it.
    pub fn apply<I, O>(value: I, f: impl FnOnce(I) -> O) -> O {
        f(value)
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

    /// Gives a record about to enter a deduplicating operator a fresh disjunction cell (see
    /// [`Prov::rebox`]).
    pub fn rebox<T>(item: Tagged<T>) -> Tagged<T> {
        let Tagged(v, p) = item;
        let p = p.rebox();
        Tagged(v, p)
    }

    /// [`rebox`] for the keyed representation.
    pub fn rebox_keyed<K, V>(item: (K, Tagged<V>)) -> (K, Tagged<V>) {
        let (k, Tagged(v, p)) = item;
        let p = p.rebox();
        (k, Tagged(v, p))
    }

    /// `Tagged(v)` → `(v, Tagged(()))`: the positive side of a set difference, keyed by the
    /// whole value so the operator compares plain values.
    pub fn diff_pos<T>(item: Tagged<T>) -> (T, Tagged<()>) {
        let Tagged(v, p) = item;
        (v, Tagged((), p))
    }

    /// `Tagged(v)` → `v`: the negative side of a set difference (its provenance is not the
    /// positive record's).
    pub fn diff_neg<T>(item: Tagged<T>) -> T {
        item.0
    }

    /// The inverse of [`diff_pos`].
    pub fn diff_out<T>(item: (T, Tagged<()>)) -> Tagged<T> {
        let (v, Tagged((), p)) = item;
        Tagged(v, p)
    }

    /// An empty provenance (for `Tagged::empty` in generated code).
    pub fn none() -> Prov {
        Prov::empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn w(ids: &[u64]) -> Witness {
        ids.to_vec()
    }

    #[test]
    fn single_derivation_reading_is_the_first_alternative_and_the_union() {
        let a = Prov::single(1).union(&Prov::single(2));
        let b = Prov::single(3);
        let cell = a.rebox();
        cell.absorb(&b);
        assert_eq!(cell.ids(), vec![1, 2]);
        let both = RecordProv::resolve(&cell, 8);
        assert_eq!(both.witnesses, vec![w(&[1, 2]), w(&[3])]);
        assert!(!both.overflow);
        assert_eq!(both.ids, vec![1, 2]);
    }

    #[test]
    fn conjunction_of_disjunctions_is_a_cross_product() {
        let x = Prov::single(1).rebox();
        x.absorb(&Prov::single(2));
        let y = Prov::single(3).rebox();
        y.absorb(&Prov::single(4));
        let z = x.union(&y);
        let resolved = RecordProv::resolve(&z, 8);
        assert_eq!(
            resolved.witnesses,
            vec![w(&[1, 3]), w(&[1, 4]), w(&[2, 3]), w(&[2, 4])]
        );
        assert_eq!(resolved.ids, vec![1, 3]);
    }

    #[test]
    fn cap_falls_back_to_the_union_witness() {
        let x = Prov::single(1).rebox();
        x.absorb(&Prov::single(2));
        let y = Prov::single(3).rebox();
        y.absorb(&Prov::single(4));
        let z = x.union(&y);
        let resolved = RecordProv::resolve(&z, 2);
        assert_eq!(resolved.witnesses, vec![w(&[1, 2, 3, 4])]);
        assert!(resolved.overflow);
        assert_eq!(resolved.ids, vec![1, 3]);
    }

    #[test]
    fn minimize_drops_supersets() {
        let out = minimize(vec![w(&[1, 2]), w(&[1]), w(&[1, 2]), w(&[3])]);
        assert_eq!(out, vec![w(&[1]), w(&[3])]);
    }

    #[test]
    fn a_hash_set_probe_absorbs_into_the_stored_copy() {
        // What DFIR's `unique` does: `contains(&probe)` then `insert(probe.clone())`.
        #[expect(
            clippy::mutable_key_type,
            reason = "the cell is not part of the hash or the equality; that is the point"
        )]
        let mut seen: HashSet<Tagged<u32>> = HashSet::new();
        let first = codegen::rebox(Tagged(7, Prov::single(1)));
        let emitted = first.clone();
        assert!(!seen.contains(&first));
        seen.insert(first);
        let duplicate = codegen::rebox(Tagged(7, Prov::single(2)));
        assert!(seen.contains(&duplicate));
        let resolved = RecordProv::resolve(&emitted.1, 8);
        assert_eq!(resolved.witnesses, vec![w(&[1]), w(&[2])]);
        assert_eq!(emitted.1.ids(), vec![1]);
    }

    #[test]
    fn plain_cells_do_not_absorb() {
        let a = Tagged(7, Prov::single(1));
        let b = Tagged(7, Prov::single(2));
        assert!(a == b);
        assert_eq!(RecordProv::resolve(&a.1, 8).witnesses, vec![w(&[1])]);
        assert_eq!(RecordProv::resolve(&b.1, 8).witnesses, vec![w(&[2])]);
    }

    #[test]
    fn a_shared_accumulator_is_not_mutated_in_place() {
        let mut acc = Prov::single(1).union(&Prov::single(2));
        let snapshot = acc.clone();
        acc.extend(&Prov::single(3));
        assert_eq!(snapshot.ids(), vec![1, 2]);
        assert_eq!(acc.ids(), vec![1, 2, 3]);
    }

    #[test]
    fn opaque() {
        let mut p = Prov::single(1);
        assert!(!p.is_opaque());
        p.set_opaque();
        assert!(p.is_opaque());
        let r = RecordProv::resolve(&Prov::single(1).union(&Prov::opaque()), 8);
        assert!(r.is_opaque());
    }
}
