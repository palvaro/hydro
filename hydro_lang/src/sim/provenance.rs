//! Lineage-tagged values for simulator tooling (the runtime half of the provenance rewrite in
//! `hydrofrac`).
//!
//! An IR pass may wrap every item flowing through a program in a [`Tagged`] carrying the set of
//! *source events* it descends from. Comparison, hashing and ordering on [`Tagged`] delegate to
//! the value, so the rewritten program makes the same decisions as the original. This module only
//! defines the types and helpers the generated code calls; it is not feature-gated, so a
//! simulation dylib always links.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Whether a source event is application data or an operational stimulus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TagKind {
    /// Ordinary application input.
    Data,
    /// A timer tick, timeout, or similar stimulus with no informational content.
    Operational,
}

/// Identifies one source event: the `seq`-th item delivered on sim input `port` at cluster
/// member `member` (`u32::MAX` for processes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tag {
    /// Data or operational.
    pub kind: TagKind,
    /// The sim input port the event arrived on.
    pub port: u32,
    /// The receiving cluster member, or `u32::MAX` for a process.
    pub member: u32,
    /// Position of the event among those delivered on this port to this member.
    pub seq: u64,
}

/// The lineage of one item: the set of source events it descends from.
pub type TagSet = BTreeSet<Tag>;

/// An item together with its lineage.
///
/// Comparison, hashing and ordering delegate to `value` only, so deduplicating, sorting and
/// set-difference operators behave exactly as they do on the untagged program.
#[derive(Clone)]
pub struct Tagged<T> {
    /// Source events this item descends from.
    pub tags: TagSet,
    /// Set when some ancestor was produced by a closure reading opaque Hydro state, in which
    /// case `tags` is an over-approximation (everything that state had accumulated).
    pub coarse: bool,
    /// The item as the untagged program would see it.
    pub value: T,
}

impl<T> Tagged<T> {
    /// An item with no lineage at all (a compile-time constant, cluster membership, ...).
    pub fn pristine(value: T) -> Self {
        Self {
            tags: TagSet::new(),
            coarse: false,
            value,
        }
    }

    /// An item entering the program as source event `tag`.
    pub fn source(tag: Tag, value: T) -> Self {
        let mut tags = TagSet::new();
        tags.insert(tag);
        Self {
            tags,
            coarse: false,
            value,
        }
    }

    /// Rebuilds an item from parts.
    pub fn from_parts(tags: TagSet, coarse: bool, value: T) -> Self {
        Self {
            tags,
            coarse,
            value,
        }
    }

    /// Splits an item into `(lineage, coarse, value)`.
    pub fn into_parts(self) -> (TagSet, bool, T) {
        (self.tags, self.coarse, self.value)
    }

    /// Absorbs the lineage of another item into this one (for folds and reference reads).
    pub fn absorb(&mut self, tags: &TagSet, coarse: bool) {
        self.tags.extend(tags.iter().copied());
        self.coarse |= coarse;
    }
}

impl<T: Debug> Debug for Tagged<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Tagged<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Tagged<T> {}

impl<T: Hash> Hash for Tagged<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T: PartialOrd> PartialOrd for Tagged<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T: Ord> Ord for Tagged<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

/// Applies an un-annotated closure to a value. Used by generated code so that rustc infers the
/// closure's parameter type from the argument (closures passed as arguments are type-checked
/// after the other arguments), which a plain `let f = |x| ..; f(v)` binding would not allow.
#[doc(hidden)]
#[inline(always)]
pub fn apply1<A, B>(f: impl FnOnce(A) -> B, a: A) -> B {
    f(a)
}

/// Merges two lineages (for joins and cross products).
pub fn union(mut a: TagSet, b: &TagSet) -> TagSet {
    a.extend(b.iter().copied());
    a
}
