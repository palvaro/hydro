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
//!
//! # Aggregate-local counterfactual witnesses
//!
//! A fold's output carries the conjunction of every input, which loses "any `k` of `n`": a
//! quorum is a count followed by a threshold predicate, and the record the predicate lets
//! through needs only `k` of the `n` contributions. Under [`AggregateMode`] (set per run by
//! the LDFI search), a fold's accumulator is an [`Node::Agg`] cell that also keeps the
//! inputs' values and provenances (bounded by [`AGGREGATE_MAX_INPUTS`]) and a type-erased
//! way to re-run the fold on an index subset; a downstream `filter` / `filter_map` whose
//! input is such a cell ([`codegen::agg_witness`]) re-folds every subset of the inputs,
//! checks that the predicate is **upward-closed** on them (a subset passing implies every
//! superset passing, in both input orders: the soundness condition for a witness to survive
//! the loss of the other inputs; checked empirically on this run's inputs, not assumed),
//! and gives its output the disjunction of the minimal passing subsets. Where the check
//! fails, the inputs overflowed, or the full set does not pass, the output keeps the
//! conjunction and the skip is counted ([`AggregateStats`]). Everywhere else an `Agg` cell
//! is the conjunction it wraps: a value-changing operator strips it ([`Prov::strip_agg`]),
//! a combination buries it in an `All`, and the resolver reads `all` through it, so with the
//! mode off nothing changes.

use std::any::Any;
use std::cell::{Cell, RefCell};
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
    /// A fold's accumulator under aggregate witnesses (see the module docs): resolves as the
    /// conjunction it wraps; a downstream predicate may read the inputs.
    Agg(Aggregate),
}

/// The bound on inputs an [`Node::Agg`] cell records (and on the subsets a predicate
/// enumerates: `2^n` re-folds, twice). Past it the cell overflows and predicates fall back.
pub const AGGREGATE_MAX_INPUTS: usize = 10;

/// The inputs of one fold accumulator, type-erased (see [`TypedAggregate`]).
pub trait AggregateInputs {
    /// Records one input (a clone of its value); `false` if the value is not of this fold's
    /// input type (cannot happen for generated code).
    fn push(&mut self, value: &dyn Any) -> bool;
    /// The number of inputs recorded.
    fn len(&self) -> usize;
    /// Whether no input was recorded.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Re-runs the fold from its initial state over the inputs at `indices`, in that order.
    fn refold(&self, indices: &[usize]) -> Box<dyn Any>;
    /// A copy (the accumulator was shared with an emitted record: copy on write).
    fn clone_box(&self) -> Box<dyn AggregateInputs>;
}

/// [`AggregateInputs`] for a fold with input type `T`, accumulator `A`, and the fold's own
/// closures (fresh instances, constructed by generated code from the same expressions;
/// shared between copies of the store, which is harmless: they are the fold's pure
/// `init` / `acc`).
pub struct TypedAggregate<T, A, I, F> {
    values: Vec<T>,
    init: Rc<RefCell<I>>,
    acc: Rc<RefCell<F>>,
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<T, A, I, F> TypedAggregate<T, A, I, F>
where
    T: Clone + 'static,
    A: 'static,
    I: FnMut() -> A + 'static,
    F: FnMut(&mut A, T) + 'static,
{
    /// A store for a fold with these closures.
    pub fn new(init: I, acc: F) -> Self {
        TypedAggregate {
            values: vec![],
            init: Rc::new(RefCell::new(init)),
            acc: Rc::new(RefCell::new(acc)),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, A, I, F> AggregateInputs for TypedAggregate<T, A, I, F>
where
    T: Clone + 'static,
    A: 'static,
    I: FnMut() -> A + 'static,
    F: FnMut(&mut A, T) + 'static,
{
    fn push(&mut self, value: &dyn Any) -> bool {
        match value.downcast_ref::<T>() {
            Some(value) => {
                self.values.push(value.clone());
                true
            }
            None => false,
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn refold(&self, indices: &[usize]) -> Box<dyn Any> {
        let mut state = (self.init.borrow_mut())();
        let mut acc = self.acc.borrow_mut();
        for &i in indices {
            acc(&mut state, self.values[i].clone());
        }
        Box::new(state)
    }

    fn clone_box(&self) -> Box<dyn AggregateInputs> {
        Box::new(TypedAggregate {
            values: self.values.clone(),
            init: self.init.clone(),
            acc: self.acc.clone(),
            _marker: std::marker::PhantomData,
        })
    }
}

/// An [`Node::Agg`] cell's content: the conjunction of every input (what the cell resolves
/// to), each input's provenance, and the inputs' values with the fold's closures.
pub struct Aggregate {
    all: Prov,
    inputs: Vec<Prov>,
    store: Box<dyn AggregateInputs>,
    /// More than [`AGGREGATE_MAX_INPUTS`] inputs arrived: the store stopped recording.
    overflow: bool,
    /// The value a predicate sees is the accumulator wrapped by these (a keyed fold's output
    /// cast to `(key, value)`): applied to each re-fold before the predicate.
    wrap: Vec<Rc<dyn Fn(Box<dyn Any>) -> Option<Box<dyn Any>>>>,
}

impl Aggregate {
    fn copy(&self) -> Self {
        Aggregate {
            all: self.all.clone(),
            inputs: self.inputs.clone(),
            store: self.store.clone_box(),
            overflow: self.overflow,
            wrap: self.wrap.clone(),
        }
    }

    /// The accumulator over the inputs at `indices`, as the predicate sees it.
    fn refold(&self, indices: &[usize]) -> Option<Box<dyn Any>> {
        let mut value = self.store.refold(indices);
        for w in &self.wrap {
            value = w(value)?;
        }
        Some(value)
    }
}

/// Whether folds record their inputs and predicates compute aggregate witnesses (see the
/// module docs). Shared with generated code through `LineageHandles`; off by default.
pub type AggregateMode = Rc<Cell<bool>>;

/// Counts of what aggregate witnesses did in one simulation instance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AggregateStats {
    /// Predicate outputs given a disjunction of two or more minimal witnesses.
    pub disjunctions: usize,
    /// Predicate outputs whose only minimal witness was a strict subset of the inputs.
    pub narrowed: usize,
    /// Predicate outputs whose only minimal witness was every input (the conjunction).
    pub unchanged: usize,
    /// Predicate outputs that kept the conjunction because the accumulator recorded more
    /// than [`AGGREGATE_MAX_INPUTS`] inputs.
    pub skipped_inputs: usize,
    /// Predicate outputs that kept the conjunction because the predicate was not upward-closed
    /// on this run's inputs (or depended on their order), or the full set did not pass.
    pub skipped_non_monotone: usize,
    /// Predicate outputs that kept the conjunction because the re-folded accumulator was not
    /// of the value type the predicate sees (an operator between the fold and the predicate
    /// the rewrite does not see through).
    pub skipped_shape: usize,
    /// Predicate outputs whose minimal witnesses exceeded the cap (the first `cap` kept).
    pub capped: usize,
}

impl AggregateStats {
    /// Predicate outputs that kept the conjunction although they came from an aggregate.
    pub fn skips(&self) -> usize {
        self.skipped_inputs + self.skipped_non_monotone + self.skipped_shape
    }
}

/// The provenance of a record: a shared, lazily-resolved formula over delivery ids (see the
/// module docs). Cloning shares the handle, so records derived from one record see the
/// alternatives it later acquires.
#[derive(Clone, Default)]
pub struct Prov(Option<Rc<RefCell<Node>>>);

impl Prov {
    /// The delivery id standing for "unknown": the record came out of a closure whose reads
    /// the rewrite cannot see (a `Reference` capture), so its lineage must be derived by
    /// counterfactual replay of the node (see `sim::ldfi`, `Level::Precise`).
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
                Node::Leaf(_) | Node::All(_) | Node::Agg(_) => alternatives.push(other.clone()),
            },
            None => alternatives.push(other.clone()),
        }
    }

    /// Whether this is an aggregate cell (see the module docs).
    pub fn is_agg(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|cell| matches!(&*cell.borrow(), Node::Agg(_)))
    }

    /// The conjunction an aggregate cell wraps; any other provenance unchanged. For the
    /// operators that change a record's value: a predicate downstream of them must not
    /// re-evaluate the aggregate.
    pub fn strip_agg(&self) -> Prov {
        if let Some(cell) = &self.0
            && let Node::Agg(agg) = &*cell.borrow()
        {
            return agg.all.clone();
        }
        self.clone()
    }

    /// Composes the value a predicate downstream of this aggregate cell sees with `wrap` (a
    /// keyed fold's output cast to `(key, value)` does this with `|v| (key, v)`); a fresh
    /// cell, so other readers of the aggregate are unchanged. Any other provenance is
    /// returned as is.
    pub fn agg_wrap(&self, wrap: Rc<dyn Fn(Box<dyn Any>) -> Option<Box<dyn Any>>>) -> Prov {
        if let Some(cell) = &self.0
            && let Node::Agg(agg) = &*cell.borrow()
        {
            let mut copy = agg.copy();
            copy.wrap.push(wrap);
            return Self::node(Node::Agg(copy));
        }
        self.clone()
    }

    /// The fold accumulator's step under aggregate witnesses: adds `input` to the
    /// conjunction (as [`Self::extend`]) and records the input's value; `make` builds the
    /// typed store the first time. Copy on write, as `extend`: a cell shared with an emitted
    /// record is left as it was.
    pub fn agg_push(
        &mut self,
        value: &dyn Any,
        input: &Prov,
        make: impl FnOnce() -> Box<dyn AggregateInputs>,
    ) {
        let shared = self
            .0
            .as_ref()
            .is_some_and(|cell| Rc::strong_count(cell) > 1);
        let is_agg = self.is_agg();
        if !is_agg || shared {
            let mut agg = match (&self.0, is_agg) {
                (Some(cell), true) => {
                    let node = cell.borrow();
                    let Node::Agg(agg) = &*node else {
                        unreachable!()
                    };
                    agg.copy()
                }
                _ => Aggregate {
                    all: self.clone(),
                    inputs: vec![],
                    store: make(),
                    overflow: false,
                    wrap: vec![],
                },
            };
            agg.push(value, input);
            *self = Self::node(Node::Agg(agg));
            return;
        }
        let Some(cell) = &self.0 else { unreachable!() };
        let Node::Agg(agg) = &mut *cell.borrow_mut() else {
            unreachable!()
        };
        agg.push(value, input);
    }

    /// The witnesses of a predicate's output whose input is this aggregate cell (see the
    /// module docs): the disjunction of the minimal input subsets on which the re-folded
    /// accumulator passes `pred`, if the predicate is upward-closed on this run's inputs;
    /// otherwise the conjunction. `None` when this is not an aggregate cell (the caller keeps
    /// the provenance it has). `pred` receives the accumulator as the predicate sees it (see
    /// [`Self::agg_wrap`]) and returns `None` when it is not of the expected type.
    pub fn agg_witness(
        &self,
        cap: usize,
        stats: &RefCell<AggregateStats>,
        mut pred: impl FnMut(Box<dyn Any>) -> Option<bool>,
    ) -> Option<Prov> {
        let cell = self.0.as_ref()?;
        let node = cell.borrow();
        let Node::Agg(agg) = &*node else {
            return None;
        };
        let n = agg.inputs.len();
        if agg.overflow || n > AGGREGATE_MAX_INPUTS || n == 0 {
            stats.borrow_mut().skipped_inputs += 1;
            return Some(agg.all.clone());
        }
        // The full set (what actually happened) must be of the predicate's type and pass.
        let total = 1usize << n;
        let everything: Vec<usize> = (0..n).collect();
        match agg.refold(&everything).and_then(&mut pred) {
            Some(true) => {}
            Some(false) => {
                stats.borrow_mut().skipped_non_monotone += 1;
                return Some(agg.all.clone());
            }
            None => {
                stats.borrow_mut().skipped_shape += 1;
                return Some(agg.all.clone());
            }
        }
        // `pass[mask]`: whether the predicate holds on the inputs in `mask`, in both orders.
        let mut pass = vec![false; total];
        let mut indices: Vec<usize> = Vec::with_capacity(n);
        for (mask, slot) in pass.iter_mut().enumerate() {
            indices.clear();
            indices.extend((0..n).filter(|i| mask & (1 << i) != 0));
            let forward = agg.refold(&indices).and_then(&mut pred);
            indices.reverse();
            let backward = agg.refold(&indices).and_then(&mut pred);
            match (forward, backward) {
                (Some(f), Some(b)) if f == b => *slot = f,
                _ => {
                    stats.borrow_mut().skipped_non_monotone += 1;
                    return Some(agg.all.clone());
                }
            }
        }
        // Upward closure: a passing set stays passing under any addition.
        let monotone = (0..total).all(|mask| {
            !pass[mask] || (0..n).all(|i| mask & (1 << i) != 0 || pass[mask | (1 << i)])
        });
        if !monotone {
            stats.borrow_mut().skipped_non_monotone += 1;
            return Some(agg.all.clone());
        }
        // The minimal passing sets, by size then index order. The empty set is never a
        // witness even where the predicate accepts it (`count < min` on a key that exists):
        // a keyed accumulator exists only once some input arrived, and leaving an
        // alternative out is sound.
        let mut minimal: Vec<usize> = (1..total)
            .filter(|&mask| {
                pass[mask]
                    && (0..n).all(|i| {
                        let sub = mask & !(1 << i);
                        mask & (1 << i) == 0 || sub == 0 || !pass[sub]
                    })
            })
            .collect();
        if minimal.is_empty() {
            // Unreachable (the full set passes); be conservative if it ever happens.
            stats.borrow_mut().skipped_non_monotone += 1;
            return Some(agg.all.clone());
        }
        minimal.sort_by_key(|mask| (mask.count_ones(), *mask));
        let mut stats = stats.borrow_mut();
        if minimal.len() > cap.max(1) {
            minimal.truncate(cap.max(1));
            stats.capped += 1;
        }
        if minimal.len() == 1 && minimal[0] == total - 1 {
            stats.unchanged += 1;
            return Some(agg.all.clone());
        }
        if minimal.len() == 1 {
            stats.narrowed += 1;
        } else {
            stats.disjunctions += 1;
        }
        let alternatives = minimal
            .into_iter()
            .map(|mask| {
                let parts: Vec<Prov> = (0..n)
                    .filter(|i| mask & (1 << i) != 0)
                    .map(|i| agg.inputs[i].clone())
                    .collect();
                Self::node(Node::All(parts))
            })
            .collect();
        Some(Self::node(Node::Alts(alternatives)))
    }
}

impl Aggregate {
    fn push(&mut self, value: &dyn Any, input: &Prov) {
        self.all.extend(input);
        if self.overflow {
            return;
        }
        if self.inputs.len() >= AGGREGATE_MAX_INPUTS || !self.store.push(value) {
            self.overflow = true;
            return;
        }
        self.inputs.push(input.clone());
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
            Node::Agg(agg) => self.resolve(&agg.all),
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
    use std::any::Any;
    use std::rc::Rc;

    use super::super::LineageHandles;
    use super::{Prov, Tagged, TypedAggregate};

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

    /// [`merge_pair`] whose left side is still keyed (`(K, Tagged<V>)`).
    pub fn merge_pair_keyed_left<K, V, B>(
        pair: ((K, Tagged<V>), Tagged<B>),
    ) -> Tagged<((K, V), B)> {
        let ((k, Tagged(v, pa)), Tagged(b, pb)) = pair;
        Tagged(((k, v), b), pa.union(&pb))
    }

    /// [`merge_pair`] whose right side is still keyed (`(K, Tagged<V>)`).
    pub fn merge_pair_keyed_right<A, K, V>(
        pair: (Tagged<A>, (K, Tagged<V>)),
    ) -> Tagged<(A, (K, V))> {
        let (Tagged(a, pa), (k, Tagged(v, pb))) = pair;
        Tagged((a, (k, v)), pa.union(&pb))
    }

    /// [`merge_pair`] whose two sides are still keyed.
    pub fn merge_pair_keyed_both<K1, V1, K2, V2>(
        pair: ((K1, Tagged<V1>), (K2, Tagged<V2>)),
    ) -> Tagged<((K1, V1), (K2, V2))> {
        let ((k1, Tagged(v1, pa)), (k2, Tagged(v2, pb))) = pair;
        Tagged(((k1, v1), (k2, v2)), pa.union(&pb))
    }

    /// `Tagged((k, v))` → `(k, Tagged(v))`, for operators that key on the first component.
    pub fn key_out<K, V>(item: Tagged<(K, V)>) -> (K, Tagged<V>) {
        let Tagged((k, v), p) = item;
        (k, Tagged(v, p))
    }

    /// [`key_out`] for a cast from the flat to the keyed representation: a predicate on the
    /// keyed side sees the value alone, so an aggregate's re-folds are projected to it.
    pub fn key_out_cast<K: 'static, V: 'static>(item: Tagged<(K, V)>) -> (K, Tagged<V>) {
        let Tagged((k, v), p) = item;
        let p = if p.is_agg() {
            p.agg_wrap(Rc::new(|value: Box<dyn Any>| {
                value
                    .downcast::<(K, V)>()
                    .ok()
                    .map(|kv| Box::new(kv.1) as Box<dyn Any>)
            }))
        } else {
            p
        };
        (k, Tagged(v, p))
    }

    /// `(k, Tagged(v))` → `Tagged((k, v))`.
    pub fn key_in<K, V>(item: (K, Tagged<V>)) -> Tagged<(K, V)> {
        let (k, Tagged(v, p)) = item;
        Tagged((k, v), p)
    }

    /// [`key_in`] for a cast from the keyed to the flat representation (`entries()`): a
    /// predicate on the flat side sees `(key, value)`, so an aggregate's re-folds (the value
    /// of a keyed fold) are paired with the key.
    pub fn key_in_cast<K: Clone + 'static, V: 'static>(item: (K, Tagged<V>)) -> Tagged<(K, V)> {
        let (k, Tagged(v, p)) = item;
        let p = if p.is_agg() {
            let key = k.clone();
            p.agg_wrap(Rc::new(move |value: Box<dyn Any>| {
                value
                    .downcast::<V>()
                    .ok()
                    .map(|v| Box::new((key.clone(), *v)) as Box<dyn Any>)
            }))
        } else {
            p
        };
        Tagged((k, v), p)
    }

    /// A zero-sized witness of a value's type, taken before the value is consumed.
    pub struct TypeWitness<X>(std::marker::PhantomData<fn() -> X>);

    /// The type of `value`, as a [`TypeWitness`].
    pub fn type_of<X>(_value: &X) -> TypeWitness<X> {
        TypeWitness(std::marker::PhantomData)
    }

    /// A fold accumulator's step: [`Prov::extend`] with `input`, and under aggregate
    /// witnesses ([`LineageHandles::aggregate`]) also the record of the input's value (see
    /// the module docs). `acc` is the accumulator (for its type), `make` builds the typed
    /// store from fresh instances of the fold's closures.
    pub fn agg_push<T, A, I, F>(
        handles: &LineageHandles,
        prov: &mut Prov,
        _acc: &A,
        value: &T,
        input: &Prov,
        make: impl FnOnce() -> TypedAggregate<T, A, I, F>,
    ) where
        T: Clone + 'static,
        A: 'static,
        I: FnMut() -> A + 'static,
        F: FnMut(&mut A, T) + 'static,
    {
        if !handles.aggregate.get() {
            prov.extend(input);
            return;
        }
        prov.agg_push(value, input, || Box::new(make()));
    }

    /// The provenance of a predicate's output: under aggregate witnesses, if `prov` (the
    /// input's) is an aggregate cell, the disjunction of the minimal input subsets on which
    /// the re-folded value passes `pred` (see [`Prov::agg_witness`]); otherwise `prov`.
    pub fn agg_witness<X: 'static>(
        handles: &LineageHandles,
        prov: &Prov,
        _like: TypeWitness<X>,
        mut pred: impl FnMut(X) -> bool,
    ) -> Prov {
        if !handles.aggregate.get() || !prov.is_agg() {
            return prov.clone();
        }
        prov.agg_witness(handles.max_witnesses(), &handles.aggregate_stats, |value| {
            value.downcast::<X>().ok().map(|x| pred(*x))
        })
        .unwrap_or_else(|| prov.clone())
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

    /// A counting fold's store, as generated code builds it.
    fn count_store() -> Box<dyn AggregateInputs> {
        Box::new(TypedAggregate::new(
            || 0usize,
            |n: &mut usize, _v: u32| *n += 1,
        ))
    }

    fn count_prov(ids: &[u64]) -> Prov {
        let mut acc = Prov::empty();
        for &id in ids {
            acc.agg_push(&(id as u32), &Prov::single(id), count_store);
        }
        acc
    }

    fn threshold(min: usize) -> impl FnMut(Box<dyn Any>) -> Option<bool> {
        move |v| v.downcast::<usize>().ok().map(|n| *n >= min)
    }

    #[test]
    fn an_aggregate_cell_resolves_as_the_conjunction() {
        let acc = count_prov(&[1, 2, 3]);
        assert!(acc.is_agg());
        assert_eq!(acc.ids(), vec![1, 2, 3]);
        let resolved = RecordProv::resolve(&acc, 8);
        assert_eq!(resolved.witnesses, vec![w(&[1, 2, 3])]);
        assert!(!resolved.overflow);
        assert_eq!(acc.strip_agg().ids(), vec![1, 2, 3]);
        assert!(!acc.strip_agg().is_agg());
    }

    #[test]
    fn a_threshold_predicate_yields_the_minimal_subsets() {
        let acc = count_prov(&[1, 2, 3]);
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(8, &stats, threshold(2)).unwrap();
        let resolved = RecordProv::resolve(&out, 8);
        assert_eq!(resolved.witnesses, vec![w(&[1, 2]), w(&[1, 3]), w(&[2, 3])]);
        assert_eq!(resolved.ids, vec![1, 2]);
        assert_eq!(stats.borrow().disjunctions, 1);
        assert_eq!(stats.borrow().skips(), 0);
    }

    #[test]
    fn an_all_of_n_predicate_keeps_the_conjunction() {
        let acc = count_prov(&[1, 2, 3]);
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(8, &stats, threshold(3)).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2, 3])]);
        assert_eq!(stats.borrow().unchanged, 1);
    }

    #[test]
    fn a_predicate_the_empty_set_passes_yields_the_singletons() {
        // `count >= 0`, or `count < min` below threshold: the accumulator still needs one
        // input to exist, so the empty set is not offered as a witness.
        let acc = count_prov(&[1, 2, 3]);
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(8, &stats, threshold(0)).unwrap();
        assert_eq!(
            RecordProv::resolve(&out, 8).witnesses,
            vec![w(&[1]), w(&[2]), w(&[3])]
        );
        assert_eq!(stats.borrow().disjunctions, 1);
    }

    #[test]
    fn a_non_monotone_predicate_falls_back() {
        let acc = count_prov(&[1, 2, 3]);
        let stats = RefCell::new(AggregateStats::default());
        // "exactly three" is upward-closed on these inputs (nothing above 3), "at most three"
        // is not: the empty set passes but so does everything, fine; "an odd count" is not.
        let odd = |v: Box<dyn Any>| v.downcast::<usize>().ok().map(|n| *n % 2 == 1);
        let out = acc.agg_witness(8, &stats, odd).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2, 3])]);
        assert_eq!(stats.borrow().skipped_non_monotone, 1);
        // A predicate the full set fails is not what happened: fall back.
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(8, &stats, threshold(4)).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2, 3])]);
        assert_eq!(stats.borrow().skipped_non_monotone, 1);
    }

    #[test]
    fn an_order_dependent_fold_falls_back() {
        // "the last input" is not a function of the set.
        let store = || -> Box<dyn AggregateInputs> {
            Box::new(TypedAggregate::new(
                || 0u32,
                |last: &mut u32, v: u32| *last = v,
            ))
        };
        let mut acc = Prov::empty();
        for id in [1u64, 2, 3] {
            acc.agg_push(&(id as u32), &Prov::single(id), store);
        }
        let stats = RefCell::new(AggregateStats::default());
        let pred = |v: Box<dyn Any>| v.downcast::<u32>().ok().map(|last| *last >= 2);
        let out = acc.agg_witness(8, &stats, pred).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2, 3])]);
        assert_eq!(stats.borrow().skipped_non_monotone, 1);
    }

    #[test]
    fn a_wrong_value_type_falls_back() {
        let acc = count_prov(&[1, 2]);
        let stats = RefCell::new(AggregateStats::default());
        let pred = |v: Box<dyn Any>| v.downcast::<u64>().ok().map(|_| true);
        let out = acc.agg_witness(8, &stats, pred).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2])]);
        assert_eq!(stats.borrow().skipped_shape, 1);
    }

    #[test]
    fn the_witness_cap_truncates_the_alternatives() {
        let acc = count_prov(&[1, 2, 3, 4]);
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(2, &stats, threshold(2)).unwrap();
        assert_eq!(
            RecordProv::resolve(&out, 8).witnesses,
            vec![w(&[1, 2]), w(&[1, 3])]
        );
        assert_eq!(stats.borrow().capped, 1);
    }

    #[test]
    fn a_shared_aggregate_cell_is_copied_before_it_grows() {
        let mut acc = count_prov(&[1, 2]);
        let emitted = acc.clone();
        acc.agg_push(&3u32, &Prov::single(3), count_store);
        let stats = RefCell::new(AggregateStats::default());
        assert_eq!(emitted.ids(), vec![1, 2]);
        assert_eq!(
            RecordProv::resolve(&emitted.agg_witness(8, &stats, threshold(1)).unwrap(), 8)
                .witnesses,
            vec![w(&[1]), w(&[2])]
        );
        assert_eq!(acc.ids(), vec![1, 2, 3]);
        assert_eq!(
            RecordProv::resolve(&acc.agg_witness(8, &stats, threshold(1)).unwrap(), 8).witnesses,
            vec![w(&[1]), w(&[2]), w(&[3])]
        );
    }

    #[test]
    fn too_many_inputs_overflow_the_cell() {
        let ids: Vec<u64> = (1..=(AGGREGATE_MAX_INPUTS as u64 + 1)).collect();
        let acc = count_prov(&ids);
        assert_eq!(acc.ids(), ids);
        let stats = RefCell::new(AggregateStats::default());
        let out = acc.agg_witness(8, &stats, threshold(1)).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![ids]);
        assert_eq!(stats.borrow().skipped_inputs, 1);
    }

    #[test]
    fn a_keyed_cast_pairs_the_refold_with_the_key() {
        let acc = count_prov(&[1, 2, 3]);
        let cast = codegen::key_in_cast(("k".to_owned(), Tagged(3usize, acc)));
        let Tagged((key, n), prov) = cast;
        assert_eq!((key.as_str(), n), ("k", 3));
        let stats = RefCell::new(AggregateStats::default());
        let pred = |v: Box<dyn Any>| {
            v.downcast::<(String, usize)>()
                .ok()
                .map(|kv| kv.0 == "k" && kv.1 >= 2)
        };
        let out = prov.agg_witness(8, &stats, pred).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses.len(), 3);
        // The other direction projects the key away again.
        let (_, Tagged(_, back)) = codegen::key_out_cast(Tagged(("k".to_owned(), 3usize), prov));
        let out = back.agg_witness(8, &stats, threshold(2)).unwrap();
        assert_eq!(RecordProv::resolve(&out, 8).witnesses.len(), 3);
    }

    #[test]
    fn with_the_mode_off_the_step_is_a_plain_extend() {
        let handles = super::super::LineageHandles::default();
        let mut acc = Prov::empty();
        for id in [1u64, 2] {
            codegen::agg_push(
                &handles,
                &mut acc,
                &0usize,
                &(id as u32),
                &Prov::single(id),
                || TypedAggregate::new(|| 0usize, |n: &mut usize, _v: u32| *n += 1),
            );
        }
        assert!(!acc.is_agg());
        assert_eq!(acc.ids(), vec![1, 2]);
        let out =
            codegen::agg_witness(&handles, &acc, codegen::type_of(&0usize), |n: usize| n >= 1);
        assert_eq!(RecordProv::resolve(&out, 8).witnesses, vec![w(&[1, 2])]);
        handles.aggregate.set(true);
        let mut acc = Prov::empty();
        for id in [1u64, 2] {
            codegen::agg_push(
                &handles,
                &mut acc,
                &0usize,
                &(id as u32),
                &Prov::single(id),
                || TypedAggregate::new(|| 0usize, |n: &mut usize, _v: u32| *n += 1),
            );
        }
        assert!(acc.is_agg());
        let out =
            codegen::agg_witness(&handles, &acc, codegen::type_of(&0usize), |n: usize| n >= 1);
        assert_eq!(
            RecordProv::resolve(&out, 8).witnesses,
            vec![w(&[1]), w(&[2])]
        );
        assert_eq!(handles.aggregate_stats.borrow().disjunctions, 1);
    }
}
