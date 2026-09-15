//! The program's half of the simulator's lineage log (E3.2 of the amplification design).
//!
//! The lineage pass ([`super::lineage_pass`], host side) rewrites the IR so that every record
//! carries a `u64` id and every operator reports how it derived its outputs. The generated code
//! calls the functions here, which live in the *compiled program's* copy of `hydro_lang` (the
//! program is a separate dylib whose statics are not shared with the host). They forward each
//! event to a sink function pointer the host passes into `__hydro_runtime`, exactly as
//! `println!` inside the program forwards to `__println_handler`. Nothing is kept on this side.

use std::cell::Cell;

use dfir_rs::bytes::Bytes;

/// What the instrumented program reports about one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageEvent {
    /// Operator `op` produced `record` from `parents` (empty for a leaf: a source, an input, a
    /// timer). An aggregate's parents are every input it saw in the tick.
    Derived {
        /// The new record's id.
        record: u64,
        /// The operator, as numbered by the pass (see the operator table in the host's log).
        op: u32,
        /// The ids of the records it was computed from.
        parents: Vec<u64>,
    },
    /// A state a staged closure may have mutated (`by_mut`) is `record` from now on: a new
    /// version derived from the previous one (`parents[0]`... the closure's inputs, including
    /// the previous version).
    StateVersion {
        /// The new version's id.
        record: u64,
        /// The operator whose closure holds the state.
        op: u32,
        /// The closure's inputs: the record it ran on, the previous version, every other
        /// referenced singleton.
        parents: Vec<u64>,
    },
    /// Operator `op` discarded `record`; `survivor` is the record it duplicated, if the operator
    /// keeps one (`unique`).
    Dropped {
        /// The discarded record.
        record: u64,
        /// The operator that discarded it.
        op: u32,
        /// The record kept in its place, if any.
        survivor: Option<u64>,
    },
    /// `record` left the program through external output `op` (`sim_output`).
    Output {
        /// The record.
        record: u64,
        /// The output operator.
        op: u32,
    },
}

/// The host's sink for lineage events.
pub type LineageSink = fn(LineageEvent);

thread_local! {
    static SINK: Cell<Option<LineageSink>> = const { Cell::new(None) };
    /// Ids start at 1; the host's own boundary ids (used when the pass is off) start at `1 << 63`.
    static NEXT_ID: Cell<u64> = const { Cell::new(1) };
}

/// Installs the host's sink for this thread; called first thing in `__hydro_runtime`.
pub fn set_sink(sink: LineageSink) {
    SINK.with(|s| s.set(Some(sink)));
}

/// A sink that discards everything (the host passes it when no lineage log is kept).
pub fn null_sink(_: LineageEvent) {}

fn emit(event: LineageEvent) {
    if let Some(sink) = SINK.with(|s| s.get()) {
        sink(event);
    }
}

/// A fresh record id.
pub fn fresh_id() -> u64 {
    NEXT_ID.with(|n| {
        let id = n.get();
        n.set(id + 1);
        id
    })
}

/// Allocates a record id for an output of operator `op` derived from `parents`, and reports it.
#[inline]
pub fn derive(op: u32, parents: &[u64]) -> u64 {
    let record = fresh_id();
    emit(LineageEvent::Derived {
        record,
        op,
        parents: parents.to_vec(),
    });
    record
}

/// Allocates the id of a new version of a mutated state, derived from `parents`, and reports it.
#[inline]
pub fn state_version(op: u32, parents: &[u64]) -> u64 {
    let record = fresh_id();
    emit(LineageEvent::StateVersion {
        record,
        op,
        parents: parents.to_vec(),
    });
    record
}

/// Reports that operator `op` discarded `record` (in favour of `survivor`, if any).
#[inline]
pub fn dropped(op: u32, record: u64, survivor: Option<u64>) {
    emit(LineageEvent::Dropped {
        record,
        op,
        survivor,
    });
}

/// Reports that `record` left the program through output `op`.
#[inline]
pub fn output(op: u32, record: u64) {
    emit(LineageEvent::Output { record, op });
}

/// A `by_mut` handle to a stream inside a staged closure, once the stream's elements carry ids:
/// stands in for the `&mut Vec<T>` the closure expects over the `&mut Vec<(u64, T)>` the
/// program now has, and gives every record pushed through it an id derived from the closure's
/// inputs (`parents`). Only the `Vec` methods the closure may use are provided.
pub struct LineageVec<'a, 'bump, T> {
    inner: &'a mut dfir_rs::bumpalo::collections::Vec<'bump, (u64, T)>,
    op: u32,
    parents: &'a [u64],
}

impl<'a, 'bump, T> LineageVec<'a, 'bump, T> {
    /// See the type's documentation. The handoff buffer is DFIR's bump-allocated `Vec`.
    pub fn new(
        inner: &'a mut dfir_rs::bumpalo::collections::Vec<'bump, (u64, T)>,
        op: u32,
        parents: &'a [u64],
    ) -> Self {
        Self { inner, op, parents }
    }

    /// Pushes `value` as a new record derived from the closure's inputs.
    pub fn push(&mut self, value: T) {
        let id = derive(self.op, self.parents);
        self.inner.push((id, value));
    }

    /// Pushes every value.
    pub fn extend(&mut self, values: impl IntoIterator<Item = T>) {
        for value in values {
            self.push(value);
        }
    }

    /// Records so far.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Removes every record.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// The payloads so far.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter().map(|(_, t)| t)
    }
}

/// Calls an unannotated staged closure with `a`. rustc type-checks closure arguments after the
/// other arguments, so the closure's parameter type is fixed by `a`, which a `let`-bound or
/// immediately-invoked closure would not get.
#[inline]
pub fn apply<A, R>(f: impl FnOnce(A) -> R, a: A) -> R {
    f(a)
}

/// Puts a record id in front of a serialized network payload, so the id crosses the wire with
/// the record and the receiving location continues the same lineage. Implemented for the two
/// shapes a network `serialize` closure produces: `Bytes` and `(member, Bytes)` (demux).
pub trait PrependId {
    /// The payload with `id` (8 bytes, little-endian) in front.
    fn prepend_id(self, id: u64) -> Self;
}

fn prepend(id: u64, bytes: Bytes) -> Bytes {
    let mut out = Vec::with_capacity(8 + bytes.len());
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&bytes);
    Bytes::from(out)
}

impl PrependId for Bytes {
    fn prepend_id(self, id: u64) -> Self {
        prepend(id, self)
    }
}

impl<M> PrependId for (M, Bytes) {
    fn prepend_id(self, id: u64) -> Self {
        (self.0, prepend(id, self.1))
    }
}

/// Takes the record id back off a received network payload (the inverse of [`PrependId`]),
/// for the two shapes a network `deserialize` closure receives: `Result<Bytes, E>` and
/// `Result<(member, Bytes), E>` (tagged receive from a cluster).
pub trait SplitId: Sized {
    /// `(id, the payload without it)`; an error is passed through with id 0.
    fn split_id(self) -> (u64, Self);
}

fn split(mut bytes: Bytes) -> (u64, Bytes) {
    assert!(
        bytes.len() >= 8,
        "lineage: network payload shorter than a record id ({} bytes)",
        bytes.len()
    );
    let head = bytes.split_to(8);
    (u64::from_le_bytes(head[..].try_into().unwrap()), bytes)
}

impl<E> SplitId for Result<Bytes, E> {
    fn split_id(self) -> (u64, Self) {
        match self {
            Ok(bytes) => {
                let (id, rest) = split(bytes);
                (id, Ok(rest))
            }
            Err(e) => (0, Err(e)),
        }
    }
}

impl<M, E> SplitId for Result<(M, Bytes), E> {
    fn split_id(self) -> (u64, Self) {
        match self {
            Ok((member, bytes)) => {
                let (id, rest) = split(bytes);
                (id, Ok((member, rest)))
            }
            Err(e) => (0, Err(e)),
        }
    }
}
