//! What a program under test and the generated harness share: how the generator's requests
//! enter the program, how its timers are driven, and how its outputs are observed.
//!
//! These live in this crate rather than in the generated code because they contain `q!` quotes
//! (the timer's period), and stageleft finds quotes by scanning a crate's source files, which
//! macro-generated code is not part of. Everything here except the quoted function is marked
//! `#[cfg(stageleft_runtime)]`, which keeps it out of the staged copy of this crate that the
//! deployed binaries compile against.

use std::cell::RefCell;
#[cfg(stageleft_runtime)]
use std::pin::Pin;
use std::time::Duration;

#[cfg(stageleft_runtime)]
use futures::Stream as FStream;
#[cfg(stageleft_runtime)]
use hydro_lang::compile::deploy::DeployResult;
#[cfg(stageleft_runtime)]
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::live_collections::boundedness::Unbounded;
use hydro_lang::live_collections::stream::{
    AtLeastOnce, ExactlyOnce, NoOrder, Ordering, Retries, Stream, TotalOrder,
};
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream, NotMany};
use hydro_lang::location::{External, Location, Process};
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[cfg(stageleft_runtime)]
/// A request body the generator can make from a sequence number. The generator offers requests
/// `0, 1, 2, …`; the program's completion output must carry the same number back (see
/// [`Observer::completions`]).
pub trait LoadItem: Serialize + DeserializeOwned + 'static {
    /// The request body for sequence number `id`.
    fn from_id(id: u64) -> Self;
}

#[cfg(stageleft_runtime)]
impl LoadItem for () {
    fn from_id(_: u64) -> Self {}
}

#[cfg(stageleft_runtime)]
impl LoadItem for String {
    fn from_id(id: u64) -> Self {
        id.to_string()
    }
}

#[cfg(stageleft_runtime)]
macro_rules! load_item_int {
    ($($t:ty),*) => {
        $(impl LoadItem for $t {
            fn from_id(id: u64) -> Self {
                id as $t
            }
        })*
    };
}

#[cfg(stageleft_runtime)]
load_item_int!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

/// One data stream's end of the `round_data` closure's `inputs`: the closure calls
/// [`RoundSender::send`] for each record a round carries, and the harness takes what was
/// buffered after the closure returns and writes it to the stream's port, in order. The harness
/// numbers every record it sends on a stream from zero, so a program that enumerates its input
/// sees the same sequence numbers (see [`Observer::completions`]).
#[derive(Debug)]
pub struct RoundSender<T> {
    buffered: RefCell<Vec<T>>,
}

impl<T> Default for RoundSender<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> RoundSender<T> {
    /// An empty buffer.
    pub fn new() -> Self {
        Self {
            buffered: RefCell::new(Vec::new()),
        }
    }

    /// Queues `item` for this round.
    pub fn send(&self, item: T) {
        self.buffered.borrow_mut().push(item);
    }

    /// Everything queued since the last take, in send order.
    pub fn take(&self) -> Vec<T> {
        std::mem::take(&mut *self.buffered.borrow_mut())
    }
}

/// A `()` stream that fires every `period`, for a program's timer parameters.
pub fn timer<'a, L>(
    location: &Process<'a, L>,
    period: Duration,
) -> Stream<(), Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce> {
    // `q!` closures can only capture primitives, so the duration crosses as nanos.
    let nanos = period.as_nanos() as u64;
    location.source_interval(q!(Duration::from_nanos(nanos)))
}

#[cfg(stageleft_runtime)]
/// An input stream fed by the generator over an external port. Returns the port the harness
/// connects to and the stream the program consumes. The stream is totally ordered and
/// exactly-once, which is what one TCP connection from the generator gives; a program whose
/// parameter asks for less can weaken it.
pub fn load_input<'a, L, T>(
    location: &Process<'a, L>,
    external: &External<'a, ()>,
) -> (
    ExternalBincodeSink<T, NotMany>,
    Stream<T, Process<'a, L>, Unbounded, TotalOrder, ExactlyOnce>,
)
where
    T: Serialize + DeserializeOwned + 'static,
{
    location.source_external_bincode::<_, T, TotalOrder, ExactlyOnce>(external)
}

#[cfg(stageleft_runtime)]
/// Collects the observable outputs of a program under test. The generated harness creates one,
/// hands it to [`Observed::observe`], and connects to whatever was registered.
///
/// Every method sends the stream to the harness over an external port. Streams are weakened to
/// `NoOrder` / `AtLeastOnce` so that one handle type covers every output; the harness treats
/// duplicates and reordering as the data they are (a second completion for an id is ignored, a
/// gauge sample is stamped when it arrives).
pub struct Observer<'a> {
    external: External<'a, ()>,
    pub(crate) completions: Option<Port<u64>>,
    pub(crate) abandoned: Option<Port<u64>>,
    pub(crate) gauges: Vec<(String, Port<f64>)>,
    pub(crate) counters: Vec<(String, Port<f64>)>,
    /// Output streams registered whole with [`Observer::stream`], to be connected once the
    /// deployment is up.
    pub(crate) streams: Vec<ObservedStream<'a>>,
}

#[cfg(stageleft_runtime)]
/// What the harness reads from one record of an output stream registered with
/// [`Observer::stream`]: the record is serialized to JSON on the harness side and read back
/// generically, so that no `q!` closure is needed in the program's crate. A plain number is a
/// level sample (and, if it is an integer, possibly an id); a record with an integer field named
/// `id` carries an id; anything else is counted and nothing more.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    /// The record's numeric value, when the record is a plain number.
    pub value: Option<f64>,
    /// The record's id: the number itself for an integer record, or its `id` field.
    pub id: Option<u64>,
}

#[cfg(stageleft_runtime)]
impl Observation {
    /// Reads a record generically through serde.
    pub fn of<T: Serialize>(record: &T) -> Self {
        match serde_json::to_value(record) {
            Ok(serde_json::Value::Number(n)) => Observation {
                value: n.as_f64(),
                id: n.as_u64(),
            },
            Ok(serde_json::Value::Object(map)) => Observation {
                value: None,
                id: map.get("id").and_then(|v| v.as_u64()),
            },
            _ => Observation {
                value: None,
                id: None,
            },
        }
    }
}

#[cfg(stageleft_runtime)]
/// A stream of observations as the harness reads it.
pub type ObservationStream = Pin<Box<dyn FStream<Item = Observation>>>;

#[cfg(stageleft_runtime)]
/// An output stream registered whole; `connect` turns the deployed port into observations.
pub struct ObservedStream<'a> {
    pub name: String,
    /// Whether the stream's item type is a plain number, read from its type name; what a
    /// per-tick level looks like.
    pub numeric: bool,
    #[allow(clippy::type_complexity)]
    pub connect: Box<
        dyn for<'n> FnOnce(
                &'n DeployResult<'a, HydroDeploy>,
            ) -> Pin<Box<dyn Future<Output = ObservationStream> + 'n>>
            + 'a,
    >,
}

#[cfg(stageleft_runtime)]
fn is_numeric<T>() -> bool {
    matches!(
        std::any::type_name::<T>(),
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i32" | "i64" | "i128"
            | "isize" | "f32" | "f64"
    )
}

#[cfg(stageleft_runtime)]
/// An output port handle with its guarantees erased.
pub(crate) type Port<T> = ExternalBincodeStream<T, NoOrder, AtLeastOnce>;

#[cfg(stageleft_runtime)]
impl<'a> Observer<'a> {
    /// An observer sending to `external`, the harness's location.
    pub fn new(external: &External<'a, ()>) -> Self {
        Self {
            external: external.clone(),
            completions: None,
            abandoned: None,
            gauges: Vec::new(),
            counters: Vec::new(),
            streams: Vec::new(),
        }
    }

    /// An output stream registered whole, under its field name: the harness counts its records
    /// per second, keeps the latest value when the records are numbers, and reads ids from a
    /// `u64` item or an `id` field. Which of these the verdict uses depends on the stream's kind
    /// (conserved, amplified or level), which comes from Cress's conservation table when one has
    /// been written for this function and configuration, and otherwise from whether the item
    /// type is numeric. `#[derive(sedge::SedgeOutputs)]` on a program's output struct registers
    /// every field this way.
    pub fn stream<L, T, O, R>(&mut self, name: &str, stream: Stream<T, Process<'a, L>, Unbounded, O, R>)
    where
        T: Serialize + DeserializeOwned + 'static,
        O: Ordering,
        R: Retries,
    {
        let port = self.port(stream);
        self.streams.push(ObservedStream {
            name: name.to_owned(),
            numeric: is_numeric::<T>(),
            connect: Box::new(move |nodes| {
                Box::pin(async move {
                    use futures::StreamExt as _;
                    let s = nodes.connect(port).await;
                    Box::pin(s.map(|t: T| Observation::of(&t))) as ObservationStream
                })
            }),
        });
    }

    fn port<L, T, O, R>(&self, stream: Stream<T, Process<'a, L>, Unbounded, O, R>) -> Port<T>
    where
        T: Serialize + DeserializeOwned,
        O: Ordering,
        R: Retries,
    {
        stream
            .weaken_ordering::<NoOrder>()
            .weaken_retries::<AtLeastOnce>()
            .send_bincode_external(&self.external)
    }

    /// The ids of requests that completed. An id is the sequence number the generator offered
    /// the request under (see [`LoadItem`]); a program that numbers its requests by enumerating
    /// its input over a single ordered connection gets this for free. The harness measures
    /// latency from offer to the arrival of the id here. At most one completions stream.
    pub fn completions<L, O, R>(&mut self, ids: Stream<u64, Process<'a, L>, Unbounded, O, R>)
    where
        O: Ordering,
        R: Retries,
    {
        assert!(
            self.completions.is_none(),
            "Observer::completions was called twice; merge the streams first"
        );
        self.completions = Some(self.port(ids));
    }

    /// The ids of requests the program gave up on.
    pub fn abandoned<L, O, R>(&mut self, ids: Stream<u64, Process<'a, L>, Unbounded, O, R>)
    where
        O: Ordering,
        R: Retries,
    {
        assert!(
            self.abandoned.is_none(),
            "Observer::abandoned was called twice; merge the streams first"
        );
        self.abandoned = Some(self.port(ids));
    }

    /// A level the program reports about itself (a queue depth, a table size), sampled whenever
    /// the stream emits. Appears as a column in the trace and is read by the verdict: a level
    /// still rising through the tail is a mark of collapse, a falling one of draining.
    pub fn gauge<L, O, R>(
        &mut self,
        name: &str,
        samples: Stream<f64, Process<'a, L>, Unbounded, O, R>,
    ) where
        O: Ordering,
        R: Retries,
    {
        self.gauges.push((name.to_owned(), self.port(samples)));
    }

    /// A count of events the program reports in increments (re-sends in the last window,
    /// requests served). Summed per second in the trace; not read by the verdict.
    pub fn counter<L, O, R>(
        &mut self,
        name: &str,
        increments: Stream<f64, Process<'a, L>, Unbounded, O, R>,
    ) where
        O: Ordering,
        R: Retries,
    {
        self.counters.push((name.to_owned(), self.port(increments)));
    }
}

#[cfg(stageleft_runtime)]
/// What a program under test returns, as the harness observes it. Implemented by hand on the
/// program's output type, in the program's crate, because the mapping from outputs to ids and
/// levels needs `q!` closures, which must live in a crate's own source:
///
/// ```ignore
/// #[cfg(feature = "sedge")]
/// impl<'a, T> sedge::Observed<'a> for RpcOutputs<'a, T> {
///     fn observe(self, obs: &mut sedge::Observer<'a>) {
///         obs.completions(self.completed.map(q!(|c| c.id)));
///         obs.abandoned(self.abandoned);
///         obs.gauge("backlog", self.server_metrics.map(q!(|(_, _, depth)| depth as f64)));
///     }
/// }
/// ```
///
/// Outputs not handed to the observer are dropped, which is fine on a deployment (unlike the
/// simulator, nothing is pruned). A program that registers no completions still gets a trace of
/// its gauges and counters, but no goodput or latency, and so no verdict beyond what the levels
/// say.
pub trait Observed<'a> {
    /// Register whatever the harness should watch.
    fn observe(self, observer: &mut Observer<'a>);
}

#[cfg(stageleft_runtime)]
impl Observed<'_> for () {
    fn observe(self, _: &mut Observer<'_>) {}
}
