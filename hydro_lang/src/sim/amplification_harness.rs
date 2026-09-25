//! Runtime support for the harness that [`amplification_check`](super::amplification::amplification_check)
//! generates: how data inputs are given values, how a program's outputs are sunk so the dataflow
//! is not pruned, and where a run's summary is recorded so `cargo-check-amplification` can
//! tabulate it. Nothing here is specific to any program.
//!
//! # Values for data inputs
//!
//! A stream parameter that is not given a `round = ...` closure receives values from
//! [`InputValue::nth`], with `n` counting from zero over the whole run. The implementations for
//! the standard types return the counter itself (integers), a value derived from it (`bool`,
//! `char`, `String`), or the unit value, so request ids come out sequential and distinct, which
//! is what the hand-written harnesses in the corpus did. A program whose input type is its own
//! (an enum of operations, say) implements the trait in a few lines and may use `n` to fix the
//! mix. bolero's `TypeGenerator` was considered and not used: its derive is only reachable from
//! a crate that depends on bolero directly, which a user's crate does not, and its values are
//! arbitrary rather than sequential, which makes a report harder to read against the input.
//!
//! # Sinking outputs
//!
//! A Hydro dataflow whose outputs are not consumed is pruned at compile time, so every stream a
//! program returns must reach a `sim_output`. [`SimOutputs::sink_all`] does that for a single
//! stream, a keyed stream, a tuple of them, and, through `#[derive(SimOutputs)]`, a struct whose
//! fields are streams. The receivers are dropped at once; a `SimReceiver` is only a port id, so
//! dropping it does not close anything.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::amplification::Report;
use crate::live_collections::boundedness::Boundedness;
use crate::live_collections::keyed_stream::KeyedStream;
use crate::live_collections::stream::{Ordering, Retries, Stream};
use crate::location::cluster::Consistency;
use crate::location::{Cluster, Process};

/// A value the generated harness can feed to a data input. See the [module docs](self).
pub trait InputValue: Sized {
    /// The `n`th value, `n` counting from zero across the whole run. Implementations must be
    /// deterministic; where the type has enough values they should be distinct for distinct `n`.
    fn nth(n: u64) -> Self;
}

impl InputValue for () {
    fn nth(_: u64) -> Self {}
}

impl InputValue for bool {
    fn nth(n: u64) -> Self {
        n % 2 == 1
    }
}

impl InputValue for char {
    fn nth(n: u64) -> Self {
        char::from_u32(('a' as u32) + (n % 26) as u32).unwrap_or('a')
    }
}

impl InputValue for String {
    fn nth(n: u64) -> Self {
        n.to_string()
    }
}

macro_rules! input_value_int {
    ($($t:ty),*) => {
        $(impl InputValue for $t {
            fn nth(n: u64) -> Self {
                n as $t
            }
        })*
    };
}

input_value_int!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize);

impl<T: InputValue> InputValue for Option<T> {
    fn nth(n: u64) -> Self {
        Some(T::nth(n))
    }
}

impl<T: InputValue> InputValue for Vec<T> {
    fn nth(n: u64) -> Self {
        vec![T::nth(n)]
    }
}

impl<A: InputValue, B: InputValue> InputValue for (A, B) {
    fn nth(n: u64) -> Self {
        (A::nth(n), B::nth(n))
    }
}

impl<A: InputValue, B: InputValue, C: InputValue> InputValue for (A, B, C) {
    fn nth(n: u64) -> Self {
        (A::nth(n), B::nth(n), C::nth(n))
    }
}

/// Hands out successive [`InputValue`]s. One counter is shared by every data input of a run, so
/// no two inputs receive the same id.
#[derive(Debug, Default)]
pub struct InputCounter {
    next: u64,
}

impl InputCounter {
    /// A counter starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// The next value.
    pub fn next<T: InputValue>(&mut self) -> T {
        let v = T::nth(self.next);
        self.next += 1;
        v
    }

    /// Back to zero, so that every run of a check sees the same input.
    pub fn reset(&mut self) {
        self.next = 0;
    }
}

/// Everything a Hydro function may return that the generated harness knows how to sink. See the
/// [module docs](self).
pub trait SimOutputs {
    /// Attach every stream in `self` to a simulation output so the compiler keeps it.
    fn sink_all(self);
}

impl SimOutputs for () {
    fn sink_all(self) {}
}

impl<'a, T, L, B, O, R> SimOutputs for Stream<T, Process<'a, L>, B, O, R>
where
    T: Serialize + DeserializeOwned,
    B: Boundedness,
    O: Ordering,
    R: Retries,
{
    fn sink_all(self) {
        let _ = self.sim_output();
    }
}

impl<'a, T, L, B, C, O, R> SimOutputs for Stream<T, Cluster<'a, L, C>, B, O, R>
where
    T: Serialize + DeserializeOwned,
    B: Boundedness,
    C: Consistency,
    O: Ordering,
    R: Retries,
{
    fn sink_all(self) {
        let _ = self.sim_cluster_output();
    }
}

impl<'a, K, V, L, B, O, R> SimOutputs for KeyedStream<K, V, Process<'a, L>, B, O, R>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    B: Boundedness,
    O: Ordering,
    R: Retries,
{
    fn sink_all(self) {
        let _ = self.entries().sim_output();
    }
}

impl<'a, K, V, L, B, C, O, R> SimOutputs for KeyedStream<K, V, Cluster<'a, L, C>, B, O, R>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    B: Boundedness,
    C: Consistency,
    O: Ordering,
    R: Retries,
{
    fn sink_all(self) {
        let _ = self.entries().sim_cluster_output();
    }
}

macro_rules! sim_outputs_tuple {
    ($($name:ident),+) => {
        impl<$($name: SimOutputs),+> SimOutputs for ($($name,)+) {
            #[allow(non_snake_case)]
            fn sink_all(self) {
                let ($($name,)+) = self;
                $($name.sink_all();)+
            }
        }
    };
}

sim_outputs_tuple!(A);
sim_outputs_tuple!(A, B);
sim_outputs_tuple!(A, B, C);
sim_outputs_tuple!(A, B, C, D);
sim_outputs_tuple!(A, B, C, D, E);
sim_outputs_tuple!(A, B, C, D, E, F);
sim_outputs_tuple!(A, B, C, D, E, F, G);
sim_outputs_tuple!(A, B, C, D, E, F, G, H);

/// What one generated check records for the summary table.
#[derive(Debug)]
pub struct Summary<'r> {
    /// The crate the annotated function lives in (`CARGO_PKG_NAME`).
    pub crate_name: &'static str,
    /// The manifest directory of that crate (`CARGO_MANIFEST_DIR`), used to find `target/`.
    pub manifest_dir: &'static str,
    /// The module path of the annotated function.
    pub module: &'static str,
    /// The annotated function's name.
    pub function: &'static str,
    /// The configuration's name, or `"default"`.
    pub configuration: &'static str,
    /// The per-round rate of every stream input, `name=rate` joined by `, `.
    pub workload: String,
    /// The checker's report.
    pub report: &'r Report,
}

/// The directory results are written to: `$AMPLIFICATION_RESULTS_DIR` if set, otherwise
/// `target/amplification/` under the nearest ancestor of `manifest_dir` that holds a
/// `Cargo.lock`, which is the workspace root.
pub fn results_dir(manifest_dir: &str) -> PathBuf {
    if let Ok(dir) = std::env::var("AMPLIFICATION_RESULTS_DIR") {
        return PathBuf::from(dir);
    }
    let mut cur: Option<&Path> = Some(Path::new(manifest_dir));
    while let Some(dir) = cur {
        if dir.join("Cargo.lock").exists() {
            return dir.join("target").join("amplification");
        }
        cur = dir.parent();
    }
    Path::new(manifest_dir).join("target").join("amplification")
}

/// The one-line form of a report for the table: verdict, location, first reaction, extra work.
fn summary_fields(report: &Report) -> BTreeMap<&'static str, String> {
    let mut f = BTreeMap::new();
    f.insert("verdict", report.verdict.to_string());
    f.insert("horizon", report.horizon.to_string());
    f.insert(
        "location",
        report
            .location
            .as_ref()
            .map(|l| super::amplification::readable_hook_name(&l.hook, false))
            .unwrap_or_else(|| "-".to_owned()),
    );
    f.insert(
        "first_reaction",
        report
            .location
            .as_ref()
            .map(|l| l.first_reaction_at.to_string())
            .unwrap_or_else(|| "-".to_owned()),
    );
    f.insert(
        "extra_work",
        report
            .location
            .as_ref()
            .map(|l| format!("{} in {}", l.extra_work, l.rose))
            .unwrap_or_else(|| "-".to_owned()),
    );
    f.insert("seconds", format!("{:.1}", report.seconds));
    f
}

/// Append one line to `results.tsv` and write the full report to `reports/`. Failures to write
/// are reported on standard error and otherwise ignored, so a missing directory never fails a
/// test.
pub fn record(summary: &Summary<'_>) {
    let dir = results_dir(summary.manifest_dir);
    let fields = summary_fields(summary.report);
    let line = [
        summary.crate_name.to_owned(),
        summary.module.to_owned(),
        summary.function.to_owned(),
        summary.configuration.to_owned(),
        summary.workload.clone(),
        fields["horizon"].clone(),
        fields["verdict"].clone(),
        fields["location"].clone(),
        fields["first_reaction"].clone(),
        fields["extra_work"].clone(),
        fields["seconds"].clone(),
    ]
    .join("\t");
    let write = || -> std::io::Result<()> {
        fs::create_dir_all(dir.join("reports"))?;
        let mut tsv = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("results.tsv"))?;
        writeln!(tsv, "{line}")?;
        let file = format!(
            "{}.{}.{}.{}.txt",
            summary.crate_name,
            summary.function,
            summary.configuration,
            summary.workload.replace([' ', ','], "").replace('=', "-")
        );
        fs::write(
            dir.join("reports").join(file),
            format!(
                "{} {} ({}) workload {}\n\n{}",
                summary.module,
                summary.function,
                summary.configuration,
                summary.workload,
                summary.report
            ),
        )?;
        Ok(())
    };
    if let Err(e) = write() {
        eprintln!("amplification check: could not record results under {}: {e}", dir.display());
    }
}
