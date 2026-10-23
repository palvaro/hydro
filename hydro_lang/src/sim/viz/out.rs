//! Automatic pictures of the counterexamples a search finds.
//!
//! When a diagram directory is configured, by [`Ldfi::viz_out`](crate::sim::ldfi::Ldfi::viz_out)
//! or by the `MOLLY_VIZ_OUT` environment variable, the search keeps what the pictures need by
//! itself (the event trace of every explored schedule, as under
//! [`Ldfi::with_traces`](crate::sim::ldfi::Ldfi::with_traces), and the why-provenance graph of
//! every derivation, as under
//! [`Ldfi::with_provenance_graphs`](crate::sim::ldfi::Ldfi::with_provenance_graphs)), and writes
//! Molly's zoom levels of every counterexample into one directory:
//!
//! | file | what | rendered with |
//! |---|---|---|
//! | `failing.dot` / `.png` | the Lamport diagram of the failing run, faults marked ([`super::lamport::dot`]) | `neato -n2` |
//! | `witness.dot` / `.png` | the last run in which the goal held, the goal's support (the sufficient set of deliveries) highlighted ([`Counterexample::support_lamport_dot`](super::Counterexample::support_lamport_dot)) | `neato -n2` |
//! | `provenance.dot` / `.png` | the why-provenance graph behind that support, executions collapsed ([`super::provenance::dot`]) | `dot` |
//! | `report.md` | all of it as Markdown with Mermaid diagrams ([`Counterexample::markdown`](super::Counterexample::markdown)) | |
//!
//! The witness and its provenance graph are missing when no run of the search had the goal
//! hold with a non-empty derivation. A search runs at one lineage level, so the files carry no
//! level suffix; the provenance graph's title and `report.md` name it. PNGs are rendered at
//! `-Gdpi=110` when the Graphviz engine is on the `PATH` (silently skipped otherwise; the
//! report says so).
//!
//! Layout, for a root `<dir>`:
//!
//! | search | under `Ldfi::viz_out(<dir>)` | under `MOLLY_VIZ_OUT=<dir>` |
//! |---|---|---|
//! | `search_all_labels` | `<dir>/<cluster>/<collection>/` | `<dir>/<test>/<cluster>/<collection>/` |
//! | `search` / `run` (hand-written goal) | `<dir>/` | `<dir>/<test>/` |
//!
//! `<cluster>` and `<collection>` are the names the report uses ([`LabelResults`]'s `Display`);
//! two collections of a cluster with the same name are told apart by their line
//! (`<collection>_<line>`). `<test>` is the name of the current test (the last segment of the
//! thread name libtest gives it; `search` outside a test). Names are sanitized for the file
//! system. A directory is claimed once per process: a second search of the same test into the
//! same directory (the same test searched at two levels, say) gets `-2`, `-3`, ….
//!
//! [`LabelResults`]: crate::sim::ldfi::LabelResults

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use super::provenance::{self, ProvOptions};
use crate::sim::ldfi::{Level, Outcome};

/// The environment variable naming the diagram directory.
pub const ENV: &str = "MOLLY_VIZ_OUT";

/// One picture: its Graphviz source and, if the engine was available, its rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Picture {
    /// The Graphviz source (`<name>.dot`).
    pub dot: PathBuf,
    /// The rendering (`<name>.png`), if the Graphviz engine was on the `PATH`.
    pub png: Option<PathBuf>,
}

impl Picture {
    /// The rendering if there is one, else the DOT source.
    pub fn path(&self) -> &Path {
        self.png.as_deref().unwrap_or(&self.dot)
    }
}

/// Where a counterexample's pictures were written (see the [module](self) docs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagram {
    /// The directory holding them.
    pub dir: PathBuf,
    /// The failing run (`failing.dot`).
    pub failing: Picture,
    /// The witness run with the goal's support highlighted (`witness.dot`), if the search saw
    /// the goal hold with a derivation.
    pub witness: Option<Picture>,
    /// The why-provenance graph behind the witness's support (`provenance.dot`).
    pub provenance: Option<Picture>,
    /// The Markdown report (`report.md`), if it could be written.
    pub report: Option<PathBuf>,
}

impl Diagram {
    /// The failing run's rendering if there is one, else its DOT source.
    pub fn path(&self) -> &Path {
        self.failing.path()
    }

    /// Every picture written, failing run first.
    pub fn pictures(&self) -> impl Iterator<Item = &Picture> {
        std::iter::once(&self.failing)
            .chain(self.witness.as_ref())
            .chain(self.provenance.as_ref())
    }
}

/// The name a lineage level goes by in titles.
fn level_name(level: Level) -> &'static str {
    match level {
        Level::Execution => "execution",
        Level::Precise => "precise",
    }
}

/// The diagram root: `configured` (from [`Ldfi::viz_out`](crate::sim::ldfi::Ldfi::viz_out)),
/// else `$MOLLY_VIZ_OUT/<test>`. `None` when neither is set.
pub(crate) fn root(configured: Option<&Path>) -> Option<PathBuf> {
    if let Some(dir) = configured {
        return Some(dir.to_owned());
    }
    let env = std::env::var_os(ENV).filter(|v| !v.is_empty())?;
    Some(PathBuf::from(env).join(sanitize(&test_name())))
}

/// The current test's name: the last `::` segment of the thread name libtest gives it, or
/// `search` outside a test.
pub(crate) fn test_name() -> String {
    match std::thread::current().name() {
        Some(name) if name != "main" => name.rsplit("::").next().unwrap_or(name).to_owned(),
        _ => "search".to_owned(),
    }
}

/// `name` as one file-system path component: ASCII letters, digits, `-`, `_` and `.` are kept,
/// runs of anything else become one `_`.
pub fn sanitize(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
            out.push(c);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let out = out.trim_matches(|c| c == '_' || c == '.');
    if out.is_empty() {
        "_".to_owned()
    } else {
        out.to_owned()
    }
}

/// Directories handed out in this process.
static CLAIMED: Mutex<Option<HashSet<PathBuf>>> = Mutex::new(None);

/// `dir`, or `dir-2`, `dir-3`, … if an earlier search of this process already wrote there.
pub(crate) fn claim(dir: PathBuf) -> PathBuf {
    let mut claimed = CLAIMED.lock().unwrap_or_else(|e| e.into_inner());
    let claimed = claimed.get_or_insert_with(HashSet::new);
    let mut candidate = dir.clone();
    let mut n = 1;
    while claimed.contains(&candidate) {
        n += 1;
        let mut name = dir.file_name().unwrap_or_default().to_owned();
        name.push(format!("-{n}"));
        candidate = dir.with_file_name(name);
    }
    claimed.insert(candidate.clone());
    candidate
}

/// Writes `outcome`'s pictures into `dir` (see the [module](self) docs). `title` names the
/// counterexample (`<cluster>/<collection>`, or the test) in the provenance graph's title;
/// `level` is the search's lineage level. `None` if `outcome` is not a counterexample with a
/// trace, or the failing run could not be written (reported on stderr: a diagram is never
/// worth failing a test over); a missing witness or provenance graph only leaves its picture
/// out.
pub(crate) fn write(dir: &Path, outcome: &Outcome, title: &str, level: Level) -> Option<Diagram> {
    let viz = outcome.viz()?;
    let failing_dot = viz.lamport_dot()?;
    if let Err(e) = std::fs::create_dir_all(dir) {
        eprintln!("hydrofrac: could not create {}: {e}", dir.display());
        return None;
    }
    let failing = write_picture(dir, "failing", &failing_dot, Engine::Neato)?;
    let witness = viz
        .support_lamport_dot()
        .and_then(|dot| write_picture(dir, "witness", &dot, Engine::Neato));
    let provenance = viz
        .witness()
        .and_then(|witness| {
            let index = witness
                .schedule
                .derivations
                .iter()
                .position(|d| std::ptr::eq(d, witness.derivation))?;
            witness.schedule.provenance.get(index)
        })
        .and_then(|graph| {
            let dot = provenance::dot(
                graph,
                &ProvOptions {
                    collapse_execs: true,
                    title: Some(format!(
                        "{title}: why the goal held ({}-level lineage)",
                        level_name(level)
                    )),
                    ..ProvOptions::default()
                },
            );
            write_picture(dir, "provenance", &dot, Engine::Dot)
        });
    let report = dir.join("report.md");
    let markdown = format!(
        "{}\n_Lineage level: {}._\n",
        viz.markdown(),
        level_name(level)
    );
    let report = match std::fs::write(&report, markdown) {
        Ok(()) => Some(report),
        Err(e) => {
            eprintln!("hydrofrac: could not write {}: {e}", report.display());
            None
        }
    };
    Some(Diagram {
        dir: dir.to_owned(),
        failing,
        witness,
        provenance,
        report,
    })
}

/// The Graphviz engine a picture is laid out with.
#[derive(Clone, Copy)]
enum Engine {
    /// Pinned positions (`neato -n2`): the Lamport diagrams.
    Neato,
    /// Ranked layout (`dot`): the provenance graph.
    Dot,
}

/// Writes `<dir>/<name>.dot` and renders it beside it.
fn write_picture(dir: &Path, name: &str, dot: &str, engine: Engine) -> Option<Picture> {
    let path = dir.join(format!("{name}.dot"));
    if let Err(e) = std::fs::write(&path, dot) {
        eprintln!("hydrofrac: could not write {}: {e}", path.display());
        return None;
    }
    let png = render(&path, engine);
    Some(Picture { dot: path, png })
}

/// Renders a DOT file to PNG beside it, if the engine is on the `PATH`.
fn render(dot: &Path, engine: Engine) -> Option<PathBuf> {
    let png = dot.with_extension("png");
    let mut command = match engine {
        Engine::Neato => {
            let mut c = Command::new("neato");
            c.arg("-n2");
            c
        }
        Engine::Dot => Command::new("dot"),
    };
    let status = command
        .args(["-Tpng", "-Gdpi=110"])
        .arg(dot)
        .arg("-o")
        .arg(&png)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?;
    status.success().then_some(png)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes() {
        assert_eq!(sanitize("learners"), "learners");
        assert_eq!(sanitize("Learner cluster"), "Learner_cluster");
        assert_eq!(sanitize("cluster loc3v1"), "cluster_loc3v1");
        assert_eq!(sanitize("../a/b"), "a_b");
        assert_eq!(sanitize("::"), "_");
    }

    #[test]
    fn claims_are_unique() {
        let base = PathBuf::from("/nonexistent/hydrofrac-claim-test/x");
        assert_eq!(claim(base.clone()), base);
        assert_eq!(
            claim(base.clone()),
            PathBuf::from("/nonexistent/hydrofrac-claim-test/x-2")
        );
        assert_eq!(
            claim(base),
            PathBuf::from("/nonexistent/hydrofrac-claim-test/x-3")
        );
    }
}
