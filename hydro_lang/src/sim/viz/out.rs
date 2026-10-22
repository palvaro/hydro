//! Automatic Lamport diagrams of the counterexamples a search finds.
//!
//! When a diagram directory is configured, by [`Ldfi::viz_out`](crate::sim::ldfi::Ldfi::viz_out)
//! or by the `MOLLY_VIZ_OUT` environment variable, every counterexample the search finds is
//! drawn as the Lamport diagram of its failing run ([`super::lamport::dot`]), written as
//! `failing.dot`, and rendered to `failing.png` with `neato -n2 -Tpng -Gdpi=110` when Graphviz's
//! `neato` is on the `PATH` (silently skipped otherwise; the report says so). The failing run's
//! trace is always retained (see [`Schedule::trace`](crate::sim::ldfi::Schedule::trace)), so no
//! extra configuration is needed.
//!
//! Layout, for a root `<dir>`:
//!
//! | search | under `Ldfi::viz_out(<dir>)` | under `MOLLY_VIZ_OUT=<dir>` |
//! |---|---|---|
//! | `search_all_labels` | `<dir>/<cluster>/<collection>/failing.dot` | `<dir>/<test>/<cluster>/<collection>/failing.dot` |
//! | `search` / `run` (hand-written goal) | `<dir>/failing.dot` | `<dir>/<test>/failing.dot` |
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

use crate::sim::ldfi::Outcome;

/// The environment variable naming the diagram directory.
pub const ENV: &str = "MOLLY_VIZ_OUT";

/// Where a counterexample's diagram was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagram {
    /// The Graphviz source (`failing.dot`).
    pub dot: PathBuf,
    /// The rendering (`failing.png`), if `neato` was available.
    pub png: Option<PathBuf>,
}

impl Diagram {
    /// The rendering if there is one, else the DOT source.
    pub fn path(&self) -> &Path {
        self.png.as_deref().unwrap_or(&self.dot)
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

/// Writes `outcome`'s failing run into `dir` (`failing.dot`, and `failing.png` if `neato` is
/// available). `None` if `outcome` is not a counterexample with a trace, or the write failed
/// (reported on stderr: a diagram is never worth failing a test over).
pub(crate) fn write(dir: &Path, outcome: &Outcome) -> Option<Diagram> {
    let dot = outcome.viz()?.lamport_dot()?;
    let path = dir.join("failing.dot");
    if let Err(e) = std::fs::create_dir_all(dir).and_then(|()| std::fs::write(&path, dot)) {
        eprintln!("hydrofrac: could not write {}: {e}", path.display());
        return None;
    }
    let png = render(&path);
    Some(Diagram { dot: path, png })
}

/// Renders a pinned-layout DOT file to PNG beside it, if `neato` is on the `PATH`.
fn render(dot: &Path) -> Option<PathBuf> {
    let png = dot.with_extension("png");
    let status = Command::new("neato")
        .args(["-n2", "-Tpng", "-Gdpi=110"])
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
