//! Writing diagrams: `<name>.dot` always, `<name>.svg` when Graphviz is installed, under the
//! conventional `target/hydrofrac/<test>/<label>/` (docs/design/hydrofrac-lite.md §2, §4.6).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use super::{Options, Trace, dot};

/// Where [`write`] put a diagram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
    /// The Graphviz source.
    pub dot: PathBuf,
    /// The rendering, if Graphviz's `neato` is on the `PATH`.
    pub svg: Option<PathBuf>,
}

impl Written {
    /// The rendering if there is one, else the source.
    pub fn path(&self) -> &Path {
        self.svg.as_deref().unwrap_or(&self.dot)
    }
}

/// Writes the diagram of `trace` to `<dir>/<name>.dot` (creating `dir`), and renders
/// `<dir>/<name>.svg` beside it when Graphviz is installed. `name` is [`sanitize`]d. Rendering
/// is best effort: Graphviz is not a build dependency, and its absence or failure only leaves the
/// `.svg` out.
pub fn write(dir: &Path, name: &str, trace: &Trace, opts: &Options) -> std::io::Result<Written> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.dot", sanitize(name)));
    std::fs::write(&path, dot(trace, opts))?;
    let svg = render_svg(&path);
    Ok(Written { dot: path, svg })
}

/// Renders a pinned-position DOT file to SVG beside it with `neato -n2`, if it is on the `PATH`.
fn render_svg(dot: &Path) -> Option<PathBuf> {
    let svg = dot.with_extension("svg");
    let status = Command::new("neato")
        .args(["-n2", "-Tsvg"])
        .arg(dot)
        .arg("-o")
        .arg(&svg)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?;
    (status.success() && svg.exists()).then_some(svg)
}

/// The Cargo target directory: `$CARGO_TARGET_DIR` if set, else the nearest ancestor of the
/// running executable that holds Cargo's `CACHEDIR.TAG` (a test binary runs from
/// `<target>/<profile>/deps/`), else `target` in the working directory.
pub fn target_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR").filter(|d| !d.is_empty()) {
        return PathBuf::from(dir);
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.ancestors()
                .skip(1)
                .find(|dir| dir.join("CACHEDIR.TAG").is_file())
                .map(Path::to_path_buf)
        })
        .unwrap_or_else(|| PathBuf::from("target"))
}

/// The conventional diagram directory of `label` in test `test`:
/// `<target>/hydrofrac/<test>/<label>/`, both [`sanitize`]d.
pub fn default_dir(test: &str, label: &str) -> PathBuf {
    target_dir()
        .join("hydrofrac")
        .join(sanitize(test))
        .join(sanitize(label))
}

/// The current test's name: the last `::` segment of the thread name libtest gives it, or
/// `run` outside a test.
pub fn test_name() -> String {
    match std::thread::current().name() {
        Some(name) if name != "main" => name.rsplit("::").next().unwrap_or(name).to_owned(),
        _ => "run".to_owned(),
    }
}

/// `name` as one file-system path component: ASCII letters, digits, `-`, `_` and `.` are kept,
/// and each run of anything else becomes one `_`.
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

/// Directories handed out by [`claim`] in this process.
static CLAIMED: Mutex<Option<HashSet<PathBuf>>> = Mutex::new(None);

/// `dir`, or `dir-2`, `dir-3`, … if this process already claimed it (two checks of the same
/// label in one test keep both diagrams).
pub fn claim(dir: PathBuf) -> PathBuf {
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
