//! Writing diagrams under the conventional `target/hydrofrac/<test>/<label>/`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use super::{Trace, dot};

/// Where [`write()`] put a diagram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
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

/// Writes the diagram of `trace` to `<dir>/<name>.dot` and, best effort (Graphviz is not a
/// build dependency), renders `<dir>/<name>.svg` beside it with `neato -n2`.
pub fn write(dir: &Path, name: &str, trace: &Trace, title: &str) -> std::io::Result<Written> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.dot", sanitize(name)));
    std::fs::write(&path, dot(trace, title))?;
    let svg = path.with_extension("svg");
    let rendered = Command::new("neato")
        .args(["-n2", "-Tsvg"])
        .arg(&path)
        .arg("-o")
        .arg(&svg)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    Ok(Written {
        dot: path,
        svg: (rendered && svg.exists()).then_some(svg),
    })
}

/// `<target>/hydrofrac/<test>/<label>/`, where `<target>` is `$CARGO_TARGET_DIR`, else the
/// ancestor of the running executable holding Cargo's `CACHEDIR.TAG`, else `target`.
pub fn default_dir(test: &str, label: &str) -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            let exe = std::env::current_exe().ok()?;
            exe.ancestors()
                .skip(1)
                .find(|dir| dir.join("CACHEDIR.TAG").is_file())
                .map(Path::to_path_buf)
        })
        .unwrap_or_else(|| PathBuf::from("target"));
    target
        .join("hydrofrac")
        .join(sanitize(test))
        .join(sanitize(label))
}

/// The current test's name (the last `::` segment of libtest's thread name), or `run`.
pub fn test_name() -> String {
    match std::thread::current().name() {
        Some(name) if name != "main" => name.rsplit("::").next().unwrap_or(name).to_owned(),
        _ => "run".to_owned(),
    }
}

/// `name` as one path component: ASCII letters, digits, `-`, `_` and `.` are kept, and each run
/// of anything else becomes one `_`.
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
    if out.is_empty() { "_" } else { out }.to_owned()
}

static CLAIMED: Mutex<Option<HashSet<PathBuf>>> = Mutex::new(None);

/// `dir`, or `dir-2`, `dir-3`, … if this process already claimed it (two checks of the same
/// label in one test keep both diagrams).
pub fn claim(dir: PathBuf) -> PathBuf {
    let mut claimed = CLAIMED.lock().unwrap_or_else(|e| e.into_inner());
    let claimed = claimed.get_or_insert_with(HashSet::new);
    let mut candidate = dir.clone();
    for n in 2.. {
        if !claimed.contains(&candidate) {
            break;
        }
        let mut name = dir.file_name().unwrap_or_default().to_owned();
        name.push(format!("-{n}"));
        candidate = dir.with_file_name(name);
    }
    claimed.insert(candidate.clone());
    candidate
}
