//! Human names for program sites in hydrofrac reports, read back from the user's source.
//!
//! A labelled collection or a network channel is identified by the `file:line` of the user's
//! call that built it (see [`super::slice`]). For reports, a name the user would recognize is
//! better: the variable the statement binds (`let learned = initial.merge_unordered(..)` names
//! `learned`), else the function whose tail expression the call is (`splice_epoch_log`). The
//! name is recovered by reading the source file at the recorded line and walking back to the
//! start of the statement. Everything here is best-effort: when the file cannot be read or the
//! statement binds nothing nameable, the caller falls back to the operator or the site.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A user frame: the file (as recorded), the line, and the enclosing function's path.
#[derive(Clone, Debug)]
pub(crate) struct UserFrame {
    /// The file, as the backtrace recorded it (absolute, or relative to some crate root).
    pub file: String,
    /// The 1-based line.
    pub line: u32,
    /// The enclosing function (its full path, closures stripped), if known.
    pub function: Option<String>,
}

/// The first frame of `backtrace` outside `hydro_lang` itself: where the program called the
/// operator (or `send`), rather than the library's own frames.
#[cfg(feature = "build")]
pub(crate) fn user_frame(
    backtrace: &crate::compile::ir::backtrace::Backtrace,
) -> Option<UserFrame> {
    let own = concat!("hydro_lang", "/src/");
    backtrace.elements().find_map(|element| {
        let file = element.filename.as_ref()?;
        if file.contains(own) {
            return None;
        }
        Some(UserFrame {
            file: file.clone(),
            line: element.lineno?,
            function: Some(element.fn_name.clone()),
        })
    })
}

#[cfg(not(feature = "build"))]
pub(crate) fn user_frame(
    _backtrace: &crate::compile::ir::backtrace::Backtrace,
) -> Option<UserFrame> {
    None
}

thread_local! {
    /// Source files read so far (by recorded path), `None` when unreadable.
    static SOURCES: RefCell<HashMap<String, Option<Vec<String>>>> = RefCell::new(HashMap::new());
}

/// Resolves a recorded source path: as is, else relative to the current directory and each of
/// its ancestors (a backtrace may record a path relative to the workspace root).
fn resolve(file: &str) -> Option<PathBuf> {
    let path = Path::new(file);
    if path.is_file() {
        return Some(path.to_owned());
    }
    if path.is_absolute() {
        return None;
    }
    let cwd = std::env::current_dir().ok()?;
    cwd.ancestors()
        .map(|dir| dir.join(path))
        .find(|candidate| candidate.is_file())
}

fn with_source<R>(file: &str, f: impl FnOnce(&[String]) -> Option<R>) -> Option<R> {
    SOURCES.with(|sources| {
        let mut sources = sources.borrow_mut();
        let lines = sources.entry(file.to_owned()).or_insert_with(|| {
            let text = std::fs::read_to_string(resolve(file)?).ok()?;
            Some(text.lines().map(str::to_owned).collect())
        });
        f(lines.as_deref()?)
    })
}

/// The name of the site at `frame`: the variable its statement binds, else the enclosing
/// function when the call is that function's tail expression. `None` when neither applies or
/// the source is unavailable.
pub(crate) fn site_name(frame: &UserFrame) -> Option<String> {
    let statement_start = with_source(&frame.file, |lines| binding_at(lines, frame.line as usize))?;
    match statement_start {
        Binding::Let(name) => Some(name),
        Binding::BlockStart => frame.function.as_deref().and_then(function_name),
    }
}

enum Binding {
    /// The statement is `let <name> = ..`.
    Let(String),
    /// The statement is the first of a block (likely a function's tail expression).
    BlockStart,
}

/// Walks back from `line` (1-based) to the start of its statement and reads its binding.
fn binding_at(lines: &[String], line: usize) -> Option<Binding> {
    let target = line.checked_sub(1)?;
    if target >= lines.len() {
        return None;
    }
    // A statement spans at most this many lines back from the call, for the walk.
    const MAX_LINES: usize = 40;
    for i in (target.saturating_sub(MAX_LINES)..=target).rev() {
        let code = strip_comment(&lines[i]);
        let trimmed = code.trim();
        if i < target && (trimmed.ends_with(';') || trimmed.ends_with('}')) {
            return completed_cycle(&lines[i + 1..=target]).map(Binding::Let);
        }
        if let Some(name) = let_name(trimmed) {
            return Some(Binding::Let(name));
        }
        if i < target && trimmed.ends_with('{') {
            return Some(Binding::BlockStart);
        }
    }
    None
}

/// For a statement `<name>_handle.complete(..)` (closing a `forward_ref` cycle) whose first
/// code line is among `lines`, `<name>`: the stream the cycle defines.
fn completed_cycle(lines: &[String]) -> Option<String> {
    let first = lines
        .iter()
        .map(|line| strip_comment(line).trim())
        .find(|line| !line.is_empty())?;
    let (receiver, _) = first.split_once(".complete(")?;
    if !receiver.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let name = receiver.strip_suffix("_handle").unwrap_or(receiver);
    simple_name(name)
}

/// The code of a line without a trailing `//` comment (a `//` inside a string is rare enough
/// in a dataflow chain to ignore).
fn strip_comment(line: &str) -> &str {
    line.find("//").map_or(line, |i| &line[..i])
}

/// The identifier `let [mut] <name>` binds, if the line starts a `let`. For a tuple pattern,
/// the last identifier: the pairs dataflow builders return put the stream last
/// (`let (handle, stream) = forward_ref()`, `let (port, stream) = sim_input()`).
fn let_name(code: &str) -> Option<String> {
    let rest = code.strip_prefix("let ")?.trim_start();
    if let Some(tuple) = rest.strip_prefix('(') {
        let tuple = &tuple[..tuple.find(')')?];
        return tuple
            .split(',')
            .rev()
            .find_map(|part| simple_name(part.trim()));
    }
    simple_name(rest)
}

/// The identifier at the start of a pattern (`mut` and leading underscores skipped).
fn simple_name(pattern: &str) -> Option<String> {
    let rest = pattern.strip_prefix("mut ").unwrap_or(pattern).trim_start();
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let name = name.trim_start_matches('_');
    (!name.is_empty() && !name.starts_with(|c: char| c.is_ascii_digit())).then(|| name.to_owned())
}

/// The last segment of a function path, without generics or closure markers.
fn function_name(path: &str) -> Option<String> {
    let segments: Vec<&str> = path
        .split("::")
        .filter(|segment| !segment.starts_with('{') && !segment.starts_with('<'))
        .map(|segment| segment.split('<').next().unwrap_or(segment))
        .collect();
    segments
        .into_iter()
        .rev()
        .find(|segment| {
            !segment.is_empty() && segment.chars().all(|c| c.is_alphanumeric() || c == '_')
        })
        .map(str::to_owned)
}

/// The short form of a Rust type name: its last path segment, without generics
/// (`hydro_test::cluster::paxos::Learner` → `Learner`).
pub(crate) fn short_type_name(type_name: &str) -> String {
    let base = type_name.split('<').next().unwrap_or(type_name);
    base.rsplit("::").next().unwrap_or(base).to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    fn name(text: &str, line: usize) -> Option<String> {
        match binding_at(&lines(text), line)? {
            Binding::Let(name) => Some(name),
            Binding::BlockStart => Some("<block>".to_owned()),
        }
    }

    #[test]
    fn binding_names() {
        let src = "\
fn f() {
    let x = 1;
    let initial = chosen
        .clone() // a comment;
        .broadcast_closed(learners, TCP.fail_stop().bincode())
        .values();
    let learned = initial.merge_unordered(echo_fwd).unique();
    let (a, b) = foo
        .bar();
    handle.complete(x
        .map());
    views_handle.complete(
        views.broadcast(),
    );
}
fn g() -> u32 {
    facts.fold(
        q!(|| 0),
    )
}";
        assert_eq!(name(src, 5).as_deref(), Some("initial"));
        assert_eq!(name(src, 7).as_deref(), Some("learned"));
        assert_eq!(name(src, 9).as_deref(), Some("b"));
        assert_eq!(name(src, 11).as_deref(), Some("handle"));
        assert_eq!(name(src, 13).as_deref(), Some("views"));
        assert_eq!(name(src, 17).as_deref(), Some("<block>"));
    }

    #[test]
    fn function_names() {
        assert_eq!(
            function_name("hydro_std::demos::splice_epoch_log").as_deref(),
            Some("splice_epoch_log")
        );
        assert_eq!(
            function_name("hydro_std::demos::paxos::{{closure}}").as_deref(),
            Some("paxos")
        );
        assert_eq!(short_type_name("a::b::Learner"), "Learner");
        assert_eq!(short_type_name("a::b::Learner<a::X>"), "Learner");
    }
}
