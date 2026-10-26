//! Source-derived names for reports (docs/design/hydrofrac-lite.md §3), read back from the
//! user's source at a site's `file:line`: the variable the statement binds, else the receiver
//! of an unbound `x.assert_has_consistency_of(..)`, else the function whose tail it is. Best
//! effort: callers fall back to the operator or the site. [`short_debug`] makes `Debug`
//! renderings of facts readable.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hydro_lang::compile::ir::backtrace::Backtrace;

/// A user frame: the file (as recorded), the 1-based line, and the enclosing function's path.
#[derive(Clone, Debug)]
pub struct UserFrame {
    pub file: String,
    pub line: u32,
    pub function: Option<String>,
}

/// The first frame of `backtrace` outside `hydro_lang` itself: where the program called it.
pub fn user_frame(backtrace: &Backtrace) -> Option<UserFrame> {
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

thread_local! {
    /// Source files read so far (by recorded path), `None` when unreadable.
    static SOURCES: RefCell<HashMap<String, Option<Vec<String>>>> = RefCell::new(HashMap::new());
}

/// Resolves a recorded source path: as is, else relative to the current directory or an
/// ancestor (a backtrace may record a path relative to the workspace root).
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

/// The name of the site at `frame` (see the module docs): a statement that binds nothing is
/// named by its receiver if it is `<name>.assert_has_consistency_of(..)`, else by the
/// enclosing function if it is the first statement of its block or the block's tail.
pub fn site_name(frame: &UserFrame) -> Option<String> {
    let line = frame.line as usize;
    let function = || frame.function.as_deref().and_then(function_name);
    with_source(&frame.file, |lines| match binding_at(lines, line)? {
        Binding::Let(name) => Some(name),
        Binding::Statement {
            start,
            first_in_block,
        } => {
            let target = line - 1;
            asserted_receiver(&lines[start..=target]).or_else(|| {
                (first_in_block || is_tail(lines, start))
                    .then(function)
                    .flatten()
            })
        }
    })
}

enum Binding {
    /// The statement is `let <name> = ..`, or closes the `forward_ref` cycle of `<name>`.
    Let(String),
    /// The statement binds nothing. It starts at 0-based line `start`, and `first_in_block`
    /// when it is the first statement of a block.
    Statement { start: usize, first_in_block: bool },
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
            return Some(match completed_cycle(&lines[i + 1..=target]) {
                Some(name) => Binding::Let(name),
                None => Binding::Statement {
                    start: first_code_line(lines, i + 1, target),
                    first_in_block: false,
                },
            });
        }
        if let Some(name) = let_name(trimmed) {
            return Some(Binding::Let(name));
        }
        if i < target && trimmed.ends_with('{') {
            return Some(Binding::Statement {
                start: first_code_line(lines, i + 1, target),
                first_in_block: true,
            });
        }
    }
    None
}

/// The first line in `from..=to` with code on it (`to` if none).
fn first_code_line(lines: &[String], from: usize, to: usize) -> usize {
    (from..=to)
        .find(|&i| !strip_comment(&lines[i]).trim().is_empty())
        .unwrap_or(to)
}

/// For a statement `<name>.assert_has_consistency_of(..)` (its code lines up to the call),
/// `<name>`.
fn asserted_receiver(statement: &[String]) -> Option<String> {
    let code: String = statement
        .iter()
        .map(|line| strip_comment(line))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let (receiver, _) = code.split_once(".assert_has_consistency_of")?;
    if receiver.is_empty() || !receiver.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    simple_name(receiver)
}

/// Whether the statement starting at 0-based line `start` is its block's tail expression: it
/// ends at the block's closing `}` rather than at a `;` (comments and literals skipped).
fn is_tail(lines: &[String], start: usize) -> bool {
    // A statement spans at most this many lines forward, for the scan.
    const MAX_LINES: usize = 200;
    let mut depth: i64 = 0;
    let mut block_comment = 0usize;
    for line in lines.iter().skip(start).take(MAX_LINES) {
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            if block_comment > 0 {
                if c == '*' && next == Some('/') {
                    block_comment -= 1;
                    i += 2;
                } else if c == '/' && next == Some('*') {
                    block_comment += 1;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }
            match c {
                '/' if next == Some('/') => break,
                '/' if next == Some('*') => {
                    block_comment += 1;
                    i += 2;
                    continue;
                }
                // A string spanning lines is rare enough in a dataflow chain to ignore.
                '"' => {
                    i = literal_end(&chars, i);
                    continue;
                }
                '\'' if is_char_literal(&chars, i) => {
                    i = literal_end(&chars, i);
                    continue;
                }
                '(' | '[' | '{' => depth += 1,
                ')' | ']' => depth -= 1,
                '}' => {
                    depth -= 1;
                    if depth < 0 {
                        return true;
                    }
                }
                ';' if depth == 0 => return false,
                _ => {}
            }
            i += 1;
        }
    }
    false
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
    // v0-mangled symbols (the default since Rust 1.98) demangle with their generic arguments
    // (`f::<a::B, c::D>`), whose paths would otherwise supply the last segment: drop every
    // balanced `<...>` group first.
    let mut stripped = String::with_capacity(path.len());
    let mut depth = 0usize;
    for c in path.chars() {
        match c {
            '<' => depth += 1,
            '>' if depth > 0 => depth -= 1,
            _ if depth == 0 => stripped.push(c),
            _ => {}
        }
    }
    let segments: Vec<&str> = stripped
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

/// The last path segment of a type name, without generics (`a::b::Learner<X>` → `Learner`).
fn short_type_name(type_name: &str) -> String {
    let base = type_name.split('<').next().unwrap_or(type_name);
    base.rsplit("::").next().unwrap_or(base).to_owned()
}

/// A `Debug` rendering made readable: module paths dropped (keeping everything from the first
/// capitalized segment, so `Kind::A` stays) and member ids as `Client@0`, the way node names
/// are. Literals are left alone.
pub fn short_debug(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || (c == '\'' && is_char_literal(&chars, i)) {
            let end = literal_end(&chars, i);
            out.extend(&chars[i..end]);
            i = end;
            continue;
        }
        let at_word_start = i == 0 || !is_ident_char(chars[i - 1]);
        if at_word_start && (c.is_alphabetic() || c == '_') {
            let (segments, end) = read_path(&chars, i);
            let kept = short_path(&segments);
            let member_id = if kept.last().is_some_and(|s| s == "MemberId") {
                member_id_parts(&chars, end)
            } else {
                None
            };
            if let Some((tag, member, after)) = member_id {
                out.push_str(&member_label(&tag));
                out.push('@');
                out.push_str(&member);
                i = after;
                continue;
            }
            out.push_str(&kept.join("::"));
            i = end;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether the `'` at `i` opens a character literal (`'x'`, `'\n'`) rather than a lifetime.
fn is_char_literal(chars: &[char], i: usize) -> bool {
    chars.get(i + 1) == Some(&'\\') || chars.get(i + 2) == Some(&'\'')
}

/// The index just past the string or character literal opening at `i`.
fn literal_end(chars: &[char], i: usize) -> usize {
    let quote = chars[i];
    let mut j = i + 1;
    while j < chars.len() && chars[j] != quote {
        j += if chars[j] == '\\' { 2 } else { 1 };
    }
    (j + 1).min(chars.len())
}

/// The path `seg::seg::…` starting at `i` (stopping before a `::<`), and the index past it.
fn read_path(chars: &[char], mut i: usize) -> (Vec<String>, usize) {
    let mut segments = vec![];
    loop {
        let start = i;
        while i < chars.len() && is_ident_char(chars[i]) {
            i += 1;
        }
        segments.push(chars[start..i].iter().collect());
        let continues = chars.get(i) == Some(&':')
            && chars.get(i + 1) == Some(&':')
            && chars
                .get(i + 2)
                .is_some_and(|&c| c.is_alphabetic() || c == '_');
        if !continues {
            return (segments, i);
        }
        i += 2;
    }
}

/// `segments` without their leading module segments: everything from the first capitalized
/// segment, else the last segment.
fn short_path(segments: &[String]) -> Vec<String> {
    let first_type = segments
        .iter()
        .position(|s| s.starts_with(|c: char| c.is_uppercase()))
        .unwrap_or(segments.len() - 1);
    segments[first_type..].to_vec()
}

/// For `::<Tag>(member)` at `i` (what follows `MemberId` in its `Debug`): the tag, the member,
/// and the index past the `)`.
fn member_id_parts(chars: &[char], i: usize) -> Option<(String, String, usize)> {
    if chars.get(i..i + 3)? != [':', ':', '<'] {
        return None;
    }
    let mut depth = 1;
    let mut j = i + 3;
    while depth > 0 {
        match chars.get(j)? {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ => {}
        }
        j += 1;
    }
    let tag: String = chars[i + 3..j - 1].iter().collect();
    if chars.get(j) != Some(&'(') {
        return None;
    }
    let close = j + chars[j..].iter().position(|&c| c == ')')?;
    let member: String = chars[j + 1..close].iter().collect();
    member
        .chars()
        .all(|c| c.is_ascii_digit())
        .then_some((tag, member, close + 1))
}

/// The node label of a member id's tag type, as [`crate::fault::NodeNames`] labels clusters.
fn member_label(tag: &str) -> String {
    let tag = tag.trim();
    if tag == "()" || tag.is_empty() {
        return "cluster".to_owned();
    }
    short_type_name(tag)
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
            Binding::Statement {
                first_in_block: true,
                ..
            } => Some("<block>".to_owned()),
            Binding::Statement {
                first_in_block: false,
                ..
            } => Some("<statement>".to_owned()),
        }
    }

    /// `site_name` on `text` written to a scratch file, at 1-based `line`, inside `function`.
    fn site(text: &str, line: u32, function: &str) -> Option<String> {
        let dir =
            std::env::temp_dir().join(format!("hydrofrac-source-names-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{function}_{line}.rs"));
        std::fs::write(&file, text).unwrap();
        site_name(&UserFrame {
            file: file.to_string_lossy().into_owned(),
            line,
            function: Some(format!("my_crate::protocols::{function}")),
        })
    }

    /// A labelling assertion that is a function's tail expression is named by its receiver; any
    /// other tail expression by the function; a statement in the middle of a block by nothing.
    #[test]
    fn unbound_statements() {
        let src = "\
pub fn two_pc<'a>(coordinator: &Process<'a, Coordinator>) -> Stream<u32> {
    let committed = coordinator.source_iter(q!([1, 2]));
    decisions_handle.complete(committed.clone());

    committed.assert_has_consistency_of::<Cluster<'a, Participant, EventualConsistency>>(
        manual_proof!(
            /** claimed, not proven: it can't hold (a `{` or `;` in here is not code) */
        ),
    )
}

fn deliveries<'a>(members: &Cluster<'a, Member>) -> Stream<u32> {
    let x = members.source_iter(q!([0]));
    x.map(q!(|v| v + 1))
        .filter(q!(|v: &u32| *v > 0))
}

fn middle<'a>(members: &Cluster<'a, Member>) {
    let x = members.source_iter(q!([0]));
    x.map(q!(|v| v + 1)).for_each(q!(|_| ()));
    let y = 1;
}

fn receiver_on_its_own_line<'a>(s: Stream<u32>) -> Stream<u32> {
    s
        .assert_has_consistency_of::<Cluster<'a, Member, EventualConsistency>>(
            manual_proof!(/** ok */),
        )
}";
        assert_eq!(site(src, 5, "two_pc").as_deref(), Some("committed"));
        assert_eq!(site(src, 14, "deliveries").as_deref(), Some("deliveries"));
        assert_eq!(site(src, 15, "deliveries").as_deref(), Some("deliveries"));
        assert_eq!(site(src, 20, "middle"), None);
        assert_eq!(
            site(src, 26, "receiver_on_its_own_line").as_deref(),
            Some("s")
        );
    }

    #[test]
    fn tails() {
        let src = lines(
            "\
fn f() -> u32 {
    a.b(q!(|x: &'a u8| { x; '}' }));
    c.d(\"};\")
        /* ; */ .e() // ;
}",
        );
        assert!(!is_tail(&src, 1));
        assert!(is_tail(&src, 2));
    }

    #[test]
    fn short_debugs() {
        assert_eq!(
            short_debug(
                "(MemberId::<hydro_std_hydro_trybuild::__staged::ec_inference_demos::abd::Client>(0), 7)"
            ),
            "(Client@0, 7)"
        );
        assert_eq!(short_debug("Some(MemberId::<()>(2))"), "Some(cluster@2)");
        assert_eq!(
            short_debug("MemberId::<a::Proposer<a::b::X>>(1)"),
            "Proposer@1"
        );
        assert_eq!(
            short_debug("Payload { kind: Kind::Decided, txn: my_crate::two_pc::Txn(1) }"),
            "Payload { kind: Kind::Decided, txn: Txn(1) }"
        );
        assert_eq!(
            short_debug("core::option::Option::Some(1)"),
            "Option::Some(1)"
        );
        assert_eq!(short_debug("a::b::c"), "c");
        // Literals are left alone; a non-numeric member id is not rewritten.
        assert_eq!(short_debug(r#"("a::b::C", 'x')"#), r#"("a::b::C", 'x')"#);
        assert_eq!(short_debug("MemberId::<T>(x)"), "MemberId::<T>(x)");
        assert_eq!(short_debug("[1, 2]"), "[1, 2]");
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
        assert_eq!(
            function_name("hydro_std::abd::abd_register_state::<u32, hydro_std::ec::Client>")
                .as_deref(),
            Some("abd_register_state")
        );
        assert_eq!(
            function_name("<hydro_lang::Stream<T> as hydro_std::Ext>::relay::{closure#0}")
                .as_deref(),
            Some("relay")
        );
        assert_eq!(short_type_name("a::b::Learner"), "Learner");
        assert_eq!(short_type_name("a::b::Learner<a::X>"), "Learner");
    }
}
