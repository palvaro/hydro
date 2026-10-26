//! Which closures are known pure (assumption A3 of docs/design/hydrofrac-soundness.md, §3).
//!
//! **An allowlist.** A closure is known pure only if it is a closure literal (after the wrappers
//! stageleft puts around a quoted expression) whose every name is known not to reach state
//! outside the call: a keyword of [`KEYWORDS`], a name that is only read (a variable, a field,
//! a type, a constant, a `stageleft` capture), a method of [`METHODS`], a macro of [`MACROS`], a
//! constructor (a capitalised name, as `Some(x)` or `Msg::Ack(x)`), or a function of a primitive
//! type or of [`STD_PATHS`]. Anything else is not known pure: a call of a function the closure
//! names (it may hide a `static`), a block that creates state before returning the closure
//! (`{ let (tx, rx) = channel(); move |x| ... }`: the state outlives each call), `unsafe`, an
//! item definition. Reading is not a call, so a `static` or a captured value can be read; to
//! change one a closure needs `unsafe` (a `static mut`) or a method, and every method it may call
//! is on the allowlist.
//!
//! A source expression (`source_iter`, a singleton source) is evaluated once per run, so it may
//! create state (`let`), but it is held to the same names.
//!
//! **What is assumed** (A3): a method, macro or `std` function on the lists is the `std` one or
//! behaves like it (a user's inherent method named `len` that bumps a counter is not seen), and
//! the `std` traits a closure uses through operators and methods (`Clone`, `PartialEq`, `Ord`,
//! `Hash`, `Add`, `Index`, `Display`, `Iterator`, ...) are implemented purely by the user's types.
//! Hydro's own quoted code (`hydro_lang`'s `__stageleft_quote_src_*` macros) is trusted, but the
//! values it captures are checked. hydrofrac's own taps are trusted.
//!
//! Not known pure costs only schedules: the operator is fragile and a schedule choice
//! (`impure_closure`).

use proc_macro2::{Delimiter, TokenStream, TokenTree};

/// Keywords a known-pure expression may use. Every other keyword (`static`, `unsafe`, `fn`,
/// `impl`, `struct`, `async`, `extern`, ...) makes it not known pure.
const KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "false", "for", "if", "in", "let",
    "loop", "match", "move", "mut", "ref", "return", "self", "Self", "super", "true", "while", "_",
];

/// Every Rust keyword, so that a name in [`KEYWORDS`] is told apart from a disallowed one.
const ALL_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "try", "type",
    "typeof", "union", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// Methods of `std` that read their receiver and arguments, or change only a value the closure
/// owns or was passed (`push` on its own accumulator), and return a function of them.
const METHODS: &[&str] = &[
    // Conversions, copies and comparisons.
    "clone",
    "cloned",
    "copied",
    "to_owned",
    "to_string",
    "to_vec",
    "into",
    "try_into",
    "from",
    "as_ref",
    "as_mut",
    "as_slice",
    "as_str",
    "as_bytes",
    "borrow",
    "deref",
    "eq",
    "ne",
    "cmp",
    "partial_cmp",
    "lt",
    "le",
    "gt",
    "ge",
    "max",
    "min",
    "clamp",
    "hash",
    "into_owned",
    // Option and Result.
    "unwrap",
    "expect",
    "unwrap_or",
    "unwrap_or_default",
    "unwrap_or_else",
    "ok",
    "err",
    "ok_or",
    "ok_or_else",
    "is_some",
    "is_none",
    "is_ok",
    "is_err",
    "is_some_and",
    "is_none_or",
    "map_or",
    "map_or_else",
    "and_then",
    "or_else",
    "or",
    "and",
    "filter",
    "flatten",
    "take",
    "replace",
    "get_or_insert",
    "get_or_insert_with",
    "insert",
    "zip",
    "xor",
    "transpose",
    "as_deref",
    "map_err",
    "unzip",
    // Collections.
    "len",
    "is_empty",
    "get",
    "get_mut",
    "contains",
    "contains_key",
    "first",
    "last",
    "iter",
    "iter_mut",
    "into_iter",
    "keys",
    "values",
    "values_mut",
    "into_keys",
    "into_values",
    "entry",
    "or_insert",
    "or_insert_with",
    "or_default",
    "and_modify",
    "push",
    "push_back",
    "push_front",
    "pop",
    "pop_back",
    "pop_front",
    "remove",
    "extend",
    "append",
    "retain",
    "clear",
    "truncate",
    "split_off",
    "sort",
    "sort_unstable",
    "sort_by",
    "sort_by_key",
    "sort_unstable_by",
    "sort_unstable_by_key",
    "dedup",
    "reverse",
    "concat",
    "join",
    "union",
    "intersection",
    "difference",
    "symmetric_difference",
    "is_subset",
    "is_superset",
    "range",
    "split_at",
    "chunks",
    "windows",
    "starts_with",
    "ends_with",
    "binary_search",
    "to_lowercase",
    "to_uppercase",
    "trim",
    "split",
    "chars",
    "bytes",
    "lines",
    "parse",
    "resize",
    "swap",
    "first_key_value",
    "last_key_value",
    "pop_first",
    "pop_last",
    // Iterators.
    "map",
    "filter_map",
    "flat_map",
    "for_each",
    "fold",
    "reduce",
    "collect",
    "sum",
    "product",
    "count",
    "any",
    "all",
    "find",
    "find_map",
    "position",
    "enumerate",
    "chain",
    "skip",
    "skip_while",
    "take_while",
    "step_by",
    "rev",
    "next",
    "nth",
    "peekable",
    "inspect",
    "max_by",
    "max_by_key",
    "min_by",
    "min_by_key",
    "partition",
    "scan",
    "last_mut",
    "zip_with",
    "cycle",
    // Numbers.
    "then",
    "then_some",
    "abs",
    "pow",
    "checked_add",
    "checked_sub",
    "checked_mul",
    "checked_div",
    "saturating_add",
    "saturating_sub",
    "saturating_mul",
    "wrapping_add",
    "wrapping_sub",
    "wrapping_mul",
    "overflowing_add",
    "overflowing_sub",
    "div_euclid",
    "rem_euclid",
    "signum",
    "sqrt",
    "floor",
    "ceil",
    "round",
    "is_positive",
    "is_negative",
    "leading_zeros",
    "trailing_zeros",
    "count_ones",
    "to_le_bytes",
    "to_be_bytes",
    "is_multiple_of",
    // Hydro's own value types: member ids, and the accessors of `hydro_std`'s quorum
    // certificates (`Durable`, `Covering`), which return a field.
    "get_raw_id",
    "from_tagless",
    "into_tagless",
    "fact",
    "into_fact",
    "aggregate",
    "into_aggregate",
];

/// Macros that compute a value from their arguments (or panic).
const MACROS: &[&str] = &[
    "vec",
    "format",
    "assert",
    "assert_eq",
    "assert_ne",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
    "panic",
    "unreachable",
    "todo",
    "unimplemented",
    "matches",
    "concat",
    "stringify",
];

/// Paths whose functions are pure: `std` collections and value types, and the pure `std`
/// modules (as `Vec::new`, `cmp::max`, `iter::once`).
const STD_PATHS: &[&str] = &[
    "Vec",
    "VecDeque",
    "String",
    "Box",
    "HashMap",
    "HashSet",
    "BTreeMap",
    "BTreeSet",
    "BinaryHeap",
    "Option",
    "Result",
    "Default",
    "From",
    "Into",
    "TryFrom",
    "TryInto",
    "Clone",
    "Ord",
    "PartialOrd",
    "PartialEq",
    "Iterator",
    "IntoIterator",
    "FromIterator",
    "ToString",
    "ToOwned",
    "iter",
    "cmp",
    "mem",
    "convert",
    "char",
    "str",
    "array",
    "option",
    "result",
    "Rc",
    "Arc",
    "MemberId",
    "ClusterId",
    "TaglessMemberId",
];

/// Primitive types: their associated functions (`u32::from`, `u64::max`) are pure.
const PRIMITIVES: &[&str] = &[
    "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize", "f32",
    "f64", "bool", "char", "str",
];

fn flat(tokens: TokenStream) -> Vec<TokenTree> {
    tokens.into_iter().collect()
}

fn is_punct(t: Option<&TokenTree>, c: char) -> bool {
    matches!(t, Some(TokenTree::Punct(p)) if p.as_char() == c)
}

fn ident(t: Option<&TokenTree>) -> Option<String> {
    match t {
        Some(TokenTree::Ident(i)) => Some(i.to_string()),
        _ => None,
    }
}

fn group(t: Option<&TokenTree>, d: Delimiter) -> Option<TokenStream> {
    match t {
        Some(TokenTree::Group(g)) if g.delimiter() == d => Some(g.stream()),
        _ => None,
    }
}

/// The index after a turbofish `::<...>` starting at `i`, if there is one.
fn skip_turbofish(t: &[TokenTree], i: usize) -> Option<usize> {
    if !(is_punct(t.get(i), ':') && is_punct(t.get(i + 1), ':') && is_punct(t.get(i + 2), '<')) {
        return None;
    }
    let mut depth = 0usize;
    for (j, tok) in t.iter().enumerate().skip(i + 2) {
        if is_punct(Some(tok), '<') {
            depth += 1;
        } else if is_punct(Some(tok), '>') && !is_punct(t.get(j.wrapping_sub(1)), '-') {
            depth -= 1;
            if depth == 0 {
                return Some(j + 1);
            }
        }
    }
    None
}

/// Drops leading `use ...;` items.
fn strip_uses(mut t: Vec<TokenTree>) -> Vec<TokenTree> {
    while ident(t.first()).as_deref() == Some("use") {
        match t.iter().position(|x| is_punct(Some(x), ';')) {
            Some(end) => {
                t.drain(..=end);
            }
            None => break,
        }
    }
    t
}

/// A quoted expression without the wrappers stageleft puts around it: what it captures (the
/// values of its free variables), whether its code is Hydro's own (trusted), and the expression.
struct Quoted {
    captures: Vec<Vec<TokenTree>>,
    trusted: bool,
    expr: Vec<TokenTree>,
}

/// A quote whose captures could not be read: not known pure.
fn out_unknown() -> Quoted {
    Quoted {
        captures: vec![],
        trusted: false,
        expr: flat(quote_unknown()),
    }
}

fn quote_unknown() -> TokenStream {
    "unsafe {}".parse().expect("tokens")
}

fn unwrap(tokens: TokenStream) -> Quoted {
    let mut out = Quoted {
        captures: vec![],
        trusted: false,
        expr: flat(tokens),
    };
    loop {
        let t = &out.expr;
        // `stageleft::runtime_support::type_hint::<T>(expr)` and its kin.
        if ident(t.first()).as_deref() == Some("stageleft")
            && ident(t.get(3)).as_deref() == Some("runtime_support")
            && let Some(name) = ident(t.get(6))
            && name.ends_with("type_hint")
        {
            let at = skip_turbofish(t, 7).unwrap_or(7);
            if at + 1 == t.len()
                && let Some(inner) = group(t.get(at), Delimiter::Parenthesis)
            {
                out.expr = flat(inner);
                continue;
            }
        }
        // A block holding only `use` items, captures (`let name__free = value;`) and one
        // expression.
        if t.len() == 1
            && let Some(inner) = group(t.first(), Delimiter::Brace)
        {
            let mut rest = strip_uses(flat(inner));
            while ident(rest.first()).as_deref() == Some("let")
                && ident(rest.get(1)).is_some_and(|n| n.ends_with("__free"))
                && is_punct(rest.get(2), '=')
                && let Some(end) = rest.iter().position(|x| is_punct(Some(x), ';'))
            {
                out.captures.push(rest[3..end].to_vec());
                rest.drain(..=end);
            }
            if !rest.iter().any(|x| is_punct(Some(x), ';')) && !rest.is_empty() {
                out.expr = rest;
                continue;
            }
        }
        // A library's quote: `krate::__stageleft_quote_src_...!([captures] [expr])`.
        if let Some(bang) = t.iter().position(|x| is_punct(Some(x), '!'))
            && bang + 2 == t.len()
            && ident(t.get(bang - 1)).is_some_and(|m| m.starts_with("__stageleft_quote_src_"))
            && let Some(args) = group(t.get(bang + 1), Delimiter::Parenthesis)
        {
            let args = flat(args);
            if let (2, Some(caps), Some(expr)) = (
                args.len(),
                group(args.first(), Delimiter::Bracket),
                group(args.get(1), Delimiter::Bracket),
            ) {
                out.trusted = ident(t.first()).as_deref() == Some("hydro_lang");
                // `name__free = value, ...`: a capture starts at each `name__free =` (a value
                // may hold commas outside any group, as in a turbofish).
                let caps = flat(caps);
                for (i, tok) in caps.iter().enumerate() {
                    let starts = ident(Some(tok)).is_some_and(|n| n.ends_with("__free"))
                        && is_punct(caps.get(i + 1), '=')
                        && (i == 0 || is_punct(caps.get(i - 1), ','));
                    if starts {
                        out.captures.push(vec![]);
                    } else if let Some(c) = out.captures.last_mut() {
                        c.push(tok.clone());
                    } else {
                        return out_unknown();
                    }
                }
                for c in &mut out.captures {
                    // Drop the `=` and the trailing `,`.
                    c.remove(0);
                    if is_punct(c.last(), ',') {
                        c.pop();
                    }
                }
                out.expr = flat(expr);
                continue;
            }
        }
        return out;
    }
}

/// Whether a closure (`closure`) or a source expression is known pure (see the
/// [module docs](self)). Text that is not a quoted expression of either shape is not.
pub(crate) fn known_pure(tokens: TokenStream, closure: bool) -> bool {
    quote_pure(tokens, closure)
}

fn quote_pure(tokens: TokenStream, closure: bool) -> bool {
    let q = unwrap(tokens);
    q.captures.iter().all(|c| capture_pure(c))
        && (q.trusted
            || if closure {
                closure_pure(&q.expr)
            } else {
                names_pure(&q.expr)
            })
}

/// A captured value: a closure the code it is captured into calls, held to the closure rule;
/// or a value it reads, which may not hold a closure (that closure could carry state the value
/// created).
fn capture_pure(c: &[TokenTree]) -> bool {
    let tokens: TokenStream = c.iter().cloned().collect();
    quote_pure(tokens.clone(), true)
        || !c.iter().any(|t| is_punct(Some(t), '|')) && quote_pure(tokens, false)
}

fn starts_closure(t: &[TokenTree]) -> bool {
    let i = usize::from(ident(t.first()).as_deref() == Some("move"));
    is_punct(t.get(i), '|')
}

/// A closure literal (`move |params| body`) whose parameters and body name nothing impure; or a
/// path to a pure function or constructor (`Some`, `u32::from`).
fn closure_pure(t: &[TokenTree]) -> bool {
    if !starts_closure(t) {
        return is_pure_path(t);
    }
    names_pure(t)
}

/// Whether `t` is a path (`a::b::c`) that [`callable`] allows.
fn is_pure_path(t: &[TokenTree]) -> bool {
    let mut segments = vec![];
    for (i, tok) in t.iter().enumerate() {
        match tok {
            TokenTree::Ident(id) if i % 3 == 0 => segments.push(id.to_string()),
            TokenTree::Punct(p) if p.as_char() == ':' && i % 3 != 0 => {}
            _ => return false,
        }
    }
    !segments.is_empty() && t.len() % 3 == 1 && callable(&segments)
}

/// Whether a function or constructor named by `path` is pure: a capitalised last segment (a
/// constructor), or a function of a primitive type or of [`STD_PATHS`].
fn callable(path: &[String]) -> bool {
    let Some(last) = path.last() else {
        return false;
    };
    if last.starts_with(|c: char| c.is_ascii_uppercase()) {
        return true;
    }
    let owner = path.iter().rev().nth(1);
    owner.is_some_and(|o| PRIMITIVES.contains(&o.as_str()) || STD_PATHS.contains(&o.as_str()))
}

/// The segments of the path whose last segment is at `i` (`a::b::<T>::f`: `[a, b, f]`), or
/// `None` for a qualified path (`<T as Trait>::f`).
fn path_to(t: &[TokenTree], i: usize) -> Option<Vec<String>> {
    let mut path = vec![ident(t.get(i))?];
    let mut j = i;
    while j >= 2 && is_punct(t.get(j - 1), ':') && is_punct(t.get(j - 2), ':') {
        let mut k = j - 2;
        // Generic arguments `::<...>` between two segments.
        if k >= 1 && is_punct(t.get(k - 1), '>') {
            let mut depth = 0usize;
            loop {
                k = k.checked_sub(1)?;
                if is_punct(t.get(k), '>') {
                    depth += 1;
                } else if is_punct(t.get(k), '<') {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
            if !(k >= 2 && is_punct(t.get(k - 1), ':') && is_punct(t.get(k - 2), ':')) {
                return None;
            }
            k -= 2;
        }
        let seg = ident(t.get(k.checked_sub(1)?))?;
        path.insert(0, seg);
        j = k - 1;
    }
    Some(path)
}

/// Whether every name in `t` (and its groups) is allowed (see the [module docs](self)).
fn names_pure(t: &[TokenTree]) -> bool {
    for (i, tok) in t.iter().enumerate() {
        match tok {
            TokenTree::Group(g) => {
                // A call through a value (`(f)(x)`, `fs[0](x)`) is not resolved here.
                let through_value = g.delimiter() == Delimiter::Parenthesis
                    && matches!(t.get(i.wrapping_sub(1)), Some(TokenTree::Group(_)));
                if through_value || !names_pure(&flat(g.stream())) {
                    return false;
                }
            }
            TokenTree::Ident(id) => {
                let name = id.to_string();
                // A lifetime (`'a`, `'static`) or a raw identifier is a name, not a keyword.
                if i > 0 && is_punct(t.get(i - 1), '\'') {
                    continue;
                }
                if ALL_KEYWORDS.contains(&name.as_str()) {
                    if !KEYWORDS.contains(&name.as_str()) {
                        return false;
                    }
                    continue;
                }
                let after = skip_turbofish(t, i + 1).unwrap_or(i + 1);
                // `name!(..)`, not `name != ..`.
                if is_punct(t.get(i + 1), '!') && matches!(t.get(i + 2), Some(TokenTree::Group(_)))
                {
                    if !MACROS.contains(&name.as_str()) {
                        return false;
                    }
                    continue;
                }
                let called = group(t.get(after), Delimiter::Parenthesis).is_some();
                if !called {
                    continue;
                }
                if i > 0 && is_punct(t.get(i - 1), '.') {
                    if !METHODS.contains(&name.as_str()) {
                        return false;
                    }
                    continue;
                }
                // A path call `a::b::f(..)`: collect the segments before `f`.
                let Some(path) = path_to(t, i) else {
                    return false;
                };
                if !callable(&path) {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}
