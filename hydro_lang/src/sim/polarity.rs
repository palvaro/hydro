//! Polarity of a specification view in the facts it observes: is the view **monotone**?
//!
//! The lineage-driven search uses a derivation of `POST` as a constraint: the next
//! hypothesis must kill at least one fact in it, on the inference that *a fault set which
//! leaves the derivation intact cannot break `POST`*. That inference is valid only if `POST` is
//! monotone in the facts it observes: a sufficient set stays sufficient when more facts
//! arrive. Positive dataflow is monotone. A view that holds by the *absence* of facts
//! (`filter_not_in`, an empty count) or that reads the *last* value of a sequence
//! (`observe_sequenced(..).max()`) may not be; `hydrofrac-negation.md` §4 has the argument
//! and the counterexamples, including a false certificate from a final-value view.
//!
//! This module decides, once at compile time, by walking the observer's dataflow from the
//! view's sink to its leaves and assigning each leaf a **polarity**: `+` if more facts at the
//! leaf can only make the view more true, `−` if only less true, both if either. Leaves are
//! **intervenable** (a network receive: facts a fault can remove) or **domain** (a `sim_input`
//! such as `correct_members`, a constant: facts no fault touches). The view is
//! [`Polarity::Monotone`] if every intervenable leaf is reached with `+` only. It is
//! [`Polarity::NonMonotone`] if an intervenable leaf is reached with `−` or with both, or if
//! an aggregate's *value* over a sequenced observation is read (a final value: a later fact
//! changes it). It is [`Polarity::Unknown`] where the walk cannot tell: a predicate it cannot
//! read on an aggregate's value (one comparing two aggregates, `delivering == correct`), a
//! fold with an arbitrary accumulator, a stateful generator (`first()`, `last()` are a
//! `scan`), an operator it has no rule for. A leaf reached only through such an operator is
//! reported as unknown, not as non-monotone: `NonMonotone` is reserved for what the walk
//! understood. Unknown is treated as not monotone by the search; the walk is deliberately not
//! clever.
//!
//! Two aspects of a collection are tracked. The **presence** of a singleton or optional (is it
//! non-empty) is what the published view is judged on, and is monotone through every positive
//! operator including aggregates (`count` is always present; `max` is present iff its input
//! is). The **value** of an aggregate matters only once a predicate reads it
//! (`filter(q!(|n| *n == 0))`), and then the direction of the comparison decides: `== 0`,
//! `<= k`, `< k` flip the polarity (an emptiness or upper-bound test is antitone in the facts),
//! `!= 0`, `>= k`, `> k` keep it, `== k` with `k > 0` needs both. A `count` is recognised by its
//! accumulator (`*n += 1`); any other fold's value is unknown; a `reduce`'s value is unknown,
//! and non-monotone when its input was sequenced (`enumerate`).
//!
//! The walk runs on the IR before the provenance rewrite, so closures are the user's.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::compile::builder::{CycleId, ExternalPortId};
use crate::compile::ir::{ClosureExpr, CollectionKind, HydroNode, HydroRoot};
use crate::location::dynamic::LocationId;

/// The verdict for one view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Polarity {
    /// Every intervenable fact is reached with positive polarity: more facts cannot make the
    /// view less true. The hitting-set inference is valid, and a 1-minimal sufficient set
    /// is a derivation.
    Monotone,
    /// Some intervenable fact is reached with negative or mixed polarity, or a final value is
    /// read. A certificate is relative to the recorded derivations persisting.
    NonMonotone {
        /// Why, one line each.
        reasons: Vec<String>,
    },
    /// The walk could not decide; treated as not monotone.
    Unknown {
        /// What it could not read, one line each.
        reasons: Vec<String>,
    },
}

impl Polarity {
    /// Whether the view is known monotone.
    pub fn is_monotone(&self) -> bool {
        matches!(self, Polarity::Monotone)
    }

    /// The reasons, if any.
    pub fn reasons(&self) -> &[String] {
        match self {
            Polarity::Monotone => &[],
            Polarity::NonMonotone { reasons } | Polarity::Unknown { reasons } => reasons,
        }
    }
}

impl std::fmt::Display for Polarity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Polarity::Monotone => write!(f, "monotone in the observed facts"),
            Polarity::NonMonotone { reasons } => {
                write!(
                    f,
                    "not monotone in the observed facts ({})",
                    reasons.join("; ")
                )
            }
            Polarity::Unknown { reasons } => {
                write!(f, "of unknown monotonicity ({})", reasons.join("; "))
            }
        }
    }
}

/// The direction in which more facts move a collection or a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Sign {
    Pos,
    Neg,
    /// Known to move it both ways (`== k`).
    Both,
    /// Not determined: an operator the walk has no rule for lies between.
    Unknown,
}

impl Sign {
    fn flip(self) -> Sign {
        match self {
            Sign::Pos => Sign::Neg,
            Sign::Neg => Sign::Pos,
            Sign::Both => Sign::Both,
            Sign::Unknown => Sign::Unknown,
        }
    }
}

/// What about a collection the parent reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Aspect {
    /// Whether it is non-empty.
    Presence,
    /// The value of a singleton or optional.
    Value,
}

struct Walker<'a> {
    /// The specification's observer: receives from other locations are intervenable facts.
    observer: &'a LocationId,
    cycles: &'a BTreeMap<CycleId, &'a HydroNode>,
    /// Intervenable leaves reached, with the signs they were reached with.
    leaves: BTreeMap<String, BTreeSet<Sign>>,
    non_monotone: Vec<String>,
    unknown: Vec<String>,
    /// `(node address, sign, aspect)` already walked.
    visited: HashSet<(*const HydroNode, Sign, Aspect)>,
    /// Whether the current path passed through `enumerate` (a sequenced observation).
    sequenced: bool,
}

/// Computes the polarity of each view published on `ports` (the `SendExternal` roots with
/// those port ids) by the specification whose observer is `observer`.
pub fn compute(
    ir: &[HydroRoot],
    observer: &LocationId,
    ports: &[ExternalPortId],
) -> BTreeMap<ExternalPortId, Polarity> {
    let cycles: BTreeMap<CycleId, &HydroNode> = ir
        .iter()
        .filter_map(|root| match root {
            HydroRoot::CycleSink {
                cycle_id, input, ..
            } => Some((*cycle_id, &**input)),
            _ => None,
        })
        .collect();
    let mut out = BTreeMap::new();
    for root in ir {
        let HydroRoot::SendExternal {
            to_port_id, input, ..
        } = root
        else {
            continue;
        };
        if !ports.contains(to_port_id) {
            continue;
        }
        let mut walker = Walker {
            observer,
            cycles: &cycles,
            leaves: BTreeMap::new(),
            non_monotone: vec![],
            unknown: vec![],
            visited: HashSet::new(),
            sequenced: false,
        };
        match published_view(input) {
            Some(view) => walker.walk(view, Sign::Pos, Aspect::Presence),
            None => walker.unknown.push(format!(
                "the view's publication chain is not the one `Spec` emits: {}",
                chain_shape(input)
            )),
        }
        out.insert(*to_port_id, walker.verdict());
    }
    out
}

/// Strips the chain `Spec::publish` emits (`view.into_singleton().into_stream().all_ticks()`:
/// `YieldConcat(Cast*(ChainFirst(Cast*(Map(Some)(view)), Cast*(SingletonSource(None)))))`)
/// and returns the view. The published value is `Some` iff the view is present, so the view's
/// presence is what the walk judges.
fn published_view(node: &HydroNode) -> Option<&HydroNode> {
    let mut node = node;
    loop {
        match node {
            HydroNode::YieldConcat { inner, .. } | HydroNode::Cast { inner, .. } => {
                node = inner;
            }
            HydroNode::ChainFirst { first, second, .. } => {
                let HydroNode::SingletonSource { .. } = strip_casts(second) else {
                    return None;
                };
                let HydroNode::Map { input, .. } = strip_casts(first) else {
                    return None;
                };
                return Some(input);
            }
            _ => return None,
        }
    }
}

fn strip_casts(node: &HydroNode) -> &HydroNode {
    let mut node = node;
    while let HydroNode::Cast { inner, .. } = node {
        node = inner;
    }
    node
}

impl Walker<'_> {
    fn verdict(self) -> Polarity {
        let mut non_monotone = self.non_monotone;
        let mut unknown = self.unknown;
        for (leaf, signs) in &self.leaves {
            if signs.contains(&Sign::Both) {
                non_monotone.push(format!("{leaf} is reached with both polarities"));
            } else if signs.contains(&Sign::Neg) {
                non_monotone.push(format!(
                    "{leaf} is reached with negative polarity (more facts make the view less true)"
                ));
            } else if signs.contains(&Sign::Unknown) {
                unknown.push(format!("{leaf} is reached with undetermined polarity"));
            }
        }
        if !non_monotone.is_empty() {
            Polarity::NonMonotone {
                reasons: non_monotone,
            }
        } else if !unknown.is_empty() {
            Polarity::Unknown { reasons: unknown }
        } else {
            Polarity::Monotone
        }
    }

    fn walk(&mut self, node: &HydroNode, sign: Sign, aspect: Aspect) {
        if !self
            .visited
            .insert((std::ptr::from_ref(node), sign, aspect))
        {
            return;
        }
        let name = node_name(node);
        match node {
            HydroNode::Placeholder => unreachable!(),

            // Leaves.
            HydroNode::Network {
                name,
                input,
                metadata,
                ..
            } => {
                // A receive at the observer from another location: an intervenable fact.
                // (A send *out of* the observer is not a specification pattern.)
                if metadata.location_id.root() == self.observer {
                    let label = format!(
                        "{} from {:?}",
                        name.as_deref().unwrap_or("observation"),
                        input.metadata().location_id.root()
                    );
                    self.leaves.entry(label).or_default().insert(sign);
                } else {
                    self.walk(input, sign, aspect);
                }
            }
            HydroNode::ExternalInput { .. }
            | HydroNode::Source { .. }
            | HydroNode::SingletonSource { .. } => {
                // Domain: no fault touches these facts.
            }
            HydroNode::CycleSource { cycle_id, .. } => match self.cycles.get(cycle_id) {
                Some(input) => self.walk(input, sign, aspect),
                None => self.unknown.push(format!("cycle {cycle_id:?} has no sink")),
            },

            // Pass-through: one input, the output's presence and value follow it.
            HydroNode::Cast { inner, .. }
            | HydroNode::ObserveNonDet { inner, .. }
            | HydroNode::BeginAtomic { inner, .. }
            | HydroNode::EndAtomic { inner, .. }
            | HydroNode::Batch { inner, .. }
            | HydroNode::YieldConcat { inner, .. } => self.walk(inner, sign, aspect),
            HydroNode::Tee { inner, .. } | HydroNode::PartitionSide { inner, .. } => {
                let inner = inner.0.borrow();
                self.walk(&inner, sign, aspect);
            }
            HydroNode::DeferTick { input, .. }
            | HydroNode::Sort { input, .. }
            | HydroNode::Unique { input, .. }
            | HydroNode::Inspect { input, .. }
            | HydroNode::Counter { input, .. }
            | HydroNode::AssertIsConsistent { inner: input, .. }
            | HydroNode::UnboundSingleton { inner: input, .. } => self.walk(input, sign, aspect),

            // A sequenced observation: positions are facts like any other for presence, but
            // an aggregate's value over them is a final value.
            HydroNode::Enumerate { input, .. } => {
                let was = self.sequenced;
                self.sequenced = true;
                self.walk(input, sign, aspect);
                self.sequenced = was;
            }

            // Selection and projection on a collection of facts: monotone. On an aggregate's
            // value, a projection keeps presence; a selection reads the value.
            HydroNode::Map { input, .. } => self.walk(input, sign, aspect),
            HydroNode::FlatMap { input, .. } => match aspect {
                Aspect::Presence => self.walk(input, sign, aspect),
                Aspect::Value => {
                    self.unknown
                        .push(format!("{name}: the value of a flat_map on an aggregate"));
                }
            },
            HydroNode::Filter { f, input, .. } | HydroNode::FilterMap { f, input, .. } => {
                if is_aggregate_valued(input) {
                    // A predicate on an aggregate's value: read the comparison.
                    if let Some(direction) = comparison_sign(f) {
                        let sign = match direction {
                            Sign::Pos => sign,
                            Sign::Neg => sign.flip(),
                            Sign::Both => Sign::Both,
                            Sign::Unknown => Sign::Unknown,
                        };
                        self.walk(input, sign, Aspect::Value);
                    } else {
                        self.unknown.push(format!(
                            "{name} on an aggregate's value with a predicate the walk cannot read: `{}`",
                            closure_text(f)
                        ));
                        // Still visit the input so leaves are named in the report.
                        self.walk(input, Sign::Unknown, Aspect::Value);
                    }
                } else {
                    self.walk(input, sign, aspect);
                }
            }

            // Combinations: every input contributes positively to presence.
            HydroNode::Chain { first, second, .. }
            | HydroNode::ChainFirst { first, second, .. }
            | HydroNode::MergeOrdered { first, second, .. } => {
                self.walk(first, sign, aspect);
                self.walk(second, sign, aspect);
            }
            HydroNode::CrossProduct { left, right, .. }
            | HydroNode::Join { left, right, .. }
            | HydroNode::JoinHalf { left, right, .. } => {
                self.walk(left, sign, aspect);
                self.walk(right, sign, aspect);
            }
            HydroNode::CrossSingleton { left, right, .. } => match aspect {
                Aspect::Presence => {
                    self.walk(left, sign, aspect);
                    self.walk(right, sign, aspect);
                }
                Aspect::Value => {
                    // A predicate compared the two sides (`d == n`): their relation is not
                    // syntactic.
                    self.unknown.push(format!(
                        "{name}: a predicate reads a pair of values (two aggregates compared)"
                    ));
                    self.walk(left, Sign::Unknown, Aspect::Value);
                    self.walk(right, Sign::Unknown, Aspect::Value);
                }
            },

            // Negation: the subtracted side is reached with the opposite sign.
            HydroNode::Difference { pos, neg, .. } | HydroNode::AntiJoin { pos, neg, .. } => {
                self.walk(pos, sign, aspect);
                self.walk(neg, sign.flip(), aspect);
            }

            // Aggregates. Presence: a fold is always present, a reduce iff its input is.
            // Value: a count grows with the facts; anything else is unknown; over a sequence,
            // a reduce's value is a final value.
            HydroNode::Fold { acc, input, .. } | HydroNode::FoldKeyed { acc, input, .. } => {
                match aspect {
                    Aspect::Presence => self.walk(input, sign, Aspect::Presence),
                    Aspect::Value => {
                        if is_count(acc) {
                            self.walk(input, sign, Aspect::Presence);
                        } else {
                            self.unknown.push(format!(
                                "{name}: the value of a fold with accumulator `{}`",
                                closure_text(acc)
                            ));
                            self.walk(input, Sign::Unknown, Aspect::Presence);
                        }
                    }
                }
            }
            HydroNode::Reduce { input, .. }
            | HydroNode::ReduceKeyed { input, .. }
            | HydroNode::ReduceKeyedWatermark { input, .. } => match aspect {
                Aspect::Presence => self.walk(input, sign, Aspect::Presence),
                Aspect::Value => {
                    if self.sequenced || contains_enumerate(input) {
                        self.non_monotone.push(format!(
                            "{name} over a sequenced observation: a final value, which a later fact changes"
                        ));
                    } else {
                        self.unknown
                            .push(format!("{name}: the value of a reduce (which element)"));
                    }
                    self.walk(input, Sign::Unknown, Aspect::Presence);
                }
            },
            HydroNode::Scan { input, .. } | HydroNode::ScanAsyncBlocking { input, .. } => {
                self.unknown.push(format!(
                    "{name}: no polarity rule (a stateful generator such as `first()`)"
                ));
                self.walk(input, Sign::Unknown, aspect);
            }

            // No rule.
            HydroNode::Reference { .. }
            | HydroNode::PartitionShared { .. }
            | HydroNode::ResolveFutures { .. }
            | HydroNode::ResolveFuturesBlocking { .. }
            | HydroNode::ResolveFuturesOrdered { .. }
            | HydroNode::FlatMapStreamBlocking { .. }
            | HydroNode::VersionedNetwork { .. }
            | HydroNode::VersionedNetworkFork { .. } => {
                self.unknown.push(format!("{name}: no polarity rule"));
                for input in node.input() {
                    self.walk(input, Sign::Unknown, aspect);
                }
            }
        }
    }
}

/// Whether a node is a singleton or optional (an aggregate's value, or a constant).
fn is_aggregate_valued(node: &HydroNode) -> bool {
    matches!(
        node.metadata().collection_kind,
        CollectionKind::Singleton { .. } | CollectionKind::Optional { .. }
    )
}

/// Whether `enumerate` appears upstream of `node` (through single-input operators).
fn contains_enumerate(node: &HydroNode) -> bool {
    match node {
        HydroNode::Enumerate { .. } => true,
        HydroNode::Tee { inner, .. } => contains_enumerate(&inner.0.borrow()),
        HydroNode::Network { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Source { .. }
        | HydroNode::SingletonSource { .. }
        | HydroNode::CycleSource { .. } => false,
        _ => node.input().into_iter().any(contains_enumerate),
    }
}

/// The variant names down a single-input chain, for diagnostics.
fn chain_shape(node: &HydroNode) -> String {
    let mut names = vec![node_name(node)];
    let mut node = node;
    while let [input] = node.input().as_slice() {
        node = input;
        names.push(node_name(node));
    }
    names.join(" < ")
}

/// The variant name of a node.
fn node_name(node: &HydroNode) -> String {
    let name = node.print_root();
    name.split('(').next().unwrap_or(&name).to_owned()
}

/// The user's closure inside a staged expression. Staging renders a closure as
/// `fn1_type_hint::<..>({ use ..; __stageleft_quote_..!([captures] [closure]) })`; the
/// closure is the last bracketed group of the macro's input.
fn staged_closure(f: &ClosureExpr) -> Option<syn::ExprClosure> {
    /// Descends through the staging wrappers (`fn1_type_hint::<..>({ use ..; use ..; <expr> })`,
    /// blocks, parentheses) to the closure, or to the `q!` macro whose last bracket group is
    /// the closure's tokens.
    fn find(expr: &syn::Expr) -> Option<syn::ExprClosure> {
        match expr {
            syn::Expr::Closure(closure) => Some(closure.clone()),
            syn::Expr::Macro(m) => {
                let groups: Vec<proc_macro2::TokenStream> = m
                    .mac
                    .tokens
                    .clone()
                    .into_iter()
                    .filter_map(|tree| match tree {
                        proc_macro2::TokenTree::Group(group)
                            if group.delimiter() == proc_macro2::Delimiter::Bracket =>
                        {
                            Some(group.stream())
                        }
                        _ => None,
                    })
                    .collect();
                syn::parse2::<syn::ExprClosure>(groups.last()?.clone()).ok()
            }
            syn::Expr::Call(call) => call.args.iter().find_map(find),
            syn::Expr::Block(block) => block.block.stmts.iter().rev().find_map(|stmt| match stmt {
                syn::Stmt::Expr(expr, _) => find(expr),
                _ => None,
            }),
            syn::Expr::Paren(paren) => find(&paren.expr),
            _ => None,
        }
    }
    find(&f.expr)
}

/// A readable rendering of a closure for reports.
fn closure_text(f: &ClosureExpr) -> String {
    match staged_closure(f) {
        Some(closure) => quote::quote!(#closure).to_string(),
        None => format!("<unreadable: {}>", f.expr),
    }
}

/// Whether a fold's accumulator is a count: `|n, _| *n += 1`.
fn is_count(acc: &ClosureExpr) -> bool {
    let Some(closure) = staged_closure(acc) else {
        return false;
    };
    let body = strip_block(&closure.body);
    if let syn::Expr::Binary(syn::ExprBinary {
        op: syn::BinOp::AddAssign(_),
        right,
        ..
    }) = body
        && is_int_literal(right, Some(1))
    {
        return true;
    }
    false
}

/// The direction of a predicate on an aggregate's value: `Pos` if more facts in the
/// aggregate's input can only make it true (`!= 0`, `>= k`, `> k`), `Neg` if only false
/// (`== 0`, `<= k`, `< k`), `Both` for `== k` with `k > 0`. `None` if the predicate is not a
/// comparison of the parameter with an integer literal.
fn comparison_sign(f: &ClosureExpr) -> Option<Sign> {
    let closure = staged_closure(f)?;
    if closure.inputs.len() != 1 {
        return None;
    }
    let param = single_ident(closure.inputs.first()?)?;
    let body = strip_block(&closure.body);
    let syn::Expr::Binary(syn::ExprBinary {
        left, op, right, ..
    }) = body
    else {
        return None;
    };
    // Normalise to `param <op> literal`.
    let (op, literal) = if refers_to(left, &param) && literal_value(right).is_some() {
        (*op, literal_value(right)?)
    } else if refers_to(right, &param) && literal_value(left).is_some() {
        (mirror(op)?, literal_value(left)?)
    } else {
        return None;
    };
    Some(match (op, literal) {
        (syn::BinOp::Eq(_), 0) => Sign::Neg,
        (syn::BinOp::Eq(_), _) => Sign::Both,
        (syn::BinOp::Ne(_), 0) => Sign::Pos,
        (syn::BinOp::Ne(_), _) => Sign::Both,
        (syn::BinOp::Ge(_) | syn::BinOp::Gt(_), _) => Sign::Pos,
        (syn::BinOp::Le(_) | syn::BinOp::Lt(_), _) => Sign::Neg,
        _ => return None,
    })
}

/// `k <op> x` as `x <op'> k`.
fn mirror(op: &syn::BinOp) -> Option<syn::BinOp> {
    Some(match op {
        syn::BinOp::Eq(t) => syn::BinOp::Eq(*t),
        syn::BinOp::Ne(t) => syn::BinOp::Ne(*t),
        syn::BinOp::Lt(_) => syn::BinOp::Gt(Default::default()),
        syn::BinOp::Le(_) => syn::BinOp::Ge(Default::default()),
        syn::BinOp::Gt(_) => syn::BinOp::Lt(Default::default()),
        syn::BinOp::Ge(_) => syn::BinOp::Le(Default::default()),
        _ => return None,
    })
}

fn strip_block(expr: &syn::Expr) -> &syn::Expr {
    let mut expr = expr;
    loop {
        match expr {
            syn::Expr::Block(block) if block.block.stmts.len() == 1 => {
                match &block.block.stmts[0] {
                    syn::Stmt::Expr(inner, _) => expr = inner,
                    _ => return expr,
                }
            }
            syn::Expr::Paren(paren) => expr = &paren.expr,
            _ => return expr,
        }
    }
}

/// The identifier a single closure parameter binds (`n`, `n: T`, `&n`, `(n)`).
fn single_ident(pat: &syn::Pat) -> Option<syn::Ident> {
    match pat {
        syn::Pat::Ident(ident) => Some(ident.ident.clone()),
        syn::Pat::Type(typed) => single_ident(&typed.pat),
        syn::Pat::Reference(reference) => single_ident(&reference.pat),
        syn::Pat::Paren(paren) => single_ident(&paren.pat),
        _ => None,
    }
}

/// Whether `expr` is the parameter (`n`, `*n`, `&n`, `(n)`).
fn refers_to(expr: &syn::Expr, param: &syn::Ident) -> bool {
    match expr {
        syn::Expr::Path(path) => path.path.is_ident(param),
        syn::Expr::Unary(unary) => refers_to(&unary.expr, param),
        syn::Expr::Reference(reference) => refers_to(&reference.expr, param),
        syn::Expr::Paren(paren) => refers_to(&paren.expr, param),
        _ => false,
    }
}

fn literal_value(expr: &syn::Expr) -> Option<u64> {
    match expr {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(int),
            ..
        }) => int.base10_parse::<u64>().ok(),
        syn::Expr::Reference(reference) => literal_value(&reference.expr),
        syn::Expr::Paren(paren) => literal_value(&paren.expr),
        _ => None,
    }
}

fn is_int_literal(expr: &syn::Expr, value: Option<u64>) -> bool {
    match literal_value(expr) {
        Some(v) => value.is_none_or(|want| v == want),
        None => false,
    }
}
