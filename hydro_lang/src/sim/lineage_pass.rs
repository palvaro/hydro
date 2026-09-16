//! The lineage pass (E3.2 of the amplification design,
//! `design_docs/2026-09_amplification_as_adversarial_scheduling.md`): an IR rewrite, run right
//! before simulator codegen, that makes every record carry an id and every operator report how
//! it derived its outputs.
//!
//! Representation. A `Stream<T>` (and `Singleton<T>`, `Optional<T>`) element becomes `(u64, T)`.
//! A `KeyedStream<K, V>` / `KeyedSingleton<K, V>` element `(K, V)` becomes `(K, (u64, V))`, so
//! the keyed operators (`join`, `anti_join`, `fold_keyed`) still key on `K`. A `Cast` between
//! the two representations re-nests without a new id.
//!
//! Rules (the design's table). Sources, `sim_input`s and timers are leaves (a fresh id, no
//! parents). `map`, `flat_map`, `filter_map`, `enumerate`: child ← input. `filter`, `inspect`,
//! `batch`, `chain`, `defer_tick` (state carry), the network: the record passes through under
//! its own id (the network carries the id in front of the payload, so the receiving location
//! continues the same lineage). `join`, `cross`: child ← both inputs. `anti_join`, `difference`:
//! child ← the surviving positive record (a derivation of its own, since it answers the absence
//! of a record: E3.3 attaches the tick's held deliveries to it as negative support). `fold`,
//! `reduce`, `scan` (and `first()`, which is a scan): child ← every input seen this tick, the
//! accumulator carrying the parent ids alongside the user's state. `unique`: the survivor keeps
//! its id, every duplicate is reported dropped with the survivor named. `sort` orders by the
//! payload alone. `sim_output` reports the id of every record it emits.
//!
//! Every operator's closure is wrapped, not rewritten: `{ let __f = <the staged closure>;
//! move |__w| { unwrap; call __f; derive; wrap } }`, so the staged code and its type hints are
//! untouched. The wrapped closures call [`super::lineage_rt`] in the compiled program, which
//! forwards each event to the host. Operators are numbered in traversal order; the table of
//! `(id, kind, source location, element type)` is returned to the host, which stores it in the
//! run's [`super::lineage::Lineage`]. Anything the pass does not know how to wrap panics at
//! compile time with the operator's location, so an unsupported program fails loudly.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

use std::cell::RefCell;
use std::rc::Rc;

use crate::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, HydroIrMetadata, HydroNode, HydroRoot, HydroSource,
    NetworkRecv, NetworkSend, SharedNode, transform_bottom_up,
};
use crate::staging_util::get_this_crate;

/// One row of the operator table the pass produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorInfo {
    /// The operator's number, as the derivation records refer to it.
    pub id: u32,
    /// The IR node kind (`Map`, `Fold`, `AntiJoin`, ...; roots as `root ForEach` etc.).
    pub kind: String,
    /// `file:line:col` of the operator in the program (the `sliced!` block for batches).
    pub location: String,
    /// The source line at that location.
    pub source_line: String,
    /// The element type of the operator's output before wrapping (for roots, of the input).
    pub element_type: String,
    /// Whether the operator's outputs can answer the *absence* of a record (E3.3): an anti-join
    /// (`filter_key_not_in`, `difference`), or a closure that reads state or this tick's
    /// aggregates through `by_ref`/`by_mut` handles (an opaque step like Raft's, whose absence
    /// tests are inside). The records a hook of the same tick holds while such an operator
    /// derives are the derivation's negative support.
    pub negative: bool,
}

/// How a collection's elements are laid out after the pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rep {
    /// `(u64, T)`.
    Stream,
    /// `(K, (u64, V))`.
    Keyed,
}

fn rep_of(kind: &CollectionKind) -> Rep {
    match kind {
        CollectionKind::Stream { .. }
        | CollectionKind::Singleton { .. }
        | CollectionKind::Optional { .. } => Rep::Stream,
        CollectionKind::KeyedStream { .. } | CollectionKind::KeyedSingleton { .. } => Rep::Keyed,
    }
}

/// The unwrapped element type of a collection, as text (for the operator table).
fn element_type_text(kind: &CollectionKind) -> String {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => quote!(#element_type).to_string(),
        CollectionKind::KeyedStream {
            key_type,
            value_type,
            ..
        }
        | CollectionKind::KeyedSingleton {
            key_type,
            value_type,
            ..
        } => quote!((#key_type, #value_type)).to_string(),
    }
}

/// Rewrites a collection kind to the wrapped representation.
fn wrap_kind(kind: &mut CollectionKind) {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => {
            let t = &**element_type;
            *element_type = ty(quote!((u64, #t))).into();
        }
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => {
            let t = &**value_type;
            *value_type = ty(quote!((u64, #t))).into();
        }
    }
}

/// A collection kind with the given element type, keeping the rest of `like`.
fn kind_with_element(like: &CollectionKind, element: syn::Type) -> CollectionKind {
    let mut kind = like.clone();
    match &mut kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => *element_type = element.into(),
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => *value_type = element.into(),
    }
    kind
}

/// `let (__id, __x) = ...;` for a by-value wrapped element `__w`, by representation. `__x` is
/// the element as the staged closure expects it.
fn unwrap_value(rep: Rep) -> TokenStream {
    match rep {
        Rep::Stream => quote!(let (__id, __x) = __w;),
        Rep::Keyed => quote!(let (__k, (__id, __v)) = __w; let __x = (__k, __v);),
    }
}

/// Like [`unwrap_value`] for a by-reference element `__w: &_`; `__x` is a reference. The keyed
/// layout has no `&(K, V)` to hand out, so it clones.
fn unwrap_ref(rep: Rep) -> TokenStream {
    match rep {
        Rep::Stream => quote!(let (__id, __x) = __w; let __id: u64 = *__id;),
        Rep::Keyed => quote!(
            let __id: u64 = (__w.1).0;
            let __t = (::std::clone::Clone::clone(&__w.0), ::std::clone::Clone::clone(&(__w.1).1));
            let __x = &__t;
        ),
    }
}

/// Wraps a produced element `__y` under record id `id`, by the output representation.
fn wrap_value(rep: Rep, id: TokenStream) -> TokenStream {
    match rep {
        Rep::Stream => quote!((#id, __y)),
        Rep::Keyed => quote!({ let (__k, __v) = __y; (__k, (#id, __v)) }),
    }
}

fn ty(tokens: TokenStream) -> syn::Type {
    syn::parse2(tokens).expect("lineage pass: generated type")
}

fn debug_expr(tokens: TokenStream) -> DebugExpr {
    DebugExpr::from(syn::Expr::Block(
        syn::parse2::<syn::ExprBlock>(tokens).expect("lineage pass: generated closure"),
    ))
}

fn closure(expr: TokenStream) -> ClosureExpr {
    ClosureExpr::from(debug_expr(expr))
}

/// Wraps a staged closure `f` that captures no singleton references.
fn rewrap(f: &ClosureExpr, body: impl FnOnce(TokenStream) -> TokenStream) -> ClosureExpr {
    assert!(
        f.singleton_refs.is_empty(),
        "lineage pass: a fold/scan/reduce closure with singleton references is not supported"
    );
    let inner = &f.expr;
    let expr = body(quote!(#inner));
    let mut out = f.clone();
    out.expr = debug_expr(expr);
    out
}

/// What [`Pass::rewrap_refs`] hands an operator arm to splice into the wrapped closure's body,
/// after the arm has bound `__id` and `__x`: `pre` binds `__parents` (the input's id and the id
/// of every referenced singleton), `call` applies the staged closure to `__x`, `post` gives
/// every mutably referenced singleton a new version derived from `__parents`.
struct Splice {
    pre: TokenStream,
    call: TokenStream,
    post: TokenStream,
}

struct Pass {
    root: TokenStream,
    operators: Vec<OperatorInfo>,
    /// Roots added by the pass (observers of records a join drops), appended after the traversal.
    new_roots: Vec<HydroRoot>,
}

impl Pass {
    fn rt(&self) -> TokenStream {
        let root = &self.root;
        quote!(#root::sim::lineage_rt)
    }

    fn register(&mut self, kind: &str, metadata: &HydroIrMetadata) -> u32 {
        let id = self.operators.len() as u32;
        let (location, source_line, _) = super::builder::location_for_op(&metadata.op);
        self.operators.push(OperatorInfo {
            id,
            kind: kind.to_owned(),
            location,
            source_line: source_line.trim().to_owned(),
            element_type: element_type_text(&metadata.collection_kind),
            negative: matches!(kind, "AntiJoin" | "Difference"),
        });
        id
    }

    fn unsupported(&self, what: &str, metadata: &HydroIrMetadata) -> ! {
        let (location, line, _) = super::builder::location_for_op(&metadata.op);
        panic!("lineage pass: unsupported operator {what} at {location}: {}", line.trim())
    }
}

/// Runs the pass over `ir` and returns the operator table.
pub fn apply_lineage(ir: &mut Vec<HydroRoot>) -> Vec<OperatorInfo> {
    let pass = RefCell::new(Pass {
        root: get_this_crate(),
        operators: vec![],
        new_roots: vec![],
    });
    transform_bottom_up(
        ir,
        &mut |root| pass.borrow_mut().rewrite_root(root),
        &mut |node| pass.borrow_mut().rewrite_node(node),
        false,
    );
    let pass = pass.into_inner();
    // The drop observers added for joins (already in the wrapped representation).
    ir.extend(pass.new_roots);
    pass.operators
}

impl Pass {
    /// A `Map` node over `input` with the given closure and output kind.
    fn map_node(
        &self,
        f: TokenStream,
        input: HydroNode,
        kind: CollectionKind,
        like: &HydroIrMetadata,
    ) -> HydroNode {
        let mut metadata = like.clone();
        metadata.collection_kind = kind;
        HydroNode::Map {
            f: closure(f),
            input: Box::new(input),
            metadata,
        }
    }

    /// Replaces a leaf node by itself followed by a `Map` that assigns fresh ids.
    fn wrap_leaf(&mut self, node: &mut HydroNode, kind: &str) {
        let op = self.register(kind, node.metadata());
        let rt = self.rt();
        let old = std::mem::replace(node, HydroNode::Placeholder);
        let mut wrapped = old.metadata().collection_kind.clone();
        wrap_kind(&mut wrapped);
        let out_rep = rep_of(&wrapped);
        let wrap = wrap_value(out_rep, quote!(#rt::derive(#op, &[])));
        let metadata = old.metadata().clone();
        *node = self.map_node(
            quote!({ move |__y| #wrap }),
            old,
            wrapped,
            &metadata,
        );
    }

    /// Wraps a staged closure that may capture singleton references (`by_ref` / `by_mut`
    /// handles, bound by codegen as `__hydro_singleton_ref_i`: `&(u64, T)` / `&mut (u64, T)` for
    /// singletons, `&Vec<(u64, T)>` / `&mut Vec<(u64, T)>` for streams). The staged closure
    /// expects the unwrapped types, so it is constructed per input inside the wrapper with the
    /// references shadowed: a singleton by a reference into its payload, a mutable stream by a
    /// [`super::lineage_rt::LineageVec`] that gives every pushed record an id, a shared stream by
    /// a clone of its payloads. A mutably referenced singleton is a state the closure may have
    /// changed, so it gets a new id after the call: state version <- previous version + the
    /// closure's inputs (the design's state-carry rule).
    fn rewrap_refs(&mut self, f: &ClosureExpr, op: u32, body: impl FnOnce(&Splice) -> TokenStream) -> ClosureExpr {
        let rt = self.rt();
        let inner = &f.expr;
        let mut out = f.clone();
        if f.singleton_refs.is_empty() {
            let splice = Splice {
                pre: quote!(let __parents: [u64; 1] = [__id];),
                call: quote!(__f(__x)),
                post: quote!(),
            };
            let body = body(&splice);
            out.expr = debug_expr(quote!({ let mut __f = #inner; move |__w| #body }));
            return out;
        }
        // A closure reading state or aggregates through handles may test absence inside (E3.3).
        self.operators[op as usize].negative = true;
        let mut parent_ids = vec![];
        let mut shadows = vec![];
        let mut bumps = vec![];
        for (i, (node, is_mut)) in f.singleton_refs.iter().enumerate() {
            let HydroNode::Reference { kind, .. } = node else {
                panic!("lineage pass: singleton reference is not a Reference node");
            };
            let ident = crate::handoff_ref::handoff_ref_ident(i);
            match (kind, is_mut) {
                (crate::handoff_ref::HandoffRefKind::Singleton, false) => {
                    parent_ids.push(quote!(__p.push((*#ident).0);));
                    shadows.push(quote!(let #ident = &(*#ident).1;));
                }
                (crate::handoff_ref::HandoffRefKind::Singleton, true) => {
                    parent_ids.push(quote!(__p.push((*#ident).0);));
                    shadows.push(quote!(let #ident = &mut (*#ident).1;));
                    bumps.push(quote!((*#ident).0 = #rt::state_version(#op, &__parents);));
                }
                (crate::handoff_ref::HandoffRefKind::Vec, false) => {
                    parent_ids.push(quote!(__p.extend(#ident.iter().map(|(__i, _)| *__i));));
                    shadows.push(quote!(
                        let #ident = #ident.iter().map(|(_, __t)| ::std::clone::Clone::clone(__t)).collect::<::std::vec::Vec<_>>();
                        let #ident = &#ident;
                    ));
                }
                (crate::handoff_ref::HandoffRefKind::Vec, true) => {
                    parent_ids.push(quote!(__p.extend(#ident.iter().map(|(__i, _)| *__i));));
                    let holder = quote::format_ident!("__lineage_vec_{}", i);
                    shadows.push(quote!(
                        let mut #holder = #rt::LineageVec::new(&mut *#ident, #op, &__parents);
                        let #ident = &mut #holder;
                    ));
                }
                (crate::handoff_ref::HandoffRefKind::Optional, false) => {
                    parent_ids.push(quote!(__p.extend(#ident.iter().map(|(__i, _)| *__i));));
                    shadows.push(quote!(
                        let #ident = #ident.as_ref().map(|(_, __t)| ::std::clone::Clone::clone(__t));
                        let #ident = &#ident;
                    ));
                }
                (crate::handoff_ref::HandoffRefKind::Optional, true) => {
                    panic!("lineage pass: a mutable optional reference is not supported")
                }
            }
        }
        let splice = Splice {
            pre: quote!(let __parents: ::std::vec::Vec<u64> = { let mut __p = vec![__id]; #(#parent_ids)* __p };),
            call: quote!({ #(#shadows)* let mut __f = #inner; __f(__x) }),
            post: quote!(#(#bumps)*),
        };
        let body = body(&splice);
        out.expr = debug_expr(quote!({ move |__w| #body }));
        out
    }

    fn wrap_predicate(&mut self, kind: &str, f: &mut ClosureExpr, input: &HydroNode, metadata: &mut HydroIrMetadata) {
        let op = self.register(kind, metadata);
        let in_rep = rep_of(&input.metadata().collection_kind);
        wrap_kind(&mut metadata.collection_kind);
        let unwrap = unwrap_ref(in_rep);
        *f = self.rewrap_refs(f, op, |Splice { pre, call, post }| quote!({
            #unwrap #pre let __r = #call; #post __r
        }));
    }

    fn rewrite_node(&mut self, node: &mut HydroNode) {
        let rt = self.rt();
        match node {
            HydroNode::Placeholder => panic!("lineage pass: placeholder"),

            // Leaves: a fresh id per record, no parents.
            HydroNode::Source { .. } => {
                if let HydroNode::Source { source: HydroSource::ExternalNetwork() | HydroSource::Spin(), metadata } = &*node {
                    let m = metadata.clone();
                    self.unsupported("Source(network/spin)", &m);
                }
                self.wrap_leaf(node, "Source");
            }
            HydroNode::SingletonSource { .. } => self.wrap_leaf(node, "SingletonSource"),
            HydroNode::ExternalInput {
                deserialize_fn,
                metadata,
                ..
            } => {
                let op = self.register("ExternalInput", metadata);
                let Some(f) = deserialize_fn else {
                    let m = metadata.clone();
                    self.unsupported("ExternalInput without deserializer", &m);
                };
                let inner = &*f;
                let wrap = wrap_value(rep_of(&metadata.collection_kind), quote!(#rt::derive(#op, &[])));
                // The staged deserializer is an unannotated `|res| ..`; `apply` lets the argument
                // fix its parameter type (rustc checks closure arguments after the others).
                *f = debug_expr(quote!({
                    move |__res| { let __y = #rt::apply(#inner, __res); #wrap }
                }));
                wrap_kind(&mut metadata.collection_kind);
            }

            // Pass-through: the record keeps its id; only the type changes.
            HydroNode::ObserveNonDet { .. }
            | HydroNode::AssertIsConsistent { .. }
            | HydroNode::UnboundSingleton { .. }
            | HydroNode::CycleSource { .. }
            | HydroNode::Tee { .. }
            | HydroNode::Reference { .. }
            | HydroNode::PartitionSide { .. }
            | HydroNode::BeginAtomic { .. }
            | HydroNode::EndAtomic { .. }
            | HydroNode::Batch { .. }
            | HydroNode::YieldConcat { .. }
            | HydroNode::Chain { .. }
            | HydroNode::MergeOrdered { .. }
            | HydroNode::ChainFirst { .. }
            | HydroNode::DeferTick { .. }
            | HydroNode::ResolveFutures { .. }
            | HydroNode::ResolveFuturesBlocking { .. }
            | HydroNode::ResolveFuturesOrdered { .. }
            | HydroNode::Counter { .. } => {
                let kind = node_kind_name(node);
                let metadata = node.metadata_mut();
                self.register(kind, metadata);
                wrap_kind(&mut metadata.collection_kind);
            }

            // A cast between representations re-nests; otherwise pass-through.
            HydroNode::Cast { inner, metadata } => {
                self.register("Cast", metadata);
                let in_rep = rep_of(&inner.metadata().collection_kind);
                wrap_kind(&mut metadata.collection_kind);
                let out_rep = rep_of(&metadata.collection_kind);
                if in_rep != out_rep {
                    let f = match (in_rep, out_rep) {
                        (Rep::Stream, Rep::Keyed) => quote!({ move |(__id, (__k, __v))| (__k, (__id, __v)) }),
                        (Rep::Keyed, Rep::Stream) => quote!({ move |(__k, (__id, __v))| (__id, (__k, __v)) }),
                        _ => unreachable!(),
                    };
                    let old_inner = std::mem::replace(&mut **inner, HydroNode::Placeholder);
                    let kind = metadata.collection_kind.clone();
                    **inner = self.map_node(f, old_inner, kind, metadata);
                }
            }

            // child <- input.
            HydroNode::Map { f, input, metadata } => {
                let op = self.register("Map", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                wrap_kind(&mut metadata.collection_kind);
                let (unwrap, wrap) = (unwrap_value(in_rep), wrap_value(rep_of(&metadata.collection_kind), quote!(#rt::derive(#op, &__parents))));
                *f = self.rewrap_refs(f, op, |Splice { pre, call, post }| quote!({
                    #unwrap #pre let __y = #call; #post #wrap
                }));
            }
            HydroNode::FilterMap { f, input, metadata } => {
                let op = self.register("FilterMap", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                wrap_kind(&mut metadata.collection_kind);
                let (unwrap, wrap) = (unwrap_value(in_rep), wrap_value(rep_of(&metadata.collection_kind), quote!(#rt::derive(#op, &__parents))));
                *f = self.rewrap_refs(f, op, |Splice { pre, call, post }| quote!({
                    #unwrap #pre let __y = #call; #post __y.map(|__y| #wrap)
                }));
            }
            HydroNode::FlatMap { f, input, metadata } => {
                let op = self.register("FlatMap", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                wrap_kind(&mut metadata.collection_kind);
                let (unwrap, wrap) = (unwrap_value(in_rep), wrap_value(rep_of(&metadata.collection_kind), quote!(#rt::derive(#op, &__parents))));
                *f = self.rewrap_refs(f, op, |Splice { pre, call, post }| quote!({
                    #unwrap #pre let __y = #call; #post ::std::iter::IntoIterator::into_iter(__y).map(move |__y| #wrap)
                }));
            }
            HydroNode::Enumerate { input, metadata } => {
                let op = self.register("Enumerate", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                if in_rep != Rep::Stream {
                    let m = metadata.clone();
                    self.unsupported("Enumerate over a keyed collection", &m);
                }
                // enumerate yields `(usize, (u64, T))`; re-nest under a new id.
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let HydroNode::Enumerate { input, mut metadata } = old else { unreachable!() };
                let final_kind = {
                    let mut k = metadata.collection_kind.clone();
                    wrap_kind(&mut k);
                    k
                };
                let inner_element: syn::Type = {
                    let CollectionKind::Stream { element_type, .. } = &input.metadata().collection_kind else { unreachable!() };
                    let t = &**element_type;
                    parse_quote!((usize, #t))
                };
                metadata.collection_kind = kind_with_element(&metadata.collection_kind, inner_element);
                let enumerate = HydroNode::Enumerate { input, metadata: metadata.clone() };
                *node = self.map_node(
                    quote!({ move |(__i, (__id, __t))| (#rt::derive(#op, &[__id]), (__i, __t)) }),
                    enumerate,
                    final_kind,
                    &metadata,
                );
            }

            // By-reference predicates: pass-through.
            HydroNode::Filter { f, input, metadata } => {
                self.wrap_predicate("Filter", f, input, metadata);
            }
            HydroNode::PartitionShared { f, input, metadata } => {
                self.wrap_predicate("PartitionShared", f, input, metadata);
            }
            HydroNode::Inspect { f, input, metadata } => {
                self.wrap_predicate("Inspect", f, input, metadata);
            }

            _ => self.rewrite_node_aggregates(node),
        }
    }
}

fn node_kind_name(node: &HydroNode) -> &'static str {
    match node {
        HydroNode::ObserveNonDet { .. } => "ObserveNonDet",
        HydroNode::AssertIsConsistent { .. } => "AssertIsConsistent",
        HydroNode::UnboundSingleton { .. } => "UnboundSingleton",
        HydroNode::CycleSource { .. } => "CycleSource",
        HydroNode::Tee { .. } => "Tee",
        HydroNode::Reference { .. } => "Reference",
        HydroNode::PartitionSide { .. } => "PartitionSide",
        HydroNode::BeginAtomic { .. } => "BeginAtomic",
        HydroNode::EndAtomic { .. } => "EndAtomic",
        HydroNode::Batch { .. } => "Batch",
        HydroNode::YieldConcat { .. } => "YieldConcat",
        HydroNode::Chain { .. } => "Chain",
        HydroNode::MergeOrdered { .. } => "MergeOrdered",
        HydroNode::ChainFirst { .. } => "ChainFirst",
        HydroNode::DeferTick { .. } => "DeferTick",
        HydroNode::ResolveFutures { .. } => "ResolveFutures",
        HydroNode::ResolveFuturesBlocking { .. } => "ResolveFuturesBlocking",
        HydroNode::ResolveFuturesOrdered { .. } => "ResolveFuturesOrdered",
        HydroNode::Counter { .. } => "Counter",
        _ => "?",
    }
}

impl Pass {
    /// The intermediate kind of an accumulator that carries parent ids: `(Vec<u64>, A)` where
    /// `A` is the node's (unwrapped) output element type.
    fn parents_kind(kind: &CollectionKind) -> CollectionKind {
        let element: syn::Type = match kind {
            CollectionKind::Stream { element_type, .. }
            | CollectionKind::Singleton { element_type, .. }
            | CollectionKind::Optional { element_type, .. } => {
                let t = &**element_type;
                parse_quote!((::std::vec::Vec<u64>, #t))
            }
            CollectionKind::KeyedStream { value_type, .. }
            | CollectionKind::KeyedSingleton { value_type, .. } => {
                let t = &**value_type;
                parse_quote!((::std::vec::Vec<u64>, #t))
            }
        };
        kind_with_element(kind, element)
    }

    fn rewrite_node_aggregates(&mut self, node: &mut HydroNode) {
        let rt = self.rt();
        match node {
            // Aggregates: the accumulator carries the ids of every input seen this tick; a `Map`
            // after the aggregate derives the output from them.
            HydroNode::Fold { .. } | HydroNode::FoldKeyed { .. } => {
                let keyed = matches!(node, HydroNode::FoldKeyed { .. });
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let (HydroNode::Fold { init, acc, input, metadata }
                | HydroNode::FoldKeyed { init, acc, input, metadata }) = old
                else {
                    unreachable!()
                };
                let op = self.register(if keyed { "FoldKeyed" } else { "Fold" }, &metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                let unwrap = unwrap_value(in_rep);
                // `fold_keyed`'s accumulator sees values only; `fold`'s sees whole elements.
                let feed = if keyed { unwrap_value(Rep::Stream) } else { unwrap };
                let init = rewrap(&init, |inner| quote!({
                    let mut __init = #inner;
                    move || (::std::vec::Vec::<u64>::new(), __init())
                }));
                let acc = rewrap(&acc, |inner| quote!({
                    let mut __acc = #inner;
                    move |__s: &mut (::std::vec::Vec<u64>, _), __w| { #feed __s.0.push(__id); __acc(&mut __s.1, __x) }
                }));
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                let out_rep = rep_of(&final_kind);
                let mut agg_metadata = metadata.clone();
                agg_metadata.collection_kind = Self::parents_kind(&metadata.collection_kind);
                let agg = if keyed {
                    HydroNode::FoldKeyed { init, acc, input, metadata: agg_metadata }
                } else {
                    HydroNode::Fold { init, acc, input, metadata: agg_metadata }
                };
                let f = match out_rep {
                    Rep::Stream => quote!({ move |(__parents, __a): (::std::vec::Vec<u64>, _)| (#rt::derive(#op, &__parents), __a) }),
                    Rep::Keyed => quote!({ move |(__k, (__parents, __a)): (_, (::std::vec::Vec<u64>, _))| (__k, (#rt::derive(#op, &__parents), __a)) }),
                };
                *node = self.map_node(f, agg, final_kind, &metadata);
            }
            HydroNode::Reduce { .. } | HydroNode::ReduceKeyed { .. } => {
                let keyed = matches!(node, HydroNode::ReduceKeyed { .. });
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let (HydroNode::Reduce { f, input, metadata } | HydroNode::ReduceKeyed { f, input, metadata }) = old
                else {
                    unreachable!()
                };
                let op = self.register(if keyed { "ReduceKeyed" } else { "Reduce" }, &metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                let feed = if keyed { unwrap_value(Rep::Stream) } else { unwrap_value(in_rep) };
                // A reduce is a fold over `Option<T>` whose first input becomes the state.
                let init = closure(quote!({ move || (::std::vec::Vec::<u64>::new(), ::std::option::Option::None) }));
                let acc = rewrap(&f, |inner| quote!({
                    let mut __f = #inner;
                    move |__s: &mut (::std::vec::Vec<u64>, ::std::option::Option<_>), __w| {
                        #feed
                        __s.0.push(__id);
                        if __s.1.is_none() {
                            __s.1 = ::std::option::Option::Some(__x);
                        } else {
                            __f(__s.1.as_mut().unwrap(), __x);
                        }
                    }
                }));
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                let out_rep = rep_of(&final_kind);
                let option_kind = {
                    let element: syn::Type = match &metadata.collection_kind {
                        CollectionKind::Stream { element_type, .. }
                        | CollectionKind::Singleton { element_type, .. }
                        | CollectionKind::Optional { element_type, .. } => { let t = &**element_type; parse_quote!((::std::vec::Vec<u64>, ::std::option::Option<#t>)) }
                        CollectionKind::KeyedStream { value_type, .. }
                        | CollectionKind::KeyedSingleton { value_type, .. } => { let t = &**value_type; parse_quote!((::std::vec::Vec<u64>, ::std::option::Option<#t>)) }
                    };
                    kind_with_element(&metadata.collection_kind, element)
                };
                let mut agg_metadata = metadata.clone();
                agg_metadata.collection_kind = option_kind;
                let agg = if keyed {
                    HydroNode::FoldKeyed { init, acc, input, metadata: agg_metadata }
                } else {
                    HydroNode::Fold { init, acc, input, metadata: agg_metadata }
                };
                let f = match out_rep {
                    Rep::Stream => quote!({ move |(__parents, __a): (::std::vec::Vec<u64>, ::std::option::Option<_>)| __a.map(|__a| (#rt::derive(#op, &__parents), __a)) }),
                    Rep::Keyed => quote!({ move |(__k, (__parents, __a)): (_, (::std::vec::Vec<u64>, ::std::option::Option<_>))| __a.map(|__a| (__k, (#rt::derive(#op, &__parents), __a))) }),
                };
                let mut fm_metadata = metadata.clone();
                fm_metadata.collection_kind = final_kind;
                *node = HydroNode::FilterMap { f: closure(f), input: Box::new(agg), metadata: fm_metadata };
            }
            HydroNode::Scan { init, acc, input, metadata } => {
                let op = self.register("Scan", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                // `KeyedStream::generator` (behind `first()` and `fold_early_stop`): keyed input,
                // and each output is `Option<(K, U)>` for the key of the input that produced it.
                // Its parents are the inputs of that key alone; any other scan is opaque and its
                // outputs descend from every input seen so far in the tick.
                let per_key = in_rep == Rep::Keyed && matches!(&metadata.collection_kind, CollectionKind::Stream { element_type, .. } if is_option(element_type));
                wrap_kind(&mut metadata.collection_kind);
                if per_key {
                    *init = rewrap(init, |inner| quote!({
                        let mut __init = #inner;
                        move || (::std::collections::HashMap::<_, ::std::vec::Vec<u64>>::new(), __init())
                    }));
                    *acc = rewrap(acc, |inner| quote!({
                        let mut __acc = #inner;
                        move |__s: &mut (::std::collections::HashMap<_, ::std::vec::Vec<u64>>, _), __w| {
                            let (__k, (__id, __v)) = __w;
                            __s.0.entry(::std::clone::Clone::clone(&__k)).or_default().push(__id);
                            let __x = (__k, __v);
                            __acc(&mut __s.1, __x).map(|__y| match &__y {
                                ::std::option::Option::Some((__k2, _)) => {
                                    let __parents = __s.0.get(__k2).cloned().unwrap_or_default();
                                    (#rt::derive(#op, &__parents), __y)
                                }
                                ::std::option::Option::None => (0u64, __y),
                            })
                        }
                    }));
                } else {
                    let unwrap = unwrap_value(in_rep);
                    let wrap = wrap_value(rep_of(&metadata.collection_kind), quote!(#rt::derive(#op, &__s.0)));
                    *init = rewrap(init, |inner| quote!({
                        let mut __init = #inner;
                        move || (::std::vec::Vec::<u64>::new(), __init())
                    }));
                    *acc = rewrap(acc, |inner| quote!({
                        let mut __acc = #inner;
                        move |__s: &mut (::std::vec::Vec<u64>, _), __w| { #unwrap __s.0.push(__id); __acc(&mut __s.1, __x).map(|__y| #wrap) }
                    }));
                }
            }

            // `unique`: a scan that keeps the first record of each payload and reports the rest
            // dropped in its favour, then flattened.
            HydroNode::Unique { .. } => {
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let HydroNode::Unique { input, mut metadata } = old else { unreachable!() };
                let op = self.register("Unique", &metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                let unwrap = unwrap_value(in_rep);
                let wrap = wrap_value(in_rep, quote!(__id));
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                let scan_kind = {
                    let (CollectionKind::Stream { element_type, .. } | CollectionKind::Singleton { element_type, .. } | CollectionKind::Optional { element_type, .. } | CollectionKind::KeyedStream { value_type: element_type, .. } | CollectionKind::KeyedSingleton { value_type: element_type, .. }) = &final_kind;
                    let t = &**element_type;
                    kind_with_element(&metadata.collection_kind, parse_quote!(::std::option::Option<#t>))
                };
                let scan_metadata = { let mut m = metadata.clone(); m.collection_kind = scan_kind; m };
                let scan = HydroNode::Scan {
                    init: closure(quote!({ move || ::std::collections::HashMap::new() })),
                    acc: closure(quote!({
                        move |__seen: &mut ::std::collections::HashMap<_, u64>, __w| {
                            #unwrap
                            ::std::option::Option::Some(match __seen.get(&__x) {
                                ::std::option::Option::Some(__survivor) => { #rt::dropped(#op, __id, ::std::option::Option::Some(*__survivor)); ::std::option::Option::None }
                                ::std::option::Option::None => { __seen.insert(::std::clone::Clone::clone(&__x), __id); let __y = __x; ::std::option::Option::Some(#wrap) }
                            })
                        }
                    })),
                    input,
                    metadata: scan_metadata,
                };
                metadata.collection_kind = final_kind;
                *node = HydroNode::FlatMap { f: closure(quote!({ move |__o: ::std::option::Option<_>| __o })), input: Box::new(scan), metadata };
            }

            // `sort` orders by payload; the id rides along.
            HydroNode::Sort { .. } => {
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let HydroNode::Sort { input, mut metadata } = old else { unreachable!() };
                self.register("Sort", &metadata);
                if rep_of(&metadata.collection_kind) != Rep::Stream {
                    self.unsupported("Sort over a keyed collection", &metadata);
                }
                let CollectionKind::Stream { element_type, .. } = &metadata.collection_kind else { unreachable!() };
                let t = (**element_type).clone();
                let swapped_kind = kind_with_element(&metadata.collection_kind, parse_quote!((#t, u64)));
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                let to_sort = self.map_node(quote!({ move |(__id, __t)| (__t, __id) }), *input, swapped_kind.clone(), &metadata);
                metadata.collection_kind = swapped_kind;
                let sorted = HydroNode::Sort { input: Box::new(to_sort), metadata: metadata.clone() };
                *node = self.map_node(quote!({ move |(__t, __id)| (__id, __t) }), sorted, final_kind, &metadata);
            }

            // Products and joins: child <- both inputs.
            HydroNode::CrossSingleton { .. } | HydroNode::CrossProduct { .. } => {
                let cross = matches!(node, HydroNode::CrossProduct { .. });
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let (HydroNode::CrossSingleton { left, right, mut metadata } | HydroNode::CrossProduct { left, right, mut metadata }) = old
                else {
                    unreachable!()
                };
                let op = self.register(if cross { "CrossProduct" } else { "CrossSingleton" }, &metadata);
                let (lrep, rrep) = (rep_of(&left.metadata().collection_kind), rep_of(&right.metadata().collection_kind));
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                if rep_of(&final_kind) != Rep::Stream {
                    self.unsupported("cross into a keyed collection", &metadata);
                }
                let (ul, ur) = (unwrap_value(lrep), unwrap_value(rrep));
                // The product yields `(left element, right element)`, each wrapped.
                let pair_kind = {
                    let lt = wrapped_element(&left.metadata().collection_kind);
                    let rtt = wrapped_element(&right.metadata().collection_kind);
                    kind_with_element(&metadata.collection_kind, parse_quote!((#lt, #rtt)))
                };
                let inner_metadata = { let mut m = metadata.clone(); m.collection_kind = pair_kind; m };
                let inner = if cross {
                    HydroNode::CrossProduct { left, right, metadata: inner_metadata }
                } else {
                    HydroNode::CrossSingleton { left, right, metadata: inner_metadata }
                };
                let f = quote!({
                    move |(__l, __r)| {
                        let (__lid, __lx) = { let __w = __l; #ul (__id, __x) };
                        let (__rid, __rx) = { let __w = __r; #ur (__id, __x) };
                        (#rt::derive(#op, &[__lid, __rid]), (__lx, __rx))
                    }
                });
                metadata.collection_kind = final_kind.clone();
                *node = self.map_node(f, inner, final_kind, &metadata);
            }
            HydroNode::Join { .. } | HydroNode::JoinHalf { .. } => {
                let half = matches!(node, HydroNode::JoinHalf { .. });
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let (HydroNode::Join { left, right, mut metadata } | HydroNode::JoinHalf { left, right, mut metadata }) = old
                else {
                    unreachable!()
                };
                let op = self.register(if half { "JoinHalf" } else { "Join" }, &metadata);
                if rep_of(&left.metadata().collection_kind) != Rep::Keyed || rep_of(&right.metadata().collection_kind) != Rep::Keyed {
                    self.unsupported("join over unkeyed inputs", &metadata);
                }
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                // Records of the probe (left) side whose key has no partner are dropped by the
                // join; an anti-join beside it reports them (design: "the join against
                // `outstanding` is where the duplicate answers die").
                let (left, right) = {
                    let left_metadata = left.metadata().clone();
                    let right_metadata = right.metadata().clone();
                    let left_rc = Rc::new(RefCell::new(*left));
                    let right_rc = Rc::new(RefCell::new(*right));
                    let keys_kind = match &right_metadata.collection_kind {
                        CollectionKind::KeyedStream { key_type, .. } | CollectionKind::KeyedSingleton { key_type, .. } => {
                            CollectionKind::Stream {
                                bound: crate::compile::ir::BoundKind::Bounded,
                                order: crate::compile::ir::StreamOrder::NoOrder,
                                retry: crate::compile::ir::StreamRetry::ExactlyOnce,
                                element_type: key_type.clone(),
                            }
                        }
                        _ => unreachable!(),
                    };
                    let right_keys = self.map_node(
                        quote!({ move |(__k, _)| __k }),
                        HydroNode::Tee { inner: SharedNode(right_rc.clone()), metadata: right_metadata.clone() },
                        keys_kind,
                        &right_metadata,
                    );
                    let unmatched = HydroNode::AntiJoin {
                        pos: Box::new(HydroNode::Tee { inner: SharedNode(left_rc.clone()), metadata: left_metadata.clone() }),
                        neg: Box::new(right_keys),
                        metadata: left_metadata.clone(),
                    };
                    self.new_roots.push(HydroRoot::ForEach {
                        f: closure(quote!({ move |(_, (__id, _))| #rt::dropped(#op, __id, ::std::option::Option::None) })),
                        input: Box::new(unmatched),
                        op_metadata: metadata.op.clone(),
                    });
                    (
                        Box::new(HydroNode::Tee { inner: SharedNode(left_rc), metadata: left_metadata }),
                        Box::new(HydroNode::Tee { inner: SharedNode(right_rc), metadata: right_metadata }),
                    )
                };
                // The join yields `(K, ((u64, V1), (u64, V2)))`.
                let pair_kind = {
                    let (l, r) = (wrapped_value(&left.metadata().collection_kind), wrapped_value(&right.metadata().collection_kind));
                    kind_with_element(&metadata.collection_kind, parse_quote!((#l, #r)))
                };
                let inner_metadata = { let mut m = metadata.clone(); m.collection_kind = pair_kind; m };
                let inner = if half {
                    HydroNode::JoinHalf { left, right, metadata: inner_metadata }
                } else {
                    HydroNode::Join { left, right, metadata: inner_metadata }
                };
                let f = match rep_of(&final_kind) {
                    Rep::Keyed => quote!({ move |(__k, ((__i1, __v1), (__i2, __v2)))| (__k, (#rt::derive(#op, &[__i1, __i2]), (__v1, __v2))) }),
                    Rep::Stream => quote!({ move |(__k, ((__i1, __v1), (__i2, __v2)))| (#rt::derive(#op, &[__i1, __i2]), (__k, (__v1, __v2))) }),
                };
                metadata.collection_kind = final_kind.clone();
                *node = self.map_node(f, inner, final_kind, &metadata);
            }

            // Anti-join: the negative side is stripped to keys; the surviving positive record is
            // re-derived under a new id at this operator (E3.3: the derivation that answers the
            // absence of a record, to which the held records of its tick are negative support).
            HydroNode::AntiJoin { .. } => {
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let HydroNode::AntiJoin { pos, neg, mut metadata } = old else { unreachable!() };
                let op = self.register("AntiJoin", &metadata);
                if rep_of(&pos.metadata().collection_kind) != Rep::Keyed || rep_of(&neg.metadata().collection_kind) != Rep::Stream {
                    self.unsupported("anti-join with these input shapes", &metadata);
                }
                let neg_kind = unwrapped_kind(&neg.metadata().collection_kind);
                let neg_metadata = neg.metadata().clone();
                let keys = self.map_node(quote!({ move |(_, __k)| __k }), *neg, neg_kind, &neg_metadata);
                wrap_kind(&mut metadata.collection_kind);
                let final_kind = metadata.collection_kind.clone();
                let anti = HydroNode::AntiJoin { pos, neg: Box::new(keys), metadata: metadata.clone() };
                *node = self.map_node(
                    quote!({ move |(__k, (__id, __v))| (__k, (#rt::derive(#op, &[__id]), __v)) }),
                    anti,
                    final_kind,
                    &metadata,
                );
            }
            // Difference: an anti-join keyed by the whole payload; the survivor re-derived likewise.
            HydroNode::Difference { .. } => {
                let old = std::mem::replace(node, HydroNode::Placeholder);
                let HydroNode::Difference { pos, neg, mut metadata } = old else { unreachable!() };
                let op = self.register("Difference", &metadata);
                if rep_of(&metadata.collection_kind) != Rep::Stream {
                    self.unsupported("difference over a keyed collection", &metadata);
                }
                let mut final_kind = metadata.collection_kind.clone();
                wrap_kind(&mut final_kind);
                let CollectionKind::Stream { element_type, .. } = &metadata.collection_kind else { unreachable!() };
                let t = (**element_type).clone();
                let pos_metadata = pos.metadata().clone();
                let keyed_pos = self.map_node(quote!({ move |(__id, __t)| (__t, __id) }), *pos, kind_with_element(&metadata.collection_kind, parse_quote!((#t, u64))), &pos_metadata);
                let neg_metadata = neg.metadata().clone();
                let keys = self.map_node(quote!({ move |(_, __t)| __t }), *neg, unwrapped_kind(&neg_metadata.collection_kind), &neg_metadata);
                let anti_metadata = { let mut m = metadata.clone(); m.collection_kind = kind_with_element(&metadata.collection_kind, parse_quote!((#t, u64))); m };
                let anti = HydroNode::AntiJoin { pos: Box::new(keyed_pos), neg: Box::new(keys), metadata: anti_metadata };
                metadata.collection_kind = final_kind.clone();
                *node = self.map_node(quote!({ move |(__t, __id)| (#rt::derive(#op, &[__id]), __t) }), anti, final_kind, &metadata);
            }

            // The network carries the id in front of the payload.
            HydroNode::Network { serialize, deserialize, input, metadata, .. } => {
                self.register("Network", metadata);
                let in_rep = rep_of(&input.metadata().collection_kind);
                wrap_kind(&mut metadata.collection_kind);
                let out_rep = rep_of(&metadata.collection_kind);
                match serialize {
                    NetworkSend::Custom { serialize_fn: Some(f) } => {
                        let inner = &*f;
                        let unwrap = unwrap_value(in_rep);
                        *f = debug_expr(quote!({
                            let mut __f = #inner;
                            move |__w| { #unwrap #rt::PrependId::prepend_id(__f(__x), __id) }
                        }));
                    }
                    _ => { let m = metadata.clone(); self.unsupported("network without a custom serializer", &m) }
                }
                match deserialize {
                    NetworkRecv::Custom { deserialize_fn: Some(f) } => {
                        let inner = &*f;
                        let wrap = wrap_value(out_rep, quote!(__id));
                        *f = debug_expr(quote!({
                            move |__res| { let (__id, __rest) = #rt::SplitId::split_id(__res); let __y = #rt::apply(#inner, __rest); #wrap }
                        }));
                    }
                    _ => { let m = metadata.clone(); self.unsupported("network without a custom deserializer", &m) }
                }
            }

            other => {
                let m = other.metadata().clone();
                self.unsupported(&format!("{other:?}").chars().take(40).collect::<String>(), &m);
            }
        }
    }

    fn rewrite_root(&mut self, root: &mut HydroRoot) {
        let rt = self.rt();
        match root {
            HydroRoot::ForEach { f, input, op_metadata } => {
                let op = self.register_root("ForEach", op_metadata, input.metadata());
                let unwrap = unwrap_value(rep_of(&input.metadata().collection_kind));
                *f = self.rewrap_refs(f, op, |Splice { pre, call, post }| quote!({
                    #unwrap #pre let __r = #call; #post __r
                }));
            }
            HydroRoot::SendExternal { serialize_fn, input, op_metadata, .. } => {
                let op = self.register_root("SendExternal", op_metadata, input.metadata());
                let Some(f) = serialize_fn else {
                    panic!("lineage pass: external output without a serializer");
                };
                let unwrap = unwrap_value(rep_of(&input.metadata().collection_kind));
                let inner = &*f;
                *f = debug_expr(quote!({
                    let mut __f = #inner;
                    move |__w| { #unwrap #rt::output(#op, __id); __f(__x) }
                }));
            }
            HydroRoot::DestSink { input, op_metadata, .. } | HydroRoot::EmbeddedOutput { input, op_metadata, .. } => {
                self.register_root("Sink", op_metadata, input.metadata());
                let unwrap = unwrap_value(rep_of(&input.metadata().collection_kind));
                let old = std::mem::replace(&mut **input, HydroNode::Placeholder);
                let kind = unwrapped_kind(&old.metadata().collection_kind);
                let metadata = old.metadata().clone();
                **input = self.map_node(quote!({ move |__w| { #unwrap let _ = __id; __x } }), old, kind, &metadata);
            }
            HydroRoot::CycleSink { input, op_metadata, .. } => {
                self.register_root("CycleSink", op_metadata, input.metadata());
            }
            HydroRoot::Null { input, op_metadata } => {
                self.register_root("Null", op_metadata, input.metadata());
            }
        }
    }

    fn register_root(&mut self, kind: &str, op_metadata: &crate::compile::ir::HydroIrOpMetadata, input: &HydroIrMetadata) -> u32 {
        let id = self.operators.len() as u32;
        let (location, source_line, _) = super::builder::location_for_op(op_metadata);
        self.operators.push(OperatorInfo {
            id,
            kind: format!("root {kind}"),
            location,
            source_line: source_line.trim().to_owned(),
            element_type: element_type_text(&input.collection_kind),
            negative: false,
        });
        id
    }
}

/// Whether a type is `Option<..>` (by its last path segment).
fn is_option(t: &syn::Type) -> bool {
    matches!(t, syn::Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Option"))
}

/// The (already wrapped) element type of a collection: `(u64, T)` or `(K, (u64, V))`.
fn wrapped_element(kind: &CollectionKind) -> syn::Type {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => (**element_type).clone(),
        CollectionKind::KeyedStream { key_type, value_type, .. }
        | CollectionKind::KeyedSingleton { key_type, value_type, .. } => {
            let (k, v) = (&**key_type, &**value_type);
            parse_quote!((#k, #v))
        }
    }
}

/// The (already wrapped) value type of a keyed collection: `(u64, V)`.
fn wrapped_value(kind: &CollectionKind) -> syn::Type {
    match kind {
        CollectionKind::KeyedStream { value_type, .. } | CollectionKind::KeyedSingleton { value_type, .. } => (**value_type).clone(),
        _ => panic!("lineage pass: keyed collection expected"),
    }
}

/// Undoes the wrapping of one element type: `(u64, T)` gives `T`. Used by the sim builder so
/// that the hooks' edge keys name the program's own type.
pub(crate) fn unwrap_element_type(t: &syn::Type) -> syn::Type {
    if let syn::Type::Tuple(tuple) = t
        && tuple.elems.len() == 2
    {
        return tuple.elems[1].clone();
    }
    panic!("lineage pass: expected a wrapped element type, got {}", quote!(#t));
}

/// Undoes [`wrap_kind`] on an already wrapped kind (the element type is `(u64, T)`).
fn unwrapped_kind(kind: &CollectionKind) -> CollectionKind {
    let strip = |t: &syn::Type| -> syn::Type {
        if let syn::Type::Tuple(tuple) = t
            && tuple.elems.len() == 2
        {
            return tuple.elems[1].clone();
        }
        panic!("lineage pass: expected a wrapped element type, got {}", quote!(#t));
    };
    let mut out = kind.clone();
    match &mut out {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => *element_type = strip(element_type).into(),
        CollectionKind::KeyedStream { value_type, .. } | CollectionKind::KeyedSingleton { value_type, .. } => *value_type = strip(value_type).into(),
    }
    out
}
