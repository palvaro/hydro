//! The record-level provenance rewrite (LDFI "Level 2"): a sim-only IR pass that wraps
//! every collection element in [`Tagged`](crate::sim::runtime::Tagged) and makes each
//! operator propagate the delivery ids its outputs were computed from.
//!
//! Physical representation after the rewrite:
//!
//! * a `Stream` / `Singleton` / `Optional` of `T` carries `Tagged<T>`;
//! * a `KeyedStream` / `KeyedSingleton` of `(K, V)` carries `(K, Tagged<V>)`, so keyed DFIR
//!   operators still see the key as the first tuple component.
//!
//! Closures are wrapped so the user's code still sees `T`; adapters are inserted where a
//! DFIR operator destructures pairs (joins, cross products) or where the representation
//! changes (a `Cast` between a flat and a keyed kind). Sources, external inputs and network
//! receives produce empty provenance; a scripted batch's releases get fresh delivery ids
//! (`tag_delivery`); network sends and external outputs are preceded by a map to
//! `(value, Vec<delivery id>)`, which `SimBuilder` (in provenance mode) serializes and
//! records.
//!
//! Unsupported operators panic at compile time with their name: the rewrite is complete for
//! the operators the LDFI examples use and explicit about what it does not cover.

use syn::parse_quote;

use crate::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, DebugType, HydroIrMetadata, HydroNode, HydroRoot,
    transform_bottom_up,
};
use crate::staging_util::get_this_crate;

/// How a collection's elements are laid out after the rewrite.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Rep {
    /// `Tagged<T>`.
    Flat,
    /// `(K, Tagged<V>)`.
    Keyed,
}

fn rep_of(kind: &CollectionKind) -> Rep {
    match kind {
        CollectionKind::Stream { .. }
        | CollectionKind::Singleton { .. }
        | CollectionKind::Optional { .. } => Rep::Flat,
        CollectionKind::KeyedStream { .. } | CollectionKind::KeyedSingleton { .. } => Rep::Keyed,
    }
}

fn tagged(ty: &DebugType) -> DebugType {
    let root = get_this_crate();
    let inner: &syn::Type = &ty.0;
    let out: syn::Type = parse_quote!(#root::sim::runtime::Tagged<#inner>);
    DebugType::from(out)
}

/// `(A, B)` → `(Tagged<A>, Tagged<B>)` (and `(K, (A, B))` → `(K, (Tagged<A>, Tagged<B>))`
/// with `key`), the element type a pair-producing DFIR operator yields after the rewrite.
fn tag_pair_type(ty: &DebugType, key: bool) -> DebugType {
    let root = get_this_crate();
    let tag = |t: &syn::Type| -> syn::Type { parse_quote!(#root::sim::runtime::Tagged<#t>) };
    let pair = |t: &syn::Type| -> syn::Type {
        if let syn::Type::Tuple(tuple) = t
            && tuple.elems.len() == 2
        {
            let a = tag(&tuple.elems[0]);
            let b = tag(&tuple.elems[1]);
            parse_quote!((#a, #b))
        } else {
            parse_quote!((_, _))
        }
    };
    let inner: &syn::Type = &ty.0;
    let out: syn::Type = if key {
        if let syn::Type::Tuple(tuple) = inner
            && tuple.elems.len() == 2
        {
            let k = &tuple.elems[0];
            let v = pair(&tuple.elems[1]);
            parse_quote!((#k, #v))
        } else {
            parse_quote!((_, (_, _)))
        }
    } else {
        pair(inner)
    };
    DebugType::from(out)
}

/// `(K, V)` → `(K, Tagged<V>)`.
fn tag_value_type(ty: &DebugType) -> DebugType {
    let root = get_this_crate();
    let inner: &syn::Type = &ty.0;
    let out: syn::Type = if let syn::Type::Tuple(tuple) = inner
        && tuple.elems.len() == 2
    {
        let k = &tuple.elems[0];
        let v = &tuple.elems[1];
        parse_quote!((#k, #root::sim::runtime::Tagged<#v>))
    } else {
        parse_quote!((_, _))
    };
    DebugType::from(out)
}

fn set_element_type(kind: &mut CollectionKind, ty: DebugType) {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => *element_type = ty,
        CollectionKind::KeyedStream { .. } | CollectionKind::KeyedSingleton { .. } => {
            unreachable!("pair-producing operators yield flat kinds")
        }
    }
}

fn element_type(kind: &CollectionKind) -> DebugType {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => element_type.clone(),
        CollectionKind::KeyedStream {
            key_type,
            value_type,
            ..
        }
        | CollectionKind::KeyedSingleton {
            key_type,
            value_type,
            ..
        } => {
            let k: &syn::Type = &key_type.0;
            let v: &syn::Type = &value_type.0;
            let out: syn::Type = parse_quote!((#k, #v));
            DebugType::from(out)
        }
    }
}

/// Rewrites the kind's element types to the tagged representation.
fn tag_kind(kind: &mut CollectionKind) {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => {
            *element_type = tagged(element_type);
        }
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => {
            *value_type = tagged(value_type);
        }
    }
}

/// The destructuring pattern and reconstructed plain value for one input element.
fn unwrap_tokens(rep: Rep) -> (syn::Pat, syn::Expr) {
    let root = get_this_crate();
    match rep {
        Rep::Flat => (
            parse_quote!(#root::sim::runtime::Tagged(__val, __prov)),
            parse_quote!(__val),
        ),
        Rep::Keyed => (
            parse_quote!((__key, #root::sim::runtime::Tagged(__val, __prov))),
            parse_quote!((__key, __val)),
        ),
    }
}

/// Wraps a plain output value `expr` with provenance `prov` in the representation `rep`.
fn wrap_tokens(rep: Rep, expr: syn::Expr, prov: syn::Expr) -> syn::Expr {
    let root = get_this_crate();
    match rep {
        Rep::Flat => parse_quote!(#root::sim::runtime::Tagged(#expr, #prov)),
        Rep::Keyed => parse_quote!({
            let (__k, __u) = #expr;
            (__k, #root::sim::runtime::Tagged(__u, #prov))
        }),
    }
}

fn wrap_closure(closure: &ClosureExpr, make: impl FnOnce(&syn::Expr) -> syn::Expr) -> ClosureExpr {
    let mut wrapped = closure.clone();
    wrapped.expr = DebugExpr::from(make(&closure.expr));
    wrapped
}

/// `map`-style: `Fn(T) -> U`.
fn wrap_map(f: &ClosureExpr, input: Rep, output: Rep) -> ClosureExpr {
    let (pat, val) = unwrap_tokens(input);
    wrap_closure(f, |f| {
        let out = wrap_tokens(output, parse_quote!(__f(#val)), parse_quote!(__prov));
        parse_quote!({
            let mut __f = #f;
            move |#pat| #out
        })
    })
}

/// `filter` / `inspect`-style: `Fn(&T) -> R`, where `T` must be reconstructed by reference.
/// `input` is the (already tagged) kind of the input, whose concrete types the closure's
/// parameter is annotated with (push-based operators infer the item type from it).
fn wrap_by_ref(f: &ClosureExpr, input: &CollectionKind) -> ClosureExpr {
    wrap_closure(f, |f| match input {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => {
            let ty: &syn::Type = &element_type.0;
            parse_quote!({
                let mut __f = #f;
                move |__x: &#ty| __f(&__x.0)
            })
        }
        CollectionKind::KeyedStream {
            key_type,
            value_type,
            ..
        }
        | CollectionKind::KeyedSingleton {
            key_type,
            value_type,
            ..
        } => {
            let k: &syn::Type = &key_type.0;
            let v: &syn::Type = &value_type.0;
            parse_quote!({
                let mut __f = #f;
                move |__x: &(#k, #v)| {
                    let __plain = (__x.0.clone(), (__x.1).0.clone());
                    __f(&__plain)
                }
            })
        }
    })
}

/// `filter_map`-style: `Fn(T) -> Option<U>`.
fn wrap_filter_map(f: &ClosureExpr, input: Rep, output: Rep) -> ClosureExpr {
    let (pat, val) = unwrap_tokens(input);
    wrap_closure(f, |f| {
        let out = wrap_tokens(output, parse_quote!(__u), parse_quote!(__prov));
        parse_quote!({
            let mut __f = #f;
            move |#pat| __f(#val).map(|__u| #out)
        })
    })
}

/// `flat_map`-style: `Fn(T) -> impl IntoIterator<Item = U>`.
fn wrap_flat_map(f: &ClosureExpr, input: Rep, output: Rep) -> ClosureExpr {
    let (pat, val) = unwrap_tokens(input);
    wrap_closure(f, |f| {
        let out = wrap_tokens(output, parse_quote!(__u), parse_quote!(__prov.clone()));
        parse_quote!({
            let mut __f = #f;
            move |#pat| __f(#val).into_iter().map(move |__u| #out)
        })
    })
}

/// `Fn() -> A` producing an accumulator with empty provenance.
fn wrap_init(init: &ClosureExpr) -> ClosureExpr {
    let root = get_this_crate();
    wrap_closure(init, |init| {
        parse_quote!({
            let mut __init = #init;
            move || #root::sim::runtime::Tagged(__init(), #root::sim::runtime::Prov::empty())
        })
    })
}

/// `Fn(&mut A, T)` over a tagged accumulator: applies, then absorbs the input's provenance.
/// `input` is the representation of the *value* the closure receives (`Flat` for keyed folds,
/// whose keys DFIR strips).
fn wrap_acc(acc: &ClosureExpr, input: Rep) -> ClosureExpr {
    let (pat, val) = unwrap_tokens(input);
    let root = get_this_crate();
    wrap_closure(acc, |acc| {
        parse_quote!({
            let mut __acc = #acc;
            move |__state: &mut #root::sim::runtime::Tagged<_>, #pat| {
                __acc(&mut __state.0, #val);
                __state.1.extend(&__prov);
            }
        })
    })
}

/// `Fn(&mut A, T) -> Option<U>` (scan): absorbs, applies, tags any output with the
/// accumulated provenance.
fn wrap_scan_acc(acc: &ClosureExpr, input: Rep, output: Rep) -> ClosureExpr {
    let (pat, val) = unwrap_tokens(input);
    let root = get_this_crate();
    wrap_closure(acc, |acc| {
        let out = wrap_tokens(output, parse_quote!(__u), parse_quote!(__state.1.clone()));
        parse_quote!({
            let mut __acc = #acc;
            move |__state: &mut #root::sim::runtime::Tagged<_>, #pat| {
                __state.1.extend(&__prov);
                __acc(&mut __state.0, #val).map(|__u| #out)
            }
        })
    })
}

/// A plain `Map` node applying `f` to `input`, with the given (already tagged) metadata.
fn map_node(f: syn::Expr, input: HydroNode, metadata: HydroIrMetadata) -> HydroNode {
    HydroNode::Map {
        f: ClosureExpr::new(DebugExpr::from(f), vec![]),
        input: Box::new(input),
        metadata,
    }
}

/// Replaces `node` by a `Map` applying `f` to it, with `metadata` on the wrapper.
fn wrap_node(node: &mut HydroNode, f: syn::Expr, metadata: HydroIrMetadata) {
    let inner = std::mem::replace(node, HydroNode::Placeholder);
    *node = map_node(f, inner, metadata);
}

/// The expression tagging a plain value with empty provenance, for the representation.
fn empty_tag_expr(rep: Rep) -> syn::Expr {
    let root = get_this_crate();
    match rep {
        Rep::Flat => parse_quote!(#root::sim::runtime::Tagged::empty),
        Rep::Keyed => parse_quote!(|(__k, __v)| (__k, #root::sim::runtime::Tagged::empty(__v))),
    }
}

/// The expression turning a tagged element into `(plain value, Vec<delivery id>)`, for the
/// send / output sites `SimBuilder` emits in provenance mode.
fn strip_expr(rep: Rep) -> syn::Expr {
    let (pat, val) = unwrap_tokens(rep);
    parse_quote!(move |#pat| (#val, __prov.ids().to_vec()))
}

/// Rewrites `ir` in place. Must run after network compilation and before slicing and
/// emission.
pub fn rewrite(ir: &mut [HydroRoot]) {
    let root = get_this_crate();
    transform_bottom_up(
        ir,
        &mut |hroot: &mut HydroRoot| match hroot {
            HydroRoot::SendExternal { input, .. } | HydroRoot::DestSink { input, .. } => {
                // The sink sees `(value, Vec<id>)`.
                let rep = rep_of(&input.metadata().collection_kind);
                let strip = strip_expr(rep);
                let metadata = input.metadata().clone();
                let inner = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
                **input = map_node(strip, inner, metadata);
            }
            HydroRoot::ForEach { f, input, .. } => {
                let rep = rep_of(&input.metadata().collection_kind);
                let (pat, val) = unwrap_tokens(rep);
                *f = wrap_closure(f, |f| {
                    parse_quote!({
                        let mut __f = #f;
                        move |#pat| __f(#val)
                    })
                });
            }
            HydroRoot::CycleSink { .. }
            | HydroRoot::EmbeddedOutput { .. }
            | HydroRoot::Null { .. } => {}
        },
        &mut |node: &mut HydroNode| {
            // Children are already rewritten; this node's own kind is still the original.
            let out_rep = rep_of(&node.metadata().collection_kind);
            let input_rep = |input: &HydroNode| rep_of(&input.metadata().collection_kind);
            let name = node.print_root();
            let name = name.split('(').next().unwrap_or(&name).to_owned();
            // A wrapper to put around this node once the match releases its borrows.
            let mut post: Option<(syn::Expr, HydroIrMetadata)> = None;

            match node {
                HydroNode::Placeholder => unreachable!(),
                // Pass-through: the representation of the input is the representation of
                // the output.
                HydroNode::Cast { inner, metadata } => {
                    let in_rep = input_rep(inner);
                    tag_kind(&mut metadata.collection_kind);
                    if in_rep != out_rep {
                        let adapter: syn::Expr = match (in_rep, out_rep) {
                            (Rep::Flat, Rep::Keyed) => {
                                parse_quote!(#root::sim::runtime::codegen::key_out)
                            }
                            (Rep::Keyed, Rep::Flat) => {
                                parse_quote!(#root::sim::runtime::codegen::key_in)
                            }
                            _ => unreachable!(),
                        };
                        let metadata = metadata.clone();
                        let old = std::mem::replace(inner.as_mut(), HydroNode::Placeholder);
                        **inner = map_node(adapter, old, metadata);
                    }
                }
                HydroNode::ObserveNonDet { metadata, .. }
                | HydroNode::YieldConcat { metadata, .. }
                | HydroNode::BeginAtomic { metadata, .. }
                | HydroNode::EndAtomic { metadata, .. }
                | HydroNode::UnboundSingleton { metadata, .. }
                | HydroNode::AssertIsConsistent { metadata, .. }
                | HydroNode::Chain { metadata, .. }
                | HydroNode::MergeOrdered { metadata, .. }
                | HydroNode::ChainFirst { metadata, .. }
                | HydroNode::Tee { metadata, .. }
                | HydroNode::CycleSource { metadata, .. }
                | HydroNode::DeferTick { metadata, .. }
                | HydroNode::Unique { metadata, .. }
                | HydroNode::Sort { metadata, .. }
                | HydroNode::Counter { metadata, .. }
                | HydroNode::Difference { metadata, .. } => {
                    tag_kind(&mut metadata.collection_kind);
                }

                // Sources: empty provenance.
                HydroNode::Source { metadata, .. } | HydroNode::ExternalInput { metadata, .. } => {
                    let mut tagged_meta = metadata.clone();
                    tag_kind(&mut tagged_meta.collection_kind);
                    post = Some((empty_tag_expr(out_rep), tagged_meta));
                }
                HydroNode::SingletonSource {
                    value, metadata, ..
                } => {
                    let inner: &syn::Expr = value;
                    let wrapped: syn::Expr =
                        parse_quote!(#root::sim::runtime::Tagged::empty(#inner));
                    *value = DebugExpr::from(wrapped);
                    tag_kind(&mut metadata.collection_kind);
                }

                // A scripted batch's releases get fresh delivery ids; `SimBuilder::batch`
                // emits the tagging (it owns the lineage handle at that point).
                HydroNode::Batch { metadata, .. } => {
                    tag_kind(&mut metadata.collection_kind);
                    assert_eq!(out_rep, Rep::Flat, "provenance rewrite: keyed batch");
                }

                // Network: the send side sees `(value, Vec<id>)`; the receive side starts
                // with empty provenance (the send side recorded the record's).
                HydroNode::Network {
                    input, metadata, ..
                } => {
                    let in_rep = input_rep(input);
                    let strip = strip_expr(in_rep);
                    let in_meta = input.metadata().clone();
                    let old = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
                    **input = map_node(strip, old, in_meta);
                    let mut tagged_meta = metadata.clone();
                    tag_kind(&mut tagged_meta.collection_kind);
                    post = Some((empty_tag_expr(out_rep), tagged_meta));
                }

                // Closures.
                HydroNode::Map { f, input, metadata } => {
                    *f = wrap_map(f, input_rep(input), out_rep);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::Filter { f, input, metadata }
                | HydroNode::Inspect { f, input, metadata } => {
                    *f = wrap_by_ref(f, &input.metadata().collection_kind);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::FilterMap { f, input, metadata } => {
                    *f = wrap_filter_map(f, input_rep(input), out_rep);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::FlatMap { f, input, metadata } => {
                    *f = wrap_flat_map(f, input_rep(input), out_rep);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::Fold {
                    init,
                    acc,
                    input,
                    metadata,
                } => {
                    assert_eq!(
                        input_rep(input),
                        Rep::Flat,
                        "provenance rewrite: keyed input to fold"
                    );
                    *init = wrap_init(init);
                    *acc = wrap_acc(acc, Rep::Flat);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::FoldKeyed {
                    init,
                    acc,
                    input,
                    metadata,
                } => {
                    assert_eq!(
                        input_rep(input),
                        Rep::Keyed,
                        "provenance rewrite: flat input to fold_keyed"
                    );
                    *init = wrap_init(init);
                    *acc = wrap_acc(acc, Rep::Flat);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::Reduce { f, input, metadata } => {
                    assert_eq!(
                        input_rep(input),
                        Rep::Flat,
                        "provenance rewrite: keyed input to reduce"
                    );
                    *f = wrap_acc(f, Rep::Flat);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::ReduceKeyed { f, input, metadata } => {
                    assert_eq!(
                        input_rep(input),
                        Rep::Keyed,
                        "provenance rewrite: flat input to reduce_keyed"
                    );
                    *f = wrap_acc(f, Rep::Flat);
                    tag_kind(&mut metadata.collection_kind);
                }
                HydroNode::Scan {
                    init,
                    acc,
                    input,
                    metadata,
                } => {
                    let in_rep = input_rep(input);
                    *init = wrap_init(init);
                    *acc = wrap_scan_acc(acc, in_rep, out_rep);
                    tag_kind(&mut metadata.collection_kind);
                }

                // Pair-producing operators: the operator yields `(Tagged<A>, Tagged<B>)`; a
                // wrapper merges the two provenances.
                HydroNode::CrossProduct { metadata, .. }
                | HydroNode::CrossSingleton { metadata, .. } => {
                    assert_eq!(
                        out_rep,
                        Rep::Flat,
                        "provenance rewrite: keyed cross product"
                    );
                    let mut tagged_meta = metadata.clone();
                    tag_kind(&mut tagged_meta.collection_kind);
                    let pair = tag_pair_type(&element_type(&metadata.collection_kind), false);
                    set_element_type(&mut metadata.collection_kind, pair);
                    post = Some((
                        parse_quote!(#root::sim::runtime::codegen::merge_pair),
                        tagged_meta,
                    ));
                }
                HydroNode::Join {
                    left,
                    right,
                    metadata,
                }
                | HydroNode::JoinHalf {
                    left,
                    right,
                    metadata,
                } => {
                    assert_eq!(out_rep, Rep::Flat, "provenance rewrite: keyed join output");
                    for side in [left, right] {
                        assert_eq!(
                            input_rep(side),
                            Rep::Flat,
                            "provenance rewrite: keyed join input"
                        );
                        let mut meta = side.metadata().clone();
                        let keyed = tag_value_type(&element_type(&side.metadata().collection_kind));
                        set_element_type(&mut meta.collection_kind, keyed);
                        let old = std::mem::replace(side.as_mut(), HydroNode::Placeholder);
                        **side = map_node(
                            parse_quote!(#root::sim::runtime::codegen::key_out),
                            old,
                            meta,
                        );
                    }
                    let mut tagged_meta = metadata.clone();
                    tag_kind(&mut tagged_meta.collection_kind);
                    let pair = tag_pair_type(&element_type(&metadata.collection_kind), true);
                    set_element_type(&mut metadata.collection_kind, pair);
                    post = Some((
                        parse_quote!(#root::sim::runtime::codegen::key_in_pair),
                        tagged_meta,
                    ));
                }
                HydroNode::AntiJoin { pos, neg, metadata } => {
                    // `pos` is `(K, V)`, `neg` is `K`; the output is `(K, V)`.
                    assert_eq!(
                        input_rep(pos),
                        Rep::Flat,
                        "provenance rewrite: keyed anti_join input"
                    );
                    let mut pos_meta = pos.metadata().clone();
                    let keyed = tag_value_type(&element_type(&pos.metadata().collection_kind));
                    set_element_type(&mut pos_meta.collection_kind, keyed);
                    let old = std::mem::replace(pos.as_mut(), HydroNode::Placeholder);
                    **pos = map_node(
                        parse_quote!(#root::sim::runtime::codegen::key_out),
                        old,
                        pos_meta,
                    );
                    let neg_meta = neg.metadata().clone();
                    let old = std::mem::replace(neg.as_mut(), HydroNode::Placeholder);
                    **neg = map_node(
                        parse_quote!(|__x: #root::sim::runtime::Tagged<_>| __x.0),
                        old,
                        neg_meta,
                    );
                    let mut tagged_meta = metadata.clone();
                    tag_kind(&mut tagged_meta.collection_kind);
                    let keyed = tag_value_type(&element_type(&metadata.collection_kind));
                    set_element_type(&mut metadata.collection_kind, keyed);
                    post = Some((
                        parse_quote!(#root::sim::runtime::codegen::key_in),
                        tagged_meta,
                    ));
                }

                HydroNode::Reference { .. }
                | HydroNode::PartitionSide { .. }
                | HydroNode::PartitionShared { .. }
                | HydroNode::Enumerate { .. }
                | HydroNode::ResolveFutures { .. }
                | HydroNode::ResolveFuturesBlocking { .. }
                | HydroNode::ResolveFuturesOrdered { .. }
                | HydroNode::FlatMapStreamBlocking { .. }
                | HydroNode::ScanAsyncBlocking { .. }
                | HydroNode::ReduceKeyedWatermark { .. }
                | HydroNode::VersionedNetwork { .. }
                | HydroNode::VersionedNetworkFork { .. } => {
                    panic!(
                        "the provenance rewrite (Level 2 lineage) does not support `{name}` yet; \
                         compile without `with_provenance()` or extend `sim::provenance`"
                    );
                }
            }
            if let Some((f, metadata)) = post {
                wrap_node(node, f, metadata);
            }
        },
        false,
    );
}
