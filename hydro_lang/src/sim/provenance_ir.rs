//! IR rewrite that threads provenance tags through a Hydro program before it is compiled for
//! simulation. See [`super::provenance`] for the runtime side and the classification.
//!
//! The pass runs bottom-up over the IR and rewrites every node so that the items it produces are
//! wrapped in [`super::provenance::Tagged`]. Two representations are used, chosen by the node's
//! `CollectionKind` so that the simulator's keyed hooks (which destructure `(k, v)` pairs) keep
//! working:
//!
//! - streams, singletons, optionals: `Tagged<T>`;
//! - keyed streams and keyed singletons: `(K, Tagged<V>)`.
//!
//! User closures are wrapped so they see exactly the values they were written for; the wrapper
//! moves tags around them. Structural operators (`join`, `cross`, `anti_join`, `enumerate`)
//! get pre/post `Map`s that reshape the item so the DFIR operator still sees the key or pair it
//! expects. Everything else passes through unchanged because `Tagged`'s `Eq`/`Hash`/`Ord`
//! delegate to the value.
//!
//! The pass never adds or removes a nondeterministic decision point, so a recorded execution
//! replays identically with and without provenance.

use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

use crate::compile::builder::ExternalPortId;
use crate::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, DebugType, HydroIrMetadata, HydroNode, HydroRoot,
    HydroSource, NetworkRecv, NetworkSend, SeenSharedNodes,
};
use crate::handoff_ref::{HandoffRefKind, handoff_ref_ident};
use crate::location::dynamic::LocationId;
use crate::staging_util::get_this_crate;

/// Item representation of a collection kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repr {
    /// `Tagged<T>`
    Flat,
    /// `(K, Tagged<V>)`
    Keyed,
}

fn repr_of(kind: &CollectionKind) -> Repr {
    match kind {
        CollectionKind::Stream { .. }
        | CollectionKind::Singleton { .. }
        | CollectionKind::Optional { .. } => Repr::Flat,
        CollectionKind::KeyedStream { .. } | CollectionKind::KeyedSingleton { .. } => Repr::Keyed,
    }
}

fn prov() -> TokenStream {
    let root = get_this_crate();
    quote!(#root::sim::provenance)
}

/// Expression evaluating to `(tags, coarse, value)` for an item `#item` of representation `repr`.
fn untag(repr: Repr, item: TokenStream) -> TokenStream {
    let p = prov();
    match repr {
        Repr::Flat => quote!(#p::Tagged::into_parts(#item)),
        Repr::Keyed => quote!({
            let (__prov_k, __prov_tv) = #item;
            let (__prov_t, __prov_c, __prov_v) = #p::Tagged::into_parts(__prov_tv);
            (__prov_t, __prov_c, (__prov_k, __prov_v))
        }),
    }
}

/// Expression rebuilding an item of representation `repr` from `(tags, coarse, value)`.
fn retag(repr: Repr, tags: TokenStream, coarse: TokenStream, value: TokenStream) -> TokenStream {
    let p = prov();
    match repr {
        Repr::Flat => quote!(#p::Tagged::from_parts(#tags, #coarse, #value)),
        Repr::Keyed => quote!({
            let (__prov_k, __prov_v) = #value;
            (__prov_k, #p::Tagged::from_parts(#tags, #coarse, __prov_v))
        }),
    }
}

/// Expression yielding a borrowed untagged view `&T` of `&item` (clones for keyed items, since
/// no `&(K, V)` exists in memory).
fn untag_ref(repr: Repr, item: TokenStream) -> (TokenStream, TokenStream) {
    match repr {
        Repr::Flat => (quote!(), quote!(&(#item).value)),
        Repr::Keyed => (
            quote!(let __prov_tmp = (::core::clone::Clone::clone(&(#item).0), ::core::clone::Clone::clone(&(#item).1.value));),
            quote!(&__prov_tmp),
        ),
    }
}

/// The concrete (already retyped) item type flowing out of a node with this kind.
fn item_type(kind: &CollectionKind) -> syn::Type {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => (*element_type.0).clone(),
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
            let k = &key_type.0;
            let v = &value_type.0;
            parse_quote!((#k, #v))
        }
    }
}

/// The value type of a keyed collection (the `Tagged<V>` handed to keyed accumulators).
fn keyed_value_type(kind: &CollectionKind) -> syn::Type {
    match kind {
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => (*value_type.0).clone(),
        _ => panic!("provenance: expected keyed collection"),
    }
}

fn tagged_type(ty: &DebugType) -> DebugType {
    let p = prov();
    let inner = &ty.0;
    DebugType(Box::new(parse_quote!(#p::Tagged<#inner>)))
}

fn retype_kind(kind: &mut CollectionKind) {
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => {
            *element_type = tagged_type(element_type);
        }
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => {
            *value_type = tagged_type(value_type);
        }
    }
}

fn retype(metadata: &mut HydroIrMetadata) {
    retype_kind(&mut metadata.collection_kind);
}

fn fresh_metadata(from: &HydroIrMetadata) -> HydroIrMetadata {
    let mut m = from.clone();
    m.op.id = None;
    m
}

/// `Some(__current_cluster_id)` when code at `loc` runs inside a cluster member, else `None`.
fn member_expr(loc: &LocationId) -> TokenStream {
    if matches!(loc.root(), LocationId::Cluster(_)) {
        quote!(::core::option::Option::Some(__current_cluster_id))
    } else {
        quote!(::core::option::Option::None)
    }
}

fn member_u32_expr(loc: &LocationId) -> TokenStream {
    if matches!(loc.root(), LocationId::Cluster(_)) {
        quote!(__current_cluster_id)
    } else {
        quote!(u32::MAX)
    }
}

// ---------------------------------------------------------------------------------------------
// Closure wrapping
// ---------------------------------------------------------------------------------------------

/// Everything needed to splice a user closure that may read Hydro state through references.
struct RefShadow {
    /// Per-run `let`s (evaluated once per subgraph run, alongside DFIR's own
    /// `let __hydro_singleton_ref_i = #{g} ref;`): bind `__prov_rt_i` / `__prov_rc_i` to the
    /// reference's tags and coarse flag and, for immutable refs, shadow the local with an
    /// untagged view.
    per_run: TokenStream,
    /// Per-item `let`s run before the user closure is constructed: reborrow the shadows so the
    /// user closure can `move` them.
    per_item: TokenStream,
    /// Statements merging the reference tags into `__prov_t` / `__prov_c` (per item).
    absorb: TokenStream,
    /// Statements writing item lineage into mutable references, run *after* the user closure
    /// was called for this item.
    writeback: TokenStream,
    /// Whether the user closure must be constructed per item (needed when it captures a
    /// mutable `Vec`/`Optional` shadow that we must read back after each call).
    per_item_closure: bool,
}

fn ref_shadow(f: &ClosureExpr) -> RefShadow {
    let p = prov();
    let mut per_run = TokenStream::new();
    let mut per_item = TokenStream::new();
    let mut absorb = TokenStream::new();
    let mut writeback = TokenStream::new();
    let mut per_item_closure = false;
    for (i, (node, is_mut)) in f.singleton_refs.iter().enumerate() {
        let HydroNode::Reference { kind, .. } = node else {
            panic!("closure singleton_refs must be Reference nodes");
        };
        let local = handoff_ref_ident(i);
        let rt = quote::format_ident!("__prov_rt_{i}");
        let rc = quote::format_ident!("__prov_rc_{i}");
        let real = quote::format_ident!("__prov_real_{i}");
        let owned = quote::format_ident!("__prov_owned_{i}");
        let seen = quote::format_ident!("__prov_seen_{i}");
        match (kind, is_mut) {
            (HandoffRefKind::Singleton, false) => per_run.extend(quote! {
                let #rt = &#local.tags;
                let #rc = &#local.coarse;
                let #local = &#local.value;
            }),
            (HandoffRefKind::Singleton, true) => {
                per_run.extend(quote! {
                    let #p::Tagged { tags: #rt, coarse: #rc, value: #real } = &mut *#local;
                });
                per_item.extend(quote! {
                    let #local = &mut *#real;
                });
                writeback.extend(quote! {
                    #rt.extend(__prov_t.iter().copied());
                    *#rc = true;
                });
            }
            (HandoffRefKind::Optional, false) => per_run.extend(quote! {
                let #owned = ::core::option::Option::as_ref(#local).map(|__t| ::core::clone::Clone::clone(&__t.value));
                let #rt: #p::TagSet = ::core::option::Option::as_ref(#local).map(|__t| __t.tags.clone()).unwrap_or_default();
                let #rc: bool = ::core::option::Option::as_ref(#local).map(|__t| __t.coarse).unwrap_or(false);
                let #local = &#owned;
            }),
            (HandoffRefKind::Vec, false) => per_run.extend(quote! {
                let #owned: ::std::vec::Vec<_> = #local.iter().map(|__t| ::core::clone::Clone::clone(&__t.value)).collect();
                let #rt: #p::TagSet = #local.iter().flat_map(|__t| __t.tags.iter().copied()).collect();
                let #rc: bool = #local.iter().any(|__t| __t.coarse);
                let #local = &#owned;
            }),
            (HandoffRefKind::Optional, true) => {
                // The user sees an owned `Option<V>`; after each call the real slot is
                // rewritten with the current item's lineage.
                per_item_closure = true;
                per_run.extend(quote! {
                    let #real = #local;
                    let mut #owned: ::core::option::Option<_> = ::core::option::Option::as_ref(&*#real).map(|__t| ::core::clone::Clone::clone(&__t.value));
                    let #rt: #p::TagSet = ::core::option::Option::as_ref(&*#real).map(|__t| __t.tags.clone()).unwrap_or_default();
                    let #rc: bool = true;
                });
                per_item.extend(quote! {
                    let #local = &mut #owned;
                });
                writeback.extend(quote! {
                    *#real = ::core::clone::Clone::clone(&#owned)
                        .map(|__v| #p::Tagged::from_parts(::core::clone::Clone::clone(&__prov_t), true, __v));
                });
            }
            (HandoffRefKind::Vec, true) => {
                // The user sees an owned `Vec<V>` mirroring the real `Vec<Tagged<V>>`; items it
                // appends are tagged with the current item's lineage and pushed to the real
                // vector after each call. (In-place edits of pre-existing items are not
                // reflected; Hydro's stream refs are append-only in practice.)
                per_item_closure = true;
                per_run.extend(quote! {
                    let #real = #local;
                    let mut #owned: ::std::vec::Vec<_> = #real.iter().map(|__t| ::core::clone::Clone::clone(&__t.value)).collect();
                    let mut #seen: usize = #owned.len();
                    let #rt: #p::TagSet = #real.iter().flat_map(|__t| __t.tags.iter().copied()).collect();
                    let #rc: bool = true;
                });
                per_item.extend(quote! {
                    let #local = &mut #owned;
                });
                writeback.extend(quote! {
                    for __v in #owned[#seen..].iter() {
                        #real.push(#p::Tagged::from_parts(::core::clone::Clone::clone(&__prov_t), true, ::core::clone::Clone::clone(__v)));
                    }
                    #seen = #owned.len();
                });
            }
        }
        absorb.extend(quote! {
            __prov_t.extend(#rt.iter().copied());
            __prov_c = true;
            let _ = &#rc;
        });
    }
    RefShadow {
        per_run,
        per_item,
        absorb,
        writeback,
        per_item_closure,
    }
}

/// The pieces of a wrapper closure body, assembled by [`wrap_closure`].
struct Body {
    /// Closure parameter list, e.g. `|__prov_item: T|`.
    params: TokenStream,
    /// Statements run before the user closure is invoked (must bind `__prov_t`, `__prov_c`).
    pre: TokenStream,
    /// The invocation of the user closure, binding whatever `result` needs.
    call: TokenStream,
    /// The wrapper's result expression.
    result: TokenStream,
}

/// Builds a new closure expression around `f`. The user closure is available as `__prov_f`
/// inside `call`. Reference shadows, lineage absorption and write-back are spliced around it.
fn wrap_closure(f: &ClosureExpr, build: impl FnOnce(&RefShadow) -> Body) -> ClosureExpr {
    let shadow = ref_shadow(f);
    let orig = &f.expr.0;
    let per_run = &shadow.per_run;
    let per_item = &shadow.per_item;
    let absorb = &shadow.absorb;
    let writeback = &shadow.writeback;
    let Body {
        params,
        pre,
        call,
        result,
    } = build(&shadow);
    let bind_f = quote! {
        #[allow(unused_mut)]
        let mut __prov_f = #orig;
    };
    let expr: syn::Expr = if shadow.per_item_closure {
        // The user closure captures mutable shadows we must read back after every call, so
        // it is constructed per item (Hydro closures are stateless across items already).
        parse_quote!({
            #per_run
            move |#params| {
                #per_item
                #bind_f
                #pre
                #absorb
                #call
                // The user closure is an opaque `impl FnMut` (via stageleft's type hints), which
                // rustc treats as possibly having drop glue; drop it explicitly so its `&mut`
                // captures of the shadows end before write-back.
                ::core::mem::drop(__prov_f);
                #writeback
                #result
            }
        })
    } else {
        parse_quote!({
            #per_run
            #per_item
            #bind_f
            move |#params| {
                #pre
                #absorb
                #call
                #writeback
                #result
            }
        })
    };
    ClosureExpr::new(DebugExpr::from(expr), f.clone().singleton_refs)
}

fn wrap_map(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type, out_repr: Repr) -> ClosureExpr {
    wrap_closure(f, |_| {
        let untag = untag(in_repr, quote!(__prov_item));
        Body {
            params: quote!(__prov_item: #in_ty),
            pre: quote! {
                #[allow(unused_mut)]
                let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
            },
            call: quote!(let __prov_y = __prov_f(__prov_x);),
            result: retag(out_repr, quote!(__prov_t), quote!(__prov_c), quote!(__prov_y)),
        }
    })
}

fn wrap_flat_map(
    f: &ClosureExpr,
    in_repr: Repr,
    in_ty: &syn::Type,
    out_repr: Repr,
) -> ClosureExpr {
    wrap_closure(f, |_| {
        let untag = untag(in_repr, quote!(__prov_item));
        let retag = retag(
            out_repr,
            quote!(::core::clone::Clone::clone(&__prov_t)),
            quote!(__prov_c),
            quote!(__prov_y),
        );
        Body {
            params: quote!(__prov_item: #in_ty),
            pre: quote! {
                #[allow(unused_mut)]
                let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
            },
            call: quote!(let __prov_out = __prov_f(__prov_x);),
            result: quote! {
                ::core::iter::IntoIterator::into_iter(__prov_out).map(move |__prov_y| #retag)
            },
        }
    })
}

fn wrap_filter_map(
    f: &ClosureExpr,
    in_repr: Repr,
    in_ty: &syn::Type,
    out_repr: Repr,
) -> ClosureExpr {
    wrap_closure(f, |_| {
        let untag = untag(in_repr, quote!(__prov_item));
        let retag = retag(out_repr, quote!(__prov_t), quote!(__prov_c), quote!(__prov_y));
        Body {
            params: quote!(__prov_item: #in_ty),
            pre: quote! {
                #[allow(unused_mut)]
                let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
            },
            call: quote!(let __prov_opt = __prov_f(__prov_x);),
            result: quote!(::core::option::Option::map(__prov_opt, move |__prov_y| #retag)),
        }
    })
}

/// For closures taking `&T` (filter, inspect, partition). The item's own lineage is untouched.
fn wrap_by_ref(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type) -> ClosureExpr {
    let p = prov();
    wrap_closure(f, |_| {
        let (prep, view) = untag_ref(in_repr, quote!(__prov_item));
        Body {
            params: quote!(__prov_item: &#in_ty),
            pre: quote! {
                #[allow(unused_mut, unused_variables)]
                let (mut __prov_t, mut __prov_c) = (#p::TagSet::new(), false);
                #prep
            },
            call: quote!(let __prov_y = __prov_f(#view);),
            result: quote!(__prov_y),
        }
    })
}

fn wrap_init(f: &ClosureExpr) -> ClosureExpr {
    let p = prov();
    wrap_closure(f, |_| Body {
        params: quote!(),
        pre: quote! {
            #[allow(unused_mut, unused_variables)]
            let (mut __prov_t, mut __prov_c) = (#p::TagSet::new(), false);
        },
        call: quote!(let __prov_y = __prov_f();),
        result: quote!(#p::Tagged::pristine(__prov_y)),
    })
}

/// `|acc: &mut Tagged<A>, item|` for fold / fold_keyed / reduce / reduce_keyed.
fn wrap_acc(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type) -> ClosureExpr {
    let p = prov();
    wrap_closure(f, |_| {
        let untag = untag(in_repr, quote!(__prov_item));
        Body {
            params: quote!(__prov_acc: &mut #p::Tagged<_>, __prov_item: #in_ty),
            pre: quote! {
                #[allow(unused_mut)]
                let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
            },
            call: quote! {
                __prov_acc.absorb(&__prov_t, __prov_c);
                let __prov_y = __prov_f(&mut __prov_acc.value, __prov_x);
            },
            result: quote!(__prov_y),
        }
    })
}

/// `|acc: &mut Tagged<A>, item| -> Option<Tagged<U>>` for scan.
fn wrap_scan(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type, out_repr: Repr) -> ClosureExpr {
    let p = prov();
    wrap_closure(f, |_| {
        let untag = untag(in_repr, quote!(__prov_item));
        let retag = retag(
            out_repr,
            quote!(::core::clone::Clone::clone(&__prov_acc.tags)),
            quote!(__prov_acc.coarse),
            quote!(__prov_u),
        );
        Body {
            params: quote!(__prov_acc: &mut #p::Tagged<_>, __prov_item: #in_ty),
            pre: quote! {
                #[allow(unused_mut)]
                let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
            },
            call: quote! {
                __prov_acc.absorb(&__prov_t, __prov_c);
                let __prov_opt = __prov_f(&mut __prov_acc.value, __prov_x);
            },
            result: quote!(::core::option::Option::map(__prov_opt, |__prov_u| #retag)),
        }
    })
}

fn plain_closure(tokens: TokenStream) -> ClosureExpr {
    let expr: syn::Expr = parse_quote!(#tokens);
    ClosureExpr::from(expr)
}

// ---------------------------------------------------------------------------------------------
// Node rewriting
// ---------------------------------------------------------------------------------------------

struct Ctx<'a> {
    operational_ports: &'a BTreeSet<ExternalPortId>,
    next_point: std::cell::Cell<u32>,
}

impl Ctx<'_> {
    fn next_point(&self) -> u32 {
        let p = self.next_point.get();
        self.next_point.set(p + 1);
        p
    }
}

/// Wraps `node` in a `Map` applying `f`; the map inherits `node`'s (already retyped) metadata.
fn wrap_after(node: &mut HydroNode, f: ClosureExpr) {
    let metadata = fresh_metadata(node.metadata());
    let inner = std::mem::replace(node, HydroNode::Placeholder);
    *node = HydroNode::Map {
        f,
        input: Box::new(inner),
        metadata,
    };
}

/// Inserts a `Map` applying `f` in front of `input`.
fn wrap_before(input: &mut Box<HydroNode>, f: ClosureExpr) {
    let metadata = fresh_metadata(input.metadata());
    let inner = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
    **input = HydroNode::Map {
        f,
        input: Box::new(inner),
        metadata,
    };
}

/// Wraps raw items of type `ty` (the untagged type) as pristine.
fn pristine_map(ty: &syn::Type) -> ClosureExpr {
    let p = prov();
    plain_closure(quote!(|__prov_x: #ty| #p::Tagged::pristine(__prov_x)))
}

/// `Tagged<(K, V)>` -> `(K, Tagged<V>)`
fn flat_to_keyed(ty: &syn::Type) -> ClosureExpr {
    let p = prov();
    plain_closure(quote!(|__prov_i: #ty| {
        let (__prov_t, __prov_c, (__prov_k, __prov_v)) = #p::Tagged::into_parts(__prov_i);
        (__prov_k, #p::Tagged::from_parts(__prov_t, __prov_c, __prov_v))
    }))
}

/// `(K, Tagged<V>)` -> `Tagged<(K, V)>`
fn keyed_to_flat(ty: &syn::Type) -> ClosureExpr {
    let p = prov();
    plain_closure(quote!(|(__prov_k, __prov_tv): #ty| {
        let (__prov_t, __prov_c, __prov_v) = #p::Tagged::into_parts(__prov_tv);
        #p::Tagged::from_parts(__prov_t, __prov_c, (__prov_k, __prov_v))
    }))
}

/// Conversion between representations; `ty` is the concrete type of the incoming item.
fn convert(from: Repr, to: Repr, ty: &syn::Type) -> Option<ClosureExpr> {
    match (from, to) {
        (Repr::Flat, Repr::Keyed) => Some(flat_to_keyed(ty)),
        (Repr::Keyed, Repr::Flat) => Some(keyed_to_flat(ty)),
        _ => None,
    }
}

/// Post-map for binary pairing operators producing `(L, R)` raw pairs.
fn pair_post(left: Repr, right: Repr, out: Repr) -> ClosureExpr {
    let p = prov();
    let ul = untag(left, quote!(__prov_l));
    let ur = untag(right, quote!(__prov_r));
    let retag = retag(
        out,
        quote!(#p::union(__prov_t1, &__prov_t2)),
        quote!(__prov_c1 || __prov_c2),
        quote!((__prov_a, __prov_b)),
    );
    plain_closure(quote!(|(__prov_l, __prov_r)| {
        let (__prov_t1, __prov_c1, __prov_a) = #ul;
        let (__prov_t2, __prov_c2, __prov_b) = #ur;
        #retag
    }))
}

/// Post-map for joins producing `(K, (Tagged<V1>, Tagged<V2>))`.
fn join_post(out: Repr) -> ClosureExpr {
    let p = prov();
    let retag = retag(
        out,
        quote!(#p::union(__prov_t1, &__prov_t2)),
        quote!(__prov_c1 || __prov_c2),
        quote!((__prov_k, (__prov_a, __prov_b))),
    );
    plain_closure(quote!(|(__prov_k, (__prov_l, __prov_r))| {
        let (__prov_t1, __prov_c1, __prov_a) = #p::Tagged::into_parts(__prov_l);
        let (__prov_t2, __prov_c2, __prov_b) = #p::Tagged::into_parts(__prov_r);
        #retag
    }))
}

fn kind_of(node: &HydroNode) -> Repr {
    repr_of(&node.metadata().collection_kind)
}

fn ty_of(node: &HydroNode) -> syn::Type {
    item_type(&node.metadata().collection_kind)
}

#[expect(clippy::too_many_arguments, reason = "one field per EmissionRecord member")]
fn emission_record(
    kind: TokenStream,
    point: u32,
    name: &str,
    member: TokenStream,
    destination: &str,
    recipient: TokenStream,
    bytes: TokenStream,
    payload_hash: TokenStream,
) -> TokenStream {
    let p = prov();
    quote!(#p::record(#p::EmissionRecord {
        kind: #p::EmissionPointKind::#kind,
        point: #point,
        name: ::std::string::String::from(#name),
        member: #member,
        destination: ::std::string::String::from(#destination),
        recipient: #recipient,
        tags: ::core::clone::Clone::clone(&__prov_t),
        coarse: __prov_c,
        bytes: #bytes,
        payload_hash: #payload_hash,
    }))
}

fn transform_node(node: &mut HydroNode, ctx: &Ctx<'_>) {
    let p = prov();
    match node {
        HydroNode::Placeholder => panic!("provenance: unexpected placeholder"),

        HydroNode::Source { source, metadata } => {
            if matches!(source, HydroSource::Stream(_) | HydroSource::Interval(_)) {
                panic!(
                    "provenance: wall-clock sources cannot be simulated; thread timers through \
                     `sim_input_operational()` instead"
                );
            }
            let raw_ty = item_type(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, pristine_map(&raw_ty));
        }
        HydroNode::SingletonSource { metadata, .. } => {
            let raw_ty = item_type(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, pristine_map(&raw_ty));
        }

        HydroNode::ExternalInput {
            from_port_id,
            deserialize_fn,
            metadata,
            ..
        } => {
            let Some(deser) = deserialize_fn.as_ref() else {
                panic!("provenance: raw-bytes external inputs are not supported");
            };
            let kind = if ctx.operational_ports.contains(from_port_id) {
                quote!(#p::TagKind::Operational)
            } else {
                quote!(#p::TagKind::Data)
            };
            let port = from_port_id.into_inner() as u32;
            let member = member_u32_expr(&metadata.location_id);
            let out = repr_of(&metadata.collection_kind);
            let retag = retag(
                out,
                quote!({
                    let mut __prov_t = #p::TagSet::new();
                    __prov_t.insert(#p::Tag { kind: #kind, port: #port, member: #member, seq: __prov_s });
                    __prov_t
                }),
                quote!(false),
                quote!(__prov_x),
            );
            let orig = &deser.0;
            let wrapped: syn::Expr = parse_quote!(
                move |__prov_res| {
                    let __prov_x = #p::apply1(#orig, __prov_res);
                    let __prov_s = #p::next_source_seq(#port, #member);
                    #retag
                }
            );
            *deserialize_fn = Some(DebugExpr::from(wrapped));
            retype(metadata);
        }

        // Pass-through nodes whose item shape is unchanged.
        HydroNode::CycleSource { metadata, .. }
        | HydroNode::Tee { metadata, .. }
        | HydroNode::Reference { metadata, .. }
        | HydroNode::PartitionSide { metadata, .. }
        | HydroNode::BeginAtomic { metadata, .. }
        | HydroNode::EndAtomic { metadata, .. }
        | HydroNode::Batch { metadata, .. }
        | HydroNode::YieldConcat { metadata, .. }
        | HydroNode::UnboundSingleton { metadata, .. }
        | HydroNode::AssertIsConsistent { metadata, .. }
        | HydroNode::Chain { metadata, .. }
        | HydroNode::MergeOrdered { metadata, .. }
        | HydroNode::ChainFirst { metadata, .. }
        | HydroNode::DeferTick { metadata, .. }
        | HydroNode::Sort { metadata, .. }
        | HydroNode::Unique { metadata, .. }
        | HydroNode::Difference { metadata, .. }
        | HydroNode::Counter { metadata, .. } => {
            retype(metadata);
        }

        // Pass-through nodes that may change keyedness.
        HydroNode::Cast { inner, metadata }
        | HydroNode::ObserveNonDet {
            inner, metadata, ..
        } => {
            let from = kind_of(inner);
            let from_ty = ty_of(inner);
            let to = repr_of(&metadata.collection_kind);
            retype(metadata);
            if let Some(f) = convert(from, to, &from_ty) {
                wrap_after(node, f);
            }
        }

        HydroNode::PartitionShared { f, input, metadata } => {
            *f = wrap_by_ref(f, kind_of(input), &ty_of(input));
            retype(metadata);
        }

        HydroNode::Map { f, input, metadata } => {
            *f = wrap_map(
                f,
                kind_of(input),
                &ty_of(input),
                repr_of(&metadata.collection_kind),
            );
            retype(metadata);
        }
        HydroNode::FlatMap { f, input, metadata } => {
            *f = wrap_flat_map(
                f,
                kind_of(input),
                &ty_of(input),
                repr_of(&metadata.collection_kind),
            );
            retype(metadata);
        }
        HydroNode::FilterMap { f, input, metadata } => {
            *f = wrap_filter_map(
                f,
                kind_of(input),
                &ty_of(input),
                repr_of(&metadata.collection_kind),
            );
            retype(metadata);
        }
        HydroNode::Filter { f, input, metadata } | HydroNode::Inspect { f, input, metadata } => {
            *f = wrap_by_ref(f, kind_of(input), &ty_of(input));
            retype(metadata);
        }

        HydroNode::Enumerate { input, metadata } => {
            assert_eq!(
                kind_of(input),
                Repr::Flat,
                "provenance: enumerate on keyed input"
            );
            let in_ty = ty_of(input);
            let out = repr_of(&metadata.collection_kind);
            retype(metadata);
            let retag = retag(
                out,
                quote!(__prov_t),
                quote!(__prov_c),
                quote!((__prov_i, __prov_v)),
            );
            wrap_after(
                node,
                plain_closure(quote!(|(__prov_i, __prov_tv): (usize, #in_ty)| {
                    let (__prov_t, __prov_c, __prov_v) = #p::Tagged::into_parts(__prov_tv);
                    #retag
                })),
            );
        }

        HydroNode::Fold {
            init,
            acc,
            input,
            metadata,
        } => {
            *init = wrap_init(init);
            *acc = wrap_acc(acc, kind_of(input), &ty_of(input));
            retype(metadata);
        }
        HydroNode::FoldKeyed {
            init,
            acc,
            input,
            metadata,
        } => {
            assert_eq!(
                kind_of(input),
                Repr::Keyed,
                "provenance: fold_keyed on flat input"
            );
            *init = wrap_init(init);
            // fold_keyed hands the accumulator the *value* (`Tagged<V>`), i.e. a flat item.
            *acc = wrap_acc(
                acc,
                Repr::Flat,
                &keyed_value_type(&input.metadata().collection_kind),
            );
            retype(metadata);
        }
        HydroNode::Reduce { f, input, metadata } => {
            *f = wrap_acc(f, kind_of(input), &ty_of(input));
            retype(metadata);
        }
        HydroNode::ReduceKeyed { f, input, metadata } => {
            assert_eq!(
                kind_of(input),
                Repr::Keyed,
                "provenance: reduce_keyed on flat input"
            );
            *f = wrap_acc(
                f,
                Repr::Flat,
                &keyed_value_type(&input.metadata().collection_kind),
            );
            retype(metadata);
        }
        HydroNode::Scan {
            init,
            acc,
            input,
            metadata,
        } => {
            *init = wrap_init(init);
            *acc = wrap_scan(
                acc,
                kind_of(input),
                &ty_of(input),
                repr_of(&metadata.collection_kind),
            );
            retype(metadata);
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
            if kind_of(left) == Repr::Flat {
                let ty = ty_of(left);
                wrap_before(left, flat_to_keyed(&ty));
            }
            if kind_of(right) == Repr::Flat {
                let ty = ty_of(right);
                wrap_before(right, flat_to_keyed(&ty));
            }
            let out = repr_of(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, join_post(out));
        }

        HydroNode::CrossProduct {
            left,
            right,
            metadata,
        }
        | HydroNode::CrossSingleton {
            left,
            right,
            metadata,
        } => {
            let l = kind_of(left);
            let r = kind_of(right);
            let out = repr_of(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, pair_post(l, r, out));
        }

        HydroNode::AntiJoin { pos, neg, metadata } => {
            if kind_of(pos) == Repr::Flat {
                let ty = ty_of(pos);
                wrap_before(pos, flat_to_keyed(&ty));
            }
            assert_eq!(
                kind_of(neg),
                Repr::Flat,
                "provenance: anti_join with keyed neg"
            );
            let neg_ty = ty_of(neg);
            wrap_before(
                neg,
                plain_closure(quote!(|__prov_i: #neg_ty| #p::Tagged::into_parts(__prov_i).2)),
            );
            let out = repr_of(&metadata.collection_kind);
            retype(metadata);
            // After anti_join the raw item is `(K, Tagged<V>)`; convert if the output is flat.
            // The type is fixed by the operator's inputs, so it can be left inferred.
            let keyed_ty: syn::Type = parse_quote!(_);
            if let Some(f) = convert(Repr::Keyed, out, &keyed_ty) {
                wrap_after(node, f);
            }
        }

        HydroNode::Network {
            name,
            serialize,
            deserialize,
            input,
            metadata,
            ..
        } => {
            let NetworkSend::Custom {
                serialize_fn: Some(ser),
            } = serialize
            else {
                panic!("provenance: only bincode-style network channels are supported");
            };
            let NetworkRecv::Custom {
                deserialize_fn: Some(deser),
            } = deserialize
            else {
                panic!("provenance: only bincode-style network channels are supported");
            };
            let point = ctx.next_point();
            let from = &input.metadata().location_id;
            let label = name.clone().unwrap_or_else(|| {
                format!("{:?} -> {:?}", from.root(), metadata.location_id.root())
            });
            let member = member_expr(from);
            let in_repr = kind_of(input);
            let in_ty = ty_of(input);
            let untag = untag(in_repr, quote!(__prov_item));
            let destination = format!("{:?}", metadata.location_id.root());
            let record = emission_record(
                quote!(Network),
                point,
                &label,
                member,
                &destination,
                quote!(#p::NetworkPayload::recipient(&__prov_out)),
                quote!(#p::NetworkPayload::payload_len(&__prov_out)),
                quote!(#p::NetworkPayload::payload_hash(&__prov_out)),
            );
            let orig_ser = &ser.0;
            let wrapped_ser: syn::Expr = parse_quote!(
                move |__prov_item: #in_ty| {
                    let (__prov_t, __prov_c, __prov_x) = #untag;
                    let __prov_out = #p::apply1(#orig_ser, __prov_x);
                    #record;
                    #p::NetworkPayload::frame(__prov_out, &__prov_t, __prov_c)
                }
            );
            *ser = DebugExpr::from(wrapped_ser);

            let out = repr_of(&metadata.collection_kind);
            let retag = retag(out, quote!(__prov_t), quote!(__prov_c), quote!(__prov_x));
            let orig_deser = &deser.0;
            // On the receive side the demux id (if any) is the *sender*; the recipient is us.
            let sender = if matches!(from.root(), LocationId::Cluster(_)) {
                quote!(#p::NetworkPayload::recipient(&__prov_inner))
            } else {
                quote!(::core::option::Option::None)
            };
            let receiver = member_expr(&metadata.location_id);
            let receipt = quote!(#p::record(#p::EmissionRecord {
                kind: #p::EmissionPointKind::Receive,
                point: #point,
                name: ::std::string::String::from(#label),
                member: #sender,
                destination: ::std::string::String::from(#destination),
                recipient: #receiver,
                tags: ::core::clone::Clone::clone(&__prov_t),
                coarse: __prov_c,
                bytes: #p::NetworkPayload::payload_len(&__prov_inner),
                payload_hash: #p::NetworkPayload::payload_hash(&__prov_inner),
            }));
            let wrapped_deser: syn::Expr = parse_quote!(
                move |__prov_res| {
                    let __prov_raw = ::core::result::Result::unwrap(__prov_res);
                    let (__prov_t, __prov_c, __prov_inner) = #p::NetworkPayload::unframe(__prov_raw);
                    #receipt;
                    let __prov_x = #p::apply1(#orig_deser, ::core::result::Result::<_, ()>::Ok(__prov_inner));
                    #retag
                }
            );
            *deser = DebugExpr::from(wrapped_deser);
            retype(metadata);
        }

        HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::FlatMapStreamBlocking { .. }
        | HydroNode::ScanAsyncBlocking { .. }
        | HydroNode::ReduceKeyedWatermark { .. }
        | HydroNode::VersionedNetworkFork { .. }
        | HydroNode::VersionedNetwork { .. } => {
            panic!("provenance: unsupported node {node:?}");
        }
    }
}

fn transform_root(root: &mut HydroRoot, ctx: &Ctx<'_>) {
    let p = prov();
    match root {
        HydroRoot::ForEach { f, input, .. } => {
            let in_repr = kind_of(input);
            let in_ty = ty_of(input);
            *f = wrap_closure(f, |_| {
                let untag = untag(in_repr, quote!(__prov_item));
                Body {
                    params: quote!(__prov_item: #in_ty),
                    pre: quote! {
                        #[allow(unused_mut)]
                        let (mut __prov_t, mut __prov_c, __prov_x) = #untag;
                    },
                    call: quote!(let __prov_y = __prov_f(__prov_x);),
                    result: quote!(__prov_y),
                }
            });
        }
        HydroRoot::SendExternal {
            to_port_id,
            serialize_fn,
            input,
            ..
        } => {
            let Some(ser) = serialize_fn.as_ref() else {
                panic!("provenance: raw-bytes external outputs are not supported");
            };
            let point = ctx.next_point();
            let label = format!("output {}", to_port_id);
            let member = member_expr(&input.metadata().location_id);
            let in_ty = ty_of(input);
            let untag = untag(kind_of(input), quote!(__prov_item));
            let record = emission_record(
                quote!(Output),
                point,
                &label,
                member,
                &label,
                quote!(::core::option::Option::None),
                quote!(#p::NetworkPayload::payload_len(&__prov_out)),
                quote!(#p::NetworkPayload::payload_hash(&__prov_out)),
            );
            let orig = &ser.0;
            let wrapped: syn::Expr = parse_quote!(
                move |__prov_item: #in_ty| {
                    let (__prov_t, __prov_c, __prov_x) = #untag;
                    let __prov_out = #p::apply1(#orig, __prov_x);
                    #record;
                    __prov_out
                }
            );
            *serialize_fn = Some(DebugExpr::from(wrapped));
        }
        HydroRoot::CycleSink {
            cycle_id, input, ..
        } => {
            let point = ctx.next_point();
            let label = format!("cycle {}", cycle_id);
            let member = member_expr(&input.metadata().location_id);
            let record = emission_record(
                quote!(Cycle),
                point,
                &label,
                member,
                &label,
                quote!(::core::option::Option::None),
                quote!(0usize),
                quote!(0u64),
            );
            let in_ty = ty_of(input);
            let f = match kind_of(input) {
                Repr::Flat => plain_closure(quote!(|__prov_item: &#in_ty| {
                    let __prov_t = &__prov_item.tags;
                    let __prov_c = __prov_item.coarse;
                    #record
                })),
                Repr::Keyed => plain_closure(quote!(|__prov_item: &#in_ty| {
                    let __prov_t = &__prov_item.1.tags;
                    let __prov_c = __prov_item.1.coarse;
                    #record
                })),
            };
            let metadata = fresh_metadata(input.metadata());
            let inner = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
            **input = HydroNode::Inspect {
                f,
                input: Box::new(inner),
                metadata,
            };
        }
        HydroRoot::Null { .. } => {}
        HydroRoot::DestSink { .. } | HydroRoot::EmbeddedOutput { .. } => {
            panic!("provenance: unsupported root {root:?}");
        }
    }
}

/// Rewrites `ir` in place so that every item carries provenance tags. `operational_ports` are
/// the sim input ports declared with `sim_input_operational`.
pub(crate) fn apply_provenance(ir: &mut [HydroRoot], operational_ports: &BTreeSet<ExternalPortId>) {
    let ctx = Ctx {
        operational_ports,
        next_point: std::cell::Cell::new(0),
    };
    let mut seen_tees: SeenSharedNodes = Default::default();
    for root in ir.iter_mut() {
        root.transform_bottom_up(
            &mut |r| transform_root(r, &ctx),
            &mut |n| transform_node(n, &ctx),
            &mut seen_tees,
            false,
        );
    }
}
