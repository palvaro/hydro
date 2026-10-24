//! The record-provenance IR rewrite, run as a `PostNetwork` IR pass.
//!
//! Ported from the simulator's forward-provenance pass (`hydro_lang::sim::provenance_ir`), with
//! source tags replaced by delivery ids and the lineage handed to the network hook as message
//! metadata instead of a side frame.
//!
//! The pass runs bottom-up over the IR and rewrites every node so that the items it produces are
//! wrapped in `hydro_lang::sim::provenance::Tagged`. Two representations are used, chosen by the
//! node's `CollectionKind` so that the simulator's keyed hooks (which destructure `(k, v)` pairs)
//! keep working:
//!
//! - streams, singletons, optionals: `Tagged<T>`;
//! - keyed streams and keyed singletons: `(K, Tagged<V>)`.
//!
//! User closures are wrapped so they see exactly the values they were written for; the wrapper
//! moves tags around them. Structural operators (`join`, `cross`, `anti_join`, `enumerate`) get
//! pre/post `Map`s that reshape the item so the DFIR operator still sees the key or pair it
//! expects. Everything else passes through unchanged because `Tagged`'s `Eq`/`Hash`/`Ord`
//! delegate to the value.
//!
//! At a `Network` send the record's lineage becomes the send's metadata
//! (`network_hook::rt::set_send_meta`); at the receive, the record's lineage is reset to the
//! delivery's own tag (`network_hook::rt::recv_tag`). External outputs carry their lineage as
//! metadata the same way.
//!
//! # Opaque islands
//!
//! Operators the pass cannot see into (`ResolveFutures*`, `VersionedNetwork*`, …) are left
//! uninstrumented: their inputs are stripped of lineage and everything they emit is
//! [`RecordProv::Opaque`](super::RecordProv::Opaque). The same applies to a node whose item
//! shape the pass cannot follow: a keyed-to-flat `Cast`/`ObserveNonDet` (for example
//! `entries_partially_ordered`) whose simulator hook is typed by the node's own collection kind
//! is instrumented by converting its *output*, never by changing the type the hook sees.
//!
//! The pass never adds or removes a nondeterministic decision point, so a schedule replays
//! identically with and without it.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

use hydro_lang::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, DebugType, HydroIrMetadata, HydroNode, HydroRoot,
    NetworkRecv, NetworkSend, SeenSharedNodes,
};
use hydro_lang::handoff_ref::{HandoffRefKind, handoff_ref_ident};
use hydro_lang::location::dynamic::LocationId;

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

/// Path of `hydro_lang` as seen from the simulation dylib.
///
/// Generated code names only `hydro_lang` and `std`: a simulation dylib sees the crate under test
/// and its normal dependencies, so it cannot name this crate when it is a dev-dependency (the
/// usual case for a test harness).
fn hydro_lang_crate() -> TokenStream {
    if let Ok(proc_macro_crate::FoundCrate::Name(name)) = proc_macro_crate::crate_name("hydro_lang")
    {
        let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
        quote!(#ident)
    } else {
        quote!(hydro_lang)
    }
}

/// The runtime representation: `hydro_lang`'s provenance types (`Tagged<T>` with a set of
/// `Tag`s). A delivery is the tag [`delivery_tag`]; [`opaque_tag`] marks unknown lineage.
pub(crate) fn prov() -> TokenStream {
    let root = hydro_lang_crate();
    quote!(#root::sim::provenance)
}

/// Reserved `Tag::port` for the tags this pass uses (no simulator input port has this id).
const PORT: u32 = u32::MAX;

/// The tag naming delivery `#id` (a `u64` expression).
fn delivery_tag(id: TokenStream) -> TokenStream {
    let p = prov();
    quote!(#p::Tag { kind: #p::TagKind::Data, port: #PORT, member: #PORT, seq: #id })
}

/// The tag marking lineage the pass cannot see.
fn opaque_tag() -> TokenStream {
    let p = prov();
    quote!(#p::Tag { kind: #p::TagKind::Operational, port: #PORT, member: #PORT, seq: u64::MAX })
}

/// The tag naming fragile site `index` (docs/design/hydrofrac-races.md §4.1). It reaches message
/// metadata as the id `FRAGILE_BIT | index` ([`super::FRAGILE_BIT`]); its `member` keeps it
/// distinct from every delivery tag and from [`opaque_tag`].
fn fragile_tag(index: usize) -> TokenStream {
    let p = prov();
    let seq = super::FRAGILE_BIT | u64::try_from(index).expect("site index fits u64");
    let member = PORT - 1;
    quote!(#p::Tag { kind: #p::TagKind::Operational, port: #PORT, member: #member, seq: #seq })
}

/// A `Map` closure adding the tag of fragile site `index` to every item of representation `repr`
/// and (tagged) type `ty`.
fn add_fragile_tag(index: usize, repr: Repr, ty: &syn::Type) -> ClosureExpr {
    let tag = fragile_tag(index);
    match repr {
        Repr::Flat => plain_closure(quote!(|mut __prov_x: #ty| {
            __prov_x.tags.insert(#tag);
            __prov_x
        })),
        Repr::Keyed => plain_closure(quote!(|(__prov_k, mut __prov_v): #ty| {
            __prov_v.tags.insert(#tag);
            (__prov_k, __prov_v)
        })),
    }
}

/// An item with opaque lineage.
fn opaque_item(value: TokenStream) -> TokenStream {
    let p = prov();
    let tag = opaque_tag();
    quote!(#p::Tagged::source(#tag, #value))
}

/// The message metadata for lineage `#tags` (a `&TagSet` expression): the bincode encoding of
/// [`RecordProv`](super::RecordProv), built without naming it (`(0u32, ids)` is how bincode
/// encodes `RecordProv::Ids(ids)`, `1u32` how it encodes `RecordProv::Opaque`).
fn meta_expr(tags: TokenStream) -> TokenStream {
    let root = hydro_lang_crate();
    let opaque = opaque_tag();
    quote!({
        let __prov_tags = #tags;
        if __prov_tags.contains(&#opaque) {
            #root::runtime_support::bincode::serialize(&1u32).unwrap()
        } else {
            let __prov_ids: ::std::collections::BTreeSet<u64> =
                __prov_tags.iter().map(|__t| __t.seq).collect();
            #root::runtime_support::bincode::serialize(&(0u32, __prov_ids)).unwrap()
        }
    })
}

/// `hydro_lang`'s network-hook runtime, as seen from the simulation dylib.
fn net() -> TokenStream {
    let root = hydro_lang_crate();
    quote!(#root::sim::network_hook::rt)
}

/// An `Option<String>` expression: the `Debug` rendering of `value` (a reference) if its type
/// implements `Debug`, by autoref specialization on the concrete type (so any type compiles).
/// Made in the simulation, where the type is known: the host only sees bytes.
pub(crate) fn debug_of(value: TokenStream) -> TokenStream {
    quote!({
        struct __ProvWrap<'__a, __T>(&'__a __T);
        trait __ProvViaDebug {
            fn __prov_debug(&self) -> ::core::option::Option<::std::string::String>;
        }
        impl<__T: ::core::fmt::Debug> __ProvViaDebug for __ProvWrap<'_, __T> {
            fn __prov_debug(&self) -> ::core::option::Option<::std::string::String> {
                ::core::option::Option::Some(::std::format!("{:?}", self.0))
            }
        }
        trait __ProvNoDebug {
            fn __prov_debug(&self) -> ::core::option::Option<::std::string::String>;
        }
        impl<__T> __ProvNoDebug for &__ProvWrap<'_, __T> {
            fn __prov_debug(&self) -> ::core::option::Option<::std::string::String> {
                ::core::option::Option::None
            }
        }
        (&__ProvWrap(#value)).__prov_debug()
    })
}

/// Calls the closure `f` on `x` so that rustc infers the closure's parameter type from `x`.
fn apply1(f: TokenStream, x: TokenStream) -> TokenStream {
    let root = hydro_lang_crate();
    quote!(#root::sim::provenance::apply1(#f, #x))
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
    for (i, (node, is_mut)) in f.singleton_refs().iter().enumerate() {
        let HydroNode::Reference { kind, .. } = node else {
            panic!("closure singleton_refs must be Reference nodes");
        };
        let local = handoff_ref_ident(i);
        let rt = quote::format_ident!("__prov_rt_{i}");
        let rc = quote::format_ident!("__prov_rc_{i}");
        let real = quote::format_ident!("__prov_real_{i}");
        let owned = quote::format_ident!("__prov_owned_{i}");
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
                });
                // Read per item, so an item sees what earlier items of the same run wrote.
                per_item.extend(quote! {
                    let #rt: #p::TagSet = ::core::option::Option::as_ref(&*#real).map(|__t| __t.tags.clone()).unwrap_or_default();
                    let #rc: bool = true;
                    let #local = &mut #owned;
                });
                writeback.extend(quote! {
                    *#real = ::core::clone::Clone::clone(&#owned)
                        .map(|__v| #p::Tagged::from_parts(::core::clone::Clone::clone(&__prov_t), true, __v));
                });
            }
            (HandoffRefKind::Vec, true) => {
                // The user sees an owned `Vec<V>` mirroring the real `Vec<Tagged<V>>`. The
                // state's lineage is read per item, so an item sees what earlier items of the
                // same run wrote.
                per_item_closure = true;
                per_run.extend(quote! {
                    let #real = #local;
                    let mut #owned: ::std::vec::Vec<_> = #real.iter().map(|__t| ::core::clone::Clone::clone(&__t.value)).collect();
                });
                per_item.extend(quote! {
                    let #rt: #p::TagSet = #real.iter().flat_map(|__t| __t.tags.iter().copied()).collect();
                    let #rc: bool = true;
                    let #local = &mut #owned;
                });
                // The user's view is written back after every call: items removed from it are
                // removed from the real vector, pre-existing items (which may have been edited
                // in place, which the pass cannot see) take the view's value and absorb the
                // current item's lineage, and appended items are pushed tagged with it. An
                // over-approximation of lineage, never a loss.
                writeback.extend(quote! {
                    #real.truncate(#owned.len());
                    for (__t, __v) in #real.iter_mut().zip(#owned.iter()) {
                        __t.absorb(&__prov_t, true);
                        __t.value = ::core::clone::Clone::clone(__v);
                    }
                    let __prov_have = #real.len();
                    for __v in #owned[__prov_have..].iter() {
                        #real.push(#p::Tagged::from_parts(::core::clone::Clone::clone(&__prov_t), true, ::core::clone::Clone::clone(__v)));
                    }
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
    let (orig, refs) = f.clone().into_parts();
    let orig = &orig.0;
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
    ClosureExpr::new(DebugExpr::from(expr), refs)
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
            result: retag(
                out_repr,
                quote!(__prov_t),
                quote!(__prov_c),
                quote!(__prov_y),
            ),
        }
    })
}

fn wrap_flat_map(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type, out_repr: Repr) -> ClosureExpr {
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
        let retag = retag(
            out_repr,
            quote!(__prov_t),
            quote!(__prov_c),
            quote!(__prov_y),
        );
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

/// For closures taking `&T` (filter, inspect, partition). The item's own lineage is untouched,
/// but it is made available as `__prov_t` so that a closure writing state through `by_mut`
/// (the `inspect`-as-side-effect idiom) carries the item's lineage into that state.
fn wrap_by_ref(f: &ClosureExpr, in_repr: Repr, in_ty: &syn::Type) -> ClosureExpr {
    wrap_closure(f, |_| {
        let (prep, view) = untag_ref(in_repr, quote!(__prov_item));
        let item_tags = match in_repr {
            Repr::Flat => quote!((
                ::core::clone::Clone::clone(&__prov_item.tags),
                __prov_item.coarse
            )),
            Repr::Keyed => quote!((
                ::core::clone::Clone::clone(&__prov_item.1.tags),
                __prov_item.1.coarse
            )),
        };
        Body {
            params: quote!(__prov_item: &#in_ty),
            pre: quote! {
                #[allow(unused_mut, unused_variables)]
                let (mut __prov_t, mut __prov_c) = #item_tags;
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

/// An identity for an IR node that survives the rewrite moving it: the heap address of its
/// collection kind's type. Every node owns that allocation, a node is moved but never copied
/// before the rewrite visits it, and a copy of its metadata (a `Map` the rewrite inserts)
/// allocates a new one. Valid only until the rewrite has visited the node.
pub(crate) fn node_key(node: &HydroNode) -> usize {
    let ty = match &node.metadata().collection_kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => element_type,
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => value_type,
    };
    std::ptr::from_ref::<syn::Type>(&ty.0) as usize
}

/// What the pass learned, for reporting, and what it was told.
#[derive(Default)]
struct Ctx {
    /// Operators left as opaque islands, by debug name.
    islands: std::cell::RefCell<Vec<String>>,
    /// The fragile steps to tag, by their key before the rewrite ([`node_key`]) to site
    /// index. Empty unless the rewrite runs in fragile mode.
    fragile: std::collections::HashMap<usize, usize>,
    /// The trusted `quorum` mints ([`super::quorum`]). Empty unless the rewrite runs in fragile
    /// mode.
    quorum: super::quorum::Mints,
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

fn transform_node(node: &mut HydroNode, ctx: &Ctx) {
    // Looked up before the node is rewritten, which invalidates its key.
    let fragile = if ctx.fragile.is_empty() || matches!(node, HydroNode::Placeholder) {
        None
    } else {
        ctx.fragile.get(&node_key(node)).copied()
    };
    if !quorum_node(node, ctx) {
        transform_node_inner(node, ctx);
    }
    // After a fragile step, every record it emits names its site.
    if let Some(index) = fragile {
        wrap_after(node, add_fragile_tag(index, kind_of(node), &ty_of(node)));
    }
}

/// Rewrites the fold and the mint closure of a trusted `quorum` ([`super::quorum`]), keeping
/// each attestation's lineage and giving each `Durable(f)` the lineage of `f`'s attestations
/// only. Whether `node` was one of them.
fn quorum_node(node: &mut HydroNode, ctx: &Ctx) -> bool {
    if ctx.quorum.mints.is_empty() || matches!(node, HydroNode::Placeholder) {
        return false;
    }
    let key = node_key(node);
    let p = prov();
    match node {
        HydroNode::Fold {
            init,
            acc,
            metadata,
            ..
        } => {
            let Some(item) = ctx.quorum.folds.get(&key) else {
                return false;
            };
            // Each attestation is kept with its own lineage; the fold itself adds none.
            *init = plain_closure(quote!(
                || #p::Tagged::pristine(::std::vec::Vec::<#p::Tagged<#item>>::new())
            ));
            *acc = plain_closure(quote!(
                |__prov_acc: &mut #p::Tagged<::std::vec::Vec<#p::Tagged<#item>>>,
                 __prov_item: #p::Tagged<#item>| {
                    __prov_acc.value.push(__prov_item);
                }
            ));
            retype(metadata);
            true
        }
        HydroNode::FlatMap { f, metadata, .. } => {
            let Some((item, _)) = ctx.quorum.mints.get(&key) else {
                return false;
            };
            let (r0, r1, r2) = (
                handoff_ref_ident(0),
                handoff_ref_ident(1),
                handoff_ref_ident(2),
            );
            let (orig, refs) = f.clone().into_parts();
            let orig = &orig.0;
            let expr: syn::Expr = parse_quote!({
                let __prov_q0 = &*#r0;
                let __prov_q1 = &mut *#r1;
                let __prov_q2 = &mut *#r2;
                move |__prov_item: #p::Tagged<()>| {
                    let __prov_x = __prov_item.value;
                    let (__prov_att, __prov_lin) = &mut __prov_q2.value;
                    // The lineage of each unfired fact's attestations, persisted with `pending`.
                    for __prov_a in __prov_q0.value.iter() {
                        if !__prov_q1.value.contains(&__prov_a.value.0) {
                            __prov_lin
                                .entry(::core::clone::Clone::clone(&__prov_a.value.0))
                                .or_default()
                                .extend(__prov_a.tags.iter().copied());
                        }
                    }
                    let __prov_view: ::std::vec::Vec<#item> = __prov_q0
                        .value
                        .iter()
                        .map(|__prov_a| ::core::clone::Clone::clone(&__prov_a.value))
                        .collect();
                    // `quorum`'s own closure, on untagged views of its state.
                    let __prov_out: ::std::vec::Vec<_> = {
                        let #r0 = &__prov_view;
                        let #r1 = &mut __prov_q1.value;
                        let #r2 = &mut *__prov_att;
                        #[allow(unused_mut)]
                        let mut __prov_f = #orig;
                        ::core::iter::IntoIterator::into_iter(__prov_f(__prov_x)).collect()
                    };
                    __prov_out
                        .into_iter()
                        .map(|__prov_d| {
                            let __prov_t = __prov_lin.remove(__prov_d.fact()).unwrap_or_default();
                            #p::Tagged::from_parts(__prov_t, false, __prov_d)
                        })
                        .collect::<::std::vec::Vec<_>>()
                }
            });
            *f = ClosureExpr::new(DebugExpr::from(expr), refs);
            retype(metadata);
            true
        }
        _ => false,
    }
}

fn transform_node_inner(node: &mut HydroNode, ctx: &Ctx) {
    let p = prov();
    match node {
        HydroNode::Placeholder => panic!("provenance: unexpected placeholder"),

        HydroNode::Source { metadata, .. } => {
            let raw_ty = item_type(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, pristine_map(&raw_ty));
        }
        HydroNode::SingletonSource { metadata, .. } => {
            let raw_ty = item_type(&metadata.collection_kind);
            retype(metadata);
            wrap_after(node, pristine_map(&raw_ty));
        }

        // External inputs are not network deliveries: no fault can remove them, so they carry
        // no lineage.
        HydroNode::ExternalInput {
            deserialize_fn,
            metadata,
            ..
        } => {
            let Some(deser) = deserialize_fn.as_ref() else {
                panic!("provenance: raw-bytes external inputs are not supported");
            };
            let orig = &deser.0;
            let call = apply1(quote!(#orig), quote!(__prov_res));
            let wrapped: syn::Expr = parse_quote!(
                move |__prov_res| #p::Tagged::pristine(#call)
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
            match (from, to) {
                (Repr::Keyed, Repr::Flat) => {
                    // e.g. `entries_partially_ordered`. The node itself still emits keyed-shaped
                    // `(K, Tagged<V>)` items (its simulator hook orders the input's entries), so
                    // its own metadata says so, and a `Map` after it converts to
                    // `Tagged<(K, V)>` under the retyped flat kind.
                    let converted = metadata.clone();
                    if let CollectionKind::Stream { element_type, .. } =
                        &mut metadata.collection_kind
                    {
                        *element_type = DebugType(Box::new(from_ty.clone()));
                    }
                    wrap_after_as(node, keyed_to_flat(&from_ty), converted);
                }
                (Repr::Flat, Repr::Keyed) => wrap_after(node, flat_to_keyed(&from_ty)),
                _ => {}
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
            let net = net();
            let meta = meta_expr(quote!(&__prov_t));
            let in_repr = kind_of(input);
            let in_ty = ty_of(input);
            let untag = untag(in_repr, quote!(__prov_item));
            let orig_ser = &ser.0;
            let call_ser = apply1(quote!(#orig_ser), quote!(__prov_x));
            // A send to a cluster is a demux: only the payload of `(destination, payload)` goes
            // on the wire.
            let payload = if matches!(metadata.location_id.root(), LocationId::Cluster(_)) {
                debug_of(quote!(&__prov_x.1))
            } else {
                debug_of(quote!(&__prov_x))
            };
            let root = hydro_lang_crate();
            // The record's lineage becomes the metadata of the send that immediately follows,
            // followed by the payload's `Debug` rendering for reports (`super::split_meta`).
            let wrapped_ser: syn::Expr = parse_quote!(
                move |__prov_item: #in_ty| {
                    let (__prov_t, __prov_c, __prov_x) = #untag;
                    let _ = __prov_c;
                    let mut __prov_meta: ::std::vec::Vec<u8> = #meta;
                    if let ::core::option::Option::Some(__prov_debug) = #payload {
                        __prov_meta.extend(
                            #root::runtime_support::bincode::serialize(&__prov_debug).unwrap(),
                        );
                    }
                    #net::set_send_meta(__prov_meta);
                    #call_ser
                }
            );
            *ser = DebugExpr::from(wrapped_ser);

            // A received record depends on exactly its delivery.
            let out = repr_of(&metadata.collection_kind);
            let retag = retag(out, quote!(__prov_t), quote!(__prov_c), quote!(__prov_x));
            let orig_deser = &deser.0;
            let call_deser = apply1(quote!(#orig_deser), quote!(__prov_res));
            // A receive the hook did not tag (an uninstrumented channel) has unknown lineage.
            let delivered = delivery_tag(quote!(__prov_id));
            let opaque = opaque_tag();
            let wrapped_deser: syn::Expr = parse_quote!(
                move |__prov_res| {
                    let __prov_tag = match #net::recv_tag() {
                        ::core::option::Option::Some(__prov_id) => #delivered,
                        ::core::option::Option::None => #opaque,
                    };
                    let __prov_x = #call_deser;
                    let (__prov_t, __prov_c, __prov_x) =
                        #p::Tagged::into_parts(#p::Tagged::source(__prov_tag, __prov_x));
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
            opaque_island(node, ctx);
        }
    }
}

/// Like [`wrap_after`], but the `Map` gets `metadata` (the node keeps its own).
fn wrap_after_as(node: &mut HydroNode, f: ClosureExpr, mut metadata: HydroIrMetadata) {
    metadata.op.id = None;
    let inner = std::mem::replace(node, HydroNode::Placeholder);
    *node = HydroNode::Map {
        f,
        input: Box::new(inner),
        metadata,
    };
}

/// Strips lineage from the items of `input` (already rewritten), giving the untagged items the
/// node was written for.
fn strip_before(input: &mut Box<HydroNode>, untagged: CollectionKind) {
    let p = prov();
    let repr = kind_of(input);
    let ty = ty_of(input);
    let f = match repr {
        Repr::Flat => plain_closure(quote!(|__prov_i: #ty| #p::Tagged::into_parts(__prov_i).2)),
        Repr::Keyed => plain_closure(quote!(|(__prov_k, __prov_tv): #ty| {
            (__prov_k, #p::Tagged::into_parts(__prov_tv).2)
        })),
    };
    let mut metadata = fresh_metadata(input.metadata());
    metadata.collection_kind = untagged;
    let inner = std::mem::replace(input.as_mut(), HydroNode::Placeholder);
    **input = HydroNode::Map {
        f,
        input: Box::new(inner),
        metadata,
    };
}

/// Leaves `node` uninstrumented: its inputs are stripped of lineage and everything it emits is
/// opaque (see the module docs).
///
/// Children are rewritten before their parent, so each input already carries `Tagged` items;
/// the node's own metadata still describes the untagged items it was written for. Its closures
/// must not read Hydro state through references, since that state is tagged.
fn opaque_island(node: &mut HydroNode, ctx: &Ctx) {
    let name = node_name(node);
    ctx.islands.borrow_mut().push(name.clone());
    let refs = |f: &ClosureExpr| {
        assert!(
            f.singleton_refs().is_empty(),
            "provenance: {name} reads Hydro state through a reference and cannot be left \
             uninstrumented",
        );
    };
    // Untagged kinds of the inputs, recovered before they are rewritten below.
    let untagged = |input: &HydroNode| {
        let mut kind = input.metadata().collection_kind.clone();
        untype_kind(&mut kind);
        kind
    };
    match node {
        HydroNode::ResolveFutures { input, .. }
        | HydroNode::ResolveFuturesBlocking { input, .. }
        | HydroNode::ResolveFuturesOrdered { input, .. } => {
            let k = untagged(input);
            strip_before(input, k);
        }
        // A multi-version channel: its senders are stripped at the fork, which only feeds the
        // channel's receive ends (`VersionedNetwork`), and those emit opaque records.
        HydroNode::VersionedNetworkFork { senders, .. } => {
            for (_, input, _) in senders.iter_mut() {
                let k = untagged(input);
                strip_before(input, k);
            }
            return;
        }
        HydroNode::VersionedNetwork { .. } => {}
        HydroNode::FlatMapStreamBlocking { f, input, .. } => {
            refs(f);
            let k = untagged(input);
            strip_before(input, k);
        }
        HydroNode::ScanAsyncBlocking {
            init, acc, input, ..
        } => {
            refs(init);
            refs(acc);
            let k = untagged(input);
            strip_before(input, k);
        }
        HydroNode::ReduceKeyedWatermark {
            f,
            input,
            watermark,
            ..
        } => {
            refs(f);
            let k = untagged(input);
            strip_before(input, k);
            let k = untagged(watermark);
            strip_before(watermark, k);
        }
        other => panic!(
            "provenance: {} is not an opaque-island operator",
            node_name(other)
        ),
    }
    let raw_ty = ty_of(node);
    let mut tagged = node.metadata().clone();
    retype(&mut tagged);
    let f = match kind_of(node) {
        Repr::Flat => {
            let item = opaque_item(quote!(__prov_x));
            plain_closure(quote!(|__prov_x: #raw_ty| #item))
        }
        Repr::Keyed => {
            let item = opaque_item(quote!(__prov_v));
            plain_closure(quote!(|(__prov_k, __prov_v): #raw_ty| (__prov_k, #item)))
        }
    };
    wrap_after_as(node, f, tagged);
}

/// Strips one level of `Tagged` from a retyped collection kind (the inverse of [`retype_kind`]).
fn untype_kind(kind: &mut CollectionKind) {
    fn untag_ty(ty: &mut DebugType) {
        if let syn::Type::Path(path) = ty.0.as_ref()
            && let Some(last) = path.path.segments.last()
            && last.ident == "Tagged"
            && let syn::PathArguments::AngleBracketed(args) = &last.arguments
            && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
        {
            *ty = DebugType(Box::new(inner.clone()));
        }
    }
    match kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => untag_ty(element_type),
        CollectionKind::KeyedStream { value_type, .. }
        | CollectionKind::KeyedSingleton { value_type, .. } => untag_ty(value_type),
    }
}

/// A short name for an IR node, for messages and the island list.
fn node_name(node: &HydroNode) -> String {
    let debug = format!("{node:?}");
    debug
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or("?")
        .to_owned()
}

fn transform_root(root: &mut HydroRoot) {
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
        // An external output carries the record's lineage to the hook's `on_output`.
        HydroRoot::SendExternal {
            serialize_fn,
            input,
            ..
        } => {
            let Some(ser) = serialize_fn.as_ref() else {
                panic!("provenance: raw-bytes external outputs are not supported");
            };
            let net = net();
            let meta = meta_expr(quote!(&__prov_t));
            let in_ty = ty_of(input);
            let untag = untag(kind_of(input), quote!(__prov_item));
            let orig = &ser.0;
            let call = apply1(quote!(#orig), quote!(__prov_x));
            let wrapped: syn::Expr = parse_quote!(
                move |__prov_item: #in_ty| {
                    let (__prov_t, __prov_c, __prov_x) = #untag;
                    let _ = __prov_c;
                    #net::set_send_meta(#meta);
                    #call
                }
            );
            *serialize_fn = Some(DebugExpr::from(wrapped));
        }
        // A cycle carries tagged items back to its source unchanged.
        HydroRoot::CycleSink { .. } | HydroRoot::Null { .. } => {}
        HydroRoot::DestSink { .. } | HydroRoot::EmbeddedOutput { .. } => {
            panic!("provenance: unsupported root {root:?}");
        }
    }
}

/// Rewrites `ir` in place so that every record carries its record provenance. Returns the
/// operators left as opaque islands (empty when the whole program is tracked precisely).
///
/// Must run after networks are compiled ([`IrPassStage::PostNetwork`]), on a flow with network
/// hook points: lineage enters at each receive through the delivery's wire tag and leaves at
/// each send and external output as message metadata.
///
/// [`IrPassStage::PostNetwork`]: hydro_lang::sim::network_hook::IrPassStage::PostNetwork
pub fn rewrite(ir: &mut [HydroRoot]) -> Vec<String> {
    rewrite_with(ir, Ctx::default())
}

fn rewrite_with(ir: &mut [HydroRoot], ctx: Ctx) -> Vec<String> {
    let mut seen_tees: SeenSharedNodes = Default::default();
    for root in ir.iter_mut() {
        root.transform_bottom_up(
            &mut transform_root,
            &mut |n| transform_node(n, &ctx),
            &mut seen_tees,
            false,
        );
    }
    ctx.islands.into_inner()
}

/// Installs the rewrite on `flow`: turns on network hook points and registers [`rewrite`] as a
/// `PostNetwork` IR pass. The islands it leaves are appended to `islands`, if given.
pub fn install<'a>(
    flow: hydro_lang::sim::flow::SimFlow<'a>,
    islands: Option<std::rc::Rc<std::cell::RefCell<Vec<String>>>>,
) -> hydro_lang::sim::flow::SimFlow<'a> {
    flow.with_network_hook_points().with_ir_pass(
        hydro_lang::sim::network_hook::IrPassStage::PostNetwork,
        move |ir| {
            let found = rewrite(ir);
            if let Some(islands) = islands {
                islands.borrow_mut().extend(found);
            }
        },
    )
}

/// Like [`install`], in *fragile mode* (docs/design/hydrofrac-races.md §4.1): before the rewrite,
/// the program's fragile steps are found (`fragile::classify`) and stored in `sites`; then
/// every record a fragile step emits also carries that step's site, which reaches message
/// metadata as the id `FRAGILE_BIT | index` ([`super::FRAGILE_BIT`]). Lineage collects them
/// ([`crate::lineage::Lineage::fragile_sites`]); lite's lineage skips them as unknown ids.
#[cfg(stageleft_runtime)]
pub fn install_fragile<'a>(
    flow: hydro_lang::sim::flow::SimFlow<'a>,
    islands: Option<std::rc::Rc<std::cell::RefCell<Vec<String>>>>,
    sites: std::rc::Rc<std::cell::RefCell<Vec<super::FragileSite>>>,
) -> hydro_lang::sim::flow::SimFlow<'a> {
    flow.with_network_hook_points().with_ir_pass(
        hydro_lang::sim::network_hook::IrPassStage::PostNetwork,
        move |ir| {
            let quorum = super::quorum::prepare(ir);
            let (found, fragile) = super::fragile::classify(ir, &quorum.inside);
            *sites.borrow_mut() = found;
            let ctx = Ctx {
                fragile,
                quorum,
                ..Ctx::default()
            };
            let found = rewrite_with(ir, ctx);
            if let Some(islands) = islands {
                islands.borrow_mut().extend(found);
            }
        },
    )
}
