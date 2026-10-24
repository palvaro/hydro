//! Trusted `quorum` (docs/design/hydrofrac-races.md §8.2).
//!
//! `hydro_std::ec_inference_demos::quorum::quorum` (`hydro_std/src/ec_inference_demos/quorum.rs`,
//! lines 121-185) mints `Durable(f)` once the number of *distinct* attestors of `f` reaches a
//! threshold. Its inside is made of fragile steps (§3.1): a batch of the attestations (:136), a
//! fold of the batch into a `Vec` inside the tick (:144-150), and a closure over `by_mut` state
//! (:158-171). And the rewrite gives that closure's output the lineage of everything its state
//! ever absorbed. Yet the set of facts it mints over a run is a function of the set of
//! attestations it receives, whatever the batching:
//! - `acks.unique()` (:133): one attestation per `(fact, attestor)`, ever;
//! - `pending` (:142) persists the attestors of each unfired fact across ticks, and only ever
//!   grows them: an attestation is never forgotten before its fact fires;
//! - `fired` (:143) makes each fact fire at most once, and once it has fired, later attestations
//!   change nothing;
//! - the test `attestors.len() >= threshold` (:162) is monotone in the attestor set.
//!
//! So if a run delivers the attestations of `f` from `threshold` distinct attestors, the mint
//! emits `Durable(f)` in that run: every batch is released by quiescence, each attestation
//! reaches `pending` (or finds `f` already fired, in which case `Durable(f)` was emitted), and
//! the count reaches the threshold. That is Lemma R's step for a set-preserving operator (§5),
//! with the lineage of `Durable(f)` taken to be the deliveries of `f`'s own attestations.
//!
//! **The trust boundary.** The certificate trusts this function's code as it trusts `unique`'s:
//! the argument above is about `quorum.rs:121-185`, read and audited, not something the checker
//! derives. To keep the trust that narrow, only the exact compiled form of that function is
//! recognised ([`prepare`]): its closures matched by their text as quoted in `hydro_std`'s
//! `quorum.rs`, its state cells by their shape. A hand-written copy of the same logic is quoted in
//! another file and stays fragile.
//!
//! **How the lineage is made per fact.** The rewrite cannot keep per-item lineage inside the
//! fold or the closure's state, so for a recognised mint:
//! - the fold keeps each attestation tagged (`Vec<Tagged<(F, M)>>` instead of one tag set);
//! - the `pending` state cell carries, next to the attestor sets, the lineage of each unfired
//!   fact's attestations so far (`(HashMap<F, HashSet<M>>, HashMap<F, TagSet>)`), persisted across
//!   ticks like the attestors themselves;
//! - the closure runs as written, on untagged views of its state, and each `Durable(f)` it emits
//!   gets the lineage of `f`'s attestations: every attestation of `f` the mint received while `f`
//!   was unfired, up to the tick it fired (so the first `threshold` attestors, and possibly more
//!   that arrived in the same tick). Nothing else: not the other facts' attestations, and no
//!   fragile tag of the mint's own steps (they are left out of the fragile sites).
//!
//! Upstream fragile steps still reach `Durable(f)`: they are in the attestations' own lineage.

use std::collections::{HashMap, HashSet};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::{CollectionKind, DebugExpr, HydroNode, HydroRoot};

use super::rewrite::node_key;

/// The quoted file of the mint, as stageleft names its quote macros.
const FILE: &str = "hydro_std::__stageleft_quote_src_ec_inference_demos_quorum_rs_";

/// The mint closure (quorum.rs:158-171), without whitespace, after its captures.
const MINT: &str = "[move|_|{letmutnewly=Vec::new();for(fact,attestor)inack_vec_ref__free.iter()\
    .cloned(){iffired_ref__free.contains(&fact){continue;}letattestors=pending_ref__free.entry(\
    fact.clone()).or_insert_with(HashSet::new);attestors.insert(attestor);ifattestors.len()>=\
    threshold__free{pending_ref__free.remove(&fact);fired_ref__free.insert(fact.clone());newly.\
    push(Durable::__mint_called_only_by_the_quorum_combinator(fact));}}newly}]";

/// The mint closure's captures: the three state references in this order, and the threshold.
const CAPTURES: &str = "[ack_vec_ref__free=__hydro_singleton_ref_0,fired_ref__free=\
    __hydro_singleton_ref_1,pending_ref__free=__hydro_singleton_ref_2,threshold__free=";

/// What the rewrite needs to know of the recognised mints.
#[derive(Default)]
pub(crate) struct Mints {
    /// The mint closures' `FlatMap` nodes, by key, with the attestation type `(F, M)` and the
    /// fact type `F`.
    pub(crate) mints: HashMap<usize, (syn::Type, syn::Type)>,
    /// The attestation folds, by key, with the attestation type.
    pub(crate) folds: HashMap<usize, syn::Type>,
    /// Every node inside a mint (its closure, its fold, the batch it folds, its state cells):
    /// none of them is a fragile site.
    pub(crate) inside: HashSet<usize>,
}

/// The text of a closure or expression, without whitespace.
fn squash(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Whether `text` holds a quote of the mint's file whose arguments start with `args`.
fn quoted(text: &str, args: &str) -> bool {
    let text = squash(text);
    text.match_indices(FILE).any(|(at, _)| {
        text[at + FILE.len()..]
            .split_once('!')
            .is_some_and(|(_, rest)| rest.starts_with(&format!("({args}")))
    })
}

/// Whether `text` is the mint closure, with any threshold.
fn is_mint_closure(text: &str) -> bool {
    let text = squash(text);
    text.match_indices(FILE).any(|(at, _)| {
        let Some((_, rest)) = text[at + FILE.len()..].split_once('!') else {
            return false;
        };
        let Some(rest) = rest.strip_prefix(&format!("({CAPTURES}")) else {
            return false;
        };
        let digits = rest.chars().take_while(char::is_ascii_digit).count();
        digits > 0 && rest[digits..].starts_with(&format!("usize,]{MINT}"))
    })
}

fn element_type(node: &HydroNode) -> Option<&syn::Type> {
    match &node.metadata().collection_kind {
        CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. }
        | CollectionKind::Stream { element_type, .. } => Some(&element_type.0),
        _ => None,
    }
}

fn set_element_type(node: &mut HydroNode, ty: &syn::Type) {
    match &mut node.metadata_mut().collection_kind {
        CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. }
        | CollectionKind::Stream { element_type, .. } => *element_type.0 = ty.clone(),
        _ => {}
    }
}

/// The single type argument of `ty` (`Vec<T>` → `T`), if it has one.
fn type_argument(ty: &syn::Type) -> Option<syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let syn::PathArguments::AngleBracketed(args) = &path.path.segments.last()?.arguments else {
        return None;
    };
    match args.args.first()? {
        syn::GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }
}

/// A state cell of `sliced!`'s `use::state`: `Cast(ChainFirst(DeferTick(CycleSource),
/// Cast(SingletonSource(init))))`, with `init` quoted in the mint's file as `init_text`. Its
/// cycle, if so.
fn state_cell(node: &HydroNode, init_text: &str) -> Option<CycleId> {
    let HydroNode::Cast { inner, .. } = node else {
        return None;
    };
    let HydroNode::ChainFirst { first, second, .. } = &**inner else {
        return None;
    };
    let HydroNode::DeferTick { input, .. } = &**first else {
        return None;
    };
    let HydroNode::CycleSource { cycle_id, .. } = &**input else {
        return None;
    };
    let HydroNode::Cast { inner: source, .. } = &**second else {
        return None;
    };
    let HydroNode::SingletonSource { value, .. } = &**source else {
        return None;
    };
    quoted(&value.to_string(), &format!("[][{init_text}]")).then_some(*cycle_id)
}

/// The inner node of a `Reference`.
fn referenced(node: &HydroNode) -> Option<std::cell::Ref<'_, HydroNode>> {
    match node {
        HydroNode::Reference { inner, .. } => Some(inner.0.borrow()),
        _ => None,
    }
}

/// What one mint closure's references look like, if they are the mint's.
struct Parts {
    /// The attestation type `(F, M)`.
    item: syn::Type,
    /// The fact type `F`.
    fact: syn::Type,
    /// `pending`'s cycle.
    pending_cycle: CycleId,
}

/// Whether `node` is the mint closure of `quorum`, with the fold and state cells it reads.
fn parts(node: &HydroNode) -> Option<Parts> {
    let HydroNode::FlatMap { f, input, .. } = node else {
        return None;
    };
    if !is_mint_closure(&format!("{f:?}")) {
        return None;
    }
    // Its input is one `()` per tick (quorum.rs:156).
    let HydroNode::Cast { inner, .. } = &**input else {
        return None;
    };
    let HydroNode::SingletonSource { value, .. } = &**inner else {
        return None;
    };
    if !quoted(&value.to_string(), "[][()]") {
        return None;
    }
    let refs = f.singleton_refs();
    let [(acks, false), (fired, true), (pending, true)] = refs else {
        return None;
    };
    // `ack_vec`: the batch folded into a `Vec` (quorum.rs:144-150).
    let acks = referenced(acks)?;
    let HydroNode::Fold {
        init, acc, input, ..
    } = &*acks
    else {
        return None;
    };
    if !quoted(&format!("{init:?}"), "[][||Vec::new()]")
        || !quoted(&format!("{acc:?}"), "[][|items,item|items.push(item)]")
        || !matches!(**input, HydroNode::Batch { .. })
    {
        return None;
    }
    let item = type_argument(element_type(&acks)?)?;
    let syn::Type::Tuple(pair) = &item else {
        return None;
    };
    let fact = pair.elems.first()?.clone();
    state_cell(&*referenced(fired)?, "HashSet::new()")?;
    let pending_cycle = state_cell(&*referenced(pending)?, "HashMap::new()")?;
    Some(Parts {
        item,
        fact,
        pending_cycle,
    })
}

/// Visits `node` and everything below it (through tees and references), mutably.
fn walk(node: &mut HydroNode, visit: &mut impl FnMut(&mut HydroNode)) {
    visit(node);
    match node {
        HydroNode::Reference { inner, .. } | HydroNode::Tee { inner, .. } => {
            walk(&mut inner.0.borrow_mut(), visit);
        }
        _ => node.transform_children(|child, _| walk(child, visit), &mut Default::default()),
    }
}

/// Finds every `quorum` mint of `ir`, and prepares it for per-fact lineage: the fold's and the
/// `pending` cell's types are widened (see the [module docs](self)). Returns what the rewrite
/// needs. A no-op on a program without the mint.
///
/// Types are widened in place (the boxed type is overwritten, not replaced), so node keys
/// ([`node_key`]) are the same before and after.
pub(crate) fn prepare(ir: &mut [HydroRoot]) -> Mints {
    let p = super::rewrite::prov();
    let mut mints = Mints::default();
    let mut pending_cycles: HashMap<CycleId, syn::Type> = HashMap::new();
    hydro_lang::compile::ir::transform_bottom_up(
        ir,
        &mut |_| {},
        &mut |node| {
            let Some(Parts {
                item,
                fact,
                pending_cycle,
            }) = parts(node)
            else {
                return;
            };
            pending_cycles.insert(pending_cycle, fact.clone());
            let key = node_key(node);
            let HydroNode::FlatMap { f, input, .. } = node else {
                unreachable!()
            };
            f.transform_children(
                &mut |r, _| {
                    let HydroNode::Reference { inner, .. } = r else {
                        return;
                    };
                    let inner = inner.0.clone();
                    let mut cell = inner.borrow_mut();
                    if let HydroNode::Fold { input: batch, .. } = &*cell {
                        // `Vec<(F, M)>` becomes `Vec<Tagged<(F, M)>>`; the batch it folds is
                        // inside, its input is not.
                        mints.inside.insert(node_key(batch));
                        let widened: syn::Type =
                            syn::parse_quote!(::std::vec::Vec<#p::Tagged<#item>>);
                        set_element_type(r, &widened);
                        set_element_type(&mut cell, &widened);
                        mints.folds.insert(node_key(&cell), item.clone());
                        mints.inside.insert(node_key(&cell));
                    } else {
                        if state_cell(&cell, "HashMap::new()").is_some() {
                            widen(r, &mut cell, &fact, &p);
                        }
                        walk(&mut cell, &mut |n| {
                            mints.inside.insert(node_key(n));
                        });
                    }
                    mints.inside.insert(node_key(r));
                },
                &mut Default::default(),
            );
            walk(input, &mut |n| {
                mints.inside.insert(node_key(n));
            });
            mints.mints.insert(key, (item, fact));
            mints.inside.insert(key);
        },
        false,
    );
    // The write-back of `pending` into its cycle carries the widened type too.
    for root in ir.iter_mut() {
        if let HydroRoot::CycleSink {
            cycle_id, input, ..
        } = root
            && let Some(fact) = pending_cycles.get(cycle_id)
            && let HydroNode::Reference { inner, .. } = &**input
        {
            let inner = inner.0.clone();
            let mut cell = inner.borrow_mut();
            widen(input, &mut cell, fact, &p);
        }
    }
    mints
}

/// Widens a reference to the `pending` cell, and the cell itself (once), from
/// `HashMap<F, HashSet<M>>` to `(HashMap<F, HashSet<M>>, HashMap<F, TagSet>)`.
fn widen(
    reference: &mut HydroNode,
    cell: &mut HydroNode,
    fact: &syn::Type,
    p: &proc_macro2::TokenStream,
) {
    let Some(old) = element_type(cell).cloned() else {
        return;
    };
    let old = if is_widened(&old) {
        let syn::Type::Tuple(t) = old else {
            unreachable!()
        };
        t.elems[0].clone()
    } else {
        old
    };
    let widened: syn::Type =
        syn::parse_quote!((#old, ::std::collections::HashMap<#fact, #p::TagSet>));
    set_element_type(reference, &widened);
    if element_type(cell).is_some_and(is_widened) {
        return;
    }
    walk(cell, &mut |n| {
        set_element_type(n, &widened);
        if let HydroNode::SingletonSource { value, .. } = n {
            let init = &value.0;
            let lineage: syn::Expr = syn::parse_quote!(
                (#init, ::std::collections::HashMap::<#fact, #p::TagSet>::new())
            );
            *value = DebugExpr::from(lineage);
        }
    });
}

/// Whether a `pending` type was already widened (a pair).
fn is_widened(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Tuple(t) if t.elems.len() == 2)
}
