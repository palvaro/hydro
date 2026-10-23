//! The tap pass: every checked labelled collection, at every member, into an external output
//! of its own (see the [module docs](super)).
//!
//! A tapped node becomes a `Tee` (as `Stream::clone` would make it) and a second branch maps
//! each element to `(bincode(element), Debug of the element)` and sends it to the simulator's
//! default external location, exactly the IR `Stream::sim_cluster_output` builds. A `Tee`
//! branch is invisible to the rest of the program, and an external output adds no schedule
//! choice, so the program's behaviour and its schedule space are unchanged. The pass runs
//! before network compilation (`IrPassStage::PreNetwork`), so the outputs are compiled like the
//! program's own; it runs before the provenance rewrite (`PostNetwork`), so record provenance
//! flows through the taps like through any other operator and arrives as the outputs' meta.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use hydro_lang::Counter;
use hydro_lang::compile::builder::ExternalPortId;
use hydro_lang::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, DebugInstantiate, DebugType, HydroIrOpMetadata,
    HydroNode, HydroRoot, SharedNode,
};
use hydro_lang::live_collections::stream::networking::serialize_bincode_with_type;
use hydro_lang::location::LocationKey;
use syn::parse_quote;

use super::collections::{EcKey, ec_key, is_ec};
use super::{LabelPlan, ec_collections, plan};
use crate::provenance::sensitivity::cones_of;

/// One observed element of a labelled collection, as the host receives it: its bincode (the
/// fact's identity) and, when the element type implements `Debug`, its `Debug` rendering for
/// reports. Equality, order and hashing use the bytes only.
#[derive(Clone, Debug)]
pub struct Fact {
    /// `bincode` of the element.
    pub bytes: Vec<u8>,
    /// `format!("{:?}", element)`, if the element type implements `Debug`, shortened for
    /// reports by [`super::source_names::short_debug`] (module paths dropped, member ids as
    /// `Client@0`). Not part of the fact's identity.
    pub debug: Option<String>,
}

impl PartialEq for Fact {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Fact {}

impl PartialOrd for Fact {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Fact {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl std::hash::Hash for Fact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl std::fmt::Display for Fact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(debug) = &self.debug {
            return f.write_str(debug);
        }
        for b in &self.bytes {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl Fact {
    /// A fact with no `Debug` rendering, from its bytes (for tests and hand-built
    /// observations).
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Fact {
            bytes: bytes.into(),
            debug: None,
        }
    }
}

/// The wire type of a tap output: `(bincode(element), Debug of the element)`.
type TapWire = (Vec<u8>, Option<String>);

/// Decodes the payload of a tap output (as `SimNetworkHook::on_output` sees it). The `Debug`
/// rendering is shortened for reports ([`super::source_names::short_debug`]); the identity (the
/// bytes) is as the tap shipped it.
pub fn decode_output(payload: &[u8]) -> Fact {
    let (bytes, debug): TapWire =
        bincode::deserialize(payload).expect("a tap output is `(bincode, Option<Debug>)`");
    Fact {
        bytes,
        debug: debug.map(|d| super::source_names::short_debug(&d)),
    }
}

/// The labelled collections of a compiled program and the external output ports their taps
/// emit on.
#[derive(Clone, Debug, Default)]
pub struct Labels {
    /// One plan per labelled collection, in enumeration order. The index into this vector is
    /// the label's identity everywhere else in hydrofrac.
    pub plans: Vec<LabelPlan>,
    /// Tap output port → index of the label it carries. A label tapped at several IR nodes
    /// (one library function instantiated twice on a cluster) has several ports.
    ports: BTreeMap<usize, usize>,
}

impl Labels {
    /// The label a tap output port carries, if `port` is a tap.
    pub fn label_of_port(&self, port: usize) -> Option<usize> {
        self.ports.get(&port).copied()
    }

    /// The tap output ports of `label`.
    pub fn ports_of(&self, label: usize) -> impl Iterator<Item = usize> + '_ {
        self.ports
            .iter()
            .filter(move |(_, l)| **l == label)
            .map(|(p, _)| *p)
    }

    /// The labels searched in their own right, with their indices.
    pub fn searched(&self) -> impl Iterator<Item = (usize, &LabelPlan)> {
        self.plans
            .iter()
            .enumerate()
            .filter(|(_, p)| p.is_searched())
    }
}

/// The host's handle on a [`tap_pass`]: the labels it found, once the simulation is compiled.
#[derive(Clone, Default)]
pub struct TapPass {
    labels: Rc<RefCell<Option<Labels>>>,
}

impl TapPass {
    /// The labels the pass found and the ports it tapped them on.
    ///
    /// # Panics
    /// If the pass has not run (the flow was not compiled with it).
    pub fn labels(&self) -> Labels {
        self.labels
            .borrow()
            .clone()
            .expect("the tap pass runs when the simulation is compiled")
    }
}

/// The tap pass, to register with `SimFlow::with_ir_pass(IrPassStage::PreNetwork, ..)`, and
/// the handle that reads what it found.
pub fn tap_pass() -> (TapPass, impl FnOnce(&mut Vec<HydroRoot>) + 'static) {
    let handle = TapPass::default();
    let out = handle.labels.clone();
    (handle, move |ir: &mut Vec<HydroRoot>| {
        *out.borrow_mut() = Some(tap(ir));
    })
}

/// Visits every node under `ir` bottom-up, in place: unlike `transform_bottom_up`, a node a
/// `Tee` shares is edited inside its cell (once) rather than moved to a fresh cell, so the
/// branches this pass creates keep pointing at it.
fn walk_in_place(ir: &mut [HydroRoot], f: &mut impl FnMut(&mut HydroNode)) {
    fn walk(
        node: &mut HydroNode,
        seen: &mut HashSet<*const RefCell<HydroNode>>,
        f: &mut impl FnMut(&mut HydroNode),
    ) {
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => {
                if seen.insert(Rc::as_ptr(&inner.0)) {
                    walk(&mut inner.0.borrow_mut(), seen, f);
                }
            }
            HydroNode::VersionedNetwork { fork, .. } => {
                if seen.insert(Rc::as_ptr(&fork.0)) {
                    walk(&mut fork.0.borrow_mut(), seen, f);
                }
            }
            _ => node.transform_children(|child, _| walk(child, seen, f), &mut HashMap::new()),
        }
        f(node);
    }
    let mut seen = HashSet::new();
    for root in ir.iter_mut() {
        walk(root.input_mut(), &mut seen, f);
    }
}

/// The largest external port id `ir` uses (inputs and outputs), so that the taps' ports are
/// fresh.
fn max_port(ir: &mut [HydroRoot]) -> Option<usize> {
    let mut max: Option<usize> = None;
    for root in ir.iter() {
        if let HydroRoot::SendExternal { to_port_id, .. } = root {
            max = max.max(Some(to_port_id.into_inner()));
        }
    }
    walk_in_place(ir, &mut |node| {
        if let HydroNode::ExternalInput { from_port_id, .. } = node {
            max = max.max(Some(from_port_id.into_inner()));
        }
    });
    max
}

/// The element type of a stream-like collection kind: `T`, or `(K, V)` for a keyed stream.
fn element_type(kind: &CollectionKind) -> Option<syn::Type> {
    match kind {
        CollectionKind::Stream { element_type, .. } => Some((*element_type.0).clone()),
        CollectionKind::KeyedStream {
            key_type,
            value_type,
            ..
        } => {
            let (k, v) = (&*key_type.0, &*value_type.0);
            Some(parse_quote!((#k, #v)))
        }
        CollectionKind::Singleton { .. }
        | CollectionKind::Optional { .. }
        | CollectionKind::KeyedSingleton { .. } => None,
    }
}

/// The closure a tap maps each element through: `(identity of x, Debug of x if T: Debug)`. The
/// identity is `bincode(x)` when the element type is `Serialize`, else the `Debug` rendering,
/// else a `Hash` digest (a labelled type need not be `Serialize`: `hydro_std`'s quorum
/// certificate `Durable` is deliberately not). Both are chosen by autoref specialization on the
/// concrete element type, so any element type compiles.
fn tap_closure(element_type: &syn::Type) -> syn::Expr {
    // The generated dylib depends on `hydro_lang` under its own name, as every IR expression
    // `hydro_lang` itself builds assumes.
    parse_quote!(
        move |__hydrofrac_x: #element_type| -> (::std::vec::Vec<u8>, ::core::option::Option<::std::string::String>) {
            struct __HydrofracWrap<'__a, __T>(&'__a __T);
            trait __HydrofracViaDebug {
                fn __hydrofrac_debug(&self) -> ::core::option::Option<::std::string::String>;
            }
            impl<__T: ::core::fmt::Debug> __HydrofracViaDebug for __HydrofracWrap<'_, __T> {
                fn __hydrofrac_debug(&self) -> ::core::option::Option<::std::string::String> {
                    ::core::option::Option::Some(::std::format!("{:?}", self.0))
                }
            }
            trait __HydrofracNoDebug {
                fn __hydrofrac_debug(&self) -> ::core::option::Option<::std::string::String>;
            }
            impl<__T> __HydrofracNoDebug for &__HydrofracWrap<'_, __T> {
                fn __hydrofrac_debug(&self) -> ::core::option::Option<::std::string::String> {
                    ::core::option::Option::None
                }
            }
            let __debug = (&__HydrofracWrap(&__hydrofrac_x)).__hydrofrac_debug();
            // The fact's identity: its bincode if the element type is `Serialize`, else its
            // `Debug` rendering, else a `Hash` digest. Deref-based autoref specialization: the
            // receiver `&&&&_` matches the most specific applicable impl first. Elements of one
            // label share a type, so they all take the same branch.
            trait __HydrofracViaSerialize {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8>;
            }
            impl<__T: hydro_lang::__staged::__deps::serde::Serialize> __HydrofracViaSerialize for &&&__HydrofracWrap<'_, __T> {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8> {
                    hydro_lang::runtime_support::bincode::serialize(self.0).unwrap()
                }
            }
            trait __HydrofracIdViaDebug {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8>;
            }
            impl<__T: ::core::fmt::Debug> __HydrofracIdViaDebug for &&__HydrofracWrap<'_, __T> {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8> {
                    ::std::format!("{:?}", self.0).into_bytes()
                }
            }
            trait __HydrofracIdViaHash {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8>;
            }
            impl<__T: ::core::hash::Hash> __HydrofracIdViaHash for &__HydrofracWrap<'_, __T> {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8> {
                    let mut __h = ::std::hash::DefaultHasher::new();
                    ::core::hash::Hash::hash(self.0, &mut __h);
                    ::core::hash::Hasher::finish(&__h).to_le_bytes().to_vec()
                }
            }
            trait __HydrofracNoId {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8>;
            }
            impl<__T> __HydrofracNoId for __HydrofracWrap<'_, __T> {
                fn __hydrofrac_id(&self) -> ::std::vec::Vec<u8> {
                    ::core::panic!(
                        "hydrofrac: a labelled collection's element type ({}) is neither `Serialize`, `Debug` nor `Hash`, so its facts have no identity to compare",
                        ::core::any::type_name::<__T>()
                    )
                }
            }
            let __id = (&&&&__HydrofracWrap(&__hydrofrac_x)).__hydrofrac_id();
            (__id, __debug)
        }
    )
}

/// Enumerates the labels of `ir`, plans them, and taps every searched one (see the module
/// docs).
pub fn tap(ir: &mut Vec<HydroRoot>) -> Labels {
    let collections = ec_collections(ir);
    // Each label's schedule sensitivity, on the IR as the program built it (before the taps).
    let cones = cones_of(ir, collections.clone());
    let mut plans = plan(collections);
    for (plan, cone) in plans.iter_mut().zip(cones) {
        debug_assert_eq!(plan.collection.key(), cone.collection.key());
        plan.sites = cone.sites;
    }
    let index_of: HashMap<EcKey, usize> = plans
        .iter()
        .enumerate()
        .map(|(i, plan)| (plan.collection.key(), i))
        .collect();

    let mut ports = Counter::<ExternalPortId>::default();
    if let Some(max) = max_port(ir) {
        while ports.next_count() <= max {
            ports.get_and_increment();
        }
    }

    let wire_type: syn::Type = parse_quote!((
        ::std::vec::Vec<u8>,
        ::core::option::Option<::std::string::String>
    ));

    // Labelled nodes are re-identified by the enumeration's key. (A `Tee` never matches: the
    // enumeration records what it shares, under that node's own key; a shared partition or
    // reference cell is not tapped as a whole.)
    let mut taps: Vec<HydroRoot> = vec![];
    let mut tap_ports: BTreeMap<usize, usize> = BTreeMap::new();
    walk_in_place(ir, &mut |node: &mut HydroNode| {
        if !is_ec(node)
            || matches!(
                node,
                HydroNode::PartitionShared { .. }
                    | HydroNode::Reference { .. }
                    | HydroNode::VersionedNetwork { .. }
                    | HydroNode::VersionedNetworkFork { .. }
            )
        {
            return;
        }
        let Some(&index) = index_of.get(&ec_key(node)) else {
            return;
        };
        if !plans[index].is_searched() {
            return;
        }
        let node_metadata = node.metadata().clone();
        let Some(element_type) = element_type(&node_metadata.collection_kind) else {
            return;
        };
        // Share the node: make it a `Tee` if it is not one, and take a second branch.
        let shared: Rc<RefCell<HydroNode>> = if let HydroNode::Tee { inner, .. } = node {
            inner.0.clone()
        } else {
            let orig = std::mem::replace(node, HydroNode::Placeholder);
            let cell = Rc::new(RefCell::new(orig));
            *node = HydroNode::Tee {
                inner: SharedNode(cell.clone()),
                metadata: node_metadata.clone(),
            };
            cell
        };
        let branch = HydroNode::Tee {
            inner: SharedNode(shared),
            metadata: node_metadata.clone(),
        };
        let mut map_metadata = node_metadata.clone();
        map_metadata.consistency = None;
        map_metadata.tag = None;
        map_metadata.op.id = None;
        map_metadata.collection_kind = match &node_metadata.collection_kind {
            CollectionKind::Stream {
                bound,
                order,
                retry,
                ..
            } => CollectionKind::Stream {
                bound: bound.clone(),
                order: order.clone(),
                retry: retry.clone(),
                element_type: DebugType::from(wire_type.clone()),
            },
            CollectionKind::KeyedStream {
                bound,
                value_order,
                value_retry,
                ..
            } => CollectionKind::Stream {
                bound: bound.clone(),
                order: value_order.clone(),
                retry: value_retry.clone(),
                element_type: DebugType::from(wire_type.clone()),
            },
            _ => unreachable!("only streams have an element type"),
        };
        let mapped = HydroNode::Map {
            f: ClosureExpr::new(DebugExpr::from(tap_closure(&element_type)), vec![]),
            input: Box::new(branch),
            metadata: map_metadata,
        };
        let port = ports.get_and_increment();
        tap_ports.insert(port.into_inner(), index);
        let mut op_metadata: HydroIrOpMetadata = node_metadata.op;
        op_metadata.id = None;
        taps.push(HydroRoot::SendExternal {
            to_external_key: LocationKey::FIRST,
            to_port_id: port,
            to_many: false,
            unpaired: true,
            serialize_fn: Some(serialize_bincode_with_type(false, &wire_type).into()),
            instantiate_fn: DebugInstantiate::Building,
            input: Box::new(mapped),
            op_metadata,
        });
    });
    ir.extend(taps);

    Labels {
        plans,
        ports: tap_ports,
    }
}
