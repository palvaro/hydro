//! The tap pass (see the [module docs](super)). A tapped node becomes a `Tee`, and a second
//! branch maps each element to `(identity, Debug)` and sends it to an external output, the IR
//! `Stream::sim_cluster_output` builds: invisible to the program, and no new schedule choice.
//! It runs `PreNetwork`, before the provenance rewrite, so record provenance flows through the
//! taps and arrives as the outputs' meta.

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
use crate::provenance::choices::{ChoiceSite, program_choices};
use crate::provenance::rewrite::debug_of;

/// One observed element of a labelled collection: its identity (normally its bincode) and its
/// `Debug` rendering, if any, for reports. Equality, order and hashing use the bytes only.
#[derive(Clone, Debug)]
pub struct Fact {
    pub bytes: Vec<u8>,
    /// Shortened by [`super::source_names::short_debug`].
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
    /// A fact with no `Debug` rendering.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Fact {
            bytes: bytes.into(),
            debug: None,
        }
    }
}

/// The wire type of a tap output: `(bincode(element), Debug of the element)`.
type TapWire = (Vec<u8>, Option<String>);

/// Decodes the payload of a tap output (as `SimNetworkHook::on_output` sees it).
pub fn decode_output(payload: &[u8]) -> Fact {
    let (bytes, debug): TapWire =
        bincode::deserialize(payload).expect("a tap output is `(bincode, Option<Debug>)`");
    Fact {
        bytes,
        debug: debug.map(|d| super::source_names::short_debug(&d)),
    }
}

/// The labelled collections of a compiled program and their taps' output ports.
#[derive(Clone, Debug, Default)]
pub struct Labels {
    /// One plan per labelled collection; the index is the label's identity in hydrofrac.
    pub plans: Vec<LabelPlan>,
    /// The program's schedule choices ([`crate::provenance::choices`]), on the IR as the
    /// program built it.
    pub choices: Vec<ChoiceSite>,
    /// Tap output port → label (a label tapped at several IR nodes has several ports).
    pub(super) ports: BTreeMap<usize, usize>,
}

impl Labels {
    /// The label a tap output port carries, if `port` is a tap.
    pub fn label_of_port(&self, port: usize) -> Option<usize> {
        self.ports.get(&port).copied()
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

/// Visits every node under `ir` bottom-up, in place: unlike `transform_bottom_up`, a shared
/// node is edited inside its cell (once), so the branches this pass creates keep pointing at it.
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
        let (HydroRoot::ForEach { input, .. }
        | HydroRoot::SendExternal { input, .. }
        | HydroRoot::DestSink { input, .. }
        | HydroRoot::CycleSink { input, .. }
        | HydroRoot::EmbeddedOutput { input, .. }
        | HydroRoot::Null { input, .. }) = root;
        walk(input, &mut seen, f);
    }
}

/// The largest external port id `ir` uses, so that the taps' ports are fresh.
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

/// The closure a tap maps each element through: `(identity of x, Debug of x if T: Debug)`. The
/// identity is `bincode(x)` if `T: Serialize`, else its `Debug`, else a `Hash` digest
/// (`hydro_std`'s `Durable` is deliberately not `Serialize`). Autoref specialization picks
/// both, so any element type compiles.
fn tap_closure(element_type: &syn::Type) -> syn::Expr {
    // The generated dylib depends on `hydro_lang` under its own name, as every IR expression
    // `hydro_lang` itself builds assumes.
    let debug = debug_of(quote::quote!(&__hydrofrac_x));
    parse_quote!(
        move |__hydrofrac_x: #element_type| -> (::std::vec::Vec<u8>, ::core::option::Option<::std::string::String>) {
            struct __HydrofracWrap<'__a, __T>(&'__a __T);
            let __debug = #debug;
            // The receiver `&&&&_` matches the most specific applicable impl first.
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

/// Enumerates the labels of `ir`, plans them, and taps every searched one.
pub fn tap(ir: &mut Vec<HydroRoot>) -> Labels {
    let collections = ec_collections(ir);
    // The program's schedule choices, on the IR as the program built it (before the taps).
    let choices = program_choices(ir);
    let plans = plan(collections);
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

    // Labelled nodes are re-identified by the enumeration's key (a `Tee` never matches: the
    // enumeration records what it shares).
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
        // The element type (`(K, V)` for a keyed stream) and the tap's stream kind.
        let (element_type, bound, order, retry): (syn::Type, _, _, _) =
            match &node_metadata.collection_kind {
                CollectionKind::Stream {
                    element_type,
                    bound,
                    order,
                    retry,
                } => ((*element_type.0).clone(), bound, order, retry),
                CollectionKind::KeyedStream {
                    key_type,
                    value_type,
                    bound,
                    value_order,
                    value_retry,
                } => {
                    let (k, v) = (&*key_type.0, &*value_type.0);
                    (parse_quote!((#k, #v)), bound, value_order, value_retry)
                }
                _ => return,
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
        map_metadata.collection_kind = CollectionKind::Stream {
            bound: bound.clone(),
            order: order.clone(),
            retry: retry.clone(),
            element_type: DebugType::from(wire_type.clone()),
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
        choices,
        ports: tap_ports,
    }
}
