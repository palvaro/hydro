//! Closed-broadcast channels and their fan-out order (docs/design/hydrofrac-races.md §4.2).
//!
//! `broadcast_closed` (`hydro_lang/src/live_collections/stream/networking.rs`, `Stream` and
//! `KeyedStream`) lowers to:
//!
//! ```text
//! Network(to: C) ← Cast ← Map(|(data, member_id)| (member_id, data))
//!     ← Map(|((), (v1, v2))| (v1, v2))
//!     ← JoinHalf(probe: Map(|v| ((), v)) ← data,
//!                build: Map(|v| ((), v)) ← Source::Iter(C's ClusterIds.iter().map(..)))
//! ```
//!
//! [`mark`] finds every `Network` of exactly that shape (the closures compared by their text,
//! the id source naming the destination cluster) and marks it, so that the races engine can
//! use the order of its sends (see [`FANOUT_MARK`]).
//!
//! **Why the order is fixed.** `JoinHalf` compiles to `join_multiset_half` (`hydro_lang`
//! `compile/ir/mod.rs`, `dfir_lang` `graph/ops/join_multiset_half.rs`): it accumulates its whole
//! build input into one `Vec` per key before it reads the probe side, then emits, for each
//! probe item, one output per build value, in the order the build values were pushed. The key is
//! `()` here, so there is one `Vec`: the cluster's ids, which the simulator lists as `0..n`
//! (`hydro_lang` `sim/graph.rs`, `__hydro_lang_cluster_ids_*`), pushed by one `source_iter`. The
//! maps and the cast between the join and the network are one-to-one, and the network sends
//! its input items in order (`sim/builder.rs`: `map(serialize) -> for_each(send)`). So every
//! record `v` the probe side emits is sent as `(v → 0), (v → 1), …, (v → n−1)`, in that order,
//! and the bytes on the wire are those of `v` alone.

use hydro_lang::compile::ir::{
    CollectionKind, HydroNode, HydroRoot, HydroSource, StreamOrder, StreamRetry,
};
use hydro_lang::location::dynamic::LocationId;

/// The `op.id` a marked `Network` carries; [`hydro_lang::sim::network_hook::ChannelInfo::op`]
/// copies it, so `CompiledSim::channels` tells marked channels apart. Nothing else sets `op.id`.
pub const FANOUT_MARK: usize = usize::MAX - 0xFA0;

/// The `op.id` of a marked `Network` whose payloads are, moreover, *sequenced*: the data its join
/// fans out is exactly a harness input of the sending process (see [`is_sequenced`]). Such a
/// channel is a closed broadcast too, so [`FANOUT_MARK`]'s order holds on it as well.
pub const SEQUENCED_MARK: usize = usize::MAX - 0xFA1;

/// Whether `op_id` (a channel's `op.id`) marks a closed broadcast, sequenced or not.
pub fn is_fan_out(op_id: Option<usize>) -> bool {
    matches!(op_id, Some(FANOUT_MARK | SEQUENCED_MARK))
}

/// The text of a closure or expression, without whitespace.
fn squash(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Whether `node` is a `Map` whose closure is `body`, quoted in `hydro_lang`'s own source file
/// `file` (so not a user closure that happens to read the same).
fn is_map(node: &HydroNode, file: &str, body: &str) -> bool {
    let HydroNode::Map { f, .. } = node else {
        return false;
    };
    quoted_in(&format!("{f:?}"), file, body)
}

/// Whether the expression text `text` is `body`, quoted in `hydro_lang`'s source file `file`.
fn quoted_in(text: &str, file: &str, body: &str) -> bool {
    let text = squash(text);
    let quote = format!("hydro_lang::__stageleft_quote_src_live_collections_stream_{file}_rs_");
    text.match_indices(&quote).any(|(at, _)| {
        // `…_rs_<line>_<column>!(<captures>[<body>])`
        text[at + quote.len()..]
            .split_once('!')
            .is_some_and(|(_, rest)| rest.starts_with(&format!("({body})")))
    })
}

/// Marks every closed-broadcast `Network` of `ir` (see the [module docs](self)); returns how many.
pub fn mark(ir: &mut [HydroRoot]) -> usize {
    let mut marked = 0;
    hydro_lang::compile::ir::transform_bottom_up(
        ir,
        &mut |_| {},
        &mut |node| {
            if let Some(data) = closed_broadcast_data(node) {
                let mark = if is_sequenced(data) {
                    SEQUENCED_MARK
                } else {
                    FANOUT_MARK
                };
                node.metadata_mut().op.id = Some(mark);
                marked += 1;
            }
        },
        false,
    );
    marked
}

/// Whether `data`, the stream a closed broadcast fans out, is *sequenced*: exactly a harness input
/// of one process, `TotalOrder` and `ExactlyOnce`, with nothing in between (no cast, tee, batch or
/// network). On such a channel the payloads are sent in the order the harness sent them, under
/// every fault set (docs/design/hydrofrac-soundness.md §6.4):
/// - the harness sends one fixed sequence (A8), and a run delivers a prefix of it;
/// - the simulator feeds a harness input through an unbounded FIFO channel into `source_stream`
///   (`hydro_lang/src/sim/builder.rs`, the external input), with no schedule hook;
/// - `join_multiset_half` emits its probe items in arrival order (§6.1), the maps and the cast
///   are one to one, and the network sends in order (A10);
/// - a crash of the process cuts a suffix of its sends (A11); an omission drops a delivery, never
///   a send.
///
/// A broadcast of data that arrives over the network (as in S2's relay) is not sequenced: the
/// arrival order there depends on the schedule and on the faults.
fn is_sequenced(data: &HydroNode) -> bool {
    let HydroNode::ExternalInput {
        from_many: false,
        metadata,
        ..
    } = data
    else {
        return false;
    };
    matches!(metadata.location_id, LocationId::Process(_))
        && matches!(
            metadata.collection_kind,
            CollectionKind::Stream {
                order: StreamOrder::TotalOrder,
                retry: StreamRetry::ExactlyOnce,
                ..
            }
        )
}

/// If `node` is a `Network` of exactly `broadcast_closed`'s shape, the data it fans out.
fn closed_broadcast_data(node: &HydroNode) -> Option<&HydroNode> {
    let HydroNode::Network {
        input, metadata, ..
    } = node
    else {
        return None;
    };
    let LocationId::Cluster(key) = metadata.location_id.root() else {
        return None;
    };
    let HydroNode::Cast { inner: swapped, .. } = &**input else {
        return None;
    };
    if !is_map(
        swapped,
        "networking",
        "[][|(data,member_id)|(member_id,data)]",
    ) {
        return None;
    }
    let HydroNode::Map { input: unit, .. } = &**swapped else {
        return None;
    };
    if !is_map(unit, "mod", "[][|((),(v1,v2))|(v1,v2)]") {
        return None;
    }
    let HydroNode::Map { input: join, .. } = &**unit else {
        return None;
    };
    let HydroNode::JoinHalf { left, right, .. } = &**join else {
        return None;
    };
    let keyed = |n: &HydroNode| is_map(n, "mod", "[][|v|((),v)]");
    if !keyed(left) || !keyed(right) {
        return None;
    }
    let HydroNode::Map { input: ids, .. } = &**right else {
        return None;
    };
    // The cluster broadcast asserts the ids' consistency first: an identity on every item.
    let ids = match &**ids {
        HydroNode::AssertIsConsistent { inner, .. } => &**inner,
        other => other,
    };
    let HydroNode::Source {
        source: HydroSource::Iter(expr),
        ..
    } = ids
    else {
        return None;
    };
    let body = format!(
        "[cluster_ids__free=__hydro_lang_cluster_ids_{key},]\
         [cluster_ids__free.iter().map(|id|MemberId::from_tagless(id.clone()))]"
    );
    let HydroNode::Map { input: data, .. } = &**left else {
        return None;
    };
    quoted_in(&expr.to_string(), "networking", &body).then_some(&**data)
}
