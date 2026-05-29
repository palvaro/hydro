//! Discovery: query cluster members and collect responses.
//!
//! Adapted from consensus-zoo's `primitives::discover::query_cluster`.

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use stageleft::q;

use crate::messages::{StateTransferRequest, TransparentReplica, View};

/// State transfer: on view change, new primary queries survivors for max seq.
///
/// Detects primary change, broadcasts request, collects max_seq responses,
/// returns the reconciled sequence number for the new primary to resume from.
pub fn state_transfer<'a>(
    replicas: &Cluster<'a, TransparentReplica>,
    current_view: Singleton<View, Cluster<'a, TransparentReplica>, Unbounded>,
    max_replicated_seq: Optional<usize, Cluster<'a, TransparentReplica>, Unbounded>,
) -> Optional<usize, Cluster<'a, TransparentReplica>, Unbounded> {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    // Detect primary change
    let primary_changed: Stream<View, Cluster<'a, TransparentReplica>, Unbounded, NoOrder> = sliced! {
        let new_view = use(current_view.clone(), nondet!(/** view changes are infrequent */));
        let mut prev_primary = use::state(|l| l.singleton(q!(u32::MAX)));

        let current_primary = new_view.clone().map(q!(|v: View| v.primary()));

        let changed_view = new_view
            .zip(prev_primary.clone())
            .filter(q!(|(view, prev): &(View, u32)| view.primary() != *prev))
            .map(q!(|(view, _)| view));

        prev_primary = current_primary;

        changed_view.into_stream()
    }.into();

    // New primary sends state transfer request to all replicas
    let requests = primary_changed
        .filter(q!(move |view: &View| {
            view.view_num > 0 && CLUSTER_SELF_ID.get_raw_id() == view.primary()
        }))
        .map(q!(move |view: View| StateTransferRequest {
            view_num: view.view_num,
            requester: CLUSTER_SELF_ID.clone(),
        }))
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** state transfer request */))
        .values();

    // Each replica responds with its max replicated seq
    let resp_tick = replicas.tick();

    let req_in_tick = requests
        .batch(&resp_tick, nondet!(/** batch */))
        .reduce(q!(|_curr: &mut StateTransferRequest, _new: StateTransferRequest| {},
            commutative = manual_proof!(/** all equivalent */)));

    let max_seq_in_tick = max_replicated_seq
        .unwrap_or(replicas.singleton(q!(0usize)).into())
        .snapshot(&resp_tick, nondet!(/** stale ok */));

    // When a request arrives, respond with our max seq routed to the requester
    let responses = req_in_tick
        .clone()
        .if_some_then(max_seq_in_tick)
        .zip(req_in_tick)
        .map(q!(|(my_max, req): (usize, StateTransferRequest)| (req.requester, my_max)))
        .into_stream()
        .all_ticks()
        .demux(replicas, TCP.fail_stop().bincode())
        .values();

    // New primary takes max of all responses
    responses.max()
}
