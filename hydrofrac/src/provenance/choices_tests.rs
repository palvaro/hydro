//! Tests of the whole-program schedule-choice scan ([`crate::provenance::choices`]).
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::panic::AssertUnwindSafe;

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use crate::provenance::choices::{ChoiceSite, program_choices};

#[cfg(stageleft_runtime)]
fn ops(choices: &[ChoiceSite]) -> Vec<&str> {
    choices.iter().map(|c| c.op.as_str()).collect()
}

/// Unknown counts as a choice: a node kind off the allowlist is one. `Placeholder`, the only
/// node with no meaning, stands in for an unknown operator, and a captured reference that does
/// not resolve to its state cell is unresolved.
#[cfg(stageleft_runtime)]
#[test]
fn what_the_scan_cannot_see_through_is_a_choice() {
    use hydro_lang::compile::ir::HydroNode;

    use crate::provenance::choices::{ALLOWED, cell, choice};

    assert_eq!(
        choice(&HydroNode::Placeholder).as_deref(),
        Some("unknown_operator")
    );
    assert!(!ALLOWED.contains(&"Batch"));
    assert_eq!(cell(&HydroNode::Placeholder), None);
}

/// A process broadcasts its input to a cluster (the label is what the members receive); the
/// input's order is `O`.
#[cfg(stageleft_runtime)]
fn broadcast_input<O: hydro_lang::live_collections::stream::Ordering>() -> FlowBuilder<'static> {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, O, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().name("to_members").bincode());
    let _out = delivered.sim_cluster_output();
    flow
}

/// A plain broadcast over a totally ordered input has no choice. The same program over an
/// input whose order is unspecified does: in a deployment the input arrives in any order.
#[cfg(stageleft_runtime)]
#[test]
fn an_unordered_external_input_is_a_choice() {
    let ordered = program_choices(broadcast_input::<TotalOrder>().finalize().ir());
    assert!(ordered.is_empty(), "{ordered:?}");
    let unordered = program_choices(broadcast_input::<NoOrder>().finalize().ir());
    assert_eq!(
        ops(&unordered),
        ["unordered_external_input"],
        "{unordered:?}"
    );
}

/// The rule is per program. A `batch` on a path that never reaches the label still counts: it
/// can reorder the process's sends, and so move a positional crash point across the label's
/// delivery.
#[cfg(stageleft_runtime)]
#[test]
fn a_choice_anywhere_in_the_program_counts() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send_a, a) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (_send_b, b) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = source.tick();
    a.broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed */
        ))
        .for_each(q!(|_| {}));
    b.batch(&tick, nondet!(/** test */))
        .all_ticks()
        .broadcast_closed(&members, TCP.fail_stop().name("b").bincode())
        .for_each(q!(|_| {}));

    let choices = program_choices(flow.finalize().ir());
    assert_eq!(ops(&choices), ["batch"], "{choices:?}");
    assert!(choices[0].site.contains("choices_tests.rs:"), "{choices:?}");
}

/// Faults that *create* a record (docs/design/hydrofrac-lite.md §4.9). A source sends
/// `100, 1, 2` over an ordered lossy channel (`TCP.lossy`, which keeps `TotalOrder`). Each
/// member reports every `x < 100` that is not at position `x` of what it received (none,
/// without faults), next to a constant `5`. Omitting the filler `100` to one member shifts `1`
/// and `2` and makes that member report them. That is a divergent fact, created by an omission
/// with no crash. The constant's derivation has no delivery, so lineage gives the search
/// nothing to kill, and the strong rule would certify the label. The channel is the only
/// choice the scan finds (its `nondet!` is the omission itself).
///
/// The simulator rejects `TCP.lossy` today. The second half of the test pins that, so that
/// support for it is noticed and this program is cross-checked against the brute force.
#[cfg(stageleft_runtime)]
#[test]
fn an_ordered_lossy_channel_is_a_choice() {
    let program = || {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<()>();
        let members = flow.cluster::<()>();
        let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let shifted = input
            .broadcast_closed(
                &members,
                TCP.lossy(nondet!(/** omissions, which the check injects */))
                    .name("ordered")
                    .bincode(),
            )
            .weaken_consistency()
            .enumerate()
            .filter_map(q!(|(i, x)| (x < 100 && i != x as usize).then_some(x)))
            .weaken_ordering::<NoOrder>();
        let constant = members
            .source_iter(q!([5u32]))
            .weaken_consistency()
            .weaken_ordering::<NoOrder>();
        let _out = shifted
            .merge_unordered(constant.into())
            .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
                /** claimed (wrongly, under omissions): every member sees the same positions */
            ))
            .sim_cluster_output();
        (flow, members, send)
    };

    let (flow, _, _) = program();
    let choices = program_choices(flow.finalize().ir());
    assert_eq!(ops(&choices), ["ordered_lossy_network"], "{choices:?}");

    let (flow, members, _send) = program();
    let rejected = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = flow.sim().with_cluster_size(&members, 2).compiled();
    }));
    let message = rejected
        .err()
        .and_then(|e| {
            e.downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
        })
        .unwrap_or_default();
    assert!(
        message.contains("only supports fail-stop and lossy-delayed-forever"),
        "the simulator now runs `TCP.lossy`: cross-check this program against the brute force \
         (`EcCheck::cross_check`) and revisit the scan's `ordered_lossy_network` ({message:?})"
    );
}
