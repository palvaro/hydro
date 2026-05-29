//! Silence detector: fires when no events arrive within a timeout.
//!
//! Adapted from consensus-zoo's `primitives::liveness::silence_detector`.

use hydro_lang::live_collections::stream::{TotalOrder, Ordering};
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use stageleft::q;

/// Generic silence detector: fires `()` when no events arrive within `timeout_ms`.
///
/// Resets whenever an event arrives. Use this for failure detection —
/// feed responses and get a timeout signal when the system goes silent.
pub fn silence_detector<'a, L: Location<'a> + NoTick, T, O: Ordering>(
    events: Stream<T, L, Unbounded, O>,
    location: &L,
    check_interval_ms: u64,
    timeout_ms: u64,
) -> Stream<(), L, Unbounded>
where
    T: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    let timer = location.source_interval(
        q!(std::time::Duration::from_millis(check_interval_ms)),
        nondet!(/** timer is wall-clock driven */),
    ).map(q!(|_| false));

    let event_signals = events.map(q!(|_| true));

    let merged = event_signals
        .interleave(timer)
        .assume_ordering::<TotalOrder>(nondet!(
            /// Event/timer interleaving is non-deterministic.
        ));

    merged.scan(
        q!(move || std::time::Instant::now()),
        q!(move |last_event: &mut std::time::Instant, is_event: bool| {
            if is_event {
                *last_event = std::time::Instant::now();
                None
            } else {
                if last_event.elapsed().as_millis() as u64 > timeout_ms {
                    *last_event = std::time::Instant::now();
                    Some(())
                } else {
                    None
                }
            }
        }),
    )
}
