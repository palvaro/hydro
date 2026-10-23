//! The open-loop load generator.

use std::time::Instant;

use crate::profile::{Arrivals, Profile};
use crate::trace::Recorder;

/// How a generator run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Driven {
    /// Rounds offered.
    pub rounds: u64,
    /// Records offered over every round.
    pub offered: u64,
    /// Arrivals whose scheduled time had already passed by more than `late_threshold` when the
    /// generator reached them: the generator itself could not keep up, and the offered load was
    /// lower than the profile says. Any nonzero count should be reported with the trial.
    pub late: u64,
}

/// Offers rounds `0, 1, 2, …` at the arrival times of `profile` under `seed`, calling
/// `send_round` for each and telling `recorder` which records (numbered from zero over the trial)
/// the round carried, timestamped with the round's scheduled arrival; `send_round` returns how many records it wrote. A round is one unit of the
/// program's workload as its Cress attribute describes it: one request for a `load = ...` check,
/// one call of the `round_data` closure otherwise. Never waits on the program: if `send_round`
/// returns an error the run stops and the error is returned with what was offered so far.
///
/// `send_round` is expected to resolve quickly (writing to a socket buffer); if it blocks because
/// the program has stopped reading, arrivals fall behind and are counted in [`Driven::late`].
/// Its second argument says whether to flush: `true` when the next arrival is not due yet (the
/// generator is about to sleep) or this is the last one, `false` when the generator is behind and
/// the next round follows at once. Writing a backlog of rounds and flushing once is what lets one
/// connection offer tens of thousands of rounds per second; the records still leave in arrival
/// order, and no later than the generator's next sleep.
pub async fn drive<F, E>(
    profile: Profile,
    seed: u64,
    recorder: &Recorder,
    mut send_round: F,
) -> Result<Driven, (E, Driven)>
where
    F: AsyncFnMut(u64, bool) -> Result<u64, E>,
{
    let start: Instant = recorder.start();
    let late_threshold = std::time::Duration::from_millis(50);
    let mut driven = Driven {
        rounds: 0,
        offered: 0,
        late: 0,
    };
    let mut arrivals = Arrivals::new(profile, seed).enumerate().peekable();
    while let Some((round, t)) = arrivals.next() {
        let due = start + t;
        let now = Instant::now();
        if now < due {
            tokio::time::sleep_until(due.into()).await;
        } else if now - due > late_threshold {
            driven.late += 1;
        }
        // A request counts as offered at its scheduled arrival, not when the write got through:
        // if the program pushes back (a full socket), the wait is part of the request's latency,
        // as it would be for a user who arrived on schedule (no coordinated omission).
        let at = due;
        let first = driven.offered;
        let flush = arrivals
            .peek()
            .is_none_or(|(_, next)| start + *next > Instant::now());
        match send_round(round as u64, flush).await {
            Ok(n) => {
                recorder.offered_round(at, first, n);
                driven.offered += n;
                driven.rounds += 1;
            }
            Err(e) => return Err((e, driven)),
        }
    }
    // Let the tail run out: arrivals stop at `total`, but the last gap may end early.
    let end = start + profile.total();
    if Instant::now() < end {
        tokio::time::sleep_until(end.into()).await;
    }
    Ok(driven)
}
