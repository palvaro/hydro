//! The open-loop load generator.

use std::time::Instant;

use crate::profile::{Arrivals, Profile};
use crate::trace::Recorder;

/// How a generator run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Driven {
    /// Requests offered.
    pub offered: u64,
    /// Arrivals whose scheduled time had already passed by more than `late_threshold` when the
    /// generator reached them: the generator itself could not keep up, and the offered load was
    /// lower than the profile says. Any nonzero count should be reported with the trial.
    pub late: u64,
}

/// Offers requests `0, 1, 2, …` at the arrival times of `profile` under `seed`, calling `send`
/// for each and telling `recorder` just before. Never waits on the program: if `send` returns an
/// error the run stops and the error is returned with what was offered so far.
///
/// `send` is expected to resolve quickly (writing to a socket buffer); if it blocks because the
/// program has stopped reading, arrivals fall behind and are counted in [`Driven::late`].
pub async fn drive<F, E>(
    profile: Profile,
    seed: u64,
    recorder: &Recorder,
    mut send: F,
) -> Result<Driven, (E, Driven)>
where
    F: AsyncFnMut(u64) -> Result<(), E>,
{
    let start: Instant = recorder.start();
    let late_threshold = std::time::Duration::from_millis(50);
    let mut driven = Driven {
        offered: 0,
        late: 0,
    };
    for (id, t) in Arrivals::new(profile, seed).enumerate() {
        let due = start + t;
        let now = Instant::now();
        if now < due {
            tokio::time::sleep_until(due.into()).await;
        } else if now - due > late_threshold {
            driven.late += 1;
        }
        recorder.offered(id as u64);
        if let Err(e) = send(id as u64).await {
            return Err((e, driven));
        }
        driven.offered += 1;
    }
    // Let the tail run out: arrivals stop at `total`, but the last gap may end early.
    let end = start + profile.total();
    if Instant::now() < end {
        tokio::time::sleep_until(end.into()).await;
    }
    Ok(driven)
}
