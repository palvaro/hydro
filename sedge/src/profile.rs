//! Offered load over time, and Poisson arrivals that follow it.

use std::time::Duration;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Offered load as a function of time since the start of a trial: a baseline rate, a window at a
/// higher rate (the trigger), and the baseline again for the tail. All rates are requests per
/// second.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Profile {
    /// Requests per second outside the trigger window.
    pub baseline_rps: f64,
    /// Requests per second inside the trigger window.
    pub trigger_rps: f64,
    /// How long the baseline runs before the trigger. Long enough for the pre-trigger window to
    /// be a fair picture of steady state.
    pub before: Duration,
    /// How long the trigger lasts.
    pub trigger: Duration,
    /// How long the baseline runs after the trigger. Must cover several timeouts of the program
    /// under test, or a run that is still draining cannot be told from one that collapsed.
    pub tail: Duration,
}

impl Profile {
    /// Start of the trigger window.
    pub fn trigger_start(&self) -> Duration {
        self.before
    }

    /// End of the trigger window.
    pub fn trigger_end(&self) -> Duration {
        self.before + self.trigger
    }

    /// Length of the whole trial.
    pub fn total(&self) -> Duration {
        self.before + self.trigger + self.tail
    }

    /// The offered rate at `t`.
    pub fn rate_at(&self, t: Duration) -> f64 {
        if t >= self.trigger_start() && t < self.trigger_end() {
            self.trigger_rps
        } else {
            self.baseline_rps
        }
    }

    /// The next instant after `t` at which the rate changes, if any.
    fn next_change(&self, t: Duration) -> Option<Duration> {
        [self.trigger_start(), self.trigger_end()]
            .into_iter()
            .find(|&b| b > t)
    }

    /// Expected number of requests over the whole trial.
    pub fn expected_requests(&self) -> f64 {
        self.baseline_rps * (self.before + self.tail).as_secs_f64()
            + self.trigger_rps * self.trigger.as_secs_f64()
    }
}

/// A sampler of Poisson arrival times following a [`Profile`]. Inter-arrival times are
/// exponential at the current rate; when the rate changes mid-gap the gap is cut at the change
/// and re-sampled at the new rate, which is the memoryless property and so is still exact.
#[derive(Debug)]
pub struct Arrivals {
    profile: Profile,
    rng: StdRng,
    t: Duration,
}

impl Arrivals {
    /// Arrivals for `profile`, seeded so that a trial can be repeated exactly on the generator's
    /// side (the program's timing is still real and will differ).
    pub fn new(profile: Profile, seed: u64) -> Self {
        Self {
            profile,
            rng: StdRng::seed_from_u64(seed),
            t: Duration::ZERO,
        }
    }

    /// The time of the next arrival, or `None` once the trial is over.
    pub fn next_arrival(&mut self) -> Option<Duration> {
        let total = self.profile.total();
        loop {
            if self.t >= total {
                return None;
            }
            let rate = self.profile.rate_at(self.t);
            let gap = if rate <= 0.0 {
                // No arrivals at this rate: jump to the next change or the end.
                match self.profile.next_change(self.t) {
                    Some(c) => {
                        self.t = c;
                        continue;
                    }
                    None => return None,
                }
            } else {
                // Exponential with mean 1/rate. `random::<f64>()` is in [0, 1); use 1 − u so the
                // argument to ln is in (0, 1].
                let u: f64 = self.rng.random();
                Duration::from_secs_f64(-(1.0 - u).ln() / rate)
            };
            let candidate = self.t + gap;
            match self.profile.next_change(self.t) {
                Some(c) if candidate >= c => {
                    // The rate changes before this arrival would have happened; restart the
                    // gap at the change.
                    self.t = c;
                }
                _ => {
                    if candidate >= total {
                        self.t = total;
                        return None;
                    }
                    self.t = candidate;
                    return Some(candidate);
                }
            }
        }
    }
}

impl Iterator for Arrivals {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        self.next_arrival()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> Profile {
        Profile {
            baseline_rps: 100.0,
            trigger_rps: 1000.0,
            before: Duration::from_secs(10),
            trigger: Duration::from_secs(5),
            tail: Duration::from_secs(20),
        }
    }

    #[test]
    fn rate_follows_the_windows() {
        let p = profile();
        assert_eq!(p.rate_at(Duration::from_secs(0)), 100.0);
        assert_eq!(p.rate_at(Duration::from_secs(10)), 1000.0);
        assert_eq!(p.rate_at(Duration::from_millis(14_999)), 1000.0);
        assert_eq!(p.rate_at(Duration::from_secs(15)), 100.0);
        assert_eq!(p.total(), Duration::from_secs(35));
    }

    #[test]
    fn arrivals_are_increasing_and_counts_match_rates() {
        let p = profile();
        let times: Vec<Duration> = Arrivals::new(p, 7).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]));
        assert!(times.iter().all(|&t| t < p.total()));
        let in_window =
            |a: Duration, b: Duration| times.iter().filter(|&&t| t >= a && t < b).count() as f64;
        let before = in_window(Duration::ZERO, p.trigger_start());
        let trigger = in_window(p.trigger_start(), p.trigger_end());
        let tail = in_window(p.trigger_end(), p.total());
        // Poisson counts: mean 1000, 5000, 2000; allow five standard deviations.
        assert!(
            (before - 1000.0).abs() < 5.0 * 1000f64.sqrt(),
            "before: {before}"
        );
        assert!(
            (trigger - 5000.0).abs() < 5.0 * 5000f64.sqrt(),
            "trigger: {trigger}"
        );
        assert!((tail - 2000.0).abs() < 5.0 * 2000f64.sqrt(), "tail: {tail}");
    }

    #[test]
    fn same_seed_same_arrivals() {
        let a: Vec<_> = Arrivals::new(profile(), 3).collect();
        let b: Vec<_> = Arrivals::new(profile(), 3).collect();
        assert_eq!(a, b);
    }
}
