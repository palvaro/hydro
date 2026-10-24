p='hydrofrac/src/check/races_engine.rs'; s=open(p).read()
o="""            let sim = self.sim;
            let sample = self.sample_schedules;
            std::panic::catch_unwind(AssertUnwindSafe(move || match (policy, sample) {"""
assert s.count(o)==1
s=s.replace(o,"""            let sim = self.sim;
            let sample = self.sample_schedules;
            if policy == Policy::All && sample.is_none() {
                refuse_truncated_exhaustive();
            }
            std::panic::catch_unwind(AssertUnwindSafe(move || match (policy, sample) {""")
o="""/// What one run revealed:"""
assert s.count(o)==1
s=s.replace(o,"""/// Panics if bolero would cut an exhaustive exploration short without saying so. With
/// `BOLERO_RANDOM_TEST_TIME_MS` set, bolero's `run_exhaustive` stops at the time limit and
/// returns normally, so an exploration would look complete when it is not (and a certificate
/// built on it would be wrong). Exhaustive runs ignore bolero's other limits.
pub(super) fn refuse_truncated_exhaustive() {
    if let Ok(limit) = std::env::var("BOLERO_RANDOM_TEST_TIME_MS") {
        panic!(
            "hydrofrac: BOLERO_RANDOM_TEST_TIME_MS={limit} would silently cut the exhaustive \\
             exploration short; unset it (or use `sample_schedules`, which never certifies)"
        );
    }
}

/// What one run revealed:""")
open(p,'w').write(s)
p='hydrofrac/src/check/brute_force.rs'; s=open(p).read()
o="""            let sim = self.sim;
            let sample = self.sample_schedules;
            std::panic::catch_unwind(AssertUnwindSafe(move || match sample {"""
assert s.count(o)==1
s=s.replace(o,"""            let sim = self.sim;
            let sample = self.sample_schedules;
            if sample.is_none() {
                super::races_engine::refuse_truncated_exhaustive();
            }
            std::panic::catch_unwind(AssertUnwindSafe(move || match sample {""")
open(p,'w').write(s)
print('ok')
