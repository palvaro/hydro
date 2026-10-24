p='ec_quorum_trust.rs'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:70]
    s=s.replace(a,b)
rep('''//! Trusted `quorum` (docs/design/hydrofrac-races.md §8.2): the races mode trusts''','''//! Trusted `quorum` (docs/design/hydrofrac-races.md §8.2): the check trusts''')
rep('''//! - [`races_urb3_threshold2`]: uniform reliable broadcast over three members, which brute force
//!   cannot exhaust, checked by the races mode alone.
//! - [`races_planted_local_attestation_lite`] / [`races_planted_local_attestation_races`]: a
//!   planted bug through the real `quorum` (members attest only their own receipt), found by both
//!   modes and by brute force.''','''//! - [`races_urb3_threshold2`]: uniform reliable broadcast over three members, which brute force
//!   cannot exhaust, checked by the search alone.
//! - [`races_planted_local_attestation`]: a planted bug through the real `quorum` (members attest
//!   only their own receipt), found by the search and by brute force.''')
rep('''/// item, checked by the races mode alone (up to two crashes).''','''/// item, checked by the search alone (up to two crashes).''')
rep('''    let mut check = EcCheck::races().max_crashes(2);''','''    let mut check = EcCheck::new().max_crashes(2);''')
rep('''#[test]
fn races_planted_local_attestation_lite() {
    planted(EcCheck::new());
}

#[test]
fn races_planted_local_attestation_races() {
    planted(EcCheck::races());
}''','''#[test]
fn races_planted_local_attestation() {
    planted(EcCheck::new());
}''')
open(p,'w').write(s)
