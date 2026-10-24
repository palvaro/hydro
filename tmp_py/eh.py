p='hydro_test/src/cluster/ec_hydrofrac.rs'
s=open(p).read()
a='''    /// Control: the generated specification separates the protocols. The plain broadcast's
    /// deliveries break at one crash of the broadcaster (exhaustively); the echoing broadcast's
    /// survive every admissible combination of crashes (any number of them, as long as two
    /// members are left correct), over [`ECHO_SCHEDULES`] sampled schedules per hypothesis.'''
b='''    /// Control: the generated specification separates the protocols. The plain broadcast's
    /// deliveries break at one crash of the broadcaster (exhaustively); the echoing broadcast's
    /// survive every admissible combination of crashes (any number of them, as long as two
    /// members are left correct). The echoing broadcast is fragile-free, so each fault set runs
    /// on one schedule even with [`ECHO_SCHEDULES`] sampled per hypothesis, and the certificate
    /// holds.'''
assert a in s; s=s.replace(a,b)
a='''        // The echoing broadcast's members batch what they receive (`reliable_broadcast.rs`'s
        // tick), so the program has schedule choices: the search finds no counterexample for
        // its deliveries under any admissible crash set, but cannot certify them
        // (docs/design/hydrofrac-lite.md §4.9).
        let reliable = echo_broadcast(true, EcCheck::new().sample_schedules(ECHO_SCHEDULES));'''
b='''        // The echoing broadcast's members batch what they receive (`reliable_broadcast.rs`'s
        // tick), so the program has schedule choices, but no fragile step: every derivation is
        // robust, and its deliveries are certified under any admissible crash set
        // (docs/design/hydrofrac-races.md §4.4).
        let reliable = echo_broadcast(true, EcCheck::new().sample_schedules(ECHO_SCHEDULES));'''
assert a in s; s=s.replace(a,b)
a='''        assert!(
            matches!(
                deliveries.verdict,
                Verdict::Exhausted {
                    tolerates: Tolerance::All,
                    ..
                }
            ),
            "reliable broadcast: expected no counterexample under any admissible crash set:\\n{reliable}"
        );'''
b='''        assert!(
            matches!(
                deliveries.verdict,
                Verdict::Certified {
                    tolerates: Tolerance::All
                }
            ),
            "reliable broadcast: expected a certificate under any admissible crash set:\\n{reliable}"
        );'''
assert a in s, 'verdict'; s=s.replace(a,b)
open(p,'w').write(s)
