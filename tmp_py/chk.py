p='tests.rs'
s=open(p).read()
s=s.replace('''    let mut check = check;
    let flow = flow.sim().with_cluster_size(&members, MEMBERS);
    let flow = if fragile {
        check.instrument_fragile(flow)
    } else {
        check.instrument(flow)
    };
    let sim = flow.compiled();''','''    let mut check = check;
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();''')
s=s.replace('''fn echo_broadcast_with(
    check: EcCheck,
    fragile: bool,
) -> (Results, Option<Vec<crate::provenance::FragileSite>>) {''','''fn echo_broadcast_with(check: EcCheck) -> (Results, Option<Vec<crate::provenance::FragileSite>>) {''')
a=s.index('''/// Fragile mode (docs/design/hydrofrac-races.md §4.1) only adds ids that lite's lineage skips,''')
b=s.index('''#[cfg(stageleft_runtime)]
#[test]
fn the_crash_cap_ends_the_sweep_with_a_bounded_certificate''')
s=s[:a]+'''/// The instrumentation lists the program's fragile steps: the echo broadcast has none.
#[cfg(stageleft_runtime)]
#[test]
fn the_echo_broadcast_has_no_fragile_step() {
    let (_, sites) = echo_broadcast_with(EcCheck::new());
    assert_eq!(sites, Some(vec![]));
}

'''+s[b:]
open(p,'w').write(s)
p='brute_tests.rs'
s=open(p).read()
s=s.replace('EcCheck::races()','EcCheck::new()')
open(p,'w').write(s)
