p='ec_abd_hydrofrac.rs'
s=open(p).read()
assert 'hydrofrac::EcCheck::races().max_crashes(0)' in s
s=s.replace('hydrofrac::EcCheck::races().max_crashes(0)','hydrofrac::EcCheck::new().max_crashes(0)')
open(p,'w').write(s)
p='ec_abd_kv_hydrofrac.rs'
s=open(p).read()
a=s.index('#[cfg(stageleft_runtime)]\n#[test]\n#[ignore = "measurement"]\nfn abd_kv_two_writes_lite()')
b=s.index('/// `EcCheck::races()`, exhaustive per hypothesis unless `ABD_KV_SAMPLES` is set.')
s=s[:a]+s[b:]
s=s.replace('''/// `EcCheck::races()`, exhaustive per hypothesis unless `ABD_KV_SAMPLES` is set.''','''/// `EcCheck::new()`, exhaustive per hypothesis unless `ABD_KV_SAMPLES` is set.''')
s=s.replace('''fn abd_kv_two_writes_races() {
    let mut check = hydrofrac::EcCheck::races();''','''fn abd_kv_two_writes() {
    let mut check = hydrofrac::EcCheck::new();''')
s=s.replace('''    ec_check(check, "races()");''','''    ec_check(check, "new()");''')
open(p,'w').write(s)
