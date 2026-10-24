import re
p='ec_races_targets.rs'
s=open(p).read()
s=s.replace('''//! the enumeration cover the same sets. Each program is checked twice, by lite
//! ([`EcCheck::new`]) and by the races mode ([`EcCheck::races`]); both must agree with the
//! enumeration, and the cost lines (`COST ...`, with `--nocapture`) put the search's sets and
//! schedules next to the enumeration's.''','''//! the enumeration cover the same sets. Each program is checked by the search ([`EcCheck::new`]),
//! which must agree with the enumeration, and the cost lines (`COST ...`, with `--nocapture`) put
//! the search's sets and schedules next to the enumeration's.''')
s=s.replace('''/// Which check a cross-check runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Lite,
    Races,
}

/// The check `mode` runs, capped at `max_crashes` crashes.
fn mode_check(mode: Mode, max_crashes: usize) -> EcCheck {
    match mode {
        Mode::Lite => EcCheck::new(),
        Mode::Races => EcCheck::races(),
    }
    .max_crashes(max_crashes)
}''','''/// The check, capped at `max_crashes` crashes.
fn capped(max_crashes: usize) -> EcCheck {
    EcCheck::new().max_crashes(max_crashes)
}''')
s=s.replace('''fn costs(name: &str, mode: Mode, cross: &CrossCheck, wall: std::time::Duration) {''','''fn costs(name: &str, cross: &CrossCheck, wall: std::time::Duration) {''')
s=s.replace('"=== {name} [{mode:?}] ({:.1}s)','"=== {name} ({:.1}s)')
s=s.replace('"COST {name} [{mode:?}] / {}','"COST {name} / {}')
s=s.replace('eprintln!("AGREE {name} [{mode:?}]");','eprintln!("AGREE {name}");')
s=s.replace('eprintln!("DISAGREEMENT {name} [{mode:?}]: {d}");','eprintln!("DISAGREEMENT {name}: {d}");')
s=s.replace('''/// Runs `build` (a cross-check of `max_faults` faults) under `mode`, capped at `max_faults`
/// crashes, prints its costs, and returns it.
fn cross(
    name: &str,
    mode: Mode,
    max_faults: usize,
    build: impl FnOnce(EcCheck, usize) -> CrossCheck,
) -> CrossCheck {
    let start = std::time::Instant::now();
    let cross = build(mode_check(mode, max_faults), max_faults);
    costs(name, mode, &cross, start.elapsed());
    cross
}''','''/// Runs `build` (a cross-check of `max_faults` faults), capped at `max_faults` crashes, prints
/// its costs, and returns it.
fn cross(
    name: &str,
    max_faults: usize,
    build: impl FnOnce(EcCheck, usize) -> CrossCheck,
) -> CrossCheck {
    let start = std::time::Instant::now();
    let cross = build(capped(max_faults), max_faults);
    costs(name, &cross, start.elapsed());
    cross
}''')
s=s.replace('''    let cross = cross(name, Mode::Races, 0, build);''','''    let cross = cross(name, 0, build);''')
# delete lite tests
s=re.sub(r'(?:/// [^\n]*\n)*#\[test\]\nfn races_targets_\w+_lite\(\) \{\n.*?\n\}\n\n', lambda m: ''.join(l+'\n' for l in m.group(0).split('\n') if l.startswith('/// ')) , s, flags=re.S)
s=re.sub(r'fn (races_targets_\w+)_races\(\)', r'fn \1()', s)
s=s.replace('        Mode::Races,\n','')
open(p,'w').write(s)
