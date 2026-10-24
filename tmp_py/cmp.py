import re, sys, collections

RENAMES = {
    # hydrofrac
    'check::tests::a_program_with_a_schedule_choice_is_exhausted_not_certified': 'check::tests::a_schedule_choice_with_robust_derivations_is_certified',
    'check::tests::assert_holds_rejects_an_exhausted_search': 'check::tests::assert_holds_rejects_a_sampled_search',
    'check::tests::fragile_mode_leaves_the_check_unchanged': None,  # deleted (compared lite with lite)
}

def rename(t):
    if t in RENAMES:
        return RENAMES[t]
    t2 = re.sub(r'::both_agree_on_', '::brute_force_agrees_on_', t)
    t2 = re.sub(r'::(races_targets_\w+)_lite$', r'::\1', t2)
    t2 = re.sub(r'::races_planted_local_attestation_lite$', '::races_planted_local_attestation', t2)
    return t2

def parse_baseline(path):
    rows = []
    section = None
    for line in open(path):
        m = re.match(r'## `(\w+)`', line)
        if m:
            section = m.group(1)
        m = re.match(r'\| `([^`]+)`( †)? \| `([^`]+)` \| ([^|]+) \| (\d+) \| (\d+) \| (\d+) \|', line)
        if m and section:
            rows.append(dict(section=section, test=m.group(1), label=m.group(3), verdict=m.group(4).strip(),
                             sets=int(m.group(5)), schedules=int(m.group(6)), wall=int(m.group(7)), races_branch=bool(m.group(2))))
    return rows

def parse_new(paths):
    rows = []
    for section, path in paths:
        for line in open(path):
            if not line.startswith('ROW\t'):
                continue
            _, test, label, verdict, sets, sch, wall, claim_b = line.rstrip('\n').split('\t')
            rows.append(dict(section=section, test=test, label=label, verdict=verdict, sets=int(sets),
                             schedules=int(sch), wall=int(wall), claim_b=claim_b == 'true'))
    return rows

def strength(v):
    """A comparable strength: higher is stronger. Counterexamples and certificates are different
    kinds; the comparison only flags a weakening within the same claim."""
    if v.startswith('CERT(All)'):
        return ('cert', 99)
    m = re.match(r'CERT\(UpTo (\d+)\)', v)
    if m:
        return ('cert', int(m.group(1)))
    m = re.match(r'CEX\((\d+) crashes, (\d+) faults; tol=(None|Some\((\d+)\))( cert)?\)', v)
    if m:
        return ('cex', 1 if m.group(5) else 0)
    if v.startswith('EXH'):
        return ('exh', 0)
    return (v, 0)

def compare(old, new):
    old = old.replace(', panic;', ';')
    so, sn = strength(old), strength(new)
    if old == new:
        return 'same'
    if so[0] == 'cert' and sn[0] == 'cert':
        return 'stronger' if sn[1] > so[1] else 'WEAKER'
    if so[0] == 'cert' and sn[0] != 'cert':
        return 'WEAKER (certificate lost)' if sn[0] != 'cex' else 'cert→cex'
    if so[0] == 'exh' and sn[0] in ('cert', 'cex'):
        return 'stronger'
    if so[0] == 'cex' and sn[0] == 'cex':
        if sn[1] < so[1]:
            return 'WEAKER (tolerance not certified)'
        return 'same claim' if sn[1] == so[1] else 'stronger'
    if so[0] == 'cex' and sn[0] == 'cert':
        return 'cex→cert'
    if so[0] == 'cex' and sn[0] == 'exh':
        return 'WEAKER (cex lost)'
    return f'{old} → {new}'

base = parse_baseline(sys.argv[1])
new = parse_new([('hydrofrac', sys.argv[2]), ('hydro_std', sys.argv[3]), ('hydro_test', sys.argv[4])])
by_test = collections.defaultdict(list)
for r in new:
    by_test[(r['section'], r['test'])].append(r)
used = collections.Counter()
out = []
missing = []
for b in base:
    t = rename(b['test'])
    if t is None:
        missing.append((b, 'test deleted'))
        continue
    cands = by_test.get((b['section'], t.split('::')[-1]), [])
    # the k-th occurrence of this label in this test
    key = (b['section'], t.split('::')[-1], b['label'])
    k = used[key]
    matches = [r for r in cands if r['label'] == b['label']]
    if k >= len(matches):
        missing.append((b, f'no new row (test {t})'))
        continue
    used[key] += 1
    n = matches[k]
    ratio = n['schedules'] / b['schedules'] if b['schedules'] else (1.0 if not n['schedules'] else float('inf'))
    sratio = n['sets'] / b['sets'] if b['sets'] else (1.0 if not n['sets'] else float('inf'))
    out.append((b, n, compare(b['verdict'], n['verdict']), sratio, ratio))

print(f'baseline rows {len(base)}, matched {len(out)}, unmatched {len(missing)}, new rows {len(new)}')
kinds = collections.Counter(c for _, _, c, _, _ in out)
print('verdicts:', dict(kinds))
tot_b = sum(b['schedules'] for b, *_ in out); tot_n = sum(n['schedules'] for _, n, *_ in out)
tot_bs = sum(b['sets'] for b, *_ in out); tot_ns = sum(n['sets'] for _, n, *_ in out)
print(f'total sets {tot_bs} -> {tot_ns}, schedules {tot_b} -> {tot_n}')
print(f'>2x schedules: {sum(1 for *_, r in out if r > 2)}; >2x sets: {sum(1 for *_, s, _ in out if s > 2)}')
mode = sys.argv[5] if len(sys.argv) > 5 else 'flags'
print()
print('| test | label | lite verdict | new verdict | comparison | lite sets / schedules | new sets / schedules | sets × | schedules × |')
print('|---|---|---|---|---|---:|---:|---:|---:|')
for b, n, c, s, r in out:
    flag = c.startswith('WEAKER') or s > 2 or r > 2 or c not in ('same', 'same claim')
    if mode == 'all' or flag:
        mark = lambda x: f'**{x:.2f}**' if x > 2 else f'{x:.2f}'
        print(f"| `{b['test']}` | `{b['label']}` | {b['verdict']} | {n['verdict']} | {c} | {b['sets']} / {b['schedules']} | {n['sets']} / {n['schedules']} | {mark(s)} | {mark(r)} |")
print()
for b, why in missing:
    print(f"UNMATCHED `{b['test']}` `{b['label']}` {b['verdict']}: {why}")
