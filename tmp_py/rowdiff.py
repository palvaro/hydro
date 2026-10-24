import sys, collections
def load(paths):
    rows = collections.OrderedDict()
    seen = collections.Counter()
    for section, path in paths:
        for line in open(path):
            if not line.startswith('ROW\t'):
                continue
            _, test, label, verdict, sets, sch, wall, claim_b = line.rstrip('\n').split('\t')
            k = (section, test, label)
            seen[k] += 1
            rows[k + (seen[k],)] = (verdict, int(sets), int(sch), claim_b)
    return rows
old = load([('hydrofrac', sys.argv[1]), ('hydro_std', sys.argv[2]), ('hydro_test', sys.argv[3])])
new = load([('hydrofrac', sys.argv[4]), ('hydro_std', sys.argv[5]), ('hydro_test', sys.argv[6])])
changed = 0
print('| suite | test | label | before | after | sets | schedules | claim B |')
print('|---|---|---|---|---|---:|---:|---|')
for k in list(old) + [k for k in new if k not in old]:
    o = old.get(k); n = new.get(k)
    if o == n or (o and n and o[0] == n[0] and o[1] == n[1] and o[2] == n[2] and o[3] == n[3]):
        continue
    changed += 1
    sec, test, label, occ = k
    fmt = lambda r: '—' if r is None else r[0]
    sets = f"{o[1] if o else '—'} → {n[1] if n else '—'}"
    sch = f"{o[2] if o else '—'} → {n[2] if n else '—'}"
    cb = f"{o[3] if o else '—'} → {n[3] if n else '—'}"
    print(f'| {sec} | `{test}` | `{label}` | {fmt(o)} | {fmt(n)} | {sets} | {sch} | {cb} |')
print(f'\n{changed} changed rows of {len(old)} before, {len(new)} after')
