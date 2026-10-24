import re, subprocess, sys, os
OLD = '7df7e0753a'
env = dict(os.environ)
def old_lines(path):
    out = subprocess.run(['git', 'show', f'{OLD}:{path}'], capture_output=True, text=True, env=env)
    return out.stdout.split('\n') if out.returncode == 0 else None
def new_lines(path):
    try:
        return open(path).read().split('\n')
    except FileNotFoundError:
        return None
cache = {}
def lines(path, which):
    k = (path, which)
    if k not in cache:
        cache[k] = old_lines(path) if which == 'old' else new_lines(path)
    return cache[k]
def remap(path, n):
    o = lines(path, 'old'); nw = lines(path, 'new')
    if o is None or nw is None or n < 1 or n > len(o):
        return None
    text = o[n - 1].strip()
    if not text:
        # use the next non-empty line as anchor
        k = n
        while k <= len(o) and not o[k - 1].strip():
            k += 1
        if k > len(o):
            return None
        m = remap(path, k)
        return None if m is None else m - (k - n)
    cands = [i + 1 for i, l in enumerate(nw) if l.strip() == text]
    if not cands:
        return None
    return min(cands, key=lambda c: abs(c - n))
doc = open(sys.argv[1]).read()
prefix = {'hf/': 'hydrofrac/src/'}
pat = re.compile(r'(hf/|hydro_lang/|hydro_std/|hydro_test/)([\w/\.\-]+\.rs):(\d+)(?:-(\d+))?')
problems = []
def sub(m):
    pre, rest, a, b = m.group(1), m.group(2), int(m.group(3)), m.group(4)
    path = prefix.get(pre, pre) + rest
    na = remap(path, a)
    if na is None:
        problems.append(m.group(0)); return m.group(0)
    s = f'{pre}{rest}:{na}'
    if b:
        nb = remap(path, int(b))
        if nb is None:
            nb = na + (int(b) - a)
        s += f'-{nb}'
    return s
new = pat.sub(sub, doc)
open(sys.argv[1], 'w').write(new)
changed = sum(1 for x, y in zip(pat.findall(doc), pat.findall(new)) if x != y)
print('refs', len(pat.findall(doc)), 'changed', changed)
for p in problems:
    print('UNRESOLVED', p)
