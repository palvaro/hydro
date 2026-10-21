# Print re-renders of the Lamport diagrams

`failing.png` is the diagram as the visualiser emits it (`neato -n2`, 110 dpi, 9 pt
labels). For the wide diagrams that is unreadable once the PDF scales them to a
column: the lanes are 310–400 pt apart, so a 7-lane multi-Paxos diagram is ~35 in
wide at native scale and a 9 pt label becomes ~2.5 pt on a landscape Letter page.

`failing-print.png` is a re-render of the same `failing.dot` with every
`fontsize` / `POINT-SIZE` multiplied by **1.6**, `arrowsize` × 1.4, `penwidth=1.5`
on nodes and edges, at **160 dpi**. Positions, labels and content are untouched
(`-n2` pins every node), so the aspect ratio is preserved to within the padding.
The multiplier is the largest that does not make stacked labels collide: the
longest labels already span 94 % of a lane gap (377 pt of 400 pt), and the event
rows are 16 pt apart.

Recipe (run inside a case directory):

```sh
python3 - failing.dot <<'PY' > /tmp/print.dot
import re, sys
k = 1.6; s = open(sys.argv[1]).read()
s = re.sub(r'fontsize=(\d+(?:\.\d+)?)', lambda m: 'fontsize=%g' % (float(m.group(1)) * k), s)
s = re.sub(r'POINT-SIZE="(\d+(?:\.\d+)?)"', lambda m: 'POINT-SIZE="%g"' % (float(m.group(1)) * k), s)
s = re.sub(r'arrowsize=(\d+(?:\.\d+)?)', lambda m: 'arrowsize=%g' % (float(m.group(1)) * 1.4), s)
s = s.replace('edge [fontname="Helvetica",', 'edge [penwidth=1.5, fontname="Helvetica",', 1)
s = s.replace('node [fontname="Helvetica",', 'node [penwidth=1.5, fontname="Helvetica",', 1)
sys.stdout.write(s)
PY
neato -n2 -Gdpi=160 -Tpng /tmp/print.dot -o failing-print.png
```

`docs/design/render-inventory.py` substitutes `failing-print.png` for `failing.png`
wherever one exists, and puts figures with aspect ratio > 1.6 on a landscape page.

## Manifest

"Text in PDF" is the on-page size of a 9 pt label (× 1.6 in the print render)
after the figure is fitted to its page: 7 in × 8.5 in for portrait, 9.8 in × 6.8 in
for landscape. Only diagrams wider than 1600 px were re-rendered; none of the
narrow ones is below 400 px tall with sub-10 pt text.

| Case | `failing.png` | `failing-print.png` | Aspect | Page | Text in PDF (before → after) |
|---|---|---|---|---|---|
| `ec_broadcast_closed_cluster_to_cluster` | 1061×450 | — | 2.36 | landscape | 9.1pt |
| `ec_broadcast_closed_lossy` | 806×425 | — | 1.90 | landscape | 12.0pt |
| `ec_isr_all_labels_343_write` | 2199×1941 | 3299×2852 | 1.13 | portrait | 3.2pt → 4.9pt |
| `ec_isr_all_labels_360_replication_fwd` | 2191×1941 | 3258×2852 | 1.13 | portrait | 3.2pt → 5.0pt |
| `ec_isr_all_labels_564_view` | 2191×1941 | 3258×2852 | 1.13 | portrait | 3.2pt → 5.0pt |
| `ec_multi_paxos_labels_281_prepares` | 3841×712 | 5742×1081 | 5.39 | landscape | 2.5pt → 3.9pt |
| `ec_multi_paxos_labels_286_accepts_fwd` | 3841×1397 | 5743×2076 | 2.75 | landscape | 2.5pt → 3.9pt |
| `ec_multi_paxos_labels_549_accepts` | 3841×1397 | 5743×2076 | 2.75 | landscape | 2.5pt → 3.9pt |
| `ec_multi_paxos_labels_582_learner_hop` | 3980×1941 | 5974×2852 | 2.05 | landscape | 2.4pt → 3.8pt |
| `ec_multi_paxos_labels_591_learner_echo` | 3980×2454 | 5974×3598 | 1.62 | landscape | 2.4pt → 3.8pt |
| `ec_reliable_broadcast_inner_echo` | 1200×1476 | — | 0.81 | portrait | 5.7pt |
| `ec_reliable_broadcast_inner_source_hop` | 1208×1476 | — | 0.82 | portrait | 5.7pt |
| `ec_two_pc_all_labels_86_prepare` | 695×401 | — | 1.73 | landscape | 14.0pt |
| `ec_two_pc_all_labels_92_commit` | 773×694 | — | 1.11 | portrait | 9.0pt |

The top-level samples (`../isr_unclean_election` 2048×1745, `../paxos_commit_quorum`
2682×1330, `../plain_broadcast`, `../two_pc`, `../reliable_delivery_gives_up`) ship
only a PNG, with no `.dot`, so they cannot be re-rendered.

## Limits

Scaling the fonts at pinned positions cannot reach 7 pt on a Letter page for the
multi-Paxos diagrams: that needs × 2.7, and the longest labels would then be
1000 pt wide across 400 pt lanes and 23 pt tall across 16 pt rows. Getting there
requires either the visualiser spacing lanes relative to the label width and
font (so the diagram is narrower in points to begin with), or the PDF giving
these figures an oversize page instead of landscape Letter.
