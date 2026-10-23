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

`docs/design/render-design-doc.py` substitutes `failing-print.png` for `failing.png`
wherever one exists, and places each figure from its natural size in inches
(pixels / dpi; 160 dpi for `failing-print.png`, 110 dpi for `failing.png`, else the
`-Gdpi=` in a sibling `.dot`), never upscaling:

* natural width ≤ 7 in — inline in the portrait flow at natural size;
* wider, but a 9 pt label stays ≥ 7 pt when shrunk to the 7 in column (natural
  width ≤ 9 in) — inline, shrunk to the column;
* natural width ≤ 9.8 in — own landscape Letter page;
* wider — an **oversize page** sized to the figure (`@page fig-N`, 0.4 in margins,
  room for the caption), the image drawn at natural size capped at 30 in wide. The
  page is for zooming on screen, not for a Letter printer.

## Manifest

"Text in PDF" is the on-page size of a 9 pt label (× 1.6 in the print render) at
the drawn size. Only diagrams wider than 1600 px were re-rendered; none of the
narrow ones is below 400 px tall with sub-10 pt text.

| Case | `failing.png` | `failing-print.png` | Natural size | Placement | Drawn | Text in PDF (before → after) |
|---|---|---|---|---|---|---|
| `ec_broadcast_closed_cluster_to_cluster` | 1061×450 | — | 9.6×4.1 in | landscape Letter | 9.6×4.1 in | 9.0pt |
| `ec_broadcast_closed_lossy` | 806×425 | — | 7.3×3.9 in | (not in the inventory) inline, shrunk to 7 in | 7.0×3.7 in | 8.6pt |
| `ec_isr_all_labels_343_write` | 2199×1941 | 3299×2852 | 20.6×17.8 in | oversize 21.4×19.4 in | 20.6×17.8 in | 3.2pt → 14.4pt |
| `ec_isr_all_labels_360_replication_fwd` | 2191×1941 | 3258×2852 | 20.4×17.8 in | (not in the inventory) oversize 21.2×19.4 in | 20.4×17.8 in | 3.2pt → 14.4pt |
| `ec_isr_all_labels_564_view` | 2191×1941 | 3258×2852 | 20.4×17.8 in | oversize 21.2×19.4 in | 20.4×17.8 in | 3.2pt → 14.4pt |
| `ec_multi_paxos_labels_281_prepares` | 3841×712 | 5742×1081 | 35.9×6.8 in | oversize 30.8×7.0 in | 30.0×5.6 in | 2.5pt → 12.0pt |
| `ec_multi_paxos_labels_286_accepts_fwd` | 3841×1397 | 5743×2076 | 35.9×13.0 in | (not in the inventory) oversize 30.8×12.2 in | 30.0×10.8 in | 2.5pt → 12.0pt |
| `ec_multi_paxos_labels_549_accepts` | 3841×1397 | 5743×2076 | 35.9×13.0 in | oversize 30.8×12.2 in | 30.0×10.8 in | 2.5pt → 12.0pt |
| `ec_multi_paxos_labels_582_learner_hop` | 3980×1941 | 5974×2852 | 37.3×17.8 in | oversize 30.8×15.9 in | 30.0×14.3 in | 2.4pt → 11.6pt |
| `ec_multi_paxos_labels_591_learner_echo` | 3980×2454 | 5974×3598 | 37.3×22.5 in | oversize 30.8×19.5 in | 30.0×18.1 in | 2.4pt → 11.6pt |
| `ec_reliable_broadcast_inner_echo` | 1200×1476 | — | 10.9×13.4 in | oversize 11.7×15.4 in | 10.9×13.4 in | 5.7pt → 9.0pt |
| `ec_reliable_broadcast_inner_source_hop` | 1208×1476 | — | 11.0×13.4 in | oversize 11.8×15.4 in | 11.0×13.4 in | 5.7pt → 9.0pt |
| `ec_two_pc_all_labels_86_prepare` | 695×401 | — | 6.3×3.6 in | inline, natural size | 6.3×3.6 in | 14.0pt → 9.0pt (no upscaling) |
| `ec_two_pc_all_labels_92_commit` | 773×694 | — | 7.0×6.3 in | inline, natural size | 7.0×6.3 in | 9.0pt |

The top-level samples ship only a PNG, with no `.dot`, so they cannot be
re-rendered; they are placed by the same rule at 110 dpi:

| Figure | PNG | Natural size | Placement | Drawn | Text in PDF |
|---|---|---|---|---|---|
| `../figures/reliable_broadcast_regions` (150 dpi per its `.dot`) | 1229×1054 | 8.2×7.0 in | inline, shrunk to 7 in | 7.0×6.0 in | — (dataflow graph) |
| `../plain_broadcast` | 967×303 | 8.8×2.8 in | inline, shrunk to 7 in | 7.0×2.2 in | 7.2pt |
| `../two_pc` | 666×450 | 6.1×4.1 in | inline, natural size | 6.1×4.1 in | 9.0pt |
| `../isr_unclean_election` | 2048×1745 | 18.6×15.9 in | oversize 19.4×17.5 in | 18.6×15.9 in | 3.8pt → 9.0pt |
| `../paxos_commit_quorum` | 2682×1330 | 24.4×12.1 in | oversize 25.2×13.7 in | 24.4×12.1 in | 3.3pt → 9.0pt |
| `../reliable_delivery_gives_up` | 822×254 | 7.5×2.3 in | inline, shrunk to 7 in | 7.0×2.2 in | 8.4pt |

## Limits

Scaling the fonts at pinned positions cannot reach 7 pt on a Letter page for the
multi-Paxos diagrams: that needs × 2.7, and the longest labels would then be
1000 pt wide across 400 pt lanes and 23 pt tall across 16 pt rows. Getting there
requires either the visualiser spacing lanes relative to the label width and
font (so the diagram is narrower in points to begin with), or the PDF giving
these figures an oversize page instead of landscape Letter — which is what
`render-design-doc.py` now does: on a 30.8 in wide page the multi-Paxos labels are
11.6–12 pt, readable when zoomed on screen, but the page itself no longer fits a
Letter printer.
