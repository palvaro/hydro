# `relay_broadcast` under Sedge

`hydro_test/src/cluster/relay_broadcast.rs`: one `Source` process, 5 `Member`s, `max_merges_per_tick = 7`,
50 ms timers, μ = 28, channel `FailStop`. Stream kinds come from Cress (`./scripts/cress hydro_test
relay_broadcast`, written to `target/cress/conservation.tsv`). Run Cress first: without that table,
`delivered` (a `u64` id stream) is read as a level, and its mean id always rises.

Command: `./scripts/sedge --factors 2,4,8 --trials 1 --tail 60 hydro_test relay_broadcast` (seed 1,
baseline [5,10), trigger [10,15) at 2μ, tail verdict window [45,75)).

| config    | f=2       | f=4      | f=8       |
|-----------|-----------|----------|-----------|
| echo_only | RECOVERED | RECOVERED | RECOVERED |
| resend_on | COLLAPSED | DRAINING | RECOVERED |

This matches the design's f_c ≈ C/(C−R) ≈ 2.3. Under the default 23 s tail, `echo_only` f=2 is
DRAINING (inbox noise, mean 13→20) and `resend_on` f=8 is DRAINING (its backlog takes about 27 s to
clear, see `resend_on_f8.svg`).

- `resend_on_f2.svg`: merged is pinned at 700/s (5 × 7 × 20, saturated) for the whole tail. Wire is
  671/s against 270/s at baseline (2.5×), and inbox depth rises from 7435 to 7838.
- `resend_on_f8.svg`: the backlog (about 6000 inbox, 3800 outstanding) drains by about second 42,
  and the tail rates return to baseline.
- `echo_only_f2.svg`: no amplification. Wire stays at 20 per round, and the trigger backlog drains
  by about second 27.

The plots were made with `scripts/sedge_plot.py TRACE OUT "title" --trigger 10,15 --tail-window 45,75 --rates delivered`.
The traces and `results.tsv` are in this directory.
