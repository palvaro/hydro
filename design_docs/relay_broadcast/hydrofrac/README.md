# HydroFrac on `relay_broadcast`

These are the Lamport diagrams that HydroFrac (`MOLLY_VIZ_OUT`) writes for the failing runs of
`hydro_test/src/cluster/relay_broadcast_ldfi.rs`. Each `<test>/members/<collection>/` directory holds:

- `failing.{dot,png}`: the failing run.
- `witness.{dot,png}`: a run where the goal held, with its support.
- `provenance.{dot,png}`: the provenance graph behind the witness.
- `report.md`: a Mermaid version of the same information.

For the setup, the full verdict table, and how these results fit with Cress and Sedge, see
[`../README.md`](../README.md#hydrofrac-details).

The diagrams in this directory come from an earlier revision, where `delivered` was at line 218
and `initial` at line 205 (now lines 250 and 237). The program is the same for these two variants.

Regenerate the diagrams with:

```sh
MOLLY_VIZ_OUT=$PWD/design_docs/relay_broadcast/hydrofrac \
  cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi::tests::relay_none_diverges_under_a_source_crash -- --nocapture --test-threads=1 --exact
MOLLY_VIZ_OUT=$PWD/design_docs/relay_broadcast/hydrofrac \
  cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi::tests::relay_echo_lossy_diverges_under_a_source_crash_and_an_omitted_relay -- --nocapture --test-threads=1 --exact
```
