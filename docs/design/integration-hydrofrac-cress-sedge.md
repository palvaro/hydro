# HydroFrac + Cress + Sedge in one tree

This tree merges `hydrofrac-input-coverage` (HydroFrac / LDFI) with `wip/sedge-cress-2026-10-15`
(Cress + Sedge).

## How the merge was done

`hydrofrac-input-coverage`'s first commit (`9d0ec39c92`, "sandbox init") is a snapshot of the
HydroFrac tree, which was based on upstream `d3dffb4afa`. The snapshot was laid on top of the
Sedge commit `4de7c45e41`, so it *reverts* the five Cress/Sedge commits
(`58f1036cfe..4de7c45e41`). A plain `git merge` would keep those deletions. The merge therefore
uses the real base explicitly:

```
git merge-recursive d3dffb4afa -- hydrofrac-input-coverage wip/sedge-cress-2026-10-15
```

Hand fixes:

- `hydro_lang/src/sim/mod.rs`, `hydro_test/src/cluster/mod.rs`: kept both sides' module lists.
- `hydro_test/src/cluster/raft.rs`: both branches added the same manual `PartialEq` impls for
  `AppendEntriesRequest` / `RaftRpc`, so one copy of each was removed. The Cress/Sedge entry
  points (`raft_check`, the Sedge entry) now set HydroFrac's new
  `RaftConfig::election_restriction_ignores_term: false`.

## Build environment

The `ldfi` feature pulls in `good_lp` → `highs-sys`. That needs CMake ≥ 3.15 and libclang. On
hosts with only CMake 2.8 and no libclang, install both from pip:

```
python3 -m pip install --target $TMPDIR/py cmake
python3 -m pip install --target $TMPDIR/pyclang libclang
export CMAKE=$TMPDIR/py/cmake/data/bin/cmake
export LIBCLANG_PATH=$TMPDIR/pyclang/clang/native
```

## Commands verified

```
cargo check -p hydro_lang --features sim
cargo check -p hydro_lang --features sim,ldfi,deploy
cargo check -p cress -p sedge -p cress_config_macro -p sedge_macro
cargo check -p hydro_test --features cress
cargo check -p hydro_test --features sedge
cargo check -p hydro_test --tests --features cress,sedge
cargo test  -p cress -p sedge
cargo test  -p hydro_lang --features sim,ldfi,deploy,viz --lib sim::
cargo test  -p hydro_test --lib ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash
cargo test  -p hydro_test --lib distributed::quorum_ldfi
cargo test  -p hydro_test --lib cluster::raft::tests
./scripts/cress hydro_test rpc
./scripts/sedge --config three_attempts --factors 2 --trials 1 --tail 20 hydro_test rpc_with_retries
```

`hydro_test`'s dev-dependency on `hydro_lang` already enables `ldfi`, so every
`cargo test -p hydro_test` run builds HydroFrac.
