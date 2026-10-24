#!/bin/bash
# usage: build.sh <srcdir> <targetdir> <log>
export CARGO_HOME=$TMPDIR/cargohome
cd $1
export CARGO_TARGET_DIR=$2
{ cargo test -j8 -p hydrofrac --features cross-check --lib --no-run && cargo test -j8 -p hydro_std --lib --no-run && cargo test -j8 -p hydro_test --lib --no-run; echo BUILD_EXIT $?; } > $3 2>&1
