#!/bin/bash
export CARGO_HOME=$TMPDIR/cargohome CARGO_TARGET_DIR=$TMPDIR/t-prof
cd $TMPDIR/prof
cargo test -j4 -p hydro_std --lib --no-run > $TMPDIR/prof-build.log 2>&1 && cargo test -j4 -p hydro_test --lib --no-run >> $TMPDIR/prof-build.log 2>&1
echo BUILD $? >> $TMPDIR/prof-build.log
touch $TMPDIR/prof-built
