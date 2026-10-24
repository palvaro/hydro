#!/bin/bash
cd $TMPDIR/lite
export CARGO_HOME=$TMPDIR/cargohome CARGO_TARGET_DIR=$TMPDIR/t-lite HF_DUMP=$TMPDIR/dump-lite1.tsv
cargo test -j8 -p hydro_test --lib --no-fail-fast -- payloads over_two_members over_three_members > $TMPDIR/lport.log 2>&1
echo DONE $? >> $TMPDIR/lport.log
