#!/bin/bash
# usage: suite.sh <srcdir> <targetdir> <tag>
export CARGO_HOME=$TMPDIR/cargohome
cd $1
export CARGO_TARGET_DIR=$2
export HF_DUMP=$TMPDIR/dump-$3.tsv
rm -f $HF_DUMP
L=$TMPDIR/suite-$3.log
{
date +%s
cargo test -j8 -p hydrofrac --features cross-check --lib --no-fail-fast; echo EXIT1 $?
cargo test -j8 -p hydro_std --lib --no-fail-fast hydrofrac; echo EXIT2 $?
cargo test -j8 -p hydro_test --lib --no-fail-fast cluster::ec_; echo EXIT3 $?
date +%s
echo SUITE_DONE
} > $L 2>&1
