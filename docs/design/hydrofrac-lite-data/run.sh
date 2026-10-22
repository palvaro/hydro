#!/bin/bash
# usage: run.sh NAME  (env vars set by caller)
source $TMPDIR/env.sh
cd $TMPDIR/tip
N=$1
date +%s.%N > $TMPDIR/start-$N
cargo test --no-fail-fast -p hydro_lang -p hydro_std -p hydro_test --lib -- sim:: ec_inference_demos ldfi ec_all_labels_classes isr_replication reliable_broadcast reliable_delivery --test-threads=3 > $TMPDIR/log-$N.txt 2>&1
echo "exit $?" > $TMPDIR/done-$N
date +%s.%N > $TMPDIR/end-$N
