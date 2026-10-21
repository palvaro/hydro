#!/bin/bash
# usage: run1.sh PKG TESTPATH STRAT OUTNAME
source $TMPDIR/env.sh
PKG=$1; T=$2; S=$3; O=$4
cd $TMPDIR/h
s=$(date +%s.%N)
LDFI_BLIND_AS_CERT=1 LDFI_STRATEGY=$S LDFI_MAX_HYPOTHESES=${CAP:-100000} timeout ${TO:-1800} cargo test -q -p $PKG --lib $T -- --exact --nocapture --test-threads=1 > $TMPDIR/logs/$O.log 2>&1
rc=$?
e=$(date +%s.%N)
echo "RC $rc WALL $(echo "$e - $s" | bc)" >> $TMPDIR/logs/$O.log
