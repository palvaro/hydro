#!/bin/bash
export CARGO_HOME=$TMPDIR/cargohome CARGO_TARGET_DIR=$TMPDIR/t-prof
cd $TMPDIR/prof
export HF_PROF=$TMPDIR/prof2.tsv
rm -f $HF_PROF
cargo test -j8 -p hydro_std --lib -- --exact --test-threads=1 ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register > $TMPDIR/profrun2.log 2>&1
cargo test -j8 -p hydro_test --lib -- --exact --test-threads=1 cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed_with_three_payloads cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies >> $TMPDIR/profrun2.log 2>&1
touch $TMPDIR/prof2-done
