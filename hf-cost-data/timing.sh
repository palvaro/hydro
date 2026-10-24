#!/bin/bash
export CARGO_HOME=$TMPDIR/cargohome
STD="ec_inference_demos::ec_abd_hydrofrac::reliable_phase_two_certifies_the_register ec_inference_demos::ec_abd_hydrofrac::the_register_breaks_when_the_client_crashes_mid_phase_two"
TST="cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed_with_two_payloads cluster::ec_bruteforce::brute_force_agrees_on_reliable_broadcast_closed_with_three_payloads cluster::ec_bruteforce::brute_force_agrees_on_two_pc_cooperative_termination cluster::ec_demos_hydrofrac::ec_demo_multi_paxos_all_labels cluster::ec_demos_hydrofrac::ec_demo_g_set_gossip_certifies cluster::ec_demos_hydrofrac::ec_demo_reliable_broadcast_certifies"
for run in 1 2 3; do
for side in lite races; do
  if [ $side = lite ]; then cd $TMPDIR/lite; else cd $1; fi
  export CARGO_TARGET_DIR=$TMPDIR/t-$side HF_DUMP=$TMPDIR/dump-$side-t$run.tsv
  rm -f $HF_DUMP
  L=$TMPDIR/timing-$side-$run.log
  { cargo test -j8 -p hydro_std --lib --no-fail-fast -- --exact --test-threads=1 $STD; echo EXIT $?
    cargo test -j8 -p hydro_test --lib --no-fail-fast -- --exact --test-threads=1 $TST; echo EXIT $?; } > $L 2>&1
  touch $TMPDIR/done-$side-$run
done; done
touch $TMPDIR/done-all
