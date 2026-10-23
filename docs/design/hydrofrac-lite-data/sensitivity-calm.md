# Schedule sensitivity per corpus label: conservative walk vs order-insensitivity analysis

Generated from `hydro_test/tests/ec_schedule_sensitivity.rs` (`corpus_table`, `--nocapture`); see
docs/design/hydrofrac-lite.md §4.9, "Which choices matter". `old`: `Analysis::Conservative`; `new`:
`Analysis::Calm`; the last column is the first site the new analysis counts, in the label's own cone if any,
else through a sender ("reorders a sender"). Two labels not shown (`closed broadcast` `out`, `hydro_std Live`
`fan_out_from_process`) are site-free under both.

| program | label (op) | old | new | new, own cone (U1) | first counted site (operator on the path) |
|---|---|---|---|---|---|
| std ReliableClosed | initial (AssertIsConsistent) | no | no | no | - |
| std ReliableClosed | echo (AssertIsConsistent) | no | no | no | - |
| std ReliableClosed | new_messages (Unique) | no | no | no | - |
| std ReliableLive | fan_out_from_process (AssertIsConsistent) | no | no | no | - |
| std ReliableLive | new_messages (Unique) | no | no | no | - |
| std ReliableLive | fan_out (AssertIsConsistent) | no | no | no | - |
| std Uniform | initial (AssertIsConsistent) | no | no | no | - |
| std Uniform | echo (AssertIsConsistent) | yes | no | no | - |
| std Uniform | distinct_acks (Unique) | yes | no | no | - |
| std Uniform | certified (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 |
| std Uniform | uniform_reliable_broadcast_closed (Map) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 |
| g_set_gossip | initial (AssertIsConsistent) | yes | yes | no | cannot see through unordered_external_input at ec_schedule_sensitivity.rs:133 (reorders a sender) |
| g_set_gossip | all_elements (Chain) | yes | yes | yes | batch at crdt_gossip.rs:70 via Scan at crdt_gossip.rs:79 |
| g_set_gossip | gset_state (AssertIsConsistent) | yes | yes | yes | fold_unordered at crdt_gossip.rs:57 via the label (not a stream outside a tick) |
| g_set_gossip | echo (AssertIsConsistent) | yes | yes | yes | batch at crdt_gossip.rs:70 via Scan at crdt_gossip.rs:79 |
| g_set_gossip_live | fan_out (AssertIsConsistent) | yes | yes | yes | batch at crdt_gossip.rs:154 via Scan at crdt_gossip.rs:163 |
| g_set_gossip_live | all_elements (Chain) | yes | yes | yes | batch at crdt_gossip.rs:154 via Scan at crdt_gossip.rs:163 |
| g_set_gossip_live | gset_state (AssertIsConsistent) | yes | yes | yes | fold_unordered at crdt_gossip.rs:141 via the label (not a stream outside a tick) |
| leader_merge_broadcast | leader_merge_broadcast (AssertIsConsistent) | yes | yes | no | observe_nondet at leader_merge.rs:97 (reorders a sender) |
| multi_paxos | prepares (AssertIsConsistent) | yes | yes | no | fold_unordered at quorum.rs:144 (reorders a sender) |
| multi_paxos | accepts_fwd (CycleSource) | yes | yes | yes | fold_unordered at multi_paxos.rs:477 via FlatMap reading state at multi_paxos.rs:503 (upstream) |
| multi_paxos | accepts (AssertIsConsistent) | yes | yes | yes | fold_unordered at multi_paxos.rs:477 via FlatMap reading state at multi_paxos.rs:503 (upstream) |
| multi_paxos | learned_at_proposers (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 |
| multi_paxos | learned_at_proposers (Map) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 |
| multi_paxos | initial (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 (upstream) |
| multi_paxos | echo (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 (upstream) |
| multi_paxos | learned (Unique) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 (upstream) |
| multi_paxos | facts (FlatMap) | yes | yes | yes | fold_unordered at quorum.rs:144 via FlatMap reading state at quorum.rs:158 (upstream) |
| multi_paxos | splice_epoch_log (AssertIsConsistent) | yes | yes | yes | fold_unordered at epoch_splice.rs:190 via the label (not a stream outside a tick) |
| abd_register | phase2_fwd (CycleSource) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register | abd_register_state (FilterMap) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register | abd_register_state (AssertIsConsistent) | yes | yes | yes | fold_unordered at abd.rs:147 via the label (not a stream outside a tick) |
| abd_register | queries (AssertIsConsistent) | yes | yes | no | fold_unordered at quorum.rs:360 (reorders a sender) |
| abd_register | phase2 (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register_reliable_phase2 | phase2_fwd (CycleSource) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register_reliable_phase2 | abd_register_state (FilterMap) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register_reliable_phase2 | abd_register_state (AssertIsConsistent) | yes | yes | yes | fold_unordered at abd.rs:147 via the label (not a stream outside a tick) |
| abd_register_reliable_phase2 | queries (AssertIsConsistent) | yes | yes | no | fold_unordered at quorum.rs:360 (reorders a sender) |
| abd_register_reliable_phase2 | initial (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| abd_register_reliable_phase2 | echo (AssertIsConsistent) | yes | yes | yes | fold_unordered at quorum.rs:360 via AntiJoin at quorum.rs:380 (upstream) |
| hydro_test broadcast (plain) | initial (AssertIsConsistent) | no | no | no | - |
| hydro_test broadcast (plain) | echoes (CycleSource) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| hydro_test broadcast (plain) | echo (AssertIsConsistent) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| hydro_test broadcast (plain) | deliveries (AssertIsConsistent) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| hydro_test broadcast (echo) | initial (AssertIsConsistent) | no | no | no | - |
| hydro_test broadcast (echo) | echoes (CycleSource) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| hydro_test broadcast (echo) | echo (AssertIsConsistent) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| hydro_test broadcast (echo) | deliveries (AssertIsConsistent) | yes | yes | no | batch at reliable_broadcast.rs:67 (reorders a sender) |
| two_pc | prepares (AssertIsConsistent) | yes | yes | no | fold_unordered at two_pc_ctp.rs:188 (reorders a sender) |
| two_pc | commits (AssertIsConsistent) | yes | yes | yes | fold_unordered at two_pc_ctp.rs:188 via Cast of a KeyedSingleton at two_pc_ctp.rs:196 (upstream) |
| two_pc | peer_in (CycleSource) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
| two_pc | peer (AssertIsConsistent) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
| two_pc | committed (AssertIsConsistent) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
| two_pc (ctp) | prepares (AssertIsConsistent) | yes | yes | no | fold_unordered at two_pc_ctp.rs:188 (reorders a sender) |
| two_pc (ctp) | commits (AssertIsConsistent) | yes | yes | yes | fold_unordered at two_pc_ctp.rs:188 via Cast of a KeyedSingleton at two_pc_ctp.rs:196 (upstream) |
| two_pc (ctp) | peer_in (CycleSource) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
| two_pc (ctp) | peer (AssertIsConsistent) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
| two_pc (ctp) | committed (AssertIsConsistent) | yes | yes | yes | batch at two_pc_ctp.rs:105 via JoinHalf at two_pc_ctp.rs:156 |
