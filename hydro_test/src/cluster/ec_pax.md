# EventuallyConsistent Paxos

Consider a (more or less) classic Paxos/Raft implementation in Hydro.

Please feel free to dispute any of the following claims:

 1. the stream of proposals (view, leader_id, slot, ballot_no) is EC (via member_broadcast; every member can broadcast to the same collection).
 2. the stream of accepts (view, leader_id, slot, ballot_no, value) is also EC (same argument).
 3. the singleton "highest ballot_no seen" across proposal and accept streams is also EC.
 4. A replica can ACK a proposal in the stream, provided that it is the highest ballot seen, but must include its accepted values for the slot. (p2p, not EC, but also highly ephemeral! a different batching means different acked proposals!)
 5. A replica can ACK an accept in the stream, provided it is the highest ballot seen; it accept that value and ballot for the slot.
 6. At a  leader, (and here is the crazy part) if you made it through phase 1 and 2 with ACKS, you *happen to know* that a quorum of nodes have accepted your value for that slot, and that *any future leader* will do the same. so, you can write to the committed log.
 7. a final view over the committed log can project away leaders, knowing slot # is unique.

 I guess the question is, how do (1) and (2) help, if at all, with 6?  Ideally I could show that the logic at 6 is downstream deterministic from EC streams...?

 I guess one important detail is that *batching effects what is chosen, NOT agreement!* Moving a batch boundary one way may make a later proposal win; moving it the other way will make the later proposal causally later also, so it sees the old proposal's view.