//! Minimal pure-heartbeat negative control for feedback-hazard research.
//!
//! Each timer event emits one fixed-size message to every member. There are no
//! acknowledgements, retries, outstanding-work state, or response path.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// A fixed-size liveness signal. Sequence numbers aid observation only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Heartbeat {
    pub sequence: u64,
}

/// Emits exactly one heartbeat per timer event at each member, then broadcasts
/// it to the closed cluster membership.
///
/// The check has no `workload_rounds`: the timer is the program's only input, so there is no
/// data to confine and the growth label would say "not read" by construction (it is benign
/// in any case).
#[cfg_attr(feature = "cress", cress::amplification_check(Node = (), cluster = 3))]
pub fn pure_heartbeat<'a, Node: 'a>(
    cluster: &Cluster<'a, Node>,
    timer: Stream<(), Cluster<'a, Node>, Unbounded, TotalOrder, ExactlyOnce>,
) -> KeyedStream<
    MemberId<Node>,
    Heartbeat,
    Cluster<'a, Node, EventualConsistency>,
    Unbounded,
    TotalOrder,
    ExactlyOnce,
> {
    timer
        .enumerate()
        .map(q!(|(sequence, ())| Heartbeat {
            sequence: sequence as u64,
        }))
        .broadcast_closed(cluster, TCP.fail_stop().bincode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_gain_without_feedback() {
        const MEMBERS: usize = 3;
        const PULSES: usize = 4;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer)
            .entries()
            .sim_cluster_output();

        flow.sim().with_cluster_size(&cluster, MEMBERS).exhaustive(
            async || {
                for member in 0..MEMBERS as u32 {
                    for _ in 0..PULSES {
                        timer_send.send(member, ());
                    }
                }
                for member in 0..MEMBERS as u32 {
                    let messages = received.collect_sorted::<Vec<_>>(member).await;
                    assert_eq!(messages.len(), PULSES * MEMBERS);
                }
            },
        );
    }
}
