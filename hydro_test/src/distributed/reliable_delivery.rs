//! Reliable delivery over a lossy network (the first example of the LDFI paper): a sender
//! must get one message to a receiver. The property is
//!
//! > if a host sends a message, and not all messages are dropped, then the message is
//! > eventually delivered.
//!
//! "Not all messages are dropped" is the end of finite failures (`EFF`): after some round
//! the network delivers everything. The only protocol that satisfies the property retries
//! until it is acknowledged. A protocol that gives up after `n` attempts is wrong: dropping
//! those `n` messages leaves nothing to deliver once the network heals. Dropping acks never
//! matters for this property (the receiver already has the message), which is what the
//! lineage must reflect: acks are outputs of the execution that satisfied the goal, not
//! inputs to it.
//!
//! Both channels are `lossy_delayed_forever`, so their outputs are `NoOrder` and their
//! receiving `batch` operators are the fault points the LDFI harness drives.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim_hooks::{BatchHook, OnProcess};
use serde::{Deserialize, Serialize};

pub struct Sender {}

pub struct Receiver {}

/// The message; `attempt` is the timer round that sent it, so retransmissions are distinct.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Msg {
    pub attempt: u32,
}

/// The receiver's acknowledgment of the message sent in round `attempt`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ack {
    pub attempt: u32,
}

/// Builds the protocol. `timer` carries the round number each time the sender's retry
/// timer fires; the sender transmits in round `t` unless it has been acknowledged or
/// `t` exceeds `max_attempts` (`None` retries until acknowledged). Returns the messages
/// the receiver received, which is where the delivery property is observed.
///
/// # Non-Determinism
/// - `nondet_messages`: the batching of messages at the receiver. Over a lossy channel this
///   is where omissions manifest.
/// - `nondet_acks`: the batching of acks at the sender, likewise.
pub fn reliable_delivery<'a>(
    sender: &Process<'a, Sender>,
    receiver: &Process<'a, Receiver>,
    timer: Stream<u32, Process<'a, Sender>>,
    max_attempts: Option<u32>,
    nondet_messages: NonDet<Option<BatchHook<Msg, NoOrder, ExactlyOnce, OnProcess<Receiver>>>>,
    nondet_acks: NonDet<Option<BatchHook<Ack, NoOrder, ExactlyOnce, OnProcess<Sender>>>>,
) -> Stream<Msg, Process<'a, Receiver>, Unbounded, NoOrder> {
    let (acks_handle, acks) =
        sender.forward_ref::<Stream<Ack, Process<'a, Sender>, Unbounded, NoOrder>>();

    // Captured into the staged closure below (stageleft captures plain integers, not
    // `Option`s).
    let attempt_cap: u32 = max_attempts.unwrap_or(u32::MAX);

    let messages: Stream<Msg, Process<'a, Sender>> = sliced! {
        let timer_batch = use::batch(timer, nondet!(
            /// When a timer round is processed relative to an in-flight ack only decides
            /// whether one more (redundant) message goes out.
        ));
        let ack_batch = use::batch(acks, nondet_acks);
        let mut acked = use::state(|l| l.singleton(q!(false)));

        let acked_now = acked
            .clone()
            .zip(ack_batch.count())
            .map(q!(|(was_acked, new_acks)| was_acked || new_acks > 0));
        acked = acked_now.clone();

        timer_batch
            .cross_singleton(acked_now)
            .filter(q!(move |(attempt, acked)| !*acked && *attempt <= attempt_cap))
            .map(q!(|(attempt, _)| Msg { attempt }))
    };

    let messages_at_receiver = messages.send(receiver, TCP.lossy_delayed_forever().bincode());

    let (received, replies): (
        Stream<Msg, Process<'a, Receiver>, Unbounded, NoOrder>,
        Stream<Ack, Process<'a, Receiver>, Unbounded, NoOrder>,
    ) = sliced! {
        let message_batch = use::batch(messages_at_receiver, nondet_messages);
        let replies = message_batch.clone().map(q!(|message| Ack {
            attempt: message.attempt
        }));
        (message_batch, replies)
    };

    acks_handle.complete(replies.send(sender, TCP.lossy_delayed_forever().bincode()));

    received
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{Ldfi, Outcome};
    use hydro_lang::sim_hooks::{BatchHook, OnProcess};

    use super::{Ack, Msg, Receiver, Sender, reliable_delivery};

    /// Runs LDFI against `reliable_delivery` with `eot` timer rounds, allowing omissions
    /// only in the first `eff` rounds. The goal: the receiver received the message by the
    /// end of the horizon.
    fn search(max_attempts: Option<u32>, eot: u32, eff: u32) -> Outcome {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<Sender>();
        let receiver = flow.process::<Receiver>();
        let msg_hook: BatchHook<Msg, NoOrder, ExactlyOnce, OnProcess<Receiver>> = flow.sim_hook();
        let ack_hook: BatchHook<Ack, NoOrder, ExactlyOnce, OnProcess<Sender>> = flow.sim_hook();

        let (timer_send, timer) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let received = reliable_delivery(
            &sender,
            &receiver,
            timer,
            max_attempts,
            nondet!(/** LDFI fault point: the message channel */ hook = msg_hook),
            nondet!(/** LDFI fault point: the ack channel */ hook = ack_hook),
        );
        let out = received.sim_output();

        let mut sim = flow.sim().test_safety_only();
        if std::env::var("LDFI_PROVENANCE").is_ok() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();
        Ldfi::new()
            .level_from_env()
            .lossy_channel("msg", msg_hook)
            .lossy_channel("ack", ack_hook)
            .goal(&out)
            .run(&compiled, async |run| {
                for round in 1..=eot {
                    if round == eff + 1 {
                        run.end_of_finite_failures();
                    }
                    timer_send.send(round);
                    hydro_lang::sim::quiesce().await;
                }
                let received: Vec<Msg> = out.collect_sorted().await;
                !received.is_empty()
            })
    }

    /// A sender that gives up after two attempts violates the property: dropping both
    /// attempts (and nothing else) leaves nothing to deliver once the network heals.
    /// Lineage makes this a 3-run search: `{msg_1}` is the first support, `{msg_2}` the
    /// second, and their union the counterexample. Acks never enter a support.
    #[test]
    fn ldfi_breaks_two_attempts_by_dropping_both() {
        let outcome = search(Some(2), 4, 2);
        eprintln!("{outcome}");
        let Outcome::Counterexample {
            faults,
            schedule,
            runs,
        } = &outcome
        else {
            panic!("expected a counterexample, got {outcome}");
        };
        let fault_names: Vec<String> = faults.iter().map(ToString::to_string).collect();
        assert_eq!(
            fault_names,
            [
                "omit(msg:Msg { attempt: 1 })",
                "omit(msg:Msg { attempt: 2 })"
            ]
        );
        assert_eq!(schedule.dropped.len(), 2);
        assert_eq!(
            runs.len(),
            3,
            "expected exactly three hypotheses: {{}}, {{msg_1}}, {{msg_1, msg_2}}"
        );
        for run in runs {
            for schedule in &run.schedules {
                for derivation in &schedule.derivations {
                    assert!(
                        derivation.iter().all(|m| m.message.channel == "msg"),
                        "an ack appeared in a goal derivation: {derivation:?}"
                    );
                }
            }
        }
    }

    /// Retrying until acknowledged satisfies the property: whatever is dropped during the
    /// failure window, the first retry after it is delivered. Every support LDFI observes
    /// after the window lies outside the fault vocabulary, so the search ends with a
    /// certificate.
    #[test]
    fn ldfi_certifies_retry_until_ack() {
        let outcome = search(None, 4, 2);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
        // {}, {msg_1}, {msg_1, msg_2}: the last run's support is empty (msg_3 is after EFF).
        assert_eq!(outcome.runs().len(), 3);
    }
}
