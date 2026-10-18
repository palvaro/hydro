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
//! Both channels are `lossy_delayed_forever`, so the simulator's filter admits omission on
//! them (see `hydro_lang::sim::ldfi`); the protocol declares its own fault model and the
//! harness registers nothing.

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
/// - `nondet_messages`: the batching of messages at the receiver.
/// - `nondet_acks`: the batching of acks at the sender.
///
/// A simulation test may bind hooks to either batch for precise lineage; the fault points
/// are the channels themselves.
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

    let messages_at_receiver =
        messages.send(receiver, TCP.lossy_delayed_forever().name("msg").bincode());

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

    acks_handle.complete(replies.send(sender, TCP.lossy_delayed_forever().name("ack").bincode()));

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
    /// end of the horizon. With `hooks`, the two batches are bound to scripted hooks (which
    /// the search traces for exact per-execution lineage); without, the lineage comes from
    /// the receives alone.
    fn search(max_attempts: Option<u32>, eot: u32, eff: u32, hooks: bool) -> Outcome {
        search_with(max_attempts, eot, eff, hooks, false)
    }

    /// [`search`], keeping every schedule's trace when `traces` (for the visualizations).
    fn search_with(
        max_attempts: Option<u32>,
        eot: u32,
        eff: u32,
        hooks: bool,
        traces: bool,
    ) -> Outcome {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<Sender>();
        let receiver = flow.process::<Receiver>();

        let (timer_send, timer) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let received = if hooks {
            let msg_hook: BatchHook<Msg, NoOrder, ExactlyOnce, OnProcess<Receiver>> =
                flow.sim_hook();
            let ack_hook: BatchHook<Ack, NoOrder, ExactlyOnce, OnProcess<Sender>> = flow.sim_hook();
            reliable_delivery(
                &sender,
                &receiver,
                timer,
                max_attempts,
                nondet!(/** traced for lineage */ hook = msg_hook),
                nondet!(/** traced for lineage */ hook = ack_hook),
            )
        } else {
            reliable_delivery(
                &sender,
                &receiver,
                timer,
                max_attempts,
                nondet!(/** explored by the simulator */),
                nondet!(/** explored by the simulator */),
            )
        };
        let out = received.sim_output();

        let mut sim = flow.sim().test_safety_only();
        if std::env::var("LDFI_PROVENANCE").is_ok() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();
        let mut ldfi = Ldfi::new().level_from_env().goal(&out);
        if traces {
            ldfi = ldfi.with_traces();
        }
        ldfi.run(&compiled, async |run| {
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
        breaks_two_attempts(false);
    }

    /// The same search with the batches hooked: the hooks only sharpen lineage, the
    /// result is the same.
    #[test]
    fn ldfi_breaks_two_attempts_by_dropping_both_with_hooks() {
        breaks_two_attempts(true);
    }

    fn breaks_two_attempts(hooks: bool) {
        let outcome = search(Some(2), 4, 2, hooks);
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
        let outcome = search(None, 4, 2, false);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
        // {}, {msg_1}, {msg_1, msg_2}: the last run's support is empty (msg_3 is after EFF).
        assert_eq!(outcome.runs().len(), 3);
    }

    /// The counterexample's two pictures (see `hydro_lang::sim::viz`): the Lamport diagram
    /// of the failing run, with both omissions marked, and the last run in which the goal
    /// held with the goal's support (`msg_2`, the retry that got through) highlighted.
    #[test]
    fn ldfi_draws_the_counterexample() {
        let outcome = search_with(Some(2), 4, 2, true, true);
        let viz = outcome.viz().expect("a counterexample");

        // Level 0: the failing run.
        let failing = viz
            .failing_trace()
            .expect("a failing schedule keeps its trace");
        assert_eq!(failing.nodes.len(), 2, "{:?}", failing.nodes);
        assert!(
            failing
                .nodes
                .iter()
                .all(|n| n.label.starts_with("Process(") && !n.crashable),
            "{:?}",
            failing.nodes
        );
        assert_eq!(
            failing
                .messages
                .iter()
                .filter(|m| m.fate == hydro_lang::sim::viz::trace::Fate::Omitted)
                .count(),
            2
        );
        assert!(failing.eff_cut.is_some(), "the body declared EFF");
        let mermaid = viz.lamport_mermaid().unwrap();
        assert_eq!(
            mermaid
                .lines()
                .filter(|l| l.contains("-xp1: msg: Msg { attempt: 1 } (omitted)"))
                .count(),
            1,
            "{mermaid}"
        );
        assert!(
            mermaid.contains("-xp1: msg: Msg { attempt: 2 } (omitted)"),
            "{mermaid}"
        );
        assert!(mermaid.contains("EFF: end of finite failures"), "{mermaid}");
        // Rounds 3 and 4 are delivered after EFF but the sender had given up.
        assert!(!mermaid.contains("attempt: 3"), "{mermaid}");
        hydro_build_utils::assert_snapshot!(mermaid);
        let dot = viz.lamport_dot().unwrap();
        assert!(dot.starts_with("digraph lamport {"), "{dot}");
        assert!(dot.contains("(omitted)"), "{dot}");
        assert!(dot.contains("t_eff [label=\"EFF\""), "{dot}");

        // Level 1: the witness is the `{msg_1}` run, whose support is `msg_2`.
        let witness = viz.witness().expect("traces were retained");
        assert_eq!(witness.hypothesis.faults.len(), 1);
        assert_eq!(witness.derivation.len(), 1);
        let support = witness
            .schedule
            .trace
            .as_ref()
            .unwrap()
            .support(witness.derivation);
        assert_eq!(support.len(), 1, "{support:?}");
        let mermaid = viz.support_lamport_mermaid().unwrap();
        assert!(
            mermaid.contains("->>p1: ★ msg: Msg { attempt: 2 }"),
            "{mermaid}"
        );
        assert!(mermaid.contains("rect rgb"), "{mermaid}");
        hydro_build_utils::assert_snapshot!(mermaid);
        let dot = viz.support_lamport_dot().unwrap();
        assert!(dot.contains("★ msg: Msg { attempt: 2 }"), "{dot}");

        // The report, and the trace survives a JSON round trip.
        let markdown = viz.markdown();
        assert!(markdown.contains("## The failing run"), "{markdown}");
        assert_eq!(markdown.matches("```mermaid").count(), 2, "{markdown}");
        let json = failing.to_json();
        assert_eq!(
            &hydro_lang::sim::viz::Trace::from_json(&json).unwrap(),
            failing
        );
    }
}
