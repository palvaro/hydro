//! Correctness specifications as Hydro views: `PRE` and `POST` over the program's outputs.
//!
//! In LDFI as published, the program and its specification are written in the same language,
//! so the specification has lineage: the search is not for a run that fails to deliver one
//! concrete message, but for a run in which a view `PRE` holds and a view `POST` does not,
//! and the derivations of `POST` are what the search negates. A [`Spec`] is the same thing
//! for a Hydro program. It is a small Hydro dataflow of its own, on an **observer** process
//! the simulator adds to the program, that receives the outputs the specification is about
//! and derives two views from them:
//!
//! - **`PRE`**, the pre-condition. A run in which it does not hold says nothing about the
//!   protocol (vacuous). The search tries to *preserve* its derivation.
//! - **`POST`**, the post-condition. A run in which `PRE` holds and `POST` does not is a
//!   counterexample. Each run in which both hold yields `POST`'s derivation as a clause.
//!
//! Both are evaluated at the end of the run (the horizon `EOT`): a view holds if its final
//! value is non-empty, as a Datalog goal holds if it is derivable at fixpoint.
//!
//! ```rust,ignore
//! let mut spec = Spec::new(&mut flow);
//! let delivered = spec.observe_cluster(deliveries);        // (member, payload) facts so far
//! let correct = spec.correct_members(&members, 0..3);      // members that do not crash
//!
//! // PRE: some member delivered m.
//! let delivered_m = delivered.filter(q!(|(_, p)| p.0 == 42));
//! spec.pre(delivered_m.clone().first());
//!
//! // POST: every correct member delivered m, as "the correct members that delivered m are
//! // as many as the correct members".
//! let delivering = delivered_m.map(q!(|(m, _)| (m, ()))).unique();
//! let correct_delivering = correct.clone().map(q!(|m| (m, ()))).join(delivering).count();
//! spec.post(correct_delivering.zip(correct.count()).filter(q!(|(d, n)| d == n)));
//!
//! Ldfi::new()
//!     .crashable("broadcaster", &broadcaster)
//!     .crashable_cluster("member", &members, 0..3)
//!     .crash_budget(1)
//!     .spec(spec)
//!     .search(&compiled, async |_run| {
//!         source.send(Payload(42));
//!         hydro_lang::sim::quiesce().await;
//!     })
//! ```
//!
//! The same specification separates protocols: plain broadcast violates it under one crash
//! (`hydro_test`'s `reliable_broadcast` harness finds the mid-fan-out crash in two runs),
//! reliable broadcast satisfies it under one and two.
//!
//! # What the views see
//!
//! [`Spec::observe`] and [`Spec::observe_cluster`] bring an output stream to the observer and
//! present it as the **set of facts so far**: a bounded, sorted stream inside the observer's
//! tick, accumulated across the tick's executions. Views are ordinary Hydro over those bounded
//! collections (`filter`, `map`, `join`, `filter_not_in`, `count`, `first`, `zip`, ...). The
//! observer's tick runs whenever a fact arrives; the final execution sees every fact.
//! [`Spec::correct_members`] is the fault model's contribution: the members of a cluster that
//! do not crash in the run, which the search feeds before the run starts (Molly's `crash`
//! relation, complemented).
//!
//! # Lineage
//!
//! The observation channels are ordinary network sends through the simulator's filter, so
//! the lineage of a view runs through them to the executions that produced the outputs. They
//! are not fault points: the search never omits them and never names a crash point by one of
//! them; a view's dependency on an observed fact is a dependency on the producing node having
//! been alive to produce it (killed by an earlier crash of that node), and on whatever the
//! fact itself was derived from. With record-level provenance ([`super::provenance`]) the
//! derivation of a view is exactly the set of deliveries its value was computed from.
//!
//! Write `POST` **positively**. A view derived through negation (`filter_not_in`, an empty
//! count) depends on the *absence* of facts, which has no lineage: its derivation is empty,
//! and an empty derivation certifies immediately (the search warns when this happens). A
//! universal ("every correct member delivered") is positive as an aggregate over the
//! matches compared with an aggregate over the domain, as in the example above: the count of
//! delivering correct members folds every delivery into the view's provenance.

use std::collections::HashSet;
use std::panic::RefUnwindSafe;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::compile::builder::{ExternalPortId, FlowBuilder};
use crate::live_collections::boundedness::{Bounded, Unbounded};
use crate::live_collections::optional::Optional;
use crate::live_collections::singleton::Singleton;
use crate::live_collections::stream::{ExactlyOnce, Ordering, Retries, Stream, TotalOrder};
use crate::location::dynamic::{DynLocation, LocationId};
use crate::location::{Cluster, Location, MemberId, Process, Tick};
use crate::networking::TCP;
use crate::nondet::NonDet;
use crate::sim::runtime::NodeKey;
use crate::sim_hooks::{BatchHook, OnProcess, SimHook};

/// The tag of the observer process a [`Spec`] adds to the program.
pub enum Observer {}

/// How the search feeds a cluster's correct members to the observer for one run.
pub(crate) struct CorrectFeed {
    /// The cluster.
    pub cluster: LocationId,
    /// Its members.
    pub members: Vec<u32>,
    /// Sends one member id into the observer.
    pub send: Box<dyn Fn(u32) + RefUnwindSafe>,
}

impl CorrectFeed {
    /// Feeds every member of the cluster that is not in `crashed`.
    pub(crate) fn feed(&self, crashed: &HashSet<NodeKey>) {
        for member in &self.members {
            if !crashed.contains(&(self.cluster.clone(), Some(*member))) {
                (self.send)(*member);
            }
        }
    }
}

/// A correctness specification: `PRE` and `POST` views over the program's outputs (see the
/// module docs).
pub struct Spec<'a> {
    observer: Process<'a, Observer>,
    tick: Tick<Process<'a, Observer>>,
    pre: Option<ExternalPortId>,
    post: Option<ExternalPortId>,
    correct: Vec<CorrectFeed>,
}

impl<'a> Spec<'a> {
    /// Adds the observer process to `flow` and returns an empty specification.
    pub fn new(flow: &mut FlowBuilder<'a>) -> Self {
        let observer = flow.process::<Observer>();
        let tick = observer.tick();
        Spec {
            observer,
            tick,
            pre: None,
            post: None,
            correct: vec![],
        }
    }

    /// The observer process, for views that need a top-level collection.
    pub fn observer(&self) -> &Process<'a, Observer> {
        &self.observer
    }

    /// The observer's tick, in which the views are evaluated.
    pub fn tick(&self) -> &Tick<Process<'a, Observer>> {
        &self.tick
    }

    /// A fresh scripted hook for a batch into the observer's tick; the search places every
    /// such hook under a traced pass-through policy.
    fn hook<T, O: Ordering, R: Retries>(&self) -> BatchHook<T, O, R, OnProcess<Observer>>
    where
        T: Serialize + DeserializeOwned + PartialEq,
    {
        let flow_state = self.observer.flow_state().clone();
        BatchHook::create(&mut move || flow_state.borrow_mut().next_sim_hook_id())
    }

    /// Batches `stream` into the observer's tick and accumulates it across executions: the
    /// set of facts so far, sorted.
    fn accumulate<T, O: Ordering, R: Retries>(
        &self,
        stream: Stream<T, Process<'a, Observer>, Unbounded, O, R>,
    ) -> Stream<T, Tick<Process<'a, Observer>>, Bounded, TotalOrder, R>
    where
        T: Ord + Clone + Serialize + DeserializeOwned + 'a,
    {
        let hook: BatchHook<T, O, R, OnProcess<Observer>> = self.hook();
        let batch = stream.batch(&self.tick, NonDet::hooked(Some(hook)));
        let (handle, previous) =
            self.tick
                .cycle::<Stream<T, Tick<Process<'a, Observer>>, Bounded, TotalOrder, R>, _>();
        let all = previous.chain(batch.sort());
        handle.complete_next_tick(all.clone());
        all
    }

    /// Brings a process output into the specification: the facts it has produced so far, in
    /// the observer's tick.
    pub fn observe<T, P, O: Ordering, R: Retries>(
        &self,
        stream: Stream<T, Process<'a, P>, Unbounded, O, R>,
    ) -> Stream<T, Tick<Process<'a, Observer>>, Bounded, TotalOrder, R>
    where
        T: Ord + Clone + Serialize + DeserializeOwned + 'a,
    {
        let at_observer = stream.send(&self.observer, TCP.fail_stop().name("observe").bincode());
        self.accumulate(at_observer)
    }

    /// Brings a cluster output into the specification: the `(member, value)` facts it has
    /// produced so far, in the observer's tick.
    pub fn observe_cluster<T, C, O: Ordering, R: Retries>(
        &self,
        stream: Stream<T, Cluster<'a, C>, Unbounded, O, R>,
    ) -> Stream<(MemberId<C>, T), Tick<Process<'a, Observer>>, Bounded, TotalOrder, R>
    where
        T: Ord + Clone + Serialize + DeserializeOwned + 'a,
        C: 'a,
    {
        let at_observer = stream
            .send(&self.observer, TCP.fail_stop().name("observe").bincode())
            .entries();
        self.accumulate(at_observer)
    }

    /// The members of `cluster` (among `members`) that do not crash in the run, as facts in
    /// the observer's tick. Fed by the search before each run from the hypothesis under test;
    /// every member is fed in a run with no crashes.
    pub fn correct_members<C>(
        &mut self,
        cluster: &Cluster<'a, C>,
        members: impl IntoIterator<Item = u32>,
    ) -> Stream<MemberId<C>, Tick<Process<'a, Observer>>, Bounded, TotalOrder, ExactlyOnce>
    where
        C: 'static,
    {
        let (sender, stream) = self
            .observer
            .sim_input::<MemberId<C>, TotalOrder, ExactlyOnce>();
        self.correct.push(CorrectFeed {
            cluster: Location::id(cluster),
            members: members.into_iter().collect(),
            send: {
                // The sender holds only a port id and `PhantomData<MemberId<C>>`.
                let sender = std::panic::AssertUnwindSafe(sender);
                Box::new(move |member| sender.send(MemberId::from_raw_id(member)))
            },
        });
        self.accumulate(stream)
    }

    /// Declares the pre-condition: the run is vacuous unless `view` is non-empty at its end.
    /// Without one, every run counts.
    pub fn pre<T>(&mut self, view: impl Into<Optional<T, Tick<Process<'a, Observer>>, Bounded>>)
    where
        T: Clone + Serialize + DeserializeOwned + 'a,
    {
        assert!(self.pre.is_none(), "a Spec has one PRE");
        self.pre = Some(self.publish(view.into()));
    }

    /// Declares the post-condition: the goal is violated in a run where the pre-condition
    /// holds and `view` is empty at the end.
    pub fn post<T>(&mut self, view: impl Into<Optional<T, Tick<Process<'a, Observer>>, Bounded>>)
    where
        T: Clone + Serialize + DeserializeOwned + 'a,
    {
        assert!(self.post.is_none(), "a Spec has one POST");
        self.post = Some(self.publish(view.into()));
    }

    /// Emits the view's value (as an `Option`) once per execution of the observer's tick on
    /// an external port; its last output is the view at the end of the run.
    fn publish<T>(&self, view: Optional<T, Tick<Process<'a, Observer>>, Bounded>) -> ExternalPortId
    where
        T: Clone + Serialize + DeserializeOwned + 'a,
    {
        let snapshots: Singleton<Option<T>, Tick<Process<'a, Observer>>, Bounded> =
            view.into_singleton();
        snapshots.into_stream().all_ticks().sim_output().0
    }

    /// Whether a view holds, from the bincode of its last published `Option<T>`.
    pub(crate) fn holds(last_output: Option<&[u8]>) -> bool {
        last_output.is_some_and(|bytes| bytes.first() == Some(&1))
    }

    pub(crate) fn into_parts(self) -> SpecParts {
        SpecParts {
            observer: Location::id(&self.observer),
            pre: self.pre,
            post: self
                .post
                .expect("a Spec needs a POST (`Spec::post`) before it is given to the search"),
            correct: self.correct,
        }
    }
}

/// A [`Spec`] as the search holds it.
pub(crate) struct SpecParts {
    pub observer: LocationId,
    pub pre: Option<ExternalPortId>,
    pub post: ExternalPortId,
    pub correct: Vec<CorrectFeed>,
}
