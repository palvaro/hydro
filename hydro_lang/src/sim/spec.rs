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
//! The derivation of a view is computed in two steps. At the observer the step is
//! **counterfactual**: the observer is replayed alone (its batches are hooked, so the
//! simulator can feed it exactly what it saw) with observed facts removed one at a time, and
//! a fact is in the derivation iff the view's final value changes without it. This is an
//! intervention on a nondeterministic decision of the run, "what the observer's batch
//! released", and it asks the same question of every formulation of a view: a view written
//! through negation (`filter_not_in`, an empty count) has no record provenance, since it
//! holds by the *absence* of facts, but removing a fact it rests on still falsifies it. The
//! broadcast harness states agreement both ways and gets the same derivations. Each kept
//! fact then continues into the observed node's own lineage at the configured level.
//!
//! The observation channels are ordinary network sends through the simulator's filter, so
//! that second step runs through them to the executions that produced the outputs. They are
//! not fault points: the search never omits them and never names a crash point by one of
//! them; a view's dependency on an observed fact is a dependency on the producing node having
//! been alive to produce it (killed by an earlier crash of that node), and on whatever the
//! fact itself was derived from. With record-level provenance ([`super::provenance`]) that is
//! exactly the set of deliveries the fact was computed from.
//!
//! What the observer replay cannot intervene on: decisions inside the observed nodes (which
//! arrival a member's deduplication picked is the record provenance's business) and the
//! fault model's own facts ([`Spec::correct_members`] comes from the hypothesis, not from a
//! replayed decision). A view that can only be falsified by *adding* a fault without removing
//! any observed fact (one that depends on a node's correctness positively, as "every member
//! that delivered is correct" does) therefore gets an empty derivation; the search warns and
//! certifies, and the certificate is only as good as the vocabulary's inability to produce
//! such a run.

use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::Hash;
use std::panic::RefUnwindSafe;
use std::rc::Rc;

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

/// The tag on the observation slot of an automatic specification ([`Spec::all_labels`]): the
/// placeholder source at the observer the simulator's compile step replaces with the union of
/// the labelled collections' observations (see `sim::observe`). Source-site substrings of
/// labels to exclude follow the tag, separated by [`SLOT_TAG_SEPARATOR`].
pub(crate) const SLOT_TAG: &str = "hydrofrac:observe-slot";
/// The separator between the slot tag and each excluded label.
pub(crate) const SLOT_TAG_SEPARATOR: char = '\u{1f}';

/// How the search feeds a cluster's correct members to the observer for one run.
#[derive(Clone)]
pub(crate) struct CorrectFeed {
    /// The cluster and its members. Fixed at construction by [`Spec::correct_members`]; set
    /// by the search, once per labelled collection, for an automatic specification. `None`
    /// feeds nothing.
    pub target: Rc<std::panic::AssertUnwindSafe<RefCell<Option<(LocationId, Vec<u32>)>>>>,
    /// Sends one member id into the observer.
    pub send: Rc<dyn Fn(u32) + RefUnwindSafe>,
}

impl CorrectFeed {
    /// Feeds every member of the cluster that is not in `crashed`.
    pub(crate) fn feed(&self, crashed: &HashSet<NodeKey>) {
        if let Some((cluster, members)) = &*self.target.borrow() {
            for member in members {
                if !crashed.contains(&(cluster.clone(), Some(*member))) {
                    (self.send)(*member);
                }
            }
        }
    }

    /// Points the feed at `cluster` and its `members` (an automatic specification's feed,
    /// between two per-collection searches).
    pub(crate) fn set_target(&self, cluster: LocationId, members: Vec<u32>) {
        *self.target.borrow_mut() = Some((cluster, members));
    }
}

/// The parts of an automatic specification ([`Spec::all_labels`]) the search drives: which
/// labelled collection the current search is about, and the correct members of its cluster.
#[derive(Clone)]
pub(crate) struct AllLabelsParts {
    /// The index (into the compiled program's labelled collections) of the collection under
    /// search; `None` before the first search.
    pub current: Rc<std::panic::AssertUnwindSafe<std::cell::Cell<Option<usize>>>>,
    /// Sends the current index into the observer.
    pub send_current: Rc<dyn Fn(usize) + RefUnwindSafe>,
    /// The correct-member feed whose cluster the search chooses per collection.
    pub correct: CorrectFeed,
}

impl AllLabelsParts {
    /// Feeds the current index, if a collection has been chosen.
    pub(crate) fn feed(&self) {
        if let Some(index) = self.current.get() {
            (self.send_current)(index);
        }
    }
}

/// How the search feeds the fact domain of a generated specification
/// ([`Spec::eventual_consistency`]) to the observer: the facts of the failure-free run, which
/// the search learns from a probe run and sends before every run after it.
#[derive(Clone)]
pub(crate) struct FactFeed {
    /// The port on which a run publishes the facts correct members held (one output per
    /// fact, bincode of the fact).
    pub port: ExternalPortId,
    /// Sends one fact (bincode) into the observer.
    pub send: Rc<dyn Fn(&[u8]) + RefUnwindSafe>,
    /// The domain, filled by the search from the probe run. (`AssertUnwindSafe`: the search
    /// writes it between runs, never inside the simulator's unwind scope.)
    pub domain: Rc<std::panic::AssertUnwindSafe<RefCell<std::collections::BTreeSet<Vec<u8>>>>>,
}

impl FactFeed {
    /// Feeds the whole domain.
    pub(crate) fn feed(&self) {
        for fact in self.domain.borrow().iter() {
            (self.send)(fact);
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
    facts: Option<FactFeed>,
    all_labels: Option<AllLabelsParts>,
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
            facts: None,
            all_labels: None,
        }
    }

    /// The generated specification for **every** collection the program labels
    /// `EventualConsistency`, with nothing named by the test. The simulator's compile step
    /// finds the labelled collections in the program's IR and observes each one that the
    /// set-based specification applies to (an unbounded stream at a cluster's top level that a
    /// network channel feeds); [`Ldfi::search_all_labels`](super::ldfi::Ldfi::search_all_labels)
    /// then runs one search per collection and reports a verdict for each, including the ones
    /// not checked and why.
    ///
    /// Observing a collection serializes its elements, so every labelled collection's element
    /// type must be `serde::Serialize` (it nearly always is: labelled cluster collections are
    /// built from network sends). If one is not, the generated crate fails to compile; use
    /// [`Self::all_labels_except`] to leave it out.
    pub fn all_labels(flow: &mut FlowBuilder<'a>) -> Self {
        Self::all_labels_except(flow, &[])
    }

    /// [`Self::all_labels`], leaving out every labelled collection whose source site (the
    /// `file:line` of the labelling operator, as the results print it) contains one of
    /// `excluded`. The left-out collections are still reported, as not checked.
    pub fn all_labels_except(flow: &mut FlowBuilder<'a>, excluded: &[&str]) -> Self {
        use crate::compile::ir::{HydroNode, HydroSource};
        use crate::live_collections::stream::NoOrder;
        use crate::spec_views::{Observed, ObservedItem};

        let mut spec = Spec::new(flow);

        // The observation slot: an empty source the compile step replaces with the union of
        // the taps. It compiles to an empty source if the program labels nothing.
        let slot_type = stageleft::quote_type::<ObservedItem>();
        let empty: syn::Expr = syn::parse_quote!(::std::vec::Vec::<#slot_type>::new());
        let mut metadata = spec.observer.new_node_metadata(Stream::<
            ObservedItem,
            Process<'a, Observer>,
            Unbounded,
            NoOrder,
            ExactlyOnce,
        >::collection_kind());
        let mut tag = SLOT_TAG.to_owned();
        for excluded in excluded {
            assert!(
                !excluded.is_empty() && !excluded.contains(SLOT_TAG_SEPARATOR),
                "an excluded label must be a non-empty source-site substring"
            );
            tag.push(SLOT_TAG_SEPARATOR);
            tag.push_str(excluded);
        }
        metadata.tag = Some(tag);
        let slot: Stream<ObservedItem, Process<'a, Observer>, Unbounded, NoOrder, ExactlyOnce> =
            Stream::new(
                spec.observer.clone(),
                HydroNode::Source {
                    source: HydroSource::Iter(empty.into()),
                    metadata,
                },
            );

        // Which collection this search is about, and the correct members of its cluster;
        // both fed by the search before each run.
        let (current_sender, current) = spec.observer.sim_input::<usize, TotalOrder, ExactlyOnce>();
        let current = spec.accumulate(current).first();
        let (correct_sender, correct) = spec
            .observer
            .sim_input::<MemberId<Observed>, TotalOrder, ExactlyOnce>();
        let correct_feed = CorrectFeed {
            target: Rc::new(std::panic::AssertUnwindSafe(RefCell::new(None))),
            send: {
                let sender = std::panic::AssertUnwindSafe(correct_sender);
                Rc::new(move |member| sender.send(MemberId::from_raw_id(member)))
            },
        };
        spec.correct.push(correct_feed.clone());
        let correct = spec.accumulate(correct);

        let observed = spec.accumulate(slot);
        let observed = crate::spec_views::select_label(observed, current);
        spec.eventual_consistency(observed, correct);
        spec.all_labels = Some(AllLabelsParts {
            current: Rc::new(std::panic::AssertUnwindSafe(std::cell::Cell::new(None))),
            send_current: {
                let sender = std::panic::AssertUnwindSafe(current_sender);
                Rc::new(move |index| sender.send(index))
            },
            correct: correct_feed,
        });
        spec
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

    /// Brings an ordered process output into the specification with its **arrival sequence**:
    /// `(n, value)` facts, `n` the position of the value in the output's order. For views about
    /// the *last* value of an output (a final leader view, a last committed index), which the
    /// set of facts alone cannot express: `observe_sequenced(views).max()` is the final view.
    /// Such a view is not monotone in the facts (a later value changes it); see the module
    /// docs on what that costs.
    pub fn observe_sequenced<T, P>(
        &self,
        stream: Stream<T, Process<'a, P>, Unbounded, TotalOrder, ExactlyOnce>,
    ) -> Stream<(usize, T), Tick<Process<'a, Observer>>, Bounded, TotalOrder, ExactlyOnce>
    where
        T: Ord + Clone + Serialize + DeserializeOwned + 'a,
    {
        let at_observer = stream
            .send(&self.observer, TCP.fail_stop().name("observe").bincode())
            .enumerate();
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
            target: Rc::new(std::panic::AssertUnwindSafe(RefCell::new(Some((
                Location::id(cluster),
                members.into_iter().collect(),
            ))))),
            send: {
                // The sender holds only a port id and `PhantomData<MemberId<C>>`.
                let sender = std::panic::AssertUnwindSafe(sender);
                Rc::new(move |member| sender.send(MemberId::from_raw_id(member)))
            },
        });
        self.accumulate(stream)
    }

    /// The specification an `EventualConsistency` label stands for, generated rather than
    /// written. It is Molly's "the good outcome persists", for the one good outcome the label
    /// is about: let `FACTS` be the facts of the collection that correct members held at the
    /// end of the failure-free run (the search learns them from a probe run and feeds them to
    /// the observer before every run, as it feeds `correct_members`); then `POST` is: every
    /// correct member holds every fact of `FACTS` that some correct member holds (as a count:
    /// the `(member, fact)` pairs at correct members over `FACTS` are as many as the `FACTS`
    /// facts present times the correct members). There is no `PRE`; if the probe run holds no
    /// facts the search is vacuous.
    ///
    /// The label promises that replicas compute the same contents *given the same inputs*;
    /// what it assumes is that faults do not withhold inputs from some replicas, and that
    /// assumption is this specification. A collection reachable from a lossy channel or a
    /// crashable node that has no certificate for it carries an undischarged premise.
    ///
    /// Why the domain is fixed: the hitting-set inference behind a certificate is sound for a
    /// view whose derivation in a run implies the view (a fault set that leaves every
    /// delivery of the derivation intact cannot break it). With `FACTS` fixed and
    /// `correct` fixed by the hypothesis, the derivation of `POST` is the set of deliveries
    /// behind every `(member, fact)` pair, and keeping them all keeps `POST`: monotone by
    /// construction, with no syntactic analysis of the view. Quantifying instead over
    /// whatever facts a run produces is not monotone (a fault that makes one replica derive a
    /// fresh fact falsifies it without touching any derivation), and a fresh fact is not the
    /// label's premise about the given inputs anyway.
    ///
    /// The view is evaluated at the end of the run the body drives; the body is what gives the
    /// protocol time to converge, and a protocol with retransmission or leader change needs the
    /// body to let that happen. A `FACTS` fact that no correct member holds under a hypothesis
    /// (its producer crashed before producing it) is not required of anyone, as in Molly.
    pub fn eventual_consistency<C, T, R: Retries>(
        &mut self,
        observed: Stream<(MemberId<C>, T), Tick<Process<'a, Observer>>, Bounded, TotalOrder, R>,
        correct: Stream<MemberId<C>, Tick<Process<'a, Observer>>, Bounded, TotalOrder>,
    ) where
        C: 'a,
        T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + 'static,
    {
        assert!(
            self.facts.is_none(),
            "a Spec has one generated specification"
        );
        let (sender, domain) = self.observer.sim_input::<T, TotalOrder, ExactlyOnce>();
        let domain = self.accumulate(domain);
        let (post, held_facts) = crate::spec_views::eventual_consistency(observed, correct, domain);
        // The facts of this run, for the search's probe.
        let port = held_facts.all_ticks().sim_output().0;
        self.facts = Some(FactFeed {
            port,
            send: {
                let sender = std::panic::AssertUnwindSafe(sender);
                Rc::new(move |bytes: &[u8]| {
                    let fact: T = bincode::deserialize(bytes)
                        .expect("a published fact deserializes as the collection's element");
                    sender.send(fact)
                })
            },
            domain: Rc::new(std::panic::AssertUnwindSafe(Default::default())),
        });
        self.post(post);
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

    /// The specification as the search holds it. A `Spec` can be given to several searches
    /// over the same compiled program.
    pub(crate) fn parts(&self) -> SpecParts {
        SpecParts {
            observer: Location::id(&self.observer),
            pre: self.pre,
            post: self
                .post
                .expect("a Spec needs a POST (`Spec::post`) before it is given to the search"),
            correct: self.correct.clone(),
            facts: self.facts.clone(),
            all_labels: self.all_labels.clone(),
        }
    }
}

/// A [`Spec`] as the search holds it.
#[derive(Clone)]
pub(crate) struct SpecParts {
    pub observer: LocationId,
    pub pre: Option<ExternalPortId>,
    pub post: ExternalPortId,
    pub correct: Vec<CorrectFeed>,
    /// The generated specification's fact domain, if the spec is one.
    pub facts: Option<FactFeed>,
    /// The per-collection feeds, if the spec is an automatic one ([`Spec::all_labels`]).
    pub all_labels: Option<AllLabelsParts>,
}
