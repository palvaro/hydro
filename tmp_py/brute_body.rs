/// What one run (a fault set over all its schedules) says about one label: its first failure.
#[derive(Default)]
struct LabelRun {
    failure: Option<Rc<Failure>>,
}

/// One fault set, run over its schedules.
struct Run {
    /// By label index.
    labels: BTreeMap<usize, LabelRun>,
    schedules: usize,
    /// The probe only: each target's fact domain, by label index.
    domains: BTreeMap<usize, BTreeSet<Fact>>,
}

/// Whether one schedule's record breaks `target`, against `domain`: the specification's
/// evaluation, over the outputs the log shows (no lineage).
fn failure_of(
    labels: &Labels,
    target: &Target,
    domain: &BTreeSet<Fact>,
    record: &Rc<InstanceRecord>,
) -> Option<Failure> {
    let observations: Vec<Observation<()>> = record
        .log
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::Output {
                port, node, bytes, ..
            } if labels.label_of_port(*port) == Some(target.label) => Some(Observation {
                label: target.label,
                member: node.member.unwrap_or(0),
                fact: decode_output(bytes),
                support: (),
            }),
            _ => None,
        })
        .collect();
    let view = MemberView::of(target.label, &observations);
    let correct = correct_members(target, record);
    let evaluation = evaluate(&view, domain, &correct);
    let divergent: Vec<Fact> = evaluation
        .outside_domain
        .iter()
        .filter(|fact| !correct.iter().all(|m| view.holds(*m, fact)))
        .cloned()
        .collect();
    (!evaluation.holds() || !divergent.is_empty()).then(|| Failure {
        missing: evaluation.missing,
        divergent,
        panic: None,
        record: record.clone(),
    })
}

/// A run being read one schedule at a time.
struct RunFold<'t> {
    targets: &'t [Target],
    /// Whether this is the failure-free probe, which learns each target's domain as it reads.
    probe: bool,
    labels: BTreeMap<usize, LabelRun>,
    domains: BTreeMap<usize, BTreeSet<Fact>>,
    schedules: usize,
    /// The latest record, read when the next one arrives or at [`Self::finish`]: when a
    /// schedule panics, the run's last record is the panicking one.
    pending: Option<InstanceRecord>,
}

impl<'t> RunFold<'t> {
    fn new(targets: &'t [Target], probe: bool) -> Self {
        RunFold {
            targets,
            probe,
            labels: targets
                .iter()
                .map(|t| (t.label, LabelRun::default()))
                .collect(),
            domains: BTreeMap::new(),
            schedules: 0,
            pending: None,
        }
    }

    /// Takes the next schedule's record (reading the one before it).
    fn push(&mut self, labels: &Labels, record: InstanceRecord) {
        if let Some(previous) = self.pending.replace(record) {
            self.read(labels, previous, None);
        }
    }

    /// Reads the last record (the panicking schedule's, if `panic`) and ends the run.
    fn finish(mut self, labels: &Labels, panic: Option<String>) -> Run {
        if let Some(last) = self.pending.take() {
            self.read(labels, last, panic.as_deref());
        }
        for lr in self.labels.values_mut() {
            // The panicking schedule left no record: still a counterexample.
            if let Some(panic) = &panic {
                lr.failure
                    .get_or_insert_with(|| Rc::new(Failure::panic(panic, Rc::default())));
            }
        }
        Run {
            labels: self.labels,
            schedules: self.schedules,
            domains: self.domains,
        }
    }

    /// Reads one schedule's record for every target.
    fn read(&mut self, labels: &Labels, record: InstanceRecord, panicked: Option<&str>) {
        self.schedules += 1;
        let record = Rc::new(record);
        for t in self.targets {
            let lr = self.labels.get_mut(&t.label).unwrap();
            if let Some(panic) = panicked {
                // A program that panics under injected faults breaks every label.
                lr.failure
                    .get_or_insert_with(|| Rc::new(Failure::panic(panic, record.clone())));
                continue;
            }
            // The probe reads each schedule against the facts its own correct members hold: a
            // schedule's facts are a subset of the domain (the union of every schedule's), so
            // that is the same reading as against the whole domain.
            let own;
            let domain = if self.probe {
                own = record_domain(labels, t, &record);
                self.domains
                    .entry(t.label)
                    .or_default()
                    .extend(own.iter().cloned());
                &own
            } else {
                &t.domain
            };
            if lr.failure.is_none() {
                lr.failure = failure_of(labels, t, domain, &record).map(Rc::new);
            }
        }
    }
}

impl<B: AsyncFnMut()> Engine<'_, B> {
    /// Runs `faults` over every schedule (or the sample), folding each record into a [`Run`] as
    /// it finishes. `seen` gets every record first.
    fn execute(
        &mut self,
        faults: &FaultSet<MessageId>,
        targets: &[Target],
        probe: bool,
        seen: impl FnMut(&Tables, &InstanceRecord),
    ) -> Run {
        let hook = Rc::new(FaultHook::new(
            &FaultPlan {
                faults: faults.clone(),
            },
            self.sim,
        ));
        self.sim.set_network_hook(hook.clone());
        let fold = RefCell::new(RunFold::new(targets, probe));
        let seen = RefCell::new(seen);
        let labels = self.labels;
        let tables = &self.tables;
        let absorb = |records: Vec<InstanceRecord>| {
            for record in records {
                (seen.borrow_mut())(tables, &record);
                fold.borrow_mut().push(labels, record);
            }
        };
        let outcome = {
            let healer = AssertUnwindSafe(hook.clone());
            let mut body = AssertUnwindSafe(&mut *self.body);
            let thunk = async || {
                (*body)().await;
                // As the search: faults are admitted while the body drives inputs and while the
                // work it caused settles; then the network heals.
                quiesce().await;
                healer.heal();
                quiesce().await;
            };
            let after = || absorb(hook.drain_instances());
            let sim = self.sim;
            let sample = self.sample_schedules;
            std::panic::catch_unwind(AssertUnwindSafe(move || match sample {
                Some(n) => sim.fuzz_bounded_then(n, thunk, after),
                None => {
                    sim.exhaustive_then(thunk, after);
                }
            }))
        };
        self.sim.clear_network_hook();
        // The schedule that panicked (and any replay of it) never reached `after`.
        absorb(hook.drain_instances());
        let panic = outcome.err().map(|p| panic_message(&*p));
        fold.into_inner().finish(labels, panic)
    }

    /// The labels to check, with their domains from the failure-free probe, and the probe's run.
    /// `seen` gets each of the probe's records first.
    fn prepare(&mut self, seen: impl FnMut(&Tables, &InstanceRecord)) -> (Vec<Target>, Rc<Run>) {
        let (_, mut targets) = self.label_targets();
        let mut probe = self.execute(&FaultSet::new(), &targets, true, seen);
        for t in &mut targets {
            t.domain = probe.domains.remove(&t.label).unwrap_or_default();
        }
        targets.retain(|t| !t.domain.is_empty());
        (targets, Rc::new(probe))
    }

