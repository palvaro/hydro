p='brute_force.rs'
s=open(p).read()
s=s.replace('''//! node, never every node, never all or all but one of the checked cluster, and the
//! [`EcCheck::max_crashes`] cap. Each set runs over the same schedules, with the same driving,
//! heal and quiesce, as the search's; the runs are read by the same evaluation. Only the choice
//! of what to run differs, so agreement tests the lineage and the hitting-set pruning.''','''//! node, never every node, never all or all but one of the checked cluster, and the
//! [`EcCheck::max_crashes`] cap. Each set runs over the same schedules, with the same driving,
//! heal and quiesce, as the search's, and the runs are read by the same evaluation of the
//! specification. Everything else is the oracle's own: its execution loop ([`Engine::execute`]
//! here) reads the outputs straight from the event log, with no lineage, no provenance and no
//! pruning. So agreement tests the lineage, the literals and the hitting-set program.''')
a=s.index('//! A label of a program with schedule choices is never certified')
b=s.index('use std::collections')
s=s[:a]+'''//! A sampled search is never certified ([`Verdict::Exhausted`]); the same comparison applies
//! to it, as a check of what its sample found.

'''+s[b:]
s=s.replace('''use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::sim::compiled::CompiledSim;

use super::{EcCheck, Engine, Run};
use crate::check::results::{LabelResult, Results, Verdict, label_name};
use crate::fault::{Event, Fate, InstanceRecord, MessageId, NodeId};
use crate::lineage::Tables;''','''use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::sim::compiled::{CompiledSim, quiesce};

use super::{EcCheck, Engine, Failure, Target, correct_members, panic_message, record_domain};
use crate::check::results::{LabelResult, Results, Verdict, label_name};
use crate::fault::{Event, Fate, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
use crate::lineage::Tables;
use crate::observe::{Fact, Labels, decode_output};
use crate::spec::{MemberView, Observation, evaluate};''')
marker="impl<B: AsyncFnMut()> Engine<'_, B> {\n"
a=s.index(marker)
s=s[:a]+open('../../../tmp_py/brute_body.rs').read()+s[a+len(marker):]
s=s.replace('''        let (_, targets, probe) =
            self.prepare(|tables, record| harvest(record, tables, &mut universe));''','''        let (targets, probe) = self.prepare(|tables, record| harvest(record, tables, &mut universe));''')
s=s.replace('''                schedules += run.schedules;
                self.runs.insert(set.clone(), run.clone());
                runs.push((set, run));''','''                schedules += run.schedules;
                runs.push((set, run));''')
s=s.replace('''                let run = self.run(&cex.faults, &targets);
                confirmed.insert(label.index, run.labels[&label.index].failure.is_some());''','''                let failed = match runs.iter().find(|(set, _)| *set == cex.faults) {
                    Some((_, run)) => run.labels[&label.index].failure.is_some(),
                    None => {
                        let run = self.execute(&cex.faults, &targets, false, |_, _| {});
                        run.labels[&label.index].failure.is_some()
                    }
                };
                confirmed.insert(label.index, failed);''')
open(p,'w').write(s)
