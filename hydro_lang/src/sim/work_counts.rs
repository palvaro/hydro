//! Passive, program-independent counts of the records a simulation moves.
//!
//! The amplification checker needs a measure of work that requires nothing from the program
//! under test: no output counters, no instrumentation, no knowledge of what a "request" is. This
//! module collects four such measures through [`WorkCountExtension`]. The simulator invokes the
//! extension only when a caller explicitly installs it; ordinary runs do no counting work.
//!
//! # Which counts can a schedule inflate on its own?
//!
//! The checker's schedules hold one `use::batch` hook for a while and then release it. Such a
//! hold changes *when* records move, not *how many*, unless the program reacts to the delay by
//! deriving records it would not otherwise have derived. A measure is fit for the verdict only
//! if a hold cannot raise it without that reaction. Reasoning about the candidates:
//!
//! - **Records released by `use::batch` hooks** ([`WorkCounts::hook_releases`]). Over a whole
//!   run, a hook's total is the records that arrived at it minus those still buffered at the
//!   end. Batch sizes do not enter; a hold makes fewer, larger batches of the same records. A
//!   hold that pushes records past the end of the run lowers the total. The total rises only if
//!   more records arrived, which means something upstream produced them. In idiomatic Hydro
//!   every network message that participates in tick logic passes through a batch hook at its
//!   receiver, so this count also covers messages, once per hop. This is the measure the
//!   checker's verdict reads, under the name *admitted records*.
//! - **Network messages** ([`WorkCounts::network_messages`]). The same argument applies: a
//!   message is sent when the sender's dataflow emits it, and a hold cannot make the sender
//!   emit more unless the program's logic does so. Messages are counted where the simulator
//!   serializes them, by the same `_network_metrics` pass-through a deployment uses, and are
//!   keyed by the sending location. Single-process programs have none.
//! - **Items read out of DFIR handoffs** ([`WorkCounts::handoff_items`]). Handoffs carry records
//!   between fused subgraphs, including the persisted state a tick re-reads every time it runs.
//!   A program that keeps a table of outstanding requests re-reads the whole table each tick,
//!   so a hold that keeps entries outstanding longer raises this count with no record ever
//!   re-derived. That is the cost of waiting, which every program that waits must pay, and it
//!   grows with delay for benign and hazardous programs alike. This count is exposed for
//!   comparison and is not fit for the verdict.
//! - **Subgraph runs** ([`WorkCounts::subgraph_runs`]) and tick executions. A hold parks the held
//!   tick and then runs it once on release, so a schedule changes these directly, in either
//!   direction, without any change in records. Not fit for the verdict; exposed for comparison.
//! - **Snapshot releases** are not counted at all. A `use::snapshot` hook releases exactly one
//!   value every time its tick runs, whether a new version or the last one again, so its release
//!   count is the tick's execution count under another name, and the count of *new* versions it
//!   observes depends on how many the schedule let it skip. Neither is a record the program
//!   produced. Snapshot hooks are therefore held by the checker (see [`super::hold_one_hook`])
//!   but contribute nothing to [`WorkCounts::admitted`]; the work a stale snapshot provokes shows
//!   up where it always does, in the records and messages the program then derives.
//!
//! # The verdict rule these counts support
//!
//! A hold is a delay and nothing else, so any rise in a count is the program's reaction to
//! delay. Not every reaction is amplification. Measured on the corpus, two reactions had to be
//! told apart: followers that start sending vote requests while the leader stops sending
//! heartbeats (amplification, hidden in the cluster's message total because the total fell), and
//! a client that records an abandonment instead of a completion when a reply is late (a
//! substitution, visible as a rise at the abandonment hook although no work was added). What
//! separates them is what competes for capacity. A message sent to another location consumes
//! that location's capacity, so messages are read *per sender*, where displacement cannot hide
//! them. Records a location admits into its own ticks consume its own capacity, so they are read
//! *in total*, where a substitution within the location nets to zero. The rule is therefore:
//! hazardous if some hold raises any member's outgoing messages
//! ([`WorkCounts::sends_by_member`]) or the program's admitted records
//! ([`WorkCounts::admitted`]) above the unheld run; benign otherwise.
//!
//! # Where the counts come from
//!
//! Hook releases are counted by the scheduler in `run_hooks`, on the host side of the dylib
//! boundary, through [`super::runtime::RuntimeHook::pending_release_count`], and keyed by the same
//! `location#index [item type]` identity that [`super::hold_one_hook`] uses, so the per-hook
//! breakdown names a source location. Network messages, handoff items and subgraph runs are read
//! from each DFIR instance's [`dfir_rs::scheduled::metrics::DfirMetrics`] when the scheduler
//! finishes, keyed by the DFIR's location (cluster members are summed). Nothing here changes a
//! scheduling decision.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use dfir_rs::scheduled::metrics::DfirMetrics;

use super::hold_one_hook::hook_id;
use super::schedule_extension::{HookMetadata, ScheduleExtension};

/// The records a simulation run moved, by kind and by place. See the [module docs](self) for
/// what each count means and which one the checker's verdict reads.
///
/// Every count is kept twice: summed over cluster members (the `*_by_place` maps' totals) and
/// per member. The per-member view matters because a total can hide a reaction: a follower
/// that starts sending vote requests while the leader stops sending heartbeats can leave the
/// cluster's message total unchanged or lower. [`WorkCounts::places`] gives the finest view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkCounts {
    /// Records released into ticks by each `use::batch` hook, keyed by the hook's
    /// `location#index [item type]` identity, summed over cluster members.
    pub hook_releases: BTreeMap<String, u64>,
    /// The same, keyed by hook identity and cluster member (`None` for a process).
    pub hook_releases_by_member: BTreeMap<(String, Option<u32>), u64>,
    /// Network messages serialized for sending, keyed by the sending location, summed over
    /// members.
    pub network_messages: BTreeMap<String, u64>,
    /// The same, keyed by sending location and member.
    pub network_messages_by_member: BTreeMap<(String, Option<u32>), u64>,
    /// Items read out of DFIR handoffs, keyed by the DFIR's location, summed over members.
    pub handoff_items: BTreeMap<String, u64>,
    /// DFIR subgraph runs, keyed by the DFIR's location, summed over members.
    pub subgraph_runs: BTreeMap<String, u64>,
}

impl WorkCounts {
    /// Total records released into ticks by `use::batch` hooks: the checker's work measure.
    pub fn admitted(&self) -> u64 {
        self.hook_releases.values().sum()
    }

    /// Total network messages sent.
    pub fn network(&self) -> u64 {
        self.network_messages.values().sum()
    }

    /// Total items read out of DFIR handoffs.
    pub fn handoffs(&self) -> u64 {
        self.handoff_items.values().sum()
    }

    /// Total DFIR subgraph runs.
    pub fn runs(&self) -> u64 {
        self.subgraph_runs.values().sum()
    }

    /// Each cluster member's (or process's) outgoing network messages, keyed
    /// `sends from Location @member`. These are the places the checker's verdict reads alongside
    /// [`WorkCounts::admitted`]: a message is work one location imposes on another, and it is
    /// counted per sender because a reaction at one member can be hidden in a total by another
    /// member going quiet.
    pub fn sends_by_member(&self) -> BTreeMap<String, u64> {
        self.network_messages_by_member
            .iter()
            .map(|((loc, member), n)| (format!("sends from {loc}{}", member_suffix(*member)), *n))
            .collect()
    }

    /// Every `use::batch` hook on every member, keyed `hook_id @member`. Reported for
    /// localization; not read by the verdict, because within one location a reaction to delay
    /// can replace one record with another (a completion becomes an abandonment) without adding
    /// work, and this view would count the replacement as a rise.
    pub fn hooks_by_member(&self) -> BTreeMap<String, u64> {
        self.hook_releases_by_member
            .iter()
            .map(|((hook, member), n)| (format!("{hook}{}", member_suffix(*member)), *n))
            .collect()
    }
}

fn member_suffix(member: Option<u32>) -> String {
    member.map(|m| format!(" @{m}")).unwrap_or_default()
}

/// Extension that collects work counts for one simulator execution.
#[derive(Default)]
pub struct WorkCountExtension {
    counts: RefCell<WorkCounts>,
}

impl WorkCountExtension {
    /// Creates an empty collector suitable for installing as a schedule extension.
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// Removes and returns all counts collected so far.
    pub fn take(&self) -> WorkCounts {
        std::mem::take(&mut *self.counts.borrow_mut())
    }
}

impl ScheduleExtension for WorkCountExtension {
    fn hook_released(&self, hook: HookMetadata, records: usize) {
        record_hook_release_into(&mut self.counts.borrow_mut(), hook, records);
    }

    fn dfir_finished(&self, location: &str, member: Option<u32>, metrics: &DfirMetrics) {
        record_dfir_into(&mut self.counts.borrow_mut(), location, member, metrics);
    }
}

fn record_hook_release_into(counts: &mut WorkCounts, hook: HookMetadata, n: usize) {
    if n == 0 {
        return;
    }
    let id = hook_id(hook.location, hook.index, hook.item_type, hook.kind);
    *counts.hook_releases.entry(id.clone()).or_default() += n as u64;
    *counts
        .hook_releases_by_member
        .entry((id, hook.member))
        .or_default() += n as u64;
}

fn record_dfir_into(
    counts: &mut WorkCounts,
    location: &str,
    member: Option<u32>,
    metrics: &DfirMetrics,
) {
    let handoffs: u64 = metrics
        .handoffs
        .values()
        .map(|h| h.total_items_count() as u64)
        .sum();
    let runs: u64 = metrics
        .subgraphs
        .values()
        .map(|s| s.total_run_count() as u64)
        .sum();
    let messages: u64 = metrics
        .subgraphs
        .values()
        .map(|s| s.network_message_count() as u64)
        .sum();
    if handoffs > 0 {
        *counts.handoff_items.entry(location.to_owned()).or_default() += handoffs;
    }
    if runs > 0 {
        *counts.subgraph_runs.entry(location.to_owned()).or_default() += runs;
    }
    if messages > 0 {
        *counts
            .network_messages
            .entry(location.to_owned())
            .or_default() += messages;
        *counts
            .network_messages_by_member
            .entry((location.to_owned(), member))
            .or_default() += messages;
    }
}
