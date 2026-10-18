//! Lineage-level ablation for reliable broadcast and reliable delivery: the same LDFI search
//! at every [`Level`] (execution, static slice, record provenance, counterfactual replay) on
//! the same provenance-compiled program, per protocol configuration.
//!
//! Run explicitly; it writes a Markdown fragment to the path in `LDFI_ABLATION_OUT` and does
//! nothing without it:
//!
//! ```text
//! LDFI_ABLATION_OUT=target/ablation.md cargo test -p hydro_test level_ablation_broadcast -- --test-threads=1 --nocapture
//! ```
//!
//! `LDFI_ABLATION_BUDGET_SECS` (default 1200) bounds the total time: configurations that
//! would start after it are reported as skipped. `LDFI_ABLATION_ONLY=broadcast|delivery`
//! runs one protocol. `LDFI_ABLATION_VERBOSE` keeps the per-hypothesis progress lines.

#[cfg(test)]
mod ablation {
    use std::fmt::Write as _;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::time::Instant;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::ldfi::{Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::Spec;
    use hydro_lang::sim_hooks::{BatchHook, OnProcess};

    use crate::cluster::reliable_broadcast::{
        Broadcaster, EchoHook, InitialHook, Member, Payload, broadcast,
    };
    use crate::distributed::reliable_delivery::{Ack, Msg, Receiver, Sender, reliable_delivery};

    const LEVELS: [(&str, Level); 4] = [
        ("L0/exec", Level::Execution),
        ("L1/slice", Level::Slice),
        ("L2/record", Level::Record),
        ("L3/replay", Level::Counterfactual),
    ];

    /// One measurement: one configuration at one level.
    struct Cell {
        verdict: String,
        hypotheses: usize,
        schedules: usize,
        /// Mean number of dependencies per derivation, over the derivations of goal-held
        /// schedules; `None` when no derivation was recorded.
        mean_deriv: Option<f64>,
        replays: usize,
        fallbacks: usize,
        secs: f64,
    }

    // Free functions rather than an `impl`: the staging copy of this crate drops impl blocks
    // in test modules.
    fn verdict(o: &Outcome) -> &'static str {
        match o {
            Outcome::Counterexample { .. } => "violated",
            Outcome::Certificate { .. } => "certificate",
            Outcome::Exhausted { .. } => "exhausted",
            Outcome::Budget { .. } => "budget",
            Outcome::Vacuous { .. } => "vacuous",
        }
    }

    fn measure(outcome: &Outcome, secs: f64) -> Cell {
        let schedules = outcome.runs().iter().flat_map(|h| &h.schedules);
        let (mut leaves, mut derivations, mut replays, mut fallbacks) = (0usize, 0usize, 0, 0);
        for s in schedules {
            replays += s.replays;
            fallbacks += s.replay_fallbacks;
            if s.goal_held {
                for d in &s.derivations {
                    leaves += d.len();
                    derivations += 1;
                }
            }
        }
        Cell {
            verdict: verdict(outcome).to_string(),
            hypotheses: outcome.runs().len(),
            schedules: outcome.schedules(),
            mean_deriv: (derivations > 0).then(|| leaves as f64 / derivations as f64),
            replays,
            fallbacks,
            secs,
        }
    }

    fn unavailable(reason: impl Into<String>) -> Cell {
        Cell {
            verdict: reason.into(),
            hypotheses: 0,
            schedules: 0,
            mean_deriv: None,
            replays: 0,
            fallbacks: 0,
            secs: 0.0,
        }
    }

    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        let msg = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "non-string panic".to_string());
        let msg = msg.lines().next().unwrap_or("").to_string();
        if msg.len() > 80 {
            format!("{}…", &msg[..80])
        } else {
            msg
        }
    }

    fn format_cell(c: &Cell) -> String {
        if c.hypotheses == 0 {
            return c.verdict.clone();
        }
        let deriv = c
            .mean_deriv
            .map(|d| format!("{d:.2}"))
            .unwrap_or_else(|| "–".to_string());
        let mut s = format!(
            "{} / {} / {} / {} / {:.1}s",
            c.verdict, c.hypotheses, c.schedules, deriv, c.secs
        );
        if c.replays > 0 || c.fallbacks > 0 {
            write!(s, " (r{} f{})", c.replays, c.fallbacks).unwrap();
        }
        s
    }

    /// Renders one protocol's table: rows are configurations, columns are levels.
    fn table(rows: &[(String, Vec<Cell>)]) -> String {
        let mut out = String::new();
        out.push_str("| config |");
        for (name, _) in LEVELS {
            write!(out, " {name} |").unwrap();
        }
        out.push('\n');
        out.push_str("|---|");
        for _ in LEVELS {
            out.push_str("---|");
        }
        out.push('\n');
        for (config, cells) in rows {
            write!(out, "| {config} |").unwrap();
            for c in cells {
                write!(out, " {} |", format_cell(c)).unwrap();
            }
            out.push('\n');
        }
        out.push_str(
            "\nCells: `verdict / hypotheses / schedules / mean derivation size / wall secs`; \
             `(rN fM)` = counterfactual replays and replay fallbacks.\n",
        );
        out
    }

    fn quiet(ldfi: Ldfi) -> Ldfi {
        if std::env::var("LDFI_ABLATION_VERBOSE").is_ok() {
            ldfi
        } else {
            ldfi.quiet()
        }
    }

    /// Runs one search per level on an already-compiled program. Each search is isolated by
    /// `catch_unwind`, so a level the program does not support is a cell, not a crash.
    fn run_levels(provenance: bool, search: impl Fn(Level) -> Outcome) -> Vec<Cell> {
        LEVELS
            .iter()
            .map(|&(name, level)| {
                if level == Level::Record && !provenance {
                    return unavailable("n/a: provenance compile failed");
                }
                let start = Instant::now();
                match catch_unwind(AssertUnwindSafe(|| search(level))) {
                    Ok(outcome) => {
                        let cell = measure(&outcome, start.elapsed().as_secs_f64());
                        eprintln!("  {name}: {}", format_cell(&cell));
                        cell
                    }
                    Err(payload) => {
                        let reason = format!("n/a: panic: {}", panic_message(payload));
                        eprintln!("  {name}: {reason}");
                        unavailable(reason)
                    }
                }
            })
            .collect()
    }

    // ---------------------------------------------------------------------------------------
    // Reliable broadcast
    // ---------------------------------------------------------------------------------------

    /// How `POST` is written (see `reliable_broadcast::tests`).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Post {
        Positive,
        Negated,
    }

    /// The broadcast search of `reliable_broadcast::tests::search`, compiled once (with
    /// provenance when `provenance`) and searched at every level.
    fn broadcast_levels(
        echo: bool,
        members_n: u32,
        crashes: usize,
        post: Post,
        provenance: bool,
    ) -> Vec<Cell> {
        let mut flow = FlowBuilder::new();
        let broadcaster = flow.process::<Broadcaster>();
        let members = flow.cluster::<Member>();
        let initial_hook: InitialHook = flow.sim_hook();
        let echo_hook: EchoHook = flow.sim_hook();

        let (source_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
        let deliveries = broadcast(
            &members,
            source,
            echo,
            nondet!(/** traced by the search */ hook = initial_hook),
            nondet!(/** traced by the search */ hook = echo_hook),
        );

        let mut spec = Spec::new(&mut flow);
        let delivered = spec.observe_cluster(deliveries);
        let correct = spec.correct_members(&members, 0..members_n);
        let delivered_m = delivered.filter(q!(|(_, p)| p.0 == 42));
        spec.pre(delivered_m.clone().first());
        match post {
            Post::Positive => {
                let delivering = delivered_m.map(q!(|(m, _)| (m, ()))).unique();
                let correct_delivering = correct
                    .clone()
                    .map(q!(|m| (m, ())))
                    .join(delivering)
                    .count();
                spec.post(correct_delivering.zip(correct.count()).filter(q!(|(
                    delivering,
                    correct,
                )| {
                    delivering == correct
                })));
            }
            Post::Negated => {
                let delivering = delivered_m.map(q!(|(m, _)| m)).unique();
                let missing = correct.clone().filter_not_in(delivering);
                spec.post(missing.count().filter(q!(|n| *n == 0)));
            }
        }

        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, members_n as usize);
        if provenance {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        run_levels(provenance, |level| {
            quiet(Ldfi::new())
                .level(level)
                .crashable("broadcaster", &broadcaster)
                .crashable_cluster("member", &members, 0..members_n)
                .crash_budget(crashes)
                .spec(&spec)
                .search(&compiled, async |_run| {
                    source_send.send(Payload(42));
                    hydro_lang::sim::quiesce().await;
                })
        })
    }

    /// Tries the provenance-compiled program first; if the rewrite panics, recompiles without
    /// provenance (Level 2 is then reported as unavailable).
    fn broadcast_config(echo: bool, members_n: u32, crashes: usize, post: Post) -> Vec<Cell> {
        match catch_unwind(AssertUnwindSafe(|| {
            broadcast_levels(echo, members_n, crashes, post, true)
        })) {
            Ok(cells) => cells,
            Err(payload) => {
                eprintln!(
                    "  provenance compile failed ({}); recompiling without",
                    panic_message(payload)
                );
                broadcast_levels(echo, members_n, crashes, post, false)
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Reliable delivery
    // ---------------------------------------------------------------------------------------

    /// The delivery search of `reliable_delivery::tests::search`, compiled once and searched
    /// at every level.
    fn delivery_levels(
        max_attempts: Option<u32>,
        eot: u32,
        eff: u32,
        hooks: bool,
        provenance: bool,
    ) -> Vec<Cell> {
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
        if provenance {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        run_levels(provenance, |level| {
            quiet(Ldfi::new())
                .level(level)
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
        })
    }

    fn delivery_config(max_attempts: Option<u32>, eot: u32, eff: u32, hooks: bool) -> Vec<Cell> {
        match catch_unwind(AssertUnwindSafe(|| {
            delivery_levels(max_attempts, eot, eff, hooks, true)
        })) {
            Ok(cells) => cells,
            Err(payload) => {
                eprintln!(
                    "  provenance compile failed ({}); recompiling without",
                    panic_message(payload)
                );
                delivery_levels(max_attempts, eot, eff, hooks, false)
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // The grid
    // ---------------------------------------------------------------------------------------

    fn budget_secs() -> f64 {
        std::env::var("LDFI_ABLATION_BUDGET_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1200.0)
    }

    fn skipped() -> Vec<Cell> {
        LEVELS
            .iter()
            .map(|_| unavailable("skipped (time budget)"))
            .collect()
    }

    /// Runs `configs` in order, skipping those that would start after the time budget.
    fn grid(
        start: &Instant,
        configs: Vec<(String, Box<dyn Fn() -> Vec<Cell>>)>,
    ) -> Vec<(String, Vec<Cell>)> {
        let budget = budget_secs();
        configs
            .into_iter()
            .map(|(name, run)| {
                eprintln!("[{:.0}s] {name}", start.elapsed().as_secs_f64());
                if start.elapsed().as_secs_f64() > budget {
                    eprintln!("  skipped: over the time budget");
                    return (name, skipped());
                }
                let cells = run();
                (name, cells)
            })
            .collect()
    }

    #[test]
    fn level_ablation_broadcast_and_delivery() {
        let Ok(out_path) = std::env::var("LDFI_ABLATION_OUT") else {
            return;
        };
        let start = Instant::now();

        let only = std::env::var("LDFI_ABLATION_ONLY").ok();
        let wanted = |section: &str| only.as_deref().is_none_or(|o| o == section);
        let mut md = String::new();

        // Reliable broadcast: `MEMBERS` members (3 in the unit tests, 4 as a size axis), the
        // broadcaster and every member crashable, under the positive and the negated `POST`.
        let broadcast_configs: Vec<(String, Box<dyn Fn() -> Vec<Cell>>)> = vec![
            (
                "plain, n=3, 1 crash, POST positive".into(),
                Box::new(|| broadcast_config(false, 3, 1, Post::Positive)),
            ),
            (
                "reliable (echo), n=3, 1 crash, POST positive".into(),
                Box::new(|| broadcast_config(true, 3, 1, Post::Positive)),
            ),
            (
                "reliable (echo), n=3, 2 crashes, POST positive".into(),
                Box::new(|| broadcast_config(true, 3, 2, Post::Positive)),
            ),
            (
                "plain, n=3, 1 crash, POST negated".into(),
                Box::new(|| broadcast_config(false, 3, 1, Post::Negated)),
            ),
            (
                "reliable (echo), n=3, 1 crash, POST negated".into(),
                Box::new(|| broadcast_config(true, 3, 1, Post::Negated)),
            ),
            (
                "plain, n=4, 1 crash, POST positive".into(),
                Box::new(|| broadcast_config(false, 4, 1, Post::Positive)),
            ),
        ];
        // Not in the grid: reliable broadcast at n=4 explores 92,160 schedules per hypothesis
        // exhaustively (about 5 minutes each), which no level brings within the budget.
        let broadcast_rows = if wanted("broadcast") {
            grid(&start, broadcast_configs)
        } else {
            vec![]
        };
        md.push_str("## Reliable broadcast\n\n");
        md.push_str(
            "`hydro_test/src/cluster/reliable_broadcast.rs`: the broadcaster and every member \
             crashable (`fail_stop` links, so crashes are the only faults), specification \
             `PRE` = some member delivered `m`, `POST` = every correct member delivered `m` \
             (written positively or through `filter_not_in`). Compiled with provenance; \
             exhaustive schedule exploration.\n\n",
        );
        md.push_str(&table(&broadcast_rows));
        md.push('\n');

        // Reliable delivery: lossy channels, EOT/EFF horizon, with and without hooks on the
        // two batches.
        let delivery_configs: Vec<(String, Box<dyn Fn() -> Vec<Cell>>)> = vec![
            (
                "give up after 2, eot=4 eff=2, no hooks".into(),
                Box::new(|| delivery_config(Some(2), 4, 2, false)),
            ),
            (
                "give up after 2, eot=4 eff=2, hooks".into(),
                Box::new(|| delivery_config(Some(2), 4, 2, true)),
            ),
            (
                "retry until ack, eot=4 eff=2, no hooks".into(),
                Box::new(|| delivery_config(None, 4, 2, false)),
            ),
            (
                "retry until ack, eot=4 eff=2, hooks".into(),
                Box::new(|| delivery_config(None, 4, 2, true)),
            ),
            (
                "give up after 3, eot=6 eff=3, hooks".into(),
                Box::new(|| delivery_config(Some(3), 6, 3, true)),
            ),
            (
                "retry until ack, eot=6 eff=3, hooks".into(),
                Box::new(|| delivery_config(None, 6, 3, true)),
            ),
        ];
        // The fragment so far, in case the second half is cut short.
        std::fs::write(&out_path, &md).expect("LDFI_ABLATION_OUT not writable");
        let delivery_rows = if wanted("delivery") {
            grid(&start, delivery_configs)
        } else {
            vec![]
        };
        md.push_str("## Reliable delivery\n\n");
        md.push_str(
            "`hydro_test/src/distributed/reliable_delivery.rs`: `msg` and `ack` channels \
             `lossy_delayed_forever` (omission is the only fault), `eot` timer rounds with \
             omissions admitted in the first `eff`; goal = the receiver received the message. \
             \"hooks\" binds the two batches to scripted hooks. Compiled with provenance; \
             exhaustive schedule exploration.\n\n",
        );
        md.push_str(&table(&delivery_rows));
        md.push('\n');

        writeln!(
            md,
            "_Total wall time: {:.0}s._\n",
            start.elapsed().as_secs_f64()
        )
        .unwrap();

        std::fs::write(&out_path, &md).expect("LDFI_ABLATION_OUT not writable");
        eprintln!("{md}");
    }
}
