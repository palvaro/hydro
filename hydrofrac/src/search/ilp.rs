//! The hitting-set integer program (docs/design/hydrofrac-lite.md §4.4).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};

use good_lp::{
    Expression, ProblemVariables, ResolutionError, Solution, SolverModel, Variable, variable,
};

use super::clause::{Clause, Fault, FaultSet, Node};

/// The bounds on the crashes of one hypothesis.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrashBounds {
    /// An explicit cap on the number of crashes. `None` is unbounded.
    pub cap: Option<usize>,
    /// The number of nodes of the program. Crashing every node is vacuous, so a hypothesis
    /// holds at most `nodes - 1` crashes. `0` means no such bound (for tests).
    pub nodes: usize,
    /// The members of the checked cluster, and how many of them may crash in one hypothesis.
    /// `None` when nothing is checked at a cluster.
    pub checked: Option<CheckedCluster>,
}

/// The members of the cluster a label is checked at, and the cap on how many of them one
/// hypothesis may crash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedCluster {
    /// The members.
    pub nodes: BTreeSet<Node>,
    /// At most this many of `nodes` crash in one hypothesis.
    pub cap: usize,
}

impl CheckedCluster {
    /// The checked cluster made of `members`, with the cap `|members| − 2`: crashing all of
    /// them, or all but one, is vacuous, since a single survivor agrees with itself.
    pub fn new(members: impl IntoIterator<Item = Node>) -> Self {
        let nodes: BTreeSet<Node> = members.into_iter().collect();
        let cap = nodes.len().saturating_sub(2);
        CheckedCluster { nodes, cap }
    }
}

impl CrashBounds {
    /// Bounds with an explicit cap and nothing else.
    pub fn cap(crashes: usize) -> Self {
        CrashBounds {
            cap: Some(crashes),
            ..CrashBounds::default()
        }
    }

    /// The largest number of crashes any hypothesis may hold under these bounds, if any. This
    /// counts the cap and the every-node bound; the checked-cluster cap bounds only a subset.
    pub fn max_crashes(&self) -> Option<usize> {
        let all_nodes = (self.nodes >= 1).then(|| self.nodes - 1);
        match (self.cap, all_nodes) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// These bounds with the cap lowered to `crashes`.
    pub fn capped_at(&self, crashes: usize) -> Self {
        CrashBounds {
            cap: Some(self.cap.map_or(crashes, |cap| cap.min(crashes))),
            ..self.clone()
        }
    }
}

impl Display for CrashBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cap {
            Some(cap) => write!(f, "at most {cap} crash(es)")?,
            None => write!(f, "crashes unbounded")?,
        }
        if self.nodes >= 1 {
            write!(f, "; not all {} nodes at once", self.nodes)?;
        }
        if let Some(checked) = &self.checked {
            write!(
                f,
                "; at most {} of the checked cluster's {} member(s)",
                checked.cap,
                checked.nodes.len()
            )?;
        }
        Ok(())
    }
}

/// The ILP backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Backend {
    /// `microlp`: pure Rust, branch-and-bound. The default.
    #[default]
    Microlp,
    /// HiGHS, for cross-checking `microlp` (crate feature `highs`).
    #[cfg(feature = "highs")]
    Highs,
}

/// The weight of each candidate in a universe of `n`, by position: `1` plus a tie-break that
/// prefers earlier (weaker) candidates.
///
/// The tie-break must never trade cardinality. Every set of `k` candidates must cost less than
/// every set of `k + 1`. Each tie-break is below `0.5 / n`, so even a set of all `n` candidates
/// gains less than `0.5` in total. (The research branch's `0.5 / n` per position let a
/// `k`-set of late candidates cost more than a `(k+1)`-set of early ones once `k ≥ 3`.)
pub fn weights(n: usize) -> impl Iterator<Item = f64> {
    let tie_break = 0.5 / (n as f64 * n as f64);
    (0..n).map(move |i| 1.0 + tie_break * i as f64)
}

/// The next hypothesis: a minimum-weight subset of `universe` (sorted weakest first, see
/// [`super::universe`]) that kills some dependency of every clause, keeps crashes within
/// `bounds` and at most one crash per node, is no superset of a vacuous set, and has not been
/// tried. Returns `None` when no such set exists.
///
/// With no clauses this enumerates every admissible subset by weight: the blind search used to
/// cross-check the lineage.
///
/// The integer program:
/// - a binary `x_f` per candidate fault `f`;
/// - minimize `Σ w_f x_f` ([`weights`]);
/// - cover: for every clause `D` with premises `P`,
///   `Σ { x_f : f kills some d ∈ D } + Σ_{p ∈ P} (1 − x_p) ≥ 1`. A premise outside the universe
///   can never be selected, so its clause applies to no hypothesis and is dropped;
/// - `Σ_{f = crash(n, ·)} x_f ≤ 1` per node. An earlier crash of the same node dominates a later
///   one;
/// - the crash bounds: `Σ_{crash} x_f ≤ cap`; `Σ_{crash} x_f ≤ N − 1`; and
///   `Σ_{crash of a member of C} x_f ≤ |C| − 2` for the checked cluster `C`;
/// - no superset of a vacuous set `V`: `Σ_{f ∈ V} x_f ≤ |V| − 1`;
/// - a no-good per tried set `T`: `Σ_{f ∈ T} x_f − Σ_{f ∉ T} x_f ≤ |T| − 1`, excluding exactly `T`.
///   A tried set with a fault outside the universe cannot be proposed, so it needs no row.
///
/// Only the rows that can cut something off are written: a tried set gets a no-good only when no
/// other row already excludes it (`already_excluded`), and implied cover rows are left out
/// (`cover_rows`). Both are exact: the feasible region is that of the full program. No-good
/// rows are dense (one nonzero per candidate) and were what made the program slow as tried sets
/// accumulated; in a search where every run that holds yields a new derivation, none is needed.
///
/// The MIP gap is zero, so a returned set is proven optimal, and `None` is proven infeasible.
pub fn hitting_set<M: Ord + Clone>(
    universe: &[Fault<M>],
    clauses: &[Clause<M>],
    tried: &BTreeSet<FaultSet<M>>,
    vacuous: &[FaultSet<M>],
    bounds: &CrashBounds,
    backend: Backend,
) -> Option<FaultSet<M>> {
    solve(
        universe,
        clauses,
        tried,
        vacuous,
        bounds,
        backend,
        Model::Lean,
    )
}

/// [`hitting_set`] with disjunctive clauses: each group is satisfied when the hypothesis kills
/// *some* clause of it (or misses one of that clause's premises), rather than every one. A group
/// of one clause is an ordinary cover row; a group with several gets a binary per clause,
/// `y_c ≤ Σ { x_f : f kills some d ∈ c } + Σ_{p ∈ P_c} (1 − x_p)`, and the row `Σ_c y_c ≥ 1`.
///
/// This is the search heuristic for schedule-sensitive labels (docs/design/hydrofrac-lite.md
/// §4.9): a run's group is the derivations its schedules observed, and a hypothesis need only
/// kill one schedule's. It is not a pruning rule a certificate can rest on.
pub fn hitting_set_any<M: Ord + Clone>(
    universe: &[Fault<M>],
    groups: &[&[Clause<M>]],
    tried: &BTreeSet<FaultSet<M>>,
    vacuous: &[FaultSet<M>],
    bounds: &CrashBounds,
    backend: Backend,
) -> Option<FaultSet<M>> {
    solve_groups(
        universe,
        groups,
        tried,
        vacuous,
        bounds,
        backend,
        Model::Lean,
    )
}

/// Whether no hypothesis can satisfy `group`: every clause of it is unkillable.
pub(crate) fn group_is_unkillable<M: Ord + Clone>(group: &[Clause<M>]) -> bool {
    !group.is_empty() && group.iter().all(Clause::is_unkillable)
}

/// How much of the integer program is written out. Both have the same feasible region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Model {
    /// Only the rows that can cut something off: no-goods only for tried sets nothing else
    /// excludes ([`already_excluded`]), and cover rows without the implied ones
    /// ([`cover_rows`]). What [`hitting_set`] uses.
    Lean,
    /// Every row, as the model's definition reads. The reference the lean model is tested
    /// against.
    #[cfg_attr(not(test), expect(dead_code, reason = "test reference"))]
    Full,
}

/// Whether the rows other than `tried`'s own no-good already exclude the tried set `tried`, so
/// its no-good can be left out without changing the feasible set.
///
/// The no-good of `T` removes exactly one point, `x = T`, from the feasible region. If some other
/// row already rejects `x = T`, then the region with and without the no-good is the same, so the
/// optimum, and infeasibility, are unchanged: the omission is exact, not a relaxation. Two kinds
/// of row reject `x = T` and are checked here, on the host, without the solver:
/// - **a cover row** of a clause `D` with premises `P`, when no fault of `T` kills a delivery of
///   `D` and `P ⊆ T`. Then the left-hand side at `x = T` is `0 − |P|`, below the bound `1 − |P|`.
///   The row exists: `P ⊆ T` and `T` lies inside the universe (otherwise `T` needs no row
///   anyway), so every premise is a candidate.
/// - **a vacuous row** of a vacuous set `V ⊆ T`: at `x = T` it sums to `|V| > |V| − 1`. If `V`
///   has a fault outside the universe then so does `T`, which then needs no row anyway.
///
/// This is the common case. Once a hypothesis `T` has run and the label held, the run's
/// derivations are clauses, and `T` did not kill them (it ran and they were observed), so `T`
/// fails to hit one of them. An explicit no-good is only needed for a `T` whose run taught the
/// search nothing it did not already know.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the one-clause-per-group form, for the tests")
)]
pub(crate) fn already_excluded<M: Ord + Clone>(
    tried: &FaultSet<M>,
    clauses: &[Clause<M>],
    vacuous: &[FaultSet<M>],
) -> bool {
    let groups: Vec<&[Clause<M>]> = clauses.iter().map(std::slice::from_ref).collect();
    already_excluded_by_groups(tried, &groups, vacuous)
}

/// [`already_excluded`] with disjunctive groups: a group rejects `x = T` when it rejects it
/// through every clause, each missed by `T` with all its premises in `T` (its `y` binaries are
/// then all forced to 0, and its row `Σ y ≥ 1` fails).
pub(crate) fn already_excluded_by_groups<M: Ord + Clone>(
    tried: &FaultSet<M>,
    groups: &[&[Clause<M>]],
    vacuous: &[FaultSet<M>],
) -> bool {
    let rejects = |clause: &Clause<M>| {
        clause.premises.is_subset(tried) && !tried.iter().any(|f| clause.hit_by(f))
    };
    vacuous.iter().any(|v| v.is_subset(tried))
        || groups
            .iter()
            .any(|group| !group.is_empty() && group.iter().all(&rejects))
}

/// One cover row: the positions of the candidates that kill a delivery of the clause, and the
/// positions of its premises. Read as "some hit is selected, or some premise is not".
pub(crate) type CoverRow = (BTreeSet<usize>, BTreeSet<usize>);

/// The cover rows of `clauses` over `universe`, without the rows that cannot cut anything off.
///
/// Dropping a row that every point satisfying the kept rows also satisfies leaves the feasible
/// region unchanged, so this is exact:
/// - a clause with a premise outside the universe binds no hypothesis (the premise can never be
///   selected);
/// - a row with a premise that is also a hit is always satisfied: either it is selected, a hit,
///   or not, a missing premise;
/// - a row `(H, P)` is implied by any row `(H', P')` with `H' ⊆ H` and `P' ⊆ P`: a point that
///   selects a hit of `H'` selects one of `H`, and a point missing a premise of `P'` misses one
///   of `P`. In particular, duplicates collapse to one.
///
/// Clauses repeat a lot in practice: every run of a hypothesis that leaves a derivation intact
/// observes it again.
pub(crate) fn cover_rows<M: Ord + Clone>(
    universe: &[Fault<M>],
    clauses: &[Clause<M>],
) -> Vec<CoverRow> {
    let rows: BTreeSet<CoverRow> = all_cover_rows(universe, clauses)
        .into_iter()
        .filter(|(hits, premises)| hits.is_disjoint(premises))
        .collect();
    // Strongest first, so a row is only ever compared against rows that might imply it.
    let mut rows: Vec<CoverRow> = rows.into_iter().collect();
    rows.sort_by_key(|(h, p)| h.len() + p.len());
    let mut kept: Vec<CoverRow> = vec![];
    for row in rows {
        let implied = kept
            .iter()
            .any(|(h, p)| h.is_subset(&row.0) && p.is_subset(&row.1));
        if !implied {
            kept.push(row);
        }
    }
    kept
}

/// One cover row per clause whose premises are all candidates: the program as defined.
pub(crate) fn all_cover_rows<M: Ord + Clone>(
    universe: &[Fault<M>],
    clauses: &[Clause<M>],
) -> Vec<CoverRow> {
    let index: BTreeMap<&Fault<M>, usize> =
        universe.iter().enumerate().map(|(i, f)| (f, i)).collect();
    clauses
        .iter()
        .filter_map(|clause| {
            let premises: BTreeSet<usize> = clause
                .premises
                .iter()
                .map(|p| index.get(p).copied())
                .collect::<Option<_>>()?;
            let hits: BTreeSet<usize> = (0..universe.len())
                .filter(|&i| clause.hit_by(&universe[i]))
                .collect();
            Some((hits, premises))
        })
        .collect()
}

pub(crate) fn solve<M: Ord + Clone>(
    universe: &[Fault<M>],
    clauses: &[Clause<M>],
    tried: &BTreeSet<FaultSet<M>>,
    vacuous: &[FaultSet<M>],
    bounds: &CrashBounds,
    backend: Backend,
    model: Model,
) -> Option<FaultSet<M>> {
    let groups: Vec<&[Clause<M>]> = clauses.iter().map(std::slice::from_ref).collect();
    solve_groups(universe, &groups, tried, vacuous, bounds, backend, model)
}

pub(crate) fn solve_groups<M: Ord + Clone>(
    universe: &[Fault<M>],
    groups: &[&[Clause<M>]],
    tried: &BTreeSet<FaultSet<M>>,
    vacuous: &[FaultSet<M>],
    bounds: &CrashBounds,
    backend: Backend,
    model: Model,
) -> Option<FaultSet<M>> {
    if groups.iter().any(|g| group_is_unkillable(g)) {
        return None;
    }
    // The single-clause groups are ordinary cover rows; the others are disjunctions.
    let clauses: Vec<Clause<M>> = groups
        .iter()
        .filter(|g| g.len() == 1)
        .map(|g| g[0].clone())
        .collect();
    let disjunctions: Vec<&[Clause<M>]> =
        groups.iter().filter(|g| g.len() > 1).copied().collect();
    let index: BTreeMap<&Fault<M>, usize> =
        universe.iter().enumerate().map(|(i, f)| (f, i)).collect();
    let position = |fault: &Fault<M>| index.get(fault).copied();

    let mut vars = ProblemVariables::new();
    let x: Vec<Variable> = universe
        .iter()
        .map(|_| vars.add(variable().binary()))
        .collect();
    let objective: Expression = x
        .iter()
        .zip(weights(universe.len()))
        .map(|(&v, w)| w * v)
        .sum();

    let mut constraints = vec![];

    let rows = match model {
        Model::Lean => cover_rows(universe, &clauses),
        Model::Full => all_cover_rows(universe, &clauses),
    };
    let satisfied = |(hits, premises): &CoverRow| -> Expression {
        hits.iter()
            .map(|&i| Expression::from(x[i]))
            .sum::<Expression>()
            + premises
                .iter()
                .map(|&i| 1.0 - Expression::from(x[i]))
                .sum::<Expression>()
    };
    for row in rows {
        constraints.push(satisfied(&row).geq(1.0));
    }
    // A disjunction: some clause of the group is satisfied. A clause whose row is always
    // satisfied (a premise outside the universe, or a premise that is also a hit) satisfies
    // the group outright, so the group needs no row.
    let mut disjunction_rows: BTreeSet<Vec<CoverRow>> = BTreeSet::new();
    for group in &disjunctions {
        let index: BTreeMap<&Fault<M>, usize> =
            universe.iter().enumerate().map(|(i, f)| (f, i)).collect();
        let mut rows: Vec<CoverRow> = vec![];
        let mut always = false;
        for clause in group.iter() {
            let Some(premises) = clause
                .premises
                .iter()
                .map(|p| index.get(p).copied())
                .collect::<Option<BTreeSet<usize>>>()
            else {
                always = true;
                break;
            };
            let hits: BTreeSet<usize> = (0..universe.len())
                .filter(|&i| clause.hit_by(&universe[i]))
                .collect();
            if !hits.is_disjoint(&premises) {
                always = true;
                break;
            }
            rows.push((hits, premises));
        }
        if always {
            continue;
        }
        rows.sort();
        rows.dedup();
        disjunction_rows.insert(rows);
    }
    for rows in disjunction_rows {
        if rows.len() == 1 {
            constraints.push(satisfied(&rows[0]).geq(1.0));
            continue;
        }
        let mut any = Expression::from(0.0);
        for row in &rows {
            let y = vars.add(variable().binary());
            constraints.push(Expression::from(y).leq(satisfied(row)));
            any += y;
        }
        constraints.push(any.geq(1.0));
    }

    let crashes: Vec<usize> = (0..universe.len())
        .filter(|&i| universe[i].crashed_node().is_some())
        .collect();
    let crash_sum = |pick: &dyn Fn(&Node) -> bool| -> Expression {
        crashes
            .iter()
            .filter(|&&i| universe[i].crashed_node().is_some_and(pick))
            .map(|&i| Expression::from(x[i]))
            .sum()
    };
    // With at most one crash per node (below), the sum of the crash variables is the number of
    // crashed nodes.
    if let Some(max) = bounds.max_crashes() {
        constraints.push(crash_sum(&|_| true).leq(max as f64));
    }
    if let Some(checked) = &bounds.checked {
        constraints.push(crash_sum(&|n| checked.nodes.contains(n)).leq(checked.cap as f64));
    }
    let nodes: BTreeSet<&Node> = crashes
        .iter()
        .filter_map(|&i| universe[i].crashed_node())
        .collect();
    for node in nodes {
        constraints.push(crash_sum(&|n| n == node).leq(1.0));
    }

    let inside = |set: &FaultSet<M>| -> Option<Vec<usize>> { set.iter().map(position).collect() };
    for set in vacuous {
        if let Some(inside) = inside(set) {
            let selected: Expression = inside.iter().map(|&i| Expression::from(x[i])).sum();
            constraints.push(selected.leq(inside.len() as f64 - 1.0));
        }
    }
    for set in tried {
        if model == Model::Lean && already_excluded_by_groups(set, groups, vacuous) {
            continue;
        }
        if let Some(inside) = inside(set) {
            let inside: BTreeSet<usize> = inside.into_iter().collect();
            let mut no_good = Expression::from(0.0);
            for (i, &v) in x.iter().enumerate() {
                if inside.contains(&i) {
                    no_good += v;
                } else {
                    no_good -= v;
                }
            }
            constraints.push(no_good.leq(inside.len() as f64 - 1.0));
        }
    }

    let chosen = match backend {
        Backend::Microlp => {
            let mut model = vars.minimise(objective).using(good_lp::microlp);
            for c in constraints {
                model.add_constraint(c);
            }
            read(model.solve(), &x)
        }
        #[cfg(feature = "highs")]
        Backend::Highs => {
            let mut model = vars
                .minimise(objective)
                .using(good_lp::highs)
                .set_mip_rel_gap(0.0)
                .expect("a zero gap is valid")
                .set_mip_abs_gap(0.0)
                .expect("a zero gap is valid");
            for c in constraints {
                model.add_constraint(c);
            }
            read(model.solve(), &x)
        }
    }?;
    Some(chosen.into_iter().map(|i| universe[i].clone()).collect())
}

/// The selected positions of a solution, `None` if the program is infeasible.
fn read<S: Solution>(solved: Result<S, ResolutionError>, x: &[Variable]) -> Option<Vec<usize>> {
    match solved {
        Ok(solution) => Some(
            x.iter()
                .enumerate()
                .filter(|(_, v)| solution.value(**v) > 0.5)
                .map(|(i, _)| i)
                .collect(),
        ),
        Err(ResolutionError::Infeasible) => None,
        Err(error) => panic!("hitting-set ILP failed: {error}"),
    }
}
