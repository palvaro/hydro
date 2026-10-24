//! The hitting-set integer program (docs/design/hydrofrac-lite.md §4.4).

use std::collections::{BTreeMap, BTreeSet};

use good_lp::{
    Expression, ProblemVariables, ResolutionError, Solution, SolverModel, Variable, variable,
};

use super::clause::{Clause, Fault, FaultSet, Node};

/// The bounds on the crashes of one hypothesis (docs/design/hydrofrac-lite.md §4.5).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrashBounds {
    /// The `max_crashes` cap; `None` is unbounded.
    pub cap: Option<usize>,
    /// The number of nodes: at most `nodes - 1` crash (`0`: no such bound, for tests).
    pub nodes: usize,
    /// The checked cluster, if the label lives on one.
    pub checked: Option<CheckedCluster>,
}

/// The members of the checked cluster, and how many of them one hypothesis may crash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedCluster {
    pub nodes: BTreeSet<Node>,
    pub cap: usize,
}

impl CheckedCluster {
    /// The cap is `|members| − 2`: a single survivor agrees with itself.
    pub fn new(members: impl IntoIterator<Item = Node>) -> Self {
        let nodes: BTreeSet<Node> = members.into_iter().collect();
        let cap = nodes.len().saturating_sub(2);
        CheckedCluster { nodes, cap }
    }
}

impl CrashBounds {
    /// The most crashes any hypothesis may hold: the cap and the every-node bound.
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
/// prefers earlier (weaker) candidates. Each tie-break is below `0.5 / n`, so it never trades
/// cardinality (the research branch's `0.5 / n` per position did, for sets of size ≥ 3; §4.4).
pub fn weights(n: usize) -> impl Iterator<Item = f64> {
    let tie_break = 0.5 / (n as f64 * n as f64);
    (0..n).map(move |i| 1.0 + tie_break * i as f64)
}

/// The next hypothesis: a minimum-weight subset of `universe` (sorted weakest first, see
/// [`super::universe`]) that satisfies some clause of every group, keeps crashes within `bounds`
/// and at most one per node, is no superset of a vacuous set, and has not been tried. `None`
/// when no such set exists (proven: the MIP gap is zero).
///
/// The integer program (§4.4), with a binary `x_f` per candidate, minimizing `Σ w_f x_f`
/// ([`weights`]):
/// - cover, per clause `D` with premises `P`: `Σ { x_f : f kills some d ∈ D } + Σ_{p ∈ P}
///   (1 − x_p) ≥ 1` (a premise outside the universe can never be selected: no row). A group of
///   several clauses gets a binary `y_c ≤` that left-hand side per clause, and `Σ_c y_c ≥ 1`:
///   the heuristic for programs with schedule choices, where a hypothesis need only kill one
///   schedule's derivations (§4.9);
/// - at most one crash per node; `Σ_{crash} x_f ≤ min(cap, N − 1)`; at most `|C| − 2` crashes
///   in the checked cluster `C`;
/// - no superset of a vacuous set `V`: `Σ_{f ∈ V} x_f ≤ |V| − 1`;
/// - a no-good per tried set `T`: `Σ_{f ∈ T} x_f − Σ_{f ∉ T} x_f ≤ |T| − 1`.
///
/// Only rows that can cut something off are written ([`cover_rows`],
/// [`already_excluded_by_groups`]); both omissions are exact. The dense no-good rows were what
/// made the program slow as tried sets accumulated.
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
    /// Only the rows that can cut something off.
    Lean,
    /// Every row: the reference the lean model is tested against.
    #[cfg(test)]
    Full,
}

/// Whether rows other than its own no-good already exclude the tried set `T`, so the no-good
/// can be left out exactly (it removes only the point `x = T`). Checked on the host:
/// - a group all of whose clauses `T` misses with all their premises in `T` (each cover row
///   sums to `−|P| < 1 − |P|` at `x = T`);
/// - a vacuous set `V ⊆ T`.
///
/// This is the common case: a tried set that held left its derivations alive, and they became
/// clauses it misses.
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

/// The cover rows of `clauses` over `universe`, without those that cannot cut anything off
/// (exact): a row with a premise that is also a hit always holds, and a row `(H, P)` is implied
/// by any `(H', P')` with `H' ⊆ H`, `P' ⊆ P` (so duplicates collapse).
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

/// One cover row per clause whose premises are all candidates (the others bind nothing).
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
        #[cfg(test)]
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
    // A disjunction: some clause of the group is satisfied. A clause whose row always holds
    // (a premise outside the universe, or one that is also a hit) satisfies the group outright.
    let mut disjunction_rows: BTreeSet<Vec<CoverRow>> = BTreeSet::new();
    for group in &disjunctions {
        let mut rows = all_cover_rows(universe, group);
        if rows.len() < group.len() || rows.iter().any(|(h, p)| !h.is_disjoint(p)) {
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

    let crash_sum = |pick: &dyn Fn(&Node) -> bool| -> Expression {
        universe
            .iter()
            .zip(&x)
            .filter(|(f, _)| f.crashed_node().is_some_and(pick))
            .map(|(_, &v)| Expression::from(v))
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
    let nodes: BTreeSet<&Node> = universe.iter().filter_map(Fault::crashed_node).collect();
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
