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
/// The MIP gap is zero, so a returned set is proven optimal, and `None` is proven infeasible.
pub fn hitting_set<M: Ord + Clone>(
    universe: &[Fault<M>],
    clauses: &[Clause<M>],
    tried: &BTreeSet<FaultSet<M>>,
    vacuous: &[FaultSet<M>],
    bounds: &CrashBounds,
    backend: Backend,
) -> Option<FaultSet<M>> {
    if clauses.iter().any(Clause::is_unkillable) {
        return None;
    }
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

    for clause in clauses {
        let mut covering: Expression = x
            .iter()
            .zip(universe)
            .filter(|(_, f)| clause.hit_by(f))
            .map(|(&v, _)| Expression::from(v))
            .sum();
        let mut bound = 1.0;
        let mut applies = true;
        for premise in &clause.premises {
            match position(premise) {
                Some(i) => {
                    covering -= x[i];
                    bound -= 1.0;
                }
                None => applies = false,
            }
        }
        if applies {
            constraints.push(covering.geq(bound));
        }
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
