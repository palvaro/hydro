//! The hitting-set integer program over pairs (docs/design/hydrofrac-races.md §4.3).
//!
//! Lite asks for a set that hits every clause. Here a set is worth running if it is admissible
//! for *some* pair: it kills every robust clause of that pair. So the program has one indicator
//! per pair, needs at least one of them, and makes each pair's cover rows conditional on its
//! indicator. A pair's exemptions ([`super::excused`]) bound its indicator from above. The rest
//! is lite's model (`search/ilp.rs`): crash bounds, no-goods, tie-breaks.

use std::collections::{BTreeMap, BTreeSet};

use good_lp::{
    Expression, ProblemVariables, ResolutionError, Solution, SolverModel, Variable, variable,
};

use super::clause::{Candidate, Dead, PairClause, PairKey, Pairs};
use crate::search::{Backend, CrashBounds, Fault, FaultSet, Node, weights};

/// Which clauses of a pair a proposal must kill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// The robust ones: the admissibility rule, the only one a certificate rests on.
    RobustOnly,
    /// Every one seen, robust or fragile: Molly's heuristic, which aims where a counterexample
    /// is likeliest (§4.4, step 2). Its proposals are admissible too, since it asks for more.
    AllKnown,
}

impl Scope {
    fn includes<M>(self, clause: &PairClause<M>) -> bool {
        match self {
            Scope::RobustOnly => clause.robust,
            Scope::AllKnown => true,
        }
    }
}

/// One cover row: the positions of the candidates that kill some delivery of a clause.
type Row = BTreeSet<usize>;

/// What the program needs of one pair: its cover rows (none: every set it does not excuse is
/// admissible for it), and its exemptions, each a set `E` of positions such that selecting all
/// of `E` excuses the pair.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PairRows {
    rows: Vec<Row>,
    exemptions: BTreeSet<Row>,
}

impl PairRows {
    /// Whether the set at these positions is admissible for the pair.
    fn admits(&self, inside: &BTreeSet<usize>) -> bool {
        self.rows.iter().all(|row| !row.is_disjoint(inside))
            && !self.exemptions.iter().any(|e| e.is_subset(inside))
    }
}

/// The cover rows of one pair's in-scope clauses over `universe`, without those another row
/// implies (`A ⊆ B`: hitting `A` hits `B`), and its exemptions over `universe`. `None` if some
/// row is empty: no set over the universe kills that clause, so the pair can never be chosen.
fn pair_rows<P: PairKey, M: Ord + Clone>(
    universe: &[Candidate<M>],
    index: &BTreeMap<&Fault<M>, usize>,
    pair: &P,
    clauses: &[PairClause<M>],
    dead: &Dead<M, P::Fact>,
    scope: Scope,
) -> Option<PairRows> {
    let rows: BTreeSet<Row> = clauses
        .iter()
        .filter(|c| scope.includes(c))
        .map(|c| {
            (0..universe.len())
                .filter(|&i| universe[i].hits(c))
                .collect()
        })
        .collect();
    if rows.iter().any(BTreeSet::is_empty) {
        return None;
    }
    let mut rows: Vec<Row> = rows.into_iter().collect();
    rows.sort_by_key(BTreeSet::len);
    let mut kept: Vec<Row> = vec![];
    for row in rows {
        if !kept.iter().any(|k| k.is_subset(&row)) {
            kept.push(row);
        }
    }
    // An exemption with a fault outside the universe can never be fully selected.
    let inside =
        |set: &FaultSet<M>| -> Option<Row> { set.iter().map(|f| index.get(f).copied()).collect() };
    let crashed = FaultSet::from([Fault::crash_after(pair.member().clone(), None)]);
    let exemptions = std::iter::once(&crashed)
        .chain(
            dead.iter()
                .filter(|(_, fact)| fact == pair.fact())
                .map(|(set, _)| set),
        )
        .filter_map(inside)
        .collect();
    Some(PairRows {
        rows: kept,
        exemptions,
    })
}

/// The next hypothesis: a minimum-weight subset of `universe` (weakest first, see
/// [`super::candidates`]) that is admissible for some pair (kills every in-scope clause of it,
/// and does not [excuse](super::excused) it), keeps crashes within `bounds` and at most one per
/// node, and is not in `tried`. `None` when no such set exists (proven: the solver closes the
/// gap).
///
/// The program, with a binary `x_f` per candidate and `z_p` per pair, minimizing `Σ w_f x_f`
/// ([`weights`]):
/// - `Σ_p z_p ≥ 1`;
/// - per pair `p` and in-scope clause `c` of `p`: `Σ { x_f : f kills c } ≥ z_p`;
/// - per pair `p` of member `m`: `z_p + x_{crash(m)} ≤ 1`;
/// - per pair `p` of fact `f` and dead set `D` of `f`: `z_p + Σ_{g ∈ D} x_g ≤ |D|`;
/// - at most one crash per node; `Σ_{crash} x_f ≤ min(cap, N − 1)`; at most the checked
///   cluster's cap among its members;
/// - a no-good per tried set `T`: `Σ_{f ∈ T} x_f − Σ_{f ∉ T} x_f ≤ |T| − 1`.
///
/// The no-good of a tried set that no pair admits is left out, exactly, as in lite: the other
/// rows already exclude it.
pub fn propose<P: PairKey, M: Ord + Clone>(
    universe: &[Candidate<M>],
    pairs: &Pairs<P, M>,
    dead: &Dead<M, P::Fact>,
    tried: &BTreeSet<FaultSet<M>>,
    bounds: &CrashBounds,
    scope: Scope,
    backend: Backend,
) -> Option<BTreeSet<Candidate<M>>> {
    let index: BTreeMap<&Fault<M>, usize> = universe
        .iter()
        .enumerate()
        .filter_map(|(i, c)| match c {
            Candidate::Fault(f) => Some((f, i)),
            Candidate::Unnamed(_) => None,
        })
        .collect();
    let pair_rows: Vec<PairRows> = pairs
        .iter()
        .filter_map(|(pair, clauses)| pair_rows(universe, &index, pair, clauses, dead, scope))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if pair_rows.is_empty() {
        return None;
    }

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

    let sum =
        |positions: &Row| -> Expression { positions.iter().map(|&i| Expression::from(x[i])).sum() };
    let mut any = Expression::from(0.0);
    for pair in &pair_rows {
        let z = vars.add(variable().binary());
        for row in &pair.rows {
            constraints.push(sum(row).geq(z));
        }
        for exemption in &pair.exemptions {
            constraints.push((sum(exemption) + z).leq(exemption.len() as f64));
        }
        any += z;
    }
    constraints.push(any.geq(1.0));

    let crash_sum = |pick: &dyn Fn(&Node) -> bool| -> Expression {
        universe
            .iter()
            .zip(&x)
            .filter(|(f, _)| f.crashed_node().is_some_and(pick))
            .map(|(_, &v)| Expression::from(v))
            .sum()
    };
    if let Some(max) = bounds.max_crashes() {
        constraints.push(crash_sum(&|_| true).leq(max as f64));
    }
    if let Some(checked) = &bounds.checked {
        constraints.push(crash_sum(&|n| checked.nodes.contains(n)).leq(checked.cap as f64));
    }
    let nodes: BTreeSet<&Node> = universe
        .iter()
        .filter_map(Candidate::crashed_node)
        .collect();
    for node in nodes {
        constraints.push(crash_sum(&|n| n == node).leq(1.0));
    }

    for set in tried {
        let Some(inside) = set
            .iter()
            .map(|f| index.get(f).copied())
            .collect::<Option<BTreeSet<usize>>>()
        else {
            // A set with a fault outside the universe can never be selected.
            continue;
        };
        if !pair_rows.iter().any(|pair| pair.admits(&inside)) {
            continue;
        }
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
