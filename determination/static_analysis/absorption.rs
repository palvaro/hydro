//! Absorption analysis: determining whether a downstream composition absorbs
//! the nondeterminism introduced by a nondet point.
//!
//! A nondet point's nondeterminism is "absorbed" if every path from it to an
//! output passes through an operator that is insensitive to the specific form
//! of nondeterminism. For example:
//!
//! - A commutative fold over a `NoOrder` stream absorbs ordering nondeterminism:
//!   regardless of the order elements arrive, the result is the same.
//! - A set-based accumulation (insert into HashSet) absorbs both ordering and
//!   batching nondeterminism.
//!
//! If ALL paths from a nondet point to outputs are absorbed, the nondet point
//! contributes depth 0 (it's not a genuine commitment). If ANY path is
//! non-absorbed, the nondet point is a genuine commitment.

/// Properties tracked along a path from a nondet point to an output.
#[derive(Debug, Clone, Copy)]
pub struct PathProperties {
    /// Whether we've passed through an absorbing operator on this path.
    pub absorbed: bool,
}

impl PathProperties {
    pub fn new() -> Self {
        PathProperties { absorbed: false }
    }

    pub fn with_absorber(mut self) -> Self {
        self.absorbed = true;
        self
    }
}

/// Classification of how a node affects nondeterminism propagation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsorptionEffect {
    /// This node absorbs ordering/batching nondeterminism.
    /// Example: commutative fold, lattice merge, set union.
    Absorbs,

    /// This node is transparent — neither absorbs nor introduces nondeterminism.
    /// Example: map, filter, flatmap (deterministic transformations).
    Transparent,

    /// This node may amplify or transform nondeterminism.
    /// Example: non-commutative fold (first element wins), reduce with order dependence.
    /// Conservative: anything we can't prove absorbs is treated as amplifying.
    MayAmplify,
}

/// Determine the absorption effect of a Fold node.
///
/// A fold absorbs nondeterminism if:
/// 1. Its input stream has `NoOrder` ordering — meaning the type system has
///    already verified that the fold is insensitive to element order, OR
/// 2. The accumulator function is annotated with `commutative = manual_proof!(...)`.
///
/// In the first pass, we use a conservative heuristic:
/// - NoOrder input → absorbs
/// - TotalOrder input → may amplify (conservative)
///
/// TODO: Inspect the accumulator expression for manual_proof! annotations
/// to detect commutativity even with TotalOrder inputs.
pub fn classify_fold(input_order: InputOrder, has_commutative_proof: bool) -> AbsorptionEffect {
    match (input_order, has_commutative_proof) {
        // NoOrder input means the fold must already handle any order → absorbs
        (InputOrder::NoOrder, _) => AbsorptionEffect::Absorbs,
        // Explicit commutativity proof → absorbs regardless of input order
        (_, true) => AbsorptionEffect::Absorbs,
        // TotalOrder without proof → might depend on order → conservative
        (InputOrder::TotalOrder, false) => AbsorptionEffect::MayAmplify,
    }
}

/// The ordering guarantee of a stream feeding into a fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOrder {
    NoOrder,
    TotalOrder,
}

/// Determine the absorption effect of a generic node.
///
/// Most operators are transparent: they transform data but don't introduce
/// or absorb nondeterminism sensitivity.
///
/// Non-monotone operators (Difference, AntiJoin) are conservative:
/// they may amplify nondeterminism (a nondeterministic input to a negation
/// can produce wildly different outputs).
pub fn classify_node(is_non_monotone: bool) -> AbsorptionEffect {
    if is_non_monotone {
        AbsorptionEffect::MayAmplify
    } else {
        AbsorptionEffect::Transparent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_with_no_order_absorbs() {
        assert_eq!(
            classify_fold(InputOrder::NoOrder, false),
            AbsorptionEffect::Absorbs
        );
    }

    #[test]
    fn fold_with_total_order_no_proof_amplifies() {
        assert_eq!(
            classify_fold(InputOrder::TotalOrder, false),
            AbsorptionEffect::MayAmplify
        );
    }

    #[test]
    fn fold_with_commutative_proof_absorbs() {
        assert_eq!(
            classify_fold(InputOrder::TotalOrder, true),
            AbsorptionEffect::Absorbs
        );
    }

    #[test]
    fn non_monotone_node_amplifies() {
        assert_eq!(classify_node(true), AbsorptionEffect::MayAmplify);
    }

    #[test]
    fn monotone_node_is_transparent() {
        assert_eq!(classify_node(false), AbsorptionEffect::Transparent);
    }
}
