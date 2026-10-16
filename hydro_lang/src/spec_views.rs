//! Specification views that are generated rather than written: the Hydro dataflow a
//! consistency label stands for, over facts observed at a specification's observer.
//!
//! These live outside `sim` because their closures are staged: the generated simulation
//! crate imports them from `hydro_lang::__staged`, which only contains modules compiled
//! under the runtime feature set. [`crate::sim::spec::Spec::eventual_consistency`] is the
//! entry point.

use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;
use stageleft::q;

use crate::live_collections::boundedness::Bounded;
use crate::live_collections::optional::Optional;
use crate::live_collections::stream::{Retries, Stream, TotalOrder};
use crate::location::{Location, MemberId};

/// `POST` of [`eventual_consistency`]: `((pairs, facts), members)` when `pairs == facts × members`.
pub type EcPost<'a, L> = Optional<((usize, usize), usize), L, Bounded>;
/// The facts correct members hold in this run (the domain of the next runs).
pub type EcFacts<'a, L, T> = Stream<T, L, Bounded, TotalOrder>;

/// The views an `EventualConsistency` label stands for, over `observed`, the `(member, fact)`
/// facts of one cluster collection seen so far; `correct`, the members the hypothesis under
/// test does not crash; and `domain`, the facts of the failure-free run (see
/// `Spec::eventual_consistency`). Returns `(POST, facts held by correct members)`:
///
/// - `POST`: every correct member holds every domain fact some correct member holds, as a
///   count: the distinct `(member, fact)` pairs at correct members over the domain are as
///   many as the distinct domain facts present times the correct members.
/// - The second view is every fact a correct member holds (domain or not), published so the
///   search can learn the domain from the failure-free run.
///
/// There is no `PRE`: non-vacuity is the search's probe run having a non-empty domain.
pub fn eventual_consistency<'a, L, C, T, R>(
    observed: Stream<(MemberId<C>, T), L, Bounded, TotalOrder, R>,
    correct: Stream<MemberId<C>, L, Bounded, TotalOrder>,
    domain: Stream<T, L, Bounded, TotalOrder>,
) -> (EcPost<'a, L>, EcFacts<'a, L, T>)
where
    L: Location<'a>,
    C: 'a,
    T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + 'a,
    R: Retries,
{
    // Facts at correct members, as (member, fact).
    let held = correct
        .clone()
        .map(q!(|member| (member, ())))
        .join(observed)
        .map(q!(|(member, ((), fact))| (member, fact)))
        .unique();
    let held_facts = held.clone().map(q!(|(_, fact)| fact)).unique().sort();
    // Restricted to the domain.
    let in_domain = held
        .map(q!(|(member, fact)| (fact, member)))
        .join(domain.map(q!(|fact| (fact, ()))))
        .map(q!(|(fact, (member, ()))| (member, fact)))
        .unique();
    let facts = in_domain.clone().map(q!(|(_, fact)| fact)).unique().count();
    let pairs = in_domain.count();
    let members = correct.count();
    let post = pairs
        .zip(facts)
        .zip(members)
        .filter(q!(|((pairs, facts), members)| *pairs == *facts * *members));
    (post, held_facts)
}
