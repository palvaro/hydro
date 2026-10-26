//! The `implies` rule's API ([`Implies`], docs/design/hydrofrac-implies.md §1).
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate,
//! while what it builds is host-side only.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::OperatorContext;
#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::boundedness::Boundedness;
#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{Ordering, Retries, Stream};
#[cfg(stageleft_runtime)]
use hydro_lang::location::Location;
#[cfg(stageleft_runtime)]
use hydro_lang::properties::{
    StreamMapFuncAlgebra, ValidMutCommutativityFor, ValidMutIdempotenceFor,
};
#[cfg(stageleft_runtime)]
use stageleft::IntoQuotedMut;

#[cfg(stageleft_runtime)]
use crate::spec::{ASide, BSide};

/// The start of the tag of a rule's key node: `PREFIX|side|file:line|name`, where `file:line`
/// is the user's `implies` call.
#[cfg(stageleft_runtime)]
pub(crate) const MARKER: &str = "hydrofrac::implies";

/// One side of a rule, read back from its key node's tag.
#[cfg(stageleft_runtime)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Marker {
    pub(crate) name: String,
    /// The `file:line` of the user's `implies` call (the key node itself is built in this
    /// file, so its backtrace names this file).
    pub(crate) site: String,
    /// The A-side's setting, or the B-side's.
    pub(crate) side: Result<ASide, BSide>,
}

#[cfg(stageleft_runtime)]
impl Marker {
    /// The file of the user's call, by its name, and the line: `file.rs:12`.
    pub(crate) fn short_site(&self) -> String {
        self.site
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(&self.site)
            .to_owned()
    }

    /// The file of the user's call, by its name.
    pub(crate) fn file(&self) -> String {
        let site = self.short_site();
        match site.rsplit_once(':') {
            Some((file, _)) => file.to_owned(),
            None => site,
        }
    }
}

#[cfg(stageleft_runtime)]
fn tag(name: &str, side: Result<ASide, BSide>, site: &str) -> String {
    let side = match side {
        Ok(ASide::Correct) => "a:correct".to_owned(),
        Ok(ASide::Any) => "a:any".to_owned(),
        Err(BSide::AllCorrect) => "b:all".to_owned(),
        Err(BSide::Some) => "b:some".to_owned(),
        Err(BSide::AtLeast(k)) => format!("b:{k}"),
    };
    format!("{MARKER}|{side}|{site}|{name}")
}

/// The rule side a node's tag names, if it is a rule's key node.
#[cfg(stageleft_runtime)]
pub(crate) fn marker(tag: Option<&str>) -> Option<Marker> {
    let rest = tag?.strip_prefix(MARKER)?.strip_prefix('|')?;
    let (side, rest) = rest.split_once('|')?;
    let (site, name) = rest.split_once('|')?;
    let side = match side {
        "a:correct" => Ok(ASide::Correct),
        "a:any" => Ok(ASide::Any),
        "b:all" => Err(BSide::AllCorrect),
        "b:some" => Err(BSide::Some),
        _ => Err(BSide::AtLeast(side.strip_prefix("b:")?.parse().ok()?)),
    };
    Some(Marker {
        name: name.to_owned(),
        site: site.to_owned(),
        side,
    })
}

/// States a rule for hydrofrac to check: **if a record `r` is in `self` (at a member `a_side`
/// chooses), then a record `r'` with `kb(r') == ka(r)` is in `b` (at the members `b_side`
/// chooses)**, at the end of every run (docs/design/hydrofrac-implies.md).
///
/// ```ignore
/// use hydrofrac::{ASide, BSide, Implies};
/// delivered.implies("uniform", &delivered, q!(|m| m), q!(|m| m), ASide::Any, BSide::AllCorrect);
/// // ... check.run(..) ...
/// results.label("uniform").assert_holds();
/// ```
///
/// The rule adds `self.clone().map(ka)` and `b.clone().map(kb)` beside the program, consumed by
/// nothing: the program is unchanged, and the check taps the two. Keys are compared by their
/// `bincode`, so they must be `Serialize`. `name` names the rule in the results (two rules with
/// one name are refused). Both streams must be at the top level of a process or a cluster, and
/// the key functions must not mutate their captures (`Fn`).
#[cfg(stageleft_runtime)]
pub trait Implies<'a, T, L: Location<'a>, B: Boundedness, O: Ordering, R: Retries> {
    /// See the [trait docs](Implies).
    #[track_caller]
    fn implies<U, LB, BB, OB, RB, K, FA, CA, IA, FB, CB, IB>(
        &self,
        name: &str,
        b: &Stream<U, LB, BB, OB, RB>,
        ka: impl IntoQuotedMut<'a, FA, OperatorContext<L, B>, StreamMapFuncAlgebra<CA, IA>>,
        kb: impl IntoQuotedMut<'a, FB, OperatorContext<LB, BB>, StreamMapFuncAlgebra<CB, IB>>,
        a_side: ASide,
        b_side: BSide,
    ) where
        U: Clone,
        LB: Location<'a>,
        BB: Boundedness,
        OB: Ordering,
        RB: Retries,
        FA: Fn(T) -> K + 'a,
        CA: ValidMutCommutativityFor<FA, T, K, O, false>,
        IA: ValidMutIdempotenceFor<FA, T, K, R, false>,
        FB: Fn(U) -> K + 'a,
        CB: ValidMutCommutativityFor<FB, U, K, OB, false>,
        IB: ValidMutIdempotenceFor<FB, U, K, RB, false>;
}

#[cfg(stageleft_runtime)]
impl<'a, T: Clone, L: Location<'a>, B: Boundedness, O: Ordering, R: Retries>
    Implies<'a, T, L, B, O, R> for Stream<T, L, B, O, R>
{
    #[track_caller]
    fn implies<U, LB, BB, OB, RB, K, FA, CA, IA, FB, CB, IB>(
        &self,
        name: &str,
        b: &Stream<U, LB, BB, OB, RB>,
        ka: impl IntoQuotedMut<'a, FA, OperatorContext<L, B>, StreamMapFuncAlgebra<CA, IA>>,
        kb: impl IntoQuotedMut<'a, FB, OperatorContext<LB, BB>, StreamMapFuncAlgebra<CB, IB>>,
        a_side: ASide,
        b_side: BSide,
    ) where
        U: Clone,
        LB: Location<'a>,
        BB: Boundedness,
        OB: Ordering,
        RB: Retries,
        FA: Fn(T) -> K + 'a,
        CA: ValidMutCommutativityFor<FA, T, K, O, false>,
        IA: ValidMutIdempotenceFor<FA, T, K, R, false>,
        FB: Fn(U) -> K + 'a,
        CB: ValidMutCommutativityFor<FB, U, K, OB, false>,
        IB: ValidMutIdempotenceFor<FB, U, K, RB, false>,
    {
        // The report and the diagrams name the rule by where the user stated it.
        let caller = std::panic::Location::caller();
        let site = format!("{}:{}", caller.file(), caller.line());
        // Dropped unconsumed, each key stream becomes a root of the program that nothing reads.
        drop(
            self.clone()
                .map(ka)
                .ir_node_named(&tag(name, Ok(a_side), &site)),
        );
        drop(
            b.clone()
                .map(kb)
                .ir_node_named(&tag(name, Err(b_side), &site)),
        );
    }
}
