//! **Speculative** demonstration libraries for *EC-inference*: how
//! `EventualConsistency` (EC) falls out of the type system when you compose safe
//! primitives, rather than being asserted by a monolithic trusted combinator.
//!
//! Companion to the design docs
//! `design_docs/2026-08_epistemic_foundations_ec_inference.md` and
//! `design_docs/2026-08_ordering_consistency_taxonomy.md`. Everything here is a
//! research artifact, not a production API — the point is to *show the
//! inference working*, and to pin the open design items (typed fault-dependency
//! `F`, dynamic membership) as compilable code.
//!
//! # What's a demo vs. what's a (tentative) primitive
//!
//! **Demos** — protocols composed from primitives, each showing EC inferred
//! around some structure:
//!
//! - [`leader_merge`] — multi-writer total-order EC via a single merging leader
//!   (primary/backup), *zero consensus*. Pins taxonomy doc §4 and the untracked
//!   `F = {leader}` motivation. Also pins the **slot route** (§3c/§8): the same
//!   pattern with a distinguished *cluster member* as leader, where the naive
//!   port is type-refused (member-locality is invisible to the types) and
//!   shipping the order as `(slot, value)` data earns EC with zero consistency
//!   assertions.
//! - [`reliable_broadcast`] — echo-based reliable broadcast; EC inferred around
//!   the re-broadcast cycle via the `forward_ref`-on-an-EC-location trick.
//! - [`crdt_gossip`] — state-based G-Set gossip; EC inferred around the
//!   folded-state re-broadcast cycle.
//!
//! **Tentative primitives** — the EC-minting substrate the *dynamic-membership*
//! demos build on. These are speculative and their soundness is bounded (see
//! their own docs): they exist to serve the dynamic case, and do not yet fully
//! "work" in the sense a production primitive would.
//!
//! - [`fan_out`] — the generic EC-minting rule: fan a source out over any
//!   `EventuallyComplete` membership view via an EC-preserving policy, and EC is
//!   minted once, coinductively, here. Subsumes both `broadcast_closed` (static
//!   view) and [`broadcast_live`] (live view).
//! - [`broadcast_live`] — the dynamic-membership generalization of
//!   `broadcast_closed`, fanning out over the *live, monotone* membership
//!   relation. Soundness is bounded to append-only data (pruning the data side
//!   breaks late-joiner catch-up), and the dynamic case is what drives the
//!   simulator's join-timing state space.
//!
//! The static demos (`leader_merge`, the `*_closed` entry points,
//! `g_set_gossip`) rest only on `broadcast_closed` from `hydro_lang`; the
//! dynamic demos (`reliable_broadcast_live`, `g_set_gossip_dynamic`) route
//! through the tentative primitives above.

pub mod broadcast_live;
pub mod crdt_gossip;
pub mod fan_out;
pub mod leader_merge;
pub mod reliable_broadcast;
