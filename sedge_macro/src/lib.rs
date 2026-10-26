//! The attribute macro that generates a Sedge overload-check harness from a Hydro function's
//! signature. Re-exported by the `sedge` crate, which is where users name it:
//! `#[sedge::overload_check(...)]`.
//!
//! # `#[overload_check(...)]`
//!
//! Placed on a Hydro function, this attribute leaves the function unchanged and registers a check
//! next to it. The check deploys the function on localhost, offers it the load profile the
//! `sedge` command asks for, and records a verdict per trial. The crate under check lists `sedge`
//! as an optional dependency, declares a `sedge` feature that enables it, and writes the attribute
//! as `#[cfg_attr(feature = "sedge", sedge::overload_check(...))]`, so that the harness is linked
//! only when the command turns that feature on.
//!
//! ## What the macro reads from the signature
//!
//! * `&Process<'a, X>` is a location. The harness deploys it as one process on localhost.
//! * `&Cluster<'a, X>` is a location. The harness deploys it as a cluster of localhost processes
//!   and needs its size from the attribute, as `x = <members>` where `x` is the parameter's name
//!   (the same form Cress's `amplification_check` takes). Streams into and out of a cluster pass
//!   through one extra relay process the harness deploys (`sedge::SedgeRelay`), because a
//!   deployment has no channel between an external process and a cluster.
//! * Every `Stream<(), Loc, Unbounded, O, R>` parameter named in `timers(...)` is a timer, bound
//!   to an in-process `source_interval` of the given period on the parameter's location
//!   (`sedge::timer`, `sedge::cluster_timer`). The timer is part of the deployed program's real
//!   execution, so CPU starvation under its quota delays it as it would in production. On a
//!   cluster, member `i`'s first tick is delayed by `i · period / members` (the deterministic
//!   form of independent per-member clocks). Only when the command names the timer in a timing
//!   fault (`--dip timer=K`) is it instead driven by the harness over an external port, so that
//!   its period can be stretched during the trigger; such runs are reported as `TIMING_FAULT`.
//! * Every other stream parameter is a data stream, fed from an external port. With
//!   `load = param` there must be exactly one, and the generator sends it one value of `T` per
//!   round (to every member, for a cluster), made from the round number by `sedge::LoadItem`.
//!   With `round_data = |round, inputs| { ... }` there may be several, and the closure decides
//!   what each round carries: `inputs` has one `sedge::RoundSender<T>` field per data stream on
//!   a process, named after the parameter, with a `send(T)` method, one
//!   `sedge::ClusterRoundSender<T>` field per data stream on a cluster, with a
//!   `send(member, T)` method, and one `<cluster>_members: u32` field per cluster parameter.
//!   This is the same shape as Cress's `round_data`, so one closure describes both tools'
//!   workloads; the harness numbers the records of each stream from zero.
//! * Anything else is a plain value that must be given in the attribute by the parameter's name,
//!   for example `policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3 }`.
//!
//! Every generic type parameter of the function must be given a concrete type, as `T = u64`.
//! Keyed streams are not supported yet and are reported as errors.
//!
//! The function's return value is handed to `sedge::Observed::observe`, which the program's crate
//! implements to name its completion, abandonment, gauge and counter outputs, or derives with
//! `#[derive(sedge::SedgeOutputs)]` to register every output stream whole (see
//! [`SedgeOutputs`](macro@SedgeOutputs)).
//!
//! ## What the attribute accepts
//!
//! * `name = ident` or `name = "string"`: the configuration's name, shown with its verdict.
//! * `T = Type`: the concrete type for generic parameter `T`.
//! * `param = expr`: the value of a plain parameter, or the size of a cluster parameter.
//! * `load = param`: which stream parameter the generator feeds one value per round. Exactly one
//!   of `load` and `round_data` is required.
//! * `round_data = |round, inputs| { ... }`: what one round of workload sends on the data
//!   streams (see above).
//! * `timers(param = Duration-expr, ...)`: the period of each timer parameter. Required for
//!   every `()` stream parameter that is not data.
//! * `mu = expr`: where the capacity probe starts its search, in rounds of workload per second.
//!   μ, which the command's overprovisioning factors divide, is measured under the nodes' limits
//!   (and cached per limits and host); `mu` is used as μ itself only with `--no-calibrate` or
//!   without a capacity backend. The command's `--mu` overrides both.
//! * `cpus = expr`: the CPU quota, in cores, of every deployed node of the program, unless the
//!   command's `--cpus` says otherwise. `cpus(Role = expr, ...)` gives a per-role quota, by
//!   location marker type name (`cpus(Client = none, Server = 0.01)`); `none` means no quota. A
//!   node no one caps runs with no quota: only the resource under test should get one, and
//!   clients, which stand for the callers offering the load, should not. The quota is set when
//!   a node's container starts and stays fixed for the run; only the command's opt-in
//!   `--cpu-dip` changes it mid-run. The harness's relay process is never capped.
//! * `cpu_period_us = expr`: the CFS period the quotas are enforced over (default 10 ms). The
//!   kernel's smallest quota is 1 ms, so a cap below `1 ms / period` needs a longer period.
//! * `ignore = "reason"`: omits this check from the command unless its `--include-ignored` option
//!   is given.
//! * `sedge = path`: the path to the `sedge` crate, `::sedge` by default.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2, TokenTree};
use quote::{ToTokens, format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit_mut::VisitMut;
use syn::{
    Expr, FnArg, GenericArgument, GenericParam, Ident, ItemFn, LitStr, Pat, PathArguments,
    ReturnType, Token, Type, TypePath, parenthesized,
};

// ---------------------------------------------------------------------------------------------
// Attribute arguments

/// One `name = tokens`, `name(...)` or bare `name` entry of the attribute.
struct Entry {
    key: Ident,
    /// Present for `key = ...`.
    value: Option<TokenStream2>,
    /// Present for `key(...)`.
    group: Option<Vec<Entry>>,
}

/// Reads token trees up to, and not including, the next top-level comma.
fn tokens_until_comma(input: ParseStream<'_>) -> syn::Result<TokenStream2> {
    input.step(|cursor| {
        let mut rest = *cursor;
        let mut out = Vec::new();
        while let Some((tt, next)) = rest.token_tree() {
            if let TokenTree::Punct(p) = &tt
                && p.as_char() == ','
            {
                break;
            }
            out.push(tt);
            rest = next;
        }
        Ok((TokenStream2::from_iter(out), rest))
    })
}

impl Parse for Entry {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            // A closure's parameter list has top-level commas, so `round_data` is parsed as an
            // expression; every other value is the raw tokens up to the next comma, because a
            // type such as `Vec<(u32, u32)>` is not an expression.
            let value = if key == "round_data" {
                input.parse::<Expr>()?.to_token_stream()
            } else {
                tokens_until_comma(input)?
            };
            if value.is_empty() {
                return Err(syn::Error::new(key.span(), "expected a value after `=`"));
            }
            Ok(Entry {
                key,
                value: Some(value),
                group: None,
            })
        } else if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            let inner: Punctuated<Entry, Token![,]> =
                content.parse_terminated(Entry::parse, Token![,])?;
            Ok(Entry {
                key,
                value: None,
                group: Some(inner.into_iter().collect()),
            })
        } else {
            Ok(Entry {
                key,
                value: None,
                group: None,
            })
        }
    }
}

struct Args {
    entries: Vec<Entry>,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let entries: Punctuated<Entry, Token![,]> =
            input.parse_terminated(Entry::parse, Token![,])?;
        Ok(Args {
            entries: entries.into_iter().collect(),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Signature analysis

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocKind {
    Process,
    Cluster,
}

/// A process or cluster the harness must create, identified by its marker type.
struct Loc {
    kind: LocKind,
    marker: Type,
    /// The variable holding it in the generated code: the parameter's name when the location is
    /// a parameter, otherwise a synthesized one.
    var: Ident,
    is_param: bool,
}

struct StreamParam {
    name: Ident,
    item: Type,
    order: Type,
    retries: Type,
    /// Index into the harness's location list.
    loc: usize,
}

enum Param {
    Loc(usize),
    Stream(usize),
    Plain(Ident),
}

fn last_ident(ty: &Type) -> Option<(&Ident, &PathArguments)> {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        let seg = path.segments.last()?;
        Some((&seg.ident, &seg.arguments))
    } else {
        None
    }
}

fn type_args(args: &PathArguments) -> Vec<&Type> {
    match args {
        PathArguments::AngleBracketed(a) => a
            .args
            .iter()
            .filter_map(|g| match g {
                GenericArgument::Type(t) => Some(t),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn unit_type() -> Type {
    syn::parse_quote!(())
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

/// Classifies a `Process<'a, X>` or `Cluster<'a, X>` type, if it is one.
fn as_location(ty: &Type) -> syn::Result<Option<(LocKind, Type)>> {
    let Some((ident, args)) = last_ident(ty) else {
        return Ok(None);
    };
    let kind = match ident.to_string().as_str() {
        "Process" => LocKind::Process,
        "Cluster" => LocKind::Cluster,
        _ => return Ok(None),
    };
    let targs = type_args(args);
    if kind == LocKind::Cluster && targs.len() > 1 {
        return Err(syn::Error::new(
            ty.span(),
            "cluster locations with an explicit consistency type parameter are not supported by #[overload_check]",
        ));
    }
    let marker = targs.first().copied().cloned().unwrap_or_else(unit_type);
    Ok(Some((kind, marker)))
}

/// Replaces single-segment type paths naming a generic parameter with the concrete type.
struct Substitute<'s> {
    map: &'s [(String, Type)],
}

impl VisitMut for Substitute<'_> {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if let Type::Path(TypePath { qself: None, path }) = ty
            && path.segments.len() == 1
            && path.segments[0].arguments.is_none()
        {
            let name = path.segments[0].ident.to_string();
            if let Some((_, concrete)) = self.map.iter().find(|(n, _)| *n == name) {
                *ty = concrete.clone();
                return;
            }
        }
        syn::visit_mut::visit_type_mut(self, ty);
    }
}

fn type_key(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

// ---------------------------------------------------------------------------------------------
// The attribute

/// See the [crate documentation](crate).
#[proc_macro_attribute]
pub fn overload_check(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = match syn::parse::<ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => {
            let item = TokenStream2::from(item);
            let err = e.to_compile_error();
            return quote!(#item #err).into();
        }
    };
    let args = match syn::parse::<Args>(attr) {
        Ok(a) => a,
        Err(e) => {
            let err = e.to_compile_error();
            return quote!(#item_fn #err).into();
        }
    };
    match expand(&item_fn, args) {
        Ok(entry) => quote!(#item_fn #entry).into(),
        Err(e) => {
            let err = e.to_compile_error();
            quote!(#item_fn #err).into()
        }
    }
}

fn expand(f: &ItemFn, args: Args) -> syn::Result<TokenStream2> {
    let fn_name = &f.sig.ident;

    let generic_names: Vec<String> = f
        .sig
        .generics
        .params
        .iter()
        .filter_map(|p| match p {
            GenericParam::Type(t) => Some(t.ident.to_string()),
            _ => None,
        })
        .collect();

    // Sort the attribute's entries.
    let mut name: Option<String> = None;
    let mut ignore: Option<LitStr> = None;
    let mut sedge: syn::Path = syn::parse_quote!(::sedge);
    let mut load: Option<Ident> = None;
    let mut round_data: Option<Expr> = None;
    let mut mu: Option<Expr> = None;
    let mut cpus: Option<Expr> = None;
    let mut cpu_period: Option<Expr> = None;
    let mut role_cpus: Vec<(Ident, Expr)> = Vec::new();
    let mut timers: Vec<(Ident, Expr)> = Vec::new();
    let mut generic_types: Vec<(String, Type)> = Vec::new();
    let mut values: Vec<(String, Expr, Span)> = Vec::new();

    for e in args.entries {
        let key = e.key.to_string();
        match (key.as_str(), &e.value, &e.group) {
            ("name", Some(v), _) => {
                name = Some(match syn::parse2::<LitStr>(v.clone()) {
                    Ok(s) => s.value(),
                    Err(_) => syn::parse2::<Ident>(v.clone())
                        .map_err(|_| {
                            syn::Error::new(e.key.span(), "`name` takes an identifier or a string")
                        })?
                        .to_string(),
                });
            }
            ("ignore", Some(v), _) => ignore = Some(syn::parse2(v.clone())?),
            ("sedge", Some(v), _) => sedge = syn::parse2(v.clone())?,
            ("load", Some(v), _) => load = Some(syn::parse2(v.clone())?),
            ("round_data", Some(v), _) => round_data = Some(syn::parse2(v.clone())?),
            ("mu", Some(v), _) => mu = Some(syn::parse2(v.clone())?),
            ("cpus", Some(v), _) => {
                cpus = Some(if v.to_string() == "none" {
                    syn::parse_quote!(0.0)
                } else {
                    syn::parse2(v.clone())?
                })
            }
            ("cpu_period_us", Some(v), _) => cpu_period = Some(syn::parse2(v.clone())?),
            ("cpus", None, Some(g)) => {
                for t in g {
                    let Some(v) = &t.value else {
                        return Err(syn::Error::new(
                            t.key.span(),
                            "per-role caps look like `cpus(Server = 0.2)`",
                        ));
                    };
                    // `none`: no quota for this role.
                    let e: Expr = if v.to_string() == "none" {
                        syn::parse_quote!(0.0)
                    } else {
                        syn::parse2(v.clone())?
                    };
                    role_cpus.push((t.key.clone(), e));
                }
            }
            ("timers", None, Some(g)) => {
                for t in g {
                    let Some(v) = &t.value else {
                        return Err(syn::Error::new(
                            t.key.span(),
                            "timer entries look like `param = Duration::from_millis(50)`",
                        ));
                    };
                    timers.push((t.key.clone(), syn::parse2(v.clone())?));
                }
            }
            (k, Some(v), _) if generic_names.iter().any(|g| g == k) => {
                generic_types.push((k.to_owned(), syn::parse2::<Type>(v.clone())?));
            }
            (_, Some(v), _) => {
                values.push((key.clone(), syn::parse2::<Expr>(v.clone())?, e.key.span()));
            }
            _ => {
                return Err(syn::Error::new(
                    e.key.span(),
                    format!(
                        "unrecognized entry `{key}`; see the documentation of #[overload_check]"
                    ),
                ));
            }
        }
    }
    for g in &generic_names {
        if !generic_types.iter().any(|(n, _)| n == g) {
            return Err(syn::Error::new(
                f.sig.generics.span(),
                format!(
                    "generic type parameter `{g}` needs a concrete type, for example `{g} = u64`"
                ),
            ));
        }
    }
    if load.is_some() == round_data.is_some() {
        return Err(syn::Error::new(
            Span::call_site(),
            "exactly one of `load = <stream parameter>` (one value per round) and `round_data = |round, inputs| { ... }` (what each round sends) is required",
        ));
    }
    let hydro: syn::Path = syn::parse_quote!(#sedge::__private::hydro_lang);
    let mut subst = Substitute {
        map: &generic_types,
    };

    // Classify the parameters.
    let mut locs: Vec<Loc> = Vec::new();
    let mut streams: Vec<StreamParam> = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    let find_or_add_loc = |kind: LocKind, marker: Type, param: Option<&Ident>, locs: &mut Vec<Loc>| -> usize {
        let key = type_key(&marker);
        if let Some(i) = locs.iter().position(|l| l.kind == kind && type_key(&l.marker) == key) {
            if let Some(p) = param
                && !locs[i].is_param
            {
                locs[i].var = p.clone();
                locs[i].is_param = true;
            }
            return i;
        }
        let var = param
            .cloned()
            .unwrap_or_else(|| format_ident!("__loc_{}", locs.len()));
        locs.push(Loc {
            kind,
            marker,
            var,
            is_param: param.is_some(),
        });
        locs.len() - 1
    };

    for arg in &f.sig.inputs {
        let FnArg::Typed(pt) = arg else {
            return Err(syn::Error::new(
                arg.span(),
                "#[overload_check] does not support `self` parameters",
            ));
        };
        let Pat::Ident(pi) = &*pt.pat else {
            return Err(syn::Error::new(
                pt.pat.span(),
                "#[overload_check] needs every parameter to have a plain name",
            ));
        };
        let pname = pi.ident.clone();
        let mut ty = (*pt.ty).clone();
        subst.visit_type_mut(&mut ty);

        if let Type::Reference(r) = &ty
            && let Some((kind, marker)) = as_location(&r.elem)?
        {
            let i = find_or_add_loc(kind, marker, Some(&pname), &mut locs);
            params.push(Param::Loc(i));
            continue;
        }
        if let Some((ident, pargs)) = last_ident(&ty)
            && (ident == "Stream" || ident == "KeyedStream")
        {
            if ident == "KeyedStream" {
                return Err(syn::Error::new(
                    ty.span(),
                    "#[overload_check] does not support KeyedStream parameters yet",
                ));
            }
            let targs = type_args(pargs);
            if targs.len() < 2 {
                return Err(syn::Error::new(
                    ty.span(),
                    "a Stream parameter needs at least its item and location types",
                ));
            }
            let item = targs[0].clone();
            let Some((kind, marker)) = as_location(targs[1])? else {
                return Err(syn::Error::new(
                    targs[1].span(),
                    "a stream parameter's location must be a Process or a Cluster",
                ));
            };
            if let Some(bound) = targs.get(2)
                && last_ident(bound).map(|(i, _)| i.to_string()) != Some("Unbounded".to_owned())
            {
                return Err(syn::Error::new(
                    bound.span(),
                    "only Unbounded stream parameters can be fed by the harness",
                ));
            }
            let order =
                targs.get(3).copied().cloned().unwrap_or_else(
                    || syn::parse_quote!(#hydro::live_collections::stream::TotalOrder),
                );
            let retries = targs.get(4).copied().cloned().unwrap_or_else(
                || syn::parse_quote!(#hydro::live_collections::stream::ExactlyOnce),
            );
            let is_load = load.as_ref().is_some_and(|l| *l == pname);
            let is_timer = timers.iter().any(|(t, _)| *t == pname);
            if !is_timer && !is_load && is_unit(&item) {
                return Err(syn::Error::new(
                    pname.span(),
                    format!(
                        "timer `{pname}` needs a period in the attribute, as `timers({pname} = Duration::from_millis(..))`"
                    ),
                ));
            }
            if load.is_some() && !is_load && !is_timer {
                return Err(syn::Error::new(
                    pname.span(),
                    format!(
                        "stream `{pname}` is neither the load nor a timer; with `load = ...` #[overload_check] feeds one data stream (use `round_data = ...` for several)"
                    ),
                ));
            }
            if is_load && is_timer {
                return Err(syn::Error::new(
                    pname.span(),
                    format!("`{pname}` is both the load and a timer"),
                ));
            }
            if is_timer && !is_unit(&item) {
                return Err(syn::Error::new(
                    pname.span(),
                    format!("timer `{pname}` must be a stream of `()`"),
                ));
            }
            let loc = find_or_add_loc(kind, marker, None, &mut locs);
            streams.push(StreamParam {
                name: pname,
                item,
                order,
                retries,
                loc,
            });
            params.push(Param::Stream(streams.len() - 1));
            continue;
        }
        params.push(Param::Plain(pname));
    }
    if let Some(load) = &load
        && !streams.iter().any(|s| s.name == *load)
    {
        return Err(syn::Error::new(
            load.span(),
            format!("`{load}` is not a stream parameter of `{fn_name}`"),
        ));
    }
    let is_data = |s: &StreamParam| !timers.iter().any(|(t, _)| *t == s.name);
    if round_data.is_some() && !streams.iter().any(is_data) {
        return Err(syn::Error::new(
            Span::call_site(),
            "`round_data` needs at least one data stream (a stream parameter not named in `timers(...)`)",
        ));
    }
    for (t, _) in &timers {
        if !streams.iter().any(|s| s.name == *t) {
            return Err(syn::Error::new(
                t.span(),
                format!("`{t}` is not a stream parameter of `{fn_name}`"),
            ));
        }
    }

    // Every plain parameter needs a value from the attribute.
    let mut plain_values: Vec<(Ident, Expr)> = Vec::new();
    for p in &params {
        if let Param::Plain(n) = p {
            let key = n.to_string();
            match values.iter().position(|(k, _, _)| *k == key) {
                Some(i) => plain_values.push((n.clone(), values.remove(i).1)),
                None => {
                    return Err(syn::Error::new(
                        n.span(),
                        format!("parameter `{n}` must be given in the attribute, as `{n} = ...`"),
                    ));
                }
            }
        }
    }
    // Every cluster needs its size from the attribute, under the parameter's name.
    let mut cluster_sizes: Vec<(usize, Expr)> = Vec::new();
    for (i, l) in locs.iter().enumerate() {
        if l.kind == LocKind::Cluster {
            if !l.is_param {
                return Err(syn::Error::new(
                    f.sig.span(),
                    format!(
                        "a stream lives on a cluster ({}) that is not a parameter, so the harness cannot learn its size",
                        type_key(&l.marker)
                    ),
                ));
            }
            let key = l.var.to_string();
            match values.iter().position(|(k, _, _)| *k == key) {
                Some(j) => cluster_sizes.push((i, values.remove(j).1)),
                None => {
                    return Err(syn::Error::new(
                        l.var.span(),
                        format!(
                            "cluster `{key}` needs its size in the attribute, as `{key} = <members>`"
                        ),
                    ));
                }
            }
        }
    }
    if let Some((k, _, span)) = values.first() {
        return Err(syn::Error::new(
            *span,
            format!("`{k}` is not a parameter of `{fn_name}`"),
        ));
    }
    let has_cluster = !cluster_sizes.is_empty();

    // ---- Code generation -----------------------------------------------------------------

    let config_name = name.clone().unwrap_or_else(|| "default".to_owned());
    let entry_name = match &name {
        Some(n) => format_ident!("__overload_check_{}_{}", fn_name, n),
        None => format_ident!("__overload_check_{}", fn_name),
    };
    let register_name = format_ident!("{}_register", entry_name);

    let make_locs = locs.iter().map(|l| {
        let var = &l.var;
        let marker = &l.marker;
        match l.kind {
            LocKind::Process => quote!(let #var = __flow.process::<#marker>();),
            LocKind::Cluster => quote!(let #var = __flow.cluster::<#marker>();),
        }
    });
    let size_var = |loc: usize| format_ident!("__members_{}", locs[loc].var);
    let make_sizes = cluster_sizes.iter().map(|(i, e)| {
        let v = size_var(*i);
        quote!(let #v: u32 = (#e) as u32;)
    });
    let is_cluster = |s: &StreamParam| locs[s.loc].kind == LocKind::Cluster;

    let port_var = |s: &StreamParam| format_ident!("__port_{}", s.name);
    let sink_var = |s: &StreamParam| format_ident!("__sink_{}", s.name);
    let make_inputs = streams.iter().map(|s| {
        let sv = &s.name;
        let (item, order, retries) = (&s.item, &s.order, &s.retries);
        let locv = &locs[s.loc].var;
        let pv = port_var(s);
        if !is_data(s) {
            // A timer runs in-process, on the deployed node's own clock, unless the command
            // names it in a timing fault (`--dip`), in which case the harness drives it over a
            // port like a data stream's so that it can stretch its period.
            let name = s.name.to_string();
            let period = &timers.iter().find(|(t, _)| *t == s.name).unwrap().1;
            return match locs[s.loc].kind {
                LocKind::Process => quote!(
                    let (#pv, #sv) = if __spec.drives_timer(#name) {
                        let (__p, __s) = #sedge::load_input::<_, ()>(&#locv, &__external);
                        (::std::option::Option::Some(__p), __s)
                    } else {
                        (::std::option::Option::None, #sedge::timer(&#locv, ::std::time::Duration::from(#period)))
                    };
                    let #sv = #sv.weaken_ordering::<#order>().weaken_retries::<#retries>();
                ),
                LocKind::Cluster => {
                    let n = size_var(s.loc);
                    quote!(
                        let (#pv, #sv) = if __spec.drives_timer(#name) {
                            let (__p, __s) = #sedge::cluster_load_input::<_, ()>(&#locv, &__relay, &__external);
                            (::std::option::Option::Some(__p), __s)
                        } else {
                            (::std::option::Option::None, #sedge::cluster_timer(&#locv, ::std::time::Duration::from(#period), #n))
                        };
                        let #sv = #sv.weaken_ordering::<#order>().weaken_retries::<#retries>();
                    )
                }
            };
        }
        match locs[s.loc].kind {
            LocKind::Process => quote!(
                let (#pv, #sv) = #sedge::load_input::<_, #item>(&#locv, &__external);
                let #sv = #sv.weaken_ordering::<#order>().weaken_retries::<#retries>();
            ),
            LocKind::Cluster => quote!(
                let (#pv, #sv) = #sedge::cluster_load_input::<_, #item>(&#locv, &__relay, &__external);
                let #sv = #sv.weaken_ordering::<#order>().weaken_retries::<#retries>();
            ),
        }
    });

    let call_args = params.iter().map(|p| match p {
        Param::Loc(i) => {
            let v = &locs[*i].var;
            quote!(&#v)
        }
        Param::Stream(i) => {
            let v = &streams[*i].name;
            quote!(#v)
        }
        Param::Plain(n) => {
            let e = &plain_values.iter().find(|(k, _)| k == n).unwrap().1;
            quote!((#e))
        }
    });
    let observe = match &f.sig.output {
        ReturnType::Default => quote!(let _ = #fn_name(#(#call_args),*);),
        ReturnType::Type(..) => quote!(
            let __outputs = #fn_name(#(#call_args),*);
            #sedge::Observed::observe(__outputs, &mut __observer);
        ),
    };

    // Connecting the data and timer ports once deployed, and the round sender.
    let connect_sinks = streams.iter().map(|s| {
        let pv = port_var(s);
        let sk = sink_var(s);
        if is_data(s) {
            // Data streams are written through a feed: the generator queues each record at its
            // scheduled arrival and never waits on the socket (open loop).
            quote!(
                let (#sk, __writer) = #sedge::harness::feed(__nodes.connect(#pv).await, __feed_stats.clone());
                __writers.push(__writer);
            )
        } else {
            quote!(let #sk = match #pv {
                ::std::option::Option::Some(__p) => ::std::option::Option::Some(__nodes.connect(__p).await),
                ::std::option::Option::None => ::std::option::Option::None,
            };)
        }
    });
    let make_timers = streams.iter().filter(|s| !is_data(s)).map(|s| {
        let sk = sink_var(s);
        let name = s.name.to_string();
        let period = &timers.iter().find(|(t, _)| *t == s.name).unwrap().1;
        let (sink, members) = if is_cluster(s) {
            let n = size_var(s.loc);
            (quote!(__sink), quote!(#n))
        } else {
            (quote!(#sedge::harness::process_timer_sink(__sink)), quote!(1u32))
        };
        quote!(if let ::std::option::Option::Some(__sink) = #sk {
            __timers.push(#sedge::harness::TimerDrive {
                name: #name.to_owned(),
                period: ::std::time::Duration::from(#period),
                sink: #sink,
                members: #members,
            });
        })
    });
    let send_round = match (&load, &round_data) {
        (Some(_), _) => {
            let s = streams.iter().find(|s| is_data(s)).unwrap();
            let sk = sink_var(s);
            let item = &s.item;
            if is_cluster(s) {
                // One value per round to every member, as Cress feeds a cluster stream.
                let n = size_var(s.loc);
                quote!(
                    let __send_round = async |__round: u64, __due: ::std::time::Instant| -> ::std::result::Result<u64, ::std::string::String> {
                        for __m in 0..#n {
                            #sk.send(__due, (__m, <#item as #sedge::LoadItem>::from_id(__round)))?;
                        }
                        ::std::result::Result::Ok(#n as u64)
                    };
                )
            } else {
                quote!(
                    let __send_round = async |__round: u64, __due: ::std::time::Instant| -> ::std::result::Result<u64, ::std::string::String> {
                        #sk.send(__due, <#item as #sedge::LoadItem>::from_id(__round))?;
                        ::std::result::Result::Ok(1)
                    };
                )
            }
        }
        (None, Some(closure)) => {
            let fields = streams.iter().filter(|s| is_data(s)).map(|s| {
                let n = &s.name;
                let item = &s.item;
                if is_cluster(s) {
                    quote!(pub #n: #sedge::ClusterRoundSender<#item>,)
                } else {
                    quote!(pub #n: #sedge::RoundSender<#item>,)
                }
            });
            let member_fields = cluster_sizes.iter().map(|(i, _)| {
                let n = format_ident!("{}_members", locs[*i].var);
                quote!(pub #n: u32,)
            });
            let inits = streams.iter().filter(|s| is_data(s)).map(|s| {
                let n = &s.name;
                if is_cluster(s) {
                    quote!(#n: #sedge::ClusterRoundSender::new(),)
                } else {
                    quote!(#n: #sedge::RoundSender::new(),)
                }
            });
            let member_inits = cluster_sizes.iter().map(|(i, _)| {
                let n = format_ident!("{}_members", locs[*i].var);
                let v = size_var(*i);
                quote!(#n: #v,)
            });
            let flushes = streams.iter().filter(|s| is_data(s)).map(|s| {
                let n = &s.name;
                let sk = sink_var(s);
                quote!(
                    for __item in __inputs.#n.take() {
                        #sk.send(__due, __item)?;
                        __n += 1;
                    }
                )
            });
            quote!(
                #[allow(non_camel_case_types, dead_code)]
                struct __Inputs { #(#fields)* #(#member_fields)* }
                fn __coerce<F: ::std::ops::FnMut(u64, &__Inputs)>(f: F) -> F { f }
                let __inputs = __Inputs { #(#inits)* #(#member_inits)* };
                let mut __user_round = __coerce(#closure);
                let __send_round = async |__round: u64, __due: ::std::time::Instant| -> ::std::result::Result<u64, ::std::string::String> {
                    __user_round(__round, &__inputs);
                    let mut __n = 0u64;
                    #(#flushes)*
                    ::std::result::Result::Ok(__n)
                };
            )
        }
        (None, None) => unreachable!(),
    };

    let role = |l: &Loc| -> String {
        last_ident(&l.marker)
            .map(|(i, _)| i.to_string())
            .unwrap_or_else(|| match l.kind {
                LocKind::Process => "process".to_owned(),
                LocKind::Cluster => "cluster".to_owned(),
            })
    };
    let roles: Vec<String> = locs.iter().map(role).collect();
    let with_processes = locs.iter().enumerate().map(|(i, l)| {
        let v = &l.var;
        let r = role(l);
        match l.kind {
            LocKind::Process => {
                quote!(.with_process(&#v, __cap.host(&mut __deployment, __run, &__meta, #r, ::std::option::Option::None)))
            }
            LocKind::Cluster => {
                let n = size_var(i);
                quote!(.with_cluster(&#v, (0..#n).map(|__m| __cap.host(&mut __deployment, __run, &__meta, #r, ::std::option::Option::Some(__m))).collect::<::std::vec::Vec<_>>()))
            }
        }
    });
    // The relay is deployed only when some stream lives on a cluster; otherwise it is an unused
    // location that the flow never sends to.
    let with_relay = if has_cluster {
        quote!(.with_process(&__relay, #sedge::harness::host(&mut __deployment, __run)))
    } else {
        quote!()
    };

    let mu_value = mu
        .as_ref()
        .map(|e| quote!(::std::option::Option::Some((#e) as f64)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let role_cpu_names = role_cpus.iter().map(|(n, _)| n);
    let role_cpu_values = role_cpus.iter().map(|(_, e)| e);
    let cpu_period_value = cpu_period
        .as_ref()
        .map(|e| quote!(::std::option::Option::Some((#e) as u64)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let cpus_value = cpus
        .as_ref()
        .map(|e| quote!(::std::option::Option::Some((#e) as f64)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let ignore_value = ignore
        .as_ref()
        .map(|reason| quote!(::std::option::Option::Some(#reason)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));

    let fn_name_str = fn_name.to_string();
    let doc = format!(
        "Generated by `#[overload_check]`: deploys `{fn_name_str}` (configuration `{config_name}`) and runs the overload trials the `sedge` command asks for."
    );

    Ok(quote!(
        #[doc = #doc]
        #[allow(non_snake_case, unused_variables, unused_mut, clippy::all)]
        fn #entry_name(
            __run: &#sedge::harness::RunConfig,
        ) -> ::std::vec::Vec<#sedge::harness::TrialResult> {
            use #hydro::location::Location as _;
            let __meta = #sedge::harness::Meta {
                crate_name: ::std::env!("CARGO_PKG_NAME"),
                module: ::std::module_path!(),
                function: stringify!(#fn_name),
                configuration: #config_name,
                mu: #mu_value,
                cpus: #cpus_value,
                cpu_period_us: #cpu_period_value,
                role_cpus: ::std::vec![#((stringify!(#role_cpu_names), (#role_cpu_values) as f64)),*],
                roles: &[#(#roles),*],
            };
            #sedge::harness::run_check(&__meta, __run, async |__spec: &#sedge::harness::Spec| -> #sedge::harness::Executed {
                let __cap = #sedge::harness::TrialCapacity::new(__run);
                let mut __deployment = #sedge::__private::hydro_deploy::Deployment::new();
                let mut __flow = #hydro::compile::builder::FlowBuilder::new();
                let __external = __flow.external::<()>();
                let __relay = __flow.process::<#sedge::SedgeRelay>();
                let mut __observer = #sedge::Observer::new(&__external, &__relay);
                #(#make_locs)*
                #(#make_sizes)*
                #(#make_inputs)*
                #observe
                let __nodes = __flow
                    .with_default_optimize()
                    #(#with_processes)*
                    #with_relay
                    .with_external(&__external, __deployment.Localhost())
                    .deploy(&mut __deployment);
                __deployment.deploy().await.expect("deployment failed");
                let __feed_stats = #sedge::harness::FeedStats::default();
                let mut __writers: ::std::vec::Vec<::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ()>>>> = ::std::vec::Vec::new();
                #(#connect_sinks)*
                let mut __timers = ::std::vec::Vec::new();
                #(#make_timers)*
                #send_round
                #sedge::harness::execute(
                    &mut __deployment,
                    &__nodes,
                    __send_round,
                    __writers,
                    __feed_stats,
                    __timers,
                    __observer,
                    &__meta,
                    __run,
                    __spec,
                    __cap,
                )
                .await
            })
        }

        #hydro::macro_support::ctor::declarative::ctor! {
            #[ctor(unsafe)]
            fn #register_name() {
                #sedge::harness::register_check(#sedge::harness::RegisteredCheck {
                    function: stringify!(#fn_name),
                    configuration: #config_name,
                    ignore: #ignore_value,
                    run: #entry_name,
                });
            }
        }
    ))
}

// ---------------------------------------------------------------------------------------------
// The derive

/// Implements `sedge::Observed` for a struct whose fields are `Stream`s on `Process` or `Cluster`
/// locations by registering every field whole with `sedge::Observer::stream`, under the field's
/// name (a cluster stream's records are tagged with their member; its levels are summed). The
/// harness then counts each stream's records per second, keeps the latest value of numeric
/// streams, and reads ids from `u64` items or `id` fields; which of these the verdict uses is
/// decided by the stream's kind, which comes from Cress's conservation table when one exists for
/// the function and configuration (see `cress::Conservation`), and otherwise from the item type.
/// No `q!` closure is needed in the program's crate. The struct must have exactly one lifetime
/// parameter, the `'a` of its streams' locations. The derive always names the crate `::sedge`.
#[proc_macro_derive(SedgeOutputs)]
pub fn derive_sedge_outputs(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::DeriveInput);
    let name = &input.ident;
    let sedge: syn::Path = syn::parse_quote!(::sedge);
    let syn::Data::Struct(data) = &input.data else {
        return syn::Error::new(input.span(), "#[derive(SedgeOutputs)] only supports structs")
            .to_compile_error()
            .into();
    };
    let lifetimes: Vec<&syn::Lifetime> = input
        .generics
        .params
        .iter()
        .filter_map(|p| match p {
            GenericParam::Lifetime(l) => Some(&l.lifetime),
            _ => None,
        })
        .collect();
    let [lt] = lifetimes.as_slice() else {
        return syn::Error::new(
            input.generics.span(),
            "#[derive(SedgeOutputs)] needs exactly one lifetime parameter, the `'a` of the streams' locations",
        )
        .to_compile_error()
        .into();
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (registrations, types): (Vec<TokenStream2>, Vec<&Type>) = match &data.fields {
        syn::Fields::Named(f) => f
            .named
            .iter()
            .map(|fld| {
                let id = fld.ident.as_ref().unwrap();
                let name = id.to_string();
                (quote!(__observer.stream(#name, self.#id);), &fld.ty)
            })
            .unzip(),
        syn::Fields::Unnamed(f) => f
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, fld)| {
                let idx = syn::Index::from(i);
                let name = i.to_string();
                (quote!(__observer.stream(#name, self.#idx);), &fld.ty)
            })
            .unzip(),
        syn::Fields::Unit => (Vec::new(), Vec::new()),
    };
    // A generic output struct (Raft's `RaftOutputs<'a, T, ClusterTag>`) is observable whenever
    // each of its fields is, which is what these bounds say; they put the `Serialize` requirement
    // on the field types rather than on the struct's parameters.
    let bounds = types.iter().map(|t| quote!(#t: #sedge::Observable<#lt>));
    let where_tokens = match where_clause {
        Some(w) => {
            let preds = w.predicates.iter();
            quote!(where #(#preds,)* #(#bounds,)*)
        }
        None => quote!(where #(#bounds,)*),
    };
    quote!(
        #[automatically_derived]
        impl #impl_generics #sedge::Observed<#lt> for #name #ty_generics #where_tokens {
            fn observe(self, __observer: &mut #sedge::Observer<#lt>) {
                #(#registrations)*
            }
        }
    )
    .into()
}
