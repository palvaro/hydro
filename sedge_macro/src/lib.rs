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
//! * The `Stream<T, Process<'a, X>, Unbounded, O, R>` parameter named by `load = ...` is the
//!   program's request input. The harness feeds it from an external port with values of `T` made
//!   from the request's sequence number by `sedge::LoadItem`.
//! * Every other `Stream<(), Process<'a, X>, Unbounded, ..>` parameter is a timer and needs a
//!   period in `timers(...)`.
//! * Anything else is a plain value that must be given in the attribute by the parameter's name,
//!   for example `policy = RetryPolicy { timeout_ticks: 40, max_attempts: 3 }`.
//!
//! Every generic type parameter of the function must be given a concrete type, as `T = u64`.
//! Cluster parameters, keyed streams, and data streams other than the load are not supported yet
//! and are reported as errors.
//!
//! The function's return value is handed to `sedge::Observed::observe`, which the program's crate
//! implements to name its completion, abandonment, gauge and counter outputs.
//!
//! ## What the attribute accepts
//!
//! * `name = ident` or `name = "string"`: the configuration's name, shown with its verdict.
//! * `T = Type`: the concrete type for generic parameter `T`.
//! * `param = expr`: the value of a plain parameter.
//! * `load = param`: which stream parameter the generator feeds. Required.
//! * `timers(param = Duration-expr, ...)`: the period of each timer parameter. Required for
//!   every `()` stream parameter other than the load.
//! * `mu = expr`: the service capacity in requests per second, the `μ` that the command's
//!   overprovisioning factors divide. The command's `--mu` overrides it; one or the other must
//!   be given.
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
            // Raw tokens up to the next comma, because a type such as `Vec<(u32, u32)>` is not
            // an expression.
            let value = tokens_until_comma(input)?;
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

/// A process the harness must create, identified by its marker type.
struct Loc {
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

/// Classifies a `Process<'a, X>` type, if it is one; rejects clusters.
fn as_process(ty: &Type) -> syn::Result<Option<Type>> {
    let Some((ident, args)) = last_ident(ty) else {
        return Ok(None);
    };
    match ident.to_string().as_str() {
        "Process" => {}
        "Cluster" => {
            return Err(syn::Error::new(
                ty.span(),
                "#[overload_check] does not support cluster locations yet",
            ));
        }
        _ => return Ok(None),
    }
    let marker = type_args(args)
        .first()
        .copied()
        .cloned()
        .unwrap_or_else(unit_type);
    Ok(Some(marker))
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
    let mut mu: Option<Expr> = None;
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
            ("mu", Some(v), _) => mu = Some(syn::parse2(v.clone())?),
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
    let Some(load) = load else {
        return Err(syn::Error::new(
            Span::call_site(),
            "`load = <stream parameter>` names the request input the generator feeds; it is required",
        ));
    };
    let hydro: syn::Path = syn::parse_quote!(#sedge::__private::hydro_lang);
    let mut subst = Substitute {
        map: &generic_types,
    };

    // Classify the parameters.
    let mut locs: Vec<Loc> = Vec::new();
    let mut streams: Vec<StreamParam> = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    let find_or_add_loc = |marker: Type, param: Option<&Ident>, locs: &mut Vec<Loc>| -> usize {
        let key = type_key(&marker);
        if let Some(i) = locs.iter().position(|l| type_key(&l.marker) == key) {
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
            && let Some(marker) = as_process(&r.elem)?
        {
            let i = find_or_add_loc(marker, Some(&pname), &mut locs);
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
            let Some(marker) = as_process(targs[1])? else {
                return Err(syn::Error::new(
                    targs[1].span(),
                    "a stream parameter's location must be a Process",
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
            let is_load = pname == load;
            let is_timer = timers.iter().any(|(t, _)| *t == pname);
            if !is_load && !is_timer {
                if is_unit(&item) {
                    return Err(syn::Error::new(
                        pname.span(),
                        format!(
                            "timer `{pname}` needs a period in the attribute, as `timers({pname} = Duration::from_millis(..))`"
                        ),
                    ));
                }
                return Err(syn::Error::new(
                    pname.span(),
                    format!(
                        "stream `{pname}` is neither the load nor a timer; #[overload_check] feeds one data stream, named by `load = ...`"
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
            let loc = find_or_add_loc(marker, None, &mut locs);
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
    if !streams.iter().any(|s| s.name == load) {
        return Err(syn::Error::new(
            load.span(),
            format!("`{load}` is not a stream parameter of `{fn_name}`"),
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
    if let Some((k, _, span)) = values.first() {
        return Err(syn::Error::new(
            *span,
            format!("`{k}` is not a parameter of `{fn_name}`"),
        ));
    }

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
        quote!(let #var = __flow.process::<#marker>();)
    });

    let make_inputs = streams.iter().map(|s| {
        let sv = &s.name;
        let (item, order, retries) = (&s.item, &s.order, &s.retries);
        let locv = &locs[s.loc].var;
        if s.name == load {
            quote!(
                let (__load, #sv) = #sedge::load_input::<_, #item>(&#locv, &__external);
                let #sv = #sv.weaken_ordering::<#order>().weaken_retries::<#retries>();
            )
        } else {
            let period = &timers.iter().find(|(t, _)| *t == s.name).unwrap().1;
            quote!(
                let #sv = #sedge::timer(&#locv, (#period))
                    .weaken_ordering::<#order>()
                    .weaken_retries::<#retries>();
            )
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

    let with_processes = locs.iter().map(|l| {
        let v = &l.var;
        quote!(.with_process(&#v, #sedge::harness::host(&mut __deployment, __run)))
    });

    let mu_value = mu
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
        #[allow(non_snake_case, unused_variables, clippy::all)]
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
            };
            #sedge::harness::block_on(async {
                let mut __results = ::std::vec::Vec::new();
                for (__factor, __trial) in __run.schedule() {
                    let mut __deployment = #sedge::__private::hydro_deploy::Deployment::new();
                    let mut __flow = #hydro::compile::builder::FlowBuilder::new();
                    let __external = __flow.external::<()>();
                    let mut __observer = #sedge::Observer::new(&__external);
                    #(#make_locs)*
                    #(#make_inputs)*
                    #observe
                    let __nodes = __flow
                        .with_default_optimize()
                        #(#with_processes)*
                        .with_external(&__external, __deployment.Localhost())
                        .deploy(&mut __deployment);
                    __results.push(
                        #sedge::harness::run_trial(
                            &mut __deployment,
                            &__nodes,
                            __load,
                            __observer,
                            &__meta,
                            __run,
                            __factor,
                            __trial,
                        )
                        .await,
                    );
                }
                __results
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
