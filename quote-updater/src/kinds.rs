//! The pricing kinds a binary registered, by the name a `[pairs.pricing]` stanza gives.
//!
//! Fixed for the life of the process: a reload can change which kind a pair uses and with
//! what settings, never which kinds exist. Each stanza is deserialized into its kind's own
//! config type (minus `kind`) when the file is checked, validated against the pair once
//! preflight has resolved it, and built when the lane is.

use std::{
    any::Any, collections::BTreeMap, marker::PhantomData, panic::AssertUnwindSafe, sync::Arc,
    time::Duration,
};

use eyre::{Result, WrapErr, bail, ensure, eyre};
use futures_util::FutureExt;
use serde::de::DeserializeOwned;

use crate::{
    config::{Config, GuardStanza, PricingSpec},
    guard::{BUILT_IN_GUARDS, BuiltGuard, GuardSide, MarketGuard, QuoteGuard},
    pricing::{
        BoxFuture, BuildCtx, Factory, FormField, MarketGuardFactory, PairShape, Pricer,
        QuoteGuardFactory,
    },
};

/// How long one `build()` may take, in line with `RPC_TIMEOUT` and the 15s feed dial: a
/// build that hangs is a failed build, not a stalled reload.
pub(crate) const BUILD_TIMEOUT: Duration = Duration::from_secs(30);

/// What a panic said, for the `Err` or the line it becomes.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("(not a string)")
}

/// Every call into a kind's code goes through here or [`Kinds::build`], so a panic in it is
/// the `Err` that code could have returned: a refused file at startup and on reload, a failed
/// lane in a reload's builds. Unwinding instead would end `run()`, drop every lane's quote at
/// once and, through the restart, clear every latched breaker. The factory is used again
/// after a panic (it is the binary's, registered once), the same trade the supervisor makes
/// for a pair loop.
fn guarded<T>(kind: &str, doing: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|payload| {
        Err(eyre!(
            "pricing `{kind}` panicked while {doing}: {}",
            panic_message(&*payload)
        ))
    })
}

/// A [`Factory`] for pricers with its config type erased, so kinds with different config
/// types sit in one table.
pub(crate) trait ErasedPricer: Send + Sync {
    fn check(&self, table: &toml::Table) -> Result<()>;
    fn validate(&self, table: &toml::Table, pair: &PairShape) -> Result<()>;
    fn fields(&self) -> &'static [FormField];
    fn form_help(&self) -> &'static str;
    fn summary(&self, table: &toml::Table) -> Result<String>;
    fn build<'a>(
        &'a self,
        table: &'a toml::Table,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, Result<Box<dyn Pricer>>>;
}

/// The stanza minus its `kind`, as the kind's own config type.
pub(crate) fn typed<C: DeserializeOwned>(table: &toml::Table) -> Result<C> {
    let mut table = table.clone();
    table.remove("kind");
    toml::Value::Table(table)
        .try_into()
        .map_err(|err| eyre!("{err}"))
}

impl<F: Factory> ErasedPricer for F {
    fn check(&self, table: &toml::Table) -> Result<()> {
        typed::<F::Config>(table).map(drop)
    }

    fn validate(&self, table: &toml::Table, pair: &PairShape) -> Result<()> {
        let cfg = typed::<F::Config>(table)?;
        Factory::validate(self, &cfg, pair)
    }

    fn fields(&self) -> &'static [FormField] {
        Factory::fields(self)
    }

    fn form_help(&self) -> &'static str {
        Factory::form_help(self)
    }

    fn summary(&self, table: &toml::Table) -> Result<String> {
        let cfg = typed::<F::Config>(table)?;
        Ok(Factory::summary(self, &cfg))
    }

    fn build<'a>(
        &'a self,
        table: &'a toml::Table,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, Result<Box<dyn Pricer>>> {
        Box::pin(async move {
            let cfg = typed::<F::Config>(table)?;
            // Boxed here, once, so a factory returns its own type and never writes the
            // `as Box<dyn Pricer>` coercion. A factory whose `Pricer` is already a
            // `Box<dyn Pricer>` gets a second box around it: one more pointer to follow per
            // tick, on the one path that chose dynamism, and no way around it without
            // specialization.
            let pricer = Factory::build(self, &cfg, ctx).await?;
            Ok(Box::new(pricer) as Box<dyn Pricer>)
        })
    }
}

/// What `pricer_fn` registers: a closure standing in for a `Factory`, with the default
/// `validate`.
pub(crate) struct FnFactory<C, P, F> {
    make: F,
    _built: PhantomData<fn() -> (C, P)>,
}

impl<C, P, F> Factory for FnFactory<C, P, F>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    P: Pricer,
    F: Fn(C, &mut BuildCtx) -> Result<P> + Send + Sync + 'static,
{
    type Config = C;
    type Pricer = P;

    fn build<'a>(&'a self, cfg: &'a C, ctx: &'a mut BuildCtx) -> BoxFuture<'a, Result<P>> {
        // The closure takes its config by value: a pricer usually keeps it, and a clone
        // here spares every closure writing one.
        Box::pin(std::future::ready((self.make)(cfg.clone(), ctx)))
    }
}

/// A guard factory with its config type and its side erased, so market and quote guards of
/// different config types sit in one table, under one namespace: a stanza names a kind
/// without saying which side it judges.
pub(crate) trait ErasedGuard: Send + Sync {
    fn side(&self) -> GuardSide;
    fn check(&self, table: &toml::Table) -> Result<()>;
    fn validate(&self, table: &toml::Table, pair: &PairShape) -> Result<()>;
    fn fields(&self) -> &'static [FormField];
    fn form_help(&self) -> &'static str;
    fn summary(&self, table: &toml::Table) -> Result<String>;
    fn build<'a>(
        &'a self,
        table: &'a toml::Table,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, Result<BuiltGuard>>;
}

/// A [`MarketGuardFactory`] behind [`ErasedGuard`].
struct MarketErased<F>(F);

impl<F: MarketGuardFactory> ErasedGuard for MarketErased<F> {
    fn side(&self) -> GuardSide {
        GuardSide::Market
    }

    fn check(&self, table: &toml::Table) -> Result<()> {
        typed::<F::Config>(table).map(drop)
    }

    fn validate(&self, table: &toml::Table, pair: &PairShape) -> Result<()> {
        let cfg = typed::<F::Config>(table)?;
        self.0.validate(&cfg, pair)
    }

    fn fields(&self) -> &'static [FormField] {
        self.0.fields()
    }

    fn form_help(&self) -> &'static str {
        self.0.form_help()
    }

    fn summary(&self, table: &toml::Table) -> Result<String> {
        Ok(self.0.summary(&typed::<F::Config>(table)?))
    }

    fn build<'a>(
        &'a self,
        table: &'a toml::Table,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, Result<BuiltGuard>> {
        Box::pin(async move {
            let cfg = typed::<F::Config>(table)?;
            let guard = self.0.build(&cfg, ctx).await?;
            Ok(BuiltGuard::Market(Box::new(guard)))
        })
    }
}

/// A [`QuoteGuardFactory`] behind [`ErasedGuard`].
struct QuoteErased<F>(F);

impl<F: QuoteGuardFactory> ErasedGuard for QuoteErased<F> {
    fn side(&self) -> GuardSide {
        GuardSide::Quote
    }

    fn check(&self, table: &toml::Table) -> Result<()> {
        typed::<F::Config>(table).map(drop)
    }

    fn validate(&self, table: &toml::Table, pair: &PairShape) -> Result<()> {
        let cfg = typed::<F::Config>(table)?;
        self.0.validate(&cfg, pair)
    }

    fn fields(&self) -> &'static [FormField] {
        self.0.fields()
    }

    fn form_help(&self) -> &'static str {
        self.0.form_help()
    }

    fn summary(&self, table: &toml::Table) -> Result<String> {
        Ok(self.0.summary(&typed::<F::Config>(table)?))
    }

    fn build<'a>(
        &'a self,
        table: &'a toml::Table,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, Result<BuiltGuard>> {
        Box::pin(async move {
            let cfg = typed::<F::Config>(table)?;
            let guard = self.0.build(&cfg, ctx).await?;
            Ok(BuiltGuard::Quote(Box::new(guard)))
        })
    }
}

/// A [`MarketGuardFactory`] as a registered guard kind.
pub(crate) fn market_guard_factory<F: MarketGuardFactory>(factory: F) -> Arc<dyn ErasedGuard> {
    Arc::new(MarketErased(factory))
}

/// A [`QuoteGuardFactory`] as a registered guard kind.
pub(crate) fn quote_guard_factory<F: QuoteGuardFactory>(factory: F) -> Arc<dyn ErasedGuard> {
    Arc::new(QuoteErased(factory))
}

/// What `market_guard_fn` registers: a closure standing in for a [`MarketGuardFactory`].
struct MarketFnFactory<C, G, F> {
    make: F,
    _built: PhantomData<fn() -> (C, G)>,
}

impl<C, G, F> MarketGuardFactory for MarketFnFactory<C, G, F>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    G: MarketGuard,
    F: Fn(C, &mut BuildCtx) -> Result<G> + Send + Sync + 'static,
{
    type Config = C;
    type Guard = G;

    fn build<'a>(&'a self, cfg: &'a C, ctx: &'a mut BuildCtx) -> BoxFuture<'a, Result<G>> {
        Box::pin(std::future::ready((self.make)(cfg.clone(), ctx)))
    }
}

/// A closure as a registered market guard kind.
pub(crate) fn market_guard_fn<C, G, F>(make: F) -> Arc<dyn ErasedGuard>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    G: MarketGuard,
    F: Fn(C, &mut BuildCtx) -> Result<G> + Send + Sync + 'static,
{
    Arc::new(MarketErased(MarketFnFactory {
        make,
        _built: PhantomData,
    }))
}

/// What `quote_guard_fn` registers: a closure standing in for a [`QuoteGuardFactory`].
struct QuoteFnFactory<C, G, F> {
    make: F,
    _built: PhantomData<fn() -> (C, G)>,
}

impl<C, G, F> QuoteGuardFactory for QuoteFnFactory<C, G, F>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    G: QuoteGuard,
    F: Fn(C, &mut BuildCtx) -> Result<G> + Send + Sync + 'static,
{
    type Config = C;
    type Guard = G;

    fn build<'a>(&'a self, cfg: &'a C, ctx: &'a mut BuildCtx) -> BoxFuture<'a, Result<G>> {
        Box::pin(std::future::ready((self.make)(cfg.clone(), ctx)))
    }
}

/// A closure as a registered quote guard kind.
pub(crate) fn quote_guard_fn<C, G, F>(make: F) -> Arc<dyn ErasedGuard>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    G: QuoteGuard,
    F: Fn(C, &mut BuildCtx) -> Result<G> + Send + Sync + 'static,
{
    Arc::new(QuoteErased(QuoteFnFactory {
        make,
        _built: PhantomData,
    }))
}

/// A closure as a registered pricing kind.
pub(crate) fn fn_factory<C, P, F>(make: F) -> Arc<dyn ErasedPricer>
where
    C: DeserializeOwned + Clone + Send + Sync + 'static,
    P: Pricer,
    F: Fn(C, &mut BuildCtx) -> Result<P> + Send + Sync + 'static,
{
    Arc::new(FnFactory {
        make,
        _built: PhantomData,
    })
}

/// The registered pricing kinds. Registration errors are kept and reported when the run
/// starts, so the builder's methods can chain.
#[derive(Clone, Default)]
pub(crate) struct Kinds {
    pricers: BTreeMap<&'static str, Arc<dyn ErasedPricer>>,
    /// Market and quote guards in one namespace.
    guards: BTreeMap<&'static str, Arc<dyn ErasedGuard>>,
    errors: Vec<String>,
}

impl Kinds {
    pub(crate) fn insert_guard(&mut self, kind: &'static str, factory: Arc<dyn ErasedGuard>) {
        if BUILT_IN_GUARDS.contains(&kind) {
            self.errors.push(format!(
                "guard kind `{kind}` is built in and cannot be registered"
            ));
        } else if self.guards.insert(kind, factory).is_some() {
            self.errors
                .push(format!("guard kind `{kind}` registered twice"));
        }
    }

    fn get_guard(&self, kind: &str, at: &str) -> Result<&Arc<dyn ErasedGuard>> {
        self.guards.get(kind).ok_or_else(|| {
            if self.guards.is_empty() {
                eyre!("{at}: unknown guard kind `{kind}`; this binary registers none")
            } else {
                let registered: Vec<&str> = self.guards.keys().copied().collect();
                eyre!(
                    "{at}: unknown guard kind `{kind}`; registered: {}",
                    registered.join(", ")
                )
            }
        })
    }

    /// Which side a registered guard kind judges, or `None` for a kind nobody registered.
    pub(crate) fn guard_side(&self, kind: &str) -> Option<GuardSide> {
        self.guards.get(kind).map(|factory| factory.side())
    }

    /// The `--check` column text for a stanza: the kind, then its factory's summary.
    pub(crate) fn guard_summary(&self, stanza: &GuardStanza, at: &str) -> Result<String> {
        let factory = self.get_guard(&stanza.kind, at)?;
        let summary = guarded(&stanza.kind, "summarizing", || {
            factory.summary(&stanza.config)
        })?;
        Ok(if summary.is_empty() {
            stanza.kind.clone()
        } else {
            format!("{} {summary}", stanza.kind)
        })
    }

    /// Builds one guard stanza's guard, bounded by [`BUILD_TIMEOUT`], with a panic caught
    /// as [`guarded`] catches one. The registered kind's name is the `&'static str` a
    /// lane labels the guard with.
    pub(crate) async fn build_guard(
        &self,
        stanza: &GuardStanza,
        ctx: &mut BuildCtx,
    ) -> Result<BuiltGuard> {
        let label = ctx.pair().label.clone();
        let kind = &stanza.kind;
        let factory = self.get_guard(kind, &label)?;
        let build = AssertUnwindSafe(factory.build(&stanza.config, ctx)).catch_unwind();
        tokio::time::timeout(BUILD_TIMEOUT, build)
            .await
            .map_err(|_| eyre!("guard `{kind}` did not build within {BUILD_TIMEOUT:?}"))?
            .unwrap_or_else(|payload| {
                Err(eyre!(
                    "guard `{kind}` panicked while building: {}",
                    panic_message(&*payload)
                ))
            })
            .wrap_err_with(|| format!("{label}: [[pairs.guards]] `{kind}`"))
    }

    /// The registered name a stanza's kind was registered under, as the `&'static str` the
    /// lane labels its guard with.
    pub(crate) fn guard_name(&self, kind: &str) -> Option<&'static str> {
        self.guards.get_key_value(kind).map(|(name, _)| *name)
    }

    /// The registered guard kinds, by name, in order: what the backoffice's form offers,
    /// market and quote guards together.
    pub(crate) fn guard_kinds(&self) -> Vec<&'static str> {
        self.guards.keys().copied().collect()
    }

    /// A registered guard kind's form fields, or `None` for a kind nobody registered.
    pub(crate) fn guard_fields(&self, kind: &str) -> Option<&'static [FormField]> {
        self.guards.get(kind).map(|factory| factory.fields())
    }

    /// A registered guard kind's form help, or `None` for a kind nobody registered.
    pub(crate) fn guard_form_help(&self, kind: &str) -> Option<&'static str> {
        self.guards.get(kind).map(|factory| factory.form_help())
    }

    /// The registered name of a pricing kind, as the `&'static str` a trip names it by.
    pub(crate) fn pricer_name(&self, kind: &str) -> Option<&'static str> {
        self.pricers.get_key_value(kind).map(|(name, _)| *name)
    }
    /// The registered pricing kinds, by name, in order: what the backoffice's form offers.
    pub(crate) fn pricer_kinds(&self) -> Vec<&'static str> {
        self.pricers.keys().copied().collect()
    }

    /// A registered kind's form fields, or `None` for a kind nobody registered.
    pub(crate) fn pricer_fields(&self, kind: &str) -> Option<&'static [FormField]> {
        self.pricers.get(kind).map(|factory| factory.fields())
    }

    /// A registered kind's form help, or `None` for a kind nobody registered.
    pub(crate) fn pricer_form_help(&self, kind: &str) -> Option<&'static str> {
        self.pricers.get(kind).map(|factory| factory.form_help())
    }

    /// One line for a pair's stanza: the kind, then its factory's summary of the stanza,
    /// for the `--check` report and the backoffice's pair list. A stanza its kind cannot
    /// read, or an unregistered kind, shows the kind alone.
    pub(crate) fn pricer_summary(&self, spec: &PricingSpec) -> String {
        let summary = self
            .pricers
            .get(spec.kind.as_str())
            .and_then(|factory| {
                guarded(&spec.kind, "summarizing", || factory.summary(&spec.config)).ok()
            })
            .unwrap_or_default();
        if summary.is_empty() {
            spec.kind.clone()
        } else {
            format!("{} {summary}", spec.kind)
        }
    }

    /// The three pricing kinds this crate ships, registered the way a binary does.
    #[cfg(test)]
    pub(crate) fn shipped() -> Self {
        let mut kinds = Self::default();
        kinds.insert("fixed", Arc::new(crate::pricing::Fixed));
        kinds.insert("feed", Arc::new(crate::pricing::Feed));
        kinds.insert("volatile", Arc::new(crate::pricing::Volatile));
        kinds
    }

    pub(crate) fn insert(&mut self, kind: &'static str, factory: Arc<dyn ErasedPricer>) {
        if self.pricers.insert(kind, factory).is_some() {
            self.errors
                .push(format!("pricing kind `{kind}` registered twice"));
        }
    }

    /// What went wrong registering, all of it at once.
    pub(crate) fn errors(&self) -> Result<()> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", self.errors.join("; "))
        }
    }

    fn get(&self, kind: &str, at: &str) -> Result<&Arc<dyn ErasedPricer>> {
        self.pricers.get(kind).ok_or_else(|| {
            if self.pricers.is_empty() {
                eyre!("{at}: unknown pricing kind `{kind}`; this binary registers none")
            } else {
                let registered: Vec<&str> = self.pricers.keys().copied().collect();
                eyre!(
                    "{at}: unknown pricing kind `{kind}`; registered: {}",
                    registered.join(", ")
                )
            }
        })
    }

    /// Every custom stanza names a registered kind and deserializes into its config, and
    /// every guard stanza likewise, a market guard only on a pair with a market, and a halted
    /// pair's custom stanza is validated against its pair as well: the part of checking a file
    /// that needs no chain, run right after it parses, at startup, on reload and under
    /// `--check`. Halted pairs too, because a resume is a reload: a stanza that cannot price
    /// its pair would otherwise be accepted now and refused only then. A running pair is
    /// validated once preflight has its on-chain label ([`Self::validate`]); a halted one
    /// gets none, so its shape is the file's alone, labelled by its lane.
    pub(crate) fn check(&self, config: &Config) -> Result<()> {
        for (number, spec, halted) in config.in_file_order() {
            // As `config.rs` numbers the file's `[[pairs]]` tables, halted ones included.
            let at = format!("pair {number}");
            let kind = &spec.pricing.kind;
            let table = &spec.pricing.config;
            let factory = self.get(kind, &at)?;
            // A config's `Deserialize` can be the binary's own code too.
            guarded(kind, "reading its stanza", || factory.check(table))
                .wrap_err_with(|| format!("{at}: [pairs.pricing] `{kind}`"))?;
            if halted {
                let shape = PairShape {
                    label: crate::config::lane_label(spec.lane),
                    tokens: spec.declared_base_quote(),
                    lane: spec.lane,
                    inverted: spec.invert,
                    price_decimals: config.price_decimals(),
                    target: config.target,
                    sources: spec.feeds.as_ref().map_or(0, |f| f.sources.len()),
                    mid_only_sources: spec.feeds.as_ref().map_or(0, |f| f.mid_only_sources()),
                    min_mid: spec.band.min,
                    max_mid: spec.band.max,
                };
                guarded(kind, "validating", || factory.validate(table, &shape))
                    .wrap_err_with(|| format!("{at} (halted): [pairs.pricing] `{kind}`"))?;
            }
            for stanza in &spec.guards {
                let factory = self.get_guard(&stanza.kind, &at)?;
                // A market guard judges the composite, and a pair that streams nothing (a
                // fixed mid, or a pricing kind with no symbol or sources) has none, so the
                // guard would never run while `guards{pair,kind}` said it did. Silently
                // accepting that would leave a pair the operator believes is guarded, the
                // reason `config.rs` refuses a breaker on a fixed mid. Here rather than at
                // parse, because only the registry knows which side a kind judges.
                ensure!(
                    factory.side() != GuardSide::Market || spec.feeds.is_some(),
                    "{at}: [[pairs.guards]] `{}` is a market guard, which judges the pair's \
                     streamed price, and this pair streams none, so it would never run; drop \
                     the stanza, or give the pair a symbol or sources",
                    stanza.kind
                );
                guarded(&stanza.kind, "reading its stanza", || {
                    factory.check(&stanza.config)
                })
                .wrap_err_with(|| format!("{at}: [[pairs.guards]] `{}`", stanza.kind))?;
            }
        }
        Ok(())
    }

    /// Every custom stanza and every guard stanza against the pair it would judge, once
    /// preflight resolved them.
    pub(crate) fn validate(
        &self,
        pairs: &[(PairShape, &PricingSpec, &[GuardStanza])],
    ) -> Result<()> {
        for (shape, pricing, guards) in pairs {
            self.validate_pricer(shape, pricing)?;
            self.validate_guards(shape, guards)?;
        }
        Ok(())
    }

    /// A pair's pricing stanza against the pair.
    pub(crate) fn validate_pricer(&self, shape: &PairShape, pricing: &PricingSpec) -> Result<()> {
        let kind = &pricing.kind;
        let factory = self.get(kind, &shape.label)?;
        guarded(kind, "validating", || {
            factory.validate(&pricing.config, shape)
        })
        .wrap_err_with(|| format!("{}: [pairs.pricing] `{kind}`", shape.label))?;
        Ok(())
    }

    /// A pair's guard stanzas against the pair, whatever prices it. Its own call so
    /// `--check` judges every pair's guards as the run does, not only a custom pair's.
    pub(crate) fn validate_guards(&self, shape: &PairShape, guards: &[GuardStanza]) -> Result<()> {
        for stanza in guards {
            let factory = self.get_guard(&stanza.kind, &shape.label)?;
            guarded(&stanza.kind, "validating", || {
                factory.validate(&stanza.config, shape)
            })
            .wrap_err_with(|| format!("{}: [[pairs.guards]] `{}`", shape.label, stanza.kind))?;
        }
        Ok(())
    }

    /// Builds a custom pair's pricer, bounded by [`BUILD_TIMEOUT`], with a panic caught as
    /// [`guarded`] catches one.
    pub(crate) async fn build(
        &self,
        kind: &str,
        table: &toml::Table,
        ctx: &mut BuildCtx,
    ) -> Result<Box<dyn Pricer>> {
        let label = ctx.pair().label.clone();
        let factory = self.get(kind, &label)?;
        let build = AssertUnwindSafe(factory.build(table, ctx)).catch_unwind();
        tokio::time::timeout(BUILD_TIMEOUT, build)
            .await
            .map_err(|_| eyre!("pricing `{kind}` did not build within {BUILD_TIMEOUT:?}"))?
            .unwrap_or_else(|payload| {
                Err(eyre!(
                    "pricing `{kind}` panicked while building: {}",
                    panic_message(&*payload)
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use ethrex_common::U256;

    use super::*;
    use crate::pricing::{Diagnostics, PricerOutput, Refusal, TickCtx};

    #[derive(Clone, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Cfg {
        #[allow(dead_code)]
        half_spread: f64,
    }

    struct P;

    impl Pricer for P {
        fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
            Ok(PricerOutput {
                delta: U256::zero(),
                mid: U256::one(),
            })
        }
    }

    fn kinds() -> Kinds {
        let mut r = Kinds::shipped();
        r.insert("skewed", fn_factory(|_: Cfg, _: &mut BuildCtx| Ok(P)));
        r
    }

    #[test]
    fn an_unknown_kind_is_refused_with_the_registered_list() {
        let config =
            crate::config::parse_config(&crate::config::tests_support::custom("fundng"), 18)
                .unwrap();
        let err = format!("{:#}", kinds().check(&config).unwrap_err());
        assert!(
            err.contains("unknown pricing kind `fundng`") && err.contains("skewed"),
            "{err}"
        );
        let err = format!("{:#}", Kinds::default().check(&config).unwrap_err());
        assert!(err.contains("registers none"), "{err}");
    }

    #[test]
    fn a_stanza_that_does_not_fit_its_kind_is_refused_naming_the_pair() {
        let config = crate::config::parse_config(
            &crate::config::tests_support::CUSTOM
                .replace("half_spread = 0.0005", "half_spread = \"wide\""),
            18,
        )
        .unwrap();
        let err = format!("{:#}", kinds().check(&config).unwrap_err());
        assert!(err.contains("pair 1"), "{err}");
        assert!(err.contains("half_spread"), "{err}");
        let unknown = crate::config::parse_config(
            &crate::config::tests_support::custom_with("skewed", "not_a_knob = 1"),
            18,
        )
        .unwrap();
        let err = format!("{:#}", kinds().check(&unknown).unwrap_err());
        assert!(err.contains("not_a_knob"), "{err}");
    }

    /// Two custom pairs: pair 1 (WETH/USDC) halted with `first` as its stanza's knobs, pair 2
    /// (USDC/USDT) running with `second`.
    fn halted_then_running(first: &str, second: &str) -> Config {
        crate::config::parse_config(
            &format!(
                r#"target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
key_env = "K_WETH_USDC"
halted  = true
[pairs.pricing]
{first}
[[pairs]]
tokens  = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "0xdAC17F958D2ee523a2206206994597C13D831ec7"]
key_env = "K_USDC_USDT"
[pairs.pricing]
{second}
[[builder]]
name = "b"
endpoint = "wss://b.example/ws"
api_key = "k"
"#
            ),
            18,
        )
        .unwrap()
    }

    /// Refuses a half-spread of 1% or more against the pair it would price.
    struct Picky;

    impl Factory for Picky {
        type Config = Cfg;
        type Pricer = P;

        fn validate(&self, cfg: &Cfg, pair: &PairShape) -> Result<()> {
            if cfg.half_spread >= 0.01 {
                bail!(
                    "half_spread {} is too wide for {}",
                    cfg.half_spread,
                    pair.label
                );
            }
            Ok(())
        }

        fn build<'a>(&'a self, _: &'a Cfg, _: &'a mut BuildCtx) -> BoxFuture<'a, Result<P>> {
            Box::pin(async { Ok(P) })
        }
    }

    /// A halted pair comes back on a reload, so a stanza that cannot price it is refused
    /// with the file, at startup, on reload and under `--check`, not when the resume finds
    /// it; and every pair is numbered as the file numbers its `[[pairs]]` tables, halted
    /// ones included.
    #[test]
    fn a_halted_pairs_stanza_is_checked_with_the_file_and_numbered_as_the_file_numbers_it() {
        let mut kinds = kinds();
        kinds.insert("picky", Arc::new(Picky));
        let good = "kind = \"picky\"\nhalf_spread = 0.0005";
        kinds
            .check(&halted_then_running(good, good))
            .expect("both stanzas fit their pairs");

        for (first, second, at, says) in [
            (
                "kind = \"pickky\"",
                good,
                "pair 1",
                "unknown pricing kind `pickky`",
            ),
            (
                "kind = \"picky\"\nhalf_spread = \"wide\"",
                good,
                "pair 1",
                "half_spread",
            ),
            (
                "kind = \"picky\"\nhalf_spread = 0.05",
                good,
                "pair 1",
                "too wide",
            ),
            (
                good,
                "kind = \"picky\"\nhalf_spread = \"wide\"",
                "pair 2",
                "half_spread",
            ),
        ] {
            let err = format!(
                "{:#}",
                kinds
                    .check(&halted_then_running(first, second))
                    .unwrap_err()
            );
            assert!(
                err.starts_with(at) && err.contains(says),
                "{at}, {says}: {err}"
            );
        }
    }

    /// A secret typed where `{ env = "NAME" }` belongs is refused, at startup, in a reload's
    /// refusal and under `--check`, without the refusal printing it: those lines reach the
    /// journal, the backoffice banner and whoever ran the check.
    #[test]
    fn a_secret_typed_inline_is_refused_without_echoing_it() {
        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Keyed {
            half_spread: f64,
            api_key: crate::pricing::EnvSecret,
        }
        let mut kinds = Kinds::default();
        kinds.insert("keyed", fn_factory(|_: Keyed, _: &mut BuildCtx| Ok(P)));
        for (inline, secret) in [
            (
                r#"api_key = "sk-live-0123456789abcdef""#,
                "sk-live-0123456789abcdef",
            ),
            ("api_key = 4242424242424242", "4242424242424242"),
            ("api_key = 0.4242424242", "4242424242"),
        ] {
            let config = crate::config::parse_config(
                &crate::config::tests_support::custom_with("keyed", inline),
                18,
            )
            .unwrap();
            let err = format!("{:#}", kinds.check(&config).unwrap_err());
            assert!(!err.contains(secret), "{err}");
            assert!(
                err.contains("{ env = "),
                "the refusal says how to write one: {err}"
            );
        }
        let config = crate::config::parse_config(
            &crate::config::tests_support::custom_with("keyed", r#"api_key = { env = "K" }"#),
            18,
        )
        .unwrap();
        kinds
            .check(&config)
            .expect("the way a stanza names a secret");
    }

    #[test]
    fn a_kind_registered_twice_is_an_error_at_run() {
        let mut r = kinds();
        r.insert("skewed", fn_factory(|_: Cfg, _: &mut BuildCtx| Ok(P)));
        let err = format!("{:#}", r.errors().unwrap_err());
        assert!(err.contains("`skewed` registered twice"), "{err}");
        kinds().errors().expect("one kind, registered once");
    }

    /// A factory that picks its model from the stanza names `Box<dyn Pricer>` as what it
    /// builds. A concrete type is boxed by the core, so a factory never writes the coercion
    /// unless it is choosing.
    #[tokio::test]
    async fn a_factory_may_build_a_pricer_chosen_at_build_time() {
        #[derive(serde::Deserialize)]
        struct Pick {
            doubled: bool,
        }
        struct Q;
        impl Pricer for Q {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Ok(PricerOutput::new(U256::zero(), U256::from(2)))
            }
        }
        struct Chooser;
        impl Factory for Chooser {
            type Config = Pick;
            type Pricer = Box<dyn Pricer>;

            fn build<'a>(
                &'a self,
                cfg: &'a Pick,
                _: &'a mut BuildCtx,
            ) -> BoxFuture<'a, Result<Box<dyn Pricer>>> {
                Box::pin(async move {
                    Ok(if cfg.doubled {
                        Box::new(Q) as Box<dyn Pricer>
                    } else {
                        Box::new(P)
                    })
                })
            }
        }
        let mut kinds = Kinds::default();
        kinds.insert("chosen", Arc::new(Chooser));
        let pair = crate::testing::pair();
        for (doubled, mid) in [(false, 1u64), (true, 2)] {
            let stanza: toml::Table =
                toml::from_str(&format!("kind = \"chosen\"\ndoubled = {doubled}")).unwrap();
            let mut pricer = kinds
                .build("chosen", &stanza, &mut BuildCtx::new(pair.clone()))
                .await
                .expect("builds");
            let out = pricer
                .price(&crate::testing::tick(&pair, None), &mut Diagnostics::new(0))
                .unwrap();
            assert_eq!(out.mid, U256::from(mid));
        }
    }

    /// A stanza with nothing but its kind.
    #[derive(Clone, serde::Deserialize)]
    struct Empty {}

    struct PassAll;

    impl crate::guard::MarketGuard for PassAll {
        fn assess(
            &mut self,
            _: &crate::guard::CompositeSample,
            _: &mut Diagnostics,
        ) -> crate::guard::Verdict {
            crate::guard::Verdict::Pass
        }
    }

    struct AllowAll;

    impl crate::guard::QuoteGuard for AllowAll {
        fn check(
            &mut self,
            _: &crate::guard::Candidate<'_>,
            _: &mut Diagnostics,
        ) -> crate::guard::Gate {
            crate::guard::Gate::Allow
        }
    }

    fn stanza(kind: &str) -> crate::config::GuardStanza {
        crate::config::GuardStanza {
            kind: kind.to_owned(),
            config: toml::from_str(&format!("kind = \"{kind}\"")).unwrap(),
        }
    }

    /// A `[[pairs.guards]]` stanza names a kind without saying which side it judges, so the
    /// two sides share one namespace: a market guard and a quote guard cannot both be `cap`.
    #[test]
    fn a_market_and_a_quote_guard_share_one_namespace() {
        let mut kinds = Kinds::default();
        kinds.insert_guard(
            "cap",
            market_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(PassAll)),
        );
        kinds.insert_guard(
            "cap",
            quote_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(AllowAll)),
        );
        let err = format!("{:#}", kinds.errors().unwrap_err());
        assert!(err.contains("guard kind `cap` registered twice"), "{err}");
        let mut kinds = Kinds::default();
        kinds.insert_guard(
            "deviation",
            market_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(PassAll)),
        );
        let err = format!("{:#}", kinds.errors().unwrap_err());
        assert!(err.contains("`deviation` is built in"), "{err}");
    }

    #[test]
    fn a_stanza_naming_an_unknown_guard_is_refused_with_the_registered_list() {
        let config =
            crate::config::parse_config(&crate::config::tests_support::guarded("dispersion"), 18)
                .unwrap();
        let err = format!("{:#}", Kinds::shipped().check(&config).unwrap_err());
        assert!(
            err.contains("unknown guard kind `dispersion`") && err.contains("registers none"),
            "{err}"
        );
        let mut kinds = Kinds::shipped();
        kinds.insert_guard(
            "cap",
            quote_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(AllowAll)),
        );
        let err = format!("{:#}", kinds.check(&config).unwrap_err());
        assert!(
            err.contains("unknown guard kind `dispersion`") && err.contains("cap"),
            "{err}"
        );
    }

    /// A market guard judges the composite, and a pair that streams nothing has none: the
    /// stanza would be accepted, counted in `guards{pair,kind}` and never run, leaving a pair
    /// the operator believes is guarded. Refused with the file, at startup, on a reload and
    /// under `--check` alike, as a breaker on a fixed mid is. A quote guard judges every
    /// tick the pair quotes, so it stays allowed there.
    #[test]
    fn a_market_guard_on_a_pair_that_streams_nothing_is_refused_and_a_quote_guard_is_not() {
        let mut kinds = kinds();
        kinds.insert_guard(
            "dispersion",
            market_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(PassAll)),
        );
        kinds.insert_guard(
            "cap",
            quote_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(AllowAll)),
        );
        let guarded = |file: &str, kind: &str| {
            let file = file.replace(
                "[[builder]]",
                &format!("[[pairs.guards]]\nkind = \"{kind}\"\n[[builder]]"),
            );
            crate::config::parse_config(&file, 18).unwrap()
        };
        // A custom pricer with no market, and a fixed mid.
        let unstreamed =
            crate::config::tests_support::CUSTOM.replace("symbol  = \"ETHUSDC\"\n", "");
        let fixed = unstreamed.replace(
            "[pairs.pricing]\nkind = \"skewed\"\nhalf_spread = 0.0005\n",
            "[pairs.pricing]\nkind = \"fixed\"\nmid = \"1.0001\"\ndelta = \"0.0002\"\n",
        );
        for file in [&unstreamed, &fixed] {
            let err = format!(
                "{:#}",
                kinds.check(&guarded(file, "dispersion")).unwrap_err()
            );
            assert!(
                err.contains("pair 1") && err.contains("`dispersion`") && err.contains("market"),
                "{err}"
            );
            kinds
                .check(&guarded(file, "cap"))
                .expect("a quote guard judges the quote, which a pair without a market has");
        }
        kinds
            .check(&guarded(crate::config::tests_support::CUSTOM, "dispersion"))
            .expect("a pair with a market runs its market guards");
    }

    /// A quote guard reads the pricer's diagnostics by handle, bound at build; a name the
    /// pricer did not declare fails the build naming both, and a market guard cannot read
    /// one at all, since it judges before the pricer runs.
    #[tokio::test]
    async fn a_quote_guard_reading_an_undeclared_diagnostic_fails_its_build() {
        let mut kinds = Kinds::default();
        kinds.insert_guard(
            "watch",
            quote_guard_fn(|_: Empty, ctx: &mut BuildCtx| {
                let _ = ctx.read_diagnostic("fundng")?;
                Ok(AllowAll)
            }),
        );
        let mut ctx = BuildCtx::new(crate::testing::pair())
            .with_pricer_diagnostics(vec!["funding".to_owned()]);
        let err = format!(
            "{:#}",
            kinds
                .build_guard(&stanza("watch"), &mut ctx)
                .await
                .unwrap_err()
        );
        assert!(err.contains("`fundng`") && err.contains("funding"), "{err}");

        let mut kinds = Kinds::default();
        kinds.insert_guard(
            "m",
            market_guard_fn(|_: Empty, ctx: &mut BuildCtx| {
                let _ = ctx.read_diagnostic("funding")?;
                Ok(PassAll)
            }),
        );
        let mut ctx = BuildCtx::new(crate::testing::pair());
        let err = format!(
            "{:#}",
            kinds.build_guard(&stanza("m"), &mut ctx).await.unwrap_err()
        );
        assert!(err.contains("market guard"), "{err}");
    }

    /// The registry knows which side a kind judges, so the lane can build market guards
    /// before the composite and quote guards after the pricer.
    #[tokio::test]
    async fn a_guard_builds_as_its_side() {
        let mut kinds = Kinds::default();
        kinds.insert_guard(
            "m",
            market_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(PassAll)),
        );
        kinds.insert_guard(
            "q",
            quote_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(AllowAll)),
        );
        assert_eq!(kinds.guard_side("m"), Some(GuardSide::Market));
        assert_eq!(kinds.guard_side("q"), Some(GuardSide::Quote));
        assert_eq!(kinds.guard_side("x"), None);
        let mut ctx = BuildCtx::new(crate::testing::pair());
        assert!(matches!(
            kinds.build_guard(&stanza("m"), &mut ctx).await.unwrap(),
            BuiltGuard::Market(_)
        ));
        let mut ctx = BuildCtx::new(crate::testing::pair()).with_pricer_diagnostics(Vec::new());
        assert!(matches!(
            kinds.build_guard(&stanza("q"), &mut ctx).await.unwrap(),
            BuiltGuard::Quote(_)
        ));
    }

    #[derive(serde::Deserialize)]
    struct PanicCfg {
        at: String,
    }

    /// Panics in whichever method its stanza's `at` names, as a factory's `unwrap()` would.
    struct Panics;

    impl Factory for Panics {
        type Config = PanicCfg;
        type Pricer = P;

        fn validate(&self, cfg: &PanicCfg, _: &PairShape) -> Result<()> {
            assert_ne!(cfg.at, "validate", "validate blew up");
            Ok(())
        }

        fn build<'a>(&'a self, cfg: &'a PanicCfg, _: &'a mut BuildCtx) -> BoxFuture<'a, Result<P>> {
            Box::pin(async move {
                assert_ne!(cfg.at, "build", "build blew up");
                Ok(P)
            })
        }
    }

    /// A factory is the binary's code, so it can panic: `Kinds` turns that into the `Err` a
    /// failed build or a refused stanza already is, naming the kind and the panic.
    #[tokio::test]
    async fn a_factory_that_panics_is_an_error_naming_its_kind() {
        let mut kinds = Kinds::default();
        kinds.insert("fragile", Arc::new(Panics));
        let pair = crate::testing::pair();
        let stanza = |at: &str| -> toml::Table {
            toml::from_str(&format!("kind = \"fragile\"\nat = \"{at}\"")).unwrap()
        };

        let err = kinds
            .build(
                "fragile",
                &stanza("build"),
                &mut BuildCtx::new(pair.clone()),
            )
            .await
            .err()
            .expect("a panicking build is an Err");
        let err = format!("{err:#}");
        assert!(
            err.contains("`fragile` panicked") && err.contains("build blew up"),
            "{err}"
        );

        let source = PricingSpec {
            kind: "fragile".to_owned(),
            config: stanza("validate"),
        };
        let err = format!("{:#}", kinds.validate(&[(pair, &source, &[])]).unwrap_err());
        assert!(
            err.contains("`fragile` panicked") && err.contains("validate blew up"),
            "{err}"
        );
    }

    /// Guard kinds declare their form fields and help as pricing kinds do, so the backoffice
    /// can edit a guard stanza; a closure-registered kind declares none.
    #[test]
    fn guard_kinds_declare_fields_and_help_like_pricers() {
        #[derive(Clone, serde::Deserialize)]
        struct Capped {
            #[allow(dead_code)]
            max_share: f64,
        }
        struct CappedFactory;
        impl QuoteGuardFactory for CappedFactory {
            type Config = Capped;
            type Guard = AllowAll;
            fn fields(&self) -> &'static [FormField] {
                const FIELDS: &[FormField] = &[FormField::number("max_share", "0.8", "")];
                FIELDS
            }
            fn form_help(&self) -> &'static str {
                "<p>cap</p>"
            }
            fn build<'a>(
                &'a self,
                _: &'a Capped,
                _: &'a mut BuildCtx,
            ) -> BoxFuture<'a, Result<AllowAll>> {
                Box::pin(std::future::ready(Ok(AllowAll)))
            }
        }
        let mut kinds = Kinds::default();
        kinds.insert_guard("cap", quote_guard_factory(CappedFactory));
        kinds.insert_guard(
            "plain",
            quote_guard_fn(|_: Empty, _: &mut BuildCtx| Ok(AllowAll)),
        );
        assert_eq!(kinds.guard_kinds(), vec!["cap", "plain"]);
        assert_eq!(
            kinds.guard_fields("cap").map(|f| f[0].name),
            Some("max_share")
        );
        assert_eq!(kinds.guard_form_help("cap"), Some("<p>cap</p>"));
        assert!(kinds.guard_fields("plain").is_some_and(|f| f.is_empty()));
        assert_eq!(kinds.guard_fields("nobody"), None);
    }
}
