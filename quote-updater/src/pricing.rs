//! Pricing: how a lane turns the market into the `[delta, mid]` it publishes.
//!
//! A [`Pricer`] is built once per lane configuration and asked for a price on every tick.
//! The core wraps it: before it, the freshness gate (a pair with `symbol`/`sources` is
//! refused `no_sample`/`stale` before any pricer runs); after it, a backstop on its output
//! and the pair's band. So a pricer only ever makes the updater more cautious: it can
//! withdraw a quote, never publish one the core would refuse.
//!
//! Everything a pricer sees is exact and in lane orientation, with helpers for the f64
//! world: [`Market::mid_f64`] reads the market the way a person says it (USDC per WETH,
//! whichever way the lane carries it), and [`Market::scaled_fraction`], [`Market::shift`]
//! and [`TickCtx::mid_from_f64`] turn f64 results back into the exact values the lane
//! publishes, inversion and scaling handled.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use ethrex_common::{Address, U256};

mod feed;
mod fixed;
mod inputs;
mod volatile;

pub use self::inputs::{
    Chain, History, Inventory, InventoryFeed, LandingFeed, LandingOutcome, LandingReport,
};
pub use self::{feed::Feed, fixed::Fixed, volatile::Volatile};
#[cfg(test)]
pub(crate) use self::{feed::FeedPricer, fixed::FixedPricer};

use crate::{
    feed::{PriceSample, parse_decimal_scaled},
    update::{Unusable, UnusableKind},
    volatile::{fraction_scaled, shifted_mid},
};

/// A scaled integer as the number it stands for: a `mid` or a `delta` at `decimals`
/// (`scaled_to_f64(delta, 18)` is a half-spread as a fraction). Goes through the decimal
/// text rather than `as_u128`, so a value above 2^128 is a number and not a panic. What
/// [`Market::half_spread`] and [`super::guard::CompositeSample::half_spread`] use; for a
/// guard reading a [`super::guard::SourceSample`]'s own `mid` and `delta`.
pub use crate::feed::scaled_to_f64;

/// One input of a pricing kind on the backoffice's pair form: the stanza key it writes, the
/// placeholder the empty input shows, and the hint under it. A kind that declares its fields
/// ([`Factory::fields`]) is editable from the page; one that declares none is edited in the
/// config file, and the page says so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormField {
    /// The stanza key the input writes, e.g. `half_spread`.
    pub name: &'static str,
    /// What an empty input shows, e.g. `0.0005`.
    pub placeholder: &'static str,
    /// One or two sentences under the input: what the key means and what blank means.
    pub hint: &'static str,
    /// Written into the stanza as a TOML number rather than a string, for a config type
    /// with `f64`/`u64` fields; the page refuses a value that is not one.
    pub number: bool,
}

impl FormField {
    /// A field whose value is written as a string, as a `const` so a kind declares its list
    /// as a `&'static [FormField]`.
    pub const fn new(name: &'static str, placeholder: &'static str, hint: &'static str) -> Self {
        FormField {
            name,
            placeholder,
            hint,
            number: false,
        }
    }

    /// A field whose value is written as a number (`half_spread = 0.0005`), for a config
    /// type that reads `f64` or integer fields.
    pub const fn number(name: &'static str, placeholder: &'static str, hint: &'static str) -> Self {
        FormField {
            name,
            placeholder,
            hint,
            number: true,
        }
    }
}

/// The future a [`Factory`] builds in: boxed and `Send`, so building needs no
/// `async-trait` crate and no dependency beyond this one.
pub use futures_util::future::BoxFuture;

/// Builds one kind of pricer from its `[pairs.pricing]` stanza. Register it with
/// [`crate::UpdaterBuilder::pricer`]; [`crate::UpdaterBuilder::pricer_fn`] wraps a closure in
/// one for the common case, and is the shorter spelling whenever the build needs no I/O and
/// the stanza needs no check against the pair.
///
/// The trait is for the other cases: a `validate` that reads the pair, or a build that awaits
/// something (a first reading, a connection) before the pricer can tick. The future is a
/// [`BoxFuture`], so the body is `Box::pin(async move { .. })`:
///
/// ```
/// use quote_updater::{eyre, prelude::*};
///
/// #[derive(serde::Deserialize)]
/// #[serde(deny_unknown_fields)]
/// struct Cfg {
///     half_spread: f64,
/// }
///
/// struct Fetched {
///     half_spread: f64,
/// }
///
/// impl Pricer for Fetched {
///     fn price(&mut self, tick: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
///         let market = tick.market()?;
///         Ok(PricerOutput::new(market.scaled_fraction(self.half_spread)?, market.mid))
///     }
/// }
///
/// struct FetchedFactory;
///
/// impl Factory for FetchedFactory {
///     type Config = Cfg;
///     type Pricer = Fetched;
///
///     fn validate(&self, cfg: &Cfg, _pair: &PairShape) -> eyre::Result<()> {
///         // The run prefixes the pair's label and the stanza, so say only what is wrong.
///         eyre::ensure!(cfg.half_spread < 0.05, "half_spread is 5% or more");
///         Ok(())
///     }
///
///     fn build<'a>(&'a self, cfg: &'a Cfg, ctx: &'a mut BuildCtx) -> BoxFuture<'a, eyre::Result<Fetched>> {
///         Box::pin(async move {
///             // Await the first reading here; `ctx.spawn` a refresher for the rest.
///             let _ = ctx.pair();
///             Ok(Fetched { half_spread: cfg.half_spread })
///         })
///     }
/// }
///
/// let updater = Updater::builder().pricer("fetched", FetchedFactory);
/// # drop(updater);
/// ```
pub trait Factory: Send + Sync + 'static {
    /// The stanza's keys other than `kind`, deserialized. Reload compares the stanza as
    /// written, so this needs no `PartialEq`.
    type Config: serde::de::DeserializeOwned + Send + Sync + 'static;
    /// The pricer this builds. A concrete type, so `build` returns it as it is and the
    /// core boxes it; a factory that picks its model at build time names `Box<dyn Pricer>`.
    type Pricer: Pricer;

    /// Checks a stanza against the pair it would price. Synchronous and without I/O: it
    /// runs for every pair before a startup or a reload touches anything, so an invalid
    /// stanza rejects the whole file.
    fn validate(&self, _cfg: &Self::Config, _pair: &PairShape) -> eyre::Result<()> {
        Ok(())
    }

    /// The stanza's keys as inputs on the backoffice's pair form, in the order shown. The
    /// page posts each as text and writes it into the stanza as a string, so a kind that
    /// declares fields reads its config as strings (the kinds this crate ships do). Empty, the default,
    /// means the stanza is edited in the file and the page says so.
    fn fields(&self) -> &'static [FormField] {
        &[]
    }

    /// HTML the pair form shows above this kind's fields: what the model does and how to
    /// tune it. Empty by default.
    fn form_help(&self) -> &'static str {
        ""
    }

    /// One line describing a stanza for the `--check` report and the backoffice's pair
    /// list, e.g. `mid 1.0001 delta 0.0002`. Empty by default, which shows the kind alone.
    fn summary(&self, _cfg: &Self::Config) -> String {
        String::new()
    }

    /// Builds the pricer, with I/O if it needs it, whenever the lane is built: at startup
    /// and on a reload that changes this pair. A failure, a panic, or taking longer than 30s
    /// keeps the old lane quoting and reports the reload as partial. The I/O is async: the
    /// 30s limit fires only when the future yields, so blocking in here stalls the startup
    /// or reload it is part of (`tokio::task::spawn_blocking` is the way out).
    fn build<'a>(
        &'a self,
        cfg: &'a Self::Config,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<Self::Pricer>>;
}

/// Builds one kind of market guard from its `[[pairs.guards]]` stanza. Register it with
/// [`crate::UpdaterBuilder::market_guard`]; [`crate::UpdaterBuilder::market_guard_fn`]
/// wraps a closure in one. Same shape as [`Factory`]: `validate` synchronous and before
/// anything is touched, `build` async and bounded by the build limit, plus `summary`, the
/// text `--check`'s `guards` column shows for this stanza.
pub trait MarketGuardFactory: Send + Sync + 'static {
    /// The stanza's keys other than `kind`, deserialized.
    type Config: serde::de::DeserializeOwned + Send + Sync + 'static;
    /// The guard this builds.
    type Guard: crate::guard::MarketGuard;

    /// Checks a stanza against the pair it would judge. Synchronous and without I/O.
    fn validate(&self, _cfg: &Self::Config, _pair: &PairShape) -> eyre::Result<()> {
        Ok(())
    }

    /// One short line for `--check`'s `guards` column, e.g. `max_spread 0.3%`. The kind's
    /// name is shown either way; this is the settings after it. Empty shows the name alone.
    fn summary(&self, _cfg: &Self::Config) -> String {
        String::new()
    }

    /// Builds the guard, with I/O if it needs it, whenever the lane is built. The market
    /// guards are built before the composite connects, so [`BuildCtx::read_diagnostic`]
    /// is not available here: a market guard judges before the pricer runs.
    fn build<'a>(
        &'a self,
        cfg: &'a Self::Config,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<Self::Guard>>;
}

/// Builds one kind of quote guard from its `[[pairs.guards]]` stanza. Register it with
/// [`crate::UpdaterBuilder::quote_guard`]; [`crate::UpdaterBuilder::quote_guard_fn`] wraps
/// a closure in one. As [`MarketGuardFactory`], and its `build` runs after the pricer's, so
/// it can bind the pricer's diagnostics with [`BuildCtx::read_diagnostic`].
pub trait QuoteGuardFactory: Send + Sync + 'static {
    /// The stanza's keys other than `kind`, deserialized.
    type Config: serde::de::DeserializeOwned + Send + Sync + 'static;
    /// The guard this builds.
    type Guard: crate::guard::QuoteGuard;

    /// Checks a stanza against the pair it would judge. Synchronous and without I/O.
    fn validate(&self, _cfg: &Self::Config, _pair: &PairShape) -> eyre::Result<()> {
        Ok(())
    }

    /// One short line for `--check`'s `guards` column; see [`MarketGuardFactory::summary`].
    fn summary(&self, _cfg: &Self::Config) -> String {
        String::new()
    }

    /// Builds the guard, with I/O if it needs it, whenever the lane is built, after the
    /// pricer.
    fn build<'a>(
        &'a self,
        cfg: &'a Self::Config,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<Self::Guard>>;
}

/// Every core reason a pricer cannot declare as its own: the label is the core's.
pub const RESERVED_REASONS: &[&str] = &[
    "no_sample",
    "stale",
    "out_of_band",
    "delta_overflow",
    "panic",
    "source_stale",
    "source_divergence",
];

/// What a pricer publishes: the half-spread and the mid, exact, in lane orientation and at
/// the pair's price decimals.
///
/// Built with [`PricerOutput::new`], never by a struct literal, which does not compile
/// outside this crate: a field added later must not break every binary that prices.
///
/// ```compile_fail,E0639
/// use quote_updater::{U256, pricing::PricerOutput};
/// let out = PricerOutput { delta: U256::zero(), mid: U256::one() };
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PricerOutput {
    /// The half-spread, as a fraction of the mid scaled by `10^price_decimals`.
    pub delta: U256,
    /// The mid, in lane orientation, scaled by `10^price_decimals`.
    pub mid: U256,
}

impl PricerOutput {
    /// A half-spread and a mid, both exact and in lane orientation.
    pub fn new(delta: U256, mid: U256) -> Self {
        PricerOutput { delta, mid }
    }
}

/// A pricing model: built once per lane configuration, asked for a price every tick.
pub trait Pricer: Send + 'static {
    /// Every tick. Never blocks and never does I/O: read what a task started with
    /// [`BuildCtx::spawn`] prepared. `out` takes the diagnostics this pricer declared.
    fn price(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal>;

    /// What `--check` prints for this pair, held to the core's bounds as every tick is.
    /// Defaults to [`Pricer::price`]. A refusal here, the core's included, is a failed
    /// check (exit 1), so a pricer whose input is not there under `--check`
    /// previews without it and says so with [`Diagnostics::note`]: the σ history is empty
    /// (the check does not wait the minute it needs), a task started with
    /// [`BuildCtx::spawn`] has not run, and a fetch of a build's own has not happened.
    /// The volatile model previews at σ = 0; a pricer that reads σ does the same:
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use quote_updater::prelude::*;
    ///
    /// struct Vol {
    ///     history: History,
    ///     warming: RefusalHandle,
    /// }
    ///
    /// impl Vol {
    ///     fn price_at(&self, tick: &TickCtx, sigma: f64) -> Result<PricerOutput, Refusal> {
    ///         let market = tick.market()?;
    ///         Ok(PricerOutput::new(market.scaled_fraction(0.0005 + sigma)?, market.mid))
    ///     }
    /// }
    ///
    /// impl Pricer for Vol {
    ///     fn price(&mut self, tick: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
    ///         let sigma = self
    ///             .history
    ///             .volatility(tick.now(), Duration::from_secs(300))
    ///             .map_err(|covered| Refusal::new(&self.warming, format!("σ has {covered:?}")))?;
    ///         self.price_at(tick, sigma)
    ///     }
    ///
    ///     fn preview(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
    ///         out.note("previewed at σ = 0; the run prices from a minute of history");
    ///         self.price_at(tick, 0.0)
    ///     }
    /// }
    /// ```
    fn preview(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        self.price(tick, out)
    }
}

/// A boxed pricer prices as the one inside it, so a [`Factory`] that picks its model at
/// build time names `Box<dyn Pricer>` as what it builds.
impl<P: Pricer + ?Sized> Pricer for Box<P> {
    fn price(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        (**self).price(tick, out)
    }

    fn preview(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        (**self).preview(tick, out)
    }
}

/// The pair a pricer prices, as its build and every tick see it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PairShape {
    /// The pair's label, e.g. `WETH/USDC`, from the tokens' on-chain `symbol()`.
    pub label: String,
    /// `[base, quote]` as configured: market orientation.
    pub tokens: (Address, Address),
    /// `keccak256(token0 ++ token1)` over the address-sorted tokens.
    pub lane: U256,
    /// Whether the lane carries the price the other way up from the market (it does when
    /// sorting the tokens flipped them).
    pub inverted: bool,
    /// The decimals the published mid and delta are scaled by.
    pub price_decimals: u32,
    /// The PropAMM the lane quotes on.
    pub target: Address,
    /// How many venues the pair's `symbol`/`sources` stream from: 0 for a pair with no
    /// market, which a kind that reads one refuses in its `validate`.
    pub sources: usize,
    /// The pair's `min_mid`, in lane orientation, which the core holds every published mid
    /// to; a kind whose mid is a setting checks it here, so a mistake fails the file instead
    /// of every tick.
    pub min_mid: Option<U256>,
    /// The pair's `max_mid`, likewise.
    pub max_mid: Option<U256>,
}

/// The pair's reference market this tick: the composite of its `symbol`/`sources`, fresh.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Market {
    /// The composite mid, exact, in lane orientation.
    pub mid: U256,
    /// The book's half-spread as a fraction of the mid, exact.
    pub delta: U256,
    sampled_at: Instant,
    inverted: bool,
    price_decimals: u32,
}

impl Market {
    pub(crate) fn from_sample(sample: &PriceSample, pair: &PairShape) -> Market {
        Market {
            mid: sample.mid,
            delta: sample.delta,
            sampled_at: sample.at,
            inverted: pair.inverted,
            price_decimals: pair.price_decimals,
        }
    }

    /// The mid in market orientation (e.g. USDC per WETH), whatever the lane's.
    pub fn mid_f64(&self) -> f64 {
        let lane = scaled_to_f64(self.mid, self.price_decimals);
        if self.inverted { 1.0 / lane } else { lane }
    }

    /// The book's half-spread as a fraction of the mid (0.0001 = 1 bp).
    pub fn half_spread(&self) -> f64 {
        scaled_to_f64(self.delta, self.price_decimals)
    }

    /// A half-spread fraction (0.0005 = 5 bps) as the delta this pair publishes.
    pub fn scaled_fraction(&self, fraction: f64) -> Result<U256, Refusal> {
        fraction_scaled(fraction, self.price_decimals)
            .map_err(|err| Refusal::core(UnusableKind::DeltaOverflow, format!("{err:#}")))
    }

    /// The mid moved by `skew` in market orientation: `+0.01` publishes a mid 1% below the
    /// market's (the volatile model's tilt direction), returned in lane orientation.
    pub fn shift(&self, skew: f64) -> Result<U256, Refusal> {
        shifted_mid(self.mid, skew, self.inverted, self.price_decimals)
            .map_err(|err| Refusal::core(UnusableKind::DeltaOverflow, format!("{err:#}")))
    }

    /// How old this market sample is at `now`.
    pub fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.sampled_at)
    }
}

/// One tick, as a pricer sees it.
#[non_exhaustive]
pub struct TickCtx<'a> {
    now: Instant,
    market: Option<&'a Market>,
    pair: &'a PairShape,
}

impl<'a> TickCtx<'a> {
    pub(crate) fn new(now: Instant, market: Option<&'a Market>, pair: &'a PairShape) -> Self {
        TickCtx { now, market, pair }
    }

    /// When this tick started.
    pub fn now(&self) -> Instant {
        self.now
    }

    /// The pair being priced.
    pub fn pair(&self) -> &PairShape {
        self.pair
    }

    /// The pair's reference market, or the core's `no_sample` refusal for a pair that
    /// configures none, so `tick.market()?` is the whole ceremony.
    pub fn market(&self) -> Result<&'a Market, Refusal> {
        self.market.ok_or_else(|| {
            Refusal::core(
                UnusableKind::NoSample,
                "this pricing kind reads the market, so the pair needs a symbol or sources",
            )
        })
    }

    /// The pair's reference market, if it configures one.
    pub fn market_if_any(&self) -> Option<&'a Market> {
        self.market
    }

    /// An absolute mid computed in market orientation (a cross rate, a model's output) as
    /// the exact value this lane publishes, inverted for an inverted lane. Every digit the
    /// f64 carries is kept, whatever the pair's price decimals, and truncated at them like
    /// every other price; a mid below one unit at that scale is refused here.
    pub fn mid_from_f64(&self, market_mid: f64) -> Result<U256, Refusal> {
        if !(market_mid.is_finite() && market_mid > 0.0) {
            return Err(Refusal::core(
                UnusableKind::OutOfBand,
                format!("computed mid {market_mid} is not a positive finite number"),
            ));
        }
        let lane = if self.pair.inverted {
            1.0 / market_mid
        } else {
            market_mid
        };
        // Not `fraction_scaled`: its fixed 15 fractional digits suit a spread of a few bps,
        // but keep five significant digits of a lane worth 1e-10 at 18 dp and none of one
        // worth 1e-17 at 30. The shortest round-trip form (`{:e}`) is every digit the f64
        // has, and `parse_decimal_scaled` places its exponent exactly.
        let decimals = self.pair.price_decimals;
        let mid = parse_decimal_scaled(&format!("{lane:e}"), decimals).map_err(|err| {
            Refusal::core(
                UnusableKind::OutOfBand,
                format!("computed mid {market_mid}: {err:#}"),
            )
        })?;
        if mid.is_zero() {
            return Err(Refusal::core(
                UnusableKind::OutOfBand,
                format!(
                    "computed mid {market_mid} is below one unit at {decimals} decimals in \
                     this lane's orientation; refusing to publish it"
                ),
            ));
        }
        Ok(mid)
    }
}

/// Why a pricer will not price this tick: the quote is withdrawn and the reason counted in
/// `quote_updater_price_unusable_total{reason}`.
#[derive(Clone, Debug, PartialEq)]
pub struct Refusal {
    reason: Reason,
    message: String,
}

#[derive(Clone, Debug, PartialEq)]
enum Reason {
    Core(UnusableKind),
    Declared { index: usize, name: Arc<str> },
}

impl Refusal {
    /// A refusal under a reason this pricer declared with [`BuildCtx::refusal`].
    pub fn new(handle: &RefusalHandle, message: impl Into<String>) -> Refusal {
        Refusal {
            reason: Reason::Declared {
                index: handle.index,
                name: Arc::clone(&handle.name),
            },
            message: message.into(),
        }
    }

    pub(crate) fn core(kind: UnusableKind, message: impl Into<String>) -> Refusal {
        Refusal {
            reason: Reason::Core(kind),
            message: message.into(),
        }
    }

    /// The reason label: a declared handle's name, or the core's (`no_sample`, ...).
    pub fn reason(&self) -> &str {
        match &self.reason {
            Reason::Core(kind) => kind.as_str(),
            Reason::Declared { name, .. } => name,
        }
    }

    /// The text the withdrawn quote is logged with.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn core_kind(&self) -> Option<UnusableKind> {
        match self.reason {
            Reason::Core(kind) => Some(kind),
            Reason::Declared { .. } => None,
        }
    }

    pub(crate) fn declared_index(&self) -> Option<usize> {
        match self.reason {
            Reason::Declared { index, .. } => Some(index),
            Reason::Core(_) => None,
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Refusal {}

/// A withdraw reason a pricer declared in its build: the label is bound once there.
#[derive(Clone, Debug)]
pub struct RefusalHandle {
    index: usize,
    name: Arc<str>,
}

/// A number a pricer declared in its build, reported every tick as
/// `quote_updater_diagnostic{pair,kind,name}`.
#[derive(Clone, Debug)]
pub struct DiagHandle {
    index: usize,
}

/// This tick's diagnostics: what the pricer set, read by the core after the tick.
#[derive(Debug)]
pub struct Diagnostics {
    values: Vec<Option<f64>>,
    /// A line `--check` prints under this pair's row; see [`Diagnostics::note`].
    pub(crate) note: Option<String>,
}

impl Diagnostics {
    pub(crate) fn new(len: usize) -> Diagnostics {
        Diagnostics {
            values: vec![None; len],
            note: None,
        }
    }

    /// Sets a declared diagnostic's value for this tick.
    pub fn set(&mut self, handle: &DiagHandle, value: f64) {
        self.set_at(handle.index, value);
    }

    /// [`Self::set`] by slot, for a test that seeds what a pricer would have set.
    pub(crate) fn set_at(&mut self, index: usize, value: f64) {
        if let Some(slot) = self.values.get_mut(index) {
            *slot = Some(value);
        }
    }

    /// What this tick set a diagnostic to, if anything.
    pub fn get(&self, handle: &DiagHandle) -> Option<f64> {
        self.value_at(handle.index)
    }

    /// The value at a declared index, for a quote guard's [`crate::guard::ReadHandle`].
    pub(crate) fn value_at(&self, index: usize) -> Option<f64> {
        self.values.get(index).copied().flatten()
    }

    /// A line `--check` prints under this pair's row, for a [`Pricer::preview`] that could
    /// not price the way the run will: the volatile model says its σ is 0 because `--check`
    /// does not wait for the history it needs, and a pricer whose input arrives later says
    /// so the same way. The run's ticks ignore it.
    pub fn note(&mut self, text: impl Into<String>) {
        self.note = Some(text.into());
    }

    pub(crate) fn values(&self) -> &[Option<f64>] {
        &self.values
    }
}

/// The core's own bounds on any pricer's output: a spread below one whole unit that fits
/// the 216 bits its storage slot leaves it, and a nonzero mid. Every kind goes through it;
/// the pair's band (`min_mid`/`max_mid`) is the operator's bound
/// on the mid and is checked after it. Severity only goes up: this can withdraw a quote,
/// never publish one.
pub(crate) fn backstop(
    out: PricerOutput,
    market: Option<&Market>,
    pair: &PairShape,
) -> Result<(), Unusable> {
    let unit = crate::config::spread_scale(pair.price_decimals);
    if out.delta >= unit {
        return Err(Unusable::new(
            UnusableKind::DeltaOverflow,
            format!(
                "the pricer's half-spread {} is not below one whole unit ({unit}); refusing \
                 to publish a spread that leaves the taker nothing",
                out.delta
            ),
        ));
    }
    crate::ensure_fits_216_bits(out.delta)
        .map_err(|err| Unusable::new(UnusableKind::DeltaOverflow, format!("{err:#}")))?;
    if out.mid.is_zero() {
        return Err(Unusable::new(
            UnusableKind::OutOfBand,
            "the pricer's mid is zero; refusing to publish it".to_owned(),
        ));
    }
    let _ = market;
    Ok(())
}

/// A secret a `[pairs.pricing]` stanza names rather than holds: written `{ env = "NAME" }`,
/// read from the environment when the lane is built. The config file is recorded to
/// Postgres and served on the tailnet, so a key typed into it is a key published; this is
/// how a custom pricer takes one instead. `Debug` prints the variable's name, never its
/// value, and a value typed in its place is refused without being repeated.
#[derive(Clone, PartialEq, Eq)]
pub struct EnvSecret {
    env: String,
}

/// By hand rather than derived: serde's own refusal of a string here is `invalid type:
/// string "sk-live-…", expected struct EnvSecret`, which prints the very secret that was
/// typed where its variable's name belongs into the startup error, the reload refusal and
/// `--check`. Every shape but the table is refused with one message that echoes nothing.
impl<'de> serde::Deserialize<'de> for EnvSecret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor, value::MapAccessDeserializer};

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Named {
            env: String,
        }

        fn inline<E: Error>() -> E {
            E::custom(
                "a secret is written `{ env = \"NAME\" }`, naming the environment variable \
                 that holds it, never as its value: the config is recorded and served, so a \
                 value typed here would be published with it (it is not repeated here)",
            )
        }

        struct Secret;

        impl<'de> Visitor<'de> for Secret {
            type Value = EnvSecret;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("`{ env = \"NAME\" }`")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<EnvSecret, A::Error> {
                let Named { env } =
                    <Named as serde::Deserialize>::deserialize(MapAccessDeserializer::new(map))?;
                Ok(EnvSecret { env })
            }

            fn visit_str<E: Error>(self, _: &str) -> Result<EnvSecret, E> {
                Err(inline())
            }

            fn visit_i64<E: Error>(self, _: i64) -> Result<EnvSecret, E> {
                Err(inline())
            }

            fn visit_u64<E: Error>(self, _: u64) -> Result<EnvSecret, E> {
                Err(inline())
            }

            fn visit_f64<E: Error>(self, _: f64) -> Result<EnvSecret, E> {
                Err(inline())
            }

            fn visit_bool<E: Error>(self, _: bool) -> Result<EnvSecret, E> {
                Err(inline())
            }
        }

        deserializer.deserialize_any(Secret)
    }
}

impl EnvSecret {
    /// The variable this secret is read from.
    pub fn name(&self) -> &str {
        &self.env
    }

    /// The secret's value, or why there is none. Under systemd the variable comes from the
    /// unit's `EnvironmentFile`, which a reload does not re-read: a new secret needs a
    /// restart, as a new updater key does.
    pub fn resolve(&self) -> eyre::Result<String> {
        std::env::var(&self.env).map_err(|_| {
            eyre::eyre!(
                "{} is not set: a [pairs.pricing] secret is read from the environment when the \
                 lane is built (under systemd, from the unit's EnvironmentFile)",
                self.env
            )
        })
    }
}

impl std::fmt::Debug for EnvSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EnvSecret({})", self.env)
    }
}

/// A background task a pricer starts in its build.
pub(crate) type LaneTask = Pin<Box<dyn Future<Output = ()> + Send>>;

/// What a pricer's factory gets while it builds: the pair, the inputs it may read (the
/// chain, the market's σ history, the vault inventory), and the places to declare what it
/// will report.
pub struct BuildCtx {
    pair: PairShape,
    diagnostics: Vec<String>,
    refusals: Vec<Arc<str>>,
    tasks: Vec<LaneTask>,
    /// The run's chain; `None` under a test build context, which has none.
    chain: Option<Chain>,
    /// The reference market's history; `None` for a pair without a market, and under a
    /// test build context that seeded none.
    history: Option<History>,
    /// A reading a test seeded, answered in place of a read through the chain.
    inventory: Option<InventoryFeed>,
    /// The process's inventory readers, so two lanes on one vault share a poller; `None`
    /// (a registry of this build's own) under a test build context.
    readers: Option<crate::vault::InventoryReaders>,
    /// The run's HTTP client, shared with the polled venues; `None` under a test build
    /// context, which hands out a default one.
    http: Option<Arc<reqwest::Client>>,
    /// The lane's landings: the sender goes into the lane's `ValueSource`, where the quote
    /// loop reports each block's read-back; every feed handed out here reads it.
    landings: (
        tokio::sync::watch::Sender<Option<LandingReport>>,
        tokio::sync::watch::Receiver<Option<LandingReport>>,
    ),
    /// The diagnostics the lane's pricer declared, for a quote guard's build to read by
    /// name; `None` for a pricer's or a market guard's build, which run before there is a
    /// pricer to read.
    pricer_diagnostics: Option<Vec<String>>,
    /// The lane's latch. A fresh one under a test build context, so a test can read what a
    /// component tripped.
    latch: crate::guard::Latch,
}

impl BuildCtx {
    pub(crate) fn new(pair: PairShape) -> BuildCtx {
        BuildCtx {
            pair,
            diagnostics: Vec::new(),
            refusals: Vec::new(),
            tasks: Vec::new(),
            chain: None,
            history: None,
            inventory: None,
            readers: None,
            http: None,
            landings: tokio::sync::watch::channel(None),
            pricer_diagnostics: None,
            latch: crate::guard::Latch::new(),
        }
    }

    /// The run's HTTP client, for the run.
    pub(crate) fn with_http(mut self, http: Arc<reqwest::Client>) -> BuildCtx {
        self.http = Some(http);
        self
    }

    /// The diagnostics the lane's pricer declared, for a quote guard's build; empty for a
    /// build with no pricer before it.
    pub(crate) fn pricer_diagnostic_names(&self) -> &[String] {
        self.pricer_diagnostics.as_deref().unwrap_or(&[])
    }

    /// The run's client as the context holds it, for a test of the wiring.
    #[cfg(test)]
    pub(crate) fn http_shared(&self) -> Option<Arc<reqwest::Client>> {
        self.http.clone()
    }

    /// An HTTP client, for a pricer that fetches something (a reference price, a
    /// parameter file) in `build` or from a task started with [`BuildCtx::spawn`]: the
    /// run's one `reqwest::Client`, shared with the polled venues, so every lane draws on
    /// one connection pool. Its requests time out after 10s unless one sets a timeout of
    /// its own. Cheap to clone and to keep. The type is `quote_updater::reqwest`'s, the
    /// version the client was built with; a test build context hands out a default one.
    pub fn http(&self) -> reqwest::Client {
        self.http
            .as_ref()
            .map_or_else(reqwest::Client::default, |shared| (**shared).clone())
    }

    /// The sender the lane's quote loop reports each block's landing through.
    pub(crate) fn landings_sender(&self) -> tokio::sync::watch::Sender<Option<LandingReport>> {
        self.landings.0.clone()
    }

    /// Reports `seeded` in order, so the feed answers the last: a test build context's.
    pub(crate) fn with_landings(self, seeded: Vec<LandingReport>) -> BuildCtx {
        for report in seeded {
            self.landings.0.send_replace(Some(report));
        }
        self
    }

    /// The process's inventory readers, for the run.
    pub(crate) fn with_readers(mut self, readers: crate::vault::InventoryReaders) -> BuildCtx {
        self.readers = Some(readers);
        self
    }

    /// The lane's latch, for the run: the one its composite and quote loop share.
    pub(crate) fn with_latch(mut self, latch: crate::guard::Latch) -> BuildCtx {
        self.latch = latch;
        self
    }

    /// The lane's latch, for a component to trip its own lane from its code or a task of
    /// its own (`cause = external`): a model that sees its inputs go bad, an oracle that
    /// disagrees for too long. Not a kill switch: a reload re-arms every latch, so
    /// "stop until I say so" is a halt in the config file, never a trip. Under a test build
    /// context this is a latch of the context's own, readable back through the same call.
    pub fn latch(&self) -> crate::guard::Latch {
        self.latch.clone()
    }

    /// For a quote guard's build: the names the lane's pricer declared.
    pub(crate) fn with_pricer_diagnostics(mut self, names: Vec<String>) -> BuildCtx {
        self.pricer_diagnostics = Some(names);
        self
    }

    pub(crate) fn with_chain(mut self, chain: Chain) -> BuildCtx {
        self.chain = Some(chain);
        self
    }

    pub(crate) fn with_history(mut self, history: History) -> BuildCtx {
        self.history = Some(history);
        self
    }

    pub(crate) fn with_inventory(mut self, inventory: InventoryFeed) -> BuildCtx {
        self.inventory = Some(inventory);
        self
    }

    /// The pair being built.
    pub fn pair(&self) -> &PairShape {
        &self.pair
    }

    /// The chain, for a read in `build` or from a task started with [`BuildCtx::spawn`]:
    /// `eth_call`s bounded by the updater's RPC timeout. The run and `--check` always have
    /// one; a test build context ([`crate::testing::build_ctx`]) has none, so a pricer that
    /// reads the chain is tested under a runtime against a mock.
    pub fn chain(&self) -> eyre::Result<Chain> {
        self.chain.clone().ok_or_else(|| {
            eyre::eyre!(
                "{}: no chain in this build context (a test's): a pricer that reads the chain \
                 is tested under a runtime against a mock",
                self.pair.label
            )
        })
    }

    /// The reference market's σ history, for a pricer with a volatility term of its own:
    /// the samples the market's composite records from the moment the lane first runs, kept
    /// across reloads. A pair with no `symbol`/`sources` has none, and neither has a test
    /// build context that seeded none (`testing::Inputs::history`). Under `--check` it is
    /// empty: the check does not wait the minute σ needs, as the volatile model's row says.
    /// Under `--check` the history is empty and a refusal in `preview` fails the check: see
    /// [`Pricer::preview`] for the two lines that preview at σ = 0.
    pub fn history(&self) -> eyre::Result<History> {
        self.history.clone().ok_or_else(|| {
            eyre::eyre!(
                "{}: no market history: this pricing kind reads σ, so the pair needs a symbol \
                 or sources (a test seeds one with testing::Inputs)",
                self.pair.label
            )
        })
    }

    /// The pair's vault inventory: `target.vaultFor(base, quote)` and both balances, read
    /// now so that a target or token the run would refuse fails this build, then refreshed
    /// every 12s by a task that stops when the feed is dropped. Async because of that first
    /// read, which makes reading the vault the [`Factory`] path rather than `pricer_fn`'s.
    /// A test build context answers the reading `testing::Inputs::inventory` seeded, and
    /// never refreshes it.
    ///
    /// Not an `async fn`: that would hold `&self` across the read, and a build's future
    /// must be `Send` while a `BuildCtx` is not `Sync` (it holds the tasks its pricer
    /// spawned). The future returned borrows nothing, so `ctx.inventory().await` compiles
    /// inside any `Box::pin(async move { .. })`.
    pub fn inventory(&self) -> impl Future<Output = eyre::Result<InventoryFeed>> + Send + 'static {
        let seeded = self.inventory.clone();
        let chain = self.chain.clone();
        let readers = self.readers.clone().unwrap_or_default();
        let (base, quote) = self.pair.tokens;
        let label = self.pair.label.clone();
        async move {
            if let Some(seeded) = seeded {
                return Ok(seeded);
            }
            let Some(chain) = chain else {
                eyre::bail!(
                    "{label}: no chain and no seeded inventory in this build context (a \
                     test's): seed a reading with testing::build_ctx_with(pair, Inputs {{ \
                     inventory: Some(..), .. }})"
                );
            };
            crate::pair::connect_inventory(
                &readers,
                &chain.client,
                chain.target,
                base,
                quote,
                &label,
            )
            .await
        }
    }

    /// This lane's landings, block by block: after each block passes the run reads the
    /// lane back and reports whether the update it quoted for that block landed (the
    /// read-back `quote_updater_landings_total` counts), and the report reaches this feed
    /// then, so a strategy can adapt to what happened to its own quotes. Empty until the
    /// first block passes, and a block behind: the tick quoting block `n` sees at most
    /// block `n - 1`'s. A test build context answers what `testing::Inputs::landings`
    /// seeded.
    pub fn landings(&self) -> LandingFeed {
        LandingFeed(self.landings.1.clone())
    }

    /// Declares a diagnostic: a number [`Diagnostics::set`] reports every tick. Names are
    /// `snake_case`, unique per pricer.
    pub fn diagnostic(&mut self, name: &str) -> eyre::Result<DiagHandle> {
        check_name("diagnostic", name)?;
        eyre::ensure!(
            !self.diagnostics.iter().any(|n| n == name),
            "diagnostic `{name}` is declared twice"
        );
        self.diagnostics.push(name.to_owned());
        Ok(DiagHandle {
            index: self.diagnostics.len() - 1,
        })
    }

    /// Declares a withdraw reason for [`Refusal::new`]. Names are `snake_case`, unique per
    /// pricer, and none of the core's ([`RESERVED_REASONS`]).
    pub fn refusal(&mut self, name: &str) -> eyre::Result<RefusalHandle> {
        check_name("refusal", name)?;
        eyre::ensure!(
            !RESERVED_REASONS.contains(&name),
            "refusal `{name}` is one of the core's reasons; name this pricer's own"
        );
        eyre::ensure!(
            !self.refusals.iter().any(|n| &**n == name),
            "refusal `{name}` is declared twice"
        );
        let name: Arc<str> = Arc::from(name);
        self.refusals.push(Arc::clone(&name));
        Ok(RefusalHandle {
            index: self.refusals.len() - 1,
            name,
        })
    }

    /// Binds a diagnostic the lane's pricer declared, for a quote guard to read every tick
    /// through [`crate::guard::Candidate::get`]. Fails the build when the pricer declared
    /// no such name, naming what it did declare, and in a market guard's build, which runs
    /// before the pricer.
    pub fn read_diagnostic(&mut self, name: &str) -> eyre::Result<crate::guard::ReadHandle> {
        let Some(declared) = &self.pricer_diagnostics else {
            eyre::bail!(
                "{}: a market guard cannot read the pricer's diagnostic `{name}`: it judges \
                 the market before the pricer runs; a quote guard can",
                self.pair.label
            );
        };
        match declared.iter().position(|n| n == name) {
            Some(index) => Ok(crate::guard::ReadHandle { index }),
            None => eyre::bail!(
                "{}: the pricer declared no diagnostic `{name}` (declared: {})",
                self.pair.label,
                if declared.is_empty() {
                    "none".to_owned()
                } else {
                    declared.join(", ")
                }
            ),
        }
    }

    /// Starts `task` once the build has succeeded, and stops it with the lane: a build that
    /// fails starts nothing, and nothing runs while the build does.
    pub fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks.push(Box::pin(task));
    }

    pub(crate) fn diagnostic_names(&self) -> &[String] {
        &self.diagnostics
    }

    pub(crate) fn refusal_names(&self) -> &[Arc<str>] {
        &self.refusals
    }

    pub(crate) fn take_tasks(&mut self) -> Vec<LaneTask> {
        std::mem::take(&mut self.tasks)
    }
}

/// A declared name: `snake_case` (a lowercase letter, then lowercase letters, digits and
/// `_`), at most 64 bytes, because it becomes a metric label.
fn check_name(what: &str, name: &str) -> eyre::Result<()> {
    let ok = name.len() <= 64
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    eyre::ensure!(
        ok,
        "{what} name `{name}` must be snake_case (a lowercase letter, then lowercase \
         letters, digits and `_`) and at most 64 bytes: it becomes a metric label"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::PriceSample;

    /// A test build context answers the landings a test seeded, latest last, and a run's
    /// feed hands out each report the lane's loop sends until the lane is gone.
    #[tokio::test]
    async fn a_landing_feed_answers_what_was_seeded_and_then_each_report_sent() {
        use crate::testing::{Inputs, build_ctx_with, landing};
        let seeded = build_ctx_with(
            &pair(false, 6),
            Inputs {
                landings: vec![
                    landing(10, LandingOutcome::Missed),
                    landing(11, LandingOutcome::Landed),
                ],
                ..Inputs::default()
            },
        );
        let latest = seeded.landings().latest().expect("seeded");
        assert_eq!((latest.block, latest.landing), (11, LandingOutcome::Landed));

        let ctx = BuildCtx::new(pair(false, 6));
        let mut feed = ctx.landings();
        assert_eq!(feed.latest(), None);
        let sender = ctx.landings_sender();
        drop(ctx);
        let report = landing(12, LandingOutcome::NotQuoted);
        sender.send_replace(Some(report));
        assert_eq!(feed.next().await, Some(report));
        assert_eq!(feed.latest(), Some(report));
        drop(sender);
        assert_eq!(feed.next().await, None, "the lane is gone");
    }

    fn pair(inverted: bool, price_decimals: u32) -> PairShape {
        PairShape {
            sources: 1,
            min_mid: None,
            max_mid: None,
            label: "WETH/USDC".into(),
            tokens: (Address::repeat_byte(1), Address::repeat_byte(2)),
            lane: U256::from(7),
            inverted,
            price_decimals,
            target: Address::repeat_byte(3),
        }
    }

    fn sample(mid: &str, delta: &str, decimals: u32) -> PriceSample {
        PriceSample {
            mid: crate::feed::parse_decimal_scaled(mid, decimals).unwrap(),
            delta: crate::feed::parse_decimal_scaled(delta, decimals).unwrap(),
            at: Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn the_market_speaks_market_orientation_whatever_the_lane() {
        // The lane carries USDC priced in WETH (inverted): 1/4000.
        let market = Market::from_sample(&sample("0.00025", "0.0001", 18), &pair(true, 18));
        assert!((market.mid_f64() - 4000.0).abs() < 1e-9);
        assert!((market.half_spread() - 0.0001).abs() < 1e-15);
    }

    #[test]
    fn a_fraction_and_a_shift_come_back_exact_and_in_lane_orientation() {
        let p = pair(false, 18);
        let market = Market::from_sample(&sample("4000", "0", 18), &p);
        assert_eq!(
            market.scaled_fraction(0.0005).unwrap(),
            U256::from(500_000_000_000_000u64)
        );
        // +1% skew lowers the market-orientation mid by 1%.
        assert_eq!(
            market.shift(0.01).unwrap(),
            crate::feed::parse_decimal_scaled("3960", 18).unwrap()
        );
    }

    #[test]
    fn mid_from_f64_inverts_for_an_inverted_lane() {
        let p = pair(true, 18);
        let tick = TickCtx::new(Instant::now(), None, &p);
        assert_eq!(
            tick.mid_from_f64(4000.0).unwrap(),
            crate::feed::parse_decimal_scaled("0.00025", 18).unwrap()
        );
        assert!(tick.mid_from_f64(0.0).is_err());
        assert!(tick.mid_from_f64(f64::NAN).is_err());
    }

    /// Every digit the f64 has, at any scale: a fixed 15 fractional digits kept five
    /// significant digits of a lane worth 1/3e10 at 18 dp (33333000, not 33333333) and
    /// none of one worth 1e-17 at 30 dp, which came back as a zero mid the backstop then
    /// blamed on the pricer.
    #[test]
    fn mid_from_f64_keeps_the_f64s_precision_at_any_scale() {
        let mid = |inverted: bool, decimals: u32, market_mid: f64| {
            let p = pair(inverted, decimals);
            TickCtx::new(Instant::now(), None, &p).mid_from_f64(market_mid)
        };
        assert_eq!(mid(true, 18, 3e10).unwrap(), U256::from(33_333_333u64));
        assert_eq!(mid(false, 30, 1e-17).unwrap(), U256::exp10(13));
        assert_eq!(mid(true, 30, 1e17).unwrap(), U256::exp10(13));
        // Truncated at the scale, like every other price: the shortest decimal of 4000.123
        // is exactly that, with no binary tail.
        assert_eq!(
            mid(false, 18, 4000.123).unwrap(),
            crate::feed::parse_decimal_scaled("4000.123", 18).unwrap()
        );
        assert_eq!(mid(false, 6, 1.0000019).unwrap(), U256::from(1_000_001u64));
        // Below one unit at the scale is the computation's refusal, said as such, not a zero
        // mid for the backstop to find.
        let refusal = mid(false, 6, 4e-7).unwrap_err();
        assert_eq!(refusal.reason(), "out_of_band");
        assert!(
            refusal.message().contains("below one unit"),
            "{}",
            refusal.message()
        );
    }

    #[test]
    fn a_pricer_without_a_market_is_refused_as_no_sample_naming_the_fix() {
        let p = pair(false, 18);
        let tick = TickCtx::new(Instant::now(), None, &p);
        let refusal = tick.market().unwrap_err();
        assert_eq!(refusal.reason(), "no_sample");
        assert!(
            refusal.message().contains("symbol or sources"),
            "{}",
            refusal.message()
        );
    }

    #[test]
    fn the_backstop_refuses_what_no_pricer_may_publish() {
        let p = pair(false, 18);
        let unit = crate::config::spread_scale(18);
        let market = Market::from_sample(&sample("4000", "0", 18), &p);
        let at = |delta: U256, mid: &str| {
            PricerOutput::new(delta, crate::feed::parse_decimal_scaled(mid, 18).unwrap())
        };
        let kind = |r: Result<(), crate::update::Unusable>| r.unwrap_err().kind();
        assert_eq!(
            kind(backstop(at(unit, "4000"), Some(&market), &p)),
            Some(UnusableKind::DeltaOverflow)
        );
        assert_eq!(
            kind(backstop(
                PricerOutput::new(U256::zero(), U256::zero()),
                Some(&market),
                &p
            )),
            Some(UnusableKind::OutOfBand)
        );
        // No bound on the mid against the market: the pair's band is the operator's bound, and
        // a kind is free to quote wherever its model says.
        backstop(at(U256::zero(), "6156"), Some(&market), &p)
            .expect("a far mid is the band's business");
        backstop(at(U256::from(1), "123"), None, &p).expect("no market: nothing to compare");
    }

    #[test]
    fn declared_names_are_unique_and_the_core_ones_are_reserved() {
        let mut ctx = BuildCtx::new(pair(false, 18));
        let a = ctx.refusal("too_wide").unwrap();
        assert!(ctx.refusal("too_wide").is_err(), "declared twice");
        for reserved in RESERVED_REASONS {
            assert!(ctx.refusal(reserved).is_err(), "{reserved} is the core's");
        }
        assert!(ctx.refusal("Not Snake").is_err());
        let refusal = Refusal::new(&a, "spread above 5%");
        assert_eq!(
            (refusal.reason(), refusal.message()),
            ("too_wide", "spread above 5%")
        );
        let d = ctx.diagnostic("funding").unwrap();
        assert!(ctx.diagnostic("funding").is_err());
        let mut out = Diagnostics::new(ctx.diagnostic_names().len());
        out.set(&d, 0.25);
        assert_eq!(out.get(&d), Some(0.25));
    }

    /// A custom pricer's preview note reaches `--check`'s row the way the volatile model's
    /// does: `preview_row` hands it back beside the price, whatever the kind.
    #[test]
    fn a_previews_note_reaches_the_check_row() {
        struct Warming;
        impl Pricer for Warming {
            fn price(
                &mut self,
                tick: &TickCtx,
                _: &mut Diagnostics,
            ) -> Result<PricerOutput, Refusal> {
                Ok(PricerOutput::new(U256::one(), tick.market()?.mid))
            }
            fn preview(
                &mut self,
                tick: &TickCtx,
                out: &mut Diagnostics,
            ) -> Result<PricerOutput, Refusal> {
                out.note("warming: the funding input has not arrived; previewing at 0");
                self.price(tick, out)
            }
        }
        let p = pair(false, 18);
        let sample = sample("4000", "0.0001", 18);
        let (row, note) = crate::pair::preview_row(&mut Warming, Some(&sample), &p, 0);
        assert_eq!(row.map(|(delta, _)| delta), Ok(U256::one()));
        assert_eq!(
            note.as_deref(),
            Some("warming: the funding input has not arrived; previewing at 0")
        );
    }

    /// A preview that panics is the row's error under `--check`, naming the panic; there
    /// is no lane to trip and the check goes on to the next pair.
    #[test]
    fn a_preview_that_panics_is_a_row_error_not_a_crash() {
        struct Broken;
        impl Pricer for Broken {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                panic!("unwrap on an empty feed")
            }
        }
        let p = pair(false, 18);
        let (row, note) = crate::pair::preview_row(&mut Broken, None, &p, 0);
        assert_eq!(
            row,
            Err("preview panicked: unwrap on an empty feed".to_owned())
        );
        assert!(note.is_none());
    }

    #[test]
    fn an_env_secret_names_its_variable_and_never_prints_its_value() {
        #[derive(serde::Deserialize)]
        struct C {
            key: EnvSecret,
        }
        let c: C = toml::from_str("key = { env = \"PATH\" }").unwrap();
        assert_eq!(format!("{:?}", c.key), "EnvSecret(PATH)");
        assert!(
            !c.key.resolve().unwrap().is_empty(),
            "PATH is set in any test process"
        );
        let unset: C =
            toml::from_str("key = { env = \"QUOTE_UPDATER_TEST_NEVER_SET_7F3A\" }").unwrap();
        let err = format!("{:#}", unset.key.resolve().unwrap_err());
        assert!(err.contains("QUOTE_UPDATER_TEST_NEVER_SET_7F3A"), "{err}");
    }
}
