//! Building blocks for testing a pricer with no runtime, no chain and no feed.
//!
//! A pricer's `price()` is synchronous and reads only what it is handed, so a plain
//! `#[test]` can build one, give it a tick over a market it chose, and check what it
//! published or why it refused, and then whether the core would publish it ([`backstop`]):
//!
//! ```
//! # use quote_updater::{prelude::*, testing};
//! # struct Half;
//! # impl Pricer for Half {
//! #     fn price(&mut self, tick: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
//! #         let market = tick.market()?;
//! #         Ok(PricerOutput::new(market.scaled_fraction(0.001)?, market.mid))
//! #     }
//! # }
//! let pair = testing::pair();
//! let market = testing::market(&pair, 4000.0, 0.0001);
//! let ctx = testing::build_ctx(&pair);
//! let priced = Half
//!     .price(&testing::tick(&pair, Some(&market)), &mut testing::diagnostics(&ctx))
//!     .unwrap();
//! assert_eq!(priced.mid, market.mid);
//! ```

use std::time::Instant;

use serde::de::DeserializeOwned;

use crate::{
    feed::PriceSample,
    pricing::{
        BuildCtx, Diagnostics, Factory, History, Inventory, InventoryFeed, Market, PairShape,
        Pricer, PricerOutput, TickCtx,
    },
};

/// A pair on a direct lane at 18 decimals, with placeholder addresses.
pub fn pair() -> PairShape {
    pair_with(false, 18)
}

/// A pair as the run shapes it from its config: `label` as the tokens' on-chain symbols
/// render it, `tokens` as `[base, quote]` in the file (market orientation), on `target`,
/// at `price_decimals`. The lane and the orientation are derived as `config.rs` derives
/// them (the lane over the address-sorted tokens, inverted when sorting flipped them),
/// never chosen: a `validate` is tested against the pair it will really see.
pub fn pair_for(
    label: &str,
    tokens: (ethrex_common::Address, ethrex_common::Address),
    target: ethrex_common::Address,
    price_decimals: u32,
) -> PairShape {
    let (base, quote) = tokens;
    // PropAMM sorts by uint160, which for H160 is byte-wise order: `config.rs`'s rule.
    let inverted = base > quote;
    let (token0, token1) = if inverted {
        (quote, base)
    } else {
        (base, quote)
    };
    PairShape {
        label: label.to_owned(),
        tokens,
        lane: crate::config::lane_of(token0, token1),
        inverted,
        price_decimals,
        target,
        sources: 1,
        min_mid: None,
        max_mid: None,
    }
}

/// [`pair`], inverted or not, at `price_decimals`. An inverted lane is the case a pricer
/// should never have to notice: test it on both.
pub fn pair_with(inverted: bool, price_decimals: u32) -> PairShape {
    PairShape {
        label: "BASE/QUOTE".into(),
        tokens: (
            ethrex_common::Address::repeat_byte(1),
            ethrex_common::Address::repeat_byte(2),
        ),
        lane: ethrex_common::U256::from(1),
        inverted,
        price_decimals,
        target: ethrex_common::Address::repeat_byte(3),
        sources: 1,
        min_mid: None,
        max_mid: None,
    }
}

/// A fresh market for `pair`, from numbers as a person says them: `market_mid` in market
/// orientation (e.g. 4000 USDC per WETH), `half_spread` as a fraction of the mid. Converted
/// the way the lane converts them, inversion included.
///
/// # Panics
///
/// On a mid that is not positive and finite, or a half-spread that is not a non-negative
/// finite fraction: a test helper, not a validator.
pub fn market(pair: &PairShape, market_mid: f64, half_spread: f64) -> Market {
    market_at(pair, market_mid, half_spread, Instant::now())
}

/// [`market`], sampled at `sampled_at` rather than now: a pricer that reads
/// [`Market::age`] is tested on its aged branch with a sample from a minute ago against
/// [`tick_at`] now. (The core's own freshness gate is not here: it runs before any pricer,
/// so a sample older than its limit never reaches one.)
///
/// # Panics
///
/// As [`market`].
pub fn market_at(
    pair: &PairShape,
    market_mid: f64,
    half_spread: f64,
    sampled_at: Instant,
) -> Market {
    let mid = TickCtx::new(sampled_at, None, pair)
        .mid_from_f64(market_mid)
        .unwrap_or_else(|refusal| panic!("testing::market mid {market_mid}: {refusal}"));
    let delta = crate::volatile::fraction_scaled(half_spread, pair.price_decimals)
        .unwrap_or_else(|err| panic!("testing::market half_spread {half_spread}: {err:#}"));
    Market::from_sample(
        &PriceSample {
            delta,
            mid,
            at: sampled_at,
            // The wall-clock stamp is the recorder's; nothing a pricer sees reads it.
            wall: std::time::SystemTime::now(),
        },
        pair,
    )
}

/// A composite sample for `pair` at `at`, from numbers as a person says them, the way
/// [`market`] builds a market: what a market guard is handed to judge. It names no venues;
/// [`composite_sample_with`] does.
pub fn composite_sample(
    pair: &PairShape,
    market_mid: f64,
    half_spread: f64,
    at: Instant,
) -> crate::guard::CompositeSample {
    composite_sample_with(pair, market_mid, half_spread, at, Vec::new())
}

/// [`composite_sample`] with the venues that went into it, so a guard that judges venue
/// agreement (a dispersion cap) is tested with no run around it.
pub fn composite_sample_with(
    pair: &PairShape,
    market_mid: f64,
    half_spread: f64,
    at: Instant,
    sources: Vec<crate::guard::SourceSample>,
) -> crate::guard::CompositeSample {
    let market = market_at(pair, market_mid, half_spread, at);
    crate::guard::CompositeSample::new(
        at,
        market.mid,
        market.delta,
        pair.inverted,
        pair.price_decimals,
        sources,
    )
}

/// One venue's contribution to a composite, from numbers as a person says them, for
/// [`composite_sample_with`]: `weight` its share of the composite (0 to 1 over the fresh
/// sources), `age` how old its sample was when the composite was computed. Converted the
/// way the lane converts them, inversion included.
pub fn source_sample(
    pair: &PairShape,
    venue: &'static str,
    weight: f64,
    market_mid: f64,
    half_spread: f64,
    age: std::time::Duration,
) -> crate::guard::SourceSample {
    let market = market(pair, market_mid, half_spread);
    crate::guard::SourceSample::new(
        venue,
        weight,
        market.mid,
        market.delta,
        age,
        pair.inverted,
        pair.price_decimals,
    )
}

/// A tick now, over `market` (or none, as for a pair with no `symbol`/`sources`).
pub fn tick<'a>(pair: &'a PairShape, market: Option<&'a Market>) -> TickCtx<'a> {
    tick_at(pair, market, Instant::now())
}

/// [`tick`] at a chosen instant, so a test controls what [`Market::age`] and
/// [`TickCtx::now`] answer.
pub fn tick_at<'a>(pair: &'a PairShape, market: Option<&'a Market>, now: Instant) -> TickCtx<'a> {
    TickCtx::new(now, market, pair)
}

/// What the run does with a pricer's answer before it signs it: the core's backstop, the
/// same function the lane calls after `price()`, over the tick's pair and market. A
/// half-spread below one whole unit that fits its storage slot, and a nonzero mid. `Err`
/// is the withdrawal the run would make in place of publishing, as the line it logs. The
/// pair's `min_mid`/`max_mid` band is the file's, not the pricer's, and is not applied.
///
/// A test that stops at `price()` can pass where the run withdraws every quote:
///
/// ```
/// # use quote_updater::{prelude::*, testing};
/// let pair = testing::pair();
/// let market = testing::market(&pair, 4000.0, 0.0001);
/// let tick = testing::tick(&pair, Some(&market));
/// // A half-spread of one whole unit: a pricer can compute it, the core will not publish it.
/// let whole = PricerOutput::new(U256::exp10(18), market.mid);
/// assert!(testing::backstop(whole, &tick).is_err());
/// ```
pub fn backstop(out: PricerOutput, tick: &TickCtx) -> eyre::Result<PricerOutput> {
    crate::pricing::backstop(out, tick.market().ok(), tick.pair())
        .map_err(|withdrawn| eyre::eyre!("{withdrawn}"))?;
    Ok(out)
}

/// A build context for `pair`, as a factory would get one, with none of the run's inputs:
/// no chain, no history, no inventory. Tasks spawned on it are kept, never run. A pricer
/// that reads an input gets a context from [`build_ctx_with`].
pub fn build_ctx(pair: &PairShape) -> BuildCtx {
    BuildCtx::new(pair.clone())
}

/// What a build context answers with in place of the run's inputs, for a pricer that reads
/// them. An input left unseeded is refused the way a pair without it is. No chain: a pricer
/// that reads the chain is tested under a runtime against a mock.
#[derive(Debug, Default)]
pub struct Inputs {
    /// The vault reading [`BuildCtx::inventory`] hands out, never refreshed.
    pub inventory: Option<Inventory>,
    /// The market mids [`BuildCtx::history`] has seen, as `(at, market_mid)` in any order:
    /// a minute's worth or more gives σ, less is a history still warming up.
    pub history: Vec<(Instant, f64)>,
    /// The reports [`BuildCtx::landings`] has carried, in order: the feed answers the last
    /// with `latest()`, as a run's does. See [`landing`].
    pub landings: Vec<crate::pricing::LandingReport>,
    /// The diagnostics the lane's pricer declared, by name, for a quote guard's build:
    /// what its `read_diagnostic` binds to. See [`pricer_diagnostics`] and [`candidate`].
    pub pricer_diagnostics: Vec<String>,
}

/// What a quote guard is handed for one tick: the pricer's tick, what it priced (or why
/// it refused) and the diagnostics it set, as [`pricer_diagnostics`] seeds them.
pub fn candidate<'a>(
    tick: &'a TickCtx<'a>,
    price: Result<&'a crate::pricing::PricerOutput, &'a crate::pricing::Refusal>,
    diagnostics: &'a Diagnostics,
) -> crate::guard::Candidate<'a> {
    crate::guard::Candidate::new(tick, price, diagnostics)
}

/// The diagnostics a pricer set this tick, by the names [`Inputs::pricer_diagnostics`]
/// seeded the context with: what a quote guard reads through its `ReadHandle`s. A name the
/// pricer did not declare is ignored, as a `set` on a slot the tick has no room for is.
pub fn pricer_diagnostics(ctx: &BuildCtx, values: &[(&str, f64)]) -> Diagnostics {
    let names = ctx.pricer_diagnostic_names();
    let mut out = Diagnostics::new(names.len());
    for (name, value) in values {
        if let Some(index) = names.iter().position(|n| n == name) {
            out.set_at(index, *value);
        }
    }
    out
}

/// A report of what happened to the lane's update for `block`, answered now, for
/// [`Inputs::landings`].
pub fn landing(
    block: u64,
    landing: crate::pricing::LandingOutcome,
) -> crate::pricing::LandingReport {
    crate::pricing::LandingReport {
        block,
        landing,
        at: Instant::now(),
    }
}

/// [`build_ctx`] with `inputs` seeded.
pub fn build_ctx_with(pair: &PairShape, inputs: Inputs) -> BuildCtx {
    let mut ctx = build_ctx(pair);
    if let Some(reading) = inputs.inventory {
        // The sender is dropped at once: the receiver keeps the value, and nothing will
        // ever refresh it, which is the point of a seeded reading.
        let (_tx, rx) = tokio::sync::watch::channel(reading);
        ctx = ctx.with_inventory(InventoryFeed::from_receiver(rx));
    }
    ctx = ctx.with_landings(inputs.landings);
    if !inputs.pricer_diagnostics.is_empty() {
        ctx = ctx.with_pricer_diagnostics(inputs.pricer_diagnostics);
    }
    if !inputs.history.is_empty() {
        let mut samples = inputs.history;
        // The history records in time order and treats an older sample as a late write.
        samples.sort_by_key(|(at, _)| *at);
        let mut history = crate::volatile::PriceHistory::new(samples[0].0);
        for (at, mid) in samples {
            history.record(mid, at);
        }
        ctx = ctx.with_history(History(std::sync::Arc::new(std::sync::Mutex::new(history))));
    }
    ctx
}

/// A vault reading for a test: `base` and `quote` in whole tokens, read now, from a
/// placeholder vault.
pub fn inventory(base: f64, quote: f64) -> Inventory {
    inventory_at(base, quote, Instant::now())
}

/// [`inventory`] read at `at`, for a pricer's stale-reading branch.
pub fn inventory_at(base: f64, quote: f64, at: Instant) -> Inventory {
    Inventory {
        vault: ethrex_common::Address::repeat_byte(4),
        base,
        quote,
        at,
    }
}

/// A tick's diagnostics buffer, sized for what `ctx`'s build declared.
pub fn diagnostics(ctx: &BuildCtx) -> Diagnostics {
    Diagnostics::new(ctx.diagnostic_names().len())
}

/// The stanza's keys (a `kind` line, if present, is ignored) as a kind's config type.
fn config<C: DeserializeOwned>(stanza: &str) -> eyre::Result<C> {
    let mut table: toml::Table = toml::from_str(stanza)?;
    table.remove("kind");
    Ok(toml::Value::Table(table).try_into()?)
}

/// Builds a pricer the way `UpdaterBuilder::pricer_fn` would: `make` gets `stanza`'s config
/// and a fresh build context, which is returned beside the pricer so a test can size its
/// diagnostics with [`diagnostics`].
pub fn build_fn<C, P, F>(make: F, stanza: &str, pair: &PairShape) -> eyre::Result<(P, BuildCtx)>
where
    C: DeserializeOwned,
    P: Pricer,
    F: Fn(C, &mut BuildCtx) -> eyre::Result<P>,
{
    build_fn_with(make, stanza, pair, Inputs::default())
}

/// [`build_fn`] with `inputs` seeded into the build context.
pub fn build_fn_with<C, P, F>(
    make: F,
    stanza: &str,
    pair: &PairShape,
    inputs: Inputs,
) -> eyre::Result<(P, BuildCtx)>
where
    C: DeserializeOwned,
    P: Pricer,
    F: Fn(C, &mut BuildCtx) -> eyre::Result<P>,
{
    let cfg = config::<C>(stanza)?;
    let mut ctx = build_ctx_with(pair, inputs);
    let pricer = make(cfg, &mut ctx)?;
    Ok((pricer, ctx))
}

/// Builds a pricer the way `UpdaterBuilder::pricer` would: `validate`, then `build`. The
/// pricer comes back as the factory's own type, so a test can read its fields. A build that
/// awaits I/O is reported rather than hung: test that one under a runtime.
pub fn build<F: Factory>(
    factory: &F,
    stanza: &str,
    pair: &PairShape,
) -> eyre::Result<(F::Pricer, BuildCtx)> {
    build_with(factory, stanza, pair, Inputs::default())
}

/// [`build`] with `inputs` seeded into the build context: a build that awaits a seeded
/// `ctx.inventory()` completes at once, with no runtime.
pub fn build_with<F: Factory>(
    factory: &F,
    stanza: &str,
    pair: &PairShape,
    inputs: Inputs,
) -> eyre::Result<(F::Pricer, BuildCtx)> {
    let cfg = config::<F::Config>(stanza)?;
    factory.validate(&cfg, pair)?;
    let mut ctx = build_ctx_with(pair, inputs);
    let pricer = ready_now(factory.build(&cfg, &mut ctx))??;
    Ok((pricer, ctx))
}

/// Polls `future` once with a waker that does nothing. A build that is ready at once (every
/// `pricer_fn` build, and any `Factory::build` that does no I/O) completes; one that would
/// wait on I/O is reported rather than hung.
fn ready_now<T>(future: impl std::future::Future<Output = T>) -> eyre::Result<T> {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(value) => Ok(value),
        std::task::Poll::Pending => {
            eyre::bail!("this build awaits I/O; test it under #[tokio::test]")
        }
    }
}

/// Asserts that `C` refuses a key it does not know: `valid_stanza` must deserialize, and
/// the same stanza with one extra key must not, naming it. `deny_unknown_fields` cannot be
/// forced onto a user's config type; this makes forgetting it a failing test, so a typo in
/// a stanza is refused rather than silently ignored.
///
/// # Panics
///
/// When either half does not hold, which is the point.
pub fn assert_rejects_unknown_fields<C: DeserializeOwned>(valid_stanza: &str) {
    if let Err(err) = config::<C>(valid_stanza) {
        panic!("the valid stanza does not deserialize: {err:#}");
    }
    let extra = format!("{valid_stanza}\nnot_a_field_of_this_config = 1");
    match config::<C>(&extra) {
        Ok(_) => panic!(
            "an unknown key was accepted: add #[serde(deny_unknown_fields)] to the config type"
        ),
        Err(err) => assert!(
            format!("{err:#}").contains("not_a_field_of_this_config"),
            "the refusal does not name the unknown key: {err:#}"
        ),
    }
}

// ----------------------------------------------------------------------------------------
// A stock design's adapters: an HTTP API on this machine, and a chain answering only what
// it was told to.
// ----------------------------------------------------------------------------------------

mod mock_http;
pub use mock_http::MockApi;

use std::collections::HashMap;

use crate::{
    Address,
    calldata::{Value, encode_calldata},
    pricing::{BoxFuture, CallOutcome},
    stocks::chain::Calls,
};

/// `values` ABI-encoded as a call's return data: calldata without its selector.
pub fn returned(types: &str, values: &[Value]) -> Vec<u8> {
    encode_calldata(&format!("f({types})"), values).unwrap()[4..].to_vec()
}

/// Answers each call it was given, by contract and exact calldata (and, for a call made
/// as an account, that account), and nothing else: a call to another contract, another
/// function, with other arguments or as another account fails, as a read that could
/// not be made.
#[derive(Debug, Default)]
pub struct FakeChain {
    answers: HashMap<(Address, Vec<u8>), Vec<u8>>,
    outcomes: HashMap<(Address, Address, Vec<u8>), CallOutcome>,
}

impl FakeChain {
    /// `to.sig(args)` answers `values`, of the return types `types` (as `address`).
    pub fn answer(
        mut self,
        to: Address,
        sig: &str,
        args: &[Value],
        types: &str,
        values: &[Value],
    ) -> FakeChain {
        let calldata = encode_calldata(sig, args).unwrap();
        self.answers.insert((to, calldata), returned(types, values));
        self
    }

    /// `to.sig(args)`, made as `from`, ends in `outcome`.
    pub fn outcome(
        mut self,
        from: Address,
        to: Address,
        sig: &str,
        args: &[Value],
        outcome: CallOutcome,
    ) -> FakeChain {
        let calldata = encode_calldata(sig, args).unwrap();
        self.outcomes.insert((from, to, calldata), outcome);
        self
    }
}

impl Calls for FakeChain {
    fn call_sig<'a>(
        &'a self,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<Vec<u8>>> {
        let answer = encode_calldata(sig, args)
            .ok()
            .and_then(|calldata| self.answers.get(&(to, calldata)).cloned());
        Box::pin(async move {
            answer.ok_or_else(|| eyre::eyre!("no contract at {to:#x} answers {sig} so called"))
        })
    }

    fn call_sig_as<'a>(
        &'a self,
        from: Address,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<CallOutcome>> {
        let outcome = encode_calldata(sig, args)
            .ok()
            .and_then(|calldata| self.outcomes.get(&(from, to, calldata)).cloned());
        Box::pin(async move {
            outcome
                .ok_or_else(|| eyre::eyre!("no call of {sig} on {to:#x} as {from:#x} was expected"))
        })
    }
}
