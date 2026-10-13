//! The pricer every tokenized-stock kind builds (`ondo` today): the token's price as the mid,
//! a fixed `delta`, an optional inventory `skew`, and a quote only while every input says the
//! token's price is what it seems. The inputs come through the ports ([`crate::ports`]) from
//! the adapters of the kind's design; the gates are the same whoever the issuer:
//!
//! - **Fresh.** The latest price is under `max_price_age_secs` old by the source's own
//!   timestamp, and not stamped in the future.
//! - **Moving.** The price has changed within `max_unchanged_secs`, by the source's
//!   timestamps. A stock halted on its exchange (a limit-up-limit-down pause, news pending)
//!   stops moving while its source may keep stamping the last price as current; the issuer
//!   reports the halt only as fast as its status is read. A liquid stock's price moves
//!   every few seconds in session.
//! - **In session, nothing on the asset.** For a design whose issuer reports a status
//!   ([`MarketStatus`]): its readings are under `max_status_age_secs` old, and say the market
//!   is in the session the pair quotes and nothing stops the asset: no event under way, none
//!   that has reached its start, and none that will move the multiplier starting within
//!   `pause_lead_secs`.
//! - **No corporate action.** The token schedules none to start within `pause_lead_secs`,
//!   and none is under way ([`Valuation`]). A split or a dividend changes how many shares a
//!   token is worth, and the issuer's pause around it has no effect on chain: the token
//!   keeps trading through it, against anyone who has not stopped quoting.
//! - **Consistent.** The source's token price over its stock price (the `k` it prices with)
//!   matches the token's `k` within `max_multiplier_gap`. A gap means one of the two has
//!   applied a corporate action the other has not.
//!
//! The stock token is listed first (`tokens = [stock, USDC]`), and the build checks it is the
//! token the stanza's `symbol` names by the token's own `symbol()`. The price is USDC per
//! token, USDC taken at $1, and the library publishes it the way the lane carries it: the
//! other way up, token per USDC, when the stock token's address sorts after USDC's `0xA0b8…`.
//! `min_mid`/`max_mid` are in the lane's orientation too, the number `--check`'s mid column
//! prints; a band written the other way up fails the build, since `--check` applies no band
//! and the run would refuse every mid.
//!
//! The pair has no `symbol`/`sources`: the price is this pricer's own, polled by a task the
//! build starts, so the library's feed staleness check and its market guards
//! (`max_deviation` included) do not apply. The gates above are how the core makes up for it.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use quote_updater::{Address, U256, eyre, prelude::*};

use crate::{
    chain,
    poll::{self, Failures, Stamped, lock, wall_clock_ms},
    ports::{Action, MarketStatus, Price, PriceSource, Shares, StatusFeed, Valuation},
};

/// The oldest a stanza may let a price or a status reading be.
const MAX_READING_AGE_SECS: u64 = 3_600;
/// The longest lead a stanza may stop quoting ahead of a corporate action.
const MAX_PAUSE_LEAD_SECS: u64 = 7 * 86_400;

/// How often the token's `k` is read: once a block.
const SHARES_POLL: Duration = Duration::from_secs(12);
/// The oldest vault reading the skew is computed from.
const MAX_INVENTORY_AGE: Duration = Duration::from_secs(60);

/// A stock stanza as the core reads it: the keys every kind's stanza has, whatever its
/// design adds. Each kind fills it from a stanza of its own, which keeps its own defaults
/// and refuses unknown keys.
#[derive(Clone, Debug)]
pub struct Stanza<'a> {
    /// The kind, as messages name it: `ondo`.
    pub kind: &'static str,
    /// Who prices the token, as messages name it: `Ondo`.
    pub source: &'static str,
    /// The stock token's `symbol()`.
    pub symbol: &'a str,
    /// The half-spread as a fraction of the mid, as written (`"0.003"` = 30 bp).
    pub delta: &'a str,
    /// How often the price is polled.
    pub price_poll: Duration,
    /// How often the issuer's status is read, for a design whose issuer reports one.
    pub status_poll: Option<Duration>,
    /// The oldest price, by the source's timestamp, that is quoted.
    pub max_price_age_secs: u64,
    /// The longest the price may stand still, by the source's timestamps, and be quoted.
    pub max_unchanged_secs: u64,
    /// The oldest status or `k` reading that is trusted.
    pub max_status_age_secs: u64,
    /// How long before a scheduled corporate action quoting stops.
    pub pause_lead_secs: u64,
    /// The largest relative gap between the source's `k` and the token's.
    pub max_multiplier_gap: f64,
    /// Inventory skew: the mid moves by `skew × (base share − 0.5)`. 0 reads no inventory.
    pub skew: f64,
}

/// A stanza, checked against its pair: what `validate` proves and `build` uses.
#[derive(Clone, Debug)]
pub struct Settings {
    kind: &'static str,
    source: &'static str,
    symbol: String,
    delta: U256,
    price_poll: Duration,
    max_price_age_ms: i64,
    max_unchanged_ms: i64,
    max_status_age: Duration,
    pause_lead_secs: i64,
    max_multiplier_gap: f64,
    skew: f64,
}

impl Settings {
    pub fn new(s: &Stanza<'_>, pair: &PairShape) -> eyre::Result<Settings> {
        eyre::ensure!(
            pair.sources == 0,
            "an `{}` pair takes its price from {}: remove its `symbol`/`sources`, whose \
             freshness would otherwise withdraw it on its own schedule",
            s.kind,
            s.source
        );
        eyre::ensure!(!s.symbol.is_empty(), "symbol is empty");
        // The label is the tokens' on-chain symbols, base first, when they were read; it is
        // `lane 0x…` when they were not, as for every halted pair, which is never read. The
        // build ties the stanza to the token's own symbol() either way; this catches a
        // mismatch the label already shows without waiting for it.
        if let Some((first, _)) = pair.label.split_once('/') {
            check_symbol(s.symbol, first)?;
        }
        let delta = parse_fraction(s.delta, pair.price_decimals)
            .map_err(|err| eyre::eyre!("delta: {err}"))?;
        let tenth = parse_fraction("0.1", pair.price_decimals).map_err(|err| eyre::eyre!(err))?;
        eyre::ensure!(
            !delta.is_zero() && delta < tenth,
            "delta {} is not in (0, 0.1)",
            s.delta
        );
        // Bounded before any arithmetic: a u64 setting multiplied into milliseconds or cast to
        // i64 would otherwise wrap, and a wrapped age or lead disables its gate.
        for (name, secs) in [
            ("max_price_age_secs", s.max_price_age_secs),
            ("max_unchanged_secs", s.max_unchanged_secs),
            ("max_status_age_secs", s.max_status_age_secs),
        ] {
            eyre::ensure!(
                secs <= MAX_READING_AGE_SECS,
                "{name} {secs} is over an hour: a reading that old says nothing about now"
            );
        }
        eyre::ensure!(
            s.pause_lead_secs <= MAX_PAUSE_LEAD_SECS,
            "pause_lead_secs {} is over a week",
            s.pause_lead_secs
        );
        eyre::ensure!(!s.price_poll.is_zero(), "the price poll period is zero");
        eyre::ensure!(
            s.status_poll.is_none_or(|every| !every.is_zero()),
            "the status poll period is zero"
        );
        eyre::ensure!(
            Duration::from_secs(s.max_price_age_secs) > s.price_poll,
            "max_price_age_secs {} is not longer than the {}ms poll: every price would be stale",
            s.max_price_age_secs,
            s.price_poll.as_millis()
        );
        eyre::ensure!(
            Duration::from_secs(s.max_unchanged_secs) > s.price_poll,
            "max_unchanged_secs {} is not longer than the {}ms poll: every price would read as \
             standing still",
            s.max_unchanged_secs,
            s.price_poll.as_millis()
        );
        let slowest_reading = s.status_poll.unwrap_or_default().max(SHARES_POLL);
        eyre::ensure!(
            Duration::from_secs(s.max_status_age_secs) > slowest_reading,
            "max_status_age_secs {} is not longer than the {}s between readings: every \
             reading would be stale",
            s.max_status_age_secs,
            slowest_reading.as_secs()
        );
        eyre::ensure!(
            s.max_multiplier_gap > 0.0 && s.max_multiplier_gap < 0.05,
            "max_multiplier_gap {} is not in (0, 0.05)",
            s.max_multiplier_gap
        );
        eyre::ensure!(
            (0.0..=0.5).contains(&s.skew),
            "skew {} is not in 0..=0.5",
            s.skew
        );
        Ok(Settings {
            kind: s.kind,
            source: s.source,
            symbol: s.symbol.to_owned(),
            delta,
            price_poll: s.price_poll,
            max_price_age_ms: (s.max_price_age_secs * 1_000) as i64,
            max_unchanged_ms: (s.max_unchanged_secs * 1_000) as i64,
            max_status_age: Duration::from_secs(s.max_status_age_secs),
            pause_lead_secs: s.pause_lead_secs as i64,
            max_multiplier_gap: s.max_multiplier_gap,
            skew: s.skew,
        })
    }
}

/// That the stanza's `symbol` is the pair's first token's, `first` (its on-chain symbol()).
/// It is what stops a TSLAon stanza pricing AMZNon.
fn check_symbol(symbol: &str, first: &str) -> eyre::Result<()> {
    eyre::ensure!(
        first == symbol,
        "symbol {symbol} is not the pair's first token, whose symbol() is {first}: the stock \
         token is listed first (tokens = [stock, USDC]) and the stanza prices it"
    );
    Ok(())
}

/// Where the source's price falls against the pair's band.
#[derive(Debug, PartialEq, Eq)]
enum Band {
    /// Inside, or no band.
    Inside,
    /// Outside either way up: the market moved, and the core withdraws until it is back.
    Outside,
    /// Outside as the lane carries it, inside the other way up: the band was written in
    /// the other orientation, which no price will ever pass.
    Backwards,
}

/// Where `token_price` (USDC per token, as the source prices it) falls against the pair's
/// band, which is in the lane's orientation: token per USDC when the stock token's address
/// sorts after USDC's.
fn band_of(token_price: f64, pair: &PairShape) -> Band {
    let number = |bound: U256| {
        bound.to_string().parse::<f64>().unwrap_or(f64::INFINITY)
            / 10f64.powi(pair.price_decimals as i32)
    };
    let (min, max) = (pair.min_mid.map(number), pair.max_mid.map(number));
    let holds = |mid: f64| min.is_none_or(|min| mid >= min) && max.is_none_or(|max| mid <= max);
    let lane_mid = if pair.inverted {
        1.0 / token_price
    } else {
        token_price
    };
    if holds(lane_mid) {
        Band::Inside
    } else if holds(1.0 / lane_mid) {
        Band::Backwards
    } else {
        Band::Outside
    }
}

/// `written`, a decimal fraction such as `0.003`, exactly, at `decimals` decimals.
fn parse_fraction(written: &str, decimals: u32) -> Result<U256, String> {
    let bad = || format!("{written:?} is not a decimal number such as 0.003");
    let (whole, fraction) = written.split_once('.').unwrap_or((written, ""));
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|c| c.is_ascii_digit());
    if !digits(whole) || (written.contains('.') && !digits(fraction)) {
        return Err(bad());
    }
    if fraction.len() > decimals as usize {
        return Err(format!(
            "{written:?} has more than the pair's {decimals} decimals"
        ));
    }
    let scaled = format!("{whole}{fraction:0<width$}", width = decimals as usize);
    U256::from_dec_str(&scaled).map_err(|_| bad())
}

/// How far ahead of our clock a source may stamp a price before it is refused as impossible.
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

/// Whether a price just read should replace the one held (`Ok(true)`), keep it (`Ok(false)`,
/// an older answer that crossed a newer one on the wire), or be refused: one stamped further in
/// the future than clocks drift, which would otherwise read as fresh forever and, being the
/// newest, keep every correct price after it out. `source` names who stamped it.
fn admit(held: Option<Price>, fresh: Price, now_ms: i64, source: &str) -> Result<bool, String> {
    let positive = |value: f64| value.is_finite() && value > 0.0;
    if !(positive(fresh.token) && positive(fresh.stock)) {
        return Err(format!(
            "{source} answered a price that is not a positive number (token {}, stock {}); \
             refusing it",
            fresh.token, fresh.stock
        ));
    }
    let ahead_ms = fresh.at_ms.saturating_sub(now_ms);
    if ahead_ms > MAX_CLOCK_SKEW_MS {
        return Err(format!(
            "{source} stamped its price {:.1}s in the future (timestamp {}); refusing it",
            ahead_ms as f64 / 1_000.0,
            fresh.at_ms
        ));
    }
    Ok(held.is_none_or(|held| fresh.at_ms >= held.at_ms))
}

/// The price held, and since when the source has priced it the same.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Held {
    price: Price,
    /// The source's timestamp on the first answer with this price: how long it has stood
    /// still is the latest answer's age from here.
    since_ms: i64,
}

/// `fresh`, an answer [`admit`] took as newer, held after `held`: still since `held`'s
/// start if the source prices it the same, since its own stamp if it moved.
fn hold(held: Option<Held>, fresh: Price) -> Held {
    let since_ms = match held {
        Some(held) if held.price.token == fresh.token && held.price.stock == fresh.stock => {
            held.since_ms
        }
        _ => fresh.at_ms,
    };
    Held {
        price: fresh,
        since_ms,
    }
}

/// `shares` as a reading the core can quote from: `k` a positive number, as every token's
/// is. Anything else is a broken read, refused like a failed one.
fn check_shares(shares: Shares) -> eyre::Result<Shares> {
    eyre::ensure!(
        shares.k.is_finite() && shares.k > 0.0,
        "the token's share multiplier {} is not a positive number",
        shares.k
    );
    Ok(shares)
}

/// What the pair's own readers leave for `price` to read: `Copy`, so a tick copies it out
/// of the lock and holds nothing.
#[derive(Clone, Copy, Debug, Default)]
struct Feeds {
    price: Option<Held>,
    shares: Option<Stamped<Shares>>,
}

/// The reasons and numbers the pricer declares.
struct Handles {
    price_stale: RefusalHandle,
    price_frozen: RefusalHandle,
    status_stale: RefusalHandle,
    market_session: RefusalHandle,
    asset_event: RefusalHandle,
    multiplier_paused: RefusalHandle,
    multiplier_mismatch: RefusalHandle,
    inventory_stale: RefusalHandle,
    token_price: DiagHandle,
    stock_price: DiagHandle,
    price_age_secs: DiagHandle,
    price_unchanged_secs: DiagHandle,
    svalue: DiagHandle,
    multiplier_gap: DiagHandle,
    base_share: DiagHandle,
}

impl Handles {
    fn declare(ctx: &mut BuildCtx) -> eyre::Result<Handles> {
        Ok(Handles {
            price_stale: ctx.refusal("price_stale")?,
            price_frozen: ctx.refusal("price_frozen")?,
            status_stale: ctx.refusal("status_stale")?,
            market_session: ctx.refusal("market_session")?,
            asset_event: ctx.refusal("asset_event")?,
            multiplier_paused: ctx.refusal("multiplier_paused")?,
            multiplier_mismatch: ctx.refusal("multiplier_mismatch")?,
            inventory_stale: ctx.refusal("inventory_stale")?,
            token_price: ctx.diagnostic("token_price")?,
            stock_price: ctx.diagnostic("stock_price")?,
            price_age_secs: ctx.diagnostic("price_age_secs")?,
            price_unchanged_secs: ctx.diagnostic("price_unchanged_secs")?,
            svalue: ctx.diagnostic("svalue")?,
            multiplier_gap: ctx.diagnostic("multiplier_gap")?,
            base_share: ctx.diagnostic("base_share")?,
        })
    }
}

/// A stock pair's pricer, whatever the design behind the token.
pub struct StockPricer {
    settings: Settings,
    /// What the issuer's status may call the asset: the token's symbol, and the stock's
    /// ticker when the price source names it.
    names: Vec<String>,
    feeds: Arc<Mutex<Feeds>>,
    /// The issuer's status, for a design whose issuer reports one.
    status: Option<Box<dyn MarketStatus>>,
    /// The vault, read only when `skew` is on.
    inventory: Option<InventoryFeed>,
    h: Handles,
    /// Milliseconds since the epoch now: the wall clock in the run, a fixed instant in a test.
    clock: fn() -> i64,
}

impl StockPricer {
    /// `reading`, if there is one and, on a gated tick, it is recent enough to trust.
    fn fresh<'a, T>(
        &self,
        reading: Option<&'a Stamped<T>>,
        what: &str,
        now: Instant,
        gated: bool,
    ) -> Result<&'a Stamped<T>, Refusal> {
        let Some(reading) = reading else {
            return Err(Refusal::new(
                &self.h.status_stale,
                format!("no {what} reading yet"),
            ));
        };
        let age = now.saturating_duration_since(reading.at);
        if gated && age > self.settings.max_status_age {
            return Err(Refusal::new(
                &self.h.status_stale,
                format!(
                    "the {what} reading is {}s old (limit {}s)",
                    age.as_secs(),
                    self.settings.max_status_age.as_secs()
                ),
            ));
        }
        Ok(reading)
    }

    /// The issuer's session and asset status, if it reports them. The adapter judges its
    /// readings for this asset; the core times the events it reports, as it times the
    /// corporate actions scheduled on chain.
    fn check_status(&self, now: Instant, now_ms: i64, gated: bool) -> Result<(), Refusal> {
        let Some(status) = &self.status else {
            return Ok(());
        };
        let reading = status.read(&self.names);
        let session = self.fresh(reading.session.as_ref(), "market session", now, gated)?;
        let asset = self.fresh(reading.asset.as_ref(), "asset status", now, gated)?;
        if !gated {
            return Ok(());
        }
        if let Err(why) = &session.value {
            return Err(Refusal::new(&self.h.market_session, why.clone()));
        }
        let lead_ms = self.settings.pause_lead_secs.saturating_mul(1_000);
        let mut stopping = asset
            .value
            .iter()
            .filter(|event| event.stops_quoting(now_ms, lead_ms))
            .peekable();
        if stopping.peek().is_some() {
            let why: Vec<&str> = stopping.map(|event| event.why.as_str()).collect();
            return Err(Refusal::new(
                &self.h.asset_event,
                format!(
                    "{} reports {}: {}",
                    self.settings.source,
                    self.settings.symbol,
                    why.join("; ")
                ),
            ));
        }
        Ok(())
    }

    /// The quote, with every gate when `gated`, or from whatever was read when not (a preview).
    fn quote(
        &self,
        tick: &TickCtx,
        out: &mut Diagnostics,
        gated: bool,
    ) -> Result<PricerOutput, Refusal> {
        let s = &self.settings;
        let h = &self.h;
        let now_ms = (self.clock)();
        let Feeds { price, shares } = *lock(&self.feeds);

        let Held { price, since_ms } = price.ok_or_else(|| {
            Refusal::new(&h.price_stale, format!("no price from {} yet", s.source))
        })?;
        let age_ms = now_ms.saturating_sub(price.at_ms);
        // Admission refuses a price from the future; one held can still be, if the clock
        // stepped back since. Never read it as fresh.
        if gated && age_ms < -MAX_CLOCK_SKEW_MS {
            return Err(Refusal::new(
                &h.price_stale,
                format!(
                    "{}'s price for {} is stamped {:.1}s in the future",
                    s.source,
                    s.symbol,
                    -age_ms as f64 / 1_000.0
                ),
            ));
        }
        let age_ms = age_ms.max(0);
        out.set(&h.token_price, price.token);
        out.set(&h.stock_price, price.stock);
        out.set(&h.price_age_secs, age_ms as f64 / 1_000.0);
        if gated && age_ms > s.max_price_age_ms {
            return Err(Refusal::new(
                &h.price_stale,
                format!(
                    "{}'s price for {} is {:.1}s old (limit {}s)",
                    s.source,
                    s.symbol,
                    age_ms as f64 / 1_000.0,
                    s.max_price_age_ms / 1_000
                ),
            ));
        }
        let unchanged_ms = now_ms.saturating_sub(since_ms).max(0);
        out.set(&h.price_unchanged_secs, unchanged_ms as f64 / 1_000.0);
        if gated && unchanged_ms > s.max_unchanged_ms {
            return Err(Refusal::new(
                &h.price_frozen,
                format!(
                    "{}'s price for {} has stood at {} for {:.1}s (limit {}s): the stock may \
                     be halted",
                    s.source,
                    s.symbol,
                    price.token,
                    unchanged_ms as f64 / 1_000.0,
                    s.max_unchanged_ms / 1_000
                ),
            ));
        }

        self.check_status(tick.now(), now_ms, gated)?;
        let reading = self.fresh(shares.as_ref(), "share multiplier", tick.now(), gated)?;
        let shares = reading.value;
        out.set(&h.svalue, shares.k);
        let now_secs = now_ms.div_euclid(1_000);
        let stops = |action: &Action| gated && action.stops_quoting(now_secs, s.pause_lead_secs);
        if let Some(action) = shares.action.filter(stops) {
            let when = if action.is_on(now_secs) {
                "is on"
            } else {
                "starts within the lead"
            };
            return Err(Refusal::new(
                &h.multiplier_paused,
                format!(
                    "a corporate-action pause on {} {when} (from {}, lead {}s)",
                    s.symbol, action.starts, s.pause_lead_secs
                ),
            ));
        }
        let gap = price.token / price.stock / shares.k - 1.0;
        out.set(&h.multiplier_gap, gap);
        // Not `gap.abs() > max`, which a gap that is not a number passes: this is false for one.
        let agrees = gap.abs() <= s.max_multiplier_gap;
        if gated && !agrees {
            return Err(Refusal::new(
                &h.multiplier_mismatch,
                format!(
                    "{} prices {} at {:.6} times the stock, the chain's multiplier is {:.6}: \
                     a {:.3}% gap (limit {:.3}%)",
                    s.source,
                    s.symbol,
                    price.token / price.stock,
                    shares.k,
                    gap * 100.0,
                    s.max_multiplier_gap * 100.0
                ),
            ));
        }

        let mut mid = price.token;
        if let Some(inventory) = &self.inventory {
            let reading = inventory.latest();
            if gated && reading.age(tick.now()) > MAX_INVENTORY_AGE {
                return Err(Refusal::new(
                    &h.inventory_stale,
                    format!(
                        "the vault reading is over {}s old",
                        MAX_INVENTORY_AGE.as_secs()
                    ),
                ));
            }
            let share = reading.base_share(price.token);
            out.set(&h.base_share, share);
            // Long the stock (share over one half) moves the mid down, so takers buy it.
            mid *= 1.0 - s.skew * (share - 0.5);
        }
        Ok(PricerOutput::new(s.delta, tick.mid_from_f64(mid)?))
    }
}

impl Pricer for StockPricer {
    fn price(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        self.quote(tick, out, true)
    }

    /// `--check` runs whenever the operator does, the market open or not: it prices from the
    /// build's first reads without the gates that depend on the hour, and says so.
    fn preview(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        out.note(
            "previewed from the build's first reads, without the age, session, event and \
             corporate-action gates the run applies every tick",
        );
        self.quote(tick, out, false)
    }
}

/// What a kind hands the core to build its pricer: its design's adapters.
pub struct Adapters {
    pub prices: Box<dyn PriceSource>,
    pub valuation: Box<dyn Valuation>,
    /// Opens the issuer's status, for a design whose issuer reports one. The build awaits it
    /// after the first price, as one of its reads: a wrong key or an answer that does not
    /// parse fails the build, not every tick. Opening it starts nothing: the build runs its
    /// refresh as one of the pair's tasks.
    pub status: Option<BoxFuture<'static, eyre::Result<StatusFeed>>>,
}

/// Builds the pair's pricer from its design's adapters: reads everything once, so a wrong
/// key, symbol, oracle or token fails this build (and `--check`) instead of withdrawing every
/// tick, then starts the tasks that keep the price and `k` current.
pub async fn build(
    ctx: &mut BuildCtx,
    settings: Settings,
    adapters: Adapters,
) -> eyre::Result<StockPricer> {
    let Adapters {
        prices,
        valuation,
        status,
    } = adapters;
    let chain = ctx.chain()?;
    let pair = ctx.pair().clone();
    let token = pair.tokens.0;
    let h = Handles::declare(ctx)?;

    // The stanza prices the token the pair lists first, by that token's own symbol(): the
    // label `validate` saw is the lane's whenever the library had not read it.
    let symbol = chain::read_symbol(&chain, token)
        .await
        .map_err(first_token("the symbol"))?;
    check_symbol(&settings.symbol, &symbol)?;

    let first = prices.fetch().await?;
    admit(None, first.price, wall_clock_ms(), settings.source).map_err(|err| eyre::eyre!(err))?;
    judge_band(&first.price, &pair, &settings)?;
    let status = match status {
        Some(open) => Some(open.await?),
        None => None,
    };
    let status = status.map(|StatusFeed { reader, refresh }| {
        ctx.spawn(refresh);
        reader
    });
    let shares = valuation
        .read()
        .await
        .and_then(check_shares)
        .map_err(first_token("the share multiplier"))?;
    let feeds = Arc::new(Mutex::new(Feeds {
        price: Some(hold(None, first.price)),
        shares: Some(Stamped::now(shares)),
    }));
    let inventory = if settings.skew > 0.0 {
        Some(ctx.inventory().await?)
    } else {
        None
    };
    let names = std::iter::once(settings.symbol.clone())
        .chain(first.ticker.filter(|ticker| !ticker.is_empty()))
        .collect();

    ctx.spawn(poll_prices(prices, settings.clone(), Arc::clone(&feeds)));
    ctx.spawn(poll_shares(valuation, token, Arc::clone(&feeds)));
    Ok(StockPricer {
        settings,
        names,
        feeds,
        status,
        inventory,
        h,
        clock: wall_clock_ms,
    })
}

async fn poll_prices(prices: Box<dyn PriceSource>, s: Settings, feeds: Arc<Mutex<Feeds>>) {
    let mut failures = Failures::new(format!("{} price for {}", s.source, s.symbol));
    let mut ticks = poll::every(s.price_poll);
    loop {
        ticks.tick().await;
        let read = prices.fetch().await.and_then(|fetched| {
            let mut feeds = lock(&feeds);
            let held = feeds.price.map(|held| held.price);
            let newer = admit(held, fetched.price, wall_clock_ms(), s.source)
                .map_err(|err| eyre::eyre!(err))?;
            if newer {
                feeds.price = Some(hold(feeds.price, fetched.price));
            }
            Ok(())
        });
        // Logged with the lock released: a slow log sink must not hold up the quote loop.
        match read {
            Ok(()) => failures.succeeded(),
            Err(err) => failures.failed(&err),
        }
    }
}

async fn poll_shares(valuation: Box<dyn Valuation>, token: Address, feeds: Arc<Mutex<Feeds>>) {
    let mut failures = Failures::new(format!("share multiplier of {token:#x}"));
    let mut ticks = poll::every(SHARES_POLL);
    loop {
        ticks.tick().await;
        match valuation.read().await.and_then(check_shares) {
            Ok(shares) => {
                lock(&feeds).shares = Some(Stamped::now(shares));
                failures.succeeded();
            }
            Err(err) => failures.failed(&err),
        }
    }
}

/// A read of the pair's first token that failed, said so that a pair listed USDC first is
/// recognised for what it is.
fn first_token(what: &str) -> impl FnOnce(eyre::Report) -> eyre::Report {
    move |err| {
        eyre::eyre!(
            "{what} of the pair's first token: {err:#} (the stock token is listed first, tokens \
             = [stock, USDC])"
        )
    }
}

/// The band against the source's first price, as the build judges it: refused when written
/// the other way up, warned of when the market is merely outside it.
fn judge_band(first: &Price, pair: &PairShape, s: &Settings) -> eyre::Result<()> {
    let (price, symbol, source) = (first.token, &s.symbol, s.source);
    let carried = if pair.inverted {
        format!(
            "{symbol}'s address sorts after USDC's, so the lane carries {symbol} per USDC, {}",
            1.0 / price
        )
    } else {
        format!("the lane carries USDC per {symbol}, {price}")
    };
    match band_of(price, pair) {
        Band::Inside => Ok(()),
        Band::Backwards => eyre::bail!(
            "min_mid/max_mid are the other way up from this lane: {source} prices {symbol} at \
             {price} USDC and {carried}, which the band refuses and would hold the other way \
             up. Write the band as the lane carries it (the number --check's mid column prints)"
        ),
        Band::Outside => {
            tracing::warn!(
                "[{}] {}: {source} prices {symbol} at {price} USDC and {carried}, outside \
                 min_mid/max_mid: every quote is withdrawn until it is back inside",
                pair.label,
                s.kind
            );
            Ok(())
        }
    }
}

/// What the kinds' tests build a pricer from: a pair, its settings, the readings its tasks
/// would have left, and the design's status adapter, without a chain or a task.
#[cfg(test)]
pub mod testkit {
    use quote_updater::testing::{self, Inputs};

    use super::*;
    use crate::{chain::parse_address, ports::StatusReading};

    pub const AAPLON: &str = "0x14c3abF95Cb9C93a8b82C1CdCB76D72Cb87b2d4c";
    pub const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    /// Thursday 2026-10-08 14:00:00 UTC, 10:00 in New York: the wall clock every tick reads.
    pub const NOW_MS: i64 = 1_791_468_000_000;

    /// The monotonic instant every tick runs at: fixed, so a reading's age is exact.
    pub fn now() -> Instant {
        static NOW: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        *NOW.get_or_init(Instant::now)
    }

    /// `ms` before [`now`], on the monotonic clock.
    pub fn ago(ms: u64) -> Instant {
        now()
            .checked_sub(Duration::from_millis(ms))
            .expect("the monotonic clock started longer ago than that")
    }

    /// When [`healthy`] read everything: a second ago.
    pub fn read_at() -> Instant {
        ago(1_000)
    }

    /// The pair as the run shapes it: AAPLon first, so the lane is not inverted, and no
    /// venue market (`testing::pair_for` gives every pair one).
    pub fn pair() -> PairShape {
        let mut pair = testing::pair_for(
            "AAPLon/USDC",
            (parse_address(AAPLON).unwrap(), parse_address(USDC).unwrap()),
            Address::repeat_byte(0x77),
            18,
        );
        assert!(!pair.inverted, "AAPLon sorts before USDC");
        pair.sources = 0;
        pair
    }

    /// A stock token whose address sorts after USDC's, so the lane carries token per USDC:
    /// the other way up from the source's price, which the library turns over.
    pub fn inverted_pair() -> PairShape {
        let mut pair = testing::pair_for(
            "AAPLon/USDC",
            (Address::repeat_byte(0xB0), parse_address(USDC).unwrap()),
            Address::repeat_byte(0x77),
            18,
        );
        assert!(pair.inverted, "0xB0… sorts after USDC's 0xA0b8…");
        pair.sources = 0;
        pair
    }

    /// What a tick reads: the pair's own readings, and the issuer's status, if any.
    pub struct Reads {
        pub price: Option<Price>,
        /// Since when the source has priced it the same: the price's own stamp if `None`.
        pub price_since_ms: Option<i64>,
        pub shares: Option<Stamped<Shares>>,
        pub status: Option<Box<dyn MarketStatus>>,
    }

    /// Everything a tick reads, consistent, with `status` for the issuer's: $256.79 a token,
    /// a multiplier of 1.002124 on chain and in the source's price, no corporate action, all
    /// read a second ago.
    pub fn healthy(status: Option<Box<dyn MarketStatus>>) -> Reads {
        Reads {
            price: Some(Price {
                token: 256.79,
                stock: 256.79 / 1.002124,
                at_ms: NOW_MS - 1_000,
            }),
            price_since_ms: None,
            shares: Some(Stamped {
                at: read_at(),
                value: Shares {
                    k: 1.002124,
                    action: None,
                },
            }),
            status,
        }
    }

    /// A status adapter answering `reading` whatever the asset: the core's own tests'.
    impl MarketStatus for StatusReading {
        fn read(&self, _names: &[String]) -> StatusReading {
            self.clone()
        }
    }

    /// An issuer reporting the market in session and nothing on the asset, a second ago.
    pub fn in_session() -> StatusReading {
        StatusReading {
            session: Some(Stamped {
                at: read_at(),
                value: Ok(()),
            }),
            asset: Some(Stamped {
                at: read_at(),
                value: Vec::new().into(),
            }),
        }
    }

    pub struct Built {
        pub pricer: StockPricer,
        pub ctx: BuildCtx,
        pub pair: PairShape,
    }

    /// The pricer as the build leaves it, for AAPLon, whose stock the source names AAPL.
    pub fn built(
        pair: PairShape,
        settings: Settings,
        reads: Reads,
        inventory: Option<Inventory>,
    ) -> Built {
        let mut ctx = testing::build_ctx_with(
            &pair,
            Inputs {
                inventory,
                ..Inputs::default()
            },
        );
        let h = Handles::declare(&mut ctx).unwrap();
        let inventory = (settings.skew > 0.0).then(|| {
            let future = ctx.inventory();
            futures_ready(future).unwrap()
        });
        Built {
            pricer: StockPricer {
                settings,
                names: vec!["AAPLon".to_owned(), "AAPL".to_owned()],
                feeds: Arc::new(Mutex::new(Feeds {
                    price: reads.price.map(|price| Held {
                        price,
                        since_ms: reads.price_since_ms.unwrap_or(price.at_ms),
                    }),
                    shares: reads.shares,
                })),
                status: reads.status,
                inventory,
                h,
                clock: || NOW_MS,
            },
            ctx,
            pair,
        }
    }

    /// A seeded `ctx.inventory()` is ready at once.
    fn futures_ready<T>(future: impl std::future::Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => panic!("a seeded inventory was not ready"),
        }
    }

    impl Built {
        pub fn price(&mut self) -> Result<PricerOutput, Refusal> {
            let tick = testing::tick_at(&self.pair, None, now());
            let mut out = testing::diagnostics(&self.ctx);
            self.pricer.price(&tick, &mut out)
        }

        pub fn refusal(&mut self) -> Refusal {
            self.price().expect_err("the tick was priced")
        }

        pub fn preview(&mut self) -> Result<PricerOutput, Refusal> {
            let tick = testing::tick_at(&self.pair, None, now());
            let mut out = testing::diagnostics(&self.ctx);
            self.pricer.preview(&tick, &mut out)
        }
    }
}

#[cfg(test)]
mod tests {
    use quote_updater::testing;

    use super::{testkit::*, *};
    use crate::ports::{Event, StatusReading};

    /// A stanza with the `ondo` kind's defaults, changed by `edit`.
    fn stanza_with(edit: impl FnOnce(&mut Stanza<'static>)) -> Stanza<'static> {
        let mut stanza = Stanza {
            kind: "ondo",
            source: "Ondo",
            symbol: "AAPLon",
            delta: "0.003",
            price_poll: Duration::from_secs(1),
            status_poll: Some(Duration::from_secs(15)),
            max_price_age_secs: 15,
            max_unchanged_secs: 30,
            max_status_age_secs: 60,
            pause_lead_secs: 300,
            max_multiplier_gap: 0.001,
            skew: 0.0,
        };
        edit(&mut stanza);
        stanza
    }

    fn settings_with(edit: impl FnOnce(&mut Stanza<'static>)) -> Settings {
        Settings::new(&stanza_with(edit), &pair()).unwrap()
    }

    fn settings() -> Settings {
        settings_with(|_| {})
    }

    /// [`healthy`], with an issuer reporting the market in session and nothing on the asset.
    fn reads() -> Reads {
        healthy(Some(Box::new(in_session())))
    }

    /// [`healthy`], with the issuer's status as `edit` leaves [`in_session`].
    fn reads_with_status(edit: impl FnOnce(&mut StatusReading)) -> Reads {
        let mut status = in_session();
        edit(&mut status);
        healthy(Some(Box::new(status)))
    }

    fn priced(reads: Reads) -> Result<PricerOutput, Refusal> {
        built(pair(), settings(), reads, None).price()
    }

    fn scaled(value: &str) -> U256 {
        parse_fraction(value, 18).unwrap()
    }

    #[test]
    fn a_tick_publishes_the_sources_token_price_at_the_stanzas_spread() {
        let mut b = built(pair(), settings(), reads(), None);
        let out = b.price().unwrap();
        let tick = testing::tick(&b.pair, None);
        let out = testing::backstop(out, &tick).unwrap();
        assert_eq!(out.delta, U256::from(3_000_000_000_000_000u64), "30 bp");
        // AAPLon sorts before USDC, so the lane carries USDC per AAPLon as the source prices it.
        let mid = out.mid.as_u128() as f64 / 1e18;
        assert!((mid - 256.79).abs() < 1e-9, "{mid}");
    }

    #[test]
    fn an_inverted_lane_publishes_the_price_the_other_way_up() {
        let settings = Settings::new(&stanza_with(|_| {}), &inverted_pair()).unwrap();
        let mut b = built(inverted_pair(), settings, reads(), None);
        let mid = b.price().unwrap().mid.as_u128() as f64 / 1e18;
        assert!((mid - 1.0 / 256.79).abs() < 1e-12, "{mid}");
    }

    #[test]
    fn a_band_written_the_other_way_up_from_its_lane_is_caught() {
        let banded = |mut pair: PairShape, min: &str, max: &str| {
            pair.min_mid = Some(scaled(min));
            pair.max_mid = Some(scaled(max));
            pair
        };
        // USDC per token, as the source prices it, on a lane that carries token per USDC:
        // every tick's mid would be refused, and `--check` applies no band to say so.
        let wrong = banded(inverted_pair(), "150", "400");
        assert_eq!(band_of(256.79, &wrong), Band::Backwards);
        // The same band as the lane carries it.
        let right = banded(inverted_pair(), "0.0025", "0.0066667");
        assert_eq!(band_of(256.79, &right), Band::Inside);
        // On a lane that is not inverted, USDC per token is the lane's own orientation.
        let straight = banded(pair(), "150", "400");
        assert_eq!(band_of(256.79, &straight), Band::Inside);
        // A price outside the band either way up is the market, not the file: the core
        // withdraws it every tick, and the build only warns.
        assert_eq!(band_of(1_000.0, &straight), Band::Outside);
        assert_eq!(band_of(256.79, &inverted_pair()), Band::Inside, "no band");
    }

    #[test]
    fn a_price_older_than_its_limit_is_not_quoted() {
        let mut stale = reads();
        stale.price.as_mut().unwrap().at_ms = NOW_MS - 15_001;
        let refusal = built(pair(), settings(), stale, None).refusal();
        assert_eq!(refusal.reason(), "price_stale");

        let mut at_limit = reads();
        at_limit.price.as_mut().unwrap().at_ms = NOW_MS - 15_000;
        assert!(priced(at_limit).is_ok());
    }

    #[test]
    fn no_price_yet_is_not_quoted() {
        let mut none = reads();
        none.price = None;
        assert_eq!(priced(none).unwrap_err().reason(), "price_stale");
    }

    #[test]
    fn a_session_or_an_asset_the_issuer_rules_out_withdraws_under_its_reason() {
        let closed = reads_with_status(|status| {
            status.session.as_mut().unwrap().value = Err("Ondo reports the market closed".into());
        });
        let refusal = priced(closed).unwrap_err();
        assert_eq!(refusal.reason(), "market_session");
        assert_eq!(refusal.message(), "Ondo reports the market closed");
    }

    /// The issuer reports `events` on the asset, read a second ago.
    fn reads_with_events(events: Vec<Event>) -> Reads {
        reads_with_status(|status| {
            status.asset = Some(Stamped {
                at: read_at(),
                value: events.into(),
            })
        })
    }

    fn event(starts_ms: Option<i64>, moves_multiplier: bool) -> Event {
        Event {
            why: "ASSET_PAUSED: split".to_owned(),
            starts_ms,
            moves_multiplier,
        }
    }

    #[test]
    fn an_event_the_issuer_reports_stops_quoting_from_when_it_starts() {
        // Under way, or with no start given: now.
        let refusal = priced(reads_with_events(vec![event(None, false)])).unwrap_err();
        assert_eq!(refusal.reason(), "asset_event");
        assert_eq!(
            refusal.message(),
            "Ondo reports AAPLon: ASSET_PAUSED: split"
        );
        // One that leaves the multiplier alone: from its start.
        assert!(priced(reads_with_events(vec![event(Some(NOW_MS + 1), false)])).is_ok());
        assert!(priced(reads_with_events(vec![event(Some(NOW_MS), false)])).is_err());
        // One that moves it: from the lead (300s) before, as a corporate action on chain.
        assert!(priced(reads_with_events(vec![event(Some(NOW_MS + 300_001), true)])).is_ok());
        let refusal =
            priced(reads_with_events(vec![event(Some(NOW_MS + 300_000), true)])).unwrap_err();
        assert_eq!(refusal.reason(), "asset_event");
    }

    #[test]
    fn a_design_whose_issuer_reports_no_status_quotes_without_one() {
        assert!(priced(healthy(None)).is_ok());
    }

    #[test]
    fn quoting_stops_the_lead_before_a_scheduled_corporate_action_and_during_it() {
        let at = |starts_ms: i64| {
            let mut reads = reads();
            reads.shares.as_mut().unwrap().value.action = Some(Action {
                starts: (starts_ms / 1_000) as u64,
            });
            priced(reads)
        };
        // The lead is 300s.
        assert!(at(NOW_MS + 301_000).is_ok());
        let refusal = at(NOW_MS + 300_000).unwrap_err();
        assert_eq!(refusal.reason(), "multiplier_paused");
        assert!(
            refusal.message().contains("within the lead"),
            "{}",
            refusal.message()
        );
        let refusal = at(NOW_MS - 3_600_000).unwrap_err();
        assert_eq!(refusal.reason(), "multiplier_paused");
        assert!(refusal.message().contains("is on"), "{}", refusal.message());
    }

    #[test]
    fn the_sources_multiplier_and_the_tokens_must_agree() {
        // The source has applied a 2:1 split the chain has not: its token price is twice the
        // stock's.
        let mut split = reads();
        let price = split.price.as_mut().unwrap();
        price.stock = price.token / 2.0;
        assert_eq!(priced(split).unwrap_err().reason(), "multiplier_mismatch");

        // Within the default 0.1%, a rounded stock price is fine.
        let mut rounded = reads();
        rounded.price.as_mut().unwrap().stock *= 1.0009;
        assert!(priced(rounded).is_ok());
    }

    #[test]
    fn old_status_or_multiplier_readings_are_not_trusted() {
        let old = ago(60_001);
        let session = reads_with_status(|status| status.session.as_mut().unwrap().at = old);
        assert_eq!(priced(session).unwrap_err().reason(), "status_stale");
        let mut shares = reads();
        shares.shares.as_mut().unwrap().at = old;
        assert_eq!(priced(shares).unwrap_err().reason(), "status_stale");
        let unread = reads_with_status(|status| status.asset = None);
        assert_eq!(priced(unread).unwrap_err().reason(), "status_stale");
    }

    #[test]
    fn a_price_or_multiplier_that_is_not_a_positive_number_is_never_quoted() {
        // At the source: admission refuses it.
        let good = Price {
            token: 256.79,
            stock: 256.0,
            at_ms: NOW_MS,
        };
        for bad in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
            let token = Price { token: bad, ..good };
            assert!(admit(None, token, NOW_MS, "Ondo").is_err(), "token {bad}");
            let stock = Price { stock: bad, ..good };
            assert!(admit(None, stock, NOW_MS, "Ondo").is_err(), "stock {bad}");
        }
        // From the chain: a read whose multiplier is not a positive number is a failed read.
        for bad in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
            let shares = Shares {
                k: bad,
                action: None,
            };
            assert!(check_shares(shares).is_err(), "k {bad}");
        }
        assert!(
            check_shares(Shares {
                k: 1.002124,
                action: None
            })
            .is_ok()
        );
        // Held anyway: a gap that is not a number is no agreement.
        let mut nan_stock = reads();
        nan_stock.price.as_mut().unwrap().stock = f64::NAN;
        assert_eq!(
            priced(nan_stock).unwrap_err().reason(),
            "multiplier_mismatch"
        );
        let mut nan_k = reads();
        nan_k.shares.as_mut().unwrap().value.k = f64::NAN;
        assert_eq!(priced(nan_k).unwrap_err().reason(), "multiplier_mismatch");
    }

    #[test]
    fn a_readings_age_is_kept_by_the_monotonic_clock_whatever_the_wall_clock_does() {
        // The multiplier was read 61s ago and not since. The wall clock has stepped back 4s
        // meanwhile (within the price's skew), which by wall time would make it 57s old.
        let mut old = reads();
        old.shares.as_mut().unwrap().at = ago(61_000);
        let mut b = built(pair(), settings(), old, None);
        b.pricer.clock = || NOW_MS - 4_000;
        assert_eq!(b.refusal().reason(), "status_stale");
        // A reading a second old stays fresh across a step either way.
        for stepped in [|| NOW_MS - 4_000, || NOW_MS + 4_000] {
            let mut b = built(pair(), settings(), reads(), None);
            b.pricer.clock = stepped;
            assert!(b.price().is_ok());
        }
    }

    #[test]
    fn a_vault_long_the_stock_quotes_a_lower_mid() {
        // 1,000 AAPLon (~$256,790) against $85,597 of USDC: a 75% stock share.
        let inventory = testing::inventory(1_000.0, 256_790.0 / 3.0);
        let settings = settings_with(|stanza| stanza.skew = 0.02);
        let mut b = built(pair(), settings, reads(), Some(inventory));
        let mid = b.price().unwrap().mid.as_u128() as f64 / 1e18;
        // skew 0.02 × (0.75 − 0.5) = 0.5% below the source's price.
        assert!((mid - 256.79 * 0.995).abs() < 1e-6, "{mid}");
    }

    #[test]
    fn a_preview_prices_whatever_the_hour_and_says_so() {
        let mut reads = reads_with_status(|status| {
            status.session.as_mut().unwrap().value = Err("Ondo reports the market closed".into());
        });
        reads.price.as_mut().unwrap().at_ms = NOW_MS - 86_400_000;
        let mut b = built(pair(), settings(), reads, None);
        assert!(b.price().is_err(), "the run withdraws");
        assert!(b.preview().is_ok(), "the check prices");
    }

    #[test]
    fn the_stanza_is_held_to_the_cores_rules() {
        let pair = pair();
        assert!(Settings::new(&stanza_with(|_| {}), &pair).is_ok());
        type Edit = fn(&mut Stanza<'static>);
        let cases: [(Edit, &str); 20] = [
            (
                |s| s.price_poll = Duration::ZERO,
                "a price never waited for",
            ),
            (
                |s| s.status_poll = Some(Duration::ZERO),
                "a status never waited for",
            ),
            (|s| s.delta = "0", "zero delta"),
            (|s| s.delta = "0.1", "10% delta"),
            (|s| s.delta = "3bp", "not a number"),
            (|s| s.symbol = "", "no symbol"),
            (|s| s.max_status_age_secs = 15, "status always stale"),
            (
                |s| {
                    s.status_poll = None;
                    s.max_status_age_secs = 12;
                },
                "no issuer status, and a status age within the multiplier's 12s poll",
            ),
            (|s| s.max_price_age_secs = 1, "price always stale"),
            (|s| s.max_multiplier_gap = 0.0, "no gap allowed"),
            (|s| s.skew = 0.6, "skew over half"),
            (
                |s| s.pause_lead_secs = u64::MAX,
                "a lead that wraps negative",
            ),
            (|s| s.pause_lead_secs = 7 * 86_400 + 1, "a lead over a week"),
            (
                |s| s.max_price_age_secs = u64::MAX,
                "a price age that overflows",
            ),
            (|s| s.max_price_age_secs = 3_601, "a price over an hour old"),
            (|s| s.max_unchanged_secs = 1, "a price still at every poll"),
            (
                |s| s.max_unchanged_secs = 3_601,
                "a price still for over an hour",
            ),
            (
                |s| s.max_unchanged_secs = u64::MAX,
                "a stillness limit that overflows",
            ),
            (
                |s| s.max_status_age_secs = u64::MAX,
                "a status age that overflows",
            ),
            (
                |s| {
                    s.status_poll = Some(Duration::from_secs(3_601));
                    s.max_status_age_secs = 3_602;
                },
                "status read hourly",
            ),
        ];
        for (edit, why) in cases {
            let mut stanza = stanza_with(|_| {});
            edit(&mut stanza);
            assert!(Settings::new(&stanza, &pair).is_err(), "{why} was accepted");
        }
    }

    #[test]
    fn the_stanza_must_price_the_token_the_pair_lists_first() {
        // The label is the tokens' on-chain symbols, base first: a TSLAon stanza on a pair
        // whose first token is AMZNon would sell AMZNon at TSLA's price.
        let mut other = pair();
        other.label = "AMZNon/USDC".to_owned();
        let err = Settings::new(&stanza_with(|_| {}), &other).unwrap_err();
        assert!(format!("{err}").contains("AMZNon"), "{err}");
    }

    #[test]
    fn a_pair_with_a_venue_market_is_refused() {
        let mut with_market = pair();
        with_market.sources = 1;
        let err = Settings::new(&stanza_with(|_| {}), &with_market).unwrap_err();
        assert!(format!("{err}").contains("sources"), "{err}");
    }

    #[test]
    fn a_price_stamped_in_the_future_is_never_admitted() {
        let held = Price {
            token: 256.0,
            stock: 255.0,
            at_ms: NOW_MS - 1_000,
        };
        let newer = Price {
            at_ms: NOW_MS,
            ..held
        };
        let older = Price {
            at_ms: NOW_MS - 2_000,
            ..held
        };
        let admit = |held, fresh| admit(held, fresh, NOW_MS, "Ondo");
        assert_eq!(admit(None, newer), Ok(true));
        assert_eq!(admit(Some(held), newer), Ok(true));
        assert_eq!(
            admit(Some(held), older),
            Ok(false),
            "answers cross on the wire"
        );
        // Within the clock skew allowed, a stamp a little ahead is fine; beyond it, refused.
        let ahead = |ms| Price {
            at_ms: NOW_MS + ms,
            ..held
        };
        assert_eq!(admit(Some(held), ahead(MAX_CLOCK_SKEW_MS)), Ok(true));
        assert!(admit(Some(held), ahead(MAX_CLOCK_SKEW_MS + 1)).is_err());
        // Microseconds read as milliseconds land millennia ahead.
        assert!(admit(None, ahead(1_790_000_000_000_000)).is_err());
    }

    #[test]
    fn a_price_that_has_stood_still_too_long_is_not_quoted() {
        // A halted stock's price stops moving while its source keeps stamping it fresh.
        let mut still = reads();
        still.price_since_ms = Some(NOW_MS - 31_000);
        let refusal = priced(still).unwrap_err();
        assert_eq!(refusal.reason(), "price_frozen");
        let mut moving = reads();
        moving.price_since_ms = Some(NOW_MS - 29_000);
        assert!(priced(moving).is_ok());
    }

    #[test]
    fn a_price_stands_still_from_the_first_answer_with_its_value() {
        let first = Price {
            token: 256.0,
            stock: 255.0,
            at_ms: 1_000,
        };
        let held = hold(None, first);
        assert_eq!(held.since_ms, 1_000);
        let again = hold(
            Some(held),
            Price {
                at_ms: 2_000,
                ..first
            },
        );
        assert_eq!(again.since_ms, 1_000, "the same price, stamped later");
        assert_eq!(again.price.at_ms, 2_000, "aged by its latest stamp");
        let token_moved = Price {
            token: 256.01,
            at_ms: 3_000,
            ..first
        };
        assert_eq!(hold(Some(again), token_moved).since_ms, 3_000);
        let stock_moved = Price {
            stock: 255.01,
            at_ms: 3_000,
            ..first
        };
        assert_eq!(hold(Some(again), stock_moved).since_ms, 3_000);
    }

    #[test]
    fn a_held_price_from_the_future_is_not_quoted() {
        // Only if the clock stepped back after it was admitted; the tick refuses it anyway.
        let mut ahead = reads();
        ahead.price.as_mut().unwrap().at_ms = NOW_MS + 60_000;
        let refusal = priced(ahead).unwrap_err();
        assert_eq!(refusal.reason(), "price_stale");
        assert!(
            refusal.message().contains("future"),
            "{}",
            refusal.message()
        );
    }

    #[test]
    fn fractions_parse_exactly_at_the_pairs_scale() {
        assert_eq!(scaled("0.003"), U256::from(3_000_000_000_000_000u64));
        assert_eq!(scaled("0.000000000000000001"), U256::one());
        assert_eq!(parse_fraction("0.5", 6), Ok(U256::from(500_000)));
        for bad in [
            "",
            ".",
            "0.",
            ".5",
            "1e-3",
            "-0.1",
            "0.1.2",
            "0.0000000000000000001",
        ] {
            assert!(parse_fraction(bad, 18).is_err(), "{bad:?} parsed");
        }
    }
}
