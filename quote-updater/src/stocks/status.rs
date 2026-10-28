//! The issuer-status guard every tokenized-stock design registers (`ondo` today): a quote
//! guard that withdraws a stock pair's quote while the issuer says the token's price is
//! not what it seems. The price itself comes from the issuer as a venue (`venues/ondo.rs`
//! for Ondo), like any other pair's, so the composite's freshness and lateness checks, the
//! circuit breaker and the band apply to it unchanged. What this guard adds is what the
//! issuer knows and a price alone does not say. The inputs come through the ports
//! ([`crate::stocks::ports`]) from the adapters of the kind's design; the gates are the
//! same whoever the issuer:
//!
//! - **In session, nothing on the asset.** For a design whose issuer reports a status
//!   ([`MarketStatus`]): its readings are under `max_status_age_secs` old, and say the market
//!   is in a session the pair quotes and nothing stops the asset: no event under way, none
//!   that has reached its start, and none that will move the multiplier starting within
//!   `pause_lead_secs`.
//! - **No corporate action.** The token schedules none to start within `pause_lead_secs`,
//!   and none is under way ([`Valuation`]). A split or a dividend changes how many shares a
//!   token is worth, and the issuer's pause around it has no effect on chain: the token
//!   keeps trading through it, against anyone who has not stopped quoting.
//! - **Consistent.** The issuer's token price over its stock price (the `k` it prices with,
//!   read from the [`PriceSource`] on the status schedule) matches the token's `k` on chain
//!   within `max_multiplier_gap`. A gap means one of the two has applied a corporate action
//!   the other has not.
//!
//! The last two read the token's `k` from the design's [`Valuation`], which lists some
//! tokens and not others (Ondo's oracle holds entries for a few of its GM tokens). A pair
//! whose token it does not list runs without those two gates: the build says so, and reads
//! the valuation again on the next reload. The issuer's status still reports the asset's
//! events, corporate actions included.
//!
//! The stock token is listed first (`tokens = [stock, USDC]`), and the build checks it is the
//! token the stanza's `symbol` names by the token's own `symbol()`.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{Address, eyre, prelude::*};

use crate::{
    poll::{self, Failures, Stamped, lock, wall_clock_ms},
    stocks::{
        chain,
        ports::{MarketStatus, Price, PriceSource, Shares, StatusFeed, Valuation},
    },
};

/// The oldest a stanza may let a status reading be.
const MAX_READING_AGE_SECS: u64 = 3_600;
/// The longest lead a stanza may stop quoting ahead of a corporate action.
const MAX_PAUSE_LEAD_SECS: u64 = 7 * 86_400;

/// How often the token's `k` is read: once a block.
const SHARES_POLL: Duration = Duration::from_secs(12);

/// A stock stanza as the core reads it: the keys every kind's stanza has, whatever its
/// design adds. Each kind fills it from a stanza of its own, which refuses unknown keys:
/// [`Self::new`] with the core's defaults, then each key the kind lets the operator set.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Stanza<'a> {
    /// The kind, as messages name it: `ondo`.
    pub kind: &'static str,
    /// Who prices the token, as messages name it: `Ondo`.
    pub source: &'static str,
    /// The stock token's `symbol()`.
    pub symbol: &'a str,
    /// How often the issuer's status is read, for a design whose issuer reports one; the
    /// issuer's price ratio is read on the same schedule.
    pub status_poll: Option<Duration>,
    /// The oldest status or `k` reading that is trusted.
    pub max_status_age_secs: u64,
    /// How long before a scheduled corporate action quoting stops.
    pub pause_lead_secs: u64,
    /// The largest relative gap between the source's `k` and the token's.
    pub max_multiplier_gap: f64,
}

impl<'a> Stanza<'a> {
    /// A stanza for `symbol`, priced by `source` under the kind `kind`, with the core's
    /// defaults for everything else: the status read every 15s and trusted, like the
    /// multiplier, for 60s; quoting stopped 300s ahead of a corporate action; a 0.1%
    /// multiplier gap. The `ondo` kind documents the same numbers.
    pub fn new(kind: &'static str, source: &'static str, symbol: &'a str) -> Stanza<'a> {
        Stanza {
            kind,
            source,
            symbol,
            status_poll: Some(Duration::from_secs(15)),
            max_status_age_secs: 60,
            pause_lead_secs: 300,
            max_multiplier_gap: 0.001,
        }
    }
}

/// A stanza, checked against its pair: what `validate` proves and `build` uses.
#[derive(Clone, Debug)]
pub struct Settings {
    kind: &'static str,
    source: &'static str,
    symbol: String,
    /// How often the issuer's price ratio is read: the status period, or 15s for a design
    /// whose issuer reports no status.
    ratio_poll: Duration,
    max_status_age: Duration,
    pause_lead_secs: i64,
    max_multiplier_gap: f64,
}

impl Settings {
    /// Reads a stanza's settings and checks them against the pair they guard.
    pub fn new(s: &Stanza<'_>, pair: &PairShape) -> eyre::Result<Settings> {
        eyre::ensure!(
            pair.sources > 0,
            "an `{}` pair takes its price from its sources, and this pair has none: add {} \
             as a venue (`sources = [{{ venue = \"{}\", symbol = \"{}\" }}]`)",
            s.kind,
            s.source,
            s.kind,
            s.symbol
        );
        eyre::ensure!(!s.symbol.is_empty(), "symbol is empty");
        // The label is the tokens' on-chain symbols, base first, when they were read; it is
        // `lane 0x…` when they were not, as for every halted pair, which is never read. The
        // build ties the stanza to the token's own symbol() either way; this catches a
        // mismatch the label already shows without waiting for it.
        if let Some((first, _)) = pair.label.split_once('/') {
            check_symbol(s.symbol, first)?;
        }
        eyre::ensure!(
            s.max_status_age_secs <= MAX_READING_AGE_SECS,
            "max_status_age_secs {} is over an hour: a reading that old says nothing about now",
            s.max_status_age_secs
        );
        eyre::ensure!(
            s.pause_lead_secs <= MAX_PAUSE_LEAD_SECS,
            "pause_lead_secs {} is over a week",
            s.pause_lead_secs
        );
        eyre::ensure!(
            s.status_poll.is_none_or(|every| !every.is_zero()),
            "the status poll period is zero"
        );
        let ratio_poll = s.status_poll.unwrap_or(Duration::from_secs(15));
        let slowest_reading = ratio_poll.max(SHARES_POLL);
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
        Ok(Settings {
            kind: s.kind,
            source: s.source,
            symbol: s.symbol.to_owned(),
            ratio_poll,
            max_status_age: Duration::from_secs(s.max_status_age_secs),
            pause_lead_secs: s.pause_lead_secs as i64,
            max_multiplier_gap: s.max_multiplier_gap,
        })
    }
}

/// That the stanza's `symbol` is the pair's first token's, `first` (its on-chain symbol()),
/// in any case, as every symbol compare in the stocks module. It is what stops a TSLAon
/// stanza guarding AMZNon.
fn check_symbol(symbol: &str, first: &str) -> eyre::Result<()> {
    eyre::ensure!(
        first.eq_ignore_ascii_case(symbol),
        "symbol {symbol} is not the pair's first token, whose symbol() is {first}: the stock \
         token is listed first (tokens = [stock, USDC]) and the stanza guards it"
    );
    Ok(())
}

/// `price` as a reading the core can judge: both prices positive numbers, so the ratio
/// is one. Anything else is a broken read, refused like a failed one.
fn check_price(price: Price) -> eyre::Result<Price> {
    let positive = |value: f64| value.is_finite() && value > 0.0;
    eyre::ensure!(
        positive(price.token) && positive(price.stock),
        "a price that is not a positive number (token {}, stock {})",
        price.token,
        price.stock
    );
    Ok(price)
}

/// `shares` as a reading the core can judge: `k` a positive number, as every token's is.
/// Anything else is a broken read, refused like a failed one.
fn check_shares(shares: Shares) -> eyre::Result<Shares> {
    eyre::ensure!(
        shares.k.is_finite() && shares.k > 0.0,
        "the token's share multiplier {} is not a positive number",
        shares.k
    );
    Ok(shares)
}

/// What the pair's own readers leave for `check` to read: `Copy`, so a tick copies it out
/// of the lock and holds nothing. Both are read only for a token the valuation lists.
#[derive(Clone, Copy, Debug, Default)]
struct Feeds {
    /// The issuer's latest token and stock prices, for the `k` it prices with.
    ratio: Option<Stamped<Price>>,
    shares: Option<Stamped<Shares>>,
}

/// The reasons and numbers the guard declares.
struct Handles {
    status_stale: RefusalHandle,
    multiplier_stale: RefusalHandle,
    market_session: RefusalHandle,
    asset_event: RefusalHandle,
    multiplier_paused: RefusalHandle,
    multiplier_mismatch: RefusalHandle,
    svalue: DiagHandle,
    multiplier_gap: DiagHandle,
}

impl Handles {
    fn declare(ctx: &mut BuildCtx) -> eyre::Result<Handles> {
        Ok(Handles {
            status_stale: ctx.refusal("status_stale")?,
            multiplier_stale: ctx.refusal("multiplier_stale")?,
            market_session: ctx.refusal("market_session")?,
            asset_event: ctx.refusal("asset_event")?,
            multiplier_paused: ctx.refusal("multiplier_paused")?,
            multiplier_mismatch: ctx.refusal("multiplier_mismatch")?,
            svalue: ctx.diagnostic("svalue")?,
            multiplier_gap: ctx.diagnostic("multiplier_gap")?,
        })
    }
}

/// A stock pair's issuer-status guard, whatever the design behind the token.
pub struct StatusGuard {
    settings: Settings,
    /// What the issuer's status may call the asset: the token's symbol, and the stock's
    /// ticker when the price source names it.
    names: Vec<String>,
    feeds: Arc<Mutex<Feeds>>,
    /// Whether the valuation source lists the token, decided at the build. When it does
    /// not, the multiplier gates (its freshness, its corporate-action pause, its agreement
    /// with the price source) do not run and nothing reads it.
    checks_multiplier: bool,
    /// The issuer's status, for a design whose issuer reports one.
    status: Option<Box<dyn MarketStatus>>,
    h: Handles,
    /// Milliseconds since the epoch now: the wall clock in the run, a fixed instant in a test.
    clock: fn() -> i64,
}

impl StatusGuard {
    /// `reading`, if there is one and it is recent enough to trust; refused under `stale`
    /// otherwise, which names where the reading comes from (the issuer's status, the
    /// chain's multiplier), since each fails on its own.
    fn fresh<'a, T>(
        &self,
        reading: Option<&'a Stamped<T>>,
        what: &str,
        stale: &RefusalHandle,
        now: Instant,
    ) -> Result<&'a Stamped<T>, Refusal> {
        let Some(reading) = reading else {
            return Err(Refusal::new(stale, format!("no {what} reading yet")));
        };
        let age = now.saturating_duration_since(reading.at);
        if age > self.settings.max_status_age {
            return Err(Refusal::new(
                stale,
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
    fn check_status(&self, now: Instant, now_ms: i64) -> Result<(), Refusal> {
        let Some(status) = &self.status else {
            return Ok(());
        };
        let reading = status.read(&self.names);
        let stale = &self.h.status_stale;
        let session = self.fresh(reading.session.as_ref(), "market session", stale, now)?;
        let asset = self.fresh(reading.asset.as_ref(), "asset status", stale, now)?;
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

    /// The multiplier gates, for a token the valuation source lists: its reading and the
    /// issuer's price ratio are fresh, no corporate action is on or within the lead, and
    /// the issuer prices with the same `k` as the chain holds.
    fn check_multiplier(
        &self,
        now: Instant,
        now_ms: i64,
        out: &mut Diagnostics,
        feeds: Feeds,
    ) -> Result<(), Refusal> {
        let s = &self.settings;
        let h = &self.h;
        let reading = self.fresh(
            feeds.shares.as_ref(),
            "share multiplier",
            &h.multiplier_stale,
            now,
        )?;
        let shares = reading.value;
        out.set(&h.svalue, shares.k);
        let now_secs = now_ms.div_euclid(1_000);
        if let Some(action) = shares
            .action
            .filter(|action| action.stops_quoting(now_secs, s.pause_lead_secs))
        {
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
        let ratio = self.fresh(
            feeds.ratio.as_ref(),
            "issuer price",
            &h.multiplier_stale,
            now,
        )?;
        let price = ratio.value;
        let gap = price.token / price.stock / shares.k - 1.0;
        out.set(&h.multiplier_gap, gap);
        // Not `gap.abs() > max`, which a gap that is not a number passes: this is false for one.
        if gap.abs() <= s.max_multiplier_gap {
            return Ok(());
        }
        Err(Refusal::new(
            &h.multiplier_mismatch,
            format!(
                "{} prices {} at {:.6} times the stock, the chain's multiplier is {:.6}: a \
                 {:.3}% gap (limit {:.3}%)",
                s.source,
                s.symbol,
                price.token / price.stock,
                shares.k,
                gap * 100.0,
                s.max_multiplier_gap * 100.0
            ),
        ))
    }

    fn judge(&self, now: Instant, out: &mut Diagnostics) -> Result<(), Refusal> {
        let now_ms = (self.clock)();
        self.check_status(now, now_ms)?;
        if self.checks_multiplier {
            let feeds = *lock(&self.feeds);
            self.check_multiplier(now, now_ms, out, feeds)?;
        }
        Ok(())
    }
}

impl QuoteGuard for StatusGuard {
    /// Every tick, whatever the pricer did: a refusal here withdraws the quote, and on a
    /// tick already refused it says the issuer's reason beside the core's.
    fn check(&mut self, candidate: &Candidate<'_>, out: &mut Diagnostics) -> Gate {
        match self.judge(candidate.tick().now(), out) {
            Ok(()) => Gate::Allow,
            Err(refusal) => Gate::Withdraw(refusal),
        }
    }
}

/// What a kind hands the core to build its guard: its design's adapters, from
/// [`Self::new`].
#[non_exhaustive]
pub struct Adapters {
    /// The issuer's own price for the token and its stock: read once at the build, for the
    /// stock's ticker, and on the status schedule for the `k` the issuer prices with, when
    /// the valuation lists the token. The price the pair quotes is the venue's, not this.
    pub prices: Box<dyn PriceSource>,
    /// The token's shares per token, and its corporate actions.
    pub valuation: Box<dyn Valuation>,
    /// Opens the issuer's status, for a design whose issuer reports one. The build awaits it
    /// after the first price, as one of its reads: a wrong key or an answer that does not
    /// parse fails the build, not every tick. Opening it starts nothing: the build runs its
    /// refresh as one of the pair's tasks.
    pub status: Option<BoxFuture<'static, eyre::Result<StatusFeed>>>,
}

impl Adapters {
    /// The design's price source and valuation, with no issuer status: a design whose
    /// issuer reports one sets `status`.
    pub fn new(prices: Box<dyn PriceSource>, valuation: Box<dyn Valuation>) -> Adapters {
        Adapters {
            prices,
            valuation,
            status: None,
        }
    }
}

/// Builds the pair's guard from its design's adapters: reads everything once, so a wrong
/// key, symbol, oracle or token fails this build (and `--check`) instead of withdrawing every
/// tick, then starts the tasks that keep the readings current.
pub async fn build(
    ctx: &mut BuildCtx,
    settings: Settings,
    adapters: Adapters,
) -> eyre::Result<StatusGuard> {
    let Adapters {
        prices,
        valuation,
        status,
    } = adapters;
    let chain = ctx.chain()?;
    let pair = ctx.pair().clone();
    let token = pair.tokens.0;
    let h = Handles::declare(ctx)?;

    // The stanza guards the token the pair lists first, by that token's own symbol(): the
    // label `validate` saw is the lane's whenever the library had not read it.
    let symbol = chain::read_symbol(&chain, token)
        .await
        .map_err(first_token("the symbol"))?;
    check_symbol(&settings.symbol, &symbol)?;

    let first = prices.fetch().await?;
    let price = check_price(first.price).map_err(|err| {
        eyre::eyre!(
            "{}'s price for {}: {err:#}",
            settings.source,
            settings.symbol
        )
    })?;
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
        .and_then(|listed| listed.map(check_shares).transpose())
        .map_err(first_token("the share multiplier"))?;
    // Not a failed build: the valuation source lists some tokens and not others, and a
    // pair whose token it does not list runs without the multiplier gates. Said here so
    // `--check` and the startup log show it.
    if shares.is_none() {
        tracing::warn!(
            "[{}] {}: the valuation source does not list {}, so its share multiplier is not \
             checked against {}'s and no corporate-action pause is read from it; {}'s own \
             status still reports the asset's events. Read again on the next reload.",
            pair.label,
            settings.kind,
            settings.symbol,
            settings.source,
            settings.source
        );
    }
    let checks_multiplier = shares.is_some();
    let feeds = Arc::new(Mutex::new(Feeds {
        ratio: Some(Stamped::now(price)),
        shares: shares.map(Stamped::now),
    }));
    let names = std::iter::once(settings.symbol.clone())
        .chain(first.ticker.filter(|ticker| !ticker.is_empty()))
        .collect();

    if checks_multiplier {
        ctx.spawn(poll_ratio(prices, settings.clone(), Arc::clone(&feeds)));
        ctx.spawn(poll_shares(valuation, token, Arc::clone(&feeds)));
    }
    let guard = StatusGuard {
        settings,
        names,
        feeds,
        checks_multiplier,
        status,
        h,
        clock: wall_clock_ms,
    };
    // Not a failed build: the quote starts withdrawn and resumes once the issuer's answers
    // change. Said here so `--check` shows it, as the restriction guard says its own.
    let mut out = crate::pricing::Diagnostics::new(ctx.diagnostic_names().len());
    if let Err(refusal) = guard.judge(Instant::now(), &mut out) {
        tracing::warn!(
            "[{}] {}: the quote starts withdrawn ({}): {}",
            pair.label,
            guard.settings.kind,
            refusal.reason(),
            refusal.message()
        );
    }
    Ok(guard)
}

async fn poll_ratio(prices: Box<dyn PriceSource>, s: Settings, feeds: Arc<Mutex<Feeds>>) {
    let mut failures = Failures::new(format!("{} price for {}", s.source, s.symbol));
    let mut ticks = poll::every(s.ratio_poll);
    loop {
        ticks.tick().await;
        match prices
            .fetch()
            .await
            .and_then(|fetched| check_price(fetched.price))
        {
            Ok(price) => {
                lock(&feeds).ratio = Some(Stamped::now(price));
                failures.succeeded();
            }
            Err(err) => failures.failed(&err),
        }
    }
}

async fn poll_shares(valuation: Box<dyn Valuation>, token: Address, feeds: Arc<Mutex<Feeds>>) {
    let mut failures = Failures::new(format!("share multiplier of {token:#x}"));
    let mut ticks = poll::every(SHARES_POLL);
    loop {
        ticks.tick().await;
        // A token the source listed at the build and no longer does is read as a failed
        // read: the last reading ages out and the pair withdraws, as on any outage, until
        // a reload decides afresh.
        let read = valuation.read().await.and_then(|listed| {
            listed
                .ok_or_else(|| eyre::eyre!("the valuation source no longer lists the token"))
                .and_then(check_shares)
        });
        match read {
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

/// What the kinds' tests build a guard from: a pair, its settings, the readings its tasks
/// would have left, and the design's status adapter, without a chain or a task.
#[cfg(test)]
pub mod testkit {
    #![allow(missing_docs)]

    use crate::testing;

    use super::*;
    use crate::{stocks::chain::parse_address, stocks::ports::StatusReading};

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

    /// The pair as the run shapes it: AAPLon first, so the lane is not inverted, with one
    /// venue, the issuer's.
    pub fn pair() -> PairShape {
        let pair = testing::pair_for(
            "AAPLon/USDC",
            (parse_address(AAPLON).unwrap(), parse_address(USDC).unwrap()),
            Address::repeat_byte(0x77),
            18,
        );
        assert!(!pair.inverted, "AAPLon sorts before USDC");
        assert_eq!(pair.sources, 1);
        pair
    }

    /// What a tick reads: the pair's own readings, and the issuer's status, if any.
    pub struct Reads {
        /// The issuer's latest token and stock prices, for the `k` it prices with.
        pub ratio: Option<Stamped<Price>>,
        pub shares: Option<Stamped<Shares>>,
        /// Whether the valuation source lists the token, as the build decided.
        pub listed: bool,
        pub status: Option<Box<dyn MarketStatus>>,
    }

    /// Everything a tick reads, consistent, with `status` for the issuer's: $256.79 a token,
    /// a multiplier of 1.002124 on chain and in the issuer's price, no corporate action, all
    /// read a second ago.
    pub fn healthy(status: Option<Box<dyn MarketStatus>>) -> Reads {
        Reads {
            ratio: Some(Stamped {
                at: read_at(),
                value: Price {
                    token: 256.79,
                    stock: 256.79 / 1.002124,
                    at_ms: NOW_MS - 1_000,
                },
            }),
            shares: Some(Stamped {
                at: read_at(),
                value: Shares {
                    k: 1.002124,
                    action: None,
                },
            }),
            listed: true,
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
        pub guard: StatusGuard,
        pub ctx: BuildCtx,
        pub pair: PairShape,
    }

    /// The guard as the build leaves it, for AAPLon, whose stock the issuer names AAPL.
    pub fn built(pair: PairShape, settings: Settings, reads: Reads) -> Built {
        let mut ctx = testing::build_ctx(&pair);
        let h = Handles::declare(&mut ctx).unwrap();
        Built {
            guard: StatusGuard {
                settings,
                names: vec!["AAPLon".to_owned(), "AAPL".to_owned()],
                feeds: Arc::new(Mutex::new(Feeds {
                    ratio: reads.ratio,
                    shares: reads.shares,
                })),
                checks_multiplier: reads.listed,
                status: reads.status,
                h,
                clock: || NOW_MS,
            },
            ctx,
            pair,
        }
    }

    impl Built {
        /// One tick at [`now`], on a priced candidate: `Allow`, or the withdrawal.
        pub fn check(&mut self) -> Result<(), Refusal> {
            let market = testing::market_at(&self.pair, 256.79, 0.003, now());
            let tick = testing::tick_at(&self.pair, Some(&market), now());
            let priced = PricerOutput::new(U256::from(3_000_000_000_000_000u64), market.mid);
            let seen = testing::diagnostics(&self.ctx);
            let mut out = testing::diagnostics(&self.ctx);
            match self
                .guard
                .check(&testing::candidate(&tick, Ok(&priced), &seen), &mut out)
            {
                Gate::Allow => Ok(()),
                Gate::Withdraw(refusal) => Err(refusal),
                Gate::Halt(reason) => panic!("the guard halted: {reason:?}"),
            }
        }

        pub fn refusal(&mut self) -> Refusal {
            self.check().expect_err("the tick was allowed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{testkit::*, *};
    use crate::stocks::ports::{Action, StatusReading, Stoppage};

    /// A stanza with the `ondo` kind's defaults, changed by `edit`.
    fn stanza_with(edit: impl FnOnce(&mut Stanza<'static>)) -> Stanza<'static> {
        let mut stanza = Stanza {
            kind: "ondo",
            source: "Ondo",
            symbol: "AAPLon",
            status_poll: Some(Duration::from_secs(15)),
            max_status_age_secs: 60,
            pause_lead_secs: 300,
            max_multiplier_gap: 0.001,
        };
        edit(&mut stanza);
        stanza
    }

    fn settings() -> Settings {
        Settings::new(&stanza_with(|_| {}), &pair()).unwrap()
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

    fn judged(reads: Reads) -> Result<(), Refusal> {
        built(pair(), settings(), reads).check()
    }

    #[test]
    fn a_healthy_tick_is_allowed() {
        assert!(judged(reads()).is_ok());
    }

    #[test]
    fn a_session_or_an_asset_the_issuer_rules_out_withdraws_under_its_reason() {
        let closed = reads_with_status(|status| {
            status.session.as_mut().unwrap().value = Err("Ondo reports the market closed".into());
        });
        let refusal = judged(closed).unwrap_err();
        assert_eq!(refusal.reason(), "market_session");
        assert_eq!(refusal.message(), "Ondo reports the market closed");
    }

    fn reads_with_events(events: Vec<Stoppage>) -> Reads {
        reads_with_status(|status| {
            status.asset = Some(Stamped {
                at: read_at(),
                value: events.into(),
            })
        })
    }

    fn event(starts_ms: Option<i64>, moves_multiplier: bool) -> Stoppage {
        let mut event = Stoppage::new("ASSET_PAUSED: split", moves_multiplier);
        event.starts_ms = starts_ms;
        event
    }

    #[test]
    fn an_event_the_issuer_reports_stops_quoting_from_when_it_starts() {
        // Under way, or with no start the issuer gives: now.
        let refusal = judged(reads_with_events(vec![event(None, false)])).unwrap_err();
        assert_eq!(refusal.reason(), "asset_event");
        assert_eq!(
            refusal.message(),
            "Ondo reports AAPLon: ASSET_PAUSED: split"
        );
        // One that leaves the multiplier alone: from its start.
        assert!(judged(reads_with_events(vec![event(Some(NOW_MS + 1), false)])).is_ok());
        assert!(judged(reads_with_events(vec![event(Some(NOW_MS), false)])).is_err());
        // One that moves it: from the lead (300s) before, as a corporate action on chain.
        assert!(judged(reads_with_events(vec![event(Some(NOW_MS + 300_001), true)])).is_ok());
        let refusal =
            judged(reads_with_events(vec![event(Some(NOW_MS + 300_000), true)])).unwrap_err();
        assert_eq!(refusal.reason(), "asset_event");
    }

    #[test]
    fn a_design_whose_issuer_reports_no_status_judges_without_one() {
        assert!(judged(healthy(None)).is_ok());
    }

    #[test]
    fn quoting_stops_the_lead_before_a_scheduled_corporate_action_and_during_it() {
        let at = |starts_ms: i64| {
            let mut reads = reads();
            reads.shares.as_mut().unwrap().value.action = Some(Action {
                starts: (starts_ms / 1_000) as u64,
            });
            judged(reads)
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
    fn the_issuers_multiplier_and_the_tokens_must_agree() {
        // The issuer has applied a 2:1 split the chain has not: its token price is twice
        // the stock's.
        let mut split = reads();
        let price = &mut split.ratio.as_mut().unwrap().value;
        price.stock = price.token / 2.0;
        assert_eq!(judged(split).unwrap_err().reason(), "multiplier_mismatch");

        // Within the default 0.1%, a rounded stock price is fine.
        let mut rounded = reads();
        rounded.ratio.as_mut().unwrap().value.stock *= 1.0009;
        assert!(judged(rounded).is_ok());
    }

    /// The valuation source lists some tokens and not others. For one it does not list
    /// the build reads no multiplier, and nothing that needs one withdraws the quote: not
    /// its absence, not an issuer price whose `k` could not be compared.
    #[test]
    fn a_token_the_valuation_source_does_not_list_is_judged_without_the_multiplier_gates() {
        let unlisted = || {
            let mut reads = reads();
            reads.listed = false;
            reads.shares = None;
            reads.ratio = None;
            reads
        };
        assert!(judged(unlisted()).is_ok());
        // What would be a mismatch on a listed token.
        let mut split = unlisted();
        split.ratio = reads().ratio;
        let price = &mut split.ratio.as_mut().unwrap().value;
        price.stock = price.token / 2.0;
        assert!(judged(split).is_ok());
        // The other gates stand.
        let mut closed = unlisted();
        let mut status = in_session();
        status.session.as_mut().unwrap().value = Err("closed".to_owned());
        closed.status = Some(Box::new(status));
        assert_eq!(judged(closed).unwrap_err().reason(), "market_session");
    }

    #[test]
    fn old_status_or_multiplier_readings_are_not_trusted() {
        let old = ago(60_001);
        let session = reads_with_status(|status| status.session.as_mut().unwrap().at = old);
        assert_eq!(judged(session).unwrap_err().reason(), "status_stale");
        let unread = reads_with_status(|status| status.asset = None);
        assert_eq!(judged(unread).unwrap_err().reason(), "status_stale");
        // The multiplier is read from the chain, not the issuer: an oracle outage must not
        // read as an issuer-status outage on the dashboards.
        let mut shares = reads();
        shares.shares.as_mut().unwrap().at = old;
        assert_eq!(judged(shares).unwrap_err().reason(), "multiplier_stale");
        let mut unread = reads();
        unread.shares = None;
        assert_eq!(judged(unread).unwrap_err().reason(), "multiplier_stale");
        // So must the issuer's price the ratio is read from.
        let mut ratio = reads();
        ratio.ratio.as_mut().unwrap().at = old;
        assert_eq!(judged(ratio).unwrap_err().reason(), "multiplier_stale");
    }

    #[test]
    fn a_price_or_multiplier_that_is_not_a_positive_number_is_a_failed_read() {
        let good = Price {
            token: 256.79,
            stock: 256.0,
            at_ms: NOW_MS,
        };
        for bad in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
            assert!(
                check_price(Price { token: bad, ..good }).is_err(),
                "token {bad}"
            );
            assert!(
                check_price(Price { stock: bad, ..good }).is_err(),
                "stock {bad}"
            );
            assert!(
                check_shares(Shares {
                    k: bad,
                    action: None,
                })
                .is_err(),
                "k {bad}"
            );
        }
        assert!(check_price(good).is_ok());
        assert!(
            check_shares(Shares {
                k: 1.002124,
                action: None
            })
            .is_ok()
        );
        // Held anyway: a gap that is not a number is no agreement.
        let mut nan_k = reads();
        nan_k.shares.as_mut().unwrap().value.k = f64::NAN;
        assert_eq!(judged(nan_k).unwrap_err().reason(), "multiplier_mismatch");
    }

    #[test]
    fn a_readings_age_is_kept_by_the_monotonic_clock_whatever_the_wall_clock_does() {
        // The multiplier was read 61s ago and not since. The wall clock has stepped back 4s
        // meanwhile, which by wall time would make it 57s old.
        let mut old = reads();
        old.shares.as_mut().unwrap().at = ago(61_000);
        let mut b = built(pair(), settings(), old);
        b.guard.clock = || NOW_MS - 4_000;
        assert_eq!(b.refusal().reason(), "multiplier_stale");
        // A reading a second old stays fresh across a step either way.
        for stepped in [|| NOW_MS - 4_000, || NOW_MS + 4_000] {
            let mut b = built(pair(), settings(), reads());
            b.guard.clock = stepped;
            assert!(b.check().is_ok());
        }
    }

    #[test]
    fn the_stanza_is_held_to_the_cores_rules() {
        let pair = pair();
        assert!(Settings::new(&stanza_with(|_| {}), &pair).is_ok());
        type Edit = fn(&mut Stanza<'static>);
        let cases: [(Edit, &str); 7] = [
            (
                |s| s.status_poll = Some(Duration::ZERO),
                "a status never waited for",
            ),
            (|s| s.symbol = "", "no symbol"),
            (|s| s.max_status_age_secs = 15, "status always stale"),
            (
                |s| s.max_status_age_secs = 3_601,
                "status trusted over an hour",
            ),
            (|s| s.pause_lead_secs = 8 * 86_400, "a lead over a week"),
            (|s| s.max_multiplier_gap = 0.0, "no gap allowed"),
            (|s| s.max_multiplier_gap = 0.05, "a 5% gap"),
        ];
        for (edit, what) in cases {
            assert!(
                Settings::new(&stanza_with(edit), &pair).is_err(),
                "{what} was accepted"
            );
        }
        // A design whose issuer reports no status reads its price ratio every 15s, and the
        // readings must still be trusted longer than that.
        assert!(Settings::new(&stanza_with(|s| s.status_poll = None), &pair).is_ok());
        assert!(
            Settings::new(
                &stanza_with(|s| {
                    s.status_poll = None;
                    s.max_status_age_secs = 12;
                }),
                &pair
            )
            .is_err()
        );
    }

    #[test]
    fn the_stanza_must_guard_the_token_the_pair_lists_first() {
        let mut pair = pair();
        pair.label = "USDC/AAPLon".to_owned();
        let err = Settings::new(&stanza_with(|_| {}), &pair).unwrap_err();
        assert!(format!("{err:#}").contains("listed first"), "{err:#}");
        // The symbol matches in any case.
        pair.label = "aaplon/USDC".to_owned();
        assert!(Settings::new(&stanza_with(|_| {}), &pair).is_ok());
        // A pair labelled by its lane (never read) is checked at the build instead.
        pair.label = "lane 0x1234".to_owned();
        assert!(Settings::new(&stanza_with(|_| {}), &pair).is_ok());
    }

    #[test]
    fn a_pair_with_no_venue_is_refused() {
        let mut pair = pair();
        pair.sources = 0;
        let err = Settings::new(&stanza_with(|_| {}), &pair).unwrap_err();
        assert!(format!("{err:#}").contains("sources"), "{err:#}");
    }
}
