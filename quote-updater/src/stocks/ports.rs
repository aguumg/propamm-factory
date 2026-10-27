//! What the stock core asks of a tokenized-stock design, and the shape of each
//! answer. A design answers through adapters of its own (`issuers/ondo/` for Ondo's Global
//! Markets tokens); the core judges every answer the same way, whoever gave it:
//!
//! - [`PriceSource`]: the token's price, polled.
//! - [`Valuation`]: how the token's price follows its stock (one token is worth `k` shares),
//!   and the corporate action that will stop that holding.
//! - [`MarketStatus`]: what the issuer says about the market and the asset, for a design
//!   whose issuer says anything (Ondo's session and asset events).
//! - [`Restrictions`]: what would stop the token moving for the parties of a fill.
//!
//! Each read is a [`BoxFuture`], so the core holds adapters as trait objects: a new design is
//! a module of adapters, and the kinds a binary registers.

use std::sync::Arc;

use crate::{Address, eyre, prelude::BoxFuture};

pub use crate::poll::Stamped;

/// A token's price, as a source quotes it.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct Price {
    /// The token's price in USD: the stock's price times the token's `k`.
    pub token: f64,
    /// The underlying stock's price in USD, against which the core checks `k`.
    pub stock: f64,
    /// When the source priced it, in milliseconds since the epoch.
    pub at_ms: i64,
}

impl Price {
    /// The token at `token` and its stock at `stock`, in USD, as the source stamped them
    /// at `at_ms` (milliseconds since the epoch, on the wall clock).
    pub fn new(token: f64, stock: f64, at_ms: i64) -> Price {
        Price {
            token,
            stock,
            at_ms,
        }
    }
}

/// One read of a [`PriceSource`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Fetched {
    /// The prices, and when the source stamped them.
    pub price: Price,
    /// The underlying stock's ticker, when the source names it: the asset's other name in
    /// what the issuer reports (see [`MarketStatus::read`]).
    pub ticker: Option<String>,
}

impl Fetched {
    /// A read of `price`, with no ticker named: a source that names one sets `ticker`.
    pub fn new(price: Price) -> Fetched {
        Fetched {
            price,
            ticker: None,
        }
    }
}

/// Where the token's price comes from. Polled by a task of the core's.
pub trait PriceSource: Send + Sync + 'static {
    /// The latest price.
    fn fetch(&self) -> BoxFuture<'_, eyre::Result<Fetched>>;
}

/// How the token's price follows its stock now.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct Shares {
    /// Shares per token: the token is worth the stock's price times this.
    pub k: f64,
    /// The corporate action the token has scheduled, if any.
    pub action: Option<Action>,
}

impl Shares {
    /// `k` shares per token, with no corporate action scheduled: a token that has one
    /// sets `action`.
    pub fn new(k: f64) -> Shares {
        Shares { k, action: None }
    }
}

/// A corporate action the token has scheduled: a pause the issuer starts at `starts`
/// (seconds since the epoch) and lifts when it is done, which the next reading shows as no
/// action, with the multiplier after the change. Quoting stops a lead before it starts and
/// stays stopped until then.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Action {
    /// When it starts, in seconds since the epoch.
    pub starts: u64,
}

impl Action {
    /// One starting at `starts`, in seconds since the epoch.
    pub fn new(starts: u64) -> Action {
        Action { starts }
    }

    /// Whether it is under way at `now_secs`.
    pub fn is_on(&self, now_secs: i64) -> bool {
        self.reached(now_secs)
    }

    /// Whether quoting stops at `now_secs`: from `lead_secs` before it starts.
    pub fn stops_quoting(&self, now_secs: i64, lead_secs: i64) -> bool {
        self.reached(now_secs.saturating_add(lead_secs))
    }

    /// Whether `unix_secs` is at or past its start; a time before the epoch is before any.
    fn reached(&self, unix_secs: i64) -> bool {
        u64::try_from(unix_secs).is_ok_and(|at| at >= self.starts)
    }
}

/// Where the token's `k` and its scheduled corporate actions are read: on chain, by a task of
/// the core's, a block apart.
pub trait Valuation: Send + Sync + 'static {
    /// The shares one token is worth now, and the corporate action scheduled on it, if any.
    /// `None` for a token the source does not list: the core then quotes the pair on the
    /// price source alone, without the multiplier gates, and says so at the build. An
    /// `Err` is a read that failed, which the build refuses and a running pair ages out.
    fn read(&self) -> BoxFuture<'_, eyre::Result<Option<Shares>>>;
}

/// Something the issuer reports on the asset that stops quoting: a pause, a size limit, a
/// corporate action, under way or to come.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stoppage {
    /// Why, worded for the refusal: `ASSET_PAUSED: split`.
    pub why: String,
    /// When it starts, in milliseconds since the epoch; `None` for one under way, and for
    /// one to come whose start the issuer does not give, which stops quoting now.
    pub starts_ms: Option<i64>,
    /// Whether it changes how many shares the token is worth (a split, a dividend). One
    /// that does stops quoting `pause_lead_secs` before it starts, as a corporate action
    /// scheduled on chain does; one that does not, from its start.
    pub moves_multiplier: bool,
}

impl Stoppage {
    /// One for `why`, under way or with no start the issuer gives (so stopping quoting
    /// now); one to come at a known time sets `starts_ms`. `moves_multiplier` says
    /// whether it changes how many shares the token is worth, which stops quoting a lead
    /// ahead: when an issuer does not say, `true` is the safer answer.
    pub fn new(why: impl Into<String>, moves_multiplier: bool) -> Stoppage {
        Stoppage {
            why: why.into(),
            starts_ms: None,
            moves_multiplier,
        }
    }

    /// Whether it stops quoting at `now_ms`, `lead_ms` ahead of its start if it moves the
    /// multiplier.
    pub fn stops_quoting(&self, now_ms: i64, lead_ms: i64) -> bool {
        let lead_ms = if self.moves_multiplier { lead_ms } else { 0 };
        self.starts_ms
            .is_none_or(|starts| now_ms.saturating_add(lead_ms) >= starts)
    }
}

/// What the issuer says, judged for one asset; `None` until first read. Starts as
/// [`Default`], nothing read, and an adapter sets what it has.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StatusReading {
    /// Whether the market is in the session the pair quotes: `Ok`, or why not, worded for
    /// the refusal (`Ondo reports the market closed`).
    pub session: Option<Stamped<Result<(), String>>>,
    /// What the issuer reports on the asset that stops quoting, now or from its start.
    pub asset: Option<Stamped<Arc<[Stoppage]>>>,
}

/// What the issuer says about its market and its assets. The core reads the latest
/// readings every tick, and runs the task that keeps them current as one of the pair's.
pub trait MarketStatus: Send + Sync + 'static {
    /// The latest readings, judged for the asset `names` names: the token's symbol first,
    /// then the stock's ticker when the price source named it. Never waits on I/O.
    fn read(&self, names: &[String]) -> StatusReading;
}

/// An issuer's status, opened by the pair's build: the reader the core reads every tick,
/// and the task that keeps it current, which the core starts once the build has
/// succeeded and which stops with the pair.
#[non_exhaustive]
pub struct StatusFeed {
    /// Answers the status for an asset's names.
    pub reader: Box<dyn MarketStatus>,
    /// The task that keeps the reader current; it stops with the pair.
    pub refresh: BoxFuture<'static, ()>,
}

impl StatusFeed {
    /// `reader`, kept current by `refresh`.
    pub fn new(reader: Box<dyn MarketStatus>, refresh: BoxFuture<'static, ()>) -> StatusFeed {
        StatusFeed { reader, refresh }
    }
}

/// Where the token's transfer rules are read: on chain, by the restriction guard's task.
pub trait Restrictions: Send + Sync + 'static {
    /// What the token checks on every transfer now. Read on each poll, so a change on the
    /// issuer's side (a new list, a new pause manager) is followed without a config change.
    fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>>;
}

/// One read of the token's transfer rules, applied to the parties of a fill.
pub trait Rules: Send + Sync {
    /// What stops every transfer, whoever the parties: `Ondo paused the token`.
    fn problems(&self) -> Vec<String>;
    /// Why the token would refuse to move for `party`, named `name` in each reason, as
    /// `vault 0x… is on Ondo's blocklist`.
    fn refusals<'a>(
        &'a self,
        name: &'a str,
        party: Address,
    ) -> BoxFuture<'a, eyre::Result<Vec<String>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pause_stops_quoting_from_the_lead_before_it_on() {
        let pause = Action {
            starts: 1_791_500_000,
        };
        assert!(!pause.stops_quoting(1_791_499_699, 300));
        assert!(pause.stops_quoting(1_791_499_700, 300), "the lead");
        assert!(!pause.is_on(1_791_499_999));
        assert!(pause.is_on(1_791_500_000));
        assert!(
            pause.stops_quoting(1_791_600_000, 300),
            "until it is cleared"
        );
        assert!(!pause.is_on(-1), "before the epoch");
        assert!(!pause.stops_quoting(-1, 0), "before the epoch");
        assert!(
            pause.stops_quoting(i64::MAX, i64::MAX),
            "a lead that would wrap"
        );
    }
}
