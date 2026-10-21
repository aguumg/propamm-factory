//! The places a pair's mid can be read from, and how each one is reached.
//!
//! Every venue delivers the same thing to the rest of the pusher: a price for one market,
//! turned into a [`crate::feed::PriceSample`] by the shared loop in `feed.rs`. What differs
//! per venue is only the wire: where to connect, what to send to be told about a market,
//! what an inbound frame looks like, and how the connection is kept alive. That is all a
//! venue module contains (see `venues/`), so adding one is a URL, a subscribe message and
//! a parser, and the reconnecting, the scaling, the averaging and the metrics come for
//! free.
//!
//! Dispatched through an enum rather than a trait object: a venue's connection step can
//! need an HTTP round trip before the socket is dialled (KuCoin hands out its URL that
//! way), and an `async fn` on an enum is the plain way to have that without boxing.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use eyre::{Result, eyre};

use crate::venues::*;

/// A venue as it is named in the config and the backoffice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VenueId {
    Binance,
    Coinbase,
    Kraken,
    Okx,
    Bybit,
    Kucoin,
    Bitget,
    Gate,
    Mexc,
}

impl VenueId {
    /// Whether this venue's frames carry the exchange's own time for the price, so its age
    /// lateness can be judged by that clock (`max_source_lag_ms`). Binance's spot bookTicker carries
    /// none, and MEXC is polled over REST, whose body carries none.
    pub fn stamps_frames(self) -> bool {
        !matches!(self, VenueId::Binance | VenueId::Mexc)
    }

    /// Every venue the binary knows, in the order the backoffice lists them.
    pub const ALL: &'static [VenueId] = &[
        VenueId::Binance,
        VenueId::Coinbase,
        VenueId::Kraken,
        VenueId::Okx,
        VenueId::Bybit,
        VenueId::Kucoin,
        VenueId::Bitget,
        VenueId::Gate,
        VenueId::Mexc,
    ];

    /// The name a config file uses for this venue.
    pub fn as_str(self) -> &'static str {
        match self {
            VenueId::Binance => "binance",
            VenueId::Coinbase => "coinbase",
            VenueId::Kraken => "kraken",
            VenueId::Okx => "okx",
            VenueId::Bybit => "bybit",
            VenueId::Kucoin => "kucoin",
            VenueId::Bitget => "bitget",
            VenueId::Gate => "gate",
            VenueId::Mexc => "mexc",
        }
    }

    /// The endpoint dialled when the settings name none: a WebSocket base for a streamed
    /// venue, an HTTP base for a polled one (and for KuCoin, whose socket URL is fetched).
    pub fn default_endpoint(self) -> &'static str {
        match self {
            VenueId::Binance => binance::DEFAULT_ENDPOINT,
            VenueId::Coinbase => coinbase::DEFAULT_ENDPOINT,
            VenueId::Kraken => kraken::DEFAULT_ENDPOINT,
            VenueId::Okx => okx::DEFAULT_ENDPOINT,
            VenueId::Bybit => bybit::DEFAULT_ENDPOINT,
            VenueId::Kucoin => kucoin::DEFAULT_ENDPOINT,
            VenueId::Bitget => bitget::DEFAULT_ENDPOINT,
            VenueId::Gate => gate::DEFAULT_ENDPOINT,
            VenueId::Mexc => mexc::DEFAULT_ENDPOINT,
        }
    }

    /// How `symbol` is reached at `endpoint`. Async because some venues need an HTTP round
    /// trip first; `http` is the client for that and for polled venues.
    pub async fn connection(
        self,
        endpoint: &str,
        symbol: &str,
        http: &reqwest::Client,
    ) -> Result<Connection> {
        Ok(match self {
            VenueId::Binance => binance::connection(endpoint, symbol),
            VenueId::Coinbase => coinbase::connection(endpoint, symbol),
            VenueId::Kraken => kraken::connection(endpoint, symbol),
            VenueId::Okx => okx::connection(endpoint, symbol),
            VenueId::Bybit => bybit::connection(endpoint, symbol),
            VenueId::Kucoin => kucoin::connection(endpoint, symbol, http).await?,
            VenueId::Bitget => bitget::connection(endpoint, symbol),
            VenueId::Gate => gate::connection(endpoint, symbol),
            VenueId::Mexc => mexc::connection(endpoint, symbol),
        })
    }

    /// Reads one inbound text frame, or one polled response body. `Ok(None)` is a frame
    /// that carries no price (an acknowledgement, a heartbeat, a pong sent as text), which
    /// the loop treats as a sign of life and nothing more. `Err` is a frame that should
    /// have carried a price and did not, and is counted against the feed.
    pub fn parse(self, text: &str) -> Result<Option<Quote>> {
        match self {
            VenueId::Binance => binance::parse(text),
            VenueId::Coinbase => coinbase::parse(text),
            VenueId::Kraken => kraken::parse(text),
            VenueId::Okx => okx::parse(text),
            VenueId::Bybit => bybit::parse(text),
            VenueId::Kucoin => kucoin::parse(text),
            VenueId::Bitget => bitget::parse(text),
            VenueId::Gate => gate::parse(text),
            VenueId::Mexc => mexc::parse(text),
        }
    }

    /// [`Self::parse`], plus the exchange's own time for the price when the frame carries
    /// one: Kraken's `timestamp` and Coinbase's `time` (RFC 3339), OKX's `ts`, Bybit's `ts`,
    /// KuCoin's `time`, Bitget's `ts` and Gate's `t` (epoch milliseconds), and Binance's `E`
    /// on a stream that sends it (its spot bookTicker does not). `None` where there is none,
    /// and for MEXC, which is polled: those prices are aged by when they arrived. Kept
    /// beside `parse` rather than replacing it so a venue that carries no time needs
    /// nothing new.
    pub fn parse_stamped(self, text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
        match self {
            VenueId::Binance => binance::parse_stamped(text),
            VenueId::Coinbase => coinbase::parse_stamped(text),
            VenueId::Kraken => kraken::parse_stamped(text),
            VenueId::Okx => okx::parse_stamped(text),
            VenueId::Bybit => bybit::parse_stamped(text),
            VenueId::Kucoin => kucoin::parse_stamped(text),
            VenueId::Bitget => bitget::parse_stamped(text),
            VenueId::Gate => gate::parse_stamped(text),
            // Polled over REST, whose body carries no time: aged by the fetch.
            VenueId::Mexc => Ok(self.parse(text)?.map(|quote| (quote, None))),
        }
    }

    /// The market this venue would list for a pair whose tokens read `base`/`quote` on
    /// chain, for the startup cross-check against what the config declares.
    pub fn expected_symbol(self, base: &str, quote: &str) -> String {
        match self {
            VenueId::Binance => binance::expected_symbol(base, quote),
            VenueId::Coinbase => coinbase::expected_symbol(base, quote),
            VenueId::Kraken => kraken::expected_symbol(base, quote),
            VenueId::Okx => okx::expected_symbol(base, quote),
            VenueId::Bybit => bybit::expected_symbol(base, quote),
            VenueId::Kucoin => kucoin::expected_symbol(base, quote),
            VenueId::Bitget => bitget::expected_symbol(base, quote),
            VenueId::Gate => gate::expected_symbol(base, quote),
            VenueId::Mexc => mexc::expected_symbol(base, quote),
        }
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for VenueId {
    type Err = eyre::Report;

    fn from_str(s: &str) -> Result<Self> {
        VenueId::ALL
            .iter()
            .copied()
            .find(|v| v.as_str().eq_ignore_ascii_case(s))
            .ok_or_else(|| {
                eyre!(
                    "unknown venue {s:?}; one of {}",
                    VenueId::ALL
                        .iter()
                        .map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }
}

/// Where each venue is dialled: the settings' overrides (a mock, a regional endpoint such
/// as binance.us) over the venue's default, and the one HTTP client the run reaches the
/// polled ones with.
#[derive(Clone, Debug, Default)]
pub struct Endpoints {
    overrides: HashMap<VenueId, String>,
    /// Built once per run, shared by every feed and offered to every pricer's build:
    /// `Arc`'d so a build context can be seen to carry the run's rather than one of its own.
    http: std::sync::Arc<reqwest::Client>,
}

impl Endpoints {
    /// The overrides a run uses: the `[settings.endpoints]` table, with `binance_ws` (the
    /// flag, the variable or the older settings key) as the Binance entry. The flag wins
    /// over the table for Binance, like every flag wins over the file.
    pub fn resolve(
        binance_ws: Option<&str>,
        table: &std::collections::BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut overrides = HashMap::new();
        for (name, url) in table {
            // Not a venue: where the vault's USD prices are asked for (`vault.rs`).
            if name == "coingecko" {
                continue;
            }
            let venue: VenueId = name
                .parse()
                .map_err(|err| eyre!("[settings.endpoints] {err}"))?;
            overrides.insert(venue, url.clone());
        }
        if let Some(url) = binance_ws {
            overrides.insert(VenueId::Binance, url.to_owned());
        }
        Ok(Self {
            overrides,
            http: std::sync::Arc::new(crate::feed::http_client()),
        })
    }

    /// One override and the defaults for everything else.
    #[cfg(test)]
    pub fn single(venue: VenueId, endpoint: String) -> Self {
        Self {
            overrides: HashMap::from([(venue, endpoint)]),
            http: std::sync::Arc::new(crate::feed::http_client()),
        }
    }

    /// The run's HTTP client, for a feed or a build context.
    pub fn http(&self) -> std::sync::Arc<reqwest::Client> {
        std::sync::Arc::clone(&self.http)
    }

    pub fn for_venue(&self, venue: VenueId) -> &str {
        self.overrides
            .get(&venue)
            .map_or_else(|| venue.default_endpoint(), String::as_str)
    }
}

/// How a venue delivers a market's price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Connection {
    /// A WebSocket the venue pushes prices over. `subscribe`, when given, is the text frame
    /// sent right after connecting; `keepalive` is what to send when the socket has been
    /// quiet long enough to be probed.
    Stream {
        url: String,
        subscribe: Option<String>,
        keepalive: Keepalive,
    },
    /// An HTTP resource fetched every `every`, for venues that have no stream. Each fetch
    /// that parses is a sample stamped at the fetch.
    Poll {
        url: String,
        headers: Vec<(String, String)>,
        every: Duration,
    },
}

/// What a quiet stream is probed with.
#[derive(Clone, Debug)]
pub enum Keepalive {
    /// A protocol-level ping frame, which every server answers with a pong.
    Ping,
    /// A text frame the venue's protocol defines as its ping, answered in text (which the
    /// venue's `parse` reports as `Ok(None)`).
    Text(String),
    /// A text ping built at send time, for a protocol whose ping carries the current time.
    Fresh(fn() -> String),
}

impl Keepalive {
    /// The text frame to send now, or `None` for a protocol-level ping.
    pub fn frame(&self) -> Option<String> {
        match self {
            Keepalive::Ping => None,
            Keepalive::Text(text) => Some(text.clone()),
            Keepalive::Fresh(build) => Some(build()),
        }
    }
}

impl PartialEq for Keepalive {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Keepalive::Ping, Keepalive::Ping) => true,
            (Keepalive::Text(a), Keepalive::Text(b)) => a == b,
            (Keepalive::Fresh(a), Keepalive::Fresh(b)) => std::ptr::fn_addr_eq(*a, *b),
            _ => false,
        }
    }
}

impl Eq for Keepalive {}

/// A price as a venue states it, before scaling: decimal strings, because that is how the
/// venues send them and the scaling to integers must see every digit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Quote {
    /// The best bid and ask of an order book.
    Book { bid: String, ask: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_venue_round_trips_through_its_name() {
        for venue in VenueId::ALL {
            assert_eq!(venue.as_str().parse::<VenueId>().unwrap(), *venue);
            assert_eq!(
                venue.as_str().to_uppercase().parse::<VenueId>().unwrap(),
                *venue,
                "config spelling is case-insensitive"
            );
        }
        let err = "bogus".parse::<VenueId>().unwrap_err();
        assert!(err.to_string().contains("binance"), "{err}");
    }

    /// Every venue spells a wrapped token's market as the underlying's, in its own shape.
    #[test]
    fn every_venue_spells_the_weth_usdc_market() {
        for venue in VenueId::ALL {
            let spelling = venue.expected_symbol("WETH", "USDC");
            assert!(
                !spelling.contains("WETH"),
                "{venue} spells the wrapped token: {spelling}"
            );
        }
        assert_eq!(VenueId::Binance.expected_symbol("WETH", "USDC"), "ETHUSDC");
        assert_eq!(
            VenueId::Coinbase.expected_symbol("WETH", "USDC"),
            "ETH-USDC"
        );
        assert_eq!(VenueId::Kraken.expected_symbol("WBTC", "USDC"), "BTC/USDC");
        assert_eq!(VenueId::Okx.expected_symbol("WETH", "USDC"), "ETH-USDC");
        assert_eq!(VenueId::Bybit.expected_symbol("WETH", "USDC"), "ETHUSDC");
        assert_eq!(VenueId::Kucoin.expected_symbol("WETH", "USDC"), "ETH-USDC");
        assert_eq!(VenueId::Bitget.expected_symbol("WETH", "USDC"), "ETHUSDC");
        assert_eq!(VenueId::Gate.expected_symbol("WETH", "USDC"), "ETH_USDC");
        assert_eq!(VenueId::Mexc.expected_symbol("WETH", "USDC"), "ETHUSDC");
    }
}
