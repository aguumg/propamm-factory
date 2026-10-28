//! Finding where a pair trades, so the backoffice can fill the venue table instead of an
//! operator looking up how each venue spells the market.
//!
//! The token names come from the chain (`symbol()` on each contract, read by the caller).
//! Then every exchange is asked for its own list of markets, and a market is the pair's
//! when it is tradable and its base and quote are exactly the two names, wrapped tokens
//! read as their underlying (`WETH` is `ETH`). The symbol filled in is the one the
//! exchange gives that market, so there is no spelling to get wrong. Each exchange's code
//! here is only where its list is and which of its fields hold the base, the quote, the
//! status and the 24h volume; the rule is the same for all of them.
//!
//! A market that does not match exactly is not guessed at: the row stays empty for the
//! operator to fill, and the live preview shows whether what they typed works.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eyre::{Result, WrapErr, eyre};
use serde_json::Value;

use crate::venue::VenueId;
use crate::venues::{dollar_stand_ins, unwrap_alias};

/// Dollar quotes, for telling a volume in dollars from one in something else.
const DOLLARS: &[&str] = &["USDC", "USDT", "USD", "FDUSD", "DAI", "USDE", "PYUSD"];

/// How long a market list is reused. Listings do not move in ten
/// minutes in any way this suggestion cares about, and Binance's list is megabytes.
const CACHE_FOR: Duration = Duration::from_secs(600);

/// The exchanges this asks: every venue that lists markets. Ondo is an issuer's price for
/// its own token, with no list to read; its row is the operator's to fill.
fn exchanges() -> impl Iterator<Item = VenueId> {
    VenueId::ALL
        .iter()
        .copied()
        .filter(|venue| *venue != VenueId::Ondo)
}

/// Where each exchange's REST API is. Not the feed endpoints: those are WebSocket hosts.
#[derive(Clone, Debug)]
pub struct Rest {
    hosts: HashMap<VenueId, String>,
}

impl Default for Rest {
    fn default() -> Self {
        let hosts = [
            (VenueId::Binance, "https://api.binance.com"),
            (VenueId::Coinbase, "https://api.exchange.coinbase.com"),
            (VenueId::Kraken, "https://api.kraken.com"),
            (VenueId::Okx, "https://www.okx.com"),
            (VenueId::Bybit, "https://api.bybit.com"),
            (VenueId::Kucoin, "https://api.kucoin.com"),
            (VenueId::Bitget, "https://api.bitget.com"),
            (VenueId::Gate, "https://api.gateio.ws"),
            (VenueId::Mexc, "https://api.mexc.com"),
        ]
        .into_iter()
        .map(|(v, h)| (v, h.to_owned()))
        .collect();
        Self { hosts }
    }
}

impl Rest {
    /// Every exchange at one host, for a test's mock.
    #[cfg(test)]
    pub fn all_at(host: &str) -> Self {
        Self {
            hosts: exchanges().map(|v| (v, host.to_owned())).collect(),
        }
    }

    fn host(&self, venue: VenueId) -> &str {
        self.hosts.get(&venue).map_or("", String::as_str)
    }
}

/// One tradable market, as an exchange lists it.
#[derive(Clone, Debug, PartialEq)]
struct Market {
    /// Upper case, as the exchange names the assets.
    base: String,
    quote: String,
    /// What the feed is configured with: the exchange's own name for the market.
    symbol: String,
    /// What the exchange's ticker endpoint is asked with; the same as `symbol` everywhere
    /// but Kraken.
    ticker: String,
}

/// An exchange's market list and when it was fetched.
type Fetched = (Instant, Arc<Vec<Market>>);

/// Market lists already fetched. One per backoffice. A failed request is never kept.
#[derive(Default)]
pub struct Cache {
    markets: Mutex<HashMap<VenueId, Fetched>>,
}

/// One venue's suggested row.
#[derive(Clone, Debug, PartialEq)]
pub struct Found {
    pub venue: VenueId,
    pub symbol: String,
    pub weight: String,
    /// The last trade price, in the market's own quote.
    pub last: Option<f64>,
    /// 24h volume in dollars, when the market is quoted in one.
    pub volume_usd: Option<f64>,
    /// What the operator should know about this row: that the market is quoted in a
    /// stand-in for the pair's quote token, if it is.
    pub note: String,
}

/// What the lookup learned about a pair.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Discovery {
    /// The pair was asked the wrong way round (nothing prices token0 in token1) and the
    /// rows are for the other orientation: `base_symbol` and `quote_symbol` say which.
    pub swapped: bool,
    pub base_symbol: String,
    pub quote_symbol: String,
    pub rows: Vec<Found>,
    /// Exchanges whose market list could not be read, so whether they list the pair is
    /// unknown, which is not the same as not listing it.
    pub unreachable: Vec<VenueId>,
    /// Lines for the top of the form: what was found, and which exchanges could not be
    /// asked.
    pub notes: Vec<String>,
}

impl Discovery {
    pub fn row(&self, venue: VenueId) -> Option<&Found> {
        self.rows.iter().find(|r| r.venue == venue)
    }
}

/// Looks the pair up at every exchange, from its two tokens' on-chain `symbol()`s. An
/// exchange that cannot be asked is a note and an unknown row, never a failed lookup.
pub async fn discover(
    http: &reqwest::Client,
    cache: &Cache,
    rest: &Rest,
    base: &str,
    quote: &str,
) -> Result<Discovery> {
    let lists = futures_util::future::join_all(
        exchanges().map(|venue| async move { (venue, markets(http, cache, rest, venue).await) }),
    )
    .await;
    let mut notes = Vec::new();
    let mut unreachable = Vec::new();
    let mut known: Vec<(VenueId, Arc<Vec<Market>>)> = Vec::new();
    for (venue, list) in lists {
        match list {
            Ok(list) => known.push((venue, list)),
            Err(err) => {
                notes.push(format!("{venue}: could not read its markets ({err:#})"));
                unreachable.push(venue);
            }
        }
    }
    let name = |symbol: &str| unwrap_alias(&symbol.to_uppercase()).to_owned();
    let (base_name, quote_name) = (name(base), name(quote));
    let choose = |b: &str, q: &str| -> Vec<(VenueId, Vec<Market>, bool)> {
        known
            .iter()
            .filter_map(|(venue, list)| {
                let (markets, exact) = candidates(list, b, q);
                (!markets.is_empty())
                    .then(|| (*venue, markets.into_iter().cloned().collect(), exact))
            })
            .collect()
    };
    // Markets are listed one way round (a token priced in USDC, never USDC priced in the
    // token). If nothing lists token0 in token1 but something lists token1 in token0, the
    // pair was asked backwards, and the rows for the right way round are the answer.
    let mut swapped = false;
    let mut chosen = choose(&base_name, &quote_name);
    let (mut base, mut quote) = (base, quote);
    if chosen.is_empty() {
        let reversed = choose(&quote_name, &base_name);
        if !reversed.is_empty() {
            swapped = true;
            chosen = reversed;
            std::mem::swap(&mut base, &mut quote);
        }
    }
    let (base_name, quote_name) = (name(base), name(quote));

    // A ticker per candidate: the exact market, or every dollar stand-in, of which the one
    // that trades the most is kept.
    let picked = futures_util::future::join_all(chosen.into_iter().map(
        |(venue, markets, exact)| async move {
            let tickers = futures_util::future::join_all(
                markets.iter().map(|m| ticker(http, rest, venue, m)),
            )
            .await;
            let volume_of = |t: &Result<(f64, Option<f64>)>| t.as_ref().map_or(-1.0, |(v, _)| *v);
            let (market, ticker) = markets
                .into_iter()
                .zip(tickers)
                .max_by(|a, b| volume_of(&a.1).total_cmp(&volume_of(&b.1)))
                .expect("a venue is only chosen with at least one candidate");
            ((venue, market, exact), ticker)
        },
    ))
    .await;
    let (chosen, volumes): (Vec<_>, Vec<_>) = picked.into_iter().unzip();
    let quote_volumes: Vec<Option<f64>> = volumes
        .iter()
        .map(|v| v.as_ref().ok().map(|(volume, _)| *volume))
        .collect();
    let max_volume = quote_volumes
        .iter()
        .flatten()
        .copied()
        .fold(0.0_f64, f64::max);
    let mut rows: Vec<Found> = chosen
        .into_iter()
        .zip(volumes)
        .map(|((venue, market, exact), volume)| {
            let (volume, last) = match volume {
                Ok((volume, last)) => (Some(volume), last),
                Err(_) => (None, None),
            };
            Found {
                venue,
                weight: suggested_weight(volume.unwrap_or(0.0), max_volume),
                last,
                volume_usd: volume.filter(|_| DOLLARS.contains(&market.quote.as_str())),
                note: if exact {
                    String::new()
                } else {
                    format!("quoted in {}, not {quote_name}", market.quote)
                },
                symbol: market.symbol,
            }
        })
        .collect();
    rows.sort_by_key(|r| VenueId::ALL.iter().position(|v| *v == r.venue));

    let listed = rows.len();
    notes.insert(
        0,
        match listed {
            0 => format!(
                "no exchange lists {base_name} against {quote_name} or a dollar stand-in; \
                 fill the table by hand"
            ),
            n => format!(
                "{n} exchange{} list {base_name} against {quote_name} or a dollar stand-in",
                if n == 1 { "" } else { "s" }
            ),
        },
    );
    Ok(Discovery {
        swapped,
        unreachable,
        base_symbol: base_name,
        quote_symbol: quote_name,
        rows,
        notes,
    })
}

/// The pair's markets in `list`: base `base` quoted in `quote` if the exchange has it,
/// else every market of `base` in one of `quote`'s [`dollar_stand_ins`] (a USDC or USDT
/// pair only), for the caller to keep the one that trades the most. The bool is whether
/// the quote matched exactly.
fn candidates<'a>(list: &'a [Market], base: &str, quote: &str) -> (Vec<&'a Market>, bool) {
    if let Some(m) = list.iter().find(|m| m.base == base && m.quote == quote) {
        return (vec![m], true);
    }
    let allowed = dollar_stand_ins(quote);
    let stand_ins = list
        .iter()
        .filter(|m| m.base == base && allowed.contains(&m.quote.as_str()))
        .collect();
    (stand_ins, false)
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value> {
    http.get(url)
        .send()
        .await
        .wrap_err("request failed")?
        .error_for_status()
        .wrap_err("refused")?
        .json()
        .await
        .wrap_err("not JSON")
}

/// An exchange's tradable markets, from the cache or freshly fetched.
async fn markets(
    http: &reqwest::Client,
    cache: &Cache,
    rest: &Rest,
    venue: VenueId,
) -> Result<Arc<Vec<Market>>> {
    if let Some((at, list)) = cache
        .markets
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&venue)
        && at.elapsed() < CACHE_FOR
    {
        return Ok(Arc::clone(list));
    }
    let host = rest.host(venue).trim_end_matches('/');
    let path = match venue {
        VenueId::Binance => {
            "/api/v3/exchangeInfo?permissions=SPOT&showPermissionSets=false&symbolStatus=TRADING"
        }
        VenueId::Coinbase => "/products",
        VenueId::Kraken => "/0/public/AssetPairs",
        VenueId::Okx => "/api/v5/public/instruments?instType=SPOT",
        VenueId::Bybit => "/v5/market/instruments-info?category=spot&limit=1000",
        VenueId::Kucoin => "/api/v2/symbols",
        VenueId::Bitget => "/api/v2/spot/public/symbols",
        VenueId::Gate => "/api/v4/spot/currency_pairs",
        VenueId::Mexc => "/api/v3/exchangeInfo",
        VenueId::Ondo => return Err(eyre!("ondo lists no markets")),
    };
    let body = get_json(http, &format!("{host}{path}")).await?;
    let list = Arc::new(parse_markets(venue, &body)?);
    cache
        .markets
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(venue, (Instant::now(), Arc::clone(&list)));
    Ok(list)
}

/// A JSON string field, or "".
fn text<'a>(row: &'a Value, field: &str) -> &'a str {
    row[field].as_str().unwrap_or("")
}

/// A number an exchange sends as a string or as a JSON number.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse().ok(),
        other => other.as_f64(),
    }
}

/// Kraken's own names for two assets, where the rest of the world (and its v2 WebSocket,
/// which the feed reads) says BTC and DOGE.
fn kraken_asset(name: &str) -> &str {
    match name {
        "XBT" => "BTC",
        "XDG" => "DOGE",
        other => other,
    }
}

/// One exchange's market list, each with its base, quote and name read from that
/// exchange's fields, and only the markets it says are tradable.
fn parse_markets(venue: VenueId, body: &Value) -> Result<Vec<Market>> {
    let rows = |v: &Value| -> Result<Vec<Value>> {
        v.as_array()
            .cloned()
            .ok_or_else(|| eyre!("no market list in the answer"))
    };
    // (base field, quote field, name field, status field, tradable status value)
    let simple =
        |list: Vec<Value>, base: &str, quote: &str, name: &str, ok: &dyn Fn(&Value) -> bool| {
            list.iter()
                .filter(|r| ok(r))
                .map(|r| Market {
                    base: text(r, base).to_uppercase(),
                    quote: text(r, quote).to_uppercase(),
                    symbol: text(r, name).to_owned(),
                    ticker: text(r, name).to_owned(),
                })
                .filter(|m| !m.base.is_empty() && !m.quote.is_empty() && !m.symbol.is_empty())
                .collect::<Vec<_>>()
        };
    Ok(match venue {
        VenueId::Binance => simple(
            rows(&body["symbols"])?,
            "baseAsset",
            "quoteAsset",
            "symbol",
            &|r| text(r, "status") == "TRADING",
        ),
        VenueId::Coinbase => simple(rows(body)?, "base_currency", "quote_currency", "id", &|r| {
            text(r, "status") == "online" && r["trading_disabled"] != Value::Bool(true)
        }),
        VenueId::Kraken => {
            let pairs = body["result"]
                .as_object()
                .ok_or_else(|| eyre!("no market list in the answer"))?;
            pairs
                .iter()
                .filter(|(_, r)| text(r, "status") == "online")
                .filter_map(|(id, r)| {
                    let (b, q) = text(r, "wsname").split_once('/')?;
                    let (b, q) = (kraken_asset(b), kraken_asset(q));
                    Some(Market {
                        base: b.to_uppercase(),
                        quote: q.to_uppercase(),
                        symbol: format!("{b}/{q}"),
                        ticker: id.clone(),
                    })
                })
                .collect()
        }
        VenueId::Okx => simple(
            rows(&body["data"])?,
            "baseCcy",
            "quoteCcy",
            "instId",
            &|r| text(r, "state") == "live",
        ),
        VenueId::Bybit => simple(
            rows(&body["result"]["list"])?,
            "baseCoin",
            "quoteCoin",
            "symbol",
            &|r| text(r, "status") == "Trading",
        ),
        VenueId::Kucoin => simple(
            rows(&body["data"])?,
            "baseCurrency",
            "quoteCurrency",
            "symbol",
            &|r| r["enableTrading"] == Value::Bool(true),
        ),
        VenueId::Bitget => simple(
            rows(&body["data"])?,
            "baseCoin",
            "quoteCoin",
            "symbol",
            &|r| text(r, "status") == "online",
        ),
        VenueId::Gate => simple(rows(body)?, "base", "quote", "id", &|r| {
            text(r, "trade_status") == "tradable"
        }),
        VenueId::Mexc => simple(
            rows(&body["symbols"])?,
            "baseAsset",
            "quoteAsset",
            "symbol",
            &|r| text(r, "status") == "1" && r["isSpotTradingAllowed"] != Value::Bool(false),
        ),
        VenueId::Ondo => return Err(eyre!("ondo lists no markets")),
    })
}

/// A market's 24h volume in its quote currency, and its last price.
async fn ticker(
    http: &reqwest::Client,
    rest: &Rest,
    venue: VenueId,
    market: &Market,
) -> Result<(f64, Option<f64>)> {
    let host = rest.host(venue).trim_end_matches('/');
    let t = &market.ticker;
    let url = match venue {
        VenueId::Binance => format!("{host}/api/v3/ticker/24hr?symbol={t}"),
        VenueId::Coinbase => format!("{host}/products/{t}/stats"),
        VenueId::Kraken => format!("{host}/0/public/Ticker?pair={t}"),
        VenueId::Okx => format!("{host}/api/v5/market/ticker?instId={t}"),
        VenueId::Bybit => format!("{host}/v5/market/tickers?category=spot&symbol={t}"),
        VenueId::Kucoin => format!("{host}/api/v1/market/stats?symbol={t}"),
        VenueId::Bitget => format!("{host}/api/v2/spot/market/tickers?symbol={t}"),
        VenueId::Gate => format!("{host}/api/v4/spot/tickers?currency_pair={t}"),
        VenueId::Mexc => format!("{host}/api/v3/ticker/24hr?symbol={t}"),
        VenueId::Ondo => return Err(eyre!("ondo lists no markets")),
    };
    parse_ticker(venue, &get_json(http, &url).await?)
}

fn parse_ticker(venue: VenueId, body: &Value) -> Result<(f64, Option<f64>)> {
    // (quote volume, last) directly, or (base volume × last) where only that is given.
    let (volume, last) = match venue {
        VenueId::Binance | VenueId::Mexc => {
            (number(&body["quoteVolume"]), number(&body["lastPrice"]))
        }
        VenueId::Coinbase => {
            let last = number(&body["last"]);
            (number(&body["volume"]).zip(last).map(|(v, l)| v * l), last)
        }
        VenueId::Kraken => {
            let row = body["result"]
                .as_object()
                .and_then(|m| m.values().next())
                .ok_or_else(|| eyre!("no ticker in the answer"))?;
            let last = number(&row["c"][0]);
            (number(&row["v"][1]).zip(last).map(|(v, l)| v * l), last)
        }
        VenueId::Okx => (
            number(&body["data"][0]["volCcy24h"]),
            number(&body["data"][0]["last"]),
        ),
        VenueId::Bybit => {
            let row = &body["result"]["list"][0];
            (number(&row["turnover24h"]), number(&row["lastPrice"]))
        }
        VenueId::Kucoin => (
            number(&body["data"]["volValue"]),
            number(&body["data"]["last"]),
        ),
        VenueId::Bitget => (
            number(&body["data"][0]["quoteVolume"]),
            number(&body["data"][0]["lastPr"]),
        ),
        VenueId::Gate => (number(&body[0]["quote_volume"]), number(&body[0]["last"])),
        VenueId::Ondo => return Err(eyre!("ondo lists no markets")),
    };
    Ok((
        volume.ok_or_else(|| eyre!("no 24h volume in the answer"))?,
        last,
    ))
}

/// Relative 24h volume, largest venue 10, never below 1, one decimal.
fn suggested_weight(volume: f64, max_volume: f64) -> String {
    if max_volume <= 0.0 || volume <= 0.0 {
        return "1".to_owned();
    }
    let w = (volume / max_volume * 10.0).max(1.0);
    let rounded = (w * 10.0).round() / 10.0;
    if rounded.fract() == 0.0 {
        format!("{rounded:.0}")
    } else {
        format!("{rounded}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn market(base: &str, quote: &str, symbol: &str) -> Market {
        Market {
            base: base.into(),
            quote: quote.into(),
            symbol: symbol.into(),
            ticker: symbol.into(),
        }
    }

    #[test]
    fn the_exact_quote_wins_else_every_dollar_stand_in_is_a_candidate() {
        let list = vec![
            market("ETH", "USDT", "ETH-USDT"),
            market("ETH", "USDC", "ETH-USDC"),
            market("ETH", "USD", "ETH-USD"),
            market("ETH", "EUR", "ETH-EUR"),
            market("ETH", "FDUSD", "ETH-FDUSD"),
            market("BTC", "USDC", "BTC-USDC"),
        ];
        assert_eq!(candidates(&list, "ETH", "USDC"), (vec![&list[1]], true));
        // No USDC market: USDT and USD, never the euro or FDUSD one.
        let no_usdc = vec![
            list[0].clone(),
            list[2].clone(),
            list[3].clone(),
            list[4].clone(),
        ];
        assert_eq!(
            candidates(&no_usdc, "ETH", "USDC"),
            (vec![&no_usdc[0], &no_usdc[1]], false)
        );
        // Any other quote, dollar or not, has to match exactly.
        assert_eq!(
            candidates(&no_usdc, "ETH", "DAI"),
            (Vec::<&Market>::new(), false)
        );
        assert_eq!(
            candidates(&list, "ETH", "GBP"),
            (Vec::<&Market>::new(), false)
        );
        // The base is never stood in for.
        assert_eq!(
            candidates(&list, "WETH", "USDC"),
            (Vec::<&Market>::new(), false)
        );
    }

    /// Each exchange's list, in the shape it really sends (trimmed), read the same way:
    /// its name for the market, its base and quote, and only what it says is tradable.
    #[test]
    fn every_exchanges_list_is_read_from_its_own_fields() {
        let cases = [
            (
                VenueId::Binance,
                json!({"symbols":[
                    {"symbol":"ETHUSDC","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDC"},
                    {"symbol":"ETHUSDT","status":"BREAK","baseAsset":"ETH","quoteAsset":"USDT"}]}),
                "ETHUSDC",
            ),
            (
                VenueId::Coinbase,
                json!([
                    {"id":"ETH-USDC","base_currency":"ETH","quote_currency":"USDC","status":"delisted","trading_disabled":true},
                    {"id":"ETH-USD","base_currency":"ETH","quote_currency":"USD","status":"online","trading_disabled":false}]),
                "ETH-USD",
            ),
            (
                VenueId::Kraken,
                json!({"error":[],"result":{
                    "XETHZUSD":{"altname":"ETHUSD","wsname":"ETH/USD","base":"XETH","quote":"ZUSD","status":"online"},
                    "XBTUSDC":{"altname":"XBTUSDC","wsname":"XBT/USDC","base":"XXBT","quote":"USDC","status":"online"}}}),
                "ETH/USD",
            ),
            (
                VenueId::Okx,
                json!({"code":"0","data":[
                    {"instId":"ETH-USDC","baseCcy":"ETH","quoteCcy":"USDC","state":"live"},
                    {"instId":"ETH-USDT","baseCcy":"ETH","quoteCcy":"USDT","state":"suspend"}]}),
                "ETH-USDC",
            ),
            (
                VenueId::Bybit,
                json!({"retCode":0,"result":{"list":[
                    {"symbol":"ETHUSDC","baseCoin":"ETH","quoteCoin":"USDC","status":"Trading"}]}}),
                "ETHUSDC",
            ),
            (
                VenueId::Kucoin,
                json!({"code":"200000","data":[
                    {"symbol":"ETH-USDC","baseCurrency":"ETH","quoteCurrency":"USDC","enableTrading":true},
                    {"symbol":"ETH-USDT","baseCurrency":"ETH","quoteCurrency":"USDT","enableTrading":false}]}),
                "ETH-USDC",
            ),
            (
                VenueId::Bitget,
                json!({"code":"00000","data":[
                    {"symbol":"ETHUSDC","baseCoin":"ETH","quoteCoin":"USDC","status":"online"}]}),
                "ETHUSDC",
            ),
            (
                VenueId::Gate,
                json!([
                    {"id":"ETH_USDC","base":"ETH","quote":"USDC","trade_status":"tradable"},
                    {"id":"ETH_USDT","base":"ETH","quote":"USDT","trade_status":"untradable"}]),
                "ETH_USDC",
            ),
            (
                VenueId::Mexc,
                json!({"symbols":[
                    {"symbol":"ETHUSDC","status":"1","baseAsset":"ETH","quoteAsset":"USDC","isSpotTradingAllowed":true},
                    {"symbol":"ETHUSDT","status":"2","baseAsset":"ETH","quoteAsset":"USDT","isSpotTradingAllowed":true}]}),
                "ETHUSDC",
            ),
        ];
        for (venue, body, want) in cases {
            let list = parse_markets(venue, &body).unwrap();
            let names: Vec<&str> = list
                .iter()
                .filter(|m| m.base == "ETH")
                .map(|m| m.symbol.as_str())
                .collect();
            assert_eq!(names, [want], "{venue}: only the tradable market");
            assert!(
                parse_markets(venue, &json!({"unexpected": 1})).is_err(),
                "{venue}"
            );
        }
        // Kraken's XBT is BTC, the way its v2 stream names it, and its ticker is asked by
        // the pair's own key.
        let kraken = parse_markets(
            VenueId::Kraken,
            &json!({"result":{
            "XBTUSDC":{"wsname":"XBT/USDC","status":"online"}}}),
        )
        .unwrap();
        assert_eq!(
            kraken,
            [Market {
                base: "BTC".into(),
                quote: "USDC".into(),
                symbol: "BTC/USDC".into(),
                ticker: "XBTUSDC".into(),
            }]
        );
    }

    /// Each exchange's ticker, in the shape it really sends (trimmed), as a volume in the
    /// quote currency.
    #[test]
    fn every_exchanges_volume_is_in_the_quote_currency() {
        let cases = [
            (
                VenueId::Binance,
                json!({"lastPrice":"2000","quoteVolume":"271938654.08"}),
                271_938_654.08,
            ),
            (
                VenueId::Coinbase,
                json!({"volume":"10","last":"2000"}),
                20_000.0,
            ),
            (
                VenueId::Kraken,
                json!({"error":[],"result":{"ETHUSDC":{"c":["2000","0.1"],"v":["1","5"]}}}),
                10_000.0,
            ),
            (
                VenueId::Okx,
                json!({"data":[{"last":"2000","volCcy24h":"9952115.9"}]}),
                9_952_115.9,
            ),
            (
                VenueId::Bybit,
                json!({"result":{"list":[{"lastPrice":"2000","turnover24h":"7016180.8"}]}}),
                7_016_180.8,
            ),
            (
                VenueId::Kucoin,
                json!({"data":{"last":"2000","volValue":"13717780.5"}}),
                13_717_780.5,
            ),
            (
                VenueId::Bitget,
                json!({"data":[{"lastPr":"2000","quoteVolume":"9113804.06"}]}),
                9_113_804.06,
            ),
            (
                VenueId::Gate,
                json!([{"last":"2000","quote_volume":"574274.4"}]),
                574_274.4,
            ),
            (
                VenueId::Mexc,
                json!({"lastPrice":"2000","quoteVolume":"1343253.03"}),
                1_343_253.03,
            ),
        ];
        for (venue, body, want) in cases {
            let (volume, last) = parse_ticker(venue, &body).unwrap();
            assert!((volume - want).abs() < 1e-6, "{venue}: {volume}");
            assert_eq!(last, Some(2000.0), "{venue}");
        }
        assert!(parse_ticker(VenueId::Binance, &json!({"code":-1121})).is_err());
    }

    #[test]
    fn weights_are_relative_volume_largest_ten() {
        assert_eq!(suggested_weight(0.0, 0.0), "1");
        assert_eq!(suggested_weight(300.0, 400.0), "7.5");
        assert_eq!(suggested_weight(400.0, 400.0), "10");
        assert_eq!(suggested_weight(1.0, 400.0), "1");
    }
}

#[cfg(test)]
mod live {
    /// Against the real exchanges, by hand: `cargo test live_weth_usdc -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn live_weth_usdc() {
        live_pair("USDC").await;
        live_pair("USDT").await;
    }

    #[allow(clippy::print_stdout)] // a terminal tool a person runs, not library output
    async fn live_pair(quote: &str) {
        println!("== WETH/{quote}");
        let http = reqwest::Client::builder()
            .user_agent("quote-updater")
            .build()
            .unwrap();
        let found = super::discover(
            &http,
            &super::Cache::default(),
            &super::Rest::default(),
            "WETH",
            quote,
        )
        .await
        .unwrap();
        for row in &found.rows {
            println!(
                "{:<10} {:<14} w={:<5} vol={:?} {}",
                row.venue,
                row.symbol,
                row.weight,
                row.volume_usd.map(|v| v as u64),
                row.note
            );
        }
        println!("{:?}", found.notes);
    }
}
