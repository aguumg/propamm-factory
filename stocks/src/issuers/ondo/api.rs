//! The three reads Ondo's adapters make of Ondo's GM API (`https://api.gm.ondo.finance`):
//! the latest token price ([`OndoPrices`], the `ondo` kind's [`PriceSource`]), the market
//! session, and the per-asset events (pauses, limits), which `status.rs` reads.
//!
//! The API is partner-only: a key from Ondo's onboarding, sent as `x-api-key`. Its responses
//! are cached for a second (prices) to a minute (status) on Ondo's side, so polling faster
//! than once a second buys nothing.
//!
//! The key only goes over https ([`check_api`]; plain http to this machine, for the smoke
//! test's mock), through a client of its own that never follows a redirect ([`client`]).

use std::{sync::OnceLock, time::Duration};

use quote_updater::{eyre, prelude::BoxFuture, reqwest};
use serde::Deserialize;

use crate::{
    ports::{Event, Fetched, Price, PriceSource},
    stock::hours::days_from_civil,
};

/// A GM token's price, from `GET /v1/assets/{symbol}/prices/latest`: the token's price (the
/// stock's times the token's share multiplier), the stock's, and the stock's ticker.
pub struct OndoPrices {
    pub http: reqwest::Client,
    pub api: String,
    pub key: String,
    pub symbol: String,
    /// The stock's ticker as the stanza states it, for an answer that does not name it.
    pub ticker: Option<String>,
}

impl PriceSource for OndoPrices {
    fn fetch(&self) -> BoxFuture<'_, eyre::Result<Fetched>> {
        Box::pin(async move {
            let path = format!("/v1/assets/{}/prices/latest", self.symbol);
            let body = get(&self.http, &self.api, &path, &self.key).await?;
            let mut fetched = parse_price(&body, &self.symbol).map_err(|err| eyre::eyre!(err))?;
            let ticker = resolve_ticker(fetched.ticker, self.ticker.as_deref(), &self.symbol)
                .map_err(|err| eyre::eyre!(err))?;
            fetched.ticker = Some(ticker);
            Ok(fetched)
        })
    }
}

/// The stock's ticker, which Ondo may file an asset's events under instead of its GM symbol:
/// the one Ondo's answer names, which a stanza's `ticker` must agree with, or the stanza's when
/// the answer names none. Neither is refused: events filed under the ticker would go unseen.
fn resolve_ticker(
    answered: Option<String>,
    configured: Option<&str>,
    symbol: &str,
) -> Result<String, String> {
    match (answered.filter(|ticker| !ticker.is_empty()), configured) {
        (Some(answered), Some(configured)) if !answered.eq_ignore_ascii_case(configured) => {
            Err(format!(
                "Ondo names {symbol}'s stock {answered}, and the stanza's ticker is {configured}"
            ))
        }
        (Some(answered), _) => Ok(answered),
        (None, Some(configured)) => Ok(configured.to_owned()),
        (None, None) => Err(format!(
            "Ondo's price answer for {symbol} names no stock ticker, so events Ondo files under \
             it would go unseen: set `ticker` in the stanza"
        )),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestPrice {
    primary_market: PrimaryMarket,
    underlying_market: UnderlyingMarket,
    timestamp: i64,
}

#[derive(Deserialize)]
struct PrimaryMarket {
    symbol: String,
    price: String,
}

#[derive(Deserialize)]
struct UnderlyingMarket {
    #[serde(default)]
    ticker: Option<String>,
    price: String,
}

/// Parses a latest-price answer for `symbol`, refusing one for another asset.
pub fn parse_price(body: &str, symbol: &str) -> Result<Fetched, String> {
    let answer: LatestPrice = serde_json::from_str(body)
        .map_err(|err| format!("the latest-price answer does not parse: {err}"))?;
    if answer.primary_market.symbol != symbol {
        return Err(format!(
            "asked for {symbol}, Ondo answered for {}",
            answer.primary_market.symbol
        ));
    }
    // The spec says milliseconds and its own example is in seconds: `epoch_ms` reads both.
    let at_ms = epoch_ms(answer.timestamp).ok_or_else(|| {
        format!(
            "timestamp {} is not a time after the epoch",
            answer.timestamp
        )
    })?;
    let price = Price {
        token: positive("token price", &answer.primary_market.price)?,
        stock: positive("stock price", &answer.underlying_market.price)?,
        at_ms,
    };
    Ok(Fetched {
        price,
        ticker: answer.underlying_market.ticker,
    })
}

/// A price as Ondo writes it, a decimal string, which must be a positive finite number.
fn positive(what: &str, written: &str) -> Result<f64, String> {
    match written.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => Ok(value),
        _ => Err(format!("{what} {written:?} is not a positive price")),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketStatus {
    market_status: String,
}

/// The `marketStatus` of `GET /v1/status/market`: `regular`, `premarket`, `postmarket`,
/// `overnight`, `offhours`, `paused` or `closed`.
pub fn parse_market_session(body: &str) -> Result<String, String> {
    serde_json::from_str::<MarketStatus>(body)
        .map(|status| status.market_status)
        .map_err(|err| format!("the market-status answer does not parse: {err}"))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetStatus {
    symbol: String,
    status: String,
    #[serde(default)]
    update_shares_multiplier: Option<bool>,
    #[serde(default)]
    reason: Option<EventReason>,
    /// When it starts: the spec gives the field and not its form, so any of the forms
    /// Ondo writes a time in elsewhere is read ([`instant_ms`]).
    #[serde(default)]
    start: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct EventReason {
    code: Option<String>,
    message: Option<String>,
}

/// The list as the spec describes it, or wrapped in an object, which the spec's examples
/// are not consistent enough to rule out.
#[derive(Deserialize)]
#[serde(untagged)]
enum AssetStatuses {
    Bare(Vec<AssetStatus>),
    Wrapped { assets: Vec<AssetStatus> },
}

/// The statuses of an event with nothing left to happen.
const OVER: [&str; 6] = [
    "ended",
    "completed",
    "resolved",
    "expired",
    "cancelled",
    "canceled",
];

/// Where an event stands, by its `status` in any case.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Stage {
    Active,
    Upcoming,
    /// One of [`OVER`].
    Over,
    /// A word this updater does not know, kept to name it.
    Unknown(String),
}

impl Stage {
    fn of(status: &str) -> Stage {
        let is = |word: &str| status.eq_ignore_ascii_case(word);
        if is("active") {
            Stage::Active
        } else if is("upcoming") {
            Stage::Upcoming
        } else if OVER.into_iter().any(is) {
            Stage::Over
        } else {
            Stage::Unknown(status.to_owned())
        }
    }
}

/// One event of `GET /v1/status/assets`: what [`events_for`] needs to judge it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetEvent {
    symbol: String,
    stage: Stage,
    /// Whether it moves the token's share multiplier (a split, a dividend). The spec
    /// always sends it; an event that does not say is taken to, the costlier mistake
    /// being to quote through one that does.
    moves_multiplier: bool,
    /// Its reason, as `ASSET_PAUSED: split`.
    reason: String,
    /// When it starts, in milliseconds since the epoch, if Ondo says and it reads as a
    /// time.
    start_ms: Option<i64>,
}

/// Every event `GET /v1/status/assets` lists, for any asset.
pub fn parse_asset_events(body: &str) -> Result<Vec<AssetEvent>, String> {
    let statuses = match serde_json::from_str::<AssetStatuses>(body) {
        Ok(AssetStatuses::Bare(statuses) | AssetStatuses::Wrapped { assets: statuses }) => statuses,
        Err(err) => return Err(format!("the asset-status answer does not parse: {err}")),
    };
    Ok(statuses
        .into_iter()
        .map(|event| {
            let reason = event.reason.as_ref();
            let reason = match (
                reason.and_then(|r| r.code.as_deref()),
                reason.and_then(|r| r.message.as_deref()),
            ) {
                (Some(code), Some(message)) => format!("{code}: {message}"),
                (Some(code), None) => code.to_owned(),
                (None, Some(message)) => message.to_owned(),
                (None, None) => format!("{} event", event.status.to_ascii_lowercase()),
            };
            AssetEvent {
                symbol: event.symbol,
                stage: Stage::of(&event.status),
                moves_multiplier: event.update_shares_multiplier.unwrap_or(true),
                reason,
                start_ms: event.start.as_ref().and_then(instant_ms),
            }
        })
        .collect())
}

/// The events that stop quoting the asset known by `names` (its GM symbol, the stock's
/// ticker; in any case), each with when it does ([`Event`]; the core times them):
///
/// - an active event: a pause, or an `ASSET_LIMITED` size limit, now;
/// - an upcoming one that moves the share multiplier, the split or dividend Ondo has
///   scheduled, from the lead before its `start`, ahead of the oracle's `pauseStartTime`
///   (if that is ever set). With no `start`, or one that does not read as a time, there is
///   nothing to quote up to: now;
/// - an upcoming one that leaves the multiplier alone, from its `start`, on this updater's
///   clock rather than once Ondo's cached status turns it active. With no `start`, not
///   until Ondo says it is active;
/// - one in a status this updater does not know, which might be either: now.
///
/// An event keyed or spelt some other way than `names` is invisible here: the ticker comes
/// from Ondo's own price answer (or the stanza's `ticker`, which must agree with it), so the
/// two names are Ondo's.
pub fn events_for(events: &[AssetEvent], names: &[String]) -> Vec<Event> {
    events
        .iter()
        .filter(|event| {
            names
                .iter()
                .any(|name| event.symbol.eq_ignore_ascii_case(name))
        })
        .filter_map(|event| {
            let (why, starts_ms) = match &event.stage {
                Stage::Active => (event.reason.clone(), None),
                Stage::Upcoming if event.moves_multiplier => (
                    format!(
                        "upcoming{}, will move the share multiplier: {}",
                        starting(event.start_ms),
                        event.reason
                    ),
                    event.start_ms,
                ),
                Stage::Upcoming => (
                    format!("upcoming{}: {}", starting(event.start_ms), event.reason),
                    Some(event.start_ms?),
                ),
                Stage::Over => return None,
                Stage::Unknown(status) => (
                    format!(
                        "an event in status {status:?}, which this updater does not know: {}",
                        event.reason
                    ),
                    None,
                ),
            };
            Some(Event {
                why,
                starts_ms,
                moves_multiplier: event.moves_multiplier,
            })
        })
        .collect()
}

/// ` from <start>`, in seconds since the epoch as the on-chain corporate actions are named,
/// for an event's reason, or nothing when it gives none.
fn starting(start_ms: Option<i64>) -> String {
    start_ms.map_or_else(String::new, |ms| format!(" from {}", ms.div_euclid(1_000)))
}

/// An instant as Ondo might write one, in milliseconds since the epoch: a number of
/// milliseconds (or of seconds, when too small to be milliseconds since 1973, as
/// [`parse_price`] reads a timestamp), the same as a string, or an RFC 3339 time with its
/// offset (`2026-10-08T14:05:01Z`, `...01.250-04:00`). `None` for anything else, including
/// a time with no offset, which could be New York's or UTC.
fn instant_ms(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => epoch_ms(number.as_i64()?),
        serde_json::Value::String(written) => match written.parse::<i64>() {
            Ok(number) => epoch_ms(number),
            Err(_) => rfc3339_ms(written),
        },
        _ => None,
    }
}

/// A positive count since the epoch, in milliseconds or in seconds: milliseconds below
/// 1e11 would be 1973, so a count that small can only be seconds.
fn epoch_ms(count: i64) -> Option<i64> {
    match count {
        ..=0 => None,
        1..100_000_000_000 => count.checked_mul(1_000),
        _ => Some(count),
    }
}

/// `YYYY-MM-DDTHH:MM:SS[.fff…](Z|±HH:MM)` (a space for the `T` too), in milliseconds since
/// the epoch.
fn rfc3339_ms(written: &str) -> Option<i64> {
    let bytes = written.as_bytes();
    if !written.is_ascii() || bytes.len() < 20 || !matches!(bytes[10], b'T' | b't' | b' ') {
        return None;
    }
    let date: crate::stock::hours::Date = written[..10].parse().ok()?;
    let number = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = written.get(range)?;
        part.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut rest = &written[19..];
    let mut millis = 0;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let first_three = format!("{:0<3}", &fraction[..digits.min(3)]);
        millis = first_three.parse::<i64>().ok()?;
        rest = &fraction[digits..];
    }
    let offset_secs = match rest.as_bytes() {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let (hours, minutes) = (
                rest[1..3].parse::<i64>().ok()?,
                rest[4..6].parse::<i64>().ok()?,
            );
            if hours > 23 || minutes > 59 {
                return None;
            }
            let secs = hours * 3_600 + minutes * 60;
            if *sign == b'+' { secs } else { -secs }
        }
        _ => return None,
    };
    let days = days_from_civil(date.year, date.month, date.day);
    let local = days * 86_400 + hour * 3_600 + minute * 60 + second;
    (local - offset_secs)
        .checked_mul(1_000)?
        .checked_add(millis)
        .filter(|ms| *ms > 0)
}

/// Whether the key may be sent to `api`: over https, or plain http to this machine only (the
/// smoke test's mock). Anywhere else, plain http would hand the key to the network.
pub fn check_api(api: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(api).map_err(|err| format!("api {api:?} is not a URL: {err}"))?;
    let host = url.host_str().unwrap_or_default();
    match url.scheme() {
        "https" if !host.is_empty() => Ok(()),
        "http" if ["127.0.0.1", "localhost", "[::1]"].contains(&host) => Ok(()),
        "http" => Err(format!(
            "api {api:?} is plain http: the key would cross the network in the clear (http is \
             for this machine only)"
        )),
        _ => Err(format!("api {api:?} is not an https URL")),
    }
}

/// How long one read of Ondo's API may take.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The client every read of Ondo's API goes through: one for the process. Not the run's
/// shared one, which follows redirects, and reqwest carries a custom header like
/// `x-api-key` along to wherever a redirect points. Ondo's API has no reason to redirect,
/// so one is refused (an error naming its status) rather than followed.
pub fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(TIMEOUT)
                .user_agent(concat!("propamm-stocks/", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("a reqwest client with no redirects builds")
        })
        .clone()
}

/// `GET {api}{path}` with the key, answering the body of a 2xx and an error naming the status
/// otherwise. The key is never part of an error.
pub async fn get(http: &reqwest::Client, api: &str, path: &str, key: &str) -> eyre::Result<String> {
    let url = format!("{}{path}", api.trim_end_matches('/'));
    let response = http
        .get(&url)
        .header("x-api-key", key)
        .send()
        .await
        .map_err(|err| eyre::eyre!("GET {url}: {err}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| eyre::eyre!("GET {url}: reading the body: {err}"))?;
    if !status.is_success() {
        let excerpt: String = body.chars().take(200).collect();
        eyre::bail!("GET {url}: HTTP {status}: {excerpt}");
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRICE: &str = r#"{
        "primaryMarket": {"symbol": "AAPLon", "price": "256.791808070580506978"},
        "underlyingMarket": {"ticker": "AAPL", "price": "256.2475"},
        "timestamp": 1791460800123
    }"#;

    #[test]
    fn a_price_answer_gives_the_token_and_stock_prices_and_when() {
        assert_eq!(
            parse_price(PRICE, "AAPLon").map(|fetched| fetched.price),
            Ok(Price {
                token: "256.791808070580506978".parse().unwrap(),
                stock: 256.2475,
                at_ms: 1_791_460_800_123,
            })
        );
    }

    /// The spec says milliseconds, its own example is in seconds: a value too small to be
    /// milliseconds since 1973 is read as seconds.
    #[test]
    fn a_timestamp_in_seconds_is_read_as_seconds() {
        let body = PRICE.replace("1791460800123", "1791460800");
        assert_eq!(
            parse_price(&body, "AAPLon").unwrap().price.at_ms,
            1_791_460_800_000
        );
    }

    #[test]
    fn a_timestamp_that_is_not_a_time_after_the_epoch_is_refused() {
        // The last would overflow once read as seconds.
        for bad in ["0", "-1", "-9223372036854775"] {
            let body = PRICE.replace("1791460800123", bad);
            assert!(
                parse_price(&body, "AAPLon").is_err(),
                "timestamp {bad} parsed"
            );
        }
    }

    #[test]
    fn a_price_for_another_asset_is_refused() {
        let err = parse_price(PRICE, "TSLAon").unwrap_err();
        assert!(err.contains("AAPLon"), "{err}");
    }

    #[test]
    fn a_price_that_is_not_a_positive_number_is_refused() {
        for bad in ["\"0\"", "\"-1\"", "\"abc\"", "\"NaN\"", "\"inf\"", "256.79"] {
            let body = PRICE.replace("\"256.791808070580506978\"", bad);
            assert!(
                parse_price(&body, "AAPLon").is_err(),
                "token price {bad} parsed"
            );
        }
        let body = PRICE.replace("\"256.2475\"", "\"0\"");
        assert!(
            parse_price(&body, "AAPLon").is_err(),
            "a zero stock price parsed"
        );
        assert!(parse_price("{}", "AAPLon").is_err());
        assert!(parse_price("not json", "AAPLon").is_err());
    }

    #[test]
    fn an_instant_reads_in_any_form_ondo_writes_one() {
        // 2026-10-08 14:05:01 UTC.
        let at = 1_791_468_301_000;
        for (written, expected) in [
            (r#""2026-10-08T14:05:01Z""#, Some(at)),
            (r#""2026-10-08T14:05:01.250Z""#, Some(at + 250)),
            (r#""2026-10-08T14:05:01+00:00""#, Some(at)),
            (r#""2026-10-08T10:05:01-04:00""#, Some(at)),
            (r#""2026-10-08 14:05:01Z""#, Some(at)),
            ("1791468301000", Some(at)),
            ("1791468301", Some(at)),
            (r#""1791468301000""#, Some(at)),
            (r#""2026-13-08T14:05:01Z""#, None),
            (r#""2026-10-08T24:05:01Z""#, None),
            (r#""2026-10-08T14:05:01""#, None),
            (r#""next week""#, None),
            ("-1", None),
            ("true", None),
        ] {
            let value: serde_json::Value = serde_json::from_str(written).unwrap();
            assert_eq!(instant_ms(&value), expected, "{written}");
        }
    }

    #[test]
    fn the_market_session_is_its_status_word() {
        let body = r#"{"timestamp":"2026-10-08T14:00:00Z","isOpen":true,"marketStatus":"regular",
            "nextOpenSession":"postmarket","nextOpen":"2026-10-08T20:01:00Z","nextClose":"2026-10-08T19:59:00Z"}"#;
        assert_eq!(parse_market_session(body), Ok("regular".to_owned()));
        assert!(parse_market_session(r#"{"isOpen":true}"#).is_err());
    }

    #[test]
    fn a_price_answer_names_the_underlying_ticker() {
        assert_eq!(
            parse_price(PRICE, "AAPLon").unwrap().ticker.as_deref(),
            Some("AAPL")
        );
        let untickered = PRICE.replace(r#""ticker": "AAPL", "#, "");
        assert_eq!(parse_price(&untickered, "AAPLon").unwrap().ticker, None);
    }

    /// Why the events in `body` stop quoting AAPLon (by its GM symbol and its ticker), at
    /// some point, or `None`.
    fn judge(body: &str) -> Result<Option<String>, String> {
        let names = ["AAPLon".to_owned(), "AAPL".to_owned()];
        parse_asset_events(body).map(|events| whys(&events_for(&events, &names)))
    }

    fn whys(events: &[Event]) -> Option<String> {
        let whys: Vec<&str> = events.iter().map(|event| event.why.as_str()).collect();
        (!whys.is_empty()).then(|| whys.join("; "))
    }

    #[test]
    fn an_active_event_on_another_asset_is_not_this_ones() {
        let body = r#"[
            {"symbol":"TSLAon","status":"active","type":"unscheduled","eventId":"2",
             "updateSharesMultiplier":false,"reason":{"code":"ASSET_PAUSED","message":"halt"}}
        ]"#;
        assert_eq!(judge(body), Ok(None));
        let tsla = ["TSLAon".to_owned()];
        assert_eq!(
            whys(&events_for(&parse_asset_events(body).unwrap(), &tsla)),
            Some("ASSET_PAUSED: halt".to_owned())
        );
    }

    #[test]
    fn an_upcoming_event_that_moves_the_multiplier_withdraws_ahead_of_any_pause_on_chain() {
        // A split Ondo has scheduled: the only warning if the oracle's pauseStartTime is set
        // late or never.
        let body = r#"[
            {"symbol":"AAPLon","status":"upcoming","type":"scheduled","eventId":"1",
             "updateSharesMultiplier":true,"reason":{"code":"ASSET_PAUSED","message":"split"}}
        ]"#;
        let why = judge(body)
            .unwrap()
            .expect("a scheduled split was quoted through");
        assert!(why.contains("split") && why.contains("multiplier"), "{why}");
        // Not saying whether it moves the multiplier is no better than saying it does.
        let unsaid = body.replace(r#""updateSharesMultiplier":true,"#, "");
        assert!(judge(&unsaid).unwrap().is_some());
    }

    #[test]
    fn upcoming_events_that_leave_the_multiplier_alone_and_ended_ones_do_not_withdraw() {
        let body = r#"[
            {"symbol":"AAPLon","status":"upcoming","updateSharesMultiplier":false,
             "reason":{"code":"ASSET_LIMITED","message":"earnings"}},
            {"symbol":"AAPLon","status":"ended","updateSharesMultiplier":true,
             "reason":{"code":"ASSET_PAUSED","message":"split"}},
            {"symbol":"AAPLon","status":"completed"}
        ]"#;
        assert_eq!(judge(body), Ok(None));
    }

    #[test]
    fn events_under_the_ticker_or_in_another_case_are_this_assets() {
        for body in [
            r#"[{"symbol":"AAPL","status":"active","reason":{"code":"ASSET_PAUSED","message":"halt"}}]"#,
            r#"[{"symbol":"aaplon","status":"ACTIVE","reason":{"code":"ASSET_PAUSED","message":"halt"}}]"#,
        ] {
            assert_eq!(
                judge(body),
                Ok(Some("ASSET_PAUSED: halt".to_owned())),
                "{body}"
            );
        }
    }

    #[test]
    fn an_event_in_a_status_this_updater_does_not_know_withdraws() {
        let body = r#"[{"symbol":"AAPLon","status":"halted","reason":{"code":"ASSET_PAUSED"}}]"#;
        let why = judge(body)
            .unwrap()
            .expect("an unknown status was read as nothing");
        assert!(why.contains("halted"), "{why}");
    }

    #[test]
    fn several_active_events_are_all_named_and_a_reason_may_be_missing() {
        let body = r#"[
            {"symbol":"AAPLon","status":"active","reason":{"code":"ASSET_LIMITED","message":"earnings"}},
            {"symbol":"AAPLon","status":"active"}
        ]"#;
        assert_eq!(
            judge(body),
            Ok(Some("ASSET_LIMITED: earnings; active event".to_owned()))
        );
        assert_eq!(judge("[]"), Ok(None));
        assert_eq!(
            judge(r#"{"assets":[]}"#),
            Ok(None),
            "the array may come wrapped"
        );
        assert!(judge(r#"{"other":1}"#).is_err());
        assert!(
            judge(r#"[{"status":"active"}]"#).is_err(),
            "an event for no asset"
        );
    }

    #[test]
    fn the_key_goes_over_https_or_to_this_machine_only() {
        for good in [
            "https://api.gm.ondo.finance",
            "https://api.gm.ondo.finance/",
            "http://127.0.0.1:8556",
            "http://localhost:8556",
            "http://[::1]:8556",
        ] {
            assert_eq!(check_api(good), Ok(()), "{good}");
        }
        for bad in [
            "http://api.gm.ondo.finance",
            "http://127.0.0.1.example.com",
            "ftp://api.gm.ondo.finance",
            "api.gm.ondo.finance",
            "https://",
        ] {
            assert!(check_api(bad).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn the_ticker_is_ondos_or_the_stanzas_and_never_unknown() {
        let ondo = |ticker: &str| Some(ticker.to_owned());
        assert_eq!(
            resolve_ticker(ondo("AAPL"), None, "AAPLon"),
            Ok("AAPL".to_owned())
        );
        assert_eq!(
            resolve_ticker(ondo("AAPL"), Some("aapl"), "AAPLon"),
            Ok("AAPL".to_owned()),
            "the same ticker in another case"
        );
        assert_eq!(
            resolve_ticker(None, Some("AAPL"), "AAPLon"),
            Ok("AAPL".to_owned())
        );
        assert_eq!(
            resolve_ticker(ondo(""), Some("AAPL"), "AAPLon"),
            Ok("AAPL".to_owned())
        );
        let mismatch = resolve_ticker(ondo("TSLA"), Some("AAPL"), "AAPLon").unwrap_err();
        assert!(
            mismatch.contains("TSLA") && mismatch.contains("AAPL"),
            "{mismatch}"
        );
        // Neither: an event Ondo files under the stock's ticker would go unseen.
        let unknown = resolve_ticker(None, None, "AAPLon").unwrap_err();
        assert!(unknown.contains("ticker"), "{unknown}");
        assert!(resolve_ticker(ondo(""), None, "AAPLon").is_err());
    }

    #[tokio::test]
    async fn a_price_answer_without_a_ticker_takes_the_stanzas() {
        use crate::mock_http::{MockApi, json};
        let untickered = PRICE.replace(r#""ticker": "AAPL", "#, "");
        let api = MockApi::serve(vec![("/v1/assets/AAPLon/prices/latest", json(&untickered))]);
        let prices = |ticker: Option<&str>| OndoPrices {
            http: client(),
            api: api.url.clone(),
            key: "key".to_owned(),
            symbol: "AAPLon".to_owned(),
            ticker: ticker.map(str::to_owned),
        };
        let fetched = prices(Some("AAPL")).fetch().await.unwrap();
        assert_eq!(fetched.ticker.as_deref(), Some("AAPL"));
        assert!(prices(None).fetch().await.is_err());
    }

    #[tokio::test]
    async fn a_redirect_is_refused_rather_than_followed_with_the_key() {
        use crate::mock_http::{MockApi, json, respond};
        let elsewhere = MockApi::serve(vec![("/collect", json("[]"))]);
        let location = format!("{}/collect", elsewhere.url);
        let api = MockApi::serve(vec![(
            "/v1/status/assets",
            respond("302 Found", &[("location", &location)], ""),
        )]);
        let err = get(&client(), &api.url, "/v1/status/assets", "secret")
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("302"), "{err}");
        assert_eq!(api.hits("/v1/status/assets"), 1);
        assert_eq!(
            elsewhere.hits("/collect"),
            0,
            "the key followed the redirect"
        );
    }
}
