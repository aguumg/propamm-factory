//! Ondo Global Markets: the issuer's own price for one of its stock tokens (AAPLon, ...),
//! polled from its GM API. That price is the one Ondo mints and redeems the token around,
//! the stock's price times the token's share multiplier, so it already carries every
//! split and reinvested dividend. The API is partner-only: its key comes from the
//! `ONDO_API_KEY` environment variable and goes with every request as `x-api-key`, over
//! https only (plain http to this machine, for a mock). Ondo caches the price for a
//! second, so it is polled once a second. Markets are the token's symbol, `AAPLon`.
//!
//! The answer carries one price, not a book, and the stamp is Ondo's own `timestamp`.

use std::time::{Duration, SystemTime};

use eyre::{Result, eyre};
use serde::Deserialize;

use crate::venue::{Connection, Quote};

pub const DEFAULT_ENDPOINT: &str = "https://api.gm.ondo.finance";

/// The environment variable holding the GM API key.
pub const API_KEY_VAR: &str = "ONDO_API_KEY";

/// Ondo caches the price for a second, so polling faster buys nothing.
pub const POLL_EVERY: Duration = Duration::from_secs(1);

/// The poll of `symbol`'s latest price at `endpoint`, with the key from [`API_KEY_VAR`].
/// Refused without a key, and on an endpoint the key must not cross the network to.
pub fn connection(endpoint: &str, symbol: &str) -> Result<Connection> {
    let key = std::env::var(API_KEY_VAR)
        .ok()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| eyre!("{API_KEY_VAR} is not set, and Ondo's API needs its key"))?;
    let endpoint = endpoint.trim_end_matches('/');
    check_endpoint(endpoint).map_err(|err| eyre!(err))?;
    Ok(Connection::Poll {
        url: format!("{endpoint}/v1/assets/{symbol}/prices/latest"),
        headers: vec![("x-api-key".to_owned(), key)],
        every: POLL_EVERY,
        // The key would go along to wherever a redirect pointed; Ondo's API has no reason
        // to redirect, so one is a refused request instead.
        follow_redirects: false,
    })
}

/// The key only goes over https, or over plain http to this machine (a mock).
pub fn check_endpoint(endpoint: &str) -> std::result::Result<(), String> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|err| format!("endpoint {endpoint:?} is not a URL: {err}"))?;
    let host = url.host_str().unwrap_or_default();
    match url.scheme() {
        "https" if !host.is_empty() => Ok(()),
        "http" if ["127.0.0.1", "localhost", "[::1]"].contains(&host) => Ok(()),
        "http" => Err(format!(
            "endpoint {endpoint:?} is plain http: the key would cross the network in the \
             clear (http is for this machine only)"
        )),
        _ => Err(format!("endpoint {endpoint:?} is not an https URL")),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Body {
    primary_market: Option<PrimaryMarket>,
    /// When Ondo priced it: milliseconds since the epoch by the spec, seconds in its own
    /// example, so both are read.
    timestamp: Option<i64>,
    error: Option<serde_json::Value>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct PrimaryMarket {
    price: String,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with Ondo's `timestamp` as the stamp. A count below 1e11 can only be
/// seconds (milliseconds that small would be 1973).
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let body: Body =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not an ondo body"))?;
    let Some(market) = body.primary_market else {
        let why = body
            .message
            .or_else(|| body.error.map(|error| error.to_string()))
            .unwrap_or_else(|| text.to_owned());
        return Err(eyre!("ondo price answer without a price: {why}"));
    };
    let stamp = body.timestamp.and_then(|count| match count {
        ..=0 => None,
        1..100_000_000_000 => Some(count as u64 * 1_000),
        _ => Some(count as u64),
    });
    Ok(Some((
        Quote::Mid(market.price),
        stamp.map(crate::venues::epoch_ms),
    )))
}

/// The token's own symbol: Ondo names the market by it, whatever the quote token.
pub fn expected_symbol(base: &str, _quote: &str) -> String {
    base.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANSWER: &str = r#"{"primaryMarket":{"symbol":"AAPLon","price":"256.79"},"underlyingMarket":{"ticker":"AAPL","price":"256.24573"},"timestamp":1791468000000}"#;

    #[test]
    fn reads_the_token_price_stamped_with_ondos_time() {
        let (quote, stamp) = parse_stamped(ANSWER).unwrap().unwrap();
        assert_eq!(quote, Quote::Mid("256.79".into()));
        assert_eq!(stamp, Some(crate::venues::epoch_ms(1_791_468_000_000)));
        // Seconds are read as seconds.
        let seconds = ANSWER.replace("1791468000000", "1791468000");
        assert_eq!(
            parse_stamped(&seconds).unwrap().unwrap().1,
            Some(crate::venues::epoch_ms(1_791_468_000_000))
        );
        let unstamped = r#"{"primaryMarket":{"symbol":"AAPLon","price":"256.79"}}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }

    #[test]
    fn an_answer_without_a_price_is_refused_with_its_message() {
        let err = parse(r#"{"message":"asset not found"}"#).unwrap_err();
        assert!(err.to_string().contains("asset not found"), "{err}");
        assert!(parse("nope").is_err());
    }

    #[test]
    fn the_key_goes_over_https_or_to_this_machine_only() {
        assert!(check_endpoint(DEFAULT_ENDPOINT).is_ok());
        assert!(check_endpoint("http://127.0.0.1:8556").is_ok());
        assert!(check_endpoint("http://localhost:8556/").is_ok());
        assert!(check_endpoint("http://api.example.com").is_err());
        assert!(check_endpoint("ftp://api.example.com").is_err());
        assert!(check_endpoint("not a url").is_err());
    }

    #[test]
    fn the_market_is_the_tokens_own_symbol() {
        assert_eq!(expected_symbol("AAPLon", "USDC"), "AAPLon");
        assert_eq!(expected_symbol("TSLAon", "USDT"), "TSLAon");
    }
}
