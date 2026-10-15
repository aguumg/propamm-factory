//! Binance: the `<symbol>@bookTicker` stream, one frame per change of the best bid or ask.
//! The market is named in the URL, so there is no subscribe message, and the server pings
//! on its own, so a protocol ping is the right probe for a quiet book.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms, unwrap_alias};

/// The main Binance stream. Overridden from the settings for binance.us or a mock.
pub const DEFAULT_ENDPOINT: &str = "wss://stream.binance.com:9443/ws";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: format!(
            "{}/{}@bookTicker",
            endpoint.trim_end_matches('/'),
            symbol.to_lowercase()
        ),
        subscribe: None,
        keepalive: Keepalive::Ping,
    }
}

/// Best bid/offer update as sent on a `<symbol>@bookTicker` stream.
#[derive(Deserialize)]
struct BookTicker {
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
    /// The event time, in milliseconds. The spot bookTicker stream does not send one (its
    /// frame is `u s b B a A`), so on the default endpoint a Binance price is aged by when
    /// it arrived, which sees a stream gone silent but not one delivering a backlog on time
    /// (`max_source_divergence` is what catches that); read when present, for an endpoint
    /// whose bookTicker carries it.
    #[serde(rename = "E")]
    event_ms: Option<u64>,
}

/// Every frame on a bookTicker stream is a ticker, so anything else is malformed rather
/// than ignorable.
pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the frame's event time when it carries one.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let ticker: BookTicker = serde_json::from_str(text)
        .map_err(|err| eyre!(err).wrap_err("not a bookTicker message"))?;
    Ok(Some((
        Quote::Book {
            bid: ticker.bid,
            ask: ticker.ask,
        },
        ticker.event_ms.map(epoch_ms),
    )))
}

/// The stream a pair declared as `(base, quote)` should be reading: `ETHUSDC`. Order
/// matters: it is what makes a transposed `tokens` array detectable.
pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_market_is_named_in_the_url_in_lower_case() {
        let Connection::Stream {
            url,
            subscribe,
            keepalive,
        } = connection("wss://stream.binance.com:9443/ws/", "ETHUSDC")
        else {
            panic!("streams")
        };
        assert_eq!(url, "wss://stream.binance.com:9443/ws/ethusdc@bookTicker");
        assert_eq!(subscribe, None);
        assert_eq!(keepalive, Keepalive::Ping);
    }

    #[test]
    fn a_book_ticker_frame_yields_its_bid_and_ask_verbatim() {
        let msg = r#"{"u":1,"s":"USDCUSDT","b":"0.99980000","B":"2.0","a":"1.00000000","A":"3.0"}"#;
        assert_eq!(
            parse(msg).unwrap(),
            Some(Quote::Book {
                bid: "0.99980000".into(),
                ask: "1.00000000".into(),
            })
        );
        assert!(parse("{}").is_err());
        assert!(parse("not json").is_err());
    }

    /// The spot frame carries no time, so the price is aged by its arrival; a frame that
    /// does carry `E` is stamped with it.
    #[test]
    fn the_event_time_is_read_only_when_the_frame_has_one() {
        let spot = r#"{"u":1,"s":"ETHUSDT","b":"4000.00","B":"2.0","a":"4000.01","A":"3.0"}"#;
        assert_eq!(parse_stamped(spot).unwrap().unwrap().1, None);
        let stamped = r#"{"e":"bookTicker","u":1,"E":1760000000123,"s":"ETHUSDT","b":"4000.00","B":"2.0","a":"4000.01","A":"3.0"}"#;
        let (quote, at) = parse_stamped(stamped).unwrap().unwrap();
        assert_eq!(at, Some(epoch_ms(1_760_000_000_123)));
        assert_eq!(Some(quote), parse(stamped).unwrap());
    }

    #[test]
    fn wrapped_tokens_map_to_their_underlying_market() {
        assert_eq!(expected_symbol("WETH", "USDC"), "ETHUSDC");
        assert_eq!(expected_symbol("WBTC", "USDT"), "BTCUSDT");
        assert_eq!(expected_symbol("USDC", "USDT"), "USDCUSDT");
    }
}
