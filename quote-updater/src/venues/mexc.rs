//! MEXC spot: polled over REST. Its v3 WebSocket streams switched to protobuf frames, and
//! decoding those buys nothing over a one-second poll of the book ticker for a number that
//! is one input to an average. Markets are `ETHUSDC`.

use std::time::Duration;

use eyre::{Result, eyre};
use serde::Deserialize;

use crate::venue::{Connection, Quote};
use crate::venues::unwrap_alias;

pub const DEFAULT_ENDPOINT: &str = "https://api.mexc.com";

/// The book ticker's rate-limit weight is 1 against a budget of hundreds per ten seconds,
/// so once a second is far inside it and close enough to live for an averaged input.
pub const POLL_EVERY: Duration = Duration::from_secs(1);

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Poll {
        url: format!(
            "{}/api/v3/ticker/bookTicker?symbol={symbol}",
            endpoint.trim_end_matches('/')
        ),
        headers: Vec::new(),
        every: POLL_EVERY,
        follow_redirects: true,
    }
}

#[derive(Deserialize)]
struct Body {
    #[serde(rename = "bidPrice")]
    bid: Option<String>,
    #[serde(rename = "askPrice")]
    ask: Option<String>,
    msg: Option<String>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    let body: Body =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a mexc body"))?;
    match (body.bid, body.ask) {
        (Some(bid), Some(ask)) => Ok(Some(Quote::Book { bid, ask })),
        _ => Err(eyre!(
            "mexc bookTicker without prices: {}",
            body.msg.unwrap_or_else(|| text.to_owned())
        )),
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polls_the_book_ticker() {
        assert_eq!(
            connection(DEFAULT_ENDPOINT, "ETHUSDC"),
            Connection::Poll {
                url: "https://api.mexc.com/api/v3/ticker/bookTicker?symbol=ETHUSDC".into(),
                headers: Vec::new(),
                every: POLL_EVERY,
                follow_redirects: true,
            }
        );
    }

    #[test]
    fn reads_the_body_and_reports_an_error_body() {
        let body = r#"{"symbol":"AEUSDT","bidPrice":"0.11001","bidQty":"115.59","askPrice":"0.11127","askQty":"215.48"}"#;
        assert_eq!(
            parse(body).unwrap(),
            Some(Quote::Book {
                bid: "0.11001".into(),
                ask: "0.11127".into()
            })
        );
        let err = parse(r#"{"code":-1121,"msg":"Invalid symbol."}"#).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol"));
    }
}
