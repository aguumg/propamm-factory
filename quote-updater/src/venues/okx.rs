//! OKX v5 public WebSocket: the `tickers` channel. The keepalive is the literal text
//! `ping`, answered with the literal `pong`, and OKX drops a socket silent for 30s, which
//! the feed loop's probe interval stays well inside. Markets are `ETH-USDC`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.trim_end_matches('/').to_owned(),
        subscribe: Some(
            json!({
                "op": "subscribe",
                "args": [{ "channel": "tickers", "instId": symbol }],
            })
            .to_string(),
        ),
        keepalive: Keepalive::Text("ping".to_owned()),
    }
}

#[derive(Deserialize)]
struct Frame {
    event: Option<String>,
    msg: Option<String>,
    data: Option<Vec<Ticker>>,
}

#[derive(Deserialize)]
struct Ticker {
    #[serde(rename = "bidPx")]
    bid: String,
    #[serde(rename = "askPx")]
    ask: String,
    /// When OKX generated the ticker, in milliseconds, as a decimal string.
    ts: Option<String>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the ticker's `ts`. A `ts` that is not a number is read as absent rather
/// than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    if text == "pong" {
        return Ok(None);
    }
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not an okx frame"))?;
    match frame.event.as_deref() {
        Some("error") => Err(eyre!("okx error: {}", frame.msg.unwrap_or_default())),
        Some(_) => Ok(None),
        None => match frame.data.and_then(|mut data| data.pop()) {
            Some(ticker) => Ok(Some((
                Quote::Book {
                    bid: ticker.bid,
                    ask: ticker.ask,
                },
                ticker
                    .ts
                    .and_then(|ts| ts.parse::<u64>().ok())
                    .map(epoch_ms),
            ))),
            None => Err(eyre!("okx frame with neither event nor data")),
        },
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}-{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_and_pings_in_plain_text() {
        let Connection::Stream {
            subscribe,
            keepalive,
            ..
        } = connection(DEFAULT_ENDPOINT, "ETH-USDC")
        else {
            panic!("streams")
        };
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["args"][0]["channel"], "tickers");
        assert_eq!(sub["args"][0]["instId"], "ETH-USDC");
        assert_eq!(keepalive, Keepalive::Text("ping".into()));
    }

    #[test]
    fn reads_a_ticker_push_and_ignores_acks_and_pongs() {
        let push = r#"{"arg":{"channel":"tickers","instId":"ETH-USDC"},"data":[{"instType":"SPOT","instId":"ETH-USDC","last":"9999.99","lastSz":"0.1","askPx":"9999.99","askSz":"11","bidPx":"8888.88","bidSz":"5","open24h":"9000","high24h":"10000","low24h":"8888.88","volCcy24h":"2222","vol24h":"2222","sodUtc0":"2222","sodUtc8":"2222","ts":"1597026383085"}]}"#;
        assert_eq!(
            parse(push).unwrap(),
            Some(Quote::Book {
                bid: "8888.88".into(),
                ask: "9999.99".into()
            })
        );
        assert_eq!(parse("pong").unwrap(), None);
        assert_eq!(
            parse(r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"ETH-USDC"},"connId":"a"}"#).unwrap(),
            None
        );
        let err = r#"{"event":"error","code":"60018","msg":"Wrong URL or channel:tickers,instId:ETH-XYZ doesn't exist","connId":"a"}"#;
        assert!(
            parse(err)
                .unwrap_err()
                .to_string()
                .contains("doesn't exist")
        );
        assert!(parse("{}").is_err());
    }

    #[test]
    fn a_ticker_push_is_stamped_with_its_ts() {
        let push = r#"{"arg":{"channel":"tickers","instId":"ETH-USDC"},"data":[{"instId":"ETH-USDC","askPx":"4000.01","bidPx":"4000.00","ts":"1597026383085"}]}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            Some(epoch_ms(1_597_026_383_085))
        );
        let unstamped = r#"{"arg":{"channel":"tickers","instId":"ETH-USDC"},"data":[{"instId":"ETH-USDC","askPx":"4000.01","bidPx":"4000.00"}]}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
