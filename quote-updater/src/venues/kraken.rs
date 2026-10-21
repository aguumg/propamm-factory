//! Kraken WebSocket v2: the `ticker` channel, one frame per book change, with bid and ask
//! as JSON numbers rather than strings. Heartbeats arrive every second on their own
//! channel and the application-level ping is a JSON method. Markets are `ETH/USD`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{rfc3339_utc, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://ws.kraken.com/v2";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.trim_end_matches('/').to_owned(),
        subscribe: Some(
            json!({
                "method": "subscribe",
                "params": { "channel": "ticker", "symbol": [symbol] },
            })
            .to_string(),
        ),
        keepalive: Keepalive::Text(json!({ "method": "ping" }).to_string()),
    }
}

#[derive(Deserialize)]
struct Frame {
    channel: Option<String>,
    method: Option<String>,
    success: Option<bool>,
    error: Option<String>,
    /// Typed only once the channel is known: the status channel also carries `data`, in
    /// a shape that is not a ticker, and must not read as a malformed frame. Raw, so the
    /// prices inside reach `Ticker` as the text on the wire.
    data: Option<Box<serde_json::value::RawValue>>,
}

/// Kraken sends prices as JSON numbers. Kept as the text on the wire (`RawValue`), not
/// re-printed from an f64: a small price would come back as `8.31e-6`, which the scaling
/// accepts but which is one more transformation than a published number should go through.
#[derive(Deserialize)]
struct Ticker {
    bid: Box<serde_json::value::RawValue>,
    ask: Box<serde_json::value::RawValue>,
    /// When the exchange made the ticker, RFC 3339 (`2023-09-25T09:04:31.742648Z`).
    timestamp: Option<String>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the ticker's `timestamp`. One that does not read as RFC 3339 is taken
/// as absent rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a kraken frame"))?;
    if let Some(false) = frame.success {
        return Err(eyre!(
            "kraken refused {}: {}",
            frame.method.unwrap_or_default(),
            frame.error.unwrap_or_default()
        ));
    }
    match frame.channel.as_deref() {
        Some("ticker") => {
            let mut data: Vec<Ticker> = frame
                .data
                .map(|raw| serde_json::from_str(raw.get()))
                .transpose()
                .map_err(|err| eyre!(err).wrap_err("kraken ticker data"))?
                .unwrap_or_default();
            let ticker = data
                .pop()
                .ok_or_else(|| eyre!("ticker frame without data"))?;
            Ok(Some((
                Quote::Book {
                    bid: ticker.bid.get().trim_matches('"').to_owned(),
                    ask: ticker.ask.get().trim_matches('"').to_owned(),
                },
                ticker.timestamp.as_deref().and_then(rfc3339_utc),
            )))
        }
        // heartbeat, status, and method replies (subscribe ack, pong).
        _ => Ok(None),
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}/{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_with_a_json_ping() {
        let Connection::Stream {
            subscribe,
            keepalive,
            ..
        } = connection(DEFAULT_ENDPOINT, "ETH/USD")
        else {
            panic!("streams")
        };
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["params"]["channel"], "ticker");
        assert_eq!(sub["params"]["symbol"], json!(["ETH/USD"]));
        assert_eq!(keepalive, Keepalive::Text(r#"{"method":"ping"}"#.into()));
    }

    #[test]
    fn reads_numeric_bid_and_ask_and_ignores_heartbeats() {
        let snapshot = r#"{"channel":"ticker","type":"snapshot","data":[{"symbol":"ETH/USD","bid":3999.5,"bid_qty":740.0,"ask":4000.25,"ask_qty":740.0,"last":4000.0,"volume":997038.98,"vwap":0.10148,"low":0.09979,"high":0.10285,"change":-0.00017,"change_pct":-0.17,"timestamp":"2023-09-25T09:04:31.742648Z"}]}"#;
        assert_eq!(
            parse(snapshot).unwrap(),
            Some(Quote::Book {
                bid: "3999.5".into(),
                ask: "4000.25".into()
            })
        );
        // A small price keeps its digits exactly as sent, exponent or not.
        let small = r#"{"channel":"ticker","type":"update","data":[{"symbol":"X/USD","bid":0.00000831,"ask":8.32e-6}]}"#;
        assert_eq!(
            parse(small).unwrap(),
            Some(Quote::Book {
                bid: "0.00000831".into(),
                ask: "8.32e-6".into()
            })
        );
        assert_eq!(parse(r#"{"channel":"heartbeat"}"#).unwrap(), None);
        assert_eq!(
            parse(r#"{"channel":"status","type":"update","data":[{"system":"online"}]}"#).unwrap(),
            None
        );
        assert_eq!(
            parse(r#"{"method":"pong","req_id":101,"time_in":"t","time_out":"t"}"#).unwrap(),
            None
        );
        assert_eq!(
            parse(r#"{"method":"subscribe","result":{"channel":"ticker","symbol":"ETH/USD"},"success":true,"time_in":"t","time_out":"t"}"#).unwrap(),
            None
        );
        let refused = r#"{"method":"subscribe","success":false,"error":"Currency pair not supported","time_in":"t","time_out":"t"}"#;
        assert!(
            parse(refused)
                .unwrap_err()
                .to_string()
                .contains("not supported")
        );
    }

    #[test]
    fn a_ticker_is_stamped_with_its_timestamp() {
        let push = r#"{"channel":"ticker","type":"update","data":[{"symbol":"ETH/USD","bid":3999.5,"ask":4000.25,"timestamp":"2023-09-25T09:04:31.742648Z"}]}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            rfc3339_utc("2023-09-25T09:04:31.742648Z")
        );
        let unstamped = r#"{"channel":"ticker","type":"update","data":[{"symbol":"ETH/USD","bid":3999.5,"ask":4000.25}]}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
