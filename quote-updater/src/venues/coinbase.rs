//! Coinbase Exchange: the `ticker` channel of the market-data feed, one frame per match,
//! plus the `heartbeat` channel so a quiet market still shows the socket is alive. No
//! authentication for either. Markets are `ETH-USD`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{rfc3339_utc, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://ws-feed.exchange.coinbase.com";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.trim_end_matches('/').to_owned(),
        subscribe: Some(
            json!({
                "type": "subscribe",
                "product_ids": [symbol],
                "channels": ["ticker", "heartbeat"],
            })
            .to_string(),
        ),
        keepalive: Keepalive::Ping,
    }
}

#[derive(Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
    message: Option<String>,
    /// When the exchange made the ticker, RFC 3339 (`2022-10-19T23:28:22.061769Z`).
    time: Option<String>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the ticker's `time`. One that does not read as RFC 3339 is taken as
/// absent rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a coinbase frame"))?;
    match frame.kind.as_str() {
        "ticker" => match (frame.best_bid, frame.best_ask) {
            (Some(bid), Some(ask)) => Ok(Some((
                Quote::Book { bid, ask },
                frame.time.as_deref().and_then(rfc3339_utc),
            ))),
            _ => Err(eyre!("ticker without best_bid/best_ask")),
        },
        "error" => Err(eyre!(
            "coinbase error: {}",
            frame.message.unwrap_or_default()
        )),
        // `subscriptions`, `heartbeat`, and anything else the channel adds later.
        _ => Ok(None),
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}-{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_to_the_ticker_and_heartbeat_of_one_product() {
        let Connection::Stream { url, subscribe, .. } = connection(DEFAULT_ENDPOINT, "ETH-USD")
        else {
            panic!("streams")
        };
        assert_eq!(url, DEFAULT_ENDPOINT);
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["type"], "subscribe");
        assert_eq!(sub["product_ids"], json!(["ETH-USD"]));
        assert_eq!(sub["channels"], json!(["ticker", "heartbeat"]));
    }

    #[test]
    fn reads_a_ticker_and_ignores_the_rest() {
        let ticker = r#"{"type":"ticker","sequence":37475248783,"product_id":"ETH-USD","price":"1285.22","open_24h":"1310.79","volume_24h":"245532.79269678","low_24h":"1280.52","high_24h":"1313.8","volume_30d":"9788783.60117027","best_bid":"1285.04","best_bid_size":"0.46688654","best_ask":"1285.27","best_ask_size":"1.56637040","side":"buy","time":"2022-10-19T23:28:22.061769Z","trade_id":370843401,"last_size":"11.4396987"}"#;
        assert_eq!(
            parse(ticker).unwrap(),
            Some(Quote::Book {
                bid: "1285.04".into(),
                ask: "1285.27".into()
            })
        );
        let heartbeat = r#"{"type":"heartbeat","sequence":90,"last_trade_id":20,"product_id":"ETH-USD","time":"2014-11-07T08:19:28.464459Z"}"#;
        assert_eq!(parse(heartbeat).unwrap(), None);
        let ack =
            r#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["ETH-USD"]}]}"#;
        assert_eq!(parse(ack).unwrap(), None);
        let err = r#"{"type":"error","message":"Failed to subscribe","reason":"ETH-XYZ is not a valid product"}"#;
        assert!(
            parse(err)
                .unwrap_err()
                .to_string()
                .contains("Failed to subscribe")
        );
        assert!(parse("nope").is_err());
    }

    #[test]
    fn a_ticker_is_stamped_with_its_time() {
        let push = r#"{"type":"ticker","product_id":"ETH-USD","best_bid":"1285.04","best_ask":"1285.27","time":"2022-10-19T23:28:22.061769Z"}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            rfc3339_utc("2022-10-19T23:28:22.061769Z")
        );
        let unstamped =
            r#"{"type":"ticker","product_id":"ETH-USD","best_bid":"1285.04","best_ask":"1285.27"}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
