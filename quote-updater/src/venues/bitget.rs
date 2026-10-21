//! Bitget v2 spot public WebSocket: the `ticker` channel. The keepalive is the literal
//! text `ping`, answered `pong`; Bitget drops a socket that sends none for two minutes.
//! Markets are `ETHUSDC`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms_value, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://ws.bitget.com/v2/ws/public";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.trim_end_matches('/').to_owned(),
        subscribe: Some(
            json!({
                "op": "subscribe",
                "args": [{ "instType": "SPOT", "channel": "ticker", "instId": symbol }],
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
    #[serde(rename = "bidPr")]
    bid: String,
    #[serde(rename = "askPr")]
    ask: String,
    /// When the exchange made the ticker, milliseconds since the epoch as a string.
    ts: Option<serde_json::Value>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the ticker's `ts`. One that is not a millisecond stamp is taken as
/// absent rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    if text == "pong" {
        return Ok(None);
    }
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a bitget frame"))?;
    match frame.event.as_deref() {
        Some("error") => Err(eyre!("bitget error: {}", frame.msg.unwrap_or_default())),
        Some(_) => Ok(None),
        None => match frame.data.and_then(|mut data| data.pop()) {
            Some(ticker) => Ok(Some((
                Quote::Book {
                    bid: ticker.bid,
                    ask: ticker.ask,
                },
                ticker.ts.as_ref().and_then(epoch_ms_value),
            ))),
            None => Err(eyre!("bitget frame with neither event nor data")),
        },
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_to_the_spot_ticker() {
        let Connection::Stream {
            subscribe,
            keepalive,
            ..
        } = connection(DEFAULT_ENDPOINT, "ETHUSDC")
        else {
            panic!("streams")
        };
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["args"][0]["instType"], "SPOT");
        assert_eq!(sub["args"][0]["channel"], "ticker");
        assert_eq!(sub["args"][0]["instId"], "ETHUSDC");
        assert_eq!(keepalive, Keepalive::Text("ping".into()));
    }

    #[test]
    fn reads_a_snapshot_and_ignores_acks_and_pongs() {
        let push = r#"{"action":"snapshot","arg":{"instType":"SPOT","channel":"ticker","instId":"ETHUSDC"},"data":[{"instId":"ETHUSDC","lastPr":"2000.1","open24h":"1990","high24h":"2010","low24h":"1980","change24h":"0.005","bidPr":"2000.0","askPr":"2000.2","bidSz":"1.2","askSz":"0.8","baseVolume":"100","quoteVolume":"200000","openUtc":"1995","changeUtc24h":"0.002","ts":"1700000000000"}],"ts":1700000000000}"#;
        assert_eq!(
            parse(push).unwrap(),
            Some(Quote::Book {
                bid: "2000.0".into(),
                ask: "2000.2".into()
            })
        );
        assert_eq!(parse("pong").unwrap(), None);
        assert_eq!(
            parse(r#"{"event":"subscribe","arg":{"instType":"SPOT","channel":"ticker","instId":"ETHUSDC"}}"#).unwrap(),
            None
        );
        assert!(
            parse(r#"{"event":"error","code":30001,"msg":"instId:NOPE doesn't exist"}"#).is_err()
        );
    }

    #[test]
    fn a_ticker_push_is_stamped_with_its_ts() {
        let push = r#"{"action":"snapshot","arg":{"instType":"SPOT","channel":"ticker","instId":"ETHUSDC"},"data":[{"instId":"ETHUSDC","bidPr":"2000.0","askPr":"2000.2","ts":"1700000000000"}],"ts":1700000000000}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            Some(crate::venues::epoch_ms(1_700_000_000_000))
        );
        let unstamped = r#"{"action":"snapshot","arg":{"instType":"SPOT","channel":"ticker","instId":"ETHUSDC"},"data":[{"instId":"ETHUSDC","bidPr":"2000.0","askPr":"2000.2"}]}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
