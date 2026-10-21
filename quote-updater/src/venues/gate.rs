//! Gate spot WebSocket v4: the `spot.book_ticker` channel, one frame per best bid/ask
//! change, with an application-level `spot.ping`. Markets are `ETH_USDC`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://api.gateio.ws/ws/v4/";

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Gate's ping carries the current time, so it is built when sent rather than once per
/// connection: a stale timestamp is not documented as refused, but there is no reason
/// to find out on a long-lived socket.
fn ping() -> String {
    json!({ "time": now_secs(), "channel": "spot.ping" }).to_string()
}

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.to_owned(),
        subscribe: Some(
            json!({
                "time": now_secs(),
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": [symbol],
            })
            .to_string(),
        ),
        keepalive: Keepalive::Fresh(ping),
    }
}

#[derive(Deserialize)]
struct Frame {
    channel: String,
    event: Option<String>,
    error: Option<serde_json::Value>,
    result: Option<Ticker>,
}

#[derive(Deserialize)]
struct Ticker {
    b: Option<String>,
    a: Option<String>,
    /// The book's update time, in milliseconds.
    t: Option<u64>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the update's `t`.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a gate frame"))?;
    if let Some(error) = frame.error.filter(|e| !e.is_null()) {
        return Err(eyre!("gate error on {}: {error}", frame.channel));
    }
    match (frame.channel.as_str(), frame.event.as_deref()) {
        ("spot.book_ticker", Some("update")) => match frame.result {
            Some(Ticker {
                b: Some(bid),
                a: Some(ask),
                t,
            }) => Ok(Some((Quote::Book { bid, ask }, t.map(epoch_ms)))),
            _ => Err(eyre!("book_ticker update without b/a")),
        },
        // subscribe ack, spot.pong.
        _ => Ok(None),
    }
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}_{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_to_the_book_ticker() {
        let Connection::Stream {
            url,
            subscribe,
            keepalive,
        } = connection(DEFAULT_ENDPOINT, "ETH_USDC")
        else {
            panic!("streams")
        };
        assert_eq!(url, DEFAULT_ENDPOINT);
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["channel"], "spot.book_ticker");
        assert_eq!(sub["event"], "subscribe");
        assert_eq!(sub["payload"], json!(["ETH_USDC"]));
        let ping: serde_json::Value =
            serde_json::from_str(&keepalive.frame().expect("gate pings in text")).unwrap();
        assert_eq!(ping["channel"], "spot.ping");
        assert!(ping["time"].as_u64().unwrap() > 1_700_000_000);
    }

    #[test]
    fn reads_an_update_and_ignores_acks_and_pongs() {
        let update = r#"{"time":1606293275,"time_ms":1606293275723,"channel":"spot.book_ticker","event":"update","result":{"t":1606293275123,"u":48733182,"s":"BTC_USDT","b":"19177.79","B":"0.0003341504","a":"19179.38","A":"0.09"}}"#;
        assert_eq!(
            parse(update).unwrap(),
            Some(Quote::Book {
                bid: "19177.79".into(),
                ask: "19179.38".into()
            })
        );
        let ack = r#"{"time":1611541000,"time_ms":1611541000001,"channel":"spot.book_ticker","event":"subscribe","error":null,"result":{"status":"success"}}"#;
        assert_eq!(parse(ack).unwrap(), None);
        let pong =
            r#"{"time":1545404023,"channel":"spot.pong","event":"","error":null,"result":null}"#;
        assert_eq!(parse(pong).unwrap(), None);
        let refused = r#"{"time":1,"channel":"spot.book_ticker","event":"subscribe","error":{"code":2,"message":"unknown currency pair"},"result":null}"#;
        assert!(
            parse(refused)
                .unwrap_err()
                .to_string()
                .contains("unknown currency pair")
        );
    }

    #[test]
    fn an_update_is_stamped_with_its_t() {
        let update = r#"{"time":1606293275,"time_ms":1606293275723,"channel":"spot.book_ticker","event":"update","result":{"t":1606293275123,"u":48733182,"s":"BTC_USDT","b":"19177.79","B":"0.0003341504","a":"19179.38","A":"0.09"}}"#;
        assert_eq!(
            parse_stamped(update).unwrap().unwrap().1,
            Some(epoch_ms(1_606_293_275_123))
        );
    }
}
