//! Bybit v5 spot public WebSocket: the level-1 order book topic, `orderbook.1.<symbol>`,
//! which for spot is snapshot-only (each frame carries the whole top of book) and pushes
//! every 10ms while it changes. Bybit's spot `tickers` topic carries no bid or ask, which
//! is why this is not it. The keepalive is a JSON `ping` op. Markets are `ETHUSDC`.

use std::time::SystemTime;

use eyre::{Result, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms_value, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "wss://stream.bybit.com/v5/public/spot";

pub fn connection(endpoint: &str, symbol: &str) -> Connection {
    Connection::Stream {
        url: endpoint.trim_end_matches('/').to_owned(),
        subscribe: Some(
            json!({ "op": "subscribe", "args": [format!("orderbook.1.{symbol}")] }).to_string(),
        ),
        keepalive: Keepalive::Text(json!({ "op": "ping" }).to_string()),
    }
}

#[derive(Deserialize)]
struct Frame {
    op: Option<String>,
    success: Option<bool>,
    ret_msg: Option<String>,
    data: Option<Book>,
    /// When the exchange made the frame, milliseconds since the epoch.
    ts: Option<serde_json::Value>,
}

/// Each level is `[price, size]`; level 1 has at most one on each side, and an empty side
/// is a book with nothing on it, which the shared scaling refuses as one-sided.
#[derive(Deserialize)]
struct Book {
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the frame's `ts`. One that is not a millisecond stamp is taken as absent
/// rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a bybit frame"))?;
    if let Some(false) = frame.success {
        return Err(eyre!(
            "bybit refused {}: {}",
            frame.op.unwrap_or_default(),
            frame.ret_msg.unwrap_or_default()
        ));
    }
    if frame.op.is_some() {
        // A ping's pong or a subscribe's ack.
        return Ok(None);
    }
    let book = frame
        .data
        .ok_or_else(|| eyre!("bybit frame with neither op nor data"))?;
    let side = |levels: Vec<Vec<String>>| {
        levels
            .into_iter()
            .next()
            .and_then(|level| level.into_iter().next())
            .unwrap_or_else(|| "0".to_owned())
    };
    Ok(Some((
        Quote::Book {
            bid: side(book.b),
            ask: side(book.a),
        },
        frame.ts.as_ref().and_then(epoch_ms_value),
    )))
}

pub fn expected_symbol(base: &str, quote: &str) -> String {
    format!("{}{}", unwrap_alias(base), unwrap_alias(quote))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribes_to_the_top_of_book() {
        let Connection::Stream {
            subscribe,
            keepalive,
            ..
        } = connection(DEFAULT_ENDPOINT, "ETHUSDC")
        else {
            panic!("streams")
        };
        assert_eq!(
            subscribe.unwrap(),
            r#"{"args":["orderbook.1.ETHUSDC"],"op":"subscribe"}"#
        );
        assert_eq!(keepalive, Keepalive::Text(r#"{"op":"ping"}"#.into()));
    }

    #[test]
    fn reads_the_top_level_and_ignores_acks() {
        let push = r#"{"topic":"orderbook.1.BTCUSDT","ts":"1772694601512","type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":18521288,"seq":7961638724}}"#;
        assert_eq!(
            parse(push).unwrap(),
            Some(Quote::Book {
                bid: "16493.50".into(),
                ask: "16611.00".into()
            })
        );
        // An empty side is a one-sided book, handed on as a zero for the shared check.
        let empty = r#"{"topic":"orderbook.1.X","ts":"1","type":"snapshot","data":{"s":"X","b":[],"a":[["1.0","2"]],"u":1,"seq":1}}"#;
        assert_eq!(
            parse(empty).unwrap(),
            Some(Quote::Book {
                bid: "0".into(),
                ask: "1.0".into()
            })
        );
        assert_eq!(
            parse(r#"{"success":true,"ret_msg":"pong","conn_id":"c","op":"ping"}"#).unwrap(),
            None
        );
        assert_eq!(
            parse(r#"{"success":true,"ret_msg":"subscribe","conn_id":"c","req_id":"1","op":"subscribe"}"#).unwrap(),
            None
        );
        let refused = r#"{"success":false,"ret_msg":"Invalid symbol :[orderbook.1.NOPE]","conn_id":"c","op":"subscribe"}"#;
        assert!(
            parse(refused)
                .unwrap_err()
                .to_string()
                .contains("Invalid symbol")
        );
    }

    #[test]
    fn a_book_push_is_stamped_with_its_ts() {
        let push = r#"{"topic":"orderbook.1.BTCUSDT","ts":1772694601512,"type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":18521288,"seq":7961638724}}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            Some(crate::venues::epoch_ms(1_772_694_601_512))
        );
        let unstamped = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":1,"seq":1}}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
