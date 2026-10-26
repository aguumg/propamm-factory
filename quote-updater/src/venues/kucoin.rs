//! KuCoin spot public WebSocket. The socket URL is not fixed: a `bullet-public` POST hands
//! out a token and an endpoint, and the socket is dialled at `endpoint?token=…`, so the
//! configured endpoint is the REST host and the connection step needs the HTTP client.
//! Then the `/spotMarket/level1:<symbol>` topic, one frame per best bid/ask change, each
//! stamped with the book's own time, and a JSON ping. Markets are `ETH-USDC`.
//!
//! Not `/market/ticker`: that frame's `time` is the last trade's, not the book's. The book
//! moves without a trade and the frame repeats the old stamp, so on a market that trades
//! every few seconds the stamp reads seconds behind the price and a lateness check on it
//! measures how long since the last trade rather than how late the price is. `level1`
//! stamps the book change itself.

use std::time::SystemTime;

use eyre::{Result, WrapErr, eyre};
use serde::Deserialize;
use serde_json::json;

use crate::venue::{Connection, Keepalive, Quote};
use crate::venues::{epoch_ms_value, unwrap_alias};

pub const DEFAULT_ENDPOINT: &str = "https://api.kucoin.com";

#[derive(Deserialize)]
struct Bullet {
    code: String,
    data: Option<BulletData>,
    msg: Option<String>,
}

#[derive(Deserialize)]
struct BulletData {
    token: String,
    #[serde(rename = "instanceServers")]
    servers: Vec<Server>,
}

#[derive(Deserialize)]
struct Server {
    endpoint: String,
}

pub async fn connection(
    endpoint: &str,
    symbol: &str,
    http: &reqwest::Client,
) -> Result<Connection> {
    let url = format!("{}/api/v1/bullet-public", endpoint.trim_end_matches('/'));
    let bullet: Bullet = http
        .post(&url)
        .send()
        .await
        .wrap_err("kucoin bullet-public request failed")?
        .error_for_status()
        .wrap_err("kucoin bullet-public refused")?
        .json()
        .await
        .wrap_err("kucoin bullet-public is not the expected JSON")?;
    let data = match (bullet.code.as_str(), bullet.data) {
        ("200000", Some(data)) => data,
        (code, _) => {
            return Err(eyre!(
                "kucoin bullet-public answered {code}: {}",
                bullet.msg.unwrap_or_default()
            ));
        }
    };
    let server = data
        .servers
        .first()
        .ok_or_else(|| eyre!("kucoin bullet-public listed no servers"))?;
    Ok(Connection::Stream {
        url: format!("{}?token={}", server.endpoint, data.token),
        subscribe: Some(
            json!({
                "id": "1",
                "type": "subscribe",
                "topic": format!("/spotMarket/level1:{symbol}"),
                "privateChannel": false,
                "response": true,
            })
            .to_string(),
        ),
        keepalive: Keepalive::Text(json!({ "id": "1", "type": "ping" }).to_string()),
    })
}

#[derive(Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    data: Option<Level1>,
}

/// The best level of each side as `[price, size]`, empty when the side is. A missing side
/// is passed on as a zero price, which the shared book check rejects as one-sided.
#[derive(Deserialize)]
struct Level1 {
    #[serde(default)]
    bids: Vec<String>,
    #[serde(default)]
    asks: Vec<String>,
    /// When the book changed, milliseconds since the epoch.
    timestamp: Option<serde_json::Value>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the book's `timestamp`. One that is not a millisecond stamp is taken as
/// absent rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a kucoin frame"))?;
    match frame.kind.as_str() {
        "message" => {
            let book = frame
                .data
                .ok_or_else(|| eyre!("kucoin message without level1 data"))?;
            let side =
                |level: Vec<String>| level.into_iter().next().unwrap_or_else(|| "0".to_owned());
            Ok(Some((
                Quote::Book {
                    bid: side(book.bids),
                    ask: side(book.asks),
                },
                book.timestamp.as_ref().and_then(epoch_ms_value),
            )))
        }
        "error" => Err(eyre!("kucoin error: {text}")),
        // welcome, ack, pong.
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
    fn reads_a_level1_message_and_ignores_the_handshake() {
        let push = r#"{"type":"message","topic":"/spotMarket/level1:BTC-USDT","subject":"level1","data":{"asks":["0.08","0.18"],"bids":["0.049","0.036"],"timestamp":1704873323416}}"#;
        assert_eq!(
            parse(push).unwrap(),
            Some(Quote::Book {
                bid: "0.049".into(),
                ask: "0.08".into()
            })
        );
        // A side the book does not have is an empty array: a zero price, for the shared
        // one-sided check, not a refused frame.
        let one_sided = r#"{"type":"message","topic":"/spotMarket/level1:BTC-USDT","subject":"level1","data":{"asks":[],"bids":["0.049","0.036"],"timestamp":1704873323416}}"#;
        assert_eq!(
            parse(one_sided).unwrap(),
            Some(Quote::Book {
                bid: "0.049".into(),
                ask: "0".into()
            })
        );
        assert_eq!(
            parse(r#"{"id":"hQvf8jkno","type":"welcome"}"#).unwrap(),
            None
        );
        assert_eq!(parse(r#"{"id":"1","type":"ack"}"#).unwrap(), None);
        assert_eq!(parse(r#"{"id":"1","type":"pong"}"#).unwrap(), None);
        assert!(parse(r#"{"id":"1","type":"error","code":404,"data":"topic /spotMarket/level1:NOPE is not found"}"#).is_err());
    }

    /// The bullet call is the one venue step that needs a server; a local one that answers
    /// like KuCoin's proves the URL is assembled from the token and the endpoint.
    #[tokio::test]
    async fn the_socket_url_comes_from_the_bullet_call() {
        use axum::{Router, routing::post};
        let app = Router::new().route(
            "/api/v1/bullet-public",
            post(|| async {
                r#"{"code":"200000","data":{"token":"tok123","instanceServers":[{"endpoint":"wss://ws-api-spot.kucoin.com/","encrypt":true,"protocol":"websocket","pingInterval":18000,"pingTimeout":10000}]}}"#
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let Connection::Stream { url, subscribe, .. } = connection(
            &format!("http://{addr}"),
            "ETH-USDC",
            &reqwest::Client::new(),
        )
        .await
        .unwrap() else {
            panic!("streams")
        };
        assert_eq!(url, "wss://ws-api-spot.kucoin.com/?token=tok123");
        let sub: serde_json::Value = serde_json::from_str(&subscribe.unwrap()).unwrap();
        assert_eq!(sub["topic"], "/spotMarket/level1:ETH-USDC");
    }

    #[test]
    fn a_book_is_stamped_with_its_timestamp() {
        let push = r#"{"type":"message","topic":"/spotMarket/level1:BTC-USDT","subject":"level1","data":{"asks":["0.08","0.18"],"bids":["0.049","0.036"],"timestamp":1704873323416}}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            Some(crate::venues::epoch_ms(1_704_873_323_416))
        );
        let unstamped = r#"{"type":"message","topic":"/spotMarket/level1:BTC-USDT","subject":"level1","data":{"asks":["0.08","0.18"],"bids":["0.049","0.036"]}}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
