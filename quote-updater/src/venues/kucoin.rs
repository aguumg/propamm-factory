//! KuCoin spot public WebSocket. The socket URL is not fixed: a `bullet-public` POST hands
//! out a token and an endpoint, and the socket is dialled at `endpoint?token=…`, so the
//! configured endpoint is the REST host and the connection step needs the HTTP client.
//! Then the `/market/ticker:<symbol>` topic, one frame per best bid/ask change, and a
//! JSON ping. Markets are `ETH-USDC`.

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
                "topic": format!("/market/ticker:{symbol}"),
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
    data: Option<Ticker>,
}

#[derive(Deserialize)]
struct Ticker {
    #[serde(rename = "bestBid")]
    bid: String,
    #[serde(rename = "bestAsk")]
    ask: String,
    /// When the exchange made the ticker, milliseconds since the epoch.
    time: Option<serde_json::Value>,
}

pub fn parse(text: &str) -> Result<Option<Quote>> {
    Ok(parse_stamped(text)?.map(|(quote, _)| quote))
}

/// [`parse`], with the ticker's `time`. One that is not a millisecond stamp is taken as
/// absent rather than refusing the price: the price is what the frame is for.
pub fn parse_stamped(text: &str) -> Result<Option<(Quote, Option<SystemTime>)>> {
    let frame: Frame =
        serde_json::from_str(text).map_err(|err| eyre!(err).wrap_err("not a kucoin frame"))?;
    match frame.kind.as_str() {
        "message" => {
            let ticker = frame
                .data
                .ok_or_else(|| eyre!("kucoin message without ticker data"))?;
            Ok(Some((
                Quote::Book {
                    bid: ticker.bid,
                    ask: ticker.ask,
                },
                ticker.time.as_ref().and_then(epoch_ms_value),
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
    fn reads_a_ticker_message_and_ignores_the_handshake() {
        let push = r#"{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"sequence":"1545896668986","price":"0.08","size":"0.011","bestAsk":"0.08","bestAskSize":"0.18","bestBid":"0.049","bestBidSize":"0.036","time":1704873323416}}"#;
        assert_eq!(
            parse(push).unwrap(),
            Some(Quote::Book {
                bid: "0.049".into(),
                ask: "0.08".into()
            })
        );
        assert_eq!(
            parse(r#"{"id":"hQvf8jkno","type":"welcome"}"#).unwrap(),
            None
        );
        assert_eq!(parse(r#"{"id":"1","type":"ack"}"#).unwrap(), None);
        assert_eq!(parse(r#"{"id":"1","type":"pong"}"#).unwrap(), None);
        assert!(parse(r#"{"id":"1","type":"error","code":404,"data":"topic /market/ticker:NOPE is not found"}"#).is_err());
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
        assert_eq!(sub["topic"], "/market/ticker:ETH-USDC");
    }

    #[test]
    fn a_ticker_is_stamped_with_its_time() {
        let push = r#"{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"bestAsk":"0.08","bestBid":"0.049","time":1704873323416}}"#;
        assert_eq!(
            parse_stamped(push).unwrap().unwrap().1,
            Some(crate::venues::epoch_ms(1_704_873_323_416))
        );
        let unstamped = r#"{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"bestAsk":"0.08","bestBid":"0.049"}}"#;
        assert_eq!(parse_stamped(unstamped).unwrap().unwrap().1, None);
    }
}
