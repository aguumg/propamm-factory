//! One module per venue: where to connect, what to send to be told about a market, what
//! its frames look like, and how it spells a market. Nothing else; see `venue.rs`.

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod coinbase;
pub mod coingecko;
pub mod gate;
pub mod kraken;
pub mod kucoin;
pub mod mexc;
pub mod okx;

/// An exchange's millisecond timestamp as a wall-clock time: what a venue that stamps its
/// frames (OKX's `ts`, Gate's `t`) says the price is as of, for the feed's source age.
pub fn epoch_ms(ms: u64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms)
}

/// The quotes a market may be in for a pair quoted in `quote`, besides `quote` itself:
/// for a USDC or USDT pair, the other of the two and USD, since the busiest market an
/// exchange has for a dollar pair is often in one of the others (Coinbase's ETH-USDC is
/// delisted; ETH-USD is where it trades). Nothing for any other quote. The lookup picks a
/// market by this, and the startup cross-check accepts one by it, so the two agree.
pub fn dollar_stand_ins(quote: &str) -> &'static [&'static str] {
    match quote.to_uppercase().as_str() {
        "USDC" => &["USDT", "USD"],
        "USDT" => &["USDC", "USD"],
        _ => &[],
    }
}

/// The spelling most venues share for the underlying of a wrapped ERC-20: they trade
/// ETH and BTC, not WETH, WBTC or cbBTC. Every wrapper `discover::underlying` looks up by
/// its underlying's markets has to be here too, or the lookup finds those markets and then
/// discards them all for naming a different base (a test holds the two lists together).
pub fn unwrap_alias(symbol: &str) -> &str {
    match symbol {
        "WETH" => "ETH",
        "WBTC" | "CBBTC" => "BTC",
        other => other,
    }
}
