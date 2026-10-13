//! propamm-stocks: the quote updater for tokenized stocks, assembled from the `quote_updater`
//! library with a stock core of its own and one module per token design:
//!
//! - `ports`: what the core asks of a design (a price, the token's share multiplier and its
//!   corporate actions, the issuer's status, the token's transfer rules), and the shape of
//!   each answer.
//! - `stock`: what every stock pair does, whoever issued the token: the pricer and its gates,
//!   the guard on the token's transfer rules, and `us_market_hours`, which withdraws outside
//!   09:30–16:00 New York time on trading days by a calendar the config states.
//! - `issuers`: each design's adapters and the kinds built from them. Ondo Global Markets
//!   today: `ondo`, the pricer over Ondo's price, multiplier and status, and `ondo_token`,
//!   the guard over Ondo's pause, blocklist and sanctions list.
//!
//! A new design is a module under `issuers/` and its lines below. Everything else is the
//! library's. See README.md, and `config.example.toml` for a pair.

mod chain;
mod issuers;
#[cfg(test)]
mod mock_http;
mod poll;
mod ports;
mod stock;

use quote_updater::prelude::*;

fn main() -> std::process::ExitCode {
    Updater::builder()
        .log_target(env!("CARGO_CRATE_NAME"))
        .systemd_unit("propamm-stocks")
        // The library's kinds, so a non-stock pair (USDC/USDT, say) can share the process.
        .pricer("fixed", quote_updater::pricers::Fixed)
        .pricer("feed", quote_updater::pricers::Feed)
        .pricer("volatile", quote_updater::pricers::Volatile)
        .quote_guard("us_market_hours", stock::hours::UsMarketHoursFactory)
        // Ondo Global Markets.
        .pricer("ondo", issuers::ondo::OndoFactory)
        .quote_guard("ondo_token", issuers::ondo::token_guard())
        .run_from_env()
}
