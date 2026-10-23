//! A binary that quotes tokenized stocks: the library's own kinds, then the two stock
//! registrations, `us_market_hours()` for the NYSE calendar guard and `stock_issuer(..)` once
//! per issuer. It exists so the workspace build proves that path against the public API
//! alone; `example/` is the binary the deployment runs, and `docs/stocks.md` the guide.

use quote_updater::prelude::*;

fn main() -> std::process::ExitCode {
    Updater::builder()
        .pricer("fixed", quote_updater::pricers::Fixed)
        .pricer("feed", quote_updater::pricers::Feed)
        .pricer("volatile", quote_updater::pricers::Volatile)
        .us_market_hours()
        .stock_issuer(quote_updater::stocks::issuers::ondo::Ondo)
        .run_from_env()
}
