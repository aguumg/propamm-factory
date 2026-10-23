//! Tokenized stocks: a stock core of its own and one module per token design, registered on
//! the builder by a binary that quotes stock pairs (see `docs/stocks.md`):
//!
//! - `ports`: what the core asks of a design (a price, the token's share multiplier and its
//!   corporate actions, the issuer's status, the token's transfer rules), and the shape of
//!   each answer.
//! - `pricer`, `guard`, `hours`: what every stock pair does, whoever issued the token: the
//!   pricer and its gates, the guard on the token's transfer rules, and `us_market_hours`,
//!   which withdraws outside 09:30–16:00 New York time on trading days by a calendar the
//!   config states.
//! - `issuers`: each design's adapters and the kinds built from them. Ondo Global Markets
//!   today: `ondo`, the pricer over Ondo's price, multiplier and status, and `ondo_token`,
//!   the guard over Ondo's pause, blocklist and sanctions list.
//! - `chain`: the reads of the chain every design shares, behind the [`chain::Calls`] trait
//!   a test answers with [`crate::testing::FakeChain`].
//!
//! A new design is a module under `issuers/`. See `example/config.stocks.toml` for a pair.

pub mod chain;
pub mod guard;
pub mod hours;
pub mod issuers;
pub mod ports;
pub mod pricer;

/// A tokenized-stock design a binary integrates: its pricing kind, which is the issuer
/// itself as a [`crate::pricing::Factory`] over the design's adapters, and the guard on its
/// token's transfer rules. Register one with [`crate::UpdaterBuilder::stock_issuer`], which
/// uses the two names declared here, so a binary names the issuers it integrates and nothing
/// registers one implicitly. Ondo Global Markets is [`issuers::ondo::Ondo`]; a second
/// issuer, in this crate or in a crate of its own, implements this over the ports the same
/// way and needs no change to the builder.
pub trait Issuer: crate::pricing::Factory {
    /// The pricing kind's registered name, e.g. `ondo`.
    const PRICER_KIND: &'static str;
    /// The token guard's registered name, e.g. `ondo_token`.
    const TOKEN_GUARD_KIND: &'static str;
    /// The factory of the guard on the token's transfer rules:
    /// [`guard::RestrictionGuardFactory`] for a design whose rules the core's guard reads
    /// through [`ports::Restrictions`].
    type TokenGuard: crate::pricing::QuoteGuardFactory;
    /// The token guard's factory.
    fn token_guard(&self) -> Self::TokenGuard;
}
