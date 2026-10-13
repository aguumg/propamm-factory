//! The PropAMM quote updater as a library, so that a downstream binary can assemble an
//! updater of its own: `Updater::builder()`, then `run(args)` inside the binary's own
//! runtime, `start(args)` for a handle to it, or `run_from_env()` as its whole `main`. The
//! extension points such a binary plugs into (pricers, guards, observers) are registered
//! on the builder.
#![warn(missing_docs)]
// The library reports through `tracing` (see output.rs); a print would bypass a binary's
// own subscriber. The `#[ignore]`d tools under `mod tests` and `report_error` opt out.
#![warn(clippy::print_stdout, clippy::print_stderr)]

/// Re-exported for a binary that flattens [`Args`] into a command line of its own
/// (`#[command(flatten)]`): the derive it uses must be the clap this crate's flags were
/// declared with, and `use quote_updater::clap;` puts that one in scope for it.
pub use clap;
/// Re-exported so a binary can name the error type `UpdaterBuilder::run` returns without
/// depending on `eyre` itself.
pub use eyre;
/// Re-exported for a pricer that keeps the client [`pricing::BuildCtx::http`] hands it: a
/// `Client` from a binary's own reqwest, at another version, would be a different type.
pub use reqwest;

pub use cli::Args;
/// The numeric and address types a pricer works in, re-exported so a user's `U256` is
/// the same type as the library's: the ethrex crates are git dependencies, and a second
/// copy at another revision would be a different type.
pub use ethrex_common::{Address, U256};
pub use updater::{Outcome, Updater, UpdaterBuilder, UpdaterHandle};

/// ABI encoding for [`pricing::Chain::call_sig`] and [`pricing::Chain::call`]: the `Value`
/// a call's arguments are given as and the encoder, from the ethrex crates this library is
/// built on, and [`crate::calldata::decode_return_data`] for the answer. Re-exported for
/// the same reason as [`Address`] and [`U256`]: a downstream would otherwise pin
/// `ethrex-l2-sdk` at this library's git revision to get the same types.
///
/// `decode_calldata` decodes calldata, selector first: it is not the decoder for a call's
/// answer, which has no selector and which it would read four bytes late.
pub mod calldata {
    pub use crate::rpc::decode_return_data;
    pub use ethrex_l2_common::calldata::Value;
    pub use ethrex_l2_sdk::calldata::{decode_calldata, encode_calldata};
}

/// Everything a custom pricer and its binary name, in one import:
/// `use quote_updater::prelude::*;`. Types and traits only: the `eyre` crate a
/// [`pricing::Factory`] names in its signatures is `quote_updater::eyre`, imported beside
/// the prelude rather than through it, because a crate name arriving through a glob is
/// ambiguous with a binary's own dependency of that name whenever the two differ.
pub mod prelude {
    pub use crate::{
        Address, Args, Outcome, U256, Updater, UpdaterBuilder, UpdaterHandle,
        guard::{
            Candidate, Cause, CompositeSample, Gate, Latch, MarketGuard, QuoteGuard, ReadHandle,
            SourceSample, TripReason, Verdict,
        },
        observe::{BlockOutcome, Event, Observer, ReloadOutcome},
        pricing::{
            BoxFuture, BuildCtx, CallOutcome, Chain, DiagHandle, Diagnostics, EnvSecret, Factory,
            FormField, History, Inventory, InventoryFeed, LandingFeed, LandingOutcome,
            LandingReport, Market, MarketGuardFactory, PairShape, Pricer, PricerOutput,
            QuoteGuardFactory, Refusal, RefusalHandle, TickCtx,
        },
    };
}

mod backoffice;
mod breaker;
mod builder;
mod cli;
mod composite;
mod config;
mod discover;
mod exporter;
mod feed;
pub mod guard;
mod head;
mod hints;
mod kinds;
mod landing;
mod metrics;
mod node;
pub mod observe;
pub mod output;
mod pair;
mod preflight;
pub mod pricing;
/// The pricing kinds this crate ships, each an ordinary [`pricing::Factory`] a binary
/// registers by name (`Updater::builder().pricer("volatile", pricers::Volatile)`) and a
/// pair picks with `[pairs.pricing] kind = "volatile"`: nothing distinguishes them from a
/// kind of your own, and their source is the worked example of writing one.
pub mod pricers {
    pub use crate::pricing::{Feed, Fixed, Volatile};
}
mod pusher;
mod quoting;
mod record;
mod reload;
mod rpc;
#[cfg(test)]
mod rpc_mock;
mod service;
mod supervisor;
mod tasks;
pub mod testing;
mod update;
mod updater;
mod vault;
mod venue;
mod venues;
mod volatile;
mod watchdog;
mod ws;

use std::time::Duration;

// Shared with `config` (static deltas at parse time) and `preflight` (runway pricing),
// which reach them as `crate::…`, keeping one definition for both send paths and every
// check that prices an update.
pub(crate) use update::{UPDATE_GAS_LIMIT, ensure_fits_216_bits};
// The items other modules reach as `crate::…`. Re-exported from the modules they moved
// to, so nothing outside this file had to change when main.rs was split.
pub(crate) use landing::{Landing, LandingTracker, landing_label, runway_check, verify_landed};
pub(crate) use pair::{Live, SendOpts};
pub(crate) use rpc::{bounded, call_view, decode_return_data};

/// Ceiling on `--price-decimals` (and on `[settings] price_decimals`). Beyond this a
/// non-inverted 10^d overflows the arithmetic downstream; an inverted pair is capped
/// tighter still, at 38, by `config`, because inverting squares the scale.
pub(crate) const MAX_PRICE_DECIMALS: u32 = 59;

/// How many consecutive target blocks a pair may go without a landed update before the
/// builder loop says so loudly.
///
/// Only read-backs that failed count: a block with no update in it is ordinary, because a
/// builder includes the update only when a swap hits the lane in that block, and most
/// blocks have no swap. What is not ordinary is this many blocks (~10 minutes) in a row
/// where the pusher could not read the lane back at all: the RPC is down or answering
/// garbage, and nothing else would say so.
pub(crate) const NOT_LANDING_BLOCKS: u32 = 50;

/// How many target blocks between re-reads of a pair's own signer balance (~10 minutes).
/// Preflight prices the runway once at startup; a key drains during the run, which is
/// exactly when nothing was watching it.
pub(crate) const RUNWAY_CHECK_BLOCKS: u64 = 50;

/// How long any one RPC read may take before it is abandoned. `EthClient` is built on a
/// reqwest client with no request timeout of its own, so without this a hung RPC (TCP
/// alive, never answering) holds its caller forever, with no line saying why. Three
/// seconds is long for a read that has a 12s slot to be useful in, and short enough that
/// the retry lands well inside it.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(3);
