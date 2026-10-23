//! my-propamm: a quote updater assembled from the `quote_updater` library with three pieces
//! of our own, one per extension point the library offers.
//!
//! - A pricing model, `inventory_skew`: a fixed half-spread, with the mid pushed away from
//!   the side our vault is long, so fills rebalance us. It reads the vault through the
//!   library (`ctx.inventory()`), so it is a `Factory` (its build is async).
//! - A market guard, `venue_spread`: trips the lane when the venues behind the composite
//!   price disagree by more than a limit. Runs on every composite sample. A factory, like
//!   the quote guard below, so the backoffice has a form for its knob.
//! - A quote guard, `share_cap`: withdraws our quote while the vault is more lopsided than a
//!   cap, and halts the lane if the pricer reports a share that cannot exist.
//! - An observer, `block_log`: logs lifecycle events (a lane starting, stopping, tripping,
//!   a reload) and withdrawn blocks with the pricer's diagnostics. Watches, never decides.
//!
//! Everything else is the library's: config file and reload, the price feeds, the builders,
//! `--check`, metrics, the backoffice, the deviation breaker and the core's bounds.
//!
//! Run: `cargo run -- --config config.toml` (see README.md).

use std::time::Duration;

use quote_updater::{eyre, prelude::*};
use serde::Deserialize;

// ----------------------------------------------------------------------------------------
// 1. The pricing model
// ----------------------------------------------------------------------------------------

/// The `[pairs.pricing]` stanza for `kind = "inventory_skew"`. Everything but `kind` is ours.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventorySkewCfg {
    /// Published as the delta: a fraction of the mid (0.0005 = 5 bp).
    half_spread: f64,
    /// How far the mid moves per unit of imbalance: skew = strength * (base_share - 0.5).
    /// At strength 0.02 a vault that is 100% base pushes the mid 1% down.
    strength: f64,
    /// A vault reading older than this refuses the tick (seconds).
    #[serde(default = "default_max_inventory_age_secs")]
    max_inventory_age_secs: u64,
}

fn default_max_inventory_age_secs() -> u64 {
    60
}

struct InventorySkew {
    cfg: InventorySkewCfg,
    inventory: InventoryFeed,
    /// Diagnostics this pricer publishes, reachable by quote guards and Prometheus.
    base_share: DiagHandle,
    skew: DiagHandle,
    /// The refusal reason this pricer declared, counted under `price_unusable_total`.
    stale_inventory: RefusalHandle,
}

impl Pricer for InventorySkew {
    fn price(&mut self, tick: &TickCtx, out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        // The market in human orientation (quote per base, e.g. USDC per WETH). The library
        // converts to whatever orientation and scale the lane uses on chain.
        let market = tick.market()?;

        let reading = self.inventory.latest();
        let max_age = Duration::from_secs(self.cfg.max_inventory_age_secs);
        if reading.age(tick.now()) > max_age {
            return Err(Refusal::new(
                &self.stale_inventory,
                format!(
                    "vault reading older than {}s",
                    self.cfg.max_inventory_age_secs
                ),
            ));
        }

        let share = reading.base_share(market.mid_f64());
        let skew = self.cfg.strength * (share - 0.5);
        out.set(&self.base_share, share);
        out.set(&self.skew, skew);

        Ok(PricerOutput::new(
            market.scaled_fraction(self.cfg.half_spread)?,
            // Long base (share > 0.5) gives a positive skew, and we want to sell base, so the
            // mid must go down. The library's `shift(+x)` publishes a mid x below the market
            // (the volatile model's tilt direction), so the skew is passed as is.
            market.shift(skew)?,
        ))
    }
}

struct InventorySkewFactory;

/// What the backoffice shows for a pair priced by this kind: one input per knob, written
/// into the stanza as numbers since the config reads `f64` and `u64`.
const FIELDS: &[FormField] = &[
    FormField::number(
        "half_spread",
        "0.0005",
        "the spread, as a fraction of the mid. 0.0005 means we sell at mid + 0.05% and buy at \
         mid − 0.05%.",
    ),
    FormField::number(
        "strength",
        "0.02",
        "how far the mid moves per unit of imbalance. 0.02: a vault that is all base pushes \
         the mid 1% down.",
    ),
    FormField::number(
        "max_inventory_age_secs",
        "60",
        "a vault reading older than this many seconds stops the pair quoting. Blank is 60.",
    ),
];

impl Factory for InventorySkewFactory {
    type Config = InventorySkewCfg;

    fn fields(&self) -> &'static [FormField] {
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>A fixed spread, with the mid moved toward the token the vault holds \
         too much of, so fills rebalance it.</p>"
    }

    fn summary(&self, cfg: &InventorySkewCfg) -> String {
        format!("half_spread {} strength {}", cfg.half_spread, cfg.strength)
    }
    type Pricer = InventorySkew;

    /// Runs at startup, on reload and under `--check`, before anything is built.
    fn validate(&self, cfg: &InventorySkewCfg, _pair: &PairShape) -> eyre::Result<()> {
        eyre::ensure!(
            cfg.half_spread > 0.0 && cfg.half_spread < 0.1,
            "half_spread {} is not in (0, 0.1)",
            cfg.half_spread
        );
        eyre::ensure!(
            (0.0..=0.5).contains(&cfg.strength),
            "strength {} is not in 0..=0.5",
            cfg.strength
        );
        Ok(())
    }

    fn build<'a>(
        &'a self,
        cfg: &'a InventorySkewCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<InventorySkew>> {
        Box::pin(async move {
            let base_share = ctx.diagnostic("base_share")?;
            let skew = ctx.diagnostic("skew")?;
            let stale_inventory = ctx.refusal("stale_inventory")?;
            // Reads the vault once now, so a wrong target fails this lane's build, then the
            // library refreshes it in the background for the life of the lane.
            let inventory = ctx.inventory().await?;
            Ok(InventorySkew {
                cfg: cfg.clone(),
                inventory,
                base_share,
                skew,
                stale_inventory,
            })
        })
    }
}

// ----------------------------------------------------------------------------------------
// 2. A market guard: judges the venues behind every composite sample
// ----------------------------------------------------------------------------------------

/// `[[pairs.guards]]` stanza with `kind = "venue_spread"`.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueSpreadCfg {
    /// Trip when (max venue mid - min venue mid) / composite mid exceeds this (0.01 = 1%).
    max_dispersion: f64,
}

struct VenueSpread {
    cfg: VenueSpreadCfg,
    dispersion: DiagHandle,
}

impl MarketGuard for VenueSpread {
    fn assess(&mut self, sample: &CompositeSample, out: &mut Diagnostics) -> Verdict {
        let mids = sample.sources().iter().map(|s| s.mid_f64());
        let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
        for mid in mids {
            lo = lo.min(mid);
            hi = hi.max(mid);
        }
        if !lo.is_finite() || !hi.is_finite() {
            // One venue or none: nothing to compare.
            return Verdict::Pass;
        }
        let dispersion = (hi - lo) / sample.mid_f64();
        out.set(&self.dispersion, dispersion);
        if dispersion > self.cfg.max_dispersion {
            let venues: Vec<&str> = sample.sources().iter().map(|s| s.venue).collect();
            return Verdict::Trip(TripReason::new(format!(
                "venues {} disagree by {:.2}% (limit {:.2}%)",
                venues.join(", "),
                dispersion * 100.0,
                self.cfg.max_dispersion * 100.0
            )));
        }
        Verdict::Pass
    }
}

/// Registered as `venue_spread`: a factory rather than a closure, so the guard declares its
/// knob for the backoffice and checks it under `--check`.
struct VenueSpreadFactory;

impl MarketGuardFactory for VenueSpreadFactory {
    type Config = VenueSpreadCfg;
    type Guard = VenueSpread;

    fn validate(&self, cfg: &VenueSpreadCfg, _pair: &PairShape) -> eyre::Result<()> {
        eyre::ensure!(cfg.max_dispersion > 0.0, "max_dispersion must be positive");
        Ok(())
    }

    fn fields(&self) -> &'static [FormField] {
        const FIELDS: &[FormField] = &[FormField::number(
            "max_dispersion",
            "0.01",
            "trip when the venues' mids disagree by more than this fraction of the composite: \
             0.01 is 1%.",
        )];
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>Trips the lane while the venues behind the composite price disagree \
         by more than a limit.</p>"
    }

    fn summary(&self, cfg: &VenueSpreadCfg) -> String {
        format!("max_dispersion {:.2}%", cfg.max_dispersion * 100.0)
    }

    fn build<'a>(
        &'a self,
        cfg: &'a VenueSpreadCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<VenueSpread>> {
        Box::pin(async move {
            let dispersion = ctx.diagnostic("venue_dispersion")?;
            Ok(VenueSpread {
                cfg: cfg.clone(),
                dispersion,
            })
        })
    }
}

// ----------------------------------------------------------------------------------------
// 3. A quote guard: the last word on every tick, after the pricer and the core's bounds
// ----------------------------------------------------------------------------------------

/// `[[pairs.guards]]` stanza with `kind = "share_cap"`.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShareCapCfg {
    /// Withdraw while base_share is outside [1 - max_share, max_share].
    max_share: f64,
}

struct ShareCap {
    cfg: ShareCapCfg,
    /// Bound at build to the pricer's `base_share` diagnostic. The build fails if the pair's
    /// pricer declares no such diagnostic, which `--check` reports under the pair's row.
    base_share: ReadHandle,
    lopsided: RefusalHandle,
}

impl QuoteGuard for ShareCap {
    fn check(&mut self, candidate: &Candidate<'_>, _out: &mut Diagnostics) -> Gate {
        // On a tick the core refused before the pricer ran (no sample, stale feed) there is
        // no diagnostic; we have nothing to add.
        let Some(share) = candidate.get(&self.base_share) else {
            return Gate::Allow;
        };
        if !(0.0..=1.0).contains(&share) {
            // A share outside [0, 1] cannot exist: the pricer is broken. Stop the lane; a
            // human re-arms it with a reload.
            return Gate::Halt(TripReason::new(format!("base share {share} is impossible")));
        }
        let lo = 1.0 - self.cfg.max_share;
        if share > self.cfg.max_share || share < lo {
            return Gate::Withdraw(Refusal::new(
                &self.lopsided,
                format!(
                    "base share {share:.3} outside [{lo:.2}, {:.2}]",
                    self.cfg.max_share
                ),
            ));
        }
        Gate::Allow
    }
}

/// Registered as `share_cap`; a factory for the same reasons as [`VenueSpreadFactory`].
struct ShareCapFactory;

impl QuoteGuardFactory for ShareCapFactory {
    type Config = ShareCapCfg;
    type Guard = ShareCap;

    fn validate(&self, cfg: &ShareCapCfg, _pair: &PairShape) -> eyre::Result<()> {
        eyre::ensure!(
            (0.5..=1.0).contains(&cfg.max_share),
            "max_share {} is not in 0.5..=1.0",
            cfg.max_share
        );
        Ok(())
    }

    fn fields(&self) -> &'static [FormField] {
        const FIELDS: &[FormField] = &[FormField::number(
            "max_share",
            "0.8",
            "withdraw while the vault's base share is outside [1 − max_share, max_share]: \
             0.8 allows 20/80 either way.",
        )];
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>Withdraws the quote while the vault is more lopsided than the cap, \
         and halts the lane if the pricer reports a share that cannot exist.</p>"
    }

    fn summary(&self, cfg: &ShareCapCfg) -> String {
        format!("max_share {:.2}", cfg.max_share)
    }

    fn build<'a>(
        &'a self,
        cfg: &'a ShareCapCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<ShareCap>> {
        Box::pin(async move {
            Ok(ShareCap {
                cfg: cfg.clone(),
                base_share: ctx.read_diagnostic("base_share")?,
                lopsided: ctx.refusal("lopsided")?,
            })
        })
    }
}

// ----------------------------------------------------------------------------------------
// 4. An observer: watches the run, decides nothing
// ----------------------------------------------------------------------------------------

struct BlockLog;

impl Observer for BlockLog {
    fn name(&self) -> &'static str {
        "block_log"
    }

    fn on_event<'a>(&'a mut self, event: &'a Event) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Every enum here is #[non_exhaustive]: match fields with `..` and keep a `_` arm.
            match event {
                Event::LaneStarted { pair, .. } => {
                    tracing::info!("[{pair}] observer: lane started")
                }
                Event::LaneStopped { pair, .. } => {
                    tracing::info!("[{pair}] observer: lane stopped")
                }
                Event::Tripped {
                    pair,
                    source,
                    cause,
                    ..
                } => {
                    tracing::warn!(
                        "[{pair}] observer: tripped by `{source}` ({})",
                        cause.as_str()
                    )
                }
                Event::Rearmed { pair, .. } => tracing::info!("[{pair}] observer: re-armed"),
                // A block that was withdrawn, with the diagnostics the pricer set that tick:
                // the one per-block case worth a line. Quoted blocks and landings are
                // already in the library's own log.
                Event::Block {
                    pair,
                    block,
                    outcome: BlockOutcome::Withdrawn { reason, .. },
                    diagnostics,
                    ..
                } => {
                    let diag: Vec<String> = diagnostics
                        .iter()
                        .map(|(k, v)| format!("{k}={v:.4}"))
                        .collect();
                    tracing::info!(
                        "[{pair}] observer: block {block} withdrawn: {reason} {}",
                        diag.join(" ")
                    )
                }
                Event::Reload { outcome, .. } => tracing::info!("observer: reload {outcome:?}"),
                _ => {}
            }
        })
    }
}

// ----------------------------------------------------------------------------------------
// The assembly
// ----------------------------------------------------------------------------------------

fn main() -> std::process::ExitCode {
    Updater::builder()
        // So the observer's tracing lines are rendered beside the library's.
        .log_target(env!("CARGO_CRATE_NAME"))
        // Named in the operator hints ("systemctl --user reload my-propamm"). Optional.
        .systemd_unit("my-propamm")
        // The three kinds the library ships, registered exactly like our own below.
        .pricer("fixed", quote_updater::pricers::Fixed)
        .pricer("feed", quote_updater::pricers::Feed)
        .pricer("volatile", quote_updater::pricers::Volatile)
        .pricer("inventory_skew", InventorySkewFactory)
        // Tokenized stocks, from the library: the NYSE calendar guard, then each issuer's
        // pricer and token guard under the issuer's own names (docs/stocks.md).
        .us_market_hours()
        .stock_issuer(quote_updater::stocks::issuers::ondo::Ondo)
        .market_guard("venue_spread", VenueSpreadFactory)
        .quote_guard("share_cap", ShareCapFactory)
        .observer(BlockLog)
        // A whole `main`: parses the command line and the environment, runs, maps the
        // outcome to an exit code. Nothing in the library calls process::exit.
        .run_from_env()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example's guards declare their one knob each, so WETH/USDC's guards are edited
    /// from the backoffice like its pricing.
    #[test]
    fn the_examples_guards_declare_their_one_knob_each() {
        let names = |fields: &[FormField]| fields.iter().map(|f| f.name).collect::<Vec<_>>();
        assert_eq!(names(VenueSpreadFactory.fields()), ["max_dispersion"]);
        assert_eq!(names(ShareCapFactory.fields()), ["max_share"]);
    }
}
