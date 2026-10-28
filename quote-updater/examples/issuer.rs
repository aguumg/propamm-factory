//! A second tokenized-stock issuer, `acme`, beside Ondo: a constant price ratio, one share
//! per token, an always-open status, and a token whose one rule is a pause on a registry of
//! its own. It is not a real design. It exists so the workspace build proves that an
//! issuer written outside the library reaches everything it needs, since an example is
//! compiled against the public API alone: the ports and their constructors, the core's
//! `Stanza` and `Adapters`, the restriction guard with a closure that carries the issuer's
//! own setting, the shared chain reads, and the `poll` conventions a refresher is built
//! from. The price a pair quotes is a venue, which lives in the library (`venues/`); an
//! issuer outside it guards the pair. `docs/stocks.md` walks through it.

use std::{sync::Arc, time::Duration};

use quote_updater::{
    calldata::Value,
    eyre,
    poll::{Failures, every, wall_clock_ms},
    prelude::*,
    stocks::{
        Issuer,
        chain::{Calls, parse_address, read_bool},
        guard::RestrictionGuardFactory,
        ports::{
            Fetched, MarketStatus, Price, PriceSource, Restrictions, Rules, Shares, Stamped,
            StatusFeed, StatusReading, Stoppage, Valuation,
        },
        status::{self, Adapters, Settings, Stanza, StatusGuard},
    },
};
use serde::Deserialize;

/// The `[[pairs.guards]]` stanza for `kind = "acme"`: the core's keys it exposes, and the
/// issuer's own.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcmeCfg {
    /// The token's `symbol()`.
    symbol: String,
    /// The registry Acme pauses tokens on.
    registry: String,
    /// How often the status is read, in seconds; the core's default when absent.
    #[serde(default)]
    status_poll_secs: Option<u64>,
}

impl AcmeCfg {
    /// The core's stanza: its defaults, then what this kind lets the operator set.
    fn stanza(&self) -> Stanza<'_> {
        let mut stanza = Stanza::new("acme", "Acme", &self.symbol);
        if let Some(secs) = self.status_poll_secs {
            stanza.status_poll = Some(Duration::from_secs(secs));
        }
        stanza
    }

    fn check(&self, pair: &PairShape) -> eyre::Result<(Settings, Address)> {
        let settings = Settings::new(&self.stanza(), pair)?;
        let registry =
            parse_address(&self.registry).map_err(|err| eyre::eyre!("registry: {err}"))?;
        Ok((settings, registry))
    }
}

/// The issuer's price for the token and its stock, stamped on the library's clock: what the
/// core reads the issuer's `k` and the stock's ticker from.
struct ConstPrices {
    token: f64,
    stock: f64,
}

impl PriceSource for ConstPrices {
    fn fetch(&self) -> BoxFuture<'_, eyre::Result<Fetched>> {
        Box::pin(async move {
            let mut fetched = Fetched::new(Price::new(self.token, self.stock, wall_clock_ms()));
            fetched.ticker = Some("ACME".to_owned());
            Ok(fetched)
        })
    }
}

/// One share per token, no corporate action scheduled.
struct OneShare;

impl Valuation for OneShare {
    fn read(&self) -> BoxFuture<'_, eyre::Result<Option<Shares>>> {
        Box::pin(async { Ok(Some(Shares::new(1.0))) })
    }
}

/// An issuer that reports its market always in session and nothing on the asset.
struct AlwaysOpen;

impl MarketStatus for AlwaysOpen {
    fn read(&self, _names: &[String]) -> StatusReading {
        let mut reading = StatusReading::default();
        reading.session = Some(Stamped::now(Ok(())));
        reading.asset = Some(Stamped::now(Arc::<[Stoppage]>::from(Vec::new())));
        reading
    }
}

/// The task that would keep [`AlwaysOpen`] current, on the library's schedule and with its
/// failure logging: here it has nothing to read.
async fn keep_fresh() {
    let mut failures = Failures::new("Acme status".to_owned());
    let mut ticks = every(Duration::from_secs(15));
    loop {
        ticks.tick().await;
        failures.succeeded();
    }
}

/// Acme's transfer rules: a pause flag per token on its registry, read through the chain
/// the guard hands over.
struct AcmeRestrictions {
    chain: Arc<dyn Calls>,
    token: Address,
    registry: Address,
}

impl Restrictions for AcmeRestrictions {
    fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>> {
        Box::pin(async move {
            let paused = read_bool(
                &*self.chain,
                self.registry,
                "isPaused(address)",
                &[Value::Address(self.token)],
            )
            .await?;
            Ok(Box::new(AcmeRules { paused }) as Box<dyn Rules>)
        })
    }
}

struct AcmeRules {
    paused: bool,
}

impl Rules for AcmeRules {
    fn problems(&self) -> Vec<String> {
        self.paused
            .then(|| "Acme paused the token".to_owned())
            .into_iter()
            .collect()
    }

    fn refusals<'a>(
        &'a self,
        _name: &'a str,
        _party: Address,
    ) -> BoxFuture<'a, eyre::Result<Vec<String>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// The issuer: registered as `acme` (the status guard) and `acme_token` (the token guard).
struct Acme;

impl QuoteGuardFactory for Acme {
    type Config = AcmeCfg;
    type Guard = StatusGuard;

    fn validate(&self, cfg: &AcmeCfg, pair: &PairShape) -> eyre::Result<()> {
        cfg.check(pair).map(drop)
    }

    fn build<'a>(
        &'a self,
        cfg: &'a AcmeCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<StatusGuard>> {
        Box::pin(async move {
            let (settings, _registry) = cfg.check(ctx.pair())?;
            let mut adapters = Adapters::new(
                Box::new(ConstPrices {
                    token: 100.0,
                    stock: 100.0,
                }),
                Box::new(OneShare),
            );
            adapters.status = Some(Box::pin(async {
                Ok(StatusFeed::new(
                    Box::new(AlwaysOpen),
                    Box::pin(keep_fresh()),
                ))
            }));
            status::build(ctx, settings, adapters).await
        })
    }
}

impl Issuer for Acme {
    const STATUS_GUARD_KIND: &'static str = "acme";
    const TOKEN_GUARD_KIND: &'static str = "acme_token";
    type TokenGuard = RestrictionGuardFactory;

    fn token_guard(&self) -> RestrictionGuardFactory {
        // The registry is the issuer's, not the pair's: carried by the closure.
        let registry = Address::repeat_byte(0x42);
        RestrictionGuardFactory::new("Acme", move |chain, token| {
            Box::new(AcmeRestrictions {
                chain,
                token,
                registry,
            })
        })
    }
}

fn main() -> std::process::ExitCode {
    Updater::builder()
        .us_market_hours()
        .stock_issuer(quote_updater::stocks::issuers::ondo::Ondo)
        .stock_issuer(Acme)
        .run_from_env()
}
