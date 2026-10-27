//! Ondo Global Markets stock tokens (AAPLon, ...): Ondo's adapters to the ports, and the two
//! kinds built from them.
//!
//! - **`ondo`**, the stock pricer ([`crate::stocks::pricer`]) over Ondo's adapters: the price
//!   from Ondo's own API ([`api::OndoPrices`]), the price Ondo itself mints and redeems
//!   around; the share multiplier and its corporate-action pause from the chain's
//!   `SyntheticSharesOracle` ([`oracle::OndoOracle`]), for a token that oracle lists (it
//!   holds entries for some GM tokens and not others; one it does not list is quoted on
//!   Ondo's price alone, without the multiplier gates); and Ondo's market session and asset
//!   events ([`status::OndoStatus`]), which withdraw on any session but `regular`, on an
//!   active pause or size limit under the token's symbol or the stock's ticker, and on an
//!   upcoming event that will move the share multiplier.
//! - **`ondo_token`**, the restriction guard ([`crate::stocks::guard`]) over Ondo's pause
//!   manager, blocklist and sanctions list ([`compliance::OndoCompliance`]).
//!
//! ```toml
//! [[pairs]]
//! tokens  = ["0x14c3abF95Cb9C93a8b82C1CdCB76D72Cb87b2d4c", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]  # AAPLon, USDC
//! key_env = "UPDATER_KEY_AAPLON_USDC"
//! allow_symbol_mismatch = true   # no venue symbol to check: the price comes from Ondo
//! min_mid = "100"
//! max_mid = "500"
//!
//! [pairs.pricing]
//! kind    = "ondo"
//! symbol  = "AAPLon"
//! api_key = { env = "ONDO_API_KEY" }
//! delta   = "0.003"                 # half-spread, a fraction of the mid (30 bp)
//! ```

pub mod api;
pub mod compliance;
pub mod oracle;
pub mod status;

use std::{sync::Arc, time::Duration};

use crate::{Address, eyre, prelude::*};
use serde::Deserialize;

use crate::stocks::Issuer;

use crate::{
    stocks::chain::parse_address,
    stocks::ports::StatusFeed,
    stocks::{
        guard::RestrictionGuardFactory,
        pricer::{self, Adapters, Settings, Stanza, StockPricer},
    },
};

/// The `[pairs.pricing]` stanza for `kind = "ondo"`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OndoCfg {
    /// The asset's GM symbol in Ondo's API, e.g. `AAPLon`.
    pub symbol: String,
    /// The stock's ticker, e.g. `AAPL`: needed only if Ondo's price answer does not name it,
    /// and checked against the answer when it does. Ondo may file the asset's events under it.
    #[serde(default)]
    pub ticker: Option<String>,
    /// The Ondo GM API key, as `{ env = "ONDO_API_KEY" }`.
    pub api_key: EnvSecret,
    /// Published as the delta: the half-spread as a fraction of the mid (`"0.003"` = 30 bp).
    pub delta: String,
    /// The sessions Ondo reports (`/v1/status/market`'s `marketStatus`) in which the pair
    /// quotes: any of `regular`, `premarket`, `postmarket`, `overnight` and `offhours`, in
    /// any case. Default `regular` only, and an empty list means the default. Ondo's
    /// `paused` and `closed` are never quoted in: nothing trades then.
    #[serde(default = "default_sessions")]
    pub sessions: Vec<String>,
    /// Ondo's GM API.
    #[serde(default = "default_api")]
    pub api: String,
    /// The `SyntheticSharesOracle`; Ethereum mainnet's by default.
    #[serde(default)]
    pub multiplier_oracle: Option<String>,
    /// How often the price is polled, in milliseconds (default 1000; Ondo caches it 1s).
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    /// How often the market and asset status are polled (default 15s).
    #[serde(default = "default_status_poll_secs")]
    pub status_poll_secs: u64,
    /// The oldest price, by Ondo's timestamp, that is quoted (default 15s).
    #[serde(default = "default_max_price_age_secs")]
    pub max_price_age_secs: u64,
    /// The longest Ondo's price may stand still and be quoted (default 30s): a halted
    /// stock's price stops moving before Ondo's status says so. Raise it for a stock that
    /// trades thinly enough to sit on one price that long in session.
    #[serde(default = "default_max_unchanged_secs")]
    pub max_unchanged_secs: u64,
    /// The oldest status or multiplier reading that is trusted (default 60s).
    #[serde(default = "default_max_status_age_secs")]
    pub max_status_age_secs: u64,
    /// How long before a scheduled corporate-action pause quoting stops (default 300s).
    #[serde(default = "default_pause_lead_secs")]
    pub pause_lead_secs: u64,
    /// The largest relative gap between Ondo's multiplier and the chain's (default 0.001).
    #[serde(default = "default_max_multiplier_gap")]
    pub max_multiplier_gap: f64,
    /// Inventory skew: the mid moves by `skew × (base share − 0.5)`, down when the vault is
    /// long the stock, so fills rebalance it. 0, the default, reads no inventory.
    #[serde(default)]
    pub skew: f64,
}

fn default_api() -> String {
    "https://api.gm.ondo.finance".to_owned()
}
fn default_sessions() -> Vec<String> {
    vec!["regular".to_owned()]
}

/// The sessions Ondo reports in which a token trades. `paused` and `closed` are the others.
const TRADING_SESSIONS: [&str; 5] = [
    "regular",
    "premarket",
    "postmarket",
    "overnight",
    "offhours",
];
fn default_poll_ms() -> u64 {
    1_000
}
fn default_status_poll_secs() -> u64 {
    15
}
fn default_max_price_age_secs() -> u64 {
    15
}
fn default_max_unchanged_secs() -> u64 {
    30
}
fn default_max_status_age_secs() -> u64 {
    60
}
fn default_pause_lead_secs() -> u64 {
    300
}
fn default_max_multiplier_gap() -> f64 {
    0.001
}

impl OndoCfg {
    /// The sessions the pair quotes in, lower case, each one Ondo reports a token trading
    /// in; an empty list is the default, `regular` alone, so a blank form field means that.
    fn sessions(&self) -> eyre::Result<Vec<String>> {
        let mut sessions = Vec::new();
        for written in &self.sessions {
            let session = written.trim().to_ascii_lowercase();
            if session.is_empty() {
                continue;
            }
            eyre::ensure!(
                TRADING_SESSIONS.contains(&session.as_str()),
                "sessions: {written:?} is not a session Ondo trades in; one of {}",
                TRADING_SESSIONS.join(", ")
            );
            if !sessions.contains(&session) {
                sessions.push(session);
            }
        }
        if sessions.is_empty() {
            sessions = default_sessions();
        }
        Ok(sessions)
    }

    /// The keys the core reads, as the core reads them.
    fn stanza(&self) -> Stanza<'_> {
        Stanza {
            kind: "ondo",
            source: "Ondo",
            symbol: &self.symbol,
            delta: &self.delta,
            price_poll: Duration::from_millis(self.poll_ms),
            status_poll: Some(Duration::from_secs(self.status_poll_secs)),
            max_price_age_secs: self.max_price_age_secs,
            max_unchanged_secs: self.max_unchanged_secs,
            max_status_age_secs: self.max_status_age_secs,
            pause_lead_secs: self.pause_lead_secs,
            max_multiplier_gap: self.max_multiplier_gap,
            skew: self.skew,
        }
    }

    /// The stanza checked against its pair: the core's rules, then Ondo's own. Answers the
    /// core's settings and the oracle the multiplier is read from.
    fn check(&self, pair: &PairShape) -> eyre::Result<(Settings, Address)> {
        let settings = Settings::new(&self.stanza(), pair)?;
        self.sessions()?;
        api::check_api(&self.api).map_err(|err| eyre::eyre!(err))?;
        eyre::ensure!(
            (250..=60_000).contains(&self.poll_ms),
            "poll_ms {} is not in 250..=60000: Ondo caches prices for a second",
            self.poll_ms
        );
        eyre::ensure!(
            self.ticker.as_ref().is_none_or(|ticker| !ticker.is_empty()),
            "ticker is empty"
        );
        let oracle = parse_address(
            self.multiplier_oracle
                .as_deref()
                .unwrap_or(oracle::MAINNET_MULTIPLIER_ORACLE),
        )
        .map_err(|err| eyre::eyre!("multiplier_oracle: {err}"))?;
        Ok((settings, oracle))
    }
}

/// Ondo Global Markets: registered with [`crate::UpdaterBuilder::stock_issuer`] as
/// `ondo` (the pricer) and `ondo_token` (the guard on the token's rules).
pub struct Ondo;

/// What the backoffice shows for a pair priced by Ondo: every key of the stanza, the
/// numbers written as numbers since the config reads `u64` and `f64`, the API key as the
/// name of the variable holding it.
const FIELDS: &[FormField] = &[
    FormField::new(
        "symbol",
        "AAPLon",
        "the asset's GM symbol in Ondo's API; the pair's first token must be it.",
    ),
    FormField::new(
        "ticker",
        "AAPL",
        "the stock's ticker, needed only if Ondo's price answer does not name it. Blank takes \
         the answer's.",
    ),
    FormField::env(
        "api_key",
        "ONDO_API_KEY",
        "the environment variable holding the Ondo GM API key, never the key itself.",
    ),
    FormField::new(
        "delta",
        "0.003",
        "the half-spread as a fraction of the mid: 0.003 is 30 bp.",
    ),
    FormField::list(
        "sessions",
        "regular",
        "the sessions Ondo reports in which the pair quotes, one per line: regular, \
         premarket, postmarket, overnight, offhours. Blank is regular only.",
    ),
    FormField::new(
        "api",
        "https://api.gm.ondo.finance",
        "Ondo's GM API. Blank is the production one.",
    ),
    FormField::new(
        "multiplier_oracle",
        oracle::MAINNET_MULTIPLIER_ORACLE,
        "the SyntheticSharesOracle the share multiplier is read from. Blank is Ethereum \
         mainnet's. A token it does not list is quoted on Ondo's price alone, without the \
         multiplier checks.",
    ),
    FormField::number(
        "poll_ms",
        "1000",
        "milliseconds between price polls; Ondo caches prices for a second. Blank is 1000.",
    ),
    FormField::number(
        "status_poll_secs",
        "15",
        "seconds between polls of Ondo's market and asset status. Blank is 15.",
    ),
    FormField::number(
        "max_price_age_secs",
        "15",
        "the oldest price, by Ondo's timestamp, that is quoted. Blank is 15.",
    ),
    FormField::number(
        "max_unchanged_secs",
        "30",
        "the longest Ondo's price may stand still and be quoted: a halted stock's price stops \
         before the status says so. Blank is 30.",
    ),
    FormField::number(
        "max_status_age_secs",
        "60",
        "the oldest status or multiplier reading that is trusted. Blank is 60.",
    ),
    FormField::number(
        "pause_lead_secs",
        "300",
        "how long before a scheduled corporate-action pause quoting stops. Blank is 300.",
    ),
    FormField::number(
        "max_multiplier_gap",
        "0.001",
        "the largest relative gap between Ondo's multiplier and the chain's. Blank is 0.001.",
    ),
    FormField::number(
        "skew",
        "0",
        "inventory skew: the mid moves by skew × (base share − 0.5), down when the vault is \
         long the stock. Blank is 0, which reads no inventory.",
    ),
];

impl Factory for Ondo {
    type Config = OndoCfg;
    type Pricer = StockPricer;

    fn fields(&self) -> &'static [FormField] {
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>Ondo's own price for the token is the mid, less and plus a fixed \
         half-spread. The quote is withdrawn while the price is stale or frozen, outside \
         the Ondo sessions listed (the regular session alone unless the stanza says \
         otherwise), around an asset event or a corporate action, and when \
         Ondo's share multiplier and the chain's disagree (for a token the oracle lists; \
         one it does not list skips that check). The band above is in the lane's \
         orientation: USDC per token when the stock token's address sorts before USDC's, \
         token per USDC (1/price) when it sorts after.</p>"
    }

    fn validate(&self, cfg: &OndoCfg, pair: &PairShape) -> eyre::Result<()> {
        cfg.check(pair).map(drop)
    }

    fn summary(&self, cfg: &OndoCfg) -> String {
        let sessions = cfg.sessions().unwrap_or_default().join("+");
        format!(
            "{} delta {} skew {} in {sessions}",
            cfg.symbol, cfg.delta, cfg.skew
        )
    }

    fn build<'a>(
        &'a self,
        cfg: &'a OndoCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<StockPricer>> {
        Box::pin(async move {
            let (settings, oracle) = cfg.check(ctx.pair())?;
            let sessions = cfg.sessions()?;
            let key = cfg.api_key.resolve()?;
            let chain = ctx.chain()?;
            let token = ctx.pair().tokens.0;
            let (api, every) = (cfg.api.clone(), Duration::from_secs(cfg.status_poll_secs));
            let status_key = key.clone();
            let adapters = Adapters {
                prices: Box::new(api::OndoPrices {
                    http: api::client(),
                    api: cfg.api.clone(),
                    key,
                    symbol: cfg.symbol.clone(),
                    ticker: cfg.ticker.clone(),
                }),
                valuation: Box::new(oracle::OndoOracle {
                    chain: Arc::new(chain),
                    oracle,
                    token,
                }),
                status: Some(Box::pin(async move {
                    let shared = status::Status::shared(&api, &status_key, every).await?;
                    Ok(StatusFeed {
                        reader: Box::new(status::OndoStatus::new(Arc::clone(&shared), sessions)),
                        refresh: Box::pin(status::keep_fresh(shared)),
                    })
                })),
            };
            pricer::build(ctx, settings, adapters).await
        })
    }
}

impl Issuer for Ondo {
    const PRICER_KIND: &'static str = "ondo";
    const TOKEN_GUARD_KIND: &'static str = "ondo_token";
    type TokenGuard = RestrictionGuardFactory;

    /// The restriction guard over Ondo's pause and lists.
    fn token_guard(&self) -> RestrictionGuardFactory {
        RestrictionGuardFactory::new("Ondo", |chain, token| {
            Box::new(compliance::OndoCompliance { chain, token })
        })
    }
}

#[cfg(test)]
mod tests {
    /// The core's default stanza is this kind's: a design that takes `Stanza::new`'s
    /// defaults quotes under the gates `ondo` documents, and the two lists cannot drift.
    #[test]
    fn the_cores_default_stanza_is_the_ondo_kinds() {
        let cfg: super::OndoCfg = toml::from_str(
            "symbol = \"AAPLon\"\napi_key = { env = \"ONDO_API_KEY\" }\ndelta = \"0.003\"",
        )
        .unwrap();
        let core = super::Stanza::new("ondo", "Ondo", "AAPLon", "0.003");
        assert_eq!(core, cfg.stanza());
    }

    use std::sync::Arc;

    use crate::testing;

    use super::{
        status::{OndoStatus, Readings, Status},
        *,
    };
    use crate::{
        poll::Stamped,
        stocks::pricer::testkit::{self, Built, read_at},
    };

    fn cfg(extra: serde_json::Value) -> OndoCfg {
        let mut stanza = serde_json::json!({
            "symbol": "AAPLon",
            "api_key": {"env": "ONDO_API_KEY"},
            "delta": "0.003",
        });
        for (key, value) in extra.as_object().unwrap() {
            stanza[key] = value.clone();
        }
        serde_json::from_value(stanza).unwrap()
    }

    /// Ondo reporting `session` and the events in `events_body` (`/v1/status/assets`'s
    /// answer), a second ago.
    fn ondo_reports(session: &str, events_body: &str) -> Arc<Status> {
        Status::of(Readings {
            session: Some(Stamped {
                at: read_at(),
                value: session.to_owned(),
            }),
            events: Some(Stamped {
                at: read_at(),
                value: api::parse_asset_events(events_body).unwrap(),
            }),
        })
    }

    /// The `ondo` pricer as its build leaves it, over the status Ondo reports, for AAPLon,
    /// whose stock Ondo's price answer names AAPL.
    fn built(status: Arc<Status>) -> Built {
        built_with(cfg(serde_json::json!({})), status)
    }

    /// [`built`], from `cfg`'s stanza.
    fn built_with(cfg: OndoCfg, status: Arc<Status>) -> Built {
        let pair = testkit::pair();
        let (settings, _) = cfg.check(&pair).unwrap();
        let sessions = cfg.sessions().unwrap();
        let reads = testkit::healthy(Some(Box::new(OndoStatus::new(status, sessions))));
        testkit::built(pair, settings, reads, None)
    }

    #[test]
    fn the_regular_session_with_no_event_quotes() {
        assert!(built(ondo_reports("regular", "[]")).price().is_ok());
    }

    #[test]
    fn the_regular_session_quotes_however_ondo_capitalises_it() {
        for session in ["REGULAR", "Regular"] {
            assert!(
                built(ondo_reports(session, "[]")).price().is_ok(),
                "{session}"
            );
        }
    }

    /// `/v1/status/assets` listing one upcoming event on AAPLon, starting at `start` (a JSON
    /// value as written, or none).
    fn upcoming(moves_multiplier: bool, code: &str, start: Option<&str>) -> String {
        let start = start.map_or_else(String::new, |start| format!(r#""start":{start},"#));
        format!(
            r#"[{{"symbol":"AAPLon","status":"upcoming","type":"scheduled",{start}
                 "updateSharesMultiplier":{moves_multiplier},
                 "reason":{{"code":"{code}","message":"split"}}}}]"#
        )
    }

    #[test]
    fn an_upcoming_corporate_action_withdraws_from_the_lead_before_the_start_ondo_gives() {
        // NOW is 2026-10-08 14:00:00 UTC, and the lead is 300s.
        let at = |start| {
            built(ondo_reports(
                "regular",
                &upcoming(true, "ASSET_PAUSED", start),
            ))
        };
        assert!(at(Some(r#""2026-10-08T14:05:01Z""#)).price().is_ok());
        let refusal = at(Some(r#""2026-10-08T14:05:00Z""#)).refusal();
        assert_eq!(refusal.reason(), "asset_event");
        // In milliseconds, as Ondo writes its other timestamps.
        assert!(at(Some("1791468301000")).price().is_ok());
        assert!(at(Some("1791468300000")).price().is_err());
        // No start, or one that does not read as a time: from the moment Ondo lists it.
        assert!(at(None).price().is_err());
        assert!(at(Some(r#""next week""#)).price().is_err());
    }

    #[test]
    fn an_upcoming_event_that_leaves_the_multiplier_alone_withdraws_from_its_start() {
        let at = |start| {
            built(ondo_reports(
                "regular",
                &upcoming(false, "ASSET_LIMITED", start),
            ))
        };
        assert!(at(Some(r#""2026-10-08T14:00:01Z""#)).price().is_ok());
        assert_eq!(
            at(Some(r#""2026-10-08T14:00:00Z""#)).refusal().reason(),
            "asset_event"
        );
        // With no start there is nothing to withdraw from until Ondo says it is on.
        assert!(at(None).price().is_ok());
    }

    /// Ondo trades its tokens outside the regular session too; a stanza lists the sessions
    /// it quotes in, and the market-session gate follows the list.
    #[test]
    fn the_stanza_lists_the_sessions_the_pair_quotes_in() {
        let extended =
            || cfg(serde_json::json!({"sessions": ["regular", "Postmarket", " overnight "]}));
        for session in ["regular", "postmarket", "overnight", "OVERNIGHT"] {
            assert!(
                built_with(extended(), ondo_reports(session, "[]"))
                    .price()
                    .is_ok(),
                "{session}"
            );
        }
        for session in ["premarket", "offhours", "paused", "closed"] {
            let refusal = built_with(extended(), ondo_reports(session, "[]")).refusal();
            assert_eq!(refusal.reason(), "market_session", "{session}");
        }
        assert_eq!(
            extended().sessions().unwrap(),
            ["regular", "postmarket", "overnight"]
        );
        // An empty list (a blank form field) is the default.
        let blank = cfg(serde_json::json!({"sessions": []}));
        assert_eq!(blank.sessions().unwrap(), ["regular"]);
        assert!(
            built_with(blank, ondo_reports("postmarket", "[]"))
                .price()
                .is_err()
        );
        // A session nothing trades in, or a word Ondo does not report, fails the stanza.
        for bad in ["paused", "closed", "post-market", "after hours"] {
            let err = cfg(serde_json::json!({"sessions": ["regular", bad]}))
                .check(&testkit::pair())
                .unwrap_err();
            assert!(format!("{err:#}").contains("sessions"), "{bad}: {err:#}");
        }
    }

    #[test]
    fn any_session_but_regular_withdraws() {
        for session in [
            "premarket",
            "postmarket",
            "overnight",
            "offhours",
            "paused",
            "closed",
        ] {
            let refusal = built(ondo_reports(session, "[]")).refusal();
            assert_eq!(refusal.reason(), "market_session", "{session}");
            assert!(refusal.message().contains(session), "{}", refusal.message());
        }
    }

    #[test]
    fn an_active_asset_event_withdraws() {
        let refusal = built(ondo_reports(
            "regular",
            r#"[{"symbol":"AAPLon","status":"active","reason":{"code":"ASSET_PAUSED","message":"halt"}}]"#,
        ))
        .refusal();
        assert_eq!(refusal.reason(), "asset_event");
        assert!(
            refusal.message().contains("ASSET_PAUSED"),
            "{}",
            refusal.message()
        );
        // Another asset's event is not this one's.
        let other = ondo_reports(
            "regular",
            r#"[{"symbol":"TSLAon","status":"active","reason":{"code":"ASSET_PAUSED","message":"halt"}}]"#,
        );
        assert!(built(other).price().is_ok());
    }

    #[test]
    fn an_event_under_the_stocks_ticker_withdraws() {
        let refusal = built(ondo_reports(
            "regular",
            r#"[{"symbol":"AAPL","status":"active","reason":{"code":"ASSET_PAUSED","message":"halt"}}]"#,
        ))
        .refusal();
        assert_eq!(refusal.reason(), "asset_event");
        assert_eq!(refusal.message(), "Ondo reports AAPLon: ASSET_PAUSED: halt");
    }

    #[test]
    fn a_corporate_action_ondo_lists_as_upcoming_withdraws_before_the_chain_schedules_it() {
        // No pauseStartTime on chain (healthy's multiplier has none): Ondo's list is all
        // there is.
        let refusal = built(ondo_reports(
            "regular",
            r#"[{"symbol":"AAPLon","status":"upcoming","type":"scheduled","updateSharesMultiplier":true,
                 "reason":{"code":"ASSET_PAUSED","message":"split"}}]"#,
        ))
        .refusal();
        assert_eq!(refusal.reason(), "asset_event");
        assert!(refusal.message().contains("split"), "{}", refusal.message());
    }

    #[test]
    fn the_stanza_is_checked_against_its_pair() {
        let pair = testkit::pair();
        assert!(Ondo.validate(&cfg(serde_json::json!({})), &pair).is_ok());
        assert!(
            Ondo.validate(
                &cfg(serde_json::json!({"api": "http://127.0.0.1:8556"})),
                &pair
            )
            .is_ok(),
            "the smoke test's mock"
        );
        for (bad, why) in [
            (serde_json::json!({"delta": "0"}), "zero delta"),
            (serde_json::json!({"delta": "0.1"}), "10% delta"),
            (serde_json::json!({"delta": "3bp"}), "not a number"),
            (serde_json::json!({"symbol": ""}), "no symbol"),
            (
                serde_json::json!({"poll_ms": 100}),
                "polling faster than Ondo caches",
            ),
            (
                serde_json::json!({"status_poll_secs": 0}),
                "status never polled",
            ),
            (
                serde_json::json!({"max_status_age_secs": 15}),
                "status always stale",
            ),
            (
                serde_json::json!({"max_price_age_secs": 1, "poll_ms": 1000}),
                "price always stale",
            ),
            (
                serde_json::json!({"max_unchanged_secs": 1}),
                "a price still at every poll",
            ),
            (
                serde_json::json!({"max_multiplier_gap": 0.0}),
                "no gap allowed",
            ),
            (serde_json::json!({"skew": 0.6}), "skew over half"),
            (
                serde_json::json!({"multiplier_oracle": "0x1234"}),
                "short address",
            ),
            (serde_json::json!({"api": "ftp://ondo"}), "not http"),
            (serde_json::json!({"ticker": ""}), "an empty ticker"),
            (
                serde_json::json!({"api": "http://api.gm.ondo.finance"}),
                "the key in the clear",
            ),
            (
                serde_json::json!({"pause_lead_secs": u64::MAX}),
                "a lead that wraps negative",
            ),
            (
                serde_json::json!({"pause_lead_secs": 7 * 86_400 + 1}),
                "a lead over a week",
            ),
            (
                serde_json::json!({"max_price_age_secs": u64::MAX}),
                "a price age that overflows",
            ),
            (
                serde_json::json!({"max_price_age_secs": 3_601}),
                "a price over an hour old",
            ),
            (
                serde_json::json!({"max_status_age_secs": u64::MAX}),
                "a status age that overflows",
            ),
            (
                serde_json::json!({"status_poll_secs": 3_601, "max_status_age_secs": 3_602}),
                "status read hourly",
            ),
        ] {
            assert!(
                Ondo.validate(&cfg(bad), &pair).is_err(),
                "{why} was accepted"
            );
        }
    }

    #[test]
    fn a_pair_labelled_by_its_lane_validates_so_it_can_be_halted() {
        // A halted pair is never read, so the library labels it by its lane, as it does a
        // pair whose symbol() it could not read. Refusing that refused every halt of a stock
        // pair, and every reload while a symbol() read timed out; the build reads the token's
        // own symbol() instead, and only a live pair is built.
        let mut halted = testkit::pair();
        halted.label = "lane 0x95a4117f".to_owned();
        assert!(Ondo.validate(&cfg(serde_json::json!({})), &halted).is_ok());
    }

    #[test]
    fn the_stanza_refuses_unknown_keys() {
        testing::assert_rejects_unknown_fields::<OndoCfg>(
            "symbol = \"AAPLon\"\napi_key = { env = \"ONDO_API_KEY\" }\ndelta = \"0.003\"",
        );
    }

    #[test]
    fn the_mainnet_oracle_is_the_default() {
        let (_, oracle) = cfg(serde_json::json!({})).check(&testkit::pair()).unwrap();
        assert_eq!(
            format!("{oracle:#x}"),
            "0x9bc39db6fbb44b91a48b8d5a6c208b82b1741be6"
        );
    }

    /// Every field the form declares is a key the stanza reads, and every key the example
    /// stanza sets is declared, so a form round trip rebuilds the stanza unchanged.
    #[test]
    fn every_declared_field_is_a_key_the_stanza_reads_and_every_key_is_declared() {
        use crate::{
            backoffice::{field_value, kind_stanza},
            kinds::typed,
            pricing::Factory,
        };
        let factory = Ondo;
        let fields = factory.fields();
        assert!(!fields.is_empty(), "the kind declares no fields");
        let posted: std::collections::HashMap<String, String> = fields
            .iter()
            .map(|f| (format!("pricing_ondo_{}", f.name), f.placeholder.to_owned()))
            .collect();
        let table = kind_stanza("pricing", fields, "ondo", None, &posted).unwrap();
        typed::<OndoCfg>(&table).unwrap();

        let example: toml::Value =
            toml::from_str(include_str!("../../../../../example/config.stocks.toml")).unwrap();
        let stanza = example["pairs"][0]["pricing"].as_table().unwrap().clone();
        assert_eq!(stanza["kind"].as_str(), Some("ondo"));
        let posted: std::collections::HashMap<String, String> = fields
            .iter()
            .map(|f| {
                (
                    format!("pricing_ondo_{}", f.name),
                    field_value(f, stanza.get(f.name)),
                )
            })
            .collect();
        let rebuilt = kind_stanza("pricing", fields, "ondo", Some(&stanza), &posted).unwrap();
        assert_eq!(rebuilt, stanza);
    }
}
