//! The restriction guard every tokenized-stock design registers (`ondo_token` today): it
//! withdraws a stock pair's quote while the token would refuse the fill it offers: paused,
//! or the vault or this PropAMM refused. Two reads answer that each poll: the design's
//! reading of the token's transfer rules ([`Restrictions`]), which names the reason, and
//! the transfer a fill makes, tried on the token as the PropAMM, which catches a refusal no
//! rule the design reads names.
//!
//! PropAMM cannot see any of this: its `isActive` and `quote` answer as usual while every swap
//! reverts inside the token (`contracts/test/PropAMMStockToken.t.sol` pins it, for each
//! design's token).
//! Withdrawing the quote is what makes `isActive` false from the next block, so routers stop
//! sending fills that can only revert. Withdraw, not halt: when the issuer lifts the
//! restriction, quoting resumes.
//!
//! ```toml
//! [[pairs.guards]]
//! kind         = "ondo_token"
//! poll_secs    = 12    # one read a block
//! max_age_secs = 60    # older than this, the status is unknown and the quote withdrawn
//! ```
//!
//! The stock token is the pair's first (`tokens = [stock, USDC]`), as the pricer has it.
//! The recipient of a fill is the taker's business: a refused one reverts that fill alone.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{U256, calldata::Value, eyre, prelude::*};
use serde::Deserialize;

use crate::{
    poll::{self, Failures, lock},
    stocks::chain::{self, Calls},
    stocks::ports::{Restrictions, Rules},
};

/// The `[[pairs.guards]]` stanza of a restriction guard, whatever the design.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardCfg {
    /// Seconds between reads of the token's status (default 12, one block).
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
    /// The oldest reading the guard trusts, in seconds (default 60).
    #[serde(default = "default_max_age_secs")]
    pub max_age_secs: u64,
}

/// The oldest reading any stanza may trust; `poll_secs` is under it too.
const MAX_AGE_LIMIT_SECS: u64 = 3_600;

fn default_poll_secs() -> u64 {
    12
}
fn default_max_age_secs() -> u64 {
    60
}

/// The latest reading: everything that would revert a fill now, and when it was read.
struct Reading {
    at: Instant,
    problems: Vec<String>,
}

/// A restriction guard.
pub struct RestrictionGuard {
    reading: Arc<Mutex<Option<Reading>>>,
    max_age: Duration,
    /// The token would refuse the fill.
    refused: RefusalHandle,
    /// No recent enough reading to say.
    unverified: RefusalHandle,
}

impl QuoteGuard for RestrictionGuard {
    fn check(&mut self, candidate: &Candidate<'_>, _out: &mut Diagnostics) -> Gate {
        let now = candidate.tick().now();
        match &*lock(&self.reading) {
            None => Gate::Withdraw(Refusal::new(
                &self.unverified,
                "no reading of the token's status yet",
            )),
            Some(reading) if now.saturating_duration_since(reading.at) > self.max_age => {
                Gate::Withdraw(Refusal::new(
                    &self.unverified,
                    format!(
                        "the token's status was last read {}s ago (limit {}s)",
                        now.saturating_duration_since(reading.at).as_secs(),
                        self.max_age.as_secs()
                    ),
                ))
            }
            Some(reading) if reading.problems.is_empty() => Gate::Allow,
            Some(reading) => {
                Gate::Withdraw(Refusal::new(&self.refused, reading.problems.join("; ")))
            }
        }
    }
}

/// What builds a design's [`Restrictions`] for a token, read through a chain.
type Restricted = dyn Fn(Arc<dyn Calls>, Address) -> Box<dyn Restrictions> + Send + Sync;

/// A restriction guard's kind: the [`Restrictions`] of its design, and who keeps them.
/// Built with [`Self::new`]; a design whose rules need a setting of its own (a registry's
/// address, say) carries it in the closure.
pub struct RestrictionGuardFactory {
    /// Who keeps the rules, as messages name it: `Ondo`.
    issuer: &'static str,
    /// The design's rules for a token, read through the chain the guard hands over.
    restrictions: Box<Restricted>,
}

impl RestrictionGuardFactory {
    /// The guard over the rules `issuer` keeps, as `restrictions` reads them for the pair's
    /// stock token through the run's chain.
    pub fn new(
        issuer: &'static str,
        restrictions: impl Fn(Arc<dyn Calls>, Address) -> Box<dyn Restrictions> + Send + Sync + 'static,
    ) -> RestrictionGuardFactory {
        RestrictionGuardFactory {
            issuer,
            restrictions: Box::new(restrictions),
        }
    }
}

/// What the backoffice shows for the guard, whatever the design: its two timings.
const FIELDS: &[FormField] = &[
    FormField::number(
        "poll_secs",
        "12",
        "seconds between reads of the token's status; 12 is one block. Blank is 12.",
    ),
    FormField::number(
        "max_age_secs",
        "60",
        "the oldest reading the guard trusts; past it the status is unknown and the quote \
         withdrawn. Blank is 60.",
    ),
];

impl QuoteGuardFactory for RestrictionGuardFactory {
    type Config = GuardCfg;
    type Guard = RestrictionGuard;

    fn fields(&self) -> &'static [FormField] {
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>Withdraws while the token would refuse the fill: paused, the vault \
         or the PropAMM refused by the issuer's lists, or a transfer tried on the token as \
         the PropAMM refused for a reason no list names.</p>"
    }

    fn validate(&self, cfg: &GuardCfg, _pair: &PairShape) -> eyre::Result<()> {
        eyre::ensure!(cfg.poll_secs >= 1, "poll_secs is zero");
        eyre::ensure!(
            cfg.max_age_secs <= MAX_AGE_LIMIT_SECS,
            "max_age_secs {} is over an hour: a reading that old says nothing about the token \
             now",
            cfg.max_age_secs
        );
        eyre::ensure!(
            cfg.max_age_secs > cfg.poll_secs,
            "max_age_secs {} is not longer than poll_secs {}: every reading would be stale",
            cfg.max_age_secs,
            cfg.poll_secs
        );
        Ok(())
    }

    fn summary(&self, cfg: &GuardCfg) -> String {
        format!(
            "every {}s, stale after {}s",
            cfg.poll_secs, cfg.max_age_secs
        )
    }

    fn build<'a>(
        &'a self,
        cfg: &'a GuardCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<RestrictionGuard>> {
        Box::pin(async move {
            let chain: Arc<dyn Calls> = Arc::new(ctx.chain()?);
            let pair = ctx.pair().clone();
            let restrictions = (self.restrictions)(Arc::clone(&chain), pair.tokens.0);
            // One read now, so a pair whose first token is not the design's fails the build
            // (and `--check`), not every tick.
            let problems = read(&*chain, &pair, &*restrictions, self.issuer).await?;
            // Not a failed build: the quote starts withdrawn and resumes once the token moves
            // again. Said here so `--check` shows it.
            if !problems.is_empty() {
                tracing::warn!(
                    "[{}] {} token status: the token would refuse a fill now, so the quote \
                     starts withdrawn: {}",
                    pair.label,
                    self.issuer,
                    problems.join("; ")
                );
            }
            let reading = Arc::new(Mutex::new(Some(Reading {
                at: Instant::now(),
                problems,
            })));
            ctx.spawn(poll(
                chain,
                pair,
                restrictions,
                self.issuer,
                Duration::from_secs(cfg.poll_secs),
                Arc::clone(&reading),
            ));
            Ok(RestrictionGuard {
                reading,
                max_age: Duration::from_secs(cfg.max_age_secs),
                refused: ctx.refusal("token_refused")?,
                unverified: ctx.refusal("token_unverified")?,
            })
        })
    }
}

/// Reads the vault (the owner can move it) beside the token's rules, then judges a fill by
/// them, and tries the transfer a fill makes. The rules say why the token would refuse;
/// the try is what catches a refusal they do not name (a check the issuer added, a rule
/// the design's adapter does not read), so the guard follows the token as it is, not as
/// its adapter was written.
async fn read(
    chain: &dyn Calls,
    pair: &PairShape,
    restrictions: &dyn Restrictions,
    issuer: &str,
) -> eyre::Result<Vec<String>> {
    let (stock, quote) = pair.tokens;
    let rules = async {
        restrictions.read().await.map_err(|err| {
            eyre::eyre!(
                "{issuer}'s status for the pair's first token: {err:#} (the stock token is \
                 listed first, tokens = [stock, USDC])"
            )
        })
    };
    let vault = chain::read_vault(chain, pair.target, stock, quote);
    let (vault, rules) = tokio::try_join!(vault, rules)?;
    let (named, tried) = tokio::try_join!(
        judge(&*rules, vault, pair.target),
        try_transfer(chain, stock, vault, pair.target),
    )?;
    Ok(match tried {
        CallOutcome::Reverted(data) if named.is_empty() => vec![format!(
            "the token refuses a transfer the PropAMM makes out of the vault, for a reason \
             {issuer}'s rules do not name: it reverts with 0x{}",
            hex::encode(&data)
        )],
        CallOutcome::Returned(data) if named.is_empty() && !succeeded(&data) => vec![format!(
            "the token refuses a transfer the PropAMM makes out of the vault, for a reason \
             {issuer}'s rules do not name: transferFrom returned 0x{} rather than true, \
             which the PropAMM's SafeERC20 reverts on",
            hex::encode(&data)
        )],
        _ => named,
    })
}

/// Whether `data`, what `transferFrom` returned, is a success to the PropAMM. It moves
/// tokens with OpenZeppelin's SafeERC20, which takes `true` or no data at all (a token that
/// returns nothing, as USDT does) and reverts on anything else: `false`, which the standard
/// lets a token refuse with, included.
fn succeeded(data: &[u8]) -> bool {
    data.is_empty() || (data.len() >= 32 && U256::from_big_endian(&data[..32]) == U256::one())
}

/// `stock.transferFrom(vault, vault, 0)` as the PropAMM: every check a fill's transfers
/// make that does not depend on the taker. The PropAMM moves the stock as the spender of
/// the vault's (out) and as the sender to the vault (in), so the token checks its pause,
/// the PropAMM and the vault on both legs, all of which this one call reaches. Zero
/// tokens needs no balance and no allowance, and an ERC-20's transfer hooks run whatever
/// the amount.
async fn try_transfer(
    chain: &dyn Calls,
    stock: Address,
    vault: Address,
    propamm: Address,
) -> eyre::Result<CallOutcome> {
    let sig = "transferFrom(address,address,uint256)";
    let args = [
        Value::Address(vault),
        Value::Address(vault),
        Value::Uint(U256::zero()),
    ];
    chain
        .call_sig_as(propamm, stock, sig, &args)
        .await
        .map_err(|err| eyre::eyre!("trying {sig} on {stock:#x} as the PropAMM: {err:#}"))
}

/// Everything in `rules` that would revert a fill through the vault and the PropAMM, each
/// said in full: what stops every transfer first, then each party's refusals.
async fn judge(rules: &dyn Rules, vault: Address, propamm: Address) -> eyre::Result<Vec<String>> {
    let (vault_refused, propamm_refused) = tokio::try_join!(
        rules.refusals("vault", vault),
        rules.refusals("PropAMM", propamm),
    )?;
    Ok([rules.problems(), vault_refused, propamm_refused].concat())
}

async fn poll(
    chain: Arc<dyn Calls>,
    pair: PairShape,
    restrictions: Box<dyn Restrictions>,
    issuer: &'static str,
    every: Duration,
    reading: Arc<Mutex<Option<Reading>>>,
) {
    let mut failures = Failures::new(format!("[{}] {issuer} token status", pair.label));
    let mut ticks = poll::every(every);
    loop {
        ticks.tick().await;
        match read(&*chain, &pair, &*restrictions, issuer).await {
            Ok(problems) => {
                *lock(&reading) = Some(Reading {
                    at: Instant::now(),
                    problems,
                });
                failures.succeeded();
            }
            Err(err) => failures.failed(&err),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::testing;

    use super::*;
    use crate::testing::FakeChain;

    fn guard_with(reading: Option<Reading>) -> (RestrictionGuard, BuildCtx) {
        let mut ctx = testing::build_ctx(&testing::pair());
        let guard = RestrictionGuard {
            reading: Arc::new(Mutex::new(reading)),
            max_age: Duration::from_secs(60),
            refused: ctx.refusal("token_refused").unwrap(),
            unverified: ctx.refusal("token_unverified").unwrap(),
        };
        (guard, ctx)
    }

    fn verdict(guard: &mut RestrictionGuard, ctx: &BuildCtx, now: Instant) -> Gate {
        let pair = testing::pair();
        let tick = testing::tick_at(&pair, None, now);
        let priced = PricerOutput::new(U256::exp10(15), U256::exp10(18));
        let seen = testing::diagnostics(ctx);
        let mut out = testing::diagnostics(ctx);
        guard.check(&testing::candidate(&tick, Ok(&priced), &seen), &mut out)
    }

    fn read_now(problems: &[&str]) -> Option<Reading> {
        Some(Reading {
            at: Instant::now(),
            problems: problems.iter().map(|p| p.to_string()).collect(),
        })
    }

    #[test]
    fn a_token_that_moves_for_every_party_lets_the_quote_through() {
        let (mut guard, ctx) = guard_with(read_now(&[]));
        assert_eq!(verdict(&mut guard, &ctx, Instant::now()), Gate::Allow);
    }

    #[test]
    fn a_paused_token_or_a_refused_party_withdraws_and_says_which() {
        let (mut guard, ctx) = guard_with(read_now(&["PropAMM 0x77 is on the sanctions list"]));
        let Gate::Withdraw(refusal) = verdict(&mut guard, &ctx, Instant::now()) else {
            panic!("a sanctioned PropAMM kept quoting");
        };
        assert_eq!(refusal.reason(), "token_refused");
        assert!(
            refusal.message().contains("sanctions"),
            "{}",
            refusal.message()
        );

        let (mut guard, ctx) = guard_with(read_now(&[
            "Ondo paused the token",
            "vault 0x01 is on Ondo's blocklist",
        ]));
        let Gate::Withdraw(refusal) = verdict(&mut guard, &ctx, Instant::now()) else {
            panic!("a paused token kept quoting");
        };
        assert_eq!(refusal.reason(), "token_refused");
        assert_eq!(
            refusal.message(),
            "Ondo paused the token; vault 0x01 is on Ondo's blocklist"
        );
    }

    #[test]
    fn without_a_recent_reading_the_quote_is_withdrawn() {
        let (mut guard, ctx) = guard_with(None);
        assert!(matches!(
            verdict(&mut guard, &ctx, Instant::now()),
            Gate::Withdraw(r) if r.reason() == "token_unverified"
        ));

        let read_at = Instant::now();
        let (mut guard, ctx) = guard_with(Some(Reading {
            at: read_at,
            problems: Vec::new(),
        }));
        assert_eq!(
            verdict(&mut guard, &ctx, read_at + Duration::from_secs(60)),
            Gate::Allow
        );
        assert!(matches!(
            verdict(&mut guard, &ctx, read_at + Duration::from_secs(61)),
            Gate::Withdraw(r) if r.reason() == "token_unverified"
        ));
    }

    /// Rules with `problems` for everyone, refusing each party in `refused`.
    struct Listed {
        problems: Vec<String>,
        refused: Vec<Address>,
    }

    impl Rules for Listed {
        fn problems(&self) -> Vec<String> {
            self.problems.clone()
        }

        fn refusals<'a>(
            &'a self,
            name: &'a str,
            party: Address,
        ) -> BoxFuture<'a, eyre::Result<Vec<String>>> {
            let refused = self.refused.contains(&party);
            Box::pin(async move {
                Ok(refused
                    .then(|| format!("{name} {party:#x} is listed"))
                    .into_iter()
                    .collect())
            })
        }
    }

    /// Restrictions whose every read finds `refused` refused.
    struct Refusing(Vec<Address>);

    impl Restrictions for Refusing {
        fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>> {
            let rules = Listed {
                problems: Vec::new(),
                refused: self.0.clone(),
            };
            Box::pin(async move { Ok(Box::new(rules) as Box<dyn Rules>) })
        }
    }

    /// The pair's chain: the PropAMM names `vault` for the pair, and the transfer the
    /// guard tries, `transferFrom(vault, vault, 0)` on the stock token as the PropAMM, ends
    /// in `trial`. Any other call fails.
    fn chain_with(vault: Address, trial: Option<CallOutcome>) -> FakeChain {
        let pair = testing::pair();
        let (stock, quote) = pair.tokens;
        let chain = FakeChain::default().answer(
            pair.target,
            "vaultFor(address,address)",
            &[Value::Address(stock), Value::Address(quote)],
            "address",
            &[Value::Address(vault)],
        );
        match trial {
            Some(outcome) => chain.outcome(
                pair.target,
                stock,
                "transferFrom(address,address,uint256)",
                &[
                    Value::Address(vault),
                    Value::Address(vault),
                    Value::Uint(U256::zero()),
                ],
                outcome,
            ),
            None => chain,
        }
    }

    /// What a token answers a transfer it refuses: a custom error naming 0x01…01 (selector
    /// made up).
    fn user_blocked() -> Vec<u8> {
        let mut data = vec![0x7a, 0x0f, 0x4b, 0x2f];
        data.extend_from_slice(&[0; 12]);
        data.extend_from_slice(&[0x01; 20]);
        data
    }

    #[tokio::test]
    async fn the_vault_judged_is_the_one_the_propamm_names_for_the_pair_now() {
        let pair = testing::pair();
        let vault = Address::repeat_byte(0x01);
        let chain = chain_with(vault, Some(CallOutcome::Reverted(user_blocked())));
        let problems = read(&chain, &pair, &Refusing(vec![vault]), "Test")
            .await
            .unwrap();
        assert_eq!(problems, vec![format!("vault {vault:#x} is listed")]);
    }

    #[tokio::test]
    async fn a_transfer_the_token_refuses_withdraws_though_no_rule_names_why() {
        let pair = testing::pair();
        let vault = Address::repeat_byte(0x01);
        let chain = chain_with(vault, Some(CallOutcome::Reverted(user_blocked())));
        let problems = read(&chain, &pair, &Refusing(Vec::new()), "Test")
            .await
            .unwrap();
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("0x7a0f4b2f"),
            "the refusal names what the token reverted with: {}",
            problems[0]
        );
    }

    #[tokio::test]
    async fn a_transfer_the_token_lets_through_adds_nothing() {
        let pair = testing::pair();
        let vault = Address::repeat_byte(0x01);
        let chain = chain_with(vault, Some(CallOutcome::Returned(returned_true())));
        let problems = read(&chain, &pair, &Refusing(Vec::new()), "Test")
            .await
            .unwrap();
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[tokio::test]
    async fn a_transfer_the_token_answers_false_to_withdraws_though_no_rule_names_why() {
        // The ERC-20 standard lets a token refuse by returning false rather than reverting,
        // and the PropAMM moves tokens with SafeERC20, which reverts on that answer.
        let pair = testing::pair();
        let vault = Address::repeat_byte(0x01);
        let chain = chain_with(vault, Some(CallOutcome::Returned(vec![0; 32])));
        let problems = read(&chain, &pair, &Refusing(Vec::new()), "Test")
            .await
            .unwrap();
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("rather than true") && problems[0].contains(&"0".repeat(64)),
            "the refusal says what the token returned instead: {}",
            problems[0]
        );
    }

    #[tokio::test]
    async fn a_transfer_the_token_answers_nothing_to_adds_nothing() {
        // SafeERC20 takes no return data as success (USDT answers that way), so the guard
        // must not withdraw on it.
        let pair = testing::pair();
        let vault = Address::repeat_byte(0x01);
        let chain = chain_with(vault, Some(CallOutcome::Returned(Vec::new())));
        let problems = read(&chain, &pair, &Refusing(Vec::new()), "Test")
            .await
            .unwrap();
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[tokio::test]
    async fn a_transfer_that_cannot_be_tried_is_a_failed_read_not_a_refusal() {
        let pair = testing::pair();
        let chain = chain_with(Address::repeat_byte(0x01), None);
        assert!(
            read(&chain, &pair, &Refusing(Vec::new()), "Test")
                .await
                .is_err()
        );
    }

    /// `true`, as `transferFrom` returns it.
    fn returned_true() -> Vec<u8> {
        let mut word = vec![0; 32];
        word[31] = 1;
        word
    }

    #[tokio::test]
    async fn a_fill_is_judged_through_the_vault_and_the_propamm_after_what_stops_everyone() {
        let (vault, propamm) = (Address::repeat_byte(0x01), Address::repeat_byte(0x77));
        let everything = Listed {
            problems: vec!["the token is paused".to_owned()],
            refused: vec![vault, propamm],
        };
        assert_eq!(
            judge(&everything, vault, propamm).await.unwrap(),
            vec![
                "the token is paused".to_owned(),
                format!("vault {vault:#x} is listed"),
                format!("PropAMM {propamm:#x} is listed"),
            ]
        );
        let nothing = Listed {
            problems: Vec::new(),
            refused: Vec::new(),
        };
        assert!(judge(&nothing, vault, propamm).await.unwrap().is_empty());
    }

    /// Rules that are never read: the factory's stanza checks need none.
    struct Unread;

    impl Restrictions for Unread {
        fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>> {
            unreachable!("validate reads nothing")
        }
    }

    fn factory() -> RestrictionGuardFactory {
        RestrictionGuardFactory::new("Test", |_, _| Box::new(Unread))
    }

    /// The design's rules are built by a closure, not a fn pointer, so an issuer whose rules
    /// need a setting of its own (a registry's address, say) carries it in.
    #[tokio::test]
    async fn the_rules_closure_carries_the_issuers_own_settings() {
        struct Registry(Address);
        impl Restrictions for Registry {
            fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>> {
                let registry = self.0;
                Box::pin(async move { Ok(Box::new(Listed(registry)) as Box<dyn Rules>) })
            }
        }
        struct Listed(Address);
        impl Rules for Listed {
            fn problems(&self) -> Vec<String> {
                vec![format!("registry {:#x}", self.0)]
            }
            fn refusals<'a>(
                &'a self,
                _: &'a str,
                _: Address,
            ) -> BoxFuture<'a, eyre::Result<Vec<String>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let registry = Address::repeat_byte(0x42);
        let factory =
            RestrictionGuardFactory::new("Test", move |_, _| Box::new(Registry(registry)));
        let chain: Arc<dyn Calls> = Arc::new(testing::FakeChain::default());
        let rules = (factory.restrictions)(chain, Address::repeat_byte(0x14))
            .read()
            .await
            .unwrap();
        assert_eq!(rules.problems(), vec![format!("registry {registry:#x}")]);
    }

    #[test]
    fn the_stanza_is_checked() {
        let pair = testing::pair();
        let ok = GuardCfg {
            poll_secs: 12,
            max_age_secs: 60,
        };
        assert!(factory().validate(&ok, &pair).is_ok());
        // Past an hour, a poll is no longer watching the token, and a reading that old says
        // nothing about it now; u64::MAX seconds would not even fit an Instant.
        for (poll_secs, max_age_secs) in [
            (0, 60),
            (12, 12),
            (30, 20),
            (3_601, 7_200),
            (12, 3_601),
            (u64::MAX - 1, u64::MAX),
        ] {
            let bad = GuardCfg {
                poll_secs,
                max_age_secs,
            };
            assert!(
                factory().validate(&bad, &pair).is_err(),
                "poll {poll_secs}s, max age {max_age_secs}s was accepted"
            );
        }
        testing::assert_rejects_unknown_fields::<GuardCfg>("poll_secs = 12");
    }

    /// Every field the form declares is a key the stanza reads, and every key the example
    /// stanza sets is declared, so a form round trip rebuilds the stanza unchanged.
    #[test]
    fn every_declared_field_is_a_key_the_stanza_reads_and_every_key_is_declared() {
        use crate::{
            backoffice::{field_value, kind_stanza},
            kinds::typed,
            pricing::QuoteGuardFactory,
        };
        let factory = factory();
        let fields = factory.fields();
        assert!(!fields.is_empty(), "the kind declares no fields");
        let posted: std::collections::HashMap<String, String> = fields
            .iter()
            .map(|f| {
                (
                    format!("guard_ondo_token_{}", f.name),
                    f.placeholder.to_owned(),
                )
            })
            .collect();
        let table = kind_stanza("guard", fields, "ondo_token", None, &posted).unwrap();
        typed::<GuardCfg>(&table).unwrap();

        let example: toml::Value =
            toml::from_str(include_str!("../../../example/config.stocks.toml")).unwrap();
        let stanza = example["pairs"][0]["guards"][1].as_table().unwrap().clone();
        assert_eq!(stanza["kind"].as_str(), Some("ondo_token"));
        let posted: std::collections::HashMap<String, String> = fields
            .iter()
            .map(|f| {
                (
                    format!("guard_ondo_token_{}", f.name),
                    field_value(f, stanza.get(f.name)),
                )
            })
            .collect();
        let rebuilt = kind_stanza("guard", fields, "ondo_token", Some(&stanza), &posted).unwrap();
        assert_eq!(rebuilt, stanza);
    }
}
