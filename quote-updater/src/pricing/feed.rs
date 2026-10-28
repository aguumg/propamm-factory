//! `feed`: the market's mid as it is, at the book's own half-spread or the one the stanza
//! states. What a pair that only wants to follow an exchange runs.
//!
//! ```toml
//! [pairs.pricing]
//! kind  = "feed"
//! delta = "0.0005"   # optional: the half-spread to publish; blank copies the venue's book
//! ```

use ethrex_common::U256;

use super::{
    BoxFuture, BuildCtx, Diagnostics, Factory, FormField, PairShape, Pricer, PricerOutput, Refusal,
    TickCtx,
};
use crate::{ensure_fits_216_bits, update::UnusableKind};

/// The `feed` stanza. `delta` is the half-spread to publish; blank publishes the book's own
/// `(ask − bid) / (ask + bid)`, which is only allowed for a single source: there is one book
/// to copy, and a made-up average of several venues' spreads is not a number anyone asked
/// to charge.
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedConfig {
    #[serde(default)]
    pub delta: Option<String>,
}

/// The `feed` pricing kind. Register it with `Updater::builder().pricer("feed", Feed)`.
pub struct Feed;

impl Feed {
    fn resolve(cfg: &FeedConfig, pair: &PairShape) -> eyre::Result<Option<U256>> {
        eyre::ensure!(
            pair.sources > 0,
            "feed prices the pair's market, so the pair needs a symbol or sources"
        );
        match &cfg.delta {
            Some(text) => crate::config::parse_delta(text, pair.price_decimals).map(Some),
            None => {
                eyre::ensure!(
                    pair.sources == 1,
                    "a pair with more than one source needs a delta; only a single source can \
                     copy its venue's spread"
                );
                Ok(None)
            }
        }
    }

    /// What the build says when a blank delta would copy a half-spread of zero: the pair's
    /// one source quotes a single price with no book behind it (`ondo`), so the pair would
    /// quote at the mid itself. A warning, not a refusal, said at the build so `--check`
    /// and the startup log show it.
    fn zero_spread_warning(cfg: &FeedConfig, pair: &PairShape) -> Option<String> {
        (cfg.delta.is_none() && pair.mid_only_sources > 0).then(|| {
            "the delta is blank, so the venue's half-spread is copied, and the venue quotes one \
             price with no book: that half-spread is zero and the pair quotes at the mid with \
             no spread; set a delta (\"0.003\" is 30 bp)"
                .to_owned()
        })
    }
}

const FIELDS: &[FormField] = &[FormField::new(
    "delta",
    "0.0005",
    "the spread, as a fraction of the mid. 0.0005 means we sell at mid + 0.05% and buy at \
     mid − 0.05%. Blank with a single venue: we copy that venue's spread.",
)];

impl Factory for Feed {
    type Config = FeedConfig;
    type Pricer = FeedPricer;

    fn validate(&self, cfg: &FeedConfig, pair: &PairShape) -> eyre::Result<()> {
        Self::resolve(cfg, pair).map(drop)
    }

    fn fields(&self) -> &'static [FormField] {
        FIELDS
    }

    fn form_help(&self) -> &'static str {
        "<p class=explain>We publish the venues' mid as it is, at the spread below, or at the \
         venue's own spread when the field is blank and there is one venue.</p>"
    }

    fn summary(&self, cfg: &FeedConfig) -> String {
        match &cfg.delta {
            Some(delta) => format!("delta {delta}"),
            None => "the book's spread".to_owned(),
        }
    }

    fn build<'a>(
        &'a self,
        cfg: &'a FeedConfig,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<FeedPricer>> {
        let pair = ctx.pair();
        Box::pin(std::future::ready(Self::resolve(cfg, pair).map(|delta| {
            if let Some(warning) = Self::zero_spread_warning(cfg, pair) {
                tracing::warn!("[{}] feed: {warning}", pair.label);
            }
            FeedPricer {
                delta,
                spread_scale: crate::config::spread_scale(pair.price_decimals),
            }
        })))
    }
}

/// A pair following its market: the configured half-spread, or the book's.
pub struct FeedPricer {
    pub(crate) delta: Option<U256>,
    pub(crate) spread_scale: U256,
}

impl Pricer for FeedPricer {
    fn price(&mut self, tick: &TickCtx, _out: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        let market = tick.market()?;
        let delta = match self.delta {
            Some(delta) => delta,
            None => {
                // PropAMM subtracts the spread as a fraction of the mid, so a whole unit
                // leaves the taker nothing and anything beyond reverts. A book that gapes
                // that wide is not one to quote against anyway.
                if market.delta >= self.spread_scale {
                    return Err(Refusal::core(
                        UnusableKind::DeltaOverflow,
                        format!(
                            "the book's half-spread {} is not below one whole unit ({}); \
                             refusing to publish a spread that leaves the taker nothing",
                            market.delta, self.spread_scale
                        ),
                    ));
                }
                market.delta
            }
        };
        ensure_fits_216_bits(delta)
            .map_err(|err| Refusal::core(UnusableKind::DeltaOverflow, format!("{err:#}")))?;
        Ok(PricerOutput::new(delta, market.mid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[test]
    fn without_a_delta_the_books_half_spread_is_published() {
        let pair = testing::pair();
        let market = testing::market(&pair, 4000.0, 0.0001);
        let (mut pricer, ctx) = testing::build(&Feed, "", &pair).unwrap();
        let out = pricer
            .price(
                &testing::tick(&pair, Some(&market)),
                &mut testing::diagnostics(&ctx),
            )
            .unwrap();
        assert_eq!((out.delta, out.mid), (market.delta, market.mid));
        assert!(ctx.diagnostic_names().is_empty(), "declares nothing");
    }

    /// A stated delta replaces the book's, and is not reoriented on an inverted lane:
    /// (ask-bid)/(ask+bid) is invariant under inversion, and a fraction standing in for it
    /// is too.
    #[test]
    fn a_delta_replaces_the_books_half_spread_on_either_orientation() {
        for inverted in [false, true] {
            let pair = testing::pair_with(inverted, 18);
            let market = testing::market(&pair, 4000.0, 0.0001);
            let (mut pricer, ctx) = testing::build(&Feed, "delta = \"0.0005\"", &pair).unwrap();
            let out = pricer
                .price(
                    &testing::tick(&pair, Some(&market)),
                    &mut testing::diagnostics(&ctx),
                )
                .unwrap();
            assert_eq!(out.delta, U256::from_dec_str("500000000000000").unwrap());
            assert_eq!(
                out.mid, market.mid,
                "the mid is the market's, inverted or not"
            );
        }
    }

    /// PropAMM subtracts the spread as a fraction of the mid, so a book gaping a whole unit
    /// leaves the taker nothing: refused per sample, while a stated delta never looks at it.
    #[test]
    fn a_book_half_spread_of_a_whole_unit_is_refused_and_a_stated_delta_ignores_it() {
        let pair = testing::pair();
        let gaping = testing::market(&pair, 4000.0, 1.0);
        let tick = testing::tick(&pair, Some(&gaping));
        let (mut copying, ctx) = testing::build(&Feed, "", &pair).unwrap();
        let refusal = copying
            .price(&tick, &mut testing::diagnostics(&ctx))
            .unwrap_err();
        assert_eq!(refusal.reason(), "delta_overflow");
        assert!(
            refusal.message().contains("one whole unit"),
            "{}",
            refusal.message()
        );
        let (mut stated, ctx) = testing::build(&Feed, "delta = \"0.0005\"", &pair).unwrap();
        let out = stated
            .price(&tick, &mut testing::diagnostics(&ctx))
            .expect("the stated delta is published");
        assert_eq!(out.delta, U256::from_dec_str("500000000000000").unwrap());
    }

    /// A source that quotes one price with no book behind it (`ondo`, an issuer's own
    /// price) has a half-spread of zero, so a blank delta would publish the mid at no
    /// spread. The build warns of it; a stated delta, or a source with a book, does not.
    #[test]
    fn a_blank_delta_over_a_source_with_no_book_is_warned_of() {
        let mut pair = testing::pair();
        let blank = FeedConfig::default();
        assert_eq!(Feed::zero_spread_warning(&blank, &pair), None);
        pair.mid_only_sources = 1;
        let warning = Feed::zero_spread_warning(&blank, &pair)
            .expect("a blank delta over a source with no book is warned of");
        assert!(warning.contains("zero"), "{warning}");
        assert!(warning.contains("set a delta"), "{warning}");
        let stated: FeedConfig = toml::from_str("delta = \"0.003\"").unwrap();
        assert_eq!(Feed::zero_spread_warning(&stated, &pair), None);
    }

    /// There is one book to copy; an average of several venues' spreads is not a number
    /// anyone asked to charge, so several sources need a delta. No source at all is not a
    /// feed pair.
    #[test]
    fn several_sources_need_a_delta_and_no_source_is_refused() {
        let mut pair = testing::pair();
        pair.sources = 2;
        let err = format!("{:#}", testing::build(&Feed, "", &pair).err().unwrap());
        assert!(err.contains("more than one source"), "{err}");
        testing::build(&Feed, "delta = \"0.0005\"", &pair).expect("a delta makes it whole");
        pair.sources = 0;
        let err = format!(
            "{:#}",
            testing::build(&Feed, "delta = \"0.0005\"", &pair)
                .err()
                .unwrap()
        );
        assert!(err.contains("symbol or sources"), "{err}");
    }

    #[test]
    fn a_malformed_delta_and_an_unknown_key_fail_the_build() {
        let pair = testing::pair();
        let err = format!(
            "{:#}",
            testing::build(&Feed, "delta = \"1\"", &pair).err().unwrap()
        );
        assert!(err.contains("not below 1"), "{err}");
        testing::assert_rejects_unknown_fields::<FeedConfig>("delta = \"0.0005\"");
        let names: Vec<&str> = Feed.fields().iter().map(|f| f.name).collect();
        assert_eq!(names, ["delta"]);
        assert_eq!(Feed.summary(&FeedConfig::default()), "the book's spread");
        assert_eq!(
            Feed.summary(&toml::from_str("delta = \"0.0005\"").unwrap()),
            "delta 0.0005"
        );
    }
}
