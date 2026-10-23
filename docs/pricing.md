# Pricing models

A pricing model is the piece of the quote updater that decides the price. Every block it
gets the market price of the pair (from the exchanges the pair follows) and answers with two
numbers: the mid to publish and the half-spread around it. The library does everything
around that: feeds, signing, builders, metrics, config.

A pair picks its model in its config, in the `pricing` table, by name plus the model's own
settings:

```toml
[pairs.pricing]
kind  = "feed"
delta = "0.0005"
```

The backoffice edits these settings live, without a restart.

## The models that ship with the library

**`fixed`**: always publishes the same mid and the same spread. Settings: `mid`, `delta`
(half-spread as a fraction, `0.0005` is 5 bp). For a pair whose price does not move, or to
test the wiring.

**`feed`**: publishes the exchange mid as it is. Settings: `delta`, optional. With it, the
spread you set; without it, the exchange's own spread (only allowed when the pair follows a
single exchange).

**`volatile`**: for pairs that move, like WETH/USDC. The spread is computed every block from
how much the price has been moving (sigma, measured over the last `volatility_window_secs`)
and widens when it moves more; the mid tilts to push fills towards rebalancing the vault to
a target split, and widens one side when the vault is too far off target. Settings:

- `gamma`: how much to charge for the risk of holding what you just bought until the next
  block. Bigger means wider spread and stronger tilt.
- `k`: how fast traders leave when you charge more. Bigger means tighter spread.
- `kappa`: how much to charge for the price moving between your quote and the trade.
- `target_share` (default 0.5): the fraction of the vault's value you want in the base token.
- `hold_secs` (12), `fill_delay_secs` (6), `volatility_window_secs` (600): the time horizons
  the formula uses.
- `inventory_aversion` (0, off), `inventory_band_lower`, `inventory_band_upper`: once the
  vault's base share leaves the band, trades that push it further out get more expensive, by
  this much per unit of distance. `inventory_aversion_hard`, `inventory_band_hard_lower`,
  `inventory_band_hard_upper`: a second, wider band where the charge grows faster.

The formula itself, term by term, is in [manual.md](manual.md), "Volatile pairs".

**`inventory_skew`** is not in the library but in [`example/`](../example/src/main.rs): a fixed
half-spread with the mid pushed away from the token the vault holds too much of. Settings:
`half_spread`, `strength` (how far a fully lopsided vault pushes the mid),
`max_inventory_age_secs`. It is the one to read to see how a model that looks at the vault is
written, start to finish, with comments.

**`ondo`**, for tokenized stocks, ships in the library's `stocks` module with the two guards a
stock pair needs, and the example registers it in two lines; see [stocks.md](stocks.md).

## Writing your own

A model is a Rust struct with one method. It gets the market, returns a mid and a
half-spread, or refuses to quote this block:

```rust
use quote_updater::prelude::*;

#[derive(Clone, serde::Deserialize)]
struct Skewed { half_spread: f64, skew: f64 }   // the settings in [pairs.pricing]

struct SkewedPricer { cfg: Skewed }

impl Pricer for SkewedPricer {
    fn price(&mut self, tick: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
        let market = tick.market()?;                         // the exchange price, as a person reads it
        Ok(PricerOutput::new(
            market.scaled_fraction(self.cfg.half_spread)?,   // 0.0005 = 5 bp
            market.shift(self.cfg.skew)?,                    // +0.01 = a mid 1% below market
        ))
    }
}
```

Register it under a name in `main`, next to the shipped ones:

```rust
fn main() -> std::process::ExitCode {
    Updater::builder()
        .pricer("fixed", quote_updater::pricers::Fixed)
        .pricer("feed", quote_updater::pricers::Feed)
        .pricer("volatile", quote_updater::pricers::Volatile)
        .pricer_fn("skewed", |cfg: Skewed, _: &mut BuildCtx| Ok(SkewedPricer { cfg }))
        .run_from_env()
}
```

From then on a pair can say `kind = "skewed"` with `half_spread` and `skew` in its `pricing`
table, and the backoffice shows those fields. That whole program is
[`quote-updater/examples/custom_pricer.rs`](../quote-updater/examples/custom_pricer.rs).

## What the library gives you to work with

Inside `price`, from the tick:

- `tick.market()`: the pair's current market price. `market.mid_f64()` is the price as a
  number; `market.scaled_fraction(x)` turns a fraction into a half-spread the contract
  understands; `market.shift(x)` moves the mid by a fraction (positive shifts it below the
  market).
- `Refusal::new(handle, "why")`: do not quote this block. The reason is counted in the
  metrics under the name you declared.

When the model is built, from the build context (`BuildCtx`), it can ask for:

- `ctx.inventory()`: the vault's balances of both tokens, refreshed every few seconds.
- `ctx.history()`: the last minutes of the market price, for volatility.
- `ctx.chain()`: read anything on chain.
- `ctx.http()`: an HTTP client, for a price or a signal from elsewhere.
- `ctx.spawn(...)`: a background task of your own.
- `ctx.diagnostic("name")`: a number you set every tick that shows up in Prometheus as
  `quote_updater_diagnostic{kind, name}` and is recorded with each quote.
- `ctx.refusal("name")`: a named reason for not quoting, counted in the metrics.

A model that needs any of these is a `Factory` instead of a closure: a `validate` step that
checks the settings before anything runs, and an async `build`. `example/` shows one.

Around every model, whatever it returns:

- The core refuses a spread of a whole unit or more and a zero mid, and the pair's
  `min_mid`/`max_mid` band refuses a mid outside it.
- `--check` runs the model once against the live market and prints what it would publish,
  without sending anything.
- `quote_updater::testing` builds a model and runs `price` on a market you make up, with no
  chain and no network, so a model is unit tested like any function.

[building-your-own.md](building-your-own.md) has the full detail, plus guards (code that
stops a pair) and observers (code told what happened).
