//! Ondo's [`Valuation`]: a GM token's share multiplier (`sValue`) and its corporate-action
//! pause, as the `SyntheticSharesOracle` holds them for the token.

use std::sync::Arc;

use crate::{
    Address, U256,
    calldata::{Value, decode_return_data},
    eyre,
    prelude::BoxFuture,
};

use crate::{
    stocks::chain::Calls,
    stocks::ports::{Action, Shares, Valuation},
};

/// Ondo's `SyntheticSharesOracle` on Ethereum mainnet.
pub const MAINNET_MULTIPLIER_ORACLE: &str = "0x9BC39DB6fbB44B91a48b8D5A6C208B82B1741bE6";

/// What the oracle's `assetData(token)` holds for a GM token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Multiplier {
    /// Shares per token at 18 decimals: the token is worth the stock's price times this.
    pub s_value: U256,
    /// When a scheduled corporate action pauses the asset, in seconds since the epoch; 0 for
    /// none scheduled.
    pub pause_start: u64,
}

impl Multiplier {
    /// Shares per token, as a number.
    pub fn as_f64(&self) -> f64 {
        // A uint128 on chain, so it fits; the 1e18 scale leaves f64 every digit it can hold.
        self.s_value.as_u128() as f64 / 1e18
    }

    /// As the core reads it. The pause is on by the oracle's own rule (`pauseStartTime > 0
    /// && block.timestamp >= pauseStartTime`), until Ondo clears it.
    pub fn shares(&self) -> Shares {
        Shares {
            k: self.as_f64(),
            action: (self.pause_start > 0).then_some(Action {
                starts: self.pause_start,
            }),
        }
    }
}

/// `assetData(token)` as `(uint128 sValue, uint128 pendingSValue, uint256 lastUpdate,
/// uint256 pauseStartTime, uint16 allowedDriftBps, uint48 driftCooldown)`. A zero `sValue` is
/// a token the oracle does not list (`getSValue` reverts on it, `AssetNotFound`), answered
/// as `None`: the oracle holds entries for some GM tokens and not others.
pub fn decode_asset_data(values: &[Value]) -> Result<Option<Multiplier>, String> {
    let [
        Value::Uint(s_value),
        Value::Uint(_pending),
        Value::Uint(_last_update),
        Value::Uint(pause_start),
        Value::Uint(_drift_bps),
        Value::Uint(_drift_cooldown),
    ] = values
    else {
        return Err(format!(
            "answered {} values, not the six of (uint128,uint128,uint256,uint256,uint16,uint48)",
            values.len()
        ));
    };
    if s_value.is_zero() {
        return Ok(None);
    }
    if *s_value > U256::from(u128::MAX) || *pause_start > U256::from(u64::MAX) {
        return Err("answered values outside their declared types".to_owned());
    }
    Ok(Some(Multiplier {
        s_value: *s_value,
        pause_start: pause_start.as_u64(),
    }))
}

/// The oracle's multiplier for `token`, or `None` for a token it does not list.
pub async fn read_multiplier(
    chain: &dyn Calls,
    oracle: Address,
    token: Address,
) -> eyre::Result<Option<Multiplier>> {
    let sig = "assetData(address)";
    let answer = chain
        .call_sig(oracle, sig, &[Value::Address(token)])
        .await
        .map_err(|err| eyre::eyre!("{sig} on {oracle:#x}: {err:#}"))?;
    let values = decode_return_data("r(uint128,uint128,uint256,uint256,uint16,uint48)", &answer)
        .map_err(|err| eyre::eyre!("{sig} on {oracle:#x}: {err}"))?;
    decode_asset_data(&values).map_err(|err| eyre::eyre!("{sig}({token:#x}) on {oracle:#x}: {err}"))
}

/// A GM token's [`Shares`], read from `oracle`.
pub struct OndoOracle {
    /// The chain the oracle is read through.
    pub chain: Arc<dyn Calls>,
    /// The `SyntheticSharesOracle`.
    pub oracle: Address,
    /// The GM token whose shares are read.
    pub token: Address,
}

impl Valuation for OndoOracle {
    fn read(&self) -> BoxFuture<'_, eyre::Result<Option<Shares>>> {
        Box::pin(async move {
            read_multiplier(&*self.chain, self.oracle, self.token)
                .await
                .map(|multiplier| multiplier.map(|multiplier| multiplier.shares()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeChain;

    #[tokio::test]
    async fn the_multiplier_is_the_oracles_answer_for_this_token() {
        let (oracle, token) = (Address::repeat_byte(0x9B), Address::repeat_byte(0x14));
        let chain = FakeChain::default().answer(
            oracle,
            "assetData(address)",
            &[Value::Address(token)],
            "uint128,uint128,uint256,uint256,uint16,uint48",
            &asset_data(U256::from(1_002_124_000_000_000_000u64), 1_791_500_000),
        );
        let ondo = OndoOracle {
            chain: Arc::new(chain),
            oracle,
            token,
        };
        let shares = ondo.read().await.unwrap().expect("listed");
        assert!((shares.k - 1.002124).abs() < 1e-15, "{}", shares.k);
        assert_eq!(shares.action.map(|a| a.starts), Some(1_791_500_000));
        // Another token's is not asked for.
        let other = OndoOracle {
            token: Address::repeat_byte(0x15),
            ..ondo
        };
        assert!(other.read().await.is_err());
    }

    fn asset_data(s_value: U256, pause_start: u64) -> Vec<Value> {
        vec![
            Value::Uint(s_value),
            Value::Uint(U256::zero()),
            Value::Uint(U256::from(1_790_000_000u64)),
            Value::Uint(U256::from(pause_start)),
            Value::Uint(U256::from(50)),
            Value::Uint(U256::from(3_600)),
        ]
    }

    #[test]
    fn asset_data_gives_the_multiplier_and_any_scheduled_pause() {
        let one_and_a_bit = U256::from(1_002_124_000_000_000_000u64);
        assert_eq!(
            decode_asset_data(&asset_data(one_and_a_bit, 0)),
            Ok(Some(Multiplier {
                s_value: one_and_a_bit,
                pause_start: 0
            }))
        );
        let m = decode_asset_data(&asset_data(one_and_a_bit, 1_791_500_000))
            .unwrap()
            .unwrap();
        assert_eq!(m.pause_start, 1_791_500_000);
        assert!((m.as_f64() - 1.002124).abs() < 1e-15, "{}", m.as_f64());
    }

    /// A zero `sValue` is the oracle's answer for a token it was never given, as distinct
    /// from an answer that does not decode, which is a failed read.
    #[test]
    fn a_token_the_oracle_does_not_list_is_none_and_a_broken_answer_is_an_error() {
        assert_eq!(decode_asset_data(&asset_data(U256::zero(), 0)), Ok(None));
        assert!(decode_asset_data(&asset_data(U256::exp10(18), 0)[..5]).is_err());
    }

    #[test]
    fn a_scheduled_pause_is_an_action_and_none_is_no_action() {
        let scheduled = Multiplier {
            s_value: U256::exp10(18),
            pause_start: 1_791_500_000,
        };
        assert_eq!(
            scheduled.shares(),
            Shares {
                k: 1.0,
                action: Some(Action {
                    starts: 1_791_500_000,
                }),
            }
        );
        let none = Multiplier {
            s_value: U256::exp10(18),
            pause_start: 0,
        };
        assert_eq!(none.shares().action, None, "0 schedules nothing");
    }
}
