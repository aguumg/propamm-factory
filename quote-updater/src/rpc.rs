//! The RPC helpers: `bounded`, a timeout at `RPC_TIMEOUT` that folds into the caller's
//! ordinary error path; `eth_call` with return-data decoding; and a raw request for anvil
//! cheatcodes. Not every read goes through `bounded`: the head watcher's block-number poll,
//! for one, is bounded by `head::POLL_TIMEOUT` instead (head.rs).

use std::future::Future;

use ethrex_common::{Address, Bytes};
use ethrex_l2_common::calldata::Value;
use ethrex_l2_sdk::calldata::decode_calldata;
use ethrex_rpc::{
    clients::eth::{
        EthClient, Overrides,
        errors::{EthClientError, RpcRequestError},
    },
    types::block_identifier::BlockIdentifier,
    utils::RpcRequest,
};
use eyre::{Result, WrapErr, eyre};

use crate::{RPC_TIMEOUT, pricing::CallOutcome};

/// Decodes an `eth_call`'s return data against its return types, written as a tuple:
/// `(uint80,int256,uint256,uint256,uint80)` for Chainlink's `latestRoundData()`. A name
/// before the tuple (`r(bool)`) is accepted and ignored.
///
/// Not `decode_calldata`: ethrex's decoder reads calldata, so it starts at byte 4, after a
/// selector, and return data carries none. Handed an answer directly it reads every word
/// four bytes late, which decodes without an error as numbers some 2^32 times too large.
/// This pads a dummy selector in front, so the words land where they are.
pub fn decode_return_data(types: &str, ret: &[u8]) -> Result<Vec<Value>> {
    let mut buf = vec![0u8; 4];
    buf.extend_from_slice(ret);
    Ok(decode_calldata(types, Bytes::from(buf))?)
}

/// Awaits one RPC read for at most [`RPC_TIMEOUT`], folding a timeout into the same error
/// any other failed read produces, so every caller's retry-or-report path covers it.
pub(crate) async fn bounded<T, E>(call: impl Future<Output = Result<T, E>>) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match tokio::time::timeout(RPC_TIMEOUT, call).await {
        Ok(result) => Ok(result?),
        Err(_) => Err(eyre!("no answer in {RPC_TIMEOUT:?}")),
    }
}

pub(crate) async fn call_view(
    client: &EthClient,
    to: Address,
    calldata: Vec<u8>,
    from: Option<Address>,
    block: Option<BlockIdentifier>,
) -> Result<Vec<u8>> {
    let ret = bounded(client.call(
        to,
        calldata.into(),
        Overrides {
            from,
            block,
            ..Default::default()
        },
    ))
    .await?;
    hex::decode(ret.strip_prefix("0x").unwrap_or(&ret)).wrap_err("non-hex eth_call return")
}

/// An `eth_call` made as `from`, bounded like [`call_view`], with a revert answered as
/// [`CallOutcome::Reverted`] rather than folded into the error a failed read is.
pub(crate) async fn call_as(
    client: &EthClient,
    from: Address,
    to: Address,
    calldata: Vec<u8>,
) -> Result<CallOutcome> {
    let overrides = Overrides {
        from: Some(from),
        ..Default::default()
    };
    let answer = tokio::time::timeout(RPC_TIMEOUT, client.call(to, calldata.into(), overrides))
        .await
        .map_err(|_| eyre!("no answer in {RPC_TIMEOUT:?}"))?;
    match answer {
        Ok(ret) => Ok(CallOutcome::Returned(
            hex::decode(ret.strip_prefix("0x").unwrap_or(&ret))
                .wrap_err("non-hex eth_call return")?,
        )),
        Err(err) => match revert_data(&err) {
            Some(data) => Ok(CallOutcome::Reverted(data)),
            None => Err(err.into()),
        },
    }
}

/// The data an `eth_call` reverted with, when `err` is a revert: an error the node answered
/// whose message says so ("execution reverted" from geth, reth and anvil alike, with or
/// without a reason after it). ethrex's error keeps the message and the data but not the
/// code (3), so the message is what is left to tell. Anything else is not a revert: a
/// transport failure, a rate limit, a method the node does not serve.
fn revert_data(err: &EthClientError) -> Option<Vec<u8>> {
    let EthClientError::RpcRequestError(RpcRequestError::RPCError { message, data, .. }) = err
    else {
        return None;
    };
    if !message.to_ascii_lowercase().contains("revert") {
        return None;
    }
    let data = data.as_deref().unwrap_or("0x");
    Some(hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default())
}

/// Sends a bare RPC request (anvil cheatcodes) and surfaces JSON-RPC errors.
pub(crate) async fn raw_rpc(
    client: &EthClient,
    method: &str,
    params: Option<Vec<serde_json::Value>>,
) -> Result<()> {
    client
        .send_request_parsed::<serde_json::Value>(RpcRequest::new(method, params))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethrex_common::U256;
    use ethrex_l2_sdk::calldata::encode_calldata;

    #[test]
    fn decode_return_data_round_trips_get_state_tuple() {
        let values = vec![
            Value::Uint(U256::from(1_755_000_000u64)),
            Value::Array(vec![Value::Uint(U256::from(42))]),
        ];
        let encoded = encode_calldata("r(uint32,uint256[])", &values).unwrap();
        // Strip the selector: return data is the bare tuple encoding.
        let decoded = decode_return_data("r(uint32,uint256[])", &encoded[4..]).unwrap();
        assert_eq!(decoded, values);
    }

    /// What a pricer reading an oracle through `Chain::call_sig` gets back: Chainlink's
    /// `latestRoundData()` answers five bare words, 160 bytes with no selector. Decoded
    /// through the public `calldata` module the way its docs say, every word lands where it
    /// is; read as calldata it would start four bytes in and every word would be shifted.
    #[test]
    fn a_latest_round_data_answer_decodes_through_the_public_calldata_module() {
        let answer = U256::from(300_000_000_000u64); // $3000 at 8 decimals
        let words = [
            U256::from(18_446_744_073_709_562_301u128), // roundId (uint80)
            answer,
            U256::from(1_755_000_000u64),               // startedAt
            U256::from(1_755_000_012u64),               // updatedAt
            U256::from(18_446_744_073_709_562_301u128), // answeredInRound
        ];
        let mut ret = Vec::with_capacity(160);
        for word in words {
            ret.extend_from_slice(&word.to_big_endian());
        }
        assert_eq!(ret.len(), 160);
        let decoded =
            crate::calldata::decode_return_data("(uint80,int256,uint256,uint256,uint80)", &ret)
                .unwrap();
        assert_eq!(decoded[1], Value::Int(answer));
        assert_eq!(decoded[3], Value::Uint(words[3]));
    }
}
