//! Reads of the chain that every tokenized-stock design shares: a token's symbol, the pair's
//! vault, and the single-value answers the issuers' adapters decode their contracts with.
//!
//! Every read goes through [`Calls`], which the run's [`Chain`] answers and a test's
//! [`fake::FakeChain`] answers only for the exact calls it was given, so an adapter's wiring
//! (which contract, which argument) is tested without a node.

use quote_updater::{
    Address,
    calldata::{Value, decode_return_data},
    eyre,
    prelude::{BoxFuture, CallOutcome, Chain},
};

/// What a read asks of the chain: one `eth_call` by signature, answering the undecoded
/// return data, or one made as an account, answering whether it reverted.
pub trait Calls: Send + Sync + 'static {
    fn call_sig<'a>(
        &'a self,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<Vec<u8>>>;

    /// `to.sig(args)` as if `from` sent it: `Reverted` when the contract refused it, `Err`
    /// only when the call could not be made.
    fn call_sig_as<'a>(
        &'a self,
        from: Address,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<CallOutcome>>;
}

impl Calls for Chain {
    fn call_sig<'a>(
        &'a self,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<Vec<u8>>> {
        Box::pin(Chain::call_sig(self, to, sig, args))
    }

    fn call_sig_as<'a>(
        &'a self,
        from: Address,
        to: Address,
        sig: &'a str,
        args: &'a [Value],
    ) -> BoxFuture<'a, eyre::Result<CallOutcome>> {
        Box::pin(Chain::call_sig_as(self, from, to, sig, args))
    }
}

/// `0x` and 40 hex digits: `Address`'s own parse, which takes them without the `0x` too.
pub fn parse_address(written: &str) -> Result<Address, String> {
    let bad = || format!("{written:?} is not an address (0x and 40 hex digits)");
    if !written.starts_with("0x") {
        return Err(bad());
    }
    written.parse().map_err(|_| bad())
}

/// `token.symbol()`.
pub async fn read_symbol(chain: &dyn Calls, token: Address) -> eyre::Result<String> {
    let sig = "symbol()";
    let answer = chain
        .call_sig(token, sig, &[])
        .await
        .map_err(|err| eyre::eyre!("{sig} on {token:#x}: {err:#}"))?;
    decode_symbol(&answer).map_err(|err| eyre::eyre!("{sig} on {token:#x}: {err}"))
}

/// A `symbol()` answer: a string, or the zero-padded bytes32 of older tokens, as the
/// library reads the labels it shows.
fn decode_symbol(answer: &[u8]) -> Result<String, String> {
    let bytes = match decode_return_data("r(bytes)", answer).as_deref() {
        Ok([Value::Bytes(bytes)]) => bytes.to_vec(),
        _ => match decode_return_data("r(bytes32)", answer).as_deref() {
            Ok([Value::FixedBytes(bytes)]) => bytes.to_vec(),
            _ => return Err("did not answer a string".to_owned()),
        },
    };
    let symbol = String::from_utf8(bytes).map_err(|_| "answered a symbol that is not UTF-8")?;
    let symbol = symbol.trim_end_matches('\0');
    if symbol.is_empty() {
        return Err("answered an empty symbol".to_owned());
    }
    Ok(symbol.to_owned())
}

/// The pair's vault, `target.vaultFor(base, quote)`. Read on every poll: the owner can move it.
pub async fn read_vault(
    chain: &dyn Calls,
    target: Address,
    base: Address,
    quote: Address,
) -> eyre::Result<Address> {
    read_address(
        chain,
        target,
        "vaultFor(address,address)",
        &[Value::Address(base), Value::Address(quote)],
    )
    .await
}

/// A call answering one address.
pub async fn read_address(
    chain: &dyn Calls,
    to: Address,
    sig: &str,
    args: &[Value],
) -> eyre::Result<Address> {
    let answer = chain
        .call_sig(to, sig, args)
        .await
        .map_err(|err| eyre::eyre!("{sig} on {to:#x}: {err:#}"))?;
    match decode_return_data("r(address)", &answer).as_deref() {
        Ok([Value::Address(address)]) => Ok(*address),
        _ => eyre::bail!("{sig} on {to:#x} did not answer an address"),
    }
}

/// A call answering one bool.
pub async fn read_bool(
    chain: &dyn Calls,
    to: Address,
    sig: &str,
    args: &[Value],
) -> eyre::Result<bool> {
    let answer = chain
        .call_sig(to, sig, args)
        .await
        .map_err(|err| eyre::eyre!("{sig} on {to:#x}: {err:#}"))?;
    match decode_return_data("r(bool)", &answer).as_deref() {
        Ok([Value::Bool(answer)]) => Ok(*answer),
        _ => eyre::bail!("{sig} on {to:#x} did not answer a bool"),
    }
}

/// A chain for tests, answering only what it was told to.
#[cfg(test)]
pub mod fake {
    use std::collections::HashMap;

    use quote_updater::calldata::encode_calldata;

    use super::*;

    /// `values` ABI-encoded as a call's return data: calldata without its selector.
    pub fn returned(types: &str, values: &[Value]) -> Vec<u8> {
        encode_calldata(&format!("f({types})"), values).unwrap()[4..].to_vec()
    }

    /// Answers each call it was given, by contract and exact calldata (and, for a call made
    /// as an account, that account), and nothing else: a call to another contract, another
    /// function, with other arguments or as another account fails, as a read that could
    /// not be made.
    #[derive(Debug, Default)]
    pub struct FakeChain {
        answers: HashMap<(Address, Vec<u8>), Vec<u8>>,
        outcomes: HashMap<(Address, Address, Vec<u8>), CallOutcome>,
    }

    impl FakeChain {
        /// `to.sig(args)` answers `values`, of the return types `types` (as `address`).
        pub fn answer(
            mut self,
            to: Address,
            sig: &str,
            args: &[Value],
            types: &str,
            values: &[Value],
        ) -> FakeChain {
            let calldata = encode_calldata(sig, args).unwrap();
            self.answers.insert((to, calldata), returned(types, values));
            self
        }

        /// `to.sig(args)`, made as `from`, ends in `outcome`.
        pub fn outcome(
            mut self,
            from: Address,
            to: Address,
            sig: &str,
            args: &[Value],
            outcome: CallOutcome,
        ) -> FakeChain {
            let calldata = encode_calldata(sig, args).unwrap();
            self.outcomes.insert((from, to, calldata), outcome);
            self
        }
    }

    impl Calls for FakeChain {
        fn call_sig<'a>(
            &'a self,
            to: Address,
            sig: &'a str,
            args: &'a [Value],
        ) -> BoxFuture<'a, eyre::Result<Vec<u8>>> {
            let answer = encode_calldata(sig, args)
                .ok()
                .and_then(|calldata| self.answers.get(&(to, calldata)).cloned());
            Box::pin(async move {
                answer.ok_or_else(|| eyre::eyre!("no contract at {to:#x} answers {sig} so called"))
            })
        }

        fn call_sig_as<'a>(
            &'a self,
            from: Address,
            to: Address,
            sig: &'a str,
            args: &'a [Value],
        ) -> BoxFuture<'a, eyre::Result<CallOutcome>> {
            let outcome = encode_calldata(sig, args)
                .ok()
                .and_then(|calldata| self.outcomes.get(&(from, to, calldata)).cloned());
            Box::pin(async move {
                outcome.ok_or_else(|| {
                    eyre::eyre!("no call of {sig} on {to:#x} as {from:#x} was expected")
                })
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fake::returned, *};

    #[test]
    fn a_symbol_reads_as_a_string_or_a_padded_bytes32() {
        let string = returned("string", &[Value::String("AAPLon".to_owned())]);
        assert_eq!(decode_symbol(&string), Ok("AAPLon".to_owned()));
        let mut padded = [0u8; 32];
        padded[..3].copy_from_slice(b"MKR");
        let legacy = returned("bytes32", &[Value::FixedBytes(padded.to_vec().into())]);
        assert_eq!(decode_symbol(&legacy), Ok("MKR".to_owned()));
        let empty = returned("string", &[Value::String(String::new())]);
        assert!(
            decode_symbol(&empty).is_err(),
            "an empty symbol names nothing"
        );
        assert!(decode_symbol(&[]).is_err());
    }

    #[test]
    fn addresses_parse_with_their_prefix_only() {
        assert_eq!(
            parse_address("0x9BC39DB6fbB44B91a48b8D5A6C208B82B1741bE6").map(|a| format!("{a:#x}")),
            Ok("0x9bc39db6fbb44b91a48b8d5a6c208b82b1741be6".to_owned())
        );
        assert!(parse_address("9BC39DB6fbB44B91a48b8D5A6C208B82B1741bE6").is_err());
        assert!(parse_address("0x9BC39DB6fbB44B91a48b8D5A6C208B82B1741bE").is_err());
        assert!(parse_address("0xZZC39DB6fbB44B91a48b8D5A6C208B82B1741bE6").is_err());
    }
}
