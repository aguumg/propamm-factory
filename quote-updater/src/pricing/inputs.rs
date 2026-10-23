//! What a pricer may read beyond its stanza and the reference market: the chain, the
//! market's σ history, the pair's vault inventory and the lane's own landings. Each is
//! handed out by [`super::BuildCtx`] while the pricer is built, so `price()` reads what its
//! build prepared and stays synchronous. All four exist for the core already; this is the
//! same plumbing, made reachable.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ethrex_common::Address;
use ethrex_rpc::clients::eth::EthClient;
use tokio::sync::watch;

pub use crate::volatile::Inventory;
use crate::volatile::{self, PriceHistory};

/// A read-only view of the chain, for a pricer that anchors to an oracle or reads a
/// contract of its own. Every call is bounded by the updater's RPC timeout (3s), so a hung
/// node fails the read rather than the build or the task making it. Cheap to clone into a
/// task started with [`super::BuildCtx::spawn`].
#[derive(Clone)]
pub struct Chain {
    pub(crate) client: EthClient,
    pub(crate) target: Address,
}

impl Chain {
    pub(crate) fn new(client: EthClient, target: Address) -> Chain {
        Chain { client, target }
    }

    /// `eth_call` at the latest block: `calldata` in, the return data out, neither decoded.
    /// ABI encoding is the caller's: [`Self::call_sig`] does it from a signature with the
    /// types re-exported as [`crate::calldata`], and bytes carry no type identity across
    /// the ethrex git pins the way `U256` does, so any encoder works here. The answer is
    /// read with [`crate::calldata::decode_return_data`].
    pub async fn call(&self, to: Address, calldata: Vec<u8>) -> eyre::Result<Vec<u8>> {
        crate::rpc::call_view(&self.client, to, calldata, None, None).await
    }

    /// [`Self::call`] with the calldata encoded here: `signature` as `balanceOf(address)`,
    /// `args` as [`crate::calldata::Value`]s. The answer is still undecoded:
    /// [`crate::calldata::decode_return_data`] reads it against the return types, as in
    /// `decode_return_data("(uint256)", &answer)`. Not `decode_calldata`, which expects a
    /// selector the answer does not have and would read every word four bytes late.
    pub async fn call_sig(
        &self,
        to: Address,
        signature: &str,
        args: &[crate::calldata::Value],
    ) -> eyre::Result<Vec<u8>> {
        let calldata = crate::calldata::encode_calldata(signature, args)
            .map_err(|err| eyre::eyre!("encoding `{signature}`: {err}"))?;
        self.call(to, calldata).await
    }

    /// [`Self::call`] as if `from` sent it, telling a call the contract refused from one
    /// that could not be made: what a guard needs to ask whether a contract would let an
    /// account do something, by trying it (a token transfer the PropAMM would make, say).
    /// `Ok(CallOutcome::Reverted)` when the node ran the call and it reverted; `Err` only
    /// when no answer came back: the node unreachable or slow, or an error that is not a
    /// revert.
    pub async fn call_as(
        &self,
        from: Address,
        to: Address,
        calldata: Vec<u8>,
    ) -> eyre::Result<CallOutcome> {
        crate::rpc::call_as(&self.client, from, to, calldata).await
    }

    /// [`Self::call_as`] with the calldata encoded here, as [`Self::call_sig`] does.
    pub async fn call_sig_as(
        &self,
        from: Address,
        to: Address,
        signature: &str,
        args: &[crate::calldata::Value],
    ) -> eyre::Result<CallOutcome> {
        let calldata = crate::calldata::encode_calldata(signature, args)
            .map_err(|err| eyre::eyre!("encoding `{signature}`: {err}"))?;
        self.call_as(from, to, calldata).await
    }
}

/// What a call made with [`Chain::call_as`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallOutcome {
    /// It ran to the end: its return data, undecoded.
    Returned(Vec<u8>),
    /// It reverted: the data it reverted with (a custom error, `Error(string)`, or none).
    Reverted(Vec<u8>),
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chain(target {:#x})", self.target)
    }
}

/// The reference market's recent mids, one per second, from which σ is measured. Kept at
/// the process level per market, so a reload that rebuilds the lane does not restart the
/// minute of history σ needs; the volatile model reads the same samples.
#[derive(Clone, Debug)]
pub struct History(pub(crate) Arc<Mutex<PriceHistory>>);

impl History {
    /// A history with nothing in it yet: what `--check` hands a pricer, since it does not
    /// wait the minute σ needs.
    pub(crate) fn empty() -> History {
        History(Arc::new(Mutex::new(PriceHistory::new(Instant::now()))))
    }

    /// σ per √second over the last `window`, from the log returns of the samples in it, or
    /// `Err(covered)` with how much history there is when that is under a minute, which is
    /// what the volatile model refuses as `warming_up`. An hour is kept, so a longer
    /// `window` reads an hour.
    pub fn volatility(&self, now: Instant, window: Duration) -> Result<f64, Duration> {
        volatile::lock(&self.0).volatility(now, window)
    }
}

/// The pair's vault balances, refreshed every 12s by a task shared by every lane reading
/// the vault, which stops when the last of them drops its feed. Cheap to clone and to read.
#[derive(Clone)]
pub struct InventoryFeed(pub(crate) Arc<crate::vault::SharedReader>);

impl InventoryFeed {
    /// A feed over a receiver of its own: what a test seeds, and what a run's reader is
    /// registered as.
    pub(crate) fn from_receiver(rx: watch::Receiver<Inventory>) -> InventoryFeed {
        InventoryFeed(Arc::new(crate::vault::SharedReader { rx }))
    }

    /// The latest reading. Its age is the health signal: a refresher that cannot reach the
    /// chain leaves the reading where it was, and the volatile model refuses to tilt on one
    /// over a minute old (`no_inventory`).
    pub fn latest(&self) -> Inventory {
        *self.0.rx.borrow()
    }
}

impl std::fmt::Debug for InventoryFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InventoryFeed({:?})", self.latest())
    }
}

impl Inventory {
    /// How old this reading is at `now`.
    pub fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.at)
    }
}

/// What happened to this lane's update for one block, as the read-back after the block
/// saw it: the same outcome `quote_updater_landings_total` counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LandingReport {
    /// The block the update was quoted for.
    pub block: u64,
    /// How the block went for the lane.
    pub landing: LandingOutcome,
    /// When the read-back answered, on this process's clock.
    pub at: Instant,
}

/// The four ways a block can go for a lane. `#[non_exhaustive]` like every enum an
/// observer or a pricer matches on: a fifth would not be a breaking change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LandingOutcome {
    /// The lane holds this pair's update for the block.
    Landed,
    /// An update was quoted for the block and the lane does not hold it.
    Missed,
    /// The read-back failed, so whether it landed is not known. Never a healthy sign.
    Unknown,
    /// Nothing was quoted for the block (the feed was stale, or the quote withdrawn), so
    /// nothing was expected to land and the block says nothing about the lane.
    NotQuoted,
}

impl From<crate::landing::Landing> for LandingOutcome {
    fn from(landing: crate::landing::Landing) -> LandingOutcome {
        use crate::landing::Landing;
        match landing {
            Landing::Landed => LandingOutcome::Landed,
            Landing::Missed => LandingOutcome::Missed,
            Landing::Unknown => LandingOutcome::Unknown,
            Landing::NotQuoted => LandingOutcome::NotQuoted,
        }
    }
}

/// This lane's landings, block by block, from the read-back the quote loop runs after each
/// block passes. Holds the latest report; a task started with [`super::BuildCtx::spawn`]
/// can await each one with [`Self::next`]. Cheap to clone.
#[derive(Clone)]
pub struct LandingFeed(pub(crate) watch::Receiver<Option<LandingReport>>);

impl LandingFeed {
    /// The latest report, or `None` before the first block has passed. A block behind the
    /// quote it answers for: the tick that quotes block `n` sees at most block `n - 1`'s.
    pub fn latest(&self) -> Option<LandingReport> {
        *self.0.borrow()
    }

    /// Waits for the next report after the last one this feed handed out (or after it was
    /// created), and hands it out; `None` once the lane is gone, which ends a task's loop.
    pub async fn next(&mut self) -> Option<LandingReport> {
        self.0.changed().await.ok()?;
        *self.0.borrow_and_update()
    }
}

impl std::fmt::Debug for LandingFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LandingFeed({:?})", self.latest())
    }
}

#[cfg(test)]
mod tests {
    use ethrex_common::U256;
    use ethrex_l2_common::calldata::Value;

    use super::*;
    use crate::rpc_mock::MockRpc;

    async fn chain_on(rpc: &MockRpc) -> Chain {
        let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
        Chain::new(client, Address::repeat_byte(0x77))
    }

    /// Ondo's `UserBlocked(address)` custom error for 0x01…01: what a token answers a
    /// transfer it refuses, as revert data.
    fn user_blocked() -> Vec<u8> {
        crate::calldata::encode_calldata(
            "UserBlocked(address)",
            &[Value::Address(Address::repeat_byte(0x01))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_call_the_contract_refuses_is_told_apart_from_one_that_fails() {
        let rpc = MockRpc::spawn(100).await;
        rpc.set_revert("transferFrom(address,address,uint256)", user_blocked());
        let chain = chain_on(&rpc).await;
        let (spender, token, vault) = (
            Address::repeat_byte(0x77),
            Address::repeat_byte(0x14),
            Address::repeat_byte(0x01),
        );
        let transfer = [
            Value::Address(vault),
            Value::Address(vault),
            Value::Uint(U256::zero()),
        ];

        let refused = chain
            .call_sig_as(
                spender,
                token,
                "transferFrom(address,address,uint256)",
                &transfer,
            )
            .await
            .unwrap();
        assert_eq!(refused, CallOutcome::Reverted(user_blocked()));

        let answered = chain
            .call_sig_as(spender, token, "decimals()", &[])
            .await
            .unwrap();
        let mut eighteen = [0u8; 32];
        eighteen[31] = 18;
        assert_eq!(answered, CallOutcome::Returned(eighteen.to_vec()));

        // An error that is not a revert is a read that failed, not a refusal.
        rpc.set_failing("eth_call", true);
        let failed = chain
            .call_sig_as(
                spender,
                token,
                "transferFrom(address,address,uint256)",
                &transfer,
            )
            .await;
        assert!(failed.is_err(), "{failed:?}");
    }

    #[tokio::test]
    async fn the_call_is_made_as_the_account_named() {
        let rpc = MockRpc::spawn(100).await;
        let chain = chain_on(&rpc).await;
        let spender = Address::repeat_byte(0x77);
        chain
            .call_sig_as(spender, Address::repeat_byte(0x14), "decimals()", &[])
            .await
            .unwrap();
        assert_eq!(rpc.call_froms(), vec![spender]);
    }
}
