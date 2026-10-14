//! `--mode node`: send `updateState` straight to the RPC and verify it by reading the lane
//! back, for anvil, a fork, or a real network. The builder path is `quoting.rs`; this one
//! cannot withdraw what it sent, which is why breakers are refused under it (`cli.rs`).

use std::time::SystemTime;

use ethrex_common::{H256, U256, types::TxType};
use ethrex_l2_common::calldata::Value;
use ethrex_l2_sdk::{build_generic_tx, send_generic_transaction, wait_for_transaction_receipt};
use ethrex_rpc::{
    clients::eth::{EthClient, Overrides},
    types::block_identifier::{BlockIdentifier, BlockTag},
};
use eyre::{Result, WrapErr, ensure, eyre};

use crate::{
    UPDATE_GAS_LIMIT, feed,
    landing::{get_state_calldata, runway_check},
    metrics::NodePush,
    pair::{Live, SendOpts},
    quote_id, record,
    rpc::{bounded, call_view, decode_return_data, raw_rpc},
    update::{self, BLOCK_TIME_SECS, UpdateParams, UpdateStamp, next_block_max_fee},
};

/// wait_for_transaction_receipt polls every 2s, so this bounds the wait at ~20s per push.
const RECEIPT_RETRIES: u64 = 10;

/// Stamps, sends, and verifies one update: updateState(target, lane, ts, [delta, mid])
/// with ts one block time ahead of the latest block, read back through getState as the
/// target.
pub(crate) async fn push_update(
    client: &EthClient,
    live: &Live,
    opts: &SendOpts,
    check_runway: bool,
) -> Result<()> {
    // The latch, before anything: a lane a pricer's panic or a component tripped is halted
    // in this mode too, and node mode has no reload to re-arm it, so the refusal stands
    // until a restart. Read before pricing, so a pricer that recovered is not asked again.
    if let Some(trip) = live.latch.tripped() {
        eyre::bail!(
            "halted on a trip from `{}` ({}): {}; node mode has no reload, so this pair \
             will not push again until a restart",
            trip.source,
            trip.cause.as_str(),
            trip.reason.alarm
        );
    }
    // Recorded here as well as in `drive`, because this is node mode's own boundary and it
    // was the only one not recording: `price_unusable_total{reason="out_of_band"}` is what
    // PusherMidOutOfBand fires on, and with nothing incrementing it under `--mode node` the
    // one series standing between a corrupt book and an arbitrary on-chain price could never
    // move there. Read off the typed error before `?` folds it into an `eyre::Report`, which
    // is where the kind stops being recoverable — the same reason `drive` reads it early.
    let priced = match live.values.current_detailed(&live.pair.band) {
        Ok(priced) => priced,
        Err(err) => {
            err.count(&live.metrics);
            return Err(err.into());
        }
    };
    let (delta, mid) = (priced.delta, priced.mid);
    let signer = &live.pair.signer;
    let params = UpdateParams {
        registry: opts.registry,
        target: opts.target,
        lane: live.pair.lane,
        chain_id: opts.chain_id,
    };

    let parent = client
        .get_block_by_number(BlockIdentifier::Tag(BlockTag::Latest), false)
        .await
        .wrap_err("failed to fetch latest block")?;
    // A failed derivation (no base fee, ts overflow) is a chain property that will never
    // heal, but unlike builder mode's loop (which fails hard) this lands in the caller's
    // per-tick catch and retries.
    let UpdateStamp {
        ts,
        parent_base_fee,
    } = UpdateStamp::from_parent(&parent.header)?;

    // Node mode had no mid-run runway check at all, so `signer_runway_updates` kept the +Inf
    // `build_live` seeds it with for the whole life of the process and the page-severity
    // PusherSignerNearlyDry could never fire — on the one mode where a key draining is not
    // hypothetical, since every push spends gas. Priced off the parent this push already
    // fetched, so the check costs one eth_getBalance and no extra block read, on the same
    // RUNWAY_CHECK_BLOCKS cadence builder mode uses (counted in pushes rather than blocks:
    // one push is one target block's worth of spending).
    if check_runway {
        let balance = bounded(client.get_balance(
            live.pair.signer.address(),
            BlockIdentifier::Tag(BlockTag::Latest),
        ))
        .await
        .ok();
        let check = runway_check(
            live.pair.signer.address(),
            balance,
            Some(U256::from(parent_base_fee)),
        );
        if let Some(alert) = check.alert {
            tracing::warn!("[{}] {alert}", live.pair.label);
        }
        live.metrics.signer_balance_wei.set(check.balance_wei);
        live.metrics.signer_runway_updates.set(check.runway);
    }

    if opts.no_pin {
        // Without pinning, the update lands only if included in the very next block. A
        // parent already a full block time behind the wall clock (a missed slot, a lagging
        // RPC view) means the next block's timestamp must exceed ts, so the transaction is
        // doomed: skip the send instead of burning gas on a guaranteed revert. Not checked
        // when pinning — anvil chain time drifts freely from the wall clock.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        ensure!(
            now < parent.header.timestamp + BLOCK_TIME_SECS,
            "latest block timestamp ({}) is {}s behind the wall clock, so an update \
             stamped {ts} cannot land in the next block; skipping this push",
            parent.header.timestamp,
            now.saturating_sub(parent.header.timestamp),
        );
    } else {
        raw_rpc(
            client,
            "evm_setNextBlockTimestamp",
            Some(vec![serde_json::json!(ts)]),
        )
        .await
        .wrap_err(
            "evm_setNextBlockTimestamp failed; is this anvil? pass --no-pin on real networks",
        )?;
    }

    let quote_id = opts.quote_id.then(|| U256::from(quote_id::mint()));
    let calldata = update::update_calldata(&params, ts, delta, mid, quote_id)?;
    // Nonce from the pending block, not latest (the SDK's default): the pool can hold
    // queued transactions from this account (an earlier tick's, or a user's on the
    // no-mining anvil), and the latest-block count would reuse their nonce and collide.
    let nonce = client
        .get_nonce(signer.address(), BlockIdentifier::Tag(BlockTag::Pending))
        .await
        .wrap_err("failed to fetch pending nonce")?;
    // Fetched explicitly so the max fee can include the tip on top of the base-fee
    // headroom (see next_block_max_fee). Unlike the SDK default this makes a missing
    // eth_maxPriorityFeePerGas RPC a per-tick error instead of quietly falling back to
    // eth_gasPrice; every node this targets (anvil, ethrex, geth, reth) supports it.
    let priority_fee: u64 = client
        .get_max_priority_fee()
        .await
        .wrap_err("failed to fetch max priority fee")?
        .try_into()
        .map_err(|_| eyre!("max priority fee overflows u64"))?;
    let tx = build_generic_tx(
        client,
        TxType::EIP1559,
        params.registry,
        signer.address(),
        calldata.into(),
        Overrides {
            gas_limit: Some(UPDATE_GAS_LIMIT),
            nonce: Some(nonce),
            max_fee_per_gas: Some(next_block_max_fee(parent_base_fee, priority_fee)),
            max_priority_fee_per_gas: Some(priority_fee),
            chain_id: Some(params.chain_id),
            ..Default::default()
        },
    )
    .await?;
    // Named rather than left to the pair label: this is the wrap that carries "insufficient
    // funds", and the address is what the operator tops up.
    let sent = send_generic_transaction(client, tx, signer)
        .await
        .wrap_err_with(|| format!("failed to send updateState from {:#x}", signer.address()));
    if opts.mine {
        // Mine even when the send failed: a transaction left sitting in the pool by an
        // earlier tick would otherwise wedge every later tick with "transaction already
        // imported" (an unmined parent block means an identical rebuilt transaction).
        let mined = raw_rpc(client, "anvil_mine", Some(vec![])).await;
        if sent.is_ok() {
            mined.wrap_err("anvil_mine failed; is this anvil? drop --mine on real networks")?;
        }
    }
    if sent.is_err() {
        live.metrics.node_pushes(NodePush::Failed).inc();
    }
    let tx_hash = sent?;
    // Everything past this point already has a sent transaction, so `node_pushes_total`
    // must classify however it ends — including outcomes with no dedicated variant, which
    // is exactly what went missing before this fix (I2 in the final review): a receipt
    // that never arrives, or a read-back RPC/decode failure, used to `?` straight out of
    // this function with no counter touched at all, so "sent and never mined" — node
    // mode's characteristic failure — landed in no bucket. `confirm_and_verify` still uses
    // plain `?` throughout; the one classification point below is what makes a future `?`
    // added inside it safe by default (`NodePush::Failed`) instead of silently uncounted.
    if let Err(err) = confirm_and_verify(
        client,
        live,
        opts,
        &params,
        tx_hash,
        ts,
        (delta, mid),
        quote_id,
    )
    .await
    {
        let outcome = err
            .downcast_ref::<PostSendFailure>()
            .map_or(NodePush::Failed, |f| f.outcome);
        live.metrics.node_pushes(outcome).inc();
        return Err(err);
    }
    // Landed and read back: the same row the builder path records after a successful sign
    // and send, for the block this push was mined into.
    if let Some(recorder) = &opts.recorder {
        let to_f64 = |v: U256| feed::scaled_to_f64(v, opts.price_decimals);
        recorder.quote(record::QuoteRow {
            prop_amm: opts.target,
            lane: live.pair.lane,
            label: live.pair.label.clone(),
            block: parent.header.number + 1,
            ts: update::wall_clock_secs() as i64,
            feed_mid: to_f64(priced.feed_mid),
            published_mid: to_f64(mid),
            delta: to_f64(delta),
            terms: record::terms_from(&live.values.diagnostics_now()),
        });
    }
    Ok(())
}

/// Marks the two [`confirm_and_verify`] outcomes that already have a specific
/// `NodePush` classification (a mined-but-reverted transaction, and a read-back that does
/// not match what was sent), so `push_update` can recover the right counter by
/// downcasting rather than matching on message text. Its `Display` is exactly the
/// operator-facing message — wrapping a concrete error this way renders identically to a
/// bare `eyre!(...)` for both `{}` and `{:#}` (there is no extra context layer, since
/// nothing calls `.wrap_err` on it), so building it costs nothing in the printed output.
/// Anything `confirm_and_verify` returns that is *not* one of these — including any error
/// a future `?` added inside it produces — has no `PostSendFailure` to downcast to, and
/// `push_update` above falls back to `NodePush::Failed` for it, matching I2's rule: record
/// `Failed` on any post-send error that is not already `Reverted` or `Mismatch`.
#[derive(Debug)]
struct PostSendFailure {
    outcome: NodePush,
    message: String,
}

impl std::fmt::Display for PostSendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PostSendFailure {}

/// The confirm-and-verify tail of `push_update`: waits for the sent transaction's receipt,
/// then reads the lane back the way the target would. Split out so `push_update` has one
/// place to classify however this ends — see the comment at its call site.
#[allow(clippy::too_many_arguments)]
async fn confirm_and_verify(
    client: &EthClient,
    live: &Live,
    opts: &SendOpts,
    params: &UpdateParams,
    tx_hash: H256,
    ts: u32,
    (delta, mid): (U256, U256),
    quote_id: Option<U256>,
) -> Result<()> {
    let label = &live.pair.label;
    let receipt = wait_for_transaction_receipt(tx_hash, client, RECEIPT_RETRIES).await?;
    if !receipt.receipt.status {
        return Err(PostSendFailure {
            outcome: NodePush::Reverted,
            message: format!(
                "updateState reverted: tx {tx_hash:#x}{}",
                if opts.no_pin {
                    " (without pinning, an update lands only when included in the very next \
                     block; a missed slot or delayed inclusion fails the registry's exact \
                     timestamp match and is retried with a fresh timestamp next tick)"
                } else {
                    ""
                }
            ),
        }
        .into());
    }
    tracing::info!(
        "[{label}] update landed: tx {tx_hash:#x} in block {}",
        receipt.block_info.block_number
    );

    // Read the state back the way the target would (getState is scoped to msg.sender).
    let calldata = get_state_calldata(params.lane)?;
    // Read at the update's own block: at `latest`, another writer to the same
    // target+lane could already have overwritten it, failing a push that landed.
    let ret = call_view(
        client,
        params.registry,
        calldata,
        Some(params.target),
        Some(BlockIdentifier::Number(receipt.block_info.block_number)),
    )
    .await?;
    let got = decode_return_data("r(uint32,uint256[])", &ret)?;
    // The registry stores the slots as sent, so the third is read back too when there is one.
    let mut slots = vec![Value::Uint(delta), Value::Uint(mid)];
    slots.extend(quote_id.map(Value::Uint));
    let want = vec![Value::Uint(ts.into()), Value::Array(slots)];
    if got != want {
        return Err(PostSendFailure {
            outcome: NodePush::Mismatch,
            message: format!("read-back mismatch: got {got:?}, want {want:?}"),
        }
        .into());
    }
    // `Landed` fires only here, after the read-back has been checked against what was
    // sent — not right after the receipt's status bit above. A mined, non-reverted
    // transaction whose read-back does not match is not a success: see `verify_landed`'s
    // doc comment for why an unverifiable (or wrong) state must never render as a healthy
    // one. Moving this earlier would let a mismatch double-count as both `Landed` and
    // `Mismatch`, and no test would catch it — the two `.inc()` calls are far enough apart
    // in the function that nothing else enforces the ordering.
    live.metrics.node_pushes(NodePush::Landed).inc();
    let id = quote_id.map_or(String::new(), |id| format!(" quote_id {id:#x}"));
    tracing::info!(
        "[{label}] verified via getState as {:#x}: lane {} -> slots = [{delta}, {mid}] at timestamp {ts}{id}",
        params.target,
        params.lane
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{self, Pair, PricingSpec, ResolvedPair},
        metrics,
        pair::build_live,
        venue, volatile,
    };
    use ethrex_common::Address;
    use url::Url;

    /// `PostSendFailure` exists only to carry a `NodePush` classification across the `?`
    /// boundary in `confirm_and_verify`; it must not change what the operator sees. Proves
    /// both the exact rendering (the two forms the codebase actually prints:
    /// `to_string()`/`{}`, used when the error is displayed directly, and `{:#}`, used at
    /// the retry-loop's `eprintln!`) and that `push_update`'s downcast recovers the
    /// classification it was built with.
    #[test]
    fn post_send_failure_renders_unchanged_and_downcasts_back_to_its_outcome() {
        let err: eyre::Report = PostSendFailure {
            outcome: NodePush::Reverted,
            message: "updateState reverted: tx 0x00".to_owned(),
        }
        .into();
        assert_eq!(err.to_string(), "updateState reverted: tx 0x00");
        assert_eq!(format!("{err:#}"), "updateState reverted: tx 0x00");
        assert_eq!(
            err.downcast_ref::<PostSendFailure>().map(|f| f.outcome),
            Some(NodePush::Reverted)
        );

        // A plain eyre report (what a bare `?` inside confirm_and_verify would produce)
        // downcasts to nothing, which is exactly how push_update's fallback works: no
        // PostSendFailure to recover means NodePush::Failed.
        let other = eyre!("some other post-send error");
        assert!(other.downcast_ref::<PostSendFailure>().is_none());
    }

    /// For a safety feature, silently not protecting is the worst failure mode: an
    /// operator who arms `max_deviation` on a pair and then runs `--mode node` must be
    /// told the breaker does nothing there, not left believing they are covered. Node
    /// mode has no live quote to withdraw — the update goes straight to the RPC.
    /// The node-mode twin of `quoting.rs`'s
    /// `an_out_of_band_static_mid_is_recorded_before_the_gate_converts_it`.
    ///
    /// `price_unusable_total{reason="out_of_band"}` is what PusherMidOutOfBand fires on, and
    /// it was recorded only in `drive` — so under `--mode node` the one series standing
    /// between a corrupt book and an arbitrary on-chain price could never move, on the mode
    /// that submits transactions directly. A `Static` mid pinned outside its own band reaches
    /// the recording site with no chain, no feed and no network: `current` refuses it before
    /// `push_update` fetches anything.
    /// Node mode reads the latch too: a lane a pricer's panic (or a component) tripped is
    /// halted, and the next tick must not price it again and push, however well the
    /// pricer recovers. Refused before the first RPC call, like an out-of-band mid.
    #[tokio::test]
    async fn node_mode_does_not_push_a_tripped_lane() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x45; 32]).unwrap();
        let resolved = ResolvedPair {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane: U256::from(9),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "NODE/TEST".to_owned(),
                band: config::MidBand {
                    min: None,
                    max: None,
                },
            },
            pricing: PricingSpec::fixed_for_tests("1", "0.000000000000000001"),
            feeds: None,
            invert: false,
            breaker: None,
            guards: Vec::new(),
        };
        let metrics = metrics::Metrics::new().unwrap();
        let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
        let live = build_live(
            resolved,
            &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
            18,
            &metrics,
            (&client, Address::zero()),
            &volatile::Histories::default(),
            &crate::vault::InventoryReaders::default(),
            None,
            &crate::kinds::Kinds::shipped(),
        )
        .await
        .unwrap()
        .live;
        let opts = SendOpts {
            registry: Address::from_slice(&[0x11; 20]),
            target: Address::from_slice(&[0x77; 20]),
            chain_id: 1,
            no_pin: false,
            mine: false,
            once: true,
            requote_ms: 50,
            disable_cross_region: false,
            price_decimals: 18,
            recorder: None,
            quote_id: false,
        };
        live.latch.trip(
            "funding",
            crate::guard::Cause::Panic,
            crate::guard::TripReason::new("price of `funding` panicked: boom"),
        );
        let err = push_update(&client, &live, &opts, false)
            .await
            .expect_err("a tripped lane is halted in node mode too");
        let text = format!("{err:#}");
        assert!(
            text.contains("halted") && text.contains("funding"),
            "the refusal names the trip: {text}"
        );
        assert_eq!(
            live.metrics.target_blocks.get(),
            0,
            "nothing was priced or pushed"
        );
    }

    #[tokio::test]
    async fn node_mode_records_an_unusable_price_before_it_gives_up() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x44; 32]).unwrap();
        let resolved = ResolvedPair {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane: U256::from(9),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "NODE/TEST".to_owned(),
                // The mid below sits an order of magnitude under this floor. The fixed
                // kind would refuse the combination when it is built, so the pair is priced
                // by a kind that only produces the mid at tick time, as a kind reading a
                // market does; building `Live` directly is what makes the path reachable,
                // as it is in the builder-mode twin.
                band: config::MidBand {
                    min: Some(U256::exp10(18)),
                    max: None,
                },
            },
            pricing: PricingSpec::for_tests("low", &[]),
            feeds: None,
            invert: false,
            breaker: None,
            guards: Vec::new(),
        };
        struct Low;
        impl crate::pricing::Pricer for Low {
            fn price(
                &mut self,
                _: &crate::pricing::TickCtx,
                _: &mut crate::pricing::Diagnostics,
            ) -> Result<crate::pricing::PricerOutput, crate::pricing::Refusal> {
                Ok(crate::pricing::PricerOutput::new(
                    U256::one(),
                    U256::exp10(17),
                ))
            }
        }
        #[derive(Clone, serde::Deserialize)]
        struct NoKeys {}
        let mut kinds = crate::kinds::Kinds::default();
        kinds.insert(
            "low",
            crate::kinds::fn_factory(|_: NoKeys, _: &mut crate::pricing::BuildCtx| Ok(Low)),
        );
        let metrics = metrics::Metrics::new().unwrap();
        let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
        let live = build_live(
            resolved,
            &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
            18,
            &metrics,
            (&client, Address::zero()),
            &volatile::Histories::default(),
            &crate::vault::InventoryReaders::default(),
            None,
            &kinds,
        )
        .await
        .unwrap()
        .live;
        let opts = SendOpts {
            registry: Address::from_slice(&[0x11; 20]),
            target: Address::from_slice(&[0x77; 20]),
            chain_id: 1,
            no_pin: false,
            mine: false,
            once: true,
            requote_ms: 50,
            disable_cross_region: false,
            price_decimals: 18,
            recorder: None,
            quote_id: false,
        };
        // Port 1: nothing listens, and nothing needs to — the refusal happens before the
        // first RPC call. If that ever stops being true this test will hang rather than pass
        // quietly, which is the failure mode to prefer.
        let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
        assert!(
            push_update(&client, &live, &opts, false).await.is_err(),
            "an out-of-band mid must not be pushed"
        );
        assert_eq!(
            live.metrics
                .price_unusable(crate::update::UnusableKind::OutOfBand)
                .get(),
            1,
            "the kind must be recorded before `?` folds it into an eyre::Report"
        );
    }
}
