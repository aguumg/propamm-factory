//! A local JSON-RPC endpoint for driving the quote loop in tests without a chain. It
//! answers the handful of methods `quoting::drive` makes, records when each request
//! arrived, and can delay any method's response — which is what a test of *when* the
//! loop makes its calls, rather than what it makes of the answers, needs.
//!
//! Hand-rolled HTTP/1.1 over a `TcpListener` rather than a server crate: the client is
//! `reqwest`, which sends one `Content-Length`-framed POST per request over a kept-alive
//! connection, and that is all this has to understand.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use ethrex_common::{Address, H256, U256, types::BlockHeader};
use ethrex_l2_common::calldata::Value;
use ethrex_l2_sdk::calldata::encode_calldata;
use ethrex_rpc::types::block::{BlockBodyWrapper, OnlyHashesBlockBody, RpcBlock};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::watch,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Mainnet block time, the spacing this chain stamps its blocks at.
const BLOCK_TIME_SECS: u64 = 12;

pub(crate) struct MockRpc {
    /// The HTTP JSON-RPC endpoint.
    pub url: String,
    /// The WebSocket JSON-RPC endpoint: answers `eth_subscribe` for `newHeads` and
    /// pushes a notification for every `set_head` after that, the way a node does.
    pub ws_url: String,
    state: Arc<Mutex<State>>,
    /// Raised by `set_head`; each subscription forwards it.
    heads: watch::Sender<u64>,
}

struct State {
    /// What `eth_blockNumber` answers, and the block `eth_getBlockByNumber` describes.
    head: u64,
    /// Block `n` is stamped `genesis + BLOCK_TIME_SECS * n`.
    genesis: u64,
    nonce: u64,
    /// The timestamp `eth_call` (the registry's `getState`) reports for any lane.
    stored_ts: u32,
    /// What `vaultFor(address,address)` answers for any pair.
    vault: Address,
    /// What `balanceOf(address)` answers per holder, for any token. Unset holders read zero.
    balances: HashMap<Address, U256>,
    /// What an ERC-20's `symbol()` answers, per token. Unset tokens revert.
    symbols: HashMap<Address, String>,
    /// What the target's `getPairs()` answers, once set; until then it gets the `getState`
    /// answer every unknown call does, which does not decode as a pair list.
    target_pairs: Option<Vec<(Address, Address)>>,
    /// What an ERC-20's `decimals()` answers, per token. Unset tokens answer 18.
    decimals: HashMap<Address, u32>,
    /// Per-method response delay, applied after the request is recorded.
    delays: HashMap<String, Duration>,
    /// Methods to answer with a JSON-RPC error instead of a result. `set_delay` can only
    /// make a call slow, and a caller's failure path is often reached by an error rather
    /// than a timeout — waiting out RPC_TIMEOUT to test one costs the suite three seconds
    /// per retry for something the endpoint can just refuse.
    failing: HashSet<String>,
    /// `eth_call` selectors that revert, with the data each reverts with, for a refusal one
    /// call reaches while every other call (preflight's included) still answers. Answered
    /// the way a node answers a call that reverts: error code 3, "execution reverted".
    reverts: HashMap<Vec<u8>, Vec<u8>>,
    /// The `from` of every `eth_call`, in order.
    call_froms: Vec<Address>,
    /// Every request, by method, with the moment it arrived.
    calls: Vec<(String, Instant)>,
}

impl MockRpc {
    /// Serves a chain whose latest block is `head`, stamped about two seconds ago so
    /// that an update for the next block is due in the near future, the way it is on a
    /// live chain.
    pub(crate) async fn spawn(head: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State {
            head,
            genesis: now - 2 - BLOCK_TIME_SECS * head,
            nonce: 0,
            stored_ts: 0,
            vault: Address::zero(),
            balances: HashMap::new(),
            symbols: HashMap::new(),
            target_pairs: None,
            decimals: HashMap::new(),
            delays: HashMap::new(),
            failing: HashSet::new(),
            reverts: HashMap::new(),
            call_froms: Vec::new(),
            calls: Vec::new(),
        }));
        let served = state.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = served.clone();
                tokio::spawn(async move {
                    let (rd, mut wr) = stream.into_split();
                    let mut rd = BufReader::new(rd);
                    // One connection carries many requests: reqwest keeps it alive.
                    while let Some(request) = read_request(&mut rd).await {
                        let method = request["method"].as_str().unwrap_or("").to_owned();
                        let (delay, result) = {
                            let mut state = state.lock().unwrap();
                            state.calls.push((method.clone(), Instant::now()));
                            (
                                state.delays.get(&method).copied(),
                                state.respond(&method, &request["params"]),
                            )
                        };
                        if let Some(delay) = delay {
                            tokio::time::sleep(delay).await;
                        }
                        let body = match result {
                            Ok(result) => serde_json::json!({
                                "id": request["id"],
                                "jsonrpc": "2.0",
                                "result": result,
                            }),
                            Err(error) => serde_json::json!({
                                "id": request["id"],
                                "jsonrpc": "2.0",
                                "error": error,
                            }),
                        }
                        .to_string();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if wr.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let (heads, _) = watch::channel(head);
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let subscribed = state.clone();
        let pushes = heads.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = ws_listener.accept().await {
                let state = subscribed.clone();
                // Marked seen at accept, like a node's: only heads after subscribing
                // are pushed, never the current one.
                let mut heads = pushes.subscribe();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    let mut subscription: Option<String> = None;
                    loop {
                        tokio::select! {
                            msg = ws.next() => {
                                let Some(Ok(Message::Text(text))) = msg else {
                                    return;
                                };
                                let Ok(request) = serde_json::from_str::<serde_json::Value>(&text) else {
                                    return;
                                };
                                let method = request["method"].as_str().unwrap_or("").to_owned();
                                state.lock().unwrap().calls.push((method.clone(), Instant::now()));
                                let reply = if method == "eth_subscribe" {
                                    subscription = Some("0x1".to_owned());
                                    serde_json::json!({"id": request["id"], "jsonrpc": "2.0", "result": "0x1"})
                                } else {
                                    serde_json::json!({"id": request["id"], "jsonrpc": "2.0", "error": {
                                        "code": -32601, "message": format!("mock ws rpc: unsupported method {method}"), "data": null,
                                    }})
                                };
                                if ws.send(Message::text(reply.to_string())).await.is_err() {
                                    return;
                                }
                            }
                            changed = heads.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                let Some(id) = &subscription else {
                                    continue;
                                };
                                let number = *heads.borrow_and_update();
                                let header = state.lock().unwrap().header(number);
                                let notification = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "eth_subscription",
                                    "params": {"subscription": id, "result": serde_json::to_value(header).unwrap()},
                                });
                                if ws.send(Message::text(notification.to_string())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        Self {
            url: format!("http://{addr}"),
            ws_url: format!("ws://{ws_addr}"),
            state,
            heads,
        }
    }

    pub(crate) fn set_head(&self, head: u64) {
        self.state.lock().unwrap().head = head;
        self.heads.send_replace(head);
    }

    /// What block `number` is stamped.
    pub(crate) fn timestamp_of(&self, number: u64) -> u64 {
        self.state.lock().unwrap().genesis + BLOCK_TIME_SECS * number
    }

    /// Re-anchors the chain so that block `number` is stamped `timestamp`.
    pub(crate) fn set_timestamp_of(&self, number: u64, timestamp: u64) {
        self.state.lock().unwrap().genesis = timestamp - BLOCK_TIME_SECS * number;
    }

    /// Makes `method` answer with a JSON-RPC error until this is called again with `false`.
    pub(crate) fn set_failing(&self, method: &str, failing: bool) {
        let mut state = self.state.lock().unwrap();
        if failing {
            state.failing.insert(method.to_owned());
        } else {
            state.failing.remove(method);
        }
    }

    pub(crate) fn set_delay(&self, method: &str, delay: Duration) {
        self.state
            .lock()
            .unwrap()
            .delays
            .insert(method.to_owned(), delay);
    }

    /// What `vaultFor` answers from now on.
    pub(crate) fn set_vault(&self, vault: Address) {
        self.state.lock().unwrap().vault = vault;
    }

    /// What `balanceOf(holder)` answers on any token from now on.
    pub(crate) fn set_balance(&self, holder: Address, balance: U256) {
        self.state.lock().unwrap().balances.insert(holder, balance);
    }

    /// What `getPairs()` answers from now on, on any contract.
    pub(crate) fn set_target_pairs(&self, pairs: Vec<(Address, Address)>) {
        self.state.lock().unwrap().target_pairs = Some(pairs);
    }

    /// What `symbol()` answers on `token` from now on.
    /// What `decimals()` answers on `token` from now on (18 for any other token).
    pub(crate) fn set_decimals(&self, token: Address, decimals: u32) {
        self.state.lock().unwrap().decimals.insert(token, decimals);
    }

    pub(crate) fn set_symbol(&self, token: Address, symbol: &str) {
        self.state
            .lock()
            .unwrap()
            .symbols
            .insert(token, symbol.to_owned());
    }

    /// Makes every `eth_call` to `signature` (as `transferFrom(address,address,uint256)`)
    /// revert with `data`, on any contract, until the mock is dropped.
    pub(crate) fn set_revert(&self, signature: &str, data: Vec<u8>) {
        let selector = ethrex_common::utils::keccak(signature.as_bytes()).0[..4].to_vec();
        self.state.lock().unwrap().reverts.insert(selector, data);
    }

    /// The `from` of every `eth_call` so far, in order.
    pub(crate) fn call_froms(&self) -> Vec<Address> {
        self.state.lock().unwrap().call_froms.clone()
    }

    /// When each request for `method` arrived, in order.
    pub(crate) fn calls(&self, method: &str) -> Vec<Instant> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, at)| *at)
            .collect()
    }

    /// Waits until at least `n` requests for `method` have arrived. Returns false on
    /// timeout, so a failing assertion can report the count it did reach.
    pub(crate) async fn wait_for_calls(&self, method: &str, n: usize, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            while self.calls(method).len() < n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }
}

impl State {
    /// The header of block `number` on this chain.
    fn header(&self, number: u64) -> BlockHeader {
        BlockHeader {
            number,
            timestamp: self.genesis + BLOCK_TIME_SECS * number,
            base_fee_per_gas: Some(1_000_000_000),
            ..Default::default()
        }
    }

    /// The JSON result for `method`, or the JSON-RPC error object it is answered with: a
    /// revert for an `eth_call` whose selector `set_revert` named, "unsupported method" for
    /// anything [`Self::answer`] does not serve.
    fn respond(
        &mut self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, serde_json::Value> {
        if method == "eth_call" && !self.failing.contains(method) {
            let call = &params[0];
            let from = call["from"].as_str().unwrap_or("0x");
            let from = hex::decode(from.strip_prefix("0x").unwrap_or(from)).unwrap_or_default();
            if from.len() == 20 {
                self.call_froms.push(Address::from_slice(&from));
            }
            let data = call["input"]
                .as_str()
                .or_else(|| call["data"].as_str())
                .unwrap_or("0x");
            let data = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
            if let Some(reverted) = data
                .get(..4)
                .and_then(|selector| self.reverts.get(selector))
            {
                return Err(serde_json::json!({
                    "code": 3,
                    "message": "execution reverted",
                    "data": format!("0x{}", hex::encode(reverted)),
                }));
            }
        }
        self.answer(method, params).ok_or_else(|| {
            serde_json::json!({
                "code": -32601,
                "message": format!("mock rpc: unsupported method {method}"),
                "data": null,
            })
        })
    }

    /// The JSON result for `method`, or None for one this mock does not serve.
    fn answer(&self, method: &str, params: &serde_json::Value) -> Option<serde_json::Value> {
        if self.failing.contains(method) {
            return None;
        }
        Some(match method {
            "eth_chainId" => serde_json::json!("0x1"),
            "eth_blockNumber" => serde_json::json!(format!("{:#x}", self.head)),
            "eth_getBlockByNumber" => serde_json::to_value(RpcBlock {
                hash: H256::from_low_u64_be(self.head),
                size: 0,
                header: self.header(self.head),
                body: BlockBodyWrapper::OnlyHashes(OnlyHashesBlockBody {
                    transactions: Vec::new(),
                    uncles: Vec::new(),
                    withdrawals: Vec::new(),
                }),
            })
            .unwrap(),
            "eth_getTransactionCount" => serde_json::json!(format!("{:#x}", self.nonce)),
            "eth_getBalance" => serde_json::json!(format!("{:#x}", U256::exp10(20))),
            // Any address is a contract here: preflight refuses a target or registry with no
            // code, and a test that runs the whole startup needs both to pass that check.
            "eth_getCode" => serde_json::json!("0x6001600052"),
            // A view call, dispatched on the selector. Served: the registry's isUpdater
            // (always yes) and the PropAMM's pairVaults, which preflight reads; vaultFor and
            // an ERC-20's balanceOf, which the volatile inventory reads; and anything else
            // is the registry's getState — the stored timestamp and an empty slot array —
            // which is what the quote loop reads. All as the bare ABI encoding (no
            // selector) an eth_call returns.
            "eth_call" => {
                // Our own client (`ethrex_rpc`'s `EthClient`) sends the calldata under
                // "input"; some other clients say "data" for the same field, so both are
                // accepted rather than tying this mock to one client's naming.
                let data = params[0]["input"]
                    .as_str()
                    .or_else(|| params[0]["data"].as_str())
                    .unwrap_or("0x");
                let data = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
                if let Some(pairs) = &self.target_pairs
                    && data.starts_with(&selector("getPairs()", 0))
                {
                    // `(address, address)[]`, by hand: the offset to the array, its length,
                    // then the two addresses of each pair, one word apiece.
                    let mut words = vec![
                        H256::from_low_u64_be(32),
                        H256::from_low_u64_be(pairs.len() as u64),
                    ];
                    for (a, b) in pairs {
                        words.push(H256::from(*a));
                        words.push(H256::from(*b));
                    }
                    let encoded: Vec<u8> = words.iter().flat_map(|w| w.0).collect();
                    return Some(serde_json::json!(format!("0x{}", hex::encode(encoded))));
                }
                if data.starts_with(&selector("symbol()", 0)) {
                    let to = params[0]["to"].as_str().unwrap_or("");
                    let to = hex::decode(to.strip_prefix("0x").unwrap_or(to)).unwrap_or_default();
                    if to.len() != 20 {
                        return None;
                    }
                    let symbol = self.symbols.get(&Address::from_slice(&to))?;
                    let encoded = encode_calldata(
                        "r(bytes)",
                        &[Value::Bytes(symbol.as_bytes().to_vec().into())],
                    )
                    .unwrap();
                    return Some(serde_json::json!(format!(
                        "0x{}",
                        hex::encode(&encoded[4..])
                    )));
                }
                // An ERC-20's decimals(): 18 unless `set_decimals` said otherwise. Read by
                // every inventory reader (the volatile model's and a custom pricer's) to
                // scale balances into whole tokens.
                if data.starts_with(&selector("decimals()", 0)) {
                    let to = params[0]["to"].as_str().unwrap_or("");
                    let to = hex::decode(to.strip_prefix("0x").unwrap_or(to)).unwrap_or_default();
                    let decimals = (to.len() == 20)
                        .then(|| self.decimals.get(&Address::from_slice(&to)).copied())
                        .flatten()
                        .unwrap_or(18);
                    let encoded =
                        encode_calldata("r(uint8)", &[Value::Uint(U256::from(decimals))]).unwrap();
                    return Some(serde_json::json!(format!(
                        "0x{}",
                        hex::encode(&encoded[4..])
                    )));
                }
                // pairVaults takes a uint256, which `selector` does not build.
                let pair_vaults =
                    encode_calldata("pairVaults(uint256)", &[Value::Uint(U256::zero())]).unwrap();
                let encoded = if data.starts_with(&selector("isUpdater(address,address)", 2)) {
                    encode_calldata("r(bool)", &[Value::Bool(true)]).unwrap()
                } else if data.starts_with(&pair_vaults[..4])
                    || data.starts_with(&selector("vaultFor(address,address)", 2))
                {
                    encode_calldata("r(address)", &[Value::Address(self.vault)]).unwrap()
                } else if data.starts_with(&selector("balanceOf(address)", 1)) && data.len() >= 36 {
                    // The holder is the last 20 bytes of the one 32-byte argument. A shorter
                    // payload falls through to the getState answer instead of panicking here,
                    // inside the state lock, which would poison it for every later test call.
                    let holder = Address::from_slice(&data[16..36]);
                    let balance = self.balances.get(&holder).copied().unwrap_or_default();
                    encode_calldata("r(uint256)", &[Value::Uint(balance)]).unwrap()
                } else {
                    encode_calldata(
                        "r(uint32,uint256[])",
                        &[
                            Value::Uint(U256::from(self.stored_ts)),
                            Value::Array(Vec::new()),
                        ],
                    )
                    .unwrap()
                };
                serde_json::json!(format!("0x{}", hex::encode(&encoded[4..])))
            }
            _ => return None,
        })
    }
}

/// The four-byte selector of `sig`, which takes `addresses` address arguments. Computed by
/// encoding a call with zero addresses rather than hardcoding a hash, so a typo in the
/// signature fails loudly here instead of silently matching nothing.
fn selector(sig: &str, addresses: usize) -> Vec<u8> {
    let args = vec![Value::Address(Address::zero()); addresses];
    encode_calldata(sig, &args).unwrap()[..4].to_vec()
}

/// Reads one `Content-Length`-framed HTTP request and returns its JSON body, or None
/// once the connection is closed.
async fn read_request<R: tokio::io::AsyncBufRead + Unpin>(rd: &mut R) -> Option<serde_json::Value> {
    let mut content_length = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        if rd.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().ok()?;
        }
    }
    let mut body = vec![0u8; content_length];
    rd.read_exact(&mut body).await.ok()?;
    serde_json::from_slice(&body).ok()
}
