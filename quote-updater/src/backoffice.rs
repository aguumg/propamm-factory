//! A web UI for changing the pusher's configuration without a deploy.
//!
//! Server-rendered HTML with plain `<form>` posts. Each change writes `config.toml` and
//! asks the service to reload it; if the reload is rejected the previous file is written
//! back, so what is on disk always describes what is running.
//!
//! No login: `backoffice_addr` is the host's tailnet address, so reaching this page means
//! being past the tailnet ACL. Binding a routable address is refused, because unlike
//! `/metrics` this page writes. Every change logs the peer that made it.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use askama::Template;
use axum::{
    Form, Json, Router,
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use eyre::{Result, WrapErr, bail, eyre};
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::{sync::oneshot, task::JoinHandle};

use crate::config::{self, BuilderConfig, RawConfig, RawPair, RawSource};
use crate::service::ReloadFailure;
use crate::venue::VenueId;

/// A reload the backoffice, or an `UpdaterHandle`, is asking the service to perform.
/// Unlike SIGHUP it waits for the verdict, because a rejection means putting the
/// operator's file back.
pub(crate) struct ReloadRequest {
    pub(crate) reply: oneshot::Sender<std::result::Result<String, crate::service::ReloadFailure>>,
}

/// What the running pusher is doing with each pair, by `key_env`: for every pair that has
/// a task, the flag its circuit breaker raises when it trips. The service keeps it current
/// as pairs start and stop, so a row can say what is actually running rather than only what
/// the file asks for.
pub type PairStates = Arc<std::sync::Mutex<std::collections::BTreeMap<String, Arc<AtomicBool>>>>;

#[derive(Clone)]
struct Backoffice {
    config_path: PathBuf,
    states: PairStates,
    /// The registered pricing kinds: what the pair form offers, with each kind's fields.
    kinds: Arc<crate::kinds::Kinds>,
    reloads: tokio::sync::mpsc::Sender<ReloadRequest>,
    /// For the venue lookup (`discover.rs`): the node that reads the tokens' `symbol()`, the
    /// client that asks the exchanges, and where each exchange's API is.
    client: ethrex_rpc::clients::eth::EthClient,
    http: reqwest::Client,
    rest: Arc<crate::discover::Rest>,
    /// Market lists already fetched, kept for a while: Binance's alone is megabytes.
    lookups: Arc<crate::discover::Cache>,
    /// Open preview pages, capped at PREVIEW_PAGES. Each page connects to each venue at
    /// most once, so this bounds the venue connections previews can hold at once.
    preview_pages: Arc<tokio::sync::Semaphore>,
    /// Serializes edits. Two at once would each read the file and write it back, and the
    /// second would drop the first's change. Also covers the window where a rejected edit
    /// is being reverted. Shared with the `UpdaterHandle` of a `start`ed run, whose halts
    /// and resumes are edits of the same file.
    edits: Arc<tokio::sync::Mutex<()>>,
}

/// `0.0.0.0:8088` binds every interface, including whatever the host has facing the
/// internet. The easy mistake, and the dangerous one.
pub(crate) fn is_unspecified_bind(addr: SocketAddr) -> bool {
    addr.ip().is_unspecified()
}

/// Binds `addr` and serves the page. Fatal on failure: an operator who believes this is up
/// and whose changes are going nowhere is worse off than one whose service refused to
/// start.
pub async fn bind(
    addr: SocketAddr,
    config_path: PathBuf,
    reloads: tokio::sync::mpsc::Sender<ReloadRequest>,
    client: ethrex_rpc::clients::eth::EthClient,
    states: PairStates,
    edits: Arc<tokio::sync::Mutex<()>>,
    kinds: Arc<crate::kinds::Kinds>,
) -> Result<(SocketAddr, JoinHandle<()>)> {
    bind_with(
        addr,
        config_path,
        reloads,
        client,
        states,
        edits,
        kinds,
        crate::discover::Rest::default(),
    )
    .await
}

/// [`bind`], with the exchanges' APIs somewhere else: a test's mock.
#[allow(clippy::too_many_arguments)] // two call sites (bind and a test), each naming them
async fn bind_with(
    addr: SocketAddr,
    config_path: PathBuf,
    reloads: tokio::sync::mpsc::Sender<ReloadRequest>,
    client: ethrex_rpc::clients::eth::EthClient,
    states: PairStates,
    edits: Arc<tokio::sync::Mutex<()>>,
    kinds: Arc<crate::kinds::Kinds>,
    rest: crate::discover::Rest,
) -> Result<(SocketAddr, JoinHandle<()>)> {
    eyre::ensure!(
        !is_unspecified_bind(addr),
        "backoffice_addr is {addr}, which binds every interface on this host. This page \
         rewrites the pusher's config and has no authentication of its own; bind it to the \
         host's tailnet address instead, where the tailnet ACL is the access control."
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .wrap_err_with(|| format!("failed to bind the backoffice listener on {addr}"))?;
    let local = listener
        .local_addr()
        .wrap_err("bound backoffice listener has no local address")?;

    let state = Backoffice {
        config_path,
        states,
        kinds,
        reloads,
        client,
        rest: Arc::new(rest),
        // A User-Agent, because CoinGecko and Coinbase answer 403 to a request without one.
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent(concat!("quote-updater/", env!("CARGO_PKG_VERSION")))
            .build()
            .wrap_err("http client")?,
        lookups: Arc::new(crate::discover::Cache::default()),
        preview_pages: Arc::new(tokio::sync::Semaphore::new(PREVIEW_PAGES)),
        edits,
    };

    let app = Router::new()
        // A list page, and a page per thing being added or edited. The plainest shape a
        // CRUD can have, and the one a browser gives for free: Back leaves a form, a
        // bookmark reaches one pair, and the list is a list rather than a stack of forms.
        .route("/", get(index))
        .route("/pairs/new", get(new_pair_page))
        .route("/pairs/edit/{key_env}", get(edit_pair_page))
        .route("/pairs/add", post(add_pair))
        .route("/pairs/lookup", get(lookup_pair))
        .route("/pairs/preview", get(preview_socket))
        .route("/static/pair.js", get(pair_script))
        .route("/pairs/edit", post(edit_pair))
        .route("/pairs/remove", post(remove_pair))
        .route("/pairs/halt", post(halt_pair))
        .route("/pairs/resume", post(resume_pair))
        .route("/pairs/halt-all", post(halt_all))
        .route("/settings", get(settings_page))
        .route("/settings/save", post(save_settings))
        .route("/builders/new", get(new_builder_page))
        .route("/builders/add", post(add_builder))
        .route("/builders/remove", post(remove_builder))
        .route("/reload", post(reload_only))
        .with_state(state);

    let task = tokio::spawn(async move {
        // With connect info, so every mutation can be logged against its peer.
        if let Err(err) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            tracing::warn!("backoffice listener stopped: {err}");
        }
    });

    Ok((local, task))
}

// ---------------------------------------------------------------------------
// Applying a change
// ---------------------------------------------------------------------------

/// Reads the config, hands it to `change`, writes it back and reloads, restoring the
/// previous file if the reload is refused. The one place that is done, so no handler can
/// forget the revert.
async fn apply(
    state: &Backoffice,
    peer: SocketAddr,
    what: &str,
    change: impl FnOnce(&mut RawConfig) -> Result<()>,
) -> Outcome {
    let _guard = state.edits.lock().await;
    apply_locked(state, peer, what, change).await
}

/// [`apply`] for a caller that already holds `state.edits`, so it can look at the file and
/// decide what to change under the same lock the change is made under.
async fn apply_locked(
    state: &Backoffice,
    peer: SocketAddr,
    what: &str,
    change: impl FnOnce(&mut RawConfig) -> Result<()>,
) -> Outcome {
    Outcome::from(change_locked(state, peer, what, change).await)
}

/// [`apply_locked`] before it becomes a banner, for a caller that has to tell a partial
/// apply from a rejection.
async fn change_locked(
    state: &Backoffice,
    peer: SocketAddr,
    what: &str,
    change: impl FnOnce(&mut RawConfig) -> Result<()>,
) -> std::result::Result<String, ReloadFailure> {
    // The write, reload and revert live in the service, shared with `UpdaterHandle`.
    crate::service::change_config(
        &state.config_path,
        &state.reloads,
        &format!("backoffice: {peer}"),
        what,
        change,
    )
    .await
}

/// Percent-encoding for the banner message in the redirect's query string.
fn urlencode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// What a handler tells the page to say after a change.
struct Outcome {
    ok: bool,
    message: String,
}

/// A partial apply is not shown as a success: the page would be saying a change is live
/// while a pair still runs its previous config. Its message says the file kept the change.
impl From<std::result::Result<String, ReloadFailure>> for Outcome {
    fn from(result: std::result::Result<String, ReloadFailure>) -> Self {
        match result {
            Ok(message) => Outcome::ok(message),
            Err(failure) => Outcome::failed(failure.line().to_owned()),
        }
    }
}

impl Outcome {
    fn ok(message: String) -> Self {
        Self { ok: true, message }
    }
    fn failed(message: String) -> Self {
        Self { ok: false, message }
    }

    /// Post/redirect/get, so a refresh does not repeat the change.
    fn redirect(self) -> Response {
        let kind = if self.ok { "ok" } else { "err" };
        Redirect::to(&format!("/?{kind}={}", urlencode(&self.message))).into_response()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// The venue table of the pair form: one symbol and one weight per venue the binary
/// knows, named `symbol_<venue>` and `weight_<venue>`. A blank symbol is a venue the pair
/// does not read. Named fields rather than a map because the form decoder does not do
/// repeated keys; every field defaults so a form rendered before a venue was added still
/// posts.
#[derive(Deserialize, Default)]
struct SourcesForm {
    #[serde(default)]
    symbol_binance: String,
    #[serde(default)]
    weight_binance: String,
    #[serde(default)]
    symbol_coinbase: String,
    #[serde(default)]
    weight_coinbase: String,
    #[serde(default)]
    symbol_kraken: String,
    #[serde(default)]
    weight_kraken: String,
    #[serde(default)]
    symbol_okx: String,
    #[serde(default)]
    weight_okx: String,
    #[serde(default)]
    symbol_bybit: String,
    #[serde(default)]
    weight_bybit: String,
    #[serde(default)]
    symbol_kucoin: String,
    #[serde(default)]
    weight_kucoin: String,
    #[serde(default)]
    symbol_bitget: String,
    #[serde(default)]
    weight_bitget: String,
    #[serde(default)]
    symbol_gate: String,
    #[serde(default)]
    weight_gate: String,
    #[serde(default)]
    symbol_mexc: String,
    #[serde(default)]
    weight_mexc: String,
    #[serde(default)]
    min_sources: String,
}

impl SourcesForm {
    /// The row for `venue`, so the list of venues is `VenueId::ALL` and not a second copy
    /// of it here.
    fn row(&self, venue: VenueId) -> (&str, &str) {
        match venue {
            VenueId::Binance => (&self.symbol_binance, &self.weight_binance),
            VenueId::Coinbase => (&self.symbol_coinbase, &self.weight_coinbase),
            VenueId::Kraken => (&self.symbol_kraken, &self.weight_kraken),
            VenueId::Okx => (&self.symbol_okx, &self.weight_okx),
            VenueId::Bybit => (&self.symbol_bybit, &self.weight_bybit),
            VenueId::Kucoin => (&self.symbol_kucoin, &self.weight_kucoin),
            VenueId::Bitget => (&self.symbol_bitget, &self.weight_bitget),
            VenueId::Gate => (&self.symbol_gate, &self.weight_gate),
            VenueId::Mexc => (&self.symbol_mexc, &self.weight_mexc),
        }
    }

    /// The venues with a symbol filled in, in `VenueId::ALL` order.
    fn sources(&self) -> Vec<RawSource> {
        VenueId::ALL
            .iter()
            .filter_map(|venue| {
                let (symbol, weight) = self.row(*venue);
                optional(symbol).map(|symbol| RawSource {
                    venue: venue.as_str().to_owned(),
                    symbol,
                    weight: optional(weight),
                })
            })
            .collect()
    }
}

/// Writes the venue table into a pair the way the file spells it: one Binance market with
/// no weight and no minimum is `symbol = "..."`, the form every existing file uses, so a
/// pair saved from this page without touching the table reads back unchanged; anything
/// else is `sources`. `legacy_symbol` is the old single `symbol` input, still posted by a
/// form a browser held open from before the table existed.
fn apply_sources(pair: &mut RawPair, sources: &SourcesForm, legacy_symbol: &str) -> Result<()> {
    for venue in VenueId::ALL {
        let (symbol, weight) = sources.row(*venue);
        if optional(symbol).is_none() && optional(weight).is_some() {
            return Err(eyre!(
                "{venue} has a weight but no symbol; fill in the market to use it, or clear \
                 the weight"
            ));
        }
    }
    let mut rows = sources.sources();
    if rows.is_empty()
        && let Some(symbol) = optional(legacy_symbol)
    {
        rows.push(RawSource {
            venue: VenueId::Binance.as_str().to_owned(),
            symbol,
            weight: None,
        });
    }
    let min_sources = optional(&sources.min_sources);
    match rows.as_slice() {
        [only]
            if only.venue == VenueId::Binance.as_str()
                && only.weight.is_none()
                && min_sources.is_none() =>
        {
            pair.symbol = Some(only.symbol.clone());
            pair.sources = Vec::new();
            pair.min_sources = None;
        }
        _ => {
            pair.symbol = None;
            pair.sources = rows;
            pair.min_sources = min_sources;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct AddPair {
    token0: String,
    token1: String,
    key_env: String,
    /// The single Binance market, from a form rendered before the venue table existed.
    #[serde(default)]
    symbol: String,
    #[serde(flatten)]
    sources: SourcesForm,
    min_mid: String,
    max_mid: String,
    max_deviation: String,
    // Defaulted like every field added after this struct's first version: a form a
    // browser is still holding open from before a pusher restart added this field does
    // not post it at all, and that must still save rather than 422.
    #[serde(default)]
    max_deviation_window: String,
    #[serde(default)]
    max_deviation_window_blocks: String,
    /// The late-venue key. `None` is a form rendered before the field existed, which
    /// keeps whatever the file sets; a blank field clears the key.
    #[serde(default)]
    max_source_lag_ms: Option<String>,
    #[serde(default)]
    allow_symbol_mismatch: Option<String>,
    /// The pricing kind the operator chose. Every kind's section is posted whichever is
    /// showing, so this decides which kind's fields are kept. Defaulted so a form rendered
    /// before it existed still posts; blank keeps the pair's stanza.
    #[serde(default)]
    pricing: String,
    /// Every other posted input: the chosen kind's fields, named `pricing_<kind>_<field>`
    /// by the form, which [`pricing_stanza`] reads for the chosen kind alone.
    #[serde(flatten)]
    kind_fields: std::collections::HashMap<String, String>,
}

/// A blank field means the key is absent, not set to `""`, which every one of them rejects.
fn optional(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// A field a form may not post at all: absent keeps `key` as it is (a page rendered before
/// the field existed), present sets it as [`optional`] reads it, blank clearing it.
fn set_if_posted(key: &mut Option<String>, posted: Option<&str>) {
    if let Some(value) = posted {
        *key = optional(value);
    }
}

/// The `[pairs.pricing]` stanza a form describes: the chosen kind, and the values it posted
/// for that kind's declared fields, each written as the string it was typed as and left out
/// when blank. Every kind's section is posted whichever is showing, so only the chosen kind's
/// inputs are read. A kind with no declared fields keeps the pair's stanza when it already
/// prices it (the page cannot edit one, and says so) and otherwise gets a stanza naming it
/// alone, for the operator to fill in the file. A blank choice (a form from before the
/// choice existed) keeps the stanza as it is.
fn pricing_stanza(
    kinds: &crate::kinds::Kinds,
    kind: &str,
    existing: Option<&toml::Table>,
    posted: &std::collections::HashMap<String, String>,
) -> Result<toml::Table> {
    let kind = kind.trim();
    if kind.is_empty() {
        return existing
            .cloned()
            .ok_or_else(|| eyre!("choose a pricing kind"));
    }
    let fields = kinds.pricer_fields(kind).ok_or_else(|| {
        let registered = kinds.pricer_kinds();
        if registered.is_empty() {
            eyre!("unknown pricing kind {kind:?}; this binary registers none")
        } else {
            eyre!(
                "unknown pricing kind {kind:?}; registered: {}",
                registered.join(", ")
            )
        }
    })?;
    let existing_kind = existing
        .and_then(|t| t.get("kind"))
        .and_then(|k| k.as_str());
    if fields.is_empty() && existing_kind == Some(kind) {
        return Ok(existing.cloned().unwrap_or_default());
    }
    let mut table = toml::Table::new();
    table.insert("kind".to_owned(), toml::Value::String(kind.to_owned()));
    for field in fields {
        let key = format!("pricing_{kind}_{}", field.name);
        if let Some(value) = posted.get(&key).and_then(|v| optional(v)) {
            let value = if field.number {
                match (value.parse::<i64>(), value.parse::<f64>()) {
                    (Ok(int), _) => toml::Value::Integer(int),
                    (_, Ok(float)) if float.is_finite() => toml::Value::Float(float),
                    _ => bail!("`{}` must be a number, got {value:?}", field.name),
                }
            } else {
                toml::Value::String(value)
            };
            table.insert(field.name.to_owned(), value);
        }
    }
    Ok(table)
}

async fn add_pair(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<AddPair>,
) -> Response {
    let what = format!("added pair {}/{}", form.token0, form.token1);
    let kinds = Arc::clone(&state.kinds);
    apply(&state, peer, &what, move |config| {
        let pair = pair_from_form(&form, &kinds)?;
        config::check_source_keys(&pair)?;
        if config
            .pairs
            .iter()
            .any(|existing| existing.key_env == pair.key_env)
        {
            return Err(eyre!(
                "a pair already uses key_env {:?}; every pair needs its own key, since two \
                 pairs on one address share a nonce and only one of them could land per \
                 block",
                pair.key_env
            ));
        }
        config.pairs.push(pair);
        Ok(())
    })
    .await
    .redirect()
}

/// The stanza an add form describes, every rule applied.
fn pair_from_form(form: &AddPair, kinds: &crate::kinds::Kinds) -> Result<RawPair> {
    let mut pair = RawPair {
        tokens: vec![form.token0.trim().to_owned(), form.token1.trim().to_owned()],
        key_env: form.key_env.trim().to_owned(),
        symbol: None,
        sources: Vec::new(),
        min_sources: None,
        max_source_lag_ms: form.max_source_lag_ms.as_deref().and_then(optional),
        min_mid: optional(&form.min_mid),
        max_mid: optional(&form.max_mid),
        max_deviation: optional(&form.max_deviation),
        max_deviation_window: optional(&form.max_deviation_window),
        max_deviation_window_blocks: optional(&form.max_deviation_window_blocks),
        allow_symbol_mismatch: form.allow_symbol_mismatch.is_some(),
        halted: false,
        halt_reason: None,
        pricing: pricing_stanza(kinds, &form.pricing, None, &form.kind_fields)?,
        guards: Vec::new(),
    };
    apply_sources(&mut pair, &form.sources, &form.symbol)?;
    Ok(pair)
}

/// A hash of the pair form's script, put in the URL the page loads it from. Cloudflare,
/// in front of the backoffice, keeps a `.js` file for hours whatever the server says, so
/// under one fixed URL a deploy's new script did not reach the browser; a URL that changes
/// with the script's content is fetched fresh the first time.
fn pair_script_version() -> &'static str {
    static VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        include_str!("../templates/pair.js").hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    });
    &VERSION
}

/// The pair form's script, served from the binary like the templates are. `no-cache`
/// because it changes with every deploy and its URL does not: without it a browser keeps
/// running the previous deploy's script against the new server.
async fn pair_script() -> Response {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!("../templates/pair.js"),
    )
        .into_response()
}

#[derive(Deserialize)]
struct Tokens {
    token0: String,
    token1: String,
}

/// What the pair form asks as soon as both addresses are typed (see `templates/pair.js`):
/// which venues list the pair and how (`discover.rs`), as JSON, one entry per venue the
/// pusher knows so the table can be drawn whole. Saves nothing.
async fn lookup_pair(State(state): State<Backoffice>, Query(tokens): Query<Tokens>) -> Response {
    // Checked before anything is asked: the addresses are read on chain.
    let (Ok(base), Ok(quote)) = (
        config::parse_address(tokens.token0.trim()),
        config::parse_address(tokens.token1.trim()),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "token addresses are not valid"})),
        )
            .into_response();
    };
    let (base_symbol, quote_symbol) = tokio::join!(
        crate::preflight::token_symbol(&state.client, base),
        crate::preflight::token_symbol(&state.client, quote),
    );
    let found = match (base_symbol, quote_symbol) {
        (Some(base_symbol), Some(quote_symbol)) => {
            crate::discover::discover(
                &state.http,
                &state.lookups,
                &state.rest,
                &base_symbol,
                &quote_symbol,
            )
            .await
        }
        _ => Err(eyre!(
            "could not read symbol() from both tokens on chain, so their names are unknown"
        )),
    };
    let (found, error) = match found {
        Ok(found) => (found, None),
        Err(err) => (
            crate::discover::Discovery::default(),
            Some(format!("{err:#}")),
        ),
    };
    let venues: Vec<serde_json::Value> = VenueId::ALL
        .iter()
        .map(|venue| {
            let row = found.row(*venue);
            serde_json::json!({
                "venue": venue.as_str(),
                "listed": row.is_some(),
                "unreachable": found.unreachable.contains(venue),
                "symbol": row.map(|r| r.symbol.clone()),
                "example": venue.expected_symbol("ETH", "USDC"),
                "weight": row.map(|r| r.weight.clone()),
                "last": row.and_then(|r| r.last),
                "volume_usd": row.and_then(|r| r.volume_usd),
                "note": row.map(|r| r.note.clone()).unwrap_or_else(|| match venue {
                    VenueId::Mexc => "polled every second".to_owned(),
                    _ => String::new(),
                }),
                "stand_in": row.is_some_and(|r| r.note.starts_with("quoted in")),
            })
        })
        .collect();
    Json(serde_json::json!({
        "ok": error.is_none(),
        "error": error,
        "swapped": found.swapped,
        "base": found.base_symbol,
        "quote": found.quote_symbol,
        "notes": found.notes,
        "venues": venues,
    }))
    .into_response()
}

/// A message from the page: tick or re-weight a venue, or untick it.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PreviewCommand {
    Set {
        venue: String,
        symbol: String,
        weight: String,
    },
    Remove {
        venue: String,
    },
}

/// Most preview pages open at once; a page past it is refused. With one connection per
/// venue per page, previews hold at most `PREVIEW_PAGES × venues` venue connections.
const PREVIEW_PAGES: usize = 4;

/// Least time between two connections to the same venue from one page, whatever the page
/// sends: a symbol edited letter by letter, or a venue ticked and unticked over and over,
/// connects at most this often, with whatever the page asked for last.
const PREVIEW_REDIAL_GAP: std::time::Duration = std::time::Duration::from_secs(2);

/// One ticked venue of a preview page. `rx` is `None` until the venue is connected to
/// `source.symbol`: just ticked, or its symbol just changed, and waiting out the redial gap.
struct PreviewVenue {
    source: config::Source,
    rx: Option<tokio::sync::watch::Receiver<Option<crate::feed::PriceSample>>>,
}

/// The pair form's live prices, over one websocket per open page. The page sends which
/// venues are ticked, one at a time as they change, and the server connects to exactly
/// those, with the pusher's own feed code: ticking a venue connects only it, unticking
/// disconnects only it, and a new weight reconnects nothing. Twice a second the server
/// sends each venue's mid and the weighted composite, computed the way the composite that
/// would publish computes it. Closing the page closes every venue it opened.
async fn preview_socket(
    State(state): State<Backoffice>,
    Query(tokens): Query<Tokens>,
    headers: axum::http::HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    let bad = |status: StatusCode, msg: &str| {
        (status, Json(serde_json::json!({"error": msg}))).into_response()
    };
    if !same_origin(&headers) {
        return bad(
            StatusCode::FORBIDDEN,
            "the preview only opens from the backoffice's own page",
        );
    }
    let config = match read_config(&state.config_path) {
        Ok(config) => config,
        Err(response) => return *response,
    };
    let (Ok(base), Ok(quote)) = (
        config::parse_address(tokens.token0.trim()),
        config::parse_address(tokens.token1.trim()),
    ) else {
        return bad(StatusCode::BAD_REQUEST, "token addresses are not valid");
    };
    let endpoints = match crate::venue::Endpoints::resolve(
        config.settings.binance_ws.as_deref(),
        &config.settings.endpoints,
    ) {
        Ok(endpoints) => endpoints,
        Err(err) => return bad(StatusCode::BAD_REQUEST, &format!("{err:#}")),
    };
    let Ok(page) = Arc::clone(&state.preview_pages).try_acquire_owned() else {
        return bad(
            StatusCode::TOO_MANY_REQUESTS,
            "too many preview pages are open; close one and reload",
        );
    };
    // The composite prices the sorted lane; a declared base that sorts second is inverted,
    // and the preview turns it back so the numbers read as the venues quote them.
    let inverted = base > quote;
    ws.on_upgrade(move |socket| async move {
        preview_session(socket, endpoints, inverted).await;
        drop(page);
    })
}

/// Whether the websocket was opened by a page served from this same host. A browser
/// sends the login cookie with a websocket opened from any site, and unlike a fetch, the
/// browser does not stop another site's script from reading the answer, so without this
/// any page a logged-in operator visits could open the preview in their name. The browser
/// always sets `Origin` on a websocket and a script cannot change it; Caddy passes both it
/// and `Host` through unchanged.
fn same_origin(headers: &axum::http::HeaderMap) -> bool {
    let (Some(origin), Some(host)) = (
        headers.get(axum::http::header::ORIGIN),
        headers.get(axum::http::header::HOST),
    ) else {
        return false;
    };
    let (Ok(origin), Ok(host)) = (origin.to_str(), host.to_str()) else {
        return false;
    };
    url::Url::parse(origin).is_ok_and(|url| {
        let origin_host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        origin_host.eq_ignore_ascii_case(host)
    })
}

async fn preview_session(
    mut socket: axum::extract::ws::WebSocket,
    endpoints: crate::venue::Endpoints,
    inverted: bool,
) {
    use axum::extract::ws::Message;

    let Ok(metrics) = crate::metrics::Metrics::new() else {
        return;
    };
    let adopted = crate::feed::Adoption::new();
    adopted.adopt();
    let mut venues: std::collections::BTreeMap<VenueId, PreviewVenue> =
        std::collections::BTreeMap::new();
    // When each venue was last connected, kept after it is unticked so that ticking it
    // again does not skip the gap.
    let mut dialed: std::collections::BTreeMap<VenueId, std::time::Instant> =
        std::collections::BTreeMap::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let text = match incoming {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => continue,
                };
                if let Err(err) = apply_preview_command(&mut venues, &text) {
                    let reply = serde_json::json!({"error": format!("{err:#}")}).to_string();
                    if socket.send(Message::Text(reply.into())).await.is_err() {
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                let now = std::time::Instant::now();
                for (venue, state) in &mut venues {
                    let due = dialed
                        .get(venue)
                        .is_none_or(|at| now.duration_since(*at) >= PREVIEW_REDIAL_GAP);
                    if state.rx.is_some() || !due {
                        continue;
                    }
                    state.rx = Some(crate::feed::spawn_feed(
                        endpoints.http(),
                        *venue,
                        endpoints.for_venue(*venue),
                        &state.source.symbol,
                        PREVIEW_DECIMALS,
                        inverted,
                        crate::feed::Gated::new(
                            metrics.for_venue("preview", venue.as_str()),
                            adopted.clone(),
                        ),
                        None,
                        Default::default(),
                    ));
                    dialed.insert(*venue, now);
                }
                let body = preview_snapshot(&venues, inverted);
                if socket.send(Message::Text(body.to_string().into())).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Applies one command from the page. A new weight changes only the average; a new symbol
/// drops that venue's connection at once and leaves it for the next tick to redial.
fn apply_preview_command(
    venues: &mut std::collections::BTreeMap<VenueId, PreviewVenue>,
    text: &str,
) -> Result<()> {
    match serde_json::from_str::<PreviewCommand>(text).wrap_err("not a preview command")? {
        PreviewCommand::Set {
            venue,
            symbol,
            weight,
        } => {
            let venue: VenueId = venue.parse()?;
            let symbol = symbol.trim().to_owned();
            // The symbol goes into the venue's URL or subscribe message, so only the
            // characters market names are spelled with.
            eyre::ensure!(
                !symbol.is_empty()
                    && symbol.len() <= 40
                    && symbol
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "-_/.".contains(c)),
                "{venue}: {symbol:?} is not a market name"
            );
            let weight = crate::feed::parse_decimal_scaled(
                if weight.trim().is_empty() {
                    "1"
                } else {
                    weight.trim()
                },
                config::WEIGHT_DECIMALS,
            )
            .ok()
            .filter(|w| !w.is_zero())
            .ok_or_else(|| eyre!("{venue}: weight must be a positive number"))?;
            // The same ceiling saving applies: above it `weight × mid` can overflow, and the
            // composite would read as "waiting for a fresh venue" with every venue fresh.
            eyre::ensure!(
                weight
                    <= ethrex_common::U256::from(config::MAX_WEIGHT)
                        * ethrex_common::U256::exp10(config::WEIGHT_DECIMALS as usize),
                "{venue}: weight is above {}; weights are relative, so scale them down",
                config::MAX_WEIGHT
            );
            match venues.get_mut(&venue) {
                Some(state) => {
                    state.source.weight = weight;
                    if symbol != state.source.symbol {
                        // Dropping the receiver is what closes the old connection.
                        state.source.symbol = symbol;
                        state.rx = None;
                    }
                }
                None => {
                    venues.insert(
                        venue,
                        PreviewVenue {
                            source: config::Source {
                                venue,
                                symbol,
                                weight,
                            },
                            rx: None,
                        },
                    );
                }
            }
        }
        PreviewCommand::Remove { venue } => {
            // Dropping the receiver is what closes that venue's connection.
            venues.remove(&venue.parse::<VenueId>()?);
        }
    }
    Ok(())
}

/// What the page is sent twice a second: each ticked venue's mid, in the venue's own
/// orientation, and the composite of the fresh ones, averaged the way the pusher averages
/// the number it publishes.
fn preview_snapshot(
    venues: &std::collections::BTreeMap<VenueId, PreviewVenue>,
    inverted: bool,
) -> serde_json::Value {
    let now = std::time::Instant::now();
    let wall = std::time::SystemTime::now();
    let market = |sample: Option<crate::feed::PriceSample>| -> Option<f64> {
        let sample = sample?;
        let mid = crate::feed::scaled_to_f64(sample.mid, PREVIEW_DECIMALS);
        if mid <= 0.0 {
            return None;
        }
        Some(if inverted { 1.0 / mid } else { mid })
    };
    let mut fresh = Vec::new();
    let rows: Vec<serde_json::Value> = venues
        .values()
        .map(|state| {
            let sample = state.rx.as_ref().and_then(|rx| *rx.borrow());
            let is_fresh =
                sample.is_some_and(|s| s.age_at(now, wall) <= crate::update::MAX_PRICE_AGE);
            if let (true, Some(sample)) = (is_fresh, sample) {
                fresh.push((&state.source, sample));
            }
            serde_json::json!({
                "venue": state.source.venue.as_str(),
                "symbol": state.source.symbol,
                "mid": market(sample),
                "age_ms": sample.map(|s| s.age_at(now, wall).as_millis() as u64),
                "fresh": is_fresh,
            })
        })
        .collect();
    let composite =
        crate::composite::weighted_average(&fresh).map(|(delta, mid)| crate::feed::PriceSample {
            delta,
            mid,
            at: now,
            wall,
        });
    serde_json::json!({
        "composite": market(composite),
        "fresh": fresh.len(),
        "venues": rows,
    })
}

/// The preview's price scale. Mids are displayed, never published, so this only has to be
/// fine enough to read; 18 is what production uses.
const PREVIEW_DECIMALS: u32 = 18;

#[derive(Deserialize)]
struct SaveSettings {
    mode: String,
    rpc_url: String,
    rpc_ws_url: String,
    registry: String,
    price_decimals: String,
    /// The old single Binance endpoint input, from a form rendered before the venue table.
    #[serde(default)]
    binance_ws: String,
    #[serde(flatten)]
    endpoints: EndpointsForm,
    interval: String,
    requote_ms: String,
    metrics_addr: String,
    backoffice_addr: String,
    #[serde(default)]
    disable_cross_region: Option<String>,
}

/// The venue table of the settings form: one endpoint override per venue,
/// `endpoint_<venue>`, blank for the default. Named fields for the same reason as
/// [`SourcesForm`].
#[derive(Deserialize, Default)]
struct EndpointsForm {
    #[serde(default)]
    endpoint_binance: String,
    #[serde(default)]
    endpoint_coinbase: String,
    #[serde(default)]
    endpoint_kraken: String,
    #[serde(default)]
    endpoint_okx: String,
    #[serde(default)]
    endpoint_bybit: String,
    #[serde(default)]
    endpoint_kucoin: String,
    #[serde(default)]
    endpoint_bitget: String,
    #[serde(default)]
    endpoint_gate: String,
    #[serde(default)]
    endpoint_mexc: String,
}

impl EndpointsForm {
    fn row(&self, venue: VenueId) -> &str {
        match venue {
            VenueId::Binance => &self.endpoint_binance,
            VenueId::Coinbase => &self.endpoint_coinbase,
            VenueId::Kraken => &self.endpoint_kraken,
            VenueId::Okx => &self.endpoint_okx,
            VenueId::Bybit => &self.endpoint_bybit,
            VenueId::Kucoin => &self.endpoint_kucoin,
            VenueId::Bitget => &self.endpoint_bitget,
            VenueId::Gate => &self.endpoint_gate,
            VenueId::Mexc => &self.endpoint_mexc,
        }
    }
}

fn optional_number<T: std::str::FromStr>(name: &str, value: &str) -> Result<Option<T>> {
    match optional(value) {
        None => Ok(None),
        Some(text) => text
            .parse()
            .map(Some)
            .map_err(|_| eyre!("{name} must be a whole number, got {text:?}")),
    }
}

/// Writes the `[settings]` table and says a restart is pending. These are read once at
/// startup, so a reload would be refused, and refusing it here would revert a change the
/// operator does want.
async fn save_settings(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<SaveSettings>,
) -> Response {
    let _guard = state.edits.lock().await;

    let mut config = match config::load_raw(&state.config_path) {
        Ok(config) => config,
        Err(err) => return Outcome::failed(format!("{err:#}")).redirect(),
    };

    let settings = match build_settings(&form) {
        Ok(settings) => settings,
        Err(err) => return Outcome::failed(format!("{err:#}")).redirect(),
    };
    if settings == config.settings {
        return Outcome::ok("settings unchanged".to_owned()).redirect();
    }
    let changed = config.settings.changed_keys(&settings).join(", ");
    config.settings = settings;

    if let Err(err) = config::write_raw_atomically(&state.config_path, &config) {
        return Outcome::failed(format!("{err:#}")).redirect();
    }
    tracing::info!("backoffice: {peer} changed [settings] {changed}; restart pending");
    Outcome::ok(format!(
        "saved {changed}. These are read once at startup, so {} to apply them.",
        crate::hints::restart_instruction()
    ))
    .redirect()
}

/// Applies every rule startup would, now rather than at the next start: a bad value
/// written here is a service that fails to come back from a restart hours later, with
/// nobody at this page. Each check below has a twin that startup runs, named beside it:
/// `bind` is this file's, `run` is `pusher::run`, the rest are in `cli.rs`. A value that
/// passes here and fails there is a bug in this function.
fn build_settings(form: &SaveSettings) -> Result<config::FileSettings> {
    let mode = optional(&form.mode);
    if let Some(mode) = &mode {
        eyre::ensure!(
            mode == "builder" || mode == "node",
            "mode must be \"builder\" or \"node\", got {mode:?}"
        );
    }
    if let Some(registry) = optional(&form.registry) {
        config::parse_address(&registry).wrap_err("registry")?;
    }
    for (name, value) in [
        ("metrics_addr", optional(&form.metrics_addr)),
        ("backoffice_addr", optional(&form.backoffice_addr)),
    ] {
        if let Some(addr) = value {
            addr.parse::<SocketAddr>()
                .map_err(|_| eyre!("{name} must be host:port, got {addr:?}"))?;
        }
    }
    // `bind` refuses this at startup. Here it would be saved and only found at the restart.
    if let Some(addr) = optional(&form.backoffice_addr) {
        let parsed: SocketAddr = addr.parse().expect("checked above");
        eyre::ensure!(
            !is_unspecified_bind(parsed),
            "backoffice_addr {addr:?} binds every interface, including any facing the \
             internet; use this host's tailnet address"
        );
    }
    // `refuse_backoffice_in_node_mode`: node mode has no reload for this page to drive.
    eyre::ensure!(
        mode.as_deref() != Some("node") || optional(&form.backoffice_addr).is_none(),
        "mode \"node\" cannot run with a backoffice_addr: node mode has no config reload \
         for the page to drive. Clear backoffice_addr, or keep mode \"builder\""
    );
    // `apply_file_settings`: the arithmetic downstream overflows past this.
    let price_decimals = optional_number("price_decimals", &form.price_decimals)?;
    if let Some(decimals) = price_decimals {
        eyre::ensure!(
            decimals <= crate::MAX_PRICE_DECIMALS,
            "price_decimals is {decimals}; the maximum is {}",
            crate::MAX_PRICE_DECIMALS
        );
    }
    // `run`: `interval must be at least 1 second`.
    let interval = optional_number("interval", &form.interval)?;
    eyre::ensure!(interval != Some(0), "interval must be at least 1 second");
    // `run` parses these before anything connects. `Url::parse` accepts anything with a
    // scheme, so the two websocket endpoints are also held to ws:// or wss://, as startup
    // holds rpc_ws_url.
    if let Some(url) = optional(&form.rpc_url) {
        url::Url::parse(&url).map_err(|err| eyre!("rpc_url {url:?} is not a URL: {err}"))?;
    }
    // Every venue's endpoint too, held to the scheme its transport needs: a WebSocket
    // venue to ws:// or wss://, a polled one (and KuCoin, whose socket URL is fetched over
    // HTTP) to http:// or https://. The old `binance_ws` input, if a stale form still posts
    // it, is the Binance row.
    let mut endpoints = std::collections::BTreeMap::new();
    for venue in VenueId::ALL {
        let value = match (venue, optional(form.endpoints.row(*venue))) {
            (VenueId::Binance, None) => optional(&form.binance_ws),
            (_, value) => value,
        };
        let Some(url) = value else { continue };
        let parsed = url::Url::parse(&url)
            .map_err(|err| eyre!("{venue} endpoint {url:?} is not a URL: {err}"))?;
        let streamed = !matches!(venue, VenueId::Kucoin | VenueId::Mexc);
        let schemes: &[&str] = if streamed {
            &["ws", "wss"]
        } else {
            &["http", "https"]
        };
        eyre::ensure!(
            schemes.contains(&parsed.scheme()),
            "{venue} endpoint must be {}, got {}://",
            schemes.join(":// or "),
            parsed.scheme()
        );
        endpoints.insert(venue.as_str().to_owned(), url);
    }
    for (name, value) in [("rpc_ws_url", optional(&form.rpc_ws_url))] {
        if let Some(url) = value {
            let parsed =
                url::Url::parse(&url).map_err(|err| eyre!("{name} {url:?} is not a URL: {err}"))?;
            eyre::ensure!(
                parsed.scheme() == "ws" || parsed.scheme() == "wss",
                "{name} must be a ws:// or wss:// endpoint, got {}://",
                parsed.scheme()
            );
        }
    }
    Ok(config::FileSettings {
        mode,
        rpc_url: optional(&form.rpc_url),
        rpc_ws_url: optional(&form.rpc_ws_url),
        registry: optional(&form.registry),
        price_decimals,
        // Superseded by the Binance row of `endpoints`, so a save moves the value there and
        // the file has one place that names the Binance endpoint.
        binance_ws: None,
        endpoints,
        interval,
        requote_ms: optional_number("requote_ms", &form.requote_ms)?,
        metrics_addr: optional(&form.metrics_addr),
        disable_cross_region: form.disable_cross_region.is_some().then_some(true),
        backoffice_addr: optional(&form.backoffice_addr),
    })
}

#[derive(Deserialize)]
struct EditPair {
    key_env: String,
    /// See [`AddPair::symbol`].
    #[serde(default)]
    symbol: String,
    #[serde(flatten)]
    sources: SourcesForm,
    min_mid: String,
    max_mid: String,
    max_deviation: String,
    // Defaulted like every field added after this struct's first version: a form a
    // browser is still holding open from before a pusher restart added this field does
    // not post it at all, and that must still save rather than 422.
    #[serde(default)]
    max_deviation_window: String,
    #[serde(default)]
    max_deviation_window_blocks: String,
    /// The late-venue key. `None` is a form rendered before the field existed, which
    /// keeps whatever the file sets; a blank field clears the key.
    #[serde(default)]
    max_source_lag_ms: Option<String>,
    #[serde(default)]
    allow_symbol_mismatch: Option<String>,
    /// The pricing kind the operator chose. Every kind's section is posted whichever is
    /// showing, so this decides which kind's fields are kept. Defaulted so a form rendered
    /// before it existed still posts; blank keeps the pair's stanza.
    #[serde(default)]
    pricing: String,
    /// Every other posted input: the chosen kind's fields, named `pricing_<kind>_<field>`
    /// by the form, which [`pricing_stanza`] reads for the chosen kind alone.
    #[serde(flatten)]
    kind_fields: std::collections::HashMap<String, String>,
}

/// Keyed by `key_env`, not by index: the page may have been rendered before someone else
/// added a pair, and an index would then edit the wrong row.
async fn edit_pair(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<EditPair>,
) -> Response {
    let what = format!("edited pair {}", form.key_env);
    let kinds = Arc::clone(&state.kinds);
    let form = Arc::new(form);
    let result = {
        let (form, kinds) = (Arc::clone(&form), Arc::clone(&kinds));
        let _guard = state.edits.lock().await;
        change_locked(&state, peer, &what, move |config| {
            let pair = config
                .pairs
                .iter_mut()
                .find(|pair| pair.key_env == form.key_env.trim())
                .ok_or_else(|| {
                    eyre!(
                        "no pair with key_env {:?}; it may have been removed since this page \
                         was loaded",
                        form.key_env
                    )
                })?;
            edit_from_form(pair, &form, &kinds)?;
            config::check_source_keys(pair)
        })
        .await
    };
    let outcome = match result {
        Ok(message) => return Outcome::ok(message).redirect(),
        // Written and applied to some lanes: not a rejection, and not the form again, which
        // would read as one. The list, saying it was saved and what the reload could not do.
        Err(ReloadFailure::Partial(line)) => {
            return Outcome::failed(format!("Saved; the reload applied in part: {line}"))
                .redirect();
        }
        Err(ReloadFailure::Rejected(line)) => Outcome::failed(line),
    };
    // Refused: the same form again, as it was posted and with the reason above it, so the
    // field that made it invalid (an age of zero, say) can be fixed or cleared in the same
    // form rather than lost on the way back to the list.
    let Ok(config) = config::load_raw(&state.config_path) else {
        return outcome.redirect();
    };
    match config
        .pairs
        .iter()
        .find(|pair| pair.key_env == form.key_env.trim())
    {
        Some(pair) => {
            let mut posted = pair.clone();
            // Best effort: what could be applied is shown, the rest as the file has it.
            let _ = edit_from_form(&mut posted, &form, &kinds);
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Html(render_pair_form_with(
                    Some(&posted),
                    &kinds,
                    Some(&outcome.message),
                )),
            )
                .into_response()
        }
        None => outcome.redirect(),
    }
}

/// An edit form's changes to `pair`, without the checks the reload makes.
fn edit_from_form(pair: &mut RawPair, form: &EditPair, kinds: &crate::kinds::Kinds) -> Result<()> {
    apply_sources(pair, &form.sources, &form.symbol)?;
    pair.pricing = pricing_stanza(kinds, &form.pricing, Some(&pair.pricing), &form.kind_fields)?;
    pair.min_mid = optional(&form.min_mid);
    pair.max_mid = optional(&form.max_mid);
    pair.max_deviation = optional(&form.max_deviation);
    pair.max_deviation_window = optional(&form.max_deviation_window);
    pair.max_deviation_window_blocks = optional(&form.max_deviation_window_blocks);
    set_if_posted(
        &mut pair.max_source_lag_ms,
        form.max_source_lag_ms.as_deref(),
    );
    pair.allow_symbol_mismatch = form.allow_symbol_mismatch.is_some();
    Ok(())
}

#[derive(Deserialize)]
struct RemovePair {
    key_env: String,
}

async fn remove_pair(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<RemovePair>,
) -> Response {
    let what = format!("removed pair {}", form.key_env);
    apply(&state, peer, &what, move |config| {
        let before = config.pairs.len();
        config
            .pairs
            .retain(|pair| pair.key_env != form.key_env.trim());
        if config.pairs.len() == before {
            return Err(eyre!("no pair with key_env {:?}", form.key_env));
        }
        // Removing the last pair is allowed: a process with a backoffice may run with none,
        // which is the state a fresh host starts in, and this page is how it gets out of.
        Ok(())
    })
    .await
    .redirect()
}

/// Sets `halted` on the pair named by `key_env`, or on every pair when it is `None`.
/// Refuses a change that would do nothing, so the banner never reports a halt that did
/// not happen.
///
/// A halt set here carries no `halt_reason`. One that has a reason was explained by
/// whoever set it: a binary's `UpdaterHandle::halt` always writes one, since that is its
/// kill switch, and an operator may write one by hand. The bulk resume (`key_env` `None`,
/// from "Restart halted pairs") leaves such a halt for that pair's own Resume, so the
/// button pressed to bring back what was halted here cannot also lift a kill switch;
/// [`explained_halts`] is what the banner then says it left. A pair's own Resume clears
/// either kind.
fn set_halted(config: &mut RawConfig, key_env: Option<&str>, halted: bool) -> Result<()> {
    let mut found = false;
    let mut changed = false;
    for pair in &mut config.pairs {
        if key_env.is_some_and(|key| pair.key_env != key.trim()) {
            continue;
        }
        if key_env.is_none() && !halted && is_explained(pair) {
            continue;
        }
        found = true;
        if pair.halted != halted {
            changed = true;
            // The reason goes with the halt it explained, and a halt set here has none: a
            // stale one left beside a cleared flag would make it look explained.
            pair.halt_reason = None;
        }
        pair.halted = halted;
    }
    match key_env {
        Some(key) if !found => Err(eyre!(
            "no pair with key_env {key:?}; it may have been removed since this page was loaded"
        )),
        None if !found => Err(eyre!("there are no pairs to halt")),
        _ if !changed && halted => Err(eyre!("already halted")),
        _ if !changed => Err(eyre!("not halted")),
        _ => Ok(()),
    }
}

/// A halt with a reason beside it: see [`set_halted`] for why the bulk resume skips it.
fn is_explained(pair: &RawPair) -> bool {
    pair.halted && pair.halt_reason.is_some()
}

/// The tail of the banner after a bulk restart, naming each halt it left and its reason;
/// empty when it left none. Said rather than left to be noticed, because the operator who
/// pressed "Restart halted pairs" expects every halted row to come back.
fn explained_halts(config: &RawConfig) -> String {
    let kept: Vec<String> = config
        .pairs
        .iter()
        .filter(|pair| is_explained(pair))
        .map(|pair| {
            format!(
                "{} ({})",
                pair.key_env,
                pair.halt_reason.as_deref().unwrap_or_default()
            )
        })
        .collect();
    if kept.is_empty() {
        return String::new();
    }
    format!(
        ". Left halted, because each was halted with a reason (a binary's kill switch \
         writes one) that only its own Resume clears: {}",
        kept.join(", ")
    )
}

/// Stops a pair quoting and keeps it, with every setting, in the file. The reload treats it
/// as gone, so its quote is withdrawn at every builder exactly as a removal would.
async fn halt_pair(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<RemovePair>,
) -> Response {
    let what = format!("halted pair {}", form.key_env);
    apply(&state, peer, &what, move |config| {
        set_halted(config, Some(&form.key_env), true)
    })
    .await
    .redirect()
}

async fn resume_pair(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<RemovePair>,
) -> Response {
    let what = format!("resumed pair {}", form.key_env);
    apply(&state, peer, &what, move |config| {
        set_halted(config, Some(&form.key_env), false)
    })
    .await
    .redirect()
}

async fn halt_all(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> Response {
    apply(&state, peer, "halted every pair", |config| {
        set_halted(config, None, true)
    })
    .await
    .redirect()
}

#[derive(Deserialize)]
struct AddBuilder {
    name: String,
    endpoint: String,
    api_key: String,
    #[serde(default)]
    disable_cross_region: Option<String>,
}

async fn add_builder(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<AddBuilder>,
) -> Response {
    let builder = BuilderConfig {
        name: form.name.trim().to_owned(),
        endpoint: form.endpoint.trim().to_owned(),
        api_key: form.api_key.trim().to_owned(),
        disable_cross_region: form.disable_cross_region.is_some().then_some(true),
    };
    let what = format!("added builder {}", builder.name);
    apply(&state, peer, &what, move |config| {
        if config
            .builders
            .iter()
            .any(|existing| existing.name == builder.name)
        {
            return Err(eyre!(
                "a builder is already named {:?}; names identify a builder in the logs, \
                     so they must be unique (try {:?}-eu and {:?}-us)",
                builder.name,
                builder.name,
                builder.name
            ));
        }
        config.builders.push(builder);
        config::validate_builders(&config.builders)
    })
    .await
    .redirect()
}

#[derive(Deserialize)]
struct RemoveBuilder {
    name: String,
}

async fn remove_builder(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<RemoveBuilder>,
) -> Response {
    let what = format!("removed builder {}", form.name);
    apply(&state, peer, &what, move |config| {
        let before = config.builders.len();
        config.builders.retain(|b| b.name != form.name.trim());
        if config.builders.len() == before {
            return Err(eyre!("no builder named {:?}", form.name));
        }
        Ok(())
    })
    .await
    .redirect()
}

/// A reload with no edit behind it. Every other button already reloads, so this covers the
/// ways the file and the running pairs diverge on their own: a halted pair (much the
/// commonest), a hand edit over SSH, or a pair whose feed never came up.
///
/// Pairs halted from this page are resumed too: their flag is cleared and the reload
/// starts them, through `apply_locked` so a refused reload puts the flags back. A halt with
/// a reason is left as it is and named in the banner (see [`set_halted`]). The file is
/// read under the edit lock, so two presses at once cannot both see a flag the first one
/// is about to clear, which would have the second report an error for a restart that
/// worked.
async fn reload_only(
    State(state): State<Backoffice>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> Response {
    let _guard = state.edits.lock().await;
    let (any_flagged, kept) = match config::load_raw(&state.config_path) {
        Ok(config) => (
            config
                .pairs
                .iter()
                .any(|pair| pair.halted && !is_explained(pair)),
            explained_halts(&config),
        ),
        Err(_) => (false, String::new()),
    };
    if any_flagged {
        let result = change_locked(&state, peer, "restarted halted pairs", |config| {
            set_halted(config, None, false)
        })
        .await;
        // Not on a rejection: a refused restart changed nothing, so nothing was "left".
        let applied = !matches!(result, Err(ReloadFailure::Rejected(_)));
        let mut outcome = Outcome::from(result);
        if applied {
            outcome.message.push_str(&kept);
        }
        return outcome.redirect();
    }
    match crate::service::request_reload(&state.reloads).await {
        Ok(summary) => {
            tracing::info!("backoffice: {peer} reloaded; {summary}");
            // The service's no-op line names the config path, which is not what someone
            // pressing this button wants to know.
            Outcome::ok(match (summary.contains("nothing to do"), kept.is_empty()) {
                (true, true) => {
                    "nothing was halted; the running pairs already match the config".to_owned()
                }
                (true, false) => format!("nothing was restarted{kept}"),
                (false, _) => format!("{summary}{kept}"),
            })
        }
        Err(ReloadFailure::Partial(line)) => {
            Outcome::failed(format!("the reload applied in part: {line}{kept}"))
        }
        Err(ReloadFailure::Rejected(err)) => {
            Outcome::failed(format!("the reload was rejected: {err}"))
        }
    }
    .redirect()
}

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Banner {
    ok: Option<String>,
    err: Option<String>,
}

/// Reads the config, or renders why it could not be read: an unparseable config is exactly
/// when someone comes looking at this page.
fn read_config(path: &Path) -> std::result::Result<RawConfig, Box<Response>> {
    config::load_raw(path).map_err(|err| {
        Box::new(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(
                    ConfigErrorPage {
                        error: format!("{err:#}"),
                    }
                    .render()
                    .unwrap_or_else(render_failed),
                ),
            )
                .into_response(),
        )
    })
}

async fn index(
    State(state): State<Backoffice>,
    axum::extract::Query(banner): axum::extract::Query<Banner>,
) -> Response {
    match read_config(&state.config_path) {
        Ok(config) => Html(render_index(
            &config,
            &state.config_path,
            &banner,
            &state.states,
            &state.kinds,
        ))
        .into_response(),
        Err(response) => *response,
    }
}

async fn new_pair_page(State(state): State<Backoffice>) -> Response {
    match read_config(&state.config_path) {
        Ok(_) => Html(render_pair_form(None, &state.kinds)).into_response(),
        Err(response) => *response,
    }
}

async fn edit_pair_page(
    State(state): State<Backoffice>,
    axum::extract::Path(key_env): axum::extract::Path<String>,
) -> Response {
    let config = match read_config(&state.config_path) {
        Ok(config) => config,
        Err(response) => return *response,
    };
    match config.pairs.iter().find(|pair| pair.key_env == key_env) {
        Some(pair) => Html(render_pair_form(Some(pair), &state.kinds)).into_response(),
        // A stale link, most likely. Back to the list rather than a bare 404.
        None => Outcome::failed(format!("no pair with key_env {key_env:?}")).redirect(),
    }
}

async fn settings_page(State(state): State<Backoffice>) -> Response {
    match read_config(&state.config_path) {
        Ok(config) => Html(render_settings_form(&config)).into_response(),
        Err(response) => *response,
    }
}

async fn new_builder_page(State(state): State<Backoffice>) -> Response {
    match read_config(&state.config_path) {
        Ok(_) => Html(render_builder_form()).into_response(),
        Err(response) => *response,
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------
//
// The pages are `.html` files under `templates/`, compiled in by askama.

/// One input, as a template sees it.
pub struct Field {
    pub name: &'static str,
    pub placeholder: &'static str,
    pub hint: &'static str,
    /// Masked and kept out of autofill. Only the builder API key, the one live credential
    /// this page takes.
    pub secret: bool,
}

impl Field {
    const fn new(name: &'static str, placeholder: &'static str, hint: &'static str) -> Self {
        Self {
            name,
            placeholder,
            hint,
            secret: false,
        }
    }

    const fn secret(name: &'static str, hint: &'static str) -> Self {
        Self {
            name,
            placeholder: "",
            hint,
            secret: true,
        }
    }
}

/// A field and its value. Flat rather than holding a `&Field`, because `field.html` is
/// included under the name `field` and `field.field.name` would read worse.
struct Filled {
    /// The input's `name`: the key it posts as.
    name: String,
    /// What the page shows beside it: the key as the config spells it.
    label: &'static str,
    placeholder: &'static str,
    hint: &'static str,
    secret: bool,
    value: String,
    readonly: bool,
}

impl Filled {
    fn new(field: &'static Field, value: Option<&str>) -> Self {
        Self {
            name: field.name.to_owned(),
            label: field.name,
            placeholder: field.placeholder,
            hint: field.hint,
            secret: field.secret,
            value: value.unwrap_or_default().to_owned(),
            readonly: false,
        }
    }

    /// A pricing kind's declared field, posted as `pricing_<kind>_<field>` so every kind's
    /// section can be on the page at once ([`pricing_stanza`] reads the chosen kind's).
    fn kind_field(kind: &str, field: &crate::pricing::FormField, value: Option<&str>) -> Self {
        Self {
            name: format!("pricing_{kind}_{}", field.name),
            label: field.name,
            placeholder: field.placeholder,
            hint: field.hint,
            secret: false,
            value: value.unwrap_or_default().to_owned(),
            readonly: false,
        }
    }

    fn readonly(field: &'static Field, value: Option<&str>) -> Self {
        Self {
            readonly: true,
            ..Self::new(field, value)
        }
    }
}

/// The price source and its guards. Used by both the add form and the edit form, so the
/// two cannot drift.
const SOURCE_FIELDS: &[Field] = &[
    Field::new("min_mid", "0.0001", "refuse a published mid below this"),
    Field::new("max_mid", "0.001", "refuse a published mid above this"),
    Field::new(
        "max_deviation",
        "0.02",
        "circuit breaker: halt on a move this large",
    ),
    Field::new(
        "max_deviation_window",
        "0.20",
        "circuit breaker: halt on this much total movement over the window below",
    ),
    Field::new(
        "max_deviation_window_blocks",
        "1000",
        "how many blocks that window covers",
    ),
    Field::new(
        "max_source_lag_ms",
        "1500",
        "leave a venue out of the average while its prices arrive more than this many \
         milliseconds behind the exchange's own timestamp; needs a venue that sends one \
         (not binance, not mexc). Blank is off",
    ),
];

/// How many venues must have a fresh price before the pair publishes.
const MIN_SOURCES_FIELD: Field = Field::new(
    "min_sources",
    "1",
    "how many of the venues above must have a fresh price for us to publish at all. Blank \
     is 1: keep quoting as long as any one venue is up",
);

/// Locked on an edit: the lane and the signer are derived from these, so changing one is a
/// delete and an add.
const PAIR_IDENTITY: &[Field] = &[
    Field::new("token0", "0xC02aaA39…", "base token, market orientation"),
    Field::new("token1", "0xA0b86991…", "quote token"),
    Field::new(
        "key_env",
        "UPDATER_KEY_WETH_USDC",
        "variable holding this pair's key",
    ),
];

const BUILDER_FIELDS: &[Field] = &[
    Field::new("name", "titan-eu", "how it appears in the logs; unique"),
    Field::new(
        "endpoint",
        "wss://eu.rpc.titanbuilder.xyz/ws/sendquoteupdate",
        "maker WebSocket URL",
    ),
    Field::secret("api_key", "sent as the Authorization header"),
];

/// Has to be exhaustive: a key missing here is one nobody can change without SSH.
const SETTING_FIELDS: &[Field] = &[
    Field::new(
        "mode",
        "builder",
        "builder quotes to the builders; node sends updateState",
    ),
    Field::new(
        "rpc_url",
        "https://…",
        "JSON-RPC endpoint, read on the hot loop",
    ),
    Field::new(
        "rpc_ws_url",
        "wss://…",
        "optional: push new block headers instead of polling",
    ),
    Field::new(
        "requote_ms",
        "50",
        "requote cadence; keep it under the ~400ms eviction",
    ),
    Field::new(
        "metrics_addr",
        "127.0.0.1:9464",
        "Prometheus endpoint; blank disables it",
    ),
    Field::new(
        "backoffice_addr",
        "100.x.y.z:8088",
        "where this page is served",
    ),
    Field::new(
        "registry",
        "0xDa7A…",
        "PrioUpdateRegistry; blank is the mainnet one",
    ),
    Field::new(
        "price_decimals",
        "18",
        "scale of the published mid and delta",
    ),
    Field::new("interval", "12", "seconds between pushes in node mode"),
];

/// `0xA0b86991…3606eB48`, with the whole thing on hover. A row carries two, and 42
/// characters twice over is most of the width.
struct ShortAddress {
    full: String,
    short: String,
}

impl ShortAddress {
    fn new(address: &str) -> Self {
        let short = if address.len() <= 16 {
            address.to_owned()
        } else {
            format!("{}…{}", &address[..8], &address[address.len() - 6..])
        };
        Self {
            full: address.to_owned(),
            short,
        }
    }
}

/// One row of the pairs table, pre-rendered so the template holds no logic beyond "is this
/// set".
struct PairRow {
    key_env: String,
    halted: bool,
    /// Why, when a binary's handle halted it; read-only here.
    halt_reason: Option<String>,
    /// What the running pusher is doing with the pair, and the badge colour for it.
    status: &'static str,
    status_class: &'static str,
    tokens: Vec<ShortAddress>,
    /// The venues the pair streams from, if any.
    source: Option<String>,
    /// The kind that prices it, with its summary of the stanza.
    pricing: String,
    band: Option<String>,
    breaker: Option<String>,
}

impl PairRow {
    /// `running` is the pair's breaker flag if the pusher has a task for it, `None` if not.
    fn new(pair: &RawPair, running: Option<bool>, kinds: &crate::kinds::Kinds) -> Self {
        let (status, status_class) = match (pair.halted, running) {
            (true, _) => ("halted", "halted"),
            (false, Some(true)) => ("breaker tripped", "halted"),
            (false, Some(false)) => ("live", "live"),
            (false, None) => ("not running", "idle"),
        };
        Self {
            key_env: pair.key_env.clone(),
            halted: pair.halted,
            halt_reason: pair.halt_reason.clone(),
            status,
            status_class,
            tokens: pair.tokens.iter().map(|t| ShortAddress::new(t)).collect(),
            source: describe_sources(pair),
            pricing: kinds.pricer_summary(&pricing_spec(pair)),
            band: match (&pair.min_mid, &pair.max_mid) {
                (None, None) => None,
                (min, max) => Some(format!(
                    "{}–{}",
                    min.as_deref().unwrap_or("*"),
                    max.as_deref().unwrap_or("*")
                )),
            },
            breaker: pair.max_deviation.clone(),
        }
    }
}

/// The pair's venues as the table shows them: `ETHUSDC` for the short form, and
/// `binance:ETHUSDC ×2 + kraken:ETH/USD ×1` for a list. Spelled from the raw stanza,
/// because the index renders whatever is in the file, parsed or not.
fn describe_sources(pair: &RawPair) -> Option<String> {
    if let Some(symbol) = &pair.symbol {
        return Some(symbol.clone());
    }
    if pair.sources.is_empty() {
        return None;
    }
    let mut text = pair
        .sources
        .iter()
        .map(|s| {
            format!(
                "{}:{} ×{}",
                s.venue.to_lowercase(),
                s.symbol,
                s.weight.as_deref().unwrap_or("1")
            )
        })
        .collect::<Vec<_>>()
        .join(" + ");
    if let Some(min) = &pair.min_sources {
        text.push_str(&format!(" (min {min})"));
    }
    Some(text)
}

struct BuilderRow {
    name: String,
    endpoint: String,
    region: Option<String>,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexPage {
    target: String,
    config_path: String,
    ok: Option<String>,
    err: Option<String>,
    pairs: Vec<PairRow>,
    builders: Vec<BuilderRow>,
}

#[derive(Template)]
#[template(path = "pair_form.html")]
struct PairFormPage {
    /// Why the save that brought this form back was refused.
    error: Option<String>,
    /// See [`pair_script_version`].
    script_version: &'static str,
    /// The `key_env` being edited, or `None` when adding.
    editing: Option<String>,
    identity: Vec<Filled>,
    source: Vec<Filled>,
    /// One row per venue the binary knows.
    sources: Vec<SourceRow>,
    min_sources: Filled,
    /// One section per registered pricing kind, the pair's own checked.
    kinds: Vec<KindSection>,
    allow_symbol_mismatch: bool,
}

#[derive(Template)]
#[template(path = "builder_form.html")]
struct BuilderFormPage {
    fields: Vec<Filled>,
}

#[derive(Template)]
#[template(path = "settings_form.html")]
struct SettingsFormPage {
    fields: Vec<Filled>,
    endpoints: Vec<EndpointRow>,
    disable_cross_region: bool,
}

/// One venue's row of the settings form's endpoint table.
struct EndpointRow {
    venue: &'static str,
    value: String,
    placeholder: &'static str,
}

/// One venue's row of the pair form's source table.
struct SourceRow {
    venue: &'static str,
    symbol: String,
    weight: String,
    /// How this venue spells a market, shown as the symbol's placeholder.
    example: String,
    /// What is different about this venue, if anything: polled, or no book. The page's
    /// script replaces it with what the lookup found.
    note: &'static str,
}

#[derive(Template)]
#[template(path = "config_error.html")]
struct ConfigErrorPage {
    error: String,
}

fn render_index(
    config: &RawConfig,
    path: &Path,
    banner: &Banner,
    states: &PairStates,
    kinds: &crate::kinds::Kinds,
) -> String {
    // A snapshot, so every row reads the same moment and the lock is not held while
    // rendering. A poisoned lock still holds a usable map.
    let states: std::collections::BTreeMap<String, bool> = states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .map(|(key, tripped)| (key.clone(), tripped.load(Ordering::SeqCst)))
        .collect();
    IndexPage {
        target: config.target.clone(),
        config_path: path.display().to_string(),
        ok: banner.ok.clone(),
        err: banner.err.clone(),
        pairs: config
            .pairs
            .iter()
            .map(|pair| PairRow::new(pair, states.get(&pair.key_env).copied(), kinds))
            .collect(),
        builders: config
            .builders
            .iter()
            .map(|builder| BuilderRow {
                name: builder.name.clone(),
                endpoint: builder.endpoint.clone(),
                region: (builder.disable_cross_region == Some(true)).then(|| "pinned".to_owned()),
            })
            .collect(),
    }
    .render()
    .unwrap_or_else(render_failed)
}

/// The kind a pair's `[pairs.pricing]` stanza names, if it names one as a string.
fn pricing_kind(pair: &RawPair) -> Option<&str> {
    pair.pricing.get("kind")?.as_str()
}

/// The pair's stanza as the kind table reads it, for the summary a row shows.
fn pricing_spec(pair: &RawPair) -> crate::config::PricingSpec {
    crate::config::PricingSpec {
        kind: pricing_kind(pair).unwrap_or("").to_owned(),
        config: pair.pricing.clone(),
    }
}

/// One pricing kind's section of the pair form: its radio, its help and its fields filled
/// from the pair's stanza when the pair is priced by it.
struct KindSection {
    name: String,
    checked: bool,
    help: &'static str,
    fields: Vec<Filled>,
    /// A kind with no declared fields: the page says the stanza is edited in the file.
    in_file: bool,
}

fn render_pair_form(pair: Option<&RawPair>, kinds: &crate::kinds::Kinds) -> String {
    render_pair_form_with(pair, kinds, None)
}

/// [`render_pair_form`], with why the last save of it was refused.
fn render_pair_form_with(
    pair: Option<&RawPair>,
    kinds: &crate::kinds::Kinds,
    error: Option<&str>,
) -> String {
    let editing_key = pair.map(|pair| pair.key_env.clone());
    let editing = editing_key.is_some();
    let identity_values: Vec<Option<&str>> = match pair {
        Some(pair) => vec![
            pair.tokens.first().map(String::as_str),
            pair.tokens.get(1).map(String::as_str),
            Some(pair.key_env.as_str()),
        ],
        None => vec![None; PAIR_IDENTITY.len()],
    };
    let source_values: Vec<Option<&str>> = match pair {
        Some(pair) => vec![
            pair.min_mid.as_deref(),
            pair.max_mid.as_deref(),
            pair.max_deviation.as_deref(),
            pair.max_deviation_window.as_deref(),
            pair.max_deviation_window_blocks.as_deref(),
            pair.max_source_lag_ms.as_deref(),
        ],
        None => vec![None; SOURCE_FIELDS.len()],
    };
    let knob = |field: &'static Field, value: fn(&RawPair) -> Option<&str>| {
        Filled::new(field, pair.and_then(value))
    };
    // Every registered kind, the pair's own first checked (a new pair starts on the first
    // registered). A pair priced by a kind nobody registered still shows it, so its stanza
    // is not lost on save.
    let current = pair.and_then(pricing_kind);
    let mut names: Vec<String> = kinds.pricer_kinds().iter().map(|k| k.to_string()).collect();
    if let Some(current) = current
        && !names.iter().any(|n| n == current)
    {
        names.push(current.to_owned());
    }
    let checked_kind = current
        .map(str::to_owned)
        .or_else(|| names.first().cloned());
    let kind_sections: Vec<KindSection> = names
        .iter()
        .map(|name| {
            let fields = kinds.pricer_fields(name).unwrap_or(&[]);
            let mine = current == Some(name.as_str());
            KindSection {
                name: name.clone(),
                checked: checked_kind.as_deref() == Some(name.as_str()),
                help: kinds.pricer_form_help(name).unwrap_or(""),
                fields: fields
                    .iter()
                    .map(|field| {
                        let value = if mine {
                            pair.and_then(|p| p.pricing.get(field.name))
                                .and_then(|v| v.as_str())
                        } else {
                            None
                        };
                        Filled::kind_field(name, field, value)
                    })
                    .collect(),
                in_file: fields.is_empty(),
            }
        })
        .collect();

    PairFormPage {
        error: error.map(str::to_owned),
        script_version: pair_script_version(),
        editing: editing_key,
        identity: PAIR_IDENTITY
            .iter()
            .zip(identity_values)
            .filter(|(field, _)| !(editing && field.name == "key_env"))
            .map(|(field, value)| {
                if editing {
                    Filled::readonly(field, value)
                } else {
                    Filled::new(field, value)
                }
            })
            .collect(),
        source: SOURCE_FIELDS
            .iter()
            .zip(source_values)
            .map(|(field, value)| Filled::new(field, value))
            .collect(),
        sources: VenueId::ALL
            .iter()
            .map(|venue| {
                // The pair's row for this venue: from `sources`, or for Binance from the
                // short-form `symbol`.
                let row = pair.and_then(|pair| {
                    pair.sources
                        .iter()
                        .find(|s| s.venue.eq_ignore_ascii_case(venue.as_str()))
                        .map(|s| (s.symbol.clone(), s.weight.clone().unwrap_or_default()))
                        .or_else(|| {
                            (*venue == VenueId::Binance)
                                .then(|| pair.symbol.clone().map(|s| (s, String::new())))
                                .flatten()
                        })
                });
                let (symbol, weight) = row.unwrap_or_default();
                let note = match venue {
                    VenueId::Mexc => "polled every second",
                    _ => "",
                };
                SourceRow {
                    venue: venue.as_str(),
                    symbol,
                    weight,
                    example: venue.expected_symbol("ETH", "USDC"),
                    note,
                }
            })
            .collect(),
        min_sources: knob(&MIN_SOURCES_FIELD, |p| p.min_sources.as_deref()),
        kinds: kind_sections,
        allow_symbol_mismatch: pair.is_some_and(|pair| pair.allow_symbol_mismatch),
    }
    .render()
    .unwrap_or_else(render_failed)
}

fn render_builder_form() -> String {
    BuilderFormPage {
        fields: BUILDER_FIELDS
            .iter()
            .map(|field| Filled::new(field, None))
            .collect(),
    }
    .render()
    .unwrap_or_else(render_failed)
}

fn render_settings_form(config: &RawConfig) -> String {
    let s = &config.settings;
    // In the order SETTING_FIELDS declares them.
    let values: Vec<Option<String>> = vec![
        s.mode.clone(),
        s.rpc_url.clone(),
        s.rpc_ws_url.clone(),
        s.requote_ms.map(|v| v.to_string()),
        s.metrics_addr.clone(),
        s.backoffice_addr.clone(),
        s.registry.clone(),
        s.price_decimals.map(|v| v.to_string()),
        s.interval.map(|v| v.to_string()),
    ];
    SettingsFormPage {
        fields: SETTING_FIELDS
            .iter()
            .zip(&values)
            .map(|(field, value)| Filled::new(field, value.as_deref()))
            .collect(),
        endpoints: VenueId::ALL
            .iter()
            .map(|venue| EndpointRow {
                venue: venue.as_str(),
                value: s
                    .endpoints
                    .get(venue.as_str())
                    .cloned()
                    // The older key, shown in the Binance row until a save moves it.
                    .or_else(|| {
                        (*venue == VenueId::Binance)
                            .then(|| s.binance_ws.clone())
                            .flatten()
                    })
                    .unwrap_or_default(),
                placeholder: venue.default_endpoint(),
            })
            .collect(),
        disable_cross_region: s.disable_cross_region == Some(true),
    }
    .render()
    .unwrap_or_else(render_failed)
}

/// Templates are checked at compile time, so this is only reachable through a formatter
/// error. It still says something: an empty body would look like a page with nothing on it.
fn render_failed(err: askama::Error) -> String {
    tracing::warn!("backoffice: a page failed to render: {err}");
    format!("<h1>This page failed to render</h1><p>{err}</p>")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kinds a test binary registers: the three shipped ones, and `skewed`, a kind with
    /// no form fields, so its stanza is edited in the file.
    fn test_kinds() -> Arc<crate::kinds::Kinds> {
        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Skewed {
            #[allow(dead_code)]
            half_spread: f64,
        }
        struct SkewedPricer;
        impl crate::pricing::Pricer for SkewedPricer {
            fn price(
                &mut self,
                tick: &crate::pricing::TickCtx,
                _: &mut crate::pricing::Diagnostics,
            ) -> Result<crate::pricing::PricerOutput, crate::pricing::Refusal> {
                let market = tick.market()?;
                Ok(crate::pricing::PricerOutput::new(market.delta, market.mid))
            }
        }
        let mut kinds = crate::kinds::Kinds::default();
        kinds.insert("fixed", Arc::new(crate::pricing::Fixed));
        kinds.insert("feed", Arc::new(crate::pricing::Feed));
        kinds.insert("volatile", Arc::new(crate::pricing::Volatile));
        kinds.insert(
            "skewed",
            crate::kinds::fn_factory(|_: Skewed, _: &mut crate::pricing::BuildCtx| {
                Ok(SkewedPricer)
            }),
        );
        Arc::new(kinds)
    }

    fn a_config() -> RawConfig {
        toml::from_str(
            r#"
target = "0x1111111111111111111111111111111111111111"

[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
           "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
symbol  = "ETHUSDC"
key_env = "UPDATER_KEY_WETH_USDC"
pricing = { kind = "feed" }

[[builder]]
name = "titan-eu"
endpoint = "wss://eu.example/ws"
api_key = "k"
"#,
        )
        .unwrap()
    }

    #[test]
    fn halting_keeps_the_pair_and_refuses_a_change_that_does_nothing() {
        let mut config = a_config();
        // Left behind by a hand edit: the page's own halt must not inherit it, or it would
        // read as explained and the bulk restart would skip it.
        config.pairs[0].halt_reason = Some("stale".to_owned());
        set_halted(&mut config, Some("UPDATER_KEY_WETH_USDC"), true).unwrap();
        assert_eq!(config.pairs.len(), 1, "a halt must not remove the pair");
        assert!(config.pairs[0].halted);
        assert_eq!(config.pairs[0].halt_reason, None);
        assert!(set_halted(&mut config, Some("UPDATER_KEY_WETH_USDC"), true).is_err());
        assert!(set_halted(&mut config, Some("NO_SUCH_KEY"), true).is_err());
        set_halted(&mut config, None, false).unwrap();
        assert!(!config.pairs[0].halted);
        assert!(set_halted(&mut config, None, false).is_err());

        // An explained halt is skipped by the bulk resume and cleared by its own.
        config.pairs[0].halted = true;
        config.pairs[0].halt_reason = Some("vault audit".to_owned());
        assert!(set_halted(&mut config, None, false).is_err());
        assert!(config.pairs[0].halted);
        set_halted(&mut config, Some("UPDATER_KEY_WETH_USDC"), false).unwrap();
        assert!(!config.pairs[0].halted);
        assert_eq!(config.pairs[0].halt_reason, None);
    }

    #[test]
    fn each_row_says_what_the_pusher_is_doing_with_the_pair() {
        let mut config = a_config();
        let key = "UPDATER_KEY_WETH_USDC".to_owned();
        let states = PairStates::default();
        let tripped = Arc::new(AtomicBool::new(false));
        let page = |config: &RawConfig, states: &PairStates| {
            render_index(
                config,
                Path::new("config.toml"),
                &Banner {
                    ok: None,
                    err: None,
                },
                states,
                &test_kinds(),
            )
        };

        // In the file, no task: not running, and it can be halted.
        let html = page(&config, &states);
        assert!(
            html.contains(r#"class="status idle">not running"#),
            "{html}"
        );
        assert!(html.contains("action=/pairs/halt\n"), "{html}");

        // Running and quoting.
        states
            .lock()
            .unwrap()
            .insert(key.clone(), Arc::clone(&tripped));
        assert!(page(&config, &states).contains(r#"class="status live">live"#));

        // Its breaker tripped: the task still exists but it is not quoting.
        tripped.store(true, Ordering::SeqCst);
        assert!(page(&config, &states).contains(r#"class="status halted">breaker tripped"#));

        // Halted from the page wins over whatever is running, and offers Resume.
        config.pairs[0].halted = true;
        let html = page(&config, &states);
        assert!(html.contains(r#"class="status halted">halted"#), "{html}");
        assert!(html.contains("action=/pairs/resume"), "{html}");
        assert!(!html.contains("action=/pairs/halt\n"), "{html}");
    }

    /// In its own directory, so the writer's temp-file-and-rename has somewhere to work.
    /// A chain client for tests that never read the chain.
    fn test_client() -> ethrex_rpc::clients::eth::EthClient {
        ethrex_rpc::clients::eth::EthClient::new(url::Url::parse("http://127.0.0.1:1").unwrap())
            .unwrap()
    }

    fn config_on_disk(
        name: &str,
    ) -> (
        PathBuf,
        Backoffice,
        tokio::sync::mpsc::Receiver<ReloadRequest>,
    ) {
        let dir = std::env::temp_dir().join(format!(
            "quote-updater-backoffice-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, config::render_raw(&a_config()).unwrap()).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let state = Backoffice {
            states: PairStates::default(),
            config_path: path.clone(),
            reloads: tx,
            client: test_client(),
            rest: Arc::new(crate::discover::Rest::default()),
            http: reqwest::Client::new(),
            lookups: Arc::new(crate::discover::Cache::default()),
            preview_pages: Arc::new(tokio::sync::Semaphore::new(PREVIEW_PAGES)),
            edits: Arc::new(tokio::sync::Mutex::new(())),
            kinds: test_kinds(),
        };
        (path, state, rx)
    }

    /// The property the whole page rests on: a refused change leaves the file exactly as it
    /// was, so what is on disk always describes what is running.
    #[tokio::test]
    async fn a_rejected_reload_puts_the_file_back() {
        let (path, state, mut reloads) = config_on_disk("reject");
        let before = std::fs::read_to_string(&path).unwrap();

        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            // What the service says when preflight fails on the new stanza.
            let _ = request.reply.send(Err(ReloadFailure::Rejected(
                "preflight failed for the new config".to_owned(),
            )));
        });

        let outcome = apply(
            &state,
            "100.64.0.9:1234".parse().unwrap(),
            "added pair TEST",
            |config| {
                config.pairs.push(RawPair {
                    tokens: vec!["0xaa".to_owned(), "0xbb".to_owned()],
                    key_env: "UPDATER_KEY_TEST".to_owned(),
                    symbol: Some("TESTUSDC".to_owned()),
                    sources: Vec::new(),
                    min_sources: None,
                    max_source_lag_ms: None,
                    min_mid: None,
                    max_mid: None,
                    max_deviation: None,
                    max_deviation_window: None,
                    max_deviation_window_blocks: None,
                    allow_symbol_mismatch: false,
                    halted: false,
                    halt_reason: None,
                    pricing: toml::from_str("kind = \"feed\"").unwrap(),
                    guards: Vec::new(),
                });
                Ok(())
            },
        )
        .await;
        service.await.unwrap();

        assert!(!outcome.ok, "{}", outcome.message);
        assert!(outcome.message.contains("put back"), "{}", outcome.message);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            before,
            "the file must be byte-identical to what it was before the rejected edit"
        );
    }

    /// A reload that applied in part (another pair's feed would not come up) keeps the edit:
    /// it is live on every lane the reload reached, and putting the file back would have
    /// the next reload undo it. The banner is not a success all the same, since a pair is
    /// still on its previous config, and it says the file kept the change.
    #[tokio::test]
    async fn a_partly_applied_reload_keeps_the_edit_and_says_so() {
        let (path, state, mut reloads) = config_on_disk("partial");

        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Err(ReloadFailure::Partial(
                "reload: 1 halted, 1 failed — some pairs kept their previous config".to_owned(),
            )));
        });

        let outcome = apply(
            &state,
            "100.64.0.9:1234".parse().unwrap(),
            "halted pair UPDATER_KEY_WETH_USDC",
            |config| set_halted(config, Some("UPDATER_KEY_WETH_USDC"), true),
        )
        .await;
        service.await.unwrap();

        assert!(!outcome.ok, "a partial apply is not a clean success");
        assert!(
            outcome.message.contains("keeps the change"),
            "{}",
            outcome.message
        );
        assert!(
            !outcome.message.contains("nothing changed"),
            "{}",
            outcome.message
        );
        assert!(config::load_raw(&path).unwrap().pairs[0].halted);
    }

    /// And an accepted one keeps the edit, at 0600, still loadable by the real parser.
    #[tokio::test]
    async fn an_accepted_reload_keeps_the_edit() {
        let (path, state, mut reloads) = config_on_disk("accept");

        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 added".to_owned()));
        });

        let outcome = apply(
            &state,
            "100.64.0.9:1234".parse().unwrap(),
            "edited pair UPDATER_KEY_WETH_USDC",
            |config| {
                config.pairs[0].max_deviation = Some("0.02".to_owned());
                Ok(())
            },
        )
        .await;
        service.await.unwrap();

        assert!(outcome.ok, "{}", outcome.message);
        let written = config::load_raw(&path).unwrap();
        assert_eq!(written.pairs[0].max_deviation.as_deref(), Some("0.02"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the config holds builder API keys");
        }
    }

    /// A page that rewrites the config, with no login, on whatever the host faces.
    #[tokio::test]
    async fn an_unspecified_bind_is_refused() {
        assert!(is_unspecified_bind("0.0.0.0:8088".parse().unwrap()));
        assert!(is_unspecified_bind("[::]:8088".parse().unwrap()));
        assert!(!is_unspecified_bind("127.0.0.1:8088".parse().unwrap()));
        assert!(!is_unspecified_bind("100.64.1.2:8088".parse().unwrap()));

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let err = bind(
            "0.0.0.0:0".parse().unwrap(),
            PathBuf::from("config.toml"),
            _tx,
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .expect_err("an unspecified bind must be refused");
        assert!(format!("{err:#}").contains("tailnet"), "{err:#}");
    }

    /// Through a socket rather than by calling handlers, so the routing, `ConnectInfo` and
    /// `Form` wiring is exercised too.
    async fn http(addr: SocketAddr, request: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    }

    /// The page renders over a real socket, and a real form post reaches a handler and
    /// comes back as the redirect the browser needs.
    #[tokio::test]
    async fn the_page_serves_and_a_form_post_reaches_its_handler() {
        let (path, state, mut reloads) = config_on_disk("http");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();

        let page = http(
            local,
            "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(page.contains("200 OK"), "{page}");
        assert!(page.contains("UPDATER_KEY_WETH_USDC"), "{page}");
        assert!(page.contains("titan-eu"), "{page}");
        assert!(page.contains("/pairs/edit/UPDATER_KEY_WETH_USDC"), "{page}");

        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
        });

        let body = "key_env=UPDATER_KEY_WETH_USDC&symbol=ETHUSDC&pricing=feed\
                    &pricing_feed_delta=0.0005&pricing_fixed_mid=&pricing_fixed_delta=\
                    &min_mid=&max_mid=&max_deviation=0.02&max_deviation_window=0.20\
                    &max_deviation_window_blocks=1000&pricing_volatile_gamma=";
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        service.await.unwrap();

        assert!(posted.contains("303 See Other"), "{posted}");
        assert!(posted.contains("location: /?ok="), "{posted}");

        // And the edit is on disk, which is the only thing that actually matters.
        let written = config::load_raw(&path).unwrap();
        assert_eq!(
            written.pairs[0].pricing.get("delta"),
            Some(&toml::Value::String("0.0005".to_owned()))
        );
        assert_eq!(
            written.pairs[0].pricing.get("kind"),
            Some(&toml::Value::String("feed".to_owned()))
        );
        assert_eq!(written.pairs[0].max_deviation.as_deref(), Some("0.02"));
        assert_eq!(
            written.pairs[0].max_deviation_window.as_deref(),
            Some("0.20")
        );
        assert_eq!(
            written.pairs[0].max_deviation_window_blocks.as_deref(),
            Some("1000")
        );
        // An empty form field is an absent key, not an empty string; another kind's
        // inputs, blank or not, are not the chosen kind's.
        assert_eq!(written.pairs[0].min_mid, None);
        assert_eq!(written.pairs[0].pricing.get("mid"), None);
        assert_eq!(written.pairs[0].pricing.get("gamma"), None);
    }

    /// A POST of `body` to `route`, through the socket, answered with the banner the page
    /// would show: the redirect's message, decoded, and whether it was the ok kind.
    async fn post(addr: SocketAddr, route: &str, body: &str) -> (bool, String) {
        let response = http(
            addr,
            &format!(
                "POST {route} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        let location = response
            .lines()
            .find_map(|line| line.strip_prefix("location: /?"))
            .unwrap_or_else(|| panic!("no redirect in {response}"));
        let (kind, message) = location.split_once('=').expect("kind=message");
        (kind == "ok", urldecode(message.trim()))
    }

    /// A halt a binary's handle set is its kill switch: "Restart halted pairs" brings back
    /// what was halted from this page and leaves that one halted, saying which and why.
    /// That pair's own Resume still clears it.
    #[tokio::test]
    async fn restarting_halted_pairs_leaves_a_handle_halt_alone() {
        let (path, state, mut reloads) = config_on_disk("kill-switch");
        let mut config = a_config();
        let mut second = config.pairs[0].clone();
        second.tokens = vec![
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_owned(),
            "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_owned(),
        ];
        second.symbol = Some("USDCUSDT".to_owned());
        second.key_env = "UPDATER_KEY_USDC_USDT".to_owned();
        config.pairs.push(second);
        // The first, halted exactly as `UpdaterHandle::halt` writes it.
        crate::updater::halt_line(&mut config.pairs[0], "vault audit").unwrap();
        std::fs::write(&path, config::render_raw(&config).unwrap()).unwrap();
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        // The service, accepting every reload it is asked for.
        tokio::spawn(async move {
            while let Some(request) = reloads.recv().await {
                let _ = request.reply.send(Ok("reload: 1 changed".to_owned()));
            }
        });

        let (ok, banner) = post(local, "/pairs/halt", "key_env=UPDATER_KEY_USDC_USDT").await;
        assert!(ok, "{banner}");
        let (ok, banner) = post(local, "/reload", "").await;
        assert!(ok, "{banner}");

        let written = config::load_raw(&path).unwrap();
        assert!(
            written.pairs[0].halted,
            "the handle's halt survives the bulk restart"
        );
        assert_eq!(written.pairs[0].halt_reason.as_deref(), Some("vault audit"));
        assert!(!written.pairs[1].halted, "the page's own halt is restarted");
        assert!(
            banner.contains("UPDATER_KEY_WETH_USDC") && banner.contains("vault audit"),
            "the page says which pair it left halted, and why: {banner}"
        );

        // Pressed again with only the handle's halt left: nothing to restart, and it says
        // why that pair is still down rather than "nothing was halted".
        let (ok, banner) = post(local, "/reload", "").await;
        assert!(ok, "{banner}");
        assert!(banner.contains("UPDATER_KEY_WETH_USDC"), "{banner}");
        assert!(!banner.contains("nothing was halted"), "{banner}");
        assert!(config::load_raw(&path).unwrap().pairs[0].halted);

        let (ok, banner) = post(local, "/pairs/resume", "key_env=UPDATER_KEY_WETH_USDC").await;
        assert!(ok, "{banner}");
        let written = config::load_raw(&path).unwrap();
        assert!(!written.pairs[0].halted, "its own Resume clears it");
        assert_eq!(written.pairs[0].halt_reason, None);
    }

    /// The venue table posts one symbol and one weight per venue through the real form
    /// decoder (the flattened struct is the part worth proving), and lands in the file as
    /// `sources`; a single Binance row with no weight lands as the short-form `symbol`.
    #[tokio::test]
    async fn the_venue_table_is_saved_as_sources_or_as_the_short_form() {
        let (path, state, mut reloads) = config_on_disk("venues");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            for _ in 0..2 {
                let request = reloads.recv().await.expect("a reload must be requested");
                let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
            }
        });
        let post = |body: String| {
            format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };

        // Two venues and a minimum: `sources`, weights kept as written, blank weight absent.
        let body = "key_env=UPDATER_KEY_WETH_USDC&symbol_binance=ETHUSDC&weight_binance=2\
                    &symbol_kraken=ETH%2FUSD&weight_kraken=&symbol_okx=&weight_okx=\
                    &min_sources=2&pricing=feed&pricing_feed_delta=0.0005&min_mid=&max_mid=\
                    &max_deviation=&max_deviation_window=&max_deviation_window_blocks=";
        let posted = http(local, &post(body.to_owned())).await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        let written = config::load_raw(&path).unwrap();
        assert_eq!(written.pairs[0].symbol, None);
        assert_eq!(
            written.pairs[0].sources,
            vec![
                RawSource {
                    venue: "binance".into(),
                    symbol: "ETHUSDC".into(),
                    weight: Some("2".into()),
                },
                RawSource {
                    venue: "kraken".into(),
                    symbol: "ETH/USD".into(),
                    weight: None,
                },
            ]
        );
        assert_eq!(written.pairs[0].min_sources.as_deref(), Some("2"));
        // The file parses, and the index and the edit form show the rows.
        config::parse_config(&std::fs::read_to_string(&path).unwrap(), 18)
            .expect("a saved venue table is a config that loads");
        let page = http(
            local,
            "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            page.contains("binance:ETHUSDC ×2 + kraken:ETH/USD ×1 (min 2)"),
            "{page}"
        );
        let form = http(
            local,
            "GET /pairs/edit/UPDATER_KEY_WETH_USDC HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            form.contains(r#"name="symbol_kraken" value="ETH/USD""#),
            "{form}"
        );
        assert!(
            form.contains(r#"name="weight_binance" value="2""#),
            "{form}"
        );
        assert!(form.contains(r#"name="min_sources""#), "{form}");

        // Back to one Binance row with no weight and no minimum: the short form.
        let body = "key_env=UPDATER_KEY_WETH_USDC&symbol_binance=ETHUSDC&weight_binance=\
                    &min_sources=&pricing=feed&pricing_feed_delta=0.0005&min_mid=&max_mid=\
                    &max_deviation=&max_deviation_window=&max_deviation_window_blocks=";
        let posted = http(local, &post(body.to_owned())).await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        service.await.unwrap();
        let written = config::load_raw(&path).unwrap();
        assert_eq!(written.pairs[0].symbol.as_deref(), Some("ETHUSDC"));
        assert!(written.pairs[0].sources.is_empty());
        assert_eq!(written.pairs[0].min_sources, None);
    }

    /// The source check through the real form decoder: set, kept by a form that predates
    /// the field (absent), cleared by a blank one. An edit the file parser would refuse (an
    /// age of zero) is refused before anything is written, and the same form comes back with
    /// the reason and the posted values, so the field can be fixed right there.
    #[tokio::test]
    async fn the_source_lag_key_is_set_kept_cleared_and_refused_in_the_form() {
        let (path, state, mut reloads) = config_on_disk("source-checks");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let _service = {
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                while let Some(request) = reloads.recv().await {
                    served.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
                }
            })
        };
        let post = |body: &str| {
            format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let common = "key_env=UPDATER_KEY_WETH_USDC&pricing=feed&pricing_feed_delta=0.0005\
                      &min_mid=&max_mid=&max_deviation=&max_deviation_window=\
                      &max_deviation_window_blocks=";
        // OKX stamps its frames, which the key needs one venue to do.
        let two = "&symbol_binance=ETHUSDC&weight_binance=&symbol_okx=ETH-USDC&weight_okx=2";
        let one = "&symbol_binance=&weight_binance=&symbol_okx=ETH-USDC&weight_okx=";
        let read = || config::load_raw(&path).unwrap().pairs.remove(0);

        // Set.
        let posted = http(
            local,
            &post(&format!("{common}{two}&max_source_lag_ms=1500")),
        )
        .await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        let pair = read();
        assert_eq!(pair.max_source_lag_ms.as_deref(), Some("1500"));

        // A form from before the field existed does not post it: kept.
        let posted = http(local, &post(&format!("{common}{two}"))).await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        let pair = read();
        assert_eq!(pair.max_source_lag_ms.as_deref(), Some("1500"));

        // Down to one venue, the key kept: saved.
        let posted = http(
            local,
            &post(&format!("{common}{one}&max_source_lag_ms=1500")),
        )
        .await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        let pair = read();
        assert_eq!(pair.sources.len(), 1);
        assert_eq!(pair.max_source_lag_ms.as_deref(), Some("1500"));

        // An age the file parser refuses is refused here too: nothing written, and the
        // form comes back with the reason and what was posted.
        let refused = http(local, &post(&format!("{common}{one}&max_source_lag_ms=0"))).await;
        assert!(refused.starts_with("HTTP/1.1 422"), "{refused}");
        assert!(refused.contains("class=banner-err"), "{refused}");
        assert!(refused.contains("max_source_lag_ms is zero"), "{refused}");
        assert!(
            refused.contains("name=\"max_source_lag_ms\"\n         value=\"0\""),
            "{refused}"
        );
        assert_eq!(
            read().max_source_lag_ms.as_deref(),
            Some("1500"),
            "nothing written"
        );

        // Cleared by a blank: saved.
        let posted = http(local, &post(&format!("{common}{one}&max_source_lag_ms="))).await;
        assert!(posted.contains("location: /?ok="), "{posted}");
        assert_eq!(read().max_source_lag_ms, None);
        config::parse_config(&std::fs::read_to_string(&path).unwrap(), 18)
            .expect("what the form saved loads");

        assert_eq!(
            served.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "the refused edit asked for no reload"
        );
    }

    /// A reload that applies an edit in part is not a rejection: the file keeps the change,
    /// so the page goes back to the list saying it was saved, not to the form as if the
    /// edit had been refused.
    #[tokio::test]
    async fn an_edit_the_reload_applies_in_part_is_saved_not_refused() {
        let (path, state, mut reloads) = config_on_disk("partial-edit");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let _service = tokio::spawn(async move {
            if let Some(request) = reloads.recv().await {
                let _ = request.reply.send(Err(ReloadFailure::Partial(
                    "1 restarted, 1 failed to start".to_owned(),
                )));
            }
        });
        let body = "key_env=UPDATER_KEY_WETH_USDC&pricing=feed&pricing_feed_delta=0.0005\
                    &min_mid=&max_mid=&max_deviation=&max_deviation_window=\
                    &max_deviation_window_blocks=&symbol_binance=&weight_binance=\
                    &symbol_okx=ETH-USDC&weight_okx=&max_source_lag_ms=1500";
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        assert!(posted.starts_with("HTTP/1.1 303"), "{posted}");
        assert!(posted.contains("location: /?err=Saved"), "{posted}");
        assert!(
            !posted.contains("banner-err"),
            "not the form again: {posted}"
        );
        assert_eq!(
            config::load_raw(&path).unwrap().pairs[0]
                .max_source_lag_ms
                .as_deref(),
            Some("1500"),
            "the file keeps the change"
        );
    }

    /// The source-check field is on both forms, the edit form showing the file's value.
    #[test]
    fn the_source_lag_field_renders_on_both_forms() {
        assert!(
            render_pair_form(None, &test_kinds()).contains(r#"name="max_source_lag_ms""#),
            "max_source_lag_ms is missing from the new-pair form"
        );
        let mut config = a_config();
        config.pairs[0].max_source_lag_ms = Some("10000".to_owned());
        let form = render_pair_form(Some(&config.pairs[0]), &test_kinds());
        assert!(
            form.contains("name=\"max_source_lag_ms\"\n         value=\"10000\""),
            "{form}"
        );
        assert!(!form.contains("class=banner-err"), "{form}");
    }

    /// The lookup the page's script calls once both addresses are typed: the token names
    /// read on chain, each exchange's own market list matched on base and quote, one
    /// entry per venue, and nothing saved. An exchange that cannot be asked is a note, not
    /// a failed lookup.
    #[tokio::test]
    async fn the_lookup_answers_one_entry_per_venue_and_saves_nothing() {
        use axum::{Router, routing::get};
        const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        let rpc = crate::rpc_mock::MockRpc::spawn(1).await;
        rpc.set_symbol(config::parse_address(WETH).unwrap(), "WETH");
        rpc.set_symbol(config::parse_address(USDC).unwrap(), "USDC");
        // Three exchanges answer; the rest are missing from this mock, which is how an
        // exchange that is down looks. Binance lists ETH/USDC; Coinbase's ETH-USDC is
        // delisted, so its ETH-USD stands in; OKX has only a suspended ETH-USDT.
        let app = Router::new()
            .route(
                "/api/v3/exchangeInfo",
                get(|| async {
                    r#"{"symbols":[{"symbol":"ETHUSDC","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDC"}]}"#
                }),
            )
            .route(
                "/api/v3/ticker/24hr",
                get(|| async { r#"{"lastPrice":"2500.5","quoteVolume":"300000000"}"# }),
            )
            .route(
                "/products",
                get(|| async {
                    r#"[{"id":"ETH-USDC","base_currency":"ETH","quote_currency":"USDC","status":"delisted","trading_disabled":true},
                        {"id":"ETH-USD","base_currency":"ETH","quote_currency":"USD","status":"online","trading_disabled":false}]"#
                }),
            )
            .route(
                "/products/ETH-USD/stats",
                get(|| async { r#"{"volume":"160000","last":"2500"}"# }),
            )
            .route(
                "/api/v5/public/instruments",
                get(|| async {
                    r#"{"code":"0","data":[{"instId":"ETH-USDT","baseCcy":"ETH","quoteCcy":"USDT","state":"suspend"}]}"#
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let exchanges = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let (path, state, _reloads) = config_on_disk("lookup");
        let before = std::fs::read_to_string(&path).unwrap();
        let (local, _task) = bind_with(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            ethrex_rpc::clients::eth::EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
            crate::discover::Rest::all_at(&format!("http://{exchanges}")),
        )
        .await
        .unwrap();

        let page = http(
            local,
            &format!(
                "GET /pairs/lookup?token0={WETH}&token1={USDC} HTTP/1.1\r\nHost: t\r\n\
                 Connection: close\r\n\r\n"
            ),
        )
        .await;
        assert!(page.contains("200 OK"), "{page}");
        let body = page.split("\r\n\r\n").nth(1).expect("a body");
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(json["ok"], true, "{json}");
        assert_eq!(json["base"], "ETH", "WETH is read as its underlying");
        assert_eq!(json["quote"], "USDC");
        let venues = json["venues"].as_array().unwrap();
        assert_eq!(venues.len(), VenueId::ALL.len(), "one entry per venue");
        let by = |name: &str| venues.iter().find(|v| v["venue"] == name).unwrap().clone();
        assert_eq!(by("binance")["symbol"], "ETHUSDC");
        assert_eq!(
            by("binance")["weight"],
            "7.5",
            "300M against Coinbase's 400M"
        );
        assert_eq!(by("binance")["last"], 2500.5);
        assert_eq!(by("binance")["stand_in"], false);
        assert_eq!(
            by("coinbase")["symbol"],
            "ETH-USD",
            "the delisted ETH-USDC is skipped"
        );
        assert_eq!(by("coinbase")["weight"], "10");
        assert_eq!(by("coinbase")["stand_in"], true);
        assert_eq!(
            by("okx")["listed"],
            false,
            "a suspended market is not listed"
        );
        let notes: Vec<&str> = json["notes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap())
            .collect();
        assert!(
            notes[0].starts_with("2 exchanges list ETH against USDC"),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|n| n.starts_with("bybit: could not read its markets")),
            "an exchange that did not answer is named: {notes:?}"
        );
        // And its row says so, rather than claiming it does not list the pair.
        assert_eq!(by("bybit")["unreachable"], true);
        assert_eq!(
            by("okx")["unreachable"],
            false,
            "okx answered; it just has no market"
        );
        // Nothing saved.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    /// Two mock venues on one port, standing in for what the preview dials: Binance on any
    /// `/<symbol>@bookTicker` path, quoting 1999/2001, and Bybit on `/bybit`, quoting
    /// 2099/2101 once it is subscribed. Each socket is held until the other end drops it.
    /// Records the path of every connection ever accepted and how many are open per path.
    #[derive(Clone, Default)]
    struct MockVenues {
        accepted: Arc<std::sync::Mutex<Vec<String>>>,
        open: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    }

    impl MockVenues {
        #[allow(clippy::result_large_err)]
        async fn start() -> (Self, SocketAddr) {
            use futures_util::{SinkExt, StreamExt};
            use tokio_tungstenite::tungstenite::Message;
            let mocks = Self::default();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let shared = mocks.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let mocks = shared.clone();
                    tokio::spawn(async move {
                        let mut path = String::new();
                        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(
                            stream,
                            |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                             resp| {
                                path = req.uri().path().to_owned();
                                Ok(resp)
                            },
                        )
                        .await
                        else {
                            return;
                        };
                        mocks.accepted.lock().unwrap().push(path.clone());
                        *mocks.open.lock().unwrap().entry(path.clone()).or_default() += 1;
                        if path.ends_with("@bookTicker") {
                            let _ = ws
                                .send(Message::text(
                                    r#"{"u":1,"s":"X","b":"1999.0","B":"1","a":"2001.0","A":"1"}"#,
                                ))
                                .await;
                        } else if let Some(Ok(_subscribe)) = ws.next().await {
                            let _ = ws
                                .send(Message::text(
                                    r#"{"topic":"orderbook.1.ETHUSDC","ts":"1","type":"snapshot","data":{"s":"ETHUSDC","b":[["2099.0","1"]],"a":[["2101.0","1"]],"u":1,"seq":1}}"#,
                                ))
                                .await;
                        }
                        while let Some(Ok(_)) = ws.next().await {}
                        *mocks.open.lock().unwrap().get_mut(&path).unwrap() -= 1;
                    });
                }
            });
            (mocks, addr)
        }

        fn open(&self, path: &str) -> usize {
            self.open.lock().unwrap().get(path).copied().unwrap_or(0)
        }

        fn accepted(&self, path: &str) -> usize {
            self.accepted
                .lock()
                .unwrap()
                .iter()
                .filter(|p| *p == path)
                .count()
        }

        /// Waits until `path` has `want` open connections.
        async fn until_open(&self, path: &str, want: usize) {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while self.open(path) != want {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{path}: {} open, expected {want}", self.open(path)));
        }
    }

    const BINANCE_ETH: &str = "/ethusdc@bookTicker";
    const BYBIT: &str = "/bybit";

    /// A backoffice whose Binance and Bybit endpoints are the mocks.
    async fn preview_backoffice(name: &str, venues: SocketAddr) -> SocketAddr {
        let (path, state, _reloads) = config_on_disk(name);
        let mut config = config::load_raw(&path).unwrap();
        config
            .settings
            .endpoints
            .insert("binance".to_owned(), format!("ws://{venues}"));
        config
            .settings
            .endpoints
            .insert("bybit".to_owned(), format!("ws://{venues}{BYBIT}"));
        config::write_raw_atomically(&path, &config).unwrap();
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        local
    }

    type PreviewPage = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    /// Opens the preview for WETH/USDC, declared base first, so the lane is inverted and
    /// the preview has to turn it back to read 2000 rather than 0.0005.
    async fn open_preview(
        local: SocketAddr,
    ) -> Result<PreviewPage, tokio_tungstenite::tungstenite::Error> {
        open_preview_from(local, &format!("http://{local}")).await
    }

    async fn open_preview_from(
        local: SocketAddr,
        origin: &str,
    ) -> Result<PreviewPage, tokio_tungstenite::tungstenite::Error> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!(
            "ws://{local}/pairs/preview?token0=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2\
             &token1=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        )
        .into_client_request()
        .unwrap();
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        tokio_tungstenite::connect_async(request)
            .await
            .map(|(ws, _)| ws)
    }

    async fn send(page: &mut PreviewPage, command: serde_json::Value) {
        use futures_util::SinkExt;
        page.send(tokio_tungstenite::tungstenite::Message::text(
            command.to_string(),
        ))
        .await
        .unwrap();
    }

    /// Reads the page's messages until one satisfies `want`.
    async fn snapshot_where(
        page: &mut PreviewPage,
        want: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        use futures_util::StreamExt;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = page.next().await.expect("the page stays open").unwrap();
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if want(&value) {
                        return value;
                    }
                }
            }
        })
        .await
        .expect("no such message within 10s")
    }

    /// The preview connects to the ticked venues with the pusher's own feed code and sends
    /// what they quote: each venue's mid and the composite, oriented the way the venues
    /// quote, averaged the way the pusher averages what it publishes.
    #[tokio::test]
    async fn the_preview_sends_the_venues_mids_and_the_composite() {
        let (_mocks, venues) = MockVenues::start().await;
        let local = preview_backoffice("preview", venues).await;
        let mut page = open_preview(local).await.unwrap();
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETHUSDC", "weight": "1"}),
        )
        .await;
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "bybit", "symbol": "ETHUSDC", "weight": ""}),
        )
        .await;
        let found = snapshot_where(&mut page, |v| v["fresh"] == 2).await;
        assert_eq!(found["venues"][0]["venue"], "binance");
        assert!((found["venues"][0]["mid"].as_f64().unwrap() - 2000.0).abs() < 0.01);
        assert_eq!(found["venues"][1]["venue"], "bybit");
        assert!((found["venues"][1]["mid"].as_f64().unwrap() - 2100.0).abs() < 0.01);
        // The inverted lane averages 1/2000 and 1/2100, so read back the way the venues
        // quote it the composite is their harmonic mean, 2048.78, not 2050.
        let composite = found["composite"].as_f64().unwrap();
        assert!((composite - 2048.78).abs() < 0.01, "{found}");

        // Something that is not a command is answered with an error, and the page lives on.
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETH USDC?x=1", "weight": "1"}),
        )
        .await;
        let refused = snapshot_where(&mut page, |v| v.get("error").is_some()).await;
        assert!(
            refused["error"]
                .as_str()
                .unwrap()
                .contains("not a market name"),
            "{refused}"
        );
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETHUSDC", "weight": "1000001"}),
        )
        .await;
        let refused = snapshot_where(&mut page, |v| v.get("error").is_some()).await;
        assert!(
            refused["error"].as_str().unwrap().contains("above 1000000"),
            "{refused}"
        );
        snapshot_where(&mut page, |v| v["fresh"] == 2).await;
    }

    /// The point of the websocket: unticking a venue closes that venue's connection and no
    /// other, a new weight reconnects nothing, and a new symbol reconnects only its venue.
    /// Closing the page closes everything it opened.
    #[tokio::test]
    async fn unticking_a_venue_disconnects_only_that_venue() {
        let (mocks, venues) = MockVenues::start().await;
        let local = preview_backoffice("preview-untick", venues).await;
        let mut page = open_preview(local).await.unwrap();
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETHUSDC", "weight": "1"}),
        )
        .await;
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "bybit", "symbol": "ETHUSDC", "weight": "1"}),
        )
        .await;
        snapshot_where(&mut page, |v| v["fresh"] == 2).await;

        send(
            &mut page,
            serde_json::json!({"type": "remove", "venue": "bybit"}),
        )
        .await;
        mocks.until_open(BYBIT, 0).await;
        let after = snapshot_where(&mut page, |v| v["venues"].as_array().unwrap().len() == 1).await;
        assert_eq!(after["venues"][0]["venue"], "binance");
        assert_eq!(after["fresh"], 1);
        assert_eq!(mocks.open(BINANCE_ETH), 1, "binance stays connected");
        assert_eq!(mocks.accepted(BINANCE_ETH), 1, "and was never redialled");

        // A new weight changes the average and nothing else.
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETHUSDC", "weight": "3"}),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
        assert_eq!(
            mocks.accepted(BINANCE_ETH),
            1,
            "a weight is not a reconnect"
        );

        // A new symbol closes the old market's connection and opens the new one.
        send(
            &mut page,
            serde_json::json!({"type": "set", "venue": "binance", "symbol": "BTCUSDC", "weight": "3"}),
        )
        .await;
        mocks.until_open(BINANCE_ETH, 0).await;
        mocks.until_open("/btcusdc@bookTicker", 1).await;

        drop(page);
        mocks.until_open("/btcusdc@bookTicker", 0).await;
    }

    /// Ticking and unticking the same venue over and over connects it at most once per
    /// PREVIEW_REDIAL_GAP, however fast the page sends.
    #[tokio::test]
    async fn a_venue_ticked_and_unticked_fast_is_not_redialled_fast() {
        let (mocks, venues) = MockVenues::start().await;
        let local = preview_backoffice("preview-flap", venues).await;
        let mut page = open_preview(local).await.unwrap();
        let tick = serde_json::json!({"type": "set", "venue": "binance", "symbol": "ETHUSDC", "weight": "1"});
        let untick = serde_json::json!({"type": "remove", "venue": "binance"});
        send(&mut page, tick.clone()).await;
        mocks.until_open(BINANCE_ETH, 1).await;
        let started = std::time::Instant::now();
        while started.elapsed() < PREVIEW_REDIAL_GAP - std::time::Duration::from_millis(500) {
            send(&mut page, untick.clone()).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            send(&mut page, tick.clone()).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(mocks.accepted(BINANCE_ETH), 1, "redialled inside the gap");
        // Once the gap has passed, the venue the page last ticked is connected again.
        mocks.until_open(BINANCE_ETH, 1).await;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while mocks.accepted(BINANCE_ETH) != 2 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("redialled once the gap passed");
    }

    /// A page on another site cannot open the preview, even from a browser that is logged
    /// in; the backoffice's own page can, whether reached by its tailnet address or by name.
    #[tokio::test]
    async fn only_the_backoffices_own_page_can_open_the_preview() {
        let (_mocks, venues) = MockVenues::start().await;
        let local = preview_backoffice("preview-origin", venues).await;
        for origin in ["https://evil.example", "http://127.0.0.1:1", "null"] {
            match open_preview_from(local, origin).await {
                Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                    assert_eq!(response.status(), 403, "{origin}")
                }
                other => panic!("{origin}: expected a 403, got {:?}", other.map(|_| ())),
            }
        }
        open_preview(local).await.expect("its own page opens it");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("host", "backoffice.example.com".parse().unwrap());
        headers.insert("origin", "https://backoffice.example.com".parse().unwrap());
        assert!(same_origin(&headers));
        headers.insert("origin", "https://grafana.example.com".parse().unwrap());
        assert!(!same_origin(&headers));
        headers.remove("origin");
        assert!(!same_origin(&headers), "no origin is not a browser page");
    }

    /// At most PREVIEW_PAGES pages at once; one more is refused before it opens anything,
    /// and closing a page frees its place.
    #[tokio::test]
    async fn the_number_of_open_preview_pages_is_capped() {
        let (_mocks, venues) = MockVenues::start().await;
        let local = preview_backoffice("preview-cap", venues).await;
        let mut pages = Vec::new();
        for _ in 0..PREVIEW_PAGES {
            pages.push(open_preview(local).await.unwrap());
        }
        match open_preview(local).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), 429)
            }
            other => panic!("expected a 429, got {:?}", other.map(|_| ())),
        }
        drop(pages.pop());
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while open_preview(local).await.is_err() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("a closed page frees its place");
    }

    /// Anything that is not an address is refused before it can reach CoinGecko's URL.
    #[tokio::test]
    async fn the_lookup_refuses_what_is_not_an_address() {
        let (path, state, _reloads) = config_on_disk("lookup-validation");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        for token0 in [
            "0xabc%3Fx%3D1",
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2%23x",
            "weth",
        ] {
            let answer = http(
                local,
                &format!(
                    "GET /pairs/lookup?token0={token0}&token1=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
                     HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
                ),
            )
            .await;
            assert!(answer.contains("400 Bad Request"), "{token0}: {answer}");
            assert!(answer.contains("not valid"), "{token0}: {answer}");
        }
    }

    /// The volatile knobs go through the same edit form and land in the file, which is what
    /// makes them tunable without a deploy.
    #[tokio::test]
    async fn the_volatile_knobs_are_edited_from_the_page() {
        let (path, state, mut reloads) = config_on_disk("knobs");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
        });

        // Deliberately omits max_deviation_window and max_deviation_window_blocks: a
        // browser holding this form open from before those keys existed posts exactly
        // this body, and it must still save rather than 422 on the two missing fields.
        let body = "key_env=UPDATER_KEY_WETH_USDC&symbol=ETHUSDC&pricing=volatile\
                    &min_mid=&max_mid=&max_deviation=&pricing_volatile_gamma=0.2\
                    &pricing_volatile_k=700&pricing_volatile_kappa=1\
                    &pricing_volatile_target_share=0.6&pricing_volatile_hold_secs=\
                    &pricing_volatile_fill_delay_secs=&pricing_volatile_volatility_window_secs=300\
                    &pricing_feed_delta=0.0005";
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        service.await.unwrap();
        assert!(posted.contains("303 See Other"), "{posted}");

        let written = config::load_raw(&path).unwrap();
        let pair = &written.pairs[0];
        let knob = |name: &str| pair.pricing.get(name).and_then(|v| v.as_str());
        assert_eq!(knob("kind"), Some("volatile"));
        assert_eq!(knob("gamma"), Some("0.2"));
        assert_eq!(knob("k"), Some("700"));
        assert_eq!(knob("kappa"), Some("1"));
        assert_eq!(knob("target_share"), Some("0.6"));
        assert_eq!(knob("volatility_window_secs"), Some("300"));
        assert_eq!(knob("hold_secs"), None, "a blank input is an absent key");
        assert_eq!(
            knob("delta"),
            None,
            "another kind's input is not this kind's"
        );
        // And the file parses as a volatile pair.
        let text = std::fs::read_to_string(&path).unwrap();
        let parsed = config::parse_config(&text, 18).unwrap();
        assert_eq!(parsed.pairs[0].pricing.kind, "volatile");
        // The index shows the kind and its summary, and the edit form opens on its section.
        let page = http(
            local,
            "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(page.contains("volatile γ=0.2 k=700 κ=1"), "{page}");
        let form = render_pair_form(Some(&written.pairs[0]), &test_kinds());
        assert!(
            form.contains(r#"id="pricing-volatile" name=pricing value="volatile" checked"#),
            "{form}"
        );
        assert!(
            form.contains("name=\"pricing_volatile_gamma\"\n         value=\"0.2\""),
            "{form}"
        );
    }

    /// Every kind's inputs are posted whichever section is showing, so the stanza is built
    /// from the chosen kind's alone; a blank choice keeps the stanza, an unknown kind is
    /// refused, and a kind with no fields keeps a stanza it already prices.
    /// A kind whose config reads numbers declares its fields as numbers, and the page writes
    /// them as TOML numbers; a value that is not one is refused rather than written.
    #[test]
    fn a_numeric_field_is_written_as_a_number() {
        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Numeric {
            #[allow(dead_code)]
            half_spread: f64,
            #[allow(dead_code)]
            age_secs: u64,
        }
        struct Fields;
        impl crate::pricing::Factory for Fields {
            type Config = Numeric;
            type Pricer = crate::pricing::FixedPricer;
            fn fields(&self) -> &'static [crate::pricing::FormField] {
                const FIELDS: &[crate::pricing::FormField] = &[
                    crate::pricing::FormField::number("half_spread", "0.0005", ""),
                    crate::pricing::FormField::number("age_secs", "60", ""),
                ];
                FIELDS
            }
            fn build<'a>(
                &'a self,
                _: &'a Numeric,
                _: &'a mut crate::pricing::BuildCtx,
            ) -> crate::pricing::BoxFuture<'a, eyre::Result<Self::Pricer>> {
                Box::pin(std::future::ready(Err(eyre!("not built here"))))
            }
        }
        let mut kinds = crate::kinds::Kinds::default();
        kinds.insert("numeric", Arc::new(Fields));
        let posted = |pairs: &[(&str, &str)]| -> std::collections::HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect()
        };
        let table = pricing_stanza(
            &kinds,
            "numeric",
            None,
            &posted(&[
                ("pricing_numeric_half_spread", "0.0005"),
                ("pricing_numeric_age_secs", "60"),
            ]),
        )
        .unwrap();
        assert_eq!(table.get("half_spread"), Some(&toml::Value::Float(0.0005)));
        assert_eq!(table.get("age_secs"), Some(&toml::Value::Integer(60)));
        let err = format!(
            "{:#}",
            pricing_stanza(
                &kinds,
                "numeric",
                None,
                &posted(&[("pricing_numeric_half_spread", "five")]),
            )
            .unwrap_err()
        );
        assert!(err.contains("`half_spread` must be a number"), "{err}");
    }

    #[test]
    fn the_pricing_choice_decides_which_kinds_fields_are_kept() {
        let kinds = test_kinds();
        let posted: std::collections::HashMap<String, String> = [
            ("pricing_fixed_mid", "1.0001"),
            ("pricing_fixed_delta", "0.0005"),
            ("pricing_feed_delta", ""),
            ("pricing_volatile_gamma", "0.1"),
            ("pricing_volatile_k", "2000"),
            ("pricing_volatile_kappa", "1"),
            ("pricing_volatile_hold_secs", ""),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        let text = |table: &toml::Table, key: &str| {
            table.get(key).and_then(|v| v.as_str()).map(str::to_owned)
        };

        let fixed = pricing_stanza(&kinds, "fixed", None, &posted).unwrap();
        assert_eq!(text(&fixed, "kind").as_deref(), Some("fixed"));
        assert_eq!(text(&fixed, "mid").as_deref(), Some("1.0001"));
        assert_eq!(text(&fixed, "delta").as_deref(), Some("0.0005"));
        assert_eq!(fixed.get("gamma"), None);

        let volatile = pricing_stanza(&kinds, "volatile", None, &posted).unwrap();
        assert_eq!(text(&volatile, "gamma").as_deref(), Some("0.1"));
        assert_eq!(text(&volatile, "k").as_deref(), Some("2000"));
        assert_eq!(volatile.get("hold_secs"), None, "blank is absent");
        assert_eq!(volatile.get("delta"), None);

        let feed = pricing_stanza(&kinds, "feed", None, &posted).unwrap();
        assert_eq!(
            feed.len(),
            1,
            "a blank delta leaves only the kind: {feed:?}"
        );

        // No choice posted: an older form. The stanza stays as it is.
        let existing: toml::Table =
            toml::from_str("kind = \"skewed\"\nhalf_spread = 0.0005").unwrap();
        assert_eq!(
            pricing_stanza(&kinds, "", Some(&existing), &posted).unwrap(),
            existing
        );
        assert!(pricing_stanza(&kinds, "", None, &posted).is_err());
        // A kind with no fields keeps the stanza it already prices, and names itself alone
        // on a pair it did not.
        assert_eq!(
            pricing_stanza(&kinds, "skewed", Some(&existing), &posted).unwrap(),
            existing
        );
        let bare = pricing_stanza(&kinds, "skewed", None, &posted).unwrap();
        assert_eq!(bare.len(), 1);
        // A kind nobody registered names the ones that are.
        let err = pricing_stanza(&kinds, "nope", None, &posted)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope") && err.contains("volatile"), "{err}");
    }

    /// The form opens on the section of the kind that prices the pair, offers every
    /// registered kind, and carries each kind's help and fields under their posted names.
    #[test]
    fn the_pair_form_opens_on_the_pairs_pricing() {
        let form = render_pair_form(Some(&a_config().pairs[0]), &test_kinds());
        assert!(
            form.contains(r#"id="pricing-feed" name=pricing value="feed" checked"#),
            "{form}"
        );
        assert!(!form.contains(r#"value="volatile" checked"#), "{form}");
        assert!(form.contains("ln(1 + γ/k)"), "{form}");
        for name in [
            "pricing_volatile_gamma",
            "pricing_volatile_k",
            "pricing_volatile_kappa",
            "pricing_volatile_target_share",
            "pricing_volatile_hold_secs",
            "pricing_volatile_fill_delay_secs",
            "pricing_volatile_volatility_window_secs",
            "pricing_volatile_inventory_aversion",
            "pricing_volatile_inventory_band_lower",
            "pricing_volatile_inventory_band_upper",
            "pricing_volatile_inventory_aversion_hard",
            "pricing_volatile_inventory_band_hard_lower",
            "pricing_volatile_inventory_band_hard_upper",
            "pricing_feed_delta",
            "pricing_fixed_mid",
            "pricing_fixed_delta",
        ] {
            assert!(
                form.contains(&format!(r#"name="{name}""#)),
                "{name} is missing from the form"
            );
        }
        // A new pair starts on the first registered kind.
        let new = render_pair_form(None, &test_kinds());
        assert!(
            new.contains(r#"id="pricing-feed" name=pricing value="feed" checked"#),
            "{new}"
        );
    }

    /// The two window fields are ordinary `SOURCE_FIELDS` entries, appended after
    /// `max_deviation` — a regression guard for the length-mismatched `zip` this would
    /// silently produce: `source_values` for the "editing" branch is a hand-written list,
    /// so an unmatched addition to `SOURCE_FIELDS` drops the last fields from the rendered
    /// form instead of failing to compile.
    #[test]
    fn the_window_fields_render_on_both_forms_and_edit_shows_the_current_value() {
        for name in ["max_deviation_window", "max_deviation_window_blocks"] {
            assert!(
                render_pair_form(None, &test_kinds()).contains(&format!(r#"name="{name}""#)),
                "{name} is missing from the new-pair form"
            );
        }

        let mut config = a_config();
        config.pairs[0].max_deviation_window = Some("0.20".to_owned());
        config.pairs[0].max_deviation_window_blocks = Some("1000".to_owned());
        let form = render_pair_form(Some(&config.pairs[0]), &test_kinds());
        assert!(
            form.contains("name=\"max_deviation_window\"\n         value=\"0.20\""),
            "{form}"
        );
        assert!(
            form.contains("name=\"max_deviation_window_blocks\"\n         value=\"1000\""),
            "{form}"
        );
    }

    /// A value an operator typed and left blank means "leave this key out", not "set it to
    /// the empty string", which every one of these keys would reject.
    #[test]
    fn a_blank_field_is_an_absent_key() {
        assert_eq!(optional(""), None);
        assert_eq!(optional("   "), None);
        assert_eq!(optional("  0.02 "), Some("0.02".to_owned()));
    }

    /// A key missing from the page is one nobody can change without SSH.
    #[test]
    fn the_settings_page_covers_every_key() {
        // Round-tripped through the writer, so this compares against the real serialised
        // field names rather than a list written out by hand beside the struct.
        let full = config::FileSettings {
            mode: Some("builder".to_owned()),
            rpc_url: Some("https://a".to_owned()),
            rpc_ws_url: Some("wss://a".to_owned()),
            registry: Some("0x1".to_owned()),
            price_decimals: Some(18),
            binance_ws: Some("wss://b".to_owned()),
            endpoints: Default::default(),
            interval: Some(12),
            requote_ms: Some(50),
            metrics_addr: Some("127.0.0.1:9464".to_owned()),
            disable_cross_region: Some(true),
            backoffice_addr: Some("100.1.2.3:8088".to_owned()),
        };
        let rendered = toml::to_string_pretty(&full).unwrap();
        let keys: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.split_once(" = ").map(|(key, _)| key))
            .collect();
        assert_eq!(keys.len(), 11, "the struct grew: {keys:?}");

        let mut config = a_config();
        config.settings = full;
        let page = render_settings_form(&config);
        for key in keys {
            // `binance_ws` is the older spelling of the Binance row of the endpoint table,
            // and its value is what that row shows.
            let key = if key == "binance_ws" {
                assert!(page.contains(r#"value="wss://b""#), "{page}");
                "endpoint_binance"
            } else {
                key
            };
            // disable_cross_region is the checkbox, not a text input.
            let present = page.contains(&format!(r#"name="{key}""#))
                || page.contains(&format!("name={key}>"))
                || page.contains(&format!("name={key} "));
            assert!(present, "{key} is not on the settings page");
        }
        // And the endpoint table has a row per venue.
        for venue in VenueId::ALL {
            assert!(
                page.contains(&format!(r#"name="endpoint_{venue}""#)),
                "{venue} has no endpoint row"
            );
        }
    }

    /// The endpoint table is saved as `[settings.endpoints]`, held to each venue's
    /// transport, and a stale form's `binance_ws` lands in the Binance row.
    #[test]
    fn venue_endpoints_are_saved_under_their_venue() {
        let mut form = SaveSettings {
            mode: "builder".to_owned(),
            rpc_url: "https://a".to_owned(),
            rpc_ws_url: String::new(),
            registry: String::new(),
            price_decimals: String::new(),
            binance_ws: "wss://old-binance".to_owned(),
            endpoints: EndpointsForm {
                endpoint_kraken: "wss://kraken-mock".to_owned(),
                endpoint_mexc: "https://mexc-mock".to_owned(),
                ..EndpointsForm::default()
            },
            interval: String::new(),
            requote_ms: "50".to_owned(),
            metrics_addr: String::new(),
            backoffice_addr: String::new(),
            disable_cross_region: None,
        };
        let built = build_settings(&form).unwrap();
        assert_eq!(built.binance_ws, None, "moved into the table");
        assert_eq!(
            built.endpoints,
            [
                ("binance".to_owned(), "wss://old-binance".to_owned()),
                ("kraken".to_owned(), "wss://kraken-mock".to_owned()),
                ("mexc".to_owned(), "https://mexc-mock".to_owned()),
            ]
            .into()
        );
        // The row wins over the stale field.
        form.endpoints.endpoint_binance = "wss://new-binance".to_owned();
        assert_eq!(
            build_settings(&form).unwrap().endpoints["binance"],
            "wss://new-binance"
        );
        // A streamed venue is held to ws(s), a polled one to http(s).
        form.endpoints.endpoint_kraken = "https://kraken-mock".to_owned();
        let err = build_settings(&form).unwrap_err().to_string();
        assert!(err.contains("kraken endpoint must be ws"), "{err}");
        form.endpoints.endpoint_kraken = String::new();
        form.endpoints.endpoint_mexc = "wss://mexc-mock".to_owned();
        let err = build_settings(&form).unwrap_err().to_string();
        assert!(err.contains("mexc endpoint must be http"), "{err}");
    }

    /// The current values come back in the inputs, so saving one key does not blank the
    /// others.
    #[test]
    fn the_settings_page_shows_the_current_values() {
        let mut config = a_config();
        config.settings.requote_ms = Some(25);
        config.settings.rpc_url = Some("https://from-the-file".to_owned());
        let page = render_settings_form(&config);
        for expected in [
            r#"name="requote_ms""#,
            r#"value="25""#,
            r#"name="rpc_url""#,
            r#"value="https://from-the-file""#,
        ] {
            assert!(page.contains(expected), "{expected} missing from:\n{page}");
        }
    }

    /// A bad value here is a service that fails to come back from a restart hours later.
    #[test]
    fn a_malformed_setting_is_refused_before_it_is_written() {
        fn form() -> SaveSettings {
            SaveSettings {
                mode: "builder".to_owned(),
                rpc_url: "https://a".to_owned(),
                rpc_ws_url: String::new(),
                registry: String::new(),
                price_decimals: String::new(),
                binance_ws: String::new(),
                endpoints: EndpointsForm::default(),
                interval: String::new(),
                requote_ms: "50".to_owned(),
                metrics_addr: "127.0.0.1:9464".to_owned(),
                backoffice_addr: "100.1.2.3:8088".to_owned(),
                disable_cross_region: None,
            }
        }

        let built = build_settings(&form()).expect("a good form saves");
        assert_eq!(built.requote_ms, Some(50));
        // A blank field is an absent key, not an empty string: every one of these rejects
        // an empty value, so writing "" would be a config that no longer loads.
        assert_eq!(built.registry, None);
        assert_eq!(built.price_decimals, None);
        assert_eq!(built.disable_cross_region, None);

        type Mutate = Box<dyn Fn(&mut SaveSettings)>;
        let cases: Vec<(&str, Mutate)> = vec![
            ("mode", Box::new(|f| f.mode = "builders".to_owned())),
            (
                "metrics_addr",
                Box::new(|f| f.metrics_addr = "nope".to_owned()),
            ),
            ("registry", Box::new(|f| f.registry = "0xnothex".to_owned())),
            ("requote_ms", Box::new(|f| f.requote_ms = "fast".to_owned())),
            (
                "backoffice_addr",
                Box::new(|f| f.backoffice_addr = "8088".to_owned()),
            ),
            // Each of these parses, saves, and kills the next start; see the twin check
            // named in `build_settings`.
            (
                "backoffice_addr",
                Box::new(|f| f.backoffice_addr = "0.0.0.0:8088".to_owned()),
            ),
            ("node", Box::new(|f| f.mode = "node".to_owned())),
            (
                "price_decimals",
                Box::new(|f| f.price_decimals = "60".to_owned()),
            ),
            ("interval", Box::new(|f| f.interval = "0".to_owned())),
            ("rpc_url", Box::new(|f| f.rpc_url = "not a url".to_owned())),
            (
                "rpc_ws_url",
                Box::new(|f| f.rpc_ws_url = "https://a".to_owned()),
            ),
            (
                "binance endpoint",
                Box::new(|f| f.binance_ws = "stream.binance.com".to_owned()),
            ),
        ];
        // And the values startup accepts still save.
        let mut fine = form();
        fine.price_decimals = "59".to_owned();
        fine.interval = "1".to_owned();
        fine.rpc_ws_url = "wss://a".to_owned();
        fine.binance_ws = "wss://stream.binance.com:9443/ws".to_owned();
        build_settings(&fine).expect("values startup accepts must save");
        let mut node = form();
        node.mode = "node".to_owned();
        node.backoffice_addr = String::new();
        build_settings(&node).expect("node mode without a backoffice saves");
        for (key, mutate) in cases {
            let mut bad = form();
            mutate(&mut bad);
            let err = match build_settings(&bad) {
                Ok(_) => panic!("{key}: expected a rejection"),
                Err(err) => format!("{err:#}"),
            };
            assert!(err.contains(key), "{key}: {err}");
        }
    }

    /// The one live credential this page takes.
    #[test]
    fn only_the_api_key_field_is_masked() {
        let form = render_builder_form();
        assert!(form.contains(r#"type="password""#), "{form}");
        assert!(form.contains(r#"autocomplete="new-password""#), "{form}");
        // And only that one field: a masked endpoint or name would just make the form
        // harder to check.
        assert_eq!(form.matches(r#"type="password""#).count(), 1, "{form}");

        let pair = render_pair_form(None, &test_kinds());
        assert!(!pair.contains(r#"type="password""#), "{pair}");
    }

    /// Characters in the config that would otherwise end an attribute and swallow the form.
    #[test]
    fn rendered_values_are_escaped() {
        let mut config = a_config();
        config.builders[0].name = "ti\"tan<eu>&".to_owned();
        let html = render_index(
            &config,
            Path::new("config.toml"),
            &Banner {
                ok: None,
                err: None,
            },
            &PairStates::default(),
            &test_kinds(),
        );
        assert!(!html.contains("ti\"tan<eu>&"), "{html}");
        // askama's numeric entities, not the named ones a hand-rolled escaper would emit.
        assert!(html.contains("ti&#34;tan&#60;eu&#62;&#38;"), "{html}");
    }

    /// What axum's `Query` extractor does, so the assertion is about the encoder.
    fn urldecode(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'+' => {
                    out.push(b' ');
                    i += 1;
                }
                b'%' if i + 2 < bytes.len() => {
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                    out.push(u8::from_str_radix(hex, 16).unwrap());
                    i += 3;
                }
                byte => {
                    out.push(byte);
                    i += 1;
                }
            }
        }
        String::from_utf8(out).unwrap()
    }

    /// A raw `&` would truncate the message at the query separator.
    #[test]
    fn the_banner_round_trips_through_the_query_string() {
        let message = "pair \"WETH/USDC\" was rejected: min_mid > max_mid (2% & rising)";
        let encoded = urlencode(message);
        // Not '%': it is the escape marker, so it appears in the output by construction.
        // The message's own '%' is checked by the round-trip below.
        for raw in [' ', '"', '&', '='] {
            assert!(!encoded.contains(raw), "{raw:?} left raw in {encoded}");
        }
        assert_eq!(urldecode(&encoded), message);
    }

    /// A renamed route leaves a button that 405s and a link that 404s, invisible until
    /// someone clicks it.
    #[test]
    fn every_form_and_link_reaches_a_real_route() {
        let config = a_config();
        let index = render_index(
            &config,
            Path::new("config.toml"),
            &Banner {
                ok: None,
                err: None,
            },
            &PairStates::default(),
            &test_kinds(),
        );
        for target in [
            "action=/reload",
            "action=/pairs/remove",
            "action=/builders/remove",
            r#"href="/pairs/new""#,
            r#"href="/builders/new""#,
            r#"href="/settings""#,
            r#"href="/pairs/edit/UPDATER_KEY_WETH_USDC""#,
        ] {
            assert!(index.contains(target), "the index has no {target}");
        }

        let new_pair = render_pair_form(None, &test_kinds());
        assert!(new_pair.contains(r#"action="/pairs/add""#), "{new_pair}");
        let edit_pair = render_pair_form(Some(&config.pairs[0]), &test_kinds());
        assert!(edit_pair.contains(r#"action="/pairs/edit""#), "{edit_pair}");
        assert!(render_builder_form().contains("action=/builders/add"));
    }

    const CUSTOM_PAIR: &str = r#"target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
symbol  = "ETHUSDC"
key_env = "UPDATER_KEY_WETH_USDC"

[pairs.pricing]
kind = "skewed"
half_spread = 0.0005

[[builder]]
name = "titan-eu"
endpoint = "wss://eu.rpc.titanbuilder.xyz/ws/sendquoteupdate"
api_key = "k"
"#;

    /// The backoffice cannot edit the stanza of a kind that declares no fields, so it must
    /// carry it through an edit of anything else untouched, and add no other kind's keys.
    #[tokio::test]
    async fn editing_a_custom_pair_keeps_its_stanza_and_adds_no_pricing_keys() {
        let (path, state, mut reloads) = config_on_disk("custom-stanza");
        std::fs::write(&path, CUSTOM_PAIR).unwrap();
        let before = config::load_raw(&path).unwrap().pairs[0].pricing.clone();
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
        });
        // Every kind's inputs are posted; with the pair's own kind chosen, none of them is
        // read and the stanza the page has no form for is kept as it was.
        let body = "key_env=UPDATER_KEY_WETH_USDC&symbol=ETHUSDC&pricing=skewed\
                    &pricing_fixed_mid=1&pricing_fixed_delta=0.001\
                    &min_mid=0.0002&max_mid=&max_deviation=&pricing_volatile_gamma=0.1";
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        service.await.unwrap();
        assert!(posted.contains("303 See Other"), "{posted}");

        let pair = &config::load_raw(&path).unwrap().pairs[0];
        assert_eq!(pair.pricing, before, "the stanza survives value-equal");
        assert_eq!(pair.min_mid.as_deref(), Some("0.0002"));
    }

    /// What a browser posts from the page's first form: every named `<input>`, a checkbox
    /// or radio only when checked, values unescaped. Enough HTML for these templates
    /// (attribute values quoted or bare; the pair form has no `<select>` or `<textarea>`).
    fn posted_fields(page: &str) -> Vec<(String, String)> {
        let form = page
            .split("</form>")
            .next()
            .expect("the page has a form")
            .split_once("<form")
            .expect("the page has a form")
            .1;
        let mut fields = Vec::new();
        for tag in form.split("<input").skip(1) {
            let tag = tag.split('>').next().unwrap_or_default();
            let mut attrs = std::collections::HashMap::new();
            let mut rest = tag.trim_start();
            while !rest.is_empty() {
                let end = rest
                    .find(|c: char| c == '=' || c.is_whitespace())
                    .unwrap_or(rest.len());
                let name = rest[..end].to_owned();
                rest = &rest[end..];
                let value = if let Some(after) = rest.strip_prefix('=') {
                    let (value, after) = match after.strip_prefix('"') {
                        Some(quoted) => quoted.split_once('"').expect("a closed quote"),
                        None => {
                            after.split_at(after.find(char::is_whitespace).unwrap_or(after.len()))
                        }
                    };
                    rest = after;
                    value.to_owned()
                } else {
                    String::new()
                };
                attrs.insert(name, value);
                rest = rest.trim_start();
            }
            let Some(name) = attrs.get("name") else {
                continue;
            };
            let kind = attrs.get("type").map(String::as_str).unwrap_or("text");
            if matches!(kind, "checkbox" | "radio") && !attrs.contains_key("checked") {
                continue;
            }
            let value = attrs
                .get("value")
                .cloned()
                .unwrap_or_else(|| "on".to_owned())
                .replace("&#34;", "\"")
                .replace("&#39;", "'")
                .replace("&#60;", "<")
                .replace("&#62;", ">")
                .replace("&#38;", "&");
            fields.push((name.clone(), value));
        }
        fields
    }

    /// The custom pair's own page, saved with exactly what it posts, read off the rendered
    /// form rather than written by hand: the page posts neither `mid` nor `delta`, and a
    /// body written to match the handler instead of the form is how this page shipped
    /// unsavable (a 422 for want of `mid`).
    #[tokio::test]
    async fn a_custom_pairs_page_saves_with_exactly_what_it_posts() {
        let (path, state, mut reloads) = config_on_disk("custom-form");
        std::fs::write(&path, CUSTOM_PAIR).unwrap();
        let raw = config::load_raw(&path).unwrap();
        let before = raw.pairs[0].pricing.clone();
        let mut fields = posted_fields(&render_pair_form(Some(&raw.pairs[0]), &test_kinds()));
        assert!(
            fields.iter().any(|(name, _)| name == "key_env")
                && !fields
                    .iter()
                    .any(|(name, _)| name == "mid" || name == "delta"),
            "{fields:?}"
        );
        // The operator's edit: a floor on the mid.
        for (name, value) in &mut fields {
            if name == "min_mid" {
                *value = "0.0002".to_owned();
            }
        }
        let body = fields
            .iter()
            .map(|(name, value)| format!("{}={}", urlencode(name), urlencode(value)))
            .collect::<Vec<_>>()
            .join("&");
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
        });
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        assert!(posted.contains("303 See Other"), "{posted}");
        service.await.unwrap();

        let pair = &config::load_raw(&path).unwrap().pairs[0];
        assert_eq!(pair.pricing, before, "the stanza survives value-equal");
        assert_eq!(pair.min_mid.as_deref(), Some("0.0002"));
        assert_eq!(
            pair.symbol.as_deref(),
            Some("ETHUSDC"),
            "its market is kept"
        );
    }

    /// A body with no pricing choice (a page from before the choice existed, a hand-made
    /// request) keeps the pair's stanza as it is rather than reading as "clear it".
    #[tokio::test]
    async fn an_edit_without_a_pricing_choice_keeps_the_stanza() {
        let (path, state, mut reloads) = config_on_disk("no-choice");
        let before = config::load_raw(&path).unwrap().pairs[0].pricing.clone();
        let (local, _task) = bind(
            "127.0.0.1:0".parse().unwrap(),
            path.clone(),
            state.reloads.clone(),
            test_client(),
            PairStates::default(),
            std::sync::Arc::new(tokio::sync::Mutex::new(())),
            test_kinds(),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            let request = reloads.recv().await.expect("a reload must be requested");
            let _ = request.reply.send(Ok("reload: 1 restarted".to_owned()));
        });
        let body =
            "key_env=UPDATER_KEY_WETH_USDC&symbol=ETHUSDC&min_mid=0.0002&max_mid=&max_deviation=";
        let posted = http(
            local,
            &format!(
                "POST /pairs/edit HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\
                 Content-Type: application/x-www-form-urlencoded\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        service.await.unwrap();
        assert!(posted.contains("303 See Other"), "{posted}");
        let pair = &config::load_raw(&path).unwrap().pairs[0];
        assert_eq!(pair.pricing, before);
        assert_eq!(pair.min_mid.as_deref(), Some("0.0002"));
    }

    #[test]
    fn the_pair_form_says_where_a_custom_pairs_pricing_is_edited() {
        let config: config::RawConfig = toml::from_str(CUSTOM_PAIR).unwrap();
        let form = render_pair_form(Some(&config.pairs[0]), &test_kinds());
        for expected in ["[pairs.pricing]", "config.toml", "skewed"] {
            assert!(form.contains(expected), "{expected}:\n{form}");
        }
        assert!(
            form.contains(r#"id="pricing-skewed" name=pricing value="skewed" checked"#),
            "{form}"
        );
        assert!(!form.contains(r#"name="pricing_skewed_"#), "{form}");
    }
}
