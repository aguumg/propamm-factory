//! Startup validation and the `--check` report: resolves what each pair will actually
//! publish, proves the chain agrees, and prints it for a human to sanity-check before a
//! single update is sent.

use ethrex_common::{Address, U256};
use std::{collections::HashMap, fmt::Write as _};

/// Below this many updates of runway, a pair's key is flagged. A key that runs dry stops
/// quoting exactly one pair while the process still looks healthy.
pub const LOW_RUNWAY: u64 = 1_000;

/// The outcome of comparing a pair's declared Binance stream against its tokens' on-chain
/// symbols. Separated from `build_report` so every branch is testable without a chain.
#[derive(Debug, PartialEq)]
pub enum SymbolCheck {
    /// The stream matches the declared pair.
    Match,
    /// The stream is the pair's base quoted in another dollar (`dollar_stand_ins`): ETH-USD
    /// for a WETH/USDC pair. Accepted, with a warning; not a mismatch.
    StandIn { quoted_in: String },
    /// The stream does not match, and the pair did not opt out.
    Mismatch { expected: String },
    /// The stream does not match, but the pair set allow_symbol_mismatch.
    MismatchAllowed { expected: String },
    /// One or both symbols could not be read, so the check could not run at all.
    Unreadable,
}

/// Compares the configured `stream` at `venue` against the pair's on-chain `base`/`quote`
/// symbols, spelled the way that venue spells them.
///
/// `allow_mismatch` only ever downgrades a `Mismatch` to a `MismatchAllowed` — it must
/// never turn `Unreadable` into anything else. `allow_symbol_mismatch` means "I know this
/// stream differs from the tokens"; it is not a claim that the check itself may be
/// skipped, so a missing symbol always surfaces as `Unreadable` regardless of the flag.
pub fn check_symbol(
    venue: crate::venue::VenueId,
    stream: &str,
    base: Option<&str>,
    quote: Option<&str>,
    allow_mismatch: bool,
) -> SymbolCheck {
    let (base, quote) = match (base, quote) {
        (Some(base), Some(quote)) => (base, quote),
        _ => return SymbolCheck::Unreadable,
    };
    let expected = venue.expected_symbol(base, quote);
    // Case-insensitive, because the venues are: Binance lowercases the symbol into the
    // stream URL, so "ethusdc" is the canonical stream name and streams prices perfectly.
    // Comparing exactly would hard-fail a config that works, and send the operator to set
    // `allow_symbol_mismatch = true` on a pair that does not mismatch.
    if stream.eq_ignore_ascii_case(&expected) {
        return SymbolCheck::Match;
    }
    // A USDC or USDT pair read from the base's market in another of those dollars or USD
    // is a choice the lookup makes on purpose, not a wrong asset, so it passes without the
    // opt-out. A different base, or any other quote, is still a mismatch.
    let quote_name = crate::venues::unwrap_alias(&quote.to_uppercase()).to_owned();
    if let Some(dollar) = crate::venues::dollar_stand_ins(&quote_name)
        .iter()
        .find(|d| stream.eq_ignore_ascii_case(&venue.expected_symbol(base, d)))
    {
        return SymbolCheck::StandIn {
            quoted_in: (*dollar).to_owned(),
        };
    }
    if allow_mismatch {
        SymbolCheck::MismatchAllowed { expected }
    } else {
        SymbolCheck::Mismatch { expected }
    }
}

/// What the code at the target address says about whether the target-side checks — the
/// registered-pair and vault-balance reads in `build_report` — can run at all.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TargetKind {
    /// Ordinary contract code: run the target-side checks.
    Contract,
    /// No code. A target need not be a contract at all — any address can be one, it only
    /// has to call `addUpdater` — so this skips rather than fails.
    ///
    /// Note this is *not* what a run against a mainnet fork with an anvil account as its
    /// target hits: on a real mainnet fork that address (anvil index 1) carries a 7702
    /// delegation, so it classifies as `DelegatedEoa` below. Verified by running one.
    Eoa,
    /// EIP-7702: an EOA carrying the 23-byte `0xef0100 ‖ address` delegation designator.
    DelegatedEoa,
    /// The code read itself failed, so nothing is known. Never treated as a contract: a
    /// failed read must not default to a reassuring value.
    Unknown,
}

/// Classifies the target from its code, where `None` means the `eth_getCode` call failed.
///
/// This is what retires the "has code ⇒ is a PropAMM" assumption, rather than patching
/// the branches where it happened to blow up. A delegated EOA has code but is not a
/// contract, and calling `pairVaults(uint256)` on it reaches whatever it delegates to:
/// today that reverts or returns undecodable data, but a delegate whose fallback returns
/// 32 zero bytes decodes cleanly as the zero address and would read as "lane is not a
/// registered pair", refusing to start a run against a perfectly good config. That is reachable, not
/// theoretical — the Makefile's `TARGET_ADDR` is anvil account 1, whose private key is
/// public, so anyone can re-delegate it at any time.
pub fn classify_target(code: Option<&[u8]>) -> TargetKind {
    match code {
        None => TargetKind::Unknown,
        Some([]) => TargetKind::Eoa,
        // EIP-7702 fixes both the length and the prefix, so this cannot collide with real
        // bytecode: no contract is exactly 23 bytes starting 0xef0100 (0xef is a reserved
        // opcode and EIP-3541 forbids deploying code that starts with it).
        Some(code) if code.len() == 23 && code.starts_with(&[0xef, 0x01, 0x00]) => {
            TargetKind::DelegatedEoa
        }
        Some(_) => TargetKind::Contract,
    }
}

/// The report-level warning a target's classification deserves, or `None` when the
/// target-side checks can simply run.
///
/// Every kind except `Contract` skips those checks, and every skip has to say so. The
/// three reasons are not interchangeable: `Unknown` means the RPC failed and the target may
/// be a perfectly good PropAMM, `DelegatedEoa` means it has code that is not its own, and
/// `Eoa` means there is no code at all — deliberate when a test run names a plain account
/// as its target, but also what a mistyped or not-yet-deployed address looks like. Reporting nothing for `Eoa` left the one
/// case an operator is most likely to hit by accident reading as a clean report.
pub fn target_warning(target: Address, kind: TargetKind) -> Option<String> {
    let skipped = "so the target-side checks (registered pair, vault balance) were skipped";
    match kind {
        TargetKind::Contract => None,
        TargetKind::Unknown => Some(format!(
            "could not read code at target {target:#x}, {skipped} because the RPC failed, not \
             because the target is not a contract"
        )),
        TargetKind::DelegatedEoa => Some(format!(
            "target {target:#x} is an EIP-7702 delegated EOA — its code is the 23-byte \
             0xef0100 designator, not contract code — {skipped}; whatever it delegates to \
             can change without notice and is not this PropAMM"
        )),
        TargetKind::Eoa => Some(format!(
            "target {target:#x} has no code, {skipped}; a target need not be a contract, so \
             this may be deliberate — but it is also what a mistyped or not-yet-deployed \
             PropAMM address looks like"
        )),
    }
}

/// What `pairVaults(lane)` answered about the target: the account it fills this lane from,
/// which is also whether it reads the lane at all, since a pair is registered exactly when
/// its vault is set.
#[derive(Debug, PartialEq)]
pub enum RegisteredPair {
    /// The target reads this lane and fills it from this account.
    Registered(Address),
    /// The target answered, and the answer was the zero address: no such pair.
    NotRegistered,
    /// The call failed, or its answer did not decode as an address, so the check did not run.
    /// Kept distinct from `NotRegistered` because one of them is a hard failure and the
    /// other is an un-run check — which must be reported, never silently dropped.
    Unreadable,
}

/// Classifies `pairVaults(lane)`'s answer, where `None` means the call itself failed.
/// Separated from `build_report` so every branch is testable without a chain, like
/// [`check_symbol`].
pub fn check_registered_pair(ret: Option<&[u8]>) -> RegisteredPair {
    let Some(ret) = ret else {
        return RegisteredPair::Unreadable;
    };
    match decode_return_data("r(address)", ret).as_deref() {
        Ok([Value::Address(vault)]) if vault.is_zero() => RegisteredPair::NotRegistered,
        Ok([Value::Address(vault)]) => RegisteredPair::Registered(*vault),
        _ => RegisteredPair::Unreadable,
    }
}

/// The single value an eth_call answered, or `None` when the call failed (`ret` is `None`)
/// or its answer did not decode as `pseudo_sig`. Both mean the same thing to a caller: the
/// check that needed this value did not run.
pub fn decode_one(pseudo_sig: &str, ret: Option<&[u8]>) -> Option<Value> {
    decode_return_data(pseudo_sig, ret?)
        .ok()?
        .into_iter()
        .next()
}

/// The failures to record for pairs whose keys resolve to the *same* signer address,
/// returned as `(row index, failure)` so the caller can attach each to its own row.
///
/// `resolve_keys` resolves every pair's key independently, so two pairs holding the same
/// key each look fine on their own row; only `resolve_pairs` — the run path — compares
/// addresses. Without this, an operator who copy-pastes three `UPDATER_KEY_*` lines and
/// forgets to change one gets three green rows and exit 0 from `--check`, funds and
/// authorizes two addresses, and discovers the problem when the service refuses to start.
/// Sharing an address means sharing a nonce, so only one of the two lanes could ever land
/// per block — the reason the run refuses. See `resolve_pairs`.
pub fn duplicate_updater_failures(rows: &[Row]) -> Vec<(usize, String)> {
    let mut failures = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        // Rows whose key did not resolve carry no address, and two *unresolved* rows are
        // not two rows sharing a key.
        let Some(address) = row.updater else { continue };
        let others: Vec<&str> = rows
            .iter()
            .enumerate()
            .filter(|(j, other)| *j != i && other.updater == Some(address))
            .map(|(_, other)| other.key_env.as_str())
            .collect();
        if !others.is_empty() {
            failures.push((
                i,
                format!(
                    "{address:#x} is also the updater of {}; each pair needs its own key so \
                     their nonces stay independent, and a run refuses to start on this config",
                    others.join(", ")
                ),
            ));
        }
    }
    failures
}

/// The failure to record about a pair's authorization, or `None` when it is authorized.
///
/// `authorized` is `Some(false)` only when the registry actually answered "no"; `None`
/// means the `isUpdater` call failed or did not decode, so the check did not run. The two
/// must not share a message: `Some(false)`'s remedy is `addUpdater`, and giving that remedy
/// for an unreadable answer sends the operator to spend a mainnet transaction authorizing a
/// key that may already be authorized, watch the run fail again, and have no reason to
/// suspect the endpoint. Both are failures — a run must not start on an authorization
/// nobody could confirm — but they point at different things.
pub fn authorization_failure(
    target: Address,
    updater: Address,
    authorized: Option<bool>,
) -> Option<String> {
    match authorized {
        Some(true) => None,
        Some(false) => Some(format!(
            "{updater:#x} is not an authorized updater; from the PropAMM owner call \
             addUpdater({updater:#x}) on {target:#x}"
        )),
        None => Some(format!(
            "could not check whether {updater:#x} is an authorized updater: the \
             isUpdater({target:#x}, {updater:#x}) call on the registry failed or did not \
             return a bool. This is the RPC endpoint, not the authorization — do not call \
             addUpdater until the check runs"
        )),
    }
}

/// What a pair's row should say about funding, from a balance and a base fee that may each
/// have been unreadable: the runway to render, and one warning if there is anything to say.
///
/// The runway is `Some` exactly when both inputs were readable, so the column renders
/// `unknown` in precisely the cases where it is unknown. A failed balance read must never
/// render as "has no ETH": that is a definite, false statement whose remedy — top the key up
/// — changes nothing, because the key may be perfectly funded and only the RPC unreachable.
/// It is the mirror of the unreadable base fee that had to stop rendering as "unlimited".
pub fn runway_row(
    updater: Address,
    balance: Option<U256>,
    base_fee: Option<U256>,
) -> (Option<u64>, Option<String>) {
    let Some(balance) = balance else {
        return (
            None,
            Some(format!(
                "could not read {updater:#x}'s balance, so its funding runway is unknown; \
                 this is not a claim that the key is empty"
            )),
        );
    };
    // `None` (rather than a computed value) when the base fee could not be read — the
    // report-level warning about that covers every pair at once, so no per-row line here.
    let runway = base_fee.map(|fee| runway_updates(balance, UPDATE_GAS_LIMIT, fee));
    let warning = match runway {
        Some(0) => Some(format!(
            "{updater:#x} has no ETH; its updates cannot pay base fee"
        )),
        Some(runway) if runway < LOW_RUNWAY => Some(format!(
            "{updater:#x} covers only ~{runway} updates at the current base fee"
        )),
        _ => None,
    };
    (runway, warning)
}

/// How many more updates this balance can pay for at this base fee. Truncates: a partial
/// update is not an update.
pub fn runway_updates(balance: U256, gas_limit: u64, base_fee: U256) -> u64 {
    // `checked_mul`, because U256's `*` panics on overflow and `base_fee` comes off the
    // wire. A base fee big enough to overflow this product is one no balance could cover,
    // so zero is the arithmetically correct answer rather than a fallback.
    let Some(per_update) = U256::from(gas_limit).checked_mul(base_fee) else {
        return 0;
    };
    if per_update.is_zero() {
        // A zero base fee (anvil with no fee market) buys unlimited updates.
        return u64::MAX;
    }
    let updates = balance / per_update;
    if updates > U256::from(u64::MAX) {
        u64::MAX
    } else {
        updates.as_u64()
    }
}

/// Width of the `breaker` column. Shared with the test in `breaker` that keeps `percent`'s
/// longest rendering inside it, so the two cannot drift apart: widening the column here
/// without the rendering, or the other way round, fails that test.
pub(crate) const BREAKER_COLUMN_WIDTH: usize = 11;

/// Width of the `window` column: `percent`'s own longest rendering (a sub-basis-point
/// threshold like `"0.00000010%"`) plus `@`, the widest legal block count
/// (`MAX_BREAKER_WINDOW_BLOCKS`, four digits) and its trailing `b` — `"0.00000010%@1000b"`
/// at 17. Pinned by the test below, mirroring how `BREAKER_COLUMN_WIDTH` pins `percent`
/// alone: widening the column here without the rendering, or the other way round, fails
/// that test.
pub(crate) const WINDOW_COLUMN_WIDTH: usize = 17;

pub struct Row {
    pub label: String,
    pub lane: U256,
    pub orientation: String,
    /// The pair's market as the file names it, `venue:symbol` (several, with weights, for an
    /// averaged mid), or `-` for a pair that streams none.
    pub stream: String,
    /// The pricing kind the pair's `[pairs.pricing]` names.
    pub pricing: String,
    /// The account this pair fills from, `target.vaultFor(token0, token1)`, or `None` when
    /// that could not be read. Vaults are per pair, which is the whole reason there is a
    /// column: a mis-set vault on one row of a multi-pair config is exactly the class of
    /// mistake `--check` exists to show before anything is published.
    pub vault: Option<Address>,
    /// This pair's `max_deviation` as a percentage, or `"-"` for a pair with no breaker.
    ///
    /// Reported because a typo'd or forgotten `max_deviation` on one row of a multi-pair
    /// config otherwise produces an unguarded pair that looks exactly like a guarded one.
    /// `--check` is the surface built to catch that class of mistake, and it was silent
    /// about the one setting whose whole justification is that an operator must never
    /// believe a pair is protected when it is not.
    pub breaker: String,
    /// This pair's windowed limit and its block count, or `"-"` for a pair without one.
    /// Reported for the same reason as `breaker`: an operator must never believe a pair is
    /// protected when it is not.
    pub window: String,
    /// The pair's registered guards, each as its kind and its factory's summary, or `"-"`.
    /// Shown as a column only when some pair has one, so a file with none renders as it
    /// always has.
    pub guards: String,
    pub key_env: String,
    pub updater: Option<Address>,
    pub authorized: Option<bool>,
    pub runway: Option<u64>,
    pub failures: Vec<String>,
    pub warnings: Vec<String>,
}

/// Width of the `guards` column, when it is shown.
const GUARDS_COLUMN_WIDTH: usize = 28;

/// `cap limit 2, dispersion 0.3%`, or `"-"` for a pair with no registered guard. The kind
/// table has already checked every stanza names a registered kind (`Kinds::check` runs
/// before the report), so a summary that cannot be made is a factory's own failure, shown
/// as such rather than hidden.
fn guards_cell(spec: &crate::config::PairSpec, kinds: &crate::kinds::Kinds, at: &str) -> String {
    if spec.guards.is_empty() {
        return "-".to_owned();
    }
    spec.guards
        .iter()
        .map(|stanza| {
            kinds
                .guard_summary(stanza, at)
                .unwrap_or_else(|_| format!("{} (summary failed)", stanza.kind))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `"20.00%@1000b"`, or `"-"` when this pair configures no window.
fn window_cell(breaker: Option<crate::breaker::BreakerConfig>) -> String {
    let Some(breaker) = breaker else {
        return "-".to_owned();
    };
    match (breaker.window_limit(), breaker.window) {
        (Some(limit), Some(window)) => format!("{limit}@{}b", window.blocks),
        _ => "-".to_owned(),
    }
}

pub struct Report {
    pub target: Address,
    pub registry: Address,
    /// `None` when the base fee could not be read, which makes every runway unknown
    /// rather than unlimited: a zero base fee and an unreadable one must not render the
    /// same way, since only one of them means every key is truly infinitely funded.
    pub base_fee: Option<U256>,
    /// Warnings about the report as a whole rather than one pair.
    pub warnings: Vec<String>,
    pub rows: Vec<Row>,
}

impl Report {
    /// Warnings do not fail the check; only failures do.
    pub fn ok(&self) -> bool {
        self.rows.iter().all(|row| row.failures.is_empty())
    }

    /// Every failure, one line each, named by its pair: what a refused reload tells the
    /// operator, so the reason is on the page rather than in a report only `--check` prints.
    pub fn failure_lines(&self) -> Vec<String> {
        self.rows
            .iter()
            .flat_map(|row| {
                row.failures
                    .iter()
                    .map(move |failure| format!("{}: {failure}", row.label))
            })
            .collect()
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "target    {:#x}", self.target);
        let _ = writeln!(out, "registry  {:#x}", self.registry);
        let _ = writeln!(
            out,
            "base fee  {}",
            self.base_fee
                .map_or_else(|| "unknown".to_owned(), |fee| format!("{fee} wei"))
        );
        for warning in &self.warnings {
            let _ = writeln!(out, "warn: {warning}");
        }
        let _ = writeln!(out);
        // The `guards` column exists only when some pair has a registered guard, between
        // `window` and `updater`: a file with none renders byte for byte as it always has.
        let show_guards = self.rows.iter().any(|row| row.guards != "-");
        let guards_cell = |cell: &str| {
            if show_guards {
                format!(
                    "{:<g$} ",
                    truncate(cell, GUARDS_COLUMN_WIDTH),
                    g = GUARDS_COLUMN_WIDTH
                )
            } else {
                String::new()
            }
        };
        let _ = writeln!(
            out,
            "{:<16} {:<14} {:<11} {:<18} {:<15} {:<11} {:<w$} {:<v$} {}{:<11} {:<5} {:>9}  key_env",
            "pair",
            "lane",
            "orientation",
            "stream",
            "pricing",
            "vault",
            "breaker",
            "window",
            guards_cell("guards"),
            "updater",
            "auth",
            "runway",
            w = BREAKER_COLUMN_WIDTH,
            v = WINDOW_COLUMN_WIDTH
        );
        for row in &self.rows {
            let lane = format!("{:#x}", row.lane);
            // Short, like the vault, so the table fits a terminal. The one time the full
            // address is needed, a key the target has not authorized, the `addUpdater` hint
            // under the table prints it in full.
            let updater = row.updater.map_or_else(|| "-".to_owned(), short_address);
            let auth = match row.authorized {
                Some(true) => "yes".to_owned(),
                Some(false) => "NO".to_owned(),
                None => "-".to_owned(),
            };
            let runway = match row.runway {
                Some(u64::MAX) => "unlimited".to_owned(),
                Some(n) if n < LOW_RUNWAY => format!("{n} low"),
                Some(n) => n.to_string(),
                // Distinct from "unlimited": this pair's balance was read, but the base
                // fee to price it against was not, so runway could not be computed.
                None => "unknown".to_owned(),
            };
            let vault = row.vault.map_or_else(|| "-".to_owned(), short_address);
            let _ = writeln!(
                out,
                "{:<16} {:<14} {:<11} {:<18} {:<15} {:<11} {:<w$} {:<v$} {}{:<11} {:<5} {:>9}  {}",
                // Width is a minimum, not a maximum, so an over-long label would push every
                // later column out of line. Truncated rather than trusted: `lane_label`
                // keeps the fallback short, but an ERC-20 free to return any string is not
                // this table's to trust.
                truncate(&row.label, 16),
                // Lanes are 32 bytes; the leading digits identify one uniquely enough to
                // cross-check against a block explorer.
                &lane[..lane.len().min(14)],
                row.orientation,
                truncate(&row.stream, 18),
                truncate(&row.pricing, 15),
                vault,
                // Width is a minimum, so an over-long value would push every later column
                // out of line — the one thing a table read row-against-row must not do.
                // `percent` extends its precision for sub-basis-point thresholds and can
                // return up to `0.00000010%`, which is what the width above accommodates;
                // truncation is the backstop, as it is for `label`.
                truncate(&row.breaker, BREAKER_COLUMN_WIDTH),
                truncate(&row.window, WINDOW_COLUMN_WIDTH),
                guards_cell(&row.guards),
                updater,
                auth,
                runway,
                row.key_env,
                w = BREAKER_COLUMN_WIDTH,
                v = WINDOW_COLUMN_WIDTH,
            );
        }
        for row in &self.rows {
            for failure in &row.failures {
                let _ = writeln!(out, "\nFAIL {}: {failure}", row.label);
            }
            for warning in &row.warnings {
                let _ = writeln!(out, "warn {}: {warning}", row.label);
            }
        }
        out
    }
}

/// Clips a label to `width` so a table column stays a column. Rust's `{:<n}` is a minimum
/// width, so anything longer silently shifts every field after it.
fn truncate(label: &str, width: usize) -> String {
    // By chars, not bytes: an ERC-20 symbol is free to be non-ASCII, and slicing a byte
    // range mid-codepoint panics.
    if label.chars().count() <= width {
        return label.to_owned();
    }
    label
        .chars()
        .take(width.saturating_sub(1))
        .chain(['…'])
        .collect()
}

/// `0x1234…abcd`: the two ends of an address, enough to recognise it against a config and
/// not enough to paste. Eleven characters, the width of the `vault` column.
fn short_address(address: Address) -> String {
    let hex = format!("{address:#x}");
    format!("{}…{}", &hex[..6], &hex[hex.len() - 4..])
}

/// One row of `--check`'s second table: what a pair would publish right now.
pub struct PriceRow {
    pub label: String,
    /// True when the lane's sorted token order reverses the declared one, so the published
    /// mid is the reciprocal of the market's own quote and needs the extra line below it.
    pub invert: bool,
    /// The `(delta, mid)` this pair would publish, or why it has none. A feed that never
    /// delivers is an `Err` and fails the check, because the run path aborts on the same
    /// condition — a `--check` that passes where the run refuses to start is the exact
    /// defect this table exists to catch.
    pub sample: Result<(U256, U256), String>,
    /// A line printed under the row when the figures need qualifying: a volatile pair's
    /// delta is shown without its volatility terms, and this says so.
    pub note: Option<String>,
    /// What each venue delivered, for a pair averaging more than one: `binance:ETHUSDC`
    /// and its mid in lane orientation, or `None` for a venue that had not delivered when
    /// the row was taken. Empty for a single-source pair, whose venue is the row.
    pub venues: Vec<(String, Option<U256>)>,
    /// Why one of the pair's `[[pairs.guards]]` could not be built, when one could not.
    /// The run builds every guard before the lane quotes, so a guard that cannot be built
    /// is a run that refuses to start, and the check fails on it as it fails on a feed
    /// that never delivers.
    pub guard_error: Option<String>,
}

/// How to word an inverted pair's reciprocal, given a `base/quote` label.
fn reciprocal_direction(label: &str) -> String {
    match label.split_once('/') {
        Some((base, quote)) => format!("{quote} per {base}"),
        // The lane fallback, when preflight could not read the tokens' symbols.
        None => "in the market's own direction".to_owned(),
    }
}

/// Whether every pair produced a price. Feeds are part of the check, so this is part of
/// the exit code.
pub fn prices_ok(rows: &[PriceRow]) -> bool {
    rows.iter()
        .all(|row| row.sample.is_ok() && row.guard_error.is_none())
}

/// Renders `--check`'s second table.
///
/// The reciprocal line under an inverted pair earns its place: `0.000249876` is
/// unverifiable by eye and `≈ 4001.98 USDC per WETH` is instantly either right or
/// obviously wrong. Printing the human-familiar direction is what makes an inverted lane
/// auditable at all.
pub fn render_prices(rows: &[PriceRow], price_decimals: u32) -> String {
    use crate::feed::{format_scaled, reciprocal_scaled};

    let mut out = String::new();
    let _ = writeln!(out);
    let _ = writeln!(out, "{:<16} {:<28} delta", "pair", "mid");
    for row in rows {
        match &row.sample {
            Err(err) => {
                let _ = writeln!(out, "{:<16} no price: {err}", truncate(&row.label, 16));
            }
            Ok((delta, mid)) => {
                let _ = writeln!(
                    out,
                    "{:<16} {:<28} {}",
                    truncate(&row.label, 16),
                    format_scaled(*mid, price_decimals),
                    format_scaled(*delta, price_decimals),
                );
                if row.invert {
                    let reciprocal = reciprocal_scaled(*mid, price_decimals).map_or_else(
                        || "not computable from this mid".to_owned(),
                        |value| format_scaled(value, price_decimals),
                    );
                    let _ = writeln!(
                        out,
                        "{:<16} ≈ {reciprocal} {}",
                        "",
                        reciprocal_direction(&row.label)
                    );
                }
                if let Some(note) = &row.note {
                    let _ = writeln!(out, "{:<16} {note}", "");
                }
            }
        }
        // A guard the run could not build, under the row whichever way the price went:
        // the check fails on it, and the operator reads why beside the pair.
        if let Some(err) = &row.guard_error {
            let _ = writeln!(out, "{:<16} guards: {err}", "");
        }
        // Under the row either way: a venue that never delivered is worth seeing beside a
        // row that has no price at all, since it is likely the reason.
        for (venue, mid) in &row.venues {
            let mid = mid.map_or_else(
                || "no price".to_owned(),
                |mid| format_scaled(mid, price_decimals),
            );
            let _ = writeln!(out, "{:<16}   {venue}: {mid}", "");
        }
    }
    out
}

use crate::{
    UPDATE_GAS_LIMIT, call_view,
    config::{Config, resolve_keys},
    decode_return_data,
};
use ethrex_l2_common::calldata::Value;
use ethrex_l2_sdk::calldata::encode_calldata;
use ethrex_rpc::{
    clients::eth::EthClient,
    types::block_identifier::{BlockIdentifier, BlockTag},
};
use eyre::Result;

const IS_UPDATER_SIG: &str = "isUpdater(address,address)";
const PAIR_VAULTS_SIG: &str = "pairVaults(uint256)";
const ERC20_SYMBOL_SIG: &str = "symbol()";
const BALANCE_OF_SIG: &str = "balanceOf(address)";

/// Reads an ERC-20 `symbol()`, or `None` when the token does not implement it in a form
/// we can decode.
///
/// Decoded as `bytes`, not `string`: ethrex's `decode_calldata` has no `string` type —
/// `DataType::parse` accepts address/bool/bytes/bytesN/uint*/int*/arrays/tuples and errors
/// with "unknown type string" on anything else. A dynamic `string` and `bytes` share an
/// identical ABI encoding, so decoding as `bytes` and converting is exact, not a hack.
/// A few pre-ERC-20-standard tokens return a fixed `bytes32` symbol instead, hence the
/// second attempt.
pub(crate) async fn token_symbol(client: &EthClient, token: Address) -> Option<String> {
    let calldata = encode_calldata(ERC20_SYMBOL_SIG, &[]).ok()?;
    let ret = call_view(client, token, calldata, None, None).await.ok()?;
    let bytes = match decode_return_data("r(bytes)", &ret).ok().as_deref() {
        Some([Value::Bytes(bytes)]) => bytes.to_vec(),
        // Legacy tokens (e.g. MKR-era) return bytes32, zero-padded on the right.
        _ => match decode_return_data("r(bytes32)", &ret).ok().as_deref() {
            Some([Value::FixedBytes(bytes)]) => bytes.to_vec(),
            _ => return None,
        },
    };
    let symbol = String::from_utf8(bytes).ok()?;
    let symbol = symbol.trim_end_matches('\0').to_owned();
    (!symbol.is_empty()).then_some(symbol)
}

/// Builds the report. Every check is recorded on its pair's row rather than aborting, so
/// a half-configured deployment still produces a full, actionable report.
pub async fn build_report(
    client: &EthClient,
    config: &Config,
    registry: Address,
    lookup: impl Fn(&str) -> Option<String>,
    kinds: &crate::kinds::Kinds,
) -> Result<Report> {
    let signers = resolve_keys(config, lookup);
    let mut warnings = Vec::new();

    // The parent block's own base fee, not `eth_gasPrice`: the latter adds a priority-tip
    // suggestion, and these updates pay no priority fee (`sign_update_tx` sets it to zero),
    // so pricing the runway against it would understate every pair's by the tip and print a
    // "base fee" no block ever had.
    //
    // A failed read must not default to a reassuring value: a zero base fee reads as
    // unlimited runway, so an RPC failure here must surface as "unknown", never as
    // "every key is infinitely funded".
    let base_fee = client
        .get_block_by_number(BlockIdentifier::Tag(BlockTag::Latest), false)
        .await
        .ok()
        .and_then(|block| block.header.base_fee_per_gas)
        .map(U256::from);
    if base_fee.is_none() {
        warnings.push(
            "could not read the latest block's base fee, so funding runway is unknown for \
             every pair; a zero base fee would have rendered as unlimited, which is why this \
             is reported rather than assumed"
                .to_owned(),
        );
    }

    // The target-side checks need a PropAMM, and a target is not required to be one, so a
    // non-contract target skips them rather than failing — but each reason for skipping is
    // reported, so "not a contract" is never confused with "the RPC failed" or with the
    // EIP-7702 case, where the target has code and still is not a contract. That last one is
    // what a run on a mainnet fork with an anvil account as its target actually hits; see
    // `target_warning`.
    let target_code = client
        .get_code(config.target, BlockIdentifier::Tag(BlockTag::Latest))
        .await;
    let target_kind = classify_target(target_code.as_deref().ok());
    warnings.extend(target_warning(config.target, target_kind));
    let target_has_code = target_kind == TargetKind::Contract;

    // `symbol()` is read per pair, and pairs share tokens — all three in the example config
    // quote against USDC, so an uncached read fetches the same answer three times. Memoised
    // across the whole report, including the `None` of a token that has no readable symbol,
    // so a missing one is not retried per pair either.
    let mut symbols: HashMap<Address, Option<String>> = HashMap::new();

    let mut rows = Vec::with_capacity(config.pairs.len());
    for (spec, signer) in config.pairs.iter().zip(signers) {
        let (base, quote) = spec.declared_base_quote();
        for token in [base, quote] {
            // Entry API rather than contains_key-then-insert: one lookup, and it keeps the
            // await inside the vacant branch so a cached token costs no work at all.
            if let std::collections::hash_map::Entry::Vacant(slot) = symbols.entry(token) {
                slot.insert(token_symbol(client, token).await);
            }
        }
        let base_symbol = symbols[&base].clone();
        let quote_symbol = symbols[&quote].clone();
        let label = match (&base_symbol, &quote_symbol) {
            (Some(b), Some(q)) => format!("{b}/{q}"),
            _ => crate::config::lane_label(spec.lane),
        };
        let orientation = if spec.invert { "inverted" } else { "direct" }.to_owned();
        let stream = spec
            .feeds
            .as_ref()
            .map_or_else(|| "-".to_owned(), |feeds| feeds.describe());

        let mut row = Row {
            label: label.clone(),
            lane: spec.lane,
            orientation,
            stream: stream.clone(),
            pricing: spec.pricing.kind.clone(),
            vault: None,
            breaker: spec
                .breaker
                .and_then(|breaker| breaker.tick_limit())
                .unwrap_or_else(|| "-".to_owned()),
            window: window_cell(spec.breaker),
            guards: guards_cell(spec, kinds, &label),
            key_env: spec.key_env.clone(),
            updater: None,
            authorized: None,
            runway: None,
            failures: Vec::new(),
            warnings: Vec::new(),
        };

        match signer {
            Err(err) => row.failures.push(format!("{err:#}")),
            Ok(signer) => {
                let updater = signer.address();
                row.updater = Some(updater);

                let calldata = encode_calldata(
                    IS_UPDATER_SIG,
                    &[Value::Address(config.target), Value::Address(updater)],
                )?;
                // Concurrently: the authorization answer and the balance are independent
                // reads, so paying their latency one after the other is pure waste on a
                // remote endpoint.
                //
                // A decode failure is as good as a call failure: whatever answered did not
                // return a clean bool, so it did not confirm authorization. Kept distinct
                // from a confirmed "no" — see `authorization_failure`.
                let (answer, balance) = tokio::join!(
                    call_view(client, registry, calldata, None, None),
                    client.get_balance(updater, BlockIdentifier::Tag(BlockTag::Latest)),
                );
                row.authorized = decode_one("r(bool)", answer.ok().as_deref())
                    .map(|value| value == Value::Bool(true));
                row.failures.extend(authorization_failure(
                    config.target,
                    updater,
                    row.authorized,
                ));

                let (runway, warning) = runway_row(updater, balance.ok(), base_fee);
                row.runway = runway;
                row.warnings.extend(warning);
            }
        }

        row.warnings.extend(band_warning(spec));
        if let Some(feeds) = &spec.feeds {
            row.warnings.extend(fx_warnings(feeds));
        }

        // A venue symbol is the one hand-written field that can be catastrophically wrong:
        // publishing BTC's price on the WETH lane is arbitraged within a block. Every source
        // is checked, since one wrong venue in an average is still a wrong price. The
        // decision itself lives in `check_symbol`, tested independently of the chain.
        if let Some(feeds) = &spec.feeds {
            let mut unreadable = false;
            for source in &feeds.sources {
                let check = check_symbol(
                    source.venue,
                    &source.symbol,
                    base_symbol.as_deref(),
                    quote_symbol.as_deref(),
                    spec.allow_symbol_mismatch,
                );
                row.warnings
                    .extend(quote_warning(source, &check, quote_symbol.as_deref()));
                match check {
                    SymbolCheck::Match
                    | SymbolCheck::MismatchAllowed { .. }
                    | SymbolCheck::StandIn { .. } => {}
                    SymbolCheck::Mismatch { expected } => {
                        // `check_symbol` only returns `Mismatch` when both symbols were read.
                        let b = base_symbol
                            .as_deref()
                            .expect("Mismatch implies both symbols were read");
                        let q = quote_symbol
                            .as_deref()
                            .expect("Mismatch implies both symbols were read");
                        row.failures.push(format!(
                            "{} stream {:?} does not match the declared pair {b}/{q}, which \
                             reads {expected:?} there; set allow_symbol_mismatch = true if \
                             this is deliberate",
                            source.venue, source.symbol
                        ));
                    }
                    SymbolCheck::Unreadable => unreadable = true,
                }
            }
            for leg in feeds.fx.iter().flat_map(|fx| &fx.legs) {
                row.failures
                    .extend(fx_leg_failure(leg, quote_symbol.as_deref()));
            }
            // The cross-check is the guard against publishing one asset's price on
            // another's lane, so losing it must be visible rather than silent. Once per
            // pair, not per source: the symbols are the pair's.
            if unreadable {
                row.warnings.push(
                    "could not read symbol() from both tokens, so the stream/token cross-check \
                     did not run for this pair"
                        .to_owned(),
                );
            }
        }

        // Both checks below can fail to run against a target that *is* a contract — an RPC
        // blip, a response that does not decode. A check that did not run is not a check
        // that passed, so each such path warns rather than leaving the row looking clean.
        if target_has_code {
            // One read answers both questions: pairVaults(lane) is the account this lane fills
            // from, and a pair is registered exactly when that is nonzero. It is the same slot
            // the pusher's `vaultFor` read resolves on the run path.
            let calldata = encode_calldata(PAIR_VAULTS_SIG, &[Value::Uint(spec.lane)])?;
            let answer = call_view(client, config.target, calldata, None, None)
                .await
                .ok();
            match check_registered_pair(answer.as_deref()) {
                RegisteredPair::Registered(vault) => {
                    row.vault = Some(vault);
                    // Both sides of the pair at once: two independent balanceOf reads.
                    let calldata = encode_calldata(BALANCE_OF_SIG, &[Value::Address(vault)])?;
                    let (token0_balance, token1_balance) = tokio::join!(
                        call_view(client, spec.tokens.0, calldata.clone(), None, None),
                        call_view(client, spec.tokens.1, calldata.clone(), None, None),
                    );
                    for (token, answer) in [
                        (spec.tokens.0, token0_balance),
                        (spec.tokens.1, token1_balance),
                    ] {
                        match decode_one("r(uint256)", answer.ok().as_deref()) {
                            Some(Value::Uint(balance)) if balance.is_zero() => {
                                row.warnings.push(format!(
                                    "vault {vault:#x} holds none of {token:#x}; swaps on this \
                                     pair revert with EmptyVault"
                                ))
                            }
                            Some(Value::Uint(_)) => {}
                            _ => row.warnings.push(format!(
                                "could not read balanceOf({vault:#x}) on {token:#x}, so the \
                                 vault-balance check did not run for that token"
                            )),
                        }
                    }
                }
                RegisteredPair::NotRegistered => row.failures.push(format!(
                    "lane {:#x} is not a registered pair on {:#x}; the target would never \
                     read it",
                    spec.lane, config.target
                )),
                RegisteredPair::Unreadable => row.warnings.push(format!(
                    "could not read pairVaults({:#x}) on {:#x}, so the registered-pair and \
                     vault-balance checks did not run for this pair",
                    spec.lane, config.target
                )),
            }
        }

        rows.push(row);
    }

    // After every row exists, because the check is about two rows at once.
    for (index, failure) in duplicate_updater_failures(&rows) {
        rows[index].failures.push(failure);
    }

    Ok(Report {
        target: config.target,
        registry,
        base_fee,
        warnings,
        rows,
    })
}
/// The warning a pair with no `min_mid`/`max_mid` earns, if it earns one. Carried over from
/// the single-pair CLI's --min-mid/--max-mid note: bounds are optional, so a pair without
/// them is the default and would otherwise be invisible. A fixed mid gets none: it cannot
/// drift, and its kind already proved it sits inside whatever band was declared. A pair
/// with a market does, and so does a kind with no market, which is bounded by less: only
/// `delta < 1` and a nonzero mid stand between its output and the chain.
fn band_warning(spec: &crate::config::PairSpec) -> Option<String> {
    if !spec.band.is_unset() {
        return None;
    }
    match (&spec.feeds, spec.pricing.kind.as_str()) {
        // A fixed mid cannot drift, and its kind checked it against the band at parse time.
        (None, "fixed") => None,
        (None, kind) => Some(format!(
            "no min_mid/max_mid set, and pricing `{kind}` has no market to be checked against, \
             so nothing bounds this pair's published mid but a nonzero value; whatever the \
             pricer computes goes on chain as-is"
        )),
        (Some(_), _) => Some(
            "no min_mid/max_mid set, so nothing bounds this pair's published mid; a corrupt \
             feed would go on chain as-is and the first fill would price against it"
                .to_owned(),
        ),
    }
}

/// What a source's symbol check says about the currency it is quoted in, if anything. A
/// market in another dollar (`StandIn`) is averaged as if at par, unless the source
/// declares that quote and the pair's FX legs convert it. A source declaring a quote whose
/// market reads in the pair's own quote token would be converted all the same.
fn quote_warning(
    source: &crate::config::Source,
    check: &SymbolCheck,
    quote_symbol: Option<&str>,
) -> Option<String> {
    let quote_symbol = quote_symbol.unwrap_or("the quote token");
    match (check, source.quote) {
        (SymbolCheck::StandIn { quoted_in }, Some(quote)) if quote.as_str() == quoted_in => None,
        (SymbolCheck::StandIn { quoted_in }, _) => Some(format!(
            "{} stream {:?} is quoted in {quoted_in}, not {quote_symbol}; averaged as if they \
             were at par",
            source.venue, source.symbol
        )),
        (SymbolCheck::Match, Some(quote)) => Some(format!(
            "{} stream {:?} reads as quoted in {quote_symbol}, but is set quote = {:?}; it is \
             divided by the FX rate as if it were",
            source.venue,
            source.symbol,
            quote.as_str()
        )),
        _ => None,
    }
}

/// Why an FX leg's symbol is wrong, if it is. A leg is the pair's quote token's market in
/// USD, not the pair's market: on a USDT pair `USDT/USD` is what is expected there, and
/// anything else is refused like a source naming the wrong asset. `allow_symbol_mismatch`
/// does not reach it: that key is the pair's opt-out for its sources, and a leg reading
/// the wrong asset converts every USD source by it. A token whose symbol could not be read
/// is the report's warning about the sources already.
fn fx_leg_failure(leg: &crate::config::FxLeg, quote_symbol: Option<&str>) -> Option<String> {
    match check_symbol(leg.venue, &leg.symbol, quote_symbol, Some("USD"), false) {
        SymbolCheck::Match | SymbolCheck::MismatchAllowed { .. } | SymbolCheck::Unreadable => None,
        SymbolCheck::StandIn { .. } | SymbolCheck::Mismatch { .. } => Some(format!(
            "fx {} stream {:?} is not {} in USD, which reads {:?} there",
            leg.venue,
            leg.symbol,
            quote_symbol.unwrap_or_default(),
            leg.venue
                .expected_symbol(quote_symbol.unwrap_or_default(), "USD")
        )),
    }
}

/// What a pair's `[pairs.fx]` is set up to do that it may not mean: a rate read from one
/// leg, which nothing checks against another, and a `min_sources` that only the converted
/// sources can meet, so the FX rate failing withdraws the whole pair instead of the USD
/// sources alone.
fn fx_warnings(feeds: &crate::config::Feeds) -> Vec<String> {
    let Some(fx) = &feeds.fx else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if fx.min_sources == 1 {
        warnings.push(
            "fx min_sources is 1, so the USD sources are converted by a single leg that no \
             other checks; a bad print there moves the mid by up to max_depeg times their \
             weight"
                .to_owned(),
        );
    }
    let unconverted = feeds.unconverted();
    if feeds.min_sources > unconverted {
        warnings.push(format!(
            "min_sources is {} but only {unconverted} sources need no FX rate, so the rate \
             failing withdraws the whole pair rather than leaving out the USD sources; set it \
             to at most {unconverted} to keep quoting on the rest",
            feeds.min_sources
        ));
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pair with no `min_mid`/`max_mid` says so when something could push its mid
    /// anywhere: a feed, or a custom pricer with no market, which nothing but `delta < 1`
    /// and a nonzero mid bounds (the core's mid-shift check needs a market to compare
    /// against). A static mid, or any pair with a band, does not.
    #[test]
    fn a_pair_nothing_bounds_is_warned_about() {
        let spec = |text: &str| {
            crate::config::parse_config(text, 18)
                .unwrap()
                .pairs
                .remove(0)
        };
        let custom = crate::config::tests_support::CUSTOM;
        let no_market = custom.replace("symbol  = \"ETHUSDC\"\n", "");
        assert!(!no_market.contains("symbol"), "{no_market}");
        let banded = no_market.replace("key_env", "min_mid = \"1000\"\nkey_env");

        let feed = band_warning(&spec(custom)).expect("a custom pricer over a feed");
        assert!(feed.contains("min_mid/max_mid"), "{feed}");
        let alone = band_warning(&spec(&no_market)).expect("a custom pricer with no market");
        assert!(
            alone.contains("min_mid/max_mid") && alone.contains("skewed"),
            "{alone}"
        );
        assert_eq!(band_warning(&spec(&banded)), None);
    }

    /// Builds the cell the way `rows` does, from a threshold and a block count. Named
    /// `cell` rather than `window_cell` so it cannot shadow the function under test.
    fn cell(window: Option<((&str, u32), u64)>) -> String {
        let breaker = window.map(
            |((fraction, decimals), blocks)| crate::breaker::BreakerConfig {
                threshold_scaled: None,
                window: Some(crate::breaker::WindowConfig {
                    threshold_scaled: crate::feed::parse_decimal_scaled(fraction, decimals)
                        .unwrap(),
                    blocks,
                    span: std::time::Duration::from_secs(blocks * crate::update::BLOCK_TIME_SECS),
                }),
                decimals,
            },
        );
        super::window_cell(breaker)
    }

    /// A configured window is reported, and a pair without one reads as unarmed rather
    /// than as blank — the same reason the `breaker` column exists at all.
    #[test]
    fn the_window_column_reports_both_halves_of_the_setting() {
        let armed = cell(Some((("0.20", 6), 1000)));
        assert_eq!(armed, "20.00%@1000b");
        assert!(
            armed.len() <= WINDOW_COLUMN_WIDTH,
            "{armed} overflows its column"
        );
        assert_eq!(cell(None), "-");
    }

    /// Mirrors `percent_never_exceeds_the_width_the_check_table_reserves` in `breaker.rs`
    /// for this column: `window_cell` appends `@<blocks>b` to `percent`'s own longest
    /// rendering, and `MAX_BREAKER_WINDOW_BLOCKS` caps the block count at four digits, so
    /// the longest legal cell is `percent`'s widest string plus up to six more characters.
    #[test]
    fn window_cell_never_exceeds_the_width_the_check_table_reserves() {
        // The block count comes from the cap, not a literal: raising
        // `MAX_BREAKER_WINDOW_BLOCKS` to five digits must fail this test rather than
        // silently truncate the column again.
        let blocks = crate::config::MAX_BREAKER_WINDOW_BLOCKS;
        let widest = cell(Some((("0.000000001", 9), blocks)));
        assert_eq!(widest, format!("0.00000010%@{blocks}b"));
        assert!(
            widest.len() <= WINDOW_COLUMN_WIDTH,
            "{widest} is {} chars, wider than the column reserves",
            widest.len()
        );
    }

    #[test]
    fn aliases_wrapped_tokens_to_their_binance_asset() {
        assert_eq!(crate::venues::unwrap_alias("WETH"), "ETH");
        assert_eq!(crate::venues::unwrap_alias("WBTC"), "BTC");
        // Unknown symbols compare literally.
        assert_eq!(crate::venues::unwrap_alias("USDC"), "USDC");
        assert_eq!(crate::venues::unwrap_alias("PEPE"), "PEPE");
    }

    #[test]
    fn expected_symbol_follows_the_declared_order() {
        // A pair declared WETH/USDC reads ETHUSDC, never USDCETH: order is what makes a
        // transposed tokens array detectable.
        assert_eq!(
            crate::venue::VenueId::Binance.expected_symbol("WETH", "USDC"),
            "ETHUSDC"
        );
        assert_eq!(
            crate::venue::VenueId::Binance.expected_symbol("USDC", "WETH"),
            "USDCETH"
        );
        assert_eq!(
            crate::venue::VenueId::Binance.expected_symbol("WBTC", "USDC"),
            "BTCUSDC"
        );
        assert_eq!(
            crate::venue::VenueId::Binance.expected_symbol("USDC", "USDT"),
            "USDCUSDT"
        );
    }

    #[test]
    fn check_symbol_matches_every_real_pair() {
        assert_eq!(
            check_symbol(
                crate::venue::VenueId::Binance,
                "ETHUSDC",
                Some("WETH"),
                Some("USDC"),
                false
            ),
            SymbolCheck::Match
        );
        assert_eq!(
            check_symbol(
                crate::venue::VenueId::Binance,
                "BTCUSDC",
                Some("WBTC"),
                Some("USDC"),
                false
            ),
            SymbolCheck::Match
        );
        assert_eq!(
            check_symbol(
                crate::venue::VenueId::Binance,
                "USDCUSDT",
                Some("USDC"),
                Some("USDT"),
                false
            ),
            SymbolCheck::Match
        );
    }

    /// A lowercase symbol is the canonical Binance *stream* name, and `spawn_feed`
    /// lowercases whatever it is given to build the URL — so a lowercase config entry
    /// streams prices perfectly. Comparing case-sensitively made that config a hard
    /// preflight failure that refused to start the run, and told the operator to set
    /// `allow_symbol_mismatch = true` on a pair that does not mismatch.
    #[test]
    fn check_symbol_ignores_case_because_the_feed_does() {
        for stream in ["ETHUSDC", "ethusdc", "EthUsdc"] {
            assert_eq!(
                check_symbol(
                    crate::venue::VenueId::Binance,
                    stream,
                    Some("WETH"),
                    Some("USDC"),
                    false
                ),
                SymbolCheck::Match,
                "{stream} is the same market as ETHUSDC"
            );
        }
        // Case tolerance must not extend to a genuinely different asset.
        assert_eq!(
            check_symbol(
                crate::venue::VenueId::Binance,
                "btcusdc",
                Some("WETH"),
                Some("USDC"),
                false
            ),
            SymbolCheck::Mismatch {
                expected: "ETHUSDC".to_owned()
            }
        );
    }

    /// Every kind but `Contract` skips the target-side checks, and every skip must say so
    /// — `Eoa` included, which is both the deliberate shape of a test run targeting a plain
    /// account and what a mistyped or not-yet-deployed PropAMM address looks like. Reporting nothing for it
    /// left a report with two un-run checks looking identical to a clean one.
    #[test]
    fn every_target_that_skips_the_chain_checks_says_why() {
        let target = Address::from_low_u64_be(0xbeef);
        assert_eq!(target_warning(target, TargetKind::Contract), None);

        let eoa = target_warning(target, TargetKind::Eoa).expect("a codeless target must warn");
        assert!(eoa.contains("no code"), "unexpected wording: {eoa}");

        // The three reasons must stay distinguishable: same skip, different causes, and
        // only one of them ("the RPC failed") leaves the target possibly fine.
        let unknown = target_warning(target, TargetKind::Unknown).expect("must warn");
        let delegated = target_warning(target, TargetKind::DelegatedEoa).expect("must warn");
        assert!(unknown.contains("RPC failed"));
        assert!(delegated.contains("7702"));
        for warning in [&eoa, &unknown, &delegated] {
            assert!(
                warning.contains("skipped") && warning.contains(&format!("{target:#x}")),
                "every warning must name the skip and the target: {warning}"
            );
        }
        assert_ne!(eoa, unknown);
        assert_ne!(eoa, delegated);
    }

    /// `U256`'s `*` panics on overflow and the base fee comes off the wire, so this must
    /// answer rather than abort. Zero is not a fallback here — it is the correct answer, as
    /// no balance can cover a per-update cost that does not fit a uint256.
    #[test]
    fn runway_survives_a_base_fee_that_overflows_the_cost_of_an_update() {
        assert_eq!(runway_updates(U256::MAX, UPDATE_GAS_LIMIT, U256::MAX), 0);
        // Just inside the boundary the product still fits, so a real answer comes back.
        let biggest_priceable = U256::MAX / U256::from(UPDATE_GAS_LIMIT);
        assert_eq!(
            runway_updates(U256::MAX, UPDATE_GAS_LIMIT, biggest_priceable),
            1
        );
    }

    #[test]
    fn check_symbol_flags_a_stream_naming_a_different_asset() {
        // The case this check exists for: publishing BTC's price on the WETH/USDC lane.
        let result = check_symbol(
            crate::venue::VenueId::Binance,
            "BTCUSDC",
            Some("WETH"),
            Some("USDC"),
            false,
        );
        assert_eq!(
            result,
            SymbolCheck::Mismatch {
                expected: "ETHUSDC".to_owned()
            }
        );
    }

    #[test]
    fn check_symbol_flags_a_transposed_pair() {
        // Same tokens, reversed order: the declared orientation is what makes this
        // detectable, so it gets its own case rather than folding into the mismatch above.
        let result = check_symbol(
            crate::venue::VenueId::Binance,
            "ETHUSDC",
            Some("USDC"),
            Some("WETH"),
            false,
        );
        assert_eq!(
            result,
            SymbolCheck::Mismatch {
                expected: "USDCETH".to_owned()
            }
        );
    }

    /// A USD market on a USDT pair is averaged at par, and said so, unless the source
    /// declares `quote = "USD"`, which the FX legs convert. A source declaring USD whose
    /// market reads in USDT would be converted all the same, which is said too.
    #[test]
    fn a_declared_usd_market_is_converted_and_not_warned_as_at_par() {
        use crate::config::{Quote, Source};
        use crate::venue::VenueId;
        let source = |symbol: &str, quote| Source {
            venue: VenueId::Coinbase,
            symbol: symbol.to_owned(),
            weight: U256::one(),
            quote,
        };
        let warning = |source: &Source| {
            let check = check_symbol(
                source.venue,
                &source.symbol,
                Some("WETH"),
                Some("USDT"),
                false,
            );
            quote_warning(source, &check, Some("USDT"))
        };
        let at_par = warning(&source("ETH-USD", None)).expect("a USD market at par is warned");
        assert!(
            at_par.contains("averaged as if they were at par"),
            "{at_par}"
        );
        assert_eq!(warning(&source("ETH-USD", Some(Quote::Usd))), None);
        let usdt = warning(&source("ETH-USDT", Some(Quote::Usd)))
            .expect("a USDT market set to quote USD is warned");
        assert!(usdt.contains("divided by the FX rate"), "{usdt}");
        assert_eq!(warning(&source("ETH-USDT", None)), None);
    }

    /// An FX leg is read against the quote token's USD market, not the pair's: USDT/USD on
    /// a USDT pair is expected there, any other asset is refused (the pair's
    /// `allow_symbol_mismatch` is for its sources, not its legs), and a token whose symbol
    /// could not be read is not a leg's failure.
    #[test]
    fn an_fx_leg_is_checked_against_the_quote_tokens_usd_market() {
        use crate::config::FxLeg;
        use crate::venue::VenueId;
        let leg = |venue, symbol: &str| FxLeg {
            venue,
            symbol: symbol.to_owned(),
        };
        assert_eq!(
            fx_leg_failure(&leg(VenueId::Kraken, "USDT/USD"), Some("USDT")),
            None
        );
        assert_eq!(
            fx_leg_failure(&leg(VenueId::Coinbase, "USDT-USD"), Some("USDT")),
            None
        );
        let wrong = fx_leg_failure(&leg(VenueId::Kraken, "BTC/USD"), Some("USDT"))
            .expect("another asset is refused");
        assert!(wrong.contains("\"USDT/USD\""), "{wrong}");
        assert_eq!(
            fx_leg_failure(&leg(VenueId::Kraken, "USDT/USD"), None),
            None
        );
    }

    /// `[pairs.fx]` is warned about when one leg sets the rate, which nothing cross-checks,
    /// and when the pair's `min_sources` needs a converted source, so the rate failing
    /// withdraws the whole pair. A pair without the stanza earns neither.
    #[test]
    fn an_fx_stanza_is_warned_about_when_it_cannot_do_what_it_is_for() {
        let feeds = |min_sources: &str, fx_min: &str| {
            let text = format!(
                r#"
target = "0x00000000000000000000000000000000000000aa"
[[pairs]]
tokens = ["0x00000000000000000000000000000000000000bb", "0x00000000000000000000000000000000000000cc"]
key_env = "K"
pricing = {{ kind = "feed" }}
min_sources = "{min_sources}"
sources = [
  {{ venue = "coinbase", symbol = "ETH-USD", quote = "USD" }},
  {{ venue = "okx", symbol = "ETH-USDT" }},
  {{ venue = "bybit", symbol = "ETHUSDT" }},
]
[pairs.fx]
min_sources = "{fx_min}"
sources = [
  {{ venue = "kraken", symbol = "USDT/USD" }},
  {{ venue = "coinbase", symbol = "USDT-USD" }},
]
"#
            );
            crate::config::parse_config(&text, 18).unwrap().pairs[0]
                .feeds
                .clone()
                .unwrap()
        };
        assert!(fx_warnings(&feeds("2", "2")).is_empty());
        let one_leg = fx_warnings(&feeds("2", "1"));
        assert!(
            matches!(one_leg.as_slice(), [w] if w.contains("fx min_sources is 1")),
            "{one_leg:?}"
        );
        let halts = fx_warnings(&feeds("3", "2"));
        assert!(
            matches!(halts.as_slice(), [w] if w.contains("only 2 sources need no FX rate")),
            "{halts:?}"
        );
        assert!(fx_warnings(&crate::config::Feeds::single_binance("ETHUSDT")).is_empty());
    }

    /// A USDC or USDT pair read from the base's USDC, USDT or USD market passes without the
    /// opt-out, as a stand-in; a different base never does, nor a stand-in for any other
    /// quote.
    #[test]
    fn a_dollar_stand_in_passes_and_a_wrong_asset_does_not() {
        use crate::venue::VenueId;
        let check =
            |venue, stream, quote| check_symbol(venue, stream, Some("WETH"), Some(quote), false);
        assert_eq!(
            check(VenueId::Coinbase, "ETH-USD", "USDC"),
            SymbolCheck::StandIn {
                quoted_in: "USD".into()
            }
        );
        assert_eq!(
            check(VenueId::Okx, "ETH-USDT", "USDC"),
            SymbolCheck::StandIn {
                quoted_in: "USDT".into()
            }
        );
        assert_eq!(
            check(VenueId::Binance, "ethusdc", "USDT"),
            SymbolCheck::StandIn {
                quoted_in: "USDC".into()
            }
        );
        // The wrong asset is still a mismatch, in any dollar.
        assert!(matches!(
            check(VenueId::Coinbase, "BTC-USD", "USDC"),
            SymbolCheck::Mismatch { .. }
        ));
        assert!(matches!(
            check(VenueId::Binance, "BTCUSDC", "USDC"),
            SymbolCheck::Mismatch { .. }
        ));
        // Other dollars, and other quotes, are not stand-ins.
        assert!(matches!(
            check(VenueId::Binance, "ETHFDUSD", "USDC"),
            SymbolCheck::Mismatch { .. }
        ));
        assert!(matches!(
            check_symbol(
                VenueId::Binance,
                "ETHUSDT",
                Some("WETH"),
                Some("DAI"),
                false
            ),
            SymbolCheck::Mismatch { .. }
        ));
    }

    #[test]
    fn check_symbol_lets_allow_mismatch_downgrade_a_real_mismatch() {
        // Identical inputs to the mismatch case above, but for the opt-out flag: proves
        // allow_mismatch is what changes the outcome, and nothing else.
        let result = check_symbol(
            crate::venue::VenueId::Binance,
            "BTCUSDC",
            Some("WETH"),
            Some("USDC"),
            true,
        );
        assert_eq!(
            result,
            SymbolCheck::MismatchAllowed {
                expected: "ETHUSDC".to_owned()
            }
        );
    }

    #[test]
    fn check_symbol_is_unreadable_when_either_symbol_is_missing_even_with_allow_mismatch() {
        // allow_symbol_mismatch means "I know this stream differs from the tokens"; it
        // must not be readable as "skip this check silently" when the check cannot even
        // run. Covers both partial cases and both-missing, each with the flag on and off.
        for allow_mismatch in [false, true] {
            assert_eq!(
                check_symbol(
                    crate::venue::VenueId::Binance,
                    "ETHUSDC",
                    None,
                    Some("USDC"),
                    allow_mismatch
                ),
                SymbolCheck::Unreadable
            );
            assert_eq!(
                check_symbol(
                    crate::venue::VenueId::Binance,
                    "ETHUSDC",
                    Some("WETH"),
                    None,
                    allow_mismatch
                ),
                SymbolCheck::Unreadable
            );
            assert_eq!(
                check_symbol(
                    crate::venue::VenueId::Binance,
                    "ETHUSDC",
                    None,
                    None,
                    allow_mismatch
                ),
                SymbolCheck::Unreadable
            );
        }
    }

    /// The 0xef0100 designator is what an EIP-7702 delegated EOA carries. It has code and
    /// is not a contract, so equating the two would run PropAMM checks against whatever it
    /// currently delegates to.
    #[test]
    fn a_delegated_eoa_is_not_mistaken_for_a_contract() {
        // The real code at the Makefile's TARGET_ADDR (anvil account 1) on mainnet, read
        // with `cast code 0x70997970C51812dc3A010C7d01b50e0d17dc79C8`. 0xef0100 followed
        // by the 20-byte delegate address.
        let delegated =
            hex::decode("ef01000e04736a85433445ef602d07946671685ec94647").expect("valid hex");
        assert_eq!(delegated.len(), 23);
        assert_eq!(
            classify_target(Some(&delegated)),
            TargetKind::DelegatedEoa,
            "the target-side checks cannot apply to a delegated EOA"
        );
    }

    #[test]
    fn classify_target_separates_the_four_states_the_report_renders_differently() {
        // Real contract bytecode starts with the constructor/dispatcher, never 0xef.
        assert_eq!(
            classify_target(Some(&hex::decode("60806040523480156100").unwrap())),
            TargetKind::Contract
        );
        assert_eq!(classify_target(Some(&[])), TargetKind::Eoa);
        // A failed read is its own state: never silently promoted to "contract".
        assert_eq!(classify_target(None), TargetKind::Unknown);
        // Both halves of the designator are required. 23 bytes with another prefix is
        // ordinary (if implausible) code, and the prefix on any other length is not a
        // designator — neither may be waved through as a delegation.
        let mut wrong_prefix = vec![0u8; 23];
        wrong_prefix[..3].copy_from_slice(&[0xef, 0x01, 0x01]);
        assert_eq!(classify_target(Some(&wrong_prefix)), TargetKind::Contract);
        let mut wrong_length = vec![0u8; 24];
        wrong_length[..3].copy_from_slice(&[0xef, 0x01, 0x00]);
        assert_eq!(classify_target(Some(&wrong_length)), TargetKind::Contract);
    }

    #[test]
    fn an_unreadable_registered_pair_check_is_not_a_failed_one() {
        let encode =
            |vault| encode_calldata("r(address)", &[Value::Address(vault)]).unwrap()[4..].to_vec();
        let vault = Address::from_low_u64_be(0xfeed);
        assert_eq!(
            check_registered_pair(Some(&encode(vault))),
            RegisteredPair::Registered(vault)
        );
        // The one hard failure: the target answered, and the answer was "no such pair".
        assert_eq!(
            check_registered_pair(Some(&encode(Address::zero()))),
            RegisteredPair::NotRegistered
        );
        // The call itself failed — the check did not run, which is not the same as the
        // target saying the lane is unregistered, and must not render as a clean row.
        assert_eq!(check_registered_pair(None), RegisteredPair::Unreadable);
        // Answered, but with something that is not an address.
        assert_eq!(
            check_registered_pair(Some(b"not abi encoded")),
            RegisteredPair::Unreadable
        );
    }

    #[test]
    fn decode_one_treats_a_failed_call_and_an_undecodable_answer_alike() {
        let vault = Address::from_low_u64_be(0xfeed);
        let encoded =
            encode_calldata("r(address)", &[Value::Address(vault)]).unwrap()[4..].to_vec();
        assert_eq!(
            decode_one("r(address)", Some(&encoded)),
            Some(Value::Address(vault))
        );
        assert_eq!(decode_one("r(address)", None), None);
        assert_eq!(decode_one("r(address)", Some(b"garbage")), None);
        // Decoded against the wrong type: an answer that does not fit is not an answer.
        assert_eq!(decode_one("r(bool)", Some(b"")), None);
    }

    #[test]
    fn two_pairs_sharing_a_signer_address_fail_and_name_each_other() {
        let shared = Address::from_low_u64_be(0xaaa1);
        let mut first = row("WETH/USDC", Some(true), Some(4_100));
        first.key_env = "UPDATER_KEY_WETH_USDC".to_owned();
        first.updater = Some(shared);
        let mut second = row("WBTC/USDC", Some(true), Some(3_880));
        second.key_env = "UPDATER_KEY_WBTC_USDC".to_owned();
        second.updater = Some(shared);
        // Otherwise identical to the two above — authorized, funded, its key resolved —
        // so the only thing that can spare it is having its own address.
        let mut third = row("USDC/USDT", Some(true), Some(2_000));
        third.key_env = "UPDATER_KEY_USDC_USDT".to_owned();
        third.updater = Some(Address::from_low_u64_be(0xbbb2));

        let mut rows = vec![first, second, third];
        let failures = duplicate_updater_failures(&rows);
        assert_eq!(
            failures.len(),
            2,
            "both participants must be flagged: {failures:?}"
        );
        for (index, failure) in failures {
            rows[index].failures.push(failure);
        }
        assert!(rows[0].failures[0].contains("UPDATER_KEY_WBTC_USDC"));
        assert!(rows[1].failures[0].contains("UPDATER_KEY_WETH_USDC"));
        assert!(
            rows[2].failures.is_empty(),
            "the pair with its own key must stay clean"
        );

        // And the report the operator sees says so, rather than exiting 0 on a config the
        // run then refuses to start.
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows,
        };
        assert!(!report.ok(), "a duplicate signer must fail --check");
    }

    #[test]
    fn pairs_whose_keys_did_not_resolve_are_not_duplicates_of_each_other() {
        // Two rows with no address at all — the bootstrap case, where nothing is set yet.
        // Reporting them as sharing a key would be a fabricated failure on the exact run
        // an operator uses to find out what to configure.
        let mut first = row("WETH/USDC", None, None);
        first.updater = None;
        let mut second = row("WBTC/USDC", None, None);
        second.updater = None;
        assert!(duplicate_updater_failures(&[first, second]).is_empty());
    }

    #[test]
    fn distinct_signer_addresses_produce_no_failures() {
        let mut first = row("WETH/USDC", Some(true), Some(4_100));
        first.updater = Some(Address::from_low_u64_be(0xaaa1));
        let mut second = row("WBTC/USDC", Some(true), Some(3_880));
        second.updater = Some(Address::from_low_u64_be(0xbbb2));
        assert!(duplicate_updater_failures(&[first, second]).is_empty());
    }

    /// The addresses an operator has to fund, authorize and monitor must be visible on a
    /// report where nothing is wrong — every other mention of them lives inside a failure
    /// or a warning that a healthy deployment never produces.
    #[test]
    fn a_healthy_report_still_shows_every_signer_address() {
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![
                row("WETH/USDC", Some(true), Some(4_100)),
                row("WBTC/USDC", Some(true), Some(3_880)),
            ],
        };
        assert!(report.ok(), "this report has nothing wrong with it");
        let text = report.render();
        assert!(
            text.contains("updater"),
            "the column needs a header: {text}"
        );
        // Short, like the vault, so the table fits a terminal; the `addUpdater` hint prints
        // the address in full when the target does not know it.
        assert!(
            text.contains(&short_address(Address::from_low_u64_be(0xaaa1))),
            "the signer address must appear on a clean row: {text}"
        );
    }

    #[test]
    fn a_row_whose_key_did_not_resolve_renders_no_address() {
        let mut unset = row("USDC/USDT", None, None);
        unset.updater = None;
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![unset],
        };
        let text = report.render();
        assert!(text.contains("USDC/USDT"));
        assert!(
            !text.contains("0x0000000000000000000000000000000000000000"),
            "an unresolved key must not render as the zero address: {text}"
        );
    }

    /// A guard the run could not build fails the check and is shown under its row: the
    /// contract is that `--check` passes only where the run would start.
    #[test]
    fn a_guard_that_cannot_be_built_fails_the_check_and_is_shown() {
        let rows = vec![PriceRow {
            label: "WETH/USDC".to_owned(),
            invert: false,
            sample: Ok((U256::from(1u64), U256::from(2u64))),
            note: None,
            venues: Vec::new(),
            guard_error: Some(
                "[[pairs.guards]] `share_band`: the pricer declared no diagnostic `base_share`"
                    .to_owned(),
            ),
        }];
        assert!(
            !prices_ok(&rows),
            "a guard that cannot build is a run that cannot start"
        );
        let text = render_prices(&rows, 18);
        assert!(
            text.contains("guards: [[pairs.guards]] `share_band`"),
            "{text}"
        );
        // A feed that never delivered and a guard that cannot build are two failures, and
        // the row shows both rather than stopping at the first.
        let both = vec![PriceRow {
            label: "WETH/USDC".to_owned(),
            invert: false,
            sample: Err("no price for ETHUSDC within 15s".to_owned()),
            note: None,
            venues: Vec::new(),
            guard_error: Some("[[pairs.guards]] `spread_cap`: no such kind".to_owned()),
        }];
        let text = render_prices(&both, 18);
        assert!(
            text.contains("no price: no price for ETHUSDC")
                && text.contains("guards: [[pairs.guards]] `spread_cap`"),
            "{text}"
        );
    }

    /// An inverted lane publishes a number nobody can check by eye. The reciprocal line is
    /// what makes it auditable, so it is the thing to assert.
    #[test]
    fn an_inverted_pair_prints_the_price_in_its_market_direction() {
        let mid = crate::feed::parse_decimal_scaled("0.000249876", 18).unwrap();
        let delta = crate::feed::parse_decimal_scaled("0.00005", 18).unwrap();
        let rows = vec![PriceRow {
            label: "WETH/USDC".to_owned(),
            invert: true,
            sample: Ok((delta, mid)),
            note: None,
            venues: Vec::new(),
            guard_error: None,
        }];
        assert!(prices_ok(&rows));
        let text = render_prices(&rows, 18);
        assert!(text.contains("0.000249876"), "the published mid: {text}");
        assert!(text.contains("0.00005"), "the published delta: {text}");
        // ~1/0.000249876, in the direction the label reads.
        assert!(text.contains("4001.9"), "the reciprocal: {text}");
        assert!(text.contains("USDC per WETH"), "which direction: {text}");
    }

    #[test]
    fn a_direct_pair_prints_no_reciprocal() {
        // Same shape of row as the inverted case above, differing only in `invert`, so
        // the absence of the extra line can only come from the orientation.
        let mid = crate::feed::parse_decimal_scaled("0.9998", 18).unwrap();
        let delta = crate::feed::parse_decimal_scaled("0.0005", 18).unwrap();
        let rows = vec![PriceRow {
            label: "USDC/USDT".to_owned(),
            invert: false,
            sample: Ok((delta, mid)),
            note: None,
            venues: Vec::new(),
            guard_error: None,
        }];
        let text = render_prices(&rows, 18);
        assert!(text.contains("0.9998"));
        assert!(
            !text.contains('≈'),
            "a direct pair's mid already reads the familiar way: {text}"
        );
    }

    /// A feed that never delivers fails `--check`, because it also aborts the run. A check
    /// that passes where the run refuses to start is worse than no check.
    #[test]
    fn a_pair_whose_feed_delivered_nothing_fails_the_check_and_says_why() {
        let rows = vec![
            PriceRow {
                label: "USDC/USDT".to_owned(),
                invert: false,
                sample: Ok((U256::from(5u64), U256::from(9u64))),
                note: None,
                venues: Vec::new(),
                guard_error: None,
            },
            PriceRow {
                label: "WBTC/USDC".to_owned(),
                invert: false,
                sample: Err("no price for BTCUSDC within 15s".to_owned()),
                note: None,
                venues: Vec::new(),
                guard_error: None,
            },
        ];
        assert!(!prices_ok(&rows), "a dead feed must fail --check");
        let text = render_prices(&rows, 18);
        assert!(text.contains("WBTC/USDC"), "which pair: {text}");
        assert!(text.contains("no price for BTCUSDC"), "and why: {text}");
        // The pairs that did work are still listed, so one dead symbol does not hide the
        // rest of the table.
        assert!(text.contains("USDC/USDT"), "{text}");
    }

    #[test]
    fn a_pair_with_no_readable_symbols_still_gets_a_reciprocal_line() {
        // The lane fallback has no `base/quote` to word the direction with, so it says so
        // rather than printing a nonsense pairing.
        let rows = vec![PriceRow {
            label: "lane 0x85053f65".to_owned(),
            invert: true,
            sample: Ok((U256::from(1u64), U256::exp10(18))),
            note: None,
            venues: Vec::new(),
            guard_error: None,
        }];
        let text = render_prices(&rows, 18);
        assert!(text.contains("≈ 1 in the market's own direction"), "{text}");
    }

    /// An `isUpdater` call that failed did not answer "no". The remedy for a real "no" is a
    /// mainnet transaction, so handing it out for an unreadable answer costs money and
    /// teaches the operator nothing — they authorize an already-authorized key, watch the
    /// run fail again, and never suspect the endpoint.
    #[test]
    fn an_unreadable_authorization_check_does_not_tell_the_operator_to_call_add_updater() {
        let target = Address::from_low_u64_be(0x1234);
        let updater = Address::from_low_u64_be(0xaaa1);

        assert_eq!(authorization_failure(target, updater, Some(true)), None);

        // A confirmed "no" keeps the remedy, because there the remedy is right.
        let refused = authorization_failure(target, updater, Some(false))
            .expect("an unauthorized key must fail the check");
        assert!(
            refused.contains("addUpdater"),
            "unexpected wording: {refused}"
        );
        assert!(refused.contains(&format!("{updater:#x}")));

        // The same key, the same target, the same fail-closed outcome — only the answer's
        // readability differs, so only that can change the message.
        let unreadable = authorization_failure(target, updater, None)
            .expect("an unverifiable authorization must still fail the check");
        assert!(
            !unreadable.contains("addUpdater("),
            "an unreadable answer must not prescribe a mainnet transaction: {unreadable}"
        );
        assert!(
            unreadable.contains("could not check"),
            "and must say the check did not run: {unreadable}"
        );
    }

    /// A failed balance read used to become balance zero, which the report stated as
    /// "{updater} has no ETH; its updates cannot pay base fee" — definite, possibly false,
    /// and with a remedy that changes nothing. The mirror of the unreadable base fee that
    /// had to stop rendering as "unlimited".
    #[test]
    fn an_unreadable_balance_renders_as_unknown_rather_than_as_an_empty_key() {
        let updater = Address::from_low_u64_be(0xaaa1);
        let base_fee = U256::from(1_000_000_000u64);

        let (runway, warning) = runway_row(updater, None, Some(base_fee));
        assert_eq!(
            runway, None,
            "an unreadable balance must not price a runway at all"
        );
        let warning = warning.expect("and must not pass silently either");
        assert!(warning.contains("unknown"), "unexpected wording: {warning}");
        assert!(
            !warning.contains("has no ETH"),
            "an unreadable balance is not an empty one: {warning}"
        );
        // Rendered, the column reads `unknown` and never `0`.
        let mut row = row("WETH/USDC", Some(true), runway);
        row.warnings.push(warning);
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(base_fee),
            warnings: Vec::new(),
            rows: vec![row],
        };
        let text = report.render();
        assert!(text.contains("unknown"), "{text}");
        assert!(
            report.ok(),
            "an unreadable balance is a warning, not a failure"
        );
    }

    #[test]
    fn a_genuinely_empty_key_still_says_so() {
        // Same call as the unreadable case above but with a real, readable zero: the
        // "has no ETH" warning must survive the fix that stopped fabricating it.
        let updater = Address::from_low_u64_be(0xaaa1);
        let base_fee = U256::from(1_000_000_000u64);
        let (runway, warning) = runway_row(updater, Some(U256::zero()), Some(base_fee));
        assert_eq!(runway, Some(0));
        let warning = warning.expect("a key with no ETH must be warned about");
        assert!(
            warning.contains("has no ETH"),
            "unexpected wording: {warning}"
        );

        // And a low-but-nonzero balance keeps its own distinct warning.
        let per_update = U256::from(UPDATE_GAS_LIMIT) * base_fee;
        let (runway, warning) = runway_row(updater, Some(per_update * 10u64), Some(base_fee));
        assert_eq!(runway, Some(10));
        assert!(warning.expect("10 updates is low").contains("~10 updates"));

        // A comfortable balance says nothing at all.
        let (runway, warning) =
            runway_row(updater, Some(per_update * (LOW_RUNWAY + 1)), Some(base_fee));
        assert_eq!(runway, Some(LOW_RUNWAY + 1));
        assert_eq!(warning, None);
    }

    #[test]
    fn an_unreadable_base_fee_leaves_the_runway_unknown_without_a_per_row_warning() {
        // The report-level warning already covers every pair at once, so a per-row line
        // would be noise — but the runway must still be `None`, not a number.
        let updater = Address::from_low_u64_be(0xaaa1);
        let (runway, warning) = runway_row(updater, Some(U256::exp10(18)), None);
        assert_eq!(runway, None);
        assert_eq!(warning, None);
    }

    #[test]
    fn runway_counts_updates_a_balance_can_pay_for() {
        let gas = 36_740u64;
        let base_fee = U256::from(1_000_000_000u64); // 1 gwei
        let per_update = U256::from(gas) * base_fee;
        assert_eq!(runway_updates(per_update * 10u64, gas, base_fee), 10);
        // Truncates rather than rounding up: a partial update is not an update.
        assert_eq!(
            runway_updates(per_update * 10u64 - U256::one(), gas, base_fee),
            9
        );
        assert_eq!(runway_updates(U256::zero(), gas, base_fee), 0);
        // A zero base fee must not divide by zero.
        assert_eq!(runway_updates(per_update, gas, U256::zero()), u64::MAX);
    }

    /// A refused reload names every failure by its pair, so the reason reaches the page.
    #[test]
    fn failure_lines_name_each_failure_by_its_pair() {
        let mut bad = row("WETH/USDC", Some(true), Some(10_000));
        bad.failures
            .push("coinbase stream \"BTC-USD\" does not match the declared pair".to_owned());
        let good = row("USDC/USDT", Some(true), Some(10_000));
        let report = Report {
            target: Address::zero(),
            registry: Address::zero(),
            base_fee: None,
            warnings: Vec::new(),
            rows: vec![good, bad],
        };
        assert!(!report.ok());
        assert_eq!(
            report.failure_lines(),
            ["WETH/USDC: coinbase stream \"BTC-USD\" does not match the declared pair"]
        );
    }

    fn row(label: &str, authorized: Option<bool>, runway: Option<u64>) -> Row {
        Row {
            label: label.to_owned(),
            lane: U256::from(0x85053f65u64),
            orientation: "USDC/WETH inverted".to_owned(),
            stream: "ETHUSDC".to_owned(),
            pricing: "feed".to_owned(),
            vault: Some(Address::from_low_u64_be(0xfeed)),
            breaker: "-".to_owned(),
            window: "-".to_owned(),

            guards: "-".to_owned(),
            key_env: "UPDATER_KEY_WETH_USDC".to_owned(),
            updater: Some(Address::from_low_u64_be(0xaaa1)),
            authorized,
            runway,
            failures: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// `symbol()` returns a dynamic string, which ethrex cannot decode as `string` — it has
    /// no such type. `bytes` has an identical ABI encoding, so this proves the substitution
    /// round-trips rather than assuming it.
    #[test]
    fn a_dynamic_string_return_decodes_through_the_bytes_type() {
        // Imported locally: this test is written in Step 1, before the module-level
        // `Value` import arrives with the on-chain checks in Step 5.
        use ethrex_l2_common::calldata::Value;
        use ethrex_l2_sdk::calldata::encode_calldata;
        let encoded = encode_calldata(
            "r(bytes)",
            &[Value::Bytes("WETH".as_bytes().to_vec().into())],
        )
        .expect("encoding must succeed");
        // Return data carries no selector, so strip the one encode_calldata prepended.
        let decoded = crate::decode_return_data("r(bytes)", &encoded[4..]).expect("decode");
        match decoded.as_slice() {
            [Value::Bytes(bytes)] => {
                assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "WETH");
            }
            other => panic!("expected one Bytes value, got {other:?}"),
        }
        // And the type this code originally tried to use is genuinely unavailable.
        assert!(
            crate::decode_return_data("r(string)", &encoded[4..]).is_err(),
            "if ethrex gains a string type, token_symbol can be simplified"
        );
    }

    #[test]
    fn a_clean_report_is_ok_and_lists_every_pair() {
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![
                row("WETH/USDC", Some(true), Some(4_100)),
                row("WBTC/USDC", Some(true), Some(3_880)),
            ],
        };
        assert!(report.ok());
        let text = report.render();
        assert!(text.contains("WETH/USDC"));
        assert!(text.contains("WBTC/USDC"));
        // The lane column, truncated to its identifying leading digits.
        assert!(text.contains("0x85053f65"));
    }

    #[test]
    fn short_address_keeps_the_ends() {
        assert_eq!(
            short_address(Address::from_low_u64_be(0xfeed)),
            "0x0000…feed"
        );
    }

    /// Each row names the account its pair fills from. Vaults are per pair, so a table that
    /// showed one vault for the target would say nothing; a row whose vault could not be read
    /// shows a dash like every other unread column, and the warning says why.
    #[test]
    fn the_report_names_each_pairs_vault_in_short_form() {
        let with_vault = row("WETH/USDC", Some(true), Some(4_100));
        let mut without = row("WBTC/USDC", Some(true), Some(3_880));
        without.vault = None;
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![with_vault, without],
        };
        let text = report.render();
        let header = text.lines().find(|l| l.starts_with("pair ")).unwrap();
        assert!(header.contains(" vault "), "{header}");
        let weth = text.lines().find(|l| l.starts_with("WETH/USDC")).unwrap();
        assert!(weth.contains("0x0000…feed"), "{weth}");
        let wbtc = text.lines().find(|l| l.starts_with("WBTC/USDC")).unwrap();
        assert!(!wbtc.contains("0x0000…feed"), "{wbtc}");
    }

    #[test]
    fn a_failing_report_still_renders_every_row_and_is_not_ok() {
        // The bootstrap case: nothing is authorized, one key is missing entirely. The
        // report must still show all three rows so the operator can act on them.
        let mut unauthorized = row("WETH/USDC", Some(false), Some(4_100));
        unauthorized
            .failures
            .push("updater 0x0000…aaa1 is not authorized; call addUpdater(0x0000…aaa1)".to_owned());
        let mut unset = row("USDC/USDT", None, None);
        unset.updater = None;
        unset
            .failures
            .push("UPDATER_KEY_USDC_USDT is not set in the environment".to_owned());
        let mut unfunded = row("WBTC/USDC", Some(true), Some(0));
        unfunded.warnings.push("updater has no ETH".to_owned());

        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![unauthorized, unfunded, unset],
        };
        assert!(!report.ok(), "a report with failures must not be ok");
        let text = report.render();
        assert!(text.contains("WETH/USDC"));
        assert!(text.contains("WBTC/USDC"));
        assert!(text.contains("USDC/USDT"));
        assert!(text.contains("addUpdater"));
        assert!(text.contains("is not set"));
        assert!(text.contains("no ETH"));
    }

    #[test]
    fn warnings_alone_keep_the_report_ok() {
        let mut warned = row("WBTC/USDC", Some(true), Some(10));
        warned.warnings.push("vault holds no USDC".to_owned());
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_000_000_000u64)),
            warnings: Vec::new(),
            rows: vec![warned],
        };
        assert!(report.ok(), "warnings must not fail the check");
    }

    #[test]
    fn runway_renders_as_unknown_when_the_base_fee_could_not_be_read() {
        // A row whose runway is None because the base fee itself could not be read —
        // distinct from a row whose key was never resolved, but rendered the same way,
        // since in both cases runway genuinely is not known.
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: None,
            warnings: Vec::new(),
            rows: vec![row("WETH/USDC", Some(true), None)],
        };
        let text = report.render();
        assert!(text.contains("unknown"));
        assert!(
            !text.contains("unlimited"),
            "an unreadable base fee must never render as unlimited runway: {text}"
        );
    }

    #[test]
    fn a_genuine_zero_base_fee_still_renders_as_unlimited() {
        // A zero base fee is real on a local anvil with no fee market, so that path must
        // stay distinguishable from an RPC failure: same runway value, different base_fee.
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::zero()),
            warnings: Vec::new(),
            rows: vec![row("WETH/USDC", Some(true), Some(u64::MAX))],
        };
        assert!(report.render().contains("unlimited"));
    }

    #[test]
    fn render_shows_the_base_fee_value_or_unknown() {
        let readable = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: Some(U256::from(1_500_000_000u64)),
            warnings: Vec::new(),
            rows: vec![row("WETH/USDC", Some(true), Some(4_100))],
        };
        assert!(readable.render().contains("base fee  1500000000 wei"));

        let unreadable = Report {
            base_fee: None,
            ..readable
        };
        assert!(unreadable.render().contains("base fee  unknown"));
    }

    /// A `guards` column appears only when some pair configured a registered guard, so a
    /// file with none renders exactly as it always has.
    #[test]
    fn check_shows_a_guards_column_only_when_some_pair_has_one() {
        let plain = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: None,
            warnings: Vec::new(),
            rows: vec![
                row("WETH/USDC", Some(true), None),
                row("USDC/USDT", Some(true), None),
            ],
        };
        assert!(!plain.render().contains("guards"), "{}", plain.render());
        let mut guarded = plain;
        guarded.rows[1].guards = "cap limit 2, dispersion 0.3%".to_owned();
        let text = guarded.render();
        let header = text
            .lines()
            .find(|l| l.starts_with("pair "))
            .expect("header");
        assert!(
            header.contains(" window ") && header.contains(" guards "),
            "{header}"
        );
        assert!(text.contains("cap limit 2, dispersion 0.3%"), "{text}");
        let plain_row = text.lines().find(|l| l.starts_with("WETH/USDC")).unwrap();
        assert!(
            plain_row.contains(" - "),
            "the unguarded row shows `-` in the column: {plain_row}"
        );
    }

    #[test]
    fn report_level_warnings_are_rendered() {
        let report = Report {
            target: Address::from_low_u64_be(0x1234),
            registry: Address::from_low_u64_be(0xda7a),
            base_fee: None,
            warnings: vec!["could not read the base fee".to_owned()],
            rows: vec![row("WETH/USDC", Some(true), None)],
        };
        assert!(report.render().contains("could not read the base fee"));
        assert!(report.ok(), "warnings must not fail the check");
    }
}
